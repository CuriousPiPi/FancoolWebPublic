(function(){
  // 对外 API
  const API = { mount, render, resize, setOnXAxisChange, __ensureDock: () => ensureSpectrumDock() };

  // 内部状态
  let root = null;
  let chart = null;
  let onXAxisChange = null;

  let lastPayload = null;
  let lastOption  = null;
  let lastIsNarrow = null;
  let isFs = false;

  let spectrumRoot = null;
  let spectrumInner = null;
  let spectrumChart = null;
  let spectrumEnabled = false;
  let lastSpectrumOption = null;
  const spectrumModelCache = new Map();
  const SPECTRUM_X_MIN = 20;
  const SPECTRUM_X_MAX = 20000;
  const SPECTRUM_Y_MIN = -10;
  let spectrumResolutionMode = '1_48';
  let __spectrumRaf = null;
  let __skipSpectrumOnce = false;

  let __legendRailEl = null;
  let __legendScrollEl = null;
  let __legendActionsEl = null;
  let __specPending = false;
  let __specFetchInFlight = false;
  let __specRerunQueued = false;
  
  const NARROW_BREAKPOINT = 1024;   // 窄屏阈值（可按需调整）
  const NARROW_HYSTERESIS = 48;     // 迟滞窗口（像素），用于防抖
  const LEGEND_OFFSET = 50;     // Legend 顶部下移像素
  let spectrumDockEl = null;

// IEC 61260 频带工具：从 1/48 基础频带聚合到 1/3 / 1/12 等
const IEC_RENARD_MANTISSAS = {
  3:  [1.00, 2.00, 5.00],
  10: [1.00, 1.25, 1.60, 2.00, 2.50, 3.15, 4.00, 5.00, 6.30, 8.00],
  20: [1.00, 1.12, 1.25, 1.40, 1.60, 1.80, 2.00, 2.24, 2.50, 2.80,
       3.15, 3.55, 4.00, 4.50, 5.00, 5.60, 6.30, 7.10, 8.00, 9.00],
  40: [1.00, 1.06, 1.12, 1.18, 1.25, 1.32, 1.40, 1.50, 1.60, 1.70,
       1.80, 1.90, 2.00, 2.12, 2.24, 2.36, 2.50, 2.65, 2.80, 3.00,
       3.15, 3.35, 3.55, 3.75, 4.00, 4.25, 4.50, 4.75, 5.00, 5.30,
       5.60, 6.00, 6.30, 6.70, 7.10, 7.50, 8.00, 8.50, 9.00, 9.50]
};

function iecBandsPerDecadeFromNpo(nPerOct) {
  // 与后端 bands_per_decade_from_npo 完全一致
  return Math.round((nPerOct * 10) / 3);
}

function iecSnapToRenard(vals, bpd) {
  const mans = IEC_RENARD_MANTISSAS[bpd];
  if (!mans || !mans.length) return vals.slice();
  const mansArr = mans.map(Number);
  const out = [];
  for (let i = 0; i < vals.length; i++) {
    const f = Number(vals[i]);
    if (!(Number.isFinite(f) && f > 0)) continue;
    const e = Math.floor(Math.log10(f));
    const m = f / Math.pow(10, e);
    let bestIdx = 0;
    let bestErr = Infinity;
    for (let j = 0; j < mansArr.length; j++) {
      const err = Math.abs(mansArr[j] - m);
      if (err < bestErr) { bestErr = err; bestIdx = j; }
    }
    const snapped = mansArr[bestIdx] * Math.pow(10, e);
    out.push(snapped);
  }
  // 去重 + 排序
  const uniq = Array.from(new Set(out.map(v => Number(v.toFixed(6))))).sort((a,b)=>a-b);
  return uniq;
}

function iecMakeCenters(nPerOct, fmin, fmax) {
  const bpd = iecBandsPerDecadeFromNpo(nPerOct);
  const fMinClamped = Math.max(fmin, 1e-12);
  const fMaxClamped = Math.max(fmax, 1e-12);
  const kMin = Math.ceil(bpd * Math.log10(fMinClamped));
  const kMax = Math.floor(bpd * Math.log10(fMaxClamped));
  if (kMax < kMin) return [];
  const ks = [];
  for (let k = kMin; k <= kMax; k++) ks.push(k);
  const centersExact = ks.map(k => Math.pow(10, k / bpd));
  const centersNom = iecSnapToRenard(centersExact, bpd);
  return centersNom.filter(f => f >= fmin && f <= fmax);
}

function iecBandEdgesFromCenters(centers, nPerOct) {
  const bpd = iecBandsPerDecadeFromNpo(nPerOct);
  // IEC decimal grid: g = 10^(1/(2*bpd))
  const g = Math.pow(10, 1 / (2 * bpd));
  const f1 = [];
  const f2 = [];
  for (const fc of centers) {
    f1.push(fc / g);
    f2.push(fc * g);
  }
  return { f1, f2 };
}

function getSpectrumResolutionMode() {
  return spectrumResolutionMode;
}

/*
 * 将高分辨率 IEC 频带（例如 1/48 OCT）聚合成低分辨率 IEC 频带（1/3 或 1/12）。
 * 使用“按带宽重叠比例分配能量”的方式，保证能量守恒：
 *   - 每个细带先转换为能量 E_i，对应的实际带宽为 [f1Fine[i], f2Fine[i]]
 *   - 粗带 j 的能量为所有细带在 [f1Coarse[j], f2Coarse[j]] 区间内的重叠能量之和：
 *       overlap = max(0, min(f2Fine, f2Coarse) - max(f1Fine, f1Coarse))
 *       w = overlap / (f2Fine - f1Fine)
 *       Eacc_j += E_i * w
 *   - 最后把 Eacc_j 转回 dB。
 *
 * @param {number[]} fineCenters  高分辨率中心频数组（Hz）
 * @param {number[]} fineValuesDb 同长 dB 数组，每个元素是该带 A 计权声级（dB）
 * @param {number}  fineNPerOct   高分辨率 n_per_oct（例如 48）
 * @param {number}  coarseNPerOct 目标 n_per_oct（例如 3 或 12）
 * @returns {{centers:number[], valuesDb:number[]}}
 */
function downsampleSpectrumBands(fineCenters, fineValuesDb, fineNPerOct, coarseNPerOct) {
  const centersFine = fineCenters.map(Number);
  const valsFine = fineValuesDb.map(Number);
  const K = Math.min(centersFine.length, valsFine.length);
  if (!K || !Number.isFinite(fineNPerOct) || !Number.isFinite(coarseNPerOct)) {
    return { centers: [], valuesDb: [] };
  }

  // 1) 目标 IEC 频带中心 & 带宽（粗分辨率）
  const fmin = SPECTRUM_X_MIN;
  const fmax = SPECTRUM_X_MAX;
  const centersCoarse = iecMakeCenters(coarseNPerOct, fmin, fmax);
  if (!centersCoarse.length) {
    return { centers: [], valuesDb: [] };
  }
  const { f1: f1Coarse, f2: f2Coarse } = iecBandEdgesFromCenters(centersCoarse, coarseNPerOct);

  // 2) 细分频带边界（基于 fineNPerOct）
  const { f1: f1Fine, f2: f2Fine } = iecBandEdgesFromCenters(centersFine, fineNPerOct);

  // 3) 高分辨率带能量（线性域）
  const P0_2 = Math.pow(20e-6, 2);
  const EsFine = new Array(K);
  for (let i = 0; i < K; i++) {
    const dB = valsFine[i];
    if (!Number.isFinite(dB)) {
      EsFine[i] = 0;
    } else {
      EsFine[i] = P0_2 * Math.pow(10, dB / 10);
    }
  }

  const centersOut = [];
  const valsOut = [];

  // 4) 按重叠带宽比例，将细带能量分配到粗带
  for (let j = 0; j < centersCoarse.length; j++) {
    const loC = f1Coarse[j];
    const hiC = f2Coarse[j];

    let Eacc = 0;
    for (let i = 0; i < K; i++) {
      const E = EsFine[i];
      if (E <= 0) continue;

      const loF = f1Fine[i];
      const hiF = f2Fine[i];
      if (!(hiF > loF)) continue;

      // 细带与当前粗带的交集
      const lo = Math.max(loC, loF);
      const hi = Math.min(hiC, hiF);
      if (hi <= lo) continue; // 无重叠

      const overlap = hi - lo;
      const widthFine = hiF - loF || 1e-9;
      const w = overlap / widthFine;  // 重叠比例
      if (w > 0) {
        Eacc += E * w;
      }
    }

    if (Eacc > 0) {
      const dB = 10 * Math.log10(Eacc / P0_2);
      centersOut.push(centersCoarse[j]);
      valsOut.push(dB);
    }
  }

  return { centers: centersOut, valuesDb: valsOut };
}

// Register shared helper to window if not already defined
if (!window.__forceSpectrumDockFromUrl) {
  window.__forceSpectrumDockFromUrl = function(){
    try {
      const usp = new URLSearchParams(window.location.search);
      return usp.get('force_spectrum_dock') === '1';
    } catch(_) { return false; }
  };
}
function isSpectrumDockAllowed(){
  // URL 强制优先
  if (window.__forceSpectrumDockFromUrl?.()) return true;
  // 前端配置（可能尚未加载，未加载时视为 false）
  return !!(window.APP_CONFIG && window.APP_CONFIG.spectrumDockEnabled);
}

// Play-audio button visibility control (same pattern as spectrum dock)
if (!window.__forcePlayAudioFromUrl) {
  window.__forcePlayAudioFromUrl = function(){
    try {
      const usp = new URLSearchParams(window.location.search);
      return usp.get('force_play_audio') === '1';
    } catch(_) { return false; }
  };
}
function isPlayAudioAllowed(){
  // URL 强制优先
  if (window.__forcePlayAudioFromUrl?.()) return true;
  // 前端配置（可能尚未加载，未加载时视为 false）
  return !!(window.APP_CONFIG && window.APP_CONFIG.playAudioEnabled);
}

function ensureSpectrumDock() {
  // 若不允许显示，且之前已经有按钮 → 移除并返回
  if (!isSpectrumDockAllowed()) {
    if (spectrumDockEl && spectrumDockEl.parentElement) {
      try { spectrumDockEl.remove(); } catch(_) {}
      spectrumDockEl = null;
    }
    return null;
  }

  if (spectrumDockEl && spectrumDockEl.isConnected) return spectrumDockEl;

  const btn = document.createElement('button');
  btn.id = 'spectrumDock';
  btn.type = 'button';
  btn.className = 'spectrum-dock';
  btn.setAttribute('aria-label', '展开/收起频谱');

  btn.innerHTML = `
    <svg class="chev" viewBox="0 0 16 16" width="16" height="16" aria-hidden="true" focusable="false">
      <path d="M3.2 6.2a1 1 0 0 1 1.4 0L8 9.6l3.4-3.4a1 1 0 1 1 1.4 1.4L8.7 11.7a1 1 0 0 1-1.4 0L3.2 7.6a1 1 0 0 1 0-1.4z" fill="currentColor"></path>
    </svg>
    <span class="label">展开频谱</span>
  `;

  btn.addEventListener('click', (e) => {
    e.preventDefault();
    SpectrumController.setEnabled(!SpectrumController.isEnabled(), { animate: true });
  });

  const shell =
    document.getElementById('chart-settings') ||
    (root && root.closest('.fc-chart-container')) ||
    document.body;

  try { shell.appendChild(btn); } catch(_) { document.body.appendChild(btn); }

  spectrumDockEl = btn;
  syncSpectrumDockUi();
  return spectrumDockEl;
}

function syncSpectrumDockUi() {
  if (!spectrumDockEl) return;
  if (!isSpectrumDockAllowed()) {
    spectrumDockEl.style.visibility = 'hidden';
    return;
  }
  const open = !!spectrumEnabled;
  spectrumDockEl.classList.toggle('is-open', open);
  const label = spectrumDockEl.querySelector('.label');
  if (label) label.textContent = open ? '收起频谱' : '展开频谱';
  spectrumDockEl.style.visibility = 'visible';
}

function placeSpectrumDock() {
  if (!isSpectrumDockAllowed()) return;
  const el = ensureSpectrumDock();
  if (!el) return;
  const shell =
    document.getElementById('chart-settings') ||
    (root && root.closest('.fc-chart-container')) ||
    null;
  if (!shell) { el.style.visibility = 'hidden'; return; }
  if (el.parentElement !== shell) {
    try { shell.appendChild(el); } catch(_) {}
  }
  el.style.visibility = 'visible';
}

  let __specEpoch = 0;
  const __specTimers = new Set();
  function specBumpEpochAndClearTimers() {
    __specEpoch++;
    __specTimers.forEach(id => { try { clearTimeout(id); } catch(_) {} });
    __specTimers.clear();
  }
  function specSetTimeout(fn, ms) {
    const myEpoch = __specEpoch;
    const id = setTimeout(() => {
      __specTimers.delete(id);
      if (myEpoch === __specEpoch) fn();
    }, ms);
    __specTimers.add(id);
    return id;
  }

function ensureLegendRail(){
  const shell = document.getElementById('chart-settings') || (root && root.closest('.fc-chart-container')) || null;
  if (!shell) return null;
  shell.classList.add('chart-flex');

  let stack = shell.querySelector('.chart-stack');
  if (!stack) {
    stack = document.createElement('div');
    stack.className = 'chart-stack';

    if (root) stack.appendChild(root);
    if (spectrumRoot) stack.appendChild(spectrumRoot);
    shell.insertBefore(stack, shell.firstChild); 
  }

  if (__legendRailEl && __legendRailEl.isConnected) return __legendRailEl;

  const rail = document.createElement('aside');
  rail.id = 'legendRail';
  // rail-actions 放在 legend-scroll 之前（顶部）
  rail.innerHTML = `
    <div class="rail-actions"></div>
    <div class="legend-scroll" id="legendRailScroll"></div>
  `;
  shell.appendChild(rail);

  __legendRailEl = rail;
  __legendScrollEl = rail.querySelector('#legendRailScroll');
  __legendActionsEl = rail.querySelector('.rail-actions');
  return rail;
}

function updateLegendRailLayout(){
  const shell = document.getElementById('chart-settings') || (root && root.closest('.fc-chart-container')) || null;
  if (!shell) return;
  shell.classList.add('chart-flex');

  const narrow = layoutIsNarrow();
  const integrated = isIntegratedLegendMode();
  shell.classList.toggle('legend-integrated', integrated);
  shell.classList.toggle('is-narrow', !!narrow);
  if (!__legendRailEl) return;

  try {
    const topGap = narrow ? 0 : LEGEND_OFFSET;
    __legendRailEl.style.setProperty('--legend-top-gap', `${topGap}px`);
  } catch(_){}

  // rail-actions 显隐逻辑：仅在“全屏 + 集成 + 拟合开启”时隐藏
  const hideActions = isFs && integrated && showFitCurves;
  if (__legendActionsEl) {
    __legendActionsEl.style.display = hideActions ? 'none' : 'flex';
  }

  // 窄屏且非全屏直接 100%
  if (narrow && !isFs) {
    const prev = __legendRailEl.getAttribute('data-last-width');
    if (prev !== '0') {
      __legendRailEl.style.width = '100%';
      try { shell.style.setProperty('--legend-rail-w', '0px'); } catch(_){}
      __legendRailEl.setAttribute('data-last-width','0');
    }
    return;
  }

  const hostW = Math.max(0, Math.round(shell.getBoundingClientRect().width || 0));
  const baseW = (hostW < 200)
    ? Math.max(window.innerWidth || 0, document.documentElement.clientWidth || 0)
    : hostW;

  const sList = Array.isArray(lastPayload?.chartData?.series) ? lastPayload.chartData.series : [];
  const themeName = (lastPayload && lastPayload.theme) || (document.documentElement.getAttribute('data-theme') || 'light');
  const t = tokens(themeName);
  const fitOn = !!showFitCurves && !!sList.length && integrated;

  const applied = computeLegendRailWidth({
    sList,
    integrated,
    fitOn,
    t,
    baseW,
    hasPlayBtn: !!(integrated && fitOn && isPlayAudioAllowed())  // 集成 + 拟合模式下有播放按钮 + 允许显示
  });

  const lastW = __legendRailEl.getAttribute('data-last-width');
  if (lastW !== String(applied)) {
    __legendRailEl.style.width = `${applied}px`;
    if (integrated) {
      __legendRailEl.style.minWidth = '0px';
    }
    try { shell.style.setProperty('--legend-rail-w', `${applied}px`); } catch(_){}
    __legendRailEl.setAttribute('data-last-width', String(applied));
  }
}

function renderLegendRailItems(){
  ensureLegendRail();
  if (!__legendScrollEl) return;

  const sList = getSeriesArray();
  const selMap = getLegendSelectionMap();
  const isNarrowNow = layoutIsNarrow();
  const integrated = isIntegratedLegendMode();
  const fitOn = integrated && showFitCurves && sList.length;

  // 非集成或未开启拟合：基础 Legend
  if (!fitOn) {
    const items = sList.map(s => {
      const baseName = s.name || `${s.brand||''} ${s.model||''} - ${s.condition||''}`;
      return {
        name: baseName,
        brand: s.brand || '',
        model: s.model || '',
        condition: s.condition || '',
        color: s.color,
        selected: selMap ? (selMap[baseName] !== false) : true
      };
    });

    __legendScrollEl.innerHTML = items.map(it => {
      const base = (it.brand || it.model) ? `${it.brand} ${it.model}` : it.name;
      const offClass = it.selected ? '' : 'is-off';
      if (isNarrowNow) {
        // Check viewport width for narrow integrated mode
        const vw = window.innerWidth || document.documentElement.clientWidth || 0;
        const isVeryNarrow = vw < 500;
        
        if (integrated && isVeryNarrow) {
          // Very narrow + non-fullscreen integrated: condition on separate line, no prefix, lighter color
          return `
            <div class="legend-row hoverable-row ${offClass}" data-name="${it.name}">
              <span class="dot" style="background:${it.color}"></span>
              <span class="name has-l2" title="${base}${it.condition ? ' / ' + it.condition : ''}">
                <span class="l1">${base}</span>
                ${it.condition ? `<span class="l2" style="color: var(--text-secondary);">${it.condition}</span>` : ``}
              </span>
            </div>
          `;
        } else if (integrated) {
          // Non-fullscreen integrated but not very narrow: inline with " - " prefix, lighter color
          const condInline = it.condition ? `<span class="cond-inline" style="color: var(--text-secondary);"> - ${it.condition}</span>` : '';
          return `
            <div class="legend-row hoverable-row ${offClass}" data-name="${it.name}">
              <span class="dot" style="background:${it.color}"></span>
              <span class="name" title="${base}${it.condition ? ' / ' + it.condition : ''}">
                <span class="l1">${base}</span>${condInline}
              </span>
            </div>
          `;
        } else {
          // Narrow but not integrated (original behavior)
          const condInline = it.condition ? `<span class="cond-inline"> - ${it.condition}</span>` : '';
          return `
            <div class="legend-row hoverable-row ${offClass}" data-name="${it.name}">
              <span class="dot" style="background:${it.color}"></span>
              <span class="name" title="${base}${it.condition ? ' / ' + it.condition : ''}">
                <span class="l1">${base}</span>${condInline}
              </span>
            </div>
          `;
        }
      } else {
        return `
          <div class="legend-row hoverable-row ${offClass}" data-name="${it.name}">
            <span class="dot" style="background:${it.color}"></span>
            <span class="name has-l2" title="${base}${it.condition ? ' / ' + it.condition : ''}">
              <span class="l1">${base}</span>
              ${it.condition ? `<span class="l2">${it.condition}</span>` : ``}
            </span>
          </div>
        `;
      }
    }).join('');

    // 行交互
    __legendScrollEl.querySelectorAll('.legend-row').forEach(node => {
      const name = node.getAttribute('data-name') || '';
      node.addEventListener('click', () => {
        if (!name || !chart) return;
        stopAllSweepAudio();
        const sel = getLegendSelectionMap();
        const currentlyVisible = sel ? (sel[name] !== false) : true;
        const actionType = currentlyVisible ? 'legendUnSelect' : 'legendSelect';
        node.classList.toggle('is-off', currentlyVisible);
        try { chart.dispatchAction({ type: actionType, name }); } catch(_){}
        if (spectrumEnabled && spectrumChart) {
          try { spectrumChart.dispatchAction({ type: actionType, name }); } catch(_){}
        }
        if (showFitCurves) refreshFitPanel();
      });
      node.addEventListener('mouseenter', () => { if (name && chart) try { chart.dispatchAction({ type: 'highlight', seriesName: name }); } catch(_){} });
      node.addEventListener('mouseleave', () => { if (name && chart) try { chart.dispatchAction({ type: 'downplay', seriesName: name }); } catch(_){} });
    });

    return;
  }

  // 集成拟合模式：新的合并头部（标题 + 输入 + 单位 + 关闭按钮）
  const headHtml = `
    <div class="integrated-fit-head is-active">
      <div class="title">
        ${FIT_ALGO_NAME} 拟合当前位置
        <input id="fitXInputLegend" type="number" step="1" />
        <span id="fitXUnitLegend" class="unit"></span>
      </div>
      <button id="fitCloseBtnLegend" class="btn-close-top integrated" type="button" aria-label="关闭"></button>
    </div>
  `;
  __legendScrollEl.innerHTML = `
    <div class="legend-integrated-wrapper fit-on">
      ${headHtml}
      <div class="legend-fit-grid is-fit-on"></div>
    </div>
  `;

  // 绑定关闭按钮事件
  const btnCloseLegend = __legendScrollEl.querySelector('#fitCloseBtnLegend');
  if (btnCloseLegend) {
    btnCloseLegend.addEventListener('click', (e) => {
      e.preventDefault();
      e.stopPropagation();
      stopAllSweepAudio();
      showFitCurves = false;
      // 同步按钮状态（侧栏底部的实时拟合按钮）
      const btnsRoot = getById('fitButtons');
      const btnFit = btnsRoot ? btnsRoot.querySelector('#btnFit') : null;
      if (btnFit) btnFit.classList.remove('active');
      toggleFitUI(false);
      updateRailParkedState();
      repaintPointer();
      refreshFitPanel();
      updateLegendRailLayout();
      try { chart && chart.resize(); } catch(_){}
    });
  }
  // 内容实际渲染交给 refreshFitPanel
}

function syncLegendRailFromChart(){
  if (!__legendScrollEl) return;
  const sel = getLegendSelectionMap();
  __legendScrollEl.querySelectorAll('.legend-row').forEach(node => {
    const name = node.getAttribute('data-name');
    const selected = sel ? (sel[name] !== false) : true;
    node.classList.toggle('is-off', !selected);
  });
}

function updateLegendRail(){
  ensureLegendRail();
  renderLegendRailItems();
  updateLegendRailLayout();

  // 将 #fitButtons 放到 rail 底部
  const btns = getById('fitButtons');
  if (btns && __legendActionsEl && btns.parentElement !== __legendActionsEl) {
    try { __legendActionsEl.appendChild(btns); } catch(_){}
  }
}

  function getCssTransitionMs(){
    try {
      const raw = getComputedStyle(document.documentElement).getPropertyValue('--transition-speed').trim();
      if (!raw) return 250;
      if (raw.endsWith('ms')) return Math.max(0, parseFloat(raw));
      if (raw.endsWith('s'))  return Math.max(0, parseFloat(raw) * 1000);
      const n = parseFloat(raw);
      return Number.isFinite(n) ? n : 250;
    } catch(_) { return 250; }
  }
  // NEW: 追踪 root 的几何变化（位置/尺寸），用于在容器“移动但不改变尺寸”时重放置拟合气泡
  let __lastRootRect = { left:0, top:0, width:0, height:0 };
  let __posWatchRaf = null;
  let __posWatchUntil = 0;

  function setOnXAxisChange(fn){
    onXAxisChange = (typeof fn === 'function') ? fn : null;
  }

  // 拟合/指针状态
  const FIT_ALGO_NAME = 'PCHIP';
  let showFitCurves = true;
  let fitUIInstalled = false;

  const xQueryByMode = { rpm: null, noise_db: null };
  const fitModelsCache = { rpm: new Map(), noise_db: new Map() };

  // 复用测量上下文
  const __textMeasureCtx = (() => {
    const c = document.createElement('canvas');
    return c.getContext('2d');
  })();

  
  // -------- 工具 --------
  function warnOnce(msg){ if (!warnOnce._s) warnOnce._s=new Set(); if(warnOnce._s.has(msg))return; warnOnce._s.add(msg); console.warn(msg); }
  function getById(id){
    // 优先在 root 内查找；未命中则回退到全局（document）
    if (!id) return null;
    let el = null;
    if (root && typeof root.querySelector === 'function') {
      try { el = root.querySelector('#' + id); } catch(_) {}
    }
    return el || document.getElementById(id);
  }
  function appendToRoot(el){ if (root) root.appendChild(el); else document.body.appendChild(el); }

  // NEW: root 几何辅助
  function getRootRect(){
    if (!root || !root.getBoundingClientRect) return { left:0, top:0, width:0, height:0 };
    const r = root.getBoundingClientRect();
    return { left: Math.round(r.left), top: Math.round(r.top), width: Math.round(r.width), height: Math.round(r.height) };
  }

  function primeRootRect(){ __lastRootRect = getRootRect(); }

  function maybeStartRootPosWatch(ms=800){
    const until = performance.now() + Math.max(0, ms|0);
    __posWatchUntil = Math.max(__posWatchUntil, until);
    if (!__posWatchRaf) {
      const tick = () => {
        __posWatchRaf = null;
        const now = performance.now();
        const cur = getRootRect();
        // 当 root 的位置或尺寸变化时，重放置拟合 UI 与外置频谱按钮
        if (cur.left !== __lastRootRect.left || cur.top !== __lastRootRect.top ||
            cur.width !== __lastRootRect.width || cur.height !== __lastRootRect.height) {
          __lastRootRect = cur;
          try { placeFitUI(); repaintPointer(); placeSpectrumDock(); } catch(_){}
        }
        if (now < __posWatchUntil) {
          __posWatchRaf = requestAnimationFrame(tick);
        } else {
          __posWatchUntil = 0;
        }
      };
      __posWatchRaf = requestAnimationFrame(tick);
    }
  }

  function ensureEcharts(){
    if (chart || !root) return;
    if (!window.echarts){ echartsReady = false; return; }
    echartsReady = true;
    chart = echarts.init(root, null, { renderer:'canvas', devicePixelRatio: window.devicePixelRatio || 1 });
    installChartResizeObserver();
    bindGlobalListeners();
    bindChartListeners();
    primeRootRect();                 // NEW: 记录初始几何
    if (!fitUIInstalled) {
      ensureFitUI();
      fitUIInstalled = true;
      toggleFitUI(showFitCurves /* narrow 也显示 */);
      placeFitUI();
      requestAnimationFrame(repaintPointer);
    }
  }

  function adoptBubbleHost() {
    const bubble = document.getElementById('fitBubble');
    if (!bubble) return;

    // 若页面有全屏元素，则必须把气泡作为“全屏元素”的后代，才能处于 Top Layer 之上被看见
    const fsEl = document.fullscreenElement || null;
    const shouldHost = fsEl ? fsEl : document.body;

    if (bubble.parentElement !== shouldHost) {
      try { shouldHost.appendChild(bubble); } catch(_) {}
    }

    // 保持 fixed，不用切换 absolute。位置仍由 placeFitUI 按“相对 chart root 偏移”计算
    bubble.style.position = 'fixed';
  }

function bindGlobalListeners(){
  window.addEventListener('resize', onWindowResize, { passive:true });

  // 滚动时基于“相对图表偏移”重算一次位置（rAF 节流）
  let __scrollRaf = null;
  const onAnyScroll = () => {
    if (__scrollRaf) return;
    __scrollRaf = requestAnimationFrame(() => {
      __scrollRaf = null;
      try { placeFitUI(); placeSpectrumDock(); } catch(_) {}
    });
  };
  window.addEventListener('scroll', onAnyScroll, { passive: true, capture: true });

  (function hookLayoutMovers(){
    const watchMovement = () => { try { placeFitUI(); repaintPointer(); placeSpectrumDock(); } catch(_) {} };
    const kickWatch = () => { watchMovement(); maybeStartRootPosWatch(900); };

    const sidebar = document.getElementById('sidebar');
    if (sidebar) {
      ['transitionrun','transitionstart','transitionend'].forEach(ev=>{
        sidebar.addEventListener(ev, kickWatch, { passive:true });
      });
      try {
        const mo = new MutationObserver(kickWatch);
        mo.observe(sidebar, { attributes:true, attributeFilter:['class','style'] });
      } catch(_){}
    }
    const mainPanels = document.getElementById('main-panels');
    if (mainPanels) {
      ['transitionrun','transitionstart','transitionend'].forEach(ev=>{
        mainPanels.addEventListener(ev, kickWatch, { passive:true });
      });
      try {
        const mo2 = new MutationObserver(kickWatch);
        mo2.observe(mainPanels, { attributes:true, attributeFilter:['class','style'] });
      } catch(_){}
    }
  })();

  document.addEventListener('fullscreenchange', async () => {
    stopAllSweepAudio();
    isFs = !!document.fullscreenElement;

    const modeHost =
      document.getElementById('chart-settings') ||
      (root && root.closest('.fc-chart-container')) ||
      document.documentElement;

    adoptBubbleHost();
    bubbleUserMoved = false;

    if (window.visualViewport) {
      try { window.visualViewport.removeEventListener('resize', onWindowResize); } catch(_) {}
      if (isFs) {
        try { window.visualViewport.addEventListener('resize', onWindowResize, { passive: true }); } catch(_) {}
      }
    }

    try { chart && chart.dispatchAction({ type: 'hideTip' }); } catch(_) {}
    try { spectrumChart && spectrumChart.dispatchAction({ type: 'hideTip' }); } catch(_) {}

    if (!isFs) {
      if (spectrumRoot) spectrumRoot.style.marginTop = '0px';

      // 非全屏：统一清理模式标记与内联高度属性，回到 CSS 自适应
      try { modeHost.removeAttribute('data-chart-mode'); } catch(_) {}
      if (root) {
        try { root.style.minHeight = ''; } catch(_) {}
      }
      try {
        if (spectrumRoot) {
          spectrumRoot.style.removeProperty('max-height');
          spectrumRoot.style.removeProperty('flex');
          spectrumRoot.style.removeProperty('--fs-spec-h');
        }
      } catch(_) {}

      if (screen.orientation && screen.orientation.unlock) {
        try { screen.orientation.unlock(); } catch(_) {}
      }
    }

    updateFullscreenHeights();

    if (lastPayload) render(lastPayload); else if (chart) chart.resize();

    requestAnimationFrame(() => {
      try {
        // 交由 SpectrumController 在模式切换后统一同步（避免复写 tooltip 附着语义）
        SpectrumController.onFullscreenChange(isFs);

        placeFitUI();
        repaintPointer();
        updateSpectrumLayout();
        updateLegendRailLayout();
        updateLegendRail();
        updateRailParkedState();
        // 外置按钮：全屏下暂时隐藏，常规模式显示并重放置
        syncSpectrumDockUi();
        placeSpectrumDock();

        if (chart) chart.resize();
        if (spectrumChart) spectrumChart.resize();
      } catch(_) {}
    });
  }, { passive:true });
}

function bindChartListeners(){
  chart.on('legendmouseover', (p) => {
    if (!spectrumEnabled || !spectrumChart || !p || !p.name) return;
    try { spectrumChart.dispatchAction({ type: 'highlight', seriesName: p.name }); } catch(_) {}
  });
  chart.on('legendmouseout', (p) => {
    if (!spectrumEnabled || !spectrumChart || !p || !p.name) return;
    try { spectrumChart.dispatchAction({ type: 'downplay', seriesName: p.name }); } catch(_) {}
  });
  chart.on('highlight', (p) => {
    if (!spectrumEnabled || !spectrumChart || !p || !p.seriesName) return;
    try { spectrumChart.dispatchAction({ type: 'highlight', seriesName: p.seriesName }); } catch(_) {}
  });
  chart.on('downplay', (p) => {
    if (!spectrumEnabled || !spectrumChart || !p || !p.seriesName) return;
    try { spectrumChart.dispatchAction({ type: 'downplay', seriesName: p.seriesName }); } catch(_) {}
  });

  chart.on('dataZoom', () => {
    clampXQueryIntoVisibleRange();
    layoutScheduler.mark('pointer');
    if (showFitCurves) layoutScheduler.mark('fitUI');

    if (__skipSpectrumOnce || suppress.active('spectrum')) return;
    SpectrumController.onXQueryChange(xQueryByMode[currentXModeFromPayload(lastPayload)]);
  });
}

function onWindowResize(){
  if (!chart) return;

  suppress.run('spectrum', 500);

  updateFullscreenHeights();
  const nowNarrow = layoutIsNarrow();
  if (lastIsNarrow === null) lastIsNarrow = nowNarrow;
  maybeStartRootPosWatch(900);

  if (nowNarrow !== lastIsNarrow) {
    lastIsNarrow = nowNarrow;
    if (lastPayload) render(lastPayload); else chart.resize();
  } else {
    chart.resize();
    try { if (spectrumEnabled) placeSpectrumSwitchOverlay(); } catch(_) {}
    try { spectrumEnabled && spectrumChart && spectrumChart.resize(); } catch(_) {}

    if (lastOption) {
      const { x, y, visible } = computePrefixCenter(lastOption);
      placeAxisOverlayAt(x, y, visible && !lastOption.__empty);
    }
    layoutScheduler.mark('fitUI','pointer');
  }

  layoutScheduler.mark('spectrum','legend','railPark','dock','axisSwitch');
  __refreshFsSpecMaxHeightIfExpanded();
}

  let __chartRO = null;
function installChartResizeObserver(){
  if (__chartRO || !root || typeof ResizeObserver === 'undefined') return;
  __chartRO = new ResizeObserver(entries => {
    for (const entry of entries) {
      const cr = entry.contentRect || {};
      if (chart && cr.width > 0 && cr.height > 0) {
        suppress.run('spectrum', 500);

        primeRootRect();
        maybeStartRootPosWatch(900);

        const nowNarrow = layoutIsNarrow();
        if (lastIsNarrow === null) lastIsNarrow = nowNarrow;
        if (nowNarrow !== lastIsNarrow) {
          lastIsNarrow = nowNarrow;
          try { if (lastPayload) { render(lastPayload); } else { chart.resize(); } } catch(_){}
          layoutScheduler.mark('spectrum','legend','railPark','dock');
          continue;
        }

        try { chart.resize(); } catch(_){}
        try { spectrumEnabled && spectrumChart && spectrumChart.resize(); } catch(_){}

        try {
          if (lastOption) {
            const { x, y, visible } = computePrefixCenter(lastOption);
            placeAxisOverlayAt(x, y, visible && !lastOption.__empty);
          }
        } catch(_) {}

        layoutScheduler.mark('fitUI','pointer','axisSwitch','spectrum','legend','dock');
      }
    }
  });
  __chartRO.observe(root);
}

  function isMobile(){
    return /Mobi|Android|iPhone|iPad|iPod/i.test(navigator.userAgent)
      || (window.matchMedia && window.matchMedia('(pointer:coarse)').matches);
  }

function layoutIsNarrow() {
  // 以“图表外层容器”的实际宽度判定，避免 rail 改变自身宽度导致的反馈抖动
  const host = document.getElementById('chart-settings') || (root && root.closest('.fc-chart-container')) || document.documentElement;
  const w =
    (host && host.getBoundingClientRect && Math.floor(host.getBoundingClientRect().width)) ||
    (window.innerWidth || 0);

  // 迟滞窗口：进入阈值略小、退出阈值略大，避免边界来回切换
  const half = Math.max(0, Math.floor(NARROW_HYSTERESIS / 2));
  const enterNarrowAt = NARROW_BREAKPOINT - half; // 进入窄屏阈值
  const exitNarrowAt  = NARROW_BREAKPOINT + half; // 退出窄屏阈值

  let narrow;
  if (lastIsNarrow === true) {
    // 已经是窄屏 → 只有当宽度明显超过退出阈值才切回桌面
    narrow = (w < exitNarrowAt);
  } else if (lastIsNarrow === false) {
    // 已经是桌面 → 只有当宽度明显小于进入阈值才切换到窄屏
    narrow = (w < enterNarrowAt);
  } else {
    // 初次判定
    narrow = (w < NARROW_BREAKPOINT);
  }

  // 全屏 + 移动端不视为窄屏（保持原规则）
  if (isFs && isMobile()) narrow = false;
  return narrow;
}

function mount(rootEl) {
  if (!rootEl) {
    warnOnce('[ChartRenderer] mount(rootEl) 需要一个有效的 DOM 容器');
    return;
  }
  root = rootEl; // 在函数最开始设置 root 变量

  ensureSpectrumHost();

  // 确保 DOM 结构正确
  const shell = root.closest('.fc-chart-container');
  if (shell) {
    let stack = shell.querySelector('.chart-stack');
    if (!stack) {
      stack = document.createElement('div');
      stack.className = 'chart-stack';
      
      if (root.parentElement) {
        root.parentElement.insertBefore(stack, root);
      }
      stack.appendChild(root);
    }

    // 确保 spectrumRoot 也被移入 stack
    if (spectrumRoot && spectrumRoot.parentElement !== stack) {
        stack.appendChild(spectrumRoot);
    }
  }

  ensureLegendRail();
  updateLegendRailLayout();

  ensureEcharts();
  const initialTheme =
    (window.ThemePref && typeof window.ThemePref.resolve === 'function')
      ? window.ThemePref.resolve()
      : (document.documentElement.getAttribute('data-theme') || 'light');

  if (window.ThemePref && typeof window.ThemePref.setDom === 'function') {
    window.ThemePref.setDom(initialTheme);
  } else {
    document.documentElement.setAttribute('data-theme', initialTheme);
  }

  if (!chart) return;
  
  const emptyPayload = { chartData: { series: [] }, theme: initialTheme };
  render(emptyPayload);
}

function ensureSpectrumResolutionSwitch() {
  if (!spectrumInner) return null;

  // 移除旧的非 slider 版本
  const oldEl = spectrumInner.querySelector('.spectrum-res-switch');
  if (oldEl && !oldEl.classList.contains('is-slider-type')) {
    try { oldEl.remove(); } catch(_) {}
  }

  let container = spectrumInner.querySelector('.spec-switch-container');
  if (!container) {
    // 创建 wrapper（不再插入“频带分辨率”文字标签）
    const wrapper = document.createElement('div');
    wrapper.className = 'spectrum-res-switch is-slider-type';
    wrapper.style.display = 'flex';
    wrapper.style.alignItems = 'center';
    wrapper.style.gap = '8px';

    // 保持绝对定位 + z-index，逻辑仍由 placeSpectrumSwitchOverlay 覆盖到标题上
    wrapper.style.position = 'absolute';
    wrapper.style.zIndex = '200';

    // 仅保留 slider 容器
    container = document.createElement('div');
    container.className = 'spec-switch-container';
    container.innerHTML = `
      <div class="spec-switch-track">
        <div class="spec-switch-strip" id="specResSlider">
          <div class="spec-item">1/48</div>
          <div class="spec-item">1/12</div>
          <div class="spec-item">1/3</div>
        </div>
      </div>
    `;
    wrapper.appendChild(container);

    // 插入到频谱 inner 顶部
    spectrumInner.insertBefore(wrapper, spectrumInner.firstChild);

    // 绑定交互
    bindSpecResSwitch(container);

    // 首次定位（异步，等图表尺寸稳定）
    requestAnimationFrame(placeSpectrumSwitchOverlay);
  }

  updateSpecResSwitchPos(container);
  return container;
}

function bindSpecResSwitch(container) {
  const slider = container.querySelector('#specResSlider');
  if (!slider) return;

  const MODES = ['1_48', '1_12', '1_3'];
  
  let itemWidth = 36;
  let containerWidth = 72;
  let centerOffset = 0;
  
  let dragging = false;
  let dragMoved = false;
  let startX = 0;
  let currentTranslate = 0;
  let activePointerId = null;

  function getModeIndex() {
    const cur = getSpectrumResolutionMode();
    const idx = MODES.indexOf(cur);
    return idx >= 0 ? idx : 0;
  }

  function updateMetrics() {
    containerWidth = container.offsetWidth || 72;
    const firstItem = slider.querySelector('.spec-item');
    if (firstItem) itemWidth = firstItem.offsetWidth || 36;
    centerOffset = (containerWidth - itemWidth) / 2;
  }

  function updateItemScales(tx) {
    const items = slider.querySelectorAll('.spec-item');
    if (!items.length) return;
    
    const cwHalf = containerWidth / 2;
    const centerInStrip = cwHalf - tx;

    items.forEach((el, i) => {
      const itemCenterInStrip = (i * itemWidth) + (itemWidth / 2);
      const dist = Math.abs(itemCenterInStrip - centerInStrip);
      
      let scale = 0.6;
      let opacity = 0.4;

      if (dist < itemWidth * 1.2) {
        const ratio = dist / (itemWidth * 1.2);
        scale = 1.0 - (0.4 * ratio);    
        opacity = 1.0 - (0.6 * ratio);  
      }
      
      el.style.transform = `scale(${scale.toFixed(3)})`;
      el.style.opacity = opacity.toFixed(2);
    });
  }

  function syncPos(animate = true) {
    const idx = getModeIndex();
    const x = centerOffset - (idx * itemWidth);
    
    slider.style.transition = animate ? 'transform .25s cubic-bezier(0.25, 0.1, 0.25, 1)' : 'none';
    slider.style.transform = `translateX(${x}px)`;
    currentTranslate = x;
    updateItemScales(x);
  }

  function commitMode(idx) {
    const safeIdx = Math.max(0, Math.min(MODES.length - 1, idx));
    const newMode = MODES[safeIdx];
    setSpectrumResolutionMode(newMode);
    updateMetrics(); 
    syncPos(true);
    // 切换模式后立即刷新位置，因为标题文本长度变了
    setTimeout(placeSpectrumSwitchOverlay, 0);
  }

  // ... pointer events (保持不变) ...
  function onPointerDown(e) {
    if (e.button !== undefined && e.button !== 0) return;
    updateMetrics();
    dragging = true; dragMoved = false; startX = e.clientX;
    activePointerId = e.pointerId ?? null;
    slider.style.transition = 'none';
    const style = window.getComputedStyle(slider);
    currentTranslate = new DOMMatrix(style.transform).m41;
    updateItemScales(currentTranslate);
    try { if (activePointerId != null) slider.setPointerCapture(activePointerId); } catch(_) {}
    e.preventDefault?.();
  }

  function onPointerMove(e) {
    if (!dragging) return;
    const dx = e.clientX - startX;
    if (!dragMoved && Math.abs(dx) > 2) dragMoved = true;
    let nextTx = currentTranslate + dx;
    const minTx = centerOffset - ((MODES.length - 1) * itemWidth) - 24;
    const maxTx = centerOffset + 24;
    if (nextTx > maxTx) nextTx = maxTx + (nextTx - maxTx) * 0.3;
    if (nextTx < minTx) nextTx = minTx + (nextTx - minTx) * 0.3;
    slider.style.transform = `translateX(${nextTx}px)`;
    updateItemScales(nextTx);
  }

  function onPointerUp(e) {
    if (!dragging) return;
    dragging = false;
    try { if (activePointerId != null) slider.releasePointerCapture(activePointerId); } catch(_) {}
    activePointerId = null;
    if (!dragMoved) {
      let idx = getModeIndex(); idx = (idx + 1) % MODES.length; commitMode(idx);
    } else {
      const style = window.getComputedStyle(slider);
      const finalTx = new DOMMatrix(style.transform).m41;
      let idx = Math.round((centerOffset - finalTx) / itemWidth);
      commitMode(idx);
    }
  }

  container.addEventListener('pointerdown', onPointerDown);
  window.addEventListener('pointermove', onPointerMove, { passive: true });
  window.addEventListener('pointerup', onPointerUp, { passive: true });
  window.addEventListener('pointercancel', onPointerUp);
  
  requestAnimationFrame(() => { updateMetrics(); syncPos(false); });
  container.__updatePos = () => { updateMetrics(); syncPos(true); };
}

function updateSpecResSwitchPos(container) {
  if (container && container.__updatePos) {
    container.__updatePos();
  }
}

// 在 setSpectrumResolutionMode 中添加 UI 同步调用
function setSpectrumResolutionMode(mode) {
  const m = String(mode || '').trim();
  if (!['1_3', '1_12', '1_48'].includes(m)) return;
  if (m === spectrumResolutionMode) return;
  spectrumResolutionMode = m;
  
  // 同步 UI 位置
  if (spectrumInner) {
    const container = spectrumInner.querySelector('.spec-switch-container');
    if (container) updateSpecResSwitchPos(container);
  }

  // 仅影响前端聚合方式，重建频谱即可
  try {
    if (spectrumEnabled && spectrumChart) {
      buildAndSetSpectrumOption(true);
    }
  } catch (_) {}
}

function ensureSpectrumHost() {
  // 若已存在 host，仅保证有 .spectrum-inner 子元素
  if (spectrumRoot && spectrumRoot.isConnected) {
    let inner = spectrumRoot.querySelector('.spectrum-inner');
    if (!inner) {
      inner = document.createElement('div');
      inner.className = 'spectrum-inner';
      spectrumRoot.appendChild(inner);
    }
    try {
      inner.style.position = 'relative';
      inner.style.width = '100%';
      inner.style.overflow = 'visible';
    } catch(_) {}
    spectrumInner = inner;
    return spectrumRoot;
  }

  // 新建 Host + inner（无剪裁层）
  let host = document.getElementById('spectrumHost');
  if (!host) { host = document.createElement('div'); host.id = 'spectrumHost'; }

  const shell = document.getElementById('chart-settings') || (root && root.closest('.fc-chart-container')) || document.body;
  let stack = shell && shell.querySelector('.chart-stack');
  if (!stack && root && root.parentElement) {
    stack = document.createElement('div');
    stack.className = 'chart-stack';
    root.parentElement.insertBefore(stack, root);
    stack.appendChild(root);
    if (shell && stack.parentElement !== shell) shell.insertBefore(stack, shell.firstChild);
  }
  if (stack && host.parentElement !== stack) stack.appendChild(host);
  else if (!stack && root && host.parentElement !== root.parentElement) root.parentElement?.insertBefore(host, root.nextSibling);

  let inner = host.querySelector('.spectrum-inner');
  if (!inner) { inner = document.createElement('div'); inner.className = 'spectrum-inner'; host.appendChild(inner); }
  try {
    inner.style.position = 'relative';
    inner.style.width = '100%';
    inner.style.overflow = 'visible';
  } catch(_) {}

  spectrumRoot = host;
  spectrumInner = inner;
  try { ensureSpectrumResolutionSwitch(); } catch(_) {}
  return host;
}

function updateFullscreenHeights() {
  const fsEl = document.fullscreenElement;
  const activeFs = !!fsEl;
  isFs = activeFs;

  requestAnimationFrame(() => {
    try { chart && chart.resize(); } catch (_) { }
    try { spectrumEnabled && spectrumChart && spectrumChart.resize(); } catch (_) { }
  });
}

function render(payload){
  stopAllSweepAudio();
  lastPayload = payload || lastPayload;
  if (!root){ warnOnce('[ChartRenderer] 请先调用 mount(rootEl)'); return; }
  if (!window.echarts){ requestAnimationFrame(()=>render(lastPayload)); return; }
  ensureEcharts();
  if (!chart) return;

  __dataEpoch++;
  try {
    // 后端已输出 canonicalSeries：直接使用
    canonicalSeries = Array.isArray(lastPayload?.chartData?.series) ? lastPayload.chartData.series : [];
    __canonicalEpoch = __dataEpoch;
  } catch(_) {
    canonicalSeries = [];
  }

  // 构建快照（rpm + noise_db）
  buildDerivedSnapshots(canonicalSeries);

  const prevXMode = currentXModeFromPayload(lastPayload);
  const prevEmpty = !!(lastOption && lastOption.__empty);

  syncThemeAttr((lastPayload && lastPayload.theme) || 'light');

  if (!fitUIInstalled && showFitCurves) { ensureFitUI(); fitUIInstalled = true; }

  const option = buildOption(lastPayload);
  const nextXMode = currentXModeFromPayload(lastPayload);
  const nextEmpty = !!option.__empty;

  if (prevXMode !== nextXMode || prevEmpty !== nextEmpty) {
    try { chart.clear(); } catch(_){}
  }

  chart.setOption(option, true);
  chart.resize();

  lastOption = option;
  syncSpectrumBgWithMain(option.backgroundColor);

  requestAnimationFrame(() => updateAxisSwitchPosition({ force:true, animate:false }));
  if (option.__empty) {
    try { chart.dispatchAction({ type: 'updateAxisPointer', currTrigger: 'leave' }); } catch(_){}
  }

  const { x, y, visible } = computePrefixCenter(option);
  placeAxisOverlayAt(x, y, visible && !option.__empty);

  lastIsNarrow = layoutIsNarrow();

  toggleFitUI(showFitCurves);
  placeFitUI();
  updateRailParkedState();

  ensureSpectrumDock();
  syncSpectrumDockUi();
  placeSpectrumDock();

  updateLegendRail();

  primeRootRect();
  maybeStartRootPosWatch(600);

  // ==== 关键改动：主图 setOption 后立即触发频谱 ====
  if (spectrumEnabled && !__skipSpectrumOnce) {
    try {
      // 直接用最新 series / legend 可见性构建频谱；
      // requestAndRenderSpectrum 内部已经有缓存 / pending / 请求去重逻辑
      requestAndRenderSpectrum(true);
    } catch(_) {}
  }
  // __skipSpectrumOnce 仍只负责“跳过下一次自动触发”，不再依赖 finished
  setTimeout(() => { __skipSpectrumOnce = false; }, 450);
  // ==================================================

  try {
    const onFinished = () => {
      try { chart.off('finished', onFinished); } catch(_){}
      repaintPointer();
      updateSpectrumLayout();
      // 动画结束时，再做一次轻量的频谱刷新（不强制 fullRefresh），避免在动画中期多次 repaint
      if (spectrumEnabled) {
        try { requestAndRenderSpectrum(false); } catch(_) {}
      }
      try { syncLegendRailFromChart(); } catch(_){}
    };
    chart.on('finished', onFinished);
  } catch(_){}

  requestAnimationFrame(repaintPointer);
  if (showFitCurves) refreshFitPanel();
}

  function resize(){ if (chart) chart.resize(); }

  // ===== 主题/度量 =====
  function syncThemeAttr(theme){
    const t = String(theme || 'light').toLowerCase();
    document.documentElement.setAttribute('data-theme', t);
  }

function tokens(theme) {
  const dark = (String(theme||'').toLowerCase() === 'dark');
  // 颜色仍用原逻辑；阴影与背板由 CSS 变量控制
  return {
    fontFamily:'system-ui,-apple-system,"Segoe UI","Helvetica Neue","Microsoft YaHei",Arial,sans-serif',
    axisLabel: dark ? '#d1d5db' : '#4b5563',
    axisName:  dark ? '#9ca3af' : '#6b7280',
    axisLine:  dark ? '#374151' : '#e5e7eb',
    gridLine:  dark ? 'rgba(255,255,255,0.10)' : 'rgba(0,0,0,0.08)',
    tooltipBg: 'var(--tooltip-bg, var(--bg-bubble))',
    tooltipBorder: dark ? '#374151' : '#e5e7eb',
    tooltipText: dark ? '#f3f4f6' : '#1f2937',
    tooltipShadow: 'var(--shadow-lg)',
    pagerIcon: dark ? '#93c5fd' : '#2563eb'
  };
}

function measureText(text, size, weight, family){
  const font = `${String(weight||400)} ${Number(size||14)}px ${family||'sans-serif'}`;
  return TextMeasurer.measure(font, text || '');
}

  const TITLE_GLUE = '  -  ';
  function computePrefixCenter(option){
    if (!chart || !option || !option.title) return { x: 0, y: 0, visible:false };
    if (option.__empty) return { x: 0, y: 0, visible:false };
    const title = option.title;
    const ts = title.textStyle || {};
    const size = Number(ts.fontSize || option.__titleFontSize || 14);
    const weight = ts.fontWeight || option.__titleFontWeight || 600;
    const family = ts.fontFamily || option.__titleFamily;
    const prefix = String(option.__titlePrefix || '');
    const totalText = `${prefix}${TITLE_GLUE}风量曲线`;

    const mTotal = measureText(totalText, size, weight, family);
    const mPrefix = measureText(prefix, size, weight, family);
    const chartW = chart.getWidth();
    const centerX = chartW / 2;
    const totalLeft = centerX - mTotal.width / 2;
    const prefixCenterX = totalLeft + mPrefix.width / 2;
    const top = (typeof title.top === 'number') ? title.top : 0;
    const centerY = top + (mTotal.height / 2);
    return { x: Math.round(prefixCenterX), y: Math.round(centerY), visible:true };
  }

  // ===== X 轴模式/构建 =====
  function currentXModeFromPayload(payload){
    const inPay = (payload?.chartData?.x_axis_type === 'noise_db' || payload?.chartData?.x_axis_type === 'noise') ? 'noise_db' : 'rpm';
    if (xAxisOverride) return xAxisOverride;
    return inPay;
  }

  const X_PLACEHOLDER_NEG = -1;
  const X_MIN_CLAMP = 0;
  // Line width multipliers for emphasis states to maintain visual consistency
  const EMPHASIS_WIDTH_MULTIPLIER = 1.33;        // Base series emphasis width ratio
  const FIT_EMPHASIS_WIDTH_MULTIPLIER = 1.6;     // Fit curve emphasis width ratio

  /**
   * Parse X value robustly to handle missing values.
   * Returns NaN for '', null, undefined (all treated as missing).
   * Otherwise returns Number(v).
   * This ensures that missing noise_db values are treated as missing (-1),
   * not as 0, so curves are hidden outside the chart as expected.
   */
  function parseXValue(v) {
    if (v === '' || v === null || v === undefined) {
      return NaN;
    }
    return Number(v);
  }

function buildSeries(rawSeries, xMode) {
  let maxAir = 0;
  let minX = +Infinity, maxX = -Infinity;

  const isNarrow = layoutIsNarrow();
  const lineWidth = isNarrow ? 2 : 3;
  const symbolSize = isNarrow ? 4 : 8;

  const series = (rawSeries || []).map(s => {
    // 强制要求 canonical 结构
    const rpmArr   = Array.isArray(s?.data?.rpm) ? s.data.rpm : [];
    const noiseArr = Array.isArray(s?.data?.noise_db) ? s.data.noise_db : [];
    const flowArr  = Array.isArray(s?.data?.airflow) ? s.data.airflow : [];

    const xSrc   = (xMode === 'noise_db') ? noiseArr : rpmArr;
    const tipSrc = (xMode === 'noise_db') ? rpmArr   : noiseArr;

    const n = Math.min(xSrc.length, flowArr.length);
    const data = [];
    for (let i = 0; i < n; i++) {
      const xv = parseXValue(xSrc[i]);
      const yv = Number(flowArr[i]);
      if (Number.isFinite(yv)) {
        if (Number.isFinite(xv) && xv !== X_PLACEHOLDER_NEG) {
          minX = Math.min(minX, xv);
          maxX = Math.max(maxX, xv);
          maxAir = Math.max(maxAir, yv);
          const tipRaw = tipSrc[i];
          const tip = Number.isFinite(Number(tipRaw)) ? Number(tipRaw) : undefined;
          data.push({ value: [xv, yv], tip });
        } else {
          data.push({ value: [X_PLACEHOLDER_NEG, yv], tip: undefined, __missingX: true });
        }
      }
    }

    const name = s.name || `${s.brand || ''} ${s.model || ''} - ${s.condition || ''}`;
    return {
      name,
      type: 'line',
      smooth: true,
      connectNulls: false,
      showSymbol: true,
      symbol: 'circle',
      symbolSize: symbolSize,
      lineStyle: { width: lineWidth, color: s.color },
      itemStyle: { color: s.color },
      label: { show: true, position: 'top', color: 'gray' },
      labelLayout: { hideOverlap: true },
      legendHoverLink: true,
      emphasis: {
        focus: 'series',
        blurScope: 'coordinateSystem',
        lineStyle: { width: lineWidth * EMPHASIS_WIDTH_MULTIPLIER },
        itemStyle: { borderWidth: 1.2, shadowColor: 'rgba(0,0,0,0.25)', shadowBlur: 8 },
        label: { show: true }
      },
      blur: {
        lineStyle: { opacity: 0.18 },
        itemStyle: { opacity: 0.18 },
        label: { show: false }
      },
      data
    };
  });

  if (minX === +Infinity) { minX = 0; maxX = 100; }
  if (maxAir <= 0) maxAir = 100;

  const span = Math.max(1, maxX - minX);
  const pad = Math.floor(span * 0.2);
  return { series, xMin: Math.max(minX - pad, 0), xMax: maxX + pad, yMax: Math.ceil(maxAir * 1.4) };
}

function buildOption(payload) {
  const { theme } = payload || {};
  const t = tokens(theme||'light');
  const sList = getSeriesArray();
  const xMode = currentXModeFromPayload(payload);

  const isNarrow = layoutIsNarrow();
  const exportBg = (payload && payload.chartBg) || getExportBg();
  const bgNormal = isFs ? exportBg : 'transparent';
  const transitionMs = getCssTransitionMs();

  if (!sList.length) {
    stopAllSweepAudio();
    toggleFitUI(false);
    return {
      __empty:true,
      backgroundColor: bgNormal,
      title:{ text:'请 先 添 加 数 据', left:'center', top:'middle',
        textStyle:{ color:t.axisLabel, fontFamily:t.fontFamily, fontSize: 20, fontWeight: 600 }
      },
      toolbox:{ show:false },
      tooltip:{ show:false, triggerOn:'none' }
    };
  }

  // 使用预构建快照
  const snap = getSnapshotForMode(xMode);
  const built = snap || buildSeries(sList, xMode);

  const xName = xMode==='rpm' ? '转速(RPM)' : '噪音(dB)';
  const titlePrefix = xMode==='rpm' ? '转速' : '噪音';
  const titleTop = 10;
  const titleText = `${titlePrefix}${TITLE_GLUE}风量曲线`;
  const titleMeasure = measureText(titleText, 20, 600, t.fontFamily);
  const gridTop = Math.max(54, titleTop + Math.ceil(titleMeasure.height) + 12);

  const gridRight = 30;
  const gridBottom = isNarrow ? 60 : 60;

  const legendCfg = { show: false, data: sList.map(s=> (s.name || `${s.brand||''} ${s.model||''} - ${s.condition||''}`)) };
  try {
    const prevSel = chart?.getOption?.().legend?.[0]?.selected;
    if (prevSel) legendCfg.selected = prevSel;
  } catch(_){}

  const finalSeries = [];
  built.series.forEach(s => finalSeries.push(s));

  if (showFitCurves) {
    ensureFitModels(sList, xMode);
    const width = Math.max(300, chart.getWidth ? chart.getWidth() : 800);
    const sampleCount = computeSampleCount(width);
    const fitLineWidth = isNarrow ? 0.75 : 1.5;
    sList.forEach(s => {
      const name = s.name || `${s.brand||''} ${s.model||''} - ${s.condition||''}`;
      const model = fitModelsCache[xMode].get(name);
      if (!model || model.x0 == null || model.x1 == null) return;
      const sMin = Math.min(model.x0, model.x1);
      const sMax = Math.max(model.x0, model.x1);
      const xmin = Math.max(built.xMin, sMin);
      const xmax = Math.min(built.xMax, sMax);
      if (!(xmax > xmin)) return;
      const pts = resampleSingle(model, xmin, xmax, sampleCount);
      finalSeries.push({
        id: `fit-line:${xMode}:${name}`,
        name,
        type: 'line',
        smooth: false,
        showSymbol: false,
        connectNulls: false,
        data: pts.map(p => [p.x, p.y]),
        lineStyle: { width: fitLineWidth, type:'dashed', color: s.color, opacity: 0.75 },
        itemStyle: { color: s.color },
        legendHoverLink: true,
        emphasis: { focus: 'series', blurScope: 'coordinateSystem', lineStyle: { width: fitLineWidth * FIT_EMPHASIS_WIDTH_MULTIPLIER, opacity: 0.9 }, itemStyle: { opacity: 1 } },
        blur: { lineStyle: { opacity: 0.15 }, itemStyle: { opacity: 0.15 } },
        silent: false,
        tooltip: { show: false },
        z: 3
      });
    });
  }

  const rawMin = Math.max(X_MIN_CLAMP, built.xMin);
  const rawMax = built.xMax * 1.2;
  let xMinForAxis = Math.floor(rawMin);
  let xMaxForAxis = Math.ceil(rawMax);
  if (!(xMaxForAxis > xMinForAxis)) xMaxForAxis = xMinForAxis + 1;

  const exportAllFeature = {
    show: true,
    title: '导出为图片',
    icon: 'path://M12 2v10m0 0 4-4m-4 4-4-4M4 20h16v2H4z',
    onclick: () => { try { exportCombinedImage(); } catch(e){ console.warn('导出失败', e); } }
  };

  const fitIcon = 'path://M2 18 L8 12 L12 16 L22 6 L22 9 L12 19 L8 15 L2 21 Z';
  const myFitToggle = {
    show: true,
    title: showFitCurves ? '关闭拟合' : '实时拟合',
    icon: fitIcon,
    onclick: () => {
      try { chart && chart.dispatchAction({ type: 'hideTip' }); } catch(_){}
      stopAllSweepAudio();
      showFitCurves = !showFitCurves;

      // 同步浮动/集成 UI
      toggleFitUI(showFitCurves);
      placeFitUI();
      repaintPointer();
      refreshFitPanel();
      updateLegendRailLayout();
      updateRailParkedState();

      // 关键修复：同步侧栏拟合按钮的激活态（避免与 toolbox 状态不同步）
      const btnsRoot = getById('fitButtons');
      const btnFit = btnsRoot ? btnsRoot.querySelector('#btnFit') : null;
      if (btnFit) btnFit.classList.toggle('active', !!showFitCurves);

      try { chart && chart.resize(); } catch(_){}
    }
  };

  const fsEnterIcon = 'path://M4 4h6v2H6v4H4V4Zm10 0h6v6h-2V6h-4V4Zm6 10v6h-6v-2h4v-4h2ZM4 14h2v4h4v2H4v-6z';
  const fsExitIcon  = 'path://M6 6h2v2H8v2H6V6Zm10 0h2v4h-2V8h-2V6h2Zm2 10v2h-4v-2h2v-2h2v2ZM6 16h2v2h4v2H6v-4z';
  const myFullscreen = {
    show: true,
    title: isFs ? '退出全屏' : '全屏查看',
    icon: isFs ? fsExitIcon : fsEnterIcon,
    onclick: () => {
      toggleFullscreen();
      stopAllSweepAudio();
    }
  };

  const toolboxFeatures = isNarrow ? {
    restore: {},
    myFitToggle,
    myExportAll: exportAllFeature,
    myFullscreen
  } : {
    dataZoom: { yAxisIndex: 'none' },
    restore: {},
    myFitToggle,
    myExportAll: exportAllFeature,
    myFullscreen
  };

  return {
    __empty:false,
    __titlePrefix:titlePrefix,
    backgroundColor: bgNormal,
    color: sList.map(s=>s.color),
    textStyle:{ fontFamily:t.fontFamily },
    stateAnimation: { duration: transitionMs, easing: 'cubicOut' },
    animationDurationUpdate: transitionMs,
    animationEasingUpdate: 'cubicOut',
    grid:{ left:40, right: gridRight, top: gridTop, bottom: gridBottom },
    title: { text: titleText, left: 'center', top: titleTop,
      textStyle: { color: t.axisLabel, fontSize: 18, fontWeight: 600, fontFamily:t.fontFamily } },
    legend: legendCfg,
    xAxis:{
      type:'value', name:xName, nameLocation:'middle', nameGap:25, nameMoveOverlap:true,
      nameTextStyle:{ color:t.axisName, fontWeight:600, fontFamily:t.fontFamily, textShadowColor:'rgba(0,0,0,0.28)', textShadowBlur:4, textShadowOffsetY:1 },
      axisLabel:{ color:t.axisLabel, fontSize:12, fontFamily:t.fontFamily, margin:10 },
      axisLine:{ lineStyle:{ color:t.axisLine }},
      splitLine:{ show:true, lineStyle:{ color:t.gridLine }},
      min: xMinForAxis, max: xMaxForAxis
    },
    yAxis:{
      type:'value', name:'风量(CFM)', min:0, max: built.yMax * 1.3,
      nameTextStyle:{ color:t.axisName, fontWeight:600, textShadowColor:'rgba(0,0,0,0.28)', textShadowBlur:4, textShadowOffsetY:1 },
      axisLabel:{ color:t.axisLabel }, axisLine:{ lineStyle:{ color:t.axisLine }},
      splitLine:{ show:true, lineStyle:{ color:t.gridLine }}
    },
    tooltip: optionTooltipBase(t),
    toolbox:{ top: 0, right: 10, feature:toolboxFeatures },
    dataZoom: [
      { type: 'inside', xAxisIndex: 0, throttle: 50, zoomOnMouseWheel: true, moveOnMouseWheel: true, moveOnMouseMove: true ,filterMode: "none", startValue: xMinForAxis, endValue: xMaxForAxis*0.85 },
      { type: 'inside', yAxisIndex: 0, throttle: 50, zoomOnMouseWheel: 'alt', moveOnMouseWheel: 'alt', moveOnMouseMove: true ,filterMode: "none", endValue: built.yMax}
    ],
    series: finalSeries
  };
}

function optionTooltipBase(t){
  return {
    ...buildTooltipBase(t, { appendToBody: !isFs }),
    confine: false,
    trigger: 'item',
    triggerOn: 'mousemove|click|touchstart|touchmove',
    axisPointer: { type: 'cross', label: { color: t.tooltipText } },
    borderRadius: 12,
    position: function (pos, _params, dom) {
      const x = Array.isArray(pos) ? pos[0] : 0;
      const y = Array.isArray(pos) ? pos[1] : 0;
      const vw = window.innerWidth  || document.documentElement.clientWidth || 0;
      const vh = window.innerHeight || document.documentElement.clientHeight || 0;
      const dw = dom?.offsetWidth  || 0;
      const dh = dom?.offsetHeight || 0;
      const pad = 8, gap = 12;
      let left = x + gap;
      let top  = y + gap;
      if (left + dw > vw - pad) left = Math.max(pad, x - gap - dw);
      if (top  + dh > vh - pad) top  = Math.max(pad, y - gap - dh);
      if (left < pad) left = pad;
      if (top  < pad) top = pad;
      return [Math.round(left), Math.round(top)];
    },
    formatter: function(p){
      const xModeNow = currentXModeFromPayload(lastPayload);
      const xLabel = xModeNow==='rpm' ? 'RPM, ' : 'dB, ';
      const infoLabel = xModeNow==='rpm' ? 'dB' : 'RPM';
      const x = p.value?.[0], y = p.value?.[1];
      const tip = p.data?.tip ?? '';
      const dot = `<span style="display:inline-block;width:12px;height:12px;border-radius:50%;background:${p.color};margin-right:4px;"></span>`;
      return `${dot}${p.seriesName}<br/>&nbsp;&nbsp;&nbsp;&nbsp;${y}CFM @${x}${xLabel}${tip}${infoLabel}`;
    }
  };
}

  // ===== UI：X 轴切换 =====
  let xAxisOverride = null;
  let axisSnapSuppressUntil = 0;
  let axisSnapSuppressTimer = null;

  function ensureAxisOverlay(){
    let overlay = getById('chartXAxisOverlay');
    if (!overlay){
      overlay = document.createElement('div');
      overlay.id = 'chartXAxisOverlay';
      overlay.className = 'chart-xaxis-overlay';
      overlay.setAttribute('aria-label','X轴切换');
      overlay.innerHTML = `
        <div class="switch-container" id="xAxisSwitchContainer">
          <div class="switch-track" id="xAxisSwitchTrack">
            <div class="switch-slider" id="xAxisSwitchSlider">
              <span class="switch-label switch-label-right">转速</span>
              <span class="switch-label switch-label-left">噪音</span>
            </div>
          </div>
        </div>`;
      appendToRoot(overlay);

      // 不再在 JS 中设置 z-index，统一交给 CSS 分层
      overlay.style.position = 'absolute';

      bindXAxisSwitch();
      requestAnimationFrame(() => updateAxisSwitchPosition({ force: true, animate: false }));
    }
    return overlay;
  }

function updateAxisSwitchPosition(opts = {}) {
  const { animate = false } = opts;

  const track  = getById('xAxisSwitchTrack');
  const slider = getById('xAxisSwitchSlider');
  if (!track || !slider) return;

  const sliderWidth = slider.offsetWidth || 0;
  const trackWidth  = track.offsetWidth || 0;
  const maxX = Math.max(0, trackWidth - sliderWidth);

  const currType = currentXModeFromPayload(lastPayload);
  const toNoise  = (currType === 'noise_db');

  // 只有需要动画时才覆盖 transition，避免把之前设置的过渡清掉
  if (animate) {
    slider.style.transition = 'transform .25s ease';
  }
  slider.style.transform  = `translateX(${toNoise ? maxX : 0}px)`;
  track.setAttribute('aria-checked', String(toNoise));
}

function bindXAxisSwitch(){
  const xAxisSwitchTrack = getById('xAxisSwitchTrack');
  const xAxisSwitchSlider = getById('xAxisSwitchSlider');
  if (!xAxisSwitchTrack || !xAxisSwitchSlider) return;

  let sliderWidth = 0, trackWidth = 0, maxX = 0;
  let dragging = false, dragMoved = false, startX = 0, base = 0, activePointerId = null;

  try {
    xAxisSwitchTrack.setAttribute('role', 'switch');
    xAxisSwitchTrack.setAttribute('aria-checked', String(currentXModeFromPayload(lastPayload) !== 'rpm'));
  } catch(_) {}

  function measure() {
    sliderWidth = xAxisSwitchSlider.offsetWidth || 0;
    trackWidth  = xAxisSwitchTrack.offsetWidth || 0;
    maxX = Math.max(0, trackWidth - sliderWidth);
  }

  function pos(type, animate = true) {
    const toNoise = (type === 'noise_db' || type === 'noise');
    const x = toNoise ? maxX : 0;
    xAxisSwitchSlider.style.transition = animate ? 'transform .25s ease' : xAxisSwitchSlider.style.transition || '';
    xAxisSwitchSlider.style.transform  = `translateX(${x}px)`;
    xAxisSwitchTrack.setAttribute('aria-checked', String(toNoise));
  }

  // 切换 X 模式后立即重建频谱标题（如果频谱开启）
  function applyType(newType) {
    const normalized = (newType === 'noise') ? 'noise_db' : newType;
    if (normalized !== 'rpm' && normalized !== 'noise_db') return;
    if (xAxisOverride === normalized) return;
    stopAllSweepAudio();
    xAxisOverride = normalized;
    try { localStorage.setItem('x_axis_type', normalized); } catch(_){}

    pos(normalized, true);

    // 保留跳过一次自动频谱请求的逻辑（避免重复拉取），但我们手动重建标题
    __skipSpectrumOnce = true;
    if (lastPayload) render(lastPayload);

    if (spectrumEnabled && spectrumChart) {
      try {
        buildAndSetSpectrumOption(true);   // 强制 fullRefresh 以更新 title
        placeSpectrumSwitchOverlay();      // 重新定位分辨率开关
      } catch(_) {}
    }

    if (typeof onXAxisChange === 'function') {
      try { onXAxisChange(normalized); } catch(_) {}
    }
  }

  function nearestType() {
    const m = new DOMMatrix(getComputedStyle(xAxisSwitchSlider).transform);
    const cur = m.m41 || 0;
    return cur > maxX / 2 ? 'noise_db' : 'rpm';
  }

  function onSliderPointerDown(e) {
    if (e.button !== undefined && e.button !== 0) return;
    measure();
    dragging = true; dragMoved = false;
    activePointerId = e.pointerId ?? null;
    startX = e.clientX;
    const m = new DOMMatrix(getComputedStyle(xAxisSwitchSlider).transform);
    base = m.m41 || 0;
    xAxisSwitchSlider.style.transition = 'none';
    try { if (activePointerId != null) xAxisSwitchSlider.setPointerCapture(activePointerId); } catch(_) {}
    e.preventDefault?.();
  }
  function onSliderPointerMove(e) {
    if (!dragging) return;
    const dx = e.clientX - startX;
    if (!dragMoved && Math.abs(dx) > 2) dragMoved = true;
    let x = base + dx;
    x = Math.max(0, Math.min(x, maxX));
    xAxisSwitchSlider.style.transform = `translateX(${x}px)`;
  }
  function onSliderPointerUp() {
    if (!dragging) return;
    dragging = false;
    try { if (activePointerId != null) xAxisSwitchSlider.releasePointerCapture(activePointerId); } catch(_) {}
    activePointerId = null;
    const newType = nearestType();
    pos(newType, true);
    applyType(newType);
  }
  function onSliderPointerCancel() {
    if (!dragging) return;
    dragging = false;
    try { if (activePointerId != null) xAxisSwitchSlider.releasePointerCapture(activePointerId); } catch(_) {}
    activePointerId = null;
    pos(currentXModeFromPayload(lastPayload), true);
  }

  xAxisSwitchTrack.addEventListener('click', (e) => {
    e.preventDefault();
    e.stopPropagation();
    if (dragMoved) { dragMoved = false; return; }
    const curr = currentXModeFromPayload(lastPayload);
    const next = (curr === 'rpm') ? 'noise_db' : 'rpm';
    pos(next, true);
    applyType(next);
  });

  xAxisSwitchTrack.addEventListener('pointerdown', onSliderPointerDown);
  window.addEventListener('pointermove', onSliderPointerMove, { passive: true });
  window.addEventListener('pointerup', onSliderPointerUp, { passive: true });
  window.addEventListener('pointercancel', onSliderPointerCancel);
  window.addEventListener('blur', onSliderPointerCancel);

  measure();
  pos(currentXModeFromPayload(lastPayload), false);
  window.addEventListener('resize', () => {
    const keep = currentXModeFromPayload(lastPayload);
    measure(); pos(keep, false);
  }, { passive:true });
}

  function placeAxisOverlayAt(x, y, show){
    const overlay = ensureAxisOverlay();
    const off = window.__FS_TOGGLE_OFFSET || { x: 0, y: 0 };
    overlay.style.left = (x + (Number(off.x)||0)) + 'px';
    overlay.style.top  = (y + (Number(off.y)||0)) + 'px';
    overlay.style.visibility = show ? 'visible' : 'hidden';
    requestAnimationFrame(() => updateAxisSwitchPosition({ force:true, animate:false }));
  }

function ensureFitUI(){
  let btns = getById('fitButtons');
  if (!btns){
    btns = document.createElement('div');
    btns.id = 'fitButtons';
    btns.className = 'fit-buttons';
    btns.innerHTML = `
      <button class="btn" id="btnFit" type="button">实时拟合</button>
    `;
    appendToRoot(btns);

    const btnFit = btns.querySelector('#btnFit');
    function syncButtons(){
      btnFit.classList.toggle('active', showFitCurves);
    }

    btnFit.addEventListener('click', (e)=>{
      e.preventDefault();
      e.stopPropagation();
      stopAllSweepAudio();
      showFitCurves = !showFitCurves;
      bubbleUserMoved = false;
      bubblePos.left = null;
      bubblePos.top  = null;

      syncButtons();
      toggleFitUI(showFitCurves);
      placeFitUI();
      repaintPointer();
      refreshFitPanel();
      updateLegendRailLayout();
      updateRailParkedState();
      try { chart && chart.resize(); } catch(_) {}
    });

    syncButtons();
  }

  // 指针（两种模式都需要）
  let ptr = getById('fitPointer');
  if (!ptr){
    ptr = document.createElement('div');
    ptr.id = 'fitPointer';
    ptr.className = 'fit-pointer';
    ptr.innerHTML = `<div class="line"></div><div class="handle" id="fitPointerHandle"></div>`;
    appendToRoot(ptr);
    bindPointerDrag();
  }

 let bubble = getById('fitBubble');
 if (!bubble){
   bubble = document.createElement('div');
   bubble.id = 'fitBubble';
   bubble.className = 'fit-bubble';
   bubble.innerHTML = `
     <div class="head">
       <div class="title">
         PCHIP 拟合当前位置
         <input id="fitXInput" type="number" step="1" />
         <span id="fitXUnit"></span>
       </div>
       <button id="fitCloseBtn" class="btn-close-top" type="button" aria-label="关闭">×</button>
     </div>
     <div id="fitBubbleRows"></div>
     <div class="foot">
       <div class="hint">按系列可见性（Legend）过滤，按风量从大到小排序</div>
     </div>
   `;
   document.body.appendChild(bubble);
   bubble.style.position = 'fixed';

   adoptBubbleHost();
   bindBubbleDrag(bubble);

   const xInput = bubble.querySelector('#fitXInput');
   xInput.addEventListener('input', onBubbleInputLive);
   xInput.addEventListener('change', onBubbleInputCommit);
   xInput.addEventListener('keydown', (e)=>{ if (e.key === 'Enter') { onBubbleInputCommit(); } });

   const btnsRoot = getById('fitButtons');
   const btnFit = btnsRoot ? btnsRoot.querySelector('#btnFit') : null;
   const btnClose = bubble.querySelector('#fitCloseBtn');
   if (btnClose){
     btnClose.addEventListener('click', (e) => {
       e.preventDefault();
       e.stopPropagation();
       stopAllSweepAudio();
       showFitCurves = false;
       if (btnFit) btnFit.classList.remove('active');
       toggleFitUI(false);
       updateRailParkedState();
       repaintPointer();
       refreshFitPanel();
       updateLegendRailLayout();
       try { chart && chart.resize(); } catch(_) {}
     });
   }
 }
}

  let bubbleUserMoved = false;
  const bubblePos = { left: null, top: null };

function bindBubbleDrag(bubble){
  if (!bubble) return;

  const pad = 6;

  createDragController(bubble, {
    axis: 'both',
    threshold: 4,
    onStart: (e) => {
      // 忽略输入等交互控件
      if (e.target && e.target.closest('input, textarea, select, button, [contenteditable]')) return false;
      const rect = bubble.getBoundingClientRect();
      bubble.__baseLeftFixed = rect.left;
      bubble.__baseTopFixed  = rect.top;
      bubble.__rootRectStart = root ? root.getBoundingClientRect() : { left:0, top:0 };
      bubble.classList.add('dragging');
    },
    onMove: ({ dx, dy }) => {
      const vw = window.innerWidth, vh = window.innerHeight;
      const bw = bubble.offsetWidth  || 0;
      const bh = bubble.offsetHeight || 0;

      let newLeft = bubble.__baseLeftFixed + dx;
      let newTop  = bubble.__baseTopFixed  + dy;

      newLeft = Math.min(Math.max(-bw + pad, newLeft), Math.max(pad, vw - pad));
      newTop  = Math.min(Math.max(-bh + pad, newTop ), Math.max(pad, vh - pad));

      bubble.style.left = Math.round(newLeft) + 'px';
      bubble.style.top  = Math.round(newTop)  + 'px';
      bubble.style.right  = 'auto';
      bubble.style.bottom = 'auto';

      const rNow = bubble.__rootRectStart || (root ? root.getBoundingClientRect() : { left:0, top:0 });
      bubblePos.left = newLeft - rNow.left;
      bubblePos.top  = newTop  - rNow.top;
      bubbleUserMoved = true;
    },
    onEnd: () => {
      bubble.classList.remove('dragging');
    }
  });
}

function toggleFitUI(showFit){
  const btns = getById('fitButtons');
  const ptr = getById('fitPointer');
  const empty  = !lastOption || lastOption.__empty;
  const integrated = isIntegratedLegendMode();

  const btnFit = btns ? btns.querySelector('#btnFit') : null;
  if (btnFit) {
    if (showFit) {
      btnFit.classList.add('active');
      btnFit.style.backgroundColor = '';                 // 使用 .active 的主色
      btnFit.style.color = '';                           // 使用 .active 的主色文字
    } else {
      btnFit.classList.remove('active');
      btnFit.style.backgroundColor = 'var(--fit-btn-bg)'; // 略醒目的次级背景
      btnFit.style.color = 'var(--fit-btn-fg)';           // 与整体配色协调的前景色
    }
    // 宽度撑满所在容器
    btnFit.style.width = '100%';
  }

  // 拟合按钮：始终跟随是否有内容可显示
  if (btns) btns.style.visibility = empty ? 'hidden' : 'visible';

  // 仅切换 panel 模式与可见性；内容由 refreshFitPanel/renderFitPanel 统一渲染
  if (integrated) {
    // 集成模式下始终隐藏气泡（立即，无动画）
    setFitBubbleVisible(false, { immediate: true });

    if (ptr) ptr.style.visibility = (showFit && !empty) ? 'visible' : 'hidden';
    // 集成模式下，renderLegendRailItems 会绘制基础容器；refreshFitPanel 会填充内容
    renderLegendRailItems();
  } else {
    const showFloating = showFit && !empty;

    // 非集成模式：通过统一函数控制浮动气泡淡入/淡出
    setFitBubbleVisible(showFloating);

    if (ptr) ptr.style.visibility = showFloating ? 'visible' : 'hidden';
  }

  updateRailParkedState();
}

function placeFitUI(){
  const btns = getById('fitButtons');
  const bubble = getById('fitBubble');
  if (!lastOption) return;
  const integrated = isIntegratedLegendMode();

  if (btns && __legendActionsEl) {
    btns.style.position = 'static';
    btns.style.right = '';
    btns.style.bottom = '';
    btns.style.visibility = (lastOption.__empty) ? 'hidden' : 'visible';
  } else if (btns && !integrated) {
    const narrow = layoutIsNarrow();
    btns.style.flexDirection = narrow ? 'column' : 'row';
    if (isFs) {
      btns.style.position = 'fixed';
      btns.style.right = '12px';
      btns.style.bottom = '12px';
    } else {
      btns.style.position = 'absolute';
      btns.style.right = '10px';
      btns.style.bottom = '10px';
    }
    btns.style.visibility = (lastOption.__empty) ? 'hidden' : 'visible';
  }

  const unitStr = (currentXModeFromPayload(lastPayload) === 'rpm') ? 'RPM' : 'dB';

  if (!integrated && bubble){
    const r = root ? root.getBoundingClientRect() : { left:0, top:0 };
    const grid = lastOption.grid || { left:40, top:60 };
    const left = (typeof grid.left==='number') ? grid.left : 40;
    const top = (typeof grid.top==='number') ? grid.top : 60;

    let offX, offY;
    if (!bubbleUserMoved || bubblePos.left == null || bubblePos.top == null){
      offX = left;
      offY = top;
      bubblePos.left = offX;
      bubblePos.top  = offY;
    } else {
      offX = bubblePos.left;
      offY = bubblePos.top;
    }

    const fx = r.left + offX;
    const fy = r.top  + offY;

    bubble.style.position = 'fixed';
    bubble.style.left = Math.round(fx) + 'px';
    bubble.style.top  = Math.round(fy) + 'px';
    bubble.style.right  = 'auto';
    bubble.style.bottom = 'auto';

    const showFloating = showFitCurves && !lastOption.__empty;

    // 位置更新时不再直接改 visibility，仅根据当前状态确保类名
    if (showFloating) {
      bubble.classList.add('is-visible');
      bubble.style.visibility = 'visible';
    } else {
      // 使用统一隐藏逻辑（淡出）；这里不强制 immediate，让动画正常跑
      setFitBubbleVisible(false);
    }

    const unitSpan = bubble.querySelector('#fitXUnit');
    if (unitSpan) unitSpan.textContent = unitStr;
  }

  if (integrated) {
    const unitSpan = getById('fitXUnitLegend');
    if (unitSpan) unitSpan.textContent = unitStr;
  }
}

function bindPointerDrag(){
  const handle = getById('fitPointerHandle');
  if (!handle) return;

  let baseX = 0;

  createDragController(handle, {
    axis: 'x',
    threshold: 2,
    onStart: (e) => {
      const ptr = getById('fitPointer');
      baseX = parseFloat(ptr?.style.left || '0');
      handle.style.cursor = 'grabbing';
    },
    onMove: ({ dx }) => {
      const grid = (function measureGrid(){
        if (!lastOption) return { left:40, right: (chart ? chart.getWidth() - 260 : 800), top: 60, height: 300 };
        const grid = lastOption.grid || { left:40, right: 260, top: 60, bottom: 40 };
        const chartW = chart ? chart.getWidth() : (root ? root.clientWidth : 800);
        const chartH = chart ? chart.getHeight() : (root ? root.clientHeight : 600);
        const left = (typeof grid.left==='number') ? grid.left : 40;
        const rightGap = (typeof grid.right==='number') ? grid.right : 260;
        const top = (typeof grid.top==='number') ? grid.top : 60;
        const bottomGap = (typeof grid.bottom==='number') ? grid.bottom : 40;
        const right = chartW - rightGap;
        const height = chartH - top - bottomGap;
        return { left, right, top, height };
      })();

      let x = baseX + dx;
      x = Math.max(grid.left, Math.min(x, grid.right));
      const ptr = getById('fitPointer');
      ptr.style.left = x + 'px';

      const xVal = pxToDataX(x);
      const mode = currentXModeFromPayload(lastPayload);
      if (Number.isFinite(xVal)) {
        stopAllSweepAudio();
        xQueryByMode[mode] = clampXDomain(xVal);
        syncBubbleInput();
        if (showFitCurves) refreshFitPanel();
        SpectrumController.onXQueryChange(xQueryByMode[mode]);
      }
    },
    onEnd: () => {
      handle.style.cursor = 'grab';
    }
  });
}

  function scheduleSpectrumRebuild(){
    if (!spectrumEnabled || !spectrumChart) return;
    if (__spectrumRaf) return;
    __spectrumRaf = requestAnimationFrame(() => {
      __spectrumRaf = null;
      buildAndSetSpectrumOption();
    });
  }

  function pxToDataX(xPixel){
    try { return chart.convertFromPixel({ xAxisIndex: 0 }, xPixel); } catch(e){ return NaN; }
  }
  function dataToPxX(xData){
    try { return chart.convertToPixel({ xAxisIndex: 0 }, xData); } catch(e){ return NaN; }
  }

  function repaintPointer(){
    const ptr = getById('fitPointer');
    if (!ptr || !lastOption) return;
    // 窄屏也显示拟合指针
    if (!showFitCurves || lastOption.__empty) { ptr.style.visibility = 'hidden'; return; }

    const grid = lastOption.grid || { left:40, right: 260, top: 60, bottom: 40 };
    const chartW = chart ? chart.getWidth() : (root ? root.clientWidth : 800);
    const chartH = chart ? chart.getHeight() : (root ? root.clientHeight : 600);
    const left = (typeof grid.left==='number') ? grid.left : 40;
    const rightGap = (typeof grid.right==='number') ? grid.right : 260;
    const top = (typeof grid.top==='number') ? grid.top : 60;
    const bottomGap = (typeof grid.bottom==='number') ? grid.bottom : 40;
    const height = chartH - top - bottomGap;

    const mode = currentXModeFromPayload(lastPayload);
    if (xQueryByMode[mode] == null) {
      const [vx0, vx1] = getVisibleXRange();
      xQueryByMode[mode] = (vx0 + vx1) / 2;
    }
    clampXQueryIntoVisibleRange();

    const xPixel = dataToPxX(xQueryByMode[mode]);
    if (!Number.isFinite(xPixel)) { ptr.style.visibility = 'hidden'; return; }

    ptr.style.position = 'absolute';
    ptr.style.left = xPixel + 'px';
    ptr.style.top = top + 'px';
    ptr.style.height = height + 'px';
    ptr.style.visibility = 'visible';

    syncBubbleInput();
    if (showFitCurves) refreshFitPanel();
  }

  function syncBubbleInput(){
    const inp = getById('fitXInput');
    if (!inp) return;
    const mode = currentXModeFromPayload(lastPayload);
    const val = Number(xQueryByMode[mode]);
    const rounded = (mode === 'noise_db') ? Number(val.toFixed(1)) : Math.round(val);
    inp.value = String(rounded);

    const [vx0, vx1] = getVisibleXRange();
    inp.setAttribute('min', String(Math.floor(vx0)));
    inp.setAttribute('max', String(Math.ceil(vx1)));
    inp.setAttribute('step', (mode === 'noise_db') ? '0.1' : '1');
  }

function onBubbleInputLive(){
  // Allow free typing without clamping - validation happens on commit (blur/Enter)
  // This allows users to type intermediate digits (e.g., "1000") and decimals without interruption
  const inp = getById('fitXInput');
  if (!inp) return;
  const mode = currentXModeFromPayload(lastPayload);
  const raw = Number(inp.value);
  // If not a valid number yet (e.g., user typed "." or "-"), just ignore - don't update anything
  if (!Number.isFinite(raw)) return;
  
  // Store the raw value WITHOUT clamping - let user type freely
  xQueryByMode[mode] = raw;
  
  // Note: No clamping, no pointer/fit panel updates during typing
  // All updates happen in onBubbleInputCommit when user finishes (blur/Enter)
}

function onBubbleInputCommit(){
  // On commit (blur/Enter): clamp to visible range and update UI
  const inp = getById('fitXInput');
  if (!inp) return;
  const mode = currentXModeFromPayload(lastPayload);
  const raw = Number(inp.value);
  
  // Parse and clamp to valid range
  if (Number.isFinite(raw)) {
    xQueryByMode[mode] = clampXDomain(raw);
    clampXQueryIntoVisibleRange();
  }
  
  // Update input to show clamped value
  const val = Number(xQueryByMode[mode]);
  if (Number.isFinite(val)) {
    const rounded = (mode === 'noise_db') ? Number(val.toFixed(1)) : Math.round(val);
    inp.value = String(rounded);
  }
  
  stopAllSweepAudio();
  repaintPointer();
  refreshFitPanel();                        
  SpectrumController.onXQueryChange(xQueryByMode[mode]);
}


function ensureFitModels(sList, xMode){
  const models = fitModelsCache[xMode];
  sList.forEach(s => {
    const ph = s && s.pchip
      ? (xMode === 'noise_db' ? s.pchip.noise_to_airflow : s.pchip.rpm_to_airflow)
      : null;

    const name = s.name || `${s.brand||''} ${s.model||''} - ${s.condition||''}`;
    if (ph && Array.isArray(ph.x) && Array.isArray(ph.y) && Array.isArray(ph.m) && ph.x0 != null && ph.x1 != null) {
      models.set(name, ph);
    } else {
      models.delete(name);
    }
  });
}

const __pchipModelCache = new WeakMap();
function __getPchipCache(model) {
  let cache = __pchipModelCache.get(model);
  if (!cache) {
    cache = new Map();
    __pchipModelCache.set(model, cache);
  }
  return cache;
}

function evalPchipJS(model, x){
  if (!model || !Array.isArray(model.x) || !Array.isArray(model.y) || !Array.isArray(model.m)) return NaN;

  // 基于 x 的三位小数离散化做缓存键
  const rounded = Math.round(x * 1000) / 1000;
  const cache = __getPchipCache(model);
  if (cache.has(rounded)) return cache.get(rounded);

  const xs = model.x, ys = model.y, ms = model.m;
  const n = xs.length;
  if (n === 0) return NaN;
  if (n === 1){
    const v0 = ys[0];
    cache.set(rounded, v0);
    return v0;
  }

  let xv = rounded;
  if (xv <= xs[0]) xv = xs[0];
  if (xv >= xs[n - 1]) xv = xs[n - 1];

  // 二分查找区间 i 使得 xs[i] <= xv <= xs[i+1]
  let lo = 0, hi = n - 2, i = 0;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (xs[mid] <= xv && xv <= xs[mid + 1]) { i = mid; break; }
    if (xv < xs[mid]) hi = mid - 1; else lo = mid + 1;
  }
  if (lo > hi) i = Math.max(0, Math.min(n - 2, lo));

  const x0 = xs[i], x1 = xs[i + 1];
  const h = (x1 - x0) || 1;
  const t = (xv - x0) / h;
  const y0 = ys[i], y1 = ys[i + 1];
  const m0 = ms[i] * h, m1 = ms[i + 1] * h;

  // Hermite 基函数
  const h00 = (2 * t*t*t - 3 * t*t + 1);
  const h10 = (t*t*t - 2 * t*t + t);
  const h01 = (-2 * t*t*t + 3 * t*t);
  const h11 = (t*t*t - t*t);

  const val = h00 * y0 + h10 * m0 + h01 * y1 + h11 * m1;

  // 简易 LRU：超限随机删除首项
  if (cache.size >= 256){
    const it = cache.keys().next();
    if (it && !it.done) cache.delete(it.value);
  }
  cache.set(rounded, val);
  return val;
}

  function computeSampleCount(widthPx){
    const n = Math.round(widthPx / 20); // 每 20px 一个点
    return Math.max(20, Math.min(50, n));
  }

  function resampleSingle(model, xmin, xmax, count){
    const n = Math.max(2, (count|0));
    const pts = [];
    const dom0 = Math.min(model.x0, model.x1);
    const dom1 = Math.max(model.x0, model.x1);
    const x0 = Math.max(xmin, dom0);
    const x1 = Math.min(xmax, dom1);
    if (!(x1 > x0)) return pts;
    for (let i = 0; i < n; i++){
      const t = (n === 1) ? 0 : (i / (n - 1));
      const x = x0 + (x1 - x0) * t;
      const y = evalPchipJS(model, x);
      pts.push({ x, y: Number.isFinite(y) ? y : NaN });
    }
    return pts;
  }

  function clampXDomain(val){
    if (!lastOption) return val;
    const xAxis = lastOption.xAxis || {};
    const xmin = Number(xAxis.min); const xmax = Number(xAxis.max);
    if (!Number.isFinite(xmin) || !Number.isFinite(xmax)) return val;
    return Math.max(xmin, Math.min(val, xmax));
  }

  function getVisibleXRange(){
    if (!lastOption) return [0,1];
    const xAxis = lastOption.xAxis || {};
    const xmin = Number(xAxis.min); const xmax = Number(xAxis.max);
    let vmin = xmin, vmax = xmax;
    const opt = chart.getOption() || {};
    const dz = (opt.dataZoom||[]).find(z => z.xAxisIndex === 0 || (z.xAxisIndex||0)===0) || null;
    if (dz && typeof dz.start === 'number' && typeof dz.end === 'number') {
      const span = xmax - xmin;
      vmin = xmin + span * (dz.start/100);
      vmax = xmin + span * (dz.end/100);
    }
    return [vmin, vmax];
  }

  function clampXQueryIntoVisibleRange(){
    const mode = currentXModeFromPayload(lastPayload);
    if (xQueryByMode[mode] == null) return;
    const [vmin, vmax] = getVisibleXRange();
    if (xQueryByMode[mode] < vmin) xQueryByMode[mode] = vmin;
    if (xQueryByMode[mode] > vmax) xQueryByMode[mode] = vmax;
  }

  async function enterFullscreen(){
    const shell = document.getElementById('chart-settings');
    const target = shell || (root && root.parentElement) || root || document.documentElement;
  
    try {
      if (target.requestFullscreen) { await target.requestFullscreen(); }
      else { await document.documentElement.requestFullscreen(); }
      isFs = true;
      if (isMobile() && screen.orientation && screen.orientation.lock) {
        try { await screen.orientation.lock('landscape'); } catch(_) {}
      }
    } catch(err){
      console.warn('requestFullscreen 失败：', err);
    } finally {
      adoptBubbleHost();
      bubbleUserMoved = false;
  
      updateFullscreenHeights();
      if (lastPayload) render(lastPayload); else if (chart) chart.resize();
  
      requestAnimationFrame(() => {
        try {
          placeFitUI(); repaintPointer(); updateSpectrumLayout();
          updateLegendRailLayout();  // 全屏下立刻重排 rail
          updateLegendRail();
        } catch(_) {}
      });
    }
  }

  async function exitFullscreen(){
    try {
      if (document.fullscreenElement) await document.exitFullscreen();
    } catch(err) {
      console.warn('exitFullscreen 失败：', err);
    } finally {
      isFs = false;
      adoptBubbleHost();
      bubbleUserMoved = false;

      // 退出全屏：统一清理内联高度，交给 CSS 恢复
      if (root) { try { root.style.minHeight = ''; } catch(_) {} }
      try {
        if (spectrumRoot) {
          spectrumRoot.style.removeProperty('max-height');
          spectrumRoot.style.removeProperty('flex');
          spectrumRoot.style.removeProperty('--fs-spec-h');
        }
      } catch(_) {}

      updateFullscreenHeights();
      updateSpectrumLayout();

      if (lastPayload) render(lastPayload);
      else if (chart) chart.resize();

      requestAnimationFrame(() => {
        try { placeFitUI(); repaintPointer(); } catch(_) {}
      });
    }
  }
  
  function toggleFullscreen(){ if (document.fullscreenElement) exitFullscreen(); else enterFullscreen(); }

  function ensureSpectrumChart() {
    ensureSpectrumHost();
    if (spectrumChart || !window.echarts || !spectrumInner) return;
    spectrumChart = echarts.init(spectrumInner, null, { renderer:'canvas', devicePixelRatio: window.devicePixelRatio || 1 });
  }

async function toggleSpectrumUI(show) {
  ensureSpectrumHost();
  spectrumEnabled = !!show;
  if (!spectrumRoot) return;

  specBumpEpochAndClearTimers();

  const shell =
    document.getElementById('chart-settings') ||
    (root && root.closest('.fc-chart-container')) ||
    document.documentElement;

  try { spectrumChart && spectrumChart.dispatchAction({ type: 'hideTip' }); } catch(_) {}

  const bgMs = getCssTransitionMs();
  const safetyMs = Math.max(240, bgMs + 120);
  const myEpoch = __specEpoch;

  if (show) {
    ensureSpectrumChart();

    if (isFs) {
      // 全屏展开：保持原 max-height 动画逻辑
      try { shell.setAttribute('data-chart-mode', 'spectrum'); } catch(_) {}

      spectrumRoot.style.setProperty('flex', '0 0 auto', 'important');
      spectrumRoot.style.setProperty('max-height', '0px');

      const targetPx = __computeFsSpecTargetPx();
      spectrumRoot.style.setProperty('--fs-spec-h', targetPx + 'px');
      void spectrumRoot.offsetHeight;

      requestAndRenderSpectrum().catch(()=>{}).then(() => {
        if (myEpoch !== __specEpoch) return;
        try { spectrumChart && spectrumChart.resize(); } catch(_) {}
      });

      requestAnimationFrame(() => {
        if (myEpoch !== __specEpoch) return;
        const onEnd = (e) => {
          if (myEpoch !== __specEpoch) return;
            if (e && e.propertyName && e.propertyName !== 'max-height') return;
          try { spectrumRoot.removeEventListener('transitionend', onEnd); } catch(_) {}
          try { chart && chart.resize(); } catch(_) {}
          try { spectrumChart && spectrumChart.resize(); } catch(_) {}
        };
        try { spectrumRoot.addEventListener('transitionend', onEnd, { once: true }); } catch(_) {}
        spectrumRoot.style.setProperty('max-height', targetPx + 'px');
      });
    } else {
      // 非全屏展开：height 过渡 + 合并滚动监听
      const targetPx = __computeNfSpecTargetPx();
      if (spectrumInner) {
        spectrumInner.style.height = targetPx + 'px';
        try { ensureSpectrumChart(); spectrumChart && spectrumChart.resize(); } catch(_) {}
        spectrumInner.style.transition = 'transform var(--transition-speed, .25s) ease';
        spectrumInner.style.opacity = '1';
      }
      spectrumRoot.classList.add('anim-scale', 'reveal-from-0');
      try { shell.setAttribute('data-chart-mode', 'spectrum'); } catch(_) {}

      requestAndRenderSpectrum().catch(()=>{}).then(() => {
        if (myEpoch !== __specEpoch) return;
        try { spectrumChart && spectrumChart.resize(); } catch(_) {}
      });

      requestAnimationFrame(() => {
        if (myEpoch !== __specEpoch) return;

        // 合并监听：同时负责清理类与滚动
        let scrolled = false;
        const fallbackDelay = Math.min(400, (getCssTransitionMs() || 250) + 50);
        const fallbackId = setTimeout(() => {
          if (myEpoch !== __specEpoch) return;
          if (scrolled) return;
          scrolled = true;
          try {
            const rect = spectrumRoot.getBoundingClientRect();
            const vh = window.innerHeight || document.documentElement.clientHeight || 0;
            if (rect.bottom > vh - 8) {
              spectrumRoot.scrollIntoView({ behavior: 'smooth', block: 'start' });
            }
          } catch(_) {}
        }, fallbackDelay);

        const onEnd = (e) => {
          if (myEpoch !== __specEpoch) return;
          if (e && e.propertyName !== 'height') return; // 只关心 height 动画结束
          try { spectrumRoot.removeEventListener('transitionend', onEnd); } catch(_) {}

          // 清理展开过程中使用的临时样式/类
          spectrumRoot.classList.remove('anim-scale', 'reveal-from-0', 'revealed');
          if (spectrumInner) {
            spectrumInner.style.transition = '';
            spectrumInner.style.opacity = '';
            spectrumInner.style.height = '';
          }
          try { spectrumChart && spectrumChart.resize(); } catch(_) {}

          // 条件滚动：频谱底部超出视口时才滚动
          if (!scrolled) {
            try {
              const rect = spectrumRoot.getBoundingClientRect();
              const vh = window.innerHeight || document.documentElement.clientHeight || 0;
              if (rect.bottom > vh - 8) {
                spectrumRoot.scrollIntoView({ behavior: 'smooth', block: 'start' });
              }
            } catch(_) {}
            scrolled = true;
          }

          try { clearTimeout(fallbackId); } catch(_) {}
        };

        try { spectrumRoot.addEventListener('transitionend', onEnd); } catch(_) {}
        spectrumRoot.classList.add('revealed');

        // 安全超时：过渡异常（未触发）时仍清理
        specSetTimeout(() => {
          if (myEpoch !== __specEpoch) return;
          spectrumRoot.classList.remove('anim-scale', 'reveal-from-0', 'revealed');
          if (spectrumInner) {
            spectrumInner.style.transition = '';
            spectrumInner.style.opacity = '';
            spectrumInner.style.height = '';
          }
        }, safetyMs);
      });
    }
  } else {
    // 收起逻辑保持原来结构
    const cleanupAfter = () => {
      if (myEpoch !== __specEpoch) return;
      try { shell.removeAttribute('data-chart-mode'); } catch(_) {}
      if (root) try { root.style.minHeight = ''; } catch(_) {}
      spectrumRoot.classList.remove('anim-scale', 'revealed', 'reveal-from-0', 'collapse-to-0');
      if (spectrumInner) {
        spectrumInner.style.transition = '';
        spectrumInner.style.opacity = '';
        spectrumInner.style.height = '';
      }
      try {
        spectrumRoot.style.removeProperty('max-height');
        spectrumRoot.style.removeProperty('flex');
      } catch(_) {}
    };

    if (isFs) {
      const curH = Math.max(1, Math.round((spectrumRoot.getBoundingClientRect().height || 1)));
      spectrumRoot.style.setProperty('flex', '0 0 auto', 'important');
      spectrumRoot.style.setProperty('--fs-spec-h', curH + 'px');
      spectrumRoot.style.setProperty('max-height', curH + 'px');

      requestAnimationFrame(() => {
        if (myEpoch !== __specEpoch) return;
        const onEnd = (e) => {
          if (myEpoch !== __specEpoch) return;
          if (e && e.propertyName && e.propertyName !== 'max-height') return;
          try { spectrumRoot.removeEventListener('transitionend', onEnd); } catch(_) {}
          cleanupAfter();
          try {
            spectrumRoot.style.removeProperty('max-height');
            spectrumRoot.style.removeProperty('flex');
            spectrumRoot.style.removeProperty('--fs-spec-h');
          } catch(_) {}
          updateFullscreenHeights();
          try { chart && chart.resize(); } catch(_) {}
        };
        try { spectrumRoot.addEventListener('transitionend', onEnd, { once:true }); } catch(_) {}
        spectrumRoot.style.setProperty('max-height', '0px');
      });

      specSetTimeout(() => { try { cleanupAfter(); updateFullscreenHeights(); } catch(_) {} }, Math.max(900, (getCssTransitionMs()||350)+300));
    } else {
      const curH = Math.max(1, Math.round((spectrumRoot.getBoundingClientRect().height || 1)));
      if (spectrumInner) {
        spectrumInner.style.height = curH + 'px';
        spectrumInner.style.transition = 'transform var(--transition-speed, .25s) ease';
        spectrumInner.style.opacity = '1';
      }

      spectrumRoot.classList.add('anim-scale', 'collapse-to-0');

      requestAnimationFrame(() => {
        if (myEpoch !== __specEpoch) return;
        const onEnd = (e) => {
          if (myEpoch !== __specEpoch) return;
          if (e && e.propertyName !== 'height') return;
          try { spectrumRoot.removeEventListener('transitionend', onEnd); } catch(_) {}
          cleanupAfter();
          spectrumRoot.classList.remove('anim-scale', 'collapse-to-0');
        };
        try { spectrumRoot.addEventListener('transitionend', onEnd, { once:true }); } catch(_) {}
        try { shell.removeAttribute('data-chart-mode'); } catch(_) {}
      });
    }
  }

  // 外置按钮同步
  try { syncSpectrumDockUi(); } catch(_) {}
  try { placeSpectrumDock(); } catch(_) {}
}
  
function updateSpectrumLayout() {
  if (!spectrumRoot || !spectrumInner) return;
  if (isFs) {
    spectrumRoot.style.height = '';
    spectrumInner.style.height = '';
  }
  if (spectrumChart) { 
    try { spectrumChart.resize(); } catch(_) {} 
    // 布局变化时重算开关位置
    placeSpectrumSwitchOverlay();
  }
}

function getXQueryOrDefault(mode){
  let x = xQueryByMode[mode];
  if (x == null) {
    const [vx0, vx1] = getVisibleXRange();
    x = (vx0 + vx1) / 2;
  }
  return x;
}

function getSeriesRpmForCurrentX(series, spectrumModel){
  const mode = currentXModeFromPayload(lastPayload);
  if (mode === 'rpm') {
    const rpm = Number(getXQueryOrDefault('rpm'));
    return Number.isFinite(rpm) && rpm > 0 ? rpm : 0;
  } else {
    const noiseX = Number(getXQueryOrDefault('noise_db'));
    const ph = series?.pchip?.noise_to_rpm;
    if (ph && Array.isArray(ph.x) && Array.isArray(ph.y) && Array.isArray(ph.m)) {
      const rpm = Number(evalPchipJS(ph, noiseX));
      return Number.isFinite(rpm) && rpm > 0 ? rpm : 0;
    }
    return rpmMaxForSeries(series, spectrumModel);
  }
}

async function requestAndRenderSpectrum(forceFull = false) {
  ensureSpectrumHost();
  ensureSpectrumChart();
  updateSpectrumLayout();

  const sList = getSeriesArray();
  if (!Array.isArray(sList) || !sList.length) {
    // 清空 pending/missing
    SpectrumController.__setPendingKeys([]);
    SpectrumController.__setMissingKeys([]);
    __specPending = false;
    setSpectrumLoading(false);
    buildAndSetSpectrumOption(forceFull);
    return;
  }

  const wanted = new Set();
  const pairsAll = [];
  const seen = new Set();
  sList.forEach(s => {
    const midRaw = (s.modelId != null) ? s.modelId : s.model_id;
    const cidRaw = (s.conditionId != null) ? s.conditionId : s.condition_id;
    const mid = Number(midRaw);
    const cid = Number(cidRaw);
    if (!Number.isInteger(mid) || !Number.isInteger(cid)) return;
    const key = `${mid}_${cid}`;
    if (seen.has(key)) return;
    seen.add(key);
    wanted.add(key);
    pairsAll.push({ model_id: mid, condition_id: cid });
  });

  const wantedArr = Array.from(wanted);
  if (!wantedArr.length) {
    SpectrumController.__setPendingKeys([]);
    SpectrumController.__setMissingKeys([]);
    __specPending = false;
    setSpectrumLoading(false);
    buildAndSetSpectrumOption(forceFull);
    return;
  }

  const pendingSet = new Set(SpectrumController.getPendingKeys ? SpectrumController.getPendingKeys() : []);
  const wantedHash = wantedArr.slice().sort().join('|');

  // 短路：仅在“所有 wanted key 都已命中缓存”时跳过接口调用
  const allCachedNow = wantedArr.every(k => spectrumModelCache.has(k));
  if (typeof requestAndRenderSpectrum.__lastWantedHash === 'string' &&
      requestAndRenderSpectrum.__lastWantedHash === wantedHash &&
      allCachedNow) {
    __specPending = false;
    setSpectrumLoading(false);
    SpectrumController.__setPendingKeys([]);
    buildAndSetSpectrumOption(forceFull);
    return;
  }
  requestAndRenderSpectrum.__lastWantedHash = wantedHash;

  // 若当前已经全部在缓存里，直接渲染
  if (allCachedNow) {
    __specPending = false;
    SpectrumController.__setPendingKeys([]);
    SpectrumController.__setMissingKeys([]);
    setSpectrumLoading(false);
    buildAndSetSpectrumOption(forceFull);
    return;
  }

  if (__specFetchInFlight) {
    try { buildAndSetSpectrumOption(true); } catch (_) {}
    __specRerunQueued = true;
    return;
  }

  __specFetchInFlight = true;
  try {
    // 只为“本次仍需要请求的 key”打接口：
    // - 首次：pendingSet 为空，会用所有 wanted；
    // - 之后轮询：只请求 pendingSet 里仍未完成的那几条。
    let pairsToRequest;
    if (pendingSet.size > 0) {
      const pendingKeysArr = Array.from(pendingSet);
      pairsToRequest = pendingKeysArr.map(k => {
        const [midStr, cidStr] = k.split('_');
        return { model_id: Number(midStr), condition_id: Number(cidStr) };
      }).filter(p => Number.isInteger(p.model_id) && Number.isInteger(p.condition_id));
    } else {
      pairsToRequest = pairsAll;
    }

    const fetchRes = await fetchSpectrumModelsForPairs(pairsToRequest);

    const pendingKeys = wantedArr.filter(
      k => !spectrumModelCache.has(k) && !fetchRes.missingKeys.has(k)
    );
    __specPending = pendingKeys.length > 0;

    // 写入 controller 状态
    SpectrumController.__setPendingKeys(pendingKeys);
    SpectrumController.__setMissingKeys(Array.from(fetchRes.missingKeys || []));

    buildAndSetSpectrumOption(forceFull);

    if (__specPending) {
      setSpectrumLoading(
        true,
        `${pendingKeys.length}条频谱渲染中，可能需要数分钟，你可以先浏览其他数据...`
      );
      // 轮询节奏：10 秒
      specSetTimeout(() => {
        if (spectrumEnabled) requestAndRenderSpectrum(false);
      }, 10000);
    } else {
      setSpectrumLoading(false);
    }
  } finally {
    __specFetchInFlight = false;
    if (__specRerunQueued && spectrumEnabled) {
      __specRerunQueued = false;
      try { buildAndSetSpectrumOption(true); } catch (_) {}
    }
  }
}

async function fetchSpectrumModelsForPairs(pairs) {
  if (!Array.isArray(pairs) || !pairs.length) {
    return { modelsLoaded: 0, missingKeys: new Set(), rebuildingKeys: new Set() };
  }
  const resp = await fetch('/api/spectrum-models', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ pairs })
  });
  const j = await resp.json();
  const ok = !!(j && typeof j === 'object' && j.success === true);
  if (!ok) throw new Error((j && j.error_message) || '频谱模型接口失败');

  const data = j.data || {};
  const models = Array.isArray(data.models) ? data.models : [];
  let loaded = 0;

  models.forEach(item => {
    const mid = Number(item.model_id), cid = Number(item.condition_id);
    const key = `${mid}_${cid}`;
    const model = (item && item.model) || null;
    if (model && Array.isArray(model.centers_hz) && Array.isArray(model.band_models_pchip)) {
      spectrumModelCache.set(key, model);
      loaded++;
    }
  });

  const toKeySet = (arr) => {
    const s = new Set();
    (Array.isArray(arr) ? arr : []).forEach(e => {
      const mid = Number(e && e.model_id), cid = Number(e && e.condition_id);
      if (Number.isInteger(mid) && Number.isInteger(cid)) s.add(`${mid}_${cid}`);
    });
    return s;
  };

  return {
    modelsLoaded: loaded,
    missingKeys: toKeySet(data.missing),
    rebuildingKeys: toKeySet(data.rebuilding)
  };
}

function getLegendSelectionMap(){
  try {
    const opt = chart.getOption() || {};
    return (opt.legend && opt.legend[0] && opt.legend[0].selected) || {};
  } catch(_) { return {}; }
}

function rpmMinForSeries(s, model) {
  const fromModel = Number(model && model.rpm_min);
  if (Number.isFinite(fromModel) && fromModel > 0) return fromModel;
  const arr = Array.isArray(s?.data?.rpm) ? s.data.rpm.filter(v => Number.isFinite(Number(v))) : [];
  if (arr.length) return Math.min.apply(null, arr.map(Number));
  return 0;
}

function rpmMaxForSeries(s, model) {
  const fromModel = Number(model && model.rpm_max);
  if (Number.isFinite(fromModel) && fromModel > 0) return fromModel;
  const arr = Array.isArray(s?.data?.rpm) ? s.data.rpm.filter(v => Number.isFinite(Number(v))) : [];
  if (arr.length) return Math.max.apply(null, arr.map(Number));
  return 0;
}

function buildAndSetSpectrumOption(fullRefresh = false, themeOverride) {
  if (!spectrumChart) return;
  try { ensureSpectrumResolutionSwitch(); } catch(_) {}

  const themeName =
    themeOverride ||
    (lastPayload && lastPayload.theme) ||
    (document.documentElement.getAttribute('data-theme') || 'light');

  const allSeries = getSeriesArray();
  const t = tokens(themeName);

  const gridMain = (lastOption && lastOption.grid) || { left: 40, right: 260, top: 60, bottom: 40 };
  const left = (typeof gridMain.left === 'number') ? gridMain.left : 40;
  const rightGap = (typeof gridMain.right === 'number') ? gridMain.right : 260;

  const fmtHz = (v) => {
    const n = Number(v);
    if (!Number.isFinite(n)) return String(v);
    return n >= 1000 ? (n / 1000).toFixed(1) + ' kHz' : n.toFixed(1) + ' Hz';
  };

  const selMapFromMain = getLegendSelectionMap();

  // 所有系列名称（供 legend.data 推断），显隐仍由 selected 控制
  const namesAll = allSeries.map(s => s.name || `${s.brand || ''} ${s.model || ''} - ${s.condition || ''}`);

  // 可见系列仅用于 yMax/空态等判定，避免隐藏曲线影响纵轴
  const visibleSeriesForMetrics = allSeries.map(s => {
    const name = s.name || `${s.brand || ''} ${s.model || ''} - ${s.condition || ''}`;
    return { ...s, __name: name };
  }).filter(s => (selMapFromMain ? selMapFromMain[s.__name] !== false : true));

  const visibleKeys = visibleSeriesForMetrics
    .map(s => {
      const midRaw = (s.modelId != null) ? s.modelId : s.model_id;
      const cidRaw = (s.conditionId != null) ? s.conditionId : s.condition_id;
      return `${Number(midRaw)}_${Number(cidRaw)}`;
    })
    .sort()
    .join('|');

  let modelReadyCount = 0;
  visibleSeriesForMetrics.forEach(s => {
    const midRaw = (s.modelId != null) ? s.modelId : s.model_id;
    const cidRaw = (s.conditionId != null) ? s.conditionId : s.condition_id;
    const k = `${Number(midRaw)}_${Number(cidRaw)}`;
    if (spectrumModelCache.has(k)) modelReadyCount++;
  });

  const needRecalcYMax =
    fullRefresh ||
    !lastSpectrumOption ||
    lastSpectrumOption.__visibleHash !== visibleKeys ||
    !Number.isFinite(lastSpectrumOption.__yMaxFixed) ||
    lastSpectrumOption.__modelReadyCount !== modelReadyCount;

  if (needRecalcYMax) {
    const fixedMax = computeSpectrumYMaxFixed(); // 内部已按可见 series 计算
    lastSpectrumOption = {
      __yMax: fixedMax,
      __yMaxFixed: fixedMax,
      __visibleHash: visibleKeys,
      __modelReadyCount: modelReadyCount
    };
  }

  // 构建所有“有模型”的线，显隐交给 legend.selected
  const lines = [];
  let hasAnyDataVisible = false;
  const selectedSet = new Set(
    namesAll.filter(n => (selMapFromMain ? selMapFromMain[n] !== false : true))
  );

  allSeries.forEach(s => {
    const name = s.name || `${s.brand || ''} ${s.model || ''} - ${s.condition || ''}`;
    const midRaw = (s.modelId != null) ? s.modelId : s.model_id;
    const cidRaw = (s.conditionId != null) ? s.conditionId : s.condition_id;
    const mid = Number(midRaw), cid = Number(cidRaw);
    if (!Number.isInteger(mid) || !Number.isInteger(cid)) return;

        const model = spectrumModelCache.get(`${mid}_${cid}`);
    if (!model) return;

    const centers = Array.isArray(model.centers_hz) ? model.centers_hz : (model.freq_hz || model.freq || []);
    const bands = Array.isArray(model.band_models_pchip) ? model.band_models_pchip : [];
    if (!(centers.length && bands.length)) return;

    const rpmTarget = getSeriesRpmForCurrentX(s, model);
    let pts = [];
    if (Number.isFinite(rpmTarget) && rpmTarget > 0) {
      const fineCenters = [];
      const fineValsDb = [];
      for (let i = 0; i < centers.length; i++) {
        const hz = Number(centers[i]);
        if (!Number.isFinite(hz)) continue;
        const bm = bands[i];
        if (!bm || !Array.isArray(bm.x) || !Array.isArray(bm.y) || !Array.isArray(bm.m)) continue;
        const raw = Number(evalPchipJS(bm, rpmTarget));
        const db = Number.isFinite(raw) ? raw : NaN;
        if (!Number.isFinite(db)) continue;
        fineCenters.push(hz);
        fineValsDb.push(db);
      }

      if (fineCenters.length) {
        // 根据当前前端分辨率模式决定是否做 IEC 聚合
        const modeRes = getSpectrumResolutionMode();   // '1_48' | '1_12' | '1_3'
        // 从模型中推断后端 n_per_oct，默认为 48
        const calib = model.calibration || {};
        const fineNPerOct = Number(
          calib.n_per_oct != null
            ? calib.n_per_oct
            : (model.n_per_oct != null ? model.n_per_oct : 48)
        );
        let centersUse = fineCenters;
        let valsUse = fineValsDb;

        if (modeRes === '1_3' || modeRes === '1_12') {
          const coarseN = (modeRes === '1_3') ? 3 : 12;
          const agg = downsampleSpectrumBands(fineCenters, fineValsDb, fineNPerOct, coarseN);
          centersUse = agg.centers;
          valsUse = agg.valuesDb;
        }

        pts = centersUse.map((hz, idx) => [hz, valsUse[idx]]);
      }
    }

    // 仅当“可见且有数据”时，认为当前有可渲染数据（用于空态判定）
    if (pts.length && selectedSet.has(name)) hasAnyDataVisible = true;

    if (pts.length) {
      lines.push({ id: `spec:${mid}_${cid}`, name, color: s.color, data: pts });
    }
  });

  const canvasBg = (lastOption && lastOption.backgroundColor) || getExportBg();
  const isPending =
    !!__specPending ||
    (typeof SpectrumController?.getPendingKeys === 'function' && SpectrumController.getPendingKeys().length > 0);

  const optionObj = hasAnyDataVisible ? {
    backgroundColor: canvasBg,
    textStyle: { fontFamily: t.fontFamily },
    title: {
      text: buildSpectrumTitle(),
      left: 'center',
      top: 6,
      textStyle: { color: t.axisLabel, fontWeight: 600, fontSize: 16, fontFamily: t.fontFamily }
    },
    grid: { left, right: rightGap, top: 38, bottom: 60 },
    xAxis: {
      type: 'log',
      logBase: 10,
      min: SPECTRUM_X_MIN,
      max: SPECTRUM_X_MAX,
      name: '频率',
      nameGap: 25,
      nameLocation: 'middle',
      nameTextStyle: { color: t.axisName, fontWeight: 600 },
      axisLabel: { color: t.axisLabel, formatter: fmtHz },
      axisLine: { lineStyle: { color: t.axisLine } },
      splitLine: { show: true, lineStyle: { color: t.gridLine } },
      minorTick: { show: true, splitNumber: 9 },
      minorSplitLine: { show: true, lineStyle: { color: t.gridLine, opacity: 0.4 } }
    },
    yAxis: {
      type: 'value',
      show: true,
      min: SPECTRUM_Y_MIN,
      max: Math.max(0, Number(lastSpectrumOption?.__yMaxFixed) || 60),
      name: '声级(dB)',
      nameTextStyle: { color: t.axisName, fontWeight: 600 },
      axisLabel: { color: t.axisLabel },
      axisLine: { lineStyle: { color: t.axisLine } },
      splitLine: { show: true, lineStyle: { color: t.gridLine } }
    },
    tooltip: buildSpectrumTooltip(t),
    legend: {
      show: false,
      selected: selMapFromMain,
      data: namesAll  // 确保 legend 组件知道完整的系列列表
    },
    series: lines.map(l => ({
      id: l.id,
      name: l.name,
      type: 'line',
      showSymbol: false,
      symbol: 'circle',
      symbolSize: 4,
      smooth: false,
      connectNulls: false,
      data: l.data,
      lineStyle: { width: 1.5, color: l.color },
      itemStyle: { color: l.color },
      silent: false,
      z: 1,
      clip: true,
      animation: true,
      animationDuration: 1500,
      animationEasing: 'cubicOut',
      animationDelay: idx => idx * 14,
      animationDurationUpdate: 500,
      animationEasingUpdate: 'cubicOut',
      universalTransition: true,
      emphasis: { focus: 'series', blurScope: 'coordinateSystem' }
    }))
  } : (
    isPending
      ? {
          backgroundColor: canvasBg,
          grid: { left, right: rightGap, top: 36, bottom: 18 },
          xAxis: { show: false, min: SPECTRUM_X_MIN, max: SPECTRUM_X_MAX },
          yAxis: { show: false, min: SPECTRUM_Y_MIN, max: Math.max(0, Number(lastSpectrumOption?.__yMaxFixed) || 60) },
          legend: { show: false, selected: selMapFromMain, data: namesAll },
          series: []
        }
      : {
          backgroundColor: canvasBg,
          title: {
            text: '当前无可渲染频谱',
            left: 'center',
            top: 'middle',
            textStyle: { color: t.axisLabel, fontWeight: 600, fontSize: 14, fontFamily: t.fontFamily }
          },
          grid: { left, right: rightGap, top: 36, bottom: 18 },
          xAxis: { show: false, min: SPECTRUM_X_MIN, max: SPECTRUM_X_MAX },
          yAxis: { show: false, min: SPECTRUM_Y_MIN, max: Math.max(0, Number(lastSpectrumOption?.__yMaxFixed) || 60) },
          legend: { show: false, selected: selMapFromMain, data: namesAll },
          series: []
        }
  );

  spectrumChart.setOption(optionObj, fullRefresh ? true : false);
  spectrumChart.resize();
  requestAnimationFrame(placeSpectrumSwitchOverlay);
}

function buildSpectrumTooltip(t){
  const fmtX = (v) => {
    const n = Number(v);
    if (!Number.isFinite(n)) return String(v);
    return n >= 1000 ? (n/1000).toFixed(1) + ' kHz' : n.toFixed(1) + ' Hz';
  };

  return {
    ...buildTooltipBase(t, { appendToBody: !isFs }),
    confine: false,
    trigger: 'axis',
    triggerOn: 'mousemove|click|touchstart|touchmove',
    borderRadius: 10,
    axisPointer: {
      type: 'line',
      snap: true,
      label: {
        // 横坐标无论 Hz 或 kHz 均保留一位小数
        formatter: (obj) => fmtX(obj?.value)
      }
    },
    position: function (pos) {
      const x = Array.isArray(pos) ? pos[0] : 0;
      const y = Array.isArray(pos) ? pos[1] : 0;
      const gap = 12;
      return [Math.round(x + gap), Math.round(y + gap)];
    },
    formatter: function (params) {
      if (!Array.isArray(params) || !params.length) return '';
      const x = params[0]?.axisValue;
      // 横坐标标题统一一位小数
      const head = `<div style="font-weight:800;margin-bottom:4px;">${fmtX(x)}</div>`;
      const linesHtml = params.map(p => {
        const v = Array.isArray(p.data) ? p.data[1] : (Number(p.value) || NaN);
        const dB = Number.isFinite(v) ? v.toFixed(1) + ' dB' : '-';
        return `<div style="display:flex;align-items:center;gap:6px;">
                  <span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:${p.color};"></span>
                  <span>${p.seriesName}</span>
                  <span style="margin-left:auto;color:var(--text-muted);font-variant-numeric:tabular-nums;">${dB}</span>
                </div>`;
      }).join('');
      return head + linesHtml;
    }
  };
}

function buildSpectrumTitle() {
  const mode = currentXModeFromPayload(lastPayload);
  const x = Number(getXQueryOrDefault(mode));

  const resMode = getSpectrumResolutionMode();

  // FIGURE SPACE (U+2007) 宽度接近数字字符，用来给 “1/3” 补足到与 “1/12” / “1/48” 接近的视觉长度
  const FRAC_PAD = '\u2007';

  // 基础分辨率段（仅“1/n”），对 1/3 追加占位
  let fractionPart = '\u3000\u3000\u3000\u30001/48';
  if (resMode === '1_12') fractionPart = '\u3000\u3000\u3000\u30001/12';
  if (resMode === '1_3')  fractionPart = '\u3000\u3000\u3000\u30001/3' + FRAC_PAD;

  // 两个空格分隔 “1/n” 与 “OCT”，并在 “OCT” 后留两个空格作为与后续内容以及覆盖开关的间距
  // 注意：placeSpectrumSwitchOverlay 仅覆盖 fractionPart，本实现不会让滑块遮住“OCT”
  const resLabel = `${fractionPart} OCT  `;

  if (mode === 'rpm') {
    const xr = Number.isFinite(x) ? Math.round(x) : '-';
    return `${resLabel}A计权声级频谱 @ ${xr} RPM`;
  } else {
    const v = Number.isFinite(x) ? x.toFixed(1) : '-';
    return `${resLabel}A计权声级频谱 @ ${v} dB`;
  }
}

function computeSpectrumYMaxFixed() {
  const sList = getSeriesArray();
  const selected = getLegendSelectionMap();

  // 当前前端频谱分辨率：决定是否做 IEC 聚合
  const resMode = getSpectrumResolutionMode();   // '1_48' | '1_12' | '1_3'
  const wantCoarse = (resMode === '1_3' || resMode === '1_12');

  let globalMax = 0;

  sList.forEach(s => {
    const name = s.name || `${s.brand || ''} ${s.model || ''} - ${s.condition || ''}`;
    if (selected && selected[name] === false) return;

    // 兼容 modelId / model_id
    const midRaw = (s.modelId != null) ? s.modelId : s.model_id;
    const cidRaw = (s.conditionId != null) ? s.conditionId : s.condition_id;
    const mid = Number(midRaw), cid = Number(cidRaw);
    if (!Number.isInteger(mid) || !Number.isInteger(cid)) return;

    const model = spectrumModelCache.get(`${mid}_${cid}`);
    if (!model) return;

    const centers = Array.isArray(model.centers_hz) ? model.centers_hz : (model.freq_hz || model.freq || []);
    const bands   = Array.isArray(model.band_models_pchip) ? model.band_models_pchip : [];
    if (!centers.length || !bands.length) return;

    // 用“该系列的最大 RPM”来计算一个“极端情况下”的频谱峰值
    const rpmMax = rpmMaxForSeries(s, model);
    if (!Number.isFinite(rpmMax) || rpmMax <= 0) return;

    const fineCenters = [];
    const fineValsDb  = [];
    for (let i = 0; i < centers.length; i++) {
      const hz = Number(centers[i]);
      if (!Number.isFinite(hz)) continue;
      const bandModel = bands[i];
      if (!bandModel || !Array.isArray(bandModel.x) || !Array.isArray(bandModel.y) || !Array.isArray(bandModel.m)) continue;
      const v = Number(evalPchipJS(bandModel, rpmMax));
      const db = Number.isFinite(v) ? v : NaN;
      if (!Number.isFinite(db)) continue;
      fineCenters.push(hz);
      fineValsDb.push(db);
    }
    if (!fineCenters.length) return;

    // 从模型中推断后端 n_per_oct，默认为 48
    const calib = model.calibration || {};
    const fineNPerOct = Number(
      calib.n_per_oct != null
        ? calib.n_per_oct
        : (model.n_per_oct != null ? model.n_per_oct : 48)
    );

    let valsToScan = fineValsDb;

    // 若当前显示模式是 1/3 或 1/12，则在聚合后的频带上取峰值
    if (wantCoarse) {
      const coarseN = (resMode === '1_3') ? 3 : 12;
      const agg = downsampleSpectrumBands(fineCenters, fineValsDb, fineNPerOct, coarseN);
      valsToScan = Array.isArray(agg.valuesDb) ? agg.valuesDb : [];
      if (!valsToScan.length) {
        // 降采样失败就退回细分谱
        valsToScan = fineValsDb;
      }
    }

    for (let i = 0; i < valsToScan.length; i++) {
      const db = Number(valsToScan[i]);
      if (!Number.isFinite(db)) continue;
      if (db > globalMax) globalMax = db;
    }
  });

  // 给一点裕量，避免顶到边
  const withMargin = (globalMax || 60) * 1.1;
  return Math.max(10, Math.ceil(withMargin));
}

function __ensureMainChartMinHeightForSpectrumMode() {
  if (!root) return;
  const vh = window.innerHeight || 800;
  const clampH = Math.max(480, Math.min(600, Math.round(vh * 0.62)));
  root.style.minHeight = clampH + 'px';
}

function revealSpectrumIfNeeded() {
  if (!spectrumRoot) return;
  const rect = spectrumRoot.getBoundingClientRect();
  const vh = window.innerHeight || document.documentElement.clientHeight || 0;
  // 若底部超出可视区域，则滚动让其露出（对齐到开始位置）
  if (rect.bottom > vh - 8) {
    try { spectrumRoot.scrollIntoView({ behavior: 'smooth', block: 'start' }); } catch(_) {
      // 兜底：使用 window.scrollTo
      const se = document.scrollingElement || document.documentElement || document.body;
      const top = (window.scrollY || se.scrollTop || 0) + (rect.top - 8);
      window.scrollTo({ top, behavior: 'smooth' });
    }
  }
}

async function exportCombinedImage() {
  if (!chart) return;

  try { chart.dispatchAction({ type: 'hideTip' }); } catch(_) {}
  try { spectrumChart && spectrumChart.dispatchAction({ type: 'hideTip' }); } catch(_) {}
  try { chart.resize(); } catch(_) {}
  try { spectrumEnabled && spectrumChart && spectrumChart.resize(); } catch(_) {}

  // 等一帧，确保布局稳定
  await new Promise(r => requestAnimationFrame(() => setTimeout(r, 0)));

  const exportBg = getExportBg();
  const dpr = window.devicePixelRatio || 1;

  // 与主图一致：优先使用 payload.theme，避免与 DOM data-theme 时序不一致
  const themeName =
    (lastPayload && lastPayload.theme) ||
    (document.documentElement.getAttribute('data-theme') || 'light');
  const t = tokens(themeName);

  // 2) 获取主图/频谱图片（高分辨率）
  const mainUrl = chart.getDataURL({ pixelRatio: dpr, backgroundColor: exportBg, excludeComponents: [] });
  let specUrl = null;
  if (spectrumEnabled && spectrumChart) {
    try {
      specUrl = spectrumChart.getDataURL({ pixelRatio: dpr, backgroundColor: exportBg, excludeComponents: [] });
    } catch(_) {}
  }

  // 3) 加载图片资源
  const loadImg = (src) => new Promise((res, rej) => {
    if (!src) return res(null);
    const im = new Image();
    im.onload = () => res(im);
    im.onerror = rej;
    im.crossOrigin = 'anonymous';
    im.src = src;
  });

  const mainImg = await loadImg(mainUrl);
  const specImg = await loadImg(specUrl);

  if (!mainImg) return;

  // 4) 用 CSS 像素布局（将高分辨率图按 dpr 缩放到 CSS 大小）
  const mainCssW = mainImg.width / dpr;
  const mainCssH = mainImg.height / dpr;
  const specCssW = specImg ? (specImg.width / dpr) : 0;
  const specCssH = specImg ? (specImg.height / dpr) : 0;

  const chartsW = Math.max(mainCssW, specCssW);
  const gap = 24;            // 主图与频谱之间的间距（CSS px）
  const pad = 16;            // 画布内边距
  const bottomPad = 16;      // 额外底部留白
  const chartsH = mainCssH + (specImg ? (gap + specCssH) : 0);

  // 5) 构造导出用 Legend（复刻侧栏）
  const items = __buildLegendItemsForExport();
  const lm = __measureLegendForExport(items, t);
  const legendW = lm.colW;
  const legendH = lm.totalH;

  // 6) 计算输出画布的 CSS 尺寸，并用 dpr 放大像素尺寸
  const cssW = pad + chartsW + gap + legendW + pad;
  const cssH = pad + Math.max(chartsH, legendH) + bottomPad;
  const out = document.createElement('canvas');
  out.width = Math.max(1, Math.round(cssW * dpr));
  out.height = Math.max(1, Math.round(cssH * dpr));
  const ctx = out.getContext('2d');
  ctx.scale(dpr, dpr);

  // 背景
  ctx.fillStyle = exportBg;
  ctx.fillRect(0, 0, cssW, cssH);

  // 7) 绘制主图与频谱（按 CSS 尺寸投放）
  let x = pad, y = pad;
  ctx.drawImage(mainImg, x, y, mainCssW, mainCssH);
  y += mainCssH;

  if (specImg) {
    y += gap;
    ctx.drawImage(specImg, x, y, specCssW, specCssH);
  }

  // 8) 绘制 Legend（右列，复刻两行样式）
  let lx = pad + chartsW + gap;
  let ly = pad;

  ctx.textBaseline = 'alphabetic';

  items.forEach(it => {
    const color = it.selected ? (it.color || '#888') : 'rgba(128,128,128,.35)';
    // dot
    ctx.fillStyle = color;
    const r = lm.dotW / 2;
    ctx.beginPath();
    ctx.arc(lx + r + lm.padX, ly + r + 2, r, 0, Math.PI * 2);
    ctx.fill();

    // texts
    const textX = lx + lm.padX + lm.dotW + lm.gapDotText;
    const line1Y = ly + lm.line1H - 6; // 视觉基线略向上，靠近侧栏观感
    ctx.font = lm.line1Font;
    ctx.fillStyle = it.selected ? t.tooltipText : 'rgba(0,0,0,.55)';
    ctx.fillText(it.line1 || '', textX, line1Y);

    let itemH = lm.line1H;
    if (it.line2) {
      ctx.font = lm.line2Font;
      const line2Y = ly + lm.line1H + lm.line2H - 6;
      // muted 文本色：若 CSS 变量可用则读取，否则使用兜底
      const cssMuted = getComputedStyle(document.documentElement).getPropertyValue('--text-muted').trim();
      ctx.fillStyle = cssMuted || '#6b7280';
      ctx.fillText(it.line2, textX, line2Y);
      itemH = lm.line1H + lm.line2H;
    }

    ly += itemH + lm.itemGap;
  });

  // 9) 触发下载
  const a = document.createElement('a');
  a.download = 'charts-all.png';
  a.href = out.toDataURL('image/png');
  a.click();
}

function __computeFsSpecTargetPx() {
  const shell = document.getElementById('chart-settings') || (root && root.closest('.fc-chart-container'));
  if (!shell) return 0;
  const stack = shell.querySelector('.chart-stack');
  if (!stack) return 0;

  const st = getComputedStyle(stack);
  const gap = parseFloat(st.rowGap || st.gap || '0') || 0;
  const stackH = Math.max(0, Math.round(stack.getBoundingClientRect().height));

  // 可用高度要扣掉一个列间隙（两行之间只会产生一个 gap）
  const usable = Math.max(0, stackH - gap);

  const cs = getComputedStyle(document.documentElement);
  const mainFlex = parseFloat(cs.getPropertyValue('--fs-main-flex')) || 3;
  const specFlex = parseFloat(cs.getPropertyValue('--fs-spec-flex')) || 2;

  const total = Math.max(0.0001, mainFlex + specFlex);
  const specPx = Math.round(usable * (specFlex / total));

  // 合理下限，避免 0 导致动画不触发
  return Math.max(1, specPx);
}

function __refreshFsSpecMaxHeightIfExpanded() {
  if (!(isFs && spectrumEnabled && spectrumRoot)) return;
  const px = __computeFsSpecTargetPx();
  if (px > 0) {
    spectrumRoot.style.setProperty('max-height', px + 'px');
    spectrumRoot.style.setProperty('--fs-spec-h', px + 'px');
  }
}

/* 追加：创建/获取 Loading 覆盖层 */
function ensureSpectrumLoader() {
  if (!spectrumRoot) return null;
  let el = spectrumRoot.querySelector('.spectrum-loading');
  if (!el) {
    el = document.createElement('div');
    el.className = 'spectrum-loading';
    el.innerHTML = `
      <div class="spinner" aria-hidden="true"></div>
      <div class="text" id="spectrumLoadingText">加载中...</div>
    `;
    spectrumRoot.appendChild(el);
  }
  spectrumLoadingEl = el;
  return el;
}

function setSpectrumLoading(on, text) {
  const el = ensureSpectrumLoader();
  if (!el) return;
  const txt = el.querySelector('#spectrumLoadingText');
  if (txt) {
    if (typeof text === 'string' && text.length) txt.textContent = text;
    txt.style.fontSize = '15px';
  }
  el.classList.toggle('is-active', !!on);
}

function syncSpectrumStateAcrossModes({ animate = false } = {}) {
  ensureSpectrumHost();
  const shell =
    document.getElementById('chart-settings') ||
    (root && root.closest('.fc-chart-container')) ||
    document.documentElement;

  if (!spectrumRoot) return;

  if (spectrumEnabled) {
    try { shell.setAttribute('data-chart-mode', 'spectrum'); } catch (_) {}

    if (isFs) {
      // 全屏：仍用 max-height + --fs-spec-h 表达展开高度
      const targetPx = __computeFsSpecTargetPx();
      spectrumRoot.style.removeProperty('height');
      spectrumRoot.style.setProperty('flex', '0 0 auto', 'important');
      spectrumRoot.style.setProperty('--fs-spec-h', targetPx + 'px');

      if (animate) {
        spectrumRoot.style.transition = 'max-height var(--transition-speed, .25s) ease';
        const cur = Math.max(0, Math.round(spectrumRoot.getBoundingClientRect().height || 0));
        spectrumRoot.style.setProperty('max-height', cur + 'px');
        void spectrumRoot.offsetHeight;
        spectrumRoot.style.setProperty('max-height', targetPx + 'px');
      } else {
        spectrumRoot.style.transition = 'none';
        spectrumRoot.style.setProperty('max-height', targetPx + 'px');
        void spectrumRoot.offsetHeight;
        spectrumRoot.style.transition = '';
      }
    } else {
      // 非全屏：不再写入 px；依靠 CSS 的 dvh 规则和 height 过渡
      if (animate) {
        spectrumRoot.classList.add('anim-scale');
        requestAnimationFrame(() => {
          try { spectrumRoot.classList.remove('anim-scale'); } catch(_) {}
        });
      }
      spectrumRoot.style.removeProperty('max-height');
      spectrumRoot.style.removeProperty('flex');
    }
  } else {
    try { shell.removeAttribute('data-chart-mode'); } catch (_) {}

    if (isFs) {
      spectrumRoot.style.removeProperty('height');
      if (animate) {
        spectrumRoot.style.transition = 'max-height var(--transition-speed, .25s) ease';
      } else {
        spectrumRoot.style.transition = 'none';
      }
      spectrumRoot.style.setProperty('max-height', '0px');
      if (!animate) spectrumRoot.style.transition = '';
    } else {
      // 非全屏：移除属性即回到 height:0（CSS 过渡生效）
      spectrumRoot.style.removeProperty('max-height');
      spectrumRoot.style.removeProperty('flex');
      if (animate) {
        spectrumRoot.classList.add('anim-scale');
        requestAnimationFrame(() => {
          try { spectrumRoot.classList.remove('anim-scale'); } catch(_) {}
        });
      }
    }
  }

  // 关键补丁：模式切换后立刻刷新频谱配置（含 tooltip），避免沿用旧的 appendToBody/fixed
  try {
    if (spectrumEnabled && spectrumChart) {
      buildAndSetSpectrumOption();  // 内部已根据 isFs 设置 appendToBody 和位置语义，并 replaceMerge tooltip
    }
  } catch(_) {}

  try { chart && chart.resize(); } catch (_) {}
  try { spectrumEnabled && spectrumChart && spectrumChart.resize(); } catch (_) {}
}

  function getExportBg() {
    const bgBody = getComputedStyle(document.body).backgroundColor;
    return bgBody && bgBody !== 'rgba(0, 0, 0, 0)' ? bgBody : '#ffffff';
  }

function __buildLegendItemsForExport() {
  const sList = getSeriesArray();
  const selMap = getLegendSelectionMap();
  return sList.map(s => {
    const name = s.name || `${s.brand||''} ${s.model||''} - ${s.condition||''}`;
    const brand = s.brand || '';
    const model = s.model || '';
    const line1 = (brand || model) ? `${brand} ${model}`.trim() : name;
    const line2 = s.condition || '';
    const selected = selMap ? (selMap[name] !== false) : true;
    return {
      id: name,
      color: s.color || '#888',
      line1,
      line2,
      selected
    };
  });
}

function __measureLegendForExport(items, t) {
  const padX = 10;
  const dotW = 12;
  const gapDotText = 8;
  const minCol = 160, maxCol = 360;
  const line1Font = `600 13px ${t.fontFamily}`;
  const line2Font = `500 11px ${t.fontFamily}`;
  const line1H = 22;
  const line2H = 16;
  const itemGap = 8;

  let textMax = 0;
  let totalH = 0;

  items.forEach(it => {
    const w1 = measureText(it.line1 || '', 13, 600, t.fontFamily).width;
    let w2 = 0;
    if (it.line2) {
      w2 = measureText(it.line2, 11, 500, t.fontFamily).width;
    }
    textMax = Math.max(textMax, w1, w2);
    const itemH = it.line2 ? (line1H + line2H) : line1H;
    totalH += itemH + itemGap;
  });

  if (items.length > 0) totalH -= itemGap;

  const colW = Math.min(maxCol, Math.max(minCol, Math.ceil(padX + dotW + gapDotText + textMax + padX)));
  return { colW, totalH, line1H, line2H, itemGap, dotW, gapDotText, padX, line1Font, line2Font };
}

// 将频谱背景与主图一致（仅更新 backgroundColor，不动其它配置）
function syncSpectrumBgWithMain(bgOverride){
  try {
    if (!spectrumEnabled || !spectrumChart) return;
    const fallback = (lastOption && lastOption.backgroundColor) || (isFs ? getExportBg() : 'transparent');
    const targetBg = (bgOverride !== undefined && bgOverride !== null) ? bgOverride : fallback;
    spectrumChart.setOption({ backgroundColor: targetBg });
  } catch(_) {}
}

// 与 CSS dvh 规则一致的“非常规模式下频谱目标像素高”
function __computeNfSpecTargetPx() {
  const vv = (window.visualViewport && Math.round(window.visualViewport.height)) || 0;
  const vh = vv > 0 ? vv : (window.innerHeight || document.documentElement.clientHeight || 800);
  const narrow = layoutIsNarrow();
  const frac = narrow ? 0.30 : 0.45;    // 窄屏 30dvh，桌面 45dvh
  let px = Math.round(vh * frac);
  // 与 CSS 约束对齐：上限 600；桌面最小 300
  px = Math.min(600, px);
  if (!narrow) px = Math.max(300, px);
  return Math.max(1, px);
}

function updateRailParkedState(){
  const shell =
    document.getElementById('chart-settings') ||
    (root && root.closest('.fc-chart-container')) ||
    null;
  if (!shell) return;

  const integrated = isIntegratedLegendMode();
  const narrow = layoutIsNarrow();
  const empty  = !lastOption || lastOption.__empty;

  // 集成模式强制不使用 rail-parked（legend 永远占位）
  const shouldPark = !integrated && !!(showFitCurves && !empty && !narrow);
  shell.classList.toggle('rail-parked', shouldPark);

  try { updateLegendRailLayout(); } catch(_){}
}

function isIntegratedLegendMode() {
  const narrow = layoutIsNarrow();
  if (narrow) return true;
  if (isFs && isMobile()) return true;
  return false;
}

const TextMeasurer = (() => {
  const ctx = document.createElement('canvas').getContext('2d');
  const cache = new Map(); // key = font + '||' + text
  function measure(font, text) {
    const key = font + '||' + (text || '');
    if (cache.has(key)) return cache.get(key);
    ctx.font = font;
    const m = ctx.measureText(text || '');
    const sizeMatch = /(\d+(?:\.\d+)?)px/.exec(font);
    const fallbackSize = sizeMatch ? parseFloat(sizeMatch[1]) : 14;
    const ascent = (typeof m.actualBoundingBoxAscent === 'number') ? m.actualBoundingBoxAscent : fallbackSize * 0.8;
    const descent = (typeof m.actualBoundingBoxDescent === 'number') ? m.actualBoundingBoxDescent : fallbackSize * 0.2;
    const info = { width: m.width || 0, height: ascent + descent };
    cache.set(key, info);
    return info;
  }
  return { measure };
})();

/* =========================
 * (8) 布局刷新调度器 LayoutScheduler
 * ========================= */
const layoutScheduler = (() => {
  let rafId = null;
  const dirty = new Set();
  function mark(...keys) {
    keys.forEach(k => dirty.add(k));
    if (!rafId) rafId = requestAnimationFrame(flush);
  }
  function flush() {
    rafId = null;
    try {
      if (dirty.has('legend')) { updateLegendRailLayout(); renderLegendRailItems(); }
      if (dirty.has('fitUI'))  { placeFitUI(); }
      if (dirty.has('pointer')){ repaintPointer(); }
      if (dirty.has('spectrum')){ updateSpectrumLayout(); placeSpectrumSwitchOverlay(); }
      if (dirty.has('axisSwitch')) { updateAxisSwitchPosition({ force:true, animate:false }); }
      if (dirty.has('dock')) { placeSpectrumDock(); }
      if (dirty.has('railPark')) { updateRailParkedState(); }
    } catch(_) {}
    dirty.clear();
  }
  return { mark, flush };
})();

/* =========================
 * (11) 抑制管理器 SuppressionManager
 * ========================= */
const suppress = (() => {
  const map = new Map();
  function run(key, ms) { map.set(key, performance.now() + Math.max(0, ms|0)); }
  function active(key) { return performance.now() < (map.get(key) || 0); }
  function clear(key) { map.delete(key); }
  return { run, active, clear };
})();

/* =========================
 * (10) 通用拖拽控制器 DragController
 * ========================= */
function createDragController(element, opts) {
  const {
    axis = 'both',             // 'x' | 'y' | 'both'
    threshold = 4,             // 像素阈值，超过才判定为拖拽
    onStart,                   // (e) -> boolean|void，返回 false 可取消本次拖拽
    onMove,                    // ({dx, dy, e})
    onEnd                      // () -> void
  } = opts || {};
  if (!element) return;

  let activePointerId = null;
  let startX = 0, startY = 0;
  let moved = false;

  function onPointerDown(e) {
    if (e.button !== undefined && e.button !== 0) return;
    if (activePointerId != null) return;
    startX = e.clientX; startY = e.clientY;
    activePointerId = e.pointerId ?? null;
    moved = false;
    const ok = (typeof onStart === 'function') ? onStart(e) : true;
    if (ok === false) { activePointerId = null; return; }
    try { if (activePointerId != null) element.setPointerCapture(activePointerId); } catch(_) {}
    e.preventDefault?.();
  }

  function onPointerMove(e) {
    if (activePointerId != null && (e.pointerId ?? null) !== activePointerId) return;
    if (activePointerId == null) return;
    let dx = e.clientX - startX;
    let dy = e.clientY - startY;
    if (!moved && Math.hypot(dx, dy) >= threshold) moved = true;
    if (!moved) return;
    if (axis === 'x') dy = 0;
    if (axis === 'y') dx = 0;
    if (typeof onMove === 'function') onMove({ dx, dy, e });
  }

  function onPointerUpOrCancel() {
    if (activePointerId == null) return;
    try { element.releasePointerCapture(activePointerId); } catch(_) {}
    activePointerId = null;
    moved = false;
    if (typeof onEnd === 'function') onEnd();
  }

  element.addEventListener('pointerdown', onPointerDown);
  window.addEventListener('pointermove', onPointerMove, { passive:true });
  window.addEventListener('pointerup', onPointerUpOrCancel, { passive:true });
  window.addEventListener('pointercancel', onPointerUpOrCancel);
  window.addEventListener('blur', onPointerUpOrCancel);
}

/* =========================
 * (12) Base tooltip 生成器（主图与频谱共用）
 * ========================= */
function buildTooltipBase(t, { appendToBody }) {
  return {
    appendToBody: !!appendToBody,          // 关键：补回该字段
    backgroundColor: t.tooltipBg,
    borderColor: t.tooltipBorder,
    borderWidth: 1,
    textStyle: { color: t.tooltipText },
    extraCssText: `
      position: ${appendToBody ? 'fixed' : 'absolute'};
      backdrop-filter: blur(4px) saturate(120%);
      -webkit-backdrop-filter: blur(4px) saturate(120%);
      box-shadow: ${t.tooltipShadow};
      z-index: 1000000;
    `
  };
}

//用户侧 sweep 片段播放器
const SweepAudioPlayer = (() => {
  const LOOP_CROSSFADE_SEC = 0.05;   // 50ms 交叉淡化
  const LOOKAHEAD_MS = 25;
  const SCHEDULE_AHEAD_SEC = 0.40;
  const START_DELAY_SEC = 0.10;
  const FADE_OUT_MS = 30;
  const STOP_BUFFER_SEC = 0.01;
  const CLEANUP_MS = 20;
  const MASTER_GAIN = 5.0;  // Global playback gain constant

  let ctx = null;
  let masterGain = null;
  let compressor = null;
  let schedulerTimer = null;
  let nextStartTime = 0;
  let activeNodes = [];
  let playing = null; // { name, key, rpm }

  const cache = new Map(); // key: modelId_conditionId_rpmRounded -> AudioBuffer

  function getCtx() {
    if (!ctx) ctx = new (window.AudioContext || window.webkitAudioContext)();
    return ctx;
  }
  function ensureChain() {
    const c = getCtx();
    if (!masterGain) {
      masterGain = c.createGain();
      masterGain.gain.value = MASTER_GAIN;
    }
    if (!compressor) {
      compressor = c.createDynamicsCompressor();
      compressor.threshold.value = -12;
      compressor.knee.value = 6;
      compressor.ratio.value = 6;
      compressor.attack.value = 0.003;
      compressor.release.value = 0.12;
    }
    try { masterGain.disconnect(); } catch (_) {}
    try { compressor.disconnect(); } catch (_) {}
    masterGain.connect(compressor);
    compressor.connect(c.destination);
  }
  function makeCurves(n) {
    const fi = new Float32Array(n);
    const fo = new Float32Array(n);
    for (let i = 0; i < n; i++) {
      const t = (i / Math.max(1, n - 1)) * (Math.PI / 2);
      fi[i] = Math.sin(t);
      fo[i] = Math.cos(t);
    }
    return { fi, fo };
  }
  function clearScheduler() {
    if (schedulerTimer) { clearInterval(schedulerTimer); schedulerTimer = null; }
  }
  function stopSources() {
    if (!ctx) return;
    const now = ctx.currentTime;
    const fade = FADE_OUT_MS / 1000;
    const nodes = activeNodes.slice();
    activeNodes = [];
    nodes.forEach(n => {
      try {
        n.gain.gain.cancelScheduledValues(now);
        n.gain.gain.setValueAtTime(n.gain.gain.value, now);
        n.gain.gain.linearRampToValueAtTime(0, now + fade);
      } catch (_) {}
      try { n.source.stop(now + fade + STOP_BUFFER_SEC); } catch (_) {}
    });
  }
  function scheduleSegment(buffer, when) {
    if (!buffer) return;
    const c = getCtx();
    ensureChain();

    const src = c.createBufferSource();
    src.buffer = buffer;
    const g = c.createGain();
    g.gain.cancelScheduledValues(0);
    g.gain.setValueAtTime(0, when);

    src.connect(g);
    g.connect(masterGain);

    const dur = buffer.duration;
    const cf = Math.min(LOOP_CROSSFADE_SEC, dur * 0.49);
    const samples = Math.max(32, Math.floor(cf * c.sampleRate));
    const { fi, fo } = makeCurves(samples);

    const fadeInEnd = when + cf;
    const fadeOutStart = when + dur - cf;

    g.gain.setValueCurveAtTime(fi, when, cf);
    g.gain.setValueAtTime(1, fadeInEnd);
    g.gain.setValueAtTime(1, fadeOutStart);
    g.gain.setValueCurveAtTime(fo, fadeOutStart, cf);

    src.start(when);

    const ref = { source: src, gain: g };
    activeNodes.push(ref);
    src.onended = () => {
      const idx = activeNodes.indexOf(ref);
      if (idx >= 0) activeNodes.splice(idx, 1);
      try { src.disconnect(); } catch (_) {}
      try { g.disconnect(); } catch (_) {}
    };
  }
  function schedulerTick(buffer) {
    if (!playing || !buffer) return;
    const c = getCtx();
    const now = c.currentTime;
    const dur = buffer.duration;
    const cf = Math.min(LOOP_CROSSFADE_SEC, dur * 0.49);
    const step = Math.max(0.001, dur - cf);

    while (nextStartTime < now + SCHEDULE_AHEAD_SEC) {
      scheduleSegment(buffer, nextStartTime);
      nextStartTime += step;
    }
  }

  async function fetchBuffer(modelId, conditionId, rpm) {
    const rpmRounded = Math.round(rpm);
    const key = `${modelId}_${conditionId}_${rpmRounded}`;
    if (cache.has(key)) return { buffer: cache.get(key), key };
    const resp = await fetch('/api/sweep-audio', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model_id: modelId,
        condition_id: conditionId,
        target_rpm: rpm,
      })
    });
    if (!resp.ok) {
      let msg = `HTTP ${resp.status}`;
      let errCode = null;
      try { 
        const j = await resp.json(); 
        if (j.error_message) msg = j.error_message;
        if (j.error_code) errCode = j.error_code;
      } catch(_) {}
      const err = new Error(msg);
      err.errorCode = errCode;
      throw err;
    }
    const arr = await resp.arrayBuffer();
    const buf = await getCtx().decodeAudioData(arr);
    cache.set(key, buf);
    return { buffer: buf, key };
  }

  async function play({ name, modelId, conditionId, rpm }) {
    if (!Number.isFinite(rpm) || rpm <= 0) throw new Error('RPM 无效');
    if (playing && playing.name === name) {
      stop();
      return { state: 'stopped' };
    }
    stop();
    const { buffer, key } = await fetchBuffer(modelId, conditionId, rpm);
    const c = getCtx();
    ensureChain();
    await c.resume().catch(()=>{});
    playing = { name, key, rpm };
    nextStartTime = c.currentTime + START_DELAY_SEC;
    schedulerTick(buffer);
    clearScheduler();
    schedulerTimer = setInterval(() => schedulerTick(buffer), LOOKAHEAD_MS);
    return { state: 'playing', key };
  }

  function stop() {
    playing = null;
    clearScheduler();
    stopSources();
    if (ctx) {
      setTimeout(() => { if (!playing) { try { ctx.suspend(); } catch(_) {} } }, FADE_OUT_MS + CLEANUP_MS);
    }
  }

  function isPlaying(name) {
    return !!playing && playing.name === name;
  }

  return { play, stop, isPlaying };
})();

/**
 * Helper function to extract the domain range from a PCHIP (Piecewise Cubic Hermite Interpolating Polynomial) model.
 * The PCHIP model defines its valid interpolation range using x0 and x1 properties.
 * 
 * @param {Object} pchipModel - The PCHIP model object containing x0, x1, x, y, and m arrays
 * @returns {Object|null} Object with {min, max} representing the domain range, or null if model is invalid
 */
function getPchipDomainRange(pchipModel) {
  if (!pchipModel || pchipModel.x0 == null || pchipModel.x1 == null) {
    return null;
  }
  return {
    min: Math.min(pchipModel.x0, pchipModel.x1),
    max: Math.max(pchipModel.x0, pchipModel.x1)
  };
}

// 计算播放用 RPM：rpm 轴用当前查询；噪音轴用 cross（noise_to_rpm），否则回退系列的 rpm_max
function getTargetRpmForRow(row, mode) {
  // Extract series and spectrum model needed for RPM min/max lookup and fallback
  const series = row.__series || {};
  const specModel = row.__spectrumModel;
  let targetRpm;
  
  if (mode === 'rpm') {
    // RPM mode: check if current RPM is outside the rpm_to_airflow domain
    const rpmQuery = Number(getXQueryOrDefault('rpm'));
    const pchipModel = series?.pchip?.rpm_to_airflow;
    const domain = getPchipDomainRange(pchipModel);
    
    if (domain) {
      if (rpmQuery < domain.min) {
        // RPM query is left of domain: evaluate noise_to_rpm at domain.min to get RPM
        // Since we're in RPM mode with rpm_to_airflow, we use the X value (domain.min) as the RPM
        targetRpm = domain.min;
      } else if (rpmQuery > domain.max) {
        // RPM query is right of domain: use domain.max RPM
        targetRpm = domain.max;
      } else {
        // Within domain: use the query value directly
        targetRpm = rpmQuery;
      }
    } else {
      // No PCHIP model: use query value directly
      targetRpm = rpmQuery;
    }
  } else {
    // Noise mode: check if cross (RPM) is available, or if noise_db is outside domain
    if (Number.isFinite(row.cross) && row.cross > 0) {
      // Cross value is valid, use it
      targetRpm = Number(row.cross);
    } else {
      // Cross is NaN/invalid - check if noise_db is outside the noise_to_rpm domain
      const noiseQuery = Number(getXQueryOrDefault('noise_db'));
      const pchipModel = series?.pchip?.noise_to_rpm;
      const domain = getPchipDomainRange(pchipModel);
      
      if (domain) {
        if (noiseQuery < domain.min) {
          // Noise query is left of domain: evaluate noise_to_rpm at domain.min to get RPM
          targetRpm = evalPchipJS(pchipModel, domain.min);
        } else if (noiseQuery > domain.max) {
          // Noise query is right of domain: evaluate noise_to_rpm at domain.max to get RPM
          targetRpm = evalPchipJS(pchipModel, domain.max);
        } else {
          // Within domain but cross is NaN - this shouldn't normally happen,
          // but can occur if PCHIP model data is incomplete/invalid.
          // Fallback to max RPM as safe default.
          targetRpm = rpmMaxForSeries(series, specModel);
        }
      } else {
        // No PCHIP model: fallback to max RPM
        targetRpm = rpmMaxForSeries(series, specModel);
      }
    }
  }
  
  // Final clamp to series min/max RPM range
  const minRpm = rpmMinForSeries(series, specModel);
  const maxRpm = rpmMaxForSeries(series, specModel);
  
  if (Number.isFinite(minRpm) && minRpm > 0 && targetRpm < minRpm) {
    targetRpm = minRpm;
  }
  if (Number.isFinite(maxRpm) && maxRpm > 0 && targetRpm > maxRpm) {
    targetRpm = maxRpm;
  }
  
  return targetRpm;
}

/* =========================
 * (9) 统一拟合数据计算（Legend 与气泡共享）
 * ========================= */
function computeFitRows(mode, x, seriesList, selectionMap) {
  const items = [];
  ensureFitModels(seriesList, mode);
  seriesList.forEach(s => {
    const name = s.name || `${s.brand||''} ${s.model||''} - ${s.condition||''}`;
    const selected = (selectionMap ? (selectionMap[name] !== false) : true);
    const model = fitModelsCache[mode].get(name);
    const crossModel = (s && s.pchip)
      ? (mode === 'rpm' ? s.pchip?.rpm_to_noise_db : s.pchip?.noise_to_rpm)
      : null;

    let y = NaN, crossVal = NaN, outOfDomain = false;
    if (model && model.x0 != null && model.x1 != null) {
      const dom0 = Math.min(model.x0, model.x1);
      const dom1 = Math.max(model.x0, model.x1);
      outOfDomain = x < dom0 || x > dom1;
      y = evalPchipJS(model, Math.max(dom0, Math.min(x, dom1)));
    }
    if (crossModel && crossModel.x0 != null && crossModel.x1 != null) {
      const c0 = Math.min(crossModel.x0, crossModel.x1);
      const c1 = Math.max(crossModel.x0, crossModel.x1);
      crossVal = evalPchipJS(crossModel, Math.max(c0, Math.min(x, c1)));
    }

    // Get spectrum model to check audio availability
    const mid = s.modelId ?? s.model_id;
    const cid = s.conditionId ?? s.condition_id;
    const specKey = (Number.isInteger(mid) && Number.isInteger(cid)) ? `${mid}_${cid}` : null;
    const specModel = specKey ? spectrumModelCache.get(specKey) : null;
    const perfSupportsAudio = !!(s.supports_audio);
    const specSupportsAudio = !!(specModel && specModel.supports_audio);
    const supports_audio = perfSupportsAudio || specSupportsAudio;

    items.push({
      name,
      color: s.color,
      brand: s.brand || '',
      model: s.model || '',
      condition: s.condition || '',
      selected,
      y,
      cross: crossVal,
      outOfDomain,
      modelId: mid,
      conditionId: cid,
      supports_audio,  // Audio availability flag from supports_audio
      __series: s, // 用于 rpmMax 回退
      __spectrumModel: specModel  // Add spectrum model reference
    });
  });
  return items;
}

function measureLegendNameMax(sList, { t, integrated, isNarrow }) {
  const ctx = document.createElement('canvas').getContext('2d');
  // Non-fullscreen integrated mode uses normal weight (500), not bold
  const line1Font = (integrated && !isFs) ? `500 13px ${t.fontFamily}` : (integrated ? `800 13px ${t.fontFamily}` : `600 13px ${t.fontFamily}`);
  const line2Font = `500 11px ${t.fontFamily}`;
  let maxPx = 0;
  sList.forEach(s => {
    const brand = s.brand || s.brand_name_zh || s.brand_name || '';
    const model = s.model || s.model_name || '';
    const cond  = s.condition_name_zh || s.condition || '';
    const line1 = (brand || model) ? `${brand} ${model}` : (s.name || '');
    ctx.font = line1Font;
    const w1 = ctx.measureText(line1).width || 0;
    let w2 = 0;
    if (cond) {
      ctx.font = line2Font;
      // 集成 + 窄屏模式：条件是内联“ - cond”形式；需要加上分隔符宽度
      // Check viewport width for very narrow case
      const vw = window.innerWidth || document.documentElement.clientWidth || 0;
      const isVeryNarrow = vw < 500;
      // 集成 + 窄屏模式：条件是内联" - cond"形式（除非很窄）；需要加上分隔符宽度
      const condText = (integrated && isNarrow && !isVeryNarrow) ? ` - ${cond}` : cond;
      w2 = ctx.measureText(condText).width || 0;
    }
    maxPx = Math.max(maxPx, w1, w2);
  });
  return maxPx || 0;
}

/* 统一计算 Legend Rail 宽度 */
function computeLegendRailWidth({ sList, integrated, fitOn, t, baseW, hasPlayBtn = false }) {
  if (!Array.isArray(sList) || !sList.length) {
    return integrated ? 180 : 160;
  }
  const isNarrow = layoutIsNarrow();
  const maxNamePx = measureLegendNameMax(sList, { t, integrated, isNarrow });

  const playColW = hasPlayBtn ? 42 : 0; // 播放/停止按钮列所需宽度（含左右留白）

  // 公共常量
  const dotW = 12;
  const gapDotName = 8;
  const wrapperPadLeft = 4;
  const wrapperPadRight = 8;
  const rowPadX = 4 + 4;
  const safetyName = 10;
  const containerSafety = 6;

  if (integrated) {
    if (!fitOn) {
      const structural = dotW + gapDotName + wrapperPadLeft + wrapperPadRight + rowPadX + playColW;
      const MIN_NAME = 100;
      const nameNeed = Math.max(MIN_NAME, maxNamePx + safetyName);
      let applied = structural + nameNeed + containerSafety;
      const maxW = 460;
      if (applied > maxW) applied = maxW;
      if (applied > baseW - 24) applied = Math.max(structural + MIN_NAME, baseW - 24);
      return Math.round(applied);
    } else {
      const ctx = document.createElement('canvas').getContext('2d');
      ctx.font = `500 11px ${t.fontFamily}`;
      const chW = ctx.measureText('0').width || 7;
      const valPx   = 7 * chW;
      const pctPx   = 7 * chW;
      const crossPx = 8 * chW;
      const sepW    = 8;

      let colGap = 4;
      try {
        const testRow = __legendScrollEl?.querySelector('.legend-fit-grid.is-fit-on .legend-row');
        if (testRow) {
          const cs = getComputedStyle(testRow);
          colGap = parseFloat(cs.columnGap || cs.gap || '4') || 4;
        }
      } catch(_) {}

      const gapsTotal = colGap * 6;
      const fixedCols = valPx + pctPx + sepW + crossPx + playColW;

      const structural = dotW + gapDotName + wrapperPadLeft + wrapperPadRight + rowPadX + fixedCols + gapsTotal;
      const MIN_NAME = 110;
      const nameNeed = Math.max(MIN_NAME, maxNamePx + safetyName);
      let applied = structural + nameNeed + containerSafety;
      const maxW = 560;
      const minRail = structural + MIN_NAME + containerSafety;
      if (applied < minRail) applied = minRail;
      if (applied > maxW) applied = maxW;
      if (applied > baseW - 24) applied = Math.min(maxW, Math.max(minRail, baseW - 24));
      return Math.round(applied);
    }
  } else {
    const iconW = 12;
    const gap = 8;
    const pad = 12;
    const safetyDesktop = 10;
    const need = iconW + gap + maxNamePx + safetyName + pad + safetyDesktop + playColW;
    const minW = 160, maxW = 320;
    let applied = Math.max(minW, Math.min(maxW, Math.ceil(need)));
    if (applied > baseW - 24) applied = Math.max(minW, baseW - 24);
    return applied;
  }
}

let __dataEpoch = 0;

let canonicalSeries = [];
let __canonicalEpoch = -1;

function getSeriesArray() {
  return Array.isArray(canonicalSeries) ? canonicalSeries : [];
}

/* ==== DerivedSnapshot 缓存 ==== */
let snapshotCache = {
  epoch: -1,
  isNarrow: null,
  rpm: null,        // { series:[], xMin,xMax,yMax }
  noise_db: null
};
function buildDerivedSnapshots(seriesCanonical){
  // 仅在 epoch 或 narrow 状态变化时重建
  const nowNarrow = layoutIsNarrow();
  if (__dataEpoch === snapshotCache.epoch && nowNarrow === snapshotCache.isNarrow) return;
  snapshotCache.epoch = __dataEpoch;
  snapshotCache.isNarrow = nowNarrow;
  snapshotCache.rpm = buildSeries(seriesCanonical, 'rpm');
  snapshotCache.noise_db = buildSeries(seriesCanonical, 'noise_db');
}
function getSnapshotForMode(mode){
  const m = (mode === 'noise' ? 'noise_db' : mode);
  return snapshotCache[m] || null;
}

/* === SpectrumController 单例（集中管理频谱 UI/构建/状态） === */
const SpectrumController = (() => {
  // 外部可查询：当前 pending 的 key（model_id_condition_id）
  let _pendingKeys = new Set();
  let _missingKeys = new Set();

  function setEnabled(enabled, opts = {}) {
    // 与外置 Dock 交互保持一致：切换频谱展开/收起 + 同步按钮文案
    const want = !!enabled;
    if (want === spectrumEnabled) {
      // 已是目标状态：做一次状态同步和必要的重建
      try { syncSpectrumStateAcrossModes({ animate: !!opts.animate }); } catch(_) {}
      try { if (want) requestAndRenderSpectrum(true); } catch(_) {}
      try { syncSpectrumDockUi(); placeSpectrumDock(); } catch(_) {}
      return spectrumEnabled;
    }

    spectrumEnabled = want;
    try { syncSpectrumDockUi(); placeSpectrumDock(); } catch(_) {}

    // 统一入口：仍复用现有的 toggleSpectrumUI（负责动画/高度/加载/渲染）
    try { toggleSpectrumUI(spectrumEnabled); } catch(_) {}
    return spectrumEnabled;
  }

  function onFullscreenChange(isFsNow) {
    // 全屏状态切换后，频谱容器的高度/tooltip 附着策略需要立即刷新
    try { syncSpectrumStateAcrossModes({ animate: false }); } catch(_) {}
    try { if (spectrumEnabled) buildAndSetSpectrumOption(true); } catch(_) {}
    try { syncSpectrumDockUi(); placeSpectrumDock(); } catch(_) {}
  }

  function onXQueryChange(_x) {
    // 拟合指针或 X 输入发生变化 → 若频谱开启，触发一次重建（rAF 合并）
    try {
      if (spectrumEnabled && spectrumChart) {
        // 复用现有的调度函数
        scheduleSpectrumRebuild();
      }
    } catch(_) {}
  }

  function getPendingKeys() {
    return Array.from(_pendingKeys);
  }

  function isEnabled() {
    return !!spectrumEnabled;
  }

  // 内部钩子：由请求阶段回写 pending/missing，便于上层查看
  function __setPendingKeys(keys) {
    _pendingKeys = new Set(Array.isArray(keys) ? keys : (keys ? Array.from(keys) : []));
  }
  function __setMissingKeys(keys) {
    _missingKeys = new Set(Array.isArray(keys) ? keys : (keys ? Array.from(keys) : []));
  }

  return {
    setEnabled,
    onFullscreenChange,
    onXQueryChange,
    getPendingKeys,
    isEnabled,
    __setPendingKeys,
    __setMissingKeys
  };
})();

function setFitBubbleVisible(visible, { immediate = false } = {}) {
  const bubble = getById('fitBubble');
  if (!bubble) return;

  if (visible) {
    // 立刻显示 + 淡入（opacity: 0 -> 1）
    bubble.style.visibility = 'visible';
    bubble.classList.add('is-visible');
    return;
  }

  // 隐藏
  if (immediate) {
    // 某些场景（例如全屏切换）希望立即消失，不走动画
    bubble.classList.remove('is-visible');
    bubble.style.visibility = 'hidden';
    return;
  }

  // 正常淡出：只改类，等 opacity 过渡结束后再 hidden
  bubble.classList.remove('is-visible');

  const onEnd = (e) => {
    if (e && e.propertyName && e.propertyName !== 'opacity') return;
    try { bubble.removeEventListener('transitionend', onEnd); } catch (_) {}
    bubble.style.visibility = 'hidden';
  };

  try {
    bubble.addEventListener('transitionend', onEnd);
  } catch (_) {
    // 某些旧环境可能不触发 transitionend，兜底直接隐藏
    bubble.style.visibility = 'hidden';
  }
}

function refreshFitPanel() {
  const integrated = isIntegratedLegendMode();
  const sList = getSeriesArray();
  if (!showFitCurves || !lastPayload || !chart || (lastOption && lastOption.__empty)) {
    // 关闭拟合：集成模式撤掉 is-fit-on 标记与数值；浮动模式做淡出
    if (integrated) {
      const gridWrap = __legendScrollEl?.querySelector('.legend-fit-grid');
      if (gridWrap) {
        gridWrap.classList.remove('is-fit-on');
        gridWrap.querySelectorAll('.col-val,.col-pct,.col-cross').forEach(el => { el.textContent = '—'; });
      }
    } else {
      // 统一通过 setFitBubbleVisible 做淡出
      setFitBubbleVisible(false);
    }
    return;
  }

  const mode = currentXModeFromPayload(lastPayload);
  const x = xQueryByMode[mode];
  if (x == null) return;

  const opt = chart.getOption() || {};
  const selMap = (opt.legend && opt.legend[0] && opt.legend[0].selected) || {};
  const items = computeFitRows(mode, x, sList, selMap);

  // onToggleSeries（保持原图表与频谱联动）
  const onToggleSeries = (name, shouldShow) => {
    const actionType = shouldShow ? 'legendSelect' : 'legendUnSelect';
    try { chart.dispatchAction({ type: actionType, name }); } catch(_){}
    if (spectrumEnabled && spectrumChart) {
      try { spectrumChart.dispatchAction({ type: actionType, name }); } catch(_){}
    }
    try { syncLegendRailFromChart(); } catch(_){}
  };

  const unit = (mode === 'rpm') ? 'RPM' : 'dB';
  renderFitPanel({
    mode: integrated ? 'integrated' : 'floating',
    rows: items,
    xValue: x,
    unit,
    onToggleSeries
  });
}

// 单一 FitPanel 渲染器
function renderFitPanel({ mode, rows, xValue, unit, onToggleSeries }) {
  const rowsByName = {};
  rows.forEach(r => { rowsByName[r.name] = r; });

  function bindPlayHandlers(rootEl) {
    if (!rootEl) return;
    rootEl.querySelectorAll('.play-audio-btn[data-name]').forEach(btn => {
      const name = btn.getAttribute('data-name') || '';
      btn.onclick = async (e) => {
        e.stopPropagation();
        const row = rowsByName[name];
        if (!row) return;
        const rpm = getTargetRpmForRow(row, mode === 'integrated' ? currentXModeFromPayload(lastPayload) : currentXModeFromPayload(lastPayload));
        if (!Number.isFinite(rpm) || rpm <= 0) return;
        const isPlayClick = !SweepAudioPlayer.isPlaying(name);
        btn.disabled = true;
        try {
          await SweepAudioPlayer.play({
            name,
            modelId: row.modelId,
            conditionId: row.conditionId,
            rpm
          });
          if (isPlayClick && window.Analytics && window.Analytics.logPlayAudio) {
            try {
              const xMode = currentXModeFromPayload(lastPayload);
              const pointerX = xQueryByMode[xMode];
              const dbVal = (xMode === 'noise_db')
                ? pointerX
                : (Number.isFinite(row.cross) ? row.cross : null);
              window.Analytics.logPlayAudio(
                row.modelId, row.conditionId,
                xMode === 'noise_db' ? 'db' : 'rpm',
                pointerX, rpm, dbVal
              );
            } catch (_) {}
          }
          // 切换按钮文案
          rootEl.querySelectorAll('.play-audio-btn').forEach(b => {
            const n = b.getAttribute('data-name');
            b.textContent = SweepAudioPlayer.isPlaying(n) ? '■' : '▶';
          });
        } catch (err) {
          console.warn('播放失败', err);
          // Check if this is a "no valid audio segment" error
          // First check error code for reliability, then fall back to message matching
          const errCode = err && err.errorCode ? err.errorCode : null;
          const errMsg = err && err.message ? err.message : String(err);
          const isNoValidSegmentError = (errCode === 'AUDIO_GENERATION_FAILED' && 
                                          (errMsg.includes('无法匹配到有效的音频数据') || 
                                           errMsg.includes('Cannot match valid audio data')));
          
          if (isNoValidSegmentError) {
            // Show user-facing alert for no valid segment error
            if (typeof window.showError === 'function') {
              window.showError('暂时无法匹配到有效的音频片段，请尝试其他转速或噪音值。 Unable to match valid audio segment. Please try different RPM or noise values.');
            }
          } else {
            // Show generic error for other failures
            if (typeof window.showError === 'function') {
              window.showError('音频播放失败 / Audio playback failed');
            }
          }
          SweepAudioPlayer.stop();
          rootEl.querySelectorAll('.play-audio-btn').forEach(b => b.textContent = '▶');
        } finally {
          btn.disabled = false;
        }
      };
    });
    // 初始化文案
    rootEl.querySelectorAll('.play-audio-btn').forEach(b => {
      const n = b.getAttribute('data-name');
      b.textContent = SweepAudioPlayer.isPlaying(n) ? '■' : '▶';
    });
  }

  if (mode === 'integrated') {
    if (!__legendScrollEl) return;

    __legendScrollEl.style.setProperty('--fit-col-val-ch', '7');
    __legendScrollEl.style.setProperty('--fit-col-pct-ch', '7');
    __legendScrollEl.style.setProperty('--fit-col-cross-ch', '9');
    __legendScrollEl.style.setProperty('--fit-col-play-w', '32px');

    // 计算排序与基准
    const visible = rows.filter(r => r.selected);
    const withVal = visible.filter(r => Number.isFinite(r.y)).sort((a,b)=> b.y - a.y);
    const base = (withVal.length && withVal[0].y > 0) ? withVal[0].y : 0;
    const pctVal = (v)=> (Number.isFinite(v) && base > 0) ? Math.round((v / base) * 100) : null;

    const grid = __legendScrollEl.querySelector('.legend-fit-grid');
    if (!grid) return;

    const ordered = [
      ...rows.filter(r => Number.isFinite(r.y)).sort((a,b)=> b.y - a.y),
      ...rows.filter(r => !Number.isFinite(r.y))
    ];

    grid.innerHTML = ordered.map(it => {
      const baseName = (it.brand || it.model) ? `${it.brand} ${it.model}`.trim() : it.name;
      const hasY = Number.isFinite(it.y);
      const vText = hasY ? `${Math.round(it.y)} CFM` : '—';
      const pct = hasY ? pctVal(it.y) : null;
      const pctText = (pct==null) ? '—' : `(${pct}%)`;
      let crossText = '—';
      if (Number.isFinite(it.cross)) {
        crossText = (unit === 'dB') 
          ? `<span style="font-variant-numeric:tabular-nums;">${Math.round(it.cross)}</span> RPM` 
          : `<span style="font-variant-numeric:tabular-nums;">${Number(it.cross).toFixed(1)}</span> dB`;
      }
      
      // Check viewport width for very narrow case
      const vw = window.innerWidth || document.documentElement.clientWidth || 0;
      const isVeryNarrow = vw < 500;
      
      // In fullscreen integrated mode with fit, keep has-l2 for separate line condition
      // In non-fullscreen integrated mode: use inline with " - " prefix unless very narrow
      let nameClass = 'name';
      let conditionHtml = '';
      
      if (isFs && showFitCurves && !!it.condition) {
        // Fullscreen with fit: condition on separate line (original behavior)
        nameClass = 'name has-l2';
        conditionHtml = it.condition ? `<span class="l2">${it.condition}</span>` : ``;
      } else if (!isFs && isVeryNarrow && !!it.condition) {
        // Non-fullscreen very narrow: condition on separate line with lighter color, no prefix
        nameClass = 'name has-l2';
        conditionHtml = `<span class="l2" style="color: var(--text-secondary);">${it.condition}</span>`;
      } else if (!isFs && !!it.condition) {
        // Non-fullscreen not very narrow: inline with " - " prefix and lighter color
        nameClass = 'name';
        conditionHtml = `<span class="cond-inline" style="color: var(--text-secondary);"> - ${it.condition}</span>`;
      }

      // Check if play-audio buttons should be shown
      const showPlayBtn = isPlayAudioAllowed();
      
      return `
        <div class="legend-row hoverable-row ${it.selected ? '' : 'is-off'}${it.outOfDomain ? ' is-out-of-domain' : ''}" data-name="${it.name}">
          <span class="dot" style="background:${it.color}"></span>
          <span class="${nameClass}" title="${baseName}${it.condition ? ' / ' + it.condition : ''}">
            <span class="l1">${baseName}</span>${conditionHtml}
          </span>
          <span class="col-val">${vText}</span>
          <span class="col-pct">${pctText}</span>
          <span class="col-sep">│</span>
          <span class="col-cross">${crossText}</span>
          ${showPlayBtn ? `<button class="play-audio-btn col-play ${it.supports_audio ? '' : 'disabled'}" data-name="${it.name}" aria-label="播放/停止" ${it.supports_audio ? 'data-tooltip="播放当前转速录音"' : ''} ${it.supports_audio ? '' : 'disabled'}></button>` : ''}
        </div>
      `;
    }).join('');

    // 行交互：点击切换、悬浮高亮
    grid.querySelectorAll('.legend-row[data-name]').forEach(node => {
      const name = node.getAttribute('data-name') || '';
      node.addEventListener('click', () => {
        if (!name || !chart) return;
        const sel = getLegendSelectionMap();
        const visibleNow = sel ? (sel[name] !== false) : true;
        node.classList.toggle('is-off', visibleNow);
        onToggleSeries(name, !visibleNow);
        refreshFitPanel(); // 同步数值
      });
      node.addEventListener('mouseenter', () => {
        if (!name) return;
        try { chart.dispatchAction({ type: 'highlight', seriesName: name }); } catch(_){}
        if (spectrumEnabled && spectrumChart) { try { spectrumChart.dispatchAction({ type: 'highlight', seriesName: name }); } catch(_){}} 
      }, { passive:true });
      node.addEventListener('mouseleave', () => {
        if (!name) return;
        try { chart.dispatchAction({ type: 'downplay', seriesName: name }); } catch(_){}
        if (spectrumEnabled && spectrumChart) { try { spectrumChart.dispatchAction({ type: 'downplay', seriesName: name }); } catch(_){}} 
      }, { passive:true });
    });

    bindPlayHandlers(grid);
    
    // 标题与 X 输入
    const unitSpan = getById('fitXUnitLegend');
    if (unitSpan) unitSpan.textContent = unit;
    const inp = getById('fitXInputLegend');
    if (inp) {
      const modeKey = currentXModeFromPayload(lastPayload);
      const val = Number(xQueryByMode[modeKey] ?? xValue);
      const rounded = (modeKey === 'noise_db') ? Number(val.toFixed(1)) : Math.round(val);
      inp.value = String(rounded);
      const [vx0, vx1] = getVisibleXRange();
      inp.setAttribute('min', String(Math.floor(vx0)));
      inp.setAttribute('max', String(Math.ceil(vx1)));
      inp.setAttribute('step', (modeKey === 'noise_db') ? '0.1' : '1');

      // 统一与原行为一致
      // oninput: Allow free typing without immediate clamping
      inp.oninput = () => {
        const raw = Number(inp.value);
        if (!Number.isFinite(raw)) return;
        // Store raw value without clamping - allow user to type freely
        xQueryByMode[modeKey] = raw;
        // No UI updates during typing - happens on commit
      };
      // onchange: Clamp and update on commit (blur/Enter)
      inp.onchange = () => {
        const raw = Number(inp.value);
        if (Number.isFinite(raw)) {
          xQueryByMode[modeKey] = clampXDomain(raw);
          clampXQueryIntoVisibleRange();
        }
        // Update input to show clamped value
        const val = Number(xQueryByMode[modeKey]);
        if (Number.isFinite(val)) {
          const rounded = (modeKey === 'noise_db') ? Number(val.toFixed(1)) : Math.round(val);
          inp.value = String(rounded);
        }
        repaintPointer();
        refreshFitPanel();
        SpectrumController.onXQueryChange(xQueryByMode[modeKey]);
      };
    }

    const gridWrap = __legendScrollEl.querySelector('.legend-fit-grid');
    if (gridWrap) gridWrap.classList.add('is-fit-on');
    return;
  }

  // 浮动气泡
  const bubble = getById('fitBubble');
  if (!bubble) return;
  const rowsEl = bubble.querySelector('#fitBubbleRows');
  bubble.style.setProperty('--fit-col-val-ch', '7');
  bubble.style.setProperty('--fit-col-pct-ch', '7');
  bubble.style.setProperty('--fit-col-cross-ch', '9');
  bubble.style.setProperty('--fit-col-play-w', '32px');

  const rowsSafe = Array.isArray(rows) ? rows : [];

  const visible = rowsSafe.filter(r => r.selected);
  const withVal = visible.filter(r => Number.isFinite(r.y)).sort((a, b) => b.y - a.y);
  const base = (withVal.length && withVal[0].y > 0) ? withVal[0].y : 0;
  const pctVal = (v) => (Number.isFinite(v) && base > 0) ? Math.round((v / base) * 100) : null;

  const ordered = [
    ...rowsSafe.filter(r => Number.isFinite(r.y)).sort((a, b) => b.y - a.y),
    ...rowsSafe.filter(r => !Number.isFinite(r.y))
  ];

  rowsEl.innerHTML = ordered.map(it => {
    const hasY = Number.isFinite(it.y);
    const valText = hasY ? `${Math.round(it.y)} CFM` : '—';
    const pct = hasY ? pctVal(it.y) : null;
    const pctText = (pct==null) ? '—' : `(${pct}%)`;
    let crossText = '—';
    if (Number.isFinite(it.cross)) {
      crossText = (unit === 'dB')
        ? `<span style="font-variant-numeric:tabular-nums;">${Math.round(it.cross)}</span> RPM`
        : `<span style="font-variant-numeric:tabular-nums;">${Number(it.cross).toFixed(1)}</span> dB`;
    }
    const baseName = (it.brand || it.model) ? `${it.brand} ${it.model}`.trim() : it.name;
    const condInline = it.condition
      ? `<span class="sep"> - </span><span class="cond-inline" style="color:var(--text-secondary);white-space:nowrap;">${it.condition}</span>`
      : '';

    // Check if play-audio buttons should be shown
    const showPlayBtn = isPlayAudioAllowed();

    return `
      <div class="row ${it.selected ? '' : 'is-off'}${it.outOfDomain ? ' is-out-of-domain' : ''}" data-name="${it.name}">
        <span class="dot" style="background:${it.color}"></span>
        <span></span>
        <span class="col-name">${baseName}${condInline}</span>
        <span class="col-val">${valText}</span>
        <span class="col-pct">${pctText}</span>
        <span class="col-sep">│</span>
        <span class="col-cross">${crossText}</span>
        ${showPlayBtn ? `<button class="play-audio-btn col-play ${it.supports_audio ? '' : 'disabled'}" data-name="${it.name}" aria-label="播放/停止" ${it.supports_audio ? 'data-tooltip="播放当前转速录音"' : ''} ${it.supports_audio ? '' : 'disabled'}></button>` : ''}
      </div>
    `;
  }).join('');

  rowsEl.querySelectorAll('.row[data-name]').forEach(node => {
    const name = node.getAttribute('data-name') || '';
    node.addEventListener('pointerdown', (e) => { e.stopPropagation(); }, { passive:true });
    node.addEventListener('mouseenter', () => {
      if (!name) return;
      try { chart.dispatchAction({ type: 'highlight', seriesName: name }); } catch(_){}
      if (spectrumEnabled && spectrumChart) {
        try { spectrumChart.dispatchAction({ type: 'highlight', seriesName: name }); } catch(_){}
      }
    }, { passive:true });
    node.addEventListener('mouseleave', () => {
      if (!name) return;
      try { chart.dispatchAction({ type: 'downplay', seriesName: name }); } catch(_){}
      if (spectrumEnabled && spectrumChart) {
        try { spectrumChart.dispatchAction({ type: 'downplay', seriesName: name }); } catch(_){}
      }
    }, { passive:true });
    node.addEventListener('click', (e) => {
      e.preventDefault();
      e.stopPropagation();
      if (!name) return;
      const sel = getLegendSelectionMap();
      const visibleNow = sel ? (sel[name] !== false) : true;
      onToggleSeries(name, !visibleNow);
      refreshFitPanel();
    });
  });

  const unitSpan = bubble.querySelector('#fitXUnit');
  if (unitSpan) unitSpan.textContent = unit;

  const inp = bubble.querySelector('#fitXInput');
  if (inp) {
    const modeKey = currentXModeFromPayload(lastPayload);
    const val = Number(xQueryByMode[modeKey] ?? xValue);
    const rounded = (modeKey === 'noise_db') ? Number(val.toFixed(1)) : Math.round(val);
    inp.value = String(rounded);

    const [vx0, vx1] = getVisibleXRange();
    inp.setAttribute('min', String(Math.floor(vx0)));
    inp.setAttribute('max', String(Math.ceil(vx1)));
    inp.setAttribute('step', (modeKey === 'noise_db') ? '0.1' : '1');
  }

  // 保持可见性由 toggleFitUI 控制
  bindPlayHandlers(rowsEl);
}

function stopAllSweepAudio() { try { SweepAudioPlayer.stop(); } catch(_) {} }

function placeSpectrumSwitchOverlay() {
  const wrapper = spectrumInner ? spectrumInner.querySelector('.spectrum-res-switch') : null;
  if (!wrapper) return;

  // 未启用频谱或没有图表实例 → 隐藏
  if (!spectrumEnabled || !spectrumChart) {
    wrapper.style.visibility = 'hidden';
    return;
  }

  // 频谱当前配置（用于判断是否有可渲染数据）
  let specOpt = null;
  try { specOpt = spectrumChart.getOption(); } catch(_) {}

  const hasSeries =
    !!(specOpt &&
       Array.isArray(specOpt.series) &&
       specOpt.series.length > 0);

  // 如果没有任何可渲染数据（包括 pending / 空态）直接隐藏开关
  if (!hasSeries) {
    wrapper.style.visibility = 'hidden';
    return;
  }

  const chartW = spectrumChart.getWidth();
  if (!chartW || chartW < 50) {
    requestAnimationFrame(placeSpectrumSwitchOverlay);
    return;
  }

  const titleText =
    (specOpt && specOpt.title && specOpt.title[0] && specOpt.title[0].text) ||
    (lastSpectrumOption && lastSpectrumOption.title && lastSpectrumOption.title.text) ||
    buildSpectrumTitle() || '';

  // 锚点必须存在，否则隐藏（避免在“当前无可渲染频谱”等标题下仍显示开关）
  const anchorStr = '1/';
  const anchorIndex = titleText.indexOf(anchorStr);
  if (anchorIndex < 0) {
    wrapper.style.visibility = 'hidden';
    return;
  }

  const t = tokens(lastPayload?.theme);
  const fontSize = 16;
  const fontWeight = 700;

  // 整体标题宽度
  const fullWidth = measureText(titleText, fontSize, fontWeight, t.fontFamily).width;
  const titleStartX = (chartW / 2) - (fullWidth / 2);

  // 计算到 "1/" 前缀宽度及其自身宽度
  const prefixWidth = measureText(titleText.slice(0, anchorIndex), fontSize, fontWeight, t.fontFamily).width;
  const anchorWidth = measureText(anchorStr, fontSize, fontWeight, t.fontFamily).width;

  // 将滑块中心锚在 "1/" 的中心往左轻微偏移
  const SHIFT_LEFT = 4;
  const anchorCenterX = titleStartX + prefixWidth + anchorWidth / 2;
  const targetCenterX = anchorCenterX - SHIFT_LEFT;

  // 垂直位置保持与之前一致
  const targetCenterY = 6 + 10;

  wrapper.style.left = Math.round(targetCenterX) + 'px';
  wrapper.style.top = Math.round(targetCenterY) + 'px';
  wrapper.style.transform = 'translate(-50%, -50%)';
  wrapper.style.visibility = 'visible';
}

function placeSpectrumSwitchOverlay() {
  const wrapper = spectrumInner ? spectrumInner.querySelector('.spectrum-res-switch') : null;
  if (!wrapper) return;

  // 未启用频谱或没有图表实例 → 隐藏
  if (!spectrumEnabled || !spectrumChart) {
    wrapper.style.visibility = 'hidden';
    return;
  }

  // 频谱当前配置（用于判断是否有可渲染数据）
  let specOpt = null;
  try { specOpt = spectrumChart.getOption(); } catch(_) {}

  const hasSeries =
    !!(specOpt &&
       Array.isArray(specOpt.series) &&
       specOpt.series.length > 0);

  // 如果没有任何可渲染数据（包括 pending / 空态）直接隐藏开关
  if (!hasSeries) {
    wrapper.style.visibility = 'hidden';
    return;
  }

  const chartW = spectrumChart.getWidth();
  if (!chartW || chartW < 50) {
    requestAnimationFrame(placeSpectrumSwitchOverlay);
    return;
  }

  const titleText =
    (specOpt && specOpt.title && specOpt.title[0] && specOpt.title[0].text) ||
    (lastSpectrumOption && lastSpectrumOption.title && lastSpectrumOption.title.text) ||
    (typeof buildSpectrumTitle === 'function' ? buildSpectrumTitle() : '');

  // 锚点必须存在，否则隐藏（避免在“当前无可渲染频谱”等标题下仍显示开关）
  const anchorStr = '1/';
  const anchorIndex = titleText.indexOf(anchorStr);
  if (anchorIndex < 0) {
    wrapper.style.visibility = 'hidden';
    return;
  }

  const t = tokens(lastPayload?.theme);
  const fontSize = 16;
  const fontWeight = 700;

  // 整体标题宽度
  const fullWidth = measureText(titleText, fontSize, fontWeight, t.fontFamily).width;
  const titleStartX = (chartW / 2) - (fullWidth / 2);

  // 计算到 "1/" 前缀宽度及其自身宽度
  const prefixWidth = measureText(titleText.slice(0, anchorIndex), fontSize, fontWeight, t.fontFamily).width;
  const anchorWidth = measureText(anchorStr, fontSize, fontWeight, t.fontFamily).width;

  // 将滑块中心锚在 "1/" 的中心往左轻微偏移
  const SHIFT_LEFT = 4;
  const anchorCenterX = titleStartX + prefixWidth + anchorWidth / 2;
  const targetCenterX = anchorCenterX - SHIFT_LEFT;

  // 垂直位置保持与之前一致
  const targetCenterY = 6 + 10;

  wrapper.style.left = Math.round(targetCenterX) + 'px';
  wrapper.style.top = Math.round(targetCenterY) + 'px';
  wrapper.style.transform = 'translate(-50%, -50%)';
  wrapper.style.visibility = 'visible';
}

// 可选：暴露到全局，便于调试或其他模块监听
try { window.SpectrumController = SpectrumController; } catch (_) {}


  // 挂到全局
  window.ChartRenderer = API;

})();