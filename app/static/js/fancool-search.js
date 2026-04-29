(function(window, document){
  'use strict';

  const has = {
    toast: typeof window.showLoading === 'function'
        && typeof window.hideLoading === 'function'
        && typeof window.showError === 'function'
        && typeof window.showSuccess === 'function'
        && typeof window.showInfo === 'function',
    normalize: typeof window.normalizeApiResponse === 'function',
    cache: !!(window.__APP && window.__APP.cache),
    escapeHtml: typeof window.escapeHtml === 'function',
    formatScenario: typeof window.formatScenario === 'function'
  };

  const $$ = (sel, scope) => (scope||document).querySelector(sel);
  const $$$ = (sel, scope) => Array.from((scope||document).querySelectorAll(sel));

  function EH(s){
    if (has.escapeHtml) return window.escapeHtml(s);
    return String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  }
  function FS(rt, rl){
    if (has.formatScenario) return window.formatScenario(rt, rl);
    const rtype = EH(rt || '');
    const raw = rl ?? '';
    const isEmpty = (String(raw).trim() === '' || String(raw).trim() === '无');
    return isEmpty ? rtype : `${rtype}(${EH(raw)})`;
  }

  async function fetchJSON(url, opts){
    const r = await fetch(url, opts);
    const j = await r.json();
    if (has.normalize){
      const n = window.normalizeApiResponse(j);
      return n.ok ? { ok: true, data: n.data } : { ok:false, error: n.error_message || '请求失败' };
    }
    if (j && j.success === true) return { ok:true, data:j.data };
    return { ok:false, error: (j && (j.error_message || j.message)) || '请求失败' };
  }

  const Cache = {
    get(ns, payload){ return has.cache ? window.__APP.cache.get(ns, payload) : null; },
    set(ns, payload, value, ttl){ return has.cache ? window.__APP.cache.set(ns, payload, value, ttl) : value; }
  };

function createPortalDropdown(btn, panel, {
  root = btn,         // 用于判定“外点关闭”的根容器（通常是 wrap）
  margin = 6,         // 面板与按钮的间距
  preferredMaxH = 320,// 期望的最大高度
  minMaxH = 120,      // 最小最大高度下限
  getWidth            // 自定义宽度函数 (btnRect)=>number；默认=按钮宽度
} = {}) {
  if (!panel.classList.contains('fc-portal')) panel.classList.add('fc-portal');
  if (panel.parentNode !== document.body) document.body.appendChild(panel);

  let bound = false;
  function place() {
    const r = btn.getBoundingClientRect();
    panel.style.visibility = 'hidden';
    panel.classList.remove('hidden');

    const spaceBelow = window.innerHeight - r.bottom - margin;
    const spaceAbove = r.top - margin;
    const openUp = spaceBelow < 160 && spaceAbove > spaceBelow;
    const maxH = Math.max(minMaxH, Math.min(preferredMaxH, openUp ? spaceAbove : spaceBelow));

    const width = Math.round(typeof getWidth === 'function' ? getWidth(r) : r.width);
    panel.style.minWidth = width + 'px';
    panel.style.width    = width + 'px';
    panel.style.maxHeight = Math.round(maxH) + 'px';

    panel.style.left = Math.round(r.left) + 'px';
    panel.style.top  = openUp
      ? Math.round(r.top - panel.offsetHeight - margin) + 'px'
      : Math.round(r.bottom + margin) + 'px';

    const pr = panel.getBoundingClientRect();
    const overflowRight = pr.right - window.innerWidth;
    if (overflowRight > 0) {
      panel.style.left = Math.round(r.left - overflowRight - 4) + 'px';
    }
    if (pr.left < 0) {
      panel.style.left = '4px';
    }
    panel.style.visibility = '';
  }

  function open() {
    document.querySelectorAll('.fc-custom-options').forEach(p => {
      if (p !== panel) p.classList.add('hidden');
    });
    place();
    btn.setAttribute('aria-expanded', 'true');

    if (!bound) {
      bound = true;
      window.addEventListener('scroll', place, true);
      window.addEventListener('resize', place, { passive: true });
      document.addEventListener('click', onDocClick, true);
    }
  }

  function close() {
    panel.classList.add('hidden');
    btn.setAttribute('aria-expanded', 'false');
    if (bound) {
      bound = false;
      window.removeEventListener('scroll', place, true);
      window.removeEventListener('resize', place);
      document.removeEventListener('click', onDocClick, true);
    }
  }

  function onDocClick(e) {
    const t = e.target;
    if (!t) return;
    if (panel.contains(t)) return;
    if (root && root.contains && root.contains(t)) return;
    close();
  }

  function destroy() {
    close();
    // 不移除节点本身，交由上层决定；仅解绑事件
  }

  return { place, open, close, destroy };
}

// buildCustomSelectFromNative：接入 createPortalDropdown
function buildCustomSelectFromNative(nativeSelect, {
  placeholder = '-- 请选择 --',
  filter = (opt) => opt.value !== '',
  renderLabel = (opt) => EH(opt?.text || ''),
  renderOption = (opt) => renderLabel(opt)
} = {}) {
  if (!nativeSelect || nativeSelect._customBuilt) return { refresh:()=>{}, setDisabled:()=>{}, setValue:()=>{}, getValue:()=>nativeSelect?.value };
  nativeSelect._customBuilt = true;
  nativeSelect.style.display = 'none';

  const wrap = document.createElement('div');
  wrap.className = 'fc-custom-select';

  const btn = document.createElement('button');
  btn.type = 'button';
  btn.className = 'fc-custom-button fc-field border-gray-300';
  btn.setAttribute('aria-expanded', 'false');
  btn.innerHTML = `<span class="truncate fc-custom-label">${EH(placeholder)}</span><i class="fa-solid fa-chevron-down ml-2 text-gray-500"></i>`;

  const panel = document.createElement('div');
  panel.className = 'fc-custom-options hidden fc-portal';

  wrap.appendChild(btn);
  nativeSelect.parentNode.insertBefore(wrap, nativeSelect.nextSibling);
  document.body.appendChild(panel);

  let currentPlaceholder = placeholder;

  (function syncStyle(){
    try{
      const cs = getComputedStyle(nativeSelect);
      const br = cs.borderRadius || '.375rem';
      const fs = cs.fontSize || '14px';
      btn.style.borderRadius = br;
      btn.style.fontSize = fs;
      panel.style.borderRadius = br;
      panel.style.fontSize = fs;
    }catch(_){}
  })();

  function setLabelByValue(v){
    const opt = Array.from(nativeSelect.options).find(o => String(o.value) === String(v));
    btn.querySelector('.fc-custom-label').innerHTML = opt ? renderLabel(opt) : EH(currentPlaceholder);
  }
  function renderOptions(){
    const html = Array.from(nativeSelect.options)
      .filter(filter)
      .map(o => `<div class="fc-option" data-value="${EH(o.value)}">${renderOption(o)}</div>`)
      .join('');
    panel.innerHTML = html || '<div class="px-3 py-2 text-gray-500">无可选项</div>';
    setLabelByValue(nativeSelect.value);
  }
  nativeSelect.addEventListener('change', () => setLabelByValue(nativeSelect.value));
  renderOptions();

  // 使用通用门户控制
  const portal = createPortalDropdown(btn, panel, {
    root: wrap,
    getWidth: (r) => r.width // 保持“面板宽度 = 按钮宽度”的现有行为
  });

  btn.addEventListener('click', () => {
    const isHidden = panel.classList.contains('hidden');
    if (isHidden) portal.open(); else portal.close();
  });
  panel.addEventListener('click', (e) => {
    const node = e.target.closest('.fc-option');
    if (!node) return;
    const v = node.dataset.value || '';
    nativeSelect.value = v;
    nativeSelect.dispatchEvent(new Event('change', { bubbles: true }));
    setLabelByValue(v);
    portal.close();
  });

  return {
    refresh(){ renderOptions(); },
    setDisabled(disabled, opts = {}){
      btn.disabled = !!disabled;
      btn.setAttribute('aria-disabled', disabled ? 'true' : 'false');
      if (opts.placeholder) {
        currentPlaceholder = String(opts.placeholder);
        setLabelByValue(nativeSelect.value);
      }
      if (disabled) portal.close();
    },
    setPlaceholder(text){
      currentPlaceholder = String(text || placeholder);
      setLabelByValue(nativeSelect.value);
    },
    setValue(v){
      nativeSelect.value = v;
      nativeSelect.dispatchEvent(new Event('change', { bubbles:true }));
      setLabelByValue(v);
      portal.close();
    },
    getValue(){ return nativeSelect.value; }
  };
}

// buildCustomConditionDropdown：接入 createPortalDropdown
function buildCustomConditionDropdown(sel, items){
  if (!sel || sel._customBuilt) return { setDisabled: ()=>{} };
  sel._customBuilt = true;
  sel.style.display = 'none';

  const wrap = document.createElement('div');
  wrap.className = 'fc-custom-select';

  const btn = document.createElement('button');
  btn.type = 'button';
  btn.className = 'fc-custom-button fc-field border-gray-300';
  btn.setAttribute('aria-expanded', 'false');
  btn.innerHTML = `
    <span class="truncate fc-custom-label">-- 选择测试工况 --</span>
    <i class="fa-solid fa-chevron-down ml-2 text-gray-500"></i>
  `;

  const panel = document.createElement('div');
  panel.className = 'fc-custom-options hidden fc-portal';

  panel.innerHTML = (items||[]).map(it => {
    const value = String(it.condition_id);
    const name = EH(it.condition_name_zh || '');
    const extra = FS(it.resistance_type_zh, it.resistance_location_zh);
    const extraHtml = extra ? `<span class="fc-cond-extra"> - ${extra}</span>` : '';
    return `<div class="fc-option" data-value="${value}">
              <span class="fc-cond-name">${name}</span>${extraHtml}
            </div>`;
  }).join('') || '<div class="px-3 py-2 text-gray-500">无可选项</div>';

  sel.parentNode.insertBefore(wrap, sel.nextSibling);
  wrap.appendChild(btn);
  document.body.appendChild(panel);

  (function syncStyle(){
    try{
      const cs = getComputedStyle(sel);
      const br = cs.borderRadius || '.375rem';
      const fs = cs.fontSize || '14px';
      btn.style.borderRadius = br;
      btn.style.fontSize = fs;
      panel.style.borderRadius = br;
      panel.style.fontSize = fs;
    }catch(_){}
  })();

  function setButtonLabelByValue(v){
    const rec = (items||[]).find(x => String(x.condition_id) === String(v));
    const labelBox = btn.querySelector('.fc-custom-label');
    if (!rec) { labelBox.innerHTML = '-- 选择测试工况 --'; return; }
    const name = EH(rec.condition_name_zh || '');
    const extra = FS(rec.resistance_type_zh, rec.resistance_location_zh);
    labelBox.innerHTML = `${name}${extra ? `<span class="fc-cond-extra"> - ${extra}</span>` : ''}`;
  }
  sel.addEventListener('change', () => setButtonLabelByValue(sel.value));
  setButtonLabelByValue(sel.value || '');

  // 使用通用门户控制
  const portal = createPortalDropdown(btn, panel, {
    root: wrap,
    getWidth: (r) => r.width
  });

  btn.addEventListener('click', () => {
    const isHidden = panel.classList.contains('hidden');
    if (isHidden) portal.open(); else portal.close();
  });
  panel.addEventListener('click', (e) => {
    const node = e.target.closest('.fc-option');
    if (!node) return;
    const v = node.dataset.value || '';
    sel.value = v;
    sel.dispatchEvent(new Event('change', { bubbles:true }));
    setButtonLabelByValue(v);
    portal.close();
  });

  return {
    setDisabled(disabled){
      btn.disabled = !!disabled;
      btn.setAttribute('aria-disabled', disabled ? 'true' : 'false');
      if (disabled) portal.close();
    }
  };
}

  /**
   * Execute a search with the given payload and render the results.
   * Reusable for both initial form submission and profile-switch re-fetches.
   *
   * opts.showToasts  – show loading/success/error toast messages (default: false)
   * opts.switchTab   – navigate to the search-results tab after success (default: false)
   */
  async function _executeSearch(payload, opts) {
    const showToasts = !!(opts && opts.showToasts);
    const switchTab  = !!(opts && opts.switchTab);
    const cacheNS = 'search';

    const doFetch = async () => {
      const resp = await fetch('/api/search_fans', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      const j = await resp.json();
      // Piggyback: check announcement fingerprint from this response
      if (typeof window._checkAnnouncementMeta === 'function' && j && j.meta) {
        window._checkAnnouncementMeta(j.meta);
      }
      if (has.normalize) {
        const n = window.normalizeApiResponse(j);
        if (!n.ok) return { success: false, error_message: n.error_message };
        const d = n.data || {};
        return { success: true, search_results: d.search_results, condition_label: d.condition_label };
      } else {
        if (!j || j.success !== true) return { success: false, error_message: (j && j.error_message) || '搜索失败' };
        const d = j.data || {};
        return { success: true, search_results: d.search_results, condition_label: d.condition_label };
      }
    };

    const renderResults = (data) => {
      if (window.__APP?.modules?.search?.render) {
        window.__APP.modules.search.render(data.search_results, data.condition_label);
      } else if (typeof window.renderSearchResults === 'function') {
        window.renderSearchResults(data.search_results, data.condition_label);
      }
    };

    const switchToSearchTab = () => {
      if (switchTab) {
        document.querySelector(
          '.fc-tabs[data-tab-group="right-panel"] .fc-tabs__item[data-tab="search-results"]'
        )?.click();
      }
    };

    const cached = Cache.get(cacheNS, payload);
    if (cached) {
      renderResults(cached);
      switchToSearchTab();
      showToasts && window.showInfo('已使用缓存结果...');
      // Async background refresh
      try {
        const fresh = await doFetch();
        if (fresh.success) {
          Cache.set(cacheNS, payload, fresh);
          if (!cached || JSON.stringify(cached.search_results) !== JSON.stringify(fresh.search_results)) {
            renderResults(fresh);
            showToasts && window.showInfo('已刷新最新结果');
          }
        }
      } catch (_) {}
      return;
    }

    // Cache miss
    showToasts && window.showLoading('op', '搜索中...');
    try {
      const data = await doFetch();
      if (!data.success) {
        showToasts && window.hideLoading('op');
        showToasts && window.showError(data.error_message || '搜索失败');
        return;
      }
      Cache.set(cacheNS, payload, data);
      renderResults(data);
      showToasts && window.hideLoading('op');
      showToasts && window.showSuccess('搜索完成');
      switchToSearchTab();
    } catch (err) {
      showToasts && window.hideLoading('op');
      showToasts && window.showError('搜索异常: ' + err.message);
    }
  }

  function initConditionSearch(){
    const form = $$('#searchForm');
    const sel = $$('#conditionFilterSelect');
    if (!form || !sel) return;

    // Special value for composite (综合评分) mode
    const COMPOSITE_VAL = '__composite__';

    // 工况下拉：加载 + 自定义
    (async function initSelect(){
      sel.disabled = true;
      sel.innerHTML = '<option value="">-- 选择测试工况 --</option>';
      // Prepend the special 综合评分 option to the native select
      const oComp = document.createElement('option');
      oComp.value = COMPOSITE_VAL;
      oComp.textContent = '综合性能';
      sel.appendChild(oComp);
      let ui = null;
      try{
        const r = await fetch('/get_conditions?raw=1');
        const list = await r.json();
        const arr = Array.isArray(list) ? list : [];
        arr.forEach(it=>{
          const o = document.createElement('option');
          o.value = String(it.condition_id);
          const extra = FS(it.resistance_type_zh, it.resistance_location_zh);
          const base = it.condition_name_zh || '';
          o.textContent = extra ? `${base} - ${extra}` : base;
          sel.appendChild(o);
        });
        // 自定义下拉（带灰色后缀）— prepend the composite special item
        const compositeItem = {
          condition_id: COMPOSITE_VAL,
          condition_name_zh: '综合性能',
          resistance_type_zh: null,
          resistance_location_zh: null,
        };
        ui = buildCustomConditionDropdown(sel, [compositeItem, ...arr]);
        // Default-select 综合评分 so the form starts in composite mode
        sel.value = COMPOSITE_VAL;
        sel.dispatchEvent(new Event('change', { bubbles: true }));
      }catch(_){
      } finally {
        sel.disabled = false;
        ui && ui.setDisabled(false);
      }
    })();

    // 统一当前页签内其它下拉（尺寸、限制条件）
    const sizeSel = form.querySelector('select[name="size_filter"]');
    if (sizeSel) buildCustomSelectFromNative(sizeSel, { placeholder: '尺寸(mm)' });

    const sortSel = form.querySelector('#sortBySelect');
    let sortSelUi = null;
    if (sortSel) sortSelUi = buildCustomSelectFromNative(sortSel, {
      placeholder: '排序依据',
      renderLabel: (opt) => opt?.value === 'composite_score'
        ? `<i class="fa-solid fa-lock" style="font-size:11px;margin-right:4px;opacity:0.7"></i>${EH(opt?.text || '')}`
        : EH(opt?.text || ''),
      renderOption: (opt) => opt?.value === 'composite_score'
        ? `<i class="fa-solid fa-lock" style="font-size:11px;margin-right:4px;opacity:0.7"></i>${EH(opt?.text || '')}`
        : EH(opt?.text || '')
    });

    // RGB灯光下拉：从后端加载并构建自定义选择器
    const rgbSel = form.querySelector('select[name="rgb_light"]');
    if (rgbSel) {
      (async function initRgbSelect(){
        try {
          const res = await fetchJSON('/api/rgb_options');
          const items = res.ok ? (res.data?.items || []) : [];
          items.forEach(val => {
            const o = document.createElement('option');
            o.value = val; o.textContent = val;
            rgbSel.appendChild(o);
          });
        } catch(e) { console.warn('[RGB] Failed to load rgb_options:', e); }
        buildCustomSelectFromNative(rgbSel, { placeholder: '不限' });
      })();
    }

    // ---- 综合评分 special mode: enter/exit helpers ----
    function enterCompositeMode() {
      if (!sortSel) return;
      // Add 综合评分 option to sortBySelect if not already present
      if (!Array.from(sortSel.options).find(o => o.value === 'composite_score')) {
        const o = document.createElement('option');
        o.value = 'composite_score';
        o.textContent = '综合评分';
        sortSel.insertBefore(o, sortSel.options[0] || null);
      }
      // Set value, fire change (updates label + sortValueInput disabled state), then disable
      sortSel.value = 'composite_score';
      sortSel.dispatchEvent(new Event('change', { bubbles: true }));
      if (sortSelUi) { sortSelUi.refresh(); sortSelUi.setDisabled(true); }
    }

    function exitCompositeMode() {
      if (!sortSel) return;
      // Remove the composite_score option if present
      const oComp = Array.from(sortSel.options).find(o => o.value === 'composite_score');
      if (oComp) sortSel.removeChild(oComp);
      // Restore to default sort and enable
      sortSel.value = 'condition_score';
      sortSel.dispatchEvent(new Event('change', { bubbles: true }));
      if (sortSelUi) { sortSelUi.setDisabled(false); sortSelUi.refresh(); }
    }

    // Listen for condition dropdown changes to toggle composite mode
    sel.addEventListener('change', () => {
      if (sel.value === COMPOSITE_VAL) {
        enterCompositeMode();
      } else {
        // Only exit if we were in composite mode (sortSel has composite_score selected)
        if (sortSel && sortSel.value === 'composite_score') {
          exitCompositeMode();
        }
      }
    });

    form.addEventListener('submit', async (e)=>{
      e.preventDefault();
      const cidStr = sel && sel.value ? String(sel.value).trim() : '';
      if (!cidStr){ has.toast && window.showError('请选择测试工况'); return; }
      const isComposite = (cidStr === COMPOSITE_VAL);
      if (!isComposite && !/^\d+$/.test(cidStr)){ has.toast && window.showError('工况选项未正确初始化，请刷新页面'); return; }

      const fd = new FormData(form);
      const payload = {}; fd.forEach((v,k)=>payload[k]=v);
      delete payload.condition;
      delete payload.condition_name;
      if (isComposite) {
        payload.composite_mode = true;
        delete payload.condition_id;
        delete payload.sort_by;
        delete payload.sort_value;
      } else {
        payload.condition_id = Number(cidStr);
      }
      if (!payload.rgb_light || payload.rgb_light === '不限') delete payload.rgb_light;

      // Store the normalized search payload for in-page re-submit scenarios.
      _lastSearchBasePayload = Object.assign({}, payload);

      await _executeSearch(payload, { showToasts: has.toast, switchTab: true });
    });
  }

  const CondState = {
    items: [],
    selected: new Set(),
    get allChecked() { return this.items.length > 0 && this.selected.size === this.items.length; },
    clear() { this.items = []; this.selected.clear(); }
  };

  function setCondPlaceholder(text){
    const el = $$('#condPlaceholder'); const list = $$('#conditionRadarChart'); const box = $$('#conditionMulti');
    if (!el || !box) return;
    el.textContent = text || '';
    el.classList.remove('hidden');
    list && list.classList.add('hidden');
  }

  function showCondList(){
    const el = $$('#condPlaceholder'); const list = $$('#conditionRadarChart');
    if (el) el.classList.add('hidden');
    if (list) list.classList.remove('hidden');
  }

  function getCondTitleLabel(){
    const box = $$('#conditionMulti');
    if (!box) return null;
    const row = box.closest('.fc-form-row');
    if (!row) return null;
    const label = row.querySelector('label');
    return label || null;
  }
  // Canonical CCW-from-UL order comes from the backend (injected via APP_CONFIG.radarCids).
  // Slot semantics (6 slots): UL(0) → L(1) → LL(2) → LR(3) → R(4) → UR(5).
  // The LABEL_SLOTS inside renderConditionList are indexed in CW order (UL→UR→R→LR→LL→L),
  // so we derive CW order: CW[j] = CCW[(n-j) % n].
  const _DEFAULT_RADAR_CIDS_CCW = [1, 10, 7, 8, 3, 2];
  const _APP_RADAR_CIDS = window.APP_CONFIG && window.APP_CONFIG.radarCids;
  const _RADAR_CIDS_CCW = Array.isArray(_APP_RADAR_CIDS) && _APP_RADAR_CIDS.length === 6
    ? _APP_RADAR_CIDS
    : (function(){
        if (typeof console !== 'undefined' && console && typeof console.warn === 'function' && _APP_RADAR_CIDS != null) {
          console.warn('[fancool-search] Invalid APP_CONFIG.radarCids length; expected 6 entries for radar rendering. Falling back to default radarCids.', _APP_RADAR_CIDS);
        }
        return _DEFAULT_RADAR_CIDS_CCW;
      })();
  const _N_SEARCH = _RADAR_CIDS_CCW.length;
  const RADAR_CIDS = Array.from({length: _N_SEARCH}, (_, j) => _RADAR_CIDS_CCW[(_N_SEARCH - j) % _N_SEARCH]);
  const _condLabelCache = {}; // conditionId (number) -> condition_name_zh string
  const _radarCache = {};     // model_id (string) -> radar data {conditions, composite_score, updated_at}

  // Last search payload for optional in-page re-submit flows.
  let _lastSearchBasePayload = null;

  // Expose radar cache for cross-module access
  Object.defineProperty(window, '__radarCache', { get: () => _radarCache, configurable: true });
  Object.defineProperty(window, '__condLabelCache', { get: () => _condLabelCache, configurable: true });

  // Single authoritative promise for condition-label readiness.
  // recently-removed.js sets this up early (before fancool.js runs) so that
  // rebuild() can correctly await it even though fancool-search.js loads later.
  // Detect the early-created promise and reuse its resolver; otherwise create fresh.
  const _LS_COND_KEY = 'fc_cond_labels_v1';
  let _condLabelCacheReadyResolve;
  if (window.__condLabelCacheReady instanceof Promise && typeof window.__condLabelCacheReadyResolve === 'function') {
    // Promise was created early by recently-removed.js — reuse its resolver.
    _condLabelCacheReadyResolve = window.__condLabelCacheReadyResolve;
  } else {
    // Fallback: create promise now (e.g., if recently-removed.js was not loaded).
    window.__condLabelCacheReady = new Promise(function(resolve) {
      _condLabelCacheReadyResolve = resolve;
    });
    window.__condLabelCacheReadyResolve = _condLabelCacheReadyResolve;
  }

  // Pre-populate condition-label cache from localStorage so that the promise can
  // resolve immediately on repeat loads, before the network fetch completes.
  // This ensures browsing-history cards render with correct labels even when the
  // /get_conditions fetch is still in flight.
  // Cache key includes a version suffix (_v1); bump the suffix to invalidate all
  // persisted entries when the data schema changes.
  const _RADAR_CIDS_SET = new Set(RADAR_CIDS);
  (function _hydrateCondLabelCacheFromStorage() {
    try {
      const stored = localStorage.getItem(_LS_COND_KEY);
      if (!stored) return;
      const parsed = JSON.parse(stored);
      if (!parsed || typeof parsed !== 'object') return;
      let hydrated = false;
      Object.entries(parsed).forEach(function(entry) {
        const cid = Number(entry[0]);
        if (_RADAR_CIDS_SET.has(cid) && entry[1]) {
          _condLabelCache[cid] = entry[1];
          hydrated = true;
        }
      });
      if (hydrated) {
        // Resolve the ready-promise immediately with cached labels so that rebuild()
        // — which is already queued and awaiting this promise — can render labels
        // without waiting for the network fetch.
        // The preloadConditionLabels() call below will also call _condLabelCacheReadyResolve
        // after the network fetch, but since Promises can only be resolved once, that
        // subsequent call is a safe no-op.
        _condLabelCacheReadyResolve(Object.assign({}, _condLabelCache));
      }
    } catch (_) {}
  })();

  async function preloadConditionLabels() {
    try {
      const r = await fetchJSON('/get_conditions');
      const list = r.ok ? (Array.isArray(r.data) ? r.data : []) : [];
      list.forEach(it => {
        const id = Number(it.condition_id);
        if (RADAR_CIDS.includes(id)) {
          _condLabelCache[id] = it.condition_name_zh || String(id);
        }
      });
      // Persist fresh labels to localStorage for instant availability on next load.
      try {
        const toStore = {};
        Object.entries(_condLabelCache).forEach(function(e) { toStore[e[0]] = e[1]; });
        localStorage.setItem(_LS_COND_KEY, JSON.stringify(toStore));
      } catch (_) {}
      // Resolve the shared ready-promise so all waiting modules can render labels.
      // If already resolved via localStorage hydration above, this is a no-op.
      // recently-removed.js and fancool.js await this promise directly, so no
      // per-module rebuild notifications are needed here.
      _condLabelCacheReadyResolve(Object.assign({}, _condLabelCache));
      // Notify RadarOverview so the ECharts radar renders proper condition names
      // (not numeric IDs) immediately after labels are loaded, without waiting for
      // the user to add/remove a model.
      if (window.RadarOverview && typeof window.RadarOverview.setConditionLabels === 'function') {
        window.RadarOverview.setConditionLabels(Object.assign({}, _condLabelCache));
      }
    } catch(e) {
      typeof console !== 'undefined' && console.warn('[FancoolSearch] condition label preload failed:', e);
      // Resolve with whatever was cached (possibly empty) so waiting modules unblock.
      _condLabelCacheReadyResolve(Object.assign({}, _condLabelCache));
    }
  }

  /** Convert a radar cache entry's conditions dict to the items array expected by renderConditionList. */
  function radarCacheToItems(radarData) {
    const conditions = (radarData && radarData.conditions) || {};
    return Object.entries(conditions).map(([cid, sc]) => ({
      condition_id: Number(cid),
      score_total: sc.score_total,
    }));
  }

  /** Get the composite score for a radar cache entry. */
  function radarCacheCompositeScore(radarData) {
    if (!radarData) return null;
    return radarData.composite_score !== undefined ? radarData.composite_score : null;
  }

  function renderConditionList(items, compositeScore){
    // Display-only: show scores without condition selection
    // (Condition selection is now done via the radar overview condition pills)
    CondState.items = (Array.isArray(items) ? items : [])
      .filter(it => RADAR_CIDS.includes(Number(it.condition_id)));
    CondState.selected.clear();

    const container = $$('#conditionRadarChart');
    if (!container) return;

    // Map condition_id -> item for fast lookup
    const itemMap = {};
    items.forEach(it => { itemMap[Number(it.condition_id)] = it; });

    // Prefer the shared SVG builder; keep the inline fallback for load-order safety.
    if (typeof window.buildMiniRadarSVG === 'function') {
      const svgItems = RADAR_CIDS.map(cid => {
        const it = itemMap[cid];
        return it ? { condition_id: cid, score_total: it.score_total, condition_name_zh: it.condition_name_zh } : null;
      }).filter(Boolean);
      container.innerHTML = window.buildMiniRadarSVG(svgItems, compositeScore, _condLabelCache, 'fc-radar-svg');
      showCondList();
      return;
    }

    // Inline fallback for early-load cases.
    const W = 300, H = 120, cx = 150, cy = 60;
    const gridR = 52;
    const sideR  = gridR + 30;
    const SCORE_LABEL_OFFSET = 12;
    const UNSCORED_INDICATOR_RATIO = 0.06;
    const N = 6;

    function axisAngle(i) { return -Math.PI / 2 - Math.PI / 6 + (i * 2 * Math.PI / N); }
    function vpt(r, i) {
      const a = axisAngle(i);
      return [cx + r * Math.cos(a), cy + r * Math.sin(a)];
    }
    function polyPts(rfn) {
      return RADAR_CIDS.map((cid, i) => {
        const [x, y] = vpt(rfn(cid, i), i);
        return x.toFixed(1) + ',' + y.toFixed(1);
      }).join(' ');
    }

    const rings = [0.25, 0.5, 0.75, 1.0].map(f => polyPts(() => f * gridR));

    const dataPts = polyPts((cid) => {
      const it = itemMap[cid];
      if (!it) return 0;
      if (it.score_total == null) return gridR * UNSCORED_INDICATOR_RATIO;
      return (Math.max(0, Math.min(100, it.score_total)) / 100) * gridR;
    });

    let svg = `<svg id="fc-radar-svg" class="fc-radar-svg" viewBox="0 0 ${W} ${H}" xmlns="http://www.w3.org/2000/svg" aria-label="综合评分雷达图" role="img">`;

    rings.forEach(pts => { svg += `<polygon class="fc-radar-ring" points="${pts}"/>`; });

    RADAR_CIDS.forEach((cid, i) => {
      const [vx, vy] = vpt(gridR, i);
      svg += `<line class="fc-radar-axis" x1="${cx}" y1="${cy}" x2="${vx.toFixed(1)}" y2="${vy.toFixed(1)}"/>`;
    });

    svg += `<polygon class="fc-radar-area" id="fc-radar-area" points="${dataPts}"/>`;

    // Per-vertex: score label only (no clickable elements)
    RADAR_CIDS.forEach((cid, i) => {
      const it = itemMap[cid];
      if (!it) return;
      const score = it.score_total;
      if (score != null) {
        const r = (Math.max(0, Math.min(100, score)) / 100) * gridR;
        const [slx, sly] = vpt(r + SCORE_LABEL_OFFSET, i);
        svg += `<text class="fc-radar-score-lbl" x="${slx.toFixed(1)}" y="${sly.toFixed(1)}" text-anchor="middle" dominant-baseline="middle">${Math.round(score)}</text>`;
      }
    });

    // Per-slot layout table for the 6 condition labels: [lx, ly, anchor]
    const CORNER_LBL_Y_MARGIN = 14;
    const CORNER_LBL_X_OFFSET = Math.round(sideR * 0.55);
    const LABEL_SLOTS = [
      [cx - CORNER_LBL_X_OFFSET, CORNER_LBL_Y_MARGIN,     'end'  ],
      [cx + CORNER_LBL_X_OFFSET, CORNER_LBL_Y_MARGIN,     'start'],
      [cx + sideR,               cy,                       'start'],
      [cx + CORNER_LBL_X_OFFSET, H - CORNER_LBL_Y_MARGIN, 'start'],
      [cx - CORNER_LBL_X_OFFSET, H - CORNER_LBL_Y_MARGIN, 'end'  ],
      [cx - sideR,               cy,                       'end'  ],
    ];
    RADAR_CIDS.forEach((cid, i) => {
      const rawLabel = _condLabelCache[cid] || itemMap[cid]?.condition_name_zh || String(cid);
      const label = EH(rawLabel);
      const available = !!itemMap[cid];
      const [lx, ly, anchor] = LABEL_SLOTS[i];
      if (available) {
        svg += `<text class="fc-radar-lbl" x="${lx.toFixed(1)}" y="${ly.toFixed(1)}" text-anchor="${anchor}" dominant-baseline="middle" style="pointer-events:none">${label}</text>`;
      } else {
        svg += `<text class="fc-radar-lbl unavail" x="${lx.toFixed(1)}" y="${ly.toFixed(1)}" text-anchor="${anchor}" dominant-baseline="middle" style="pointer-events:none">${label}</text>`;
      }
    });

    // Center: composite score
    const scoreText = (compositeScore != null) ? String(compositeScore) : '综评';
    svg += `<circle class="fc-radar-center-display" cx="${cx}" cy="${cy}" r="22" aria-label="综合评分"/>`;
    svg += `<text class="fc-radar-center-lbl fc-radar-center-lbl--bold" x="${cx}" y="${cy}" text-anchor="middle" dominant-baseline="middle">${EH(scoreText)}</text>`;

    svg += `</svg>`;
    container.innerHTML = svg;

    showCondList();
    // No condition count label needed (no selection)
  }

  // 数据获取
  async function fetchBrands(){
    const r = await fetchJSON('/api/brands'); return r.ok ? (r.data?.items || r.data || []) : [];
  }
  function formatBrandZhText(brand){
    const zh = String(brand?.brand_name_zh ?? '').trim();
    const legacy = String(brand?.brand_name ?? '').trim();
    return zh || legacy || '未知品牌';
  }
  function formatBrandBusinessText(brand){
    const zh = formatBrandZhText(brand);
    const en = String(brand?.brand_name_en ?? '').trim();
    return (zh && en) ? `${zh} / ${en}` : (zh || en || '未知品牌');
  }
  function formatBrandOptionText(brand){
    const base = formatBrandBusinessText(brand);
    const countNum = Number(brand?.model_count);
    const hasCount = Number.isFinite(countNum) && countNum >= 0;
    if (!hasCount || !base) return base;
    return `${base} (${Math.trunc(countNum)})`;
  }
  async function fetchModelsByBrand(brandId){
    const r = await fetchJSON(`/api/models_by_brand?brand_id=${encodeURIComponent(brandId)}`);
    return r.ok ? (r.data?.items || r.data || []) : [];
  }
  async function fetchConditionsByModel(modelId){
    const r = await fetchJSON(`/api/conditions_by_model?model_id=${encodeURIComponent(modelId)}`);
    return r.ok ? (r.data?.items || r.data || []) : [];
  }
function initModelCascade(){
  const form = $$('#fanForm');
  const brandSelect = $$('#brandSelect');
  const modelSelect = $$('#modelSelect');
  const conditionLoadingEl = $$('#conditionLoading');
  if (!form || !brandSelect || !modelSelect) return;

  const showCondLoading = () => {
    if (!conditionLoadingEl) return;
    conditionLoadingEl.classList.remove('hidden');
    conditionLoadingEl.removeAttribute('aria-hidden');
  };
  const hideCondLoading = () => {
    if (!conditionLoadingEl) return;
    conditionLoadingEl.classList.add('hidden');
    conditionLoadingEl.setAttribute('aria-hidden', 'true');
  };
  hideCondLoading();

  const uiBrand = buildCustomSelectFromNative(brandSelect, { placeholder:'-- 选择品牌 --' });

  // Show a shared score badge beside model options when a composite score exists.
  function _renderModelOption(opt) {
    const name = EH(opt.dataset.modelName || opt.text || '');
    const rawScore = opt.dataset.score;
    if (!rawScore) return name;
    const score = Number(rawScore);
    if (!Number.isFinite(score)) return name;
    const styleAttr = window.ScoreBadgeHelper
      ? window.ScoreBadgeHelper.scoreStyleAttr(score)
      : '';
    const badge = `<span class="rpv2-score-badge" style="${styleAttr}" data-score="${score}">${Math.round(score)}</span>`;
    return `<span class="fc-model-option-with-badge">${name}${badge}</span>`;
  }
  const uiModel = buildCustomSelectFromNative(modelSelect, {
    placeholder: '-- 选择型号 --',
    renderLabel:  _renderModelOption,
    renderOption: _renderModelOption,
  });

  (async function initBrands(){
    try {
      const brands = await fetchBrands();
      brandSelect.innerHTML = '<option value="">-- 选择品牌 --</option>' +
        brands.map(b=>`<option value="${EH(b.brand_id)}" data-brand-label="${EH(formatBrandZhText(b))}">${EH(formatBrandOptionText(b))}</option>`).join('');
      brandSelect.disabled = false;
      uiBrand.refresh(); uiBrand.setDisabled(false);
    } catch(_) {}
    // 型号默认禁用与占位
    modelSelect.innerHTML = '<option value="">-- 请先选择品牌 --</option>';
    modelSelect.value = '';
    modelSelect.disabled = true;
    uiModel.refresh();
    uiModel.setDisabled(true, { placeholder: '-- 请先选择品牌 --' });
    // 初始化占位，并确保加载层收起
    setCondPlaceholder('\u00A0-- 请先选择品牌 --');
    hideCondLoading();
  })();

  brandSelect.addEventListener('change', async ()=>{
    const bid = brandSelect.value;
    // 切品牌先收起加载层，避免残留
    hideCondLoading();

    if (!bid) {
      modelSelect.innerHTML = '<option value="">-- 请先选择品牌 --</option>';
      modelSelect.value = '';
      modelSelect.disabled = true;
      uiModel.refresh();
      uiModel.setDisabled(true, { placeholder: '-- 请先选择品牌 --' });

      CondState.clear();
      setCondPlaceholder('-- 请先选择品牌 --');
      return;
    }

    // 有品牌但未加载完型号前，先禁用并显示“选择型号”
    modelSelect.innerHTML = '<option value="">-- 选择型号 --</option>';
    modelSelect.value = '';
    modelSelect.disabled = true;
    uiModel.refresh();
    uiModel.setDisabled(true, { placeholder: '-- 选择型号 --' });

    CondState.clear();
    setCondPlaceholder('-- 请先选择型号 --');
    hideCondLoading(); // 确保这里也不显示加载层

    try {
      const models = await fetchModelsByBrand(bid);
      models.forEach(m=>{
        const o=document.createElement('option');
        o.value=m.model_id;
        o.dataset.modelName = m.model_name;
        const radarData = m.radar;
        const score = radarData
          ? radarCacheCompositeScore(radarData)
          : null;
        o.dataset.score = score != null ? String(score) : '';
        o.textContent = m.model_name;
        modelSelect.appendChild(o);
        if (radarData) _radarCache[String(m.model_id)] = radarData;
      });
      modelSelect.disabled=false;
      uiModel.refresh();
      uiModel.setDisabled(false);
    } catch(_){}
  });

  modelSelect.addEventListener('change', async ()=>{
    const mid = modelSelect.value;
    CondState.clear();

    if (!mid) {
      setCondPlaceholder('-- 请先选择型号 --');
      hideCondLoading();
      return;
    }

    // Use client-side radar cache if available (populated on brand change)
    const cached = _radarCache[String(mid)];
    if (cached && cached.conditions) {
      const items = radarCacheToItems(cached);
      if (items.length) {
        renderConditionList(items, radarCacheCompositeScore(cached));
        return;
      }
    }

    setCondPlaceholder('加载中...');
    showCondLoading();

    try {
      const items = await fetchConditionsByModel(mid);
      const compositeScore = (_radarCache[String(mid)])
        ? radarCacheCompositeScore(_radarCache[String(mid)])
        : null;
      if (Array.isArray(items) && items.length) {
        renderConditionList(items, compositeScore);
      } else {
        setCondPlaceholder('该型号暂无工况');
      }
    } catch(_){
      setCondPlaceholder('加载失败，请重试');
    } finally {
      hideCondLoading();
    }
  });

    // 型号关键字搜索
    (function initModelKeywordSearch(){
      const input = $$('#modelSearchInput');
      const popup = $$('#searchSuggestions');
      if (!input || !popup) return;
      let timer = null;

      input.addEventListener('input', ()=>{
        clearTimeout(timer);
        const q = input.value.trim();
        if (q.length < 2){ popup.classList.add('hidden'); return; }
        timer = setTimeout(async ()=>{
          try{
            const r = await fetch(`/search_models/${encodeURIComponent(q)}?raw=1`);
            const list = await r.json();
            const arr = Array.isArray(list) ? list : [];
            popup.innerHTML='';
            if (!arr.length){ popup.classList.add('hidden'); return; }
            arr.forEach(full=>{
              const div=document.createElement('div');
              div.className='cursor-pointer'; div.textContent=full;
              div.addEventListener('click', async ()=>{
                const parts = full.split(' ');
                const brandName = parts[0];
                const modelName = parts.slice(1).join(' ');
                try {
                  const brands = await fetchBrands();
                  const bRow = (brands || []).find(b => String(b.brand_name_zh) === String(brandName));
                  if (!bRow) throw new Error('未找到品牌ID');

                  // 用自定义下拉的 setValue，自动更新标签并派发 change
                  uiBrand.setValue(bRow.brand_id);

                  // 等待型号列表出现（延长至 2500ms）
                  await new Promise((resolve, reject)=>{
                    const deadline = Date.now() + 2000;
                    (function tryPick(){
                      const opts = Array.from(modelSelect.options || []);
                      const hit = opts.find(o => (o.dataset.modelName || o.textContent) === modelName);
                      if (hit) { resolve(hit.value); return; }
                      if (Date.now() > deadline) { reject(new Error('未找到型号ID')); return; }
                      setTimeout(tryPick, 60);
                    })();
                  }).then(mid => {
                    // 同步模型下拉（更新标签并派发 change）
                    uiModel.setValue(mid);
                  });
                  input.value=''; popup.classList.add('hidden');
                } catch(e){
                  has.toast && window.showError('无法定位到该型号（ID 级联）');
                }
              });
              popup.appendChild(div);
            });
            popup.classList.remove('hidden');
          } catch(_){
            popup.classList.add('hidden');
          }
        }, 280);
      });
      document.addEventListener('click', (e)=>{
        if (!input.contains(e.target) && !popup.contains(e.target)) popup.classList.add('hidden');
      });
    })();


    // 提交：添加至雷达对比（不直接加曲线，由 RadarState 驱动）
    form.addEventListener('submit', async (e)=>{
      e.preventDefault();
      const mid = modelSelect.value;
      if (!mid){ has.toast && window.showError('请先选择型号'); return; }

      // Get brand/label from the select elements
      const brandOption = brandSelect.options[brandSelect.selectedIndex];
      const brand = brandOption
        ? (brandOption.dataset.brandLabel || brandOption.textContent.trim())
        : '';
      const modelOption = modelSelect.options[modelSelect.selectedIndex];
      const label = (modelOption && modelOption.dataset.modelName)
        ? modelOption.dataset.modelName
        : (modelOption ? modelOption.textContent.trim() : String(mid));

      if (typeof window.addModelToRadar === 'function') {
        const added = await window.addModelToRadar(Number(mid), brand, label);
        if (added) {
          has.toast && window.showSuccess('已添加至雷达对比，请在雷达图中选择工况');
          window.__APP?.sidebar?.maybeAutoOpenSidebarOnAdd?.();
        }
      } else {
        has.toast && window.showError('雷达模块未就绪，请刷新页面');
      }
    });
  }

  function initAll(){
    preloadConditionLabels();
    initConditionSearch();
    initModelCascade();

  }

  // 自动初始化
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initAll, { once:true });
  } else {
    initAll();
  }

  window.FancoolSearch = {
    init: initAll,
    _debug: { CondState }
  };
})(window, document);
