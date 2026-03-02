window.APP_CONFIG = window.APP_CONFIG || { clickCooldownMs: 500, maxItems: 0, spectrumDockEnabled: false, playAudioEnabled: false };
/* ==== 命名空间根 ==== */
window.__APP = window.__APP || {};

const FRONT_MAX_ITEMS = (window.APP_CONFIG && window.APP_CONFIG.maxItems) || 8;
const LIKESET_VERIFY_MAX_AGE_MS = 5 * 60 * 1000;      // 5 分钟指纹过期
const PERIODIC_VERIFY_INTERVAL_MS = 3 * 60 * 1000;    // 3 分钟后台触发一次检查
const LIKE_FULL_FETCH_THRESHOLD = 20;

/* 在最前阶段就写入上限标签，避免闪烁 */
(function initMaxItemsLabel(){
  function apply(){
    const el = document.getElementById('maxItemsLabel');
    if (el && !el.dataset._inited){
      el.textContent = FRONT_MAX_ITEMS;
      el.dataset._inited = '1';
    }
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', apply, { once:true });
  } else {
    apply();
  }
})();



(async function fetchAppConfig(){
  try {
    const r = await fetch('/api/config');
    const j = await r.json();
    const resp = normalizeApiResponse(j);
    if (resp.ok) {
      const cfg = resp.data || {};
      window.APP_CONFIG.clickCooldownMs = cfg.click_cooldown_ms ?? window.APP_CONFIG.clickCooldownMs;
      window.APP_CONFIG.recentLikesLimit = cfg.recent_likes_limit ?? 50;
      window.APP_CONFIG.spectrumDockEnabled = !!cfg.spectrum_dock_enabled;
      window.APP_CONFIG.playAudioEnabled = !!cfg.play_audio_enabled;
      if (window.ChartRenderer && typeof window.ChartRenderer.__ensureDock === 'function') {
        const forceFromUrl = window.__forceSpectrumDockFromUrl?.();
        if (window.APP_CONFIG.spectrumDockEnabled || forceFromUrl) {
          window.ChartRenderer.__ensureDock();
        }
      }
    }
  } catch(_){}
})();

window.DisplayCache = (function(){
  const map = new Map(); // key => { brand, model, condition, rt, rl }
  function k(mid,cid){ return `${Number(mid)}_${Number(cid)}`; }
  return {
    setFromSeries(series){
      (series||[]).forEach(s=>{
        const mid = s.model_id, cid = s.condition_id;
        if (mid==null || cid==null) return;
        map.set(k(mid,cid), {
          brand: s.brand || s.brand_name_zh || '',
          model: s.model || s.model_name || '',
          condition: s.condition || s.condition_name_zh || '',
          rt: s.resistance_type || s.resistance_type_zh || s.res_type || s.rt || '',
          rl: s.resistance_location || s.resistance_location_zh || s.res_loc || s.rl || ''
        });
      });
    },
    setFromMeta(items){
      (items||[]).forEach(it=>{
        const mid = it.model_id, cid = it.condition_id;
        if (mid==null || cid==null) return;
        map.set(k(mid,cid), {
          brand: it.brand_name_zh || '',
          model: it.model_name || '',
          condition: it.condition_name_zh || '',
          rt: it.resistance_type_zh || '',
          rl: it.resistance_location_zh || ''
        });
      });
    },
    get(mid,cid){ return map.get(k(mid,cid)) || null; },
    clear(){ map.clear(); }
  };
})();

function installRemovedRenderHookOnce(){
  try{
    const mod = window.__APP && window.__APP.features && window.__APP.features.recentlyRemoved;
    if (!mod || !mod.rebuild) return false;
    if (mod.__ID_PATCHED__) return true;
    const orig = mod.rebuild;
    mod.rebuild = function(list){
      const enriched = (list||[]).map(it=>{
        const info = window.DisplayCache && window.DisplayCache.get(it.model_id, it.condition_id);
        return { ...it, brand: info?.brand || '', model: info?.model || '', condition: info?.condition || '加载中...' };
      });
      return orig(enriched);
    };
    mod.__ID_PATCHED__ = true;
    return true;
  }catch(_){ return false; }
}
// 若模块已可用，先试一次；否则在后续生命周期再兜底调用
installRemovedRenderHookOnce();

/* ==== P1-4 DOM 缓存与工具 ==== */
(function initDomCache(){
  const cache = Object.create(null);
  function one(sel, scope){
    if (!sel) return null;
    if (!scope && cache[sel]) return cache[sel];
    const el = (scope||document).querySelector(sel);
    if (!scope) cache[sel] = el;
    return el;
  }
  function all(sel, scope){
    return Array.from((scope||document).querySelectorAll(sel));
  }
  function clear(sel){ if(sel) delete cache[sel]; else Object.keys(cache).forEach(k=>delete cache[k]); }
  window.__APP.dom = { one, all, clear };
})();

/* ==== P1-5 帧写入调度器 ==== */
window.__APP.scheduler = (function(){
  const writeQueue = [];
  let scheduled = false;
  function flush(){
    scheduled = false;
    for (let i=0;i<writeQueue.length;i++){
      try { writeQueue[i](); } catch(e){ console.error('[scheduler write error]', e); }
    }
    writeQueue.length = 0;
  }
  function write(fn){
    writeQueue.push(fn);
    if (!scheduled){
      scheduled = true;
      requestAnimationFrame(flush);
    }
  }
  return { write };
})();

/* ==== 工具：通用延迟/防抖调度器 ==== */
function createDelayed(fn, delay){
  let timer = null;
  return function(){
    clearTimeout(timer);
    timer = setTimeout(fn, delay);
  };
}

/* ==== 工具：Snap 分页初始化（复用 left-panel / sidebar-top） ==== */
function initSnapTabScrolling(opts){
  const {
    containerId,
    group,
    persistKey,
    vertical = false,
    onActiveChange,
    clickScrollBehavior = 'smooth',
    defaultTab 
  } = opts || {};
  const container = document.getElementById(containerId);
  const nav = document.querySelector(`.fc-tabs[data-tab-group="${group}"]`);
  if (!container || !nav) return;
  const tabs = Array.from(nav.querySelectorAll('.fc-tabs__item'));
  if (!tabs.length) return;

  function go(idx, smooth=true){
    const w = vertical ? container.clientHeight : container.clientWidth;
    container.scrollTo({ [vertical?'top':'left']: w * idx, behavior: smooth?clickScrollBehavior:'auto' });
  }

  function activateIdx(idx, smooth=true, fromScroll=false){
    idx = Math.max(0, Math.min(idx, tabs.length-1));
    tabs.forEach((t,i)=>t.classList.toggle('active', i===idx));
    const tabName = tabs[idx]?.dataset.tab;
    if (!fromScroll) go(idx, smooth);
    if (persistKey && tabName) {
      try { localStorage.setItem(persistKey, tabName); } catch(_){}
    }
    if (typeof onActiveChange === 'function' && tabName){
      onActiveChange(tabName);
    }
  }

  nav.addEventListener('click', e=>{
    const item = e.target.closest('.fc-tabs__item');
    if (!item) return;
    const idx = tabs.indexOf(item);
    if (idx < 0) return;
    activateIdx(idx, true, false);
  });

  container.addEventListener('scroll', ()=>{
    clearTimeout(container._snapTimer);
    container._snapTimer = setTimeout(()=>{
      const w = vertical ? (container.clientHeight || 1) : (container.clientWidth || 1);
      const idx = Math.round( (vertical?container.scrollTop:container.scrollLeft) / w );
      activateIdx(idx, false, true);
    }, 80);
   }, { passive: true });

  let initIdx = 0;

  // 1) persistKey 优先
  if (persistKey){
    try {
      const saved = localStorage.getItem(persistKey);
      if (saved){
        const found = tabs.findIndex(t=>t.dataset.tab === saved);
        if (found >= 0) initIdx = found;
      }
    } catch(_){}
  }

  // 2) 没命中持久化，用 defaultTab
  if (initIdx === 0 && defaultTab) {
    const foundByDefault = tabs.findIndex(t=>t.dataset.tab === defaultTab);
    if (foundByDefault >= 0) initIdx = foundByDefault;
  }

  // 3) 没有 defaultTab，且导航自带 .active，则跟随 .active
  if (initIdx === 0) {
    const activeIdx = tabs.findIndex(t=>t.classList.contains('active'));
    if (activeIdx >= 0) initIdx = activeIdx;
  }

  // 4) 兜底 0
  requestAnimationFrame(()=>activateIdx(initIdx, false, false));
}

/* ==== P1-7 通用缓存 (内存+TTL) ==== */
window.__APP.cache = (function(){
  const store = new Map();
  const DEFAULT_TTL = 180000;
  function key(ns, payload){
    return ns + '::' + JSON.stringify(payload||{});
  }
  function get(ns, payload){
    const k = key(ns, payload);
    const rec = store.get(k);
    if (!rec) return null;
    if (Date.now() > rec.expire) { store.delete(k); return null; }
    return rec.value;
  }
  function set(ns, payload, value, ttl=DEFAULT_TTL){
    const k = key(ns, payload);
    store.set(k, { value, expire: Date.now()+ttl });
    return value;
  }
  function clear(ns){
    if (!ns){ store.clear(); return; }
    for (const k of store.keys()){
      if (k.startsWith(ns+'::')) store.delete(k);
    }
  }
  return { get, set, clear };
})();

const $ = (s) => window.__APP.dom.one(s);

// POLYFILL + safeClosest
(function() {
  if (typeof Element !== 'undefined') {
    if (!Element.prototype.matches) {
      Element.prototype.matches =
        Element.prototype.msMatchesSelector ||
        Element.prototype.webkitMatchesSelector ||
        function(selector) {
          const list = (this.document || this.ownerDocument).querySelectorAll(selector);
          let i = 0;
          while (list[i] && list[i] !== this) i++;
          return !!list[i];
        };
    }
    if (!Element.prototype.closest) {
      Element.prototype.closest = function(selector) {
        let el = this;
        while (el && el.nodeType === 1) {
          if (el.matches(selector)) return el;
          el = el.parentElement;
        }
        return null;
      };
    }
  }
  window.safeClosest = function safeClosest(start, selector) {
    if (!start) return null;
    let el = start;
    if (el.nodeType && el.nodeType !== 1) el = el.parentElement;
    if (!el) return null;
    if (el.closest) {
      try { return el.closest(selector); } catch(_) {}
    }
    while (el && el.nodeType === 1) {
      if (el.matches && el.matches(selector)) return el;
      el = el.parentElement;
    }
    return null;
  };
})();


/* =========================================================
   工具函数 / Toast / Throttle / HTML 转义
   ========================================================= */
function verifyLikeFingerprintIfStale(){
  try {
    if (!LocalState.likes.needRefresh(LIKESET_VERIFY_MAX_AGE_MS)) return;
    fetch('/api/like_status', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ pairs: [] })
    })
      .then(r=>r.json())
      .then(j=>{
        const resp = normalizeApiResponse(j);
        if (!resp.ok) return;
        const data = resp.data || {};
        if (data.fp){
          LocalState.likes.updateServerFP(data.fp);
          LocalState.likes.logCompare();
        }
      })
      .catch(()=>{});
  } catch(_){}
}
setInterval(verifyLikeFingerprintIfStale, PERIODIC_VERIFY_INTERVAL_MS);

const toastContainerId = 'toastContainer';
function ensureToastRoot() {
  let r = document.getElementById(toastContainerId);
  if (!r) { r = document.createElement('div'); r.id = toastContainerId; document.body.appendChild(r); }
  return r;
}
let toastIdCounter = 0;
const activeLoadingKeys = new Set();

function normalizeToastType(t){
  return ['success','error','loading','info'].includes(t) ? t : 'info';
}
function createToast(msg, type='info', opts={}) {
  type = normalizeToastType(type);
  const container = ensureToastRoot();
  const { autoClose = (type === 'loading' ? false : 2600), id = 't_'+(++toastIdCounter) } = opts;

  while (document.getElementById(id)) {
    document.getElementById(id).remove();
  }

  const iconMap = {
    success:'<i class="icon fa-solid fa-circle-check" style="color:var(--toast-success)"></i>',
    error:'<i class="icon fa-solid fa-circle-xmark" style="color:var(--toast-error)"></i>',
    loading:'<i class="icon fa-solid fa-spinner fa-spin" style="color:var(--toast-loading)"></i>',
    info:'<i class="icon fa-solid fa-circle-info" style="color:#3B82F6"></i>'
  };

  const div = document.createElement('div');
  div.className = 'fc-toast fc-toast--'+type;
  div.id = id;
  div.innerHTML = `${iconMap[type]||iconMap.info}<div class="msg">${msg}</div><span class="fc-toast__close" data-close="1">&times;</span>`;
  container.appendChild(div);

  if (autoClose) {
    setTimeout(()=>closeToast(id), autoClose);
  }
  return id;
}
function closeToast(id){
  const el = document.getElementById(id);
  if (!el) return;
  el.style.animation = 'toast-out .25s forwards';
  setTimeout(()=>el.remove(), 240);
}
document.addEventListener('click', (e)=>{
  if (e.target.closest && e.target.closest('[data-close]')) {
    const t = e.target.closest('.fc-toast'); if (t) closeToast(t.id);
  }
});

const loadingTimeoutMap = new Map();
function showLoading(key, text='加载中...') {
  if (activeLoadingKeys.has(key)) {
    const existing = document.getElementById('loading_'+key);
    if (existing) {
      const msgEl = existing.querySelector('.msg');
      if (msgEl) msgEl.textContent = text;
    }
    return;
  }
  activeLoadingKeys.add(key);
  createToast(text, 'loading', { id: 'loading_' + key });
  const to = setTimeout(()=>{
    if (activeLoadingKeys.has(key)) {
      hideLoading(key);
    }
  }, 12000);
  loadingTimeoutMap.set(key, to);
}
function hideLoading(key) {
  activeLoadingKeys.delete(key);
  const id = 'loading_' + key;
  const el = document.getElementById(id);
  if (el) el.remove();
  const t = loadingTimeoutMap.get(key);
  if (t) {
    clearTimeout(t);
    loadingTimeoutMap.delete(key);
  }
}
function autoCloseOpLoading() {
  hideLoading('op');
  document.querySelectorAll('.fc-toast.fc-toast--loading').forEach(t => {
    const msgEl = t.querySelector('.msg');
    if (!msgEl) return;
    const text = (msgEl.textContent || '').trim();
    if (/^(添加中|移除中)/.test(text)) {
      t.remove();
    }
  });
}
const showSuccess = (m)=>createToast(m,'success');
const showError   = (m)=>createToast(m,'error');
const showInfo    = (m)=>createToast(m,'info', {autoClose:1800});
// 显式挂到 window，供独立模块通过 window.showXXX 调用
window.showLoading = showLoading;
window.hideLoading = hideLoading;
window.showSuccess = showSuccess;
window.showError   = showError;
window.showInfo    = showInfo;

let lastGlobalAction = 0;
function globalThrottle(){
  const cd = Number(window.APP_CONFIG.clickCooldownMs || 2000);
  const now = Date.now();
  if (now - lastGlobalAction < cd) { showInfo('操作过于频繁，请稍后'); return false; }
  lastGlobalAction = now; return true;
}
const NO_THROTTLE_ACTIONS = new Set(['add','remove','restore','xaxis']);
const needThrottle = (action)=>!NO_THROTTLE_ACTIONS.has(action);

const ESC_MAP = { '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' };
function escapeHtml(s){ return String(s??'').replace(/[&<>"']/g,c=>ESC_MAP[c]); }
function unescapeHtml(s){
  const map = {'&amp;':'&','&lt;':'<','&gt;':'>','&quot;':'"','&#39;':"'"};
  return String(s??'').replace(/&(amp|lt|gt|quot|#39);/g,m=>map[m]);
}
// Export to window for cross-module access
window.escapeHtml = escapeHtml;
window.unescapeHtml = unescapeHtml;

/* =========================================================
   统一 API 响应归一化
   ========================================================= */
function normalizeApiResponse(json){
  if (!json || typeof json !== 'object') {
    return { ok:false, error_code:'INVALID', error_message:'响应格式错误', raw:json };
  }
  if (json.success === true) {
    return { ok:true, data: json.data, raw: json };
  }
  if (json.success === false) {
    return {
      ok:false,
      error_code: json.error_code || 'ERR',
      error_message: json.error_message || '操作失败',
      raw: json
    };
  }
  // 非标准直接认定失败（已去除旧兼容）
  return { ok:false, error_code:'LEGACY_FORMAT', error_message:'不支持的旧响应格式', raw: json };
}
// Export to window for cross-module access
window.normalizeApiResponse = normalizeApiResponse;

function asArray(maybe){
  if (Array.isArray(maybe)) return maybe;
  if (maybe && Array.isArray(maybe.data)) return maybe.data;
  // 某些接口旧格式可能是 { items: [...] }
  if (maybe && Array.isArray(maybe.items)) return maybe.items;
  return [];
}
function extractLikeKeys(dataObj){
  if (!dataObj || typeof dataObj !== 'object') return [];
  return dataObj.like_keys || dataObj.liked_keys || [];
}

function withFrontColors(chartData) {
  if (__isShareLoaded && !__shareAxisApplied && chartData && chartData.x_axis_type) {
    frontXAxisType = (chartData.x_axis_type === 'noise') ? 'noise_db' : chartData.x_axis_type;
    try { localStorage.setItem('x_axis_type', frontXAxisType); } catch (_) {}
    __shareAxisApplied = true;
  }
  const series = (chartData.series || []).map(s => {
    return {
      ...s,
      color: ColorManager.getColor(s.key),
      color_index: ColorManager.getIndex(s.key)
    };
  });
  return { ...chartData, x_axis_type: frontXAxisType, series };
}


let lastChartData = null;
let frontXAxisType = 'rpm';

(function initPersistedXAxisType(){
  try {
    const saved = localStorage.getItem('x_axis_type');
    if (saved === 'rpm' || saved === 'noise_db' || saved === 'noise') {
      frontXAxisType = (saved === 'noise') ? 'noise_db' : saved;
    }
  } catch(_) {}
})();

function getChartBg(){
  const host = document.getElementById('chart-settings') || document.body;
  let bg = '';
  try { bg = getComputedStyle(host).backgroundColor; } catch(_) {}
  if (!bg || bg === 'rgba(0, 0, 0, 0)' || bg === 'transparent') {
    try { bg = getComputedStyle(document.body).backgroundColor; } catch(_) {}
  }
  return bg && bg !== 'rgba(0, 0, 0, 0)' ? bg : '#ffffff';
}

const currentThemeStr = () =>
  (document.documentElement.getAttribute('data-theme') === 'dark' ? 'dark' : 'light');

window.applySidebarColors = function() {
  const rows = window.__APP.dom.all('#selectedFansList .fan-item');
  window.__APP.scheduler.write(()=> {
    rows.forEach(div => {
      const key = div.getAttribute('data-fan-key');
      const dot = div.querySelector('.js-color-dot');
      // 使用新的 ColorManager 接口
      if (key && dot) dot.style.backgroundColor = ColorManager.getColor(key);
    });
  });
};

function isValidNum(v) {
  return typeof v === 'number' && Number.isFinite(v);
}

function postChartData(chartData){
  lastChartData = chartData;

  // 保留后端原始 data.* 三数组，不做轴向裁剪
  const prepared = withFrontColors(chartData);
  const payload = {
    chartData: prepared,         // 直接传 canonical
    theme: currentThemeStr(),
    chartBg: getChartBg()
  };
  if (pendingShareMeta) {
    payload.shareMeta = pendingShareMeta;
    pendingShareMeta = null;
  }
  if (window.ChartRenderer && typeof ChartRenderer.render === 'function') {
    ChartRenderer.render(payload);
  }
}

function resizeChart(){
  if (window.ChartRenderer && typeof ChartRenderer.resize === 'function') {
    ChartRenderer.resize();
  }
}

/* =========================================================
   最近点赞懒加载
   ========================================================= */
let recentLikesLoaded = false;
let recentLikesLoadedCount = 0;
const recentLikesListEl = $('#recentLikesList');

function needFullLikeKeyFetch() {
  const fp = LocalState.likes.getServerFP && LocalState.likes.getServerFP();
  if (!fp) return false;
  if (fp.c >= LIKE_FULL_FETCH_THRESHOLD) return false;
  if (!LocalState.likes.isSynced() || !LocalState.likes.shouldSkipStatus(LIKESET_VERIFY_MAX_AGE_MS)) {
    return true;
  }
  return false;
}

function fetchAllLikeKeys(){
  if (fetchAllLikeKeys._pending) return;
  fetchAllLikeKeys._pending = true;
  fetch('/api/like_keys')
    .then(r=>r.json())
    .then(j=>{
      const resp = normalizeApiResponse(j);
      if (!resp.ok) return;
      const data = resp.data || {};
      const arr = extractLikeKeys(data);
      if (Array.isArray(arr)){
        LocalState.likes.setAll(arr);
        arr.forEach(k=>{
          const [m,c] = k.split('_');
          if (m && c) updateLikeIcons(m, c, true);
        });
      }
      if (data.fp){
        LocalState.likes.updateServerFP(data.fp);
      }
      LocalState.likes.logCompare();
    })
    .catch(()=>{})
    .finally(()=>{ fetchAllLikeKeys._pending = false; });
}

function rebuildRecentLikes(list){
  const wrap = recentLikesListEl;
  if (!wrap) return;
  wrap.innerHTML = '';

  if (!Array.isArray(list) || list.length === 0){
    wrap.innerHTML = '<p class="text-gray-500 text-center py-6">暂无最近点赞</p>';
    requestAnimationFrame(prepareRecentLikesMarquee);
    return;
  }

  // 分组：按 品牌+型号 聚合，同组里的不同工况作为 scenarios
  const groups = new Map();
  list.forEach(it => {
    const brand = it.brand_name_zh || it.brand || '';
    const model = it.model_name || it.model || '';
    const key = `${brand}||${model}`;

    if (!groups.has(key)) {
      groups.set(key, {
        brand,
        model,
        // 兼容若干后端字段命名
        maxSpeed: it.max_speed || it.max_rpm || it.rpm_max || '',
        size: it.size || it.frame_size || it.diameter || '',
        thickness: it.thickness || it.frame_thickness || '',
        scenarios: []
      });
    }
    const g = groups.get(key);
    g.scenarios.push({
      mid: it.model_id,
      cid: it.condition_id,
      condition: it.condition_name_zh || it.condition || '',
      // 供 formatScenario 使用的原始字段
      rt: it.resistance_type_zh || it.rt || '',
      rl: it.resistance_location_zh || it.rl || ''
    });
  });

  // 渲染
  const frag = document.createDocumentFragment();
  groups.forEach(g => {
    const metaParts = [];
    if (g.maxSpeed) metaParts.push(`${escapeHtml(g.maxSpeed)} RPM`);
    if (g.size && g.thickness) metaParts.push(`${escapeHtml(g.size)}x${escapeHtml(g.thickness)}`);
    const metaRight = metaParts.join(' · ');

    const scenariosHtml = g.scenarios.map(s => {
      const extra = (typeof formatScenario === 'function') ? formatScenario(s.rt, s.rl) : '';
      const label = extra ? `${s.condition} - ${extra}` : s.condition;
      const scenText = escapeHtml(label);
      return `
        <div class="flex items-center justify-between scenario-row">
          <div class="scenario-text text-sm text-gray-700">${scenText}</div>
          <div class="actions">
            <button type="button" class="like-button recent-like-button"
                    data-tooltip="取消点赞"
                    data-model-id="${escapeHtml(s.mid||'')}"
                    data-condition-id="${escapeHtml(s.cid||'')}">
              <i class="fa-solid fa-thumbs-up text-red-500"></i>
            </button>
            ${buildQuickBtnHTML('likes', g.brand, g.model, s.mid, s.cid, s.condition, 'liked')}
          </div>
        </div>`;
    }).join('');

    const groupDiv = document.createElement('div');
    groupDiv.className = 'recent-like-group p-3 border border-gray-200 rounded-md';
    groupDiv.innerHTML = `
      <div class="fc-group-header">
        <div class="fc-title-wrap flex items-center min-w-0">
          <div class="truncate font-medium">${escapeHtml(g.brand)} ${escapeHtml(g.model)}</div>
        </div>
        <div class="fc-meta-right text-sm text-gray-600">${metaRight}</div>
      </div>
      <div class="group-scenarios mt-2 space-y-1">${scenariosHtml}</div>`;
    frag.appendChild(groupDiv);
  });

  wrap.appendChild(frag);
  syncQuickActionButtons();
  requestAnimationFrame(() => {
    applyRecentLikesTitleMask();
    prepareRecentLikesMarquee();
  });
}

async function ensureLikeStatusBatch(pairs){
  if (!Array.isArray(pairs) || !pairs.length) return;
  if (needFullLikeKeyFetch()) {
    fetchAllLikeKeys();
    return;
  }
  const limit = window.APP_CONFIG.recentLikesLimit || 50;
  const need = [];
  const seen = new Set();
  for (const p of pairs){
    if (!p) continue;
    const mid = Number(p.model_id);
    const cid = Number(p.condition_id);
    if (!Number.isInteger(mid) || !Number.isInteger(cid)) continue;
    const key = `${mid}_${cid}`;
    if (LocalState.likes.has(key)) continue;
    if (seen.has(key)) continue;
    seen.add(key);
    need.push({ model_id: mid, condition_id: cid });
  }
  if (!need.length) return;
  if (recentLikesLoaded && recentLikesLoadedCount < limit) return;
  if (LocalState.likes.shouldSkipStatus(LIKESET_VERIFY_MAX_AGE_MS)) return;
  if (needFullLikeKeyFetch()) {
    fetchAllLikeKeys();
    return;
  }
  try {
    const resp = await fetch('/api/like_status', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ pairs: need })
    });
    if (!resp.ok) return;
    const j = await resp.json();
    const rdata = normalizeApiResponse(j);
    if (!rdata.ok) return;
    const data = rdata.data || {};
    if (data.fp) { LocalState.likes.updateServerFP(data.fp); LocalState.likes.logCompare(); }
    const list = extractLikeKeys(data);
    if (!list.length) return;
    list.forEach(k=>{
      if (!LocalState.likes.has(k)){
        LocalState.likes.add(k);
        const [m,c] = k.split('_');
        if (m && c) updateLikeIcons(m, c, true);
      }
    });
  } catch(_) {}
}
function reloadRecentLikes(){
  showLoading('recent-likes','加载最近点赞...');
  fetch('/api/recent_likes')
    .then(r=>r.json())
    .then(j=>{
      const resp = normalizeApiResponse(j);
      if (!resp.ok){ showError(resp.error_message||'获取最近点赞失败'); return; }
      const data = resp.data || {};
      if (data.fp){ LocalState.likes.updateServerFP(data.fp); LocalState.likes.logCompare(); }
      const list = data.items || data.data || [];
      recentLikesLoaded = true;
      recentLikesLoadedCount = list.length;

      let changed = false;
      list.forEach(it=>{
        if (it.model_id != null && it.condition_id != null){
          const k = `${it.model_id}_${it.condition_id}`;
          if (!LocalState.likes.has(k)){
            LocalState.likes.add(k);
            changed = true;
          }
        }
      });
      if (changed){
        list.forEach(it=>{
          if (it.model_id != null && it.condition_id != null){
            updateLikeIcons(it.model_id, it.condition_id, true);
          }
        });
      }
      rebuildRecentLikes(list);
    })
    .catch(err=>showError('获取最近点赞异常: '+err.message))
    .finally(()=>hideLoading('recent-likes'));
}
function loadRecentLikesIfNeeded(){
  if (recentLikesLoaded) return;
  reloadRecentLikes();
}

/* =========================================================
   顶部 / 左 / 右三个 Tab 管理
   ========================================================= */
function activateTab(group, tabName, animate = false) {
  // 右侧主容器启用 snap 后，交由 initSnapTabScrolling 接管
  if (group === 'sidebar-top' || group === 'left-panel' || (group === 'right-panel' && window.__RIGHT_PANEL_SNAP_ON)) return;

  const nav = document.querySelector(`.fc-tabs[data-tab-group="${group}"]`);
  const wrapper = document.getElementById(`${group}-wrapper`);
  if (!nav || !wrapper) return;
  const items = [...nav.querySelectorAll('.fc-tabs__item')];

  if (group === 'right-panel' && !animate) {
    tabName = items[0]?.dataset.tab || tabName;
  }
  let idx = items.findIndex(i => i.dataset.tab === tabName);
  if (idx < 0) { idx = 0; tabName = items[0]?.dataset.tab || ''; }
  items.forEach((it, i) => it.classList.toggle('active', i === idx));

  const percent = idx * 50;
  if (!animate) wrapper.style.transition = 'none';
  wrapper.style.transform = `translateX(-${percent}%)`;
  if (!animate) setTimeout(() => wrapper.style.transition = '', 50);

  if (group !== 'right-panel') {
    localStorage.setItem('activeTab_' + group, tabName);
  }
  if (group === 'right-panel') {
    // 原：updateRightSubseg(tabName)
    // 现：通过 RightPanel 暴露的 API 调用，避免直接依赖内部函数
    window.RightPanel?.updateSubseg?.(tabName);
    if (tabName === 'recent-updates') {
      // 通过 right-panel 内聚 API 触发懒加载
      window.RightPanel?.recentUpdates?.loadIfNeeded?.();
    }
  }
}
document.addEventListener('click',(e)=>{
  const item = safeClosest(e.target, '.fc-tabs .fc-tabs__item');
  if (!item) return;
  const nav = item.closest('.fc-tabs');
  const group = nav?.dataset?.tabGroup;
  if (!group) return;
  if (group === 'right-panel') {
    // 右侧主页签交给 Scroll Snap 初始化里的点击处理
    return;
  }
  activateTab(group, item.dataset.tab, true);
});

// 默认状态初始化：右侧交给 scroll-snap，跳过
(function initTabDefaults(){
  ['left-panel','right-panel'].forEach(group=>{
    if (group === 'right-panel') return; // 右侧跳过，交给 snap 初始化
    const saved = localStorage.getItem('activeTab_'+group);
    const fallback = document.querySelector(`.fc-tabs[data-tab-group="${group}"] .fc-tabs__item`)?.dataset.tab || '';
    activateTab(group, saved || fallback, false);
  });
  const sidebarTopActive = document.querySelector('.fc-tabs[data-tab-group="sidebar-top"] .fc-tabs__item.active')?.dataset.tab;
  if (sidebarTopActive) activateTab('sidebar-top', sidebarTopActive, false);
})();

/* ===== 顶部可视高度同步 ===== */
function computeTopPaneViewportHeight(){
  const scroller = document.querySelector('#top-panel .fc-sidebar-panel__content');
  const nav = scroller ? scroller.querySelector('nav.fc-tabs') : null;
  if (!scroller || !nav) return 0;
  const scrollerStyle = getComputedStyle(scroller);
  const padBottom = parseFloat(scrollerStyle.paddingBottom)||0;
  const navStyle = getComputedStyle(nav);
  const navMB = parseFloat(navStyle.marginBottom)||0;
  const navH = Math.ceil(nav.getBoundingClientRect().height);
  const avail = scroller.clientHeight - navH - navMB - padBottom;
  return Math.max(0, Math.floor(avail));
}
function syncTopTabsViewportHeight(){
  const container = document.querySelector('#top-panel .fc-tab-container');
  if (!container) return;
  const h = computeTopPaneViewportHeight();
  container.style.height = (h>0?h:0)+'px';
}
(function initTopTabsViewport(){
  const scroller = document.querySelector('#top-panel .fc-sidebar-panel__content');
  if (scroller && 'ResizeObserver' in window){
    const ro = new ResizeObserver(()=>requestAnimationFrame(syncTopTabsViewportHeight));
    ro.observe(scroller);
  }
  syncTopTabsViewportHeight();
  window.addEventListener('resize', ()=>requestAnimationFrame(syncTopTabsViewportHeight));
  document.addEventListener('mouseup', ()=>requestAnimationFrame(syncTopTabsViewportHeight));
})();

// 初始化按钮可达性状态（加安全判断，避免加载顺序问题）
try { window.__APP?.sidebar?.refreshToggleUI?.(); } catch(_) {}

/* =========================================================
   主题切换
   ========================================================= */
const themeToggle = $('#themeToggle');
const themeIcon = $('#themeIcon');

// 替换 currentTheme 的初始化为：
let currentTheme = (function(){
  return (window.ThemePref && typeof window.ThemePref.resolve === 'function')
    ? window.ThemePref.resolve()
    : (document.documentElement.getAttribute('data-theme') || 'light');
})();

let THEME_OP_ID = 0;

function setTheme(t) {
  const root = document.documentElement;
  const prev = root.getAttribute('data-theme') || 'light';
  const myId = ++THEME_OP_ID;

  // 每次进入深色都生成全新的渐变
  if (t === 'dark') {
    // 关键修复：清理可能遗留的浅色内联变量，避免覆盖 dark 变量
    root.style.removeProperty('--bg-primary');

    generateDarkGradient();

    // 锁住渐变层为可见，避免切换过程中掉到 0
    root.style.setProperty('--grad-opacity', '1');

    // 下一帧切 data-theme
    requestAnimationFrame(() => {
      root.setAttribute('data-theme', 'dark');
      // 交由 [data-theme=dark] 的 --grad-opacity:1 接管，微任务后清理内联
      setTimeout(() => {
        if (myId !== THEME_OP_ID) return; // 防止旧清理落到新主题
        root.style.removeProperty('--grad-opacity');
      }, 0);
    });
  } else {
    // 进入浅色：避免露底
    root.style.setProperty('--bg-primary', '#f9fafb'); // 先给浅色底
    root.style.setProperty('--grad-opacity', '1');     // 渐变仍可见以便平滑淡出

    // 持有当前渐变，确保淡出过程中不丢失
    const currGrad = (getComputedStyle(root).getPropertyValue('--dark-rand-gradient') || '').trim();
    if (currGrad && currGrad !== 'none') {
      root.style.setProperty('--dark-rand-gradient', currGrad);
    }

    // 下一帧切主题，再下一帧淡出渐变
    requestAnimationFrame(() => {
      root.setAttribute('data-theme', 'light');
      requestAnimationFrame(() => {
        root.style.setProperty('--grad-opacity', '0');
        // 动画结束后清理临时变量，并清空渐变，确保下次进入 dark 一定会生成新渐变
        setTimeout(() => {
          if (myId !== THEME_OP_ID) return; // 防止竞态
            root.style.removeProperty('--grad-opacity');
            root.style.removeProperty('--bg-primary');
            root.style.removeProperty('--dark-rand-gradient'); // 关键：清掉以便下次重新生成
        }, 520); // 略大于 .5s 过渡
      });
    });
  }

  // 同步图标
  if (themeIcon) themeIcon.className = t === 'dark' ? 'fa-solid fa-sun' : 'fa-solid fa-moon';

  // 统一保存（仅本地，不再通知服务端）
  if (window.ThemePref && typeof window.ThemePref.save === 'function') {
    window.ThemePref.save(t, { notifyServer: false });
  }

  // 等两帧再刷新图表/布局（保留原逻辑）
  requestAnimationFrame(() => {
    requestAnimationFrame(() => {
      if (lastChartData) {
        postChartData(lastChartData);
      } else {
        resizeChart();
      }
      syncTopTabsViewportHeight();
    });
  });
}

// 初始化：只调用一次
setTheme(currentTheme);

// 防重复绑定保护
if (!window.__APP_THEME_BOUND__) {
  window.__APP_THEME_BOUND__ = true;
  themeToggle?.addEventListener('click', () => {
    currentTheme = currentTheme === 'light' ? 'dark' : 'light';
    setTheme(currentTheme);
    // 侧栏颜色和图表兜底刷新
    window.applySidebarColors();
    if (lastChartData) {
      postChartData(lastChartData);
    } else {
      resizeChart();
    }
    requestAnimationFrame(syncTopTabsViewportHeight);
  });
}

/* =========================================================
   已选 & 快速按钮状态
   ========================================================= */
let selectedMapSet = new Set();
let selectedKeySet = new Set();

// NEW: 以 model_id + condition_id 为唯一键的索引，用于快捷按钮联动
let selectedPairSet = new Set();
function rebuildSelectedPairIndex(){
  selectedPairSet.clear();
  try {
    const pairs = LocalState.getSelectionPairs();
    (pairs || []).forEach(p => {
      if (p && p.model_id != null && p.condition_id != null) {
        selectedPairSet.add(`${p.model_id}_${p.condition_id}`);
      }
    });
  } catch(_) {}
}

function rebuildSelectedIndex(){
  selectedMapSet.clear();
  selectedKeySet.clear();
  window.__APP.dom.all('#selectedFansList .fan-item').forEach(div=>{
    const key = div.getAttribute('data-fan-key');
    if (key) selectedKeySet.add(key);
    const map = div.getAttribute('data-map');
    if (map) selectedMapSet.add(map);
  });
}
rebuildSelectedIndex();
rebuildSelectedPairIndex();

// 替换 buildQuickBtnHTML：统一为 type="button"
function buildQuickBtnHTML(addType, brand, model, modelId, conditionId, condition, logSource){
  const mapKey = `${escapeHtml(brand)}||${escapeHtml(model)}||${escapeHtml(condition||'')}`;
  const hasIds = (modelId != null && conditionId != null && String(modelId) !== '' && String(conditionId) !== '');
  const isDup = hasIds
    ? selectedPairSet.has(`${String(modelId)}_${String(conditionId)}`)
    : selectedMapSet.has(mapKey);

  const mode = isDup ? 'remove' : 'add';
  const tipText = isDup ? '从图表移除' : '添加到图表';
  const icon = '<i class="fa-solid fa-plus"></i>';
  const defaultSourceMap = { likes:'liked', rating:'top_rating', ranking:'top_query', search:'search' };
  const sourceAttr = logSource || defaultSourceMap[addType] || 'unknown';

  let cls;
  if (isDup) cls = 'js-list-remove';
  else if (addType==='search') cls='js-search-add';
  else if (addType==='rating') cls='js-rating-add';
  else if (addType==='ranking') cls='js-ranking-add';
  else cls='js-likes-add';

  return `
    <button type="button" class="fc-btn-icon-add ${cls} fc-tooltip-target"
            data-tooltip="${tipText}"
            data-mode="${mode}"
            data-add-type="${addType}"
            data-log-source="${escapeHtml(sourceAttr)}"
            data-brand="${escapeHtml(brand)}"
            data-model="${escapeHtml(model)}"
            data-condition="${escapeHtml(condition||'')}"
            ${modelId ? `data-model-id="${escapeHtml(modelId)}"`:''}
            ${conditionId ? `data-condition-id="${escapeHtml(conditionId)}"`:''}>
      ${icon}
    </button>`;
}
// Export to window for cross-module access (required for webpack bundling)
window.buildQuickBtnHTML = buildQuickBtnHTML;


// CHANGED: 状态切换不再改成 X；确保始终是 “+”，旋转交给 CSS
function toRemoveState(btn){
  btn.dataset.mode='remove';
  btn.classList.remove('js-ranking-add','js-rating-add','js-search-add','js-likes-add');
  btn.classList.add('js-list-remove');
  btn.setAttribute('data-tooltip','从图表移除');
  btn.removeAttribute('title');
  // 统一图标为 “+” ，旋转由 [data-mode="remove"] 控制
  if (!btn.querySelector('i')) {
    btn.innerHTML = '<i class="fa-solid fa-plus"></i>';
  } else {
    const ic = btn.querySelector('i');
    ic.className = 'fa-solid fa-plus';
  }
}

function toAddState(btn){
  const addType = btn.dataset.addType || (btn.classList.contains('js-rating-add')?'rating'
    : btn.classList.contains('js-ranking-add')?'ranking'
      : btn.classList.contains('js-search-add')?'search':'likes');
  btn.dataset.mode='add';
  btn.classList.remove('js-list-remove','js-ranking-add','js-rating-add','js-search-add','js-likes-add');
  btn.classList.add(addType==='rating'?'js-rating-add'
    : addType==='ranking'?'js-ranking-add'
      : addType==='search'?'js-search-add':'js-likes-add');
  btn.setAttribute('data-tooltip','添加到图表');
  btn.removeAttribute('title');
  // 统一图标为 “+”
  if (!btn.querySelector('i')) {
    btn.innerHTML = '<i class="fa-solid fa-plus"></i>';
  } else {
    const ic = btn.querySelector('i');
    ic.className = 'fa-solid fa-plus';
  }
}

function mapKeyFromDataset(d){
  const b = unescapeHtml(d.brand||'');
  const m = unescapeHtml(d.model||'');
  const c = unescapeHtml(d.condition||'');
  return `${b}||${m}||${c}`;
}
// CHANGED: 状态同步优先使用 (model_id, condition_id) 判断
function syncQuickActionButtons(){
  window.__APP.dom.all('.fc-btn-icon-add.fc-tooltip-target').forEach(btn=>{
    if (!btn.dataset.addType){
      if (btn.classList.contains('js-rating-add')) btn.dataset.addType='rating';
      else if (btn.classList.contains('js-ranking-add')) btn.dataset.addType='ranking';
      else if (btn.classList.contains('js-search-add')) btn.dataset.addType='search';
      else if (btn.classList.contains('js-likes-add')) btn.dataset.addType='likes';
    }
    const d = btn.dataset;
    let dup = false;
    if (d.modelId && d.conditionId) {
      dup = selectedPairSet.has(`${d.modelId}_${d.conditionId}`);
    } else {
      const key = mapKeyFromDataset(d);
      dup = selectedMapSet.has(key);
    }
    if (dup) toRemoveState(btn); else toAddState(btn);
  });
}

/* =========================================================
   Rebuild 选中 / 移除列表
   ========================================================= */
const selectedListEl = $('#selectedFansList');
const removedListEl  = $('#recentlyRemovedList');
const selectedCountEl = $('#selectedCount');
const clearAllContainer = $('#clearAllContainer');
const clearAllBtn = $('#clearAllBtn');

// 替换 rebuildSelectedFans：两个按钮都加 type="button"
function rebuildSelectedFans(fans){
  if (!Array.isArray(fans)) fans = LocalState.getSelected();
  selectedListEl.innerHTML='';
  ColorManager.assignUniqueIndices((fans || []).map(f => f.key));
  if (!fans || fans.length===0){
    selectedCountEl.textContent='0';
    clearAllContainer?.classList.add('hidden');
    rebuildSelectedIndex(); rebuildSelectedPairIndex();
    requestAnimationFrame(prepareSidebarMarquee);
    scheduleAdjust(); syncQuickActionButtons && syncQuickActionButtons();
    return;
  }
  fans.forEach(f=>{
    const keyStr = `${f.model_id}_${f.condition_id}`;
    const info = DisplayCache.get(f.model_id, f.condition_id);
    const brand = info?.brand || '';
    const model = info?.model || '';
    const condName  = info?.condition || '加载中...';
    const scenExtra = (typeof formatScenario === 'function') ? formatScenario(info?.rt, info?.rl) : '';
    const condText  = scenExtra ? `${condName} - ${scenExtra}` : condName;

    const isLiked = LocalState.likes.has(keyStr);
    const div = document.createElement('div');
    div.className='fan-item flex items-center justify-between p-3 border border-gray-200 rounded-md';
    div.dataset.fanKey = f.key;
    div.dataset.map = `${brand}||${model}||${condText}`;
    div.innerHTML=`
      <div class="flex items-center min-w-0">
        <div class="w-3 h-3 rounded-full mr-2 flex-shrink-0 js-color-dot"></div>
        <div class="truncate">
          <span class="font-medium">${escapeHtml(brand)} ${escapeHtml(model)}</span> - 
          <span class="text-gray-600 text-sm">${escapeHtml(condText)}</span>
        </div>
      </div>
      <div class="flex items-center flex-shrink-0">
        <button type="button" class="like-button mr-3" data-fan-key="${f.key}" data-model-id="${f.model_id}" data-condition-id="${f.condition_id}">
          <i class="fa-solid fa-thumbs-up ${isLiked?'text-red-500':'text-gray-400'}"></i>
        </button>
        <button type="button" class="fc-icon-remove text-lg js-remove-fan" data-fan-key="${f.key}" data-tooltip="移除">
          <i class="fa-solid fa-xmark"></i>
        </button>
      </div>`;
    selectedListEl.appendChild(div);
    const dot = div.querySelector('.js-color-dot'); if (dot) dot.style.backgroundColor = ColorManager.getColor(f.key);
  });
  selectedCountEl.textContent = fans.length.toString();
  clearAllContainer?.classList.remove('hidden');
  rebuildSelectedIndex(); rebuildSelectedPairIndex();
  requestAnimationFrame(prepareSidebarMarquee);
  scheduleAdjust(); syncQuickActionButtons && syncQuickActionButtons();
}

/* =========================================================
   统一状态处理
   ========================================================= */
let pendingShareMeta = null;
let __isShareLoaded = (function(){
  try {
    const usp = new URLSearchParams(window.location.search);
    return usp.get('share_loaded') === '1';
  } catch(_) { return false; }
})();
let __shareAxisApplied = false;

function processState(data, successMsg){
  const prevSelectedKeys = new Set(selectedKeySet);
  if (data.error_message){
    hideLoading('op'); showError(data.error_message);
  } else {
    if (successMsg) showSuccess(successMsg);
    hideLoading('op'); autoCloseOpLoading();
  }
  let pendingChart = null;
  if ('chart_data' in data) pendingChart = data.chart_data;
  if ('share_meta' in data && data.share_meta){
     ColorManager.patchIndicesFromServer(data.share_meta);
  }
  if ('like_keys' in data){
    LocalState.likes.setAll(data.like_keys || []);
  }
  if (data.fp){ LocalState.likes.updateServerFP(data.fp); LocalState.likes.logCompare(); }

  if ('selected_fans' in data){    
    // 使用新的 ColorManager 接口
    ColorManager.assignUniqueIndices((data.selected_fans || []).map(f => f.key));
    rebuildSelectedFans(data.selected_fans);
  }
  if ('recently_removed_fans' in data){
    window.__APP.features.recentlyRemoved.rebuild(data.recently_removed_fans);
  }
  if ('share_meta' in data && data.share_meta){
    pendingShareMeta = {
      show_raw_curves: data.share_meta.show_raw_curves,
      show_fit_curves: data.share_meta.show_fit_curves,
      pointer_x_rpm: data.share_meta.pointer_x_rpm,
      pointer_x_noise_db: data.share_meta.pointer_x_noise_db,
      legend_hidden_keys: data.share_meta.legend_hidden_keys
    };
    if (__isShareLoaded && !__shareAxisApplied && data.chart_data && data.chart_data.x_axis_type){
      frontXAxisType = (data.chart_data.x_axis_type === 'noise') ? 'noise_db' : data.chart_data.x_axis_type;
      try { localStorage.setItem('x_axis_type', frontXAxisType); } catch(_){}
      __shareAxisApplied = true;
    }
  }
  if (pendingChart) postChartData(pendingChart);
  syncQuickActionButtons();
  scheduleAdjust();
}

/* =========================================================
   POST 助手
   ========================================================= */
async function apiPost(url, payload){
  const resp = await fetch(url, {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify(payload||{})
  });
  if (!resp.ok) throw new Error('HTTP '+resp.status);
  return resp.json();
}

/* =========================================================
   搜索（移除跑马灯）
   ========================================================= */
const searchAirflowTbody = $('#searchAirflowTbody');
const searchLikesTbody = $('#searchLikesTbody');
let SEARCH_RESULTS_RAW = [];

// CHANGED: 搜索结果渲染，使用新签名
function fillSearchTable(tbody, list){
  if (!tbody) return;
  Array.from(tbody.querySelectorAll('.fc-marquee-cell')).forEach(td=>{
    td.classList.remove('fc-marquee-cell','nowrap');
    const inner = td.querySelector('.fc-marquee-inner');
    if (inner) td.innerHTML = inner.innerHTML;
  });
  if (!list.length){
    tbody.innerHTML='<tr><td colspan="10" class="text-center text-gray-500 py-6">没有符合条件的结果</td></tr>';
    return;
  }

  const logSource =
    (tbody.id === 'searchAirflowTbody') ? 'search_airflow' :
    (tbody.id === 'searchLikesTbody')   ? 'search_rating'  :
                                          'search';

  tbody.innerHTML = list.map(r=>{
    const brand = r.brand_name_zh;
    const model = r.model_name;
    const scenLabel = escapeHtml(r.condition_name_zh || '');
    const priceText = (r.reference_price > 0) ? escapeHtml(String(r.reference_price)) : '-';

    const axis = (r.effective_axis === 'noise') ? 'noise_db' : (r.effective_axis || 'rpm');
    const unit = axis === 'noise_db' ? 'dB' : 'RPM';
    const xVal = Number(r.effective_x);
    const xText = axis === 'noise_db' ? xVal.toFixed(1) : Math.round(xVal).toString();
    const srcText = (r.effective_source === 'fit') ? '拟合' : '原始';
    const xCell = `${xText} ${unit} (${srcText})`;

    const airflow = Number(r.effective_airflow ?? r.max_airflow ?? 0);
    const airflowText = airflow.toFixed(1);

    return `
      <tr class="hover:bg-gray-50">
        <td class="nowrap">${escapeHtml(brand)}</td>
        <td class="nowrap">${escapeHtml(model)}</td>
        <td class="nowrap">${escapeHtml(r.size)}x${escapeHtml(r.thickness)}</td>
        <td class="nowrap">${escapeHtml(r.rgb_light || '—')}</td>
        <td class="nowrap">${priceText}</td>
        <td class="nowrap">${scenLabel}</td>
        <td class="nowrap">${xCell}</td>
        <td class="text-blue-600 font-medium text-sm">${airflowText}</td>
        <td class="text-blue-600 font-medium">${r.like_count ?? 0}</td>
        <td>${buildQuickBtnHTML('search', brand, model, r.model_id, r.condition_id, r.condition_name_zh, logSource)}</td>
      </tr>`;
  }).join('');
}

function renderSearchResults(results, conditionLabel){
  SEARCH_RESULTS_RAW = results.slice();
  const byAirflow = SEARCH_RESULTS_RAW;
  const byLikes = SEARCH_RESULTS_RAW.slice().sort((a,b)=>(b.like_count||0)-(a.like_count||0));

  // 根据结果集设置“转速/噪音”表头（两个表同名）
  let axisLabel = '转速';
  if (results && results.length) {
    const ax = results[0]?.effective_axis;
    axisLabel = (ax === 'noise' || ax === 'noise_db') ? '噪音' : '转速';
  }
  const h1 = document.getElementById('searchXHeaderAir');
  const h2 = document.getElementById('searchXHeaderLikes');
  if (h1) h1.textContent = axisLabel;
  if (h2) h2.textContent = axisLabel;

  const labelEl = document.getElementById('searchConditionLabel');
  if (labelEl) labelEl.textContent = conditionLabel;

  fillSearchTable(searchAirflowTbody, byAirflow);
  fillSearchTable(searchLikesTbody, byLikes);
  syncQuickActionButtons();
}

/* =========================================================
   点赞 / 快速按钮 / 恢复 / 清空
   ========================================================= */
function updateLikeIcons(modelId, conditionId, isLiked){
  window.__APP.dom.all(`.like-button[data-model-id="${modelId}"][data-condition-id="${conditionId}"]`)
    .forEach(btn => {
      const ic = btn.querySelector('i');
      if (!ic) return;
      ic.classList.toggle('text-red-500', isLiked);
      ic.classList.toggle('text-gray-400', !isLiked);
    });
}
const RECENT_LIKES_REFRESH_DELAY = 650;
const TOP_RATINGS_REFRESH_DELAY = 800;
const scheduleRecentLikesRefresh = (function(){
  const debounced = createDelayed(()=>{ if (recentLikesLoaded) reloadRecentLikes(); }, RECENT_LIKES_REFRESH_DELAY);
  return function(){ debounced(); };
})();

document.addEventListener('click', async e=>{
  const likeBtn = safeClosest(e.target, '.like-button');
  if (likeBtn){
    if (needThrottle('like') && !globalThrottle()) return;
    const modelId = likeBtn.dataset.modelId;
    const conditionId = likeBtn.dataset.conditionId;
    if (!modelId || !conditionId) { showError('缺少点赞标识'); return; }
    const icon = likeBtn.querySelector('i');
    const prevLiked = icon.classList.contains('text-red-500');
    const nextLiked = !prevLiked;
    const url = prevLiked ? '/api/unlike' : '/api/like';
    const keyStr = `${modelId}_${conditionId}`;
    updateLikeIcons(modelId, conditionId, nextLiked);
    if (nextLiked) LocalState.likes.add(keyStr); else LocalState.likes.remove(keyStr);
    fetch(url, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ model_id: modelId, condition_id: conditionId })
    })
      .then(r=>r.json())
      .then(j=>{
        const resp = normalizeApiResponse(j);
        if (!resp.ok){
          updateLikeIcons(modelId, conditionId, prevLiked);
          if (prevLiked) LocalState.likes.add(keyStr); else LocalState.likes.remove(keyStr);
          showError(resp.error_message || '操作失败');
          return;
        }
        const data = resp.data || {};
        if (data.fp){
          LocalState.likes.updateServerFP(data.fp);
          LocalState.likes.logCompare();
        }
        const finalLiked = LocalState.likes.has(keyStr);
        updateLikeIcons(modelId, conditionId, finalLiked);
        if (data.fp && ( (!LocalState.likes.isSynced()) || !LocalState.likes.shouldSkipStatus() ) && data.fp.c < LIKE_FULL_FETCH_THRESHOLD){
          fetchAllLikeKeys();
        }
        scheduleRecentLikesRefresh();
        showSuccess(prevLiked ? '已取消点赞' : '已点赞');
      })
      .catch(err=>{
        updateLikeIcons(modelId, conditionId, prevLiked);
        if (prevLiked) LocalState.likes.add(keyStr); else LocalState.likes.remove(keyStr);
        showError('网络错误：'+err.message);
      });
    return;
  }

  const quickRemove = safeClosest(e.target, '.js-list-remove');
if (quickRemove){
    const midAttr = quickRemove.dataset.modelId;
    const cidAttr = quickRemove.dataset.conditionId;
    const sel = LocalState.getSelected();
    let target = null;

    if (midAttr && cidAttr) {
      target = sel.find(it => String(it.model_id) === String(midAttr) && String(it.condition_id) === String(cidAttr));
    }

    if (!target){
      showInfo('该数据已不在图表中');
      syncQuickActionButtons();
      return;
    }
    const ok = LocalState.removeKey(target.key);
    if (ok){
      showSuccess('已移除');
      rebuildSelectedFans(LocalState.getSelected());
      window.__APP.features.recentlyRemoved.rebuild(LocalState.getRecentlyRemoved());
      syncQuickActionButtons();
      refreshChartFromLocal(false);
    } else {
      showError('移除失败（未找到）');
    }
    return;
  }

 {
    const picker = ['.js-ranking-add','.js-search-add','.js-rating-add','.js-likes-add'];
    for (const sel of picker){
      const btn = safeClosest(e.target, sel);
      if (!btn) continue;
      const midAttr = btn.dataset.modelId;
      const cidAttr = btn.dataset.conditionId;
      if (!(midAttr && cidAttr)){
        showError('缺少标识：按钮未包含 model_id / condition_id');
        return;
      }
      showLoading('op','添加中...');
      try {
        const pairs = [{
          model_id: Number(midAttr),
          condition_id: Number(cidAttr),
          brand: btn.dataset.brand ? unescapeHtml(btn.dataset.brand) : '',
          model: btn.dataset.model ? unescapeHtml(btn.dataset.model) : '',
          condition: btn.dataset.condition ? unescapeHtml(btn.dataset.condition) : ''
        }];
        const newPairs = computeNewPairsAfterDedup(pairs);
        if (newPairs.length === 0){
          hideLoading('op'); showInfo('已存在'); return;
        }
        if (!ensureCanAdd(newPairs.length)){
          hideLoading('op'); return;
        }

        // 立即更新前端状态与 UI
        const addedSummary = LocalState.addPairs(pairs);
        rebuildSelectedFans(LocalState.getSelected());
        ensureLikeStatusBatch(addedSummary.addedDetails.map(d => ({ model_id: d.model_id, condition_id: d.condition_id })));
        window.__APP.features.recentlyRemoved.rebuild(LocalState.getRecentlyRemoved());
        syncQuickActionButtons();
        applySidebarColors();
        refreshChartFromLocal(false);

        hideLoading('op');
        showSuccess(`新增 ${addedSummary.added} 组`);
        window.__APP.sidebar.maybeAutoOpenSidebarOnAdd && window.__APP.sidebar.maybeAutoOpenSidebarOnAdd();

        // 埋点改为后台、无阻塞
        const addType = btn.dataset.addType || '';
        const fallbackMap = { likes:'liked', rating:'top_rating', ranking:'top_query', search:'search' };
        const logSource = btn.dataset.logSource || fallbackMap[addType] || 'unknown';
        Promise.resolve(logNewPairs(addedSummary.addedDetails, logSource)).catch(()=>{});
      } catch(err){
        hideLoading('op');
        showError('添加失败: '+err.message);
      }
      return;
    }
  }


  const removeBtn = safeClosest(e.target, '.js-remove-fan');
  if (removeBtn){
    const fanKey = removeBtn.dataset.fanKey;
    if (!fanKey){ showError('缺少 fan_key'); return; }
    const ok = LocalState.removeKey(fanKey);
    if (ok){
      showSuccess('已移除');
      rebuildSelectedFans(LocalState.getSelected());
      window.__APP.features.recentlyRemoved.rebuild(LocalState.getRecentlyRemoved());
      syncQuickActionButtons();
      refreshChartFromLocal(false);
    } else {
      showInfo('条目不存在');
    }
    return;
  }

  // 替换清空确认的按钮模板：加 type="button"
  if (e.target.id === 'clearAllBtn'){
    const state = e.target.getAttribute('data-state') || 'normal';
    if (state === 'normal'){
      clearAllBtn.setAttribute('data-state','confirming');
      clearAllBtn.innerHTML = `
        <div class="fc-clear-confirm">
          <button type="button" id="confirmClearAll" class="bg-red-600 text-white hover:bg-red-700">确认</button>
          <button type="button" id="cancelClearAll" class="bg-gray-400 text-white hover:bg-gray-500">取消</button>
        </div>`;
      scheduleAdjust();
    }
    return;
  }

  if (e.target.id === 'cancelClearAll'){
    clearAllBtn.setAttribute('data-state','normal');
    clearAllBtn.textContent='移除所有';
    scheduleAdjust();
    return;
  }
  if (e.target.id === 'confirmClearAll'){
    showLoading('op','清空中...');
    try {
      LocalState.clearAll();
      hideLoading('op');
      showSuccess('已全部移除');
      rebuildSelectedFans(LocalState.getSelected());
      window.__APP.features.recentlyRemoved.rebuild(LocalState.getRecentlyRemoved());
      syncQuickActionButtons();
      applySidebarColors();
      refreshChartFromLocal(false);
    } catch(err){
      hideLoading('op');
      showError('清空失败: '+err.message);
    } finally {
      clearAllBtn.setAttribute('data-state','normal');
      clearAllBtn.textContent='移除所有';
    }
    return;
  }
});

/* =========================================================
   选中数量与上限判断
   ========================================================= */
function ensureCanAdd(plannedNewCount = 1){
  if (!FRONT_MAX_ITEMS) return true;
  const curr = LocalState.getSelected().length;
  if (curr + plannedNewCount > FRONT_MAX_ITEMS){
    showInfo(`将超出最大上限（${FRONT_MAX_ITEMS}），请先移除部分曲线`);
    return false;
  }
  return true;
}
function computeNewPairsAfterDedup(pairs){
  const existing = new Set(LocalState.getSelectionPairs().map(p => `${p.model_id}_${p.condition_id}`));
  const uniq = [];
  const seen = new Set();
  pairs.forEach(p=>{
    const k = `${p.model_id}_${p.condition_id}`;
    if (seen.has(k)) return;
    seen.add(k);
    if (!existing.has(k)) uniq.push(p);
  });
  return uniq;
}

/* =========================================================
   侧栏 跑马灯
   ========================================================= */
/* 侧栏行跑马灯（修复类名统一） */
function prepareSidebarMarquee(){
  window.__APP.dom.all('#sidebar .fan-item .truncate').forEach(container=>{
    if (container.querySelector('.fc-sidebar-marquee-inner')) return;
    const inner = document.createElement('span');
    inner.className='fc-sidebar-marquee-inner';
    inner.innerHTML = container.innerHTML;
    container.innerHTML='';
    container.appendChild(inner);
  });
}
prepareSidebarMarquee();
const SIDEBAR_SCROLL_SPEED=60;

function startSingleMarquee(row, containerSel, innerSel, speed){
  const container = row.querySelector(containerSel);
  const inner = row.querySelector(innerSel);
  if (!container || !inner) return;
  const delta = inner.scrollWidth - container.clientWidth;
  if (delta > 6){
    const duration = (delta / speed).toFixed(2);
    inner.style.transition = `transform ${duration}s linear`;
    inner.style.transform = `translateX(-${delta}px)`;
  }
}
function stopSingleMarquee(row, innerSel){
  const inner = row.querySelector(innerSel);
  if (!inner) return;
  inner.style.transition='transform .35s ease';
  inner.style.transform='translateX(0)';
}
function startSidebarMarquee(row){ startSingleMarquee(row, '.truncate', '.fc-sidebar-marquee-inner', SIDEBAR_SCROLL_SPEED); }
function stopSidebarMarquee(row){ stopSingleMarquee(row, '.fc-sidebar-marquee-inner'); }
document.addEventListener('mouseenter',(e)=>{
  const row = safeClosest(e.target, '#sidebar .fan-item');
  if (!row) return;
  startSidebarMarquee(row);
}, true);
document.addEventListener('mouseleave',(e)=>{
  const row = safeClosest(e.target, '#sidebar .fan-item');
  if (!row) return;
  stopSidebarMarquee(row);
}, true);

/* =========================================================
   Export core functions to window for cross-module access
   (Required for webpack bundling where modules are scoped)
   ========================================================= */
window.syncQuickActionButtons = syncQuickActionButtons;
window.rebuildSelectedFans = rebuildSelectedFans;
window.renderSearchResults = renderSearchResults;
window.ensureCanAdd = ensureCanAdd;
window.computeNewPairsAfterDedup = computeNewPairsAfterDedup;
window.prepareSidebarMarquee = prepareSidebarMarquee;
window.ensureLikeStatusBatch = ensureLikeStatusBatch;
window.refreshChartFromLocal = refreshChartFromLocal;
window.logNewPairs = logNewPairs;
window.syncTopTabsViewportHeight = syncTopTabsViewportHeight;
window.initSnapTabScrolling = initSnapTabScrolling;

/* 最近点赞工况行跑马灯 */
function prepareRecentLikesMarquee(){
  document.querySelectorAll('#recentLikesList .scenario-row .scenario-text').forEach(container=>{
    if (container.querySelector('.fc-recent-marquee-inner')) return;
    const inner = document.createElement('span');
    inner.className = 'fc-recent-marquee-inner';
    inner.textContent = container.textContent;
    container.textContent = '';
    container.appendChild(inner);
  });
}
const RECENT_LIKES_SCROLL_SPEED = 60;
function startRecentLikesMarquee(row){ startSingleMarquee(row, '.scenario-text', '.fc-recent-marquee-inner', RECENT_LIKES_SCROLL_SPEED); }
function stopRecentLikesMarquee(row){ stopSingleMarquee(row, '.fc-recent-marquee-inner'); }
document.addEventListener('mouseenter', (e)=>{
  const row = safeClosest(e.target, '#recentLikesList .scenario-row');
  if (!row) return;
  startRecentLikesMarquee(row);
}, true);
document.addEventListener('mouseleave', (e)=>{
  const row = safeClosest(e.target, '#recentLikesList .scenario-row');
  if (!row) return;
  stopRecentLikesMarquee(row);
}, true);


/* =========================================================
   scheduleAdjust
   ========================================================= */
let _adjustQueued = false;
function scheduleAdjust(){
  // 始终读取全局标记（由 sidebar.js 维护），不要用本地快照变量
  if (window.__VERT_DRAGGING) return;
  if (window.__SIDEBAR_USER_ADJUSTED_VERTICAL) return;
  if (_adjustQueued) return;
  _adjustQueued = true;
  requestAnimationFrame(()=>{
    _adjustQueued = false;
    window.__APP?.sidebar?.adjustBottomPanelAuto?.();
  });
}

/* =========================================================
   初始数据获取
   ========================================================= */
(function mountChartRendererEarly(){
  function doMount(){
    const el = document.getElementById('chartRoot');
    if (el && window.ChartRenderer && typeof ChartRenderer.mount === 'function') {
      ChartRenderer.mount(el);

      if (typeof ChartRenderer.setOnXAxisChange === 'function') {
        ChartRenderer.setOnXAxisChange((next) => {
          const nx = (next === 'noise') ? 'noise_db' : next;
          try { localStorage.setItem('x_axis_type', nx); } catch(_) {}
          frontXAxisType = nx;
          if (typeof LocalState?.setXAxisType === 'function') {
            try { LocalState.setXAxisType(nx); } catch(_) {}
          }
          // 不重新请求，只重新渲染（使用旧的 canonicalSeries）
          if (lastChartData) {
            postChartData(lastChartData);  // 因为我们不再过滤，这相当于立即切轴
          }
        });
      }
    }
  }
  if (document.readyState !== 'loading') {
    doMount();
  } else {
    document.addEventListener('DOMContentLoaded', doMount, { once:true });
  }
})();

// NEW: 批量获取 (model_id, condition_id) 的显示元信息
async function fetchMetaForPairs(pairs){
  if (!Array.isArray(pairs) || !pairs.length) return [];
  const resp = await fetch('/api/meta_by_ids', {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ pairs })
  });
  const j = await resp.json();
  const n = normalizeApiResponse(j);
  if (!n.ok) return [];
  const items = (n.data && n.data.items) || [];
  return Array.isArray(items) ? items : [];
}

// CHANGED: “最近移除”显示缓存改用 /api/meta_by_ids（不再调用 /api/curves）
async function ensureRemovedDisplayCache(){
  try{
    const removed = (LocalState && LocalState.getRecentlyRemoved && LocalState.getRecentlyRemoved()) || [];
    if (!Array.isArray(removed) || !removed.length) return;

    const selectedPairs = new Set(
      (LocalState.getSelectionPairs?.() || []).map(p=> `${p.model_id}_${p.condition_id}`)
    );
    const need = [];
    const seen = new Set();
    for (const it of removed){
      if (!it) continue;
      const k = `${it.model_id}_${it.condition_id}`;
      if (selectedPairs.has(k)) continue;
      if (DisplayCache.get && DisplayCache.get(it.model_id, it.condition_id)) continue;
      if (seen.has(k)) continue;
      seen.add(k);
      const mid = Number(it.model_id), cid = Number(it.condition_id);
      if (Number.isInteger(mid) && Number.isInteger(cid)){
        need.push({ model_id: mid, condition_id: cid });
      }
    }
    if (!need.length) return;

    const items = await fetchMetaForPairs(need);
    if (items.length){
      DisplayCache.setFromMeta(items);
      window.__APP?.features?.recentlyRemoved?.rebuild?.(LocalState.getRecentlyRemoved());
    }
  }catch(_){}
}

(function initLocalSelectionBoot(){
  installRemovedRenderHookOnce();
  rebuildSelectedFans(LocalState.getSelected());
  primeSelectedLikeStatus();
  window.__APP.features.recentlyRemoved.rebuild(LocalState.getRecentlyRemoved());
  ensureRemovedDisplayCache(); // 关键补齐
})();

  applySidebarColors();
  refreshChartFromLocal(false); // 已选曲线依然用 /api/curves 渲染图表
  syncQuickActionButtons && syncQuickActionButtons();
  prepareSidebarMarquee();

async function primeSelectedLikeStatus(){
  try {
    const pairs = LocalState.getSelectionPairs();
    if (!pairs.length) return;

    let didFullFetch = false;
    if (needFullLikeKeyFetch()) {
      fetchAllLikeKeys();
      didFullFetch = true;
      return; // 全量后无需增量
    }
    if (LocalState.likes.shouldSkipStatus(LIKESET_VERIFY_MAX_AGE_MS)) {
      return;
    }
    const need = pairs.filter(p => !LocalState.likes.has(`${p.model_id}_${p.condition_id}`));
    if (!need.length && LocalState.likes.isSynced()) {
      return;
    }
    const resp = await fetch('/api/like_status', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ pairs: need })
    });
    if (!resp.ok) return;
    const j = await resp.json();
    const n = normalizeApiResponse(j);
    if (!n.ok) return;
    const data = n.data || {};
    if (data.fp){
       LocalState.likes.updateServerFP(data.fp); 
       LocalState.likes.logCompare(); 
      }
    const list = extractLikeKeys(data);
    if (!Array.isArray(list) || !list.length) return;
    list.forEach(k=>{
      if (!LocalState.likes.has(k)){
        LocalState.likes.add(k);
        const [m,c] = k.split('_');
        if (m && c) updateLikeIcons(m, c, true);
      }
    });

    // 二次判定：增量后若仍不同步且属于小集合 → 全量补齐
    const fp = LocalState.likes.getServerFP && LocalState.likes.getServerFP();
    if (!didFullFetch &&
        fp && typeof fp.c === 'number' &&
        fp.c < LIKE_FULL_FETCH_THRESHOLD &&
        !LocalState.likes.isSynced()) {
      fetchAllLikeKeys();
    }
  } catch(_){}

}

/* 分享模式自动滚动 */
(function autoScrollToChartOnShare(){
  try {
    const usp = new URLSearchParams(window.location.search);
    if (usp.get('share_loaded') === '1') {
      window.addEventListener('load', () => {
        setTimeout(() => {
          const el = document.getElementById('chart-settings');
          if (el) {
            el.scrollIntoView({ behavior:'smooth', block:'center' });
          }
        }, 120);
      });
    }
  } catch(_) {}
})();

/* 列表底部 padding 初始化 */
function scheduleInitListPadding(){
  const list = document.querySelector('#selectedFansList');
  if (list) list.style.paddingBottom='var(--content-bottom-gap)';
}
scheduleInitListPadding();

/* 图表窗口 Resize */
window.addEventListener('resize', ()=> {
  const collapsed = document.getElementById('sidebar')?.classList.contains('collapsed');
  if (!collapsed) resizeChart();
});

/* 顶部 Scroll Snap 初始化 */
initSnapTabScrolling({
  containerId: 'sidebar-top-container',
  group: 'sidebar-top',
  persistKey: null,
  onActiveChange: (tab)=> {
    if (tab === 'recent-liked') loadRecentLikesIfNeeded();
  }
});
initSnapTabScrolling({
  containerId: 'left-panel-container',
  group: 'left-panel',
  persistKey: 'activeTab_left-panel'
});

/* 模块注册 */
window.__APP.modules = {
  overlay: {
    open: window.overlayOpenSidebar,
    close: window.overlayCloseSidebar,
    toggle: window.overlayToggleSidebar
  },
  gesture: {
    ensureZone: window.ensureGestureZone || function(){}
  },
  layout: {
    scheduleAdjust,
    adjustBottomAuto: window.__APP.sidebar.adjustBottomPanelAuto
  },
  search: {
    render: typeof renderSearchResults === 'function' ? renderSearchResults : function(){},
    cache: window.__APP.cache
  },
  rankings: {
    reloadTopRatings: () => window.RightPanel?.ratings?.reloadTopRatings?.(false),
    loadLikesIfNeeded: () => window.RightPanel?.ratings?.loadLikesIfNeeded?.()
  },
  state: {
    processState: typeof processState === 'function' ? processState : function(){}
  },
  theme: {
    setTheme: typeof setTheme === 'function' ? setTheme : function(){}
  },
  chart: {
    postChartData: typeof postChartData === 'function' ? postChartData : function(){},
    resizeChart: typeof resizeChart === 'function' ? resizeChart : function(){}
  }
};

window.__APP.features?.recentlyRemoved?.mount?.();
installRemovedRenderHookOnce();

/* 滚动条宽度测量 */
(function setRealScrollbarWidth(){
  function measure(){
    try{
      const box = document.createElement('div');
      box.style.cssText = 'position:absolute;top:-9999px;left:-9999px;width:120px;height:120px;overflow:scroll;visibility:hidden;';
      document.body.appendChild(box);
      const sbw = Math.max(0, box.offsetWidth - box.clientWidth) || 0;
      document.documentElement.style.setProperty('--sbw', sbw + 'px');
      document.documentElement.classList.toggle('overlay-scrollbars', sbw === 0);
      box.remove();
    }catch(e){}
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', measure, { once:true });
  } else {
    measure();
  }
  const remeasure = () => setTimeout(measure, 60);
  window.addEventListener('orientationchange', remeasure);
  window.addEventListener('resize', remeasure);
})();

/* 限制条件输入锁定 */
(function initSortValueUnlockMinimal() {
  const select = document.getElementById('sortBySelect');
  const input  = document.getElementById('sortValueInput');
  if (!select || !input) return;
  function apply() {
    const none = (select.value === 'none');
    input.disabled = none;
    if (none) input.value = '';
  }
  select.addEventListener('change', apply);
  apply();
})();

/* 查询次数显示 */
function loadQueryCount() {
  fetch('/api/query_count')
    .then(r => r.json())
    .then(j => {
      // 兼容结构：新 = j.data.count；旧（如果后端改回裸 count）= j.count
      const count = (j && typeof j === 'object')
        ? (j.data && typeof j.data === 'object' && typeof j.data.count !== 'undefined'
            ? j.data.count
            : (typeof j.count !== 'undefined' ? j.count : 0))
        : 0;
      const el = document.getElementById('query-count');
      if (el) el.textContent = count;
    })
    .catch(err => {
      console.warn('获取查询次数失败:', err);
    });
}
document.addEventListener('DOMContentLoaded', () => {
  loadQueryCount();
  setInterval(loadQueryCount, 60000);
});

/* 最近点赞标题渐隐宽度测量 */
function applyRecentLikesTitleMask() {
  const groups = document.querySelectorAll('#recentLikesList .recent-like-group');
  groups.forEach(g => {
    const titleWrap = g.querySelector('.fc-group-header .fc-title-wrap');
    const titleBox  = titleWrap?.querySelector('.truncate');
    if (!titleWrap || !titleBox) return;
    const w = Math.max(0, Math.ceil(titleBox.getBoundingClientRect().width));
    titleWrap.style.setProperty('--title-w', w + 'px');
  });
}
if (typeof window.rebuildRecentLikes === 'function' && !window.__RECENT_TITLE_MASK_PATCHED__) {
  window.__RECENT_TITLE_MASK_PATCHED__ = true;
  const _orig = window.rebuildRecentLikes;
  window.rebuildRecentLikes = function(list){
    _orig(list);
    requestAnimationFrame(applyRecentLikesTitleMask);
  };
}
let __titleMaskRaf = null;
window.addEventListener('resize', () => {
  if (__titleMaskRaf) cancelAnimationFrame(__titleMaskRaf);
  __titleMaskRaf = requestAnimationFrame(applyRecentLikesTitleMask);
});

/* visit_start */
(function initVisitStartMinimal(){
  try { if (sessionStorage.getItem('visit_started') === '1') return; } catch(_) {}
  const payload = {
    screen_w: (screen && screen.width) || null,
    screen_h: (screen && screen.height) || null,
    device_pixel_ratio: window.devicePixelRatio || null,
    language: (navigator.languages && navigator.languages[0]) || navigator.language || null,
    is_touch: ('ontouchstart' in window) || (navigator.maxTouchPoints > 0)
  };
  fetch('/api/visit_start', {
    method: 'POST',
    headers: { 'Content-Type':'application/json' },
    body: JSON.stringify(payload),
    keepalive: true
  }).catch(()=>{}).finally(()=>{
    try { sessionStorage.setItem('visit_started','1'); } catch(_){}
  });
})();

(function initGlobalTooltip(){
  const MARGIN = 8;

  // 托管范围：带 data-tooltip + 展开/收起 + 快捷按钮/操作
  const EXPAND_TOGGLE_SELECTOR = '.fc-expand-toggle';
  const TOOLTIP_ANCHOR_SELECTOR = [
    '[data-tooltip]',
    EXPAND_TOGGLE_SELECTOR,
    '.fc-tooltip-target',
    '.fc-btn-icon-add',
    '.like-button',
    '.js-remove-fan',
    '.js-restore-fan',
    '#sidebar-toggle',   // 侧栏开关
    '#themeToggle',      // 主题切换
    '.fc-info-btn'       // 数据来源与说明
  ].join(', ');

  function isTopControl(el){
    return !!(el && (el.matches('#sidebar-toggle') || el.matches('#themeToggle') || el.matches('.fc-info-btn')));
  }

  // 延迟
  const EXPAND_TOGGLE_TIP_KEY = 'suppress_expand_toggle_tooltip';
  const EXPAND_TOGGLE_DELAY = 700; // 展开/收起延迟
  const QUICK_ACTION_DELAY = 700;  // 快捷按钮：轻微延迟
  const isTouch = ('ontouchstart' in window) || (navigator.maxTouchPoints > 0);

  let tip = null, currAnchor = null;
  let hideTimer = null, autoHideTimer = null, showTimer = null;

  function ensureTip(){
    if (tip) return tip;
    tip = document.createElement('div');
    tip.id = 'appTooltip';
    // 开启长文本自动换行
    tip.style.whiteSpace = 'normal';
    tip.style.wordBreak = 'break-word';
    tip.style.maxWidth = 'min(70vw, 320px)';
    document.body.appendChild(tip);
    return tip;
  }
  function setText(html){ ensureTip().innerHTML = html; }

  function placeAround(anchor, preferred='top'){
    const t = ensureTip();
    const rect = anchor.getBoundingClientRect();
    const vw = window.innerWidth, vh = window.innerHeight;

    // 先放到屏幕外，计算尺寸
    t.style.visibility = 'hidden';
    t.dataset.show = '1';
    t.style.left = '-9999px';
    t.style.top  = '-9999px';

    const tw = t.offsetWidth, th = t.offsetHeight;

    let placement = preferred;
    const topSpace = rect.top;
    const bottomSpace = vh - rect.bottom;
    if (preferred === 'top' && topSpace < th + 12) placement = 'bottom';
    if (preferred === 'bottom' && bottomSpace < th + 12) placement = 'top';

    let cx = rect.left + rect.width / 2;
    const half = tw / 2;
    cx = Math.max(MARGIN + half, Math.min(vw - MARGIN - half, cx));

    const top = placement === 'top'
      ? (rect.top - th - 10)
      : (rect.bottom + 10);

    t.dataset.placement = placement;
    t.style.left = `${Math.round(cx)}px`;
    t.style.top  = `${Math.round(top)}px`;
    t.style.visibility = '';
  }

  // 立刻托管原生 title
  function hoistNativeTitle(el){
    if (!el) return;
    if (el.hasAttribute('title')) {
      el.setAttribute('data-title', el.getAttribute('title') || '');
      el.removeAttribute('title');
    }
  }

  // 分类与抑制
  const isExpandToggle = (el) => !!(el && el.matches(EXPAND_TOGGLE_SELECTOR));
  const isQuickAction  = (el) => !!(el && el.matches('.fc-btn-icon-add, .like-button, .js-remove-fan, .js-restore-fan'));
  function isSuppressed(el){
    if (!isExpandToggle(el)) return false;
    try { return sessionStorage.getItem(EXPAND_TOGGLE_TIP_KEY) === '1'; } catch(_) { return false; }
  }
  function suppressExpandToggleTip(){ try { sessionStorage.setItem(EXPAND_TOGGLE_TIP_KEY, '1'); } catch(_) {} }

  // 点击后静默：直到 mouseleave/focusout 清除
  function markSilent(el){
    if (!el) return;
    el.setAttribute('data-tip-silence', '1');
  }
  function clearSilent(el){
    if (!el) return;
    el.removeAttribute('data-tip-silence');
  }
  function isSilent(el){ return !!(el && el.getAttribute('data-tip-silence') === '1'); }

  // 动态文案
  function getExpandToggleText(el){
    const expanded = el.getAttribute('aria-expanded') === 'true';
    return expanded ? '收起该型号的所有工况' : '展开该型号的所有工况';
  }
  function getQuickActionText(el){
    if (!el) return '';
    if (el.matches('.fc-btn-icon-add')) {
      const mode = el.dataset.mode || '';
      return mode === 'remove' ? '从图表移除' : '添加到图表';
    }
    if (el.matches('.like-button')) {
      const icon = el.querySelector('i');
      const liked = !!(icon && icon.classList.contains('text-red-500'));
      return liked ? '取消点赞' : '点赞';
    }
    if (el.matches('.js-remove-fan'))  return '移除';
    if (el.matches('.js-restore-fan')) return '恢复';
    return '';
  }
  function getTooltipText(el){
    if (isExpandToggle(el)) return getExpandToggleText(el);
    if (isQuickAction(el))  return getQuickActionText(el);
    const fromData = el.getAttribute('data-tooltip');
    if (fromData && fromData.trim() !== '') return fromData;
    if (el.hasAttribute('title')) return el.getAttribute('title') || '';
    return el.getAttribute('data-title') || '';
  }

  function cancelShow(){ if (showTimer){ clearTimeout(showTimer); showTimer = null; } }
  function show(anchor){
    cancelShow();
    clearTimeout(hideTimer);
    clearTimeout(autoHideTimer);
    if (isSilent(anchor)) return; // 静默中不显示
    currAnchor = anchor;

    const txt = getTooltipText(anchor);
    if (!txt) return;

    setText(txt);
    placeAround(anchor, anchor.getAttribute('data-tooltip-placement') || 'top');
    ensureTip().dataset.show = '1';

    if (isTouch) {
      autoHideTimer = setTimeout(() => hide(true), 1200);
    }
  }
  function scheduleShow(anchor){
    cancelShow();
    if (isSilent(anchor)) return;
    const delay =
      (anchor && (anchor.matches(EXPAND_TOGGLE_SELECTOR) || isTopControl(anchor)))
        ? EXPAND_TOGGLE_DELAY
        : (anchor && anchor.matches('.fc-btn-icon-add, .like-button, .js-remove-fan, .js-restore-fan'))
          ? QUICK_ACTION_DELAY
          : 0;
    if (delay <= 0) { show(anchor); return; }
    showTimer = setTimeout(() => show(anchor), delay);
  }
  function hide(immediate=false){
    const t = ensureTip();
    const doHide = () => { t.dataset.show = '0'; currAnchor = null; };
    if (immediate){ cancelShow(); return doHide(); }
    hideTimer = setTimeout(doHide, 60);
  }

  // Hover/focus：进入即托管 title，再调度显示
  document.addEventListener('mouseenter', (e) => {
    if (isTouch) return;
    const el = safeClosest(e.target, TOOLTIP_ANCHOR_SELECTOR);
    if (!el) return;
    if (isExpandToggle(el) && isSuppressed(el)) return;
    hoistNativeTitle(el);
    scheduleShow(el);
  }, true);

  document.addEventListener('mouseout', (e) => {
    if (isTouch) return;
    const el = safeClosest(e.target, TOOLTIP_ANCHOR_SELECTOR);
    if (!el) return;
    const to = e.relatedTarget;
    // 如果仍在同一锚点内移动，不算真正离开
    if (to && (to === el || el.contains(to))) return;
    cancelShow();
    hide(false);
    clearSilent(el); // 真正离开才解除“本次抑制”
  }, true);

  document.addEventListener('focusin', (e)=>{
    if (isTouch) return;
    const el = safeClosest(e.target, TOOLTIP_ANCHOR_SELECTOR);
    if (!el) return;
    if (isExpandToggle(el) && isSuppressed(el)) return;
    hoistNativeTitle(el);
    scheduleShow(el);
  });
  document.addEventListener('focusout', (e)=>{
    if (isTouch) return;
    const el = safeClosest(e.target, TOOLTIP_ANCHOR_SELECTOR);
    if (!el) return;
    cancelShow();
    hide(false);
    clearSilent(el); // 失焦后解除静默
  });

  // 触屏：tap 显示 + 自动隐藏
  document.addEventListener('touchstart', (e) => {
    const el = safeClosest(e.target, TOOLTIP_ANCHOR_SELECTOR);
    if (!el) return;
    if (isExpandToggle(el) && isSuppressed(el)) return;
    hoistNativeTitle(el);
    show(el);
  }, { passive: true, capture: true });

  // 点击后：立即托管 title + 本会话抑制（仅展开按钮）+ 至离开前静默该锚点
  document.addEventListener('click', (e) => {
    const el = safeClosest(e.target, TOOLTIP_ANCHOR_SELECTOR);
    if (!el) return;
    hoistNativeTitle(el);
    markSilent(el);   // 到 mouseout/focusout 为止不再显示
    cancelShow();
    hide(true);
  }, true);

  // 布局变化时重新定位
  const onRelayout = ()=>{
    if (currAnchor && document.body.contains(currAnchor)) {
      placeAround(
        currAnchor,
        currAnchor.getAttribute('data-tooltip-placement') || 'top'
      );
    }
  };
  window.addEventListener('resize', onRelayout);
  window.addEventListener('scroll', onRelayout, { passive: true, capture: true });

  // 页面卸载前恢复 title（可选）
  window.addEventListener('beforeunload', ()=>{
    document.querySelectorAll('[data-title]').forEach(el=>{
      el.setAttribute('title', el.getAttribute('data-title') || '');
      el.removeAttribute('data-title');
    });
  });
})();

/* 工况格式化 */
function formatScenario(rt, rl){
  const rtype = escapeHtml(rt || '');
  const raw = rl ?? '';
  const isEmpty = (String(raw).trim() === '' || String(raw).trim() === '无');
  return isEmpty ? rtype : `${rtype}(${escapeHtml(raw)})`;
}
// Export to window for cross-module access
window.formatScenario = formatScenario;

/* expand */
async function fetchExpandPairs(brand, model, condition){
  const payload = {
    mode: 'expand',
    brand,
    model,
    condition_name: condition
  };
  const resp = await fetch('/api/search_fans', {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify(payload)
  });
  const j = await resp.json();
  const n = normalizeApiResponse(j);
  if (!n.ok){
    throw new Error(n.error_message || n.error_code || 'expand 请求失败');
  }
  const root = n.data || {};
  const items = (root.items) || (root.data && root.data.items) || [];
  return items.map(it=>({
    model_id: it.model_id,
    condition_id: it.condition_id,
    brand: it.brand_name_zh,
    model: it.model_name,
    condition: it.condition_name_zh
  }));
}

async function refreshChartFromLocal(showToast=false){
  const pairs = LocalState.getSelectionPairs();
  if (pairs.length === 0) {
    postChartData({ x_axis_type: LocalState.getXAxisType(), series: [] });
    return;
  }
  try {
    const resp = await fetch('/api/curves', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ pairs })
    });
    const j = await resp.json();
    const n = normalizeApiResponse(j);
    if (!n.ok){ showError(n.error_message || '获取曲线失败'); return; }
    const data = n.data || {};

    // missing 清理保持不变...

    const chartData = {
      x_axis_type: LocalState.getXAxisType(),
      series: data.series || []
    };
    // NEW: 用服务器返回的元信息更新显示缓存
    DisplayCache.setFromSeries(chartData.series);
    // 刷新侧栏/最近移除的显示文本
    rebuildSelectedFans(LocalState.getSelected());
    window.__APP.features.recentlyRemoved.rebuild(LocalState.getRecentlyRemoved());

    lastChartData = chartData;
    postChartData(chartData);
    if (showToast) showSuccess('已刷新曲线');
  } catch(e){
    showError('曲线请求异常: '+e.message);
  }
}

async function logNewPairs(addedDetails, source = 'unknown') {
  if (!addedDetails || !addedDetails.length) return;
  const pairs = addedDetails.map(d => ({ model_id: d.model_id, condition_id: d.condition_id }));

  if (!window.Analytics || typeof window.Analytics.logQueryPairs !== 'function') {
    // Fail fast：显式暴露缺陷，便于定位问题
    throw new Error('Analytics module not loaded: window.Analytics.logQueryPairs is unavailable');
  }

  await window.Analytics.logQueryPairs(source, pairs);
}


function generateDarkGradient() {
  // 随机主色 & 副色 (HSL)
  const h1 = Math.floor(Math.random() * 360);
  const h2Offset = 30 + Math.floor(Math.random() * 60); // 30~90 之间偏移
  const h2 = (h1 + h2Offset) % 360;

  const s1 = 35 + Math.random() * 25; // 35-60%
  const s2 = 35 + Math.random() * 25;

  const l1 = 10 + Math.random() * 8;  // 10-18% 更暗
  const l2 = 14 + Math.random() * 12; // 14-26% 略亮

  const angle = Math.floor(Math.random() * 360);

  const stop1 = `hsl(${h1} ${s1.toFixed(1)}% ${l1.toFixed(1)}%)`;
  const stop2 = `hsl(${h2} ${s2.toFixed(1)}% ${l2.toFixed(1)}%)`;
  const gradient = `linear-gradient(${angle}deg, ${stop1} 0%, ${stop2} 100%)`;

  const root = document.documentElement;
  // 只设置渐变，不再改 --bg-primary，底色交给 CSS 里的 [data-theme="dark"] --bg-primary
  root.style.setProperty('--dark-rand-gradient', gradient);
  // 留下一个可供导出/其它用途的基色（可选，不参与底色）
  const baseIsFirst = l1 <= l2;
  root.style.setProperty('--dark-rand-base', baseIsFirst ? stop1 : stop2);
}

// ADD: 页面级滚动保护工具（兼容任意滚动容器）
function __preservePageScrollDuring(fn){
  const se = document.scrollingElement || document.documentElement || document.body;
  const sx = (typeof se.scrollLeft === 'number') ? se.scrollLeft : window.scrollX || 0;
  const sy = (typeof se.scrollTop  === 'number') ? se.scrollTop  : window.scrollY || 0;
  try { fn(); } catch(_) {}
  requestAnimationFrame(() => {
    try { se.scrollLeft = sx; se.scrollTop = sy; } catch(_){}
    try { window.scrollTo(sx, sy); } catch(_){}
  });
}

// ==== 公告栏初始化逻辑 ====
(function initAnnouncementBar(){
  const ROTATE_INTERVAL_MS = 10000; // 每 10 秒切换
  const FETCH_REFRESH_MS   = 60000; // 可选：每 60 秒刷新可用公告
  const bar      = document.getElementById('announcementBar');
  const textSpan = document.getElementById('fcAnnounceText');
  const closeBtn = document.getElementById('fcAnnounceClose');

  if (!bar || !textSpan || !closeBtn) return;

  const reduceMotion = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  let marqueeActive = false;
  let marqueeRaf = null;
  let marqueeStartTs = 0;
  let scrollDistance = 0;
  let scrollDuration = 0;
  let closedSession = false;

  // 轮播
  let announcements = [];
  let rotateTimer = null;
  let currentIndex = 0;

  // 在 .fc-announce-inner 内构造一个滚动容器，把原来的 #fcAnnounceText 移进去
  const inner = bar.querySelector('.fc-announce-inner');
  let roller = document.createElement('div');
  roller.className = 'fc-announce-roller';
  // 将文本 span 挪入 roller（位于 gradient 之前）
  inner.insertBefore(roller, inner.querySelector('.fc-announce-gradient'));
  roller.appendChild(textSpan);

  let closedId = null;
  try { closedId = sessionStorage.getItem('announce_closed_id'); } catch(_) {}

  function applyOffset(){
    const h = bar.getBoundingClientRect().height;
    document.documentElement.style.setProperty('--announce-offset', h + 'px');
  }

  function stopMarquee(){
    marqueeActive = false;
    if (marqueeRaf) cancelAnimationFrame(marqueeRaf);
    marqueeRaf = null;
    textSpan.style.transform = 'translateX(0)';
    textSpan.classList.remove('is-marquee');
  }

  function frameMarquee(ts){
    if (!marqueeActive) return;
    if (!marqueeStartTs) marqueeStartTs = ts;
    const progress = (ts - marqueeStartTs) / scrollDuration;
    const pct = progress % 1;
    const x = -scrollDistance * pct;
    textSpan.style.transform = `translateX(${x}px)`;
    marqueeRaf = requestAnimationFrame(frameMarquee);
  }

  function startMarqueeIfNeeded(){
    stopMarquee();
    const innerW = textSpan.scrollWidth;
    const boxW = textSpan.parentElement.clientWidth; // roller 的宽度
    if (innerW <= boxW + 4) return;
    textSpan.classList.add('is-marquee');
    scrollDistance = innerW - boxW;
    const rawDuration = scrollDistance / 80 * 1000;
    scrollDuration = Math.min(30000, Math.max(6000, rawDuration));
    marqueeActive = true;
    marqueeStartTs = 0;
    marqueeRaf = requestAnimationFrame(frameMarquee);
  }

  function slideToText(newText){
    if (reduceMotion) {
      textSpan.textContent = newText;
      applyOffset();
      startMarqueeIfNeeded();
      return;
    }

    // 1) 清理可能残留的旧 next 节点（防止之前动画未完成）
    Array.from(roller.children).forEach((node, idx) => {
      if (node !== textSpan && node.classList.contains('fc-announce-text')) {
        node.remove();
      }
    });

    // 2) 构造下一条
    const next = document.createElement('span');
    next.className = 'fc-announce-text';
    next.textContent = newText;
    roller.appendChild(next);

    // 3) 测量高度并锁定
    const hCurrent = textSpan.offsetHeight || textSpan.getBoundingClientRect().height || 0;
    const hNext    = next.offsetHeight    || next.getBoundingClientRect().height    || 0;
    const hLock    = Math.max(hCurrent, hNext);
    roller.style.height = hLock + 'px';

    // 停止横向跑马灯，避免 transform 冲突
    stopMarquee();

    // 确保初始 transform 状态生效（强制 reflow）
    roller.style.transform = 'translateY(0)';
    // 强制读取一次尺寸触发 reflow
    void roller.offsetWidth;

    // 4) 开始动画
    roller.classList.add('is-sliding');
    // 用 requestAnimationFrame 确保类已应用
    requestAnimationFrame(() => {
      roller.style.transform = `translateY(-${hCurrent}px)`;
    });

    // 5) 计算时长 + 兜底定时器
    let durationMs = 380; // 默认值与 CSS 变量保持一致
    try {
      const cs = getComputedStyle(roller);
      // transition-duration 可能是 '0.38s' 或 '0.38s, 0.38s'
      const td = cs.transitionDuration.split(',')[0].trim();
      if (td.endsWith('ms')) {
        durationMs = parseFloat(td);
      } else if (td.endsWith('s')) {
        durationMs = parseFloat(td) * 1000;
      }
      if (!Number.isFinite(durationMs) || durationMs <= 0) durationMs = 380;
    } catch(_) {}

    let ended = false;
    const fallbackTimer = setTimeout(() => {
      if (!ended) {
        onEnd();
      }
    }, durationMs + 60); // 略加缓冲

    function onEnd(){
      if (ended) return;
      ended = true;
      clearTimeout(fallbackTimer);
      roller.removeEventListener('transitionend', onEnd);

      roller.classList.remove('is-sliding');
      // 更新文字：保留原 textSpan，移除 next
      textSpan.textContent = newText;
      next.remove();

      // 清理内联样式（不要保留 transition: none）
      roller.style.removeProperty('transition');
      roller.style.transform = 'translateY(0)';
      roller.style.height = '';

      applyOffset();
      startMarqueeIfNeeded();
    }

    roller.addEventListener('transitionend', (e)=>{
      // 只在 transform 过渡结束时响应
      if (e.propertyName === 'transform') onEnd();
    });
  }

  function showAnnouncement(idx, animate=true){
    if (!announcements.length) return;
    idx = idx % announcements.length;
    currentIndex = idx;
    const item = announcements[idx];
    if (!item) return;

    // 使用上滑过渡
    if (animate) {
      slideToText(item.content_text);
    } else {
      textSpan.textContent = item.content_text;
      applyOffset();
      startMarqueeIfNeeded();
    }

    bar.setAttribute('data-id', item.id);
  }

  function startRotation(){
    clearInterval(rotateTimer);
    if (announcements.length <= 1) return;
    rotateTimer = setInterval(()=>{
      if (closedSession) { clearInterval(rotateTimer); return; }
      showAnnouncement(currentIndex + 1, true);
    }, ROTATE_INTERVAL_MS);
  }

  function teardown(){
    clearInterval(rotateTimer);
    rotateTimer = null;
    stopMarquee();
    document.documentElement.style.setProperty('--announce-offset','0px');
  }

  // 关闭：本会话隐藏全部公告
  closeBtn.addEventListener('click', ()=>{
    bar.classList.add('hidden');
    closedSession = true;
    teardown();
    try {
      const id = bar.getAttribute('data-id');
      if (id) sessionStorage.setItem('announce_closed_id', id);
    } catch(_){}
  });

  async function fetchAnnouncements(){
    try {
      const r = await fetch('/api/announcement');
      const j = await r.json();
      const resp = normalizeApiResponse(j);
      if (!resp.ok) return;
      const data = resp.data || {};
      let items = Array.isArray(data.items) ? data.items.slice() : [];

      // 若已关闭当前 id，这里可以按需“全局关闭”或“过滤该条继续轮播”。
      // 当前逻辑：若服务器第一条是已关闭的，直接不显示（可自行改为过滤）。
      if (closedId && items.some(i => String(i.id) === String(closedId))) {
        return;
      }

      announcements = items;
      if (!announcements.length) return;

      bar.classList.remove('hidden');
      showAnnouncement(0, false);
      startRotation();
    } catch(_){}
  }

  // 初次加载 + 周期刷新
  fetchAnnouncements();
  setInterval(()=> {
    if (closedSession) return;
    fetchAnnouncements();
  }, FETCH_REFRESH_MS);

  // 窗口变化：重置高度与跑马灯
  window.addEventListener('resize', ()=>{
    if (bar.classList.contains('hidden')) return;
    applyOffset();
    stopMarquee();
    startMarqueeIfNeeded();
  });

  // 触控/拖动横向阅读
  let dragState = null;
  function onPointerDown(e){
    if (!textSpan.classList.contains('is-marquee')) return;
    stopMarquee();
    const pt = e.touches ? e.touches[0] : e;
    dragState = {
      startX: pt.clientX,
      baseX: 0,
      currentX: 0,
      minX: -(textSpan.scrollWidth - textSpan.parentElement.clientWidth),
      maxX: 0
    };
    textSpan.classList.add('dragging');
    e.preventDefault();
  }
  function onPointerMove(e){
    if (!dragState) return;
    const pt = e.touches ? e.touches[0] : e;
    const dx = pt.clientX - dragState.startX;
    let next = dragState.baseX + dx;
    if (next < dragState.minX) next = dragState.minX;
    if (next > dragState.maxX) next = dragState.maxX;
    dragState.currentX = next;
    textSpan.style.transform = `translateX(${next}px)`;
    e.preventDefault();
  }
  function onPointerUp(){
    if (!dragState) return;
    dragState = null;
    textSpan.classList.remove('dragging');
  }
  textSpan.addEventListener('touchstart', onPointerDown, { passive:false });
  textSpan.addEventListener('touchmove', onPointerMove, { passive:false });
  textSpan.addEventListener('touchend', onPointerUp, { passive:true });
  textSpan.addEventListener('mousedown', onPointerDown);
  window.addEventListener('mousemove', onPointerMove);
  window.addEventListener('mouseup', onPointerUp);
})();
