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

/* =========================================================
   Persistent model metadata cache
   Backed by localStorage so browsing-history cards survive page reloads.
   Stores: { "<mid>": { brand, model, reference_price, max_speed, size, thickness, rgb_light } }
   ========================================================= */
const MODEL_META_LS_KEY = 'fc_model_meta_v1';
(function initModelMetaCache() {
  let stored = {};
  try { stored = JSON.parse(localStorage.getItem(MODEL_META_LS_KEY) || '{}') || {}; } catch(_) {}
  window.__modelMetaCache = window.__modelMetaCache || {};
  // Merge stored entries into the live cache (mini-radar-card.js may have already set some)
  Object.entries(stored).forEach(([mid, meta]) => {
    if (!window.__modelMetaCache[mid]) window.__modelMetaCache[mid] = meta;
  });
})();

/** Persist a single model's metadata entry to localStorage. */
function _persistModelMeta(mid, meta) {
  window.__modelMetaCache = window.__modelMetaCache || {};
  window.__modelMetaCache[String(mid)] = meta;
  try {
    const stored = JSON.parse(localStorage.getItem(MODEL_META_LS_KEY) || '{}') || {};
    stored[String(mid)] = meta;
    localStorage.setItem(MODEL_META_LS_KEY, JSON.stringify(stored));
  } catch(_) {}
}

/**
 * Fetch model metadata for given model IDs and populate __modelMetaCache.
 * Only fetches IDs that are not already cached.
 * @param {number[]} modelIds
 * @returns {Promise<void>}
 */
function _fetchAndCacheModelMeta(modelIds) {
  if (!Array.isArray(modelIds) || !modelIds.length) return Promise.resolve();
  const need = modelIds.map(Number).filter(mid => {
    const midStr = String(mid);
    const cached = window.__modelMetaCache && window.__modelMetaCache[midStr];
    return !cached || !cached.model || !('review' in cached); // re-fetch if only partial data stored
  });
  if (!need.length) {
    // All reviews already in cache – still refresh the panel (e.g. on page restore
    // where syncPageStateWithBackend pre-populated the cache with review data).
    _updateExtraPanel(null);
    return Promise.resolve();
  }
  return fetch('/api/model_meta?model_ids=' + need.map(encodeURIComponent).join(','))
    .then(r => r.json())
    .then(j => {
      const models = (j && j.data && j.data.models) || {};
      Object.entries(models).forEach(([mid, meta]) => {
        _persistModelMeta(mid, meta);
      });
      // Refresh extra panel in case newly loaded review data is relevant
      _updateExtraPanel(null);
    })
    .catch(() => {});
}
window._fetchAndCacheModelMeta = _fetchAndCacheModelMeta;

function installRemovedRenderHookOnce(){
  try{
    const mod = window.__APP && window.__APP.features && window.__APP.features.recentlyRemoved;
    if (!mod || !mod.rebuild) return false;
    if (mod.__ID_PATCHED__) return true;
    const orig = mod.rebuild;
    mod.rebuild = function(list){
      const enriched = (list||[]).map(it=>{
        const info = window.DisplayCache && window.DisplayCache.get(it.model_id, it.condition_id);
        const meta = (window.__modelMetaCache && window.__modelMetaCache[String(it.model_id)]) || {};
        return {
          ...it,
          brand:     meta.brand     || info?.brand || it.brand || '',
          model:     meta.model     || info?.model || it.model || '',
          condition: info?.condition || it.condition || '加载中...',
          // Carry full model-level metadata into the item so recently-removed.js can use it
          reference_price: meta.reference_price != null ? meta.reference_price : null,
          max_speed:  meta.max_speed  || null,
          size:       meta.size       || '',
          thickness:  meta.thickness  || '',
          rgb_light:  meta.rgb_light  || '',
          review:     meta.review != null ? meta.review : null,
        };
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

// Condition IDs used for the radar chart (must match RADAR_CIDS in fancool-search.js)
// Defined here for fancool.js-internal use; the canonical source is mini-radar-card.js
// which exports window.RADAR_CIDS_COLOR. Fallback to that if already set.
const RADAR_CIDS_COLOR   = window.RADAR_CIDS_COLOR   || [1, 2, 3, 8, 7, 10];

// buildMiniRadarSVG and buildMiniRadarCardEl are defined in mini-radar-card.js (loaded before
// this file). The shims below ensure backward compatibility if the module is not yet loaded.
function buildMiniRadarSVG(items, compositeScore, condLabelCache, svgId, opts) {
  if (typeof window.buildMiniRadarSVG === 'function' && window.buildMiniRadarSVG !== buildMiniRadarSVG) {
    return window.buildMiniRadarSVG(items, compositeScore, condLabelCache, svgId, opts);
  }
  // Minimal inline fallback (should not be reached in normal operation)
  return `<svg class="fc-radar-svg" viewBox="0 0 300 120" xmlns="http://www.w3.org/2000/svg"><text x="150" y="60" text-anchor="middle" dominant-baseline="middle" font-size="12">加载中...</text></svg>`;
}

function buildMiniRadarCardEl(cfg) {
  if (typeof window.buildMiniRadarCardEl === 'function' && window.buildMiniRadarCardEl !== buildMiniRadarCardEl) {
    return window.buildMiniRadarCardEl(cfg);
  }
  // Minimal inline fallback
  const d = document.createElement('div');
  d.className = 'fc-mini-radar-card';
  return d;
}

/* ===========================================================
   RadarState – single source of truth for radar-driven curves
   =========================================================== */
const RADAR_CURVE_CAP = FRONT_MAX_ITEMS; // max simultaneous chart series (same as FRONT_MAX_ITEMS)
const CAP_EXCEEDED_MSG = '右侧数据过多，请清除部分条目';
const RADAR_REDIRECT_MSG = '请通过左侧雷达图添加型号和选择工况';

const RadarState = (function () {
  const radarModels      = new Map(); // mid(string) → {modelId, brand, label}
  const activeConditions = new Set(); // cid (string)
  const hiddenModels     = new Set(); // mid (string) – hidden in radar legend (series removed from chart)

  return {
    getModels()           { return radarModels; },
    getActiveConditions() { return activeConditions; },
    getHiddenModels()     { return hiddenModels; },

    addModel(modelId, brand, label) {
      radarModels.set(String(modelId), { modelId: Number(modelId), brand, label });
    },
    removeModel(modelId) {
      const mid = String(modelId);
      radarModels.delete(mid);
      hiddenModels.delete(mid);
    },
    hasModel(modelId)      { return radarModels.has(String(modelId)); },

    setModelHidden(modelId, hidden) {
      const mid = String(modelId);
      if (hidden) hiddenModels.add(mid);
      else        hiddenModels.delete(mid);
    },
    isModelHidden(modelId) { return hiddenModels.has(String(modelId)); },

    activateCondition(cid)   { activeConditions.add(String(cid)); },
    deactivateCondition(cid) { activeConditions.delete(String(cid)); },

    /** How many series would be in the chart with current state (visible models × active conditions). */
    computeSeriesCount() {
      let count = 0;
      radarModels.forEach((_, mid) => {
        if (!hiddenModels.has(mid)) count += activeConditions.size;
      });
      return count;
    },

    /** How many series if we were to activate cid (assuming it's not already active). */
    computeSeriesCountIfAdd(cid) {
      const cidStr = String(cid);
      if (activeConditions.has(cidStr)) return this.computeSeriesCount();
      let count = 0;
      radarModels.forEach((_, mid) => {
        if (!hiddenModels.has(mid)) count += activeConditions.size + 1;
      });
      return count;
    },

    /** How many series if we were to show a currently-hidden model. */
    computeSeriesCountIfShow(modelId) {
      const mid = String(modelId);
      if (!hiddenModels.has(mid)) return this.computeSeriesCount();
      return this.computeSeriesCount() + activeConditions.size;
    }
  };
})();

// Export RadarState to window so other scripts (right-panel-v2.js, etc.) can access it.
window.RadarState = RadarState;

/** Add a model to radar and sync chart series. */
/** Persist current RadarState selection (model identities + active conditions) to localStorage. */
function persistRadarState() {
  if (!window.LocalState) return;
  const models = [];
  RadarState.getModels().forEach((model) => {
    models.push({ modelId: model.modelId, brand: model.brand, label: model.label });
  });
  LocalState.setRadarModels(models);
  LocalState.setRadarConditions(Array.from(RadarState.getActiveConditions()));
}

/**
 * Restore RadarState from localStorage after a page refresh.
 * Model identities and active conditions are restored from storage; radar score
 * data is NOT restored from storage – it is freshly re-fetched from the backend.
 */
async function restoreRadarFromStorage() {
  if (!window.LocalState) return;
  const savedModels = LocalState.getRadarModels();  // [{modelId, brand, label}]
  const savedConds  = LocalState.getRadarConditions(); // [cid strings]

  if (!savedModels.length) return;

  // Re-populate RadarState with identity data (no scores yet)
  savedModels.forEach(m => {
    if (!RadarState.hasModel(m.modelId)) {
      RadarState.addModel(m.modelId, m.brand, m.label);
    }
  });
  savedConds.forEach(cid => RadarState.activateCondition(cid));

  // Remove any browsing-history entries for models that are restored into radar
  // so that Browsing History stays consistent with the restored radar state.
  if (typeof LocalState.removeModelFromHistory === 'function') {
    savedModels.forEach(m => LocalState.removeModelFromHistory(m.modelId));
  }

  // Reconcile LocalState.selected with the restored RadarState so the main chart
  // reflects the radar-driven curve selection immediately, even before the radar
  // metrics re-fetch completes.  Old or stale selected pairs that are not part of
  // the current RadarState are removed; missing pairs are added.
  // Send 'restore' event type – the backend decides whether to log it based on
  // whether the current visit already has curve log entries (same-visit suppression).
  syncFromRadarState('restore');

  // Show the radar overview with skeleton (no scores) immediately
  syncRadarOverview();

  // Re-fetch fresh radar scores from backend
  const modelIds = savedModels.map(m => m.modelId).filter(Boolean);
  if (!modelIds.length) return;
  try {
    const url = '/api/radar_metrics?model_ids=' + modelIds.join(',');
    const resp = await fetch(url);
    const json = await resp.json();
    const n = (typeof normalizeApiResponse === 'function') ? normalizeApiResponse(json) : null;
    const radarModels = (n && n.ok && n.data && n.data.models) || null;
    if (radarModels && typeof radarModels === 'object') {
      Object.entries(radarModels).forEach(([mid, data]) => {
        if (window.__radarCache) window.__radarCache[mid] = data;
      });
    }
  } catch (e) {
    typeof console !== 'undefined' && console.warn('[RadarRestore] Failed to fetch radar metrics:', e);
  }

  // Re-render with fresh scores
  syncRadarOverview();

  // Sync like states for all restored models' radar conditions
  _syncAllRadarModelLikeStates();

  // Fetch/refresh model metadata (including review) for restored models
  _fetchAndCacheModelMeta(modelIds.map(Number));

  // Sync visual state of all quick-add buttons with restored radar membership.
  // This handles any buttons that were already rendered (e.g. recent-likes panel
  // loaded before the restore completed) so they reflect the correct state.
  if (typeof syncRadarToggleButtons === 'function') syncRadarToggleButtons();

  // Log radar restore to user_radar_logs; backend decides whether to record it
  // based on same-visit suppression (new visit = log, same-visit refresh = skip).
  if (window.Analytics && typeof window.Analytics.logRadarModels === 'function') {
    const restoreActionId = String(Date.now());
    savedModels.forEach(m => {
      window.Analytics.logRadarModels('restore', m.modelId, 'page_restore', restoreActionId);
    });
  }
}

/**
 * Synchronise locally stored model references with backend validity on page load.
 *
 * Reads all model IDs from persisted radar selections and browsing history,
 * validates them against the backend (available_models_info_view), then:
 *   – removes invalid/unavailable models from both stores before RestoreRadar runs,
 *   – refreshes __modelMetaCache with current metadata for valid models.
 *
 * This prevents stale or removed models from appearing indefinitely in the UI.
 * The function runs silently; no user-visible notice is shown for removed entries
 * (consistent with the existing silent-cleanup pattern in this codebase).
 */
async function syncPageStateWithBackend() {
  if (!window.LocalState) return;

  const radarModels  = LocalState.getRadarModels();   // [{modelId, brand, label}]
  const historyItems = LocalState.getRecentlyRemoved(); // [{model_id, ...}]

  const allIds = new Set();
  radarModels.forEach(m => {
    const mid = Number(m.modelId);
    if (Number.isFinite(mid) && mid > 0) allIds.add(mid);
  });
  historyItems.forEach(h => {
    const mid = Number(h.model_id);
    if (Number.isFinite(mid) && mid > 0) allIds.add(mid);
  });

  if (allIds.size === 0) return;

  try {
    const url = '/api/validate_models?model_ids=' + Array.from(allIds).join(',');
    const resp = await fetch(url);
    if (!resp.ok) return;
    const json = await resp.json();
    const n = (typeof normalizeApiResponse === 'function') ? normalizeApiResponse(json) : null;
    if (!n || !n.ok) return;

    const validMeta  = n.data.valid   || {};
    const invalidArr = n.data.invalid || [];
    if (!invalidArr.length && !Object.keys(validMeta).length) return;

    // Refresh __modelMetaCache with up-to-date metadata from backend
    if (!window.__modelMetaCache) window.__modelMetaCache = {};
    Object.entries(validMeta).forEach(([mid, meta]) => {
      window.__modelMetaCache[mid] = meta;
    });

    if (!invalidArr.length) return;

    const invalidIds = new Set(invalidArr.map(Number));

    // Remove invalid models from persisted radar selection (localStorage only –
    // RadarState has not been populated yet when this runs before restoreRadarFromStorage)
    const validRadarModels = radarModels.filter(m => !invalidIds.has(Number(m.modelId)));
    if (validRadarModels.length < radarModels.length) {
      LocalState.setRadarModels(validRadarModels);
    }

    // Remove invalid models from browsing history
    if (typeof LocalState.removeModelFromHistory === 'function') {
      invalidIds.forEach(mid => LocalState.removeModelFromHistory(mid));
    }
  } catch (e) {
    typeof console !== 'undefined' && console.warn('[syncPageState] Backend validation failed:', e);
  }
}

/**
 * Sync RadarOverview's internal like state for one model from LocalState.likes.
 * Called whenever a like change is made via the sidebar/global like-button handler so
 * that both the radar overview and the recent-likes sidebar share the same state.
 * @param {string|number} modelId
 */
function _syncRadarLikeStateFromLocal(modelId) {
  if (!window.RadarOverview || typeof window.RadarOverview.setModelLikeState !== 'function') return;
  const midStr = String(modelId);
  const likedCids = new Set();
  RADAR_CIDS_COLOR.forEach(cid => {
    if (LocalState.likes && LocalState.likes.has(`${midStr}_${cid}`)) {
      likedCids.add(String(cid));
    }
  });
  window.RadarOverview.setModelLikeState(modelId, likedCids);
}

/**
 * Fetch like state for a single model's six radar conditions and update
 * RadarOverview's legend thumb aggregate.
 * @param {string|number} modelId
 */
function _syncRadarModelLikeState(modelId) {
  const midStr = String(modelId);
  const pairs  = RADAR_CIDS_COLOR.map(cid => ({ model_id: Number(modelId), condition_id: cid }));
  fetch('/api/like_status', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ pairs }),
  })
    .then(r => r.json())
    .then(j => {
      const n = typeof normalizeApiResponse === 'function' ? normalizeApiResponse(j) : null;
      if (!n || !n.ok) return;
      const data     = n.data || {};
      const likeKeys = data.like_keys || [];
      if (data.fp && LocalState.likes && typeof LocalState.likes.updateServerFP === 'function') {
        LocalState.likes.updateServerFP(data.fp);
      }
      // Merge into local like cache
      likeKeys.forEach(k => { if (LocalState.likes) LocalState.likes.add(k); });
      // Compute liked cid set for this model
      const likedCids = new Set(
        likeKeys
          .filter(k => k.startsWith(midStr + '_'))
          .map(k => k.split('_')[1])
      );
      if (window.RadarOverview && typeof window.RadarOverview.setModelLikeState === 'function') {
        window.RadarOverview.setModelLikeState(modelId, likedCids);
      }
    })
    .catch(() => {});
}

/**
 * Fetch like state for ALL current radar models' six conditions.
 * Called on page restore (restoreRadarFromStorage) so legend thumbs are
 * immediately correct after a page reload.
 */
function _syncAllRadarModelLikeStates() {
  const mids = Array.from(RadarState.getModels().keys());
  if (!mids.length) return;
  const pairs = [];
  mids.forEach(mid => {
    RADAR_CIDS_COLOR.forEach(cid => {
      pairs.push({ model_id: Number(mid), condition_id: cid });
    });
  });
  fetch('/api/like_status', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ pairs }),
  })
    .then(r => r.json())
    .then(j => {
      const n = typeof normalizeApiResponse === 'function' ? normalizeApiResponse(j) : null;
      if (!n || !n.ok) return;
      const data     = n.data || {};
      const likeKeys = data.like_keys || [];
      if (data.fp && LocalState.likes && typeof LocalState.likes.updateServerFP === 'function') {
        LocalState.likes.updateServerFP(data.fp);
      }
      likeKeys.forEach(k => { if (LocalState.likes) LocalState.likes.add(k); });
      if (window.RadarOverview && typeof window.RadarOverview.setModelLikeState === 'function') {
        mids.forEach(mid => {
          const likedCids = new Set(
            likeKeys
              .filter(k => k.startsWith(mid + '_'))
              .map(k => k.split('_')[1])
          );
          window.RadarOverview.setModelLikeState(mid, likedCids);
        });
      }
    })
    .catch(() => {});
}

/** Rebuild the Browsing History sidebar panel from current LocalState. */
function _rebuildBrowsingHistory() {
  const mod = window.__APP && window.__APP.features && window.__APP.features.recentlyRemoved;
  if (mod && mod.rebuild) mod.rebuild(LocalState.getRecentlyRemoved());
}

function addModelToRadar(modelId, brand, label) {
  if (RadarState.hasModel(modelId)) {
    typeof showInfo === 'function' && showInfo('该型号已在雷达图中');
    return;
  }
  // Remove from Browsing History when model enters radar comparison
  if (window.LocalState && typeof LocalState.removeModelFromHistory === 'function') {
    LocalState.removeModelFromHistory(modelId);
    _rebuildBrowsingHistory();
  }
  // If adding this model's series would exceed the cap, add it hidden so it
  // appears in the radar list but doesn't contribute series to the chart.
  let addHidden = false;
  if (RadarState.getActiveConditions().size > 0) {
    const newCount = RadarState.computeSeriesCount() + RadarState.getActiveConditions().size;
    if (newCount > RADAR_CURVE_CAP) {
      addHidden = true;
    }
  }
  // Set hidden state before sync so no flash occurs (sync is deferred).
  RadarState.addModel(modelId, brand, label);
  if (addHidden) {
    // Mark hidden before syncing so the series are never added to the chart.
    // The user can unhide after reducing the active series count.
    RadarState.setModelHidden(String(modelId), true);
    typeof showInfo === 'function' && showInfo(CAP_EXCEEDED_MSG);
  }
  syncFromRadarState('model_add');
  syncRadarOverview();
  persistRadarState();
  if (typeof syncRadarToggleButtons === 'function') syncRadarToggleButtons();

  // Log radar model add event to user_radar_logs
  if (window.Analytics && typeof window.Analytics.logRadarModels === 'function') {
    window.Analytics.logRadarModels('add', modelId, 'radar_panel');
  }

  const midStr = String(modelId);

  // Fetch radar scores for the newly added model if not already cached
  if (!window.__radarCache || !window.__radarCache[midStr]) {
    fetch('/api/radar_metrics?model_ids=' + encodeURIComponent(midStr))
      .then(r => r.json())
      .then(j => {
        const n = typeof normalizeApiResponse === 'function' ? normalizeApiResponse(j) : null;
        const radarData = n && n.ok && n.data && n.data.models && n.data.models[midStr];
        if (radarData && window.__radarCache) {
          window.__radarCache[midStr] = radarData;
          syncRadarOverview();
        }
      })
      .catch(e => {
        typeof console !== 'undefined' && console.warn('[RadarAdd] Failed to fetch radar metrics:', e);
      });
  }

  // Fetch and persistently cache full model metadata (brand, price, speed, size…)
  // so that Browsing History cards continue to display correct header info even
  // after the model is removed and the page is reloaded.
  // Drop any cached review for this model so _fetchAndCacheModelMeta always
  // re-fetches the latest value from the backend (guards against stale cache
  // where review was empty when first cached but has since been populated).
  if (window.__modelMetaCache && window.__modelMetaCache[String(modelId)]) {
    delete window.__modelMetaCache[String(modelId)].review;
  }
  _fetchAndCacheModelMeta([Number(modelId)]);

  // Sync like state for newly added model's radar conditions
  _syncRadarModelLikeState(modelId);
  // Refresh extra review panel for the updated model set (may show '暂无点评'
  // briefly until the async meta fetch above completes and fires _updateExtraPanel).
  _updateExtraPanel(null);
}
function removeModelFromRadar(modelId) {
  const model = RadarState.getModels().get(String(modelId));
  RadarState.removeModel(modelId);
  // Add to Browsing History when model leaves radar comparison
  if (model && window.LocalState && typeof LocalState.addModelToHistory === 'function') {
    LocalState.addModelToHistory(model.modelId, model.brand, model.label);
    _rebuildBrowsingHistory();
  }
  syncFromRadarState('model_remove');
  syncRadarOverview();
  persistRadarState();
  if (typeof syncRadarToggleButtons === 'function') syncRadarToggleButtons();
  // Log radar model remove event to user_radar_logs
  if (window.Analytics && typeof window.Analytics.logRadarModels === 'function') {
    window.Analytics.logRadarModels('remove', modelId, 'radar_panel');
  }
  // Clear stale remembered model so the removed model id cannot linger as the
  // review target for subsequent Rule-C evaluation after a later re-add.
  if (_lastReviewModelId === String(modelId)) _lastReviewModelId = null;
  // Refresh extra review panel for the updated model set
  _updateExtraPanel(null);
}

/** Remove all models from radar comparison. */
function clearAllRadarModels() {
  const models = Array.from(RadarState.getModels().values());
  if (!models.length) return;
  // Add all models to Browsing History before clearing
  if (window.LocalState && typeof LocalState.addModelToHistory === 'function') {
    models.forEach(model => LocalState.addModelToHistory(model.modelId, model.brand, model.label));
  }
  models.forEach(model => RadarState.removeModel(String(model.modelId)));
  _rebuildBrowsingHistory();
  const clearActionId = String(Date.now());
  syncFromRadarState('radar_clear_all', clearActionId);
  syncRadarOverview();
  persistRadarState();
  if (typeof syncRadarToggleButtons === 'function') syncRadarToggleButtons();
  // Log radar clear_all event to user_radar_logs (single batch entry)
  if (window.Analytics && typeof window.Analytics.logRadarModels === 'function') {
    window.Analytics.logRadarModels('clear_all', null, 'radar_panel', clearActionId);
  }
  // Reset stale remembered model and force panel refresh so the review area
  // clears immediately even when no hover-state change was detected above.
  _lastReviewModelId = null;
  _updateExtraPanel(null);
}

/** Deactivate all active conditions (does NOT affect like state). */
function clearAllActiveConditions() {
  const cids = Array.from(RadarState.getActiveConditions());
  if (!cids.length) return;
  cids.forEach(cid => RadarState.deactivateCondition(cid));
  syncFromRadarState('reset_condition');
  if (window.RadarOverview && typeof window.RadarOverview.setActiveConditions === 'function') {
    window.RadarOverview.setActiveConditions(new Set());
  }
  persistRadarState();
}

/** Toggle condition across all radar models; blocks if cap would be exceeded. */
function activateRadarCondition(cid) {
  const cidStr = String(cid);
  if (RadarState.getActiveConditions().has(cidStr)) {
    // Already active – deactivate
    RadarState.deactivateCondition(cidStr);
    syncFromRadarState('condition_inactivate');
    // Update RadarOverview visual
    if (window.RadarOverview && typeof window.RadarOverview.setActiveConditions === 'function') {
      window.RadarOverview.setActiveConditions(new Set(RadarState.getActiveConditions()));
    }
    persistRadarState();
    return;
  }
  // Check cap before activating
  const newCount = RadarState.computeSeriesCountIfAdd(cidStr);
  if (newCount > RADAR_CURVE_CAP) {
    typeof showInfo === 'function' && showInfo(CAP_EXCEEDED_MSG);
    return;
  }
  RadarState.activateCondition(cidStr);
  syncFromRadarState('condition_activate');
  if (window.RadarOverview && typeof window.RadarOverview.setActiveConditions === 'function') {
    window.RadarOverview.setActiveConditions(new Set(RadarState.getActiveConditions()));
  }
  persistRadarState();
}

/**
 * syncFromRadarState – diffs RadarState → LocalState, enforces cap.
 * Desired series = visible (non-hidden) radarModels × activeConditions.
 * Hidden models' series are removed from LocalState; showing re-adds them.
 * @param {boolean|string} [eventTypeOrSuppress=false]
 *   - Pass false/undefined (default): log with default event types
 *     (condition_activate for adds, condition_inactivate for removes).
 *   - Pass true: suppress all curve logging (legacy page-restore path;
 *     use 'restore' to have the backend decide whether to log).
 *   - Pass a string event_type (e.g. 'restore', 'model_add', 'model_remove',
 *     'model_show', 'model_hide', 'radar_clear_all', 'reset_condition'):
 *     overrides the event_type sent for the diff. The backend handles
 *     restore-suppression logic for 'restore'.
 * @param {string} [actionId] – optional shared batch token (e.g. for clear_all).
 */
function syncFromRadarState(eventTypeOrSuppress = false, actionId) {
  // Build desired set
  const desired = new Map(); // "mid_cid" → {model_id, condition_id}
  RadarState.getModels().forEach((model, mid) => {
    if (RadarState.isModelHidden(mid)) return;
    RadarState.getActiveConditions().forEach(cidStr => {
      const key = `${model.modelId}_${Number(cidStr)}`;
      desired.set(key, { model_id: model.modelId, condition_id: Number(cidStr) });
    });
  });

  // Get current LocalState pairs
  const current = LocalState.getSelectionPairs();
  const currentMap = new Map(current.map(p => [`${p.model_id}_${p.condition_id}`, p]));

  // Remove pairs not in desired; collect for logging
  const removedPairs = [];
  const selected = LocalState.getSelected();
  current.forEach(p => {
    const key = `${p.model_id}_${p.condition_id}`;
    if (!desired.has(key)) {
      removedPairs.push({ model_id: p.model_id, condition_id: p.condition_id });
      const item = selected.find(it => it.model_id === p.model_id && it.condition_id === p.condition_id);
      // Use removePairSilent so browsing history is NOT updated here.
      // Browsing history is only updated by explicit addModelToHistory() calls
      // in removeModelFromRadar() and clearAllRadarModels() for true removals.
      if (item) LocalState.removePairSilent(item.key);
    }
  });

  // Add pairs in desired but not in current (cap enforced)
  const toAdd = [];
  desired.forEach((pair, key) => {
    if (!currentMap.has(key)) toAdd.push(pair);
  });
  if (toAdd.length > 0) LocalState.addPairs(toAdd);

  // Log curve add/remove events via the canonical /api/curve_set endpoint
  const suppressLog = eventTypeOrSuppress === true;
  if (!suppressLog && window.Analytics && typeof window.Analytics.logCurvePairs === 'function') {
    const overrideType = (typeof eventTypeOrSuppress === 'string') ? eventTypeOrSuppress : null;
    if (toAdd.length > 0) {
      const addEventType = overrideType || 'condition_activate';
      window.Analytics.logCurvePairs(addEventType, toAdd, 'radar', actionId);
    }
    if (removedPairs.length > 0) {
      const removeEventType = overrideType || 'condition_inactivate';
      window.Analytics.logCurvePairs(removeEventType, removedPairs, 'radar', actionId);
    }
  }

  rebuildSelectedFans(LocalState.getSelected());
  refreshChartFromLocal(false);
}

/** Wire condition-toggle and model-remove callbacks to RadarOverview (idempotent). */
function _wireRadarOverviewCallbacks() {
  if (!window.RadarOverview) return;
  if (window.RadarOverview.__callbacksWired) return;
  window.RadarOverview.__callbacksWired = true;

  if (typeof window.RadarOverview.onConditionToggle === 'function') {
    window.RadarOverview.onConditionToggle(activateRadarCondition);
  }
  if (typeof window.RadarOverview.onModelRemove === 'function') {
    window.RadarOverview.onModelRemove(removeModelFromRadar);
  }
  if (typeof window.RadarOverview.onModelHideToggle === 'function') {
    window.RadarOverview.onModelHideToggle((modelId, willHide) => {
      // Check cap before showing (willHide=false means making visible)
      if (!willHide && RadarState.computeSeriesCountIfShow(modelId) > RADAR_CURVE_CAP) {
        typeof showInfo === 'function' && showInfo(CAP_EXCEEDED_MSG);
        return false; // signal to RadarOverview to cancel
      }
      RadarState.setModelHidden(modelId, willHide);
      syncFromRadarState(willHide ? 'model_hide' : 'model_show');
      return true;
    });
  }
  if (typeof window.RadarOverview.onHighlightChange === 'function') {
    window.RadarOverview.onHighlightChange(_onRadarHighlightChange);
  }
}

// ---- Extra panel review display ----
let _lastReviewModelId = null; // remembered model id for Rule C (no current highlight)

/** Update the extra panel review display based on current highlight and radar models. */
function _updateExtraPanel(highlightedModelId) {
  const reviewBody = document.querySelector('.extra-panel-review-body');
  if (!reviewBody) return;

  const radarModelIds = Array.from(RadarState.getModels().keys()); // string[]
  let targetModelId = null;

  if (radarModelIds.length === 0) {
    // No models – clear
    targetModelId = null;
  } else if (radarModelIds.length === 1) {
    // Rule A: exactly one model always shown
    targetModelId = radarModelIds[0];
  } else {
    // Rule B/C: multiple models
    const hlStr = highlightedModelId != null ? String(highlightedModelId) : null;
    if (hlStr !== null && radarModelIds.includes(hlStr)) {
      // Rule B: current highlight is a valid radar model
      targetModelId = hlStr;
      _lastReviewModelId = targetModelId;
    } else {
      // Rule C: no current highlight – use remembered model
      if (_lastReviewModelId !== null && radarModelIds.includes(_lastReviewModelId)) {
        targetModelId = _lastReviewModelId;
      } else {
        // Fallback: first available model
        targetModelId = radarModelIds[0];
        _lastReviewModelId = targetModelId;
      }
    }
  }

  const meta = targetModelId && window.__modelMetaCache && window.__modelMetaCache[targetModelId];
  const review = (meta && meta.review) || '';
  // Use textContent to avoid XSS; CSS white-space:pre-wrap preserves spaces/newlines
  reviewBody.textContent = review || '暂无点评';
}

/** Called by RadarOverview when the effective highlighted model changes. */
function _onRadarHighlightChange(highlightedModelId) {
  _updateExtraPanel(highlightedModelId);
}

// Expose for cross-module use (fancool-search.js calls this when "添加至对比" submitted)
window.addModelToRadar = addModelToRadar;

function withFrontColors(chartData) {
  if (__isShareLoaded && !__shareAxisApplied && chartData && chartData.x_axis_type) {
    frontXAxisType = (chartData.x_axis_type === 'noise') ? 'noise_db' : chartData.x_axis_type;
    try { localStorage.setItem('x_axis_type', frontXAxisType); } catch (_) {}
    __shareAxisApplied = true;
  }

  // Group active curve keys by model so the stable slot allocator can sync all at once.
  const modelGroups = {};
  (chartData.series || []).forEach(s => {
    const mid = String(s.model_id || '');
    if (!mid || !s.key) return;
    if (!modelGroups[mid]) modelGroups[mid] = [];
    modelGroups[mid].push(s.key);
  });
  Object.entries(modelGroups).forEach(([mid, keys]) => {
    ColorManager.syncModelCurveSlots(mid, keys);
  });

  const series = (chartData.series || []).map(s => {
    const color = ColorManager.getDerivedColorForCurve(s.model_id, s.key);
    return {
      ...s,
      color,
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

// applySidebarColors: no-op now that the sidebar "added data" list has been removed.
window.applySidebarColors = function() {};

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
  syncRadarOverview(prepared.series);
}

/**
 * Sync RadarOverview visual from RadarState (source of truth).
 * If RadarState is empty but chart series are present, migrate series into RadarState
 * (backwards-compatibility: existing LocalState items are adopted into radar flow).
 * @param {Array} [series] – current chart series (used only for migration & score updates)
 */
function syncRadarOverview(series) {
  if (!window.RadarOverview) return;

  // ---- Migration: adopt existing LocalState items on first load ----
  if (RadarState.getModels().size === 0 && Array.isArray(series) && series.length > 0) {
    const addedCids = new Set();
    series.forEach(s => {
      const mid = String(s.model_id || '');
      if (!mid) return;
      if (!RadarState.hasModel(mid)) {
        const brand = s.brand || s.brand_name_zh || s.brand_name || '';
        const label = s.model || s.model_name || mid;
        RadarState.addModel(s.model_id, brand, label);
      }
      if (s.condition_id != null) addedCids.add(String(s.condition_id));
    });
    addedCids.forEach(cid => RadarState.activateCondition(cid));
    // Notify RadarOverview of current active conditions
    if (typeof window.RadarOverview.setActiveConditions === 'function') {
      window.RadarOverview.setActiveConditions(new Set(RadarState.getActiveConditions()));
    }
  }

  // ---- Build modelList from RadarState (augmented with scores from cache) ----
  const modelList = [];
  RadarState.getModels().forEach((model, mid) => {
    const radar = window.__radarCache && window.__radarCache[mid];
    const scores = {};
    if (radar && radar.conditions && typeof radar.conditions === 'object' && !Array.isArray(radar.conditions)) {
      Object.entries(radar.conditions).forEach(([cid, sc]) => {
        scores[Number(cid)] = sc.score_total;
      });
    }
    // Try to fill scores from incoming series too (in case cache not populated yet)
    if (Array.isArray(series)) {
      series.filter(s => String(s.model_id) === mid).forEach(s => {
        if (s.condition_id != null && scores[Number(s.condition_id)] == null) {
          // We don't have score_total in series directly, skip
        }
      });
    }
    modelList.push({
      modelId:  model.modelId,
      brand:    model.brand,
      label:    model.label,
      scores,
      hidden:   RadarState.isModelHidden(mid)
    });
  });

  window.RadarOverview.update(modelList);

  // Sync condition labels
  if (window.__condLabelCache && Object.keys(window.__condLabelCache).length) {
    window.RadarOverview.setConditionLabels(window.__condLabelCache);
  }
  // Sync active condition visual state
  if (typeof window.RadarOverview.setActiveConditions === 'function') {
    window.RadarOverview.setActiveConditions(new Set(RadarState.getActiveConditions()));
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
    return;
  }

  // Group by model_id for model-level mini radar cards.
  // Use canonical backend field names from /api/recent_likes → user_likes_view + fan_model join.
  const modelGroups = new Map();
  list.forEach(it => {
    const mid = it.model_id != null ? String(it.model_id) : null;
    if (!mid) return;
    if (!modelGroups.has(mid)) {
      modelGroups.set(mid, {
        modelId:   mid,
        brand:     it.brand_name_zh || it.brand || '',
        model:     it.model_name    || it.model || '',
        // Canonical fields from updated API (reference_price, max_speed, rgb_light)
        price:     it.reference_price != null ? it.reference_price : (it.price || null),
        maxSpeed:  it.max_speed   || '',
        size:      it.size        || '',
        thickness: it.thickness   || '',
        rgbLight:  it.rgb_light   || '',
      });
    }
  });

  // Fetch missing radar scores, then render cards
  const modelIds = Array.from(modelGroups.keys()).map(Number).filter(Boolean);
  _fetchMissingRadarScores(modelIds).then(() => {
    _renderRecentLikesCards(wrap, modelGroups);
  });
}

function _fetchMissingRadarScores(modelIds) {
  if (!Array.isArray(modelIds) || !modelIds.length) return Promise.resolve();
  const missing = modelIds.filter(mid => !window.__radarCache || !window.__radarCache[String(mid)]);
  if (!missing.length) return Promise.resolve();
  return fetch('/api/radar_metrics?model_ids=' + missing.map(encodeURIComponent).join(','))
    .then(r => r.json())
    .then(j => {
      const n = typeof normalizeApiResponse === 'function' ? normalizeApiResponse(j) : null;
      const models = (n && n.ok && n.data && n.data.models) || {};
      Object.entries(models).forEach(([mid, data]) => {
        if (window.__radarCache) window.__radarCache[mid] = data;
      });
    })
    .catch(() => {});
}

function _radarItemsFromCache(mid) {
  const radarData = window.__radarCache && window.__radarCache[String(mid)];
  if (!radarData || !radarData.conditions) return { items: null, score: null };
  const items = Object.entries(radarData.conditions).map(([cid, sc]) => ({
    condition_id: Number(cid),
    score_total: sc && sc.score_total != null ? sc.score_total : null
  }));
  return { items, score: radarData.composite_score != null ? radarData.composite_score : null };
}

function _renderRecentLikesCards(wrap, modelGroups) {
  const frag = document.createDocumentFragment();
  modelGroups.forEach(g => {
    const { items, score } = _radarItemsFromCache(g.modelId);
    const cardEl = buildMiniRadarCardEl({
      brand: g.brand, model: g.model, price: g.price,
      maxSpeed: g.maxSpeed, size: g.size, thickness: g.thickness, rgbLight: g.rgbLight,
      modelId: g.modelId,
      radarItems: items, compositeScore: score,
      condLabelCache: window.__condLabelCache,
      showHeader: true, showLikeThumbs: true, showQuickAdd: true,
    });
    frag.appendChild(cardEl);
  });
  wrap.innerHTML = '';
  wrap.appendChild(frag);
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
      // 通过 right-panel-v2 内聚 API 触发懒加载
      window.RightPanelV2?.loadRecentUpdatesIfNeeded?.();
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
const removedListEl  = $('#recentlyRemovedList');

// rebuildSelectedFans: bottom-panel list removed; keep side-effects (index rebuild, quick-btn sync)
function rebuildSelectedFans(fans){
  if (!Array.isArray(fans)) fans = LocalState.getSelected();
  // Assign stable color indices so withFrontColors() sees consistent palette mapping
  ColorManager.assignUniqueIndices((fans || []).map(f => f.key));
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
   搜索（Right Panel v2 统一表格）
   ========================================================= */
// Keep legacy references as null — the new table is rendered by RightPanelV2
const searchAirflowTbody = null;
const searchLikesTbody = null;
let SEARCH_RESULTS_RAW = [];

function renderSearchResults(results, conditionLabel) {
  SEARCH_RESULTS_RAW = results ? results.slice() : [];

  // Show condition label if the legacy element exists
  const labelEl = document.getElementById('searchConditionLabel');
  if (labelEl) labelEl.textContent = conditionLabel || '';

  // Update the search-summary bar above the results table
  const summaryEl = document.getElementById('srp-summary-bar');
  if (summaryEl) summaryEl.textContent = conditionLabel || '';

  // Determine current sort-by from the form
  const sortByEl = document.getElementById('sortBySelect');
  const sortBy = sortByEl ? sortByEl.value : 'condition_score';

  // Delegate to Right Panel v2 search results renderer
  if (typeof window.RightPanelV2 !== 'undefined' && typeof window.RightPanelV2.renderSearchResultsTable === 'function') {
    window.RightPanelV2.renderSearchResultsTable(SEARCH_RESULTS_RAW, sortBy);
  }
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

// When radar-overview like-mode changes a like, refresh the recent-likes sidebar.
document.addEventListener('fc:like-changed', () => { scheduleRecentLikesRefresh(); });

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
    // Sync radar overview like state (shared source of truth)
    _syncRadarLikeStateFromLocal(modelId);
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
          _syncRadarLikeStateFromLocal(modelId); // revert radar state too
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
        _syncRadarLikeStateFromLocal(modelId); // revert radar state too
        showError('网络错误：'+err.message);
      });
    return;
  }

  const quickRemove = safeClosest(e.target, '.js-list-remove');
if (quickRemove){
    // Redirect: table-row remove buttons now disabled – use radar × instead
    showInfo(RADAR_REDIRECT_MSG);
    return;
  }

 {
    const picker = ['.js-ranking-add','.js-search-add','.js-rating-add','.js-likes-add'];
    for (const sel of picker){
      const btn = safeClosest(e.target, sel);
      if (!btn) continue;
      // Redirect: quick-add buttons now disabled – use radar flow
      showInfo(RADAR_REDIRECT_MSG);
      return;
    }
  }


  const removeBtn = safeClosest(e.target, '.js-remove-fan');
  if (removeBtn){
    // Redirect: sidebar item remove now disabled – use radar × button
    showInfo(RADAR_REDIRECT_MSG);
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
window.syncTopTabsViewportHeight = syncTopTabsViewportHeight;
window.initSnapTabScrolling = initSnapTabScrolling;
window.RADAR_REDIRECT_MSG = RADAR_REDIRECT_MSG;
window._fetchMissingRadarScores = _fetchMissingRadarScores;
window._radarItemsFromCache = _radarItemsFromCache;

/* =========================================================
   雷达操作栏 (Radar Actions Bar) 事件绑定
   ========================================================= */
(function initRadarActionsBar(){
  const toggleBtn = document.getElementById('radarActionsToggle');
  const expanded  = document.getElementById('radarActionsExpanded');
  const clearModelsBtn = document.getElementById('clearRadarModels');
  const clearCondsBtn  = document.getElementById('clearRadarConds');
  if (!toggleBtn || !expanded) return;

  function collapse(){
    expanded.hidden = true;
    toggleBtn.hidden = false;
  }
  function expand(){
    expanded.hidden = false;
    toggleBtn.hidden = true;
  }

  toggleBtn.addEventListener('click', () => {
    if (expanded.hidden) { expand(); } else { collapse(); }
  });

  if (clearModelsBtn) {
    clearModelsBtn.addEventListener('click', () => {
      clearAllRadarModels();
      collapse();
      typeof showSuccess === 'function' && showSuccess('已清空所有型号');
    });
  }
  if (clearCondsBtn) {
    clearCondsBtn.addEventListener('click', () => {
      clearAllActiveConditions();
      collapse();
      typeof showSuccess === 'function' && showSuccess('已取消工况激活');
    });
  }

  // Collapse when clicking outside the actions bar
  document.addEventListener('click', (e) => {
    const bar = document.getElementById('radarActionsBar');
    if (bar && !bar.contains(e.target)) collapse();
  });
})();

/* =========================================================
   迷你雷达快速添加/移除按钮事件绑定 (two-state toggle)
   ========================================================= */
document.addEventListener('click', (e) => {
  const addBtn = safeClosest(e.target, '.js-mini-radar-add');
  if (!addBtn) return;
  const mid   = addBtn.dataset.modelId;
  const brand = addBtn.dataset.brand  || '';
  const label = addBtn.dataset.label  || '';
  if (!mid) return;
  if (RadarState.hasModel(mid)) {
    if (typeof removeModelFromRadar === 'function') removeModelFromRadar(mid);
  } else {
    if (typeof addModelToRadar === 'function') addModelToRadar(mid, brand, label);
  }
});

/**
 * Sync visual state of all visible radar toggle buttons with current RadarState.
 * Call after any radar model add/remove.
 */
function syncRadarToggleButtons() {
  if (typeof window.setRadarToggleBtnState !== 'function') return;
  if (typeof RadarState === 'undefined' || typeof RadarState.hasModel !== 'function') return;
  document.querySelectorAll('.js-mini-radar-add[data-model-id]').forEach(btn => {
    const mid = btn.dataset.modelId;
    if (!mid) return;
    window.setRadarToggleBtnState(btn, RadarState.hasModel(mid) ? 'added' : 'add');
  });
}
window.syncRadarToggleButtons = syncRadarToggleButtons;

/* =========================================================
   卡片标题遮罩 — no-op stub; masking is CSS-only via
   .fc-card-header-left background + ::after right-edge fade.
   Delegated to mini-radar-card.js; kept here for any remaining
   callers in this file.
   ========================================================= */
function applyCardHeaderMask(/* scope */) {
  // No-op: pure CSS handles priority masking now.
  // (The implementation in mini-radar-card.js overrides window.applyCardHeaderMask.)
}
// Only set if mini-radar-card.js hasn't already set it
if (typeof window.applyCardHeaderMask !== 'function') {
  window.applyCardHeaderMask = applyCardHeaderMask;
}

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

/* 排序依据输入锁定 — 同转速风量/同分贝风量需要限制值，其余不需要 */
(function initSortValueUnlockMinimal() {
  const select = document.getElementById('sortBySelect');
  const input  = document.getElementById('sortValueInput');
  if (!select || !input) return;
  function apply() {
    const needsValue = (select.value === 'rpm' || select.value === 'noise');
    input.disabled = !needsValue;
    if (!needsValue) input.value = '';
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

  // Mount radar overview panel, wire callbacks, then restore persisted radar state.
  // syncPageStateWithBackend() runs first to strip invalid/unavailable models from
  // localStorage before restoreRadarFromStorage() reads from it, ensuring the radar
  // only re-hydrates currently valid models.
  const radarPanel = document.getElementById('radarOverviewPanel');
  if (radarPanel && window.RadarOverview) {
    window.RadarOverview.mount(radarPanel);
    _wireRadarOverviewCallbacks();
    syncPageStateWithBackend().then(() => restoreRadarFromStorage());
  } else {
    // No radar panel on this page, but still sync browsing history validity.
    syncPageStateWithBackend();
  }
});

/* 最近点赞标题渐隐宽度测量 — delegate to generalized applyCardHeaderMask */
function applyRecentLikesTitleMask() {
  applyCardHeaderMask(document.getElementById('recentLikesList'));
}
// Note: resize is already handled by the global __cardHeaderMaskRaf listener above.

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
