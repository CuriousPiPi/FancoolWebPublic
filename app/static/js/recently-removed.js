(function initRecentlyRemovedFeature(){
  window.__APP = window.__APP || {};
  window.__APP.features = window.__APP.features || {};

  // Create the shared label promise early so fancool.js does not render with raw IDs.
  if (!(window.__condLabelCacheReady instanceof Promise)) {
    window.__condLabelCacheReady = new Promise(function(resolve) {
      window.__condLabelCacheReadyResolve = resolve;
    });
  }

  function getRemovedListEl(){ return document.getElementById('recentlyRemovedList'); }
  function selectedKeySetFromState(){
    const set = new Set();
    try {
      const sel = (window.LocalState && window.LocalState.getSelected && window.LocalState.getSelected()) || [];
      sel.forEach(it => { if (it && it.key) set.add(it.key); });
    } catch(_){}
    return set;
  }
  const htmlEscape = (s)=> (typeof window.escapeHtml === 'function') ? window.escapeHtml(s) : String(s ?? '');

  let _lastList = null; // stored for profile-switch re-render

  function _ensureCondLabels() {
    return (window.__condLabelCacheReady instanceof Promise)
      ? window.__condLabelCacheReady
      : Promise.resolve(window.__condLabelCache || {});
  }

  function _fetchModelMetaForHistory(modelIds) {
    if (typeof window._fetchAndCacheModelMeta === 'function') {
      return window._fetchAndCacheModelMeta(modelIds);
    }
    // Inline fallback if fancool.js has not exposed the shared helper yet.
    if (!Array.isArray(modelIds) || !modelIds.length) return Promise.resolve();
    const need = modelIds.filter(mid => {
      const cached = window.__modelMetaCache && window.__modelMetaCache[String(mid)];
      return !cached || !cached.model;
    });
    if (!need.length) return Promise.resolve();
    return fetch('/api/model_meta?model_ids=' + need.map(encodeURIComponent).join(','))
      .then(r => r.json())
      .then(j => {
        const models = (j && j.data && j.data.models) || {};
        window.__modelMetaCache = window.__modelMetaCache || {};
        Object.entries(models).forEach(([mid, meta]) => {
          window.__modelMetaCache[mid] = meta;
        });
      })
      .catch(() => {});
  }

  function _fetchBrowsingHistoryRadar(modelIds) {
    if (typeof window._fetchMissingRadarScores === 'function') {
      return window._fetchMissingRadarScores(modelIds);
    }
    if (!Array.isArray(modelIds) || !modelIds.length) return Promise.resolve();
    const missing = modelIds.filter(mid => !window.__radarCache || !window.__radarCache[String(mid)]);
    if (!missing.length) return Promise.resolve();
    return fetch('/api/radar_metrics?model_ids=' + missing.map(encodeURIComponent).join(','))
      .then(r => r.json())
      .then(j => {
        if (!j) return;
        const n = typeof window.normalizeApiResponse === 'function' ? window.normalizeApiResponse(j) : null;
        const models = (n && n.ok && n.data && n.data.models) || (j.data && j.data.models) || {};
        Object.entries(models).forEach(([mid, data]) => {
          if (window.__radarCache) window.__radarCache[mid] = data;
        });
      })
      .catch(() => {});
  }

  function _renderBrowsingHistoryCards(removedListEl, modelMap, condLabelCache) {
    removedListEl.innerHTML = '';
    if (modelMap.size === 0) {
      removedListEl.innerHTML = '<p class="text-gray-500 text-center py-6 empty-removed">' + htmlEscape('暂无浏览历史') + '</p>';
      return;
    }

    const labelCache = Object.assign({}, condLabelCache || {}, window.__condLabelCache || {});

    const frag = document.createDocumentFragment();
    modelMap.forEach(function(g) {
      const mid = String(g.modelId);
      const radarData = window.__radarCache && window.__radarCache[mid];
      let radarItems = null;
      let compositeScore = null;
      if (radarData && radarData.conditions) {
        radarItems = Object.entries(radarData.conditions).map(function(entry) {
          return { condition_id: Number(entry[0]), score_total: entry[1] && entry[1].score_total != null ? entry[1].score_total : null };
        });
        compositeScore = radarData.composite_score != null ? radarData.composite_score : null;
      }

      const meta = (window.__modelMetaCache && window.__modelMetaCache[mid]) || {};

      if (typeof window.buildMiniRadarCardEl === 'function') {
        const cardEl = window.buildMiniRadarCardEl({
          brand:     g.brand     || meta.brand     || '',
          model:     g.model     || meta.model     || '',
          price:     g.reference_price != null ? g.reference_price
                     : (meta.reference_price != null ? meta.reference_price : null),
          maxSpeed:  g.max_speed  || meta.max_speed  || null,
          size:      g.size       || meta.size       || null,
          thickness: g.thickness  || meta.thickness  || null,
          rgbLight:  g.rgb_light  || meta.rgb_light  || null,
          modelId: g.modelId,
          radarItems: radarItems, compositeScore: compositeScore,
          condLabelCache: labelCache,
          showHeader: true, showLikeThumbs: false, showQuickAdd: true,
        });
        frag.appendChild(cardEl);
      }
    });
    removedListEl.appendChild(frag);

    requestAnimationFrame(function() {
      try { if (typeof window.syncTopTabsViewportHeight === 'function') window.syncTopTabsViewportHeight(); } catch(_) {}
    });
  }

  function rebuild(list){
    _lastList = list;
    const removedListEl = getRemovedListEl();
    if (!removedListEl) return;
    removedListEl.innerHTML = '';
    if (!Array.isArray(list) || list.length === 0){
      removedListEl.innerHTML = '<p class="text-gray-500 text-center py-6 empty-removed">' + htmlEscape('暂无浏览历史') + '</p>';
      return;
    }

    const selectedKeys = selectedKeySetFromState();

    // Group by model_id for model-level browsing history cards
    const modelMap = new Map();
    list.forEach(function(item) {
      if (!item) return;
      if (selectedKeys.has(item.key)) return;
      const mid = item.model_id != null ? String(item.model_id) : null;
      if (!mid) return;
      if (!modelMap.has(mid)) {
        modelMap.set(mid, {
          modelId:         mid,
          brand:           item.brand           || '',
          model:           item.model           || '',
          reference_price: item.reference_price != null ? item.reference_price : null,
          max_speed:       item.max_speed        || null,
          size:            item.size             || '',
          thickness:       item.thickness        || '',
          rgb_light:       item.rgb_light        || '',
        });
      }
    });

    if (modelMap.size === 0) {
      removedListEl.innerHTML = '<p class="text-gray-500 text-center py-6 empty-removed">' + htmlEscape('暂无浏览历史') + '</p>';
      return;
    }

    const modelIds = Array.from(modelMap.keys()).map(Number).filter(Boolean);
    Promise.all([
      _fetchBrowsingHistoryRadar(modelIds),
      _fetchModelMetaForHistory(modelIds),
      _ensureCondLabels(),
    ]).then(function(results) {
      const condLabels = results[2] || {};
      _renderBrowsingHistoryCards(removedListEl, modelMap, condLabels);
    });
  }

  function mount(){
    // Quick-add button clicks handled globally via .js-mini-radar-add in fancool.js

  }

  mount();

  window.__APP.features.recentlyRemoved = { mount: mount, rebuild: rebuild };
})();
