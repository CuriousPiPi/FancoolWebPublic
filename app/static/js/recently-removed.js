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
  const i18n = () => window.FcI18n || null;
  const t = (key, params) => {
    const api = i18n();
    return api && typeof api.t === 'function' ? api.t(key, params) : '';
  };
  const pick = (obj, field) => {
    const api = i18n();
    return api && typeof api.pickLocalizedField === 'function'
      ? api.pickLocalizedField(obj, field)
      : (obj ? obj[field] : null);
  };
  function selectedKeySetFromState(){
    const set = new Set();
    try {
      const sel = (window.LocalState && window.LocalState.getSelected && window.LocalState.getSelected()) || [];
      sel.forEach(it => { if (it && it.key) set.add(it.key); });
    } catch(_){}
    return set;
  }
  let _lastList = null; // stored for profile-switch re-render
  let _renderPending = false; // deferred while the pane is hidden
  let _renderVersion = 0;
  const _cards = new Map();
  let _emptyEl = null;

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
    if (!_emptyEl || _emptyEl.parentNode !== removedListEl) {
      _emptyEl = removedListEl.querySelector('.empty-removed');
    }
    const renderLimit = Number(window.SIDEBAR_TOP_RENDER_LIMIT) || 30;
    const entries = Array.from(modelMap.values()).slice(0, renderLimit);
    const visibleIds = new Set(entries.map(g => String(g.modelId)));
    _cards.forEach(function(record, mid) {
      if (!visibleIds.has(mid) || record.el.parentNode !== removedListEl) {
        record.el.remove();
        _cards.delete(mid);
      }
    });

    if (modelMap.size === 0) {
      if (!_emptyEl || _emptyEl.parentNode !== removedListEl) {
        _emptyEl = document.createElement('p');
        _emptyEl.className = 'text-gray-500 text-center py-6 empty-removed';
        removedListEl.appendChild(_emptyEl);
      }
      _emptyEl.textContent = t('sidebar.noHistory') || '暂无浏览历史';
      return;
    }
    if (_emptyEl) {
      _emptyEl.remove();
      _emptyEl = null;
    }

    const labelCache = Object.assign({}, condLabelCache || {}, window.__condLabelCache || {});
    let cursor = removedListEl.firstChild;
    entries.forEach(function(g) {
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
        const cfg = {
          brand:     meta.brand  || g.brand        || '',
          brand_name_zh: meta.brand_name_zh || g.brand || meta.brand || '',
          brand_name_en: meta.brand_name_en || '',
          model:     pick(meta, 'model_name') || pick(g, 'model_name') || g.model || meta.model || '',
          model_name_zh: meta.model_name_zh || g.model_name_zh || g.model || meta.model || '',
          model_name_en: meta.model_name_en || g.model_name_en || '',
          price:     meta.reference_price != null ? meta.reference_price
                     : (g.reference_price != null ? g.reference_price : null),
          maxSpeed:  meta.max_speed || g.max_speed || null,
          size:      meta.size || g.size || null,
          thickness: meta.thickness || g.thickness || null,
          rgbTypeName: pick(meta, 'rgb_names') || g.rgbTypeName || null,
          modelId: g.modelId,
          radarItems: radarItems, compositeScore: compositeScore,
          condLabelCache: labelCache,
          radarSource: 'browsing_history',
          showHeader: true, showLikeThumbs: false, showQuickAdd: true,
        };
        const signature = JSON.stringify({
          cfg,
          language: i18n() && i18n().getLang ? i18n().getLang() : '',
          whistleValue: meta.whistle_value,
          whistleBaseline: meta.whistle_baseline_condition_id,
          whistlePoints: meta.whistle_points,
          whistleMissing: meta.whistle_data_missing,
        });
        let record = _cards.get(mid);
        if (!record || record.signature !== signature) {
          const cardEl = window.buildMiniRadarCardEl(cfg);
          if (record) {
            removedListEl.replaceChild(cardEl, record.el);
            if (cursor === record.el) cursor = cardEl;
          }
          record = { el: cardEl, signature };
          _cards.set(mid, record);
        }
        if (record.el !== cursor) removedListEl.insertBefore(record.el, cursor);
        cursor = record.el.nextSibling;
      }
    });

    requestAnimationFrame(function() {
      try { if (typeof window.syncTopTabsViewportHeight === 'function') window.syncTopTabsViewportHeight(); } catch(_) {}
    });
  }

  function rebuild(list){
    _lastList = list;
    const version = ++_renderVersion;
    const removedListEl = getRemovedListEl();
    if (!removedListEl) return;

    // 页签未激活 / 侧栏收起时不建卡，等 flushPending 在可见时补渲染。
    if (typeof window.shouldRenderSidebarTopPane === 'function' &&
        !window.shouldRenderSidebarTopPane('recent-removed')) {
      _renderPending = true;
      return;
    }
    _renderPending = false;

    const selectedKeys = selectedKeySetFromState();

    // Group by model_id for model-level browsing history cards
    const modelMap = new Map();
    (Array.isArray(list) ? list : []).forEach(function(item) {
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
          rgbTypeName: pick(item, 'rgb_names') || '',
        });
      }
    });

    _renderBrowsingHistoryCards(removedListEl, modelMap, window.__condLabelCache);
    if (modelMap.size === 0) return;

    const modelIds = Array.from(modelMap.keys()).map(Number).filter(Boolean);
    Promise.all([
      _fetchBrowsingHistoryRadar(modelIds),
      _fetchModelMetaForHistory(modelIds),
      _ensureCondLabels(),
    ]).then(function(results) {
      if (version !== _renderVersion) return;
      if (typeof window.shouldRenderSidebarTopPane === 'function' &&
          !window.shouldRenderSidebarTopPane('recent-removed')) {
        _renderPending = true;
        return;
      }
      const condLabels = results[2] || {};
      _renderBrowsingHistoryCards(removedListEl, modelMap, condLabels);
    }).catch(function(err) {
      console.warn('Failed to refresh browsing history cards:', err);
    });
  }

  function mount(){
  }

  function flushPending(){
    if (!_renderPending) return;
    if (typeof window.shouldRenderSidebarTopPane === 'function' &&
        !window.shouldRenderSidebarTopPane('recent-removed')) return;
    rebuild(_lastList);
  }

  mount();

  window.__APP.features.recentlyRemoved = { mount: mount, rebuild: rebuild, flushPending: flushPending };
})();
