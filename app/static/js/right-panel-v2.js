(function initRightPanelV2() {
  'use strict';

  // Canonical radar condition IDs in CW display order (UL→UR→R→LR→LL→L).
  // Sourced from window.RADAR_CIDS_COLOR (set by mini-radar-card.js, which reads
  // window.APP_CONFIG.radarCids from the backend). Must NOT be hardcoded here so
  // that right-panel rankings, search results, and recent-updates all honour the
  // same environment-variable-driven canonical set as the main radar overview.
  const _DEFAULT_RADAR_CIDS_CCW = [1, 10, 7, 8, 3, 2];
  const _DEFAULT_CONDITION_DISPLAY_ORDER = [2, 3, 10, 7, 11, 1];
  const _N_DEFAULT = _DEFAULT_RADAR_CIDS_CCW.length;
  const _DEFAULT_RADAR_CIDS_CW = Array.from(
    {length: _N_DEFAULT}, (_, j) => _DEFAULT_RADAR_CIDS_CCW[(_N_DEFAULT - j) % _N_DEFAULT]
  );
  // mini-radar-card.js is loaded before right-panel-v2.js (see app_frontend/src/app.js)
  // and exposes window.RADAR_CIDS_COLOR in the same CW slot order used for label rendering.
  const _HAS_VALID_RADAR_CIDS =
    Array.isArray(window.RADAR_CIDS_COLOR) && window.RADAR_CIDS_COLOR.length === _N_DEFAULT;
  if (window.RADAR_CIDS_COLOR != null && !_HAS_VALID_RADAR_CIDS) {
    console.warn(
      '[right-panel-v2] Invalid window.RADAR_CIDS_COLOR; expected',
      _N_DEFAULT,
      'entries in CW order. Falling back to defaults.',
      window.RADAR_CIDS_COLOR
    );
  }
  const RADAR_CIDS = _HAS_VALID_RADAR_CIDS
    ? window.RADAR_CIDS_COLOR
    : _DEFAULT_RADAR_CIDS_CW;
  const _LIGHTING_BOARD_TOOLTIP_TEXT = [
    '灯效榜按RGB灯效获赞数排序。灯效点赞方式：',
    '1. 将支持RGB的型号添加到下方雷达图。',
    '2. 点击雷达图下方该型号左侧的大拇指进入点赞模式。',
    '3. 点击雷达图上方RGB标签左侧的大拇指完成点赞。',
  ].join('<br>');
  // Keep this tooltip formula in sync with _build_canonical_facts() in app/scoring_system.py.
  const _HEAT_TOOLTIP_FORMULA_PREFIX = '热度值 = ';

  function _normalizeConditionOrder(raw) {
    if (!Array.isArray(raw)) return _DEFAULT_CONDITION_DISPLAY_ORDER.slice();
    const out = [];
    const seen = new Set();
    raw.forEach((v) => {
      const n = Number(v);
      if (!Number.isInteger(n) || n <= 0 || seen.has(n)) return;
      seen.add(n);
      out.push(n);
    });
    return out.length ? out : _DEFAULT_CONDITION_DISPLAY_ORDER.slice();
  }

  function getConditionDisplayOrder() {
    return _normalizeConditionOrder(window.APP_CONFIG?.conditionDisplayOrder);
  }

  function _normalizeHeatMetricCount(value) {
    const n = Number(value);
    return Number.isFinite(n) ? Math.max(0, Math.round(n)) : 0;
  }

  function buildHeatTooltipText(item) {
    const queryCount = _normalizeHeatMetricCount(item?.query_count);
    const likeCount = _normalizeHeatMetricCount(item?.like_count);
    const lightingLikeCount = _normalizeHeatMetricCount(item?.lighting_like_count);
    const totalLikes = likeCount + lightingLikeCount;
    const radarAddCount = _normalizeHeatMetricCount(item?.radar_add_count);
    return `${_HEAT_TOOLTIP_FORMULA_PREFIX}雷达加入次数(${radarAddCount})×2 + 工况查询数(${queryCount}) + 点赞数(${totalLikes})×10`;
  }
  // Col counts must match the visible cells in each mode.

  // ---- 排行榜 (board) ----
  // Wide: rank | brand | model | size | rgb | price | score | action  = 8 cols
  const BOARD_WIDE_COL_DEFS = [
    'rpv2-cg-rank',
    'rpv2-cg-brand',
    'rpv2-cg-model',   // greedy elastic (width:100% in CSS)
    'rpv2-cg-size',
    'rpv2-cg-rgb',
    'rpv2-cg-price',
    'rpv2-cg-score',
    'rpv2-cg-action',
  ];
  // Narrow: rank | 型号信息 | score | action  = 4 cols
  const BOARD_NARROW_COL_DEFS = [
    'rpv2-cg-rank',
    'rpv2-cg-narrow',  // elastic (width:auto)
    'rpv2-cg-score',
    'rpv2-cg-action',
  ];

  // ---- 搜索结果 normal mode (with col-A / col-B) ----
  // Wide: brand | model | size | rgb | price | score | col-A | col-B | action  = 9 cols
  const SEARCH_WIDE_COL_DEFS = [
    'rpv2-cg-brand',
    'rpv2-cg-model',   // greedy elastic (width:100% in CSS)
    'rpv2-cg-size',
    'rpv2-cg-rgb',
    'rpv2-cg-price',
    'rpv2-cg-score',
    'rpv2-cg-col-a',
    'rpv2-cg-col-b',
    'rpv2-cg-action',
  ];
  // Narrow colgroups — only include cols for VISIBLE cells in each mode.
  // (display:none cells are removed from the table grid per CSS spec, so subsequent
  //  cells would shift into wrong column slots if the colgroup included hidden cols.)

  // condition_score narrow: 型号信息 | 综合·工况评分 | 操作 = 3 cols (col-A & col-B hidden)
  const CONDITION_NARROW_COL_DEFS = [
    'rpv2-cg-narrow',  // elastic
    'rpv2-cg-score',
    'rpv2-cg-action',
  ];

  // airflow narrow (none/rpm/noise): 型号信息 | 综合·工况评分 | 风量 | 操作 = 4 cols (col-A hidden)
  const AIRFLOW_NARROW_COL_DEFS = [
    'rpv2-cg-narrow',  // elastic
    'rpv2-cg-score',
    'rpv2-cg-col-b',
    'rpv2-cg-action',
  ];



  // Wide: brand | model | size | rgb | price | score | action  = 7 cols
  const COMPOSITE_WIDE_COL_DEFS = [
    'rpv2-cg-brand',
    'rpv2-cg-model',   // greedy elastic (width:100% in CSS)
    'rpv2-cg-size',
    'rpv2-cg-rgb',
    'rpv2-cg-price',
    'rpv2-cg-score',
    'rpv2-cg-action',
  ];
  // Narrow: 型号信息 | score | action  = 3 cols
  const COMPOSITE_NARROW_COL_DEFS = [
    'rpv2-cg-narrow',  // elastic
    'rpv2-cg-score',
    'rpv2-cg-action',
  ];

  // ---- 近期更新 ----
  // Wide: brand | model | size | rgb | price | score | date | action  = 8 cols
  const RECENT_UPDATES_WIDE_COL_DEFS = [
    'rpv2-cg-brand',
    'rpv2-cg-model',   // greedy elastic (width:100% in CSS)
    'rpv2-cg-size',
    'rpv2-cg-rgb',
    'rpv2-cg-price',
    'rpv2-cg-score',
    'rpv2-cg-date',
    'rpv2-cg-action',
  ];
  // Narrow: 型号信息 | score | action  = 3 cols
  // (date has rpv2-col-wide so it's hidden in narrow)
  const RECENT_UPDATES_NARROW_COL_DEFS = [
    'rpv2-cg-narrow',  // elastic
    'rpv2-cg-score',
    'rpv2-cg-action',
  ];

  // Expand-row colspan constants — must equal the total col count for that table/mode.
  const BOARD_EXPAND_COLSPAN_WIDE            = 8;   // rank+brand+model+size+rgb+price+score+action
  const BOARD_EXPAND_COLSPAN_NARROW          = 4;   // rank+narrow+score+action
  const SEARCH_EXPAND_COLSPAN_WIDE           = 9;   // brand+model+size+rgb+price+score+col-A+col-B+action
  const CONDITION_EXPAND_COLSPAN_NARROW      = 3;   // narrow+score+action (condition_score narrow)
  const AIRFLOW_EXPAND_COLSPAN_NARROW        = 4;   // narrow+score+col-B+action (airflow narrow)
  const COMPOSITE_EXPAND_COLSPAN_WIDE        = 7;   // brand+model+size+rgb+price+score+action
  const COMPOSITE_EXPAND_COLSPAN_NARROW      = 3;   // narrow+score+action
  const RECENT_UPDATES_EXPAND_COLSPAN_WIDE   = 8;   // brand+model+size+rgb+price+score+date+action
  const RECENT_UPDATES_EXPAND_COLSPAN_NARROW = 3;   // narrow+score+action

  // Mirrors the .rp-narrow class and the CSS 600px breakpoint.
  function _isNarrowMode() {
    const card = document.querySelector('.fc-right-card');

    if (card && card.classList.contains('rp-narrow')) {
      return true;
    }

    if (typeof window !== 'undefined' && typeof window.matchMedia === 'function') {
      return window.matchMedia('(max-width: 600px)').matches;
    }

    return false;
  }

  function buildColgroup(colDefs) {
    const cg = document.createElement('colgroup');
    colDefs.forEach(cls => {
      const col = document.createElement('col');
      col.className = cls;
      cg.appendChild(col);
    });
    return cg;
  }

  function injectColgroup(tableEl, colDefs) {
    if (!tableEl) return;
    const first = tableEl.firstElementChild;
    if (first && first.tagName === 'COLGROUP') first.remove();
    tableEl.insertBefore(buildColgroup(colDefs), tableEl.firstChild);
  }

  function buildScoreBadge(score) {
    return window.ScoreBadgeHelper.buildScoreBadge(score);
  }

  function buildRankCell(rank) {
    const td = document.createElement('td');
    td.className = 'rpv2-rank-cell';
    if (rank === 1) {
      td.innerHTML = '<i class="fa-solid fa-medal rpv2-medal-gold" title="第一名"></i>';
    } else if (rank === 2) {
      td.innerHTML = '<i class="fa-solid fa-medal rpv2-medal-silver" title="第二名"></i>';
    } else if (rank === 3) {
      td.innerHTML = '<i class="fa-solid fa-medal rpv2-medal-bronze" title="第三名"></i>';
    } else {
      td.innerHTML = `<span class="rpv2-rank-num">${rank}</span>`;
    }
    return td;
  }

  function condLabel(cid) {
    const cache = window.__condLabelCache || {};
    return cache[cid] || String(cid);
  }

  function getScoreForItem(item) {
    if (!item) return null;
    return item.composite_score !== undefined ? item.composite_score : null;
  }

  function getCondScoresForItem(item, radarCids) {
    if (!item) return {};
    const cond = item.condition_scores || {};
    if (!radarCids) return cond;
    const result = {};
    radarCids.forEach(cid => { result[cid] = cond[cid] !== undefined ? cond[cid] : null; });
    return result;
  }

  const _galleryThumbCache = new Map();
  const _lightingThumbCache = new Map();
  const _ROW_TAP_MOVE_CANCEL = 8;
  const _ROW_TAP_LONG_PRESS_MS = 360;
  const _NARROW_COPY_LONG_PRESS_MS = 420;
  const _NARROW_COPY_MOVE_CANCEL = 8;
  const _NARROW_AUTO_MARQUEE_EDGE_PAUSE_MS = 2000;
  const _CAUTION_TOOLTIP_TEXT = '该型号据反馈可能存在设计或批次问题，购买前建议参考该型号的测试点评区。（点击右侧添加按钮加入下方雷达图，并取消激活所有工况标签即可查看）。';
  const _EXPAND_PROGRAMMATIC_SCROLL_LOCK_MS = 420;
  const _EXPAND_SCROLL_DEBOUNCE_MS = 120;
  const _EXPAND_HEIGHT_ANIM_MIN_FALLBACK_MS = 260;
  const _EXPAND_HEIGHT_ANIM_BUFFER_MS = 80;
  let _lastNarrowExpandMode = 'level2';
  let _hasPlayedNarrowExpandIntro = false;
  const _narrowAutoMarqueeState = new WeakMap();

  function _isMainRowInteractiveTarget(target) {
    if (!target || typeof target.closest !== 'function') return false;
    return !!target.closest('a,button,input,select,textarea,label,[role="button"],[data-no-row-expand],.rpv2-action-cell,.rpv2-expand-btn,.js-mini-radar-add');
  }

  function _syncExpandableRowKeyboardAccess(mainTr, isNarrow = _isNarrowMode()) {
    if (!mainTr) return;
    if (isNarrow) {
      mainTr.tabIndex = 0;
    } else {
      mainTr.removeAttribute('tabindex');
    }
  }

  function _copyTextToClipboard(text) {
    const plain = String(text || '').trim();
    if (!plain) return Promise.reject(new Error('empty copy text'));
    if (navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
      return navigator.clipboard.writeText(plain);
    }
    return new Promise((resolve, reject) => {
      const ta = document.createElement('textarea');
      ta.value = plain;
      ta.setAttribute('readonly', '');
      ta.style.position = 'fixed';
      ta.style.opacity = '0';
      ta.style.left = '-9999px';
      document.body.appendChild(ta);
      ta.focus();
      ta.select();
      try {
        const ok = document.execCommand('copy');
        document.body.removeChild(ta);
        if (ok) resolve();
        else reject(new Error('execCommand copy failed'));
      } catch (err) {
        document.body.removeChild(ta);
        reject(err);
      }
    });
  }

  function _stopNarrowAutoMarquee(td) {
    if (!td) return;
    const inner = td.querySelector('.rpv2-mi-name-inner');
    if (!inner) return;
    const state = _narrowAutoMarqueeState.get(inner);
    if (state) {
      state.active = false;
      if (Array.isArray(state.timers)) state.timers.forEach(clearTimeout);
      if (typeof state.transitionHandler === 'function') {
        inner.removeEventListener('transitionend', state.transitionHandler);
      }
      _narrowAutoMarqueeState.delete(inner);
    }
    inner.style.transition = 'transform 0.2s ease';
    inner.style.transform = 'translateX(0)';
    inner.style.willChange = '';
  }

  function _startNarrowAutoMarquee(td) {
    if (!td || !_isNarrowMode()) return;
    _stopNarrowAutoMarquee(td);
    const inner = td.querySelector('.rpv2-mi-name-inner');
    const wrap = td.querySelector('.rpv2-mi-name');
    if (!inner || !wrap) return;

    const delta = inner.scrollWidth - wrap.clientWidth;
    if (delta <= 4) return;

    const state = { active: true, timers: [], transitionHandler: null };
    _narrowAutoMarqueeState.set(inner, state);
    inner.style.willChange = 'transform';
    const queue = (fn, delay) => {
      const timer = setTimeout(() => {
        const idx = state.timers.indexOf(timer);
        if (idx >= 0) state.timers.splice(idx, 1);
        fn();
      }, delay);
      state.timers.push(timer);
    };

    const loop = () => {
      if (!state.active) return;
      const duration = Math.max(0.5, delta / _MODEL_MARQUEE_SPEED).toFixed(2);
      inner.style.transition = `transform ${duration}s linear`;
      inner.style.transform = `translateX(-${delta}px)`;
      state.transitionHandler = (ev) => {
        if (!state.active || ev.propertyName !== 'transform') return;
        inner.removeEventListener('transitionend', state.transitionHandler);
        state.transitionHandler = null;
        queue(() => {
          if (!state.active) return;
          inner.style.transition = 'transform 0.35s ease';
          inner.style.transform = 'translateX(0)';
          state.transitionHandler = (backEv) => {
            if (!state.active || backEv.propertyName !== 'transform') return;
            inner.removeEventListener('transitionend', state.transitionHandler);
            state.transitionHandler = null;
            queue(loop, _NARROW_AUTO_MARQUEE_EDGE_PAUSE_MS);
          };
          inner.addEventListener('transitionend', state.transitionHandler);
        }, _NARROW_AUTO_MARQUEE_EDGE_PAUSE_MS);
      };
      inner.addEventListener('transitionend', state.transitionHandler);
    };
    loop();
  }

  function _syncNarrowAutoMarqueeForMainRow(mainTr) {
    if (!mainTr) return;
    const td = mainTr.querySelector('td.rpv2-col-narrow-info');
    if (!td) return;
    const expandState = mainTr.dataset.expandState || 'collapsed';
    const shouldRun = _isNarrowMode() && expandState !== 'collapsed';
    if (shouldRun) _startNarrowAutoMarquee(td);
    else _stopNarrowAutoMarquee(td);
  }

  function _parseApiItems(json) {
    if (json && Array.isArray(json.items)) return json.items;
    if (json && json.data && Array.isArray(json.data.items)) return json.data.items;
    return [];
  }

  function _loadGalleryThumbUrl(modelId) {
    const mid = modelId != null ? parseInt(modelId, 10) : null;
    if (!mid || Number.isNaN(mid)) return Promise.resolve(null);
    const cacheKey = String(mid);
    if (_galleryThumbCache.has(cacheKey)) return _galleryThumbCache.get(cacheKey);

    const p = fetch(`/api/gallery/${mid}`)
      .then((r) => {
        if (!r.ok) throw new Error(`gallery fetch failed: ${r.status}`);
        return r.json();
      })
      .then((json) => {
        const items = _parseApiItems(json);
        if (!items.length) return null;
        const first = items[0] || null;
        const thumb = first && first.thumb_url ? String(first.thumb_url) : null;
        return thumb || null;
      })
      .catch(() => {
        _galleryThumbCache.delete(cacheKey);
        return null;
      });

    _galleryThumbCache.set(cacheKey, p);
    return p;
  }

  /**
   * Lighting-board variant of gallery thumb loader.
   * Prefers the `lighting_preferred_thumb_url` field returned by
   * /api/gallery/<model_id>: BV poster (local cache) when available,
   * otherwise first gallery thumb.  Falls back to null on any error.
   * Used exclusively for the lighting-board expand panel.
   */
  function _loadLightingThumbUrl(modelId) {
    const mid = modelId != null ? parseInt(modelId, 10) : null;
    if (!mid || Number.isNaN(mid)) return Promise.resolve(null);
    const cacheKey = String(mid);
    if (_lightingThumbCache.has(cacheKey)) return _lightingThumbCache.get(cacheKey);

    const p = fetch(`/api/gallery/${mid}`)
      .then((r) => {
        if (!r.ok) throw new Error(`gallery fetch failed: ${r.status}`);
        return r.json();
      })
      .then((json) => {
        const data = (json && json.data) ? json.data : json;
        const preferred = data && data.lighting_preferred_thumb_url
          ? String(data.lighting_preferred_thumb_url) : null;
        if (preferred) return preferred;
        // Fallback: first gallery thumb (matches existing non-lighting behaviour).
        const items = _parseApiItems(json);
        const first = items[0] || null;
        return (first && first.thumb_url) ? String(first.thumb_url) : null;
      })
      .catch(() => {
        _lightingThumbCache.delete(cacheKey);
        return null;
      });

    _lightingThumbCache.set(cacheKey, p);
    return p;
  }

  function buildMiniRadarPanel(item) {
    const radarCol = document.createElement('div');
    radarCol.className = 'rpv2-mini-radar-col';

    const resolvedCond = getCondScoresForItem(item, RADAR_CIDS);
    const hasScores = RADAR_CIDS.some(cid => {
      const s = resolvedCond[cid];
      return s !== null && s !== undefined;
    });

    if (hasScores && typeof window.buildMiniRadarSVG === 'function') {
      const resolvedScore = getScoreForItem(item);
      const radarItems = RADAR_CIDS.map(cid => ({
        condition_id: cid,
        score_total: resolvedCond[cid] !== undefined ? resolvedCond[cid] : null,
      }));
      const labelCache = window.__condLabelCache || {};
      const radarBody = document.createElement('div');
      radarBody.className = 'rpv2-mini-radar-body';
      radarBody.innerHTML = window.buildMiniRadarSVG(
        radarItems,
        resolvedScore,
        labelCache,
        null,
        { showLikeThumbs: false }
      );
      radarCol.appendChild(radarBody);
    } else {
      const ph = document.createElement('div');
      ph.className = 'rpv2-mini-radar-placeholder';
      ph.innerHTML =
        '<i class="fa-solid fa-chart-radar" style="font-size:20px;opacity:0.4"></i>' +
        '<span>综合评分数据暂未加载</span>' +
        '<span style="font-size:10px;color:#d1d5db">（首次查询该型号后评分计算完成）</span>';
      radarCol.appendChild(ph);
    }

    return radarCol;
  }

  function buildConditionMetricPanel(item, metricType) {
    const isLikes = metricType === 'likes';
    const metricCol = document.createElement('div');
    metricCol.className = 'rpv2-heat-breakdown-col';

    const title = document.createElement('div');
    title.className = 'rpv2-heat-breakdown-title';
    title.textContent = isLikes ? '工况好评' : '工况热度';
    metricCol.appendChild(title);

    const condRows = document.createElement('div');
    condRows.className = 'rpv2-cond-rows';

    const metricMap = isLikes ? (item.condition_likes || {}) : (item.condition_heat || {});
    const totalHeat = item.heat_score || 0;
    const displayOrder = getConditionDisplayOrder().slice();
    const seeded = new Set(displayOrder);
    Object.keys(metricMap || {}).forEach((key) => {
      const cid = Number(key);
      if (!Number.isInteger(cid) || cid <= 0 || seeded.has(cid)) return;
      seeded.add(cid);
      displayOrder.push(cid);
    });
    const maxMetric = Math.max(1, ...displayOrder.map(c => Number(metricMap[c]) || 0));

    displayOrder.forEach(cid => {
      const rawVal = metricMap[cid] || 0;
      const barW = Math.round((rawVal / maxMetric) * 100);

      const row = document.createElement('div');
      row.className = 'rpv2-cond-row';

      const label = condLabel(cid);
      const nameEl = document.createElement('span');
      nameEl.className = 'rpv2-cond-name';
      nameEl.textContent = label;
      nameEl.title = label;

      const barArea = document.createElement('div');
      barArea.className = 'rpv2-cond-bar-area';

      const barTrack = document.createElement('div');
      barTrack.className = 'rpv2-cond-bar-track';
      const barFill = document.createElement('div');
      barFill.className = 'rpv2-cond-bar-fill';
      barFill.style.width = `${barW}%`;
      barTrack.appendChild(barFill);

      const valEl = document.createElement('span');
      valEl.className = 'rpv2-cond-pct';
      if (isLikes) {
        valEl.textContent = String(rawVal > 0 ? rawVal : 0);
      } else {
        const pct = totalHeat > 0
          ? Math.max(0, Math.min(100, Math.round((rawVal / totalHeat) * 100)))
          : null;
        valEl.textContent = pct !== null ? `${pct}%` : '—';
      }

      barArea.appendChild(barTrack);
      barArea.appendChild(valEl);
      row.appendChild(nameEl);
      row.appendChild(barArea);
      condRows.appendChild(row);
    });

    metricCol.appendChild(condRows);
    return metricCol;
  }

  // Schema for right-panel detail: only the compact attribute set used here.
  const RP_DETAIL_SCHEMA = [
    'bearing_type_zh',
    'speed_switch_type_name_zh',
    'chain_type_name_zh',
    'color_flags',
    'reverse_opt',
  ];

  function buildExtraInfoPanel(item, onHeightChange, loadThumb, purchaseSource) {
    const extraCol = document.createElement('section');
    extraCol.className = 'rpv2-extra-col';

    const panel = window.ModelDetailPanel.build(item, {
      showPhoto: true,
      showPurchase: true,
      schema: RP_DETAIL_SCHEMA,
      loadThumb: typeof loadThumb === 'function' ? loadThumb : _loadGalleryThumbUrl,
      onHeightChange: typeof onHeightChange === 'function' ? onHeightChange : null,
      purchaseSource: purchaseSource || null,
    });

    // Add compat classes so existing rpv2 layout CSS (rpv2-extra-top, rpv2-extra-photo,
    // rpv2-extra-attrs, etc.) continues to apply alongside the new neutral classes.
    const inner = panel.el;
    const topDiv = inner.querySelector('.fc-model-detail__top');
    if (topDiv) topDiv.classList.add('rpv2-extra-top');
    const photoDiv = inner.querySelector('.fc-model-detail__photo');
    if (photoDiv) {
      photoDiv.classList.add('rpv2-extra-photo');
      const ph = photoDiv.querySelector('.fc-model-detail__photo-placeholder');
      if (ph) ph.classList.add('rpv2-extra-photo-placeholder');
    }
    const attrsDiv = inner.querySelector('.fc-model-detail__attrs');
    if (attrsDiv) attrsDiv.classList.add('rpv2-extra-attrs');
    inner.querySelectorAll('.fc-model-detail__attr-row').forEach(r => r.classList.add('rpv2-extra-attr-row'));
    inner.querySelectorAll('.fc-model-detail__attr-key').forEach(k => k.classList.add('rpv2-extra-attr-key'));
    inner.querySelectorAll('.fc-model-detail__attr-val').forEach(v => v.classList.add('rpv2-extra-attr-val'));
    const purchaseDiv = inner.querySelector('.fc-model-detail__purchase');
    if (purchaseDiv) purchaseDiv.classList.add('rpv2-extra-purchase');

    extraCol.appendChild(inner);

    return { el: extraCol, ensureImageLoaded: panel.ensureImageLoaded };
  }

  function buildUnifiedExpandPanelController(item, metricType, loadThumb, purchaseSource) {
    const root = document.createElement('div');
    root.className = 'rpv2-expand-inner rpv2-expand-inner-unified is-level1';

    const slider = document.createElement('div');
    slider.className = 'rpv2-expand-slider';
    const track = document.createElement('div');
    track.className = 'rpv2-expand-slider-track';
    slider.appendChild(track);
    root.appendChild(slider);

    let heightHandler = null;
    const notifyHeight = () => {
      if (typeof heightHandler === 'function') heightHandler();
    };

    const extraPanel = buildExtraInfoPanel(item, notifyHeight, loadThumb, purchaseSource);
    const metricsCol = document.createElement('section');
    metricsCol.className = 'rpv2-metrics-col';
    metricsCol.appendChild(buildMiniRadarPanel(item));
    metricsCol.appendChild(buildConditionMetricPanel(item, metricType));

    track.appendChild(extraPanel.el);
    track.appendChild(metricsCol);

    let mode = 'level1';
    let modeChangeHandler = null;
    let scrollDebounceTimer = null;
    let programmaticScrollUntil = 0;

    const notifyMode = (nextMode, meta) => {
      if (typeof modeChangeHandler === 'function') modeChangeHandler(nextMode, meta || {});
    };

    const syncLayout = (opts = {}) => {
      const options = opts || {};
      const narrow = _isNarrowMode();
      root.classList.toggle('is-narrow-slider', narrow);
      if (narrow) {
        const pageH = Math.max(extraPanel.el.scrollHeight, metricsCol.scrollHeight);
        slider.style.height = `${pageH}px`;
        const pageW = slider.clientWidth || 0;
        const targetLeft = mode === 'level2' ? 0 : pageW;
        if (options.reposition !== false && pageW > 0 && Math.abs(slider.scrollLeft - targetLeft) > 1) {
          programmaticScrollUntil = Date.now() + _EXPAND_PROGRAMMATIC_SCROLL_LOCK_MS;
          if (options.smooth === true) {
            slider.scrollTo({ left: targetLeft, behavior: 'smooth' });
          } else {
            slider.scrollLeft = targetLeft;
          }
        }
      } else {
        slider.style.height = '';
      }
      notifyHeight();
    };

    const scrollToModeView = (targetMode, opts = {}) => {
      if (!_isNarrowMode()) return;
      const pageW = slider.clientWidth || 0;
      if (!pageW) return;
      const behavior = opts.behavior === 'smooth' ? 'smooth' : 'auto';
      const left = targetMode === 'level2' ? 0 : pageW;
      programmaticScrollUntil = Date.now() + _EXPAND_PROGRAMMATIC_SCROLL_LOCK_MS;
      if (behavior === 'smooth') {
        slider.scrollTo({ left, behavior: 'smooth' });
      } else {
        slider.scrollLeft = left;
      }
    };

    const setMode = (nextMode, opts = {}) => {
      const options = opts || {};
      mode = nextMode === 'level2' ? 'level2' : 'level1';
      root.classList.toggle('is-level1', mode === 'level1');
      root.classList.toggle('is-level2', mode === 'level2');

      if (mode === 'level2') {
        extraPanel.ensureImageLoaded();
      }

      syncLayout({ reposition: false });
      notifyMode(mode, { source: options.source || 'set-mode' });
      if (_isNarrowMode() && !options.skipScroll) {
        scrollToModeView(mode, { behavior: options.behavior || 'smooth' });
      }
    };

    const measureHeight = () => {
      if (_isNarrowMode()) {
        const pageH = Math.max(extraPanel.el.scrollHeight, metricsCol.scrollHeight);
        const cs = window.getComputedStyle(root);
        const padY = (parseFloat(cs.paddingTop) || 0) + (parseFloat(cs.paddingBottom) || 0);
        return Math.ceil(pageH + padY);
      }
      return Math.ceil(root.scrollHeight);
    };

    if (typeof ResizeObserver !== 'undefined') {
      const ro = new ResizeObserver(() => syncLayout());
      ro.observe(extraPanel.el);
      ro.observe(metricsCol);
    }

    const syncModeByScroll = (source) => {
      if (!_isNarrowMode()) return;
      if (Date.now() < programmaticScrollUntil) return;
      const pageW = slider.clientWidth || 0;
      if (!pageW) return;
      const nextMode = (slider.scrollLeft < pageW / 2) ? 'level2' : 'level1';
      if (nextMode === mode) return;
      mode = nextMode;
      root.classList.toggle('is-level1', mode === 'level1');
      root.classList.toggle('is-level2', mode === 'level2');
      if (mode === 'level2') extraPanel.ensureImageLoaded();
      notifyMode(mode, { source: source || 'native-scroll' });
      notifyHeight();
    };
    slider.addEventListener('scroll', () => {
      if (scrollDebounceTimer) clearTimeout(scrollDebounceTimer);
      scrollDebounceTimer = setTimeout(() => {
        scrollDebounceTimer = null;
        syncModeByScroll('scroll-debounce');
      }, _EXPAND_SCROLL_DEBOUNCE_MS);
    }, { passive: true });
    slider.addEventListener('scrollend', () => syncModeByScroll('scrollend'));

    return {
      el: root,
      setMode,
      syncLayout,
      measureHeight,
      scrollToModeView,
      setHeightChangeHandler(handler) { heightHandler = handler; },
      setModeChangeHandler(handler) { modeChangeHandler = handler; },
    };
  }

  // Animate expand/collapse and keep the opened panel in view.
  function wireExpandRow(expandBtn, mainTr, expandTd, expandWrap, expandTr, item, metricType, onOpen, loadThumb, purchaseSource) {
    let panelBuilt = false;
    let expandGen = 0;
    let state = 'collapsed';
    let controller = null;
    let touchCtx = null;
    let suppressClickUntil = 0;
    let heightAnimTimer = null;

    const applyExpandState = (nextState) => {
      state = nextState;
      const willOpen = state !== 'collapsed';
      expandBtn.setAttribute('aria-expanded', String(willOpen));
      expandBtn.setAttribute('data-tooltip', willOpen ? '收起详情' : '展开详情');
      expandBtn.classList.toggle('is-level2', state === 'level2');
      expandTr.dataset.expandState = state;
      if (mainTr) mainTr.dataset.expandState = state;
      _syncNarrowAutoMarqueeForMainRow(mainTr);
    };

    const syncExpandHeight = (opts = {}) => {
      if (state === 'collapsed' || !controller) return;
      const options = opts || {};
      const animate = options.animate !== false;
      const duration = Number.isFinite(options.duration) ? options.duration : 200;
      const currentH = expandWrap.getBoundingClientRect().height;
      const targetH = controller.measureHeight();
      if (Math.abs(targetH - currentH) < 1) {
        if (expandWrap.style.height && expandWrap.style.height !== 'auto') expandWrap.style.height = 'auto';
        return;
      }
      if (expandWrap.style.height === '' || expandWrap.style.height === 'auto') {
        expandWrap.style.height = `${currentH}px`;
      }
      if (heightAnimTimer) {
        clearTimeout(heightAnimTimer);
        heightAnimTimer = null;
      }
      if (!animate) {
        expandWrap.style.transition = '';
        expandWrap.style.height = `${targetH}px`;
        expandWrap.style.height = 'auto';
        return;
      }
      void expandWrap.offsetHeight;
      expandWrap.style.transition = `height ${Math.max(0, duration)}ms ease`;
      expandWrap.style.height = `${targetH}px`;
      const onHeightEnd = (ev) => {
        if (ev.target !== expandWrap || ev.propertyName !== 'height') return;
        expandWrap.removeEventListener('transitionend', onHeightEnd);
        if (state !== 'collapsed') {
          expandWrap.style.transition = '';
          expandWrap.style.height = 'auto';
        }
      };
      expandWrap.addEventListener('transitionend', onHeightEnd);
      heightAnimTimer = setTimeout(() => {
        heightAnimTimer = null;
        expandWrap.removeEventListener('transitionend', onHeightEnd);
        if (state !== 'collapsed') {
          expandWrap.style.transition = '';
          expandWrap.style.height = 'auto';
        }
      }, Math.max(_EXPAND_HEIGHT_ANIM_MIN_FALLBACK_MS, duration + _EXPAND_HEIGHT_ANIM_BUFFER_MS));
    };

    const ensurePanel = () => {
      if (panelBuilt) return;
      controller = buildUnifiedExpandPanelController(item, metricType, loadThumb, purchaseSource);
      controller.setHeightChangeHandler(() => syncExpandHeight({ animate: true, duration: 200 }));
      controller.setModeChangeHandler((nextMode) => {
        if (state === 'collapsed') return;
        applyExpandState(nextMode === 'level2' ? 'level2' : 'level1');
        if (_isNarrowMode()) {
          _lastNarrowExpandMode = nextMode === 'level2' ? 'level2' : 'level1';
        }
      });
      expandWrap.appendChild(controller.el);
      panelBuilt = true;
    };

    expandTr._rpv2SyncLayout = () => {
      if (!panelBuilt || !controller || state === 'collapsed') return;
      controller.syncLayout();
      syncExpandHeight({ animate: false });
    };

    const syncOpenInView = () => {
      const scroller = expandTr.closest('.fc-rank-scroll');
      if (!scroller) return;
      const scrollerRect = scroller.getBoundingClientRect();
      const wrapRect = expandWrap.getBoundingClientRect();
      const bottomOverflow = wrapRect.bottom - scrollerRect.bottom;
      if (bottomOverflow > 0) scroller.scrollBy({ top: bottomOverflow, behavior: 'smooth' });
    };

    const animateToState = (nextState, source) => {
      const prevState = state;
      const wasOpen = prevState !== 'collapsed';
      const willOpen = nextState !== 'collapsed';
      const gen = ++expandGen;
      ensurePanel();

      const isNarrow = _isNarrowMode();
      const shouldPlayNarrowIntro =
        isNarrow &&
        !wasOpen &&
        willOpen &&
        source === 'row' &&
        !_hasPlayedNarrowExpandIntro &&
        nextState === 'level2';

      if (willOpen && controller) {
        if (shouldPlayNarrowIntro) {
          controller.setMode('level1', { behavior: 'auto', source });
        } else if (isNarrow && !wasOpen) {
          controller.setMode(nextState, { behavior: 'auto', source });
        } else {
          controller.setMode(nextState, { source });
        }
      }

      let doneOnce = false;
      const done = () => {
        if (doneOnce) return;
        doneOnce = true;
        if (expandGen !== gen) return;
        expandWrap.style.transition = '';
        expandWrap.style.height = willOpen ? 'auto' : '';
        if (willOpen) syncOpenInView();
        if (
          shouldPlayNarrowIntro &&
          controller &&
          state !== 'collapsed' &&
          expandTr.classList.contains('is-open')
        ) {
          controller.setMode('level2', { behavior: 'smooth', source });
          _lastNarrowExpandMode = 'level2';
          _hasPlayedNarrowExpandIntro = true;
        }
      };

      if (!wasOpen && willOpen) {
        expandWrap.style.transition = 'none';
        expandWrap.style.height = '0';
        void expandWrap.offsetHeight;
        expandTr.classList.add('is-open');
        expandWrap.style.transition = 'height 0.26s ease';
        expandWrap.style.height = `${controller ? controller.measureHeight() : 0}px`;
        if (typeof onOpen === 'function') onOpen();
      } else if (wasOpen && !willOpen) {
        const currentH = expandWrap.getBoundingClientRect().height;
        expandWrap.style.transition = 'none';
        expandWrap.style.height = `${currentH}px`;
        void expandWrap.offsetHeight;
        expandWrap.style.transition = 'height 0.26s ease';
        expandWrap.style.height = '0';
        expandTr.classList.remove('is-open');
      } else if (wasOpen && willOpen) {
        const currentH = expandWrap.getBoundingClientRect().height;
        const targetH = controller ? controller.measureHeight() : currentH;
        expandWrap.style.transition = 'none';
        expandWrap.style.height = `${currentH}px`;
        void expandWrap.offsetHeight;
        expandWrap.style.transition = 'height 0.22s ease';
        expandWrap.style.height = `${targetH}px`;
      }

      applyExpandState(nextState);

      const onTransEnd = (ev) => {
        if (ev.target !== expandWrap || ev.propertyName !== 'height') return;
        expandWrap.removeEventListener('transitionend', onTransEnd);
        done();
      };
      expandWrap.addEventListener('transitionend', onTransEnd);
      setTimeout(done, 420);
    };

    const onBtnClick = (e) => {
      e.stopPropagation();
      let next = 'level1';
      if (state === 'collapsed') next = 'level1';
      else if (state === 'level1') next = 'collapsed';
      else if (state === 'level2') next = 'level1';
      animateToState(next, 'btn');
    };

    const onMainRowClick = (e) => {
      if (Date.now() < suppressClickUntil) return;
      if (_isMainRowInteractiveTarget(e.target)) return;
      let next = 'level2';
      if (_isNarrowMode()) {
        if (state === 'collapsed') {
          if (_hasPlayedNarrowExpandIntro) {
            next = _lastNarrowExpandMode;
          } else {
            next = 'level2';
          }
        } else {
          next = 'collapsed';
        }
      } else {
        if (state === 'collapsed') next = 'level2';
        else if (state === 'level1') next = 'level2';
        else if (state === 'level2') next = 'collapsed';
      }
      animateToState(next, 'row');
    };

    const onMainRowKeydown = (e) => {
      if (!_isNarrowMode()) return;
      if (e.defaultPrevented) return;
      if (e.key !== 'Enter' && e.key !== ' ') return;
      if (_isMainRowInteractiveTarget(e.target)) return;
      e.preventDefault();
      let next = 'level2';
      if (_isNarrowMode()) {
        if (state === 'collapsed') {
          if (_hasPlayedNarrowExpandIntro) {
            next = _lastNarrowExpandMode;
          } else {
            next = 'level2';
          }
        } else {
          next = 'collapsed';
        }
      } else {
        if (state === 'collapsed') next = 'level2';
        else if (state === 'level1') next = 'level2';
        else if (state === 'level2') next = 'collapsed';
      }
      animateToState(next, 'row');
    };

    if (mainTr) {
      mainTr.classList.add('rpv2-main-row-expandable');
      _syncExpandableRowKeyboardAccess(mainTr);
      mainTr.addEventListener('click', onMainRowClick);
      mainTr.addEventListener('keydown', onMainRowKeydown);
      mainTr.addEventListener('touchstart', (e) => {
        if (_isMainRowInteractiveTarget(e.target)) {
          touchCtx = null;
          return;
        }
        const t = e.changedTouches && e.changedTouches[0];
        if (!t) return;
        touchCtx = { id: t.identifier, x: t.clientX, y: t.clientY, ts: Date.now(), moved: false };
      }, { passive: true });
      mainTr.addEventListener('touchmove', (e) => {
        if (!touchCtx) return;
        let t = null;
        for (let i = 0; i < e.changedTouches.length; i++) {
          if (e.changedTouches[i].identifier === touchCtx.id) { t = e.changedTouches[i]; break; }
        }
        if (!t) return;
        const dx = Math.abs(t.clientX - touchCtx.x);
        const dy = Math.abs(t.clientY - touchCtx.y);
        if (dx > _ROW_TAP_MOVE_CANCEL || dy > _ROW_TAP_MOVE_CANCEL) touchCtx.moved = true;
      }, { passive: true });
      mainTr.addEventListener('touchend', (e) => {
        void e;
        if (!touchCtx) return;
        const longPress = (Date.now() - touchCtx.ts) > _ROW_TAP_LONG_PRESS_MS;
        if (touchCtx.moved || longPress) suppressClickUntil = Date.now() + 400;
        touchCtx = null;
      }, { passive: true });
      mainTr.addEventListener('touchcancel', () => { touchCtx = null; }, { passive: true });
    }

    expandBtn.addEventListener('click', onBtnClick);
  }

  function buildModelCells(item) {
    const isCaution = Number(item && item.caution) === 1;
    const brandText = String(item.brand_name_zh || '').trim();
    const modelText = String(item.model_name || '').trim() || '—';

    const tdBrand = document.createElement('td');
    tdBrand.className = 'nowrap rpv2-col-wide rpv2-col-brand';
    tdBrand.textContent = brandText || '—';

    const tdModel = document.createElement('td');
    tdModel.className = 'nowrap rpv2-col-model-name-wide';
    const modelWrap = document.createElement('div');
    modelWrap.className = 'rpv2-model-name-wrap';
    const modelInner = document.createElement('span');
    modelInner.className = 'rpv2-model-name-inner';
    const modelTextSpan = document.createElement('span');
    modelTextSpan.className = 'rpv2-model-name-text';
    modelTextSpan.textContent = modelText;
    modelInner.appendChild(modelTextSpan);
    if (isCaution) {
      const cautionTag = document.createElement('span');
      cautionTag.className = 'srp-src-tag rpv2-caution-tag fc-tooltip-target fc-tip-info';
      cautionTag.textContent = '注意';
      cautionTag.setAttribute('data-tooltip', _CAUTION_TOOLTIP_TEXT);
      cautionTag.setAttribute('data-tooltip-placement', 'top');
      modelInner.appendChild(cautionTag);
    }
    if (item.max_speed != null) {
      const speedSpan = document.createElement('span');
      speedSpan.className = 'rpv2-max-speed';
      speedSpan.textContent = ` (${item.max_speed} RPM)`;
      modelInner.appendChild(speedSpan);
    }
    modelWrap.appendChild(modelInner);
    tdModel.appendChild(modelWrap);

    const tdSize = document.createElement('td');
    tdSize.className = 'nowrap rpv2-col-wide rpv2-col-size';
    const sizeStr = item.size && item.thickness
      ? `${item.size}×${item.thickness}`
      : (item.size || '—');
    tdSize.textContent = sizeStr;

    const tdRgb = document.createElement('td');
    tdRgb.className = 'nowrap rpv2-col-wide rpv2-col-rgb';
    tdRgb.textContent = item.rgb_names_zh || '—';

    const tdPrice = document.createElement('td');
    tdPrice.className = 'nowrap rpv2-col-wide rpv2-col-price';
    const priceVal = item.reference_price;
    tdPrice.textContent = priceVal != null ? `¥${priceVal}` : '—';

    const tdNarrowInfo = document.createElement('td');
    tdNarrowInfo.className = 'rpv2-col-narrow-info';
    const infoDiv = document.createElement('div');
    infoDiv.className = 'rpv2-model-info-compact';
    const nameSpan = document.createElement('span');
    nameSpan.className = 'rpv2-mi-name';
    const nameInner = document.createElement('span');
    nameInner.className = 'rpv2-mi-name-inner';
    if (brandText) {
      const brandSpan = document.createElement('span');
      brandSpan.className = 'rpv2-mi-brand-text';
      brandSpan.textContent = brandText;
      nameInner.appendChild(brandSpan);
      if (modelText && modelText !== '—') {
        nameInner.appendChild(document.createTextNode(' '));
      }
    }
    const modelKnown = modelText !== '—';
    if (!brandText || modelKnown) {
      const modelTextNarrow = document.createElement('span');
      modelTextNarrow.className = `rpv2-mi-model-text${isCaution ? ' is-caution' : ''}`;
      modelTextNarrow.textContent = modelText;
      nameInner.appendChild(modelTextNarrow);
    }
    if (item.max_speed != null) {
      const speedSpan = document.createElement('span');
      speedSpan.className = 'rpv2-max-speed';
      speedSpan.textContent = ` (${item.max_speed} RPM)`;
      nameInner.appendChild(speedSpan);
    }
    nameSpan.appendChild(nameInner);
    if (modelKnown) {
      tdNarrowInfo.dataset.copyText = [brandText, modelText].filter(Boolean).join(' ').trim();
    }
    const subSpan = document.createElement('span');
    subSpan.className = 'rpv2-mi-sub';
    const subParts = [];
    if (sizeStr && sizeStr !== '—') subParts.push({ type: 'text', value: sizeStr });
    if (priceVal != null) subParts.push({ type: 'text', value: `¥${priceVal}` });
    const rgbTypeName = String(item.rgb_names_zh || '').trim();
    const hasRgb = rgbTypeName && rgbTypeName !== '无';
    if (hasRgb) subParts.push({ type: 'rgb' });
    if (!subParts.length) {
      subSpan.textContent = '—';
    } else {
      subParts.forEach((part, idx) => {
        if (idx > 0) subSpan.appendChild(document.createTextNode(' · '));
        if (part.type === 'rgb') {
          const tag = document.createElement('span');
          tag.className = 'rpv2-mi-rgb-tag';
          tag.textContent = 'RGB';
          tag.title = `RGB灯光：${rgbTypeName}`;
          subSpan.appendChild(tag);
        } else {
          subSpan.appendChild(document.createTextNode(part.value));
        }
      });
    }
    infoDiv.appendChild(nameSpan);
    infoDiv.appendChild(subSpan);
    tdNarrowInfo.appendChild(infoDiv);

    return { tdBrand, tdModel, tdSize, tdRgb, tdPrice, tdNarrowInfo, sizeStr };
  }

  function buildScoreHeatTd(item) {
    const td = document.createElement('td');
    td.className = 'rpv2-score-heat-th';

    const cell = document.createElement('div');
    cell.className = 'rpv2-score-heat-cell';

    const expandBtn = document.createElement('button');
    expandBtn.type = 'button';
    expandBtn.className = 'rpv2-expand-btn';
    expandBtn.setAttribute('aria-expanded', 'false');
    expandBtn.setAttribute('data-tooltip', '展开详情');
    expandBtn.innerHTML = '<i class="fa-solid fa-chevron-right"></i>';

    const badge = buildScoreBadge(getScoreForItem(item));

    const sep = document.createElement('span');
    sep.className = 'rpv2-sep';
    sep.textContent = '·';

    const heatEl = document.createElement('span');
    heatEl.className = 'rpv2-heat-val';
    heatEl.innerHTML = `🔥${item.heat_score ?? 0}`;
    heatEl.title = buildHeatTooltipText(item);

    cell.appendChild(expandBtn);
    cell.appendChild(badge);
    cell.appendChild(sep);
    cell.appendChild(heatEl);
    td.appendChild(cell);

    return { td, expandBtn };
  }

  function buildScoreLightingTd(item) {
    const td = document.createElement('td');
    td.className = 'rpv2-score-heat-th';

    const cell = document.createElement('div');
    cell.className = 'rpv2-score-heat-cell';

    const expandBtn = document.createElement('button');
    expandBtn.type = 'button';
    expandBtn.className = 'rpv2-expand-btn';
    expandBtn.setAttribute('aria-expanded', 'false');
    expandBtn.setAttribute('data-tooltip', '展开详情');
    expandBtn.innerHTML = '<i class="fa-solid fa-chevron-right"></i>';

    const badge = buildScoreBadge(getScoreForItem(item));

    const sep = document.createElement('span');
    sep.className = 'rpv2-sep';
    sep.textContent = '·';

    const likeEl = document.createElement('span');
    likeEl.className = 'rpv2-heat-val';
    const likeCount = item.lighting_like_count ?? 0;
    likeEl.textContent = `👍${likeCount}`;
    likeEl.title = `灯效获赞数: ${likeCount}`;

    cell.appendChild(expandBtn);
    cell.appendChild(badge);
    cell.appendChild(sep);
    cell.appendChild(likeEl);
    td.appendChild(cell);

    return { td, expandBtn };
  }

  function buildActionTd(item, source = 'unknown') {
    const tdAction = document.createElement('td');
    tdAction.className = 'rpv2-action-cell';
    if (typeof window.buildRadarToggleBtnEl === 'function') {
      const isAdded = (typeof window.RadarState !== 'undefined' && window.RadarState.hasModel)
        ? window.RadarState.hasModel(String(item.model_id))
        : false;
      const btn = window.buildRadarToggleBtnEl({
        modelId: item.model_id,
        brand:   item.brand_name_zh || '',
        label:   item.model_name   || '',
        state:   isAdded ? 'added' : 'add',
        source,
      });
      const actionWrap = document.createElement('div');
      actionWrap.className = 'rpv2-action-wrap';
      actionWrap.appendChild(btn);
      tdAction.appendChild(actionWrap);
    }
    return tdAction;
  }

  function buildExpandRowEl(mainTr, expandBtn, item, colSpan, metricType, loadThumb, purchaseSource) {
    const expandTr = document.createElement('tr');
    expandTr.className = 'rpv2-expand-row';
    expandTr.dataset.expandFor = String(item.model_id);
    expandTr.dataset.expandState = 'collapsed';
    if (mainTr) mainTr.dataset.expandState = 'collapsed';

    const expandTd = document.createElement('td');
    expandTd.className = 'rpv2-expand-td';
    expandTd.setAttribute('colspan', String(colSpan));

    const expandWrap = document.createElement('div');
    expandWrap.className = 'rpv2-expand-td-wrap';
    expandTd.appendChild(expandWrap);
    expandTr.appendChild(expandTd);

    const onOpen = () => {
      try {
        const rawId = item.model_id;
        const mid = rawId != null ? parseInt(rawId, 10) : null;
        if (typeof window.Analytics !== 'undefined' && typeof window.Analytics.logEvent === 'function') {
          window.Analytics.logEvent({
            event_type_code: 'click_right_panel_expander',
            ...(!isNaN(mid) && mid != null ? { model_id: mid } : {}),
          });
        }
      } catch (_) {}
    };

    wireExpandRow(expandBtn, mainTr, expandTd, expandWrap, expandTr, item, metricType, onOpen, loadThumb, purchaseSource);
    return expandTr;
  }

  /* -------------------------------------------------------
     Build a single leaderboard row (main row + expand row)
     Returns: { mainTr, expandTr }
     ------------------------------------------------------- */
  function buildRow(item, boardType) {
    const tr = document.createElement('tr');
    tr.className = 'rpv2-main-row';
    tr.dataset.modelId = String(item.model_id);

    tr.appendChild(buildRankCell(item.rank));

    const { tdBrand, tdModel, tdSize, tdRgb, tdPrice, tdNarrowInfo } = buildModelCells(item);
    tr.appendChild(tdBrand);
    tr.appendChild(tdModel);
    tr.appendChild(tdSize);
    tr.appendChild(tdRgb);
    tr.appendChild(tdPrice);
    tr.appendChild(tdNarrowInfo);

    let scoreTd, expandBtn;
    if (boardType === 'lighting') {
      ({ td: scoreTd, expandBtn } = buildScoreLightingTd(item));
    } else {
      ({ td: scoreTd, expandBtn } = buildScoreHeatTd(item));
    }
    tr.appendChild(scoreTd);
    const boardSource = boardType === 'perf' ? 'ranking_perf_board'
                     : boardType === 'lighting' ? 'ranking_lighting_board'
                     : 'ranking_heat_board';
    tr.appendChild(buildActionTd(item, boardSource));

    const expandTr = buildExpandRowEl(
      tr,
      expandBtn,
      item,
      _isNarrowMode() ? BOARD_EXPAND_COLSPAN_NARROW : BOARD_EXPAND_COLSPAN_WIDE,
      'heat',
      boardType === 'lighting' ? _loadLightingThumbUrl : undefined,
      boardSource
    );
    return { mainTr: tr, expandTr };
  }

  /* -------------------------------------------------------
     Build thead for a board
     wide: 排名|品牌|型号|尺寸|RGB灯光|参考价|综合评分·热度/获赞|操作
     narrow: 排名|型号信息|综合评分·热度/获赞|操作
     -------------------------------------------------------- */
  function buildThead(thead, boardType) {
    thead.innerHTML = '';
    const tr = document.createElement('tr');

    const scoreLabel = boardType === 'lighting' ? '综合评分 · 获赞' : '综合评分 · 热度';
    const cols = [
      { label: '排名',       cls: 'rpv2-rank-cell' },
      { label: '品牌',       cls: 'nowrap rpv2-col-wide rpv2-col-brand' },
      { label: '型号',       cls: 'nowrap rpv2-col-model-name-wide' },
      { label: '尺寸',       cls: 'nowrap rpv2-col-wide rpv2-col-size' },
      { label: 'RGB灯光',    cls: 'nowrap rpv2-col-wide rpv2-col-rgb' },
      { label: '参考价',     cls: 'nowrap rpv2-col-wide rpv2-col-price' },
      { label: '型号信息',   cls: 'rpv2-col-narrow-info' },
      { label: scoreLabel,   cls: 'rpv2-score-heat-th' },
      { label: '操作',       cls: 'rpv2-action-cell' },
    ];

    cols.forEach(c => {
      const th = document.createElement('th');
      th.className = c.cls;
      if (boardType === 'lighting' && c.label === scoreLabel) {
        const labelWrap = document.createElement('span');
        labelWrap.className = 'rpv2-th-label-wrap';

        const labelText = document.createElement('span');
        labelText.textContent = c.label;

        const helpBtn = document.createElement('button');
        helpBtn.type = 'button';
        helpBtn.className = 'rpv2-th-help fc-tooltip-target fc-tip-info';
        helpBtn.setAttribute('data-tooltip', _LIGHTING_BOARD_TOOLTIP_TEXT);
        helpBtn.setAttribute('data-tooltip-placement', 'top');
        helpBtn.setAttribute('aria-label', '灯效榜排序与点赞方式说明');
        helpBtn.innerHTML = '<i class="fa-solid fa-circle-info" aria-hidden="true"></i>';

        labelWrap.appendChild(labelText);
        labelWrap.appendChild(helpBtn);
        th.appendChild(labelWrap);
      } else {
        th.textContent = c.label;
      }
      tr.appendChild(th);
    });

    thead.appendChild(tr);
  }

  /* -------------------------------------------------------
     Render a board (heat / perf / lighting) from data array
     -------------------------------------------------------- */
  function renderBoard(items, theadEl, tbodyEl, boardType) {
    // Inject colgroup matching the current layout mode.
    // Wide and narrow use separate col defs — no ghost columns.
    const narrow = _isNarrowMode();
    injectColgroup(theadEl.parentElement, narrow ? BOARD_NARROW_COL_DEFS : BOARD_WIDE_COL_DEFS);
    buildThead(theadEl, boardType);
    tbodyEl.innerHTML = '';

    if (!items || items.length === 0) {
      const tr = document.createElement('tr');
      const td = document.createElement('td');
      // Match colspan to current mode to keep colgroup and cells in sync.
      td.setAttribute('colspan', String(narrow ? BOARD_EXPAND_COLSPAN_NARROW : BOARD_EXPAND_COLSPAN_WIDE));
      td.className = 'text-center text-gray-500 py-8';
      td.style.fontSize = '14px';
      td.textContent = boardType === 'lighting' ? '当前暂无获得灯效点赞的 RGB 型号' : '暂无数据';
      tr.appendChild(td);
      tbodyEl.appendChild(tr);
      return;
    }

    items.forEach(item => {
      const { mainTr, expandTr } = buildRow(item, boardType);
      tbodyEl.appendChild(mainTr);
      tbodyEl.appendChild(expandTr);
    });
  }

  /* =============================================================
     Search Results v2 — single unified expandable table
     Reuses rpv2-* styles from right-panel-v2.css.
     Columns (wide): 品牌|型号|尺寸|RGB灯光|参考价|综合·工况评分|col-A|col-B|操作
     col-A/col-B depend on sortBy (see buildSearchThead / buildSearchResultsRow).
     Expand panel: mini radar (left) + condition likes bars (right)
     ============================================================= */

  /* -------------------------------------------------------
     Build a single search results row (main row + expand row).
     sortBy: 'condition_score' | 'none' | 'rpm' | 'noise'
     ------------------------------------------------------- */
  function buildSearchResultsRow(item, sortBy) {
    const tr = document.createElement('tr');
    tr.className = 'rpv2-main-row';
    tr.dataset.modelId = String(item.model_id);

    const { tdBrand, tdModel, tdSize, tdRgb, tdPrice, tdNarrowInfo, sizeStr } = buildModelCells(item);
    tr.appendChild(tdBrand);
    tr.appendChild(tdModel);
    tr.appendChild(tdSize);
    tr.appendChild(tdRgb);
    tr.appendChild(tdPrice);
    tr.appendChild(tdNarrowInfo);

    /* 综合 · 工况评分 — fixed combined column: expand btn + composite badge + sep + condition badge */
    const tdScore = document.createElement('td');
    tdScore.className = 'rpv2-score-heat-th';

    const scoreCell = document.createElement('div');
    scoreCell.className = 'rpv2-score-heat-cell';

    const expandBtn = document.createElement('button');
    expandBtn.type = 'button';
    expandBtn.className = 'rpv2-expand-btn';
    expandBtn.setAttribute('aria-expanded', 'false');
    expandBtn.setAttribute('data-tooltip', '展开详情');
    expandBtn.innerHTML = '<i class="fa-solid fa-chevron-right"></i>';

    // Composite score badge
    const compositeScore = getScoreForItem(item);
    const compositeBadge = buildScoreBadge(compositeScore);

    // Separator + condition score badge (can be hidden on narrow non-condition_score via CSS)
    const condSep = document.createElement('span');
    condSep.className = 'rpv2-sep srp-cond-score-sep';
    condSep.textContent = '·';

    // For condition score, use the searched condition's score from canonical payload
    let condScore = item.condition_score;
    const condBadge = buildScoreBadge(condScore);
    condBadge.className += ' srp-cond-score-badge';
    condBadge.title = '工况评分（百分制）';

    scoreCell.appendChild(expandBtn);
    scoreCell.appendChild(compositeBadge);
    scoreCell.appendChild(condSep);
    scoreCell.appendChild(condBadge);
    tdScore.appendChild(scoreCell);
    tr.appendChild(tdScore);

    /* Col A — mode-dependent */
    const tdColA = document.createElement('td');
    tdColA.className = 'srp-col-A nowrap';
    if (sortBy === 'condition_score') {
      // 最大分贝: noise at max speed
      const db = item.max_noise_db;
      tdColA.textContent = (db !== null && db !== undefined) ? Number(db).toFixed(1) : '—';
      tdColA.className += ' srp-val-secondary';
    } else {
      // 测试转速 · 分贝: show both RPM and dB together for all three airflow sort modes
      const effRpm = item.effective_rpm;
      const effDb = item.effective_noise_db;
      const src = item.effective_source; // 'raw' | 'fit'
      const rpmStr = (effRpm !== null && effRpm !== undefined) ? String(Math.round(Number(effRpm))) : null;
      const dbStr = (effDb !== null && effDb !== undefined) ? Number(effDb).toFixed(1) : null;
      if (rpmStr || dbStr) {
        if (rpmStr && dbStr) {
          tdColA.appendChild(document.createTextNode(rpmStr));
          const dotSep = document.createElement('span');
          dotSep.className = 'rpv2-sep';
          dotSep.textContent = ' · ';
          tdColA.appendChild(dotSep);
          tdColA.appendChild(document.createTextNode(dbStr));
        } else {
          tdColA.appendChild(document.createTextNode(rpmStr || dbStr));
        }
        const srcTag = document.createElement('span');
        srcTag.className = 'srp-src-tag';
        srcTag.textContent = src === 'fit' ? '拟合' : '原始';
        srcTag.dataset.src = src || 'raw';
        tdColA.appendChild(srcTag);
      } else {
        tdColA.textContent = '—';
      }
    }
    tr.appendChild(tdColA);

    /* Col B — mode-dependent: always airflow */
    const tdColB = document.createElement('td');
    tdColB.className = 'srp-col-B nowrap';
    const airflow = Number(item.effective_airflow ?? item.max_airflow ?? 0);
    if (airflow > 0) {
      tdColB.textContent = airflow.toFixed(1);
      tdColB.className += ' srp-val-airflow';
    } else {
      tdColB.textContent = '—';
    }
    tr.appendChild(tdColB);

    tr.appendChild(buildActionTd(item, 'search_results'));

    const narrowExpandColspan = sortBy === 'condition_score'
      ? CONDITION_EXPAND_COLSPAN_NARROW
      : AIRFLOW_EXPAND_COLSPAN_NARROW;
    const expandTr = buildExpandRowEl(
      tr,
      expandBtn,
      item,
      _isNarrowMode() ? narrowExpandColspan : SEARCH_EXPAND_COLSPAN_WIDE,
      'likes',
      undefined,
      'search_results'
    );
    return { mainTr: tr, expandTr };
  }

  /* -------------------------------------------------------
     Build thead for the search results table
     ------------------------------------------------------- */
  function buildSearchThead(theadEl, sortBy) {
    theadEl.innerHTML = '';
    const tr = document.createElement('tr');

    // Col-A and col-B header labels depend on sortBy
    let colALabel, colBLabel;
    if (sortBy === 'condition_score') {
      colALabel = '最大分贝';
      colBLabel = '最大风量';
    } else {
      // 'none' (全速风量), 'rpm' (同转速风量), 'noise' (同分贝风量) — all show combined RPM · dB
      colALabel = '测试转速 · 分贝';
      colBLabel = '风量';
    }

    const cols = [
      { label: '品牌',           cls: 'nowrap rpv2-col-wide rpv2-col-brand' },
      { label: '型号',           cls: 'nowrap rpv2-col-model-name-wide' },
      { label: '尺寸',           cls: 'nowrap rpv2-col-wide rpv2-col-size' },
      { label: 'RGB灯光',        cls: 'nowrap rpv2-col-wide rpv2-col-rgb' },
      { label: '参考价',         cls: 'nowrap rpv2-col-wide rpv2-col-price' },
      { label: '型号信息',       cls: 'rpv2-col-narrow-info' },
      { label: '综合 · 工况评分', cls: 'rpv2-score-heat-th' },
      { label: colALabel,        cls: 'srp-col-A nowrap' },
      { label: colBLabel,        cls: 'srp-col-B nowrap' },
      { label: '操作',           cls: 'rpv2-action-cell' },
    ];

    cols.forEach(c => {
      const th = document.createElement('th');
      th.className = c.cls;
      th.textContent = c.label;
      tr.appendChild(th);
    });

    theadEl.appendChild(tr);
  }

  /* -------------------------------------------------------
     Build thead for composite (综合评分) search results table.
     Rankings-style but without rank column:
     wide: 品牌|型号|尺寸|RGB灯光|参考价|型号信息(narrow)|综合评分 · 热度|操作
     ------------------------------------------------------- */
  function buildCompositeThead(theadEl) {
    theadEl.innerHTML = '';
    const tr = document.createElement('tr');
    const cols = [
      { label: '品牌',             cls: 'nowrap rpv2-col-wide rpv2-col-brand' },
      { label: '型号',             cls: 'nowrap rpv2-col-model-name-wide' },
      { label: '尺寸',             cls: 'nowrap rpv2-col-wide rpv2-col-size' },
      { label: 'RGB灯光',          cls: 'nowrap rpv2-col-wide rpv2-col-rgb' },
      { label: '参考价',           cls: 'nowrap rpv2-col-wide rpv2-col-price' },
      { label: '型号信息',         cls: 'rpv2-col-narrow-info' },
      { label: '综合评分 · 热度',  cls: 'rpv2-score-heat-th' },
      { label: '操作',             cls: 'rpv2-action-cell' },
    ];
    cols.forEach(c => {
      const th = document.createElement('th');
      th.className = c.cls;
      th.textContent = c.label;
      tr.appendChild(th);
    });
    theadEl.appendChild(tr);
  }

  /* -------------------------------------------------------
     Build a single composite-mode search results row.
     Matches rankings-style structure (no rank, no col-A/col-B):
       expand btn | 综合评分 badge | · | 🔥 heat value
     Expand panel: mini radar (left) + 工况热度 bars (right),
     identical to the rankings tab expand panel.
     ------------------------------------------------------- */
  function buildCompositeSearchRow(item) {
    const tr = document.createElement('tr');
    tr.className = 'rpv2-main-row';
    tr.dataset.modelId = String(item.model_id);

    const { tdBrand, tdModel, tdSize, tdRgb, tdPrice, tdNarrowInfo } = buildModelCells(item);
    tr.appendChild(tdBrand);
    tr.appendChild(tdModel);
    tr.appendChild(tdSize);
    tr.appendChild(tdRgb);
    tr.appendChild(tdPrice);
    tr.appendChild(tdNarrowInfo);

    const { td: scoreTd, expandBtn } = buildScoreHeatTd(item);
    tr.appendChild(scoreTd);
    tr.appendChild(buildActionTd(item, 'search_results'));

    const expandTr = buildExpandRowEl(
      tr,
      expandBtn,
      item,
      _isNarrowMode() ? COMPOSITE_EXPAND_COLSPAN_NARROW : COMPOSITE_EXPAND_COLSPAN_WIDE,
      'heat',
      undefined,
      'search_results'
    );
    return { mainTr: tr, expandTr };
  }

  /* =============================================================
     Recent Updates v2 — model-centric list sorted by update date
     Reuses rpv2-* styles from right-panel-v2.css.
     Columns (wide): 品牌|型号|尺寸|RGB灯光|参考价|综合评分·热度|更新日期|操作
     Expand panel: mini radar (left) + 工况热度 bars (right),
     identical to the rankings tab expand panel.
     ============================================================= */

  /* -------------------------------------------------------
     Build thead for the recent-updates table.
     Rankings-style but without rank column; adds 更新日期 before 操作.
     ------------------------------------------------------- */
  function buildRecentUpdatesThead(theadEl) {
    theadEl.innerHTML = '';
    const tr = document.createElement('tr');
    const cols = [
      { label: '品牌',             cls: 'nowrap rpv2-col-wide rpv2-col-brand' },
      { label: '型号',             cls: 'nowrap rpv2-col-model-name-wide' },
      { label: '尺寸',             cls: 'nowrap rpv2-col-wide rpv2-col-size' },
      { label: 'RGB灯光',          cls: 'nowrap rpv2-col-wide rpv2-col-rgb' },
      { label: '参考价',           cls: 'nowrap rpv2-col-wide rpv2-col-price' },
      { label: '型号信息',         cls: 'rpv2-col-narrow-info' },
      { label: '综合评分 · 热度',  cls: 'rpv2-score-heat-th' },
      { label: '更新日期',         cls: 'nowrap rpv2-col-wide rpv2-col-date' },
      { label: '操作',             cls: 'rpv2-action-cell' },
    ];
    cols.forEach(c => {
      const th = document.createElement('th');
      th.className = c.cls;
      th.textContent = c.label;
      tr.appendChild(th);
    });
    theadEl.appendChild(tr);
  }

  /* -------------------------------------------------------
     Build a single recent-updates row (main row + expand row).
     ------------------------------------------------------- */
  function buildRecentUpdatesRow(item) {
    const tr = document.createElement('tr');
    tr.className = 'rpv2-main-row';
    tr.dataset.modelId = String(item.model_id);

    const { tdBrand, tdModel, tdSize, tdRgb, tdPrice, tdNarrowInfo } = buildModelCells(item);
    tr.appendChild(tdBrand);
    tr.appendChild(tdModel);
    tr.appendChild(tdSize);
    tr.appendChild(tdRgb);
    tr.appendChild(tdPrice);
    tr.appendChild(tdNarrowInfo);

    const { td: scoreTd, expandBtn } = buildScoreHeatTd(item);
    tr.appendChild(scoreTd);

    /* 更新日期 */
    const tdDate = document.createElement('td');
    tdDate.className = 'nowrap rpv2-col-wide rpv2-col-date';
    tdDate.textContent = item.update_date || '—';
    tr.appendChild(tdDate);

    tr.appendChild(buildActionTd(item, 'recent_updates'));

    const expandTr = buildExpandRowEl(
      tr,
      expandBtn,
      item,
      _isNarrowMode() ? RECENT_UPDATES_EXPAND_COLSPAN_NARROW : RECENT_UPDATES_EXPAND_COLSPAN_WIDE,
      'heat',
      undefined,
      'recent_updates'
    );
    return { mainTr: tr, expandTr };
  }

  /* -------------------------------------------------------
     Render the recent-updates table
     ------------------------------------------------------- */
  function renderRecentUpdatesTable(items) {
    // Store for local re-renders (scores update without refetching)
    const theadEl = document.getElementById('rup-thead');
    const tbodyEl = document.getElementById('rup-tbody');
    if (!theadEl || !tbodyEl) return;

    // Inject colgroup matching the current layout mode (wide vs narrow).
    const narrow = _isNarrowMode();
    injectColgroup(document.getElementById('rup-table'),
      narrow ? RECENT_UPDATES_NARROW_COL_DEFS : RECENT_UPDATES_WIDE_COL_DEFS);

    buildRecentUpdatesThead(theadEl);
    tbodyEl.innerHTML = '';

    if (!items || items.length === 0) {
      const tr = document.createElement('tr');
      const td = document.createElement('td');
      const colspan = narrow
        ? RECENT_UPDATES_NARROW_COL_DEFS.length
        : RECENT_UPDATES_WIDE_COL_DEFS.length;
      td.setAttribute('colspan', String(colspan));
      td.className = 'text-center text-gray-500 py-8';
      td.style.fontSize = '14px';
      td.textContent = '暂无近期更新数据';
      tr.appendChild(td);
      tbodyEl.appendChild(tr);
      return;
    }

    items.forEach(item => {
      const { mainTr, expandTr } = buildRecentUpdatesRow(item);
      tbodyEl.appendChild(mainTr);
      tbodyEl.appendChild(expandTr);
    });

    syncAddButtons();
  }

  /* -------------------------------------------------------
     Lazy loading state for the recent-updates tab
     ------------------------------------------------------- */
  let _rupLoaded    = false;
  let _rupLoadedAt  = 0;
  let _rupPending   = false;
  const _RUP_TTL_MS = 600000; // 10 minutes

  function _rupNeedsReload() {
    if (!_rupLoaded) return true;
    return (Date.now() - _rupLoadedAt) > _RUP_TTL_MS;
  }

  function _rupSetLoading(on) {
    const tbodyEl = document.getElementById('rup-tbody');
    if (!tbodyEl || _rupLoaded) return;
    if (on) {
      tbodyEl.innerHTML = '';
      const tr = document.createElement('tr');
      const td = document.createElement('td');
      td.setAttribute('colspan', String(_isNarrowMode() ? RECENT_UPDATES_EXPAND_COLSPAN_NARROW : RECENT_UPDATES_EXPAND_COLSPAN_WIDE));
      td.className = 'text-center text-gray-500 py-8';
      td.style.fontSize = '14px';
      td.textContent = '加载中...';
      tr.appendChild(td);
      tbodyEl.appendChild(tr);
    }
  }

  function loadRecentUpdatesIfNeeded() {
    if (!_rupNeedsReload()) return;
    if (_rupPending) return;
    _rupPending = true;
    _rupSetLoading(true);

    fetch('/api/recent_updates')
      .then(r => r.json())
      .then(j => {
        const data = (j && j.data) ? j.data : j;
        const items = (data && Array.isArray(data.items)) ? data.items : [];
        renderRecentUpdatesTable(items);
        _rupLoaded   = true;
        _rupLoadedAt = Date.now();
        // Piggyback: check announcement fingerprint from this response
        if (typeof window._checkAnnouncementMeta === 'function' && j && j.meta) {
          window._checkAnnouncementMeta(j.meta);
        }
      })
      .catch(() => {
        const tbodyEl = document.getElementById('rup-tbody');
        if (tbodyEl) {
          tbodyEl.innerHTML = '';
          const tr = document.createElement('tr');
          const td = document.createElement('td');
          td.setAttribute('colspan', String(_isNarrowMode() ? RECENT_UPDATES_EXPAND_COLSPAN_NARROW : RECENT_UPDATES_EXPAND_COLSPAN_WIDE));
          td.className = 'text-center text-red-500 py-8';
          td.style.fontSize = '14px';
          td.textContent = '加载失败，请稍后重试';
          tr.appendChild(td);
          tbodyEl.appendChild(tr);
        }
      })
      .finally(() => { _rupPending = false; });
  }

  /* -------------------------------------------------------
     Render the search results table
     items: array of enriched search result objects
     sortBy: 'condition_score' | 'none' | 'rpm' | 'noise' | 'composite_score'
     ------------------------------------------------------- */
  function renderSearchResultsTable(items, sortBy) {
    // Store for local re-renders
    const theadEl = document.getElementById('srp-thead');
    const tbodyEl = document.getElementById('srp-tbody');
    const tableEl = document.getElementById('srp-results-table');
    if (!theadEl || !tbodyEl) return;

    const resolvedSortBy = sortBy || 'condition_score';
    const isCompositeMode = (resolvedSortBy === 'composite_score');

    // Tag the table with the current sort mode so CSS can drive narrow-layout column rules
    if (tableEl) tableEl.dataset.sortBy = resolvedSortBy;

    // Inject colgroup matching the current layout mode (wide vs narrow) and search mode.
    // In narrow mode, each search mode uses a colgroup that ONLY contains visible cols
    // so that display:none cells (removed from table grid) don't cause subsequent cells
    // to shift into wrong column slots, which would leave phantom blank space at the right.
    const narrow = _isNarrowMode();
    if (isCompositeMode) {
      injectColgroup(tableEl, narrow ? COMPOSITE_NARROW_COL_DEFS : COMPOSITE_WIDE_COL_DEFS);
    } else if (narrow && resolvedSortBy === 'condition_score') {
      injectColgroup(tableEl, CONDITION_NARROW_COL_DEFS);   // 3 cols (col-A+col-B hidden)
    } else if (narrow) {
      injectColgroup(tableEl, AIRFLOW_NARROW_COL_DEFS);     // 4 cols (col-A hidden)
    } else {
      injectColgroup(tableEl, SEARCH_WIDE_COL_DEFS);
    }

    if (isCompositeMode) {
      buildCompositeThead(theadEl);
    } else {
      buildSearchThead(theadEl, resolvedSortBy);
    }
    tbodyEl.innerHTML = '';

    if (!items || items.length === 0) {
      const tr = document.createElement('tr');
      const td = document.createElement('td');
      let colspan;
      if (isCompositeMode) {
        colspan = narrow ? COMPOSITE_EXPAND_COLSPAN_NARROW : COMPOSITE_EXPAND_COLSPAN_WIDE;
      } else if (!narrow) {
        colspan = SEARCH_EXPAND_COLSPAN_WIDE;
      } else if (resolvedSortBy === 'condition_score') {
        colspan = CONDITION_EXPAND_COLSPAN_NARROW;
      } else {
        colspan = AIRFLOW_EXPAND_COLSPAN_NARROW;
      }
      td.setAttribute('colspan', String(colspan));
      td.className = 'text-center text-gray-500 py-8';
      td.style.fontSize = '14px';
      td.textContent = '没有符合条件的结果';
      tr.appendChild(td);
      tbodyEl.appendChild(tr);
      return;
    }

    items.forEach(item => {
      const { mainTr, expandTr } = isCompositeMode
        ? buildCompositeSearchRow(item)
        : buildSearchResultsRow(item, resolvedSortBy);
      tbodyEl.appendChild(mainTr);
      tbodyEl.appendChild(expandTr);
    });

    // Sync radar-add button states after rendering
    syncAddButtons();
  }

  /* -------------------------------------------------------
     Clear the search results table (reset to initial state)
     ------------------------------------------------------- */
  function clearSearchResultsTable() {
    const theadEl = document.getElementById('srp-thead');
    const tbodyEl = document.getElementById('srp-tbody');
    const tableEl = document.getElementById('srp-results-table');
    const summaryEl = document.getElementById('srp-summary-bar');
    if (theadEl) theadEl.innerHTML = '';
    if (tableEl) {
      // Reset colgroup to match the cleared (no sort mode) placeholder layout,
      // consistent with _applyRightPanelNarrowLayout behaviour when data-sort-by is absent.
      const narrow = _isNarrowMode();
      injectColgroup(tableEl, narrow ? AIRFLOW_NARROW_COL_DEFS : SEARCH_WIDE_COL_DEFS);
      delete tableEl.dataset.sortBy;
    }
    if (tbodyEl) {
      const clearColspan = _isNarrowMode() ? AIRFLOW_EXPAND_COLSPAN_NARROW : SEARCH_EXPAND_COLSPAN_WIDE;
      tbodyEl.innerHTML = `<tr><td colspan="${clearColspan}" class="text-center text-gray-500 py-8" style="font-size:16px;">使用进阶搜索以查看结果</td></tr>`;
    }
    if (summaryEl) summaryEl.textContent = '';
  }

  /* -------------------------------------------------------
     Init: read data from DOM, render both boards
     -------------------------------------------------------- */
  // Module-level board data storage
  let _boardData = { heat: [], perf: [], lighting: [] };

  function _renderBoards() {
    const heatThead = document.getElementById('rpv2-heat-thead');
    const heatTbody = document.getElementById('rpv2-heat-tbody');
    const perfThead = document.getElementById('rpv2-perf-thead');
    const perfTbody = document.getElementById('rpv2-perf-tbody');
    const lightingThead = document.getElementById('rpv2-lighting-thead');
    const lightingTbody = document.getElementById('rpv2-lighting-tbody');
    if (heatThead && heatTbody) renderBoard(_boardData.heat, heatThead, heatTbody, 'heat');
    if (perfThead && perfTbody) renderBoard(_boardData.perf, perfThead, perfTbody, 'perf');
    if (lightingThead && lightingTbody) renderBoard(_boardData.lighting, lightingThead, lightingTbody, 'lighting');
    syncAddButtons();
  }

  function init() {
    const dataEl = document.getElementById('rpv2-data');
    if (!dataEl) return;

    let data;
    try {
      data = JSON.parse(dataEl.textContent || '{}');
    } catch (e) {
      console.warn('[rpv2] Failed to parse ranking data:', e);
      return;
    }

    _boardData.heat = data.heat_board || [];
    _boardData.perf = data.performance_board || [];
    _boardData.lighting = data.lighting_board || [];
    _renderBoards();
  }

  function initViewAllModelsBtn() {
    const btn = document.getElementById('rpv2-view-all-models-btn');
    if (!btn) return;
    btn.addEventListener('click', async () => {
      const fn = window.FancoolSearch?.runDefaultCompositeSearch;
      if (typeof fn !== 'function') return;
      window.Analytics?.logEvent?.({
        event_type_code: 'click_view_all_models',
        page_key: 'home',
      });
      try {
        await fn({ suppressAdvancedSearchLog: true });
      } catch (_) {}
    });
  }

  function updateViewAllBtnVisibility(activeTab) {
    const btn = document.getElementById('rpv2-view-all-models-btn');
    if (!btn) return;
    if (activeTab === 'top-queries') {
      btn.removeAttribute('hidden');
    } else {
      btn.setAttribute('hidden', '');
    }
  }

  /* -------------------------------------------------------
     Sync add-to-radar button states in the v2 tables
     -------------------------------------------------------- */
  function syncAddButtons() {
    if (typeof window.setRadarToggleBtnState !== 'function') return;
    if (typeof window.RadarState === 'undefined' || !window.RadarState.hasModel) return;
    document.querySelectorAll('.rpv2-main-row .js-mini-radar-add[data-model-id]').forEach(btn => {
      const mid = btn.dataset.modelId;
      if (!mid) return;
      window.setRadarToggleBtnState(btn, window.RadarState.hasModel(mid) ? 'added' : 'add');
    });
  }

  /* -------------------------------------------------------
     Infrastructure: responsive rp-narrow class — shared apply helper
     Reads the current .fc-right-card width, toggles .rp-narrow, and
     re-injects the correct colgroup for every rendered table so the
     col count always matches the visible cells in the current mode.
     Also re-synchronises every expand-row td colspan.
     Extracted to module scope so it can be called after any async
     render (e.g. _fetchAndRenderBoards), not just from ResizeObserver /
     window-resize events.
     -------------------------------------------------------- */
  const _NARROW_W = 520;
  function _applyRightPanelNarrowLayout() {
    const card = document.querySelector('.fc-right-card');
    if (!card) return;
    const w = card.getBoundingClientRect().width;
    const isNarrow = w < _NARROW_W;
    if (isNarrow) card.classList.add('rp-narrow');
    else card.classList.remove('rp-narrow');

    // Re-inject the correct colgroup for each table so the col count exactly
    // matches the number of visible (non-display:none) th/td cells in this mode.
    // display:none cells do NOT occupy column slots in table-layout:fixed, so
    // keeping a ghost col for them shifts subsequent columns into the wrong slot.

    // Board tables
    const boardColDefs = isNarrow ? BOARD_NARROW_COL_DEFS : BOARD_WIDE_COL_DEFS;
    const boardExpandColspan = isNarrow ? BOARD_EXPAND_COLSPAN_NARROW : BOARD_EXPAND_COLSPAN_WIDE;
    [
      document.getElementById('rpv2-heat-table'),
      document.getElementById('rpv2-perf-table'),
      document.getElementById('rpv2-lighting-table'),
    ].forEach(tbl => {
      if (tbl) injectColgroup(tbl, boardColDefs);
    });
    card.querySelectorAll('.rpv2-board .rpv2-expand-td').forEach(td => {
      td.setAttribute('colspan', String(boardExpandColspan));
    });

    // Search results table (col set depends on composite vs normal mode vs sort mode)
    const searchTableEl = document.getElementById('srp-results-table');
    if (searchTableEl) {
      const sortBy = searchTableEl.dataset.sortBy;
      const isComposite = sortBy === 'composite_score';
      let searchColDefs, searchExpandColspan;
      if (!isNarrow) {
        searchColDefs = isComposite ? COMPOSITE_WIDE_COL_DEFS : SEARCH_WIDE_COL_DEFS;
        searchExpandColspan = isComposite ? COMPOSITE_EXPAND_COLSPAN_WIDE : SEARCH_EXPAND_COLSPAN_WIDE;
      } else if (isComposite) {
        searchColDefs = COMPOSITE_NARROW_COL_DEFS;
        searchExpandColspan = COMPOSITE_EXPAND_COLSPAN_NARROW;
      } else if (sortBy === 'condition_score') {
        searchColDefs = CONDITION_NARROW_COL_DEFS;   // 3 cols: narrow+score+action
        searchExpandColspan = CONDITION_EXPAND_COLSPAN_NARROW;
      } else {
        // airflow modes (none/rpm/noise) or unknown — col-A hidden, col-B visible
        searchColDefs = AIRFLOW_NARROW_COL_DEFS;     // 4 cols: narrow+score+col-b+action
        searchExpandColspan = AIRFLOW_EXPAND_COLSPAN_NARROW;
      }
      injectColgroup(searchTableEl, searchColDefs);
      searchTableEl.querySelectorAll('.rpv2-expand-td').forEach(td => {
        td.setAttribute('colspan', String(searchExpandColspan));
      });
    }

    // Recent updates table
    const rupTableEl = document.getElementById('rup-table');
    if (rupTableEl) {
      const rupColDefs = isNarrow ? RECENT_UPDATES_NARROW_COL_DEFS : RECENT_UPDATES_WIDE_COL_DEFS;
      const rupExpandColspan = isNarrow ? RECENT_UPDATES_EXPAND_COLSPAN_NARROW : RECENT_UPDATES_EXPAND_COLSPAN_WIDE;
      injectColgroup(rupTableEl, rupColDefs);
      rupTableEl.querySelectorAll('.rpv2-expand-td').forEach(td => {
        td.setAttribute('colspan', String(rupExpandColspan));
      });
    }

    card.querySelectorAll('.rpv2-expand-row').forEach(expandTr => {
      if (typeof expandTr._rpv2SyncLayout === 'function') expandTr._rpv2SyncLayout();
    });
    card.querySelectorAll('.rpv2-main-row-expandable').forEach(mainTr => {
      _syncExpandableRowKeyboardAccess(mainTr, isNarrow);
      _syncNarrowAutoMarqueeForMainRow(mainTr);
    });
  }

  /* -------------------------------------------------------
     Infrastructure: responsive rp-narrow class
     Adds/removes .rp-narrow on .fc-right-card based on card width,
     then re-injects the correct colgroup for every table so wide and
     narrow modes each use only the columns that are actually visible.
     -------------------------------------------------------- */
  function initRightPanelResponsiveWrap() {
    const card = document.querySelector('.fc-right-card');
    if (!card) return;
    _applyRightPanelNarrowLayout();
    // Defer a second measurement to catch the settled layout width on initial load.
    // getBoundingClientRect() at DOMContentLoaded may return 0 or a pre-layout value
    // when the right-panel container has not yet been painted, causing the narrow-screen
    // rp-narrow class to be incorrectly applied/omitted until the next window resize.
    requestAnimationFrame(_applyRightPanelNarrowLayout);
    if ('ResizeObserver' in window) {
      const ro = new ResizeObserver(() => _applyRightPanelNarrowLayout());
      ro.observe(card);
    } else {
      window.addEventListener('resize', _applyRightPanelNarrowLayout);
    }
  }

  /* -------------------------------------------------------
     Infrastructure: move the rankings .fc-seg to #rightSubsegContainer
     so the heat/perf switcher appears in the nav bar.
     -------------------------------------------------------- */
  function mountRightSubseg() {
    const subsegContainer = document.getElementById('rightSubsegContainer');
    if (!subsegContainer) return;
    const queriesSeg = document.querySelector('#top-queries-pane .fc-seg');
    if (queriesSeg) {
      queriesSeg.dataset.paneId = 'top-queries-pane';
      // Apply 3-btn class if the seg control has 3 buttons
      const btnCount = queriesSeg.querySelectorAll('.fc-seg__btn').length;
      if (btnCount === 3) {
        queriesSeg.classList.add('fc-seg--3btn');
      } else {
        queriesSeg.classList.remove('fc-seg--3btn');
      }
      subsegContainer.appendChild(queriesSeg);
    }
    // Show only when rankings tab is active
    updateSubsegVisibility('top-queries');
    updateViewAllBtnVisibility('top-queries');
  }

  function updateSubsegVisibility(activeTab) {
    const queriesSeg = document.querySelector('#rightSubsegContainer .fc-seg[data-pane-id="top-queries-pane"]');
    if (queriesSeg) {
      queriesSeg.style.display = (activeTab === 'top-queries') ? '' : 'none';
    }
  }

  /* -------------------------------------------------------
     Infrastructure: board panel switching (heat ↔ perf)
     Handles clicks on .fc-seg__btn inside #rightSubsegContainer.

     Race-condition fix: use a generation counter (_boardGen) instead of
     a boolean lock. Each new click increments the counter; stale cleanup
     callbacks check the counter and bail out. This prevents both:
     (a) rapid clicks leaving the board area blank, and
     (b) stale timeouts interfering with a subsequent animation.
     -------------------------------------------------------- */
  function initBoardSegClicks() {
    const subsegContainer = document.getElementById('rightSubsegContainer');
    if (!subsegContainer) return;

    // Incremented on every switch; stale callbacks compare against this.
    let _boardGen = 0;

    subsegContainer.addEventListener('click', (e) => {
      const btn = e.target.closest('.fc-seg__btn');
      if (!btn || !subsegContainer.contains(btn)) return;

      const seg = btn.closest('.fc-seg');
      if (!seg) return;
      const targetId = btn.dataset.target;
      if (!targetId) return;

      const paneId = seg.dataset.paneId;
      const pane = paneId ? document.getElementById(paneId) : null;
      if (!pane) return;

      const panels = Array.from(pane.querySelectorAll('.fc-rank-panel'));
      if (!panels.length) return;

      const nextPanel = panels.find(p => p.id === targetId);
      if (!nextPanel) return;

      // Update segmented button active state immediately (no lock)
      seg.querySelectorAll('.fc-seg__btn').forEach(b => {
        b.classList.toggle('is-active', b === btn);
      });
      seg.setAttribute('data-active', targetId);

      // Find the currently active (non-leaving) panel
      const currPanel = panels.find(
        p => p.classList.contains('active') && !p.classList.contains('is-leaving')
      );

      if (nextPanel === currPanel) return;

      // Increment generation: all stale cleanup callbacks become no-ops
      const gen = ++_boardGen;

      // Immediately cancel any in-progress leaving animation so we never
      // end up with both panels in position:absolute at the same time.
      panels.forEach(p => {
        if (p !== nextPanel && p.classList.contains('is-leaving')) {
          p.classList.remove('is-leaving', 'active');
          p.style.animation = 'none';
        }
      });

      // Show next panel (restart fade-in animation)
      nextPanel.classList.remove('is-leaving');
      nextPanel.classList.add('active');
      nextPanel.style.animation = 'none';
      void nextPanel.offsetWidth;
      nextPanel.style.animation = '';

      if (!currPanel) return;

      // Fade out the outgoing panel
      currPanel.classList.add('is-leaving');

      const cleanup = () => {
        if (_boardGen !== gen) return; // stale — a newer switch happened
        currPanel.classList.remove('is-leaving', 'active');
        currPanel.style.animation = '';
      };

      const handleAnimEnd = (ev) => {
        if (ev.target !== currPanel) return;
        if (ev.animationName !== 'rpv2FadePanelOut') return;
        currPanel.removeEventListener('animationend', handleAnimEnd);
        cleanup();
      };
      currPanel.addEventListener('animationend', handleAnimEnd);
      // Fallback in case animationend doesn't fire (e.g. reduced-motion)
      setTimeout(cleanup, 400);
    });
  }

  /* -------------------------------------------------------
     Infrastructure: drag-switch on the segmented control
     -------------------------------------------------------- */
  function initBoardSegDragSwitch() {
    const segs = document.querySelectorAll('#rightSubsegContainer .fc-seg');
    if (!segs.length) return;
    segs.forEach(seg => {
      const thumb = seg.querySelector('.fc-seg__thumb');
      const btns  = seg.querySelectorAll('.fc-seg__btn');
      if (!thumb || btns.length !== 2) return;

      let dragging = false, startX = 0, basePercent = 0, lastPercent = 0, cachedThumbW = 1;
      const activeIsRight = () => {
        const a = seg.getAttribute('data-active') || '';
        return a === 'perf-board' || a.endsWith('likes-panel');
      };
      const inThumb = (x, y) => {
        const r = thumb.getBoundingClientRect();
        return x >= r.left && x <= r.right && y >= r.top && y <= r.bottom;
      };
      function start(e) {
        const cx = (e.touches ? e.touches[0].clientX : e.clientX) || 0;
        const cy = (e.touches ? e.touches[0].clientY : e.clientY) || 0;
        if (!inThumb(cx, cy)) return;
        dragging = true; startX = cx;
        basePercent = activeIsRight() ? 100 : 0;
        lastPercent = basePercent;
        // Cache thumb width at drag start to avoid repeated layout reads during move
        cachedThumbW = thumb.getBoundingClientRect().width || 1;
        thumb.style.transition = 'none';
        if (e.cancelable) e.preventDefault();
      }
      function move(e) {
        if (!dragging) return;
        const cx = (e.touches ? e.touches[0].clientX : e.clientX) || 0;
        const dx = cx - startX;
        let p = basePercent + (dx / cachedThumbW) * 100;
        p = Math.max(0, Math.min(100, p));
        lastPercent = p;
        thumb.style.transform = `translateX(${p}%)`;
        if (e.cancelable) e.preventDefault();
      }
      function end() {
        if (!dragging) return;
        dragging = false;
        const goRight = lastPercent >= 50;
        const targetBtn = goRight ? btns[1] : btns[0];
        thumb.style.transition = '';
        thumb.style.transform  = '';
        targetBtn.click();
      }
      seg.addEventListener('mousedown',  start);
      document.addEventListener('mousemove', move, { passive: false });
      document.addEventListener('mouseup',   end);
      seg.addEventListener('touchstart', start, { passive: false });
      document.addEventListener('touchmove', move,  { passive: false });
      document.addEventListener('touchend',  end);
    });
  }

  /* -------------------------------------------------------
     Infrastructure: main right-panel tab scroll-snap
     Initializes scroll-snap for 排行榜 / 搜索结果 / 近期更新.
     -------------------------------------------------------- */
  function initRightPanelSnapTabs() {
    const card      = document.querySelector('.fc-right-card');
    if (!card) return;
    const container = card.querySelector('.fc-tab-container');
    const wrapper   = card.querySelector('.fc-tab-wrapper');
    if (!container || !wrapper) return;
    if (!container.id) container.id = 'right-panel-container';
    if (!wrapper.id)   wrapper.id   = 'right-panel-wrapper';

    if (typeof window.initSnapTabScrolling === 'function') {
      window.initSnapTabScrolling({
        containerId: container.id,
        group: 'right-panel',
        persistKey: null,
        defaultTab: 'top-queries',
        onActiveChange: (tab) => {
          updateSubsegVisibility(tab);
          updateViewAllBtnVisibility(tab);
          if (tab === 'recent-updates') {
            loadRecentUpdatesIfNeeded();
          }
        },
        clickScrollBehavior: 'smooth',
      });
    }
  }

  /* -------------------------------------------------------
     型号列 hover-marquee — wide (non-narrow) mode only.
     Slides .rpv2-model-name-inner to the left on mouseenter
     only when the text genuinely overflows the wrapper.
     ------------------------------------------------------- */
  const _MODEL_MARQUEE_SPEED = 60; // px / second

  document.addEventListener('mouseenter', (e) => {
    const td = window.safeClosest(e.target, 'td.rpv2-col-model-name-wide');
    if (!td) return;
    if (td.contains(e.relatedTarget)) return;
    const card = window.safeClosest(td, '.fc-right-card');
    if (!card || card.classList.contains('rp-narrow')) return;
    const wrap  = td.querySelector('.rpv2-model-name-wrap');
    const inner = td.querySelector('.rpv2-model-name-inner');
    if (!wrap || !inner) return;
    const delta = inner.scrollWidth - wrap.clientWidth;
    if (delta > 4) {
      const duration = Math.max(0.5, delta / _MODEL_MARQUEE_SPEED).toFixed(2);
      inner.style.willChange = 'transform';
      inner.style.transition = `transform ${duration}s linear`;
      inner.style.transform  = `translateX(-${delta}px)`;
    }
  }, true);

  document.addEventListener('mouseleave', (e) => {
    const td = window.safeClosest(e.target, 'td.rpv2-col-model-name-wide');
    if (!td) return;
    if (td.contains(e.relatedTarget)) return;
    const card = window.safeClosest(td, '.fc-right-card');
    if (!card || card.classList.contains('rp-narrow')) return;
    const inner = td.querySelector('.rpv2-model-name-inner');
    if (!inner) return;
    inner.style.transition = 'transform 0.35s ease';
    inner.style.transform  = 'translateX(0)';
    const onEnd = (ev) => {
      if (ev.propertyName !== 'transform') return;
      inner.style.willChange = '';
      inner.removeEventListener('transitionend', onEnd);
    };
    inner.addEventListener('transitionend', onEnd);
  }, true);

  /* -------------------------------------------------------
     型号列长按复制 — narrow mode only.
     Long-press on 型号信息 copies "品牌 + 型号" plain text.
     ------------------------------------------------------- */
  document.addEventListener('touchstart', (e) => {
    const td = window.safeClosest(e.target, 'td.rpv2-col-narrow-info');
    if (!td || !_isNarrowMode()) return;
    if (_isMainRowInteractiveTarget(e.target)) return;

    const copyText = String(td.dataset.copyText || '').trim();
    if (!copyText) return;
    const touch = e.changedTouches && e.changedTouches[0];
    if (!touch) return;
    const touchId = touch.identifier;
    const startX = touch.clientX;
    const startY = touch.clientY;
    let ended = false;
    let moved = false;

    const timer = setTimeout(() => {
      if (ended || moved) return;
      _copyTextToClipboard(copyText)
        .then(() => {
          if (typeof window.showSuccess === 'function') window.showSuccess('已复制型号文字');
        })
        .catch(() => {});
    }, _NARROW_COPY_LONG_PRESS_MS);

    const cleanup = () => {
      clearTimeout(timer);
      document.removeEventListener('touchmove', onTouchMove, true);
      document.removeEventListener('touchend', onTouchEnd, true);
      document.removeEventListener('touchcancel', onTouchEnd, true);
    };

    const onTouchMove = (ev) => {
      if (ended) return;
      let t = null;
      for (let i = 0; i < ev.changedTouches.length; i++) {
        if (ev.changedTouches[i].identifier === touchId) { t = ev.changedTouches[i]; break; }
      }
      if (!t) return;
      const dx = Math.abs(t.clientX - startX);
      const dy = Math.abs(t.clientY - startY);
      if (dx > _NARROW_COPY_MOVE_CANCEL || dy > _NARROW_COPY_MOVE_CANCEL) {
        moved = true;
        cleanup();
      }
    };

    const onTouchEnd = (ev) => {
      if (ended) return;
      let found = false;
      for (let i = 0; i < ev.changedTouches.length; i++) {
        if (ev.changedTouches[i].identifier === touchId) { found = true; break; }
      }
      if (!found) return;
      ended = true;
      cleanup();
    };

    document.addEventListener('touchmove', onTouchMove, { capture: true, passive: true });
    document.addEventListener('touchend', onTouchEnd, { capture: true });
    document.addEventListener('touchcancel', onTouchEnd, { capture: true });
  }, { passive: true });

  /* -------------------------------------------------------
     Expose public API
     -------------------------------------------------------- */
  window.RightPanelV2 = {
    init,
    syncAddButtons,
    renderSearchResultsTable,
    clearSearchResultsTable,
    loadRecentUpdatesIfNeeded,
  };

  /* -------------------------------------------------------
     Auto-init on DOMContentLoaded
     -------------------------------------------------------- */
  function initAll() {
    init();
    if (
      typeof window.ScoreRuleModal !== 'undefined'
      && typeof window.ScoreRuleModal.initAlgorithmExplainModal === 'function'
    ) {
      window.ScoreRuleModal.initAlgorithmExplainModal();
    }
    if (
      typeof window.LadderModal !== 'undefined'
      && typeof window.LadderModal.initLadderModal === 'function'
    ) {
      window.LadderModal.initLadderModal();
    }
    initRightPanelSnapTabs();
    initRightPanelResponsiveWrap();
    mountRightSubseg();
    initViewAllModelsBtn();
    // Board switching must be wired after mountRightSubseg moves the seg element
    initBoardSegClicks();
    initBoardSegDragSwitch();
  }

  if (document.readyState !== 'loading') {
    initAll();
  } else {
    document.addEventListener('DOMContentLoaded', initAll, { once: true });
  }

  /* -------------------------------------------------------
     Re-sync add buttons whenever RadarState changes.
     We listen for the custom events fired by fancool.js.
     -------------------------------------------------------- */
  document.addEventListener('radarModelAdded',   syncAddButtons);
  document.addEventListener('radarModelRemoved',  syncAddButtons);
  document.addEventListener('radarCleared',       syncAddButtons);

  /* Also hook into syncRadarToggleButtons (called by fancool.js) */
  const _origSync = window.syncRadarToggleButtons;
  window.syncRadarToggleButtons = function () {
    if (typeof _origSync === 'function') _origSync();
    syncAddButtons();
  };

})();
