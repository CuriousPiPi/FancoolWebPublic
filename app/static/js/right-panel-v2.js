/* =============================================================
   right-panel-v2.js
   Right Panel v2 — Rankings tab (model-centric leaderboard)

   Self-contained: does NOT depend on right-panel.js or right-panel.css.
   All layout/panel/table infrastructure is handled here and in
   right-panel-v2.css.

   Implements:
   - 热度榜 (heat board): sorted by heat_score = query_count + like_count * 10
   - 性能榜 (performance board): sorted by composite_score (percentage, 0–100)
   - Wide layout: 排名 | 品牌 | 型号 | 尺寸 | RGB灯光 | 参考价 | 综合评分·热度 | 操作
   - Narrow layout: 排名 | 型号信息 | 综合评分·热度 | 操作
   - Score badge with percentage-based color bands
   - Expand button in the 综合评分·热度 column
   - Small expand panel: mini radar card + 6-condition heat-percentage breakdown

   Also owns:
   - Main right-panel tab scroll-snap (排行榜 / 搜索结果 / 近期更新)
   - Heat/perf board switcher (segmented control moved to nav subseg)
   - Responsive rp-narrow class on .fc-right-card

   Data source:
   - Rankings data embedded as JSON in #rpv2-data script tag (from server get_rankings_v2())
   - heat_score = query_count + like_count * 10
   - composite_score comes from the server's ABC cache (percentage 0–100)
   - condition_scores: {cid: score_total} for 6 radar conditions (percentage 0–100)
   - Condition labels from window.__condLabelCache (populated by fancool-search.js)

   Depends on (globals expected by DOMContentLoaded time):
   - window.buildRadarToggleBtnEl(cfg)     from radar-toggle-button.js
   - window.setRadarToggleBtnState(btn, s) from radar-toggle-button.js
   - window.buildMiniRadarSVG(...)         from mini-radar-card.js
   - window.RADAR_CIDS_COLOR              from mini-radar-card.js ([1,2,3,7,8,10])
   - window.RadarState                    from fancool.js
   - window.syncRadarToggleButtons        from fancool.js
   - window.__condLabelCache              from fancool-search.js (may be populated async)
   - window.initSnapTabScrolling          from fancool.js
   ============================================================= */

(function initRightPanelV2() {
  'use strict';

  /* -------------------------------------------------------
     Constants
     ------------------------------------------------------- */
  const RADAR_CIDS = [1, 2, 3, 8, 7, 10];
  // Rankings: rank+brand+model+size+rgb+price+narrow-info+score-heat+action = 9
  const EXPAND_TD_COLSPAN = 9;
  // Search results composite mode: brand+model+size+rgb+price+narrow-info+score+action = 8
  const SEARCH_COMPOSITE_EXPAND_COLSPAN = 8;
  // Recent updates: brand+model+size+rgb+price+narrow-info+score-heat+update-date+action = 9
  const RECENT_UPDATES_EXPAND_COLSPAN = 9;

  /* -------------------------------------------------------
     Score badge helper — delegates to the shared
     ScoreBadgeHelper (score-badge-helper.js) which applies
     continuous Oklab-interpolated anchor-based colors.
     ------------------------------------------------------- */
  function buildScoreBadge(score) {
    return window.ScoreBadgeHelper.buildScoreBadge(score);
  }

  /* -------------------------------------------------------
     Rank medal helper
     ------------------------------------------------------- */
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

  /* -------------------------------------------------------
     Get condition label (with fallback to cid string)
     ------------------------------------------------------- */
  function condLabel(cid) {
    const cache = window.__condLabelCache || {};
    return cache[cid] || String(cid);
  }

  /* -------------------------------------------------------
     Shared expand row animation helper.
     Handles the scrollHeight-based open/close transition and the
     auto-scroll correction so the panel stays in view.

     Parameters:
       expandBtn    – the chevron button that triggers the toggle
       expandTd     – the <td> hosting the expand wrapper (display:none when closed)
       expandWrap   – the wrapper div whose height is animated
       expandTr     – the <tr> that receives .is-open
       buildPanelFn – () → Element; called once on first open (lazy build)
       onOpen       – optional callback fired when the panel opens (e.g. analytics)
     ------------------------------------------------------- */
  function wireExpandRow(expandBtn, expandTd, expandWrap, expandTr, buildPanelFn, onOpen) {
    let panelBuilt = false;
    let expandGen = 0;

    expandBtn.addEventListener('click', (e) => {
      e.stopPropagation();
      const isOpen = expandTr.classList.contains('is-open');
      const gen = ++expandGen;

      if (isOpen) {
        const currentH = expandWrap.getBoundingClientRect().height;
        expandWrap.style.transition = 'none';
        expandWrap.style.height = currentH + 'px';
        void expandWrap.offsetHeight;
        expandWrap.style.transition = 'height 0.26s ease';
        expandWrap.style.height = '0';

        expandTr.classList.remove('is-open');
        expandBtn.setAttribute('aria-expanded', 'false');
        expandBtn.setAttribute('data-tooltip', '展开详情');

        const done = () => {
          if (expandGen !== gen) return;
          expandTd.style.display = 'none';
          expandWrap.style.transition = '';
          expandWrap.style.height = '';
        };
        const onTransEnd = (ev) => {
          if (ev.target !== expandWrap || ev.propertyName !== 'height') return;
          expandWrap.removeEventListener('transitionend', onTransEnd);
          done();
        };
        expandWrap.addEventListener('transitionend', onTransEnd);
        setTimeout(done, 400);
      } else {
        expandTd.style.display = '';
        if (!panelBuilt) {
          expandWrap.appendChild(buildPanelFn());
          panelBuilt = true;
        }
        expandWrap.style.transition = 'none';
        expandWrap.style.height = '0';
        void expandWrap.offsetHeight;
        const targetH = expandWrap.scrollHeight;
        expandWrap.style.transition = 'height 0.26s ease';
        expandWrap.style.height = targetH + 'px';

        expandTr.classList.add('is-open');
        expandBtn.setAttribute('aria-expanded', 'true');
        expandBtn.setAttribute('data-tooltip', '收起详情');

        if (typeof onOpen === 'function') onOpen();  // fires on click, not after transition (e.g. analytics)

        const onOpenEnd = (ev) => {
          if (ev.target !== expandWrap || ev.propertyName !== 'height') return;
          expandWrap.removeEventListener('transitionend', onOpenEnd);
          if (expandTr.classList.contains('is-open') && expandGen === gen) {
            const scroller = expandTr.closest('.fc-rank-scroll');
            let scrollAmt = 0;
            if (scroller) {
              const scrollerRect = scroller.getBoundingClientRect();
              const wrapRect = expandWrap.getBoundingClientRect();
              const bottomOverflow = wrapRect.bottom - scrollerRect.bottom;
              if (bottomOverflow > 0) {
                const mainTr = expandTr.previousElementSibling;
                let maxAllowed = bottomOverflow;
                if (mainTr) {
                  const mainRect = mainTr.getBoundingClientRect();
                  const headroom = mainRect.top - scrollerRect.top;
                  maxAllowed = Math.min(bottomOverflow, Math.max(0, headroom));
                }
                scrollAmt = maxAllowed;
              } else {
                const topOverflow = scrollerRect.top - wrapRect.top;
                if (topOverflow > 0) scrollAmt = -topOverflow;
              }
            }
            expandWrap.style.height = 'auto';
            expandWrap.style.transition = '';
            if (scrollAmt !== 0 && scroller) {
              scroller.scrollBy({ top: scrollAmt, behavior: 'smooth' });
            }
          }
        };
        expandWrap.addEventListener('transitionend', onOpenEnd);
      }
    });
  }

  /* -------------------------------------------------------
     Shared helpers used by all three row builders
     (rankings, composite search, recent updates).
     ------------------------------------------------------- */

  /**
   * Build the six model-info cells shared by every main-row builder:
   * 品牌 | 型号 | 尺寸 | RGB灯光 | 参考价 | 型号信息(narrow)
   * Returns { tdBrand, tdModel, tdSize, tdRgb, tdPrice, tdNarrowInfo, sizeStr }
   */
  function buildModelCells(item) {
    const tdBrand = document.createElement('td');
    tdBrand.className = 'nowrap rpv2-col-wide';
    tdBrand.textContent = item.brand_name_zh || '—';

    const tdModel = document.createElement('td');
    tdModel.className = 'nowrap rpv2-col-model-name-wide';
    tdModel.textContent = item.model_name || '—';
    if (item.max_speed != null) {
      const speedSpan = document.createElement('span');
      speedSpan.className = 'rpv2-max-speed';
      speedSpan.textContent = ` (${item.max_speed} RPM)`;
      tdModel.appendChild(speedSpan);
    }

    const tdSize = document.createElement('td');
    tdSize.className = 'nowrap rpv2-col-wide';
    const sizeStr = item.size && item.thickness
      ? `${item.size}×${item.thickness}`
      : (item.size || '—');
    tdSize.textContent = sizeStr;

    const tdRgb = document.createElement('td');
    tdRgb.className = 'nowrap rpv2-col-wide';
    tdRgb.textContent = item.rgb_light || '—';

    const tdPrice = document.createElement('td');
    tdPrice.className = 'nowrap rpv2-col-wide rpv2-col-price';
    const priceVal = item.reference_price;
    tdPrice.textContent = (priceVal && Number(priceVal) > 0) ? `¥${priceVal}` : '—';

    const tdNarrowInfo = document.createElement('td');
    tdNarrowInfo.className = 'rpv2-col-narrow-info';
    const infoDiv = document.createElement('div');
    infoDiv.className = 'rpv2-model-info-compact';
    const nameSpan = document.createElement('span');
    nameSpan.className = 'rpv2-mi-name';
    nameSpan.textContent = [item.brand_name_zh, item.model_name].filter(Boolean).join(' ') || '—';
    const subSpan = document.createElement('span');
    subSpan.className = 'rpv2-mi-sub';
    const subParts = [];
    if (item.max_speed != null) subParts.push(`${item.max_speed} RPM`);
    if (sizeStr && sizeStr !== '—') subParts.push(sizeStr);
    subSpan.textContent = subParts.join(' · ') || '—';
    infoDiv.appendChild(nameSpan);
    infoDiv.appendChild(subSpan);
    tdNarrowInfo.appendChild(infoDiv);

    return { tdBrand, tdModel, tdSize, tdRgb, tdPrice, tdNarrowInfo, sizeStr };
  }

  /**
   * Build the 综合评分 · 热度 cell (rankings-style).
   * Returns { td, expandBtn } so callers can wire the expand row.
   * heatTitle: optional tooltip override for the heat value element.
   */
  function buildScoreHeatTd(item, heatTitle) {
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

    const badge = buildScoreBadge(item.composite_score);

    const sep = document.createElement('span');
    sep.className = 'rpv2-sep';
    sep.textContent = '·';

    const heatEl = document.createElement('span');
    heatEl.className = 'rpv2-heat-val';
    heatEl.innerHTML = `🔥${item.heat_score ?? 0}`;
    heatEl.title = heatTitle || '热度值 = 查询数 + 点赞数 × 10';

    cell.appendChild(expandBtn);
    cell.appendChild(badge);
    cell.appendChild(sep);
    cell.appendChild(heatEl);
    td.appendChild(cell);

    return { td, expandBtn };
  }

  /**
   * Build the 操作 cell (radar toggle button).
   */
  function buildActionTd(item) {
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
      });
      const actionWrap = document.createElement('div');
      actionWrap.className = 'rpv2-action-wrap';
      actionWrap.appendChild(btn);
      tdAction.appendChild(actionWrap);
    }
    return tdAction;
  }

  /**
   * Build the expand <tr> and wire it to expandBtn.
   * Returns the expandTr element.
   */
  function buildExpandRowEl(expandBtn, item, colSpan, buildPanelFn) {
    const expandTr = document.createElement('tr');
    expandTr.className = 'rpv2-expand-row';
    expandTr.dataset.expandFor = String(item.model_id);

    const expandTd = document.createElement('td');
    expandTd.className = 'rpv2-expand-td';
    expandTd.setAttribute('colspan', String(colSpan));

    const expandWrap = document.createElement('div');
    expandWrap.className = 'rpv2-expand-td-wrap';
    expandTd.style.display = 'none';
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

    wireExpandRow(expandBtn, expandTd, expandWrap, expandTr, buildPanelFn, onOpen);
    return expandTr;
  }

  /* -------------------------------------------------------
     Build small expand panel content (called lazily at first expand)
     Uses current window.__condLabelCache so labels are resolved at
     expand time, not at init time, ensuring async preload has finished.
     ------------------------------------------------------- */
  function buildExpandPanel(item) {
    const inner = document.createElement('div');
    inner.className = 'rpv2-expand-inner';

    /* --- Mini radar col — mirrors the "按型号添加" embedded pattern:
         no card border, no quick-add button, no info row, no like button.
         We use buildMiniRadarSVG directly so we fully control the container,
         matching the cleaner embedded style used in fancool-search.js.      --- */
    const radarCol = document.createElement('div');
    radarCol.className = 'rpv2-mini-radar-col';

    const hasScores = RADAR_CIDS.some(cid => {
      const s = item.condition_scores && item.condition_scores[cid];
      return s !== null && s !== undefined;
    });

    if (hasScores && typeof window.buildMiniRadarSVG === 'function') {
      const radarItems = RADAR_CIDS.map(cid => ({
        condition_id: cid,
        score_total: item.condition_scores ? item.condition_scores[cid] : null,
      }));
      // Read label cache at expand time so async preload has had a chance to finish
      const labelCache = window.__condLabelCache || {};
      const radarBody = document.createElement('div');
      radarBody.className = 'rpv2-mini-radar-body';
      radarBody.innerHTML = window.buildMiniRadarSVG(
        radarItems,
        item.composite_score,
        labelCache,
        null,
        { showLikeThumbs: false }
      );
      radarCol.appendChild(radarBody);
    } else {
      /* Placeholder: data not available yet */
      const ph = document.createElement('div');
      ph.className = 'rpv2-mini-radar-placeholder';
      ph.innerHTML =
        '<i class="fa-solid fa-chart-radar" style="font-size:20px;opacity:0.4"></i>' +
        '<span>综合评分数据暂未加载</span>' +
        '<span style="font-size:10px;color:#d1d5db">（首次查询该型号后评分计算完成）</span>';
      radarCol.appendChild(ph);
    }

    inner.appendChild(radarCol);

    /* --- Heat breakdown col with vertical "工况热度" title on the left --- */
    const heatCol = document.createElement('div');
    heatCol.className = 'rpv2-heat-breakdown-col';

    /* Vertical "工况热度" title on the left side, no extra top-row height */
    const title = document.createElement('div');
    title.className = 'rpv2-heat-breakdown-title';
    title.textContent = '工况热度';
    heatCol.appendChild(title);

    /* Condition rows container */
    const condRows = document.createElement('div');
    condRows.className = 'rpv2-cond-rows';

    /* Heat bars: bar width is normalized to local maximum so the hottest
       condition fills the full bar area. The percentage label shows the
       actual fraction of model total heat.
       No gray remainder track — each bar stands on its own. */
    const condHeat = item.condition_heat || {};
    const totalHeat = item.heat_score || 0;

    // Find local max for normalization
    const maxCondHeat = Math.max(1, ...RADAR_CIDS.map(c => condHeat[c] || 0));

    RADAR_CIDS.forEach(cid => {
      const ch = condHeat[cid] || 0;
      // Percentage label: actual fraction of model total heat
      const pct = (totalHeat > 0)
        ? Math.max(0, Math.min(100, Math.round((ch / totalHeat) * 100)))
        : null;
      // Bar width: normalized to local maximum (hottest condition = 100%)
      const barW = Math.round((ch / maxCondHeat) * 100);

      const row = document.createElement('div');
      row.className = 'rpv2-cond-row';

      // Resolve label at expand time from the async-populated cache
      const label = condLabel(cid);

      const nameEl = document.createElement('span');
      nameEl.className = 'rpv2-cond-name';
      nameEl.textContent = label;
      nameEl.title = label;

      /* Bar + label as a single inline flex group.
         The bar fill sits inside a fixed-width track (overflow:hidden);
         the pct label sits right after the track.
         No gray remainder track — each bar stands on its own length. */
      const barArea = document.createElement('div');
      barArea.className = 'rpv2-cond-bar-area';

      const barTrack = document.createElement('div');
      barTrack.className = 'rpv2-cond-bar-track';

      const barFill = document.createElement('div');
      barFill.className = 'rpv2-cond-bar-fill';
      barFill.style.width = barW + '%';

      const pctEl = document.createElement('span');
      pctEl.className = 'rpv2-cond-pct';
      pctEl.textContent = pct !== null ? `${pct}%` : '—';

      barTrack.appendChild(barFill);
      barArea.appendChild(barTrack);
      barArea.appendChild(pctEl);

      row.appendChild(nameEl);
      row.appendChild(barArea);
      condRows.appendChild(row);
    });

    heatCol.appendChild(condRows);
    inner.appendChild(heatCol);
    return inner;
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

    const heatTitle = `热度值 = 查询数(${item.query_count}) + 点赞数(${item.like_count}) × 10`;
    const { td: scoreTd, expandBtn } = buildScoreHeatTd(item, heatTitle);
    tr.appendChild(scoreTd);
    tr.appendChild(buildActionTd(item));

    const expandTr = buildExpandRowEl(expandBtn, item, EXPAND_TD_COLSPAN, () => buildExpandPanel(item));
    return { mainTr: tr, expandTr };
  }

  /* -------------------------------------------------------
     Build thead for a board
     wide: 排名|品牌|型号|尺寸|RGB灯光|参考价|综合评分·热度|操作
     narrow: 排名|型号信息|综合评分·热度|操作
     -------------------------------------------------------- */
  function buildThead(thead) {
    thead.innerHTML = '';
    const tr = document.createElement('tr');

    const cols = [
      { label: '排名',            cls: 'rpv2-rank-cell' },
      { label: '品牌',            cls: 'nowrap rpv2-col-wide' },
      { label: '型号',            cls: 'nowrap rpv2-col-model-name-wide' },
      { label: '尺寸',            cls: 'nowrap rpv2-col-wide' },
      { label: 'RGB灯光',         cls: 'nowrap rpv2-col-wide' },
      { label: '参考价',          cls: 'nowrap rpv2-col-wide rpv2-col-price' },
      { label: '型号信息',        cls: 'rpv2-col-narrow-info' },
      { label: '综合评分 · 热度', cls: 'rpv2-score-heat-th' },
      { label: '操作',            cls: 'rpv2-action-cell' },
    ];

    cols.forEach(c => {
      const th = document.createElement('th');
      th.className = c.cls;
      th.textContent = c.label;
      tr.appendChild(th);
    });

    thead.appendChild(tr);
  }

  /* -------------------------------------------------------
     Render a board (heat or perf) from data array
     -------------------------------------------------------- */
  function renderBoard(items, theadEl, tbodyEl, boardType) {
    buildThead(theadEl);
    tbodyEl.innerHTML = '';

    if (!items || items.length === 0) {
      const tr = document.createElement('tr');
      const td = document.createElement('td');
      td.setAttribute('colspan', '9');
      td.className = 'text-center text-gray-500 py-8';
      td.style.fontSize = '14px';
      td.textContent = '暂无数据';
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
     Columns (wide): 品牌|型号|尺寸|RGB灯光|参考价|型号信息(narrow)|综合·工况评分|col-A|col-B|操作
     col-A/col-B depend on sortBy (see buildSearchThead / buildSearchResultsRow).
     Expand panel: mini radar (left) + condition likes bars (right)
     ============================================================= */

  // colspan spanning all search results columns:
  // 品牌+型号+尺寸+RGB+参考价+型号信息(narrow)+综合·工况评分+col-A+col-B+操作 = 10
  const SEARCH_EXPAND_COLSPAN = 10;

  /* -------------------------------------------------------
     Build search expand panel (left: mini radar, right: condition likes)
     ------------------------------------------------------- */
  function buildSearchExpandPanel(item) {
    const inner = document.createElement('div');
    inner.className = 'rpv2-expand-inner';

    /* --- Mini radar col (left) — identical to rankings expand panel --- */
    const radarCol = document.createElement('div');
    radarCol.className = 'rpv2-mini-radar-col';

    const hasScores = RADAR_CIDS.some(cid => {
      const s = item.condition_scores && item.condition_scores[cid];
      return s !== null && s !== undefined;
    });

    if (hasScores && typeof window.buildMiniRadarSVG === 'function') {
      const radarItems = RADAR_CIDS.map(cid => ({
        condition_id: cid,
        score_total: item.condition_scores ? item.condition_scores[cid] : null,
      }));
      const labelCache = window.__condLabelCache || {};
      const radarBody = document.createElement('div');
      radarBody.className = 'rpv2-mini-radar-body';
      radarBody.innerHTML = window.buildMiniRadarSVG(
        radarItems,
        item.composite_score,
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
        '<span>综合评分数据暂未加载</span>';
      radarCol.appendChild(ph);
    }

    inner.appendChild(radarCol);

    /* --- Condition likes col (right) — shows absolute like counts, not percentages --- */
    const likesCol = document.createElement('div');
    likesCol.className = 'rpv2-heat-breakdown-col';

    const title = document.createElement('div');
    title.className = 'rpv2-heat-breakdown-title';
    title.textContent = '工况好评';
    likesCol.appendChild(title);

    const condRows = document.createElement('div');
    condRows.className = 'rpv2-cond-rows';

    const condLikes = item.condition_likes || {};
    const maxLikes = Math.max(1, ...RADAR_CIDS.map(c => condLikes[c] || 0));

    RADAR_CIDS.forEach(cid => {
      const lk = condLikes[cid] || 0;
      // Bar width: normalized to local max so the most-liked condition fills the bar
      const barW = Math.round((lk / maxLikes) * 100);

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
      barFill.style.width = barW + '%';

      // Label shows absolute like count (not a percentage)
      const countEl = document.createElement('span');
      countEl.className = 'rpv2-cond-pct';
      countEl.textContent = lk > 0 ? String(lk) : '0';

      barTrack.appendChild(barFill);
      barArea.appendChild(barTrack);
      barArea.appendChild(countEl);

      row.appendChild(nameEl);
      row.appendChild(barArea);
      condRows.appendChild(row);
    });

    likesCol.appendChild(condRows);
    inner.appendChild(likesCol);
    return inner;
  }

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
    const compositeBadge = buildScoreBadge(item.composite_score);

    // Separator + condition score badge (can be hidden on narrow non-condition_score via CSS)
    const condSep = document.createElement('span');
    condSep.className = 'rpv2-sep srp-cond-score-sep';
    condSep.textContent = '·';

    const condBadge = buildScoreBadge(item.condition_score);
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
      // 测试转速 or 测试分贝: effective_x value + source tag
      const xVal = item.effective_x;
      const src = item.effective_source; // 'raw' | 'fit'
      const isNoise = (sortBy === 'noise');
      const valStr = (xVal !== null && xVal !== undefined)
        ? (isNoise ? Number(xVal).toFixed(1) : String(Math.round(Number(xVal))))
        : null;
      if (valStr) {
        tdColA.appendChild(document.createTextNode(valStr));
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

    tr.appendChild(buildActionTd(item));

    const expandTr = buildExpandRowEl(expandBtn, item, SEARCH_EXPAND_COLSPAN, () => buildSearchExpandPanel(item));

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
    } else if (sortBy === 'noise') {
      colALabel = '测试分贝';
      colBLabel = '风量(CFM)';
    } else {
      // 'none' (全速风量) or 'rpm' (同转速风量)
      colALabel = '测试转速';
      colBLabel = '风量(CFM)';
    }

    const cols = [
      { label: '品牌',           cls: 'nowrap rpv2-col-wide' },
      { label: '型号',           cls: 'nowrap rpv2-col-model-name-wide' },
      { label: '尺寸',           cls: 'nowrap rpv2-col-wide' },
      { label: 'RGB灯光',        cls: 'nowrap rpv2-col-wide' },
      { label: '参考价',         cls: 'nowrap rpv2-col-wide rpv2-col-price' },
      { label: '型号信息',       cls: 'rpv2-col-narrow-info' },
      // Fixed combined score column — header has two spans for narrow/wide label variants
      { label: null,             cls: 'rpv2-score-heat-th', twoSpan: ['综合 · 工况评分', '综合评分'] },
      { label: colALabel,        cls: 'srp-col-A nowrap' },
      { label: colBLabel,        cls: 'srp-col-B nowrap' },
      { label: '操作',           cls: 'rpv2-action-cell' },
    ];

    cols.forEach(c => {
      const th = document.createElement('th');
      th.className = c.cls;
      if (c.twoSpan) {
        // Full label (wide / condition_score narrow)
        const full = document.createElement('span');
        full.className = 'srp-th-full';
        full.textContent = c.twoSpan[0];
        // Short label (narrow non-condition_score)
        const short = document.createElement('span');
        short.className = 'srp-th-short';
        short.textContent = c.twoSpan[1];
        th.appendChild(full);
        th.appendChild(short);
      } else {
        th.textContent = c.label;
      }
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
      { label: '品牌',             cls: 'nowrap rpv2-col-wide' },
      { label: '型号',             cls: 'nowrap rpv2-col-model-name-wide' },
      { label: '尺寸',             cls: 'nowrap rpv2-col-wide' },
      { label: 'RGB灯光',          cls: 'nowrap rpv2-col-wide' },
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
    tr.appendChild(buildActionTd(item));

    const expandTr = buildExpandRowEl(expandBtn, item, SEARCH_COMPOSITE_EXPAND_COLSPAN, () => buildExpandPanel(item));
    return { mainTr: tr, expandTr };
  }

  /* =============================================================
     Recent Updates v2 — model-centric list sorted by update date
     Reuses rpv2-* styles from right-panel-v2.css.
     Columns (wide): 品牌|型号|尺寸|RGB灯光|参考价|型号信息(narrow)|综合评分·热度|更新日期|操作
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
      { label: '品牌',             cls: 'nowrap rpv2-col-wide' },
      { label: '型号',             cls: 'nowrap rpv2-col-model-name-wide' },
      { label: '尺寸',             cls: 'nowrap rpv2-col-wide' },
      { label: 'RGB灯光',          cls: 'nowrap rpv2-col-wide' },
      { label: '参考价',           cls: 'nowrap rpv2-col-wide rpv2-col-price' },
      { label: '型号信息',         cls: 'rpv2-col-narrow-info' },
      { label: '综合评分 · 热度',  cls: 'rpv2-score-heat-th' },
      { label: '更新日期',         cls: 'nowrap rpv2-col-wide rup-col-date' },
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
    tdDate.className = 'nowrap rpv2-col-wide rup-col-date';
    tdDate.textContent = item.update_date || '—';
    tr.appendChild(tdDate);

    tr.appendChild(buildActionTd(item));

    const expandTr = buildExpandRowEl(expandBtn, item, RECENT_UPDATES_EXPAND_COLSPAN, () => buildExpandPanel(item));
    return { mainTr: tr, expandTr };
  }

  /* -------------------------------------------------------
     Render the recent-updates table
     ------------------------------------------------------- */
  function renderRecentUpdatesTable(items) {
    const theadEl = document.getElementById('rup-thead');
    const tbodyEl = document.getElementById('rup-tbody');
    if (!theadEl || !tbodyEl) return;

    buildRecentUpdatesThead(theadEl);
    tbodyEl.innerHTML = '';

    if (!items || items.length === 0) {
      const tr = document.createElement('tr');
      const td = document.createElement('td');
      td.setAttribute('colspan', String(RECENT_UPDATES_EXPAND_COLSPAN));
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
      td.setAttribute('colspan', String(RECENT_UPDATES_EXPAND_COLSPAN));
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
      })
      .catch(() => {
        const tbodyEl = document.getElementById('rup-tbody');
        if (tbodyEl) {
          tbodyEl.innerHTML = '';
          const tr = document.createElement('tr');
          const td = document.createElement('td');
          td.setAttribute('colspan', String(RECENT_UPDATES_EXPAND_COLSPAN));
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
    const theadEl = document.getElementById('srp-thead');
    const tbodyEl = document.getElementById('srp-tbody');
    const tableEl = document.getElementById('srp-results-table');
    if (!theadEl || !tbodyEl) return;

    const resolvedSortBy = sortBy || 'condition_score';
    const isCompositeMode = (resolvedSortBy === 'composite_score');

    // Tag the table with the current sort mode so CSS can drive narrow-layout column rules
    if (tableEl) tableEl.dataset.sortBy = resolvedSortBy;

    if (isCompositeMode) {
      buildCompositeThead(theadEl);
    } else {
      buildSearchThead(theadEl, resolvedSortBy);
    }
    tbodyEl.innerHTML = '';

    if (!items || items.length === 0) {
      const tr = document.createElement('tr');
      const td = document.createElement('td');
      const colspan = isCompositeMode ? String(SEARCH_COMPOSITE_EXPAND_COLSPAN) : String(SEARCH_EXPAND_COLSPAN);
      td.setAttribute('colspan', colspan);
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
    if (tbodyEl) {
      tbodyEl.innerHTML = `<tr><td colspan="${SEARCH_EXPAND_COLSPAN}" class="text-center text-gray-500 py-8" style="font-size:14px;">使用按工况筛选进行搜索</td></tr>`;
    }
    if (tableEl) delete tableEl.dataset.sortBy;
    if (summaryEl) summaryEl.textContent = '';
  }

  /* -------------------------------------------------------
     Init: read data from DOM, render both boards
     -------------------------------------------------------- */
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

    const heatBoard = data.heat_board || [];
    const perfBoard = data.performance_board || [];

    const heatThead = document.getElementById('rpv2-heat-thead');
    const heatTbody = document.getElementById('rpv2-heat-tbody');
    const perfThead = document.getElementById('rpv2-perf-thead');
    const perfTbody = document.getElementById('rpv2-perf-tbody');

    if (heatThead && heatTbody) {
      renderBoard(heatBoard, heatThead, heatTbody, 'heat');
    }
    if (perfThead && perfTbody) {
      renderBoard(perfBoard, perfThead, perfTbody, 'perf');
    }

    // Sync add-to-radar button states after RadarState is ready
    syncAddButtons();
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
     Infrastructure: responsive rp-narrow class
     Adds/removes .rp-narrow on .fc-right-card based on card width.
     -------------------------------------------------------- */
  function initRightPanelResponsiveWrap() {
    const card = document.querySelector('.fc-right-card');
    if (!card) return;
    const NARROW_W = 520;
    const apply = (w) => {
      if (w < NARROW_W) card.classList.add('rp-narrow');
      else card.classList.remove('rp-narrow');
    };
    apply(card.getBoundingClientRect().width);
    if ('ResizeObserver' in window) {
      const ro = new ResizeObserver(entries => {
        for (const entry of entries) { apply(entry.contentRect.width); }
      });
      ro.observe(card);
    } else {
      window.addEventListener('resize', () => apply(card.getBoundingClientRect().width));
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
      subsegContainer.appendChild(queriesSeg);
    }
    // Show only when rankings tab is active
    updateSubsegVisibility('top-queries');
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
          if (tab === 'recent-updates') {
            loadRecentUpdatesIfNeeded();
          }
        },
        clickScrollBehavior: 'smooth',
      });
    }
  }

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
    initRightPanelSnapTabs();
    initRightPanelResponsiveWrap();
    mountRightSubseg();
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
