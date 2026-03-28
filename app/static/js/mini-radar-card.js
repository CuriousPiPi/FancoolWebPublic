/* =============================================================
   mini-radar-card.js
   Shared MiniRadarCard system:
     - Mini-radar SVG renderer with correct label/thumb geometry
     - Geometry / layout constants
     - Condition label positioning (with optional like-thumb outward offset)
     - Like-thumb overlay positioning (at original label anchor positions)
     - Quick-add button module
     - Card DOM assembly (header spans full width; body row = radar + add-col)
     - Card header priority/fade (CSS-only; no JS measurement needed)
   ============================================================= */

(function initMiniRadarCardSystem() {
  'use strict';

  // ----------------------------------------------------------------
  // Escape helper (falls back to inline implementation before fancool.js loads)
  // ----------------------------------------------------------------
  function EH(s) {
    if (typeof window.escapeHtml === 'function') return window.escapeHtml(s);
    const _ESC = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' };
    return String(s ?? '').replace(/[&<>"']/g, c => _ESC[c]);
  }

  // ----------------------------------------------------------------
  // Radar condition IDs in hexagon slot order
  // ----------------------------------------------------------------
  const RADAR_CIDS_COLOR = [1, 2, 3, 8, 7, 10];
  const RADAR_TOTAL_RANKS = RADAR_CIDS_COLOR.length; // 6
  window.RADAR_CIDS_COLOR  = RADAR_CIDS_COLOR;
  window.RADAR_TOTAL_RANKS = RADAR_TOTAL_RANKS;

  // ----------------------------------------------------------------
  // Mini-radar SVG geometry constants
  // ViewBox: 0 0 300 120  (W × H), center (150, 60)
  // Narrower viewBox (300 vs 400) reduces horizontal dead space on each
  // side of the radar polygon, so the graphic makes better use of the
  // available width in both sidebar cards and the "按型号添加" panel.
  // ----------------------------------------------------------------
  const _W  = 300, _H  = 120, _CX = 150, _CY = 60;
  const _GRID_R  = 52;                   // radar polygon radius (SVG units)
  const _SIDE_R  = _GRID_R + 35;        // 82 — label radius for L/R middle slots
  const _SCORE_OFFSET  = 10;            // px gap from data point to score label
  const _UNSCORED_RATIO = 0.06;         // fraction of gridR when score is null

  // Center badge geometry — mirrors .rpv2-score-badge tokens exactly so the
  // mini-radar badge matches rankings / comparison radar badge proportions.
  const _BADGE_H          = 24;  // matches .rpv2-score-badge height: 24px
  const _BADGE_MIN_W      = 36;  // matches .rpv2-score-badge min-width: 36px
  const _BADGE_RX         = 12;  // matches .rpv2-score-badge border-radius: 12px
  const _BADGE_PAD_H      = 6;   // horizontal padding per side (matches padding: 0 6px)
  const _BADGE_FONT_SZ    = 13;  // matches .rpv2-score-badge font-size: 13px
  // Approximate glyph widths in SVG units at font-size 13 for badge width estimation.
  // CJK glyphs (e.g. "综评") are roughly square at font-size 13; Latin/digit glyphs are narrower.
  const _BADGE_CJK_W      = 12;  // SVG units per CJK character
  const _BADGE_ASCII_W    = 8;   // SVG units per ASCII digit/letter
  // Unscored placeholder colors — CSS variables so they remain theme-aware even
  // when written as inline SVG fill (CSS vars inside SVG fill are supported in
  // all modern browsers).
  const _BADGE_UNSCORED_BG      = 'var(--fc-color-primary)';
  const _BADGE_UNSCORED_OPACITY = '0.10';
  const _BADGE_UNSCORED_FG      = 'var(--text-primary)';

  // Corner label horizontal offset from center
  const _CORNER_X = Math.round(_SIDE_R * 0.65); // 45

  // Corner label Y positions (top/bottom inset)
  const _CORNER_Y = 14;

  // Baseline label slots: [lx, ly, anchor]
  // Left labels  (i=0,4,5): anchor="end"   — text extends LEFT  from lx
  // Right labels (i=1,2,3): anchor="start" — text extends RIGHT from lx
  const _LABEL_SLOTS = [
    [_CX - _CORNER_X,  _CORNER_Y,      'end'  ], // i=0 UL (left)
    [_CX + _CORNER_X,  _CORNER_Y,      'start'], // i=1 UR (right)
    [_CX + _SIDE_R,    _CY,            'start'], // i=2 R  (right)
    [_CX + _CORNER_X,  _H - _CORNER_Y, 'start'], // i=3 LR (right)
    [_CX - _CORNER_X,  _H - _CORNER_Y, 'end'  ], // i=4 LL (left)
    [_CX - _SIDE_R,    _CY,            'end'  ], // i=5 L  (left)
  ];

  // When like-thumbs are shown, labels shift outward so thumbs can sit at the
  // original anchor positions without overlapping condition score text.
  const _LIKE_THUMB_EXTRA = 15; // px outward shift per side when showLikeThumbs

  // ----------------------------------------------------------------
  // Like-thumb button positions (% of SVG viewBox W × H)
  // These are the baseline LABEL_SLOT anchor positions, which place the thumbs
  // well outside the data polygon, reducing overlap with score labels.
  // ----------------------------------------------------------------
  const MINI_RADAR_THUMB_POS = _LABEL_SLOTS.map(([lx, ly]) => ({
    left: parseFloat((lx / _W * 100).toFixed(4)),
    top:  parseFloat((ly / _H * 100).toFixed(4)),
  }));

  // ----------------------------------------------------------------
  // buildMiniRadarSVG
  // ----------------------------------------------------------------
  /**
   * Build a mini-radar SVG string. Shared renderer for all card types.
   *
   * @param {Array|null}  items          [{condition_id, score_total}] or null
   * @param {number|null} compositeScore shown in center circle
   * @param {Object|null} condLabelCache {condId: label}; falls back to window.__condLabelCache
   * @param {string|null} svgId          optional SVG element id
   * @param {Object}      [opts]
   *   opts.showLikeThumbs {boolean} – when true, shift labels outward by _LIKE_THUMB_EXTRA
   * @returns {string} SVG HTML string
   */
  function buildMiniRadarSVG(items, compositeScore, condLabelCache, svgId, opts) {
    const CIDS       = RADAR_CIDS_COLOR;
    const withThumbs = !!(opts && opts.showLikeThumbs);

    const itemMap = {};
    if (Array.isArray(items)) {
      items.forEach(it => { if (it) itemMap[Number(it.condition_id)] = it; });
    }

    // Merge global cache first so an explicitly-passed empty condLabelCache does
    // not block the global labels (empty {} is truthy and would silently suppress
    // the fallback in a plain `condLabelCache || window.__condLabelCache || {}`).
    const labelCache = Object.assign({}, window.__condLabelCache || {}, condLabelCache || {});

    function axisAngle(i) { return -Math.PI / 2 - Math.PI / 6 + (i * 2 * Math.PI / 6); }
    function vpt(r, i) {
      const a = axisAngle(i);
      return [_CX + r * Math.cos(a), _CY + r * Math.sin(a)];
    }
    function polyPts(rfn) {
      return CIDS.map((cid, i) => {
        const [x, y] = vpt(rfn(cid, i), i);
        return x.toFixed(1) + ',' + y.toFixed(1);
      }).join(' ');
    }

    const rings   = [0.25, 0.5, 0.75, 1.0].map(f => polyPts(() => f * _GRID_R));
    const dataPts = polyPts((cid) => {
      const it = itemMap[cid];
      if (!it) return 0;
      if (it.score_total == null) return _GRID_R * _UNSCORED_RATIO;
      return (Math.max(0, Math.min(100, it.score_total)) / 100) * _GRID_R;
    });

    // Center badge score (computed early so it can be embedded as SVG attribute for theme-refresh).
    const scoreNum = (compositeScore != null && Number.isFinite(Number(compositeScore)))
      ? Number(compositeScore) : null;

    const idAttr = svgId ? ` id="${EH(svgId)}"` : '';
    // data-badge-score is used by _refreshMiniRadarBadgeColors() to re-apply
    // ScoreBadgeHelper colors when the page theme changes (without rebuilding the SVG).
    const badgeScoreAttr = ` data-badge-score="${scoreNum != null ? scoreNum : ''}"`;
    let svg = `<svg${idAttr}${badgeScoreAttr} class="fc-radar-svg" viewBox="0 0 ${_W} ${_H}" xmlns="http://www.w3.org/2000/svg" aria-label="综合评分雷达图" role="img">`;

    // Grid rings
    rings.forEach(pts => { svg += `<polygon class="fc-radar-ring" points="${pts}"/>`; });
    // Axis lines
    CIDS.forEach((cid, i) => {
      const [vx, vy] = vpt(_GRID_R, i);
      svg += `<line class="fc-radar-axis" x1="${_CX}" y1="${_CY}" x2="${vx.toFixed(1)}" y2="${vy.toFixed(1)}"/>`;
    });
    // Data polygon
    svg += `<polygon class="fc-radar-area" points="${dataPts}"/>`;

    // Score labels (close to data points)
    CIDS.forEach((cid, i) => {
      const it = itemMap[cid];
      if (!it || it.score_total == null) return;
      const r = (Math.max(0, Math.min(100, it.score_total)) / 100) * _GRID_R;
      const [slx, sly] = vpt(r + _SCORE_OFFSET, i);
      svg += `<text class="fc-radar-score-lbl" x="${slx.toFixed(1)}" y="${sly.toFixed(1)}" text-anchor="middle" dominant-baseline="middle">${Math.round(it.score_total)}</text>`;
    });

    // Condition labels — shifted outward when showLikeThumbs to make room for thumbs
    CIDS.forEach((cid, i) => {
      const rawLabel = labelCache[cid] || itemMap[cid]?.condition_name_zh || String(cid);
      const label    = EH(rawLabel);
      const available = !!itemMap[cid];
      let [lx, ly, anchor] = _LABEL_SLOTS[i];
      if (withThumbs) {
        // anchor="end" → left-side label; anchor="start" → right-side label
        if (anchor === 'end') lx -= _LIKE_THUMB_EXTRA;   // shift further left
        else                  lx += _LIKE_THUMB_EXTRA;   // shift further right
      }
      const cls = 'fc-radar-lbl' + (available ? '' : ' unavail');
      svg += `<text class="${cls}" x="${lx.toFixed(1)}" y="${ly.toFixed(1)}" text-anchor="${anchor}" dominant-baseline="middle" style="pointer-events:none">${label}</text>`;
    });

    // Center badge — pill shape mirroring .rpv2-score-badge geometry tokens
    // (height: 24, min-width: 36, padding: 0 6, border-radius: 12, font-size: 13, font-weight: 700)
    // so the mini-radar composite score badge matches rankings and comparison-radar exactly.
    const scoreText = scoreNum != null ? String(Math.round(scoreNum)) : '综评';
    // Always apply inline fill styles so they are never left for a CSS class rule to decide.
    // For the scored state ScoreBadgeHelper provides exact Oklab-interpolated colors;
    // for the unscored/placeholder state we use CSS-variable strings so the badge stays
    // theme-aware while the inline style still has higher specificity than any class rule.
    let bgFill    = _BADGE_UNSCORED_BG;
    let bgOpacity = _BADGE_UNSCORED_OPACITY;
    let fgFill    = _BADGE_UNSCORED_FG;
    if (scoreNum != null && window.ScoreBadgeHelper) {
      const { bg, fg } = window.ScoreBadgeHelper.scoreStyle(scoreNum);
      bgFill    = bg;
      bgOpacity = '1';
      fgFill    = fg;
    }
    const badgeRectStyle = ` style="fill:${bgFill};fill-opacity:${bgOpacity};stroke:none"`;
    // Fix vertical centering: omit dominant-baseline and use dy="0.35em" instead.
    // dominant-baseline="middle" aligns the mathematical em-box midpoint at y, which
    // places digit glyphs visually above center.  dy="0.35em" shifts the text baseline
    // down so the optical center of the digits lands at the badge rect center (_CY).
    const badgeLblStyle  = ` style="fill:${fgFill}"`;
    // Estimate text width: CJK chars ~_BADGE_CJK_W SVG units, ASCII digits ~_BADGE_ASCII_W SVG units.
    const _charW = /[\u4e00-\u9fff]/.test(scoreText) ? _BADGE_CJK_W : _BADGE_ASCII_W;
    const badgeW = Math.max(_BADGE_MIN_W, scoreText.length * _charW + _BADGE_PAD_H * 2);
    const bx = (_CX - badgeW / 2).toFixed(1);
    const by = (_CY - _BADGE_H / 2).toFixed(1);
    svg += `<rect class="fc-radar-center-badge" x="${bx}" y="${by}" width="${badgeW}" height="${_BADGE_H}" rx="${_BADGE_RX}" ry="${_BADGE_RX}" aria-label="综合评分"${badgeRectStyle}/>`;
    svg += `<text class="fc-radar-center-badge-lbl" x="${_CX}" y="${_CY}" dy="0.35em" text-anchor="middle" font-size="${_BADGE_FONT_SZ}" font-weight="700"${badgeLblStyle}>${EH(scoreText)}</text>`;
    svg += `</svg>`;
    return svg;
  }
  window.buildMiniRadarSVG = buildMiniRadarSVG;

  // ----------------------------------------------------------------
  // Theme-change color refresh for SVG center badges
  // ----------------------------------------------------------------
  // When the page theme changes, all mini-radar SVG badges need their
  // inline fill colors recalculated (because they were written with the
  // hex values from ScoreBadgeHelper at build time and cannot update
  // automatically via CSS).  The SVG carries data-badge-score so we can
  // do this without rebuilding the whole SVG.
  function _refreshMiniRadarBadgeColors() {
    if (!window.ScoreBadgeHelper) return;
    document.querySelectorAll('svg.fc-radar-svg[data-badge-score]').forEach(function (svgEl) {
      var raw = svgEl.getAttribute('data-badge-score');
      var scoreNum = (raw !== null && raw !== '') ? Number(raw) : null;
      var isScored = scoreNum !== null && Number.isFinite(scoreNum);
      var bgFill, bgOpacity, fgFill;
      if (isScored) {
        var style = window.ScoreBadgeHelper.scoreStyle(scoreNum);
        bgFill    = style.bg;
        bgOpacity = '1';
        fgFill    = style.fg;
      } else {
        bgFill    = _BADGE_UNSCORED_BG;
        bgOpacity = _BADGE_UNSCORED_OPACITY;
        fgFill    = _BADGE_UNSCORED_FG;
      }
      var rect = svgEl.querySelector('.fc-radar-center-badge');
      var text = svgEl.querySelector('.fc-radar-center-badge-lbl');
      if (rect) rect.style.cssText = 'fill:' + bgFill + ';fill-opacity:' + bgOpacity + ';stroke:none';
      if (text) text.style.fill = fgFill;
    });
  }

  // Observe data-theme attribute on <html> and refresh all mini-radar SVG badges.
  try {
    new MutationObserver(function (mutations) {
      for (var i = 0; i < mutations.length; i++) {
        if (mutations[i].type === 'attributes' && mutations[i].attributeName === 'data-theme') {
          _refreshMiniRadarBadgeColors();
          return;
        }
      }
    }).observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
  } catch (_) {}

  // ----------------------------------------------------------------
  // _scheduleThumbReposition
  // Part 3 fix: Position like-thumb buttons using actual SVG rendered
  // geometry (mirrors fancool-search.js "按型号添加" approach) so
  // buttons stay stable when sidebar width changes.
  // ----------------------------------------------------------------

  /**
   * Mirror the SVG geometry to compute the pixel position of each label slot
   * within the rendered `.fc-mini-radar-body` element.
   *
   * SVG uses `preserveAspectRatio="xMidYMid meet"` (default), so the displayed
   * SVG region may be smaller than the container and centred inside it.
   * We account for this letterboxing so thumb positions match label positions.
   *
   * @param {HTMLElement} bodyEl  – `.fc-mini-radar-body` container
   * @returns {{slotPx: Array<{x:number, y:number}>, ok:boolean}}
   */
  function _computeThumbPositionsPx(bodyEl) {
    const W_css = bodyEl.offsetWidth;
    const H_css = bodyEl.offsetHeight;
    if (!W_css || !H_css) return { slotPx: [], ok: false };

    // SVG viewBox dimensions
    const vbW = _W, vbH = _H;
    const aspect = vbW / vbH;       // 300/120 = 2.5
    const containerAspect = W_css / H_css;

    // Determine the rendered SVG rect inside the container (meet = fit inside, centred)
    let svgW, svgH, offsetX, offsetY;
    if (containerAspect >= aspect) {
      // Height-constrained: SVG fills full height, letterboxed left/right
      svgH = H_css;
      svgW = H_css * aspect;
      offsetX = (W_css - svgW) / 2;
      offsetY = 0;
    } else {
      // Width-constrained: SVG fills full width, letterboxed top/bottom
      svgW = W_css;
      svgH = W_css / aspect;
      offsetX = 0;
      offsetY = (H_css - svgH) / 2;
    }

    const scale = svgW / vbW; // px per SVG unit

    const slotPx = _LABEL_SLOTS.map(([lx, ly]) => ({
      x: offsetX + lx * scale,
      y: offsetY + ly * scale,
    }));

    return { slotPx, ok: true };
  }

  /**
   * (Re)position all `.fc-radar-thumb-btn[data-thumb-slot]` elements inside
   * bodyEl using the actual SVG geometry, then install a ResizeObserver so
   * positions stay correct when the sidebar width changes.
   *
   * @param {HTMLElement} bodyEl  – `.fc-mini-radar-body` container
   */
  function _scheduleThumbReposition(bodyEl) {
    function reposition() {
      const { slotPx, ok } = _computeThumbPositionsPx(bodyEl);
      if (!ok) return;
      bodyEl.querySelectorAll('.fc-radar-thumb-btn[data-thumb-slot]').forEach(btn => {
        const i = parseInt(btn.getAttribute('data-thumb-slot'), 10);
        if (i >= 0 && i < slotPx.length) {
          btn.style.left = slotPx[i].x + 'px';
          btn.style.top  = slotPx[i].y + 'px';
        }
      });
    }

    // Run once after layout settles (two RAF cycles for reliability)
    requestAnimationFrame(() => {
      reposition();
      requestAnimationFrame(reposition);
    });

    // Keep positions correct when the sidebar width changes
    if (typeof ResizeObserver !== 'undefined' && !bodyEl._thumbResizeObserver) {
      const ro = new ResizeObserver(reposition);
      ro.observe(bodyEl);
      bodyEl._thumbResizeObserver = ro;
    }
  }

  // ----------------------------------------------------------------
  // buildMiniRadarCardEl
  // ----------------------------------------------------------------
  /**
   * Build a mini-radar card DOM element.
   *
   * Layout:
   *   .fc-mini-radar-card
   *     ├─ .fc-card-header          (full card width; only when showHeader)
   *     │    ├─ .fc-card-header-left   (brand + model + price; priority)
   *     │    └─ .fc-card-header-right  (RPM + size; lower priority, clips first)
   *     └─ .fc-mini-radar-body-row  (flex row)
   *          ├─ .fc-mini-radar-body     (SVG + optional like thumbs)
   *          └─ .fc-mini-radar-add-col  (quick-add button; only when showQuickAdd)
   *
   * @param {Object} cfg
   *   brand, model, price (reference_price), maxSpeed, size, thickness, rgbLight,
   *   modelId, radarItems [{condition_id, score_total}], compositeScore,
   *   condLabelCache, showHeader, showLikeThumbs, showQuickAdd,
   *   minimal — shortcut that sets showHeader=false, showLikeThumbs=false, showQuickAdd=false,
   *             keeping only the radar chart, condition labels, condition scores, and
   *             composite score. Individual flags override minimal when explicitly provided.
   * @returns {HTMLElement}
   */
  function buildMiniRadarCardEl(cfg) {
    const rawCfg = cfg || {};
    // If minimal mode is requested, default the three hide-flags to false unless overridden.
    const isMinimal = !!rawCfg.minimal;
    const {
      brand = '', model = '', price = null,
      maxSpeed = null, size = null, thickness = null, rgbLight = null,
      modelId,
      radarItems    = null, compositeScore = null,
      condLabelCache = null,
      showHeader    = !isMinimal,
      showLikeThumbs = false,
      showQuickAdd   = !isMinimal,
    } = rawCfg;

    const mid = modelId != null ? String(modelId) : '';

    // Side-cache model metadata so browsing history cards can reuse it.
    // Merge with any existing entry so a previously-loaded `review` field is not lost.
    if (mid) {
      window.__modelMetaCache = window.__modelMetaCache || {};
      const existing = window.__modelMetaCache[mid] || {};
      window.__modelMetaCache[mid] = {
        ...existing,
        modelId: mid, brand, model,
        reference_price: price, max_speed: maxSpeed,
        size, thickness, rgb_light: rgbLight,
      };
    }

    const card = document.createElement('div');
    card.className = 'fc-mini-radar-card';

    // ---- Header (spans full card width) ----
    if (showHeader) {
      const priceStr = price != null ? `（¥ ${EH(String(price))}）` : '';
      const leftText = `${EH(brand)} ${EH(model)}${priceStr}`.trim();

      const rightParts = [];
      if (maxSpeed) rightParts.push(`${EH(String(maxSpeed))} RPM`);
      if (size && thickness) rightParts.push(`${EH(String(size))}×${EH(String(thickness))}`);
      else if (size)         rightParts.push(EH(String(size)));
      const rightText = rightParts.join(' · ');

      const header = document.createElement('div');
      header.className = 'fc-card-header';
      header.innerHTML =
        `<div class="fc-card-header-left">${leftText}</div>` +
        (rightText ? `<div class="fc-card-header-right">${rightText}</div>` : '');
      card.appendChild(header);
    }

    // ---- Body row: radar SVG + optional quick-add column ----
    const bodyRow = document.createElement('div');
    bodyRow.className = 'fc-mini-radar-body-row';

    const radarBody = document.createElement('div');
    radarBody.className = 'fc-mini-radar-body';
    radarBody.innerHTML = buildMiniRadarSVG(
      radarItems, compositeScore, condLabelCache, null, { showLikeThumbs }
    );

    // Like-thumb overlays (只在 showLikeThumbs=true 时添加)
    if (showLikeThumbs && mid) {
      RADAR_CIDS_COLOR.forEach((cid, i) => {
        const pos     = MINI_RADAR_THUMB_POS[i];
        const isLiked = !!(window.LocalState && window.LocalState.likes &&
                           window.LocalState.likes.has(`${mid}_${cid}`));
        const btn     = document.createElement('button');
        btn.type      = 'button';
        btn.className = 'fc-radar-thumb-btn like-button fc-tooltip-target';
        btn.setAttribute('data-model-id',     mid);
        btn.setAttribute('data-condition-id', String(cid));
        btn.setAttribute('data-tooltip',      isLiked ? '取消点赞' : '点赞');
        // Store slot index for JS repositioning
        btn.setAttribute('data-thumb-slot', String(i));
        // Initial percentage-based position (overridden by _repositionThumbs)
        btn.style.left = pos.left + '%';
        btn.style.top  = pos.top  + '%';
        btn.innerHTML  = `<i class="fa-solid fa-thumbs-up ${isLiked ? 'text-red-500' : 'text-gray-400'}"></i>`;
        radarBody.appendChild(btn);
      });

      // Re-position thumb buttons based on actual rendered SVG geometry.
      // This corrects drift caused by SVG preserveAspectRatio letterboxing.
      _scheduleThumbReposition(radarBody);
    }
    bodyRow.appendChild(radarBody);

    // Quick-add column (右侧竖向分割区，仅含按钮)
    if (showQuickAdd && mid) {
      const addCol = document.createElement('div');
      addCol.className = 'fc-mini-radar-add-col';
      // Determine initial state: is this model already in the radar?
      // Fall back to LocalState persisted radar membership so buttons render
      // correctly even when RadarState has not yet been populated during page load
      // (i.e. before restoreRadarFromStorage() has run).
      const inRadar = !!(
        (window.RadarState && typeof window.RadarState.hasModel === 'function' && window.RadarState.hasModel(mid)) ||
        (window.LocalState && typeof window.LocalState.getRadarModels === 'function' &&
          window.LocalState.getRadarModels().some(m => String(m.modelId) === String(mid)))
      );
      const addBtn = (typeof window.buildRadarToggleBtnEl === 'function')
        ? window.buildRadarToggleBtnEl({ modelId: mid, brand, label: model, state: inRadar ? 'added' : 'add' })
        : (() => {
            // Fallback if module not yet loaded (should not happen in normal load order)
            const btn = document.createElement('button');
            btn.type = 'button';
            btn.className = 'fc-radar-toggle-btn js-mini-radar-add fc-tooltip-target';
            btn.setAttribute('data-model-id', EH(mid));
            btn.setAttribute('data-brand', EH(brand));
            btn.setAttribute('data-label', EH(model));
            btn.setAttribute('data-state', 'add');
            btn.setAttribute('data-tooltip', '添加到雷达对比');
            return btn;
          })();
      addCol.appendChild(addBtn);
      bodyRow.appendChild(addCol);
    }

    card.appendChild(bodyRow);
    return card;
  }
  window.buildMiniRadarCardEl = buildMiniRadarCardEl;

  // ----------------------------------------------------------------
  // applyCardHeaderMask — backward-compatibility stub
  // Card header priority masking is now handled purely by CSS:
  //   .fc-card-header-left  has background: var(--bg-secondary) which naturally
  //   shields the absolutely-positioned .fc-card-header-right underneath it.
  // ----------------------------------------------------------------
  function applyCardHeaderMask(/* scope */) {
    // No-op: CSS-only approach; JS measurement no longer needed.
  }
  window.applyCardHeaderMask = applyCardHeaderMask;

})();
