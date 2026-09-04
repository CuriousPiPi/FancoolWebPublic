(function initMiniRadarCardSystem() {
  'use strict';

  // Escape helper before fancool.js exposes window.escapeHtml.
  function EH(s) {
    if (typeof window.escapeHtml === 'function') return window.escapeHtml(s);
    const _ESC = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' };
    return String(s ?? '').replace(/[&<>"']/g, c => _ESC[c]);
  }

  if (typeof window.buildRgbTagEl !== 'function') {
    window.buildRgbTagEl = function buildRgbTagEl(opts) {
      const options = opts || {};
      const tag = document.createElement('span');
      tag.className = 'rpv2-mi-rgb-tag';
      if (options.large) tag.classList.add('fc-radar-rgb-tag');
      if (options.muted) tag.classList.add('is-muted');
      tag.textContent = 'RGB';
      return tag;
    };
  }

  // Backend config is CCW from UL; _LABEL_SLOTS below are CW.
  const _DEFAULT_RADAR_CIDS_CCW = [1, 10, 7, 8, 3, 2];
  const _configuredRadarCids = window.APP_CONFIG && window.APP_CONFIG.radarCids;
  const _RADAR_CIDS_CCW = Array.isArray(_configuredRadarCids) && _configuredRadarCids.length === 6
    ? _configuredRadarCids
    : _DEFAULT_RADAR_CIDS_CCW;
  if (_RADAR_CIDS_CCW !== _configuredRadarCids && typeof console !== 'undefined' && typeof console.warn === 'function') {
    console.warn('mini-radar-card: APP_CONFIG.radarCids must contain exactly 6 items; falling back to default radarCids.');
  }
  const _N_RADAR = _RADAR_CIDS_CCW.length;
  // CW order paired with _LABEL_SLOTS for label rendering.
  const _LABEL_SLOT_CIDS = Array.from({ length: _N_RADAR }, (_, j) => _RADAR_CIDS_CCW[(_N_RADAR - j) % _N_RADAR]);
  const RADAR_CIDS_COLOR = _LABEL_SLOT_CIDS;
  const RADAR_TOTAL_RANKS = RADAR_CIDS_COLOR.length;
  window.RADAR_CIDS_COLOR  = RADAR_CIDS_COLOR;
  window.RADAR_TOTAL_RANKS = RADAR_TOTAL_RANKS;

  // Mini-radar SVG geometry constants.
  const _W  = 300, _H  = 120, _CX = 150, _CY = 60;
  const _GRID_R  = 52;                   // radar polygon radius (SVG units)
  const _SIDE_R  = _GRID_R + 35;        // 82 — label radius for L/R middle slots
  const _SCORE_OFFSET  = 10;            // px gap from data point to score label
  const _UNSCORED_RATIO = 0.06;         // fraction of gridR when score is null

  // Keep center badge dimensions aligned with .rpv2-score-badge.
  const _BADGE_H          = 24;  // matches .rpv2-score-badge height: 24px
  const _BADGE_MIN_W      = 36;  // matches .rpv2-score-badge min-width: 36px
  const _BADGE_RX         = 12;  // matches .rpv2-score-badge border-radius: 12px
  const _BADGE_PAD_H      = 6;   // horizontal padding per side (matches padding: 0 6px)
  const _BADGE_FONT_SZ    = 13;  // matches .rpv2-score-badge font-size: 13px
  // Approximate glyph widths for badge width estimation.
  const _BADGE_CJK_W      = 12;  // SVG units per CJK character
  const _BADGE_ASCII_W    = 8;   // SVG units per ASCII digit/letter
  // CSS variables keep the unscored badge theme-aware.
  const _BADGE_UNSCORED_BG      = 'var(--fc-color-primary)';
  const _BADGE_UNSCORED_OPACITY = '0.10';
  const _BADGE_UNSCORED_FG      = 'var(--text-primary)';

  const _CORNER_X = Math.round(_SIDE_R * 0.65); // 45
  const _CORNER_Y = 14;

  // [lx, ly, anchor] in CW order: UL, UR, R, LR, LL, L.
  const _LABEL_SLOTS = [
    [_CX - _CORNER_X,  _CORNER_Y,      'end'  ], // i=0 UL (left)
    [_CX + _CORNER_X,  _CORNER_Y,      'start'], // i=1 UR (right)
    [_CX + _SIDE_R,    _CY,            'start'], // i=2 R  (right)
    [_CX + _CORNER_X,  _H - _CORNER_Y, 'start'], // i=3 LR (right)
    [_CX - _CORNER_X,  _H - _CORNER_Y, 'end'  ], // i=4 LL (left)
    [_CX - _SIDE_R,    _CY,            'end'  ], // i=5 L  (left)
  ];

  // When thumbs are visible, move labels outward to preserve separation.
  const _LIKE_THUMB_EXTRA = 15; // px outward shift per side when showLikeThumbs
  const _WHISTLE_LEVELS = Object.freeze({
    low: 2,
    mid: 4,
  });
  const _LEFT_SIDE_CONDITION_IDS = new Set(
    _LABEL_SLOT_CIDS.reduce((acc, cid, index) => {
      if (_LABEL_SLOTS[index] && _LABEL_SLOTS[index][2] === 'end') acc.push(Number(cid));
      return acc;
    }, [])
  );

  // Thumb anchors as percentages of the SVG viewBox.
  const MINI_RADAR_THUMB_POS = _LABEL_SLOTS.map(([lx, ly]) => ({
    left: parseFloat((lx / _W * 100).toFixed(4)),
    top:  parseFloat((ly / _H * 100).toFixed(4)),
  }));

  function _whistleLevelClass(value) {
    if (value < _WHISTLE_LEVELS.low) return 'is-level-low';
    if (value < _WHISTLE_LEVELS.mid) return 'is-level-mid';
    return 'is-level-high';
  }

  function _resolveWhistleMeta(rawCfg, mid) {
    const cfg = rawCfg || {};
    const cacheMeta = (mid && window.__modelMetaCache && window.__modelMetaCache[mid]) || {};
    const rawValue = cfg.whistleValue ?? cfg.whistle_value ?? cacheMeta.whistle_value;
    const rawBaselineCid = cfg.whistleBaselineConditionId ?? cfg.whistle_baseline_condition_id ?? cacheMeta.whistle_baseline_condition_id;
    const value = Number(rawValue);
    const baselineCid = Number(rawBaselineCid);
    if (!Number.isFinite(value) || value < 0 || !Number.isInteger(baselineCid) || baselineCid <= 0) {
      return null;
    }
    return { value, baselineCid };
  }

  function _whistleAnchorForConditionId(cid) {
    return _LEFT_SIDE_CONDITION_IDS.has(Number(cid)) ? 'left' : 'right';
  }

  function _applyMiniRadarWhistleBadgeFallbackPosition(badge, slotIndex, rawCfg) {
    let [lx, ly] = _LABEL_SLOTS[slotIndex];
    if (rawCfg && rawCfg.showLikeThumbs) {
      if (badge.dataset.anchor === 'left') lx -= _LIKE_THUMB_EXTRA;
      else lx += _LIKE_THUMB_EXTRA;
    }
    badge.style.left = (lx / _W * 100).toFixed(4) + '%';
    badge.style.top = (ly / _H * 100).toFixed(4) + '%';
  }

  function _positionMiniRadarWhistleBadge(hostEl, badge, slotIndex, rawCfg) {
    if (!hostEl || !badge || slotIndex < 0) return false;
    const labelEls = hostEl.querySelectorAll('.fc-radar-lbl');
    const labelEl = labelEls[slotIndex];
    if (!labelEl || !hostEl.isConnected) {
      _applyMiniRadarWhistleBadgeFallbackPosition(badge, slotIndex, rawCfg);
      return false;
    }
    const hostRect = hostEl.getBoundingClientRect();
    const labelRect = labelEl.getBoundingClientRect();
    if (!hostRect.width || !hostRect.height || !labelRect.width || !labelRect.height) {
      _applyMiniRadarWhistleBadgeFallbackPosition(badge, slotIndex, rawCfg);
      return false;
    }
    const edgeX = badge.dataset.anchor === 'left'
      ? (labelRect.left - hostRect.left)
      : (labelRect.right - hostRect.left);
    const centerY = (labelRect.top - hostRect.top) + (labelRect.height / 2);
    badge.style.left = edgeX.toFixed(2) + 'px';
    badge.style.top = centerY.toFixed(2) + 'px';
    return true;
  }

  function attachMiniRadarWhistleBadge(hostEl, rawCfg, mid) {
    if (!hostEl) return null;
    const oldBadge = hostEl.querySelector('.fc-mini-radar-whistle-badge');
    if (oldBadge) oldBadge.remove();
    const whistleMeta = _resolveWhistleMeta(rawCfg, mid);
    if (!whistleMeta) return null;
    const slotIndex = _LABEL_SLOT_CIDS.findIndex((cid) => Number(cid) === whistleMeta.baselineCid);
    if (slotIndex < 0) return null;
    const badge = document.createElement('span');
    badge.className = `fc-mini-radar-whistle-badge fc-tooltip-target fc-tip-info ${_whistleLevelClass(whistleMeta.value)}`;
    badge.textContent = '啸叫';
    badge.tabIndex = 0;
    const tooltipText = `进气遮挡啸叫等级：+${whistleMeta.value.toFixed(1)} dB`;
    badge.setAttribute('data-tooltip', tooltipText);
    badge.setAttribute('data-tooltip-align', 'anchor');
    badge.setAttribute('aria-label', tooltipText);
    badge.setAttribute('title', tooltipText);
    badge.dataset.anchor = _whistleAnchorForConditionId(whistleMeta.baselineCid);
    hostEl.appendChild(badge);
    _positionMiniRadarWhistleBadge(hostEl, badge, slotIndex, rawCfg);
    if (typeof window.requestAnimationFrame === 'function') {
      window.requestAnimationFrame(() => {
        _positionMiniRadarWhistleBadge(hostEl, badge, slotIndex, rawCfg);
      });
    }
    return badge;
  }
  window.attachMiniRadarWhistleBadge = attachMiniRadarWhistleBadge;

  /* ---------------------------------------------------------
     Shared grid <symbol>
     The 4 rings + 6 axis lines are pure geometry, identical on every card.
     Emitting them once and referencing them with <use> removes 10 elements
     per card, which matters a lot on the sidebar lists (dozens of cards).
     Note: CSS class selectors do not reach <use> shadow content, so the
     shapes carry their own presentation attributes and inherit `stroke`
     (an inherited SVG property) from the <use> element.
     --------------------------------------------------------- */
  const _GRID_SYMBOL_ID = 'fc-radar-grid-symbol';
  let _gridSymbolInstalled = false;

  function _gridShapesMarkup(useClasses) {
    function angle(i) { return -Math.PI / 2 - Math.PI / 6 + (i * 2 * Math.PI / 6); }
    function pt(r, i) {
      const a = angle(i);
      return [_CX + r * Math.cos(a), _CY + r * Math.sin(a)];
    }
    function ring(f) {
      return RADAR_CIDS_COLOR.map((_, i) => {
        const [x, y] = pt(f * _GRID_R, i);
        return x.toFixed(1) + ',' + y.toFixed(1);
      }).join(' ');
    }
    let inner = '';
    [0.25, 0.5, 0.75, 1.0].forEach(f => {
      inner += useClasses
        ? `<polygon class="fc-radar-ring" points="${ring(f)}"/>`
        : `<polygon points="${ring(f)}" fill="none" stroke-width="0.6"/>`;
    });
    RADAR_CIDS_COLOR.forEach((_, i) => {
      const [vx, vy] = pt(_GRID_R, i);
      const coords = `x1="${_CX}" y1="${_CY}" x2="${vx.toFixed(1)}" y2="${vy.toFixed(1)}"`;
      inner += useClasses
        ? `<line class="fc-radar-axis" ${coords}/>`
        : `<line ${coords} stroke-width="0.8"/>`;
    });
    return inner;
  }

  /** @returns {boolean} true when the shared <symbol> is available for <use>. */
  function _ensureRadarGridSymbol() {
    if (_gridSymbolInstalled) return true;
    if (typeof document === 'undefined' || !document.body) return false;
    if (document.getElementById(_GRID_SYMBOL_ID)) { _gridSymbolInstalled = true; return true; }

    const host = document.createElement('div');
    host.setAttribute('aria-hidden', 'true');
    host.style.cssText = 'position:absolute;width:0;height:0;overflow:hidden';
    host.innerHTML =
      `<svg xmlns="http://www.w3.org/2000/svg" width="0" height="0" focusable="false">` +
      `<symbol id="${_GRID_SYMBOL_ID}" viewBox="0 0 ${_W} ${_H}">${_gridShapesMarkup(false)}</symbol></svg>`;
    document.body.appendChild(host);
    _gridSymbolInstalled = true;
    return true;
  }

  /**
   * Build a shared mini-radar SVG string.
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

    // Merge global cache first so an explicit empty object does not suppress fallback labels.
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

    // Grid rings + axis lines are identical geometry on every card, so they are
    // referenced from one shared <symbol> instead of being emitted 10 more
    // elements per card.  `stroke` is inherited from the <use> element (styled
    // by .fc-radar-grid-use), which is how CSS custom properties reach the
    // shadow content; per-shape widths live on presentation attributes.
    if (_ensureRadarGridSymbol()) {
      svg += `<use class="fc-radar-grid-use" x="0" y="0" width="${_W}" height="${_H}" href="#${_GRID_SYMBOL_ID}"/>`;
    } else {
      // document.body 尚未就绪时退回内联，保证首屏不会缺网格
      svg += _gridShapesMarkup(true);
    }    // Data polygon
    svg += `<polygon class="fc-radar-area" points="${dataPts}"/>`;

    // Score labels (close to data points)
    CIDS.forEach((cid, i) => {
      const it = itemMap[cid];
      if (!it || it.score_total == null) return;
      const r = (Math.max(0, Math.min(100, it.score_total)) / 100) * _GRID_R;
      const [slx, sly] = vpt(r + _SCORE_OFFSET, i);
      svg += `<text class="fc-radar-score-lbl" x="${slx.toFixed(1)}" y="${sly.toFixed(1)}" text-anchor="middle" dominant-baseline="middle">${Math.round(it.score_total)}</text>`;
    });

    // Shift labels outward when thumbs are shown.
    CIDS.forEach((cid, i) => {
      const rawLabel = labelCache[cid] || itemMap[cid]?.condition_name_zh || String(cid);
      const label    = EH(rawLabel);
      const available = !!itemMap[cid];
      let [lx, ly, anchor] = _LABEL_SLOTS[i];
      if (withThumbs) {
        if (anchor === 'end') lx -= _LIKE_THUMB_EXTRA;   // shift further left
        else                  lx += _LIKE_THUMB_EXTRA;   // shift further right
      }
      const cls = 'fc-radar-lbl' + (available ? '' : ' unavail');
      svg += `<text class="${cls}" x="${lx.toFixed(1)}" y="${ly.toFixed(1)}" text-anchor="${anchor}" dominant-baseline="middle" style="pointer-events:none">${label}</text>`;
    });

    // Mirror the shared score badge geometry and colors.
    const scoreText = scoreNum != null ? String(Math.round(scoreNum)) : '综评';
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
    const badgeLblStyle  = ` style="fill:${fgFill}"`;
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

  // Refresh inline badge colors when the page theme changes.
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

  /**
   * Build a mini-radar card element.
   * @param {Object} cfg
   * @returns {HTMLElement}
   */
  function buildMiniRadarCardEl(cfg) {
    const rawCfg = cfg || {};
    const isMinimal = !!rawCfg.minimal;
    const {
      brand = '', model = '', price = null,
      maxSpeed = null, size = null, thickness = null, rgbTypeName = null,
      modelId,
      radarItems    = null, compositeScore = null,
      condLabelCache = null,
      showHeader    = !isMinimal,
      showLikeThumbs = false,
      showQuickAdd   = !isMinimal,
      radarSource    = 'unknown',
      hasLightingLike = false,
      rgbFlags = 0,
    } = rawCfg;

    const mid = modelId != null ? String(modelId) : '';
    const rgbTypeNameText = String(rgbTypeName || '').trim();
    const hasRgbTypeName = !!rgbTypeNameText && rgbTypeNameText !== '无';
    const hasRgbSupport = !!rgbFlags || hasRgbTypeName;

    // Preserve any previously cached review field while updating model metadata.
    if (mid) {
      window.__modelMetaCache = window.__modelMetaCache || {};
      const existing = window.__modelMetaCache[mid] || {};
      window.__modelMetaCache[mid] = {
        ...existing,
        modelId: mid, brand, model,
        reference_price: price, max_speed: maxSpeed,
        size, thickness, rgb_names_zh: rgbTypeName,
      };
    }

    const card = document.createElement('div');
    card.className = 'fc-mini-radar-card';

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

    const bodyRow = document.createElement('div');
    bodyRow.className = 'fc-mini-radar-body-row';

    const radarBody = document.createElement('div');
    radarBody.className = 'fc-mini-radar-body';
    // The SVG lives inside a wrapper whose box is pinned to the viewBox aspect
    // ratio (see .fc-mini-radar-canvas), so `preserveAspectRatio` produces no
    // letterboxing.  Thumb overlays can then be placed with pure percentages —
    // no per-card measurement and no per-card ResizeObserver.
    const radarCanvas = document.createElement('div');
    radarCanvas.className = 'fc-mini-radar-canvas';
    radarCanvas.innerHTML = buildMiniRadarSVG(
      radarItems, compositeScore, condLabelCache, null, { showLikeThumbs }
    );

    attachMiniRadarWhistleBadge(radarCanvas, rawCfg, mid);
    radarBody.appendChild(radarCanvas);

    let lightingLikeBtn = null;

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
        btn.setAttribute('data-thumb-slot', String(i));
        btn.style.left = pos.left + '%';
        btn.style.top  = pos.top  + '%';
        btn.innerHTML  = `<i class="fa-solid fa-thumbs-up ${isLiked ? 'text-red-500' : 'text-gray-400'}"></i>`;
        radarCanvas.appendChild(btn);
      });
    }

    if (hasRgbSupport) {
      const badge = document.createElement('div');
      badge.className = 'fc-mini-radar-lighting-badge';

      const rgbTag = window.buildRgbTagEl();
      badge.appendChild(rgbTag);
      radarBody.appendChild(badge);
    }

    // Lighting like button — shown for RGB models in the recent-likes context
    if (showLikeThumbs && mid && rgbFlags) {
      const isLightingLiked = !!(window.LocalState && window.LocalState.lightingLikes &&
                                  window.LocalState.lightingLikes.has(mid))
                              || hasLightingLike;
      lightingLikeBtn = document.createElement('button');
      lightingLikeBtn.type = 'button';
      lightingLikeBtn.className = 'fc-radar-lighting-like-btn fc-tooltip-target fc-mini-radar-add-col-lighting-like';
      lightingLikeBtn.setAttribute('data-model-id', mid);
      lightingLikeBtn.setAttribute('data-tooltip', isLightingLiked ? '取消灯效点赞' : '点赞灯效');
      if (isLightingLiked) lightingLikeBtn.classList.add('is-liked');
      lightingLikeBtn.innerHTML = `<i class="fa-solid fa-thumbs-up ${isLightingLiked ? 'text-red-500' : 'text-gray-400'}"></i>`;
    }

    bodyRow.appendChild(radarBody);

    if (showQuickAdd && mid) {
      const addCol = document.createElement('div');
      addCol.className = 'fc-mini-radar-add-col';
      // Fall back to persisted radar membership during page restore.
      const inRadar = !!(
        (window.RadarState && typeof window.RadarState.hasModel === 'function' && window.RadarState.hasModel(mid)) ||
        (window.LocalState && typeof window.LocalState.getRadarModels === 'function' &&
          window.LocalState.getRadarModels().some(m => String(m.modelId) === String(mid)))
      );
      const addBtn = (typeof window.buildRadarToggleBtnEl === 'function')
        ? window.buildRadarToggleBtnEl({
            modelId: mid,
            brand,
            label: model,
            state: inRadar ? 'added' : 'add',
            source: radarSource,
          })
        : (() => {
            const btn = document.createElement('button');
            btn.type = 'button';
            btn.className = 'fc-radar-toggle-btn js-mini-radar-add fc-tooltip-target';
            btn.setAttribute('data-model-id', EH(mid));
            btn.setAttribute('data-brand', EH(brand));
            btn.setAttribute('data-label', EH(model));
            btn.setAttribute('data-radar-source', EH(String(radarSource || 'unknown')));
            btn.setAttribute('data-state', 'add');
            btn.setAttribute('data-tooltip', '添加到雷达对比');
            return btn;
          })();
      if (lightingLikeBtn) addCol.appendChild(lightingLikeBtn);
      addCol.appendChild(addBtn);
      bodyRow.appendChild(addCol);
    }

    card.appendChild(bodyRow);
    return card;
  }
  window.buildMiniRadarCardEl = buildMiniRadarCardEl;

  function applyCardHeaderMask(/* scope */) {
  }
  window.applyCardHeaderMask = applyCardHeaderMask;

})();
