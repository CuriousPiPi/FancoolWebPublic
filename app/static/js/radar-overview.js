/**
 * radar-overview.js  – Phase 1 ECharts Radar Overview
 *
 * Architecture:
 *  - ECharts radar chart for multi-model comparison rendering (polygons, tooltip)
 *  - Condition labels rendered as a DOM overlay layer (6 positioned buttons)
 *    outside the radar polygon – these are the authoritative interactive selectors
 *  - Model legend row below chart (HTML) with hide-toggle and remove buttons
 *  - Public API unchanged from pre-ECharts version so fancool.js needs no API changes
 */
window.RadarOverview = (function () {
  // ---- constants ----

  // RADAR_CIDS – canonical condition ID list used for DOM overlay labels and score labels.
  // Indexed together with OVERLAY_ANGLES so that RADAR_CIDS[i] is placed at OVERLAY_ANGLES[i].
  // Clockwise visual order starting from UL: UL(1) → UR(2) → R(3) → LR(8) → LL(7) → L(10).
  const RADAR_CIDS = [1, 2, 3, 8, 7, 10];

  // ECHARTS_CIDS – condition IDs for ECharts radar indicators and series values.
  //
  // ECharts 5 radar places slot i at angle (startAngle + i * 2π/N) with a Y-flip,
  // producing CCW on-screen traversal.  With startAngle=120° the six slots land at:
  //   i=0→UL(120°), i=1→L(180°), i=2→LL(−120°), i=3→LR(−60°), i=4→R(0°), i=5→UR(60°).
  //
  // RADAR_CIDS uses CW visual order (UL→UR→R→LR→LL→L); using it directly in ECharts
  // (which is CCW) maps 4 of 6 conditions to the wrong vertex.  ECHARTS_CIDS reorders
  // to match ECharts' CCW sequence:
  //   UL→1, L→10, LL→7, LR→8, R→3, UR→2.
  const ECHARTS_CIDS = [1, 10, 7, 8, 3, 2];

  const RADAR_MODEL_LIMIT = 8;

  // Angles (degrees) for DOM overlay positioning.
  // RADAR_CIDS[i] is placed at OVERLAY_ANGLES[i] (CW visual order from UL).
  const OVERLAY_ANGLES = [120, 60, 0, -60, -120, 180];

  // ---- Label geometry ----
  // Labels sit at radarR + CONDITION_RADIUS_OFFSET_PX, with an extra horizontal outward
  // shift (SIDE/CORNER) so L/R middle labels extend farther out than the four corners.
  const CONDITION_RADIUS_OFFSET_PX = 0;
  const CORNER_H_OFFSET_PX         = 26; // UL/UR/LR/LL corner labels
  const SIDE_H_OFFSET_PX           = 26; // L/R middle labels

  // i=2 → 0° (right), i=5 → 180° (left)
  const SIDE_SLOT_INDICES = new Set([2, 5]);

  // Score labels (shown on legend hover) use their own, smaller radial offset so they sit
  // close to the data points and do not compete visually with the outer condition buttons.
  const SCORE_RADIUS_OFFSET_PX = 8;

  // ---- module-level mutable state ----
  let panelEl          = null;  // the containing panel div
  let _ecEl            = null;  // ECharts mount div
  let _ecInstance      = null;  // echarts instance
  let _ecResizeObserver = null; // ResizeObserver for auto-resize
  let _overlayEl       = null;  // DOM overlay for interactive condition labels
  let _scoreOverlayEl  = null;  // DOM overlay for per-dimension scores on hover
  let _overlayLayoutFrameId = null; // deferred overlay relayout after render/layout settle
  let _overlayLayoutFollowupFrameId = null; // second-stage relayout after an additional paint
  let collapsed        = false;

  let models           = [];       // [{modelId, brand, label, scores, hidden}]
  let condLabels       = {};       // cid (number) → label string
  let hiddenModels     = new Set(); // mid strings visually hidden
  let activeConditions = new Set(); // cid strings currently active

  // ---- hover state tracking ----
  // These prevent conflicts between radar-legend-originated hover and
  // reverse hover from main chart / spectrum chart.
  let _legendHoverModelId  = null; // non-null while a radar legend item is hovered
  let _reverseHoverModelId = null; // non-null while a main/spectrum chart curve is hovered

  // ---- persistent highlight mode ----
  let _persistModelId  = null;         // non-null while a model is locked in persistent highlight mode
  let _modelLikeState  = new Map();    // mid(string) → Set<cidString> of liked condition IDs for radar CIDs
  let _switchTimer     = null;         // pending two-phase switch timer ID
  let _globalDismissBound = false;     // guard: bind document dismiss listener only once

  // ---- callbacks ----
  let _onConditionToggle = null; // (cid: number) => void
  let _onModelRemove     = null; // (modelId: string|number) => void
  let _onModelHideToggle = null; // (modelId, willHide) => boolean|void
  let _onHighlightChange = null; // (modelId: string|null) => void – fires when effective highlighted model changes

  // ---- helpers ----
  function EH(s) {
    return String(s ?? '').replace(/[&<>"']/g, c =>
      ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c]));
  }

  /** Compute effective highlighted model id (persist > legend hover > reverse hover > null). */
  function _effectiveHighlightId() {
    if (_persistModelId !== null) return _persistModelId;
    if (_legendHoverModelId !== null) return _legendHoverModelId;
    if (_reverseHoverModelId !== null) return _reverseHoverModelId;
    return null;
  }

  let _lastNotifiedHighlightId = undefined; // undefined means never notified

  /** Fire _onHighlightChange if the effective highlighted model changed. */
  function _notifyHighlightChange() {
    if (typeof _onHighlightChange !== 'function') return;
    const cur = _effectiveHighlightId();
    if (cur === _lastNotifiedHighlightId) return;
    _lastNotifiedHighlightId = cur;
    try { _onHighlightChange(cur); } catch(_) {}
  }

  /** Return condition label for cid, or string(cid) as fallback. */
  function _label(cid) { return condLabels[cid] || String(cid); }

  /**
   * Compute aggregate like state for a model based on how many of its
   * radar conditions are liked: 'none' | 'partial' | 'all'.
   */
  function _getModelThumbState(mid) {
    const liked = _modelLikeState.get(String(mid)) || new Set();
    const count = RADAR_CIDS.filter(cid => liked.has(String(cid))).length;
    if (count === 0) return 'none';
    if (count === RADAR_CIDS.length) return 'all';
    return 'partial';
  }

  // ---- ECharts option builder ----
  function _buildOption() {
    // Use ECHARTS_CIDS (CCW slot order) for all ECharts data so that each
    // polygon vertex lands at the correct screen position.
    const indicators = ECHARTS_CIDS.map(cid => ({
      name: _label(cid),
      max: 100,
    }));

    // One series per visible model
    const seriesData = models
      .filter(m => !hiddenModels.has(String(m.modelId)))
      .map(m => {
        const baseColor = (window.ColorManager && typeof window.ColorManager.getModelBaseColor === 'function')
          ? window.ColorManager.getModelBaseColor(m.modelId)
          : '#3e6bff';
        // Build value array in ECHARTS_CIDS order so value[i] aligns with indicator[i].
        const value = ECHARTS_CIDS.map(cid => {
          const v = (m.scores || {})[cid];
          return v != null ? Math.max(0, Math.min(100, Number(v))) : null;
        });
        return {
          value,
          name: m.label,
          itemStyle:  { color: baseColor },
          lineStyle:  { color: baseColor, width: 1.8 },
          areaStyle:  { color: baseColor, opacity: 0.15 },
        };
      });

    const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
    const splitLineColor   = isDark ? 'rgba(255,255,255,0.12)' : 'rgba(0,0,0,0.12)';
    const axisLineColor    = isDark ? 'rgba(255,255,255,0.15)' : 'rgba(0,0,0,0.15)';
    const splitAreaColors  = isDark
      ? ['rgba(255,255,255,0.02)', 'rgba(255,255,255,0.04)']
      : ['rgba(0,0,0,0.02)',       'rgba(0,0,0,0.04)'];

    return {
      radar: {
        indicator: indicators,
        shape: 'polygon',
        // startAngle: 120 → ECHARTS_CIDS[0] (CID1) lands at 120° (UL).
        // ECharts traverses CCW: slot i is at (startAngle + i*60°) with Y-flip rendering,
        // so the on-screen order is UL→L→LL→LR→R→UR which matches ECHARTS_CIDS = [1,10,7,8,3,2].
        startAngle: 120,
        splitNumber: 4,
        radius: '62%',    // leave room for labels
        center: ['50%', '52%'],
        triggerEvent: false,
        axisName: {
          show: false,  // Condition labels are rendered by the DOM overlay instead
        },
        splitArea: {
          show: true,
          areaStyle: { color: splitAreaColors },
        },
        splitLine: { lineStyle: { color: splitLineColor } },
        axisLine:  { lineStyle: { color: axisLineColor  } },
      },
      series: [{
        type: 'radar',
        data: seriesData,
        symbol: 'circle',
        symbolSize: 4,
        emphasis: {
          lineStyle: { width: 2.5 },
          areaStyle: { opacity: 0.35 },
        },
      }],
      // Tooltip: iterate ECHARTS_CIDS so that value[i] matches the correct condition.
      tooltip: {
        show: true,
        trigger: 'item',
        formatter: function (params) {
          const data = params.data;
          if (!data || !data.value) return '';
          const lines = ECHARTS_CIDS.map((cid, i) => {
            const v = data.value[i];
            return `${EH(_label(cid))}: ${v != null ? Math.round(v) : '–'}`;
          });
          return `<b>${EH(data.name)}</b><br>${lines.join('<br>')}`;
        },
      },
    };
  }

  // ---- ECharts instance lifecycle ----
  function _mountEc(containerEl) {
    if (!window.echarts) return;
    if (_ecInstance) {
      _ecInstance.dispose();
      _ecInstance = null;
    }
    _ecEl = containerEl;
    _ecInstance = window.echarts.init(_ecEl, null, {
      renderer: 'canvas',
      devicePixelRatio: window.devicePixelRatio || 1,
    });

    // Build DOM overlay for condition labels (no ECharts event needed)
    _buildOverlay();
    // Build score overlay for hover display
    _buildScoreOverlay();

    // Auto-resize via ResizeObserver
    if (_ecResizeObserver) { _ecResizeObserver.disconnect(); _ecResizeObserver = null; }
    if (typeof ResizeObserver !== 'undefined') {
      _ecResizeObserver = new ResizeObserver(() => {
        if (_ecInstance) _ecInstance.resize();
        _scheduleOverlayRelayout({ immediate: true });
        _positionScoreLabels();
      });
      _ecResizeObserver.observe(_ecEl);
    }
  }

  function _disposeEc() {
    _cancelScheduledOverlayRelayout();
    if (_ecResizeObserver) { _ecResizeObserver.disconnect(); _ecResizeObserver = null; }
    if (_ecInstance) { _ecInstance.dispose(); _ecInstance = null; }
    if (_overlayEl) { _overlayEl.remove(); _overlayEl = null; }
    if (_scoreOverlayEl) { _scoreOverlayEl.remove(); _scoreOverlayEl = null; }
    _ecEl = null;
  }

  // ---- Legend HTML ----
  function _buildLegendHtml() {
    if (!models.length) return '';
    return models.map(m => {
      const baseColor = (window.ColorManager && typeof window.ColorManager.getModelBaseColor === 'function')
        ? window.ColorManager.getModelBaseColor(m.modelId)
        : '#3e6bff';
      const mid          = EH(String(m.modelId));
      const isHidden     = hiddenModels.has(String(m.modelId));
      const displayName  = EH((m.brand ? m.brand + ' ' : '') + m.label);
      const thumbState   = _getModelThumbState(m.modelId);
      const isPersist    = _persistModelId === String(m.modelId);
      return `<div class="radar-overview-legend-item${isHidden ? ' is-hidden' : ''}" data-legend-model-id="${mid}" title="${displayName}">
        <button type="button" class="radar-overview-legend-thumb thumb-${thumbState}${isPersist ? ' is-persist-active' : ''}" data-legend-thumb-id="${mid}" aria-label="点赞 / 进入工况赞模式" title="点赞工况 / 持久高亮模式"><i class="fa-solid fa-thumbs-up"></i></button>
        <span class="radar-overview-legend-toggle" role="button" tabindex="0" aria-label="${isHidden ? '显示' : '隐藏'} ${displayName}">
          <span class="radar-overview-legend-dot" style="background:${baseColor}"></span>
          <span class="radar-overview-legend-label">${displayName}</span>
        </span>
        <button type="button" class="radar-overview-legend-remove fc-tooltip-target" data-legend-remove-id="${mid}" data-tooltip="从雷达图移除" aria-label="移除 ${displayName}">×</button>
      </div>`;
    }).join('');
  }

  // ---- DOM overlay for condition labels ----

  /** Build (or rebuild) the overlay div with 6 condition label buttons. */
  function _buildOverlay() {
    if (!_ecEl) return;
    const wrap = _ecEl.parentElement;
    if (!wrap) return;
    // Remove any existing overlay
    if (_overlayEl) { _overlayEl.remove(); _overlayEl = null; }

    _overlayEl = document.createElement('div');
    _overlayEl.className = 'radar-overlay';

    // Condition thumb buttons FIRST so that labels (appended after) render above them.
    RADAR_CIDS.forEach((cid, i) => {
      const angleDeg = OVERLAY_ANGLES[i];
      const angleRad = angleDeg * Math.PI / 180;
      const cosA = Math.cos(angleRad);
      const side = cosA < 0 ? 'left' : 'right';
      const thumb = document.createElement('button');
      thumb.type = 'button';
      thumb.className = 'radar-overlay-thumb';
      thumb.dataset.thumbCid = String(cid);
      thumb.dataset.thumbSide = side;
      thumb.setAttribute('aria-label', `赞: ${_label(cid)}`);
      thumb.innerHTML = '<i class="fa-solid fa-thumbs-up"></i>';
      _overlayEl.appendChild(thumb);
    });

    // Condition label buttons AFTER thumbs so labels sit visually on top.
    RADAR_CIDS.forEach((cid, i) => {
      const angleDeg = OVERLAY_ANGLES[i];
      const angleRad = angleDeg * Math.PI / 180;
      const cosA = Math.cos(angleRad);
      const side = cosA < 0 ? 'left' : 'right';
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'radar-overlay-lbl';
      btn.dataset.cid = String(cid);
      btn.dataset.labelSide = side;   // used by CSS for label-shift animation
      btn.textContent = _label(cid);
      _overlayEl.appendChild(btn);
    });

    wrap.appendChild(_overlayEl);
    _bindOverlayEvents();
    _scheduleOverlayRelayout({ immediate: true });
  }

  function _cancelScheduledOverlayRelayout() {
    if (_overlayLayoutFrameId !== null) {
      cancelAnimationFrame(_overlayLayoutFrameId);
      _overlayLayoutFrameId = null;
    }
    if (_overlayLayoutFollowupFrameId !== null) {
      cancelAnimationFrame(_overlayLayoutFollowupFrameId);
      _overlayLayoutFollowupFrameId = null;
    }
  }

  /**
   * Keep the initial render path aligned with the later resize path by re-running
   * overlay measurement after one and two paint cycles. This avoids caching the
   * first pre-settle overlay geometry as the thumb-positioning baseline, even
   * when an immediate pass is also requested for the current interaction.
   */
  function _scheduleOverlayRelayout(opts = {}) {
    const immediate = !!opts.immediate;
    if (!_overlayEl || !_ecEl) return;
    if (immediate) _positionOverlayLabels();

    _cancelScheduledOverlayRelayout();
    _overlayLayoutFrameId = requestAnimationFrame(() => {
      _overlayLayoutFrameId = null;
      _positionOverlayLabels();
      _overlayLayoutFollowupFrameId = requestAnimationFrame(() => {
        _overlayLayoutFollowupFrameId = null;
        _positionOverlayLabels();
      });
    });
  }

  /**
   * Compute pixel positions for each overlay label based on ECharts radar geometry.
   * Mirrors radar config: center ['50%','52%'], radius '62%', startAngle: -60.
   *
   * Positioning model:
   *   1. Place the label at (radarR + CONDITION_RADIUS_OFFSET_PX) along its radar angle.
   *   2. Shift it an additional SIDE_H_OFFSET_PX (for L/R middle) or CORNER_H_OFFSET_PX
   *      (for UL/UR/LR/LL) horizontally outward.  The sign of the shift is derived from
   *      the cosine of the angle so it always moves away from the vertical centre-line.
   *
   * This keeps the hexagon-silhouette feel while letting L/R middle labels extend
   * further outward than the corner labels, without forcing a rigid two-column layout.
   */
  function _positionOverlayLabels() {
    if (!_overlayEl || !_ecEl) return;
    const W = _ecEl.offsetWidth;
    const H = _ecEl.offsetHeight;
    if (!W || !H) return;

    // Mirror ECharts radar config
    const cx = W * 0.50;
    const cy = H * 0.52;
    // ECharts radius '62%' = 62% of min(W,H)/2
    const radarR = Math.min(W, H) / 2 * 0.62;

    const btns = _overlayEl.querySelectorAll('.radar-overlay-lbl');
    btns.forEach((btn, i) => {
      const angleDeg = OVERLAY_ANGLES[i];
      const angleRad = angleDeg * Math.PI / 180;
      const cosA = Math.cos(angleRad);

      // 1. Base radial position (same for all labels)
      const labelR = radarR + CONDITION_RADIUS_OFFSET_PX;
      let lx = cx + labelR * cosA;
      const ly = cy - labelR * Math.sin(angleRad);

      // 2. Extra horizontal offset: side labels extend farther, corner labels extend less
      const hOffset = SIDE_SLOT_INDICES.has(i) ? SIDE_H_OFFSET_PX : CORNER_H_OFFSET_PX;
      // Math.sign(cosA) is +1 for right-side labels and -1 for left-side labels
      lx += Math.sign(cosA) * hOffset;

      btn.style.left = lx + 'px';
      btn.style.top  = ly + 'px';
    });

    // Position condition thumbs at the same pixel coordinates as their
    // corresponding labels.  The label (z-index: 2) naturally covers the
    // thumb (z-index: 1) in the resting state.  When the overlay enters
    // has-persist mode, CSS shifts the labels outward via translateX,
    // revealing the thumbs without any JS thumb-position change.
    const THUMB_LEFT_OFFSET_PX  = -10;
    const THUMB_RIGHT_OFFSET_PX = 10;
    const THUMB_Y_OFFSET_PX     = 0;

    const thumbs = _overlayEl.querySelectorAll('.radar-overlay-thumb');
    thumbs.forEach((thumb, i) => {
      // Reuse the label's computed position directly
      const labelEl = btns[i];
      if (labelEl && labelEl.style.left) {
        const side = thumb.dataset.thumbSide;
        const sideOffset = side === 'left' ? THUMB_LEFT_OFFSET_PX : THUMB_RIGHT_OFFSET_PX;
        thumb.style.left = (parseFloat(labelEl.style.left) + sideOffset) + 'px';
        thumb.style.top  = (parseFloat(labelEl.style.top)  + THUMB_Y_OFFSET_PX) + 'px';
      }
    });
  }

  /** Update label text and active/inactive visual state without rebuilding DOM. */
  function _refreshOverlayState() {
    if (!_overlayEl) return;
    _overlayEl.querySelectorAll('.radar-overlay-lbl').forEach(btn => {
      const cid = btn.dataset.cid;
      btn.classList.toggle('is-active', activeConditions.has(cid));
      btn.textContent = _label(Number(cid));
    });
  }

  /** Attach click and keyboard event handlers to all overlay label buttons and condition thumbs. */
  function _bindOverlayEvents() {
    if (!_overlayEl) return;
    _overlayEl.querySelectorAll('.radar-overlay-lbl').forEach(btn => {
      btn.addEventListener('click', () => {
        const cid = Number(btn.dataset.cid);
        if (typeof _onConditionToggle === 'function') _onConditionToggle(cid);
      });
      btn.addEventListener('keydown', e => {
        if (e.key === 'Enter' || e.key === ' ' || e.code === 'Space') {
          e.preventDefault();
          const cid = Number(btn.dataset.cid);
          if (typeof _onConditionToggle === 'function') _onConditionToggle(cid);
        }
      });
    });

    // Condition thumb click: toggle like state for (persistModelId, cid)
    _overlayEl.querySelectorAll('.radar-overlay-thumb').forEach(thumb => {
      thumb.addEventListener('click', e => {
        e.stopPropagation(); // don't let blank-space handler fire
        if (_persistModelId === null) return;
        const cid = String(thumb.dataset.thumbCid);
        _toggleConditionLike(_persistModelId, cid);
      });
    });
  }

  // ---- Score overlay for hover display ----

  /** Build (or rebuild) the overlay div with score labels for hover display. */
  function _buildScoreOverlay() {
    if (!_ecEl) return;
    const wrap = _ecEl.parentElement;
    if (!wrap) return;
    if (_scoreOverlayEl) { _scoreOverlayEl.remove(); _scoreOverlayEl = null; }

    _scoreOverlayEl = document.createElement('div');
    _scoreOverlayEl.className = 'radar-score-overlay';
    _scoreOverlayEl.style.display = 'none';

    // Six per-dimension score labels
    RADAR_CIDS.forEach((cid) => {
      const span = document.createElement('span');
      span.className = 'radar-score-lbl';
      span.dataset.scoreCid = String(cid);
      _scoreOverlayEl.appendChild(span);
    });

    // Center composite score label
    const center = document.createElement('span');
    center.className = 'radar-score-center';
    _scoreOverlayEl.appendChild(center);

    wrap.appendChild(_scoreOverlayEl);
    _positionScoreLabels();
  }

  /** Compute pixel positions for each score label (same geometry as condition labels). */
  function _positionScoreLabels() {
    if (!_scoreOverlayEl || !_ecEl) return;
    const W = _ecEl.offsetWidth;
    const H = _ecEl.offsetHeight;
    if (!W || !H) return;

    const cx = W * 0.50;
    const cy = H * 0.52;
    const radarR = Math.min(W, H) / 2 * 0.62;
    // Score labels sit between polygon edge and the condition label
    const scoreR = radarR + SCORE_RADIUS_OFFSET_PX;

    const labels = _scoreOverlayEl.querySelectorAll('.radar-score-lbl');
    labels.forEach((lbl, i) => {
      const angleDeg = OVERLAY_ANGLES[i];
      const angleRad = angleDeg * Math.PI / 180;
      const lx = cx + scoreR * Math.cos(angleRad);
      const ly = cy - scoreR * Math.sin(angleRad);
      lbl.style.left = lx + 'px';
      lbl.style.top  = ly + 'px';
    });

    // Center composite score
    const centerEl = _scoreOverlayEl.querySelector('.radar-score-center');
    if (centerEl) {
      centerEl.style.left = cx + 'px';
      centerEl.style.top  = cy + 'px';
    }
  }

  /**
   * Show score overlay for the given model.
   * @param {object} model  – entry from models[]
   * @param {string} color  – hex/rgb color for the per-dimension score labels
   */
  function _showScoreOverlay(model, color) {
    if (!_scoreOverlayEl) return;
    const scoreColor = color || '#3e6bff';

    // Per-dimension scores
    _scoreOverlayEl.querySelectorAll('.radar-score-lbl').forEach(lbl => {
      const cid = Number(lbl.dataset.scoreCid);
      const v = (model.scores || {})[cid];
      lbl.textContent = v != null ? Math.round(Number(v)) : '';
      lbl.style.color = scoreColor;
    });

    // Composite score — displayed as a score badge using the shared
    // ScoreBadgeHelper for continuous color (same family as rankings).
    const centerEl = _scoreOverlayEl.querySelector('.radar-score-center');
    if (centerEl) {
      const radarCache = window.__radarCache && window.__radarCache[String(model.modelId)];
      const composite = radarCache && radarCache.composite_score != null
        ? Math.round(Number(radarCache.composite_score))
        : null;
      centerEl.textContent = composite != null ? String(composite) : '';
      if (composite != null && window.ScoreBadgeHelper) {
        const { bg, fg } = window.ScoreBadgeHelper.scoreStyle(composite);
        centerEl.style.background = bg;
        centerEl.style.color = fg;
        centerEl.classList.add('radar-score-center--badge');
      } else {
        centerEl.style.background = '';
        centerEl.style.color = scoreColor;
        centerEl.classList.remove('radar-score-center--badge');
      }
    }

    _scoreOverlayEl.style.display = '';
  }

  /** Hide the score overlay. */
  function _hideScoreOverlay() {
    // When exactly one model is visible its scores are always shown – don't hide.
    const single = _getSingleVisibleModel();
    if (single) {
      _showScoreOverlay(single, _getModelColor(single.modelId));
      return;
    }
    if (_scoreOverlayEl) _scoreOverlayEl.style.display = 'none';
  }

  /** Return the sole visible model when exactly one model is not hidden, else null. */
  function _getSingleVisibleModel() {
    const visible = models.filter(m => !hiddenModels.has(String(m.modelId)));
    return visible.length === 1 ? visible[0] : null;
  }

  // ---- Persistent highlight mode ----

  /** Enter persistent highlight mode for the given modelId. */
  function _enterPersistMode(mid) {
    _persistModelId = String(mid);
    _highlightModel(mid);
    const model = models.find(m => String(m.modelId) === _persistModelId);
    if (model) _showScoreOverlay(model, _getModelColor(mid));
    _scheduleOverlayRelayout({ immediate: true });
    if (_overlayEl) _overlayEl.classList.add('has-persist');
    _refreshOverlayThumbs();
    _refreshLegendThumbs();
    // Sync main chart / spectrum chart highlight to the newly activated model.
    if (window.ChartRenderer && typeof window.ChartRenderer.highlightModelSeries === 'function') {
      window.ChartRenderer.highlightModelSeries(mid);
    }
    _notifyHighlightChange();
  }

  /** Exit persistent highlight mode. */
  function _exitPersistMode() {
    // Cancel any pending two-phase switch timer
    if (_switchTimer !== null) { clearTimeout(_switchTimer); _switchTimer = null; }
    const prevMid = _persistModelId;
    _persistModelId = null;
    // Only clear radar highlight if no hover is currently active
    if (_legendHoverModelId === null && _reverseHoverModelId === null) {
      _clearHighlight();
      _hideScoreOverlay();
      // Clear the corresponding highlight in main chart / spectrum chart.
      if (prevMid && window.ChartRenderer && typeof window.ChartRenderer.downplayModelSeries === 'function') {
        window.ChartRenderer.downplayModelSeries(prevMid);
      }
    }
    if (_overlayEl) _overlayEl.classList.remove('has-persist');
    _refreshOverlayThumbs();
    _refreshLegendThumbs();
    _notifyHighlightChange();
  }

  /**
   * Two-phase transition when switching the active model in persistent mode.
   * Phase 1: animate current model's condition thumbs back behind labels.
   * Phase 2: after the CSS transition completes, enter the new model.
   * @param {string|number} newMid – model to switch to
   */
  function _switchPersistMode(newMid) {
    // Cancel any already-pending switch
    if (_switchTimer !== null) { clearTimeout(_switchTimer); _switchTimer = null; }
    // Clear main/spectrum chart highlight for the model we are leaving.
    if (_persistModelId !== null && window.ChartRenderer &&
        typeof window.ChartRenderer.downplayModelSeries === 'function') {
      window.ChartRenderer.downplayModelSeries(_persistModelId);
    }
    // Phase 1: slide thumbs back (remove has-persist)
    if (_overlayEl) _overlayEl.classList.remove('has-persist');
    _persistModelId = null;
    _refreshLegendThumbs();
    _notifyHighlightChange();
    // Phase 2: enter new model after CSS transition (220 ms transition + small margin)
    _switchTimer = setTimeout(() => {
      _switchTimer = null;
      _enterPersistMode(newMid);
    }, 240);
  }

  /**
   * Bind a one-time document-level click listener that exits persistent mode when the
   * user clicks outside the radar like-mode controls.
   * Legend-thumb and condition-thumb clicks call e.stopPropagation() so they never
   * reach this handler; condition-label clicks (condition toggles) are exempted explicitly.
   */
  function _bindGlobalDismiss() {
    if (_globalDismissBound) return;
    _globalDismissBound = true;
    document.addEventListener('click', e => {
      if (_persistModelId === null) return;
      // Condition labels: clicking them toggles conditions but must NOT exit persist mode
      if (e.target.closest('.radar-overlay-lbl')) return;
      _exitPersistMode();
    });
  }

  /**
   * Update just the legend thumb buttons without a full re-render.
   * Called after like state changes or persist mode transitions.
   */
  function _refreshLegendThumbs() {
    if (!panelEl) return;
    const legendEl = panelEl.querySelector('.radar-overview-legend');
    if (!legendEl) return;
    legendEl.querySelectorAll('.radar-overview-legend-thumb').forEach(btn => {
      const mid = btn.dataset.legendThumbId;
      const state = _getModelThumbState(mid);
      btn.classList.toggle('thumb-none',    state === 'none');
      btn.classList.toggle('thumb-partial', state === 'partial');
      btn.classList.toggle('thumb-all',     state === 'all');
      btn.classList.toggle('is-persist-active', _persistModelId === String(mid));
    });
  }

  /**
   * Update condition thumb liked/unliked visual state for the current persist model.
   */
  function _refreshOverlayThumbs() {
    if (!_overlayEl) return;
    _overlayEl.querySelectorAll('.radar-overlay-thumb').forEach(thumb => {
      const cid = thumb.dataset.thumbCid;
      if (_persistModelId !== null) {
        const liked = _modelLikeState.get(_persistModelId) || new Set();
        thumb.classList.toggle('is-liked', liked.has(String(cid)));
        // Update aria label based on condition name
        thumb.setAttribute('aria-label', `赞: ${_label(Number(cid))}`);
      } else {
        thumb.classList.remove('is-liked');
      }
    });
  }

  /**
   * Toggle like/unlike for a (modelId, conditionId) pair.
   * Optimistically updates UI; reverts on API failure.
   */
  function _toggleConditionLike(mid, cidStr) {
    const midStr   = String(mid);
    const liked    = _modelLikeState.get(midStr) || new Set();
    const wasLiked = liked.has(String(cidStr));
    const nowLiked = !wasLiked;
    const url      = wasLiked ? '/api/unlike' : '/api/like';
    const keyStr   = `${midStr}_${cidStr}`;

    // Optimistic update
    const newLiked = new Set(liked);
    if (nowLiked) newLiked.add(String(cidStr));
    else          newLiked.delete(String(cidStr));
    _modelLikeState.set(midStr, newLiked);

    if (window.LocalState && window.LocalState.likes) {
      if (nowLiked) window.LocalState.likes.add(keyStr);
      else          window.LocalState.likes.remove(keyStr);
    }
    _refreshOverlayThumbs();
    _refreshLegendThumbs();
    // Notify global listeners (e.g. recent-likes sidebar) of the state change
    document.dispatchEvent(new CustomEvent('fc:like-changed', {
      detail: { modelId: midStr, conditionId: cidStr, liked: nowLiked },
    }));

    fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ model_id: Number(midStr), condition_id: Number(cidStr) }),
    })
      .then(r => r.json())
      .then(j => {
        if (j && j.ok === false) {
          // Revert on server error
          _modelLikeState.set(midStr, liked);
          if (window.LocalState && window.LocalState.likes) {
            if (wasLiked) window.LocalState.likes.add(keyStr);
            else          window.LocalState.likes.remove(keyStr);
          }
          _refreshOverlayThumbs();
          _refreshLegendThumbs();
          return;
        }
        const data = (j && j.data) || {};
        if (data.fp && window.LocalState && window.LocalState.likes &&
            typeof window.LocalState.likes.updateServerFP === 'function') {
          window.LocalState.likes.updateServerFP(data.fp);
        }
      })
      .catch(() => {
        // Revert on network error
        _modelLikeState.set(midStr, liked);
        if (window.LocalState && window.LocalState.likes) {
          if (wasLiked) window.LocalState.likes.add(keyStr);
          else          window.LocalState.likes.remove(keyStr);
        }
        _refreshOverlayThumbs();
        _refreshLegendThumbs();
      });
  }

  // ---- Full render ----
  function _render() {
    if (!panelEl) return;
    const contentEl = panelEl.querySelector('.radar-overview-content');
    if (!contentEl) return;

    panelEl.classList.toggle('is-collapsed', !!collapsed);
    const toggleBtn = panelEl.querySelector('.radar-overview-toggle');
    if (toggleBtn) {
      toggleBtn.setAttribute('aria-label',    collapsed ? '展开雷达图' : '收起雷达图');
      toggleBtn.setAttribute('aria-expanded', String(!collapsed));
    }

    // Preserve #radarActionsBar across innerHTML replacements.
    // Must grab the reference BEFORE any wipe: once detached, getElementById won't find it.
    const actionsBar = document.getElementById('radarActionsBar') ||
                       panelEl.querySelector('#radarActionsBar');
    if (actionsBar && actionsBar.parentElement === contentEl) {
      actionsBar.remove(); // temporarily detach; re-attached at the end via _ensureActionsBarInContent
    }

    if (!models.length) {
      // Exit persist mode if all models removed
      if (_persistModelId !== null) {
        _persistModelId = null;
        if (_overlayEl) _overlayEl.classList.remove('has-persist');
      }
      _disposeEc();
      contentEl.innerHTML = '<div class="radar-overview-empty">搜索并添加型号查看详细数据</div>';
      _ensureActionsBarInContent(contentEl, actionsBar);
      return;
    }

    // Exit persist mode if the persist model was removed
    if (_persistModelId !== null && !models.find(m => String(m.modelId) === _persistModelId)) {
      _persistModelId = null;
      if (_overlayEl) _overlayEl.classList.remove('has-persist');
    }

    // Build or re-use content structure
    let ecWrap   = contentEl.querySelector('.radar-overview-ec-wrap');
    let legendEl = contentEl.querySelector('.radar-overview-legend');

    if (!ecWrap) {
      // First render: build skeleton
      contentEl.innerHTML =
        `<div class="radar-overview-title">工况评分对比</div>` +
        `<div class="radar-overview-body">` +
          `<div class="radar-overview-chart-wrap">` +
            `<div class="radar-overview-ec-wrap"><div class="radar-overview-ec"></div></div>` +
          `</div>` +
          `<div class="radar-overview-legend"></div>` +
        `</div>`;
      ecWrap   = contentEl.querySelector('.radar-overview-ec-wrap');
      legendEl = contentEl.querySelector('.radar-overview-legend');

      const ecDiv = ecWrap.querySelector('.radar-overview-ec');
      _mountEc(ecDiv);
      _bindToggle();
    }

    // Update legend HTML (cheap)
    if (legendEl) legendEl.innerHTML = _buildLegendHtml();

    // Update ECharts option
    if (_ecInstance) {
      _ecInstance.setOption(_buildOption(), { notMerge: true });
      _ecInstance.resize();
      _scheduleOverlayRelayout({ immediate: true });
    }

    // Re-bind legend events each render
    if (legendEl) _bindLegendEvents(contentEl, legendEl);

    // Refresh overlay label text and active state (handles condLabel updates too)
    _refreshOverlayState();

    // Re-apply persist overlay class (overlay may have been rebuilt)
    if (_overlayEl) {
      _overlayEl.classList.toggle('has-persist', _persistModelId !== null);
    }
    _refreshOverlayThumbs();

    // Re-apply persistent highlight if active (e.g. after a scores update re-render)
    if (_persistModelId !== null) {
      _highlightModel(_persistModelId);
      const pModel = models.find(m => String(m.modelId) === _persistModelId);
      if (pModel) _showScoreOverlay(pModel, _getModelColor(_persistModelId));
    }

    // When exactly one model is visible, always show its scores (no hover required).
    // This runs after the persist-mode check so persist mode still takes priority
    // when it is active for that model.
    // When the visible-model count moves away from 1, the auto-shown overlay is hidden.
    if (_persistModelId === null) {
      const single = _getSingleVisibleModel();
      if (single) {
        _showScoreOverlay(single, _getModelColor(single.modelId));
      } else if (_scoreOverlayEl) {
        _scoreOverlayEl.style.display = 'none';
      }
    }

    // Ensure the radar-actions-bar is the last child of radar-overview-content
    // (bottom-centered), not a sibling of radar-overview-content.
    _ensureActionsBarInContent(contentEl, actionsBar);
  }

  /**
   * Move #radarActionsBar into radar-overview-content as its last child so it
   * appears bottom-centred inside the content area rather than outside of it.
   * Safe to call on every render; no-op if already in the right place.
   *
   * @param {HTMLElement} contentEl
   * @param {HTMLElement} [barEl] - optional pre-fetched element reference; used
   *   when the element may be temporarily detached (getElementById won't find it).
   */
  function _ensureActionsBarInContent(contentEl, barEl) {
    const actionsBar = barEl || document.getElementById('radarActionsBar');
    if (!actionsBar || !contentEl) return;
    if (actionsBar.parentElement !== contentEl) {
      contentEl.appendChild(actionsBar);
    }
  }

  // ---- Module-scope radar highlight helpers ----
  // Used by both legend hover and reverse hover from main/spectrum chart.

  function _getModelColor(modelId) {
    return (window.ColorManager && typeof window.ColorManager.getModelBaseColor === 'function')
      ? window.ColorManager.getModelBaseColor(modelId)
      : '#3e6bff';
  }

  function _highlightModel(modelId) {
    if (!_ecInstance) return;
    models.forEach(m => {
      const isMine = String(m.modelId) === String(modelId);
      _ecInstance.dispatchAction({
        type: isMine ? 'highlight' : 'downplay',
        seriesIndex: 0,
        name: m.label,
      });
    });
  }

  function _clearHighlight() {
    if (!_ecInstance) return;
    models.forEach(m => {
      _ecInstance.dispatchAction({ type: 'downplay', seriesIndex: 0, name: m.label });
    });
  }

  // ---- Legend event binding ----
  function _bindLegendEvents(contentEl, legendEl) {
    const items = legendEl.querySelectorAll('.radar-overview-legend-item');

    items.forEach(item => {
      const modelId  = item.dataset.legendModelId;
      const toggleEl = item.querySelector('.radar-overview-legend-toggle');
      const removeEl = item.querySelector('.radar-overview-legend-remove');
      const thumbEl  = item.querySelector('.radar-overview-legend-thumb');

      // Thumb click: enter/exit/switch persistent highlight mode
      if (thumbEl) {
        thumbEl.addEventListener('click', e => {
          e.stopPropagation();
          if (_persistModelId === String(modelId)) {
            // Same model thumb: exit persist mode
            _exitPersistMode();
          } else if (_persistModelId !== null) {
            // Different model thumb while in persist mode: two-phase switch
            _switchPersistMode(modelId);
          } else {
            // Not in persist mode: enter for this model
            _enterPersistMode(modelId);
          }
        });
      }

      // Hover highlight: radar + score overlay + main/spectrum chart
      // Suppressed while persistent mode is active.
      item.addEventListener('mouseenter', () => {
        if (_persistModelId !== null) return; // persistent mode owns the highlight
        if (hiddenModels.has(String(modelId))) return;
        _legendHoverModelId = String(modelId);
        _highlightModel(modelId);

        // Show score overlay
        const model = models.find(m => String(m.modelId) === String(modelId));
        if (model) _showScoreOverlay(model, _getModelColor(modelId));

        // Bridge: highlight all visible series for this model in main/spectrum chart
        if (window.ChartRenderer && typeof window.ChartRenderer.highlightModelSeries === 'function') {
          window.ChartRenderer.highlightModelSeries(modelId);
        }
        _notifyHighlightChange();
      });

      item.addEventListener('mouseleave', () => {
        if (_persistModelId !== null) {
          _legendHoverModelId = null;
          return; // persistent mode stays active
        }
        _legendHoverModelId = null;
        // If a reverse hover from main/spectrum chart is still active, restore it;
        // otherwise clear completely.
        if (_reverseHoverModelId !== null) {
          _highlightModel(_reverseHoverModelId);
          const rModel = models.find(m => String(m.modelId) === _reverseHoverModelId);
          if (rModel) _showScoreOverlay(rModel, _getModelColor(_reverseHoverModelId));
        } else {
          _clearHighlight();
          _hideScoreOverlay();
        }
        // Always downplay in main/spectrum chart (those have their own native hover)
        if (window.ChartRenderer && typeof window.ChartRenderer.downplayModelSeries === 'function') {
          window.ChartRenderer.downplayModelSeries(modelId);
        }
        _notifyHighlightChange();
      });

      // Hide/show toggle
      if (toggleEl) {
        const toggleHide = () => {
          const mid      = String(modelId);
          const willHide = !hiddenModels.has(mid);

          // Update local state optimistically
          if (willHide) hiddenModels.add(mid);
          else          hiddenModels.delete(mid);

          if (typeof _onModelHideToggle === 'function') {
            const result = _onModelHideToggle(mid, willHide);
            if (result === false) {
              // Cancelled – revert
              if (willHide) hiddenModels.delete(mid);
              else          hiddenModels.add(mid);
              _render();
              return;
            }
          }
          _render();
        };
        toggleEl.addEventListener('click', toggleHide);
        toggleEl.addEventListener('keydown', ev => {
          if (ev.key === ' ') ev.preventDefault();
          if (ev.key === 'Enter' || ev.key === ' ') toggleHide();
        });
      }

      // Remove (×) button
      if (removeEl) {
        removeEl.addEventListener('click', ev => {
          ev.stopPropagation();
          if (typeof _onModelRemove === 'function') _onModelRemove(modelId);
        });
      }
    });
  }

  // ---- Toggle collapse button ----
  function _bindToggle() {
    if (!panelEl) return;
    const btn = panelEl.querySelector('.radar-overview-toggle');
    if (!btn || btn._bound) return;
    btn._bound = true;
    btn.addEventListener('click', () => {
      collapsed = !collapsed;
      try { localStorage.setItem('radarOverviewCollapsed', collapsed ? '1' : '0'); } catch (_) {}
      _render();
      if (window.ChartRenderer && typeof window.ChartRenderer.resize === 'function') {
        setTimeout(() => window.ChartRenderer.resize(), 310);
      }
    });
  }

  // ---- Public API ----
  function update(modelList) {
    models = (modelList || []).slice(0, RADAR_MODEL_LIMIT);
    const ids = new Set(models.map(m => String(m.modelId)));
    // Prune hidden set of removed models
    hiddenModels.forEach(id => { if (!ids.has(id)) hiddenModels.delete(id); });
    // Sync hidden flag from incoming model list (RadarState is source of truth)
    models.forEach(m => {
      const mid = String(m.modelId);
      if (m.hidden) hiddenModels.add(mid);
      else          hiddenModels.delete(mid);
    });
    // Reset stale hover state for models that are no longer in the list.
    // Without this, _lastNotifiedHighlightId can remain pointing at a removed
    // model ID; when that model is re-added and hovered, _notifyHighlightChange
    // sees no change and skips the callback, preventing the review from updating.
    let hoverStateChanged = false;
    if (_legendHoverModelId !== null && !ids.has(_legendHoverModelId)) {
      _legendHoverModelId = null;
      hoverStateChanged = true;
    }
    if (_reverseHoverModelId !== null && !ids.has(_reverseHoverModelId)) {
      _reverseHoverModelId = null;
      hoverStateChanged = true;
    }
    // Also reset stale persistent-highlight state.  If the persisted model is no
    // longer in the radar (remove or clear), clear the lock so that the highlight
    // state is clean for subsequent hover / single-model display logic.
    if (_persistModelId !== null && !ids.has(_persistModelId)) {
      _persistModelId = null;
      if (_overlayEl) _overlayEl.classList.remove('has-persist');
      hoverStateChanged = true;
    }
    if (hoverStateChanged) _notifyHighlightChange();
    _render();
  }

  function setConditionLabels(labelsMap) {
    condLabels = labelsMap || {};
    // Full re-render: updates both ECharts axis labels and HTML pill text
    _render();
  }

  function setActiveConditions(condSet) {
    activeConditions = condSet instanceof Set ? condSet : new Set(condSet);
    // Update ECharts option (even though axisName is hidden, keeps option in sync)
    if (_ecInstance) {
      _ecInstance.setOption(_buildOption(), { notMerge: true });
    }
    // Refresh DOM overlay label states
    _refreshOverlayState();
  }

  function isAtLimit() { return models.length >= RADAR_MODEL_LIMIT; }

  function onConditionToggle(fn)  { _onConditionToggle = fn; }
  function onModelRemove(fn)      { _onModelRemove = fn; }
  function onModelHideToggle(fn)  { _onModelHideToggle = fn; }

  /**
   * Called by chart-renderer when the user hovers a curve in the main chart
   * or spectrum chart.  Highlights the corresponding radar polygon and shows
   * dimension score labels, unless a radar legend hover or persistent mode
   * already owns the state.
   * @param {string|number} modelId
   */
  function reverseHoverModel(modelId) {
    // Persistent mode owns the highlight – don't override it
    if (_persistModelId !== null) return;
    _reverseHoverModelId = String(modelId);
    // Radar legend hover takes precedence – don't override it
    if (_legendHoverModelId !== null) return;
    _highlightModel(modelId);
    const model = models.find(m => String(m.modelId) === String(modelId));
    if (model) _showScoreOverlay(model, _getModelColor(modelId));
    _notifyHighlightChange();
  }

  /**
   * Called by chart-renderer when the user's cursor leaves the main chart
   * or spectrum chart canvas.  Clears the reverse-hover radar highlight
   * unless a radar legend hover or persistent mode is currently active.
   */
  function reverseHoverClear() {
    // Persistent mode owns the highlight – don't clear it
    if (_persistModelId !== null) { _reverseHoverModelId = null; return; }
    _reverseHoverModelId = null;
    // Radar legend hover is still active – leave its state intact
    if (_legendHoverModelId !== null) return;
    _clearHighlight();
    _hideScoreOverlay();
    _notifyHighlightChange();
  }

  /**
   * Set the per-condition like state for a model.
   * Called externally (e.g. by fancool.js) after fetching /api/like_status.
   * @param {string|number} modelId
   * @param {Set|Array<string>} likedCids  – condition ID strings that are liked
   */
  function setModelLikeState(modelId, likedCids) {
    const s = likedCids instanceof Set
      ? likedCids
      : new Set(Array.isArray(likedCids) ? likedCids.map(String) : []);
    _modelLikeState.set(String(modelId), s);
    _refreshLegendThumbs();
  }

  function onHighlightChange(fn) { _onHighlightChange = fn; }

  return {
    mount(containerEl) {
      panelEl = containerEl;
      try { collapsed = localStorage.getItem('radarOverviewCollapsed') === '1'; } catch (_) {}
      _bindGlobalDismiss();
      _bindToggle();
      _render();
    },
    update,
    setConditionLabels,
    setActiveConditions,
    isAtLimit,
    onConditionToggle,
    onModelRemove,
    onModelHideToggle,
    onHighlightChange,
    reverseHoverModel,
    reverseHoverClear,
    setModelLikeState,
  };
})();
