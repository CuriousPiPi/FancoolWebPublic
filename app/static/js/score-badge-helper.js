/* =============================================================
   score-badge-helper.js
   Shared continuous anchor-based score badge color/style helper.

   Provides smooth, perceptually uniform color transitions for
   score badges across rankings, radar center score, and the
   add-by-model dropdown.

   Exports window.ScoreBadgeHelper with:
     scoreStyle(score)       → { bg, fg } hex color strings
     buildScoreBadge(score)  → DOM <span class="rpv2-score-badge">
     scoreStyleAttr(score)   → inline style string (for HTML templates)
     SCORE_ANCHORS           → the configurable anchor array (tunable)

   Color interpolation uses Oklab – a perceptually uniform color
   space that prevents the saturation dip of plain RGB and the hue
   shift of HSL, resulting in smooth and predictable gradients.
   ============================================================= */

(function () {
  'use strict';

  // ---- Oklab color math ----------------------------------------
  // References: https://bottosson.github.io/posts/oklab/

  function _linearize(c) {
    return c <= 0.04045 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
  }

  function _delinearize(c) {
    return c <= 0.0031308 ? 12.92 * c : 1.055 * Math.pow(c, 1 / 2.4) - 0.055;
  }

  function _hexToRgb(hex) {
    const h = hex.replace('#', '');
    return [
      parseInt(h.slice(0, 2), 16) / 255,
      parseInt(h.slice(2, 4), 16) / 255,
      parseInt(h.slice(4, 6), 16) / 255,
    ];
  }

  function _rgbToHex(r, g, b) {
    return '#' + [r, g, b]
      .map(c => Math.max(0, Math.min(255, Math.round(c * 255))).toString(16).padStart(2, '0'))
      .join('');
  }

  function _rgbToOklab(rgb) {
    const r = rgb[0], g = rgb[1], b = rgb[2];
    const rl = _linearize(r), gl = _linearize(g), bl = _linearize(b);
    const l = Math.cbrt(0.4122214708 * rl + 0.5363325363 * gl + 0.0514459929 * bl);
    const m = Math.cbrt(0.2119034982 * rl + 0.6806995451 * gl + 0.1073969566 * bl);
    const s = Math.cbrt(0.0883024619 * rl + 0.2817188376 * gl + 0.6299787005 * bl);
    return [
      0.2104542553 * l + 0.7936177850 * m - 0.0040720468 * s,
      1.9779984951 * l - 2.4285922050 * m + 0.4505937099 * s,
      0.0259040371 * l + 0.7827717662 * m - 0.8086757660 * s,
    ];
  }

  function _oklabToRgb(lab) {
    const L = lab[0], a = lab[1], b = lab[2];
    const l = Math.pow(L + 0.3963377774 * a + 0.2158037573 * b, 3);
    const m = Math.pow(L - 0.1055613458 * a - 0.0638541728 * b, 3);
    const s = Math.pow(L - 0.0894841775 * a - 1.2914855480 * b, 3);
    return [
      _delinearize(+4.0767416621 * l - 3.3077115913 * m + 0.2309699292 * s),
      _delinearize(-1.2684380046 * l + 2.6097574011 * m - 0.3413193965 * s),
      _delinearize(-0.0041960863 * l - 0.7034186147 * m + 1.7076147010 * s),
    ].map(c => Math.max(0, Math.min(1, c)));
  }

  function _lerpOklab(hexA, hexB, t) {
    const labA = _rgbToOklab(_hexToRgb(hexA));
    const labB = _rgbToOklab(_hexToRgb(hexB));
    const mixed = [0, 1, 2].map(i => labA[i] + t * (labB[i] - labA[i]));
    const rgb = _oklabToRgb(mixed);
    return _rgbToHex(rgb[0], rgb[1], rgb[2]);
  }

  // ---- Configurable anchor points ------------------------------
  // Each anchor: { score, light: { bg, fg }, dark: { bg, fg } }
  //
  // For any score between two anchors the colors are smoothly
  // Oklab-interpolated. To tune the distribution, adjust the
  // score values or change the hex colors.
  //
  // Design rationale for this default set:
  //  • 0–50   → red → orange          (clearly bad)
  //  • 50–65  → orange → amber/yellow (poor to below average)
  //  • 65–78  → yellow → lime         (below average to decent)
  //    — This keeps scores in the low-to-mid 70s looking neutral
  //      rather than the harsh yellow of a simple 70-79 band.
  //  • 78–88  → lime → teal           (decent to good)
  //  • 88–96  → teal → emerald        (good to excellent)
  //  • 96+    → bright emerald        (outstanding)
  const SCORE_ANCHORS = [
    { score: 0,  light: { bg: '#fecaca', fg: '#991b1b' }, dark: { bg: '#7f1d1d', fg: '#fca5a5' } },
    { score: 50, light: { bg: '#fed7aa', fg: '#c2410c' }, dark: { bg: '#7c2d12', fg: '#fdba74' } },
    { score: 65, light: { bg: '#fef9c3', fg: '#a16207' }, dark: { bg: '#713f12', fg: '#fef08a' } },
    { score: 78, light: { bg: '#ecfccb', fg: '#4d7c0f' }, dark: { bg: '#365314', fg: '#d9f99d' } },
    { score: 88, light: { bg: '#d1fae5', fg: '#047857' }, dark: { bg: '#065f46', fg: '#a7f3d0' } },
    { score: 96, light: { bg: '#dcfce7', fg: '#15803d' }, dark: { bg: '#166534', fg: '#bbf7d0' } },
  ];

  // ---- Dark mode detection ------------------------------------
  function _isDark() {
    return document.documentElement.dataset.theme === 'dark';
  }

  // ---- Core: { bg, fg } hex strings for a score ---------------
  function scoreStyle(score) {
    if (score == null || !Number.isFinite(Number(score))) {
      return _isDark()
        ? { bg: '#374151', fg: '#d1d5db' }
        : { bg: '#f3f4f6', fg: '#6b7280' };
    }

    const n = Math.max(0, Math.min(100, Number(score)));
    const key = _isDark() ? 'dark' : 'light';
    const anchors = SCORE_ANCHORS;

    if (n <= anchors[0].score) {
      return { bg: anchors[0][key].bg, fg: anchors[0][key].fg };
    }
    if (n >= anchors[anchors.length - 1].score) {
      const last = anchors[anchors.length - 1];
      return { bg: last[key].bg, fg: last[key].fg };
    }

    let lo = 0;
    for (let i = 0; i < anchors.length - 1; i++) {
      if (n >= anchors[i].score && n <= anchors[i + 1].score) {
        lo = i;
        break;
      }
    }
    const a = anchors[lo], b = anchors[lo + 1];
    const t = (n - a.score) / (b.score - a.score);
    return {
      bg: _lerpOklab(a[key].bg, b[key].bg, t),
      fg: _lerpOklab(a[key].fg, b[key].fg, t),
    };
  }

  // ---- Build a DOM score badge element ------------------------
  function buildScoreBadge(score) {
    const { bg, fg } = scoreStyle(score);
    const label = (score != null && Number.isFinite(Number(score)))
      ? String(Math.round(Number(score)))
      : '—';
    const el = document.createElement('span');
    el.className = 'rpv2-score-badge';
    el.style.background = bg;
    el.style.color = fg;
    el.textContent = label;
    el.title = '综合评分（百分制）';
    // Store score for refreshAllBadges() to re-apply colors on theme change.
    el.dataset.score = (score != null && Number.isFinite(Number(score)))
      ? String(Number(score))
      : '';
    return el;
  }

  // ---- Inline style string for HTML template contexts ---------
  function scoreStyleAttr(score) {
    const { bg, fg } = scoreStyle(score);
    return `background:${bg};color:${fg}`;
  }

  // ---- Re-apply colors to all live score badges on theme change ----
  // Called automatically by the MutationObserver below; also exposed for
  // manual use (e.g. after a programmatic theme switch).
  function refreshAllBadges() {
    document.querySelectorAll('.rpv2-score-badge[data-score]').forEach(function (el) {
      const raw = el.dataset.score;
      const score = raw !== '' ? Number(raw) : null;
      const { bg, fg } = scoreStyle(score);
      el.style.background = bg;
      el.style.color = fg;
    });
  }

  // Watch for data-theme attribute changes on <html> and refresh all badges.
  try {
    new MutationObserver(function (mutations) {
      for (var i = 0; i < mutations.length; i++) {
        if (mutations[i].type === 'attributes' && mutations[i].attributeName === 'data-theme') {
          refreshAllBadges();
          return;
        }
      }
    }).observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
  } catch (_) {}

  // ---- Expose as global ----------------------------------------
  window.ScoreBadgeHelper = { scoreStyle, buildScoreBadge, scoreStyleAttr, refreshAllBadges, SCORE_ANCHORS };
})();
