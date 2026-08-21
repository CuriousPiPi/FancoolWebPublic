/* =============================================================
   ladder-modal.js
   Fan tier-list (天梯图) modal UI and canvas rendering.
   ============================================================= */

(function initLadderModalModule() {
  'use strict';

  // ── Export / display configuration ────────────────────────────────────────
  // Logical design coordinates stay at 600 px; EXPORT_SCALE controls the
  // actual canvas pixel size (1200 px at ×2) for high-DPI saved images.
  // The canvas is shown at ≤ 600 px in the browser via CSS.
  var DESIGN_W      = 600;
  var EXPORT_SCALE  = 2;

  // ── Canvas layout constants (all values are logical design pixels) ─────────
  var TITLE_H         = 105;  // title + date + note area (increased to fit caution note)
  var HEADER_H        = 28;   // column label row
  var ROW_H           = 44;   // per-item row height
  var BOTTOM_PAD      = 16;   // extra space at bottom
  var NOTE_BOTTOM_PAD = 6;

  // Horizontal zones (logical pixel positions within the 600 px design width)
  // Visual layout: [rank] [←score bar] [model/brand] [price bar→]
  var RANK_RIGHT      = 25;   // rank text drawn right-aligned here
  var SCORE_BAR_X     = 35;   // leftmost edge the score bar can reach (max-width bar)
  var SCORE_BAR_MAX_W = 230;  // maximum score bar width
  var SCORE_BAR_RIGHT = SCORE_BAR_X + SCORE_BAR_MAX_W;  // 254 – right edge (fixed, near model text)
  var TEXT_X          = SCORE_BAR_RIGHT + 14;            // 268 – model/brand text left edge
  var TEXT_W          = 170;  // model/brand text zone width
  var PRICE_BAR_X     = TEXT_X + TEXT_W + 6;            // 426 – price bar left edge
  var PRICE_BAR_MAX_W = 100;  // maximum price bar width
  var MIN_BAR_W       = 4;    // minimum rendered bar width in px

  // ── Ladder display names ──────────────────────────────────────────────────
  var LADDER_DISPLAY_NAMES = {
    composite:      '120\u98ce\u6247\u7efc\u5408\u5929\u68af\u56fe',
    intake_exhaust: '120\u98ce\u6247\u8fdb\u6392\u6c14\u5929\u68af\u56fe',
    radiator:       '120\u98ce\u6247\u5439\u51b7\u6392\u5929\u68af\u56fe',
  };
  var LADDER_SORT_NOTES = {
    composite: '\u7efc\u5408\u5929\u68af\uff1a\u6309\u7f51\u7ad9\u7efc\u5408\u8bc4\u5206\u964d\u5e8f\u6392\u5e8f\uff1b\u540c\u8bc4\u5206\u6309\u53c2\u8003\u4ef7\u5347\u5e8f\uff1b\u540c\u4ef7\u6309\u540e\u53f0 model_id \u964d\u5e8f\u3002',
    intake_exhaust: '\u8fdb\u6392\u6c14\u5929\u68af\uff1a\u6309\u6392\u6c14\u4e0e\u8fdb\u6c14\u5e73\u5747\u5206\u964d\u5e8f\u6392\u5e8f\uff1b\u540c\u8bc4\u5206\u6309\u53c2\u8003\u4ef7\u5347\u5e8f\uff1b\u540c\u4ef7\u6309\u540e\u53f0 model_id \u964d\u5e8f\u3002',
    radiator: '\u5439\u51b7\u6392\u5929\u68af\uff1a\u6309\u5439\u51b7\u6392\u8bc4\u5206\u964d\u5e8f\u6392\u5e8f\uff1b\u540c\u8bc4\u5206\u6309\u53c2\u8003\u4ef7\u5347\u5e8f\uff1b\u540c\u4ef7\u6309\u540e\u53f0 model_id \u964d\u5e8f\u3002',
  };
  var LADDER_COMMON_NOTE = '\u8bc4\u5206\u6838\u5fc3\u601d\u8def\u4e3a\u98ce\u6247\u7684\u201c\u98ce\u566a\u6bd4\u201d\uff1a\u540c\u5206\u8d1d\u566a\u97f3\u4e0b\u6709\u6548\u98ce\u91cf\u8d8a\u5927\uff0c\u8bc4\u5206\u8d8a\u9ad8\uff1b\u4e0e\u6700\u9ad8\u8f6c\u901f\u6216\u6700\u5927\u98ce\u91cf\u65e0\u76f4\u63a5\u5173\u8054\u3002';

  var LADDER_EXTRA_NOTES = {
  intake_exhaust: '\u6ce8\u610f\uff1a\u90e8\u5206\u673a\u7bb1\u8fdb\u6c14\u4f4d\u4e3a\u786c\u8d28\u5b54\u677f\u4e14\u7d27\u8d34\u98ce\u6247\u8fdb\u6c14\u4fa7\uff0c\u53ef\u80fd\u5f15\u8d77\u5578\u53eb\uff1b\u53ef\u901a\u8fc7\u57ab\u9ad8\u98ce\u6247\u6216\u9009\u7528\u4f4e\u901f\u98ce\u6247\u6539\u5584\u3002',
  };

  var LADDER_CAUTION_NOTE = '\u6a59\u8272\u80cc\u666f\u6807\u6ce8\u7684\u578b\u53f7\u636e\u53cd\u9988\u53ef\u80fd\u5b58\u5728\u8bbe\u8ba1\u6216\u6279\u6b21\u95ee\u9898\uff0c\u8d2d\u4e70\u524d\u5efa\u8bae\u67e5\u770b\u7f51\u7ad9\u5bf9\u5e94\u578b\u53f7\u7684\u6d4b\u8bd5\u70b9\u8bc4\u533a\u3002';
  var CAUTION_NOTE_HIGHLIGHT = '\u6a59\u8272\u80cc\u666f'; // "橙色背景" — prefix to highlight in the note line

  // ── Drawing colours (fixed light theme for a portable saved image) ─────────
  var C_TITLE_BG      = '#1e293b';
  var C_TITLE_NAME    = '#f1f5f9';
  var C_TITLE_DATE    = '#94a3b8';
  var C_HEADER_BG     = '#f1f5f9';
  var C_HEADER_TEXT   = '#475569';
  var C_ROW_ODD       = '#ffffff';
  var C_ROW_EVEN      = '#f8fafc';
  var C_DIVIDER       = '#e2e8f0';
  var C_RANK          = '#9ca3af';
  var C_SCORE_BAR     = '#3b82f6';
  var C_SCORE_TEXT    = '#1e40af';
  var C_SCORE_TEXT_ON = '#ffffff'; // score label drawn on a wide bar
  var C_MODEL         = '#111827';
  var C_BRAND         = '#6b7280';
  var C_PRICE_BAR     = '#eb8100';
  var C_PRICE_TEXT    = '#d27300';
  var C_NO_PRICE      = '#9ca3af';
  var C_NO_DATA       = '#9ca3af';
  var C_CAUTION_BG    = (function () {
    try {
      var val = window.getComputedStyle(document.documentElement).getPropertyValue('--fc-caution-bg');
      var resolved = String(val || '').trim();
      return resolved || '#ffd97a';
    } catch (_) {
      return '#ffd97a';
    }
  }()); // light orange — marks models with reported defects
  var CAUTION_PAD_X   = 3;        // horizontal padding for caution background
  var NOTE_HL_PAD_X   = 1;        // horizontal padding for note highlight background
  var NOTE_HL_H       = 12;       // height of note highlight background (fits 11px font)
  var NOTE_HL_TOP_OFF = 10;        // ascent offset above alphabetic baseline for 11px font
  var NOTE_HL_RADIUS  = 4;        // corner radius of note highlight background
  var RGB_TAG_TEXT    = 'RGB';
  var RGB_TAG_PAD_X   = 5;
  var RGB_TAG_H       = 12;
  var RGB_TAG_GAP     = 6;
  var RGB_TAG_RADIUS  = 6;

  // ── Helpers ────────────────────────────────────────────────────────────────

  function clipTextToWidth(ctx, text, maxWidth) {
    var source = String(text || '');
    if (!source || maxWidth <= 0) return '';
    if (ctx.measureText(source).width <= maxWidth) return source;
    var ellipsis = '\u2026';
    var clipped = source;
    while (clipped.length > 0 && ctx.measureText(clipped + ellipsis).width > maxWidth) {
      clipped = clipped.slice(0, -1);
    }
    return clipped ? clipped + ellipsis : ellipsis;
  }

  /** Draw text truncated with "…" if it exceeds maxWidth pixels. */
  function fillTextClipped(ctx, text, x, y, maxWidth) {
    var clipped = clipTextToWidth(ctx, text, maxWidth);
    if (!clipped) return '';
    ctx.fillText(clipped, x, y);
    return clipped;
  }

  function wrapTextLines(ctx, text, maxWidth) {
    var source = String(text || '').trim();
    if (!source) return [];
    var lines = [];
    var line = '';
    for (var i = 0; i < source.length; i += 1) {
      var ch = source.charAt(i);
      var next = line + ch;
      if (line && ctx.measureText(next).width > maxWidth) {
        lines.push(line);
        line = ch;
      } else {
        line = next;
      }
    }
    if (line) lines.push(line);
    return lines;
  }

  /** Draw a rounded rectangle path (helper; call fill/stroke after). */
  function roundRect(ctx, x, y, w, h, r) {
    if (w < 2 * r) r = w / 2;
    if (h < 2 * r) r = h / 2;
    ctx.beginPath();
    ctx.moveTo(x + r, y);
    ctx.lineTo(x + w - r, y);
    ctx.quadraticCurveTo(x + w, y, x + w, y + r);
    ctx.lineTo(x + w, y + h - r);
    ctx.quadraticCurveTo(x + w, y + h, x + w - r, y + h);
    ctx.lineTo(x + r, y + h);
    ctx.quadraticCurveTo(x, y + h, x, y + h - r);
    ctx.lineTo(x, y + r);
    ctx.quadraticCurveTo(x, y, x + r, y);
    ctx.closePath();
  }

  /** Format a price value for display (e.g. 1200 → "¥1.2k", 199 → "¥199"). */
  function formatPrice(price) {
    if (price === null || price === undefined) return null;
    if (price >= 1000) return '\uffe5' + (price / 1000).toFixed(1) + 'k';
    return '\uffe5' + Math.round(price);
  }

  function hasVisibleRgb(item) {
    var rgbTypeName = String(item && item.rgb_names_zh || '').trim();
    return !!rgbTypeName && rgbTypeName !== '\u65e0';
  }

  function setRgbTagFont(ctx) {
    ctx.font = 'bold 9px sans-serif';
  }

  function measureRgbTagWidth(ctx) {
    var prevFont = ctx.font;
    setRgbTagFont(ctx);
    var width = ctx.measureText(RGB_TAG_TEXT).width + RGB_TAG_PAD_X * 2;
    ctx.font = prevFont;
    return width;
  }

  function drawRgbTag(ctx, x, centerY) {
    var width = measureRgbTagWidth(ctx);
    var top = centerY - RGB_TAG_H / 2;
    var gradient = ctx.createLinearGradient(x, top, x + width, top + RGB_TAG_H);
    gradient.addColorStop(0.2, 'rgb(255, 80, 80)');
    gradient.addColorStop(0.5, 'rgb(80, 255, 80)');
    gradient.addColorStop(0.8, 'rgb(80, 80, 255)');
    ctx.fillStyle = gradient;
    roundRect(ctx, x, top, width, RGB_TAG_H, RGB_TAG_RADIUS);
    ctx.fill();

    ctx.fillStyle = '#ffffff';
    var prevFont = ctx.font;
    setRgbTagFont(ctx);
    ctx.textAlign = 'left';
    ctx.textBaseline = 'middle';
    ctx.fillText(RGB_TAG_TEXT, x + RGB_TAG_PAD_X, centerY + 0.5);
    ctx.font = prevFont;
  }

  // ── Per-row renderers ──────────────────────────────────────────────────────

  /**
   * Draw the score bar.
   * The bar grows right-to-left: right edge is fixed at SCORE_BAR_RIGHT
   * (adjacent to the model/brand text), extending leftward based on score.
   */
  function drawScoreBar(ctx, cy, score, maxScore) {
    // score > 0 guard prevents MIN_BAR_W from producing a visible bar for zero-score entries
    var bw = (maxScore > 0 && score > 0) ? Math.max(MIN_BAR_W, Math.round((score / maxScore) * SCORE_BAR_MAX_W)) : 0;
    var bh = 18;
    var by = cy - bh / 2;
    var barRight = SCORE_BAR_RIGHT;
    var barX = barRight - bw;

    if (bw > 0) {
      ctx.fillStyle = C_SCORE_BAR;
      roundRect(ctx, barX, by, bw, bh, 3);
      ctx.fill();
    }

    var scoreLabel = String(Math.round(score));
    ctx.font = 'bold 12px sans-serif';
    ctx.textBaseline = 'middle';
    if (bw >= 30) {
      // Wide bar: draw score inside the bar, near the right edge
      ctx.fillStyle = C_SCORE_TEXT_ON;
      ctx.textAlign = 'right';
      ctx.fillText(scoreLabel, barRight - 4, cy);
    } else {
      // Short bar: draw score to the left of the bar to avoid overlap
      ctx.fillStyle = C_SCORE_TEXT;
      ctx.textAlign = 'right';
      ctx.fillText(scoreLabel, barX - 4, cy);
    }
  }

  function drawPriceBar(ctx, cy, price, maxPrice) {
    if (price === null || price === undefined) {
      ctx.fillStyle = C_NO_PRICE;
      ctx.font = '11px sans-serif';
      ctx.textAlign = 'left';
      ctx.textBaseline = 'middle';
      ctx.fillText('\u6682\u65e0\u4ef7\u683c', PRICE_BAR_X, cy); // 暂无价格
      return;
    }
    var bw = maxPrice > 0 ? Math.max(MIN_BAR_W, Math.round((price / maxPrice) * PRICE_BAR_MAX_W)) : MIN_BAR_W;
    var bh = 14;
    var by = cy - bh / 2;

    ctx.fillStyle = C_PRICE_BAR;
    roundRect(ctx, PRICE_BAR_X, by, bw, bh, 2);
    ctx.fill();

    var priceLabel = formatPrice(price);
    if (priceLabel) {
      ctx.fillStyle = C_PRICE_TEXT;
      ctx.font = '11px sans-serif';
      ctx.textAlign = 'left';
      ctx.textBaseline = 'middle';
      ctx.fillText(priceLabel, PRICE_BAR_X + bw + 3, cy);
    }
  }

  // ── Main render function ───────────────────────────────────────────────────

  /**
   * Render a single ladder onto the given canvas element.
   *
   * The canvas is drawn at DESIGN_W×EXPORT_SCALE actual pixels but displayed
   * at ≤ DESIGN_W CSS pixels via inline style, giving a crisp high-DPI image.
   * Right-click save / download preserves the full 1200 px width.
   *
   * @param {HTMLCanvasElement} canvas
   * @param {{ displayName: string, date: string, items: Array<object>, ladderType?: string }} ladderData
   */
  function renderLadder(canvas, ladderData) {
    var items = Array.isArray(ladderData.items) ? ladderData.items : [];
    var rowCount = items.length > 0 ? items.length : 3; // reserve space for empty message
    var designHeight = TITLE_H + HEADER_H + rowCount * ROW_H + BOTTOM_PAD;

    // High-res canvas: actual pixel size = logical design size × export scale.
    // Guard against browser max-canvas limits (often ~8192 px on one axis).
    var MAX_CANVAS_PX = 8192;
    var scale = EXPORT_SCALE;
    if (designHeight * scale > MAX_CANVAS_PX) {
      scale = Math.max(1, Math.floor(MAX_CANVAS_PX / designHeight));
    }
    canvas.width  = DESIGN_W * scale;
    canvas.height = designHeight * scale;
    // CSS: display at ≤ 600 px, shrink to fit narrower containers automatically
    canvas.style.width    = '100%';
    canvas.style.maxWidth = DESIGN_W + 'px';
    canvas.style.height   = 'auto';
    canvas.style.display  = 'block';

    var ctx = canvas.getContext('2d');
    if (!ctx) return;
    // Scale all subsequent drawing operations to the export resolution
    ctx.setTransform(scale, 0, 0, scale, 0, 0);
    // 1. Title background
    ctx.fillStyle = C_TITLE_BG;
    ctx.fillRect(0, 0, DESIGN_W, TITLE_H);

    // Watermark / site URL at the top-left corner
    ctx.fillStyle = '#ffffffc6';
    ctx.font = '15px sans-serif';
    ctx.textAlign = 'left';
    ctx.textBaseline = 'top';
    ctx.fillText('fancool.cc·风扇库 ', 4, 4);

    // 2. Title: ladder name + date (same row)
    var titleY = 24;
    ctx.fillStyle = C_TITLE_NAME;
    ctx.font = 'bold 17px sans-serif';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    var titleText = ladderData.displayName || '';
    ctx.fillText(titleText, DESIGN_W / 2, titleY);
    var titleWidth = ctx.measureText(titleText).width;

    ctx.fillStyle = C_TITLE_DATE;
    ctx.font = '12px sans-serif';
    var dateText = ladderData.date || '';
    if (dateText) {
      var dateWidth = ctx.measureText(dateText).width;
      var dateX = DESIGN_W / 2 + titleWidth / 2 + 12;
      var maxDateX = DESIGN_W - 10 - dateWidth;
      if (dateX > maxDateX) dateX = maxDateX;
      if (dateX < DESIGN_W / 2 + 8) dateX = DESIGN_W / 2 + 8;
      ctx.textAlign = 'left';
      ctx.textBaseline = 'middle';
      ctx.fillText(dateText, dateX, titleY);
    }

    // 3. Notes under title (common note + active ladder specific note)
    var noteLines = [];
    var noteMaxWidth = DESIGN_W - 24;
    ctx.font = '11px sans-serif';
    noteLines = noteLines
      .concat(wrapTextLines(ctx, LADDER_COMMON_NOTE, noteMaxWidth))
      .concat(wrapTextLines(ctx, LADDER_SORT_NOTES[ladderData.ladderType], noteMaxWidth));

    if (LADDER_EXTRA_NOTES[ladderData.ladderType]) {
    noteLines = noteLines.concat(wrapTextLines(ctx, LADDER_EXTRA_NOTES[ladderData.ladderType], noteMaxWidth));
    }

    noteLines = noteLines.concat(wrapTextLines(ctx, LADDER_CAUTION_NOTE, noteMaxWidth));

    ctx.fillStyle = C_TITLE_DATE;
    ctx.textAlign = 'left';
    ctx.textBaseline = 'alphabetic';
    var noteX = 12;
    var noteY = 56;
    var noteLineH = 13;
    noteLines.forEach(function (line, i) {
      var lineY = noteY + i * noteLineH;
      if (lineY > TITLE_H - NOTE_BOTTOM_PAD) return;
      if (line.indexOf(CAUTION_NOTE_HIGHLIGHT) === 0) {
        var hlW = ctx.measureText(CAUTION_NOTE_HIGHLIGHT).width;
        ctx.fillStyle = C_CAUTION_BG;
        roundRect(ctx, noteX - NOTE_HL_PAD_X, lineY - NOTE_HL_TOP_OFF, hlW + NOTE_HL_PAD_X * 2, NOTE_HL_H, NOTE_HL_RADIUS);
        ctx.fill();
        ctx.fillStyle = C_TITLE_DATE;
      }
      ctx.fillText(line, noteX, lineY);
    });

    // 4. Column header row
    ctx.fillStyle = C_HEADER_BG;
    ctx.fillRect(0, TITLE_H, DESIGN_W, HEADER_H);

    ctx.fillStyle = C_HEADER_TEXT;
    ctx.font = 'bold 12px sans-serif';
    ctx.textBaseline = 'middle';
    var hy = TITLE_H + HEADER_H / 2;

    ctx.textAlign = 'right';
    ctx.fillText('#', RANK_RIGHT, hy);

    ctx.textAlign = 'left';
    ctx.fillText('\u8bc4\u5206', SCORE_BAR_X, hy);           // 评分
    ctx.fillText('\u578b\u53f7 / \u54c1\u724c', TEXT_X, hy); // 型号 / 品牌
    ctx.fillText('\u53c2\u8003\u4ef7', PRICE_BAR_X, hy);     // 参考价

    // Divider below header
    ctx.strokeStyle = C_DIVIDER;
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(0, TITLE_H + HEADER_H - 0.5);
    ctx.lineTo(DESIGN_W, TITLE_H + HEADER_H - 0.5);
    ctx.stroke();

    var rowsTop = TITLE_H + HEADER_H;

    // 5. No-data message when the ladder has no items
    if (items.length === 0) {
      ctx.fillStyle = C_ROW_ODD;
      ctx.fillRect(0, rowsTop, DESIGN_W, rowCount * ROW_H + BOTTOM_PAD);
      ctx.fillStyle = C_NO_DATA;
      ctx.font = '14px sans-serif';
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';
      ctx.fillText('\u6682\u65e0\u53ef\u7528\u6570\u636e', DESIGN_W / 2, rowsTop + (rowCount * ROW_H) / 2);
      // 暂无可用数据
      return;
    }

    // 6. Pre-compute normalisation max values
    var maxScore = 0;
    var maxPrice = 0;
    items.forEach(function (item) {
      if (item.score > maxScore) maxScore = item.score;
      if (item.reference_price != null && item.reference_price > maxPrice) maxPrice = item.reference_price;
    });

    // 7. Per-item rows
    items.forEach(function (item, idx) {
      var rowY = rowsTop + idx * ROW_H;
      var cy   = rowY + ROW_H / 2;

      // Row background
      ctx.fillStyle = idx % 2 === 0 ? C_ROW_ODD : C_ROW_EVEN;
      ctx.fillRect(0, rowY, DESIGN_W, ROW_H);

      // Row bottom divider
      ctx.strokeStyle = C_DIVIDER;
      ctx.lineWidth = 0.5;
      ctx.beginPath();
      ctx.moveTo(0, rowY + ROW_H - 0.5);
      ctx.lineTo(DESIGN_W, rowY + ROW_H - 0.5);
      ctx.stroke();

      // Rank number
      ctx.fillStyle = C_RANK;
      ctx.font = 'bold 13px sans-serif';
      ctx.textAlign = 'right';
      ctx.textBaseline = 'middle';
      ctx.fillText(String(item.rank), RANK_RIGHT, cy);

      // Score bar (grows right-to-left, right edge fixed at SCORE_BAR_RIGHT)
      drawScoreBar(ctx, cy, item.score, maxScore);

      // Model name (bold, larger)
      ctx.font = 'bold 14px sans-serif';
      ctx.textAlign = 'left';
      ctx.textBaseline = 'alphabetic';
      var showRgbTag = hasVisibleRgb(item);
      var rgbTagWidth = showRgbTag ? measureRgbTagWidth(ctx) : 0;
      var modelMaxWidth = TEXT_W - (showRgbTag ? (rgbTagWidth + RGB_TAG_GAP) : 0);
      var modelText = clipTextToWidth(ctx, item.model_name || '', modelMaxWidth);
      var modelTextWidth = modelText ? ctx.measureText(modelText).width : 0;

      if (item.caution == 1 && modelText) {
        ctx.fillStyle = C_CAUTION_BG;
        roundRect(ctx, TEXT_X - CAUTION_PAD_X, cy - 17.5, modelTextWidth + CAUTION_PAD_X * 2, 16, 6);
        ctx.fill();
      }

      ctx.fillStyle = C_MODEL;
      if (modelText) ctx.fillText(modelText, TEXT_X, cy - 4);

      if (showRgbTag) {
        var tagX = item.caution == 1
          ? TEXT_X + modelTextWidth + CAUTION_PAD_X + 3
          : TEXT_X + modelTextWidth + RGB_TAG_GAP - 2;
        drawRgbTag(ctx, tagX, cy - 9.2);
      }

      // Brand name (smaller, below model)
      ctx.fillStyle = C_BRAND;
      ctx.font = '12px sans-serif';
      ctx.textAlign = 'left';
      ctx.textBaseline = 'alphabetic';
      var brandName = (item.brand_name || '').trim();
      var bearingType = (item.bearing_type_zh || '').trim();
      var brandLine = brandName;
      if (brandName && bearingType) {
        brandLine = brandName + ' · ' + bearingType;
      }
      fillTextClipped(ctx, brandLine, TEXT_X, cy + 13, TEXT_W);

      // Price bar (grows left-to-right)
      drawPriceBar(ctx, cy, item.reference_price, maxPrice);
    });

    // 8. Bottom padding fill
    var bottomY = rowsTop + items.length * ROW_H;
    ctx.fillStyle = C_ROW_ODD;
    ctx.fillRect(0, bottomY, DESIGN_W, BOTTOM_PAD);
  }

  // ── Modal controller ──────────────────────────────────────────────────────

  function initLadderModal() {
    var openBtn     = document.getElementById('ladderOpenBtn');
    var modal       = document.getElementById('ladderModal');
    var closeBtn    = document.getElementById('ladderClose');
    var backdrop    = document.getElementById('ladderBackdrop');
    var tabs        = Array.from(document.querySelectorAll('#ladderTabs .fc-sr-tab'));
    var loading     = document.getElementById('ladderLoading');
    var canvasWrap  = document.getElementById('ladderCanvasWrap');
    var canvas      = document.getElementById('ladderCanvas');
    var downloadBtn = document.getElementById('ladderDownloadBtn');
    var updateHint  = document.getElementById('ladderUpdateHint');

    if (!openBtn || !modal) return;

    var ladderCache   = null;  // fetched once per page load
    var activeType    = 'composite';
    var lastFocused   = null;
    var prevOverflow  = '';
    var trapBound     = false;

    // ── Focus trap ──────────────────────────────────────────────────────────

    function getFocusable() {
      return Array.from(modal.querySelectorAll(
        'a[href],button:not([disabled]),input:not([disabled]),[tabindex]:not([tabindex="-1"])'
      )).filter(function (el) { return el.offsetParent !== null; });
    }

    function onTrapKey(e) {
      if (!modal || modal.hasAttribute('hidden') || e.key !== 'Tab') return;
      var list = getFocusable();
      if (!list.length) { e.preventDefault(); modal.focus(); return; }
      var first = list[0], last = list[list.length - 1];
      if (e.shiftKey) {
        if (document.activeElement === first || !modal.contains(document.activeElement)) {
          e.preventDefault(); last.focus({ preventScroll: true });
        }
      } else if (document.activeElement === last || !modal.contains(document.activeElement)) {
        e.preventDefault(); first.focus({ preventScroll: true });
      }
    }

    function activateTrap()   { if (!trapBound) { document.addEventListener('keydown', onTrapKey, true); trapBound = true; } }
    function deactivateTrap() { if (trapBound)  { document.removeEventListener('keydown', onTrapKey, true); trapBound = false; } }

    // ── Open / close ────────────────────────────────────────────────────────

    function openModal() {
      lastFocused = document.activeElement;
      modal.removeAttribute('hidden');
      prevOverflow = document.body.style.overflow;
      document.body.style.overflow = 'hidden';
      var f = getFocusable();
      try { (f[0] || modal).focus({ preventScroll: true }); } catch (_) {}
      activateTrap();
    }

    function closeModal() {
      modal.setAttribute('hidden', '');
      deactivateTrap();
      document.body.style.overflow = prevOverflow;
      if (lastFocused && typeof lastFocused.focus === 'function') lastFocused.focus();
      lastFocused = null;
    }

    // ── Data loading ────────────────────────────────────────────────────────

    function showLoading(msg) {
      if (loading) { loading.textContent = msg || '\u52a0\u8f7d\u4e2d\u2026'; loading.style.display = ''; }
      if (canvasWrap)  canvasWrap.setAttribute('hidden', '');
      if (downloadBtn) downloadBtn.setAttribute('hidden', '');
    }

    function showCanvas() {
      if (loading) loading.style.display = 'none';
      if (canvasWrap)  canvasWrap.removeAttribute('hidden');
      if (downloadBtn) downloadBtn.removeAttribute('hidden');
    }

    function renderActive() {
      if (!ladderCache) return;
      var ladders = ladderCache.ladders || {};
      var entry   = ladders[activeType];
      if (!entry) return;

      var data = {
        displayName: LADDER_DISPLAY_NAMES[activeType] || entry.name,
        date:        ladderCache.date || '',
        items:       entry.items || [],
        ladderType:  activeType,
      };
      renderLadder(canvas, data);
      showCanvas();
      if (updateHint) updateHint.textContent = '\u6bcf\u65e5\u66f4\u65b0\uff0c\u5f53\u524d\u699c\u5355\u65e5\u671f\uff1a' + (ladderCache.date || '');
      // 每日更新，当前榜单日期：
    }

    function loadData() {
      showLoading();

      var normalizeResp = window.normalizeApiResponse || function (j) { return { ok: j && j.success, data: j && j.data }; };

      fetch('/api/ladder')
        .then(function (r) {
          var status = r.status;
          var ok = r.ok;
          return r.text().then(function (text) {
            var json = null;
            try { json = JSON.parse(text); } catch (_parseErr) { /* non-JSON body (e.g. gateway error page) */ }
            if (!ok) {
              var msg = json && (json.error_message || json.message || json.error);
              throw new Error(msg || ('\u8bf7\u6c42\u5931\u8d25\uff08' + status + '\uff09'));
              // 请求失败（<status>）
            }
            return json;
          });
        })
        .then(function (body) {
          var resp = normalizeResp(body);
          if (!resp.ok) throw new Error((body && (body.error_message || body.message)) || '\u8bf7\u6c42\u5931\u8d25');
          ladderCache = resp.data || {};
          renderActive();
        })
        .catch(function (err) {
          if (loading) {
            loading.textContent = '\u52a0\u8f7d\u5931\u8d25\uff1a' + (err && err.message ? String(err.message) : '\u8bf7\u7a0d\u540e\u518d\u8bd5\u3002');
            loading.style.display = '';
          }
          if (canvasWrap) canvasWrap.setAttribute('hidden', '');
          if (downloadBtn) downloadBtn.setAttribute('hidden', '');
        });
    }

    // ── Tab switching ────────────────────────────────────────────────────────

    function activateTab(type) {
      activeType = type;
      tabs.forEach(function (btn) {
        var isActive = btn.getAttribute('data-ladder') === type;
        if (isActive) {
          btn.classList.add('active');
          btn.setAttribute('aria-selected', 'true');
          btn.setAttribute('tabindex', '0');
          if (canvasWrap && btn.id) canvasWrap.setAttribute('aria-labelledby', btn.id);
        } else {
          btn.classList.remove('active');
          btn.setAttribute('aria-selected', 'false');
          btn.setAttribute('tabindex', '-1');
        }
      });
      if (ladderCache) renderActive();
    }

    tabs.forEach(function (btn) {
      btn.addEventListener('click', function () { activateTab(btn.getAttribute('data-ladder')); });
      btn.addEventListener('keydown', function (e) {
        if (!tabs.length) return;
        var i = tabs.indexOf(btn);
        if (i < 0) return;
        var target = null;
        if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') target = tabs[(i - 1 + tabs.length) % tabs.length];
        if (e.key === 'ArrowRight' || e.key === 'ArrowDown') target = tabs[(i + 1) % tabs.length];
        if (e.key === 'Home') target = tabs[0];
        if (e.key === 'End') target = tabs[tabs.length - 1];
        if (!target) return;
        e.preventDefault();
        var targetType = target.getAttribute('data-ladder');
        activateTab(targetType);
        target.focus();
      });
    });

    // ── Download ─────────────────────────────────────────────────────────────

    if (downloadBtn) {
      downloadBtn.addEventListener('click', function () {
        if (!canvas || !ladderCache) return;
        var name = (LADDER_DISPLAY_NAMES[activeType] || activeType) + (ladderCache.date ? '_' + ladderCache.date : '');
        var a = document.createElement('a');
        a.href = canvas.toDataURL('image/png');
        a.download = name + '.png';
        a.click();
      });
    }

    // ── Wire up open / close / ESC ───────────────────────────────────────────

    openBtn.addEventListener('click', function () {
      if (ladderCache) {
        openModal();
        renderActive();
      } else {
        openModal();
        loadData();
      }
    });

    if (closeBtn)   closeBtn.addEventListener('click', closeModal);
    if (backdrop)   backdrop.addEventListener('click', closeModal);

    document.addEventListener('keydown', function (e) {
      if (e.key === 'Escape' && !modal.hasAttribute('hidden')) closeModal();
    });
  }

  window.LadderModal = { initLadderModal: initLadderModal };
})();
