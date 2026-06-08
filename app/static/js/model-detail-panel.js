/**
 * model-detail-panel.js
 *
 * Common module for rendering model detail info panels.
 * Exposed as window.ModelDetailPanel.
 *
 * Usage:
 *   const { el, ensureImageLoaded } = window.ModelDetailPanel.build(item, options);
 *   window.ModelDetailPanel.renderInto(container, item, options);
 *
 * Options:
 *   showPhoto     {boolean}  default true  — show product photo
 *   showPurchase  {boolean}  default true  — show purchase link section
 *   schema        {string[]} — ordered list of field keys to render;
 *                              defaults to FULL_SCHEMA
 *   loadThumb     {function} — async (modelId) => thumbUrl|null; for lazy photo loading
 *   imageLoading  {string}   default 'lazy' — img loading mode ('lazy' | 'eager')
 */
(function () {
  'use strict';

  // Full schema: all supported field keys in display order.
  var FULL_SCHEMA = [
    'size',
    'max_speed',
    'rgb_names_zh',
    'reference_price',
    'bearing_type_zh',
    'speed_switch_type_name_zh',
    'chain_type_name_zh',
    'color_flags',
    'reverse_opt',
  ];

  // Labels for each key.
  var LABELS = {
    size:                     '尺寸',
    max_speed:                '最大转速',
    rgb_names_zh:             'RGB灯光',
    reference_price:          '参考价格',
    bearing_type_zh:          '轴承类型',
    speed_switch_type_name_zh: '最大转速切换',
    chain_type_name_zh:       '串联方式',
    color_flags:              '可选颜色',
    reverse_opt:              '可选反叶',
  };

  var EMPTY_TEXT = '暂无相关数据';

  // ---- Helpers ----

  function _textOrNA(v) {
    if (v === null || v === undefined) return EMPTY_TEXT;
    var s = String(v).trim();
    return s ? s : EMPTY_TEXT;
  }

  function _resolveColorDefs(colorFlags) {
    var raw = Number(colorFlags);
    var n = Number.isFinite(raw) ? raw : 0;
    if (!n) return [];
    var chips = [];
    if (n & 1) chips.push({ text: '黑', cls: 'is-black' });
    if (n & 2) chips.push({ text: '白', cls: 'is-white' });
    if (n & 4) chips.push({ text: '猫头鹰', cls: 'is-noctua' });
    if (n & 8) chips.push({ text: '银', cls: 'is-silver' });
    if (n & 128) chips.push({ text: '其它', cls: 'is-other' });
    return chips;
  }

  function _buildColorValueEl(colorFlags) {
    var wrap = document.createElement('div');
    wrap.className = 'fc-model-detail__color-wrap';
    var chips = _resolveColorDefs(colorFlags);
    if (!chips.length) {
      var empty = document.createElement('span');
      empty.className = 'fc-model-detail__empty';
      empty.textContent = EMPTY_TEXT;
      wrap.appendChild(empty);
      return wrap;
    }
    chips.forEach(function (chip) {
      var chipEl = document.createElement('span');
      chipEl.className = 'fc-model-detail__color-chip ' + chip.cls;
      var swatch = document.createElement('span');
      swatch.className = 'fc-model-detail__color-swatch fc-color-swatch ' + chip.cls;
      var txt = document.createElement('span');
      txt.className = 'fc-model-detail__color-chip-text';
      txt.textContent = chip.text;
      chipEl.appendChild(swatch);
      chipEl.appendChild(txt);
      wrap.appendChild(chipEl);
    });
    return wrap;
  }

  /**
   * Get the display value (element or string) for a given field key + item.
   * Returns { value, isElement } or null to skip the row.
   */
  function _resolveField(key, item) {
    switch (key) {
      case 'size': {
        var s = item.size, t = item.thickness;
        var str = (s && t) ? (s + '×' + t) : (s || '');
        return { value: str ? _textOrNA(str) : EMPTY_TEXT, isElement: false };
      }
      case 'max_speed': {
        var spd = item.max_speed;
        return {
          value: (spd != null && spd !== '') ? (spd + ' RPM') : EMPTY_TEXT,
          isElement: false,
        };
      }
      case 'reference_price': {
        var p = item.reference_price;
        return {
          value: (p != null && p !== '') ? ('¥' + p) : EMPTY_TEXT,
          isElement: false,
        };
      }
      case 'color_flags':
        return { value: _buildColorValueEl(item.color_flags), isElement: true };
      case 'reverse_opt': {
        var ro = item.reverse_opt;
        var txt;
        if (ro === 1 || ro === '1') txt = '是';
        else if (ro === 0 || ro === '0') txt = '否';
        else txt = EMPTY_TEXT;
        return { value: txt, isElement: false };
      }
      default:
        return { value: _textOrNA(item[key]), isElement: false };
    }
  }

  // ---- Core build function ----

  /**
   * Build a model detail panel element.
   *
   * @param {object} item - model metadata object
   * @param {object} [options]
   * @param {boolean} [options.showPhoto=true]
   * @param {boolean} [options.showPurchase=true]
   * @param {string[]} [options.schema] - field keys to show; defaults to FULL_SCHEMA
   * @param {function} [options.loadThumb] - async (modelId) => url|null
   * @param {string} [options.imageLoading='lazy'] - img loading mode
   * @param {function} [options.onHeightChange] - callback after async image load
   * @returns {{ el: HTMLElement, ensureImageLoaded: function }}
   */
  function build(item, options) {
    var opts = options || {};
    var showPhoto    = opts.showPhoto    !== false;
    var showPurchase = opts.showPurchase !== false;
    var schema       = Array.isArray(opts.schema) ? opts.schema : FULL_SCHEMA;
    var loadThumb    = typeof opts.loadThumb === 'function' ? opts.loadThumb : null;
    var imageLoading = opts.imageLoading === 'eager' ? 'eager' : 'lazy';
    var onHeightChange = typeof opts.onHeightChange === 'function' ? opts.onHeightChange : null;

    var root = document.createElement('div');
    root.className = 'fc-model-detail';

    // ---- Photo ----
    var photoBox = null;
    if (showPhoto) {
      var top = document.createElement('div');
      top.className = 'fc-model-detail__top';

      photoBox = document.createElement('div');
      photoBox.className = 'fc-model-detail__photo';
      var photoPh = document.createElement('div');
      photoPh.className = 'fc-model-detail__photo-placeholder';
      photoPh.textContent = '暂无图片';
      photoBox.appendChild(photoPh);
      top.appendChild(photoBox);

      var attrsBox = _buildAttrsTable(schema, item);
      top.appendChild(attrsBox);
      root.appendChild(top);
    } else {
      root.appendChild(_buildAttrsTable(schema, item));
    }

    // ---- Purchase ----
    if (showPurchase) {
      var purchase = document.createElement('div');
      purchase.className = 'fc-model-detail__purchase';
      var purchaseTitle = document.createElement('div');
      purchaseTitle.className = 'fc-model-detail__purchase-title';
      purchaseTitle.textContent = '施工中';
      var purchaseEmpty = document.createElement('div');
      purchaseEmpty.className = 'fc-model-detail__purchase-empty';
      purchaseEmpty.textContent = '敬请期待';
      purchase.appendChild(purchaseTitle);
      purchase.appendChild(purchaseEmpty);
      root.appendChild(purchase);
    }

    // ---- Lazy image loading ----
    var imageLoadingStarted = false;
    var ensureImageLoaded = function () {
      if (!showPhoto || !photoBox || !loadThumb) return;
      if (imageLoadingStarted) return;
      imageLoadingStarted = true;
      loadThumb(item.model_id).then(function (thumbUrl) {
        photoBox.innerHTML = '';
        if (!thumbUrl) {
          var ph = document.createElement('div');
          ph.className = 'fc-model-detail__photo-placeholder';
          ph.textContent = '暂无图片';
          photoBox.appendChild(ph);
          if (onHeightChange) onHeightChange();
          return;
        }
        var img = document.createElement('img');
        img.className = 'fc-model-detail__photo-img';
        img.loading = imageLoading;
        img.alt = [item.brand_name_zh, item.model_name].filter(Boolean).join(' ') || '风扇照片';
        img.src = thumbUrl;
        img.addEventListener('load', function () { if (onHeightChange) onHeightChange(); });
        img.addEventListener('error', function () { if (onHeightChange) onHeightChange(); });
        photoBox.appendChild(img);
        if (onHeightChange) onHeightChange();
      });
    };

    return { el: root, ensureImageLoaded: ensureImageLoaded };
  }

  function _buildAttrsTable(schema, item) {
    var table = document.createElement('div');
    table.className = 'fc-model-detail__attrs';
    schema.forEach(function (key) {
      var label = LABELS[key];
      if (!label) return; // skip unknown keys
      var resolved = _resolveField(key, item);
      if (!resolved) return;

      var row = document.createElement('div');
      row.className = 'fc-model-detail__attr-row';

      var keyEl = document.createElement('span');
      keyEl.className = 'fc-model-detail__attr-key';
      keyEl.textContent = label;

      var valEl = document.createElement('span');
      valEl.className = 'fc-model-detail__attr-val';
      if (resolved.isElement) {
        valEl.appendChild(resolved.value);
      } else {
        valEl.textContent = resolved.value;
      }

      row.appendChild(keyEl);
      row.appendChild(valEl);
      table.appendChild(row);
    });
    return table;
  }

  /**
   * Clear host and render model detail panel into it.
   *
   * @param {HTMLElement} host - container element to render into
   * @param {object|null} item - model metadata; if null/falsy, shows empty state
   * @param {object} [options] - same as build()
   * @returns {{ ensureImageLoaded: function }}
   */
  function renderInto(host, item, options) {
    host.innerHTML = '';
    if (!item) {
      return { ensureImageLoaded: function () {} };
    }
    var result = build(item, options);
    host.appendChild(result.el);
    return result;
  }

  // Expose
  window.ModelDetailPanel = {
    build: build,
    renderInto: renderInto,
    FULL_SCHEMA: FULL_SCHEMA,
    LABELS: LABELS,
  };

})();
