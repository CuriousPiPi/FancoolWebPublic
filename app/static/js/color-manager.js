const ColorManager = (function() {

  // --- 私有状态与常量 ---

  const STORAGE_KEY = 'colorIndexMap_v1';
  let colorIndexMap = {}; // 核心状态：{ fanKey: colorIndex }

  const DARK_BASE_PALETTE = [
    "#3e6bff", "#FFF958", "#1aed03", "#FF4848", "#DB68FF",
    "#3fe9ff", "#F59916", "#ff91ce", "#8b5cf6", "#22ffb5"
  ];
  const LIGHT_LINEAR_SCALE = 0.66;

  // --- 私有辅助函数 ---

  function _loadMap() {
    try {
      return JSON.parse(localStorage.getItem(STORAGE_KEY) || '{}');
    } catch (e) {
      console.error("Failed to load color map from localStorage", e);
      return {};
    }
  }

  function _saveMap() {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(colorIndexMap));
    } catch (e) {
      console.error("Failed to save color map to localStorage", e);
    }
  }

  function _srgbToLinear(c) {
    return c <= 0.04045 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
  }

  function _linearToSrgb(c) {
    return c <= 0.0031308 ? 12.92 * c : 1.055 * Math.pow(c, 1 / 2.4) - 0.055;
  }

  function _hexToHsl(hex) {
    const h = hex.replace('#', '');
    let r = parseInt(h.slice(0, 2), 16) / 255;
    let g = parseInt(h.slice(2, 4), 16) / 255;
    let b = parseInt(h.slice(4, 6), 16) / 255;
    const max = Math.max(r, g, b), min = Math.min(r, g, b);
    let hue = 0, sat = 0;
    const lit = (max + min) / 2;
    if (max !== min) {
      const d = max - min;
      sat = lit > 0.5 ? d / (2 - max - min) : d / (max + min);
      if (max === r) hue = ((g - b) / d + (g < b ? 6 : 0)) / 6;
      else if (max === g) hue = ((b - r) / d + 2) / 6;
      else hue = ((r - g) / d + 4) / 6;
    }
    return [hue * 360, sat, lit];
  }

  function _hslToHex(h, s, l) {
    h = h / 360;
    const hue2rgb = (p, q, t) => {
      if (t < 0) t += 1;
      if (t > 1) t -= 1;
      if (t < 1 / 6) return p + (q - p) * 6 * t;
      if (t < 1 / 2) return q;
      if (t < 2 / 3) return p + (q - p) * (2 / 3 - t) * 6;
      return p;
    };
    let r, g, b;
    if (s === 0) {
      r = g = b = l;
    } else {
      const q = l < 0.5 ? l * (1 + s) : l + s - l * s;
      const p = 2 * l - q;
      r = hue2rgb(p, q, h + 1 / 3);
      g = hue2rgb(p, q, h);
      b = hue2rgb(p, q, h - 1 / 3);
    }
    const toHex = v => Math.round(v * 255).toString(16).padStart(2, '0');
    return '#' + toHex(r) + toHex(g) + toHex(b);
  }

  function _darkToLightLinear(hex) {
    const h = hex.replace('#', '');
    let r = parseInt(h.slice(0, 2), 16) / 255;
    let g = parseInt(h.slice(2, 4), 16) / 255;
    let b = parseInt(h.slice(4, 6), 16) / 255;

    r = _srgbToLinear(r) * LIGHT_LINEAR_SCALE;
    g = _srgbToLinear(g) * LIGHT_LINEAR_SCALE;
    b = _srgbToLinear(b) * LIGHT_LINEAR_SCALE;

    r = Math.round(_linearToSrgb(r) * 255);
    g = Math.round(_linearToSrgb(g) * 255);
    b = Math.round(_linearToSrgb(b) * 255);

    const toHex = v => v.toString(16).padStart(2, '0');
    return '#' + toHex(r) + toHex(g) + toHex(b);
  }
  
  function _getCurrentTheme() {
    return document.documentElement.getAttribute('data-theme') === 'dark' ? 'dark' : 'light';
  }

  function _getPalette() {
    return _getCurrentTheme() === 'dark' 
      ? DARK_BASE_PALETTE 
      : DARK_BASE_PALETTE.map(_darkToLightLinear);
  }

  function _nextFreeIndex(assignedIndices) {
    let i = 0;
    while (assignedIndices.has(i)) {
      i++;
    }
    return i;
  }

  // --- 公共 API ---

  function init() {
    colorIndexMap = _loadMap();
  }

  function getColor(key) {
    const index = colorIndexMap[key] ?? 0;
    const palette = _getPalette();
    return palette[index % palette.length];
  }

  function getIndex(key) {
    return colorIndexMap[key] ?? 0;
  }

  function releaseIndex(key) {
    if (key && Object.prototype.hasOwnProperty.call(colorIndexMap, key)) {
      delete colorIndexMap[key];
      _saveMap();
    }
  }

  /**
   * [已修复] 为一组曲线分配唯一的颜色索引。
   * @param {string[]} keys - 需要分配索引的曲线 key 数组
   */
  function assignUniqueIndices(keys) {
    if (!Array.isArray(keys)) return;

    const uniqueKeys = [...new Set(keys.filter(Boolean))];
    const finalMap = {};
    const assignedIndices = new Set();

    // 第一次遍历：优先保留已存在且无冲突的颜色
    const indexCounts = {};
    uniqueKeys.forEach(key => {
      if (Object.prototype.hasOwnProperty.call(colorIndexMap, key)) {
        const index = colorIndexMap[key];
        indexCounts[index] = (indexCounts[index] || 0) + 1;
      }
    });

    uniqueKeys.forEach(key => {
      if (Object.prototype.hasOwnProperty.call(colorIndexMap, key)) {
        const index = colorIndexMap[key];
        if (indexCounts[index] === 1 && !assignedIndices.has(index)) {
          finalMap[key] = index;
          assignedIndices.add(index);
        }
      }
    });

    // 第二次遍历：为剩余的（全新的或有冲突的）曲线分配新颜色
    uniqueKeys.forEach(key => {
      if (!Object.prototype.hasOwnProperty.call(finalMap, key)) {
        const newIndex = _nextFreeIndex(assignedIndices);
        finalMap[key] = newIndex;
        assignedIndices.add(newIndex);
      }
    });

    // 更新全局的 colorIndexMap
    colorIndexMap = finalMap;
    _saveMap();
  }

  function patchIndicesFromServer(shareMeta) {
    if (shareMeta && typeof shareMeta.color_indices === 'object') {
      try {
        Object.entries(shareMeta.color_indices).forEach(([key, value]) => {
          if (Number.isFinite(value)) {
            colorIndexMap[key] = value | 0;
          }
        });
        _saveMap();
      } catch (e) {
        console.error("Failed to patch color indices from server meta", e);
      }
    }
  }

  // --- Model base color support (for radar overview) ---

  const MODEL_COLOR_STORAGE_KEY = 'modelColorIndexMap_v1';
  let modelColorIndexMap = {};

  function _loadModelMap() {
    try {
      return JSON.parse(localStorage.getItem(MODEL_COLOR_STORAGE_KEY) || '{}');
    } catch (e) {
      console.error("Failed to load model color map from localStorage", e);
      return {};
    }
  }

  function _saveModelMap() {
    try {
      localStorage.setItem(MODEL_COLOR_STORAGE_KEY, JSON.stringify(modelColorIndexMap));
    } catch (e) {
      console.error("Failed to save model color map to localStorage", e);
    }
  }

  function getModelBaseColor(modelId, themeOverride) {
    const key = String(modelId);
    if (!Object.prototype.hasOwnProperty.call(modelColorIndexMap, key)) {
      const assignedIndices = new Set(Object.values(modelColorIndexMap));
      modelColorIndexMap[key] = _nextFreeIndex(assignedIndices);
      _saveModelMap();
    }
    const index = modelColorIndexMap[key];
    const theme = themeOverride || _getCurrentTheme();
    const palette = theme === 'dark' ? DARK_BASE_PALETTE : DARK_BASE_PALETTE.map(_darkToLightLinear);
    return palette[index % palette.length];
  }

  function releaseModelColor(modelId) {
    const key = String(modelId);
    if (Object.prototype.hasOwnProperty.call(modelColorIndexMap, key)) {
      delete modelColorIndexMap[key];
      _saveModelMap();
    }
  }

  /**
   * Returns a condition-derived color for a model at a given rank.
   * Rank 1 (best/highest score) → model base color (full saturation).
   * Higher ranks → progressively lower saturation and lightness toward gray.
   * @param {string|number} modelId
   * @param {number} rank - 1 = best, totalRanks = worst
   * @param {number} totalRanks - total number of rank levels (e.g. 6)
   * @param {string} [themeOverride]
   */
  function getDerivedColor(modelId, rank, totalRanks, themeOverride) {
    const base = getModelBaseColor(modelId, themeOverride);
    if (!rank || rank <= 1 || !totalRanks || totalRanks <= 1) return base;
    const t = Math.min(1, (rank - 1) / (totalRanks - 1)); // 0 (best) → 1 (worst)
    const [h, s, l] = _hexToHsl(base);
    const newS = Math.max(0, s * (1 - 0.65 * t));
    const newL = l + (0.5 - l) * 0.35 * t;
    return _hslToHex(h, newS, newL);
  }

  // --- Stable per-curve derived-color slot system ---

  const MODEL_CURVE_SLOT_STORAGE_KEY = 'modelCurveSlotMap_v2';
  let modelCurveSlotMap = {}; // { modelId: { curveKey: slotIndex } }

  /**
   * 6 fixed derived-color slot definitions.
   * Slot numbers are stable identifiers, not ordered by lightness. The assignment order for
   * new curves is determined dynamically by _pickBestFreeSlot (max visual distinction).
   * Each slot uses a fixed absolute lightness target (or null for base lightness) and a
   * saturation multiplier applied to the base HSL saturation.
   */
  const NUM_DERIVED_SLOTS = 6;
  const DERIVED_SLOT_DEFS = [
    { lTarget: null, sMult: 1.00 },  // slot 0: exact base color
    { lTarget: 0.73, sMult: 0.82 },  // slot 1: bright (narrowed from 0.80 to avoid near-white)
    { lTarget: 0.32, sMult: 1.00 },  // slot 2: dark saturated (raised from 0.28 to avoid near-black)
    { lTarget: 0.65, sMult: 0.82 },  // slot 3: medium-bright (radically narrowed from 0.90; sMult raised from 0.40 to preserve hue)
    { lTarget: 0.38, sMult: 1.00 },  // slot 4: medium-dark (radically raised from 0.18 to avoid near-black)
    { lTarget: 0.52, sMult: 0.90 },  // slot 5: medium (adjusted from 0.55)
  ];

  function _loadCurveSlotMap() {
    try {
      return JSON.parse(localStorage.getItem(MODEL_CURVE_SLOT_STORAGE_KEY) || '{}');
    } catch (e) {
      return {};
    }
  }

  function _saveCurveSlotMap() {
    try {
      localStorage.setItem(MODEL_CURVE_SLOT_STORAGE_KEY, JSON.stringify(modelCurveSlotMap));
    } catch (e) {}
  }

  /** Returns the effective lightness for a slot given the base lightness (for slot 0). */
  function _slotLightness(slot, baseL) {
    const def = DERIVED_SLOT_DEFS[slot];
    return def.lTarget !== null ? def.lTarget : baseL;
  }

  /**
   * Derives a hex color from a base hex by applying the slot's lightness and saturation.
   * @param {string} baseHex
   * @param {number} slot - 0 to NUM_DERIVED_SLOTS-1
   */
  function _getDerivedColorBySlot(baseHex, slot) {
    if (slot === 0) return baseHex;
    const [h, s, l] = _hexToHsl(baseHex);
    const def = DERIVED_SLOT_DEFS[slot];
    const newS = Math.max(0, Math.min(1, s * def.sMult));
    const newL = def.lTarget !== null ? def.lTarget : l;
    return _hslToHex(h, newS, newL);
  }

  /**
   * Picks the free slot that maximises the minimum lightness-distance to any active slot.
   * When no active slots exist, prefers slot 0 (base color).
   * If no free slots remain (>NUM_DERIVED_SLOTS active curves), falls back to the slot with
   * the highest minimum-distance among all slots — a graceful-degradation path that should not
   * occur in normal operation since NUM_DERIVED_SLOTS equals the number of radar condition IDs.
   * @param {number[]} freeSlots
   * @param {number[]} activeSlots
   * @param {number} baseL - base color lightness (for slot 0 distance calculation)
   */
  function _pickBestFreeSlot(freeSlots, activeSlots, baseL) {
    if (activeSlots.length === 0) {
      return freeSlots.length > 0 ? (freeSlots.includes(0) ? 0 : freeSlots[0]) : 0;
    }
    const candidates = freeSlots.length > 0
      ? freeSlots
      : Array.from({ length: NUM_DERIVED_SLOTS }, (_, i) => i); // graceful degradation
    const activeLightnesses = activeSlots.map(s => _slotLightness(s, baseL));
    let bestSlot = candidates[0];
    let bestDist = -1;
    for (const slot of candidates) {
      const slotL = _slotLightness(slot, baseL);
      const minDist = Math.min(...activeLightnesses.map(al => Math.abs(slotL - al)));
      if (minDist > bestDist) {
        bestDist = minDist;
        bestSlot = slot;
      }
    }
    return bestSlot;
  }

  /**
   * Syncs the stable slot assignments for all active curves of a model.
   * - Restores a curve's previous slot if it is currently free.
   * - Assigns the best available slot (max visual distinction) to curves with no free prior slot.
   * - Does NOT release slots for inactive curves (memory is preserved for future re-adds).
   * @param {string|number} modelId
   * @param {string[]} activeCurveKeys - all curve keys currently active for this model
   * @param {string} [themeOverride]
   */
  function syncModelCurveSlots(modelId, activeCurveKeys, themeOverride) {
    const mid = String(modelId);
    if (!modelCurveSlotMap[mid]) modelCurveSlotMap[mid] = {};
    const slotMap = modelCurveSlotMap[mid];

    const baseColor = getModelBaseColor(modelId, themeOverride);
    const [, , baseL] = _hexToHsl(baseColor);

    const allSlots = Array.from({ length: NUM_DERIVED_SLOTS }, (_, i) => i);

    // First pass: restore slots for active curves that have a remembered (conflict-free) slot
    const occupiedSlots = new Set();
    const needsSlot = [];

    for (const key of activeCurveKeys) {
      if (Object.prototype.hasOwnProperty.call(slotMap, key)) {
        const assignedSlot = slotMap[key];
        if (!occupiedSlots.has(assignedSlot)) {
          occupiedSlots.add(assignedSlot);
        } else {
          // Conflict: another active key already claimed this slot
          needsSlot.push(key);
        }
      } else {
        needsSlot.push(key);
      }
    }

    // Second pass: assign best available slot to each key that needs one
    for (const key of needsSlot) {
      const freeSlots = allSlots.filter(s => !occupiedSlots.has(s));
      const activeSlotList = Array.from(occupiedSlots);
      const chosenSlot = _pickBestFreeSlot(freeSlots, activeSlotList, baseL);
      slotMap[key] = chosenSlot;
      occupiedSlots.add(chosenSlot);
    }

    _saveCurveSlotMap();
  }

  /**
   * Returns the stable derived color for a specific curve key belonging to a model.
   * The slot must have been assigned by syncModelCurveSlots() beforehand.
   * Falls back to the base color if no slot is recorded.
   * @param {string|number} modelId
   * @param {string} curveKey - e.g. "42_3" (model_id_condition_id)
   * @param {string} [themeOverride]
   */
  function getDerivedColorForCurve(modelId, curveKey, themeOverride) {
    const mid = String(modelId);
    const slotMap = modelCurveSlotMap[mid] || {};
    const slot = Object.prototype.hasOwnProperty.call(slotMap, curveKey)
      ? slotMap[curveKey]
      : 0;
    const base = getModelBaseColor(modelId, themeOverride);
    return _getDerivedColorBySlot(base, slot);
  }

  init();
  modelColorIndexMap = _loadModelMap();
  modelCurveSlotMap = _loadCurveSlotMap();

  return {
    getColor,
    getIndex,
    assignUniqueIndices,
    releaseIndex,
    patchIndicesFromServer,
    getModelBaseColor,
    releaseModelColor,
    getDerivedColor,
    syncModelCurveSlots,
    getDerivedColorForCurve
  };

})();

// Expose ColorManager as a global for legacy scripts that reference it
if (typeof window !== 'undefined') {
  window.ColorManager = ColorManager;
}