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

  init();

  return {
    getColor,
    getIndex,
    assignUniqueIndices,
    releaseIndex,
    patchIndicesFromServer
  };

})();

// Expose ColorManager as a global for legacy scripts that reference it
if (typeof window !== 'undefined') {
  window.ColorManager = ColorManager;
}