(function(window, document){
  'use strict';

  const has = {
    toast: typeof window.showLoading === 'function'
        && typeof window.hideLoading === 'function'
        && typeof window.showError === 'function'
        && typeof window.showSuccess === 'function'
        && typeof window.showInfo === 'function',
    normalize: typeof window.normalizeApiResponse === 'function',
    cache: !!(window.__APP && window.__APP.cache),
    escapeHtml: typeof window.escapeHtml === 'function',
    formatScenario: typeof window.formatScenario === 'function'
  };

  const $$ = (sel, scope) => (scope||document).querySelector(sel);
  const $$$ = (sel, scope) => Array.from((scope||document).querySelectorAll(sel));
  const i18n = () => window.FcI18n || null;
  const isEn = () => !!(i18n() && i18n().isEnglish && i18n().isEnglish());
  const t = (key, params) => (i18n() && i18n().t ? i18n().t(key, params) : key);
  const pick = (obj, baseKey) => (i18n() && i18n().pickLocalizedField ? i18n().pickLocalizedField(obj, baseKey) : (obj && obj[baseKey]));

  function EH(s){
    if (has.escapeHtml) return window.escapeHtml(s);
    return String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  }
  function FS(rt, rl){
    if (has.formatScenario) return window.formatScenario(rt, rl);
    const rtype = EH(rt || '');
    const raw = rl ?? '';
    const isEmpty = (String(raw).trim() === '' || String(raw).trim() === '无' || String(raw).trim() === 'None');
    return isEmpty ? rtype : `${rtype}(${EH(raw)})`;
  }

  function conditionBaseLabel(item) {
    return pick(item, 'condition_name') || item?.condition_name || '';
  }

  function defaultColorFlagOptions(){
    return [
      { value: 1, text: isEn() ? 'Black' : '黑', cls: 'is-black' },
      { value: 2, text: isEn() ? 'White' : '白', cls: 'is-white' },
      { value: 4, text: 'Noctua', cls: 'is-noctua' },
      { value: 128, text: isEn() ? 'Other' : '其它', cls: 'is-other' }
    ];
  }
  const DEFAULT_CONDITION_DISPLAY_ORDER = [2, 3, 10, 7, 11, 1];
  const _zhNaturalCollator = (typeof Intl !== 'undefined' && typeof Intl.Collator === 'function')
    ? new Intl.Collator('zh-Hans-CN', { numeric: true, sensitivity: 'base' })
    : null;
  let COLOR_FLAG_OPTIONS = defaultColorFlagOptions();

  function naturalCompareText(a, b){
    const left = String(a ?? '').trim();
    const right = String(b ?? '').trim();
    if (_zhNaturalCollator) return _zhNaturalCollator.compare(left, right);
    return left.localeCompare(right, 'zh-Hans-CN', { numeric: true, sensitivity: 'base' });
  }

  function normalizeConditionDisplayOrder(raw){
    if (!Array.isArray(raw)) return DEFAULT_CONDITION_DISPLAY_ORDER.slice();
    const out = [];
    const seen = new Set();
    raw.forEach((v) => {
      const n = Number(v);
      if (!Number.isInteger(n) || n <= 0 || seen.has(n)) return;
      seen.add(n);
      out.push(n);
    });
    return out.length ? out : DEFAULT_CONDITION_DISPLAY_ORDER.slice();
  }

  function getConditionDisplayOrder(){
    return normalizeConditionDisplayOrder(window.APP_CONFIG?.conditionDisplayOrder);
  }

  function orderConditionsForDisplay(conditions){
    const list = Array.isArray(conditions) ? conditions.slice() : [];
    const byCid = new Map();
    list.forEach((it) => {
      const cid = Number(it?.condition_id);
      if (!Number.isInteger(cid) || cid <= 0 || byCid.has(cid)) return;
      byCid.set(cid, { ...it, condition_id: cid });
    });
    const ordered = [];
    const seen = new Set();
    getConditionDisplayOrder().forEach((cid) => {
      const hit = byCid.get(cid);
      if (!hit) return;
      ordered.push(hit);
      seen.add(cid);
    });
    const tail = Array.from(byCid.values())
      .filter((it) => !seen.has(Number(it.condition_id)))
      .sort((a, b) => {
        const byName = naturalCompareText(a?.condition_name_zh, b?.condition_name_zh);
        if (byName !== 0) return byName;
        return Number(a?.condition_id || 0) - Number(b?.condition_id || 0);
      });
    return ordered.concat(tail);
  }

  function getColorDefsByMask(mask){
    const n = Number(mask) || 0;
    return COLOR_FLAG_OPTIONS.filter(it => (n & it.value) === it.value);
  }

  function getColorDefByMask(mask){
    return getColorDefsByMask(mask)[0] || null;
  }

  function renderColorChip(def){
    if (!def) return '';
    return `<span class="fc-search-color-option"><span class="fc-color-swatch ${EH(def.cls)}"></span><span class="fc-search-color-text">${EH(def.text)}</span></span>`;
  }

  let _atLeastOneErrorTs = 0;
  let _suppressAdvancedSearchLogOnce = false;
  function showAtLeastOneError(){
    const now = Date.now();
    if (now - _atLeastOneErrorTs < 250) return;
    _atLeastOneErrorTs = now;
    if (has.toast && typeof window.showError === 'function') {
      window.showError(isEn() ? 'Select at least one option' : '至少选一项');
      return;
    }
    if (typeof window.alert === 'function') window.alert(isEn() ? 'Select at least one option' : '至少选一项');
  }

  function _toNumOrNull(v){
    if (v === null || v === undefined || v === '') return null;
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }

  function _hasNonEmptyArray(v){
    return Array.isArray(v) && v.some(it => String(it ?? '').trim() !== '');
  }

  // "默认进阶搜索"：综合模式 + 未设置任何筛选/范围/限制值。
  function _isDefaultAdvancedSearchPayload(payload, form){
    const p = payload || {};
    const compositeMode = !!p.composite_mode;
    const sortBy = String(p.sort_by || '').trim();
    const sortValue = String(p.sort_value || '').trim();
    const colorMask = _toNumOrNull(p.color_mask);
    const rgbMask = _toNumOrNull(p.rgb_mask);
    const priceMin = _toNumOrNull(p.price_min);
    const priceMax = _toNumOrNull(p.price_max);
    const thicknessMin = _toNumOrNull(p.thickness_min);
    const thicknessMax = _toNumOrNull(p.thickness_max);
    const maxSpeedMin = _toNumOrNull(p.max_speed_min);
    const maxSpeedMax = _toNumOrNull(p.max_speed_max);

    return compositeMode
      && !p.condition_id
      && (sortBy === '' || sortBy === 'composite_score')
      && sortValue === ''
      && !_hasNonEmptyArray(p.size_values)
      && !p.rgb_include_none
      && !rgbMask
      && !colorMask
      && !_hasNonEmptyArray(p.other_features)
      && _isFormDefaultInputValue(form, 'price_min', p.price_min)
      && _isFormDefaultInputValue(form, 'price_max', p.price_max)
      && _isFormDefaultInputValue(form, 'thickness_min', p.thickness_min)
      && _isFormDefaultInputValue(form, 'thickness_max', p.thickness_max)
      && _isFormDefaultInputValue(form, 'max_speed_min', p.max_speed_min)
      && _isFormDefaultInputValue(form, 'max_speed_max', p.max_speed_max);
  }

  async function fetchJSON(url, opts){
    const r = await fetch(url, opts);
    const j = await r.json();
    if (has.normalize){
      const n = window.normalizeApiResponse(j);
      return n.ok ? { ok: true, data: n.data } : { ok:false, error: n.error_message || (isEn() ? 'Request failed' : '请求失败') };
    }
    if (j && j.success === true) return { ok:true, data:j.data };
    return { ok:false, error: (j && (j.error_message || j.message)) || (isEn() ? 'Request failed' : '请求失败') };
  }

  let _searchMetadataPromise = null;
  let _latestSearchMetadata = null;
  let _conditionSearchRerender = null;
  let _modelCascadeRerender = null;
  let _pendingConditionSearchLanguageRefresh = false;
  let _pendingModelCascadeLanguageRefresh = false;
  async function fetchSearchMetadata(){
    if (!_searchMetadataPromise) {
      _searchMetadataPromise = (async () => {
        const res = await fetchJSON('/api/search_metadata');
        if (!res.ok) throw new Error(res.error || (isEn() ? 'Failed to load search metadata' : '加载搜索元数据失败'));
        _latestSearchMetadata = res.data || {};
        return _latestSearchMetadata;
      })().catch(err => {
        // Allow retry on transient failures
        _searchMetadataPromise = null;
        throw err;
      });
    }
    return _searchMetadataPromise;
  }
  async function fetchSearchMetadataFresh(){
    _searchMetadataPromise = null;
    return fetchSearchMetadata();
  }

  const Cache = {
    get(ns, payload){ return has.cache ? window.__APP.cache.get(ns, payload) : null; },
    set(ns, payload, value, ttl){ return has.cache ? window.__APP.cache.set(ns, payload, value, ttl) : value; }
  };

  function destroyAndRebuildDropdown(api, build){
    if (api && typeof api.destroy === 'function') api.destroy();
    return typeof build === 'function' ? build() : null;
  }

  function consumePendingLanguageRefresh(flagName, rerender){
    if (!flagName || typeof rerender !== 'function') return;
    const pending = flagName === '_pendingConditionSearchLanguageRefresh'
      ? _pendingConditionSearchLanguageRefresh
      : flagName === '_pendingModelCascadeLanguageRefresh'
        ? _pendingModelCascadeLanguageRefresh
        : false;
    if (!pending) return;
    if (flagName === '_pendingConditionSearchLanguageRefresh') {
      _pendingConditionSearchLanguageRefresh = false;
    } else if (flagName === '_pendingModelCascadeLanguageRefresh') {
      _pendingModelCascadeLanguageRefresh = false;
    }
    if (window.FancoolSearch) window.FancoolSearch[flagName] = false;
    rerender();
  }

function createPortalDropdown(btn, panel, {
  root = btn,         // 用于判定“外点关闭”的根容器（通常是 wrap）
  margin = 6,         // 面板与按钮的间距
  preferredMaxH = 320,// 期望的最大高度
  minMaxH = 120,      // 最小最大高度下限
  getWidth            // 自定义宽度函数 (btnRect)=>number；默认=按钮宽度
} = {}) {
  if (!panel.classList.contains('fc-portal')) panel.classList.add('fc-portal');
  if (panel.parentNode !== document.body) document.body.appendChild(panel);

  let bound = false;
  function place() {
    const r = btn.getBoundingClientRect();
    panel.style.visibility = 'hidden';
    panel.classList.remove('hidden');

    const spaceBelow = window.innerHeight - r.bottom - margin;
    const spaceAbove = r.top - margin;
    const openUp = spaceBelow < 160 && spaceAbove > spaceBelow;
    const maxH = Math.max(minMaxH, Math.min(preferredMaxH, openUp ? spaceAbove : spaceBelow));

    const width = Math.round(typeof getWidth === 'function' ? getWidth(r) : r.width);
    panel.style.minWidth = width + 'px';
    panel.style.width    = width + 'px';
    panel.style.maxHeight = Math.round(maxH) + 'px';

    panel.style.left = Math.round(r.left) + 'px';
    panel.style.top  = openUp
      ? Math.round(r.top - panel.offsetHeight - margin) + 'px'
      : Math.round(r.bottom + margin) + 'px';

    const pr = panel.getBoundingClientRect();
    const overflowRight = pr.right - window.innerWidth;
    if (overflowRight > 0) {
      panel.style.left = Math.round(r.left - overflowRight - 4) + 'px';
    }
    if (pr.left < 0) {
      panel.style.left = '4px';
    }
    panel.style.visibility = '';
  }

  function open() {
    document.querySelectorAll('.fc-custom-options').forEach(p => {
      if (p !== panel) p.classList.add('hidden');
    });
    place();
    btn.setAttribute('aria-expanded', 'true');

    if (!bound) {
      bound = true;
      window.addEventListener('scroll', place, true);
      window.addEventListener('resize', place, { passive: true });
      document.addEventListener('click', onDocClick, true);
    }
  }

  function close() {
    panel.classList.add('hidden');
    btn.setAttribute('aria-expanded', 'false');
    if (bound) {
      bound = false;
      window.removeEventListener('scroll', place, true);
      window.removeEventListener('resize', place);
      document.removeEventListener('click', onDocClick, true);
    }
  }

  function onDocClick(e) {
    const t = e.target;
    if (!t) return;
    if (panel.contains(t)) return;
    if (root && root.contains && root.contains(t)) return;
    close();
  }

  function destroy() {
    close();
    // 不移除节点本身，交由上层决定；仅解绑事件
  }

  return { place, open, close, destroy };
}

// buildCustomSelectFromNative：接入 createPortalDropdown
function buildCustomSelectFromNative(nativeSelect, {
  placeholder = '',
  filter = (opt) => opt.value !== '',
  renderLabel = (opt) => EH(opt?.text || ''),
  renderOption = (opt) => renderLabel(opt)
} = {}) {
  if (!nativeSelect) return { refresh:()=>{}, setDisabled:()=>{}, setValue:()=>{}, getValue:()=>nativeSelect?.value };
  if (nativeSelect._customBuilt && nativeSelect._customApi) return nativeSelect._customApi;
  nativeSelect._customBuilt = true;
  nativeSelect.style.display = 'none';

  const wrap = document.createElement('div');
  wrap.className = 'fc-custom-select';

  const btn = document.createElement('button');
  btn.type = 'button';
  btn.className = 'fc-custom-button fc-field border-gray-300';
  btn.setAttribute('aria-expanded', 'false');
  const syncAriaLabel = () => {
    const fieldLabel = nativeSelect.closest('.fc-form-row')?.querySelector('label')?.textContent?.trim();
    if (fieldLabel) btn.setAttribute('aria-label', isEn() ? `Filter ${fieldLabel}` : `${fieldLabel}筛选`);
  };
  syncAriaLabel();
  btn.innerHTML = `<span class="truncate fc-custom-label">${EH(placeholder)}</span><i class="fa-solid fa-chevron-down ml-2 text-gray-500"></i>`;

  const panel = document.createElement('div');
  panel.className = 'fc-custom-options hidden fc-portal';

  wrap.appendChild(btn);
  nativeSelect.parentNode.insertBefore(wrap, nativeSelect.nextSibling);
  document.body.appendChild(panel);

  let currentPlaceholder = placeholder;

  (function syncStyle(){
    try{
      const cs = getComputedStyle(nativeSelect);
      const br = cs.borderRadius || '.375rem';
      const fs = cs.fontSize || '14px';
      btn.style.borderRadius = br;
      btn.style.fontSize = fs;
      panel.style.borderRadius = br;
      panel.style.fontSize = fs;
    }catch(_){}
  })();

  function setLabelByValue(v){
    const opt = Array.from(nativeSelect.options).find(o => String(o.value) === String(v));
    btn.querySelector('.fc-custom-label').innerHTML = opt ? renderLabel(opt) : EH(currentPlaceholder);
  }
  function renderOptions(){
    const html = Array.from(nativeSelect.options)
      .filter(filter)
      .map(o => `<div class="fc-option" data-value="${EH(o.value)}">${renderOption(o)}</div>`)
      .join('');
    panel.innerHTML = html || `<div class="px-3 py-2 text-gray-500">${EH(isEn() ? 'No options' : '无可选项')}</div>`;
    setLabelByValue(nativeSelect.value);
  }
  const onNativeChange = () => setLabelByValue(nativeSelect.value);
  nativeSelect.addEventListener('change', onNativeChange);
  renderOptions();

  // 使用通用门户控制
  const portal = createPortalDropdown(btn, panel, {
    root: wrap,
    getWidth: (r) => r.width // 保持“面板宽度 = 按钮宽度”的现有行为
  });

  btn.addEventListener('click', () => {
    const isHidden = panel.classList.contains('hidden');
    if (isHidden) portal.open(); else portal.close();
  });
  panel.addEventListener('click', (e) => {
    const node = e.target.closest('.fc-option');
    if (!node) return;
    const v = node.dataset.value || '';
    nativeSelect.value = v;
    nativeSelect.dispatchEvent(new Event('change', { bubbles: true }));
    setLabelByValue(v);
    portal.close();
  });

  const api = {
    refresh(opts = {}){
      if (Object.prototype.hasOwnProperty.call(opts, 'placeholder')) currentPlaceholder = String(opts.placeholder || '');
      syncAriaLabel();
      renderOptions();
    },
    setDisabled(disabled, opts = {}){
      btn.disabled = !!disabled;
      btn.setAttribute('aria-disabled', disabled ? 'true' : 'false');
      if (opts.placeholder) {
        currentPlaceholder = String(opts.placeholder);
        setLabelByValue(nativeSelect.value);
      }
      syncAriaLabel();
      if (disabled) portal.close();
    },
    setPlaceholder(text){
      currentPlaceholder = String(text || placeholder);
      setLabelByValue(nativeSelect.value);
    },
    setValue(v){
      nativeSelect.value = v;
      nativeSelect.dispatchEvent(new Event('change', { bubbles:true }));
      setLabelByValue(v);
      portal.close();
    },
    getValue(){ return nativeSelect.value; },
    destroy(){
      portal.close();
      nativeSelect.removeEventListener('change', onNativeChange);
      wrap.remove();
      panel.remove();
      nativeSelect.style.display = '';
      delete nativeSelect._customBuilt;
      delete nativeSelect._customApi;
    }
  };
  nativeSelect._customApi = api;
  return api;
}

function buildCustomMultiSelectFromNative(nativeSelect, {
  placeholder = '',
  emptyText = '',
  multipleText = '',
  allSelectedIsPlaceholder = true,
  minSelection = 0,
  filter = (opt) => opt.value !== '',
  renderLabel = (opt) => EH(opt?.text || ''),
  renderOption = (opt, state) => renderLabel(opt, state)
} = {}) {
  if (!nativeSelect) {
    return {
      refresh:()=>{}, setDisabled:()=>{}, getSelectedValues:()=>[],
      getSelectedOptions:()=>[], getAllOptions:()=>[]
    };
  }
  if (nativeSelect._multiCustomBuilt && nativeSelect._multiCustomApi) return nativeSelect._multiCustomApi;
  nativeSelect._multiCustomBuilt = true;
  nativeSelect.style.display = 'none';
  let currentPlaceholder = placeholder;
  let currentEmptyText = emptyText;
  let currentMultipleText = multipleText;

  const wrap = document.createElement('div');
  wrap.className = 'fc-custom-select';

  const btn = document.createElement('button');
  btn.type = 'button';
  btn.className = 'fc-custom-button fc-field border-gray-300';
  btn.setAttribute('aria-expanded', 'false');
  const syncAriaLabel = () => {
    const fieldLabel = nativeSelect.closest('.fc-form-row')?.querySelector('label')?.textContent?.trim();
    if (fieldLabel) btn.setAttribute('aria-label', isEn() ? `Filter ${fieldLabel}` : `${fieldLabel}筛选`);
  };
  syncAriaLabel();
  btn.innerHTML = `<span class="truncate fc-custom-label">${EH(currentPlaceholder)}</span><i class="fa-solid fa-chevron-down ml-2 text-gray-500"></i>`;

  const panel = document.createElement('div');
  panel.className = 'fc-custom-options hidden fc-portal';

  wrap.appendChild(btn);
  nativeSelect.parentNode.insertBefore(wrap, nativeSelect.nextSibling);
  document.body.appendChild(panel);

  (function syncStyle(){
    try{
      const cs = getComputedStyle(nativeSelect);
      const br = cs.borderRadius || '.375rem';
      const fs = cs.fontSize || '14px';
      btn.style.borderRadius = br;
      btn.style.fontSize = fs;
      panel.style.borderRadius = br;
      panel.style.fontSize = fs;
    }catch(_){}
  })();

  const getOptions = () => Array.from(nativeSelect.options).filter(filter);
  const getOptionState = (opt) => {
    if (!opt) return 'off';
    if (String(opt.dataset?.state || '').trim() === 'exclude') return 'exclude';
    return opt.selected ? 'include' : 'off';
  };
  const getTriStateIndicatorHtml = (opt, idx) => {
    const state = getOptionState(opt);
    const stateClass = state === 'include'
      ? ' is-include'
      : state === 'exclude'
        ? ' is-exclude'
        : '';
    const stateIcon = state === 'include'
      ? `
        <svg class="fc-option-multi-icon" viewBox="0 0 14 14" aria-hidden="true" focusable="false">
          <path d="M2 8L5.5 11.3L12 2.8"></path>
        </svg>
      `
      : state === 'exclude'
        ? `
          <svg class="fc-option-multi-icon fc-option-multi-icon--exclude" viewBox="0 0 14 14" aria-hidden="true" focusable="false">
            <path d="M3.5 3.5L10.5 10.5M10.5 3.5L3.5 10.5"></path>
          </svg>
        `
        : '';
    return `<span class="fc-option-multi-check${stateClass}" data-index="${idx}" aria-hidden="true">${stateIcon}</span>`;
  };
  const getTriStateAriaLabel = (opt) => {
    const text = String(opt?.text || opt?.value || (isEn() ? 'Option' : '选项')).trim() || (isEn() ? 'Option' : '选项');
    const state = getOptionState(opt);
    if (state === 'include') return isEn() ? `${text}, included` : `${text}，已选中`;
    if (state === 'exclude') return isEn() ? `${text}, excluded` : `${text}，已反选`;
    return isEn() ? `${text}, not selected` : `${text}，未选中`;
  };
  const getActiveOptions = () => getOptions().filter(opt => getOptionState(opt) !== 'off');
  const getSelectedOptions = () => getOptions().filter(o => o.selected);
  const getSelectedValues = () => getActiveOptions()
    .map((opt) => {
      const state = getOptionState(opt);
      if (state === 'exclude') return String(opt.dataset?.excludeValue || '').trim();
      if (state === 'include') return String(opt.value || '').trim();
      return '';
    })
    .filter(Boolean);

  function updateLabel() {
    const all = getOptions();
    const active = getActiveOptions();
    const box = btn.querySelector('.fc-custom-label');
    if (!all.length) { box.innerHTML = EH(currentEmptyText); return; }
    if (active.length === 0) { box.innerHTML = EH(currentEmptyText); return; }
    if (allSelectedIsPlaceholder && active.length === all.length && active.every(opt => getOptionState(opt) === 'include')) {
      box.innerHTML = EH(currentPlaceholder); return;
    }
    if (active.length === 1) {
      const only = active[0];
      const onlyState = getOptionState(only);
      if (onlyState === 'exclude') {
        const excludeLabel = String(only.dataset?.excludeLabel || '').trim();
        if (excludeLabel) {
          box.innerHTML = EH(excludeLabel);
          return;
        }
      }
      box.innerHTML = renderLabel(only, onlyState);
      return;
    }
    box.innerHTML = EH(currentMultipleText);
  }

  function renderOptions() {
    const html = getOptions().map((o, idx) => `
      <label class="fc-option fc-option--multi" data-value="${EH(o.value)}"${String(o.dataset?.excludeValue || '').trim() ? ` aria-label="${EH(getTriStateAriaLabel(o))}"` : ''}>
        ${
          String(o.dataset?.excludeValue || '').trim()
            ? getTriStateIndicatorHtml(o, idx)
            : `<input type="checkbox" class="fc-option-multi-check" data-index="${idx}" aria-label="${EH(o.text || o.value || (isEn() ? 'Option' : '选项'))}" ${o.selected ? 'checked' : ''}>`
        }
        <span class="fc-option-multi-content">${renderOption(o, getOptionState(o))}</span>
      </label>
    `).join('');
    panel.innerHTML = html || `<div class="px-3 py-2 text-gray-500">${EH(isEn() ? 'No options' : '无可选项')}</div>`;
    updateLabel();
  }

  renderOptions();
  const onNativeChange = () => updateLabel();
  nativeSelect.addEventListener('change', onNativeChange);

  const portal = createPortalDropdown(btn, panel, { root: wrap, getWidth: (r) => r.width });

  btn.addEventListener('click', () => {
    const isHidden = panel.classList.contains('hidden');
    if (isHidden) portal.open(); else portal.close();
  });

  panel.addEventListener('click', (e) => {
    const row = e.target.closest('.fc-option--multi');
    if (!row) return;
    const value = String(row.dataset.value || '');
    const opt = getOptions().find(o => String(o.value) === value);
    if (!opt) return;

    const selectedCount = getActiveOptions().length;
    const currentState = getOptionState(opt);
    const hasExcludeState = !!String(opt.dataset?.excludeValue || '').trim();
    let nextState = 'off';
    if (hasExcludeState) {
      nextState = currentState === 'off'
        ? 'include'
        : currentState === 'include'
          ? 'exclude'
          : 'off';
    } else {
      nextState = currentState === 'include' ? 'off' : 'include';
    }
    if (nextState === 'off' && selectedCount <= minSelection) {
      showAtLeastOneError();
      return;
    }

    opt.selected = nextState === 'include';
    if (hasExcludeState) {
      if (nextState === 'exclude') opt.dataset.state = 'exclude';
      else delete opt.dataset.state;
    }
    nativeSelect.dispatchEvent(new Event('change', { bubbles: true }));
    renderOptions();
    portal.place();
  });

  const api = {
    refresh(opts = {}){
      if (Object.prototype.hasOwnProperty.call(opts, 'placeholder')) currentPlaceholder = String(opts.placeholder || '');
      if (Object.prototype.hasOwnProperty.call(opts, 'emptyText')) currentEmptyText = String(opts.emptyText || '');
      if (Object.prototype.hasOwnProperty.call(opts, 'multipleText')) currentMultipleText = String(opts.multipleText || '');
      syncAriaLabel();
      renderOptions();
    },
    setDisabled(disabled){
      btn.disabled = !!disabled;
      btn.setAttribute('aria-disabled', disabled ? 'true' : 'false');
      if (disabled) portal.close();
    },
    destroy(){
      portal.close();
      nativeSelect.removeEventListener('change', onNativeChange);
      wrap.remove();
      panel.remove();
      nativeSelect.style.display = '';
      delete nativeSelect._multiCustomBuilt;
      delete nativeSelect._multiCustomApi;
    },
    getSelectedValues,
    getSelectedOptions,
    getAllOptions: getOptions
  };
  nativeSelect._multiCustomApi = api;
  return api;
}

// buildCustomConditionDropdown：接入 createPortalDropdown
function buildCustomConditionDropdown(sel, items){
  if (!sel) return { setDisabled: ()=>{}, refresh: ()=>{} };
  if (sel._customBuilt && sel._customApi) return sel._customApi;
  sel._customBuilt = true;
  sel.style.display = 'none';
  let itemList = items || [];

  const wrap = document.createElement('div');
  wrap.className = 'fc-custom-select';

  const btn = document.createElement('button');
  btn.type = 'button';
  btn.className = 'fc-custom-button fc-field border-gray-300';
  btn.setAttribute('aria-expanded', 'false');
  btn.innerHTML = `
    <span class="truncate fc-custom-label">${EH(t('home.selectCondition'))}</span>
    <i class="fa-solid fa-chevron-down ml-2 text-gray-500"></i>
  `;

  const panel = document.createElement('div');
  panel.className = 'fc-custom-options hidden fc-portal';

  function renderPanel() {
    panel.innerHTML = itemList.map(it => {
    const value = String(it.condition_id);
    const name = EH(conditionBaseLabel(it) || '');
    const extra = FS(pick(it, 'resistance_type'), pick(it, 'resistance_location'));
    const extraHtml = extra ? `<span class="fc-cond-extra"> - ${extra}</span>` : '';
    return `<div class="fc-option" data-value="${value}">
              <span class="fc-cond-name">${name}</span>${extraHtml}
            </div>`;
    }).join('') || `<div class="px-3 py-2 text-gray-500">${EH(isEn() ? 'No options' : '无可选项')}</div>`;
  }
  renderPanel();

  sel.parentNode.insertBefore(wrap, sel.nextSibling);
  wrap.appendChild(btn);
  document.body.appendChild(panel);

  (function syncStyle(){
    try{
      const cs = getComputedStyle(sel);
      const br = cs.borderRadius || '.375rem';
      const fs = cs.fontSize || '14px';
      btn.style.borderRadius = br;
      btn.style.fontSize = fs;
      panel.style.borderRadius = br;
      panel.style.fontSize = fs;
    }catch(_){}
  })();

  function setButtonLabelByValue(v){
    const rec = itemList.find(x => String(x.condition_id) === String(v));
    const labelBox = btn.querySelector('.fc-custom-label');
    if (!rec) { labelBox.innerHTML = EH(t('home.selectCondition')); return; }
    const name = EH(conditionBaseLabel(rec) || '');
    const extra = FS(pick(rec, 'resistance_type'), pick(rec, 'resistance_location'));
    labelBox.innerHTML = `${name}${extra ? `<span class="fc-cond-extra"> - ${extra}</span>` : ''}`;
  }
  const onSelectChange = () => setButtonLabelByValue(sel.value);
  sel.addEventListener('change', onSelectChange);
  setButtonLabelByValue(sel.value || '');

  // 使用通用门户控制
  const portal = createPortalDropdown(btn, panel, {
    root: wrap,
    getWidth: (r) => r.width
  });

  btn.addEventListener('click', () => {
    const isHidden = panel.classList.contains('hidden');
    if (isHidden) portal.open(); else portal.close();
  });
  panel.addEventListener('click', (e) => {
    const node = e.target.closest('.fc-option');
    if (!node) return;
    const v = node.dataset.value || '';
    sel.value = v;
    sel.dispatchEvent(new Event('change', { bubbles:true }));
    setButtonLabelByValue(v);
    portal.close();
  });

  const api = {
    setDisabled(disabled){
      btn.disabled = !!disabled;
      btn.setAttribute('aria-disabled', disabled ? 'true' : 'false');
      if (disabled) portal.close();
    },
    refresh(nextItems){
      if (Array.isArray(nextItems)) itemList = nextItems;
      renderPanel();
      setButtonLabelByValue(sel.value || '');
    },
    destroy(){
      portal.close();
      sel.removeEventListener('change', onSelectChange);
      wrap.remove();
      panel.remove();
      sel.style.display = '';
      delete sel._customBuilt;
      delete sel._customApi;
    },
  };
  sel._customApi = api;
  return api;
}

  /**
   * Execute a search with the given payload and render the results.
   * Reusable for both initial form submission and profile-switch re-fetches.
   *
   * opts.showToasts  – show loading/success/error toast messages (default: false)
   * opts.switchTab   – navigate to the search-results tab after success (default: false)
   */
  async function _executeSearch(payload, opts) {
    const showToasts = !!(opts && opts.showToasts);
    const switchTab  = !!(opts && opts.switchTab);
    const cacheNS = 'search';

    const doFetch = async () => {
      const resp = await fetch('/api/search_fans', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      const j = await resp.json();
      // Piggyback: check announcement fingerprint from this response
      if (typeof window._checkAnnouncementMeta === 'function' && j && j.meta) {
        window._checkAnnouncementMeta(j.meta);
      }
      if (has.normalize) {
        const n = window.normalizeApiResponse(j);
        if (!n.ok) return { success: false, error_message: n.error_message };
        const d = n.data || {};
        return { success: true, search_results: d.search_results, condition_label: d.condition_label };
      } else {
        if (!j || j.success !== true) return { success: false, error_message: (j && j.error_message) || (isEn() ? 'Search failed' : '搜索失败') };
        const d = j.data || {};
        return { success: true, search_results: d.search_results, condition_label: d.condition_label };
      }
    };

    const renderResults = (data) => {
      if (window.__APP?.modules?.search?.render) {
        window.__APP.modules.search.render(data.search_results, data.condition_label);
      } else if (typeof window.renderSearchResults === 'function') {
        window.renderSearchResults(data.search_results, data.condition_label);
      }
    };

    const switchToSearchTab = () => {
      if (switchTab) {
        document.querySelector(
          '.fc-tabs[data-tab-group="right-panel"] .fc-tabs__item[data-tab="search-results"]'
        )?.click();
      }
    };

    const cached = Cache.get(cacheNS, payload);
    if (cached) {
      renderResults(cached);
      switchToSearchTab();
      showToasts && window.showInfo(isEn() ? 'Loaded cached results...' : '已使用缓存结果...');
      // Async background refresh
      try {
        const fresh = await doFetch();
        if (fresh.success) {
          Cache.set(cacheNS, payload, fresh);
          if (!cached || JSON.stringify(cached.search_results) !== JSON.stringify(fresh.search_results)) {
            renderResults(fresh);
            showToasts && window.showInfo(isEn() ? 'Latest results refreshed' : '已刷新最新结果');
          }
        }
      } catch (_) {}
      return;
    }

    // Cache miss
    showToasts && window.showLoading('op', isEn() ? 'Searching...' : '搜索中...');
    try {
      const data = await doFetch();
      if (!data.success) {
        showToasts && window.hideLoading('op');
        showToasts && window.showError(data.error_message || (isEn() ? 'Search failed' : '搜索失败'));
        return;
      }
      Cache.set(cacheNS, payload, data);
      renderResults(data);
      showToasts && window.hideLoading('op');
      showToasts && window.showSuccess(isEn() ? 'Search completed' : '搜索完成');
      switchToSearchTab();
    } catch (err) {
      showToasts && window.hideLoading('op');
      showToasts && window.showError((isEn() ? 'Search request failed: ' : '搜索异常: ') + err.message);
    }
  }

  function initConditionSearch(){
    const form = $$('#searchForm');
    const sel = $$('#conditionFilterSelect');
    if (!form || !sel) return;

    // Special value for composite (综合评分) mode
    const COMPOSITE_VAL = '__composite__';
    let sizeMultiUi = null;
    let rgbMultiUi = null;
    let colorMultiUi = null;
    let otherFeaturesMultiUi = null;
    let conditionUi = null;
    let finishMetadataReady = () => {};
    const metadataReady = new Promise((resolve) => { finishMetadataReady = resolve; });
    const sortSel = form.querySelector('#sortBySelect');
    let sortSelUi = null;
    let metadataLoading = false;
    let _pendingConditionSearchSnapshot = null;
    let _conditionSearchRerenderSeq = 0;

    function captureAdvancedSearchSnapshot() {
      const currentSize = form.querySelector('select[name="size_values"]');
      const currentRgb = form.querySelector('select[name="rgb_mask"]');
      const currentColor = form.querySelector('select[name="color_mask"]');
      const currentOther = form.querySelector('select[name="other_features"]');
      return {
        language: isEn() ? 'en' : 'zh',
        conditionValue: String(sel.value || COMPOSITE_VAL),
        sortByValue: String(sortSel ? (sortSel.value || 'condition_score') : 'condition_score'),
        isCompositeCondition: String(sel.value || COMPOSITE_VAL) === COMPOSITE_VAL,
        selectedSize: currentSize ? Array.from(currentSize.options).filter(o => o.selected).map(o => o.value) : [],
        selectedRgb: currentRgb ? Array.from(currentRgb.options).filter(o => o.selected).map(o => o.value) : [],
        selectedColor: currentColor ? Array.from(currentColor.options).filter(o => o.selected).map(o => o.value) : [],
        selectedOther: currentOther ? Array.from(currentOther.options).filter(o => o.selected || o.dataset.state === 'exclude').map(o => ({
          value: o.value,
          state: o.dataset.state || 'include',
          excludeValue: o.dataset.excludeValue || '',
          excludeLabel: o.dataset.excludeLabel || '',
        })) : [],
      };
    }

    function createSortSelUi() {
      if (!sortSel) return null;
      return buildCustomSelectFromNative(sortSel, {
        placeholder: t('home.sortBy'),
        renderLabel: (opt) => opt?.value === 'composite_score'
          ? `<i class="fa-solid fa-lock" style="font-size:11px;margin-right:4px;opacity:0.7"></i>${EH(opt?.text || '')}`
          : EH(opt?.text || ''),
        renderOption: (opt) => opt?.value === 'composite_score'
          ? `<i class="fa-solid fa-lock" style="font-size:11px;margin-right:4px;opacity:0.7"></i>${EH(opt?.text || '')}`
          : EH(opt?.text || '')
      });
    }

    function getSortOptions() {
      return {
        condition_score: isEn() ? 'Condition score' : '工况评分',
        noise: isEn() ? 'Same-dBA airflow' : '同分贝风量',
        rpm: isEn() ? 'Same-RPM airflow' : '同转速风量',
        none: isEn() ? 'Max-speed airflow' : '全速风量',
        composite_score: isEn() ? 'Composite score' : '综合评分',
      };
    }

    function conditionOptionLabel(item) {
      const extra = FS(pick(item, 'resistance_type'), pick(item, 'resistance_location'));
      const base = conditionBaseLabel(item);
      return extra ? `${base} - ${extra}` : base;
    }

    function multiUiText() {
      return {
        placeholder: isEn() ? 'Any' : '不限',
        emptyText: t('common.none'),
        multipleText: isEn() ? 'Multiple' : '多个',
      };
    }

    function rebuildMetadataDrivenControls(metadata, snapshot) {
      const data = metadata || _latestSearchMetadata || {};
      _refreshConditionLabelCache(data);
      const state = snapshot || captureAdvancedSearchSnapshot();
      const currentCondition = state.conditionValue || COMPOSITE_VAL;
      const currentSortBy = state.sortByValue || 'condition_score';
      const isCompositeCondition = !!state.isCompositeCondition;
      const selectedSize = Array.isArray(state.selectedSize) ? state.selectedSize : [];
      const selectedRgb = Array.isArray(state.selectedRgb) ? state.selectedRgb : [];
      const selectedColor = Array.isArray(state.selectedColor) ? state.selectedColor : [];
      const selectedOther = Array.isArray(state.selectedOther) ? state.selectedOther : [];

      const conditions = Array.isArray(data.conditions) ? data.conditions : [];
      const sizes = Array.isArray(data.sizes) ? data.sizes : [];
      const rgbFlags = Array.isArray(data.rgb_flags) ? data.rgb_flags : [];
      const colorFlags = Array.isArray(data.color_flags) ? data.color_flags : [];
      const otherFeatures = Array.isArray(data.other_features) ? data.other_features : [];

      sel.innerHTML = `<option value="">${EH(t('home.selectCondition'))}</option>`;
      const oComp = document.createElement('option');
      oComp.value = COMPOSITE_VAL;
      oComp.textContent = t('home.compositePerformance');
      sel.appendChild(oComp);
      conditions.forEach((it) => {
        const o = document.createElement('option');
        o.value = String(it.condition_id);
        o.textContent = conditionOptionLabel(it);
        sel.appendChild(o);
      });
      const compositeItem = {
        condition_id: COMPOSITE_VAL,
        condition_name_zh: '综合性能',
        condition_name_en: 'Composite performance',
        resistance_type_zh: null,
        resistance_type_en: null,
        resistance_location_zh: null,
        resistance_location_en: null,
      };
      sel.value = Array.from(sel.options).some((o) => o.value === currentCondition) ? currentCondition : COMPOSITE_VAL;
      conditionUi = destroyAndRebuildDropdown(conditionUi, null);
      conditionUi = destroyAndRebuildDropdown(conditionUi, () => buildCustomConditionDropdown(sel, [compositeItem, ...conditions]));
      sel.value = Array.from(sel.options).some((o) => o.value === currentCondition) ? currentCondition : COMPOSITE_VAL;
      if (conditionUi && typeof conditionUi.refresh === 'function') conditionUi.refresh([compositeItem, ...conditions]);

      const multiText = multiUiText();

      const sizeSel = form.querySelector('select[name="size_values"]');
      if (sizeSel) {
        sizeSel.innerHTML = '';
        sizes.forEach((it) => {
          const value = String(it?.value ?? '').trim();
          if (!value) return;
          const o = document.createElement('option');
          o.value = value;
          o.textContent = String(it?.label || value);
          o.selected = selectedSize.length ? selectedSize.includes(value) : true;
          sizeSel.appendChild(o);
        });
        sizeMultiUi = destroyAndRebuildDropdown(sizeMultiUi, null);
        sizeMultiUi = destroyAndRebuildDropdown(sizeMultiUi, () => buildCustomMultiSelectFromNative(sizeSel, { ...multiText, minSelection: 1 }));
        if (sizeMultiUi && typeof sizeMultiUi.refresh === 'function') sizeMultiUi.refresh(multiText);
      }

      const rgbSel = form.querySelector('select[name="rgb_mask"]');
      if (rgbSel) {
        rgbSel.innerHTML = '';
        rgbFlags.forEach((rt) => {
          const id = Number(rt?.value);
          if (!Number.isFinite(id) || id < 0) return;
          const o = document.createElement('option');
          o.value = String(id);
          o.textContent = pick(rt, 'label') || rt.label || String(id);
          o.selected = selectedRgb.length ? selectedRgb.includes(String(id)) : true;
          rgbSel.appendChild(o);
        });
        rgbMultiUi = destroyAndRebuildDropdown(rgbMultiUi, null);
        rgbMultiUi = destroyAndRebuildDropdown(rgbMultiUi, () => buildCustomMultiSelectFromNative(rgbSel, { ...multiText, minSelection: 1 }));
        if (rgbMultiUi && typeof rgbMultiUi.refresh === 'function') rgbMultiUi.refresh(multiText);
      }

      const colorSel = form.querySelector('select[name="color_mask"]');
      if (colorSel) {
        colorSel.innerHTML = '';
        COLOR_FLAG_OPTIONS = colorFlags.map((flag) => {
          const value = Number(flag?.value);
          if (!Number.isFinite(value) || value <= 0) return null;
          const defaultDef = defaultColorFlagOptions().find((x) => x.value === value);
          return {
            value,
            text: pick(flag, 'label') || String(value),
            cls: defaultDef?.cls || 'is-other',
          };
        }).filter(Boolean);
        if (!COLOR_FLAG_OPTIONS.length) {
          COLOR_FLAG_OPTIONS = defaultColorFlagOptions();
        }
        COLOR_FLAG_OPTIONS.forEach((def) => {
          const o = document.createElement('option');
          o.value = String(def.value);
          o.textContent = def.text;
          o.selected = selectedColor.length ? selectedColor.includes(String(def.value)) : true;
          colorSel.appendChild(o);
        });
        colorMultiUi = destroyAndRebuildDropdown(colorMultiUi, null);
        colorMultiUi = destroyAndRebuildDropdown(colorMultiUi, () => buildCustomMultiSelectFromNative(colorSel, {
          ...multiText,
          minSelection: 1,
          renderLabel: (opt) => {
            const def = getColorDefByMask(opt?.value);
            return def ? renderColorChip(def) : EH(opt?.text || '');
          },
          renderOption: (opt) => {
            const def = getColorDefByMask(opt?.value);
            return def ? renderColorChip(def) : EH(opt?.text || '');
          }
        }));
        if (colorMultiUi && typeof colorMultiUi.refresh === 'function') colorMultiUi.refresh(multiText);
      }

      const otherSel = form.querySelector('select[name="other_features"]');
      if (otherSel) {
        otherSel.innerHTML = '';
        otherFeatures.forEach((it) => {
          const key = String(it?.key || '').trim();
          if (!key) return;
          const saved = selectedOther.find((row) => row.value === key);
          const o = document.createElement('option');
          o.value = key;
          o.textContent = pick(it, 'label') || key;
          const excludeValue = String(it?.exclude_key || '').trim();
          const excludeLabel = pick(it, 'exclude_label') || '';
          if (excludeValue) o.dataset.excludeValue = excludeValue;
          if (excludeLabel) o.dataset.excludeLabel = excludeLabel;
          o.selected = saved ? saved.state !== 'exclude' : false;
          if (saved && saved.state === 'exclude') o.dataset.state = 'exclude';
          otherSel.appendChild(o);
        });
        otherFeaturesMultiUi = destroyAndRebuildDropdown(otherFeaturesMultiUi, null);
        otherFeaturesMultiUi = destroyAndRebuildDropdown(otherFeaturesMultiUi, () => buildCustomMultiSelectFromNative(otherSel, {
          ...multiText,
          allSelectedIsPlaceholder: false,
          minSelection: 0,
        }));
        if (otherFeaturesMultiUi && typeof otherFeaturesMultiUi.refresh === 'function') otherFeaturesMultiUi.refresh(multiText);
      }

      if (sortSel) {
        const labels = getSortOptions();
        sortSel.innerHTML = '';
        ['condition_score', 'noise', 'rpm', 'none'].forEach((key) => {
          const o = document.createElement('option');
          o.value = key;
          o.textContent = labels[key];
          sortSel.appendChild(o);
        });
        if (isCompositeCondition) {
          const o = document.createElement('option');
          o.value = 'composite_score';
          o.textContent = labels.composite_score;
          sortSel.insertBefore(o, sortSel.options[0] || null);
        }
        if (isCompositeCondition) {
          sortSel.value = 'composite_score';
        } else {
          sortSel.value = Array.from(sortSel.options).some((o) => o.value === currentSortBy && currentSortBy !== 'composite_score')
            ? currentSortBy
            : 'condition_score';
        }
        sortSelUi = destroyAndRebuildDropdown(sortSelUi, null);
        sortSelUi = destroyAndRebuildDropdown(sortSelUi, createSortSelUi);
      }
      sel.dispatchEvent(new Event('change', { bubbles: true }));
      if (sortSelUi) sortSelUi.refresh();
    }

    function refreshAdvancedSearchLanguageWithFreshMetadata(snapshot, opts = {}) {
      const refreshSortUi = typeof opts.refreshSortUi === 'function'
        ? opts.refreshSortUi
        : () => { if (sortSelUi) sortSelUi.refresh(); };
      const rerenderSeq = ++_conditionSearchRerenderSeq;
      fetchSearchMetadataFresh()
        .then((metadata) => {
          if (rerenderSeq !== _conditionSearchRerenderSeq) return;
          rebuildMetadataDrivenControls(metadata, snapshot);
          refreshSortUi();
        })
        .catch(() => {
          if (rerenderSeq !== _conditionSearchRerenderSeq) return;
          if (_latestSearchMetadata) {
            rebuildMetadataDrivenControls(_latestSearchMetadata, snapshot);
          }
          refreshSortUi();
        });
    }

    // 工况和候选项：统一由 /api/search_metadata 一次性初始化
    (async function initSelectsFromMetadata(){
      metadataLoading = true;
      sel.disabled = true;
      sel.innerHTML = `<option value="">${EH(t('home.selectCondition'))}</option>`;
      let loadedMetadata = null;
      try{
        loadedMetadata = await fetchSearchMetadata();
        rebuildMetadataDrivenControls(loadedMetadata);
      }catch(_){
      } finally {
        metadataLoading = false;
        sel.disabled = false;
        conditionUi && conditionUi.setDisabled(false);
        finishMetadataReady();
        if (_pendingConditionSearchLanguageRefresh && _pendingConditionSearchSnapshot) {
          const pendingSnapshot = _pendingConditionSearchSnapshot;
          _pendingConditionSearchSnapshot = null;
          _pendingConditionSearchLanguageRefresh = false;
          if (window.FancoolSearch) window.FancoolSearch._pendingConditionSearchLanguageRefresh = false;
          refreshAdvancedSearchLanguageWithFreshMetadata(pendingSnapshot, {
            refreshSortUi: () => { if (sortSelUi) sortSelUi.refresh(); }
          });
        } else {
          consumePendingLanguageRefresh('_pendingConditionSearchLanguageRefresh', _conditionSearchRerender);
        }
      }
    })();

    if (sortSel) sortSelUi = createSortSelUi();

    // ---- 综合评分 special mode: enter/exit helpers ----
    function enterCompositeMode() {
      if (!sortSel) return;
      // Add 综合评分 option to sortBySelect if not already present
      if (!Array.from(sortSel.options).find(o => o.value === 'composite_score')) {
        const o = document.createElement('option');
        o.value = 'composite_score';
        o.textContent = getSortOptions().composite_score;
        sortSel.insertBefore(o, sortSel.options[0] || null);
      }
      // Set value, fire change (updates label + sortValueInput disabled state), then disable
      sortSel.value = 'composite_score';
      sortSel.dispatchEvent(new Event('change', { bubbles: true }));
      if (sortSelUi) { sortSelUi.refresh(); sortSelUi.setDisabled(true); }
    }

    function exitCompositeMode() {
      if (!sortSel) return;
      // Remove the composite_score option if present
      const oComp = Array.from(sortSel.options).find(o => o.value === 'composite_score');
      if (oComp) sortSel.removeChild(oComp);
      // Restore to default sort and enable
      sortSel.value = 'condition_score';
      sortSel.dispatchEvent(new Event('change', { bubbles: true }));
      if (sortSelUi) { sortSelUi.setDisabled(false); sortSelUi.refresh(); }
    }

    // Listen for condition dropdown changes to toggle composite mode
    sel.addEventListener('change', () => {
      if (sel.value === COMPOSITE_VAL) {
        enterCompositeMode();
      } else {
        // Exit composite mode whenever the composite option/lock state is still present.
        if (sortSel && (sortSel.disabled || Array.from(sortSel.options).some((o) => o.value === 'composite_score'))) {
          exitCompositeMode();
        }
      }
    });

    form.addEventListener('submit', async (e)=>{
      e.preventDefault();
      const cidStr = sel && sel.value ? String(sel.value).trim() : '';
      if (!cidStr){ has.toast && window.showError(isEn() ? 'Please select a test condition' : '请选择测试工况'); return; }
      const isComposite = (cidStr === COMPOSITE_VAL);
      if (!isComposite && (!/^\d+$/.test(cidStr) || Number(cidStr) <= 0)){ has.toast && window.showError(isEn() ? 'Condition options were not initialized correctly. Please refresh the page.' : '工况选项未正确初始化，请刷新页面'); return; }

      const fd = new FormData(form);
      const payload = {}; fd.forEach((v,k)=>payload[k]=v);
      delete payload.condition;
      delete payload.rgb_mask;
      delete payload.rgb_include_none;
      delete payload.color_mask;
      delete payload.other_features;
      delete payload.size_values;
      if (isComposite) {
        payload.composite_mode = true;
        delete payload.condition_id;
        delete payload.sort_by;
        delete payload.sort_value;
      } else {
        payload.condition_id = Number(cidStr);
      }
      const rgbAll = rgbMultiUi ? rgbMultiUi.getAllOptions() : [];
      const rgbSelected = rgbMultiUi ? rgbMultiUi.getSelectedValues() : [];
      if (rgbSelected.length > 0 && rgbSelected.length < rgbAll.length) {
        const rgbParsed = rgbSelected.reduce((accumulator, selectedValue) => {
          const parsedValue = parseInt(selectedValue, 10);
          if (isNaN(parsedValue) || parsedValue < 0) return accumulator;
          if (parsedValue === 0) accumulator.rgbIncludeNone = true;
          else accumulator.rgbMask |= parsedValue;
          return accumulator;
        }, { rgbIncludeNone: false, rgbMask: 0 });
        const rgbIncludeNone = rgbParsed.rgbIncludeNone;
        const rgbMask = rgbParsed.rgbMask;
        if (rgbIncludeNone) payload.rgb_include_none = true;
        if (rgbMask > 0) payload.rgb_mask = rgbMask;
      }

      const sizeAll = sizeMultiUi ? sizeMultiUi.getAllOptions() : [];
      const sizeSelected = sizeMultiUi ? sizeMultiUi.getSelectedValues() : [];
      if (sizeSelected.length > 0 && sizeSelected.length < sizeAll.length) {
        payload.size_values = sizeSelected.map(v => String(v).trim()).filter(Boolean);
      }

      const colorSelectedOpts = colorMultiUi ? colorMultiUi.getSelectedOptions() : [];
      const colorMask = colorSelectedOpts.reduce((sum, opt) => sum | (Number(opt.value) || 0), 0);
      const colorAllMask = (colorMultiUi ? colorMultiUi.getAllOptions() : []).reduce((sum, opt) => sum | (Number(opt.value) || 0), 0);
      if (colorMask > 0 && colorMask !== colorAllMask) payload.color_mask = colorMask;

      const otherSelected = otherFeaturesMultiUi ? otherFeaturesMultiUi.getSelectedValues() : [];
      if (otherSelected.length > 0) {
        payload.other_features = otherSelected.map(v => String(v).trim()).filter(Boolean);
      }

      // Store the normalized search payload for in-page re-submit scenarios.
      _lastSearchBasePayload = Object.assign({}, payload);

      if (_suppressAdvancedSearchLogOnce) {
        _suppressAdvancedSearchLogOnce = false;
      } else {
        window.Analytics?.logAdvancedSearch?.(payload, {
          is_default: _isDefaultAdvancedSearchPayload(payload, form)
        });
      }

      await _executeSearch(payload, { showToasts: has.toast, switchTab: true });
    });

    const runDefaultCompositeSearch = async (opts = {}) => {
      await metadataReady;
      const leftSearchTab = document.querySelector('.fc-tabs[data-tab-group="left-panel"] .fc-tabs__item[data-tab="filter-by-scenario"]');
      leftSearchTab?.click();

      form.reset();

      // Restore defaults for dynamically populated multi-selects (默认=全选“不限”)
      const sizeSel = form.querySelector('select[name="size_values"]');
      if (sizeSel) Array.from(sizeSel.options).forEach(o => { o.selected = true; });
      const rgbSel = form.querySelector('select[name="rgb_mask"]');
      if (rgbSel) Array.from(rgbSel.options).forEach(o => { o.selected = true; });
      const colorSel = form.querySelector('select[name="color_mask"]');
      if (colorSel) Array.from(colorSel.options).forEach(o => { o.selected = true; });
      const otherSel = form.querySelector('select[name="other_features"]');
      if (otherSel) Array.from(otherSel.options).forEach(o => {
        o.selected = false;
        delete o.dataset.state;
      });

      sel.value = COMPOSITE_VAL;
      sel.dispatchEvent(new Event('change', { bubbles: true }));
      sizeMultiUi && sizeMultiUi.refresh();
      rgbMultiUi && rgbMultiUi.refresh();
      colorMultiUi && colorMultiUi.refresh();
      otherFeaturesMultiUi && otherFeaturesMultiUi.refresh();

      if (opts && opts.suppressAdvancedSearchLog) {
        _suppressAdvancedSearchLogOnce = true;
      }

      if (typeof form.requestSubmit === 'function') {
        form.requestSubmit();
      } else {
        form.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));
      }
    };

    _conditionSearchRerender = function () {
      const rerenderSnapshot = captureAdvancedSearchSnapshot();
      const refreshSortUi = () => {
        if (sortSelUi) sortSelUi.refresh();
      };
      if (metadataLoading && !_latestSearchMetadata) {
        _pendingConditionSearchLanguageRefresh = true;
        _pendingConditionSearchSnapshot = rerenderSnapshot;
        if (window.FancoolSearch) window.FancoolSearch._pendingConditionSearchLanguageRefresh = true;
        refreshSortUi();
        return;
      }
      refreshAdvancedSearchLanguageWithFreshMetadata(rerenderSnapshot, { refreshSortUi });
    };
    consumePendingLanguageRefresh('_pendingConditionSearchLanguageRefresh', _conditionSearchRerender);

    window.FancoolSearch = window.FancoolSearch || {};
    window.FancoolSearch.runDefaultCompositeSearch = runDefaultCompositeSearch;
  }

  const CondState = {
    items: [],
    selected: new Set(),
    get allChecked() {
      const selectable = this.items.filter(it => Number(it?.condition_id) > 0);
      return selectable.length > 0 && selectable.every(it => this.selected.has(Number(it.condition_id)));
    },
    clear() { this.items = []; this.selected.clear(); }
  };

  function setCondPlaceholder(text){
    const el = $$('#condPlaceholder'); const list = $$('#conditionRadarChart'); const box = $$('#conditionMulti');
    if (!el || !box) return;
    el.textContent = text || '';
    el.classList.remove('hidden');
    list && list.classList.add('hidden');
  }

  function showCondList(){
    const el = $$('#condPlaceholder'); const list = $$('#conditionRadarChart');
    if (el) el.classList.add('hidden');
    if (list) list.classList.remove('hidden');
  }

  function getCondTitleLabel(){
    const box = $$('#conditionMulti');
    if (!box) return null;
    const row = box.closest('.fc-form-row');
    if (!row) return null;
    const label = row.querySelector('label');
    return label || null;
  }
  // Canonical CCW-from-UL order comes from the backend (injected via APP_CONFIG.radarCids).
  // Slot semantics (6 slots): UL(0) → L(1) → LL(2) → LR(3) → R(4) → UR(5).
  // The LABEL_SLOTS inside renderConditionList are indexed in CW order (UL→UR→R→LR→LL→L),
  // so we derive CW order: CW[j] = CCW[(n-j) % n].
  const _DEFAULT_RADAR_CIDS_CCW = [1, 10, 7, 8, 3, 2];
  const _APP_RADAR_CIDS = window.APP_CONFIG && window.APP_CONFIG.radarCids;
  const _RADAR_CIDS_CCW = Array.isArray(_APP_RADAR_CIDS) && _APP_RADAR_CIDS.length === 6
    ? _APP_RADAR_CIDS.map(Number)
    : (function(){
        if (typeof console !== 'undefined' && console && typeof console.warn === 'function' && _APP_RADAR_CIDS != null) {
          console.warn('[fancool-search] Invalid APP_CONFIG.radarCids length; expected 6 entries for radar rendering. Falling back to default radarCids.', _APP_RADAR_CIDS);
        }
        return _DEFAULT_RADAR_CIDS_CCW;
      })();
  const _N_SEARCH = _RADAR_CIDS_CCW.length;
  const RADAR_CIDS = Array.from({length: _N_SEARCH}, (_, j) => _RADAR_CIDS_CCW[(_N_SEARCH - j) % _N_SEARCH]);
  const _condLabelCache = {}; // conditionId (number) -> condition_name_zh string
  const _radarCache = {};     // model_id (string) -> radar data {conditions, composite_score, updated_at}

  // Last search payload for optional in-page re-submit flows.
  let _lastSearchBasePayload = null;

  // Expose radar cache for cross-module access
  Object.defineProperty(window, '__radarCache', { get: () => _radarCache, configurable: true });
  Object.defineProperty(window, '__condLabelCache', { get: () => _condLabelCache, configurable: true });

  // Single authoritative promise for condition-label readiness.
  // recently-removed.js sets this up early (before fancool.js runs) so that
  // rebuild() can correctly await it even though fancool-search.js loads later.
  // Detect the early-created promise and reuse its resolver; otherwise create fresh.
  const _LS_COND_KEY = 'fc_cond_labels_v2';
  let _condLabelCacheReadyResolve;
  if (window.__condLabelCacheReady instanceof Promise && typeof window.__condLabelCacheReadyResolve === 'function') {
    // Promise was created early by recently-removed.js — reuse its resolver.
    _condLabelCacheReadyResolve = window.__condLabelCacheReadyResolve;
  } else {
    // Fallback: create promise now (e.g., if recently-removed.js was not loaded).
    window.__condLabelCacheReady = new Promise(function(resolve) {
      _condLabelCacheReadyResolve = resolve;
    });
    window.__condLabelCacheReadyResolve = _condLabelCacheReadyResolve;
  }

  function _refreshConditionLabelCache(metadata) {
    const source = metadata || _latestSearchMetadata || {};
    const list = Array.isArray(source.conditions) ? source.conditions : [];
    if (!list.length) return;
    list.forEach((it) => {
      const id = Number(it.condition_id);
      if (!Number.isInteger(id) || id <= 0) return;
      _condLabelCache[id] = conditionBaseLabel(it) || String(id);
    });
  }

  // Pre-populate condition-label cache from localStorage so that the promise can
  // resolve immediately on repeat loads, before the network fetch completes.
  // This ensures browsing-history cards render with correct labels even when the
  // /api/search_metadata fetch is still in flight.
  // Cache key includes a version suffix (_v1); bump the suffix to invalidate all
  // persisted entries when the data schema changes.
  (function _hydrateCondLabelCacheFromStorage() {
    try {
      const stored = localStorage.getItem(_LS_COND_KEY);
      if (!stored) return;
      const parsed = JSON.parse(stored);
      if (!parsed || typeof parsed !== 'object') return;
      let hydrated = false;
      Object.entries(parsed).forEach(function(entry) {
        const cid = Number(entry[0]);
        if (Number.isInteger(cid) && cid > 0 && entry[1]) {
          _condLabelCache[cid] = entry[1];
          hydrated = true;
        }
      });
      if (hydrated) {
        // Resolve the ready-promise immediately with cached labels so that rebuild()
        // — which is already queued and awaiting this promise — can render labels
        // without waiting for the network fetch.
        // The preloadConditionLabels() call below will also call _condLabelCacheReadyResolve
        // after the network fetch, but since Promises can only be resolved once, that
        // subsequent call is a safe no-op.
        _condLabelCacheReadyResolve(Object.assign({}, _condLabelCache));
      }
    } catch (_) {}
  })();

  async function preloadConditionLabels() {
    try {
      const metadata = await fetchSearchMetadata();
      _refreshConditionLabelCache(metadata);
      // Persist fresh labels to localStorage for instant availability on next load.
      try {
        const toStore = {};
        Object.entries(_condLabelCache).forEach(function(e) { toStore[e[0]] = e[1]; });
        localStorage.setItem(_LS_COND_KEY, JSON.stringify(toStore));
      } catch (_) {}
      // Resolve the shared ready-promise so all waiting modules can render labels.
      // If already resolved via localStorage hydration above, this is a no-op.
      // recently-removed.js and fancool.js await this promise directly, so no
      // per-module rebuild notifications are needed here.
      _condLabelCacheReadyResolve(Object.assign({}, _condLabelCache));
      // Notify RadarOverview so the ECharts radar renders proper condition names
      // (not numeric IDs) immediately after labels are loaded, without waiting for
      // the user to add/remove a model.
      if (window.RadarOverview && typeof window.RadarOverview.setConditionLabels === 'function') {
        window.RadarOverview.setConditionLabels(Object.assign({}, _condLabelCache));
      }
    } catch(e) {
      typeof console !== 'undefined' && console.warn('[FancoolSearch] condition label preload failed:', e);
      // Resolve with whatever was cached (possibly empty) so waiting modules unblock.
      _condLabelCacheReadyResolve(Object.assign({}, _condLabelCache));
    }
  }

  /** Convert a radar cache entry's conditions dict to the items array expected by renderConditionList. */
  function radarCacheToItems(radarData) {
    const conditions = (radarData && radarData.conditions) || {};
    return Object.entries(conditions).filter(([cid]) => Number(cid) > 0).map(([cid, sc]) => ({
      condition_id: Number(cid),
      score_total: sc?.score_total ?? null,
    }));
  }

  /** Get the composite score for a radar cache entry. */
  function radarCacheCompositeScore(radarData) {
    if (!radarData) return null;
    return radarData.composite_score !== undefined ? radarData.composite_score : null;
  }

  function renderConditionList(items, compositeScore, modelId){
    // Display-only: show scores without condition selection
    // (Condition selection is now done via the radar overview condition pills)
    CondState.items = (Array.isArray(items) ? items : [])
      .filter(it => Number(it?.condition_id) > 0 && RADAR_CIDS.includes(Number(it.condition_id)));
    CondState.selected.clear();

    const container = $$('#conditionRadarChart');
    if (!container) return;

    // Map condition_id -> item for fast lookup
    const itemMap = {};
    CondState.items.forEach(it => { itemMap[Number(it.condition_id)] = it; });
    if (!RADAR_CIDS.some(cid => cid > 0)) compositeScore = null;

    // Prefer the shared SVG builder; keep the inline fallback for load-order safety.
    if (typeof window.buildMiniRadarSVG === 'function') {
      const svgItems = RADAR_CIDS.map(cid => {
        const it = itemMap[cid];
        return it ? {
          condition_id: cid,
          score_total: it.score_total,
          condition_name: it.condition_name,
          condition_name_zh: it.condition_name_zh,
          condition_name_en: it.condition_name_en,
        } : null;
      }).filter(Boolean);
      const radarHost = document.createElement('div');
      radarHost.className = 'fc-condition-radar-canvas';
      radarHost.innerHTML = window.buildMiniRadarSVG(svgItems, compositeScore, _condLabelCache, 'fc-radar-svg');
      container.innerHTML = '';
      container.appendChild(radarHost);
      const badgeCfg = { modelId, radarItems: svgItems, compositeScore };
      const cacheMeta = modelId != null && window.__modelMetaCache
        ? window.__modelMetaCache[String(modelId)]
        : null;
      if (cacheMeta && typeof cacheMeta === 'object') {
        badgeCfg.whistleValue = cacheMeta.whistle_value;
        badgeCfg.whistleBaselineConditionId = cacheMeta.whistle_baseline_condition_id;
        badgeCfg.whistlePoints = cacheMeta.whistle_points;
      }
      window.attachMiniRadarWhistleBadge(radarHost, badgeCfg, modelId);
      showCondList();
      return;
    }

    // Inline fallback for early-load cases.
    const W = 300, H = 120, cx = 150, cy = 60;
    const gridR = 52;
    const sideR  = gridR + 30;
    const SCORE_LABEL_OFFSET = 12;
    const UNSCORED_INDICATOR_RATIO = 0.06;
    const N = 6;

    function axisAngle(i) { return -Math.PI / 2 - Math.PI / 6 + (i * 2 * Math.PI / N); }
    function vpt(r, i) {
      const a = axisAngle(i);
      return [cx + r * Math.cos(a), cy + r * Math.sin(a)];
    }
    function polyPts(rfn) {
      return RADAR_CIDS.map((cid, i) => {
        const [x, y] = vpt(rfn(cid, i), i);
        return x.toFixed(1) + ',' + y.toFixed(1);
      }).join(' ');
    }

    const rings = [0.25, 0.5, 0.75, 1.0].map(f => polyPts(() => f * gridR));

    const dataPts = polyPts((cid) => {
      const it = itemMap[cid];
      if (!it) return 0;
      if (it.score_total == null) return gridR * UNSCORED_INDICATOR_RATIO;
      return (Math.max(0, Math.min(100, it.score_total)) / 100) * gridR;
    });

    let svg = `<svg id="fc-radar-svg" class="fc-radar-svg" viewBox="0 0 ${W} ${H}" xmlns="http://www.w3.org/2000/svg" aria-label="综合评分雷达图" role="img">`;

    rings.forEach(pts => { svg += `<polygon class="fc-radar-ring" points="${pts}"/>`; });

    RADAR_CIDS.forEach((cid, i) => {
      const [vx, vy] = vpt(gridR, i);
      svg += `<line class="fc-radar-axis" x1="${cx}" y1="${cy}" x2="${vx.toFixed(1)}" y2="${vy.toFixed(1)}"/>`;
    });

    svg += `<polygon class="fc-radar-area" id="fc-radar-area" points="${dataPts}"/>`;

    // Per-vertex: score label only (no clickable elements)
    RADAR_CIDS.forEach((cid, i) => {
      const it = itemMap[cid];
      if (!it) return;
      const score = it.score_total;
      if (score != null) {
        const r = (Math.max(0, Math.min(100, score)) / 100) * gridR;
        const [slx, sly] = vpt(r + SCORE_LABEL_OFFSET, i);
        svg += `<text class="fc-radar-score-lbl" x="${slx.toFixed(1)}" y="${sly.toFixed(1)}" text-anchor="middle" dominant-baseline="middle">${Math.round(score)}</text>`;
      }
    });

    // Per-slot layout table for the 6 condition labels: [lx, ly, anchor]
    const CORNER_LBL_Y_MARGIN = 14;
    const CORNER_LBL_X_OFFSET = Math.round(sideR * 0.55);
    const LABEL_SLOTS = [
      [cx - CORNER_LBL_X_OFFSET, CORNER_LBL_Y_MARGIN,     'end'  ],
      [cx + CORNER_LBL_X_OFFSET, CORNER_LBL_Y_MARGIN,     'start'],
      [cx + sideR,               cy,                       'start'],
      [cx + CORNER_LBL_X_OFFSET, H - CORNER_LBL_Y_MARGIN, 'start'],
      [cx - CORNER_LBL_X_OFFSET, H - CORNER_LBL_Y_MARGIN, 'end'  ],
      [cx - sideR,               cy,                       'end'  ],
    ];
    RADAR_CIDS.forEach((cid, i) => {
      const rawLabel = cid === 0 ? (isEn() ? 'NA' : '无') : _condLabelCache[cid] || conditionBaseLabel(itemMap[cid]) || String(cid);
      const label = EH(rawLabel);
      const available = !!itemMap[cid];
      const [lx, ly, anchor] = LABEL_SLOTS[i];
      if (available) {
        svg += `<text class="fc-radar-lbl" x="${lx.toFixed(1)}" y="${ly.toFixed(1)}" text-anchor="${anchor}" dominant-baseline="middle" style="pointer-events:none">${label}</text>`;
      } else {
        svg += `<text class="fc-radar-lbl unavail${cid === 0 ? ' is-empty' : ''}" data-radar-slot="${i}" data-condition-id="${cid}" x="${lx.toFixed(1)}" y="${ly.toFixed(1)}" text-anchor="${anchor}" dominant-baseline="middle" style="pointer-events:none">${label}</text>`;
      }
    });

    // Center: composite score
    const scoreText = (compositeScore != null) ? String(compositeScore) : (isEn() ? 'All' : '综评');
    svg += `<circle class="fc-radar-center-display" cx="${cx}" cy="${cy}" r="22" aria-label="${EH(t('home.compositePerformance'))}"/>`;
    svg += `<text class="fc-radar-center-lbl fc-radar-center-lbl--bold" x="${cx}" y="${cy}" text-anchor="middle" dominant-baseline="middle">${EH(scoreText)}</text>`;

    svg += `</svg>`;
    container.innerHTML = svg;

    showCondList();
    // No condition count label needed (no selection)
  }

  // 数据获取
  async function fetchBrands(){
    try {
      const data = await fetchSearchMetadata();
      return Array.isArray(data.brands) ? data.brands : [];
    } catch (_) {
      return [];
    }
  }
  function formatBrandZhText(brand){
    const zh = String(pick(brand, 'brand_name') ?? '').trim();
    const legacy = String(brand?.brand_name ?? '').trim();
    return zh || legacy || (isEn() ? 'Unknown brand' : '未知品牌');
  }
  function formatBrandBusinessText(brand){
    const localized = formatBrandZhText(brand);
    const other = String(isEn() ? (brand?.brand_name ?? brand?.brand_name_zh ?? '') : (brand?.brand_name_en ?? '')).trim();
    return (localized && other && localized !== other) ? `${localized} / ${other}` : (localized || other || (isEn() ? 'Unknown brand' : '未知品牌'));
  }
  function formatBrandOptionText(brand){
    const base = formatBrandBusinessText(brand);
    const countNum = Number(brand?.model_count);
    const hasCount = Number.isFinite(countNum) && countNum >= 0;
    if (!hasCount || !base) return base;
    return `${base} (${Math.trunc(countNum)})`;
  }
  async function fetchModelsByBrand(brandId){
    const r = await fetchJSON(`/api/models_by_brand?brand_id=${encodeURIComponent(brandId)}`);
    return r.ok ? (r.data?.items || r.data || []) : [];
  }
  async function fetchConditionsByModel(modelId){
    const r = await fetchJSON(`/api/conditions_by_model?model_id=${encodeURIComponent(modelId)}`);
    return r.ok ? (r.data?.items || r.data || []) : [];
  }
function initModelCascade(){
  const form = $$('#fanForm');
  const brandSelect = $$('#brandSelect');
  const modelSelect = $$('#modelSelect');
  const conditionLoadingEl = $$('#conditionLoading');
  if (!form || !brandSelect || !modelSelect) return;

  const showCondLoading = () => {
    if (!conditionLoadingEl) return;
    conditionLoadingEl.classList.remove('hidden');
    conditionLoadingEl.removeAttribute('aria-hidden');
  };
  const hideCondLoading = () => {
    if (!conditionLoadingEl) return;
    conditionLoadingEl.classList.add('hidden');
    conditionLoadingEl.setAttribute('aria-hidden', 'true');
  };
  hideCondLoading();

  const uiBrand = buildCustomSelectFromNative(brandSelect, { placeholder: t('home.selectBrand') });
  let _brands = [];
  let _currentModels = [];
  let _brandModelsLoading = false;
  let _brandModelsLoadFailed = false;
  let _activeModelConditionStatus = 'idle';
  let _brandModelsRequestSeq = 0;
  let _modelConditionsRequestSeq = 0;

  // Show a shared score badge beside model options when a composite score exists.
  // RGB-capable models also show a colorful RGB tag between the name and score badge.
  function _renderModelOption(opt) {
    const name = EH(opt.dataset.modelName || opt.text || '');
    const rgbHtml = opt.dataset.rgbNamesZh ? '<span class="rpv2-mi-rgb-tag">RGB</span>' : '';
    const score = Number(opt.dataset.score);
    const badgeHtml = Number.isFinite(score) && opt.dataset.score
      ? (() => {
          const styleAttr = window.ScoreBadgeHelper ? window.ScoreBadgeHelper.scoreStyleAttr(score) : '';
          return `<span class="rpv2-score-badge" style="${styleAttr}" data-score="${score}">${Math.round(score)}</span>`;
        })()
      : '';
    return (rgbHtml || badgeHtml)
      ? `<span class="fc-model-option-with-badge">${name}${rgbHtml}${badgeHtml}</span>`
      : name;
  }
  const uiModel = buildCustomSelectFromNative(modelSelect, {
    placeholder: t('home.selectModel'),
    renderLabel:  _renderModelOption,
    renderOption: _renderModelOption,
  });

  function renderBrandOptions(selectedValue) {
    brandSelect.innerHTML = `<option value="">${EH(t('home.selectBrand'))}</option>` +
      _brands.map(b => `<option value="${EH(b.brand_id)}" data-brand-label="${EH(formatBrandZhText(b))}">${EH(formatBrandOptionText(b))}</option>`).join('');
    if (selectedValue != null && Array.from(brandSelect.options).some((opt) => String(opt.value) === String(selectedValue))) {
      brandSelect.value = String(selectedValue);
    }
    uiBrand.refresh({ placeholder: t('home.selectBrand') });
  }

  function renderModelOptions(selectedValue) {
    const selectedBrand = String(brandSelect.value || '').trim();
    if (!selectedBrand) {
      modelSelect.innerHTML = `<option value="">${EH(t('home.selectBrandFirst'))}</option>`;
      modelSelect.value = '';
      modelSelect.disabled = true;
      uiModel.refresh({ placeholder: t('home.selectBrandFirst') });
      uiModel.setDisabled(true, { placeholder: t('home.selectBrandFirst') });
      return;
    }
    if (_brandModelsLoadFailed && !_currentModels.length) {
      modelSelect.innerHTML = `<option value="">${EH(t('home.selectModel'))}</option>`;
      modelSelect.value = '';
      modelSelect.disabled = true;
      uiModel.refresh({ placeholder: t('home.selectModel') });
      uiModel.setDisabled(true, { placeholder: t('home.selectModel') });
      return;
    }

    modelSelect.innerHTML = `<option value="">${EH(t('home.selectModel'))}</option>`;
    _currentModels.forEach((m) => {
      const modelLabel = String(pick(m, 'model_name') || '').trim();
      const o = document.createElement('option');
      o.value = m.model_id;
      o.dataset.modelName = modelLabel;
      const radarData = m.radar;
      const score = radarData ? radarCacheCompositeScore(radarData) : null;
      o.dataset.score = score != null ? String(score) : '';
      const rgbName = String(pick(m, 'rgb_names') || '').trim();
      if (rgbName && rgbName !== t('common.none') && rgbName !== '无') o.dataset.rgbNamesZh = rgbName;
      o.textContent = modelLabel;
      modelSelect.appendChild(o);
    });
    if (selectedValue != null && Array.from(modelSelect.options).some((opt) => String(opt.value) === String(selectedValue))) {
      modelSelect.value = String(selectedValue);
    } else {
      modelSelect.value = '';
    }
    modelSelect.disabled = false;
    uiModel.refresh({ placeholder: t('home.selectModel') });
    uiModel.setDisabled(false);
  }

  function rerenderCascadeConditionArea(selectedBrand, selectedModel) {
    if (!selectedBrand) {
      setCondPlaceholder('\u00A0' + t('home.selectBrandFirst'));
      hideCondLoading();
      return;
    }
    if (!selectedModel) {
      setCondPlaceholder(t('home.selectModelFirst'));
      hideCondLoading();
      return;
    }
    if (_activeModelConditionStatus === 'loading') {
      setCondPlaceholder(t('common.loading'));
      showCondLoading();
      return;
    }
    hideCondLoading();
    if (_activeModelConditionStatus === 'ready' && Array.isArray(CondState.items)) {
      renderConditionList(CondState.items, radarCacheCompositeScore(_radarCache[selectedModel]), selectedModel);
      return;
    }
    const cached = _radarCache[selectedModel];
    const cachedItems = (cached && cached.conditions) ? radarCacheToItems(cached) : [];
    if (cachedItems.length || (cached && !RADAR_CIDS.some(cid => cid > 0))) {
      renderConditionList(cachedItems, radarCacheCompositeScore(cached), selectedModel);
      return;
    }
    if (_activeModelConditionStatus === 'empty') {
      setCondPlaceholder(isEn() ? 'No conditions are available for this model' : '该型号暂无工况');
      return;
    }
    if (_activeModelConditionStatus === 'error') {
      setCondPlaceholder(isEn() ? 'Failed to load. Please retry.' : '加载失败，请重试');
      return;
    }
    setCondPlaceholder(t('home.selectModelFirst'));
  }

  _modelCascadeRerender = function () {
    const selectedBrand = String(brandSelect.value || '').trim();
    let selectedModel = String(modelSelect.value || '').trim();
    renderBrandOptions(selectedBrand);
    if (_brandModelsLoading) {
      modelSelect.innerHTML = `<option value="">${EH(t('home.selectModel'))}</option>`;
      modelSelect.value = '';
      modelSelect.disabled = true;
      uiModel.refresh({ placeholder: t('home.selectModel') });
      uiModel.setDisabled(true, { placeholder: t('home.selectModel') });
      selectedModel = '';
    } else {
      renderModelOptions(selectedModel);
      selectedModel = String(modelSelect.value || '').trim();
    }
    rerenderCascadeConditionArea(selectedBrand, selectedModel);
  };
  consumePendingLanguageRefresh('_pendingModelCascadeLanguageRefresh', _modelCascadeRerender);

  (async function initBrands(){
    try {
      _brands = (await fetchBrands()).slice().sort((a, b) => {
        const left = String(pick(a, 'brand_name') || a?.brand_name || a?.brand_id || '');
        const right = String(pick(b, 'brand_name') || b?.brand_name || b?.brand_id || '');
        const byName = naturalCompareText(left, right);
        if (byName !== 0) return byName;
        return Number(a?.brand_id || 0) - Number(b?.brand_id || 0);
      });
      renderBrandOptions(brandSelect.value || '');
      brandSelect.disabled = false;
      uiBrand.refresh(); uiBrand.setDisabled(false);
    } catch(_) {}
    // 型号默认禁用与占位
    modelSelect.innerHTML = `<option value="">${EH(t('home.selectBrandFirst'))}</option>`;
    modelSelect.value = '';
    modelSelect.disabled = true;
    uiModel.refresh();
    uiModel.setDisabled(true, { placeholder: t('home.selectBrandFirst') });
    // 初始化占位，并确保加载层收起
    setCondPlaceholder('\u00A0' + t('home.selectBrandFirst'));
    hideCondLoading();
  })();

  brandSelect.addEventListener('change', async ()=>{
    const bid = brandSelect.value;
    const brandRequestSeq = ++_brandModelsRequestSeq;
    _modelConditionsRequestSeq += 1;
    // 切品牌先收起加载层，避免残留
    hideCondLoading();
    _activeModelConditionStatus = 'idle';

    if (!bid) {
      _currentModels = [];
      _brandModelsLoading = false;
      _brandModelsLoadFailed = false;
      modelSelect.innerHTML = `<option value="">${EH(t('home.selectBrandFirst'))}</option>`;
      modelSelect.value = '';
      modelSelect.disabled = true;
      uiModel.refresh();
      uiModel.setDisabled(true, { placeholder: t('home.selectBrandFirst') });

      CondState.clear();
      setCondPlaceholder(t('home.selectBrandFirst'));
      return;
    }

    // 有品牌但未加载完型号前，先禁用并显示“选择型号”
    _brandModelsLoading = true;
    _brandModelsLoadFailed = false;
    _currentModels = [];
    modelSelect.innerHTML = `<option value="">${EH(t('home.selectModel'))}</option>`;
    modelSelect.value = '';
    modelSelect.disabled = true;
    uiModel.refresh();
    uiModel.setDisabled(true, { placeholder: t('home.selectModel') });

    CondState.clear();
    setCondPlaceholder(t('home.selectModelFirst'));
    hideCondLoading(); // 确保这里也不显示加载层
    try {
      const models = (await fetchModelsByBrand(bid)).slice().sort((a, b) => {
        const byName = naturalCompareText(pick(a, 'model_name'), pick(b, 'model_name'));
        if (byName !== 0) return byName;
        return Number(a?.model_id || 0) - Number(b?.model_id || 0);
      });
      if (brandRequestSeq !== _brandModelsRequestSeq || String(brandSelect.value || '') !== String(bid)) return;
      _currentModels = models;
      _brandModelsLoadFailed = false;
      _currentModels.forEach((m) => {
        if (m.radar) _radarCache[String(m.model_id)] = m.radar;
      });
      renderModelOptions(modelSelect.value || '');
    } catch(_){
      if (brandRequestSeq !== _brandModelsRequestSeq || String(brandSelect.value || '') !== String(bid)) return;
      _brandModelsLoadFailed = true;
      _currentModels = [];
    } finally {
      if (brandRequestSeq !== _brandModelsRequestSeq || String(brandSelect.value || '') !== String(bid)) return;
      _brandModelsLoading = false;
      if (typeof _modelCascadeRerender === 'function') _modelCascadeRerender();
    }
  });

  modelSelect.addEventListener('change', async ()=>{
    const mid = modelSelect.value;
    const modelRequestSeq = ++_modelConditionsRequestSeq;
    CondState.clear();

    if (!mid) {
      _activeModelConditionStatus = 'idle';
      setCondPlaceholder(t('home.selectModelFirst'));
      hideCondLoading();
      return;
    }

    // Use client-side radar cache if available (populated on brand change)
    const cached = _radarCache[String(mid)];
    if (!RADAR_CIDS.some(cid => cid > 0)) {
      _activeModelConditionStatus = 'ready';
      renderConditionList([], null, mid);
      hideCondLoading();
      return;
    }
    if (cached && cached.conditions) {
      const items = radarCacheToItems(cached);
      if (items.length) {
        _activeModelConditionStatus = 'ready';
        renderConditionList(items, radarCacheCompositeScore(cached), mid);
        return;
      }
    }

    _activeModelConditionStatus = 'loading';
    setCondPlaceholder(t('common.loading'));
    showCondLoading();

    try {
      const items = await fetchConditionsByModel(mid);
      if (modelRequestSeq !== _modelConditionsRequestSeq || String(modelSelect.value || '') !== String(mid)) return;
      const compositeScore = (_radarCache[String(mid)])
        ? radarCacheCompositeScore(_radarCache[String(mid)])
        : null;
      if (Array.isArray(items) && items.length) {
        _activeModelConditionStatus = 'ready';
        renderConditionList(items, compositeScore, mid);
      } else {
        _activeModelConditionStatus = 'empty';
        setCondPlaceholder(isEn() ? 'No conditions are available for this model' : '该型号暂无工况');
      }
    } catch(_){
      if (modelRequestSeq !== _modelConditionsRequestSeq || String(modelSelect.value || '') !== String(mid)) return;
      _activeModelConditionStatus = 'error';
      setCondPlaceholder(isEn() ? 'Failed to load. Please retry.' : '加载失败，请重试');
    } finally {
      if (modelRequestSeq !== _modelConditionsRequestSeq || String(modelSelect.value || '') !== String(mid)) return;
      hideCondLoading();
    }
  });

    // 型号关键字搜索
    (function initModelKeywordSearch(){
      const input = $$('#modelSearchInput');
      const popup = $$('#searchSuggestions');
      if (!input || !popup) return;
      let timer = null;

      input.addEventListener('input', ()=>{
        clearTimeout(timer);
        const q = input.value.trim();
        if (q.length < 2){ popup.classList.add('hidden'); return; }
        timer = setTimeout(async ()=>{
          try{
            const res = await fetchJSON(`/api/model_suggest?q=${encodeURIComponent(q)}`);
            const arr = res.ok ? (res.data?.items || []) : [];
            popup.innerHTML='';
            if (!arr.length){ popup.classList.add('hidden'); return; }
            arr.forEach(item=>{
              const div=document.createElement('div');
              div.className='cursor-pointer';
              div.textContent = `${pick(item, 'brand_name') || ''} ${pick(item, 'model_name') || ''}`.trim();
              div.addEventListener('click', async ()=>{
                try {
                  // Use structured data directly — no string parsing needed
                  uiBrand.setValue(item.brand_id);

                  // Wait for model list to populate then set model_id
                  await new Promise((resolve, reject)=>{
                    const deadline = Date.now() + 2000;
                    (function tryPick(){
                      const opts = Array.from(modelSelect.options || []);
                      const hit = opts.find(o => String(o.value) === String(item.model_id));
                      if (hit) { resolve(item.model_id); return; }
                      if (Date.now() > deadline) { reject(new Error(isEn() ? 'Model ID not found' : '未找到型号ID')); return; }
                      setTimeout(tryPick, 60);
                    })();
                  }).then(mid => {
                    uiModel.setValue(mid);
                  });
                  input.value=''; popup.classList.add('hidden');
                } catch(e){
                  has.toast && window.showError(isEn() ? 'Unable to locate that model in the cascade' : '无法定位到该型号（ID 级联）');
                }
              });
              popup.appendChild(div);
            });
            popup.classList.remove('hidden');
          } catch(_){
            popup.classList.add('hidden');
          }
        }, 280);
      });
      document.addEventListener('click', (e)=>{
        if (!input.contains(e.target) && !popup.contains(e.target)) popup.classList.add('hidden');
      });
    })();


    // 提交：添加至雷达对比（不直接加曲线，由 RadarState 驱动）
    form.addEventListener('submit', async (e)=>{
      e.preventDefault();
      const mid = modelSelect.value;
      if (!mid){ has.toast && window.showError(isEn() ? 'Please select a model first' : '请先选择型号'); return; }

      // Get brand/label from the select elements
      const brandOption = brandSelect.options[brandSelect.selectedIndex];
      const brand = brandOption
        ? (brandOption.dataset.brandLabel || brandOption.textContent.trim())
        : '';
      const modelOption = modelSelect.options[modelSelect.selectedIndex];
      const label = (modelOption && modelOption.dataset.modelName)
        ? modelOption.dataset.modelName
        : (modelOption ? modelOption.textContent.trim() : String(mid));

      if (typeof window.addModelToRadar === 'function') {
        const added = await window.addModelToRadar(Number(mid), brand, label, { source: 'model_picker_panel' });
        if (added) {
          has.toast && window.showSuccess(isEn() ? 'Added to radar compare. Choose conditions on the radar chart.' : '已添加至雷达对比，请在雷达图中选择工况');
          window.__APP?.sidebar?.maybeAutoOpenSidebarOnAdd?.();
        }
      } else {
        has.toast && window.showError(isEn() ? 'Radar module is not ready. Please refresh the page.' : '雷达模块未就绪，请刷新页面');
      }
    });
  }

  function _isFormDefaultInputValue(form, name, payloadValue) {
    const current = String(payloadValue ?? '').trim();
    if (current === '') return true;

    const el = form?.querySelector?.(`[name="${name}"]`);
    if (!el) return false;

    const initial = String(el.defaultValue ?? '').trim();
    if (initial === '') return current === '';

    const currentNum = _toNumOrNull(current);
    const initialNum = _toNumOrNull(initial);
    if (currentNum !== null && initialNum !== null) {
      return currentNum === initialNum;
    }

    return current === initial;
  }

  function initAll(){
    preloadConditionLabels();
    initConditionSearch();
    initModelCascade();

  }

  // 自动初始化
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initAll, { once:true });
  } else {
    initAll();
  }

  window.FancoolSearch = window.FancoolSearch || {};
  window.FancoolSearch.init = initAll;
  window.FancoolSearch._pendingConditionSearchLanguageRefresh = _pendingConditionSearchLanguageRefresh;
  window.FancoolSearch._pendingModelCascadeLanguageRefresh = _pendingModelCascadeLanguageRefresh;
  window.FancoolSearch.rerenderLanguage = function () {
    const fancoolSearch = (window.FancoolSearch = window.FancoolSearch || {});
    _refreshConditionLabelCache();
    try {
      const toStore = {};
      Object.entries(_condLabelCache).forEach(([key, value]) => { toStore[key] = value; });
      localStorage.setItem(_LS_COND_KEY, JSON.stringify(toStore));
    } catch (_) {}
    if (window.RadarOverview && typeof window.RadarOverview.setConditionLabels === 'function') {
      window.RadarOverview.setConditionLabels(Object.assign({}, _condLabelCache));
    }
    if (typeof _modelCascadeRerender === 'function') {
      _modelCascadeRerender();
    } else {
      _pendingModelCascadeLanguageRefresh = true;
      fancoolSearch._pendingModelCascadeLanguageRefresh = true;
    }
    if (typeof _conditionSearchRerender === 'function') {
      _conditionSearchRerender();
    } else {
      _pendingConditionSearchLanguageRefresh = true;
      fancoolSearch._pendingConditionSearchLanguageRefresh = true;
    }
    if (typeof window.renderSearchResults === 'function' && Array.isArray(window.SEARCH_RESULTS_RAW)) {
      const currentLabel = sel => sel && sel.selectedOptions && sel.selectedOptions[0]
        ? sel.selectedOptions[0].textContent.trim()
        : '';
      window.renderSearchResults(window.SEARCH_RESULTS_RAW, currentLabel(document.getElementById('conditionFilterSelect')));
    }
    if (window.RightPanelV2 && typeof window.RightPanelV2.rerenderLanguage === 'function') {
      window.RightPanelV2.rerenderLanguage();
    }
    if (window.__APP?.features?.recentlyRemoved?.rebuild) {
      try { window.__APP.features.recentlyRemoved.rebuild(window.LocalState?.getRecentlyRemoved?.() || []); } catch (_) {}
    }
    if (window.RadarOverview && typeof window.RadarOverview.rerenderLanguage === 'function') {
      window.RadarOverview.rerenderLanguage();
    }
  };
  window.FancoolSearch._debug = { CondState };
  document.addEventListener('fc:languagechange', () => {
    if (window.FancoolSearch && typeof window.FancoolSearch.rerenderLanguage === 'function') {
      window.FancoolSearch.rerenderLanguage();
    }
  });
})(window, document);
