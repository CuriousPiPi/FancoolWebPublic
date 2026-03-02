(function(window, document){
  'use strict';

  // 依赖的全局工具（存在即用，无则降级）
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

  function EH(s){
    if (has.escapeHtml) return window.escapeHtml(s);
    return String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  }
  function FS(rt, rl){
    if (has.formatScenario) return window.formatScenario(rt, rl);
    const rtype = EH(rt || '');
    const raw = rl ?? '';
    const isEmpty = (String(raw).trim() === '' || String(raw).trim() === '无');
    return isEmpty ? rtype : `${rtype}(${EH(raw)})`;
  }

  // 通用 fetch JSON + normalize
  async function fetchJSON(url, opts){
    const r = await fetch(url, opts);
    const j = await r.json();
    if (has.normalize){
      const n = window.normalizeApiResponse(j);
      return n.ok ? { ok: true, data: n.data } : { ok:false, error: n.error_message || '请求失败' };
    }
    if (j && j.success === true) return { ok:true, data:j.data };
    return { ok:false, error: (j && (j.error_message || j.message)) || '请求失败' };
  }

  // 统一 cache 调用
  const Cache = {
    get(ns, payload){ return has.cache ? window.__APP.cache.get(ns, payload) : null; },
    set(ns, payload, value, ttl){ return has.cache ? window.__APP.cache.set(ns, payload, value, ttl) : value; }
  };

// NEW: 通用门户下拉控制（定位/开关/事件绑定）
function createPortalDropdown(btn, panel, {
  root = btn,         // 用于判定“外点关闭”的根容器（通常是 wrap）
  margin = 6,         // 面板与按钮的间距
  preferredMaxH = 320,// 期望的最大高度
  minMaxH = 120,      // 最小最大高度下限
  getWidth            // 自定义宽度函数 (btnRect)=>number；默认=按钮宽度
} = {}) {
  // 确保挂到 body + 门户类名
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
    // 收起其它面板
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
  placeholder = '-- 请选择 --',
  filter = (opt) => opt.value !== '',
  renderLabel = (opt) => EH(opt?.text || ''),
  renderOption = (opt) => renderLabel(opt)
} = {}) {
  if (!nativeSelect || nativeSelect._customBuilt) return { refresh:()=>{}, setDisabled:()=>{}, setValue:()=>{}, getValue:()=>nativeSelect?.value };
  nativeSelect._customBuilt = true;
  nativeSelect.style.display = 'none';

  const wrap = document.createElement('div');
  wrap.className = 'fc-custom-select';

  const btn = document.createElement('button');
  btn.type = 'button';
  btn.className = 'fc-custom-button fc-field border-gray-300';
  btn.setAttribute('aria-expanded', 'false');
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
    panel.innerHTML = html || '<div class="px-3 py-2 text-gray-500">无可选项</div>';
    setLabelByValue(nativeSelect.value);
  }
  nativeSelect.addEventListener('change', () => setLabelByValue(nativeSelect.value));
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

  return {
    refresh(){ renderOptions(); },
    setDisabled(disabled, opts = {}){
      btn.disabled = !!disabled;
      btn.setAttribute('aria-disabled', disabled ? 'true' : 'false');
      if (opts.placeholder) {
        currentPlaceholder = String(opts.placeholder);
        setLabelByValue(nativeSelect.value);
      }
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
    getValue(){ return nativeSelect.value; }
  };
}

// buildCustomConditionDropdown：接入 createPortalDropdown
function buildCustomConditionDropdown(sel, items){
  if (!sel || sel._customBuilt) return { setDisabled: ()=>{} };
  sel._customBuilt = true;
  sel.style.display = 'none';

  const wrap = document.createElement('div');
  wrap.className = 'fc-custom-select';

  const btn = document.createElement('button');
  btn.type = 'button';
  btn.className = 'fc-custom-button fc-field border-gray-300';
  btn.setAttribute('aria-expanded', 'false');
  btn.innerHTML = `
    <span class="truncate fc-custom-label">-- 选择测试工况 --</span>
    <i class="fa-solid fa-chevron-down ml-2 text-gray-500"></i>
  `;

  const panel = document.createElement('div');
  panel.className = 'fc-custom-options hidden fc-portal';

  panel.innerHTML = (items||[]).map(it => {
    const value = String(it.condition_id);
    const name = EH(it.condition_name_zh || '');
    const extra = FS(it.resistance_type_zh, it.resistance_location_zh);
    const extraHtml = extra ? `<span class="fc-cond-extra"> - ${extra}</span>` : '';
    return `<div class="fc-option" data-value="${value}">
              <span class="fc-cond-name">${name}</span>${extraHtml}
            </div>`;
  }).join('') || '<div class="px-3 py-2 text-gray-500">无可选项</div>';

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
    const rec = (items||[]).find(x => String(x.condition_id) === String(v));
    const labelBox = btn.querySelector('.fc-custom-label');
    if (!rec) { labelBox.innerHTML = '-- 选择测试工况 --'; return; }
    const name = EH(rec.condition_name_zh || '');
    const extra = FS(rec.resistance_type_zh, rec.resistance_location_zh);
    labelBox.innerHTML = `${name}${extra ? `<span class="fc-cond-extra"> - ${extra}</span>` : ''}`;
  }
  sel.addEventListener('change', () => setButtonLabelByValue(sel.value));
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

  return {
    setDisabled(disabled){
      btn.disabled = !!disabled;
      btn.setAttribute('aria-disabled', disabled ? 'true' : 'false');
      if (disabled) portal.close();
    }
  };
}

  // =============== 模块 A：按工况筛选（Search by Condition） ===============
  function initConditionSearch(){
    const form = $$('#searchForm');
    const sel = $$('#conditionFilterSelect');
    if (!form || !sel) return;

    // 工况下拉：加载 + 自定义
    (async function initSelect(){
      sel.disabled = true;
      sel.innerHTML = '<option value="">-- 选择测试工况 --</option>';
      let ui = null;
      try{
        const r = await fetch('/get_conditions?raw=1');
        const list = await r.json();
        const arr = Array.isArray(list) ? list : [];
        // 同步原生 select（兼容）
        arr.forEach(it=>{
          const o = document.createElement('option');
          o.value = String(it.condition_id);
          const extra = FS(it.resistance_type_zh, it.resistance_location_zh);
          const base = it.condition_name_zh || '';
          o.textContent = extra ? `${base} - ${extra}` : base;
          sel.appendChild(o);
        });
        // 自定义下拉（带灰色后缀）
        ui = buildCustomConditionDropdown(sel, arr);
      }catch(_){
      } finally {
        sel.disabled = false;
        ui && ui.setDisabled(false);
      }
    })();

    // 统一当前页签内其它下拉（尺寸、限制条件）
    const sizeSel = form.querySelector('select[name="size_filter"]');
    if (sizeSel) buildCustomSelectFromNative(sizeSel, { placeholder: '尺寸(mm)' });

    const sortSel = form.querySelector('#sortBySelect');
    if (sortSel) buildCustomSelectFromNative(sortSel, { placeholder: '限制条件' });

    // RGB灯光下拉：从后端加载并构建自定义选择器
    const rgbSel = form.querySelector('select[name="rgb_light"]');
    if (rgbSel) {
      (async function initRgbSelect(){
        try {
          const res = await fetchJSON('/api/rgb_options');
          const items = res.ok ? (res.data?.items || []) : [];
          items.forEach(val => {
            const o = document.createElement('option');
            o.value = val; o.textContent = val;
            rgbSel.appendChild(o);
          });
        } catch(e) { console.warn('[RGB] Failed to load rgb_options:', e); }
        buildCustomSelectFromNative(rgbSel, { placeholder: '不限' });
      })();
    }

    // 提交逻辑（沿用现有缓存与渲染）
    form.addEventListener('submit', async (e)=>{
      e.preventDefault();
      const cidStr = sel && sel.value ? String(sel.value).trim() : '';
      if (!cidStr){ has.toast && window.showError('请选择测试工况'); return; }
      if (!/^\d+$/.test(cidStr)){ has.toast && window.showError('工况选项未正确初始化，请刷新页面'); return; }

      const fd = new FormData(form);
      const payload = {}; fd.forEach((v,k)=>payload[k]=v);
      payload.condition_id = Number(cidStr);
      delete payload.condition;
      delete payload.condition_name;
      if (!payload.rgb_light || payload.rgb_light === '不限') delete payload.rgb_light;

      const cacheNS = 'search';
      const doFetch = async ()=>{
        const resp = await fetch('/api/search_fans', {
          method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)
        });
        const j = await resp.json();
        if (has.normalize){
          const n = window.normalizeApiResponse(j);
          if (!n.ok) return { success:false, error_message: n.error_message };
          const d = n.data || {};
          return { success:true, search_results: d.search_results, condition_label: d.condition_label };
        } else {
          if (!j || j.success !== true) return { success:false, error_message: (j && j.error_message) || '搜索失败' };
          const d = j.data || {};
          return { success:true, search_results: d.search_results, condition_label: d.condition_label };
        }
      };
      const refreshFromServer = async (cached)=>{
        try {
          const fresh = await doFetch();
          if (fresh.success) {
            Cache.set(cacheNS, payload, fresh);
            if (!cached || JSON.stringify(cached.search_results)!==JSON.stringify(fresh.search_results)){
              if (window.__APP?.modules?.search?.render) {
                window.__APP.modules.search.render(fresh.search_results, fresh.condition_label);
              } else if (typeof window.renderSearchResults === 'function') {
                window.renderSearchResults(fresh.search_results, fresh.condition_label);
              }
              has.toast && window.showInfo('已刷新最新结果');
            }
          }
        } catch(_){}
      };

      const cached = Cache.get(cacheNS, payload);
      if (cached){
        if (window.__APP?.modules?.search?.render) window.__APP.modules.search.render(cached.search_results, cached.condition_label);
        else if (typeof window.renderSearchResults === 'function') window.renderSearchResults(cached.search_results, cached.condition_label);
        document.querySelector('.fc-tabs[data-tab-group="right-panel"] .fc-tabs__item[data-tab="search-results"]')?.click();
        has.toast && window.showInfo('已使用缓存结果...');
        refreshFromServer(cached);
      } else {
        has.toast && window.showLoading('op','搜索中...');
        try {
          const data = await doFetch();
          if (!data.success){
            has.toast && window.hideLoading('op');
            has.toast && window.showError(data.error_message||'搜索失败');
            return;
          }
          Cache.set(cacheNS, payload, data);
          if (window.__APP?.modules?.search?.render) window.__APP.modules.search.render(data.search_results, data.condition_label);
          else if (typeof window.renderSearchResults === 'function') window.renderSearchResults(data.search_results, data.condition_label);
          has.toast && window.hideLoading('op');
          has.toast && window.showSuccess('搜索完成');
          document.querySelector('.fc-tabs[data-tab-group="right-panel"] .fc-tabs__item[data-tab="search-results"]')?.click();
        } catch(err){
          has.toast && window.hideLoading('op');
          has.toast && window.showError('搜索异常: '+err.message);
        }
      }
    });
  }

  // =============== 模块 B：按型号添加（Model Cascade + 条件多选） ===============
  const CondState = {
    items: [],
    selected: new Set(),
    get allChecked() { return this.items.length > 0 && this.selected.size === this.items.length; },
    clear() { this.items = []; this.selected.clear(); }
  };

  function setCondPlaceholder(text){
    const el = $$('#condPlaceholder'); const list = $$('#condList'); const box = $$('#conditionMulti');
    if (!el || !box) return;
    el.textContent = text || '';
    el.classList.remove('hidden');
    list && list.classList.add('hidden');
  }

  function showCondList(){
    const el = $$('#condPlaceholder'); const list = $$('#condList');
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
  function updateCondCountLabel(){
    const label = getCondTitleLabel();
    if (!label) return;
    if (!label.dataset.baseLabel){
      // 去掉已有尾部括号
      const base = (label.textContent || '').replace(/\s*\(\d+\)\s*$/, '');
      label.dataset.baseLabel = base;
    }
    const base = label.dataset.baseLabel || '测试工况';
    const count = CondState.selected.size; // “全部”不计入，本状态不包含“全部”
    label.textContent = count > 0 ? `${base} (${count})` : base;
  }

  function renderConditionList(items){
    CondState.items = Array.isArray(items) ? items.slice() : [];
    CondState.selected.clear();
    const listEl = $$('#condList'); if (!listEl) return;

    const renderItems = CondState.items.map(it => {
      const name = EH(it.condition_name_zh || '');
      const extra = FS(it.resistance_type_zh, it.resistance_location_zh);
      const extraHtml = extra ? `<span class="fc-cond-extra"> - ${extra}</span>` : '';
      return { id: String(it.condition_id), html: `<span class="fc-cond-name">${name}</span>${extraHtml}` };
    });

    listEl.innerHTML = `
      <div class="space-y-1">
        <label class="flex items-center gap-2">
          <input type="checkbox" id="cond_all" class="fc-checkbox">
          <span>-- 全部 --</span>
        </label>
        ${renderItems.map(it => `
          <label class="flex items-center gap-2">
            <input type="checkbox" class="cond-item fc-checkbox" data-cond-id="${it.id}">
            <span>${it.html}</span>
          </label>
        `).join('')}
      </div>`;

    const allBox = $$('#cond_all');
    const itemBoxes = $$$('.cond-item', listEl);

    function syncAllFromItems(){
      if (allBox){
        allBox.checked = CondState.allChecked;
        allBox.indeterminate = CondState.selected.size > 0 && !CondState.allChecked;
      }
      updateCondCountLabel();
    }
    function checkAll(val){
      CondState.selected.clear();
      if (val) CondState.items.forEach(it => CondState.selected.add(String(it.condition_id)));
      itemBoxes.forEach(b => b.checked = !!val);
      syncAllFromItems();
    }

    allBox?.addEventListener('change', () => checkAll(allBox.checked));
    itemBoxes.forEach(box => {
      box.addEventListener('change', () => {
        const id = box.dataset.condId || '';
        if (box.checked) CondState.selected.add(id); else CondState.selected.delete(id);
        syncAllFromItems();
      });
    });

    checkAll(false);
    showCondList();
    updateCondCountLabel();
  }

  // 数据获取
  async function fetchBrands(){
    const r = await fetchJSON('/api/brands'); return r.ok ? (r.data?.items || r.data || []) : [];
  }
  async function fetchModelsByBrand(brandId){
    const r = await fetchJSON(`/api/models_by_brand?brand_id=${encodeURIComponent(brandId)}`);
    return r.ok ? (r.data?.items || r.data || []) : [];
  }
  async function fetchConditionsByModel(modelId){
    const r = await fetchJSON(`/api/conditions_by_model?model_id=${encodeURIComponent(modelId)}`);
    return r.ok ? (r.data?.items || r.data || []) : [];
  }
  async function fetchExpandPairsById(modelId, conditionIdOrNull){
    const payload = { mode:'expand', model_id: modelId };
    if (conditionIdOrNull != null) payload.condition_id = conditionIdOrNull;
    const r = await fetchJSON('/api/search_fans', {
      method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)
    });
    if (!r.ok) throw new Error(r.error || 'expand 请求失败');
    const items = (r.data && r.data.items) || [];
    return items.map(it=>({ model_id: it.model_id, condition_id: it.condition_id }));
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
  // 页面初始化即隐藏，防止加载早期露出
  hideCondLoading();

  const uiBrand = buildCustomSelectFromNative(brandSelect, { placeholder:'-- 选择品牌 --' });
  const uiModel = buildCustomSelectFromNative(modelSelect, { placeholder:'-- 选择型号 --' });

  (async function initBrands(){
    try {
      const brands = await fetchBrands();
      brandSelect.innerHTML = '<option value="">-- 选择品牌 --</option>' +
        brands.map(b=>`<option value="${EH(b.brand_id)}">${EH(b.brand_name_zh)}</option>`).join('');
      brandSelect.disabled = false;
      uiBrand.refresh(); uiBrand.setDisabled(false);
    } catch(_) {}
    // 型号默认禁用与占位
    modelSelect.innerHTML = '<option value="">-- 请先选择品牌 --</option>';
    modelSelect.value = '';
    modelSelect.disabled = true;
    uiModel.refresh();
    uiModel.setDisabled(true, { placeholder: '-- 请先选择品牌 --' });
    // 初始化占位，并确保加载层收起
    setCondPlaceholder('\u00A0-- 请先选择品牌 --');
    hideCondLoading();
  })();

  brandSelect.addEventListener('change', async ()=>{
    const bid = brandSelect.value;
    // 切品牌先收起加载层，避免残留
    hideCondLoading();

    if (!bid) {
      modelSelect.innerHTML = '<option value="">-- 请先选择品牌 --</option>';
      modelSelect.value = '';
      modelSelect.disabled = true;
      uiModel.refresh();
      uiModel.setDisabled(true, { placeholder: '-- 请先选择品牌 --' });

      CondState.clear();
      setCondPlaceholder('-- 请先选择品牌 --');
      return;
    }

    // 有品牌但未加载完型号前，先禁用并显示“选择型号”
    modelSelect.innerHTML = '<option value="">-- 选择型号 --</option>';
    modelSelect.value = '';
    modelSelect.disabled = true;
    uiModel.refresh();
    uiModel.setDisabled(true, { placeholder: '-- 选择型号 --' });

    CondState.clear();
    setCondPlaceholder('-- 请先选择型号 --');
    hideCondLoading(); // 确保这里也不显示加载层

    try {
      const models = await fetchModelsByBrand(bid);
      models.forEach(m=>{
        const o=document.createElement('option'); o.value=m.model_id; o.textContent=m.model_name; modelSelect.appendChild(o);
      });
      modelSelect.disabled=false;
      uiModel.refresh();
      uiModel.setDisabled(false);
    } catch(_){}
  });

  modelSelect.addEventListener('change', async ()=>{
    const mid = modelSelect.value;
    CondState.clear();
    updateCondCountLabel();

    if (!mid) {
      setCondPlaceholder('-- 请先选择型号 --');
      hideCondLoading(); // 型号清空时，收起加载层
      return;
    }

    setCondPlaceholder('加载中...');
    showCondLoading(); // 仅在真正请求工况时显示

    try {
      const items = await fetchConditionsByModel(mid);
      if (Array.isArray(items) && items.length) {
        renderConditionList(items);
      } else {
        setCondPlaceholder('该型号暂无工况');
      }
    } catch(_){
      setCondPlaceholder('加载失败，请重试');
    } finally {
      hideCondLoading(); // 无论成功失败，结束时都收起
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
            const r = await fetch(`/search_models/${encodeURIComponent(q)}?raw=1`);
            const list = await r.json();
            const arr = Array.isArray(list) ? list : [];
            popup.innerHTML='';
            if (!arr.length){ popup.classList.add('hidden'); return; }
            arr.forEach(full=>{
              const div=document.createElement('div');
              div.className='cursor-pointer'; div.textContent=full;
              div.addEventListener('click', async ()=>{
                const parts = full.split(' ');
                const brandName = parts[0];
                const modelName = parts.slice(1).join(' ');
                try {
                  const brands = await fetchBrands();
                  const bRow = (brands || []).find(b => String(b.brand_name_zh) === String(brandName));
                  if (!bRow) throw new Error('未找到品牌ID');

                  // 用自定义下拉的 setValue，自动更新标签并派发 change
                  uiBrand.setValue(bRow.brand_id);

                  // 等待型号列表出现（延长至 2500ms）
                  await new Promise((resolve, reject)=>{
                    const deadline = Date.now() + 2000;
                    (function tryPick(){
                      const opts = Array.from(modelSelect.options || []);
                      const hit = opts.find(o => o.textContent === modelName);
                      if (hit) { resolve(hit.value); return; }
                      if (Date.now() > deadline) { reject(new Error('未找到型号ID')); return; }
                      setTimeout(tryPick, 60);
                    })();
                  }).then(mid => {
                    // 同步模型下拉（更新标签并派发 change）
                    uiModel.setValue(mid);
                  });
                  input.value=''; popup.classList.add('hidden');
                } catch(e){
                  has.toast && window.showError('无法定位到该型号（ID 级联）');
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

    // 提交：生成 pairs 并沿用现有添加流程
    form.addEventListener('submit', async (e)=>{
      e.preventDefault();
      const mid = modelSelect.value;
      if (!mid){ has.toast && window.showError('请先选择型号'); return; }

      const useAll = CondState.allChecked;
      const pickedIds = Array.from(CondState.selected);
      if (!useAll && pickedIds.length === 0){
        has.toast && window.showError('请选择测试工况'); return;
      }

      has.toast && window.showLoading('op','解析中...');
      try {
        let pairs = [];
        const allPairs = await fetchExpandPairsById(Number(mid), null);
        if (useAll) {
          pairs = allPairs;
        } else {
          const wanted = new Set(pickedIds.map(String));
          pairs = allPairs.filter(p => wanted.has(String(p.condition_id)));
        }

        if (!pairs.length){ has.toast && window.hideLoading('op'); has.toast && window.showInfo('没有匹配数据'); return; }

        // 先去重并过滤掉已在图表中的条目，后续逻辑统一使用 pairsToAdd
        let pairsToAdd = pairs;
        if (typeof window.computeNewPairsAfterDedup === 'function') {
          const newPairs = window.computeNewPairsAfterDedup(pairs);
          if (newPairs.length === 0){
            has.toast && window.hideLoading('op');
            has.toast && window.showInfo('全部已存在，无新增');
            return;
          }
          pairsToAdd = newPairs;
        }

        // 上限判断也用去重后的数量
        if (typeof window.ensureCanAdd === 'function' && !window.ensureCanAdd((pairsToAdd && pairsToAdd.length) || 1)){
          has.toast && window.hideLoading('op');
          return;
        }

        const addedSummary = window.LocalState.addPairs(pairsToAdd);
        has.toast && window.hideLoading('op');

        if (addedSummary.added>0){
          // 成功提示：若在频谱视图且有重建中的 key，则不弹成功提示
          const addedKeys = (addedSummary.addedDetails || []).map(d => `${d.model_id}_${d.condition_id}`);
          if (typeof window.scheduleConditionalSuccessToast === 'function') {
            window.scheduleConditionalSuccessToast(addedKeys, `新增 ${addedSummary.added} 组`);
          } else {
            has.toast && window.showSuccess(`新增 ${addedSummary.added} 组`);
          }

          if (typeof window.rebuildSelectedFans === 'function') window.rebuildSelectedFans(window.LocalState.getSelected());
          if (typeof window.ensureLikeStatusBatch === 'function')
            window.ensureLikeStatusBatch(addedSummary.addedDetails.map(d => ({ model_id: d.model_id, condition_id: d.condition_id })));
          window.__APP?.features?.recentlyRemoved?.rebuild?.(window.LocalState.getRecentlyRemoved());
          typeof window.syncQuickActionButtons === 'function' && window.syncQuickActionButtons();
          typeof window.applySidebarColors === 'function' && window.applySidebarColors();

          // 按“当前视图”刷新（频谱视图下会拉取 /api/spectrum-models 并渲染频谱）
          if (typeof window.refreshActiveChartFromLocalDebounced === 'function') {
            window.refreshActiveChartFromLocalDebounced(false);
          } else if (typeof window.refreshActiveChartFromLocal === 'function') {
            window.refreshActiveChartFromLocal(false);
          } else if (typeof window.refreshChartFromLocal === 'function') {
            window.refreshChartFromLocal(false);
          }

          window.__APP?.sidebar?.maybeAutoOpenSidebarOnAdd && window.__APP.sidebar.maybeAutoOpenSidebarOnAdd();
        } else {
          has.toast && window.showInfo('全部已存在，无新增');
        }
        if (typeof window.logNewPairs === 'function') {
          Promise.resolve(window.logNewPairs(addedSummary.addedDetails, 'direct')).catch(()=>{});
        }
      } catch(err){
        has.toast && window.hideLoading('op');
        has.toast && window.showError('添加失败: '+(err && err.message ? err.message : err));
      }
    });
  }

  function initAll(){
    initConditionSearch();
    initModelCascade();
  }

  // 自动初始化
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initAll, { once:true });
  } else {
    initAll();
  }

  window.FancoolSearch = {
    init: initAll,
    _debug: { CondState }
  };
})(window, document);