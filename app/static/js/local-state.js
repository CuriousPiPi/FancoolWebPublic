/* local-state.js
 * 统一维护前端本地状态（选中条目 / 最近移除 / 颜色索引 / X轴模式 / UI偏好 / 点赞集合+指纹 等）。
 * 所有后端不再记录的“会话/缓存性状态”都迁移到这里。
 */
(function(global){
  const LS_KEYS = {
    SELECTED: 'fc_selected_v2',
    REMOVED:  'fc_removed_v2',
    COLOR_MAP:'colorIndexMap_v1',
    X_AXIS:   'x_axis_type',
    PREFS:    'fc_prefs_v1',
    LIKES:    'fc_likes_v1',
    LIKES_FP: 'fc_likes_fp_v1'
  };

  // ========== 常量 ==========
  const MAX_RECENTLY_REMOVED = 50;
  const DEFAULT_X_AXIS = 'rpm';

  // NEW: 一次性清理旧结构（含 v1 键或对象里出现 res_type/res_loc）
  (function purgeLegacyLocalDataOnce(){
    try {
      const v1Sel = localStorage.getItem('fc_selected_v1');
      const v1Rem = localStorage.getItem('fc_removed_v1');
      let needPurge = !!(v1Sel || v1Rem);
      if (!needPurge) {
        const maybeV2 = localStorage.getItem(LS_KEYS.SELECTED);
        if (maybeV2) {
          try {
            const arr = JSON.parse(maybeV2);
            if (Array.isArray(arr) && arr.some(it => it && (it.res_type || it.res_loc || it.brand || it.model || it.condition))) {
              needPurge = true;
            }
          } catch(_){}
        }
      }
      if (needPurge) {
        localStorage.removeItem('fc_selected_v1');
        localStorage.removeItem('fc_removed_v1');
        localStorage.removeItem(LS_KEYS.SELECTED);
        localStorage.removeItem(LS_KEYS.REMOVED);
        localStorage.removeItem(LS_KEYS.COLOR_MAP);
        // 点赞集合与指纹不清理
        console.warn('[LocalState] Purged legacy local data (v1 strings).');
      }
    } catch(_){}
  })();
  
  function readJSON(k,f){ try{ const r=localStorage.getItem(k); return r?JSON.parse(r):f; }catch{return f;} }
  function writeJSON(k,v){ try{ localStorage.setItem(k, JSON.stringify(v)); }catch{} }

  // ========== 旧有本地状态 ==========
  let selected = readJSON(LS_KEYS.SELECTED, []);   // [{ key, model_id, condition_id }]
  let removed  = readJSON(LS_KEYS.REMOVED,  []);   // [{ key, model_id, condition_id }]
  let colorMap = readJSON(LS_KEYS.COLOR_MAP, {});
  let prefs    = readJSON(LS_KEYS.PREFS,    { legend_hidden_keys:[], pointer:{rpm:null, noise_db:null}});
  let xAxisType= (()=> {
    const v=(localStorage.getItem(LS_KEYS.X_AXIS)||'').trim();
    return (v==='rpm'||v==='noise_db'||v==='noise')?(v==='noise'?'noise_db':v):DEFAULT_X_AXIS;
  })();

  // ========== 点赞集合 + 指纹 ==========
  const persistedLikeArray = readJSON(LS_KEYS.LIKES, []);
  let _liked = new Set(Array.isArray(persistedLikeArray)?persistedLikeArray:[]);
  let _serverFP = null;
  let _serverFPTs = 0;

  const LIKESET_DEFAULT_MAX_AGE_MS = 5 * 60 * 1000;        // 对 /api/like_status 的刷新判定基准
  const SERVER_FP_MAX_AGE_MS = LIKESET_DEFAULT_MAX_AGE_MS; // 持久化 serverFP 的最大有效期（可单独配置）
  const FNV_OFFSET_64 = 0xcbf29ce484222325n;
  const FNV_PRIME_64  = 0x100000001b3n;
  const MASK_64       = 0xffffffffffffffffn;

  let _localFP = { c:0, x:'0000000000000000', s:'0000000000000000' };

  function _hash64(str){
    let h = FNV_OFFSET_64;
    for (let i=0;i<str.length;i++){
      h ^= BigInt(str.charCodeAt(i));
      h = (h * FNV_PRIME_64) & MASK_64;
    }
    return h;
  }
  function _recomputeLocalFP(){
    let c=0; let xor_v=0n; let sum_v=0n;
    _liked.forEach(k=>{
      const hv = _hash64(k);
      xor_v ^= hv;
      sum_v = (sum_v + hv) & MASK_64;
      c++;
    });
    _localFP = {
      c,
      x: xor_v.toString(16).padStart(16,'0'),
      s: sum_v.toString(16).padStart(16,'0')
    };
  }
  _recomputeLocalFP();

  function _persistLikes(){
    writeJSON(LS_KEYS.LIKES, Array.from(_liked));
  }

  // ---- 恢复 serverFP（持久化）----
  (function restoreServerFP(){
    try {
      const raw = localStorage.getItem(LS_KEYS.LIKES_FP);
      if (!raw) return;
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== 'object') return;
      const { fp, ts } = parsed;
      if (!fp || typeof fp !== 'object') return;
      if (!('c' in fp && 'x' in fp && 's' in fp)) return;
      if (typeof ts !== 'number') return;
      if (Date.now() - ts > SERVER_FP_MAX_AGE_MS) return; // 过期丢弃
      _serverFP = { c: fp.c, x: fp.x, s: fp.s };
      _serverFPTs = ts;
      // console.debug('[LocalState.likes] restored serverFP', _serverFP);
    } catch(_){}
  })();

  function persistServerFP(){
    try {
      if (_serverFP){
        localStorage.setItem(LS_KEYS.LIKES_FP, JSON.stringify({ fp: _serverFP, ts: _serverFPTs }));
      } else {
        localStorage.removeItem(LS_KEYS.LIKES_FP);
      }
    } catch(_){}
  }

  const likesAPI = {
    setAll(arr){
      _liked = new Set(Array.isArray(arr)?arr:[]);
      _recomputeLocalFP();
      _persistLikes();
    },
    add(key){
      if (!_liked.has(key)){
        _liked.add(key);
        _recomputeLocalFP();
        _persistLikes();
      }
    },
    remove(key){
      if (_liked.has(key)){
        _liked.delete(key);
        _recomputeLocalFP();
        _persistLikes();
      }
    },
    has: (key)=>_liked.has(key),
    getAll: ()=>Array.from(_liked),
    getCount: ()=>_liked.size,
    getLocalFP: ()=>({ ..._localFP }),
    getServerFP: ()=>(_serverFP ? { ..._serverFP } : null),
    updateServerFP(fp){
      if (fp && typeof fp === 'object' && 'c' in fp && 'x' in fp && 's' in fp){
        _serverFP = { c:fp.c, x:fp.x, s:fp.s };
        _serverFPTs = Date.now();
        persistServerFP();
      }
    },
    compare(){
      const equal = !!(_serverFP &&
        _serverFP.c === _localFP.c &&
        _serverFP.x === _localFP.x &&
        _serverFP.s === _localFP.s);
      return { equal, server: _serverFP ? { ..._serverFP } : null, local:{ ..._localFP } };
    },
    logCompare(){
      try { console.debug('[LocalState.likes] fingerprint compare:', likesAPI.compare()); } catch(_){}
    },
    isSynced(){
      return likesAPI.compare().equal;
    },
    needRefresh(maxAgeMs = LIKESET_DEFAULT_MAX_AGE_MS){
      if (!_serverFP) return true;
      if (!likesAPI.isSynced()) return true;
      if (!_serverFPTs) return true;
      return (Date.now() - _serverFPTs) > maxAgeMs;
    },
    shouldSkipStatus(maxAgeMs = LIKESET_DEFAULT_MAX_AGE_MS){
      return likesAPI.isSynced() && !likesAPI.needRefresh(maxAgeMs);
    }
  };

  // ---- 颜色索引分配（原逻辑不动）----
  function ensureColorIndex(key){
    if (!key) return 0;
    if (Object.prototype.hasOwnProperty.call(colorMap,key)) return colorMap[key]|0;
    const used = new Set(Object.values(colorMap).map(v=>v|0));
    let idx=0; while(used.has(idx)) idx++;
    colorMap[key]=idx;
    persistAll();
    return idx;
  }
  function releaseColorIndex(key){
    if (!key) return;
    if (Object.prototype.hasOwnProperty.call(colorMap,key)){
      delete colorMap[key];
      persistColorMap();
    }
  }

  function reassignUniqueIndices(){
    const counts = new Map();
    selected.forEach(it=>{
      const idx = colorMap[it.key];
      if (Number.isInteger(idx)){
        counts.set(idx,(counts.get(idx)||0)+1);
      }
    });
    const assigned = new Set();
    selected.forEach(it=>{
      const idx = colorMap[it.key];
      if (Number.isInteger(idx) && counts.get(idx)===1){
        assigned.add(idx);
      }
    });
    selected.forEach(it=>{
      const idx = colorMap[it.key];
      if (!Number.isInteger(idx) || counts.get(idx)>1){
        let cur=0; while(assigned.has(cur)) cur++;
        colorMap[it.key]=cur;
        assigned.add(cur);
      }
    });
    persistColorMap();
  }

  // ---- 工具 ----
  function makeKey(m,c){ return `${Number(m)}_${Number(c)}`; }
  function persistAll(){
    writeJSON(LS_KEYS.SELECTED, selected);
    writeJSON(LS_KEYS.REMOVED,  removed);
    writeJSON(LS_KEYS.COLOR_MAP, colorMap);
    writeJSON(LS_KEYS.PREFS,    prefs);
    try{ localStorage.setItem(LS_KEYS.X_AXIS, xAxisType);}catch{}
  }
  function persistColorMap(){ writeJSON(LS_KEYS.COLOR_MAP, colorMap); }
  function persistPrefs(){ writeJSON(LS_KEYS.PREFS, prefs); }

  function dispatchChange(reason, extra){
    window.dispatchEvent(new CustomEvent('localstate:changed',{
      detail:Object.assign({reason, selectedCount:selected.length}, extra||{})
    }));
  }
  function findSelectedIndex(key){ return selected.findIndex(it=>it.key===key); }

  function addOrUpdateRemoved(info){
    if (!info || !info.key) return;
    const now = new Date().toISOString();
    const idx = removed.findIndex(r=>r.key===info.key);
    if (idx >= 0){
      const rec = removed[idx];
      rec.removed_time = now;
      removed.splice(idx,1);
      removed.unshift(rec);
    } else {
      removed.unshift({
        key: info.key,
        model_id: info.model_id,
        condition_id: info.condition_id,
        removed_time: now
      });
    }
    if (removed.length > MAX_RECENTLY_REMOVED){
      removed.length = MAX_RECENTLY_REMOVED;
    }
  }

  // ---- 核心公开 API ----
  function getSelected(){ return selected.slice(); }
  function getRecentlyRemoved(){ return removed.slice(); }
  function getXAxisType(){ return xAxisType; }
  function setXAxisType(t){
    const norm = (t==='noise')?'noise_db':t;
    if (norm!=='rpm' && norm!=='noise_db') return;
    if (xAxisType === norm) return;
    xAxisType = norm;
    try { localStorage.setItem(LS_KEYS.X_AXIS, xAxisType); } catch(_){}
    dispatchChange('x_axis_changed',{ xAxisType });
  }

  function addPairs(pairs){
    if (!Array.isArray(pairs)) return { added:0, skipped:0, addedDetails:[] };
    let added=0, skipped=0;
    const addedDetails=[];
    pairs.forEach(p=>{
      const mid=Number(p.model_id), cid=Number(p.condition_id);
      if (!Number.isFinite(mid)||!Number.isFinite(cid)){ skipped++; return; }
      const key=makeKey(mid,cid);
      if (findSelectedIndex(key)>=0){ skipped++; return; }
      selected.push({ key, model_id: mid, condition_id: cid });
      ensureColorIndex(key);
      added++; addedDetails.push({ key, model_id: mid, condition_id: cid });
    });
    persistAll();
    if (added>0) dispatchChange('add',{ added });
    return { added, skipped, addedDetails };
  }

  function removeKey(key){
      const idx=findSelectedIndex(key);
      if (idx<0) return false;
      const info=selected[idx];
      selected.splice(idx,1);
      addOrUpdateRemoved(info); // 会带上 condition
      persistAll();
      dispatchChange('remove',{ key });
      return true;
    }

  function restoreKey(key){
    const rIdx=removed.findIndex(r=>r.key===key);
    if (rIdx<0) return { ok:false, reason:'not_in_removed' };
    if (findSelectedIndex(key)>=0){
      removed.splice(rIdx,1);
      persistAll();
      dispatchChange('restore_skip',{ key });
      return { ok:false, reason:'already_selected' };
    }
    const rec=removed[rIdx];
    removed.splice(rIdx,1);
    selected.push({ key: rec.key, model_id: rec.model_id, condition_id: rec.condition_id });
    ensureColorIndex(key);
    persistAll();
    dispatchChange('restore',{ key });
    return { ok:true, item:{ key: rec.key, model_id: rec.model_id, condition_id: rec.condition_id } };
  }

  function clearAll(){
    const snapshot = selected.slice();
    snapshot.forEach(it=> addOrUpdateRemoved(it));
    selected=[];
    persistAll();
    dispatchChange('clear_all',{});
  }

  function removeFromRecentlyRemoved(key){
    const idx = removed.findIndex(r=>r.key===key);
    if (idx>=0){
      removed.splice(idx,1);
      persistAll();
      dispatchChange('purge_removed',{ key });
      return true;
    }
    return false;
  }

  function getSelectionPairs(){ return selected.map(s=>({ model_id: s.model_id, condition_id: s.condition_id })); }

  function setLegendHiddenKeys(keys){
    prefs.legend_hidden_keys = Array.isArray(keys)?keys.slice():[];
    persistPrefs();
    dispatchChange('legend_hidden',{});
  }
  function getLegendHiddenKeys(){ return (prefs.legend_hidden_keys||[]).slice(); }

  function setPointer(mode, value){
    if (!prefs.pointer) prefs.pointer = { rpm:null, noise_db:null };
    if (mode!=='rpm' && mode!=='noise_db') return;
    prefs.pointer[mode] = Number.isFinite(value)?value:null;
    persistPrefs();
    dispatchChange('pointer_changed',{ mode, value:prefs.pointer[mode] });
  }
  function getPointer(mode){
    if (!prefs.pointer) return null;
    return prefs.pointer[mode];
  }

  /* 按 (model_id, condition_id) 回填/更新工况文本，并持久化 */
  function patchCondition(mid, cid, conditionText){
    try{
      const key = makeKey(mid, cid);
      let touched = false;
      // 更新已选
      for (let i = 0; i < selected.length; i++){
        if (selected[i] && selected[i].key === key){
          if (!selected[i].condition || selected[i].condition !== conditionText){
            selected[i].condition = conditionText || '';
            touched = true;
          }
        }
      }
      // 更新最近移除
      for (let i = 0; i < removed.length; i++){
        if (removed[i] && removed[i].key === key){
          if (!removed[i].condition || removed[i].condition !== conditionText){
            removed[i].condition = conditionText || '';
            touched = true;
          }
        }
      }
      if (touched){
        persistAll();
        dispatchChange('patch_condition', { key, condition: conditionText || '' });
      }
    } catch(_){}
  }

  // NEW:（保留）条件文本回填 API 仍可用但仅作为显示缓存填充的触发信号
  function patchCondition(mid, cid, _conditionText){ /* v2 最小存储不再写文本，这里留空以兼容旧调用 */ }

  const api = {
    getSelected: ()=>selected.slice(),
    getRecentlyRemoved: ()=> removed.slice(),
    addPairs,
    removeKey,
    restoreKey,
    clearAll,
    removeFromRecentlyRemoved:(key)=>removeFromRecentlyRemoved(key),
    ensureColorIndex,
    getXAxisType: ()=>xAxisType,
    setXAxisType:(t)=>setXAxisType(t),
    setLegendHiddenKeys:(keys)=>setLegendHiddenKeys(keys),
    getLegendHiddenKeys:()=>getLegendHiddenKeys(),
    setPointer:(mode,v)=>setPointer(mode,v),
    getPointer:(mode)=>getPointer(mode),
    getSelectionPairs:()=> getSelectionPairs(),
    persistAll,
    likes: likesAPI,
    patchCondition
  };

  global.LocalState = api;
  dispatchChange('init',{ selectedCount: selected.length, xAxisType });

})(window);