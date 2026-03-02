/* =========================================================
   Right Panel Module （右侧主容器模块）
   依赖（从全局提供）：
   - safeClosest
   - escapeHtml
   - buildQuickBtnHTML
   - syncQuickActionButtons (可选)
   - initSnapTabScrolling（通用 Scroll Snap 初始化）
   ========================================================= */

(function attachRightPanelModule(global){
  const RightPanel = { init };
  global.RightPanel = RightPanel;

  let segQueriesEl = null;
  let segSearchEl  = null;
  let __HOT_COL_LOCKED_W = null;

  const ANIM = { rowMs: 240,
                rowEase: 'ease',
                fadeOutMs: 200,
                fadeInMs: 220,
                cleanupMs: 120,
                guardMs: 200,
                relayNudgeLabelY: 0,
                unlockDelayMs: 120   
                };

 function init() {
    mountRightSubseg();
    initRightPanelSnapTabs();
    initTopQueriesAndLikesExpander();
    initRightPanelResponsiveWrap();
    initMainPanelsAdaptiveStack();
    initRightSubsegDragSwitch();
    // 初始时为热门工况列预留并锁定列宽（等待表格行渲染完成）
    initAndLockHotColWidthsOnce();
    updateRightSubseg('top-queries');
    initRightSegButtonClicks();
    updateQuickBtnCompactMode();
    // 监听窗口 resize，表格/卡片宽度变化时重新判断
    window.addEventListener('resize', () => {
      updateQuickBtnCompactMode();
    });
  }

  // 行级动画锁
  function isRowAnimating(tr){ return !!(tr && tr.dataset && tr.dataset._relay_anim === '1'); }
  function setRowAnimating(tr, on, btn, delayMs = 0){
    if (!tr) return;
    if (on) {
      tr.dataset._relay_anim = '1';
      if (btn) btn.style.pointerEvents = 'none';
    } else {
      const doUnlock = () => {
        delete tr.dataset._relay_anim;
        if (btn) btn.style.pointerEvents = '';
      };
      if (delayMs > 0) setTimeout(doUnlock, delayMs);
      else doUnlock();
    }
  }

  function mountRightSubseg(){
    const rightSubsegContainer = document.getElementById('rightSubsegContainer');
    segQueriesEl = document.querySelector('#top-queries-pane .fc-seg');
    segSearchEl  = document.querySelector('#search-results-pane .fc-seg');
    if (!rightSubsegContainer) return;
    if (segQueriesEl) { segQueriesEl.dataset.paneId = 'top-queries-pane'; rightSubsegContainer.appendChild(segQueriesEl); }
    if (segSearchEl)  { segSearchEl.dataset.paneId  = 'search-results-pane'; rightSubsegContainer.appendChild(segSearchEl); }
  }

  // 简易文本测量器（单例）
  let __measureEl = null;
  function measureTextPx(text, baseEl, fontSizePx = 14, fontWeight = 400){
    if (!__measureEl) {
      __measureEl = document.createElement('span');
      __measureEl.style.position = 'absolute';
      __measureEl.style.left = '-99999px';
      __measureEl.style.top = '-99999px';
      __measureEl.style.visibility = 'hidden';
      __measureEl.style.whiteSpace = 'nowrap';
      document.body.appendChild(__measureEl);
    }
    const cs = baseEl ? getComputedStyle(baseEl) : getComputedStyle(document.body);
    __measureEl.style.fontFamily = cs.fontFamily || 'system-ui,-apple-system,"Segoe UI","Helvetica Neue","Microsoft YaHei",Arial,sans-serif';
    __measureEl.style.fontSize = `${fontSizePx}px`;
    __measureEl.style.fontWeight = String(fontWeight);
    __measureEl.textContent = String(text || '');
    return __measureEl.offsetWidth || 0;
  }

  // 确保表格具有与表头列数一致的 colgroup
  function ensureColgroup(table){
    if (!table) return null;
    let colgroup = table.querySelector('colgroup');
    const thCount = (() => {
      const thead = table.tHead;
      if (thead && thead.rows && thead.rows[0]) return thead.rows[0].children.length || 0;
      const firstRow = table.querySelector('tr');
      return firstRow ? firstRow.children.length : 0;
    })();

    if (!colgroup) {
      colgroup = document.createElement('colgroup');
      for (let i=0;i<thCount;i++){ colgroup.appendChild(document.createElement('col')); }
      table.insertBefore(colgroup, table.firstChild);
      table.dataset.colCount = String(thCount);
    } else if (colgroup.children.length !== thCount) {
      colgroup.innerHTML = '';
      for (let i=0;i<thCount;i++){ colgroup.appendChild(document.createElement('col')); }
      table.dataset.colCount = String(thCount);
    }
    return colgroup;
  }

  // 将第 6 列直接按给定像素锁定（即使表是隐藏的）
  function lockHotColWidth(table, px){
    if (!table || !Number.isFinite(px) || px <= 0) return;
    const colgroup = ensureColgroup(table);
    const card = table.closest('.fc-right-card');
    const hostW = card ? card.getBoundingClientRect().width : window.innerWidth;
    const fracStr = card ? getComputedStyle(card).getPropertyValue('--hot-col-max-frac').trim() : '';
    const frac = Number.isFinite(parseFloat(fracStr)) ? parseFloat(fracStr) : 0.38;
    const maxAllow = Math.max(180, Math.floor(hostW * frac));
    const need = Math.min(Math.round(px), maxAllow);
    table.style.setProperty('--hot-col-min-w', need + 'px');
    const c6 = colgroup && colgroup.children && colgroup.children[6];
    if (c6) c6.style.width = need + 'px';
    table.dataset.hotColLocked = '1';
  }


  // 计算并“锁定”热门工况列宽（仅在初次加载时执行一次）
  function computeAndLockHotColWidth(table){
    try {
      if (!table || table.dataset.hotColLocked === '1') return true;
      const rows = table.querySelectorAll('tbody > tr:not(.fc-subrow)[data-conditions]');
      if (!rows.length) return false; // 没有行，留给观察者重试

      const first = rows[0];
      const td6 = first.children && first.children[6];
      if (!td6) { table.dataset.hotColLocked = '1'; return true; }

      // 计算第 7 列内部起点（优先用真实文本几何，实在不行再回退 padding 公式）
      const td6Rect = td6.getBoundingClientRect();
      const visible = (td6Rect.width || 0) > 0 && td6.offsetParent !== null;
      let anchorInTdX = 0;

      if (visible) {
        // 1) 优先用工况文字本身的 left（有跑马容器时用 .fc-marquee-inner）
        let labelEl = td6.querySelector('.fc-marquee-inner');
        if (!labelEl) {
          // 2) 没有跑马容器时，退回 data-role/js-top-cond 等内容节点
          labelEl = td6.querySelector('[data-role="top-cond"], .js-top-cond, span, div');
        }
        if (labelEl) {
          const lr = labelEl.getBoundingClientRect();
          anchorInTdX = Math.max(0, lr.left - td6Rect.left);
        }
      }

      if (!anchorInTdX) {
        // 3) 兜底：实在找不到 label，就用 padding + 按钮宽 + gap
        const cs6 = getComputedStyle(td6);
        const pl6 = parseFloat(cs6.paddingLeft) || 0;
        const expBtn = td6.querySelector('.fc-row-expander');
        const expW  = expBtn ? expBtn.getBoundingClientRect().width : 22;
        const expMr = expBtn ? parseFloat(getComputedStyle(expBtn).marginRight) || 6 : 6;
        const labelGap = 4;
        anchorInTdX = pl6 + expW + expMr + labelGap;
      }

      // 汇总所有行子工况最长主标签宽度
      let maxLabelW = 0;
      rows.forEach(tr => {
        let conds = [];
        try { conds = JSON.parse(tr.dataset.conditions || '[]') || []; } catch(_) {}
        if (!Array.isArray(conds) || !conds.length) return;
        conds.forEach(c => {
          const txt = String(c.condition_name_zh || '');
          if (!txt) return;
          maxLabelW = Math.max(maxLabelW, measureTextPx(txt, td6, 14, 400));
        });
      });

      const padRight = 12;
      let need = Math.round(anchorInTdX + maxLabelW + padRight);
      const currW = Math.round(td6Rect.width || 0);
      if (currW && need < currW) need = currW;

      // 写入并锁定
      lockHotColWidth(table, need);

      // 更新全局缓存（用于给其它隐藏表复用）
      __HOT_COL_LOCKED_W = Math.max(__HOT_COL_LOCKED_W || 0, need);
      return true;
    } catch(_) {
      return true;
    }
  }

  // 把全局已算出的宽度传播到还没锁定的表（包括隐藏的好评榜）
  function propagateHotColWidthToAllTables(){
    if (!(__HOT_COL_LOCKED_W > 0)) return;
    document.querySelectorAll('.fc-right-card .fc-rank-table').forEach(tbl => {
      if (tbl.dataset.hotColLocked === '1') return;
      lockHotColWidth(tbl, __HOT_COL_LOCKED_W);
    });
  }

  // 初始化并锁定所有相关表格的热门工况列宽（支持延迟渲染）
  function initAndLockHotColWidthsOnce(){
    const tables = Array.from(document.querySelectorAll('.fc-right-card .fc-rank-table'));

    // 先用“可见”的表（通常是查询榜）计算一次
    const visibleTables = tables.filter(t => t.offsetParent !== null);
    let anyLocked = false;
    for (const t of visibleTables) {
      const ok = computeAndLockHotColWidth(t);
      if (ok) anyLocked = true;
    }
    if (anyLocked) {
      propagateHotColWidthToAllTables(); // 立即把查询榜的宽度复用到好评榜
    }

    // 对尚未锁定的表，监听一次数据注入后锁定；若期间已有全局宽度，则直接传播并停止监听
    tables.forEach(table => {
      if (table.dataset.hotColLocked === '1') return;
      const target = table.tBodies && table.tBodies[0] ? table.tBodies[0] : table;
      const mo = new MutationObserver(() => {
        if (__HOT_COL_LOCKED_W > 0) {
          propagateHotColWidthToAllTables();
          mo.disconnect();
          return;
        }
        const ok = computeAndLockHotColWidth(table);
        if (ok) {
          propagateHotColWidthToAllTables();
          mo.disconnect();
        }
      });
      mo.observe(target, { childList: true, subtree: true });
      setTimeout(() => mo.disconnect(), 5000);
    });
  }

  function updateRightSubseg(activeTab){
    if (segQueriesEl) segQueriesEl.style.display = (activeTab === 'top-queries') ? 'inline-flex' : 'none';
    if (segSearchEl)  segSearchEl.style.display  = (activeTab === 'search-results') ? 'inline-flex' : 'none';
  }

  // 右侧 Scroll Snap 初始化（保持不变）
  function initRightPanelSnapTabs(){
    const card = document.querySelector('.fc-right-card');
    if (!card) return;
    const container = card.querySelector('.fc-tab-container');
    const wrapper   = card.querySelector('.fc-tab-wrapper');
    if (!container || !wrapper) return;
    if (!container.id) container.id = 'right-panel-container';
    if (!wrapper.id)   wrapper.id   = 'right-panel-wrapper';
    global.__RIGHT_PANEL_SNAP_ON = true;

    if (typeof initSnapTabScrolling === 'function') {
      initSnapTabScrolling({
        containerId: container.id,
        group: 'right-panel',
        persistKey: null,
        defaultTab: 'top-queries',
        onActiveChange: (tab) => {
          updateRightSubseg(tab);
          if (tab === 'recent-updates') {
            // recent-updates 懒加载，已内聚在 RightPanel
            RightPanel.recentUpdates?.loadIfNeeded?.();
          }
          updateQuickBtnCompactMode();
        },
        clickScrollBehavior: 'smooth'
      });
    }
  }

  function initRightPanelResponsiveWrap(){
    const card = document.querySelector('.fc-right-card');
    if (!card) return;
    const APPLY_W = 520;
    const apply = (w) => { if (w < APPLY_W) card.classList.add('rp-narrow'); else card.classList.remove('rp-narrow'); };
    apply(card.getBoundingClientRect().width);
    if ('ResizeObserver' in window) {
      const ro = new ResizeObserver(entries=>{ for (const entry of entries){ apply(entry.contentRect.width); } });
      ro.observe(card);
    } else {
      window.addEventListener('resize', () => apply(card.getBoundingClientRect().width));
    }
  }

  /* === 根据右侧滚动区域是否有横向滚动条，控制快捷按钮圆形遮罩 === */
  function updateQuickBtnCompactMode() {
    const card = document.querySelector('.fc-right-card');
    if (!card) return;

    // 右侧卡当前激活的 tab 面板（top-queries / search-results / recent-updates 等）
    const activePane = card.querySelector('.fc-rank-panel.active');
    const scroller = activePane?.querySelector('.fc-rank-scroll');
    if (!scroller) {
      card.classList.remove('rp-compact-quick-btns');
      return;
    }

    // 判断是否存在横向滚动：scrollWidth > clientWidth 视为需要紧凑模式
    const hasHScroll = scroller.scrollWidth > scroller.clientWidth + 1; // +1 抵消浮点误差
    if (hasHScroll) card.classList.add('rp-compact-quick-btns');
    else card.classList.remove('rp-compact-quick-btns');
  }

  function initMainPanelsAdaptiveStack(){
    if (global.__MAIN_STACK_BOUND__) return;
    const container = document.getElementById('main-panels');
    if (!container) return;
    const THRESHOLD = 980;
    const apply = (w) => { if (w < THRESHOLD) container.classList.add('fc-force-col'); else container.classList.remove('fc-force-col'); };
    apply(container.getBoundingClientRect().width);
    if ('ResizeObserver' in window) {
      const ro = new ResizeObserver(entries=>{ for (const entry of entries){ apply(entry.contentRect.width); } });
      ro.observe(container);
    } else {
      window.addEventListener('resize', () => apply(container.getBoundingClientRect().width));
    }
    global.__MAIN_STACK_BOUND__ = true;
  }

  function initRightSubsegDragSwitch() {
    const segs = document.querySelectorAll('#rightSubsegContainer .fc-seg');
    if (!segs.length) return;
    segs.forEach(seg => {
      const thumb = seg.querySelector('.fc-seg__thumb');
      const btns = seg.querySelectorAll('.fc-seg__btn');
      if (!thumb || btns.length !== 2) return;
      let dragging = false, startX = 0, basePercent = 0, lastPercent = 0;
      const activeIsRight = () => ((seg.getAttribute('data-active') || '').endsWith('likes-panel') || (seg.getAttribute('data-active') || '').endsWith('search-likes-panel'));
      const pointInThumb = (x,y)=>{ const r=thumb.getBoundingClientRect(); return x>=r.left && x<=r.right && y>=r.top && y<=r.bottom; };
      function start(e){
        const cx=(e.touches?e.touches[0].clientX:e.clientX)||0, cy=(e.touches?e.touches[0].clientY:e.clientY)||0;
        if (!pointInThumb(cx,cy)) return;
        dragging=true; startX=cx; basePercent=activeIsRight()?100:0; lastPercent=basePercent; thumb.style.transition='none';
        if (e.cancelable) e.preventDefault();
      }
      function move(e){
        if (!dragging) return;
        const cx=(e.touches?e.touches[0].clientX:e.clientX)||0, dx=cx-startX, w=thumb.getBoundingClientRect().width||1;
        let p=basePercent+(dx/w)*100; p=Math.max(0,Math.min(100,p)); lastPercent=p; thumb.style.transform=`translateX(${p}%)`;
        if (e.cancelable) e.preventDefault();
      }
      function end(){
        if (!dragging) return; dragging=false;
        const goRight=lastPercent>=50; const targetBtn=goRight?btns[1]:btns[0];
        thumb.style.transition=''; thumb.style.transform=''; targetBtn.click();
      }
      seg.addEventListener('mousedown', start);
      document.addEventListener('mousemove', move, { passive:false });
      document.addEventListener('mouseup', end);
      seg.addEventListener('touchstart', start, { passive:false });
      document.addEventListener('touchmove', move, { passive:false });
      document.addEventListener('touchend', end);
    });
  }

  /* ===== 查询榜/好评榜：展开/收起 ===== */
  // 收起时耦合 transform 与 scrollTop 的单轨动画（避免“先超额上移后回弹”）
  function animateCollapseFollowersPinned({
    scroller,
    followers,
    totalDelta,     // HExact
    freeSpacePx,    // 初始容器底部可用空间
    duration = ANIM.rowMs,
    onDone = () => {}
  }) {
    if (!followers || !followers.length || !isFinite(totalDelta) || totalDelta <= 0) {
      onDone(); return;
    }
    const easeInOut = (t)=> t<.5 ? (2*t*t) : (1 - Math.pow(-2*t+2,2)/2);

    // 预设样式
    followers.forEach(el => {
      el.classList.add('fc-row-animating');
      el.style.transition = 'none';
      el.style.transform  = 'translate3d(0,0,0)';
      el.style.willChange = 'transform';
    });

    // 追踪遮罩（与现有展开逻辑一致）
    startRowMaskTracking(followers);

    const startTop = scroller ? scroller.scrollTop : 0;
    const maxScrollable = startTop; // 可减少的最大 scrollTop
    let startTs = 0;
    let raf = 0;
    let finished = false;

    function applyFrame(progressDelta) {
      // 进度（要上移的总量）
      const current = Math.min(totalDelta, progressDelta);

      // 未超过 freeSpace 时，不滚动，只用 transform
      if (current <= freeSpacePx) {
        const ty = -current;
        followers.forEach(el => { el.style.transform = `translate3d(0, ${ty}px, 0)`; });
        if (scroller) clampScrollTop(scroller, startTop);
        return;
      }

      // 超过 freeSpace 后，优先用 scrollTop 承担 over，直到触顶
      const over = current - freeSpacePx;
      const appliedScroll = Math.min(over, maxScrollable); // 触顶前不断增加，触顶后固定
      const ty = -(current - appliedScroll); // 触顶前恰为 -freeSpacePx，触顶后继续由 transform 承担剩余位移

      followers.forEach(el => { el.style.transform = `translate3d(0, ${ty}px, 0)`; });
      if (scroller) clampScrollTop(scroller, startTop - appliedScroll);
    }

    function frame(ts) {
      if (!startTs) startTs = ts;
      const k = Math.min(1, (ts - startTs) / Math.max(1, duration));
      const e = easeInOut(k);
      applyFrame(totalDelta * e);

      if (k < 1) {
        raf = requestAnimationFrame(frame);
      } else {
        if (finished) return;
        finished = true;

        // 结束：清理样式
        followers.forEach(el => {
          el.style.transition = 'none';
          el.style.transform  = '';
          el.style.willChange = '';
          el.classList.remove('fc-row-animating');
          // 下一帧再清空 transition，避免 reflow 卡顿
          requestAnimationFrame(()=>{ el.style.transition=''; });
        });
        stopRowMaskTracking();
        onDone();
      }
    }

    raf = requestAnimationFrame(frame);
  }

  // 计算父行第 6 列文本起点和第 7 列 padding-left，并写到表级 CSS 变量
  function setSubrowAnchorVar(parentTr){
    try {
      const table = parentTr.closest('table');
      if (!table) return;
      const rowRect = parentTr.getBoundingClientRect();
      const locked = table.dataset.hotColLocked === '1';

      // 第 7 列（热门工况列）
      const td6 = parentTr.children && parentTr.children[6];
      if (td6) {
        let anchorPx;
        // 优先用真实文本节点的几何位置
        let labelEl = td6.querySelector('.fc-marquee-inner');
        if (!labelEl) {
          labelEl = td6.querySelector('[data-role="top-cond"], .js-top-cond, span, div');
        }

        if (labelEl) {
          const lr = labelEl.getBoundingClientRect();
          anchorPx = lr.left - rowRect.left;
        } else {
          // 兜底：找不到 label 时才按 padding+按钮宽来估算
          const tdBox = td6.getBoundingClientRect();
          const cs6 = getComputedStyle(td6);
          const pl6 = parseFloat(cs6.paddingLeft) || 0;
          const expBtn = td6.querySelector('.fc-row-expander');
          const expW = expBtn ? expBtn.getBoundingClientRect().width : 0;
          const expMr = expBtn ? parseFloat(getComputedStyle(expBtn).marginRight) || 0 : 0;
          const labelGap = 4;
          anchorPx = (tdBox.left + pl6 + expW + expMr + labelGap) - rowRect.left;
        }
        table.style.setProperty('--subrow-anchor-x', Math.round(anchorPx) + 'px');

        // 宽度：若已锁定，则不再更新
        if (!locked) {
          const td6Rect = td6.getBoundingClientRect();
          let anchorInTdX = 0;
          if (labelEl) {
            const lr = labelEl.getBoundingClientRect();
            anchorInTdX = Math.max(0, lr.left - td6Rect.left);
          } else {
            const cs6 = getComputedStyle(td6);
            const pl6 = parseFloat(cs6.paddingLeft) || 0;
            const expBtn = td6.querySelector('.fc-row-expander');
            const expW = expBtn ? expBtn.getBoundingClientRect().width : 0;
            const expMr = expBtn ? parseFloat(getComputedStyle(expBtn).marginRight) || 0 : 0;
            const labelGap = 4;
            anchorInTdX = pl6 + expW + expMr + labelGap;
          }
          let conds = [];
          try { conds = JSON.parse(parentTr.dataset.conditions || '[]') || []; } catch(_) {}
          if (Array.isArray(conds) && conds.length) {
            let maxLabelW = 0;
            conds.forEach(c => {
              const txt = String(c.condition_name_zh || '');
              if (!txt) return;
              maxLabelW = Math.max(maxLabelW, measureTextPx(txt, td6, 14, 400));
            });
            const padRight = 12;
            let need = Math.round(anchorInTdX + maxLabelW + padRight);
            const currW = Math.round(td6Rect.width);
            if (need < currW) need = currW;

            const card = table.closest('.fc-right-card');
            const hostW = card ? card.getBoundingClientRect().width : window.innerWidth;
            const fracStr = card ? getComputedStyle(card).getPropertyValue('--hot-col-max-frac').trim() : '';
            const frac = Number.isFinite(parseFloat(fracStr)) ? parseFloat(fracStr) : 0.38;
            const maxAllow = Math.max(180, Math.floor(hostW * frac));
            need = Math.min(need, maxAllow);

            // 确保 colgroup 存在后再写
            const colgroup = ensureColgroup(table);
            table.style.setProperty('--hot-col-min-w', need + 'px');
            const c6 = colgroup && colgroup.children && colgroup.children[6];
            if (c6) c6.style.width = need + 'px';
            table.dataset.hotColLocked = '1'; // 第一次展开若触发到这里，也锁定
          }
        }
      }

      // 第 8 列（次数列）左内边距（注入为子行计数左 padding）
      const td7 = parentTr.children && parentTr.children[7];
      if (td7) {
        const cs7 = getComputedStyle(td7);
        const pl7 = parseFloat(cs7.paddingLeft) || 0;
        table.style.setProperty('--subrow-count-pl', Math.round(pl7) + 'px');
      }
    } catch(_) {}
  }

  /* 只在暗色主题且存在渐变时才需要追踪 */
  function isDarkTheme() {
    return document.documentElement.getAttribute('data-theme') === 'dark';
  }
  function hasDarkGradient() {
    try {
      const g = getComputedStyle(document.documentElement).getPropertyValue('--dark-rand-gradient');
      return !!g && g.trim() !== '' && g.trim() !== 'none';
    } catch(_) { return false; }
  }
  function shouldTrackMask() {
    return isDarkTheme() && hasDarkGradient();
  }

  let __maskRaf = null;
  let __maskRows = null;

  function __maskStep() {
    if (!__maskRows) return;
    for (const el of __maskRows) {
      if (!el || !el.isConnected) continue;
      const r = el.getBoundingClientRect();
      el.style.setProperty('--row-vp-left', Math.round(r.left) + 'px');
      el.style.setProperty('--row-vp-top',  Math.round(r.top)  + 'px');
    }
    __maskRaf = requestAnimationFrame(__maskStep);
  }
  function startRowMaskTracking(rows) {
    // 浅色主题或无渐变时，直接跳过追踪
    if (!shouldTrackMask()) {
      stopRowMaskTracking();
      return;
    }
    cancelAnimationFrame(__maskRaf);
    __maskRows = Array.from(rows || []);
    __maskStep(); // 立即跑一帧，避免首帧错位
  }
  function stopRowMaskTracking() {
    cancelAnimationFrame(__maskRaf);
    __maskRaf = null;
    if (__maskRows) {
      for (const el of __maskRows) {
        if (!el) continue;
        el.style.removeProperty('--row-vp-left');
        el.style.removeProperty('--row-vp-top');
      }
    }
    __maskRows = null;
  }

  function animateHideEl(el, dx=0, dy=6, duration=ANIM.fadeOutMs) {
    if (!el || el.dataset._anim_state === 'hiding') return;
    el.dataset._anim_state = 'hiding';
    el.classList.add('fc-fade-slide');
    el.style.visibility = ''; el.style.opacity = '1'; el.style.transform = 'translate3d(0,0,0)';
    void el.offsetWidth;
    el.style.transition = `transform ${duration}ms ${ANIM.rowEase}, opacity ${duration}ms ${ANIM.rowEase}`;
    requestAnimationFrame(() => { el.style.opacity = '0'; el.style.transform = `translate3d(${dx}px, ${dy}px, 0)`; });
    const onEnd = (e) => { if (e.propertyName !== 'opacity') return;
      el.removeEventListener('transitionend', onEnd); el.style.visibility = 'hidden'; el.style.transition = ''; el.dataset._anim_state = ''; };
    el.addEventListener('transitionend', onEnd);
  }
  function animateShowEl(el, dx=0, dy=-6, duration=ANIM.fadeInMs) {
    if (!el || el.dataset._anim_state === 'showing') return;
    el.dataset._anim_state = 'showing';
    el.classList.add('fc-fade-slide');
    el.style.visibility = 'visible'; el.style.opacity = '0'; el.style.transform = `translate3d(${dx}px, ${dy}px, 0)`;
    void el.offsetWidth;
    el.style.transition = `transform ${duration}ms ${ANIM.rowEase}, opacity ${duration}ms ${ANIM.rowEase}`;
    requestAnimationFrame(() => { el.style.opacity = '1'; el.style.transform = 'translate3d(0,0,0)'; });
    const onEnd = (e) => { if (e.propertyName !== 'opacity') return;
      el.removeEventListener('transitionend', onEnd); el.style.transition = ''; el.dataset._anim_state = ''; };
    el.addEventListener('transitionend', onEnd);
  }

  function isLikesRow(tr){
    // “好评榜”tbody 的 table id 固定为 ratingRankTable
    const table = tr && tr.closest('table');
    return !!(table && table.id === 'ratingRankTable');
  }

  function toggleParentHotCondAndAction(tr, expanded, opts={}){
    if (!tr) return;
    const td6 = tr.children && tr.children[6];
    const tdLast = tr.children && tr.children[tr.children.length-1];
    const hot = tr.querySelector('[data-role="top-cond"] .fc-marquee-inner, td:nth-child(7) .fc-marquee-inner') 
             || tr.querySelector('[data-role="top-cond"], .js-top-cond');
    const actionBtn = tr.querySelector('td:last-child .fc-btn-icon-add') || null;

    // 接力式：仅位移，不渐隐
    if (opts.mode === 'relay') {
      const dur = opts.duration || ANIM.rowMs;
      const ease = opts.easing || 'ease';
      // 动画期间给相关列加裁切，避免越界
      addClip(td6); addClip(tdLast);

      if (expanded) {
        // 展开：父行元素向下滑出（到子行对应位置）
        if (hot && typeof opts.dyLabel === 'number') slideHideEl(hot, opts.dyLabel, dur, ease);
        if (actionBtn && typeof opts.dyBtn === 'number') slideHideEl(actionBtn, opts.dyBtn, dur, ease);
      } else {
        // 收起：父行元素自子行位置向上滑入
        if (hot && typeof opts.fromDyLabel === 'number') slideShowEl(hot, opts.fromDyLabel, dur, ease);
        if (actionBtn && typeof opts.fromDyBtn === 'number') slideShowEl(actionBtn, opts.fromDyBtn, dur, ease);
      }
      // 延迟清理裁切（给动画收尾一点余量）
      setTimeout(()=>{ removeClip(td6); removeClip(tdLast); }, (opts.duration||ANIM.rowMs) + ANIM.cleanupMs);
      return;
    }

    if (expanded) {
      if (hot) animateHideEl(hot);
      if (actionBtn) animateHideEl(actionBtn);
    } else {
      if (hot) animateShowEl(hot);
      if (actionBtn) animateShowEl(actionBtn);
    }
  }

function initTopQueriesAndLikesExpander(){

  function parseConds(tr){ try { return JSON.parse(tr.dataset.conditions || '[]') || []; } catch(_) { return []; } }

  // 子行：colspan=7 + 次数 + 操作
  function buildSubrowHTML(parentTr, cond, countValue){
    const brand = parentTr.dataset.brand || '';
    const model = parentTr.dataset.model || '';
    const mid   = parentTr.dataset.modelId || '';
    const cid   = String(cond.condition_id || '');
    const cname = String(cond.condition_name_zh || '');

    const rt = cond.resistance_type_zh || '';
    const rl = cond.resistance_location_zh || '';
    let extra = '';
    if (typeof window.formatScenario === 'function') extra = window.formatScenario(rt, rl);
    else {
      const rtype = escapeHtml(rt || ''), rloc = String(rl || '').trim();
      extra = rloc && rloc !== '无' ? `${rtype}(${escapeHtml(rl)})` : rtype;
    }
    const extraLeft = extra ? `<span class="fc-subrow__extra-left">${escapeHtml(extra)}&nbsp;&nbsp;&nbsp;</span>` : '';

    const cnt  = Number(countValue || 0);
    return `
      <tr class="fc-subrow" data-parent-mid="${escapeHtml(mid)}">
        <td colspan="7">
          <div class="fc-subrow__row">
            <div class="fc-subrow__indent">
              ${extraLeft}
              <span class="fc-subrow__dot"></span>
              <span class="fc-subrow__label">${escapeHtml(cname)}</span>
            </div>
          </div>
        </td>
        <td>
          <div class="fc-subrow__row fc-subrow__row--count">
            <span class="text-blue-600 font-medium">${escapeHtml(cnt)}</span>
          </div>
        </td>
        <td>
          <div class="fc-subrow__row fc-subrow__row--actions">
            ${buildQuickBtnHTML('ranking', brand, model, mid, cid, cname, isLikesRow(parentTr) ? 'top_rating_expand' : 'top_query_expand')}
          </div>
        </td>
      </tr>`;
  }

  function isSubrowOf(row, mid){ return row && row.classList && row.classList.contains('fc-subrow') && row.dataset.parentMid === String(mid); }
  function collectFollowers(fromTr){ const arr=[]; let n=fromTr.nextElementSibling; while(n){ arr.push(n); n=n.nextElementSibling; } return arr; }
  function collectFollowersAfter(el){ const arr=[]; let n=el?el.nextElementSibling:null; while(n){ arr.push(n); n=n.nextElementSibling; } return arr; }
  function measureTops(els){ const m=new Map(); els.forEach(el=>{ m.set(el, el.getBoundingClientRect().top); }); return m; }

  // 同时测量 top / height / bottom，供“底部为基准”使用
  function measureTopAndHeight(els){
    const m = new Map();
    els.forEach(el => {
      const r = el.getBoundingClientRect();
      m.set(el, { top: r.top, height: r.height, bottom: r.bottom });
    });
    return m;
  }

  function markAnimating(els,on){ els.forEach(el=>{ if(on) el.classList.add('fc-row-animating'); else el.classList.remove('fc-row-animating'); }); }

  // 不再做任何旧字段兼容，严格依赖统一字段
  function getChildRank(rec) { return Number(rec.cond_rank || 1e9); }
  function getChildCount(rec) { return Number(rec.count || 0); }

  // 获取“最后一条主行”（非子行）
  function getLastMainRow(tr){
    const tbody = tr?.closest?.('tbody');
    if (!tbody) return null;
    const mains = tbody.querySelectorAll('tr:not(.fc-subrow)');
    return mains.length ? mains[mains.length - 1] : null;
  }

  // 展开阶段：动态阈值滚动（正向累计）
  // totalDelta>0；freeSpacePx = 基准边到 scroller 可视底的初始空隙；
  // 限制：不能把正在展开的父行顶出表头（thead 底部为界）
  function startTrackScrollPinDynamic(scroller, totalDelta, freeSpacePx, opts){
    if (!scroller || !isFinite(totalDelta) || totalDelta <= 0) return;

    const easeInOut = (t)=> t<.5 ? (2*t*t) : (1 - Math.pow(-2*t+2,2)/2);
    const dur = ANIM.rowMs;
    const startTop = scroller.scrollTop;
    const threshold = Math.max(0, freeSpacePx || 0);

    // 可选：父行保护参数
    const anchorRow   = opts?.anchorRow   ?? null;
    const theadHeight = Number.isFinite(opts?.theadHeight) ? opts.theadHeight : 0;
    const safeMargin  = Number.isFinite(opts?.safeMargin)  ? opts.safeMargin  : 0;

    // 计算展开开始时父行在 scroller 内部的相对 top
    let parentOffsetInScroller = null;
    if (anchorRow) {
      try {
        const scRect  = scroller.getBoundingClientRect();
        const trRect0 = anchorRow.getBoundingClientRect();
        parentOffsetInScroller = trRect0.top - scRect.top; // 父行顶部距滚动容器顶部的像素
      } catch(_) {
        parentOffsetInScroller = null;
      }
    }

    // 允许的最大 scrollTop 增量：不能让父行顶过 thead 底部 + safeMargin
    let maxScrollUpByParent = Infinity;
    if (parentOffsetInScroller != null && parentOffsetInScroller > 0) {
      const minParentTop = theadHeight + safeMargin;  // 父行不应被推到这个位置之上
      // 父行相对容器 top 随 scrollTop 增加而减少，近似为：parentTop(new) = parentOffsetInScroller - scrollUp
      const maxUp = parentOffsetInScroller - minParentTop;
      maxScrollUpByParent = maxUp > 0 ? maxUp : 0;
    }

    let startTs = 0;
    function frame(ts){
      if (!startTs) startTs = ts;
      const k = Math.min(1, (ts - startTs)/Math.max(1,dur));
      const e = easeInOut(k);

      const currentDelta = e * totalDelta;
      const over = currentDelta - threshold;

      if (over <= 0) {
        // 还没“挤爆”底部空隙，只用 transform，不动 scrollTop
        clampScrollTop(scroller, startTop);
      } else {
        // 理论上为防止底部出视口所需的 scrollUp
        const idealScrollUp = over;

        // 同时受“父行不能被顶出表头”的限制
        const boundedScrollUp = Math.min(idealScrollUp, maxScrollUpByParent);

        const next = startTop + boundedScrollUp;
        clampScrollTop(scroller, next);
      }

      if (k < 1) requestAnimationFrame(frame);
    }
    requestAnimationFrame(frame);
  }

  // 收起阶段：动态阈值滚动（反向累计，触顶即止）
  // totalDelta>0；freeSpacePx = 基准边到 scroller 可视底的初始空隙；仅当 currentDelta 超过该空隙时开始减少 scrollTop
  function startTrackScrollPinDynamicCollapse(scroller, totalDelta, freeSpacePx){
    if (!scroller || !isFinite(totalDelta) || totalDelta <= 0) return;
    const easeInOut = (t)=> t<.5 ? (2*t*t) : (1 - Math.pow(-2*t+2,2)/2);
    const dur = ANIM.rowMs;
    const startTop = scroller.scrollTop;
    const threshold = Math.max(0, freeSpacePx || 0);

    let startTs = 0;
    function frame(ts){
      if (!startTs) startTs = ts;
      const k = Math.min(1, (ts - startTs)/Math.max(1,dur));
      const e = easeInOut(k);

      const currentDelta = e * totalDelta;
      const over = currentDelta - threshold;
      let next = startTop;
      if (over > 0) {
        next = startTop - over; // 收起：减少 scrollTop
      }
      clampScrollTop(scroller, next);

      if (k < 1 && scroller.scrollTop > 0) requestAnimationFrame(frame);
    }
    requestAnimationFrame(frame);
  }

// ===== 展开 =====
async function expandRow(btn){
  const tr = safeClosest(btn, 'tr'); if (!tr) return;
  if (isRowAnimating(tr)) return;
  setRowAnimating(tr, true, btn);
  const unlock = () => setRowAnimating(tr, false, btn, ANIM.unlockDelayMs);

  const scroller = getScroller(tr);
  const trRectBefore = tr.getBoundingClientRect(); // 供“最后一行”基准使用

  setSubrowAnchorVar(tr);

  // 插入前：收集“后续主行”及其可视 top
  const followers = collectFollowers(tr);
  const prevMap   = measureTops(followers);
  const prevGeom  = measureTopAndHeight(followers);
  
  // 构造并插入子行
  const condsRaw = parseConds(tr);
  const sorted = condsRaw.slice().sort((a,b)=>{
    const ra = getChildRank(a);
    const rb = getChildRank(b);
    if (ra !== rb) return ra - rb;
    const ca = getChildCount(a);
    const cb = getChildCount(b);
    return cb - ca;
  });
  tr.insertAdjacentHTML('afterend', sorted.map(c=>{
    return buildSubrowHTML(tr, c, getChildCount(c));
  }).join(''));

  // 子行总高度增量
  let HExact = 0;
  {
    let n = tr.nextElementSibling;
    while (n && n.classList.contains('fc-subrow')) {
      HExact += n.getBoundingClientRect().height;
      n = n.nextElementSibling;
    }
    if (!isFinite(HExact) || HExact <= 0) {
      const v = getComputedStyle(document.documentElement).getPropertyValue('--subrow-h').trim();
      const h = parseFloat(v) || 26;
      HExact = h * sorted.length;
    }
  }

  const firstSub = tr.nextElementSibling && tr.nextElementSibling.classList.contains('fc-subrow')
    ? tr.nextElementSibling : null;

  // 父→子接力位移
  let dyLabel = null, dyBtn = null;
  if (firstSub) {
    const pLabel = (tr.children[6]?.querySelector('.fc-marquee-inner')) ||
                   tr.querySelector('[data-role="top-cond"], .js-top-cond');
    const cLabel = firstSub.querySelector('td[colspan] .fc-subrow__label');
    const pBtn   = tr.querySelector('td:last-child .fc-btn-icon-add');
    const cBtn   = firstSub.querySelector('td:last-child .fc-btn-icon-add');
  
    if (pLabel && cLabel) {
      const pr = pLabel.getBoundingClientRect();
      const cr = cLabel.getBoundingClientRect();
      const pCenter = pr.top + pr.height / 2;
      const cCenter = cr.top + cr.height / 2;
      dyLabel = Math.round(cCenter - pCenter);

      const fromY = Math.round(pCenter - cCenter); // 子行从父行中线位置开始上移回自己的位置
      cLabel.style.transition = 'none';
      cLabel.style.transform  = `translateY(${fromY}px)`;
      addClip(firstSub.querySelector('td[colspan]'));
    }

    if (pBtn && cBtn) {
      const prb = pBtn.getBoundingClientRect();
      const crb = cBtn.getBoundingClientRect();
      dyBtn = Math.round(crb.top - prb.top);
      cBtn.style.transition = 'none';
      cBtn.style.transform  = `translateY(${Math.round(prb.top - crb.top)}px)`;
      addClip(firstSub.querySelector('td:last-child'));
    }
  
    requestAnimationFrame(()=> {
      if (cLabel) {
        cLabel.style.transition = `transform ${ANIM.rowMs}ms ${ANIM.rowEase}`;
        cLabel.style.transform  = 'translateY(0)';
      }
      if (cBtn) {
        cBtn.style.transition   = `transform ${ANIM.rowMs}ms ${ANIM.rowEase}`;
        cBtn.style.transform    = 'translateY(0)';
      }
    
      setTimeout(()=> {
        removeClip(firstSub.querySelector('td[colspan]'));
        removeClip(firstSub.querySelector('td:last-child'));
        if (cLabel) { cLabel.style.transition=''; cLabel.style.transform=''; }
        if (cBtn)   { cBtn.style.transition=''; cBtn.style.transform=''; }
      }, ANIM.rowMs + ANIM.cleanupMs);
    });
  }

  // followers 位移测量（插入后）
  const currMap = measureTops(followers);

  // 父行工况/按钮接力：父行从自己位置 slideHide 到子行位置
  toggleParentHotCondAndAction(tr, true, {
    mode: 'relay',
    duration: ANIM.rowMs,
    easing: ANIM.rowEase,
    dyLabel,
    dyBtn
  });

  // 展开滚动同步（动态阈值）——以第一条下移主行为基准
  if (scroller) {
    const scRect = scroller.getBoundingClientRect();
    let freeSpacePx = 0;

    if (followers.length === 0) {
      freeSpacePx = Math.max(0, Math.round(scRect.bottom - trRectBefore.bottom));
    } else {
      const firstFollower = followers[0];
      const g = prevGeom.get(firstFollower) || null;
      if (g && Number.isFinite(g.top) && Number.isFinite(g.height)) {
        const prevBottomFF = g.top + g.height;
        freeSpacePx = Math.max(0, Math.round(scRect.bottom - prevBottomFF));
      } else {
        const prevTopFF = prevMap.get(firstFollower);
        const rowHEst = Math.round(
          (firstFollower && firstFollower.getBoundingClientRect().height) ||
          parseFloat(getComputedStyle(document.documentElement).getPropertyValue('--rank-row-h')) ||
          32
        );
        if (typeof prevTopFF === 'number') {
          const approxBottom = prevTopFF + rowHEst;
          freeSpacePx = Math.max(0, Math.round(scRect.bottom - approxBottom));
        } else {
          const ffRect = firstFollower.getBoundingClientRect();
          freeSpacePx = Math.max(0, Math.round(scRect.bottom - (ffRect.top - HExact + rowHEst)));
        }
      }
    }

    // === 父行保护参数 ===
    let theadHeight = 0;
    try {
      const table = scroller.querySelector('.fc-rank-table');
      if (table && table.tHead && table.tHead.rows && table.tHead.rows[0]) {
        const thRect = table.tHead.rows[0].getBoundingClientRect();
        // 这里的 height 基本就是“表头占用的高度”，即父行不能被推到这个高度之上
        theadHeight = thRect.height;
      }
    } catch(_) {
      theadHeight = 0;
    }

    const safeMargin = 0; // 如需让父行离表头留点缝隙，可改为 4/6 等

    startTrackScrollPinDynamic(scroller, +HExact, freeSpacePx, {
      anchorRow:   tr,          // 当前正在展开的父行
      theadHeight,              // 表头高度，作为“顶部界限”
      safeMargin                // 父行和表头之间预留的安全距离（可调）
    });
  }

  // followers 位移动画
  markAnimating(followers, true);
  if (!followers.length) {
    setTimeout(() => {
      stopRowMaskTracking();
      unlock();
    }, ANIM.rowMs + ANIM.cleanupMs);
  } else {
    startRowMaskTracking(followers);
    followers.forEach(el => {
      const prevTop = prevMap.get(el), currTop = currMap.get(el);
      if (prevTop == null || currTop == null) return;
      const dy = prevTop - currTop;
      if (Math.abs(dy) < 0.5) return;
      el.style.transition = 'none';
      el.style.transform  = `translateY(${dy}px)`;
    });
    void document.body.offsetWidth;

    let rest = followers.length;
    requestAnimationFrame(() => {
      followers.forEach(el => {
        if (!prevMap.has(el)) return;
        el.style.transition = `transform ${ANIM.rowMs}ms ${ANIM.rowEase}`;
        el.style.transform  = 'translateY(0)';
      });
      const onEnd = (e) => {
        if (e.propertyName !== 'transform') return;
        const el = e.currentTarget;
        el.removeEventListener('transitionend', onEnd);
        el.style.transition = '';
        el.style.transform  = '';
        el.classList.remove('fc-row-animating');
        if (--rest === 0) {
          stopRowMaskTracking();
          unlock();
        }
      };
      followers.forEach(el => el.addEventListener('transitionend', onEnd));
      setTimeout(() => {
        if (rest > 0) {
          rest = 0;
          stopRowMaskTracking();
          unlock();     
        }
      }, ANIM.rowMs + ANIM.guardMs);
    });
  }

  btn.setAttribute('aria-expanded','true');
  btn.removeAttribute('title');
  btn.classList.add('is-open');

  if (typeof syncQuickActionButtons === 'function') syncQuickActionButtons();
  updateQuickBtnCompactMode();
}

// 1) 严格钉底版 collapse 动画：全程 netY = -(current - appliedScroll)，不可能出现“向下走”
function animateCollapseFollowersPinnedStrict({
  scroller,
  followers,
  totalDelta,
  freeSpacePx,
  duration = ANIM.rowMs,
  onDone = () => {}
}) {
  if (!followers || !followers.length || !isFinite(totalDelta) || totalDelta <= 0) { onDone(); return; }
  const easeInOut = (t)=> t<.5 ? (2*t*t) : (1 - Math.pow(-2*t+2,2)/2);

  followers.forEach(el => { el.classList.add('fc-row-animating'); el.style.transition='none'; el.style.transform='translate3d(0,0,0)'; el.style.willChange='transform'; });
  startRowMaskTracking(followers);

  const startTop = scroller ? scroller.scrollTop : 0;
  const maxScrollable = startTop;
  let startTs = 0;

  function frame(ts){
    if (!startTs) startTs = ts;
    const k = Math.min(1, (ts - startTs) / Math.max(1, duration));
    const current = totalDelta * easeInOut(k);

    const over = Math.max(0, current - (freeSpacePx || 0));
    const appliedScroll = scroller ? Math.min(over, maxScrollable) : 0;
    if (scroller) clampScrollTop(scroller, startTop - appliedScroll);

    // 关键：transform 总是 -current（不要减 appliedScroll）
    const tyStr = `translate3d(0, ${-current}px, 0)`;
    followers.forEach(el => { el.style.transform = tyStr; });

    if (k < 1) requestAnimationFrame(frame);
    else {
      stopRowMaskTracking();
      onDone();
      followers.forEach(el => {
        el.style.transition='none'; el.style.transform=''; el.style.willChange=''; el.classList.remove('fc-row-animating');
        requestAnimationFrame(()=>{ el.style.transition=''; });
      });
    }
  }
  requestAnimationFrame(frame);
}

// ===== 收起（严格做成展开的反向） =====
function collapseRow(btn){
  const tr = safeClosest(btn, 'tr'); if (!tr) return;
  if (isRowAnimating(tr)) return;
  setRowAnimating(tr, true, btn);
  const unlock = () => setRowAnimating(tr, false, btn, 0);

  const scroller = getScroller(tr);

  const mid = tr.dataset.modelId || '';
  const subrows = [];
  let n = tr.nextElementSibling;
  while (isSubrowOf(n, mid)) { subrows.push(n); n = n.nextElementSibling; }

  if (!subrows.length) {
    // 没有子行，直接显示父行内容
    toggleParentHotCondAndAction(tr, false);
    unlock();
    btn.setAttribute('aria-expanded','false');
    btn.classList.remove('is-open');
    btn.removeAttribute('title');
    return;
  }

  const firstSub = subrows[0] || null;

  // 子→父接力：几何上反向展开时那一套
  let fromDyLabel = null, fromDyBtn = null;
  if (firstSub) {
    const pLabel = (tr.children[6]?.querySelector('.fc-marquee-inner')) ||
                   tr.querySelector('[data-role="top-cond"], .js-top-cond');
    const cLabel = firstSub.querySelector('td[colspan] .fc-subrow__label');
    const pBtn   = tr.querySelector('td:last-child .fc-btn-icon-add');
    const cBtn   = firstSub.querySelector('td:last-child .fc-btn-icon-add');

    if (pLabel && cLabel) {
      const pr = pLabel.getBoundingClientRect();
      const cr = cLabel.getBoundingClientRect();
      const pCenter = pr.top + pr.height / 2;
      const cCenter = cr.top + cr.height / 2;

      // fromDyLabel = 子行中线到父行中线的位移（展开时 dyLabel 的反向）
      fromDyLabel = Math.round(cCenter - pCenter);

      // 收起：先把子行 label 从自己的位置拉到“父行中线”位置，再 slideHide 回去
      addClip(firstSub.querySelector('td[colspan]'));
      cLabel.style.transition = `transform ${ANIM.rowMs}ms ${ANIM.rowEase}`;
      cLabel.style.transform  = `translateY(${Math.round(pCenter - cCenter)}px)`;

      setTimeout(()=> {
        removeClip(firstSub.querySelector('td[colspan]'));
        cLabel.style.transition = '';
        cLabel.style.transform  = '';
      }, ANIM.rowMs + ANIM.cleanupMs);
    }

    if (pBtn && cBtn) {
      const prb = pBtn.getBoundingClientRect();
      const crb = cBtn.getBoundingClientRect();
      fromDyBtn = Math.round(crb.top - prb.top); // 对应展开时 dyBtn 的反向

      addClip(firstSub.querySelector('td:last-child'));
      cBtn.style.transition = `transform ${ANIM.rowMs}ms ${ANIM.rowEase}`;
      cBtn.style.transform  = `translateY(${Math.round(prb.top - crb.top)}px)`;

      setTimeout(()=> {
        removeClip(firstSub.querySelector('td:last-child'));
        cBtn.style.transition = '';
        cBtn.style.transform  = '';
      }, ANIM.rowMs + ANIM.cleanupMs);
    }
  }

  // 父行接力：父行从子行位置 slideShow 回来
  toggleParentHotCondAndAction(tr, false, {
    mode: 'relay',
    duration: ANIM.rowMs,
    easing:  ANIM.rowEase,
    fromDyLabel,
    fromDyBtn
  });

  // 子行总高度
  let HExact = 0;
  for (const sr of subrows) {
    HExact += sr.getBoundingClientRect().height;
  }
  if (!isFinite(HExact) || HExact <= 0) {
    const v = getComputedStyle(document.documentElement).getPropertyValue('--subrow-h').trim();
    const h = parseFloat(v) || 26;
    HExact = h * subrows.length;
  }

  // 是否“最后一行”的分支
  const lastSub = subrows[subrows.length - 1];
  const followers = collectFollowersAfter(lastSub);
  const isLastMainRow = followers.length === 0;

  if (isLastMainRow) {
    if (scroller) {
      collapseLastRowWithMask(tr, scroller, subrows, ANIM.rowMs, () => { unlock(); });
    } else {
      setTimeout(() => {
        subrows.forEach(sr => sr.remove());
        unlock();
      }, ANIM.rowMs + ANIM.cleanupMs);
    }
  } else {
    // 计算初始 freeSpace（以“最后一条主行”底边为基准）
    let freeSpacePx = 0;
    if (scroller) {
      const scRect = scroller.getBoundingClientRect();
      const lastMain = getLastMainRow(tr);
      if (lastMain) {
        const lmRect = lastMain.getBoundingClientRect();
        const gap = Math.round(lmRect.bottom - scRect.bottom);
        freeSpacePx = Math.abs(gap);
      }
    }

    // 严格钉底推进；动画结束后移除子行并解锁；不要再改 scrollTop
    animateCollapseFollowersPinnedStrict({
      scroller,
      followers,
      totalDelta: HExact,
      freeSpacePx,
      duration: ANIM.rowMs,
      onDone: () => {
        subrows.forEach(sr => sr.remove());
        requestAnimationFrame(() => {
          followers.forEach(el => {
            el.style.transition = 'none';
            el.style.transform  = '';
            el.style.willChange = '';
            el.classList.remove('fc-row-animating');
          });
          requestAnimationFrame(() => {
            followers.forEach(el => { el.style.transition = ''; });
            unlock();
          });
        });
      }
    });
  }

  btn.setAttribute('aria-expanded','false');
  btn.classList.remove('is-open');
  btn.removeAttribute('title');
  updateQuickBtnCompactMode();
}

  document.addEventListener('click', (e)=>{
      const toggle = safeClosest(e.target, '.fc-expand-toggle');
      if (!toggle) return;
      const tr = safeClosest(toggle, 'tr');
      if (tr && isRowAnimating(tr)) { e.preventDefault(); e.stopPropagation(); return; }
      e.preventDefault(); e.stopPropagation();
      if (toggle.getAttribute('aria-expanded') === 'true') collapseRow(toggle);
      else expandRow(toggle);
    });
  }

  function addClip(el){
    if (!el) return;
    el.classList.add('fc-col-clip');

    // 如果本来就有 inline position，就尊重现状，不改动
    if (!el.style.position) {
      const cs = getComputedStyle(el);
      // 若是 sticky 或 absolute 等，我们不去强行改成 relative，避免破坏布局语义
      if (cs.position === 'static') {
        el.dataset._orig_pos = 'static';
        el.style.position = 'relative';
      } else {
        // 记住“无需改 position”，方便 removeClip 清理 data
        el.dataset._orig_pos = 'keep';
      }
    } else {
      // 已有内联 position，标记为 keep
      el.dataset._orig_pos = 'keep';
    }
  }

  function removeClip(el){
    if (!el) return;
    el.classList.remove('fc-col-clip');

    // 只在我们确实从 static -> relative 的情况下还原；对 sticky/absolute/已有 inline 的不动
    if (el.dataset._orig_pos === 'static') {
      el.style.position = '';
    }
    delete el.dataset._orig_pos;
  }
  
  // 仅位移的隐藏/显示（保留）
  function slideHideEl(el, toDy, duration=ANIM.rowMs, easing=ANIM.rowEase){
    if (!el) return;
    el.style.visibility = 'visible';
    el.style.willChange = 'transform';
    el.style.transition = `transform ${duration}ms ${easing}`;
    requestAnimationFrame(()=> {
      el.style.transform = `translateY(${Math.round(toDy)}px)`;
    });
    const onEnd = (e)=> {
      if (e.propertyName !== 'transform') return;
      el.removeEventListener('transitionend', onEnd);
      el.style.visibility = 'hidden';
      el.style.transition = '';
      el.style.transform = '';
      el.style.willChange = '';
    };
    el.addEventListener('transitionend', onEnd);
  }
  function slideShowEl(el, fromDy, duration=ANIM.rowMs, easing=ANIM.rowEase){
    if (!el) return;
    el.style.visibility = 'visible';
    el.style.willChange = 'transform';
    el.style.transition = 'none';
    el.style.transform = `translateY(${Math.round(fromDy)}px)`;
    void el.offsetWidth;
    requestAnimationFrame(()=> {
      el.style.transition = `transform ${duration}ms ${easing}`;
      el.style.transform = 'translateY(0)';
    });
    const onEnd = (e)=> {
      if (e.propertyName !== 'transform') return;
      el.removeEventListener('transitionend', onEnd);
      el.style.transition = '';
      el.style.transform = '';
      el.style.willChange = '';
    };
    el.addEventListener('transitionend', onEnd);
  }

  // 滚动容器与贴边判定（保留基础工具）
  function getScroller(tr){
    return tr?.closest?.('.fc-rank-scroll') || null;
  }

  function clampScrollTop(scroller, v){
    const maxScroll = Math.max(0, scroller.scrollHeight - scroller.clientHeight);
    scroller.scrollTop = Math.max(0, Math.min(maxScroll, v));
  }


  // REPLACE: 最后一行“收起”专用的绝对量平滑滚动（保持不变，确保存在）
  function startTrackScrollPinCollapseAbsolute(scroller, totalDelta, duration=ANIM.rowMs){
    if (!scroller || !isFinite(totalDelta) || totalDelta <= 0) return;
    const easeInOut = (t)=> t<.5 ? (2*t*t) : (1 - Math.pow(-2*t+2,2)/2);
    const startTop = scroller.scrollTop;
    let startTs = 0, raf = 0, stopped = false;

    function frame(ts){
      if (!startTs) startTs = ts;
      const k = Math.min(1, (ts - startTs) / Math.max(1, duration));
      const e = easeInOut(k);
      const next = startTop - e * totalDelta; // 收起：scrollTop 逐步减少，容器“向下”运动
      clampScrollTop(scroller, next);
      if (scroller.scrollTop <= 0) stopped = true;  // 触顶即止
      if (k < 1 && !stopped) raf = requestAnimationFrame(frame);
    }
    raf = requestAnimationFrame(frame);
  }

  // REPLACE: 最后一行“收起”的平滑方案 —— 去掉遮罩，仅平滑滚动，结束后再移除子行
  function collapseLastRowWithMask(tr, scroller, subrows, duration=ANIM.rowMs, onDone){
    try {
      if (!subrows || !subrows.length) { if (typeof onDone==='function') onDone(); return; }

      // 计算本次将减少的总高度（不改动子行 DOM）
      let HExact = 0;
      for (const sr of subrows) HExact += sr.getBoundingClientRect().height;
      if (!isFinite(HExact) || HExact <= 0) {
        const v = getComputedStyle(document.documentElement).getPropertyValue('--subrow-h').trim();
        const h = parseFloat(v) || 26;
        HExact = h * subrows.length;
      }

      // 容器平滑“向下”移动：按绝对量 HExact 逐步减少 scrollTop（触顶即止）
      if (scroller) {
        startTrackScrollPinCollapseAbsolute(scroller, HExact, duration);
      }

      // 动画结束后再移除子行（scrollTop 已同步完成，不会瞬跳）
      const cleanup = () => {
        subrows.forEach(sr => sr.remove());
        if (typeof onDone==='function') onDone();
      };
      setTimeout(cleanup, duration + ANIM.cleanupMs);
    } catch(_) {
      if (typeof onDone==='function') onDone();
    }
  }

  // ADD: 近期更新（右侧面板内聚）
  let updatesTabLoaded = false;
  let updatesTabLastLoad = 0;
  const UPDATES_TTL = 600000; // 10 分钟
  let _updatesPending = false, _updatesDebounce = null;

  function needReloadUpdates() {
    if (!updatesTabLoaded) return true;
    return (Date.now() - updatesTabLastLoad) > UPDATES_TTL;
  }
  function applyRecentUpdatesTable(resp) {
    const tbody = document.getElementById('recentUpdatesTbody');
    if (!tbody) return;

    const list = (resp && resp.items) ||
                 (resp && resp.data && Array.isArray(resp.data.items) ? resp.data.items : []);

    if (!Array.isArray(list)) {
      tbody.innerHTML = '<tr><td colspan="8" class="text-center text-red-500 py-6">数据格式异常</td></tr>';
      return;
    }
    if (list.length === 0) {
      tbody.innerHTML = '<tr><td colspan="8" class="text-center text-gray-500 py-6">暂无近期更新数据</td></tr>';
      return;
    }

    let html = '';
    list.forEach(r => {
      const brand = r.brand_name_zh || '';
      const model = r.model_name || '';
      const maxSpeed = (r.max_speed != null) ? ` (${r.max_speed} RPM)` : '';
      const sizeText = `${escapeHtml(r.size)}x${escapeHtml(r.thickness)}`;
      const scen = escapeHtml(r.condition_name_zh || '');
      const updateText = escapeHtml(r.update_date);
      const descRaw = (r.description != null && String(r.description).trim() !== '') ? String(r.description) : '-';
      const desc = escapeHtml(descRaw);

      html += `
        <tr class="hover:bg-gray-50">
          <td class="nowrap fc-marquee-cell"><span class="fc-marquee-inner">${escapeHtml(brand)}</span></td>
          <td class="nowrap fc-marquee-cell"><span class="fc-marquee-inner">${escapeHtml(model)}${maxSpeed}</span></td>
          <td class="nowrap fc-marquee-cell"><span class="fc-marquee-inner">${sizeText}</span></td>
          <td class="nowrap">${escapeHtml(r.rgb_light || '—')}</td>
          <td class="nowrap fc-marquee-cell"><span class="fc-marquee-inner">${scen}</span></td>
          <td class="nowrap fc-marquee-cell"><span class="fc-marquee-inner">${updateText}</span></td>
          <td class="nowrap fc-marquee-cell"><span class="fc-marquee-inner">${desc}</span></td>
          <td>
            ${buildQuickBtnHTML('ranking', brand, model, r.model_id, r.condition_id, r.condition_name_zh, 'update_notice')}
          </td>
        </tr>`;
    });

    tbody.innerHTML = html;
    updatesTabLoaded = true;
    updatesTabLastLoad = Date.now();
    if (typeof syncQuickActionButtons === 'function') syncQuickActionButtons();
    updateQuickBtnCompactMode();
  }
  function reloadRecentUpdates(debounce = true) {
    if (debounce) {
      if (_updatesDebounce) clearTimeout(_updatesDebounce);
      return new Promise(resolve => {
        _updatesDebounce = setTimeout(() => resolve(reloadRecentUpdates(false)), 220);
      });
    }
    if (_updatesPending) return Promise.resolve();
    _updatesPending = true;

    const cacheNS = 'recent_updates';
    const payload = {};
    const cached = window.__APP?.cache?.get(cacheNS, payload);
    if (cached && !needReloadUpdates()) {
      applyRecentUpdatesTable(cached.data);
      _updatesPending = false;
      return Promise.resolve();
    }

    const tbody = document.getElementById('recentUpdatesTbody');
    if (tbody && !updatesTabLoaded) {
      tbody.innerHTML = '<tr><td colspan="8" class="text-center text-gray-500 py-6">加载中...</td></tr>';
    }

    return fetch('/api/recent_updates')
      .then(r => r.json())
      .then(j => {
        const n = normalizeApiResponse(j);
        if (!n.ok) {
          if (typeof showError === 'function') showError(n.error_message || '获取近期更新失败');
          return;
        }
        const data = n.data; // { items:[...] }
        window.__APP?.cache?.set(cacheNS, payload, { data }, UPDATES_TTL);
        applyRecentUpdatesTable(data);
      })
      .catch(err => { if (typeof showError === 'function') showError('获取近期更新异常: ' + err.message); })
      .finally(() => { _updatesPending = false; });
  }
  function loadRecentUpdatesIfNeeded() {
    if (!needReloadUpdates()) return;
    if (typeof showLoading === 'function') showLoading('updates-refresh', '加载近期更新...');
    reloadRecentUpdates(false).finally(() => { if (typeof hideLoading === 'function') hideLoading('updates-refresh'); });
  }

  function initRightSegButtonClicks(){
    const root = document.getElementById('rightSubsegContainer');
    if (!root) return;

    // 防抖：防止在一个淡出还没结束时连续切换
    let switching = false;

    root.addEventListener('click', (e) => {
      const btn = safeClosest(e.target, '.fc-seg__btn');
      if (!btn || !root.contains(btn)) return;
      if (switching) return; // 一个切换过程里先不接受下一个点击
      const seg = btn.closest('.fc-seg'); 
      if (!seg) return;

      const targetId = btn.dataset.target;
      if (!targetId) return;

      const paneId = seg.dataset.paneId;
      const pane   = paneId ? document.getElementById(paneId) : null;
      if (!pane) return;

      const panels = Array.from(pane.querySelectorAll('.fc-rank-panel'));
      if (!panels.length) return;

      const nextPanel = panels.find(p => p.id === targetId);
      const currPanel = panels.find(p => p.classList.contains('active'));

      if (nextPanel === currPanel) {
        // 点的就是当前面板，直接更新按钮样式即可
        seg.querySelectorAll('.fc-seg__btn').forEach(b => {
          b.classList.toggle('is-active', b === btn);
        });
        seg.setAttribute('data-active', targetId);
        return;
      }

      // 更新 segmented 按钮激活态
      seg.querySelectorAll('.fc-seg__btn').forEach(b => {
        b.classList.toggle('is-active', b === btn);
      });
      seg.setAttribute('data-active', targetId);

      // 新面板：立即加 active，让它按 CSS 做淡入
      if (nextPanel) {
        nextPanel.classList.add('active');
        // 确保重新触发 fadeIn 动画
        nextPanel.classList.remove('is-leaving');
        nextPanel.style.animation = 'none';
        // 强制 reflow 以重启 animation
        void nextPanel.offsetWidth;
        nextPanel.style.animation = '';
      }

      // 旧面板：如果不存在，就不需要淡出逻辑
      if (!currPanel) {
        switching = false;
        return;
      }

      // 给旧面板加 is-leaving，让 CSS 跑 fadePanelOut
      currPanel.classList.add('is-leaving');

      switching = true;

      const handleAnimEnd = (ev) => {
        if (ev.target !== currPanel) return;
        if (ev.animationName !== 'fadePanelOut') return;

        currPanel.removeEventListener('animationend', handleAnimEnd);
        currPanel.classList.remove('is-leaving');
        currPanel.classList.remove('active');
        switching = false;
      };

      currPanel.addEventListener('animationend', handleAnimEnd);
    });
  }

  // 对外暴露（供其它模块按需调用）
  global.RightPanel.recentUpdates = {
    loadIfNeeded: loadRecentUpdatesIfNeeded,
    reload: () => reloadRecentUpdates(false)
  };
  global.RightPanel.updateSubseg = updateRightSubseg;
  global.RightPanel.updateQuickBtnCompactMode = updateQuickBtnCompactMode;
  
})(window);

if (document.readyState !== 'loading') { window.RightPanel?.init(); }
else { document.addEventListener('DOMContentLoaded', () => window.RightPanel?.init(), { once:true }); }