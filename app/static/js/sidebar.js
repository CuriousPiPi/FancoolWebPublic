/* =========================================================
   Sidebar module (overlay / gesture / toggle / resizers / auto layout)
   Exposes: window.__APP.sidebar, and global helpers used by main code.
   ========================================================= */
(function initSidebarModule(){
  const $ = (s) => (window.__APP && window.__APP.dom && window.__APP.dom.one) ? window.__APP.dom.one(s) : document.querySelector(s);

  // Nodes
  const sidebar      = $('#sidebar');
  const sidebarToggle= document.getElementById('sidebar-toggle');
  const mainContent  = $('#main-content');
  const resizer      = document.getElementById('sidebar-resizer');

  // Helper function to get viewport width consistently
  function getViewportWidth() {
    return window.innerWidth || document.documentElement.clientWidth || 0;
  }

  // Sidebar auto-expand breakpoint
  // This value is kept separate from chart-renderer.js NARROW_BREAKPOINT but set to the same
  // value (1024px) to maintain consistent layout behavior between sidebar and chart components.
  const SIDEBAR_AUTO_EXPAND_BREAKPOINT = 1024;

  // A11y focus trap (used by overlay open/close)
  const a11yFocusTrap = (function(){
    let container = null;
    let lastFocused = null;
    let bound = false;
    function focusableElements(root){
      return Array.from(root.querySelectorAll(
        'a[href],button:not([disabled]),input:not([disabled]),select:not([disabled]),textarea:not([disabled]),[tabindex]:not([tabindex="-1"])'
      )).filter(el => el.offsetParent !== null);
    }
    function handleKey(e){
      if (e.key !== 'Tab') return;
      if (!container) return;
      const list = focusableElements(container);
      if (!list.length) {
        e.preventDefault();
        container.focus();
        return;
      }
      const first = list[0];
      const last = list[list.length - 1];
      if (e.shiftKey) {
        if (document.activeElement === first) {
          e.preventDefault();
          last.focus();
        }
      } else {
        if (document.activeElement === last) {
          e.preventDefault();
          first.focus();
        }
      }
    }
    return {
      activate(root){
        if (!root) return;
        container = root;
        lastFocused = document.activeElement;
        const list = focusableElements(root);
        (list[0] || root).focus({ preventScroll:true });
        if (!bound){
          document.addEventListener('keydown', handleKey, true);
          bound = true;
        }
      },
      deactivate(){
        if (bound){
          document.removeEventListener('keydown', handleKey, true);
          bound = false;
        }
        if (lastFocused && typeof lastFocused.focus === 'function') {
          try { lastFocused.focus({ preventScroll:true }); } catch(_){}
        }
        container = null;
        lastFocused = null;
      }
    };
  })();

  // Helper to check overlay mode
  function isOverlayMode(){
    return document.documentElement.classList.contains('sidebar-overlay-mode');
  }

  // Toggle button UI
  function refreshToggleUI(){
    const btn = document.getElementById('sidebar-toggle');
    if (!btn || !sidebar) return;
    const collapsed = sidebar.classList.contains('collapsed');
    btn.setAttribute('aria-label', collapsed ? '展开侧栏' : '收起侧栏');
    btn.setAttribute('aria-expanded', String(!collapsed));
  }

  //   (mobile) 初始化
  (function initSidebarOverlayModeOnce() {
    const vw = getViewportWidth();
    if (vw >= 600) return;
    const root = document.documentElement;
    root.classList.add('sidebar-overlay-mode');
    if (!sidebar) return;
    if (!sidebar.classList.contains('collapsed')) sidebar.classList.add('collapsed');

    let bodyLockCount = 0;
    let prevBodyOverflow = '';
    function lockBodyScroll() {
      if (bodyLockCount === 0) {
        prevBodyOverflow = document.body.style.overflow;
        document.body.style.overflow = 'hidden';
      }
      bodyLockCount++;
    }
    function unlockBodyScroll() {
      bodyLockCount = Math.max(0, bodyLockCount - 1);
      if (bodyLockCount === 0) {
        document.body.style.overflow = prevBodyOverflow;
      }
    }

    // 改为 no-op：不再创建/移除遮罩层
    function addBackdrop() { /* no-op */ }
    function removeBackdrop() { /* no-op */ }

    function overlayOpenSidebar() {
      if (!isOverlayMode()) return;
      if (!sidebar) return;
      sidebar.classList.remove('collapsed');
      addBackdrop();              // 现在为 no-op
      lockBodyScroll();
      ensureGestureZone();
      a11yFocusTrap.activate(sidebar);
      refreshToggleUI();
    }
    function overlayCloseSidebar() {
      if (!isOverlayMode()) return;
      if (!sidebar) return;
      if (!sidebar.classList.contains('collapsed')) {
        sidebar.classList.add('collapsed');
        removeBackdrop();         // 现在为 no-op
        unlockBodyScroll();
        a11yFocusTrap.deactivate();
        const mc = document.getElementById('main-content');
        if (mc) mc.style.marginLeft = '';
        refreshToggleUI();
      }
    }
    function overlayToggleSidebar() {
      if (!sidebar) return;
      if (sidebar.classList.contains('collapsed')) overlayOpenSidebar(); else overlayCloseSidebar();
    }

    window.overlayOpenSidebar = overlayOpenSidebar;
    window.overlayCloseSidebar = overlayCloseSidebar;
    window.overlayToggleSidebar = overlayToggleSidebar;

    document.addEventListener('keydown', (e)=>{
      if (e.key === 'Escape') overlayCloseSidebar();
    });
  })();

  // 右缘关闭手势热区（仅 overlay 模式）
  (function initOverlayCloseGestureZone() {
    const root = document.documentElement;
    if (!root.classList.contains('sidebar-overlay-mode')) {
      window.addEventListener('resize', tryLateInit, { once: true });
      return;
    }
    setup();
    function tryLateInit() {
      if (window.innerWidth < 600) setup();
    }
    function setup() {
      window.ensureGestureZone = function ensureGestureZone() {
        if (!sidebar) return;
        if (document.getElementById('sidebar-gesture-close-zone')) return;
        const zone = document.createElement('div');
        zone.id = 'sidebar-gesture-close-zone';
        zone.setAttribute('role','presentation');
        sidebar.appendChild(zone);
        bindZoneEvents(zone, sidebar);
      };
      window.ensureGestureZone();
    }

    const MIN_DRAG_X = 12;
    const MAX_SLOPE = 0.65;
    const CLOSE_RATIO = 0.30;
    const VELOCITY_CLOSE_PX_PER_MS = -0.8;
    const MIN_FLING_DISTANCE = 24;

    function bindZoneEvents(zone, sidebarEl) {
      let drag = null;
      function backdrop() { return document.querySelector('.sidebar-overlay-backdrop'); }
      function pt(e) {
        if (e.changedTouches && e.changedTouches.length) {
          const t = e.changedTouches[0];
          return { x: t.clientX, y: t.clientY };
        }
        return { x: e.clientX, y: e.clientY };
      }
      zone.addEventListener('pointerdown', (e)=>{
        if (e.pointerType === 'mouse') return;
        if (sidebarEl.classList.contains('collapsed')) return;
        const p = pt(e);
        drag = {
          startX: p.x,
          startY: p.y,
          lastX: p.x,
          lastY: p.y,
          width: sidebarEl.getBoundingClientRect().width,
          dragging: false,
          pointerId: e.pointerId,
          trace: [{ x: p.x, t: performance.now() }]
        };
        try { zone.setPointerCapture(e.pointerId); } catch(err){ console.error("Failed to setPointerCapture:", err); }
      }, { passive:true });

      zone.addEventListener('pointermove', (e)=>{
        if (!drag || drag.pointerId !== e.pointerId) return;
        const p = pt(e);
        drag.lastX = p.x; drag.lastY = p.y;
        const dx = p.x - drag.startX;
        const dy = p.y - drag.startY;
        if (!drag.dragging) {
          if (dx < -MIN_DRAG_X) {
            const slope = Math.abs(dy / dx);
            if (slope <= MAX_SLOPE) {
              drag.dragging = true;
              sidebarEl.style.transition='none';
            } else {
              drag = null;
            }
          }
          return;
        }
        e.preventDefault();
        const limited = Math.max(-drag.width, dx);
        sidebarEl.style.transform = `translateX(${limited}px)`;
        const bd = backdrop();
        if (bd) {
          const ratio = Math.max(0, Math.min(1, 1 + limited / drag.width));
          const eased = (function easeOutQuad(t){ return 1 - (1 - t)*(1 - t); })(ratio);
          const op = 0.8 * eased;
          bd.style.opacity = op.toFixed(3);
          const now = performance.now();
          drag.trace.push({ x: p.x, t: now });
          if (drag.trace.length > 5) drag.trace.shift();
        }
      }, { passive:false });

      function finishDrag() {
        if (!drag) return;
        const dx = drag.lastX - drag.startX;
        const dist = Math.abs(dx);
        let shouldClose = dist > drag.width * CLOSE_RATIO;

        if (!shouldClose) {
          if (dist > MIN_FLING_DISTANCE && drag.trace && drag.trace.length >= 2) {
            const a = drag.trace[drag.trace.length - 2];
            const b = drag.trace[drag.trace.length - 1];
            const dt = Math.max(1, b.t - a.t);
            const vx = (b.x - a.x) / dt;
            if (vx <= VELOCITY_CLOSE_PX_PER_MS) {
              shouldClose = true;
            }
          }
        }
        sidebarEl.style.transition='';
        if (shouldClose) {
          window.overlayCloseSidebar && window.overlayCloseSidebar();
          requestAnimationFrame(()=> { sidebarEl.style.transform=''; });
        } else {
          sidebarEl.style.transform='translateX(0)';
          const bd = document.querySelector('.sidebar-overlay-backdrop'); if (bd) bd.style.opacity='';
          requestAnimationFrame(()=>{
            if (!sidebarEl.classList.contains('collapsed')) sidebarEl.style.transform='';
          });
        }
        drag = null;
      }
      zone.addEventListener('pointerup', (e)=>{
        if (!drag || drag.pointerId !== e.pointerId) return;
        if (drag.dragging) finishDrag(); else drag = null;
      }, { passive:true });
      zone.addEventListener('pointercancel', (e)=>{
        if (!drag || drag.pointerId !== e.pointerId) return;
        if (drag.dragging) finishDrag(); else drag = null;
      }, { passive:true });
    }
  })();

  (function autoExpandSidebarOnDesktop(){
    if (!sidebar || !mainContent) return;
    if (isOverlayMode()) return; // overlay 模式保持折叠
    // Only expand by default when viewport width >= breakpoint
    const vw = getViewportWidth();
    if (vw < SIDEBAR_AUTO_EXPAND_BREAKPOINT) return; // 保持折叠状态
    requestAnimationFrame(() => {
      if (sidebar.classList.contains('collapsed')) {
        sidebar.classList.remove('collapsed');
        currentSidebarWidth = sidebar.getBoundingClientRect().width || SIDEBAR_MIN_W;
        mainContent.style.marginLeft = currentSidebarWidth + 'px';
        isCollapsed = false;
        refreshToggleUI();
        if (typeof window.scheduleAdjust === 'function') window.scheduleAdjust();
        if (typeof window.syncTopTabsViewportHeight === 'function') window.syncTopTabsViewportHeight();
      }
    });
  })();

  // 展开/收起（含非 overlay 模式布局调整）
  let currentSidebarWidth = sidebar?.getBoundingClientRect().width || 0;
  let isCollapsed = sidebar?.classList.contains('collapsed') || false;

  function expandSidebarIfCollapsed(){
    if (!sidebar) return;
    if (isOverlayMode()) {
      if (sidebar.classList.contains('collapsed')) {
        window.overlayOpenSidebar && window.overlayOpenSidebar();
      }
      return;
    }
    if (isCollapsed){
      sidebar.classList.remove('collapsed');
      if (mainContent) mainContent.style.marginLeft = currentSidebarWidth + 'px';
      isCollapsed = false;
      setTimeout(()=>window.__APP?.modules?.chart?.resizeChart && window.__APP.modules.chart.resizeChart(), 300);
      requestAnimationFrame(() => {
        if (typeof window.syncTopTabsViewportHeight === 'function') window.syncTopTabsViewportHeight();
      });
      refreshToggleUI();
    }
  }

  // 监听侧栏 collapsed class 变化，同步刷新切换按钮 UI
  if (sidebar){
    const mo = new MutationObserver(muts=>{
      for (const m of muts){
        if (m.type === 'attributes' && m.attributeName === 'class'){
          refreshToggleUI();
        }
      }
    });
    mo.observe(sidebar, { attributes:true });
  }

  // 按钮事件
  sidebarToggle?.addEventListener('click', ()=>{
    markSidebarToggleClicked();
    if (!sidebar || !mainContent) return;
    if (isOverlayMode()) {
      window.overlayToggleSidebar && window.overlayToggleSidebar();
      return;
    }
    if (isCollapsed){
      expandSidebarIfCollapsed();
    } else {
      currentSidebarWidth = sidebar.getBoundingClientRect().width;
      sidebar.classList.add('collapsed');
      mainContent.style.marginLeft='0';
      isCollapsed = true;
    }
    refreshToggleUI();
  });
  refreshToggleUI();

  // 宽度拖拽
  const SIDEBAR_MIN_W = 300;
  const SIDEBAR_MAX_W = 700;
  if (resizer && sidebar && mainContent){
    let dragging=false, startX=0, startW=0, rafId=null;
    function applyWidth(w){
      sidebar.style.width = w + 'px';
      if (!isCollapsed){
        mainContent.style.marginLeft = w + 'px';
        currentSidebarWidth = w;
        if (typeof window.updateSplitterRails === 'function') window.updateSplitterRails();
      }
    }
    function dragStart(clientX){
      dragging = true;
      startX   = clientX;
      startW   = sidebar.getBoundingClientRect().width;
      document.body.classList.add('resizing-sidebar');
      document.body.classList.add('sidebar-hdragging');
      document.body.style.userSelect = 'none';
    }
    function dragMove(clientX){
      if (!dragging) return;
      const dx = clientX - startX;
      let newW = startW + dx;
      newW = Math.max(SIDEBAR_MIN_W, Math.min(SIDEBAR_MAX_W, newW));
      if (!rafId){
        rafId = requestAnimationFrame(()=>{
          applyWidth(newW);
          rafId = null;
        });
      }
    }
    function dragEnd(){
          dragging = false;
          document.body.classList.remove('resizing-sidebar');
          document.body.classList.remove('sidebar-hdragging');
          document.body.style.userSelect = '';
          document.removeEventListener('mousemove', onMouseMove);
          document.removeEventListener('mouseup',   onMouseUp);
          window.removeEventListener('pointermove', onPtrMove);
          window.removeEventListener('pointerup',   onPtrUp);
          window.removeEventListener('pointercancel', onPtrUp);
          setTimeout(()=>window.__APP?.modules?.chart?.resizeChart && window.__APP.modules.chart.resizeChart(),120);
          requestAnimationFrame(()=>{
            if (typeof window.syncTopTabsViewportHeight === 'function') window.syncTopTabsViewportHeight();
          });
          if (typeof window.scheduleAdjust === 'function') window.scheduleAdjust();
        }

    function onMouseMove(ev){
      if (ev.cancelable) ev.preventDefault();
      dragMove(ev.clientX);
    }
    function onMouseUp(){ dragEnd(); }
    resizer.addEventListener('mousedown', e=>{
      if (isCollapsed) return;
      if (isOverlayMode()) return;
      e.preventDefault();
      dragStart(e.clientX);
      document.addEventListener('mousemove', onMouseMove, { passive:false });
      document.addEventListener('mouseup',   onMouseUp,   { passive:true  });
    });
    function onPtrMove(e){
      if (!dragging) return;
      if (e.cancelable) e.preventDefault();
      dragMove(e.clientX);
    }
    function onPtrUp(){ dragEnd(); }
    resizer.addEventListener('pointerdown', e=>{
      if (isCollapsed) return;
      if (isOverlayMode()) return;
      if (e.pointerType !== 'touch' && e.pointerType !== 'pen') return;
      e.preventDefault();
      dragStart(e.clientX);
      window.addEventListener('pointermove', onPtrMove,   { passive:false });
      window.addEventListener('pointerup',   onPtrUp,     { passive:true  });
      window.addEventListener('pointercancel', onPtrUp,   { passive:true  });
    });
  }

  const LS_KEY_SIDEBAR_TOGGLE_CLICKED = 'sidebar_toggle_clicked';
  function markSidebarToggleClicked(){
    try { localStorage.setItem(LS_KEY_SIDEBAR_TOGGLE_CLICKED, '1'); } catch(_) {}
  }
  function userHasClickedSidebarToggle(){
    try { return localStorage.getItem(LS_KEY_SIDEBAR_TOGGLE_CLICKED) === '1'; } catch(_) { return false; }
  }
  function maybeAutoOpenSidebarOnAdd(){
    if (userHasClickedSidebarToggle()) return;
    expandSidebarIfCollapsed();
  }

  // Export API
  window.__APP = window.__APP || {};
  window.__APP.sidebar = {
    open: window.overlayOpenSidebar,
    close: window.overlayCloseSidebar,
    toggle: window.overlayToggleSidebar,
    ensureGestureZone: window.ensureGestureZone || function(){},
    refreshToggleUI,
    expandSidebarIfCollapsed,
    markSidebarToggleClicked,
    userHasClickedSidebarToggle,
    maybeAutoOpenSidebarOnAdd
  };
})();