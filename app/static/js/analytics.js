/* Analytics: visit_start + generic event + query logging
   - Sends /api/visit_start once per session (includes initial theme)
   - Exposes window.Analytics.{logEvent,logQueryPairs,initVisitStartOnce}
   - Auto-tracks:
     * click_source_info on /source-info link
     * click_github_link on GitHub repo link
     * click_theme_toggle when theme toggle button clicked (records target theme)
*/
(function initAnalytics() {
  const PAGE_KEY = 'home';
  const EP = {
    visitStart: '/api/visit_start',
    logEvent: '/api/log_event',
    logQuery: '/api/log_query'
  };

  function jsonBeacon(url, payload) {
    try {
      const data = JSON.stringify(payload || {});
      if (navigator.sendBeacon) {
        const blob = new Blob([data], { type: 'application/json' });
        return navigator.sendBeacon(url, blob);
      }
      // Fallback
      fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: data,
        keepalive: true
      }).catch(() => {});
      return true;
    } catch (_) {
      return false;
    }
  }

  function initVisitStartOnce() {
    try {
      if (sessionStorage.getItem('visit_started') === '1') return;
    } catch (_) {}
    const theme = document.documentElement.getAttribute('data-theme') || 'light';
    const payload = {
      screen_w: (typeof screen !== 'undefined' && screen.width) || null,
      screen_h: (typeof screen !== 'undefined' && screen.height) || null,
      device_pixel_ratio: (typeof window !== 'undefined' && window.devicePixelRatio) || null,
      language: (typeof navigator !== 'undefined' && (navigator.languages && navigator.languages[0])) || (navigator.language || null),
      is_touch: (('ontouchstart' in window) || (navigator.maxTouchPoints > 0)),
      theme
    };
    fetch(EP.visitStart, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      keepalive: true
    }).catch((err) => { console.error("Analytics visit_start failed:", err); }).finally(() => {
      try { sessionStorage.setItem('visit_started', '1'); } catch (_) {}
    });
  }

  function logEvent(eventTypeCode, targetUrl, pageKey = PAGE_KEY) {
    if (!eventTypeCode) return;
    const payload = {
      event_type_code: String(eventTypeCode).slice(0, 64),
      page_key: String(pageKey || PAGE_KEY).slice(0, 64),
      target_url: (targetUrl == null || targetUrl === '') ? null : String(targetUrl).slice(0, 512)
    };
    jsonBeacon(EP.logEvent, payload);
  }

  // source: string, pairs: [{model_id, condition_id}]
  async function logQueryPairs(source, pairs) {
    try {
      const cleaned = Array.isArray(pairs) ? pairs.map(p => ({
        model_id: Number(p.model_id),
        condition_id: Number(p.condition_id)
      })).filter(p => Number.isInteger(p.model_id) && Number.isInteger(p.condition_id)) : [];
      if (!cleaned.length) return;

      const payload = { source: (source || '').slice(0, 64), pairs: cleaned };
      const data = JSON.stringify(payload);

      if (navigator.sendBeacon) {
        const blob = new Blob([data], { type: 'application/json' });
        navigator.sendBeacon(EP.logQuery, blob);
      } else {
        await fetch(EP.logQuery, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: data,
          keepalive: true
        });
      }
    } catch (_) {}
  }

  // Auto-wire: run visit_start once
  initVisitStartOnce();

  // Auto-track clicks for two anchors via data-track-id
  document.addEventListener('click', (e) => {
    const aInfo = e.target.closest && e.target.closest('a[data-track-id="link_source_info"]');
    if (aInfo) {
      logEvent('click_source_info', '/source-info');
    }
  }, true);

  document.addEventListener('click', (e) => {
    const aGit = e.target.closest && e.target.closest('a[data-track-id="link_github_open_source"]');
    if (aGit) {
      const href = aGit.getAttribute('href') || '';
      logEvent('click_github_link', href);
    }
  }, true);

  // Auto-track theme toggle button
  (function hookThemeToggle() {
    const btn = document.getElementById('themeToggle');
    if (!btn) return;
    btn.addEventListener('click', () => {
      try {
        // Infer target theme (toggle)
        const curr = document.documentElement.getAttribute('data-theme') || 'light';
        const next = (curr === 'light') ? 'dark' : 'light';
        logEvent('click_theme_toggle', 'theme:' + next);
      } catch (_) {}
    });
  })();

  // 右侧面板“展开/收起工况”按钮埋点（仅在将要展开时记录）
  document.addEventListener('click', (e) => {
    const toggle = e.target.closest && e.target.closest('.fc-expand-toggle');
    if (!toggle) return;
    const willExpand = toggle.getAttribute('aria-expanded') !== 'true';
    if (!willExpand) return;

    // 取所在行的 model_id
    const tr = toggle.closest('tr');
    const mid = tr && tr.dataset && tr.dataset.modelId ? String(tr.dataset.modelId) : '';
    const target = mid ? `mid:${mid}` : null;

    // 事件名：click_right_panel_expander；把 mid 写到 target_url
    logEvent('click_right_panel_expander', target, 'home');
  }, true);
  
  // Expose API
  window.Analytics = {
    initVisitStartOnce,
    logEvent,
    logQueryPairs
  };
})();