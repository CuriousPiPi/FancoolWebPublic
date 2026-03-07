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

  function logEvent({ event_type_code, page_key = PAGE_KEY, target_url = null, model_id, condition_id, payload_json } = {}) {
    if (!event_type_code) return;
    const payload = {
      event_type_code: String(event_type_code).slice(0, 64),
      page_key: String(page_key || PAGE_KEY).slice(0, 64),
      target_url: (target_url == null || target_url === '') ? null : String(target_url).slice(0, 512)
    };
    if (model_id != null) payload.model_id = model_id;
    if (condition_id != null) payload.condition_id = condition_id;
    if (payload_json != null) payload.payload_json = payload_json;
    jsonBeacon(EP.logEvent, payload);
  }

  // Throttle cache for click_play_audio: key -> last timestamp
  const _playAudioThrottleCache = {};
  const PLAY_AUDIO_THROTTLE_MS = 1000;
  const PLAY_AUDIO_THROTTLE_MAX_KEYS = 200;

  function logPlayAudio(modelId, conditionId, xAxisMode, pointerX, rpm, db) {
    const roundedRpm = Math.round(rpm);
    const roundedDb = db != null ? Math.round(db * 10) / 10 : null;
    // rpm mode: round pointer_x to integer; db mode: round to 0.1
    const roundedX = (xAxisMode === 'db') ? Math.round(pointerX * 10) / 10 : Math.round(pointerX);
    const key = `${modelId}|${conditionId}|${xAxisMode}|${roundedX}|${roundedRpm}|${roundedDb}`;
    const now = Date.now();
    if (_playAudioThrottleCache[key] && now - _playAudioThrottleCache[key] < PLAY_AUDIO_THROTTLE_MS) return;
    // Evict oldest entries when cache grows too large
    const keys = Object.keys(_playAudioThrottleCache);
    if (keys.length >= PLAY_AUDIO_THROTTLE_MAX_KEYS) {
      const oldest = keys.sort((a, b) => _playAudioThrottleCache[a] - _playAudioThrottleCache[b]);
      for (let i = 0; i < Math.floor(PLAY_AUDIO_THROTTLE_MAX_KEYS / 2); i++) delete _playAudioThrottleCache[oldest[i]];
    }
    _playAudioThrottleCache[key] = now;
    logEvent({
      event_type_code: 'click_play_audio',
      model_id: modelId,
      condition_id: conditionId,
      payload_json: { x_axis_mode: xAxisMode, pointer_x: roundedX, rpm: roundedRpm, db: roundedDb }
    });
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
      logEvent({ event_type_code: 'click_source_info', target_url: '/source-info' });
    }
  }, true);

  document.addEventListener('click', (e) => {
    const aGit = e.target.closest && e.target.closest('a[data-track-id="link_github_open_source"]');
    if (aGit) {
      const href = aGit.getAttribute('href') || '';
      logEvent({ event_type_code: 'click_github_link', target_url: href });
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
        logEvent({ event_type_code: 'click_theme_toggle', target_url: 'theme:' + next });
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
    const mid = tr && tr.dataset && tr.dataset.modelId ? parseInt(tr.dataset.modelId, 10) : null;

    // 事件名：click_right_panel_expander；把 model_id 写到 extra payload
    logEvent({ event_type_code: 'click_right_panel_expander', ...(mid != null ? { model_id: mid } : {}) });
  }, true);
  
  // Expose API
  window.Analytics = {
    initVisitStartOnce,
    logEvent,
    logPlayAudio,
    logQueryPairs
  };
})();