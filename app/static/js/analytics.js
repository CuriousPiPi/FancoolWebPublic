/* Analytics: tab-scoped visit + generic event + query logging
   - Keeps a random visit token in this tab's session storage
   - Exposes window.Analytics.{logEvent,logCurvePairs,logRadarModels,initVisitStartOnce}
   - Auto-tracks:
     * click_source_info on /source-info link
     * click_github_link on GitHub repo link
     * click_theme_toggle when theme toggle button clicked (records target theme)
     * click_ladder_open when ladder modal open button clicked
     * click_ladder_download when ladder download button clicked (records ladder type/name/date)
     * contextmenu_ladder_canvas on right-click / long-press of ladder canvas (best-effort save signal)
     * click_file_share_entry when resource download entry button clicked
*/
(function initAnalytics() {
  if (window.Analytics) return;
  const PAGE_KEY = 'home';
  const EP = {
    visitStart: '/api/visit_start',
    logEvent: '/api/log_event',
    curveSet: '/api/curve_set',
    radarModels: '/api/radar_models',
    advancedSearch: '/api/log_advanced_search'
  };

  const MAX_ATTEMPTS = 3;
  const MAX_QUEUE = 100;
  const REQUEST_TIMEOUT_MS = 8000;
  const VISIT_TOKEN_STORAGE_KEY = 'fc_analytics_visit_token';
  const queue = [];
  let initialized = false;
  let initPromise = null;
  let initAttempts = 0;
  let leaving = false;
  let cached = false;
  let delivering = false;
  const resumeWaiters = [];

  function createVisitToken() {
    try {
      if (window.crypto.randomUUID) return window.crypto.randomUUID();
      const bytes = window.crypto.getRandomValues(new Uint8Array(16));
      bytes[6] = (bytes[6] & 15) | 64;
      bytes[8] = (bytes[8] & 63) | 128;
      const hex = Array.from(bytes, b => b.toString(16).padStart(2, '0')).join('');
      return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
    } catch (_) {
      // Analytics is optional; never substitute a non-random identity.
      return null;
    }
  }

  function loadVisitToken() {
    try {
      const stored = window.sessionStorage.getItem(VISIT_TOKEN_STORAGE_KEY);
      if (typeof stored === 'string' && uuidPattern.test(stored)) return stored;
      const token = createVisitToken();
      if (token) window.sessionStorage.setItem(VISIT_TOKEN_STORAGE_KEY, token);
      return token;
    } catch (_) {
      return createVisitToken();
    }
  }

  const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
  const visitToken = loadVisitToken();

  function withVisitToken(href) {
    try {
      const url = new URL(href, window.location.href);
      if (!visitToken || url.origin !== window.location.origin
          || !/^\/go\/(?:file-share|purchase)\/[^/]+$/.test(url.pathname)) return href;
      url.searchParams.set('visit_token', visitToken);
      return url.pathname + url.search + url.hash;
    } catch (_) { return href; }
  }

  function requestOptions(options = {}) {
    if (!visitToken) return options;
    const headers = new Headers(options.headers || {});
    headers.set('X-Visit-Token', visitToken);
    const result = { ...options, headers };
    if (typeof options.body === 'string' && headers.get('Content-Type')?.includes('application/json')) {
      try { result.body = JSON.stringify({ ...JSON.parse(options.body), visit_token: visitToken }); } catch (_) {}
    }
    return result;
  }

  async function postJSON(url, payload) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
    try {
      const response = await fetch(url, requestOptions({
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
        keepalive: true,
        signal: controller.signal
      }));
      if (!response.ok) throw new Error('Analytics HTTP failure');
      const body = await response.json();
      if (!body || body.success !== true) throw new Error('Analytics API failure');
      return body.data;
    } finally { clearTimeout(timer); }
  }

  function jsonBeacon(url, payload) {
    if (!visitToken) return false;
    try {
      const data = JSON.stringify({ ...payload, visit_token: visitToken });
      if (navigator.sendBeacon) {
        const blob = new Blob([data], { type: 'application/json' });
        if (navigator.sendBeacon(url, blob)) return true;
      }
      fetch(url, requestOptions({
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: data,
        keepalive: true
      })).catch(() => {});
      return true;
    } catch (_) {
      return false;
    }
  }

  async function sendLog(url, payload) {
    for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
      if (cached) await new Promise(resolve => resumeWaiters.push(resolve));
      if (leaving) { jsonBeacon(url, payload); return; }
      try { await postJSON(url, payload); return; } catch (_) {}
      if (attempt + 1 < MAX_ATTEMPTS) await new Promise(resolve => setTimeout(resolve, 250 * (attempt + 1)));
    }
  }

  async function drainQueue() {
    if (!initialized || leaving || cached || delivering) return;
    delivering = true;
    try {
      while (queue.length && !leaving && !cached) {
        const item = queue.shift();
        await sendLog(item.url, item.payload);
      }
    } finally { delivering = false; }
  }

  function logJSON(url, payload) {
    if (!visitToken) return;
    if (leaving) { jsonBeacon(url, payload); return; }
    if (!initialized && initAttempts >= MAX_ATTEMPTS && !initPromise) return;
    if (queue.length >= MAX_QUEUE) queue.shift();
    queue.push({ url, payload });
    void drainQueue();
  }

  function initVisitStartOnce() {
    if (initialized) return Promise.resolve(true);
    if (initPromise) return initPromise;
    if (!visitToken || initAttempts >= MAX_ATTEMPTS) return Promise.resolve(false);
    initPromise = (async () => {
      if (document.prerendering) {
        await new Promise(resolve => document.addEventListener('prerenderingchange', resolve, { once: true }));
      }
      const theme = document.documentElement.getAttribute('data-theme') || 'light';
      const payload = {
        screen_w: (typeof screen !== 'undefined' && screen.width) || null,
        screen_h: (typeof screen !== 'undefined' && screen.height) || null,
        device_pixel_ratio: (typeof window !== 'undefined' && window.devicePixelRatio) || null,
        language: (typeof navigator !== 'undefined' && (navigator.languages && navigator.languages[0])) || (navigator.language || null),
        is_touch: (('ontouchstart' in window) || (navigator.maxTouchPoints > 0)),
        theme
      };
      while (initAttempts < MAX_ATTEMPTS) {
        if (cached) await new Promise(resolve => resumeWaiters.push(resolve));
        initAttempts++;
        try {
          const data = await postJSON(EP.visitStart, payload);
          if (!data || data.visit_token !== visitToken
              || !Number.isSafeInteger(data.visit_id) || data.visit_id <= 0
              || !Number.isSafeInteger(data.visit_index) || data.visit_index <= 0) {
            throw new Error('Invalid analytics visit context');
          }
          initialized = true;
          void drainQueue();
          return true;
        } catch (_) {}
        if (initAttempts < MAX_ATTEMPTS) await new Promise(resolve => setTimeout(resolve, 250 * initAttempts));
      }
      queue.length = 0;
      return false;
    })().finally(() => { initPromise = null; });
    return initPromise;
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
    logJSON(EP.logEvent, payload);
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

  /**
   * Canonical curve-set logging. Sends curve diff to /api/curve_set.
   * eventType must be one of:
   *   condition_activate / restore / model_show / model_add
   *   condition_inactivate / model_remove / model_hide / radar_clear_all / reset_condition
   * source: string, actionId: optional shared batch token
   */
  function logCurvePairs(eventType, pairs, source, actionId) {
    try {
      const cleaned = Array.isArray(pairs) ? pairs.map(p => ({
        model_id: Number(p.model_id),
        condition_id: Number(p.condition_id)
      })).filter(p => Number.isInteger(p.model_id) && Number.isInteger(p.condition_id)) : [];
      if (!cleaned.length) return;

      const payload = {
        event_type: String(eventType || '').slice(0, 32),
        pairs: cleaned,
      };
      if (source) payload.source = String(source).slice(0, 64);
      if (actionId) payload.action_id = String(actionId).slice(0, 64);
      logJSON(EP.curveSet, payload);
    } catch (_) {}
  }

  // action: 'add'|'remove'|'restore'|'clear_all', modelId: number|null, source: string, actionId: string|null
  function logRadarModels(action, modelId, source, actionId) {
    try {
      const payload = { action: String(action).slice(0, 32) };
      if (modelId != null) payload.model_id = Number(modelId);
      if (source) payload.source = String(source).slice(0, 64);
      if (actionId) payload.action_id = String(actionId).slice(0, 64);
      logJSON(EP.radarModels, payload);
    } catch (_) {}
  }

  function logAdvancedSearch(payload, meta = {}) {
    logJSON(EP.advancedSearch, {
      payload: payload || {},
      is_default: !!meta.is_default
    });
  }

  window.addEventListener('pagehide', (event) => {
    cached = !!event.persisted;
    if (cached) return;
    leaving = true;
    for (const resume of resumeWaiters.splice(0)) resume();
    for (const item of queue.splice(0)) jsonBeacon(item.url, item.payload);
  });
  window.addEventListener('pageshow', () => {
    cached = false;
    leaving = false;
    for (const resume of resumeWaiters.splice(0)) resume();
    void drainQueue();
  });

  // Also cover dynamically inserted anchors and middle-click navigation.
  function decorateRedirect(e) {
    const target = e.target && (e.target.nodeType === 1 ? e.target : e.target.parentElement);
    const anchor = target && target.closest && target.closest('a[href]');
    if (!anchor) return;
    const href = anchor.getAttribute('href');
    const decorated = withVisitToken(href);
    if (decorated !== href) anchor.setAttribute('href', decorated);
  }
  document.addEventListener('click', decorateRedirect, true);
  document.addEventListener('auxclick', decorateRedirect, true);
  document.addEventListener('contextmenu', decorateRedirect, true);

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
  
  // 天梯图打点：打开弹窗、下载图片、右键 canvas
  (function hookLadderAnalytics() {
    const LADDER_DISPLAY_NAMES = {
      composite: '风扇库综合天梯图',
      intake_exhaust: '风扇库进排气天梯图',
      radiator: '风扇库吹冷排天梯图',
    };
    const LADDER_CONTEXTMENU_THROTTLE_MS = 10000;
    let lastLadderContextmenu = { key: null, ts: 0 };

    function getActiveLadderTab() {
      return document.querySelector('#ladderTabs .fc-sr-tab.active')
        || document.querySelector('#ladderTabs .fc-sr-tab[aria-selected="true"]');
    }

    function extractLadderDate() {
      const hint = document.getElementById('ladderUpdateHint');
      const text = ((hint && hint.textContent) || '').trim();
      if (!text) return null;
      const marker = '当前榜单日期：';
      const idx = text.indexOf(marker);
      if (idx >= 0) return text.slice(idx + marker.length).trim() || null;
      const match = text.match(/\d{4}[-/]\d{1,2}[-/]\d{1,2}/);
      return match ? match[0] : null;
    }

    function getLadderMeta(extra) {
      const tab = getActiveLadderTab();
      const ladderType = ((tab && tab.getAttribute('data-ladder')) || '').trim() || 'composite';
      const ladderName = LADDER_DISPLAY_NAMES[ladderType] || ((tab && tab.textContent) || '').trim() || ladderType;
      const meta = {
        ladder_type: ladderType,
        ladder_name: ladderName,
        ladder_date: extractLadderDate(),
      };
      if (extra && typeof extra === 'object') Object.assign(meta, extra);
      return meta;
    }

    function shouldLogLadderContextmenu(meta) {
      const key = [
        meta && meta.ladder_type ? meta.ladder_type : '',
        meta && meta.ladder_name ? meta.ladder_name : '',
        meta && meta.ladder_date ? meta.ladder_date : ''
      ].join('|');
      const now = Date.now();
      if (lastLadderContextmenu.key === key && now - lastLadderContextmenu.ts < LADDER_CONTEXTMENU_THROTTLE_MS) {
        return false;
      }
      lastLadderContextmenu = { key, ts: now };
      return true;
    }

    // 点击右侧面板「天梯图」按钮 / 弹窗内「下载图片」按钮
    document.addEventListener('click', (e) => {
      if (!e.target || !e.target.closest) return;
      if (e.target.closest('#ladderOpenBtn')) {
        try { logEvent({ event_type_code: 'click_ladder_open', payload_json: { source: 'open_button' } }); } catch (_) {}
      } else if (e.target.closest('#ladderDownloadBtn')) {
        try { logEvent({ event_type_code: 'click_ladder_download', payload_json: getLadderMeta({ source: 'download_button' }) }); } catch (_) {}
      }
    }, true);

    // 右键/长按天梯图 canvas（尽力打点，不阻止默认行为）
    document.addEventListener('contextmenu', (e) => {
      if (e.target && e.target.closest && e.target.closest('#ladderCanvas')) {
        try {
          const meta = getLadderMeta({ source: 'canvas_contextmenu' });
          if (!shouldLogLadderContextmenu(meta)) return;
          logEvent({ event_type_code: 'contextmenu_ladder_canvas', payload_json: meta });
        } catch (_) {}
      }
    }, true);
  })();

  // 点击右侧面板「资源下载」入口按钮
  (function hookFileShareEntryAnalytics() {
    document.addEventListener('click', (e) => {
      const target = e.target && (e.target.nodeType === 1 ? e.target : e.target.parentElement);
      if (!target || !target.closest) return;
      if (target.closest('#fileShareEntryBtn')) {
        try { logEvent({ event_type_code: 'click_file_share_entry', payload_json: { source: 'entry_button' } }); } catch (_) {}
      }
    }, true);
  })();

  // Expose API
  window.Analytics = {
    initVisitStartOnce,
    withVisitToken,
    logEvent,
    logPlayAudio,
    logCurvePairs,
    logRadarModels,
    logAdvancedSearch
  };
  void initVisitStartOnce();
})();