export class FanGameError extends Error {
  constructor(message, code = 'NETWORK_ERROR', status = 0) {
    super(message);
    this.code = code;
    this.status = status;
  }
}

const TUNING_KEYS = [
  'rootStrength', 'hubFriction', 'bearingInitialResistance', 'bearingResistanceSlope',
  'ccwFingerForce', 'cwFingerForce', 'fingerCompression', 'coggingStrength', 'rotorInertia',
  'windNoiseGain', 'bladeToneGain', 'ballRustleGain', 'airflowSoundExponent',
];

function isBusinessConflict(error) {
  return ['INSUFFICIENT_FUNDS', 'PRINT_NOT_READY', 'PRINT_PENDING', 'POWDER_UNAVAILABLE',
    'NOT_OWNED', 'PRINT_REQUIRED', 'INVALID_ITEM', 'INVALID_ACTION'].includes(error.code);
}

export function makeGameKey() {
  const random = globalThis.crypto;
  if (typeof random?.randomUUID === 'function') return random.randomUUID();
  if (typeof random?.getRandomValues !== 'function') {
    throw new FanGameError('浏览器不支持安全随机数，请更新浏览器并使用 HTTPS。', 'UNSUPPORTED_BROWSER');
  }
  const bytes = random.getRandomValues(new Uint8Array(16));
  bytes[6] = (bytes[6] & 15) | 64;
  bytes[8] = (bytes[8] & 63) | 128;
  const hex = Array.from(bytes, byte => byte.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

export class FanGameClient {
  constructor({ fetcher = (...args) => fetch(...args), makeKey = makeGameKey,
    monotonicNow = () => performance.now() } = {}) {
    this.fetcher = fetcher;
    this.makeKey = makeKey;
    this.monotonicNow = monotonicNow;
    this.clock = null;
    this.snapshot = null;
    this.catalog = null;
    this.ready = false;
    this.seq = 0;
    this.slices = [];
    this.draft = null;
    this.report = null;
    this.reportPromise = null;
    this.operation = null;
    this.busy = false;
    this.hooks = {};
  }

  async request(path, body, keepalive = false) {
    let response;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10000);
    let receivedAt;
    let result;
    try {
      try {
        response = await this.fetcher(`/api/fan-game/${path}`, {
          method: body ? 'POST' : 'GET',
          credentials: 'same-origin',
          headers: body ? { 'Content-Type': 'application/json', 'X-CSRF-Token': this.snapshot.csrf_token } : {},
          body: body ? JSON.stringify(body) : undefined,
          keepalive,
          cache: 'no-store',
          signal: controller.signal,
        });
        receivedAt = this.monotonicNow();
      } catch (error) {
        throw new FanGameError(`连接服务器失败：${error.message}`);
      }
      try {
        result = await response.json();
      } catch (error) {
        throw new FanGameError('服务器返回了无效响应，请检查部署或稍后重试。', 'INVALID_RESPONSE', response.status);
      }
    } finally {
      clearTimeout(timeout);
    }
    if (!response.ok || result.success !== true) {
      throw new FanGameError(result.error_message || '服务器拒绝了请求。', result.error_code, response.status);
    }
    if (!result.data || !result.data.campaign || !result.data.catalog || !result.data.csrf_token) {
      throw new FanGameError('服务器存档格式不完整。', 'INVALID_RESPONSE', response.status);
    }
    const tuning = result.data.catalog.tuning;
    if (!tuning || !TUNING_KEYS.every(key => Number.isFinite(tuning[key]) && tuning[key] >= 0)) {
      throw new FanGameError('服务器物理与音效参数无效。', 'INVALID_RESPONSE', response.status);
    }
    const records = result.data.server_records;
    if (!records || !['today_rpm', 'week_rpm', 'today_turns', 'week_turns', 'today_airflow', 'week_airflow']
      .every(key => Number.isFinite(records[key]) && records[key] >= 0)) {
      throw new FanGameError('服务器记录格式不完整。', 'INVALID_RESPONSE', response.status);
    }
    // Only GET state is fresh; idempotent POST responses can contain an old timestamp.
    if (path === 'state') {
      if (!Number.isFinite(result.data.server_time_ms)) {
        throw new FanGameError('服务器存档缺少有效时间。', 'INVALID_RESPONSE', response.status);
      }
      this.clock = { serverTime: result.data.server_time_ms, receivedAt };
    }
    return result.data;
  }

  now() {
    if (!this.clock) throw new FanGameError('服务器时钟尚未初始化。', 'INVALID_STATE');
    return this.clock.serverTime + this.monotonicNow() - this.clock.receivedAt;
  }

  adopt(snapshot, replace = false) {
    this.snapshot = snapshot;
    this.catalog = snapshot.catalog;
    this.hooks.apply?.(snapshot, { replace, pending: this.pending() });
  }

  async initialize() {
    this.adopt(await this.request('state'));
    if (!this.sandbox) await this.startSession();
  }

  async startSession() {
    const data = await this.request('session/start', { idempotency_key: this.makeKey() });
    this.sessionId = data.play_session_id;
    this.sessionStartedAt = this.monotonicNow();
    this.seq = data.seq;
    this.slices = [];
    this.draft = null;
    this.report = null;
    this.adopt(data, true);
    this.ready = true;
  }

  connect(hooks) {
    this.hooks = hooks;
    this.timer = window.setInterval(() => {
      if (!document.hidden && this.ready && !this.busy) {
        void this.flush().catch(error => this.fail(error));
      }
    }, 5000);
  }

  fail(error) {
    this.ready = false;
    this.hooks.pause?.();
    this.hooks.status?.(`${error.message} 转动与结算已暂停，请重新连接。`, true);
    console.warn('Fan challenge request failed', error);
  }

  record(sample) {
    if (!this.ready || this.busy) return;
    if (this.draft && (this.draft.broken_blades.length !== sample.broken_blades.length
      || this.draft.broken_blades.some(blade => !sample.broken_blades.includes(blade)))) {
      this.slices.push(this.draft);
      this.draft = null;
    }
    let remaining = sample.duration_ms;
    while (remaining > 1e-8) {
      if (!this.draft) {
        this.draft = { duration_ms: 0, turns: 0, forward_turns: 0, peak_rpm: 0, peak_forward_rpm: 0, peak_airflow: 0, end_rpm: 0,
          press_ms: 0, contact_ms: 0, effort_ms: 0, impact_wear: 0, broken_blades: [] };
      }
      const take = Math.min(1000 - this.draft.duration_ms, remaining);
      const fraction = take / sample.duration_ms;
      for (const key of ['turns', 'forward_turns', 'press_ms', 'contact_ms', 'effort_ms', 'impact_wear']) {
        this.draft[key] += sample[key] * fraction;
      }
      this.draft.duration_ms += take;
      this.draft.peak_rpm = Math.max(this.draft.peak_rpm, sample.peak_rpm);
      this.draft.peak_forward_rpm = Math.max(this.draft.peak_forward_rpm, sample.peak_forward_rpm);
      this.draft.peak_airflow = Math.max(this.draft.peak_airflow, sample.peak_airflow);
      this.draft.end_rpm = sample.end_rpm;
      this.draft.broken_blades = [...sample.broken_blades];
      remaining -= take;
      if (this.draft.duration_ms >= 1000 - 1e-8) {
        this.slices.push(this.draft);
        this.draft = null;
      }
    }
    if (this.slices.length >= 12 && !this.reportPromise) {
      void this.flush().catch(error => this.fail(error));
    }
  }

  pending() {
    return [...this.slices, ...(this.draft ? [this.draft] : [])];
  }

  async flush() {
    if (this.reportPromise) return this.reportPromise;
    if (!this.report) {
      const slices = this.pending();
      if (!slices.length) {
        // An idle heartbeat keeps the single active session alive without earning currency.
        slices.push({ duration_ms: 1, turns: 0, forward_turns: 0, peak_rpm: 0, peak_forward_rpm: 0, peak_airflow: 0, end_rpm: 0,
          press_ms: 0, contact_ms: 0, effort_ms: 0, impact_wear: 0,
          broken_blades: [...this.snapshot.campaign.brokenBlades] });
      }
      this.slices = [];
      this.draft = null;
      this.report = {
        play_session_id: this.sessionId,
        seq: this.seq + 1,
        revision: this.snapshot.revision,
        catalog_version: this.catalog.version,
        observed_ms: Math.max(0, this.monotonicNow() - this.sessionStartedAt),
        idempotency_key: this.makeKey(),
        slices,
      };
    }
    this.reportPromise = (async () => {
      const data = await this.request('session/report', this.report, true);
      this.seq = data.seq;
      this.report = null;
      this.adopt(data);
      this.hooks.status?.('存档绑定此浏览器 cookie；已同步，未同步收益不可消费。', false, false);
      return data;
    })().finally(() => { this.reportPromise = null; });
    return this.reportPromise;
  }

  async perform(action, id) {
    if (this.busy || !this.ready) return;
    this.busy = true;
    this.hooks.pause?.();
    try {
      await this.flush();
      if (this.pending().length) await this.flush();
      this.operation = {
        path: ['buyMaterial', 'buyBearing', 'buyHand', 'buyPowder', 'equipMaterial', 'equipBearing', 'equipHand']
          .includes(action) ? 'purchase' : 'service',
        body: { action, id, revision: this.snapshot.revision, idempotency_key: this.makeKey() },
      };
      await this.completeOperation();
    } catch (error) {
      if (isBusinessConflict(error) || this.operation && error.status === 400) {
        this.operation = null;
        this.hooks.status?.(error.message, true);
      } else {
        this.fail(error);
      }
    } finally {
      this.busy = false;
    }
  }

  async completeOperation() {
    this.adopt(await this.request(this.operation.path, this.operation.body), true);
    this.operation = null;
    await this.startSession();
    this.hooks.status?.('操作完成，装备与钱包已保存到服务器。', false);
  }

  async reconnect() {
    if (this.busy) return;
    this.busy = true;
    this.ready = false;
    this.hooks.pause?.();
    try {
      if (this.report || this.operation || this.pending().length) {
        const current = await this.request('state');
        this.snapshot.csrf_token = current.csrf_token;
      }
      if (this.report || this.pending().length) {
        try {
          await this.flush();
          if (this.pending().length) await this.flush();
        } catch (error) {
          if (error.status !== 409 && error.status !== 400) throw error;
          this.report = null;
          this.hooks.status?.('旧会话已失效，该段未核定收益无法保存。正在恢复服务器存档。', true);
        }
      }
      if (this.operation) {
        try {
          await this.completeOperation();
        } catch (error) {
          if (!isBusinessConflict(error) && error.status !== 409 && error.status !== 400) throw error;
          this.operation = null;
          this.hooks.status?.(`${error.message} 操作未执行，正在恢复服务器存档。`, true);
        }
      }
      if (!this.ready) {
        this.adopt(await this.request('state'), true);
        await this.startSession();
      }
      this.hooks.status?.('已恢复服务器存档并重新连接。', false, false);
    } catch (error) {
      this.fail(error);
    } finally {
      this.busy = false;
    }
  }
}
