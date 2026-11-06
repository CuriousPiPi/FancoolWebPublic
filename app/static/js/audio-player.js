(function () {
  'use strict';

  const CROSSFADE = 0.05;
  const cache = new Map();
  let context = null;
  let master = null;
  let timer = null;
  let controller = null;
  let generation = 0;
  let state = null;
  let nodes = [];

  function getContext() {
    if (!context) {
      context = new (window.AudioContext || window.webkitAudioContext)();
      master = context.createGain();
      master.gain.value = 5;
      const compressor = context.createDynamicsCompressor();
      compressor.threshold.value = -12;
      compressor.knee.value = 6;
      compressor.ratio.value = 6;
      compressor.attack.value = 0.003;
      compressor.release.value = 0.12;
      master.connect(compressor);
      compressor.connect(context.destination);
    }
    return context;
  }

  function notify() {
    window.dispatchEvent(new CustomEvent('fc:audio-state', { detail: getState() }));
  }

  function getState() {
    return state ? { ...state } : null;
  }

  function stop(type) {
    if (type && state?.type !== type) return;
    generation++;
    controller?.abort();
    controller = null;
    clearInterval(timer);
    timer = null;
    state = null;
    const oldNodes = nodes;
    nodes = [];
    if (context) {
      const now = context.currentTime;
      oldNodes.forEach(({ source, gain }) => {
        if (gain.gain.cancelAndHoldAtTime) {
          gain.gain.cancelAndHoldAtTime(now);
        } else {
          gain.gain.cancelScheduledValues(0);
          gain.gain.setValueAtTime(gain.gain.value, now);
        }
        gain.gain.linearRampToValueAtTime(0, now + 0.03);
        source.stop(now + 0.04);
      });
      const stoppedGeneration = generation;
      setTimeout(() => {
        if (stoppedGeneration === generation && !state && context.state === 'running') {
          context.suspend().catch(reportError);
        }
      }, 60);
    }
    notify();
  }

  function reportError(error) {
    console.warn('Audio playback failed', error);
    if (typeof window.showError === 'function') {
      window.showError(window.FcI18n?.isEnglish()
        ? 'Audio playback failed' : '音频播放失败');
    }
  }

  function schedule(buffer, when) {
    const source = context.createBufferSource();
    source.buffer = buffer;
    const gain = context.createGain();
    source.connect(gain);
    gain.connect(master);
    const fade = Math.min(CROSSFADE, buffer.duration * 0.49);
    const count = Math.max(32, Math.floor(fade * context.sampleRate));
    const fadeIn = new Float32Array(count);
    const fadeOut = new Float32Array(count);
    for (let i = 0; i < count; i++) {
      fadeIn[i] = Math.sin(i / (count - 1) * Math.PI / 2);
      fadeOut[i] = Math.cos(i / (count - 1) * Math.PI / 2);
    }
    gain.gain.setValueCurveAtTime(fadeIn, when, fade);
    gain.gain.setValueAtTime(1, when + fade);
    gain.gain.setValueAtTime(1, when + buffer.duration - fade);
    gain.gain.setValueCurveAtTime(fadeOut, when + buffer.duration - fade, fade);
    const entry = { source, gain };
    nodes.push(entry);
    source.onended = () => {
      nodes = nodes.filter(node => node !== entry);
      source.disconnect();
      gain.disconnect();
    };
    source.start(when);
  }

  async function play({ type = 'sweep', name, modelId, conditionId, rpm }) {
    if (!['sweep', 'whistle'].includes(type)) throw new Error('Invalid audio source');
    if (type === 'sweep' && (!Number.isFinite(rpm) || rpm <= 0)) throw new Error('Invalid RPM');
    if (state?.type === type && state.name === name) {
      stop();
      return { state: 'stopped' };
    }
    stop();
    const requestId = generation;
    const c = getContext();
    // Resume within the user gesture, before waiting for the network.
    const resumed = c.resume();
    controller = new AbortController();
    const signal = controller.signal;
    state = { type, name, modelId, conditionId, rpm, status: 'loading' };
    notify();
    try {
      await resumed;
      if (requestId !== generation) return { state: 'cancelled' };
      const key = `${type}:${modelId}:${conditionId}:${Math.round(rpm)}`;
      let buffer = type === 'sweep' ? cache.get(key) : null;
      let actualRpm = rpm;
      if (!buffer) {
        const response = await fetch(type === 'sweep' ? '/api/sweep-audio' : '/api/whistle-audio', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(type === 'sweep'
            ? { model_id: modelId, condition_id: conditionId, target_rpm: rpm }
            : { model_id: modelId }),
          signal,
        });
        if (!response.ok) {
          const error = new Error(`HTTP ${response.status}`);
          const body = await response.json();
          error.message = body.error_message || error.message;
          error.errorCode = body.error_code;
          throw error;
        }
        actualRpm = Number(response.headers.get('X-Target-RPM')) || rpm;
        const data = await response.arrayBuffer();
        if (requestId !== generation) return { state: 'cancelled' };
        buffer = await c.decodeAudioData(data);
        if (requestId !== generation) return { state: 'cancelled' };
        if (type === 'sweep') cache.set(key, buffer);
      }
      if (requestId !== generation) return { state: 'cancelled' };
      if (!buffer.length || buffer.duration <= 0) throw new Error('Empty audio recording');
      state = { ...state, rpm: actualRpm, status: 'playing' };
      let next = c.currentTime + 0.1;
      const step = Math.max(0.001, buffer.duration - Math.min(CROSSFADE, buffer.duration * 0.49));
      const tick = () => {
        if (requestId !== generation) return;
        next = Math.max(next, c.currentTime);
        while (next < c.currentTime + 0.4) {
          schedule(buffer, next);
          next += step;
        }
      };
      tick();
      timer = setInterval(tick, 25);
      notify();
      return { state: 'playing' };
    } catch (error) {
      if (requestId !== generation) return { state: 'cancelled' };
      stop();
      throw error;
    }
  }

  window.FcAudioPlayer = {
    play, stop, getState,
    isPlaying: (name) => state?.status === 'playing' && state.name === name,
  };
  window.addEventListener('pagehide', () => stop());
})();
