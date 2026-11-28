(function () {
  'use strict';

  const CROSSFADE = 0.05;
  const TRANSITION_FADE = 0.05;
  const FILTER_WARMUP = 0.04;
  const FILTER_FADE = 0.08;
  const cache = new Map();
  let context = null;
  let master = null;
  let timer = null;
  let playbackTick = null;
  let playbackGroup = null;
  let controller = null;
  let transitionController = null;
  let transitionVersion = 0;
  let transitionEnd = 0;
  let generation = 0;
  let state = null;
  let nodes = [];
  let filter = null;
  let filterGraph = null;

  function ramp(param, value, now, delay = 0, duration = 0.03) {
    if (param.cancelAndHoldAtTime) param.cancelAndHoldAtTime(now);
    else {
      param.cancelScheduledValues(now);
      param.setValueAtTime(param.value, now);
    }
    if (delay) param.setValueAtTime(param.value, now + delay);
    param.linearRampToValueAtTime(value, now + delay + duration);
  }

  function createFilterGraph(c) {
    const input = c.createGain();
    const dry = c.createGain();
    input.connect(dry);
    dry.connect(master);
    filterGraph = { input, dry, branches: [], transition: null, cleanup: null, pending: false };
    updateFilter();
    return input;
  }

  function createFilterBranch(settings) {
    const c = context;
    const nyquist = c.sampleRate / 2;
    if (settings.lowHz >= nyquist) return null;
    const output = c.createGain();
    output.gain.value = 0;
    const stages = [];
    const addStage = (type, hz, q) => {
      const node = c.createBiquadFilter();
      node.type = type;
      node.frequency.value = hz;
      node.Q.value = q;
      stages.push(node);
    };
    // Low/high-pass Q is specified in dB in Web Audio; notch Q is linear.
    const qValues = [0.509795579, 0.601344887, 0.899976223, 2.562915448];
    const addButterworth = (type, hz) => qValues.forEach(q => addStage(type, hz, 20 * Math.log10(q)));
    if (settings.mode === 'include') {
      addButterworth('highpass', settings.lowHz);
      if (settings.highHz < nyquist) addButterworth('lowpass', settings.highHz);
    } else if (settings.highHz >= nyquist) {
      addButterworth('lowpass', settings.lowHz);
    } else {
      const low = Math.tan(Math.PI * settings.lowHz / c.sampleRate);
      const high = Math.tan(Math.PI * settings.highHz / c.sampleRate);
      const center = Math.sqrt(low * high);
      const hz = c.sampleRate / Math.PI * Math.atan(center);
      const q = Math.min(1000, center / (high - low));
      addStage('notch', hz, q);
      addStage('notch', hz, q);
    }
    let previous = filterGraph.input;
    stages.forEach(node => { previous.connect(node); previous = node; });
    previous.connect(output);
    output.connect(master);
    const branch = { stages, output };
    filterGraph.branches.push(branch);
    return branch;
  }

  function disposeFilterBranch(branch) {
    if (!filterGraph.branches.includes(branch)) return;
    filterGraph.input.disconnect(branch.stages[0]);
    branch.stages.forEach(node => node.disconnect());
    branch.output.disconnect();
    filterGraph.branches = filterGraph.branches.filter(item => item !== branch);
  }

  function updateFilter() {
    if (!filterGraph || !context) return;
    const g = filterGraph;
    const now = context.currentTime;
    const active = filter && filter.enabled;
    const nyquist = context.sampleRate / 2;
    const bypass = !active || (filter.mode === 'exclude' && filter.lowHz >= nyquist);
    if (!state || state.status !== 'playing') {
      clearTimeout(g.transition);
      clearTimeout(g.cleanup);
      g.transition = null;
      g.cleanup = null;
      g.pending = false;
      g.branches.slice().forEach(disposeFilterBranch);
      const branch = bypass ? null : createFilterBranch(filter);
      g.dry.gain.cancelScheduledValues(now);
      g.dry.gain.setValueAtTime(bypass ? 1 : 0, now);
      if (branch) branch.output.gain.value = 1;
      return;
    }
    if (bypass) {
      clearTimeout(g.transition);
      clearTimeout(g.cleanup);
      g.transition = null;
      g.pending = false;
      const old = g.branches.slice();
      ramp(g.dry.gain, 1, now);
      old.forEach(branch => ramp(branch.output.gain, 0, now));
      g.cleanup = setTimeout(() => {
        old.forEach(disposeFilterBranch);
        g.cleanup = null;
      }, 40);
      return;
    }
    if (g.transition !== null) {
      g.pending = true;
      return;
    }
    clearTimeout(g.cleanup);
    g.cleanup = null;
    const old = g.branches.slice();
    const branch = createFilterBranch(filter);
    ramp(g.dry.gain, 0, now, FILTER_WARMUP, FILTER_FADE);
    old.forEach(item => ramp(item.output.gain, 0, now, FILTER_WARMUP, FILTER_FADE));
    if (branch) ramp(branch.output.gain, 1, now, FILTER_WARMUP, FILTER_FADE);
    g.transition = setTimeout(() => {
      old.forEach(disposeFilterBranch);
      g.transition = null;
      if (g.pending) {
        g.pending = false;
        updateFilter();
      }
    }, (FILTER_WARMUP + FILTER_FADE) * 1000 + 10);
  }

  function setFrequencyFilter(next) {
    if (next != null && (!Number.isFinite(next.lowHz) || !Number.isFinite(next.highHz) ||
        next.lowHz <= 0 || next.lowHz >= next.highHz ||
        !['include', 'exclude'].includes(next.mode))) {
      throw new Error('Invalid frequency filter');
    }
    filter = next ? { ...next } : null;
    updateFilter();
  }

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
      createFilterGraph(context);
    }
    return context;
  }

  function notify() {
    window.dispatchEvent(new CustomEvent('fc:audio-state', { detail: getState() }));
  }

  function getState() {
    return state ? { ...state } : null;
  }

  function createPlaybackGroup(initialGain) {
    const output = context.createGain();
    output.gain.value = initialGain;
    output.connect(filterGraph.input);
    return { output, count: 0, retired: false };
  }

  function retirePlaybackGroup(group) {
    if (!group || group.retired) return;
    group.retired = true;
    if (!group.count) group.output.disconnect();
  }

  function stop(type) {
    if (type && state?.type !== type) return;
    generation++;
    controller?.abort();
    controller = null;
    transitionVersion++;
    transitionController?.abort();
    transitionController = null;
    transitionEnd = 0;
    clearInterval(timer);
    timer = null;
    playbackTick = null;
    state = null;
    const oldNodes = nodes;
    nodes = [];
    const oldGroups = new Set(oldNodes.map(node => node.group));
    if (playbackGroup) oldGroups.add(playbackGroup);
    playbackGroup = null;
    oldGroups.forEach(retirePlaybackGroup);
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

  function equalPowerCurves(fade) {
    const count = Math.max(32, Math.floor(fade * context.sampleRate));
    const fadeIn = new Float32Array(count);
    const fadeOut = new Float32Array(count);
    for (let i = 0; i < count; i++) {
      fadeIn[i] = Math.sin(i / (count - 1) * Math.PI / 2);
      fadeOut[i] = Math.cos(i / (count - 1) * Math.PI / 2);
    }
    return { fadeIn, fadeOut };
  }

  function schedule(buffer, when, offset = 0, duration = buffer.duration, group = playbackGroup, fadeInEnabled = true) {
    const source = context.createBufferSource();
    source.buffer = buffer;
    const gain = context.createGain();
    source.connect(gain);
    gain.connect(group.output);
    const fade = Math.min(CROSSFADE, duration * 0.49);
    const { fadeIn, fadeOut } = equalPowerCurves(fade);
    if (fadeInEnabled) gain.gain.setValueCurveAtTime(fadeIn, when, fade);
    else gain.gain.setValueAtTime(1, when);
    gain.gain.setValueAtTime(1, when + fade);
    gain.gain.setValueAtTime(1, when + duration - fade);
    gain.gain.setValueCurveAtTime(fadeOut, when + duration - fade, fade);
    const entry = { source, gain, group };
    source.onended = () => {
      nodes = nodes.filter(node => node !== entry);
      source.disconnect();
      gain.disconnect();
      group.count--;
      if (group.retired && !group.count) group.output.disconnect();
    };
    try {
      source.start(when, offset, duration);
    } catch (error) {
      source.disconnect();
      gain.disconnect();
      throw error;
    }
    group.count++;
    nodes.push(entry);
  }

  async function requestAudio(url, payload, signal, isCurrent, errorMessage = status => `HTTP ${status}`) {
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      signal,
    });
    if (!response.ok) {
      const error = new Error(errorMessage(response.status));
      let body;
      try {
        body = await response.json();
      } catch (parseError) {
        if (parseError.name !== 'SyntaxError') throw parseError;
      }
      if (body) {
        error.message = body.error_message || body.error || error.message;
        error.errorCode = body.error_code;
      }
      throw error;
    }
    const data = await response.arrayBuffer();
    if (!isCurrent()) return null;
    const buffer = await context.decodeAudioData(data);
    if (!isCurrent()) return null;
    return { response, buffer };
  }

  async function play({ type = 'sweep', name, modelId, conditionId, sourceConditionId, jobId, rpm }) {
    if (!['sweep', 'whistle', 'preview'].includes(type)) throw new Error('Invalid audio source');
    if (type !== 'whistle' && (!Number.isFinite(rpm) || rpm <= 0)) throw new Error('Invalid RPM');
    if (type === 'preview' && !jobId) throw new Error('Missing preview job ID');
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
    state = { type, name, modelId, conditionId, sourceConditionId, rpm, status: 'loading' };
    if (type === 'preview') state.jobId = jobId;
    notify();
    try {
      await resumed;
      if (requestId !== generation) return { state: 'cancelled' };
      const key = `${type}:${modelId}:${conditionId}:${sourceConditionId ?? conditionId}:${Math.round(rpm)}`;
      let buffer = type === 'sweep' ? cache.get(key) : null;
      let actualRpm = rpm;
      if (!buffer) {
        const url = type === 'sweep' ? '/api/sweep-audio'
          : type === 'preview' ? '/admin/api/sweep-audio' : '/api/whistle-audio';
        const payload = type === 'sweep'
          ? {
              model_id: modelId, condition_id: conditionId, target_rpm: rpm,
              expected_source_condition_id: sourceConditionId,
            }
          : type === 'preview' ? { target_rpm: rpm, job_id: jobId } : { model_id: modelId };
        const result = await requestAudio(url, payload, signal, () => requestId === generation,
          type === 'preview' ? status => `服务器错误 (${status})` : undefined);
        if (!result) return { state: 'cancelled' };
        actualRpm = Number(result.response.headers.get('X-Target-RPM')) || rpm;
        buffer = result.buffer;
        if (type === 'sweep') cache.set(key, buffer);
      }
      if (requestId !== generation) return { state: 'cancelled' };
      if (!buffer.length || buffer.duration <= 0) throw new Error('Empty audio recording');
      state = { ...state, rpm: actualRpm, status: 'playing' };
      const group = createPlaybackGroup(1);
      playbackGroup = group;
      let next = c.currentTime + 0.1;
      const step = Math.max(0.001, buffer.duration - Math.min(CROSSFADE, buffer.duration * 0.49));
      const tick = () => {
        if (requestId !== generation) return;
        next = Math.max(next, c.currentTime);
        while (next < c.currentTime + 0.4) {
          schedule(buffer, next, 0, buffer.duration, group);
          next += step;
        }
      };
      tick();
      playbackTick = tick;
      timer = setInterval(tick, 25);
      notify();
      return { state: 'playing' };
    } catch (error) {
      if (requestId !== generation) return { state: 'cancelled' };
      stop();
      throw error;
    }
  }

  function cancelTransitionRequest() {
    transitionVersion++;
    transitionController?.abort();
    transitionController = null;
  }

  async function transitionTo(rpm) {
    if (!Number.isFinite(rpm) || rpm <= 0) throw new Error('Invalid RPM');
    if (!state || !['sweep', 'preview'].includes(state.type) || state.status !== 'playing') return;
    const requestId = generation;
    const version = ++transitionVersion;
    transitionController?.abort();
    transitionController = null;
    while (transitionEnd > context.currentTime) {
      await new Promise(resolve => setTimeout(resolve, (transitionEnd - context.currentTime) * 1000));
      if (requestId !== generation || version !== transitionVersion) return;
    }
    if (requestId !== generation || version !== transitionVersion) return;
    if (Math.round(rpm) === Math.round(state.rpm)) return;
    const current = { ...state };
    const nextController = new AbortController();
    transitionController = nextController;
    try {
      const result = await requestAudio(
        current.type === 'preview' ? '/admin/api/sweep-audio' : '/api/sweep-audio',
        current.type === 'preview' ? {
          job_id: current.jobId, source_rpm: current.rpm, target_rpm: rpm,
        } : {
          model_id: current.modelId, condition_id: current.conditionId,
          expected_source_condition_id: current.sourceConditionId,
          source_rpm: current.rpm, target_rpm: rpm,
        },
        nextController.signal,
        () => requestId === generation && version === transitionVersion,
        current.type === 'preview' ? status => `服务器错误 (${status})` : undefined,
      );
      if (!result) return;
      const { response, buffer } = result;
      const durationHeader = response.headers.get('X-Transition-Duration-Sec');
      if (durationHeader == null || durationHeader.trim() === '') throw new Error('Missing transition duration');
      const transitionDuration = Number(durationHeader);
      const actualRpm = Number(response.headers.get('X-Target-RPM')) || rpm;
      if (!buffer.length || !Number.isFinite(transitionDuration) || transitionDuration < 0 ||
          transitionDuration >= buffer.duration) throw new Error('Invalid transition audio');

      const now = context.currentTime;
      const start = now + 0.05;
      const fadeEnd = start + TRANSITION_FADE;
      const overlap = Math.min(CROSSFADE, transitionDuration * 0.49);
      const oldGroup = playbackGroup;
      const newGroup = createPlaybackGroup(0);
      let next = start + transitionDuration - overlap;
      const duration = buffer.duration - transitionDuration;
      const step = Math.max(0.001, duration - Math.min(CROSSFADE, duration * 0.49));
      let firstLoop = transitionDuration === 0;
      const tick = () => {
        if (requestId !== generation) return;
        next = Math.max(next, context.currentTime);
        while (next < context.currentTime + 0.4) {
          schedule(buffer, next, transitionDuration, duration, newGroup, !firstLoop);
          firstLoop = false;
          next += step;
        }
      };
      try {
        if (transitionDuration > 0) schedule(buffer, start, 0, transitionDuration, newGroup, false);
        tick();
        playbackTick?.();
      } catch (error) {
        nodes.filter(node => node.group === newGroup).forEach(({ source }) => source.stop(now));
        retirePlaybackGroup(newGroup);
        throw error;
      }
      clearInterval(timer);
      const { fadeIn, fadeOut } = equalPowerCurves(TRANSITION_FADE);
      oldGroup.output.gain.setValueCurveAtTime(fadeOut, start, TRANSITION_FADE);
      newGroup.output.gain.setValueCurveAtTime(fadeIn, start, TRANSITION_FADE);
      nodes.filter(node => node.group === oldGroup).forEach(({ source }) => source.stop(fadeEnd + 0.01));
      retirePlaybackGroup(oldGroup);
      playbackGroup = newGroup;
      playbackTick = tick;
      transitionEnd = Math.max(start + transitionDuration - overlap, fadeEnd);
      timer = setInterval(tick, 25);
      state = { ...state, rpm: actualRpm };
      notify();
    } catch (error) {
      if (requestId !== generation || version !== transitionVersion) return;
      throw error;
    } finally {
      if (transitionController === nextController) transitionController = null;
    }
  }

  window.FcAudioPlayer = {
    play, stop, getState, setFrequencyFilter, transitionTo, cancelTransitionRequest,
    isPlaying: (name) => state?.status === 'playing' && state.name === name,
  };
  window.addEventListener('pagehide', () => stop());
})();
