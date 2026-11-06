(function initWhistleTooltipHelpers() {
  'use strict';

  if (window.WhistleTooltipHelpers) return;

  function _tooltipText(key, fallback) {
    try {
      if (typeof window.fcT === 'function') {
        const text = window.fcT(key);
        if (typeof text === 'string' && text && text !== key) return text;
      }
      if (window.FcI18n && typeof window.FcI18n.t === 'function') {
        const text = window.FcI18n.t(key);
        if (typeof text === 'string' && text && text !== key) return text;
      }
    } catch (_) {}
    return fallback;
  }

  function normalizeWhistlePoints(rawPoints) {
    if (!Array.isArray(rawPoints)) return [];
    const finite = (raw, nonnegative = false) => {
      if (raw === null || raw === undefined || raw === '') return null;
      const number = Number(raw);
      return Number.isFinite(number) && (!nonnegative || number >= 0) ? number : null;
    };
    return rawPoints.map((point) => {
      const rpm = Number(point && point.rpm);
      if (!Number.isFinite(rpm) || rpm <= 0) return null;
      return {
        rpm,
        tonality: finite(point.tonality, true),
        noise: finite(point.noise_db ?? point.noise),
        sone: finite(point.noise_sone ?? point.sone, true),
        airflow: finite(point.airflow_cfm ?? point.airflow),
        delta_db: finite(point.delta_db),
        delta_sone: finite(point.delta_sone),
        noise_index: finite(point.noise_index, true),
        selected: point.selected === true,
      };
    }).filter(Boolean).sort((a, b) => a.rpm - b.rpm);
  }

  function getWhistleLevel(value, points) {
    const config = window.APP_CONFIG || {};
    const levels = config.whistleLevels;
    const low = levels?.low_mid;
    const high = levels?.mid_high;
    if (!Number.isFinite(value) || !Number.isFinite(low) || low < 0
        || !Number.isFinite(high) || low >= high) return 'neutral';
    if (value >= high) return 'high';
    if (value < low) {
      const threshold = config.whistleLowTonalityThreshold;
      const tonality = normalizeWhistlePoints(points).find(point => point.selected)?.tonality;
      if (Number.isFinite(threshold) && threshold >= 0
          && Number.isFinite(tonality) && tonality < threshold) return 'low';
    }
    return 'mid';
  }

  function buildWhistleTooltip(value, points) {
    const lines = [];
    const normalized = normalizeWhistlePoints(points);
    const point = normalized.find(item => item.selected) || (normalized.length === 1 ? normalized[0] : null);
    const missing = _tooltipText('dynamic.whistleNoData', '暂无数据');
    const separator = _tooltipText('dynamic.whistleTooltipSeparator', '；');
    const number = (raw, digits, signed = false) => {
      if (raw === null || raw === undefined) return missing;
      const rounded = Number(raw.toFixed(digits));
      return `${signed && rounded > 0 ? '+' : ''}${(Object.is(rounded, -0) ? 0 : rounded).toFixed(digits)}`;
    };
    lines.push(_tooltipText('dynamic.whistleTooltipTitle', '进气遮挡啸叫等级：')
      + [`${number(point?.delta_db, 1, true)} dB`, `${number(point?.delta_sone, 2, true)} sone`].join(separator));
    lines.push(_tooltipText('dynamic.whistleTooltipTotal', '总：')
      + [`${number(point?.noise, 1)} dBA`, `${number(point?.sone, 2)} sone`, `${number(point?.tonality, 2)} tu_HMS`].join(separator));
    lines.push(_tooltipText('dynamic.whistleTooltipIndex', '总体吵闹指数：')
      + number(point?.noise_index, 2) + _tooltipText('dynamic.whistleTooltipExperimental', '（试验版）'));
    lines.push(_tooltipText('dynamic.whistleTooltipSpeed', '测试转速：{rpm} RPM（{airflow} CFM）')
      .replace('{rpm}', point ? String(Math.round(point.rpm)) : missing)
      .replace('{airflow}', point?.airflow != null ? String(point.airflow) : missing));
    lines.push(_tooltipText('dynamic.whistleTooltipDisclaimer', '注：啸叫等级指标为啸叫测试工况相对正常进气工况同转速拟合值的噪音增加量。数据仅供参考，实际表现会因机箱环境而变化。'));
    return lines.join('\n');
  }

  window.WhistleTooltipHelpers = Object.freeze({
    normalizeWhistlePoints,
    getWhistleLevel,
    buildWhistleTooltip,
  });
})();
