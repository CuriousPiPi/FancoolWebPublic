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

  function _trimTrailingZeros(value, digits) {
    const fixed = Number(value).toFixed(digits);
    return fixed.replace(/\.0+$/, '').replace(/(\.\d*?)0+$/, '$1');
  }

  function normalizeWhistlePoints(rawPoints) {
    if (!Array.isArray(rawPoints)) return [];
    const points = rawPoints.map((point) => {
      const rpm = Number(point && point.rpm);
      const delta = Number(point && point.delta_db);
      const airflowRaw = point && point.airflow_cfm;
      const airflow = airflowRaw === null || airflowRaw === undefined || airflowRaw === ''
        ? null
        : Number(airflowRaw);
      if (!Number.isFinite(rpm) || !Number.isFinite(delta)) return null;
      return {
        rpm,
        delta,
        airflow: Number.isFinite(airflow) ? airflow : null,
      };
    }).filter(Boolean);
    points.sort((a, b) => a.rpm - b.rpm);
    return points;
  }

  function buildWhistleTooltip(value, points) {
    const lines = [_tooltipText('dynamic.whistleTooltipTitle', '进气遮挡啸叫等级：')];
    const disclaimer = _tooltipText('dynamic.whistleTooltipDisclaimer', '注：数据仅供参考，实际表现会因机箱环境而变化。');
    if (Array.isArray(points) && points.length) {
      points.forEach((point) => {
        const deltaText = `${point.delta > 0 ? '+' : ''}${point.delta.toFixed(1)}dB`;
        const rpmText = `@${Math.round(point.rpm)}RPM`;
        const airflowText = point.airflow == null ? '' : `(${_trimTrailingZeros(point.airflow, 3)}CFM)`;
        lines.push(`${deltaText} ${rpmText}${airflowText}`);
      });
      lines.push(disclaimer);
      return lines.join('\n');
    }
    lines.push(`+${value.toFixed(1)}dB`);
    lines.push(disclaimer);
    return lines.join('\n');
  }

  window.WhistleTooltipHelpers = Object.freeze({
    normalizeWhistlePoints,
    buildWhistleTooltip,
  });
})();
