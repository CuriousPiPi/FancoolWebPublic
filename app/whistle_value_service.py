from __future__ import annotations

import logging
import math
import os
from typing import Any, Callable, Dict, Iterable, List, Tuple

from app.curves import perf_model_service
from app.curves.pchip_cache import eval_pchip
from app.whistle_levels import WHISTLE_COLOR_METRIC

_logger = logging.getLogger(__name__)
_TARGET_AIRFLOW_CFM = 40.0


def _as_int(raw) -> int | None:
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError, OverflowError, RuntimeError):
        return None
    return value if value > 0 else None


def _get_condition_config() -> Tuple[int | None, int | None]:
    baseline_condition_id = _as_int(os.getenv('WHISTLE_BASELINE_CONDITION_ID'))
    whistle_condition_id = _as_int(os.getenv('WHISTLE_TEST_CONDITION_ID'))
    if whistle_condition_id is None:
        return None, None
    return baseline_condition_id or whistle_condition_id, whistle_condition_id


def _build_in_params(values: Iterable[int], prefix: str = 'v') -> Tuple[str, Dict[str, int]]:
    params: Dict[str, int] = {}
    keys: List[str] = []
    for idx, value in enumerate(values):
        key = f'{prefix}{idx}'
        keys.append(f':{key}')
        params[key] = int(value)
    return ','.join(keys), params


def _finite(value) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _is_missing_tonality_column(exc: Exception) -> bool:
    message = str(exc).lower()
    return 'tonality' in message and any(
        marker in message for marker in ('no such column', 'unknown column', 'column not found')
    )


def _baseline_value(curve, rpm: float, evaluator: Callable, logger) -> float | None:
    if not isinstance(curve, dict):
        return None
    xs = curve.get('x')
    if not isinstance(xs, (list, tuple)) or not xs:
        return None
    lower, upper = _finite(xs[0]), _finite(xs[-1])
    if lower is None or upper is None or not lower <= rpm <= upper:
        return None
    try:
        value = _finite(evaluator(curve, rpm))
    except (KeyError, IndexError, TypeError, ValueError, OverflowError, RuntimeError):
        logger.warning('[whistle-value] invalid baseline curve at rpm=%s', rpm, exc_info=True)
        return None
    return value


def derive_whistle_values_for_models(
    fetch_all: Callable,
    model_ids: Iterable[int],
    *,
    get_perf_model: Callable[[int, int], dict | None] | None = None,
    eval_model: Callable[[dict, float], float] | None = None,
    logger=None,
    include_sources: bool = False,
    include_baseline: bool = True,
) -> Tuple[Dict[int, float], int | None, Dict[int, List[Dict[str, Any]]]]:
    """Compare the measured whistle row nearest 40 CFM with same-RPM baseline fits."""
    baseline_condition_id, whistle_condition_id = _get_condition_config()
    if baseline_condition_id is None or whistle_condition_id is None:
        return {}, None, {}

    normalized_model_ids = sorted({
        mid for raw in model_ids or [] if (mid := _as_int(raw)) is not None
    })
    if not normalized_model_ids:
        return {}, baseline_condition_id, {}

    placeholders, in_params = _build_in_params(normalized_model_ids, 'mid')
    test_params = {'whistle_condition_id': whistle_condition_id, **in_params}
    try:
        test_rows = fetch_all(f"""
            SELECT model_id, rpm, airflow_cfm, noise_db, noise_sone, tonality, batch_id, data_id
            FROM fan_performance_data
            WHERE is_valid = 1
              AND condition_id = :whistle_condition_id
              AND model_id IN ({placeholders})
              AND rpm IS NOT NULL
            ORDER BY model_id, rpm, data_id
        """, test_params)
    except Exception as exc:
        if not _is_missing_tonality_column(exc):
            raise
        (logger or _logger).error(
            '[whistle-value] fan_performance_data.tonality is unavailable: %s', exc,
        )
        test_rows = fetch_all(f"""
            SELECT model_id, rpm, airflow_cfm, noise_db, noise_sone,
                   NULL AS tonality, batch_id, data_id
            FROM fan_performance_data
            WHERE is_valid = 1 AND condition_id = :whistle_condition_id
              AND model_id IN ({placeholders}) AND rpm IS NOT NULL
            ORDER BY model_id, rpm, data_id
        """, test_params)

    test_by_model: Dict[int, List[dict]] = {}
    for row in test_rows or []:
        try:
            model_id = int(row['model_id'])
            rpm_value = _finite(row['rpm'])
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
        if rpm_value is None or rpm_value <= 0 or not rpm_value.is_integer():
            continue
        rpm = int(rpm_value)
        tone = row.get('tonality')
        tone_value = _finite(tone)
        if tone_value is not None and tone_value < 0:
            tone_value = None
        test_by_model.setdefault(model_id, []).append({
            'rpm': rpm,
            'airflow_cfm': _finite(row.get('airflow_cfm')),
            'noise_db': _finite(row.get('noise_db')),
            'noise_sone': _finite(row.get('noise_sone')),
            'tonality': tone_value,
            'batch_id': row.get('batch_id'),
            'data_id': row.get('data_id'),
        })

    values: Dict[int, float] = {}
    point_map: Dict[int, List[Dict[str, Any]]] = {}
    actual_baseline_id = _as_int(os.getenv('WHISTLE_BASELINE_CONDITION_ID'))
    for model_id in normalized_model_ids:
        rows = test_by_model.get(model_id) or []
        if not rows:
            continue
        candidates = [row for row in rows
                      if row['airflow_cfm'] is not None and row['airflow_cfm'] > 0]
        selected = min(candidates, key=lambda row: (
            abs(row['airflow_cfm'] - _TARGET_AIRFLOW_CFM), row['rpm'],
            row['data_id'] if row['data_id'] is not None else -1,
        )) if candidates else None
        if selected is None:
            point_map[model_id] = []
            continue
        curves = {}
        if (include_baseline and actual_baseline_id is not None
                and actual_baseline_id != whistle_condition_id):
            baseline_model = (get_perf_model or perf_model_service.get_perf_model)(
                model_id, actual_baseline_id) or {}
            curves = baseline_model.get('pchip') or {}
        row = selected
        baseline_db = _baseline_value(
            curves.get('rpm_to_noise_db'), row['rpm'], eval_model or eval_pchip, logger or _logger)
        baseline_sone = _baseline_value(
            curves.get('rpm_to_sone'), row['rpm'], eval_model or eval_pchip, logger or _logger)
        sone = row['noise_sone']
        if sone is not None and sone < 0:
            sone = None
        point: Dict[str, Any] = {
            'rpm': row['rpm'], 'airflow_cfm': row['airflow_cfm'],
            'noise_db': row['noise_db'], 'tonality': row['tonality'],
            'noise_sone': sone,
            'delta_db': _finite(row['noise_db'] - baseline_db)
                if row['noise_db'] is not None and baseline_db is not None else None,
            'delta_sone': _finite(sone - baseline_sone)
                if sone is not None and baseline_sone is not None and baseline_sone >= 0 else None,
            'noise_index': _finite(sone * (1.0 + row['tonality']))
                if sone is not None and row['tonality'] is not None else None,
            'status': 'missing' if row['tonality'] is None else 'measured',
            'selected': True,
        }
        if include_sources:
            point.update({
                'batch_id': row['batch_id'], 'source_rpm': row['rpm'],
                'source_data_id': row['data_id'],
            })
        point_map[model_id] = [point]
        color_key = {'dba': 'delta_db', 'sone': 'delta_sone'}.get(WHISTLE_COLOR_METRIC)
        color_value = point.get(color_key)
        if color_value is not None:
            values[model_id] = color_value

    if logger:
        logger.debug(
            '[whistle-value] resolved %s noise-delta values (badge_anchor=%s, test=%s, target_airflow=%.1f CFM)',
            len(values), baseline_condition_id, whistle_condition_id, _TARGET_AIRFLOW_CFM,
        )
    return values, baseline_condition_id, point_map
