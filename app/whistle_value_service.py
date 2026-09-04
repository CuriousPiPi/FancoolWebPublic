from __future__ import annotations

import os
from typing import Callable, Dict, Iterable, List, Tuple

from app.curves import perf_model_service
from app.curves.pchip_cache import eval_pchip


def _as_int(raw) -> int | None:
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError, OverflowError, RuntimeError):
        return None
    return value if value > 0 else None


def _get_condition_config() -> Tuple[int | None, int | None]:
    baseline_condition_id = _as_int(os.getenv('WHISTLE_BASELINE_CONDITION_ID'))
    whistle_condition_id = _as_int(os.getenv('WHISTLE_TEST_CONDITION_ID'))
    if baseline_condition_id is None or whistle_condition_id is None:
        return None, None
    if baseline_condition_id == whistle_condition_id:
        return None, None
    return baseline_condition_id, whistle_condition_id


def _build_in_params(values: Iterable[int], prefix: str = 'v') -> Tuple[str, Dict[str, int]]:
    params: Dict[str, int] = {}
    keys: List[str] = []
    for idx, value in enumerate(values):
        key = f'{prefix}{idx}'
        keys.append(f':{key}')
        params[key] = int(value)
    return ','.join(keys), params


def derive_whistle_values_for_models(
    fetch_all: Callable,
    model_ids: Iterable[int],
    *,
    get_perf_model: Callable[[int, int], dict | None] | None = None,
    eval_model: Callable[[dict, float], float] | None = None,
    logger=None,
) -> Tuple[Dict[int, float], int | None]:
    baseline_condition_id, whistle_condition_id = _get_condition_config()
    if baseline_condition_id is None or whistle_condition_id is None:
        return {}, None

    normalized_model_ids: List[int] = []
    for model_id in model_ids or []:
        mid = _as_int(model_id)
        if mid is not None:
            normalized_model_ids.append(mid)
    if not normalized_model_ids:
        return {}, baseline_condition_id

    placeholders, in_params = _build_in_params(normalized_model_ids, 'mid')
    rows = fetch_all(
        f"""
        SELECT model_id, rpm, noise_db
        FROM fan_performance_data
        WHERE is_valid = 1
          AND condition_id = :whistle_condition_id
          AND model_id IN ({placeholders})
          AND rpm IS NOT NULL
          AND noise_db IS NOT NULL
        ORDER BY model_id
        """,
        {'whistle_condition_id': whistle_condition_id, **in_params},
    )

    if not rows:
        return {}, baseline_condition_id

    get_perf_model_fn = get_perf_model or perf_model_service.get_perf_model
    eval_model_fn = eval_model or eval_pchip

    delta_map: Dict[int, List[float]] = {}
    baseline_curve_cache: Dict[int, dict | None] = {}

    for row in rows:
        try:
            model_id = int(row['model_id'])
            rpm = float(row['rpm'])
            whistle_noise_db = float(row['noise_db'])
        except (KeyError, TypeError, ValueError):
            continue

        if model_id not in baseline_curve_cache:
            baseline_model = get_perf_model_fn(model_id, baseline_condition_id) or {}
            pchip = baseline_model.get('pchip') if isinstance(baseline_model, dict) else {}
            baseline_curve_cache[model_id] = pchip.get('rpm_to_noise_db') if isinstance(pchip, dict) else None

        baseline_curve = baseline_curve_cache.get(model_id)
        if not isinstance(baseline_curve, dict):
            continue

        try:
            baseline_noise_db = float(eval_model_fn(baseline_curve, rpm))
        except (TypeError, ValueError, OverflowError, RuntimeError):
            if logger:
                logger.debug(
                    '[whistle-value] skip model=%s rpm=%s: baseline evaluation invalid',
                    model_id,
                    rpm,
                )
            continue
        delta = max(whistle_noise_db - baseline_noise_db, 0.0)
        delta_map.setdefault(model_id, []).append(delta)

    result: Dict[int, float] = {}
    for model_id, deltas in delta_map.items():
        if not deltas:
            continue
        result[model_id] = round(sum(deltas) / len(deltas), 3)

    if logger and result:
        logger.debug(
            '[whistle-value] derived %s model values (baseline=%s, whistle=%s)',
            len(result),
            baseline_condition_id,
            whistle_condition_id,
        )

    return result, baseline_condition_id
