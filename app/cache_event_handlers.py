from __future__ import annotations

import logging

from app import model_meta_cache, scoring_system
from app.audio_services import spectrum_reader
from app.cache_event_bus import EVENT_REFRESH_SCORING_VISIBILITY, EVENT_WARM_SCORES

_logger = logging.getLogger(__name__)


def setup(logger=None) -> None:
    global _logger
    if logger is not None:
        _logger = logger


def _as_int(value, default: int = 0) -> int:
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def warm_scores(model_id: int, condition_id: int, *, rebuild_pchip: bool = False) -> None:
    model_id = _as_int(model_id)
    condition_id = _as_int(condition_id)
    if model_id <= 0 or condition_id <= 0:
        raise ValueError('model_id and condition_id required')

    if rebuild_pchip:
        try:
            spectrum_reader.build_performance_pchips([(model_id, condition_id)])
        except Exception as exc:
            _logger.warning('[warm_scores] pchip rebuild (%s,%s): %s', model_id, condition_id, exc)

    scoring_system.refresh_model_score_cache(model_id)


def refresh_scoring_visibility(model_id: int | None, old_is_valid=None, new_is_valid=None, old_visibility_scope=None, new_visibility_scope=None) -> None:
    model_meta_cache.invalidate()
    try:
        model_meta_cache.refresh()
    except Exception as exc:
        _logger.error('[refresh_scoring_visibility] model_meta_cache reload failed: %s', exc)
    scoring_system.refresh_visibility_scoring_caches()
    _logger.info(
        '[refresh_scoring_visibility] refreshed caches for model_id=%s is_valid(%s -> %s) visibility_scope(%s -> %s)',
        _as_int(model_id) or 'unknown',
        old_is_valid,
        new_is_valid,
        old_visibility_scope,
        new_visibility_scope,
    )


def handle_event(event: dict) -> None:
    event_type = str((event or {}).get('event_type') or '').strip()
    payload = (event or {}).get('payload') or {}
    if not isinstance(payload, dict):
        payload = {}

    if event_type == EVENT_WARM_SCORES:
        model_id = _as_int(payload.get('model_id'))
        condition_id = _as_int(payload.get('condition_id'))
        if model_id <= 0 or condition_id <= 0:
            _logger.warning('[cache_event_handlers] skip invalid warm_scores payload: %r', payload)
            return
        warm_scores(model_id, condition_id, rebuild_pchip=False)
        return

    if event_type == EVENT_REFRESH_SCORING_VISIBILITY:
        refresh_scoring_visibility(
            payload.get('model_id'),
            payload.get('old_is_valid'),
            payload.get('new_is_valid'),
            payload.get('old_visibility_scope'),
            payload.get('new_visibility_scope'),
        )
        return

    _logger.warning('[cache_event_handlers] skip unknown event_type=%r payload=%r', event_type, payload)
