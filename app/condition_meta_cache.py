import threading
import time
from typing import Callable, Dict

_DEFAULT_TTL_SEC = 300

_fetch_all: Callable | None = None
_logger = None
_ttl_sec = _DEFAULT_TTL_SEC
_cache_lock = threading.RLock()
_cache_data: Dict[int, dict] = {}
_cache_loaded_at = 0.0


def setup(fetch_all: Callable, logger=None, ttl_sec: int = _DEFAULT_TTL_SEC) -> None:
    global _fetch_all, _logger, _ttl_sec
    _fetch_all = fetch_all
    _logger = logger
    _ttl_sec = max(1, int(ttl_sec or _DEFAULT_TTL_SEC))


def _cache_expired(now: float) -> bool:
    return (not _cache_loaded_at) or (now - _cache_loaded_at >= _ttl_sec)


def refresh() -> Dict[int, dict]:
    if _fetch_all is None:
        raise RuntimeError('condition_meta_cache is not configured')

    rows = _fetch_all("""
        SELECT
            condition_id,
            condition_name_zh,
            COALESCE(condition_name_en, '') AS condition_name_en,
            resistance_type_zh,
            resistance_type_en,
            resistance_location_zh,
            resistance_location_en,
            additional,
            COALESCE(is_valid, 0) AS is_valid
        FROM working_condition
        ORDER BY condition_name_zh, condition_id
    """)

    data: Dict[int, dict] = {}
    for row in rows:
        try:
            condition_id = int(row['condition_id'])
        except (KeyError, TypeError, ValueError):
            continue
        item = dict(row)
        item['condition_id'] = condition_id
        try:
            item['is_valid'] = int(item.get('is_valid') or 0)
        except (TypeError, ValueError):
            item['is_valid'] = 0
        data[condition_id] = item

    with _cache_lock:
        global _cache_data, _cache_loaded_at
        _cache_data = data
        _cache_loaded_at = time.time()
    return {cid: dict(meta) for cid, meta in data.items()}


def _ensure_loaded(force_refresh: bool = False) -> None:
    global _cache_loaded_at
    now = time.time()
    if not force_refresh:
        with _cache_lock:
            if not _cache_expired(now):
                return
    try:
        refresh()
    except Exception:
        if _logger:
            _logger.exception('condition_meta_cache refresh failed')
        # Keep stale cache (if any) and back off so we don't hammer DB on every request.
        with _cache_lock:
            _cache_loaded_at = time.time()
            has_data = bool(_cache_data)
        if has_data:
            return
        raise

def get_condition_meta(condition_id: int, *, force_refresh: bool = False) -> dict | None:
    try:
        condition_id = int(condition_id)
    except (TypeError, ValueError):
        return None
    _ensure_loaded(force_refresh=force_refresh)
    with _cache_lock:
        meta = _cache_data.get(condition_id)
        return dict(meta) if isinstance(meta, dict) else None


def get_many_condition_meta(condition_ids, *, force_refresh: bool = False) -> Dict[int, dict]:
    wanted = []
    for condition_id in condition_ids or []:
        try:
            wanted.append(int(condition_id))
        except (TypeError, ValueError):
            continue
    _ensure_loaded(force_refresh=force_refresh)
    with _cache_lock:
        return {
            condition_id: dict(_cache_data[condition_id])
            for condition_id in wanted
            if isinstance(_cache_data.get(condition_id), dict)
        }


def get_all_conditions(*, valid_only: bool = False, force_refresh: bool = False) -> list[dict]:
    _ensure_loaded(force_refresh=force_refresh)
    with _cache_lock:
        items = [dict(meta) for meta in _cache_data.values()]
    if valid_only:
        items = [item for item in items if int(item.get('is_valid') or 0) == 1]
    items.sort(key=lambda item: ((item.get('condition_name_zh') or ''), item.get('condition_id') or 0))
    return items
