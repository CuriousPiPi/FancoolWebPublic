import os
import threading
import time
from typing import Callable, Dict, List, Set, Tuple

_DEFAULT_TTL_SEC = 300

_fetch_all: Callable | None = None
_logger = None
_ttl_sec = _DEFAULT_TTL_SEC
_cache_lock = threading.RLock()
_cache_data: Dict[int, dict] = {}
_model_ids_by_brand_model: Dict[Tuple[str, str], List[int]] = {}
_cache_loaded_at = 0.0


def _parse_visible_scopes() -> Set[int]:
    """Parse VISIBLE_SCOPES env variable into a set of allowed scope ints.

    Examples:
      VISIBLE_SCOPES=1     -> {1}   (public-only, default)
      VISIBLE_SCOPES=1,2   -> {1,2} (public + internal)
    """
    raw = os.getenv('VISIBLE_SCOPES', '1')
    scopes: Set[int] = set()
    for part in raw.split(','):
        part = part.strip()
        if not part:
            continue
        try:
            v = int(part)
        except ValueError:
            continue
        if v in (1, 2):
            scopes.add(v)
    return scopes if scopes else {1}


def setup(fetch_all: Callable, logger=None, ttl_sec: int = _DEFAULT_TTL_SEC) -> None:
    global _fetch_all, _logger, _ttl_sec
    _fetch_all = fetch_all
    _logger = logger
    _ttl_sec = max(1, int(ttl_sec or _DEFAULT_TTL_SEC))


def _cache_expired(now: float) -> bool:
    return (not _cache_loaded_at) or (now - _cache_loaded_at >= _ttl_sec)


def _norm_lookup_value(value) -> str:
    if value is None:
        return ''
    return str(value).strip()


def _brand_model_key(brand_name: str | None, model_name: str | None) -> Tuple[str, str]:
    return (_norm_lookup_value(brand_name), _norm_lookup_value(model_name))


def refresh() -> Dict[int, dict]:
    if _fetch_all is None:
        raise RuntimeError('model_meta_cache is not configured')

    rows = _fetch_all("""
        SELECT
            model_id,
            brand_id,
            brand_name_zh,
            brand_name_en,
            model_name,
            size,
            thickness,
            max_speed,
            rgb_flags,
            rgb_names_zh,
            rgb_names_en,
            color_flags,
            COALESCE(reverse_opt, 0) AS reverse_opt,
            speed_switch_type_id,
            speed_switch_type_name_zh,
            speed_switch_type_name_en,
            bearing,
            bearing_type_zh,
            bearing_type_en,
            chain_type_id,
            chain_type_name_zh,
            chain_type_name_en,
            reference_price,
            COALESCE(review, '') AS review,
            COALESCE(comment, '') AS comment,
            COALESCE(tier_list, 1) AS tier_list,
            COALESCE(caution, 0) AS caution,
            COALESCE(effective_visibility_scope, 1) AS effective_visibility_scope
        FROM available_models_info_view
        ORDER BY model_id
    """)

    visible_scopes = _parse_visible_scopes()

    data: Dict[int, dict] = {}
    ids_by_brand_model: Dict[Tuple[str, str], List[int]] = {}
    for row in rows:
        try:
            model_id = int(row['model_id'])
        except (KeyError, TypeError, ValueError):
            continue
        # Scope filtering: only include models whose effective scope is allowed
        try:
            scope = int(row.get('effective_visibility_scope') or 1)
        except (TypeError, ValueError):
            scope = 1
        if scope not in visible_scopes:
            continue
        item = dict(row)
        item['model_id'] = model_id
        data[model_id] = item
        model_name = item.get('model_name')
        key_zh = _brand_model_key(item.get('brand_name_zh'), model_name)
        ids_by_brand_model.setdefault(key_zh, []).append(model_id)
        key_en = _brand_model_key(item.get('brand_name_en'), model_name)
        ids_by_brand_model.setdefault(key_en, []).append(model_id)

    purchase_links_by_model: Dict[int, List[dict]] = {}
    if data:
        try:
            from app import purchase_links
            purchase_links_by_model = purchase_links.get_active_purchase_links_by_model_ids(data.keys())
        except Exception:
            if _logger:
                _logger.exception('model_meta_cache purchase link load failed')
            purchase_links_by_model = {}
    for model_id, item in data.items():
        item['purchase_links'] = list(purchase_links_by_model.get(model_id) or [])

    with _cache_lock:
        global _cache_data, _model_ids_by_brand_model, _cache_loaded_at
        _cache_data = data
        # One model_id can be indexed by both zh/en brand keys; keep the lookup list unique.
        _model_ids_by_brand_model = {
            key: sorted(set(model_ids))
            for key, model_ids in ids_by_brand_model.items()
        }
        _cache_loaded_at = time.time()
    return {mid: dict(meta) for mid, meta in data.items()}


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
            _logger.exception('model_meta_cache refresh failed')
        # Keep stale cache (if any) and back off so we don't hammer DB on every request.
        with _cache_lock:
            _cache_loaded_at = time.time()
            has_data = bool(_cache_data)
        if has_data:
            return
        raise


def invalidate() -> None:
    """Immediately expire the in-memory cache so the next read triggers a fresh DB load.

    This is used by the visibility-refresh path so that admin changes to
    is_valid or visibility_scope are reflected without waiting for the TTL.
    """
    with _cache_lock:
        global _cache_loaded_at
        _cache_loaded_at = 0.0

def get_model_meta(model_id: int, *, force_refresh: bool = False) -> dict | None:
    try:
        model_id = int(model_id)
    except (TypeError, ValueError):
        return None
    _ensure_loaded(force_refresh=force_refresh)
    with _cache_lock:
        meta = _cache_data.get(model_id)
        return dict(meta) if isinstance(meta, dict) else None


def get_many_model_meta(model_ids, *, force_refresh: bool = False) -> Dict[int, dict]:
    wanted = []
    for model_id in model_ids or []:
        try:
            wanted.append(int(model_id))
        except (TypeError, ValueError):
            continue
    _ensure_loaded(force_refresh=force_refresh)
    with _cache_lock:
        return {
            model_id: dict(_cache_data[model_id])
            for model_id in wanted
            if isinstance(_cache_data.get(model_id), dict)
        }


def get_all_model_meta(*, force_refresh: bool = False) -> Dict[int, dict]:
    _ensure_loaded(force_refresh=force_refresh)
    with _cache_lock:
        return {model_id: dict(meta) for model_id, meta in _cache_data.items()}


def get_all_model_ids(*, force_refresh: bool = False) -> List[int]:
    _ensure_loaded(force_refresh=force_refresh)
    with _cache_lock:
        return list(_cache_data.keys())


def get_model_ids_for_brand_model(brand_name_zh: str, model_name: str, *, force_refresh: bool = False) -> List[int]:
    _ensure_loaded(force_refresh=force_refresh)
    key = _brand_model_key(brand_name_zh, model_name)
    with _cache_lock:
        return list(_model_ids_by_brand_model.get(key) or [])
