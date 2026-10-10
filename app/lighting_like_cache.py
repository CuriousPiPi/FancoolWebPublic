# -*- coding: utf-8 -*-
"""
lighting_like_cache: Full-table shared cache for active_like_d30_view (lighting likes).

Stores model_id → lighting_like_count (d30) in a module-level dict with the
same architecture as like_rank_cache (in-process hot cache + shared JSON file).

Rate-dimension semantics:
  rate_dimension_id = 2, condition_id = 0  →  lighting like
This module reads the lighting-like slice from active_like_d30_view and exposes
per-model lookups.

TTL: LIGHTING_LIKE_CACHE_TTL_SEC (default 300 seconds, env-configurable).
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
import time
from typing import Dict, Optional

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# TTL constants
# ---------------------------------------------------------------------------

LIGHTING_LIKE_CACHE_TTL_SEC: int = int(os.getenv("LIGHTING_LIKE_CACHE_TTL_SEC", "300"))

# ---------------------------------------------------------------------------
# In-process state
# ---------------------------------------------------------------------------

# Hot cache: model_id (int) -> lighting_like_count (int)
_hot_cache: Dict[int, int] = {}
_hot_cache_at: float = 0.0
_hot_cache_lock = threading.Lock()
_refresh_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Helpers that delegate to fancoolserver / pchip_cache infrastructure
# ---------------------------------------------------------------------------

def _cache_dir() -> str:
    from app.curves.pchip_cache import curve_cache_dir
    return curve_cache_dir()


def _cache_path() -> str:
    return os.path.join(_cache_dir(), "lighting_like_cache.json")


def _atomic_write(path: str, payload: dict) -> None:
    base = os.path.realpath(_cache_dir())
    resolved = os.path.realpath(path)
    try:
        if os.path.commonpath([base, resolved]) != base or resolved == base:
            raise ValueError()
    except ValueError:
        raise ValueError(f"_atomic_write: path outside cache dir: {path!r}")
    d = os.path.dirname(resolved)
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix="_tmp_llc_", suffix=".json", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        os.replace(tmp, resolved)
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass


def _fetch_from_db(fetch_all_fn) -> Dict[int, int]:
    """Query active_like_d30_view (lighting-like slice) and return a model_id → count dict."""
    rows = fetch_all_fn(
        "SELECT model_id, "
        "COALESCE(active_like_d30, 0) AS lighting_like_count "
        "FROM active_like_d30_view "
        "WHERE rate_dimension_id = 2 AND condition_id = 0"
    )
    result: Dict[int, int] = {}
    for r in rows:
        try:
            result[int(r["model_id"])] = max(0, int(r.get("lighting_like_count") or 0))
        except Exception:
            pass
    return result


def _load_from_file() -> Optional[tuple[float, Dict[int, int]]]:
    try:
        p = _cache_path()
        if not os.path.isfile(p):
            return None
        with open(p, "r", encoding="utf-8") as f:
            payload = json.load(f)
        written_at = float(payload.get("written_at") or 0)
        if (time.time() - written_at) > LIGHTING_LIKE_CACHE_TTL_SEC:
            return None
        raw = payload.get("data") or {}
        data: Dict[int, int] = {}
        for k, v in raw.items():
            try:
                data[int(k)] = max(0, int(v or 0))
            except Exception:
                pass
        return (written_at, data)
    except Exception as e:
        _logger.warning("[lighting_like_cache] load from file failed: %s", e)
        return None


def _save_to_file(data: Dict[int, int], written_at: float) -> None:
    try:
        serialisable = {str(mid): lc for mid, lc in data.items()}
        payload = {"data": serialisable, "written_at": written_at}
        _atomic_write(_cache_path(), payload)
    except Exception as e:
        _logger.warning("[lighting_like_cache] save to file failed: %s", e)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _is_hot_cache_fresh() -> bool:
    with _hot_cache_lock:
        if _hot_cache_at <= 0.0:
            return False
        return (time.time() - _hot_cache_at) < LIGHTING_LIKE_CACHE_TTL_SEC


def _update_hot_cache(data: Dict[int, int], fetched_at: float) -> None:
    with _hot_cache_lock:
        _hot_cache.clear()
        _hot_cache.update(data)
        global _hot_cache_at
        _hot_cache_at = fetched_at


def ensure_cache(fetch_all_fn, force: bool = False) -> None:
    """Ensure the in-process hot cache is populated and fresh."""
    if not force and _is_hot_cache_fresh():
        return

    with _refresh_lock:
        if not force and _is_hot_cache_fresh():
            return

        if not force:
            file_result = _load_from_file()
            if file_result is not None:
                written_at, data = file_result
                _update_hot_cache(data, written_at)
                _logger.debug(
                    "[lighting_like_cache] warmed from file (pid=%s, rows=%d)",
                    os.getpid(), len(data),
                )
                return

        try:
            now = time.time()
            data = _fetch_from_db(fetch_all_fn)
            _update_hot_cache(data, now)
            _save_to_file(data, now)
            _logger.debug(
                "[lighting_like_cache] refreshed from DB (pid=%s, rows=%d)",
                os.getpid(), len(data),
            )
        except Exception as e:
            _logger.warning("[lighting_like_cache] DB refresh failed: %s", e)
            with _hot_cache_lock:
                global _hot_cache_at
                _hot_cache_at = time.time()


def get_lighting_like_count(model_id: int, fetch_all_fn=None) -> int:
    """Return the d30 lighting like count for *model_id* from cache (0 if absent)."""
    if fetch_all_fn is not None:
        ensure_cache(fetch_all_fn)
    with _hot_cache_lock:
        return _hot_cache.get(model_id, 0)


def get_all_lighting_like_counts(fetch_all_fn=None) -> Dict[int, int]:
    """Return the full model_id → lighting_like_count snapshot."""
    if fetch_all_fn is not None:
        ensure_cache(fetch_all_fn)
    with _hot_cache_lock:
        return dict(_hot_cache)


def refresh_lighting_like_counts(fetch_all_fn, force: bool = False) -> None:
    """Explicitly refresh the cache."""
    ensure_cache(fetch_all_fn, force=force)
