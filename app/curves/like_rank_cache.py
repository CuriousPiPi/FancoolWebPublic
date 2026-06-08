# -*- coding: utf-8 -*-
"""
like_rank_cache: Full-table shared cache for like_rank_d30_view.

Loads the entire like_rank_d30_view snapshot once per TTL period and exposes
per-(model_id, condition_id) like counts via a simple dict lookup, eliminating
per-condition DB round-trips in the ABC / radar / rankings computation chains.

Cache architecture (consistent with denom_cache / rankings_v2 patterns):
  - In-process hot cache: a module-level dict protected by a threading.Lock.
  - Shared file cache: a JSON file in the same curve_cache_dir as other caches,
    written atomically so multiple workers can share the same snapshot.
  - Table-level TTL: 5 minutes (LIKE_RANK_CACHE_TTL_SEC).  After expiry the
    entire snapshot is refreshed in one query; there is no per-row TTL.

Multi-thread / multi-worker behaviour (per-process locking only):
  - On a cache miss / expiry each worker process first tries to load the shared file.
  - If the file is fresh enough the worker reuses it, avoiding a DB round-trip.
  - If the file is stale / absent the worker acquires a per-process thread lock,
    re-checks (double-checked locking) and then queries the DB and writes the file
    atomically.
  - Threads that lose the lock race within a worker simply proceed without the file
    write; all threads still update their own in-process hot cache from the DB
    result.
  - Note: locking is in-process only; separate Gunicorn worker processes may still
    perform concurrent refreshes, but they share the same on-disk JSON snapshot.

Windows / Jupyter compatibility:
  - Uses an internal JSON-on-disk cache in curve_cache_dir written via
    `_atomic_write`, with no fcntl usage, so it remains compatible with
    Windows and Jupyter environments.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
import time
from typing import Dict, Iterable, Optional, Tuple

_logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# TTL constants
# ---------------------------------------------------------------------------

#: Default full-table cache lifetime in seconds (5 minutes).
LIKE_RANK_CACHE_TTL_SEC: int = int(os.getenv("LIKE_RANK_CACHE_TTL_SEC", "300"))

# ---------------------------------------------------------------------------
# In-process state
# ---------------------------------------------------------------------------

# Hot cache: (model_id, condition_id) -> like_count
_hot_cache: Dict[Tuple[int, int], int] = {}
# Timestamp (time.time()) when the hot cache was last populated from DB or file.
_hot_cache_at: float = 0.0
_hot_cache_lock = threading.Lock()

# Separate lock to serialise DB refreshes within a single process so that
# multiple threads / workers don't all query DB simultaneously on expiry.
_refresh_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Helpers that delegate to fancoolserver infrastructure
# ---------------------------------------------------------------------------

def _cache_dir() -> str:
    """Return the shared cache directory (same as curve_cache_dir)."""
    from app.curves.pchip_cache import curve_cache_dir
    return curve_cache_dir()


def _like_cache_path() -> str:
    return os.path.join(_cache_dir(), "like_rank_cache.json")


def _atomic_write(path: str, payload: dict) -> None:
    """Atomically write *payload* as JSON to *path* within the cache dir."""
    base = os.path.realpath(_cache_dir())
    resolved = os.path.realpath(path)
    if not resolved.startswith(base + os.sep) and resolved != base:
        raise ValueError(f"_atomic_write: path outside cache dir: {path!r}")
    d = os.path.dirname(resolved)
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix="_tmp_lrc_", suffix=".json", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        os.replace(tmp, resolved)
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass


def _fetch_from_db(fetch_all_fn) -> Dict[Tuple[int, int], int]:
    """Query like_rank_d30_view and return a (model_id, condition_id) -> like_count dict."""
    rows = fetch_all_fn(
        "SELECT model_id, condition_id, "
        "COALESCE(like_by_model_condition_d30, 0) AS like_count "
        "FROM like_rank_d30_view"
    )
    result: Dict[Tuple[int, int], int] = {}
    for r in rows:
        try:
            result[(int(r["model_id"]), int(r["condition_id"]))] = max(
                0, int(r.get("like_count") or 0)
            )
        except Exception:
            pass
    return result


def _load_from_file() -> Optional[Tuple[float, Dict[Tuple[int, int], int]]]:
    """Load the shared file cache.

    Returns (written_at, data_dict) if the file exists and is within TTL,
    or None if stale / missing / corrupt.
    """
    try:
        p = _like_cache_path()
        if not os.path.isfile(p):
            return None
        with open(p, "r", encoding="utf-8") as f:
            payload = json.load(f)
        written_at = float(payload.get("written_at") or 0)
        if (time.time() - written_at) > LIKE_RANK_CACHE_TTL_SEC:
            return None
        raw = payload.get("data") or {}
        # Keys were serialised as "model_id:condition_id" strings.
        data: Dict[Tuple[int, int], int] = {}
        for k, v in raw.items():
            try:
                mid_s, cid_s = k.split(":", 1)
                data[(int(mid_s), int(cid_s))] = max(0, int(v or 0))
            except Exception:
                pass
        return (written_at, data)
    except Exception as e:
        _logger.warning("[like_rank_cache] load from file failed: %s", e)
        return None


def _save_to_file(data: Dict[Tuple[int, int], int], written_at: float) -> None:
    """Save *data* to the shared file cache (best-effort; errors are logged only)."""
    try:
        # Serialise tuple keys as "mid:cid" strings since JSON only supports string keys.
        serialisable = {f"{mid}:{cid}": lc for (mid, cid), lc in data.items()}
        payload = {"data": serialisable, "written_at": written_at}
        _atomic_write(_like_cache_path(), payload)
    except Exception as e:
        _logger.warning("[like_rank_cache] save to file failed: %s", e)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _is_hot_cache_fresh() -> bool:
    with _hot_cache_lock:
        if _hot_cache_at <= 0.0:
            # Cache has never been populated; treat as stale.
            return False
        return (time.time() - _hot_cache_at) < LIKE_RANK_CACHE_TTL_SEC


def _update_hot_cache(data: Dict[Tuple[int, int], int], fetched_at: float) -> None:
    with _hot_cache_lock:
        _hot_cache.clear()
        _hot_cache.update(data)
        global _hot_cache_at
        _hot_cache_at = fetched_at


def ensure_cache(fetch_all_fn, force: bool = False) -> None:
    """Ensure the in-process hot cache is populated and fresh.

    Call order:
      1. If hot cache is fresh (and not forced), return immediately.
      2. Acquire refresh lock (serialise DB queries within one process).
      3. Re-check hot cache (double-checked locking).
      4. If *not* forcing, try loading the shared file cache; if fresh, warm
         hot cache from file and return.
      5. Query DB, warm hot cache, write to file.

    When *force* is True steps 1, 3 and 4 are skipped so that the DB is
    always queried and the file cache is overwritten with fresh data.

    Errors at any step are caught; a best-effort result (possibly empty) is
    always stored in the hot cache so callers do not block.
    """
    if not force and _is_hot_cache_fresh():
        return

    with _refresh_lock:
        # Double-checked locking: another thread may have refreshed while we waited.
        if not force and _is_hot_cache_fresh():
            return

        # Try shared file cache only when not forcing a refresh.
        if not force:
            file_result = _load_from_file()
            if file_result is not None:
                written_at, data = file_result
                _update_hot_cache(data, written_at)
                _logger.debug("[like_rank_cache] warmed from file (pid=%s, rows=%d)", os.getpid(), len(data))
                return

        # Fall back to DB.
        try:
            now = time.time()
            data = _fetch_from_db(fetch_all_fn)
            _update_hot_cache(data, now)
            _save_to_file(data, now)
            _logger.debug("[like_rank_cache] refreshed from DB (pid=%s, rows=%d)", os.getpid(), len(data))
        except Exception as e:
            _logger.warning("[like_rank_cache] DB refresh failed: %s", e)
            # Keep whatever is in the hot cache (may be stale / empty).
            # Mark the timestamp so we don't hammer DB on every request.
            with _hot_cache_lock:
                global _hot_cache_at
                _hot_cache_at = time.time()


def get_like_count(model_id: int, condition_id: int, fetch_all_fn=None) -> int:
    """Return the like count for *(model_id, condition_id)* from cache.

    If the cache is stale and *fetch_all_fn* is provided, the cache is refreshed
    before the lookup.  Returns 0 when the pair is absent from the view.
    """
    if fetch_all_fn is not None:
        ensure_cache(fetch_all_fn)
    with _hot_cache_lock:
        return _hot_cache.get((model_id, condition_id), 0)


def get_like_counts(
    pairs: Iterable[Tuple[int, int]],
    fetch_all_fn=None,
) -> Dict[Tuple[int, int], int]:
    """Return a dict of like counts for the given *(model_id, condition_id)* pairs.

    If the cache is stale and *fetch_all_fn* is provided, the cache is refreshed.
    Missing pairs are absent from the returned dict (equivalent to 0).
    """
    if fetch_all_fn is not None:
        ensure_cache(fetch_all_fn)
    pairs_set = frozenset(pairs)
    with _hot_cache_lock:
        return {p: _hot_cache[p] for p in pairs_set if p in _hot_cache}


def get_all_like_counts(fetch_all_fn=None) -> Dict[Tuple[int, int], int]:
    """Return the full (model_id, condition_id) -> like_count snapshot.

    Refreshes the cache if stale (when *fetch_all_fn* is provided).
    """
    if fetch_all_fn is not None:
        ensure_cache(fetch_all_fn)
    with _hot_cache_lock:
        return dict(_hot_cache)


def refresh_like_counts(fetch_all_fn, force: bool = False) -> None:
    """Explicitly refresh the like count cache.

    *force=True* bypasses the TTL check *and* the shared file cache, always
    querying the DB directly and overwriting the file cache with fresh data.
    """
    ensure_cache(fetch_all_fn, force=force)
