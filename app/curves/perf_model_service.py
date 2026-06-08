# -*- coding: utf-8 -*-
"""
perf_model_service: Performance-curve service layer.

This module is the single, authoritative entry point for the business layer
when requesting PCHIP performance-curve models.  It manages the full model
lifecycle:

    * TTL-based cache validation (configurable via PERF_MODEL_TTL_SECS env var)
    * env_key validation on every hot-path load (catches parameter changes
      after a runtime reload such as SIGHUP)
    * Disk cache load / staleness detection
    * On-demand rebuild from raw DB data (delegated to perf_raw_source)
    * Batch miss / stale handling
    * Correct delegation so that raw-point validation never leaks to the
      business layer

Architecture
------------

    Business layer (fancoolserver.py)
        │
        ▼
    perf_model_service          ← this module  (TTL, env_key check, orchestrate)
        │
        ├── pchip_cache         ← disk I/O, model build, in-memory LRU
        └── perf_raw_source     ← DB fetch of raw rpm/airflow/noise points

Engine Injection
----------------
Call :func:`set_engine` once at application startup before any DB-backed
operations.  On Windows / Jupyter where a DB is unavailable, simply omit the
call — the service will still return disk-cached models (or None for misses).

TTL Configuration
-----------------
Set ``PERF_MODEL_TTL_SECS`` environment variable to control how long a
disk-cached model is trusted before being revalidated against the DB.

    * Default: 86 400 s (24 hours)
    * Set to ``0`` to always validate on every request (expensive but safest)

Note: regardless of TTL, every hot-path load also checks ``env_key`` against
the current curve-fit parameters.  A mismatch (e.g. after SIGHUP) causes the
entry to be treated as stale and triggers revalidation immediately.

In-memory LRU promotion is handled inside ``pchip_cache`` and only occurs
when a model is built or rebuilt via ``get_or_build_unified_perf_model``,
not on plain disk loads.

When the TTL is exceeded the service performs a *lightweight* revalidation:
it fetches the current raw points from the DB, computes their hash, and
compares with the stored ``data_hash`` in the cache meta.  If the hash
matches the cache is refreshed in-place (mtime updated); if not, the model
is rebuilt and re-saved to disk.
"""

from __future__ import annotations

import os
import logging
import time
from typing import Dict, Any, List, Tuple, Optional

from app.curves import pchip_cache as _pchip_cache
from app.curves import perf_raw_source as _raw_source

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Engine proxy — injecting the engine also propagates to perf_raw_source
# ---------------------------------------------------------------------------

def set_engine(engine) -> None:
    """Inject the SQLAlchemy engine used for raw-point DB queries.

    This must be called once at application startup before any database
    operations are performed by this service.

    Args:
        engine: SQLAlchemy engine instance.
    """
    _raw_source.set_engine(engine)


# ---------------------------------------------------------------------------
# TTL helpers
# ---------------------------------------------------------------------------

def _ttl_secs() -> float:
    """Return the configured model TTL in seconds (default 86 400 = 24 h)."""
    try:
        return float(os.getenv("PERF_MODEL_TTL_SECS", "86400"))
    except Exception:
        return 86400.0


def _is_cache_stale(model_id: int, condition_id: int) -> bool:
    """Return True if the on-disk cache file is older than the TTL."""
    path = _pchip_cache.unified_perf_path(model_id, condition_id)
    if not os.path.isfile(path):
        return True  # missing → treat as stale
    ttl = _ttl_secs()
    if ttl <= 0:
        return True  # TTL=0 → always revalidate
    age = time.time() - os.path.getmtime(path)
    return age > ttl


# ---------------------------------------------------------------------------
# Internal: rebuild one or many pairs from DB
# ---------------------------------------------------------------------------

def _rebuild_pairs(pairs: List[Tuple[int, int]]) -> Dict[str, Dict[str, Any]]:
    """Fetch raw points from DB and build / save unified perf models.

    Returns a dict keyed by ``"model_id_condition_id"`` containing the
    freshly built (or cache-hit) unified model dicts.
    """
    out: Dict[str, Dict[str, Any]] = {}
    if not pairs:
        return out

    try:
        bucket = _raw_source.fetch_raw_perf_rows(pairs)
    except RuntimeError:
        # Engine not set (e.g. Windows local testing without DB).
        log.debug(
            "perf_model_service: engine not available — skipping DB rebuild for %s",
            pairs,
        )
        return out
    except Exception as exc:
        log.warning("perf_model_service: DB fetch failed for %s: %s", pairs, exc)
        return out

    for key, b in bucket.items():
        mid = int(b["model_id"])
        cid = int(b["condition_id"])
        rpm_list = [(float(v) if v is not None else None) for v in (b.get("rpm") or [])]
        air_list = [(float(v) if v is not None else None) for v in (b.get("airflow") or [])]
        noi_list = [(float(v) if v is not None else None) for v in (b.get("noise") or [])]
        try:
            unified = _pchip_cache.get_or_build_unified_perf_model(
                mid, cid, rpm_list, air_list, noi_list
            )
            if unified is not None:
                out[key] = unified
        except Exception as exc:
            log.warning(
                "perf_model_service: build failed for (%s,%s): %s", mid, cid, exc
            )
    return out


def _validate_and_maybe_rebuild(
    model_id: int, condition_id: int
) -> Optional[Dict[str, Any]]:
    """Check whether the on-disk cache is still valid by recomputing the hash.

    If the raw-point hash in the DB matches the stored ``data_hash`` the
    cached model is returned as-is (after touching the file mtime so the TTL
    clock resets).  If the hash differs the model is rebuilt from scratch.

    Returns the (possibly rebuilt) unified model, or None on failure.
    """
    try:
        bucket = _raw_source.fetch_raw_perf_rows([(model_id, condition_id)])
    except RuntimeError:
        # No engine — return existing cached model without revalidating.
        return _pchip_cache.load_unified_perf_model(model_id, condition_id)
    except Exception as exc:
        log.warning(
            "perf_model_service: validation DB fetch failed (%s,%s): %s",
            model_id, condition_id, exc,
        )
        return _pchip_cache.load_unified_perf_model(model_id, condition_id)

    key = f"{model_id}_{condition_id}"
    b = bucket.get(key)
    if b is None:
        # No data in DB — keep existing cache.
        return _pchip_cache.load_unified_perf_model(model_id, condition_id)

    rpm_list = [(float(v) if v is not None else None) for v in (b.get("rpm") or [])]
    air_list = [(float(v) if v is not None else None) for v in (b.get("airflow") or [])]
    noi_list = [(float(v) if v is not None else None) for v in (b.get("noise") or [])]

    new_hash = _pchip_cache.raw_triples_hash(rpm_list, air_list, noi_list)
    env_key = _pchip_cache.env_key_for_perf()

    cached = _pchip_cache.load_unified_perf_model(model_id, condition_id)
    if cached:
        meta = cached.get("meta") or {}
        if meta.get("data_hash") == new_hash and meta.get("env_key") == env_key:
            # Hash still matches — touch the file so TTL resets.
            path = _pchip_cache.unified_perf_path(model_id, condition_id)
            try:
                os.utime(path, None)
            except Exception:
                pass
            return cached

    # Hash mismatch or no cache — rebuild.
    log.info(
        "perf_model_service: cache invalidated for (%s,%s) — rebuilding",
        model_id, condition_id,
    )
    try:
        rebuilt = _pchip_cache.get_or_build_unified_perf_model(
            model_id, condition_id, rpm_list, air_list, noi_list
        )
        return rebuilt
    except Exception as exc:
        log.warning(
            "perf_model_service: rebuild failed (%s,%s): %s", model_id, condition_id, exc
        )
        return cached  # return potentially stale cache rather than nothing


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_perf_model(model_id: int, condition_id: int) -> Optional[Dict[str, Any]]:
    """Return the unified PCHIP performance model for one (model_id, condition_id).

    Hot path (cache hit within TTL)::

        disk_cache → return immediately (no DB query)

    Warm path (cache exists but TTL exceeded)::

        fetch raw hash from DB → compare → return cache or rebuild

    Cold path (cache missing)::

        fetch raw points from DB → build model → save to disk → return

    Args:
        model_id: Model ID.
        condition_id: Condition ID.

    Returns:
        Unified PCHIP model dict (type ``perf_pchip_v1``) or ``None`` if
        unavailable (no DB access and no cached file).
    """
    if not _is_cache_stale(model_id, condition_id):
        # Fast path: TTL not exceeded — load from disk and validate env_key.
        # Catches parameter changes applied at runtime (e.g. SIGHUP reload).
        mdl = _pchip_cache.load_unified_perf_model(model_id, condition_id)
        if mdl is not None:
            meta = mdl.get("meta") or {}
            if meta.get("env_key") == _pchip_cache.env_key_for_perf():
                return mdl
            # env_key mismatch — fall through to validate / rebuild.

    # Cache is stale or absent — validate / rebuild.
    mdl = _validate_and_maybe_rebuild(model_id, condition_id)
    return mdl


def get_perf_models(
    pairs: List[Tuple[int, int]],
) -> Dict[str, Dict[str, Any]]:
    """Batch variant of :func:`get_perf_model`.

    Splits *pairs* into:

    * **hot** — disk cache present and within TTL → loaded directly.
    * **stale** — disk cache present but TTL exceeded → validated individually.
    * **cold** — disk cache absent → batch-fetched from DB and rebuilt.

    Returns a dict keyed by ``"model_id_condition_id"``.
    """
    if not pairs:
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    stale_pairs: List[Tuple[int, int]] = []
    cold_pairs: List[Tuple[int, int]] = []

    for mid, cid in pairs:
        path = _pchip_cache.unified_perf_path(mid, cid)
        if not os.path.isfile(path):
            cold_pairs.append((mid, cid))
            continue
        if _is_cache_stale(mid, cid):
            stale_pairs.append((mid, cid))
            continue
        # Hot: within TTL — load from disk.
        mdl = _pchip_cache.load_unified_perf_model(mid, cid)
        if mdl is not None:
            # Mirror single-model behavior: verify env_key (and meta) before
            # trusting a hot cache entry. If the env_key does not match the
            # current env_key_for_perf(), treat as cold so it will be rebuilt.
            current_env_key = _pchip_cache.env_key_for_perf()
            meta = None
            if isinstance(mdl, dict):
                meta = mdl.get("meta")
            cached_env_key = None
            if isinstance(meta, dict):
                cached_env_key = meta.get("env_key")
            if cached_env_key is not None and cached_env_key == current_env_key:
                out[f"{mid}_{cid}"] = mdl
            else:
                # Metadata missing or env_key mismatch — classify as cold so it
                # will be rebuilt under the current environment.
                cold_pairs.append((mid, cid))
        else:
            # File disappeared between check and load — treat as cold.
            cold_pairs.append((mid, cid))

    # Stale pairs: validate one-by-one (lightweight hash check).
    for mid, cid in stale_pairs:
        mdl = _validate_and_maybe_rebuild(mid, cid)
        if mdl is not None:
            out[f"{mid}_{cid}"] = mdl

    # Cold pairs: batch DB fetch + rebuild.
    if cold_pairs:
        rebuilt = _rebuild_pairs(cold_pairs)
        out.update(rebuilt)

    return out


def build_perf_models(
    pairs: List[Tuple[int, int]],
) -> Dict[str, Dict[str, Any]]:
    """Force-rebuild performance models for the given pairs from the DB.

    Intended for administrative operations (e.g. post-import cache warm-up)
    where the caller explicitly wants a fresh rebuild rather than a cached
    result.

    Args:
        pairs: List of ``(model_id, condition_id)`` tuples to rebuild.

    Returns:
        Dict keyed by ``"model_id_condition_id"`` with rebuilt unified model
        dicts.  Pairs for which DB data is unavailable are omitted.
    """
    return _rebuild_pairs(pairs)
