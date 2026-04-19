"""
Scoring system module for FancoolWeb.

Provides ABC score computation, rankings v2, canonical facts, and denom-refresh
background jobs extracted from fancoolserver.py.

Call ``setup(fetch_all_fn, exec_write_fn, logger, app_debug=False)`` once during
app startup before using any public functions.  Call ``start_background_threads()`` to
launch the denom-refresh and like-max-refresh daemon threads.
"""

import os
import json
import math
import time
import threading
import tempfile
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, List, Any

from sqlalchemy import exc as sa_exc

from app.curves.pchip_cache import eval_pchip
from app.curves import like_rank_cache
from app.curves.lock_utils import startup_lock
from app.audio_services import spectrum_reader

# =========================================
# Module-level state — injected via setup()
# =========================================
_fetch_all = None
_exec_write = None
_logger = None
_app_debug = False

CODE_VERSION = os.getenv('CODE_VERSION', '')


def setup(fetch_all_fn, exec_write_fn, logger, app_debug: bool = False):
    """Inject shared dependencies.  Call once during app startup."""
    global _fetch_all, _exec_write, _logger, _app_debug
    _fetch_all = fetch_all_fn
    _exec_write = exec_write_fn
    _logger = logger
    _app_debug = app_debug


# =========================================
# Scoring config helpers
# =========================================

def _parse_radar_condition_ids(env_var: str, default: list) -> list:
    """Parse an ordered comma-separated list of integer condition IDs from an environment variable.

    The order represents the canonical radar slot order: starting from upper-left (UL),
    going counter-clockwise (e.g. UL→L→LL→LR→R→UR for a 6-slot hexagon).

    Returns a deduplicated list of ints preserving first-occurrence order.
    If the environment variable is unset or empty, returns a copy of ``default``.
    Raises ``ValueError`` on invalid (non-integer) values or if the deduplicated
    result is empty or has a different length than ``default`` (e.g. duplicates reduced
    the count), so the app cannot start in a broken UI state.
    """
    raw = os.getenv(env_var, '').strip()
    if not raw:
        return list(default)
    parts = [p.strip() for p in raw.split(',') if p.strip()]
    seen: set = set()
    result: list = []
    for p in parts:
        try:
            cid = int(p)
        except ValueError as exc:
            raise ValueError(
                f"Environment variable {env_var!r}: each element must be an integer, got {p!r}"
            ) from exc
        if cid not in seen:
            seen.add(cid)
            result.append(cid)
    if not result:
        raise ValueError(
            f"Environment variable {env_var!r}: parsed list is empty (value: {raw!r}). "
            f"Expected {len(default)} comma-separated integers."
        )
    if len(result) != len(default):
        raise ValueError(
            f"Environment variable {env_var!r}: expected exactly {len(default)} unique condition IDs "
            f"but got {len(result)} after deduplication (value: {raw!r}). "
            f"Default order is: {default!r}"
        )
    return result


def _parse_json_float_map(env_var: str, key_type=str, clamp_negative: bool = False) -> dict | None:
    """Parse a JSON-encoded float mapping from an environment variable.

    Returns a dict on success, or None if the env var is unset/empty.
    Raises ValueError with a clear message on invalid JSON or non-numeric values.
    When clamp_negative is True, negative values are silently clamped to 0.0.
    """
    raw = os.getenv(env_var, '').strip()
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Environment variable {env_var!r} contains invalid JSON: {exc}"
        ) from exc
    if not isinstance(parsed, dict):
        raise ValueError(
            f"Environment variable {env_var!r} must be a JSON object (got {type(parsed).__name__})"
        )
    result = {}
    for k, v in parsed.items():
        try:
            fv = float(v)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Environment variable {env_var!r}: value for key {k!r} is not numeric: {v!r}"
            ) from exc
        if not math.isfinite(fv):
            raise ValueError(
                f"Environment variable {env_var!r}: value for key {k!r} must be finite, got {v!r}"
            )
        if clamp_negative and fv < 0:
            fv = 0.0
        try:
            typed_key = key_type(k)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Environment variable {env_var!r}: key {k!r} cannot be converted to {key_type.__name__}: {exc}"
            ) from exc
        result[typed_key] = fv
    return result


# =========================================
# ABC Score Config
# =========================================

_DEFAULT_SCORE_ALPHA = {'A': 1.0, 'B': 0.6, 'D': 1.0}
_env_score_alpha = _parse_json_float_map('CONDITION_SCORE_ALPHA_VALUES_JSON')
_score_alpha = dict(_DEFAULT_SCORE_ALPHA)
if _env_score_alpha is not None:
    _score_alpha.update(_env_score_alpha)

SCORE_ALPHA_A: float = _score_alpha.get('A', _DEFAULT_SCORE_ALPHA['A'])
SCORE_ALPHA_B: float = _score_alpha.get('B', _DEFAULT_SCORE_ALPHA['B'])
SCORE_ALPHA_D: float = _score_alpha.get('D', _DEFAULT_SCORE_ALPHA['D'])

_DEFAULT_WA2_ANCHOR_MODEL_ID = 21
_env_wa2_anchor_raw = os.getenv('WA2_ANCHOR_MODEL_ID', '').strip()
if _env_wa2_anchor_raw:
    try:
        WA2_ANCHOR_MODEL_ID: int = int(_env_wa2_anchor_raw)
    except ValueError as _exc:
        raise ValueError(
            f"WA2_ANCHOR_MODEL_ID must be an integer, got {_env_wa2_anchor_raw!r}"
        ) from _exc
else:
    WA2_ANCHOR_MODEL_ID: int = _DEFAULT_WA2_ANCHOR_MODEL_ID

_DEFAULT_SCORE_DIM_WEIGHTS = {
    'WA1': 0.20, 'WA2': 0.30, 'WA3': 0.25,
    'WB': 0.05, 'WC': 0.05, 'WD': 0.15,
}
_env_score_dim_weights = _parse_json_float_map('CONDITION_SCORE_DIMENSION_WEIGHTS_JSON')
_score_dim_weights = dict(_DEFAULT_SCORE_DIM_WEIGHTS)
if _env_score_dim_weights is not None:
    _score_dim_weights.update(_env_score_dim_weights)

SCORE_WA1: float = _score_dim_weights.get('WA1', _DEFAULT_SCORE_DIM_WEIGHTS['WA1'])
SCORE_WA2: float = _score_dim_weights.get('WA2', _DEFAULT_SCORE_DIM_WEIGHTS['WA2'])
SCORE_WA3: float = _score_dim_weights.get('WA3', _DEFAULT_SCORE_DIM_WEIGHTS['WA3'])
SCORE_WB: float = _score_dim_weights.get('WB',  _DEFAULT_SCORE_DIM_WEIGHTS['WB'])
SCORE_WC: float = _score_dim_weights.get('WC',  _DEFAULT_SCORE_DIM_WEIGHTS['WC'])
SCORE_WD: float = _score_dim_weights.get('WD',  _DEFAULT_SCORE_DIM_WEIGHTS['WD'])

SCORE_MAX = 100

# =========================================
# Canonical radar condition list (CCW from UL)
# =========================================
# SCORE_CONDITION_IDS env var: comma-separated integers in the canonical CCW slot order,
# starting from the upper-left vertex and proceeding counter-clockwise.
# For a 6-slot hexagon the slot sequence is: UL → L → LL → LR → R → UR.
# Example: "1,10,7,8,3,2"  (default, matches current frontend layout)
#
# _RADAR_CIDS is the authoritative ordered list consumed by all display logic.
# SCORE_CONDITION_IDS is a frozenset derived from it for O(1) membership tests.
_DEFAULT_RADAR_CIDS: list = [1, 10, 7, 8, 3, 2]
_RADAR_CIDS: list = _parse_radar_condition_ids('SCORE_CONDITION_IDS', _DEFAULT_RADAR_CIDS)
SCORE_CONDITION_IDS: frozenset = frozenset(_RADAR_CIDS)

_DEFAULT_COMPOSITE_WEIGHTS: Dict[int, float] = {
    1: 0.0, 2: 2.0, 3: 2.0, 7: 3.0, 8: 2.0, 10: 3.0,
}
_env_composite_weights = _parse_json_float_map('COMPOSITE_SCORE_CONDITION_WEIGHTS_JSON',
                                                key_type=int, clamp_negative=True)
_composite_weights_merged = dict(_DEFAULT_COMPOSITE_WEIGHTS)
if _env_composite_weights is not None:
    _composite_weights_merged.update(_env_composite_weights)

COMPOSITE_WEIGHTS: Dict[int, float] = {
    cid: max(0.0, _composite_weights_merged.get(cid, 1.0))
    for cid in SCORE_CONDITION_IDS
}

# =========================================
# Scoring profiles: LOW / MED / HI
# =========================================
SCORING_PROFILES = ('low', 'med', 'hi')
_DEFAULT_SCORE_PROFILE = 'med'


def _parse_wa2_anchor(env_suffix: str, fallback: int) -> int:
    raw = os.getenv(f'WA2_ANCHOR_MODEL_ID{env_suffix}', '').strip()
    if not raw:
        return fallback
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(
            f"Environment variable WA2_ANCHOR_MODEL_ID{env_suffix} must be an integer, got {raw!r}"
        ) from exc


def _resolve_alpha(env_suffix: str, fallback: dict) -> dict:
    parsed = _parse_json_float_map(f'CONDITION_SCORE_ALPHA_VALUES_JSON{env_suffix}')
    if parsed is None:
        return dict(fallback)
    result = dict(fallback)
    result.update(parsed)
    return result


def _resolve_dim_weights(env_suffix: str, fallback: dict) -> dict:
    parsed = _parse_json_float_map(f'CONDITION_SCORE_DIMENSION_WEIGHTS_JSON{env_suffix}')
    if parsed is None:
        return dict(fallback)
    result = dict(fallback)
    result.update(parsed)
    return result


def _build_profile_cfg(wa2_anchor: int, alpha: dict, dim_weights: dict) -> dict:
    return {
        'wa2_anchor': wa2_anchor,
        'alpha_a': alpha.get('A', _DEFAULT_SCORE_ALPHA['A']),
        'alpha_b': alpha.get('B', _DEFAULT_SCORE_ALPHA['B']),
        'alpha_d': alpha.get('D', _DEFAULT_SCORE_ALPHA['D']),
        'score_wa1': dim_weights.get('WA1', _DEFAULT_SCORE_DIM_WEIGHTS['WA1']),
        'score_wa2': dim_weights.get('WA2', _DEFAULT_SCORE_DIM_WEIGHTS['WA2']),
        'score_wa3': dim_weights.get('WA3', _DEFAULT_SCORE_DIM_WEIGHTS['WA3']),
        'score_wb':  dim_weights.get('WB',  _DEFAULT_SCORE_DIM_WEIGHTS['WB']),
        'score_wc':  dim_weights.get('WC',  _DEFAULT_SCORE_DIM_WEIGHTS['WC']),
        'score_wd':  dim_weights.get('WD',  _DEFAULT_SCORE_DIM_WEIGHTS['WD']),
    }


_med_alpha   = _resolve_alpha('_MED', _score_alpha)
_med_weights = _resolve_dim_weights('_MED', _score_dim_weights)
_med_wa2     = _parse_wa2_anchor('_MED', WA2_ANCHOR_MODEL_ID)
_med_cfg     = _build_profile_cfg(_med_wa2, _med_alpha, _med_weights)

_low_alpha   = _resolve_alpha('_LOW', _med_alpha)
_low_weights = _resolve_dim_weights('_LOW', _med_weights)
_low_wa2     = _parse_wa2_anchor('_LOW', _med_wa2)
_low_cfg     = _build_profile_cfg(_low_wa2, _low_alpha, _low_weights)

_hi_alpha    = _resolve_alpha('_HI', _med_alpha)
_hi_weights  = _resolve_dim_weights('_HI', _med_weights)
_hi_wa2      = _parse_wa2_anchor('_HI', _med_wa2)
_hi_cfg      = _build_profile_cfg(_hi_wa2, _hi_alpha, _hi_weights)

PROFILE_CONFIGS: Dict[str, dict] = {
    'low': _low_cfg,
    'med': _med_cfg,
    'hi':  _hi_cfg,
}

ABC_SOFT_TTL_SEC = int(os.getenv('ABC_SOFT_TTL_SEC', str(10 * 60)))
ABC_HARD_TTL_SEC = int(os.getenv('ABC_HARD_TTL_SEC', str(120 * 60)))

_cond_denom_cache: Dict[str, Dict[int, dict]] = {p: {} for p in SCORING_PROFILES}
_cond_denom_lock = threading.Lock()

_abc_cache: Dict[str, Dict[int, dict]] = {p: {} for p in SCORING_PROFILES}
_abc_cache_lock = threading.Lock()

_abc_inflight: set = set()
_abc_inflight_lock = threading.Lock()

_abc_disk_loaded: Dict[str, bool] = {p: False for p in SCORING_PROFILES}
_abc_disk_loaded_lock = threading.Lock()

# =========================================
# Rankings / display config
# =========================================
_RANKINGS_V2_DISPLAY_LIMIT = 10
# _RADAR_CIDS is defined earlier (CCW canonical order from SCORE_CONDITION_IDS env var)

_rankings_v2_cache: dict = {}
_rankings_v2_cache_lock = threading.Lock()
_RANKINGS_V2_CACHE_TTL_SEC = 600

_rankings_v2_build_locks: Dict[str, threading.Lock] = {p: threading.Lock() for p in SCORING_PROFILES}
_warmup_rankings_inflight = threading.Lock()

# =========================================
# Shared file-based caches for multi-worker environments
# =========================================
_DENOM_CACHE_FILE_TTL_SEC = int(os.getenv('DENOM_CACHE_FILE_TTL_SEC', '14400'))
_RANKINGS_CACHE_FILE_TTL_SEC = int(os.getenv('RANKINGS_CACHE_FILE_TTL_SEC', '600'))
_ABC_CACHE_FILE_TTL_SEC = int(os.getenv('ABC_CACHE_FILE_TTL_SEC', '7200'))


def _shared_cache_dir() -> str:
    # Reuses CURVE_CACHE_DIR intentionally: PCHIP curve/spectrum caches are
    # read-only after generation and have no semantic difference between dev and
    # prod, so sharing the directory saves disk space.  Only the scoring-system
    # JSON caches (denom, canonical_facts, rankings_v2, abc_cache) are
    # frequently written and invalidated, which is why we namespace *those*
    # files by environment (see _score_cache_suffix) instead of splitting the
    # whole directory.
    from app.curves.pchip_cache import curve_cache_dir
    return curve_cache_dir()


def _score_cache_suffix() -> str:
    """Return a dot-prefixed namespace suffix for scoring-system shared cache files.

    Driven by the ``CACHE_NAMESPACE`` environment variable so that dev and prod
    instances sharing the same ``CURVE_CACHE_DIR`` never collide on the
    frequently-written scoring JSON caches.  When ``CACHE_NAMESPACE`` is not
    set the suffix is empty, preserving backward-compatibility with existing
    deployments that have only one environment.

    Examples::

        CACHE_NAMESPACE=dev  -> '.dev'
        CACHE_NAMESPACE=prod -> '.prod'
        (unset)              -> ''
    """
    raw = os.getenv('CACHE_NAMESPACE', '').strip()
    # Allow only ASCII alphanumeric characters, hyphens, and underscores to
    # prevent path-traversal, injection, or unexpected filenames caused by
    # Unicode letters/digits that str.isalnum() would otherwise admit.
    ns = ''.join(
        c for c in raw
        if ('a' <= c <= 'z') or ('A' <= c <= 'Z') or ('0' <= c <= '9') or c in ('-', '_')
    )
    return f'.{ns}' if ns else ''


def _denom_cache_path() -> str:
    # Cache format v2: unified semantics (low_db/common_high_db/high_db/mid_db).
    # We intentionally use a new filename to invalidate pre-unification cache files.
    return os.path.join(_shared_cache_dir(), f'denom_cache_v2{_score_cache_suffix()}.json')


def _rankings_cache_path(profile: str) -> str:
    safe_profile = ''.join(c for c in profile if c.isalnum() or c == '_')
    return os.path.join(_shared_cache_dir(), f'rankings_v2_{safe_profile}{_score_cache_suffix()}.json')


def _abc_cache_path(profile: str) -> str:
    safe_profile = ''.join(c for c in profile if c.isalnum() or c == '_')
    return os.path.join(_shared_cache_dir(), f'abc_cache_{safe_profile}{_score_cache_suffix()}.json')


def _json_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f'Object of type {type(obj).__name__} is not JSON serializable')


def _midpoint_db(low_db, high_db):
    """Return numeric midpoint for two dB bounds, or None when unavailable."""
    if low_db is None or high_db is None:
        return None
    try:
        return (float(low_db) + float(high_db)) / 2.0
    except (TypeError, ValueError, OverflowError):
        return None


def _atomic_json_write(path: str, payload: dict) -> None:
    """Atomically write *payload* as JSON to *path* using a sibling temp file."""
    base = os.path.realpath(_shared_cache_dir())
    resolved = os.path.realpath(path)
    if not resolved.startswith(base + os.sep) and resolved != base:
        raise ValueError(f'_atomic_json_write: path outside cache dir: {path!r}')
    d = os.path.dirname(resolved)
    os.makedirs(d, exist_ok=True)
    tmp_fd, tmp_path = tempfile.mkstemp(dir=d, suffix='.tmp')
    try:
        with os.fdopen(tmp_fd, 'w', encoding='utf-8') as f:
            json.dump(payload, f, default=_json_default, ensure_ascii=False)
        os.replace(tmp_path, resolved)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _denom_cache_fingerprint() -> str:
    parts = [
        f"code={CODE_VERSION}",
        f"cids={sorted(SCORE_CONDITION_IDS)}",
    ]
    for pkey in sorted(SCORING_PROFILES):
        pcfg = PROFILE_CONFIGS[pkey]
        parts.append(
            f"{pkey}:wa2={pcfg['wa2_anchor']}"
            f",aa={pcfg['alpha_a']}"
            f",ab={pcfg['alpha_b']}"
            f",ad={pcfg['alpha_d']}"
            f",wa1={pcfg['score_wa1']}"
            f",wa2={pcfg['score_wa2']}"
            f",wa3={pcfg['score_wa3']}"
            f",wb={pcfg['score_wb']}"
            f",wc={pcfg['score_wc']}"
            f",wd={pcfg['score_wd']}"
        )
    return "|".join(parts)


def _abc_cache_fingerprint() -> str:
    base = _denom_cache_fingerprint()
    cw_str = ','.join(f'{k}:{v:.6g}' for k, v in sorted(COMPOSITE_WEIGHTS.items()))
    return f"{base}|cw={cw_str}"


def _save_denom_cache_to_disk() -> None:
    try:
        with _cond_denom_lock:
            snapshot = {
                pkey: {cid: dict(entry) for cid, entry in by_cid.items()}
                for pkey, by_cid in _cond_denom_cache.items()
            }
        payload = {
            'cond_denom_cache': snapshot,
            'written_at': time.time(),
            'fingerprint': _denom_cache_fingerprint(),
        }
        _atomic_json_write(_denom_cache_path(), payload)
    except Exception as e:
        _logger.warning('[denom_cache] save to disk failed: %s', e)


def _load_denom_cache_from_disk() -> bool:
    try:
        p = _denom_cache_path()
        if not os.path.isfile(p):
            return False
        with open(p, 'r', encoding='utf-8') as f:
            payload = json.load(f)
        written_at = float(payload.get('written_at') or 0)
        if (time.time() - written_at) > _DENOM_CACHE_FILE_TTL_SEC:
            return False
        if payload.get('fingerprint') != _denom_cache_fingerprint():
            if _app_debug:
                _logger.debug('[denom_cache] fingerprint mismatch, ignoring disk cache (pid=%s)', os.getpid())
            return False
        raw = payload.get('cond_denom_cache') or {}
        loaded: Dict[str, Dict[int, dict]] = {}
        for pkey, by_cid in raw.items():
            if pkey not in SCORING_PROFILES:
                continue
            loaded[pkey] = {}
            for k, v in (by_cid or {}).items():
                entry = dict(v or {})
                # Strict cache schema: do not migrate legacy semantics from older files.
                if 'common_high_db' not in entry or 'high_db' not in entry:
                    return False
                loaded[pkey][int(k)] = entry
        if not all(p in loaded for p in SCORING_PROFILES):
            return False
        for pkey in SCORING_PROFILES:
            if not all(cid in loaded[pkey] for cid in SCORE_CONDITION_IDS):
                return False
        with _cond_denom_lock:
            for pkey in SCORING_PROFILES:
                _cond_denom_cache[pkey].update(loaded[pkey])
        if _app_debug:
            _logger.debug('[denom_cache] loaded from disk (pid=%s)', os.getpid())
        return True
    except Exception as e:
        _logger.warning('[denom_cache] load from disk failed: %s', e)
        return False


def _save_rankings_cache(profile: str, result: dict) -> None:
    try:
        payload = {'result': result, 'written_at': time.time(), 'profile': profile}
        _atomic_json_write(_rankings_cache_path(profile), payload)
    except Exception as e:
        _logger.warning('[rankings_v2] save to disk failed (profile=%s): %s', profile, e)


def _normalize_rankings_item(item: dict) -> None:
    """Normalize condition_id (CID) keys back to int in a rankings item after JSON round-trip."""
    for key in ('condition_scores', 'condition_heat', 'condition_likes'):
        val = item.get(key)
        if isinstance(val, dict):
            item[key] = {int(k): v for k, v in val.items()}
    score_profiles = item.get('score_profiles')
    if isinstance(score_profiles, dict):
        for pkey, sp in score_profiles.items():
            if isinstance(sp, dict):
                cond_scores = sp.get('condition_scores')
                if isinstance(cond_scores, dict):
                    sp['condition_scores'] = {int(k): v for k, v in cond_scores.items()}


def _normalize_rankings_cache_payload(result: dict) -> None:
    """Normalize all CID keys in a rankings cache payload (boards + model_lookup)."""
    for board_key in ('heat_board', 'performance_board'):
        board = result.get(board_key)
        if isinstance(board, list):
            for item in board:
                _normalize_rankings_item(item)
    ml = result.get('model_lookup')
    if isinstance(ml, dict):
        int_keyed = {}
        for k, item in ml.items():
            try:
                mid = int(k)
            except Exception:
                continue
            _normalize_rankings_item(item)
            int_keyed[mid] = item
        result['model_lookup'] = int_keyed


def _load_rankings_cache_from_disk(profile: str):
    """Return (written_at, result_dict) from disk if fresh, or None if stale/missing."""
    try:
        p = _rankings_cache_path(profile)
        if not os.path.isfile(p):
            return None
        with open(p, 'r', encoding='utf-8') as f:
            payload = json.load(f)
        written_at = float(payload.get('written_at') or 0)
        if (time.time() - written_at) > _RANKINGS_CACHE_FILE_TTL_SEC:
            return None
        result = payload.get('result')
        if not isinstance(result, dict):
            return None
        _normalize_rankings_cache_payload(result)
        return (written_at, result)
    except Exception as e:
        _logger.warning('[rankings_v2] load from disk failed (profile=%s): %s', profile, e)
        return None


def _normalize_abc_entry(entry: dict) -> None:
    """Normalize condition_id (CID) keys back to int in an ABC cache entry after JSON round-trip."""
    conds = entry.get('conditions')
    if isinstance(conds, dict):
        entry['conditions'] = {int(k): v for k, v in conds.items()}


def _validate_abc_cache_entry(entry: dict) -> dict:
    if not isinstance(entry, dict):
        raise ValueError("ABC cache entry is not a dict")
    if 'cached_at' not in entry:
        raise KeyError("ABC cache entry missing 'cached_at'")
    try:
        entry['cached_at'] = float(entry['cached_at'])
    except (TypeError, ValueError):
        raise ValueError(f"ABC cache entry 'cached_at' is not numeric: {entry['cached_at']!r}")
    if 'conditions' not in entry:
        raise KeyError("ABC cache entry missing 'conditions'")
    if not isinstance(entry['conditions'], dict):
        raise ValueError("ABC cache entry 'conditions' is not a dict")
    if 'composite_score' in entry:
        cs = entry['composite_score']
        if cs is not None and not isinstance(cs, int):
            try:
                entry['composite_score'] = int(cs)
            except (TypeError, ValueError):
                raise ValueError(f"ABC cache entry 'composite_score' is not int/None: {cs!r}")
    else:
        entry['composite_score'] = None
    return entry


def _get_current_denom_written_at() -> float:
    try:
        p = _denom_cache_path()
        if not os.path.isfile(p):
            return 0.0
        with open(p, 'r', encoding='utf-8') as f:
            payload = json.load(f)
        return float(payload.get('written_at') or 0)
    except Exception:
        return 0.0


def _save_abc_cache_to_disk(profile: str) -> None:
    try:
        with _abc_cache_lock:
            snapshot = {str(mid): dict(entry)
                        for mid, entry in _abc_cache[profile].items()}
        if not snapshot:
            return
        payload = {
            'entries': snapshot,
            'written_at': time.time(),
            'fingerprint': _abc_cache_fingerprint(),
            'profile': profile,
            'denom_written_at': _get_current_denom_written_at(),
        }
        _atomic_json_write(_abc_cache_path(profile), payload)
        if _app_debug:
            _logger.debug('[abc_cache] saved %d entries to disk (profile=%s)',
                          len(snapshot), profile)
    except Exception as e:
        _logger.warning('[abc_cache] save to disk failed (profile=%s): %s', profile, e)


def _invalidate_abc_disk_cache() -> None:
    with _abc_disk_loaded_lock:
        for p in SCORING_PROFILES:
            _abc_disk_loaded[p] = False
    for p in SCORING_PROFILES:
        try:
            path = _abc_cache_path(p)
            if os.path.isfile(path):
                os.remove(path)
        except Exception as e:
            _logger.warning('[abc_cache] invalidate disk failed (profile=%s): %s', p, e)


def _try_load_abc_from_disk(profile: str) -> None:
    with _abc_disk_loaded_lock:
        if _abc_disk_loaded[profile]:
            return
        _abc_disk_loaded[profile] = True

    try:
        path = _abc_cache_path(profile)
        if not os.path.isfile(path):
            return
        with open(path, 'r', encoding='utf-8') as f:
            payload = json.load(f)
        if payload.get('fingerprint') != _abc_cache_fingerprint():
            _logger.debug('[abc_cache] fingerprint mismatch, skipping disk load (profile=%s)', profile)
            return
        written_at = float(payload.get('written_at') or 0)
        if (time.time() - written_at) > _ABC_CACHE_FILE_TTL_SEC:
            return
        abc_denom_ts = float(payload.get('denom_written_at') or 0)
        current_denom_ts = _get_current_denom_written_at()
        if current_denom_ts > abc_denom_ts:
            _logger.debug('[abc_cache] denom generation mismatch (abc_denom_ts=%.0f < '
                          'current_denom_ts=%.0f), skipping disk load (profile=%s)',
                          abc_denom_ts, current_denom_ts, profile)
            return
        entries = payload.get('entries')
        if not entries:
            return
        loaded = 0
        with _abc_cache_lock:
            for str_mid, entry in entries.items():
                try:
                    mid = int(str_mid)
                    _normalize_abc_entry(entry)
                    _validate_abc_cache_entry(entry)
                    if mid not in _abc_cache[profile]:
                        _abc_cache[profile][mid] = entry
                        loaded += 1
                except Exception as _entry_exc:
                    _logger.debug('[abc_cache] skipping entry str_mid=%r (profile=%s): %s',
                                  str_mid, profile, _entry_exc)
                    continue
        if _app_debug and loaded > 0:
            _logger.debug('[abc_cache] loaded %d entries from disk (profile=%s)', loaded, profile)
    except Exception as e:
        _logger.warning('[abc_cache] load from disk failed (profile=%s): %s', profile, e)


# =========================================
# Canonical shared facts cache
# =========================================
_CANONICAL_FACTS_CACHE_TTL_SEC = int(os.getenv('CANONICAL_FACTS_CACHE_TTL_SEC', '600'))
_canonical_facts_cache_data: dict = {}
_canonical_facts_cache_lock = threading.Lock()
_canonical_facts_build_lock = threading.Lock()


def _canonical_facts_path() -> str:
    return os.path.join(_shared_cache_dir(), f'canonical_facts{_score_cache_suffix()}.json')


def _build_canonical_facts() -> dict:
    """Compute the canonical profile-independent facts snapshot from DB."""
    RADAR_CIDS = _RADAR_CIDS
    cid_placeholders = ','.join(f':c{i}' for i in range(len(RADAR_CIDS)))
    cid_params: dict = {f'c{i}': cid for i, cid in enumerate(RADAR_CIDS)}

    sql_cond_query = f"""
        SELECT
            v.model_id,
            v.brand_name_zh, v.model_name,
            v.size, v.thickness, v.max_speed, v.reference_price,
            COALESCE(fm.rgb_light, '') AS rgb_light,
            v.condition_id,
            MAX(v.query_by_model_condition_d30) AS cond_query_count
        FROM query_rank_d30_view v
        LEFT JOIN available_models_info_view fm ON fm.model_id = v.model_id
        WHERE v.condition_id IN ({cid_placeholders})
        GROUP BY v.model_id, v.brand_name_zh, v.model_name,
                 v.size, v.thickness, v.max_speed, v.reference_price, fm.rgb_light,
                 v.condition_id
    """

    sql_radar_add = """
        SELECT model_id, COUNT(1) AS model_add_count
        FROM user_radar_logs
        WHERE event_time > (NOW() - INTERVAL 30 DAY)
          AND event_type = 'add'
        GROUP BY model_id
    """

    try:
        cq_rows = _fetch_all(sql_cond_query, cid_params)
    except Exception:
        cq_rows = []
    try:
        radar_add_rows = _fetch_all(sql_radar_add)
    except Exception:
        radar_add_rows = []

    try:
        like_rank_cache.ensure_cache(_fetch_all)
        lrc_snapshot: dict | None = like_rank_cache.get_all_like_counts()
    except Exception as e:
        _logger.warning('[canonical_facts] like cache load failed: %s', e)
        lrc_snapshot = None

    cond_data: dict[int, dict[int, dict[str, int]]] = {}
    model_meta: dict[int, dict] = {}

    for r in cq_rows:
        try:
            mid = int(r['model_id']); cid = int(r['condition_id'])
        except Exception:
            continue
        cond_data.setdefault(mid, {}).setdefault(cid, {'query': 0, 'like': 0})
        try:
            cond_data[mid][cid]['query'] = int(r['cond_query_count'] or 0)
        except Exception:
            pass
        if mid not in model_meta:
            model_meta[mid] = {
                'brand_name_zh': r.get('brand_name_zh') or '',
                'model_name':    r.get('model_name')    or '',
                'size':          r.get('size'),
                'thickness':     r.get('thickness'),
                'max_speed':     r.get('max_speed'),
                'reference_price': r.get('reference_price'),
                'rgb_light':     r.get('rgb_light') or '',
            }

    if lrc_snapshot:
        for (mid, cid), lc in lrc_snapshot.items():
            if cid not in RADAR_CIDS:
                continue
            cond_data.setdefault(mid, {}).setdefault(cid, {'query': 0, 'like': 0})
            try:
                cond_data[mid][cid]['like'] = max(0, int(lc or 0))
            except Exception:
                pass

    missing_mids = [mid for mid in cond_data if mid not in model_meta]
    if missing_mids:
        mp_placeholders = ','.join(f':mm{i}' for i in range(len(missing_mids)))
        mp_params = {f'mm{i}': mid for i, mid in enumerate(missing_mids)}
        sql_meta = f"""
            SELECT model_id, brand_name_zh, model_name,
                   size, thickness, max_speed, reference_price,
                   COALESCE(rgb_light, '') AS rgb_light
            FROM available_models_info_view
            WHERE model_id IN ({mp_placeholders})
        """
        try:
            meta_rows = _fetch_all(sql_meta, mp_params)
            for r in meta_rows:
                try:
                    rmid = int(r['model_id'])
                except Exception:
                    continue
                if rmid not in model_meta:
                    model_meta[rmid] = {
                        'brand_name_zh': r.get('brand_name_zh') or '',
                        'model_name':    r.get('model_name')    or '',
                        'size':          r.get('size'),
                        'thickness':     r.get('thickness'),
                        'max_speed':     r.get('max_speed'),
                        'reference_price': r.get('reference_price'),
                        'rgb_light':     r.get('rgb_light') or '',
                    }
        except Exception:
            pass

    model_add_counts: dict[int, int] = {}
    for r in radar_add_rows:
        try:
            mid = int(r['model_id'])
            model_add_counts[mid] = int(r.get('model_add_count') or 0)
        except Exception:
            pass

    model_lookup: dict[int, dict] = {}
    for mid, cid_map in cond_data.items():
        meta = model_meta.get(mid, {})

        total_query = sum(v['query'] for v in cid_map.values())
        total_like  = sum(v['like']  for v in cid_map.values())
        model_add   = model_add_counts.get(mid, 0)
        heat_score  = total_query + total_like * 10 + model_add * 2

        condition_heat: dict[int, int] = {}
        condition_likes: dict[int, int] = {}
        for cid in RADAR_CIDS:
            entry = cid_map.get(cid)
            if entry:
                condition_heat[cid]  = entry['query'] + entry['like'] * 10
                condition_likes[cid] = entry['like']
            else:
                condition_heat[cid]  = 0
                condition_likes[cid] = 0

        model_lookup[mid] = {
            'brand_name_zh':   meta.get('brand_name_zh', ''),
            'model_name':      meta.get('model_name',    ''),
            'size':            meta.get('size'),
            'thickness':       meta.get('thickness'),
            'max_speed':       meta.get('max_speed'),
            'reference_price': meta.get('reference_price'),
            'rgb_light':       meta.get('rgb_light', ''),
            'query_count':     total_query,
            'like_count':      total_like,
            'heat_score':      heat_score,
            'condition_heat':  condition_heat,
            'condition_likes': condition_likes,
        }

    return {'model_lookup': model_lookup, 'lrc_snapshot': lrc_snapshot}


def _normalize_canonical_facts_payload(model_lookup: dict) -> dict:
    """Restore integer model_id and CID keys in a canonical facts model_lookup after a JSON round-trip."""
    int_keyed: dict[int, dict] = {}
    for k, v in model_lookup.items():
        try:
            mid = int(k)
        except Exception:
            continue
        v_local = v
        if isinstance(v, dict):
            v_local = dict(v)
            ch = v_local.get('condition_heat')
            if isinstance(ch, dict):
                v_local['condition_heat'] = {int(ck): cv for ck, cv in ch.items()}
            cl = v_local.get('condition_likes')
            if isinstance(cl, dict):
                v_local['condition_likes'] = {int(ck): cv for ck, cv in cl.items()}
        int_keyed[mid] = v_local
    return int_keyed


def _save_canonical_facts_to_disk(model_lookup: dict) -> None:
    try:
        payload = {'model_lookup': model_lookup, 'written_at': time.time()}
        _atomic_json_write(_canonical_facts_path(), payload)
        if _app_debug:
            _logger.debug('[canonical_facts] saved %d entries to disk', len(model_lookup))
    except Exception as e:
        _logger.warning('[canonical_facts] save to disk failed: %s', e)


def _load_canonical_facts_from_disk():
    """Return (written_at, model_lookup) from disk if fresh, or None if stale/missing."""
    try:
        p = _canonical_facts_path()
        if not os.path.isfile(p):
            return None
        with open(p, 'r', encoding='utf-8') as f:
            payload = json.load(f)
        written_at = float(payload.get('written_at') or 0)
        if (time.time() - written_at) > _CANONICAL_FACTS_CACHE_TTL_SEC:
            return None
        raw_ml = payload.get('model_lookup')
        if not isinstance(raw_ml, dict):
            return None
        model_lookup = _normalize_canonical_facts_payload(raw_ml)
        return (written_at, model_lookup)
    except Exception as e:
        _logger.warning('[canonical_facts] load from disk failed: %s', e)
        return None


def get_canonical_facts() -> dict:
    """Return the canonical profile-independent facts snapshot.

    Lookup order: in-memory cache → shared disk cache → full DB build.
    """
    now = time.time()

    with _canonical_facts_cache_lock:
        cached = _canonical_facts_cache_data.get('entry')
        if cached and (now - cached[0]) < _CANONICAL_FACTS_CACHE_TTL_SEC:
            return {'model_lookup': cached[1], 'lrc_snapshot': None}

    disk = _load_canonical_facts_from_disk()
    if disk is not None:
        disk_written_at, disk_ml = disk
        with _canonical_facts_cache_lock:
            existing = _canonical_facts_cache_data.get('entry')
            if not existing or disk_written_at > existing[0]:
                _canonical_facts_cache_data['entry'] = (disk_written_at, disk_ml)
        return {'model_lookup': disk_ml, 'lrc_snapshot': None}

    with _canonical_facts_build_lock:
        with _canonical_facts_cache_lock:
            cached = _canonical_facts_cache_data.get('entry')
            if cached and (time.time() - cached[0]) < _CANONICAL_FACTS_CACHE_TTL_SEC:
                return {'model_lookup': cached[1], 'lrc_snapshot': None}

        disk = _load_canonical_facts_from_disk()
        if disk is not None:
            disk_written_at, disk_ml = disk
            with _canonical_facts_cache_lock:
                existing = _canonical_facts_cache_data.get('entry')
                if not existing or disk_written_at > existing[0]:
                    _canonical_facts_cache_data['entry'] = (disk_written_at, disk_ml)
            return {'model_lookup': disk_ml, 'lrc_snapshot': None}

        result = _build_canonical_facts()
        computed_at = time.time()
        model_lookup = result.get('model_lookup') or {}
        lrc_snapshot = result.get('lrc_snapshot')

        if model_lookup:
            with _canonical_facts_cache_lock:
                existing = _canonical_facts_cache_data.get('entry')
                if not existing or computed_at > existing[0]:
                    _canonical_facts_cache_data['entry'] = (computed_at, model_lookup)
            _save_canonical_facts_to_disk(model_lookup)
        else:
            _logger.warning(
                '[canonical_facts] build returned empty model_lookup; skipping cache persist')

    return {'model_lookup': model_lookup, 'lrc_snapshot': lrc_snapshot}


# =========================================
# ABC Score computation
# =========================================

def _compute_abc_for_model(model_id: int, profile_key: str = _DEFAULT_SCORE_PROFILE,
                           prefetched_likes: dict | None = None) -> dict | None:
    """Compute A1/A2/A3/B/C/D scores for model_id across all SCORE_CONDITION_IDS."""
    result: Dict[int, dict] = {}
    for cid in SCORE_CONDITION_IDS:
        try:
            mdl = spectrum_reader.get_perf_model(model_id, cid)
        except Exception as e:
            _logger.warning('[abc] pchip (%s,%s): %s', model_id, cid, e)
            mdl = None
        if mdl is None:
            continue

        pchip_data = mdl.get('pchip') or {}

        with _cond_denom_lock:
            denom_entry = _cond_denom_cache[profile_key].get(cid)
        low_db         = denom_entry.get('low_db')         if denom_entry else None
        common_high_db = denom_entry.get('common_high_db') if denom_entry else None
        high_db        = denom_entry.get('high_db')        if denom_entry else None
        mid_db         = denom_entry.get('mid_db')         if denom_entry else None
        if common_high_db is None:
            common_high_db = high_db
        if mid_db is None:
            mid_db = _midpoint_db(low_db, high_db)
        max_a1  = float(denom_entry.get('max_a1') or 0.0) if denom_entry else 0.0
        max_a2  = float(denom_entry.get('max_a2') or 0.0) if denom_entry else 0.0
        max_a3  = float(denom_entry.get('max_a3') or 0.0) if denom_entry else 0.0
        max_b   = float(denom_entry.get('max_b')  or 0.0) if denom_entry else 0.0
        max_c   = float(denom_entry.get('max_c')  or 0.0) if denom_entry else 0.0
        max_d   = float(denom_entry.get('max_d')  or 0.0) if denom_entry else 0.0

        if low_db is None or common_high_db is None or low_db > common_high_db:
            if low_db is not None and common_high_db is not None:
                _logger.warning(
                    '[abc] cid=%s model=%s: inverted common dB range (low_db=%s > common_high_db=%s), '
                    'skipping condition', cid, model_id, low_db, common_high_db
                )
            else:
                _logger.debug(
                    '[abc] cid=%s model=%s: denom not ready (low_db=%s, common_high_db=%s), '
                    'skipping condition', cid, model_id, low_db, common_high_db
                )
            continue

        a1_raw = a2_raw = a3_raw = 0.0
        n2a = pchip_data.get('noise_to_airflow')
        if n2a and isinstance(n2a, dict) and n2a.get('x'):
            n2a_xs = n2a.get('x') or []
            n2a_ys = n2a.get('y') or []
            model_max_db = float(n2a_xs[-1]) if n2a_xs else None
            model_max_airflow = float(max(n2a_ys)) if n2a_ys else 0.0

            try:
                val = eval_pchip(n2a, low_db)
                if val is not None and math.isfinite(val) and val > 0:
                    a1_raw = float(val)
            except Exception:
                pass

            eff_wa2 = high_db
            if eff_wa2 is None:
                pass
            elif model_max_db is not None and model_max_db < eff_wa2:
                a2_raw = model_max_airflow
            else:
                try:
                    val = eval_pchip(n2a, eff_wa2)
                    if val is not None and math.isfinite(val) and val > 0:
                        a2_raw = float(val)
                except Exception:
                    pass

            if model_max_db is not None and mid_db is not None and model_max_db < mid_db:
                a3_raw = model_max_airflow
            else:
                try:
                    val = eval_pchip(n2a, mid_db)
                    if val is not None and math.isfinite(val) and val > 0:
                        a3_raw = float(val)
                except Exception:
                    pass

        b_raw = 0.0
        r2a = pchip_data.get('rpm_to_airflow')
        if r2a and isinstance(r2a, dict):
            ys = r2a.get('y') or []
            if ys:
                try:
                    b_raw = float(max(ys))
                except Exception:
                    pass

        lc = 0
        try:
            if prefetched_likes is not None:
                lc = max(0, int(prefetched_likes.get((model_id, cid), 0) or 0))
            else:
                lc = like_rank_cache.get_like_count(model_id, cid, _fetch_all)
        except Exception as e:
            _logger.warning('[abc] like_count (%s,%s): %s', model_id, cid, e)
        c_raw = math.log1p(lc)

        d_raw = 0.0
        if n2a and isinstance(n2a, dict):
            n2a_x = n2a.get('x') or []
            n2a_y = n2a.get('y') or []
            if n2a_x and n2a_y and len(n2a_x) == len(n2a_y):
                try:
                    max_af = float(max(n2a_y))
                    max_af_idx = n2a_y.index(max_af)
                    noise_at_max = float(n2a_x[max_af_idx])
                    if max_af > 0 and noise_at_max > 0:
                        d_raw = max_af / noise_at_max
                except Exception:
                    pass

        pcfg = PROFILE_CONFIGS[profile_key]
        alpha_a = pcfg['alpha_a']
        alpha_b = pcfg['alpha_b']
        alpha_d = pcfg['alpha_d']
        p_wa1 = pcfg['score_wa1']
        p_wa2 = pcfg['score_wa2']
        p_wa3 = pcfg['score_wa3']
        p_wb  = pcfg['score_wb']
        p_wc  = pcfg['score_wc']
        p_wd  = pcfg['score_wd']
        a1_scaled = pow(a1_raw, alpha_a) if a1_raw > 0 else 0.0
        a2_scaled = pow(a2_raw, alpha_a) if a2_raw > 0 else 0.0
        a3_scaled = pow(a3_raw, alpha_a) if a3_raw > 0 else 0.0
        b_scaled  = pow(b_raw,  alpha_b) if b_raw  > 0 else 0.0
        d_scaled  = pow(d_raw,  alpha_d) if d_raw  > 0 else 0.0

        denom_a1 = pow(max_a1, alpha_a) if max_a1 > 0 else 1.0
        denom_a2 = pow(max_a2, alpha_a) if max_a2 > 0 else 1.0
        denom_a3 = pow(max_a3, alpha_a) if max_a3 > 0 else 1.0
        denom_b  = pow(max_b,  alpha_b) if max_b  > 0 else 1.0
        denom_c  = max_c if max_c > 0 else 1.0
        denom_d  = pow(max_d,  alpha_d) if max_d  > 0 else 1.0

        na1 = min(1.0, a1_scaled / denom_a1)
        na2 = min(1.0, a2_scaled / denom_a2)
        na3 = min(1.0, a3_scaled / denom_a3)
        nb  = min(1.0, b_scaled  / denom_b)
        nc  = min(1.0, c_raw     / denom_c)
        nd  = min(1.0, d_scaled  / denom_d)

        score_a1_f = na1 * SCORE_MAX
        score_a2_f = na2 * SCORE_MAX
        score_a3_f = na3 * SCORE_MAX
        score_b_f  = nb  * SCORE_MAX
        score_c_f  = nc  * SCORE_MAX
        score_d_f  = nd  * SCORE_MAX
        wa_sum = p_wa1 + p_wa2 + p_wa3
        score_total = round(
            p_wa1 * score_a1_f +
            p_wa2 * score_a2_f +
            p_wa3 * score_a3_f +
            p_wb  * score_b_f  +
            p_wc  * score_c_f  +
            p_wd  * score_d_f
        )

        result[cid] = {
            'score_a1': round(score_a1_f),
            'score_a2': round(score_a2_f),
            'score_a3': round(score_a3_f),
            'score_a': round((p_wa1 * score_a1_f + p_wa2 * score_a2_f + p_wa3 * score_a3_f)
                             / wa_sum if wa_sum > 0 else 0),
            'score_b': round(score_b_f),
            'score_c': round(score_c_f),
            'score_d': round(score_d_f),
            'score_total': score_total,
            'a1_raw': a1_raw,
            'a2_raw': a2_raw,
            'a3_raw': a3_raw,
            'b_raw': b_raw,
            'c_raw': c_raw,
            'd_raw': d_raw,
        }

    return result if result else None


def _compute_composite_score(conditions: Dict[int, dict]) -> int | None:
    weighted_sum = 0.0
    total_w = 0.0
    for cid, sc in conditions.items():
        w = COMPOSITE_WEIGHTS.get(cid, 1.0)
        weighted_sum += w * sc['score_total']
        total_w += w
    return round(weighted_sum / total_w) if total_w > 0 else None


def _sync_compute_and_cache(model_id: int, profile_key: str = _DEFAULT_SCORE_PROFILE,
                            prefetched_likes: dict | None = None) -> dict | None:
    conditions = _compute_abc_for_model(model_id, profile_key, prefetched_likes)
    if conditions is None:
        return None
    composite_score = _compute_composite_score(conditions)
    now_str = datetime.now(timezone.utc).isoformat(timespec='seconds')
    entry = {
        'conditions': conditions,
        'composite_score': composite_score,
        'updated_at': now_str,
        'cached_at': time.time(),
    }
    with _abc_cache_lock:
        _abc_cache[profile_key][model_id] = entry
    return entry


def _trigger_soft_refresh(model_id: int, profile_key: str = _DEFAULT_SCORE_PROFILE) -> None:
    key = (profile_key, model_id)
    with _abc_inflight_lock:
        if key in _abc_inflight:
            return
        _abc_inflight.add(key)

    def _refresh():
        try:
            _sync_compute_and_cache(model_id, profile_key)
        finally:
            with _abc_inflight_lock:
                _abc_inflight.discard(key)

    threading.Thread(target=_refresh, daemon=True).start()


def _get_abc_cached(model_id: int, profile_key: str = _DEFAULT_SCORE_PROFILE,
                    prefetched_likes: dict | None = None) -> dict | None:
    now = time.time()
    with _abc_cache_lock:
        entry = _abc_cache[profile_key].get(model_id)

    if entry is not None:
        age = now - entry['cached_at']
        if age < ABC_SOFT_TTL_SEC:
            return entry
        if age < ABC_HARD_TTL_SEC:
            _trigger_soft_refresh(model_id, profile_key)
            return entry

    _try_load_abc_from_disk(profile_key)
    with _abc_cache_lock:
        entry = _abc_cache[profile_key].get(model_id)
    if entry is not None:
        age = now - entry['cached_at']
        if age < ABC_SOFT_TTL_SEC:
            return entry
        if age < ABC_HARD_TTL_SEC:
            _trigger_soft_refresh(model_id, profile_key)
            return entry

    return _sync_compute_and_cache(model_id, profile_key, prefetched_likes)


def _get_abc_all_profiles(model_id: int,
                          prefetched_likes: dict | None = None) -> Dict[str, dict | None]:
    return {p: _get_abc_cached(model_id, p, prefetched_likes) for p in SCORING_PROFILES}


def _build_score_profiles_payload(all_profiles: Dict[str, dict | None],
                                  radar_cids: list) -> dict:
    result = {}
    for pkey, abc in all_profiles.items():
        composite = None
        cond_scores: dict = {}
        if abc:
            composite = abc.get('composite_score')
            conds = abc.get('conditions') or {}
            for cid in radar_cids:
                cd = conds.get(cid)
                raw = cd.get('score_total') if cd else None
                try:
                    cond_scores[cid] = int(raw) if raw is not None else None
                except (TypeError, ValueError):
                    cond_scores[cid] = None
        else:
            cond_scores = {cid: None for cid in radar_cids}
        result[pkey] = {
            'composite_score': int(composite) if composite is not None else None,
            'condition_scores': cond_scores,
        }
    return result


# =========================================
# Rankings v2
# =========================================

def _build_rankings_v2(score_profile: str = _DEFAULT_SCORE_PROFILE) -> dict:
    """Compute model-centric rankings for Right Panel v2."""
    RADAR_CIDS = _RADAR_CIDS

    cf = get_canonical_facts()
    facts_lookup: dict[int, dict] = cf.get('model_lookup') or {}
    prefetched_likes: dict | None = cf.get('lrc_snapshot')
    if prefetched_likes is None:
        try:
            like_rank_cache.ensure_cache(_fetch_all)
            prefetched_likes = like_rank_cache.get_all_like_counts()
        except Exception as e:
            _logger.warning('[rankings_v2] like cache load failed: %s', e)
            prefetched_likes = None

    items = []
    for mid, facts in facts_lookup.items():
        condition_heat:  dict[int, int] = facts.get('condition_heat')  or {cid: 0 for cid in RADAR_CIDS}
        condition_likes: dict[int, int] = facts.get('condition_likes') or {cid: 0 for cid in RADAR_CIDS}
        total_query = facts.get('query_count', 0)
        total_like  = facts.get('like_count',  0)
        heat_score  = facts.get('heat_score',  0)

        all_profiles = _get_abc_all_profiles(mid, prefetched_likes)
        abc_entry = all_profiles.get(_DEFAULT_SCORE_PROFILE)
        composite_score = None
        condition_scores: dict = {}
        if abc_entry:
            composite_score = abc_entry.get('composite_score')
            conds_abc = abc_entry.get('conditions') or {}
            for cid in RADAR_CIDS:
                cd = conds_abc.get(cid)
                raw = cd.get('score_total') if cd else None
                try:
                    condition_scores[cid] = int(raw) if raw is not None else None
                except (TypeError, ValueError):
                    condition_scores[cid] = None
        score_profiles = _build_score_profiles_payload(all_profiles, RADAR_CIDS)

        items.append({
            'model_id':        mid,
            'brand_name_zh':   facts.get('brand_name_zh', ''),
            'model_name':      facts.get('model_name',    ''),
            'size':            facts.get('size'),
            'thickness':       facts.get('thickness'),
            'max_speed':       facts.get('max_speed'),
            'reference_price': facts.get('reference_price'),
            'rgb_light':       facts.get('rgb_light', ''),
            'query_count':     total_query,
            'like_count':      total_like,
            'heat_score':      heat_score,
            'composite_score': int(composite_score) if composite_score is not None else None,
            'condition_scores': condition_scores,
            'score_profiles':  score_profiles,
            'condition_heat':  condition_heat,
            'condition_likes': condition_likes,
        })

    limit = _RANKINGS_V2_DISPLAY_LIMIT

    heat_board = []
    for i, row in enumerate(
        sorted(items, key=lambda x: (-x['heat_score'], -x['query_count'], -x['model_id']))[:limit], 1
    ):
        heat_board.append({**row, 'rank': i})

    def _perf_sort_key(row):
        sp = row.get('score_profiles', {}).get(score_profile)
        cs = (sp.get('composite_score') if sp else None)
        return (-(cs if cs is not None else -1), -row['heat_score'], -row['model_id'])

    perf_board = []
    for i, row in enumerate(
        sorted(items, key=_perf_sort_key)[:limit], 1
    ):
        perf_board.append({**row, 'rank': i})

    return {
        'heat_board': heat_board,
        'performance_board': perf_board,
        'model_lookup': {item['model_id']: item for item in items},
    }


def get_rankings_v2(score_profile: str = _DEFAULT_SCORE_PROFILE) -> dict:
    """Return model-centric rankings for Right Panel v2, with a 10-minute cache."""
    if score_profile not in SCORING_PROFILES:
        score_profile = _DEFAULT_SCORE_PROFILE

    now = time.time()
    cache_key = f'data:{score_profile}'

    with _rankings_v2_cache_lock:
        cached = _rankings_v2_cache.get(cache_key)
        if cached and (now - cached[0]) < _RANKINGS_V2_CACHE_TTL_SEC:
            return cached[1]

    disk = _load_rankings_cache_from_disk(score_profile)
    if disk is not None:
        disk_written_at, disk_result = disk
        with _rankings_v2_cache_lock:
            existing = _rankings_v2_cache.get(cache_key)
            if not existing or disk_written_at > existing[0]:
                _rankings_v2_cache[cache_key] = (disk_written_at, disk_result)
        for _pkey in SCORING_PROFILES:
            _try_load_abc_from_disk(_pkey)
        return disk_result

    build_lock = _rankings_v2_build_locks[score_profile]
    with build_lock:
        with _rankings_v2_cache_lock:
            cached = _rankings_v2_cache.get(cache_key)
            if cached and (time.time() - cached[0]) < _RANKINGS_V2_CACHE_TTL_SEC:
                return cached[1]

        disk = _load_rankings_cache_from_disk(score_profile)
        if disk is not None:
            disk_written_at, disk_result = disk
            with _rankings_v2_cache_lock:
                existing = _rankings_v2_cache.get(cache_key)
                if not existing or disk_written_at > existing[0]:
                    _rankings_v2_cache[cache_key] = (disk_written_at, disk_result)
            for _pkey in SCORING_PROFILES:
                _try_load_abc_from_disk(_pkey)
            return disk_result

        result = _build_rankings_v2(score_profile)
        computed_at = time.time()

        with _rankings_v2_cache_lock:
            existing = _rankings_v2_cache.get(cache_key)
            if not existing or computed_at > existing[0]:
                _rankings_v2_cache[cache_key] = (computed_at, result)

        for pkey in SCORING_PROFILES:
            _save_abc_cache_to_disk(pkey)

        if result.get('model_lookup'):
            _save_rankings_cache(score_profile, result)
        else:
            _logger.warning(
                '[rankings_v2] skipping disk persist: build returned empty '
                'model_lookup (profile=%s)', score_profile)

    return result


def _get_ranked_lookup_with_fallback(primary_profile: str = _DEFAULT_SCORE_PROFILE) -> dict:
    """Return the first non-empty model_lookup from the rankings cache."""
    try:
        result = get_rankings_v2(primary_profile)
        lookup = result.get('model_lookup') or {}
        if lookup:
            return lookup
    except Exception:
        pass

    now = time.time()
    for fallback_profile in SCORING_PROFILES:
        if fallback_profile == primary_profile:
            continue
        with _rankings_v2_cache_lock:
            cached = _rankings_v2_cache.get(f'data:{fallback_profile}')
        if cached and (now - cached[0]) < _RANKINGS_V2_CACHE_TTL_SEC:
            fallback_lookup = cached[1].get('model_lookup') or {}
            if fallback_lookup:
                return fallback_lookup
        try:
            disk = _load_rankings_cache_from_disk(fallback_profile)
            if disk is not None:
                fallback_lookup = disk[1].get('model_lookup') or {}
                if fallback_lookup:
                    return fallback_lookup
        except Exception:
            pass

    _warmup_rankings_async()
    return {}


def get_recent_updates(limit: int = 100) -> List[dict]:
    """Model-centric recent-updates list for Right Panel v2."""
    RADAR_CIDS = _RADAR_CIDS

    sql = """
      SELECT
        u.model_id,
        u.brand_name_zh, u.model_name,
        u.size, u.thickness, u.max_speed,
        COALESCE(fm.rgb_light, '')  AS rgb_light,
        fm.reference_price,
        DATE_FORMAT(MAX(u.update_date), '%Y-%m-%d') AS update_date
      FROM update_notice_d30_view AS u
      LEFT JOIN available_models_info_view fm ON fm.model_id = u.model_id
      GROUP BY u.model_id, u.brand_name_zh, u.model_name,
               u.size, u.thickness, u.max_speed,
               fm.rgb_light, fm.reference_price
      ORDER BY MAX(u.update_date) DESC
      LIMIT :l
    """
    rows = _fetch_all(sql, {'l': limit})
    if not rows:
        return []

    _cf = get_canonical_facts()
    facts_lookup: dict = _cf.get('model_lookup') or {}
    _prefetched_likes: dict | None = _cf.get('lrc_snapshot')
    if _prefetched_likes is None:
        try:
            like_rank_cache.ensure_cache(_fetch_all)
            _prefetched_likes = like_rank_cache.get_all_like_counts()
        except Exception as _e:
            _logger.warning('[get_recent_updates] like cache load failed: %s', _e)
            _prefetched_likes = None

    items = []
    for r in rows:
        try:
            mid = int(r['model_id'])
        except (TypeError, ValueError, KeyError):
            continue

        facts = facts_lookup.get(mid)
        heat_score     = facts.get('heat_score',     0) if facts else 0
        condition_heat = facts.get('condition_heat', {cid: 0 for cid in RADAR_CIDS}) if facts else {cid: 0 for cid in RADAR_CIDS}

        all_profiles_abc = _get_abc_all_profiles(mid, _prefetched_likes)
        abc = all_profiles_abc.get(_DEFAULT_SCORE_PROFILE)
        composite_score  = None
        condition_scores = {cid: None for cid in RADAR_CIDS}
        if abc:
            cs = abc.get('composite_score')
            try:
                composite_score = int(cs) if cs is not None else None
            except (TypeError, ValueError):
                composite_score = None
            conds_abc = abc.get('conditions') or {}
            for cid in RADAR_CIDS:
                cd  = conds_abc.get(cid)
                raw = cd.get('score_total') if cd else None
                try:
                    condition_scores[cid] = int(raw) if raw is not None else None
                except (TypeError, ValueError):
                    condition_scores[cid] = None
        score_profiles = _build_score_profiles_payload(all_profiles_abc, RADAR_CIDS)

        items.append({
            'model_id':        mid,
            'brand_name_zh':   r.get('brand_name_zh') or '',
            'model_name':      r.get('model_name')    or '',
            'size':            r.get('size'),
            'thickness':       r.get('thickness'),
            'max_speed':       r.get('max_speed'),
            'reference_price': r.get('reference_price'),
            'rgb_light':       r.get('rgb_light') or '',
            'heat_score':      heat_score,
            'composite_score': composite_score,
            'condition_scores': condition_scores,
            'score_profiles':  score_profiles,
            'condition_heat':   condition_heat,
            'update_date':     r.get('update_date') or '',
        })

    return items


# =========================================
# Denom refresh helpers
# =========================================

def _update_max_with_model(
    per_val: dict, per_mid: dict, cid: int, new_val, new_mid: int
) -> None:
    curr = per_val.get(cid, 0.0)
    if new_val > curr or (new_val == curr and new_mid > per_mid.get(cid, float('-inf'))):
        per_val[cid] = new_val
        per_mid[cid] = new_mid


def _persist_denom_audit(
    cid_list,
    per_cid_low_db, per_cid_high_db, per_cid_mid_db,
    per_cid_a1, per_cid_a1_mid,
    per_cid_a2, per_cid_a2_mid,
    per_cid_a3, per_cid_a3_mid,
    per_cid_b,  per_cid_b_mid,
    per_cid_wc_raw, per_cid_wc_mid,
    per_cid_d,  per_cid_d_mid,
):
    """Persist per-condition ABC normalization reference/max audit rows.

    Writes one row per condition_id to condition_score_ref_stats.
    Silently skips if the table does not exist.
    """
    sql = """
        INSERT INTO condition_score_ref_stats
            (condition_id,
             wa1_ref_db, wa1_max_raw, wa1_model_id,
             wa2_ref_db, wa2_max_raw, wa2_model_id,
             wa3_ref_db, wa3_max_raw, wa3_model_id,
             wb_ref_db,  wb_max_raw,  wb_model_id,
             wc_ref_db,  wc_max_raw,  wc_model_id,
             wd_ref_db,  wd_max_raw,  wd_model_id,
             updated_at)
        VALUES
            (:condition_id,
             :wa1_ref_db, :wa1_max_raw, :wa1_model_id,
             :wa2_ref_db, :wa2_max_raw, :wa2_model_id,
             :wa3_ref_db, :wa3_max_raw, :wa3_model_id,
             NULL, :wb_max_raw, :wb_model_id,
             NULL, :wc_max_raw, :wc_model_id,
             NULL, :wd_max_raw, :wd_model_id,
             NOW())
        ON DUPLICATE KEY UPDATE
            wa1_ref_db   = VALUES(wa1_ref_db),
            wa1_max_raw  = VALUES(wa1_max_raw),
            wa1_model_id = VALUES(wa1_model_id),
            wa2_ref_db   = VALUES(wa2_ref_db),
            wa2_max_raw  = VALUES(wa2_max_raw),
            wa2_model_id = VALUES(wa2_model_id),
            wa3_ref_db   = VALUES(wa3_ref_db),
            wa3_max_raw  = VALUES(wa3_max_raw),
            wa3_model_id = VALUES(wa3_model_id),
            wb_ref_db    = NULL,
            wb_max_raw   = VALUES(wb_max_raw),
            wb_model_id  = VALUES(wb_model_id),
            wc_ref_db    = NULL,
            wc_max_raw   = VALUES(wc_max_raw),
            wc_model_id  = VALUES(wc_model_id),
            wd_ref_db    = NULL,
            wd_max_raw   = VALUES(wd_max_raw),
            wd_model_id  = VALUES(wd_model_id),
            updated_at   = NOW()
    """
    for cid in cid_list:
        params = {
            'condition_id': cid,
            'wa1_ref_db':   per_cid_low_db.get(cid),
            'wa1_max_raw':  per_cid_a1.get(cid) or None,
            'wa1_model_id': per_cid_a1_mid.get(cid),
            # DB column name remains wa2_ref_db for compatibility; value is the
            # actual scoring/visible high point (high_db).
            'wa2_ref_db':   per_cid_high_db.get(cid),
            'wa2_max_raw':  per_cid_a2.get(cid) or None,
            'wa2_model_id': per_cid_a2_mid.get(cid),
            'wa3_ref_db':   per_cid_mid_db.get(cid),
            'wa3_max_raw':  per_cid_a3.get(cid) or None,
            'wa3_model_id': None,  # WA3 ref_db is a mathematical midpoint — no independent source model
            'wb_max_raw':   per_cid_b.get(cid) or None,
            'wb_model_id':  per_cid_b_mid.get(cid),
            'wc_max_raw':   per_cid_wc_raw.get(cid, 0),
            'wc_model_id':  per_cid_wc_mid.get(cid),
            'wd_max_raw':   per_cid_d.get(cid) or None,
            'wd_model_id':  per_cid_d_mid.get(cid),
        }
        try:
            _exec_write(sql, params)
        except sa_exc.OperationalError as e:
            orig = getattr(e, 'orig', None)
            code = getattr(orig, 'args', [None])[0] if orig else None
            if code == 1146:
                _logger.debug('[denom_refresh] audit table not found, skipping persist')
                return
            _logger.warning('[denom_refresh] audit persist error for cid=%s: %s', cid, e)
        except Exception as e:
            _logger.warning('[denom_refresh] audit persist error for cid=%s: %s', cid, e)


def _do_denom_refresh():
    """Rebuild per-condition denominators for all SCORE_CONDITION_IDS across all profiles."""
    try:
        cid_list = list(SCORE_CONDITION_IDS)
        parts = ', '.join([f':c{i}' for i in range(len(cid_list))])
        params = {f'c{i}': cid for i, cid in enumerate(cid_list)}
        pair_rows = _fetch_all(
            f"SELECT DISTINCT model_id, condition_id FROM meta_view "
            f"WHERE condition_id IN ({parts})",
            params
        )
        pairs = [(int(r['model_id']), int(r['condition_id'])) for r in pair_rows]

        per_cid_min_dbs: Dict[int, list] = defaultdict(list)
        per_cid_max_dbs: Dict[int, list] = defaultdict(list)
        per_cid_n2a: Dict[int, list] = defaultdict(list)
        per_model_max_db: Dict[tuple, float] = {}
        per_cid_min_db_pairs: Dict[int, list] = defaultdict(list)
        per_cid_max_db_pairs: Dict[int, list] = defaultdict(list)
        per_cid_b: Dict[int, float] = defaultdict(float)
        per_cid_b_mid: Dict[int, int] = {}
        if pairs:
            perf_map: Dict[str, Any] = {}
            try:
                perf_map = spectrum_reader.get_perf_models(pairs)
            except Exception as e:
                _logger.warning('[denom_refresh] pchip load/rebuild failed: %s', e)
            for mid, cid in pairs:
                mdl = perf_map.get(f"{mid}_{cid}")
                if mdl:
                    pchip_data = mdl.get('pchip') or {}
                    n2a = pchip_data.get('noise_to_airflow')
                    if n2a and isinstance(n2a, dict):
                        xs = n2a.get('x') or []
                        if xs:
                            per_cid_min_dbs[cid].append(float(xs[0]))
                            per_cid_max_dbs[cid].append(float(xs[-1]))
                            per_cid_n2a[cid].append((mid, n2a))
                            per_model_max_db[(cid, mid)] = float(xs[-1])
                            per_cid_min_db_pairs[cid].append((float(xs[0]), mid))
                            per_cid_max_db_pairs[cid].append((float(xs[-1]), mid))
                    r2a = pchip_data.get('rpm_to_airflow')
                    if r2a and isinstance(r2a, dict):
                        r2a_ys = r2a.get('y') or []
                        if r2a_ys:
                            try:
                                b_val = float(max(r2a_ys))
                                if b_val > 0:
                                    _update_max_with_model(per_cid_b, per_cid_b_mid, cid, b_val, mid)
                            except Exception:
                                pass

        per_cid_low_db: Dict[int, float | None] = {}
        per_cid_common_high_db: Dict[int, float | None] = {}
        per_cid_a1_ref_mid: Dict[int, int | None] = {}
        per_cid_common_high_db_mid: Dict[int, int | None] = {}
        for cid in cid_list:
            min_dbs = per_cid_min_dbs.get(cid)
            max_dbs = per_cid_max_dbs.get(cid)
            if not min_dbs or not max_dbs:
                per_cid_low_db[cid] = None
                per_cid_common_high_db[cid] = None
                per_cid_a1_ref_mid[cid] = None
                per_cid_common_high_db_mid[cid] = None
                continue
            low_db  = max(min_dbs)
            common_high_db = min(max_dbs)
            per_cid_low_db[cid]  = low_db
            per_cid_common_high_db[cid] = common_high_db
            a1_ref_cands = [m for (db, m) in per_cid_min_db_pairs[cid] if db == low_db]
            per_cid_a1_ref_mid[cid] = max(a1_ref_cands) if a1_ref_cands else None
            hi_cands = [m for (db, m) in per_cid_max_db_pairs[cid] if db == common_high_db]
            per_cid_common_high_db_mid[cid] = max(hi_cands) if hi_cands else None
            if low_db > common_high_db:
                _logger.warning(
                    '[denom_refresh] cid=%s: invalid common dB range '
                    'low_db=%.2f > common_high_db=%.2f; all models will get null score for this condition',
                    cid, low_db, common_high_db
                )

        per_profile_high_db: Dict[str, Dict[int, float | None]] = {}
        per_profile_mid_db: Dict[str, Dict[int, float | None]] = {}
        per_profile_a2_ref_mid: Dict[str, Dict[int, int | None]] = {}
        for pkey, pcfg in PROFILE_CONFIGS.items():
            anchor_id = pcfg['wa2_anchor']
            per_cid_high_db: Dict[int, float | None] = {}
            per_cid_mid_db: Dict[int, float | None] = {}
            per_cid_a2_ref_mid_map: Dict[int, int | None] = {}
            for cid in cid_list:
                common_high_db = per_cid_common_high_db.get(cid)
                low_db = per_cid_low_db.get(cid)
                if common_high_db is None:
                    per_cid_high_db[cid] = None
                    per_cid_mid_db[cid] = None
                    per_cid_a2_ref_mid_map[cid] = None
                    continue
                if anchor_id == 0:
                    per_cid_high_db[cid] = common_high_db
                    per_cid_a2_ref_mid_map[cid] = per_cid_common_high_db_mid.get(cid)
                else:
                    anchor_max = per_model_max_db.get((cid, anchor_id))
                    if anchor_max is not None and anchor_max > common_high_db:
                        per_cid_high_db[cid] = anchor_max
                        per_cid_a2_ref_mid_map[cid] = anchor_id
                    else:
                        per_cid_high_db[cid] = common_high_db if anchor_max is None else max(common_high_db, anchor_max)
                        per_cid_a2_ref_mid_map[cid] = per_cid_common_high_db_mid.get(cid)
                high_db = per_cid_high_db.get(cid)
                per_cid_mid_db[cid] = _midpoint_db(low_db, high_db)
            per_profile_high_db[pkey] = per_cid_high_db
            per_profile_mid_db[pkey] = per_cid_mid_db
            per_profile_a2_ref_mid[pkey] = per_cid_a2_ref_mid_map

        per_cid_a1: Dict[int, float] = defaultdict(float)
        per_cid_a1_mid: Dict[int, int] = {}
        for cid in cid_list:
            low_db  = per_cid_low_db.get(cid)
            common_high_db = per_cid_common_high_db.get(cid)
            if low_db is None or common_high_db is None or low_db > common_high_db:
                continue
            for mid, n2a in per_cid_n2a.get(cid, []):
                try:
                    val = eval_pchip(n2a, low_db)
                    if val is not None and math.isfinite(val) and val > 0:
                        _update_max_with_model(per_cid_a1, per_cid_a1_mid, cid, float(val), mid)
                except Exception:
                    pass

        per_profile_a3: Dict[str, Dict[int, float]] = {}
        per_profile_a3_mid: Dict[str, Dict[int, int]] = {}
        for pkey in SCORING_PROFILES:
            mid_db_map = per_profile_mid_db[pkey]
            pp_a3: Dict[int, float] = defaultdict(float)
            pp_a3_mid: Dict[int, int] = {}
            for cid in cid_list:
                low_db = per_cid_low_db.get(cid)
                common_high_db = per_cid_common_high_db.get(cid)
                mid_db = mid_db_map.get(cid)
                if low_db is None or common_high_db is None or low_db > common_high_db:
                    continue
                for mid, n2a in per_cid_n2a.get(cid, []):
                    model_max_db = per_model_max_db.get((cid, mid))
                    n2a_y = n2a.get('y') or []
                    model_max_airflow = float(max(n2a_y)) if n2a_y else 0.0
                    wa3_val = None
                    if model_max_db is not None and mid_db is not None and model_max_db < mid_db:
                        if model_max_airflow > 0:
                            wa3_val = model_max_airflow
                    else:
                        try:
                            val = eval_pchip(n2a, mid_db)
                            if val is not None and math.isfinite(val) and val > 0:
                                wa3_val = float(val)
                        except Exception:
                            pass
                    if wa3_val is not None and wa3_val > 0:
                        _update_max_with_model(pp_a3, pp_a3_mid, cid, wa3_val, mid)
            per_profile_a3[pkey] = pp_a3
            per_profile_a3_mid[pkey] = pp_a3_mid

        per_profile_a2: Dict[str, Dict[int, float]] = {}
        per_profile_a2_mid: Dict[str, Dict[int, int]] = {}
        for pkey in SCORING_PROFILES:
            high_db_map = per_profile_high_db[pkey]
            pp_a2: Dict[int, float] = defaultdict(float)
            pp_a2_mid: Dict[int, int] = {}
            for cid in cid_list:
                low_db  = per_cid_low_db.get(cid)
                common_high_db = per_cid_common_high_db.get(cid)
                high_db = high_db_map.get(cid)
                if low_db is None or common_high_db is None or low_db > common_high_db:
                    continue
                for mid, n2a in per_cid_n2a.get(cid, []):
                    model_max_db = per_model_max_db.get((cid, mid))
                    n2a_y = n2a.get('y') or []
                    model_max_airflow = float(max(n2a_y)) if n2a_y else 0.0
                    eff_wa2 = high_db
                    wa2_val = None
                    if eff_wa2 is None:
                        pass
                    elif model_max_db is not None and model_max_db < eff_wa2:
                        if model_max_airflow > 0:
                            wa2_val = model_max_airflow
                    else:
                        try:
                            val = eval_pchip(n2a, eff_wa2)
                            if val is not None and math.isfinite(val) and val > 0:
                                wa2_val = float(val)
                        except Exception:
                            pass
                    if wa2_val is not None and wa2_val > 0:
                        _update_max_with_model(pp_a2, pp_a2_mid, cid, wa2_val, mid)
            per_profile_a2[pkey] = pp_a2
            per_profile_a2_mid[pkey] = pp_a2_mid

        like_rank_cache.refresh_like_counts(_fetch_all, force=True)
        lrc_snapshot = like_rank_cache.get_all_like_counts()
        per_cid_wc_raw: Dict[int, int] = defaultdict(int)
        per_cid_wc_mid: Dict[int, int] = {}
        for (mid, cid), val in lrc_snapshot.items():
            if cid not in SCORE_CONDITION_IDS:
                continue
            _update_max_with_model(per_cid_wc_raw, per_cid_wc_mid, cid, val, mid)
        per_cid_c: Dict[int, float] = {
            cid: math.log1p(raw_max) for cid, raw_max in per_cid_wc_raw.items()
        }

        per_cid_d: Dict[int, float] = defaultdict(float)
        per_cid_d_mid: Dict[int, int] = {}
        for cid in cid_list:
            for mid, n2a in per_cid_n2a.get(cid, []):
                n2a_x = n2a.get('x') or []
                n2a_y = n2a.get('y') or []
                if n2a_x and n2a_y and len(n2a_x) == len(n2a_y):
                    try:
                        max_af = float(max(n2a_y))
                        max_af_idx = n2a_y.index(max_af)
                        noise_at_max = float(n2a_x[max_af_idx])
                        if max_af > 0 and noise_at_max > 0:
                            d_raw = max_af / noise_at_max
                            _update_max_with_model(per_cid_d, per_cid_d_mid, cid, d_raw, mid)
                    except Exception:
                        pass

        now_t = time.time()
        with _cond_denom_lock:
            for pkey in SCORING_PROFILES:
                high_db_map = per_profile_high_db[pkey]
                mid_db_map = per_profile_mid_db[pkey]
                pp_a2 = per_profile_a2[pkey]
                pp_a2_mid = per_profile_a2_mid[pkey]
                pp_a2_ref_mid = per_profile_a2_ref_mid[pkey]
                pp_a3 = per_profile_a3[pkey]
                pp_a3_mid = per_profile_a3_mid[pkey]
                for cid in cid_list:
                    _cond_denom_cache[pkey][cid] = {
                        'low_db':      per_cid_low_db.get(cid),
                        'common_high_db': per_cid_common_high_db.get(cid),
                        'high_db':     high_db_map.get(cid),
                        'mid_db':      mid_db_map.get(cid),
                        'max_a1':      per_cid_a1.get(cid, 0.0),
                        'max_a2':      pp_a2.get(cid, 0.0),
                        'max_a3':      pp_a3.get(cid, 0.0),
                        'max_b':       per_cid_b.get(cid, 0.0),
                        'max_c':       per_cid_c.get(cid, 0.0),
                        'max_d':       per_cid_d.get(cid, 0.0),
                        'wc_raw_max':  per_cid_wc_raw.get(cid, 0),
                        'model_id_a1': per_cid_a1_mid.get(cid),
                        'model_id_a2': pp_a2_mid.get(cid),
                        'model_id_a3': pp_a3_mid.get(cid),
                        'model_id_b':  per_cid_b_mid.get(cid),
                        'model_id_c':  per_cid_wc_mid.get(cid),
                        'model_id_d':  per_cid_d_mid.get(cid),
                        'ref_db_model_id_a1': per_cid_a1_ref_mid.get(cid),
                        'ref_db_model_id_a2': pp_a2_ref_mid.get(cid),
                        'cached_at':   now_t,
                    }
        if _app_debug:
            _logger.debug('[denom_refresh] completed for %d conditions × %d profiles',
                          len(cid_list), len(SCORING_PROFILES))

        with _abc_cache_lock:
            for pkey in SCORING_PROFILES:
                _abc_cache[pkey].clear()

        _invalidate_abc_disk_cache()

        # Keep audit rows profile-independent by persisting the default ('med') profile
        # reference points for WA2/WA3.
        _persist_denom_audit(
            cid_list,
            per_cid_low_db, per_profile_high_db['med'], per_profile_mid_db['med'],
            per_cid_a1, per_cid_a1_mid,
            per_profile_a2['med'], per_profile_a2_mid['med'],
            per_profile_a3['med'], per_profile_a3_mid['med'],
            per_cid_b,  per_cid_b_mid,
            per_cid_wc_raw, per_cid_wc_mid,
            per_cid_d,  per_cid_d_mid,
        )

        _save_denom_cache_to_disk()
    except Exception as e:
        _logger.warning('[denom_refresh] error: %s', e)


def _warmup_rankings_async() -> None:
    """Launch a background thread that pre-builds rankings for all scoring profiles."""
    if os.getenv('DISABLE_COND_STATS_CRON'):
        if _app_debug:
            _logger.debug('[warmup] rankings warm-up skipped because DISABLE_COND_STATS_CRON is set')
        return
    if not _warmup_rankings_inflight.acquire(blocking=False):
        return

    def _run():
        try:
            try:
                with _canonical_facts_cache_lock:
                    _canonical_facts_cache_data.pop('entry', None)
                get_canonical_facts()
                if _app_debug:
                    _logger.debug('[warmup] canonical facts warmed (pid=%s)', os.getpid())
            except Exception as _cfe:
                _logger.warning('[warmup] canonical facts warm-up failed: %s', _cfe)

            for _profile in SCORING_PROFILES:
                try:
                    _cache_key = f'data:{_profile}'
                    with _rankings_v2_cache_lock:
                        _rankings_v2_cache.pop(_cache_key, None)
                    _disk_path = _rankings_cache_path(_profile)
                    try:
                        os.remove(_disk_path)
                    except FileNotFoundError:
                        pass
                    get_rankings_v2(_profile)
                    if _app_debug:
                        _logger.debug('[warmup] rankings warmed (profile=%s, pid=%s)',
                                      _profile, os.getpid())
                except Exception as _e:
                    _logger.warning('[warmup] rankings warm-up failed (profile=%s): %s', _profile, _e)
        finally:
            _warmup_rankings_inflight.release()

    threading.Thread(target=_run, daemon=True, name='rankings-warmup').start()


def _like_max_refresh_loop():
    """Every 5 minutes: update max_c/wc_raw_max/model_id_c in _cond_denom_cache."""
    while True:
        time.sleep(5 * 60)
        try:
            like_rank_cache.refresh_like_counts(_fetch_all, force=True)
            snapshot = like_rank_cache.get_all_like_counts()
            cid_list = list(SCORE_CONDITION_IDS)
            per_cid_max: Dict[int, int] = {}
            per_cid_mid: Dict[int, int] = {}
            for (mid, cid), val in snapshot.items():
                if cid not in SCORE_CONDITION_IDS:
                    continue
                curr = per_cid_max.get(cid, -1)
                if val > curr or (val == curr and mid > per_cid_mid.get(cid, float('-inf'))):
                    per_cid_max[cid] = val
                    per_cid_mid[cid] = mid
            with _cond_denom_lock:
                for cid, max_like in per_cid_max.items():
                    new_c = math.log1p(max_like)
                    new_mid = per_cid_mid.get(cid)
                    for pkey in SCORING_PROFILES:
                        entry = _cond_denom_cache[pkey].get(cid)
                        if entry is not None and new_c > entry.get('max_c', 0.0):
                            entry['max_c'] = new_c
                            entry['wc_raw_max'] = max_like
                            if new_mid is not None:
                                entry['model_id_c'] = new_mid
        except Exception as e:
            _logger.warning('[like_max_refresh] error: %s', e)


def _denom_refresh_loop():
    """Run _do_denom_refresh at startup and then daily at 06:00 CST (UTC+8)."""
    import random as _rand
    _lock_path = os.path.join(tempfile.gettempdir(), 'fancool_denom_refresh_startup.lock')
    _startup_delay = 0

    with startup_lock(_lock_path) as _acquired:
        if _acquired:
            if _app_debug:
                _logger.debug('[denom_refresh] startup refresh triggered (pid=%s)', os.getpid())
            _do_denom_refresh()
            _warmup_rankings_async()
        else:
            _startup_delay = _rand.uniform(30, 90)
            if _app_debug:
                _logger.debug('[denom_refresh] startup back-off %.0fs (pid=%s)',
                              _startup_delay, os.getpid())

    if _startup_delay > 0:
        time.sleep(_startup_delay)
        if not _load_denom_cache_from_disk():
            _do_denom_refresh()
        _warmup_rankings_async()

    while True:
        try:
            cst = timezone(timedelta(hours=8))
            now = datetime.now(cst)
            target = now.replace(hour=6, minute=0, second=0, microsecond=0)
            if now >= target:
                target = target + timedelta(days=1)
            sleep_secs = (target - now).total_seconds()
        except Exception:
            sleep_secs = 86400
        time.sleep(max(sleep_secs, 1))
        _do_denom_refresh()
        _warmup_rankings_async()


# =========================================
# Score rule helper
# =========================================

def _build_condition_weights(cid_list: list) -> list:
    """Return normalized composite-score weights (as percentages) for each condition in cid_list."""
    raw = {cid: COMPOSITE_WEIGHTS.get(cid, 1.0) for cid in cid_list}
    total = sum(raw.values())

    if total <= 0:
        return [{'condition_id': cid, 'weight_pct': 0.0} for cid in cid_list]

    entries = []
    for cid in cid_list:
        weight = raw[cid]
        exact_pct = weight / total * 100.0
        if weight > 0:
            base_pct = math.floor(exact_pct * 10.0) / 10.0
            frac = exact_pct - base_pct
            active = True
        else:
            base_pct = 0.0
            frac = 0.0
            active = False
        entries.append({
            'condition_id': cid,
            'exact_pct': exact_pct,
            'base_pct': base_pct,
            'frac': frac,
            'active': active,
        })

    sum_base_active = sum(e['base_pct'] for e in entries if e['active'])
    remainder = round(100.0 - sum_base_active, 1)
    steps = max(0, int(round(remainder * 10)))

    active_entries = [e for e in entries if e['active']]
    active_entries.sort(key=lambda e: e['frac'], reverse=True)

    for i in range(steps):
        if not active_entries:
            break
        idx = i % len(active_entries)
        active_entries[idx]['base_pct'] = round(active_entries[idx]['base_pct'] + 0.1, 1)

    base_by_cid = {e['condition_id']: e['base_pct'] for e in entries}
    result = []
    for cid in cid_list:
        pct = base_by_cid.get(cid, 0.0)
        result.append({'condition_id': cid, 'weight_pct': pct})
    return result


# =========================================
# Background thread launcher
# =========================================

def start_background_threads():
    """Start the denom-refresh and like-max-refresh daemon threads.

    Call once during app startup after ``setup()`` has been called.
    """
    if _fetch_all is None or _exec_write is None or _logger is None:
        raise RuntimeError(
            "start_background_threads() called before setup(); "
            "missing injected dependencies"
        )
    if not os.getenv('DISABLE_COND_STATS_CRON'):
        threading.Thread(target=_denom_refresh_loop, daemon=True).start()
        threading.Thread(target=_like_max_refresh_loop, daemon=True).start()
