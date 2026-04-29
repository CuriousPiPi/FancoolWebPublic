"""
Scoring system module for FancoolWeb.

Provides model-score computation, rankings v2, canonical facts, and denom-refresh
background jobs extracted from fancoolserver.py.

Call ``setup(fetch_all_fn, exec_write_fn, logger, app_debug=False)`` once during
app startup before using any public functions.  Call ``start_background_threads()``
to launch scoring background daemon threads.
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

from app.curves.pchip_cache import eval_pchip, perf_interp_contract
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


def _parse_bounded_int_env(env_var: str, default: int, min_value: int, max_value: int | None = None) -> int:
    raw = os.getenv(env_var, '').strip()
    if not raw:
        value = default
    else:
        try:
            value = int(raw)
        except ValueError as exc:
            raise ValueError(f"Environment variable {env_var!r} must be an integer, got {raw!r}") from exc
    if value < min_value:
        raise ValueError(f"Environment variable {env_var!r} must be >= {min_value}, got {value}")
    if max_value is not None and value > max_value:
        raise ValueError(f"Environment variable {env_var!r} must be <= {max_value}, got {value}")
    return value


def _parse_bounded_float_env(env_var: str, default: float, min_value: float, max_value: float | None = None) -> float:
    raw = os.getenv(env_var, '').strip()
    if not raw:
        value = default
    else:
        try:
            value = float(raw)
        except ValueError as exc:
            raise ValueError(f"Environment variable {env_var!r} must be a float, got {raw!r}") from exc
    if not math.isfinite(value):
        raise ValueError(f"Environment variable {env_var!r} must be finite, got {value!r}")
    if value < min_value:
        raise ValueError(f"Environment variable {env_var!r} must be >= {min_value}, got {value}")
    if max_value is not None and value > max_value:
        raise ValueError(f"Environment variable {env_var!r} must be <= {max_value}, got {value}")
    return value


# =========================================
# Model-score config
# =========================================

SCORE_MAX = 100

# =========================================
# Baseline valid-interval bound thresholds
# =========================================
# Both the lower bound (valid_db_min) and upper bound (valid_db_max) of the scoring
# interval use an independent dual-threshold MAX rule:
#
#   required_coverage = max(min_count_threshold, ceil(min_ratio_threshold * total_curves))
#
# where total_curves = total number of curves participating in scoring for that condition.
# This ensures the bound is never based on fewer than min_count_threshold curves AND that
# a meaningful fraction of the pool must cover the point.

# Lower bound (valid_db_min) parameters:
#   first dB point in the best segment where coverage >= max(lower_min_count, ceil(lower_min_ratio * N))
BASELINE_VALID_LOWER_MIN_COUNT: int = _parse_bounded_int_env(
    'BASELINE_VALID_LOWER_MIN_COUNT', 5, min_value=1, max_value=1000
)
BASELINE_VALID_LOWER_MIN_RATIO: float = _parse_bounded_float_env(
    'BASELINE_VALID_LOWER_MIN_RATIO', 0.5, min_value=0.0, max_value=1.0
)

# Upper bound (valid_db_max) parameters:
#   last dB point in the best segment where coverage >= max(upper_min_count, ceil(upper_min_ratio * N))
BASELINE_VALID_UPPER_MIN_COUNT: int = _parse_bounded_int_env(
    'BASELINE_VALID_UPPER_MIN_COUNT', 5, min_value=1, max_value=1000
)
BASELINE_VALID_UPPER_MIN_RATIO: float = _parse_bounded_float_env(
    'BASELINE_VALID_UPPER_MIN_RATIO', 0.5, min_value=0.0, max_value=1.0
)

BASELINE_DB_STEP = _parse_bounded_float_env('BASELINE_DB_STEP', 1.0, min_value=0.1, max_value=5.0)
MAX_BASELINE_GRID_POINTS = _parse_bounded_int_env('MAX_BASELINE_GRID_POINTS', 400, min_value=10, max_value=5000)

# =========================================
# Equal-airflow sampling
# =========================================
# The baseline is an exponential (airflow = exp(intercept + slope*dB)), so it can be
# inverted analytically.  Instead of sampling at fixed dB steps (which over-samples the
# low-airflow / idle region due to the log-scale compression), we:
#   1. Determine the airflow range [af_min, af_max] at [valid_db_min, valid_db_max].
#   2. Divide that airflow range into RAW_SCORE_AIRFLOW_SAMPLE_COUNT equally spaced steps.
#   3. Map each airflow value back to its dB position via the inverted baseline.
# This produces a shared dB grid that is denser in the high-airflow / high-load region
# and naturally lighter-weighted in the low-airflow / idle region.
RAW_SCORE_AIRFLOW_SAMPLE_COUNT: int = _parse_bounded_int_env(
    'RAW_SCORE_AIRFLOW_SAMPLE_COUNT', 31, min_value=2, max_value=5000
)

# =========================================
# Adaptive per-sample coverage weighting
# =========================================
# When enabled, each shared sample point is weighted by the fraction of participating
# curves that cover that dB point (coverage_count / total_curves).  Points covered by
# all curves get weight 1.0; points covered by only half the curves get weight 0.5.
# The per-model raw score is then a weighted mean over the model's covered sample points.
# When disabled, an equal-weight mean is used instead.
def _parse_bool_env(env_var: str, default: bool) -> bool:
    raw = os.getenv(env_var, '').strip().lower()
    if not raw:
        return default
    if raw in ('1', 'true', 'yes'):
        return True
    if raw in ('0', 'false', 'no'):
        return False
    raise ValueError(
        f"Environment variable {env_var!r} must be '1'/'true'/'yes' or '0'/'false'/'no', got {raw!r}"
    )

RAW_SCORE_ADAPTIVE_WEIGHTS: bool = _parse_bool_env('RAW_SCORE_ADAPTIVE_WEIGHTS', True)
DB_GRID_PRECISION = 6
DB_GRID_END_TOLERANCE_FACTOR = 0.25
BASELINE_SEGMENT_CONTIGUITY_FACTOR = 1.5
BASELINE_MIN_SEGMENT_POINTS = 5

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
_DEFAULT_RADAR_CIDS: list = [1, 10, 7, 3, 2, 11]
_RADAR_CIDS: list = _parse_radar_condition_ids('SCORE_CONDITION_IDS', _DEFAULT_RADAR_CIDS)
SCORE_CONDITION_IDS: frozenset = frozenset(_RADAR_CIDS)

_DEFAULT_COMPOSITE_WEIGHTS: Dict[int, float] = {
    1: 0.0, 2: 2.0, 3: 2.0, 7: 2.0, 8: 2.0, 11: 2.0,
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
# Canonical scoring key
# =========================================
_CANONICAL_SCORE_KEY = 'canonical'


MODEL_SCORE_CACHE_SOFT_TTL_SEC = int(
    os.getenv('MODEL_SCORE_CACHE_SOFT_TTL_SEC')
    or str(10 * 60)
)
MODEL_SCORE_CACHE_HARD_TTL_SEC = int(
    os.getenv('MODEL_SCORE_CACHE_HARD_TTL_SEC')
    or str(120 * 60)
)

_cond_denom_cache: Dict[int, dict] = {}
_cond_denom_lock = threading.Lock()

_model_score_cache: Dict[int, dict] = {}
_model_score_cache_lock = threading.Lock()

_model_score_inflight: set = set()
_model_score_inflight_lock = threading.Lock()

_model_score_disk_loaded = False
_model_score_disk_loaded_lock = threading.Lock()
_MODEL_SCORE_DISK_LOAD_RETRY_INTERVAL_SEC = 3
_model_score_disk_last_attempt_at = 0.0

# =========================================
# Rankings / display config
# =========================================
_RANKINGS_V2_DISPLAY_LIMIT = 10
# _RADAR_CIDS is defined earlier (CCW canonical order from SCORE_CONDITION_IDS env var)

_rankings_v2_cache: dict = {}
_rankings_v2_cache_lock = threading.Lock()
_RANKINGS_V2_CACHE_TTL_SEC = 600

_rankings_v2_build_lock = threading.Lock()
_warmup_rankings_inflight = threading.Lock()
_VISIBILITY_DIFF_INCREMENTAL_MAX = 5
_VISIBLE_MODEL_SET_WATCH_INTERVAL_SEC = max(
    5,
    int(os.getenv('VISIBLE_MODEL_SET_WATCH_INTERVAL_SEC') or '30'),
)

# =========================================
# Shared file-based caches for multi-worker environments
# =========================================
_DENOM_CACHE_FILE_TTL_SEC = int(os.getenv('DENOM_CACHE_FILE_TTL_SEC', '14400'))
_RANKINGS_CACHE_FILE_TTL_SEC = int(os.getenv('RANKINGS_CACHE_FILE_TTL_SEC', '600'))
_MODEL_SCORE_CACHE_FILE_TTL_SEC = int(
    os.getenv('MODEL_SCORE_CACHE_FILE_TTL_SEC')
    or '7200'
)
# Startup backoff for workers that missed the startup lock; keeps wait short while
# giving the lock-holder time to write the fresh denom cache.
_DENOM_STARTUP_CACHE_RETRY_INTERVALS_SEC = (3, 5, 8)


def _shared_cache_dir() -> str:
    # Reuses CURVE_CACHE_DIR intentionally: PCHIP curve/spectrum caches are
    # read-only after generation and have no semantic difference between dev and
    # prod, so sharing the directory saves disk space.  Only the scoring-system
    # JSON caches (denom, canonical_facts, rankings_v2, model_score cache) are
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
    # Cache format v5: curve-relative baseline payload + ratio-to-best normalization metadata
    # + per-fan 1 dB PCHIP-sampled raw-score semantics.
    return os.path.join(_shared_cache_dir(), f'denom_cache_v5{_score_cache_suffix()}.json')


def _rankings_cache_path() -> str:
    safe_key = ''.join(c for c in _CANONICAL_SCORE_KEY if c.isalnum() or c == '_')
    return os.path.join(_shared_cache_dir(), f'rankings_v2_{safe_key}{_score_cache_suffix()}.json')


def _model_score_cache_path() -> str:
    safe_key = ''.join(c for c in _CANONICAL_SCORE_KEY if c.isalnum() or c == '_')
    return os.path.join(_shared_cache_dir(), f'model_score_cache_{safe_key}{_score_cache_suffix()}.json')


def _json_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f'Object of type {type(obj).__name__} is not JSON serializable')


def _safe_median(values: list[float]) -> float | None:
    vals = sorted(v for v in values if isinstance(v, (int, float)) and math.isfinite(v))
    if not vals:
        return None
    n = len(vals)
    m = n // 2
    if n % 2 == 1:
        return float(vals[m])
    return float((vals[m - 1] + vals[m]) / 2.0)


def _build_unified_db_grid(min_db: float, max_db: float, step: float) -> list[float]:
    if not (math.isfinite(min_db) and math.isfinite(max_db) and math.isfinite(step) and step > 0):
        return []
    start = math.ceil(min_db / step) * step
    end = math.floor(max_db / step) * step
    if start > end:
        return []
    approx_count = int(math.floor((end - start) / step)) + 1
    if approx_count > MAX_BASELINE_GRID_POINTS:
        if MAX_BASELINE_GRID_POINTS <= 1:
            return [round(start, DB_GRID_PRECISION)]
        lin_step = (end - start) / (MAX_BASELINE_GRID_POINTS - 1)
        sampled = [round(start + i * lin_step, DB_GRID_PRECISION) for i in range(MAX_BASELINE_GRID_POINTS)]
        sampled[-1] = round(end, DB_GRID_PRECISION)
        deduped = []
        seen = set()
        for db in sampled:
            if db in seen:
                continue
            seen.add(db)
            deduped.append(db)
        return deduped
    points = []
    i = 0
    # Guard against floating-point error by snapping each point to DB_GRID_PRECISION decimals.
    while True:
        db = start + i * step
        if db > end + (step * DB_GRID_END_TOLERANCE_FACTOR):
            break
        points.append(round(db, DB_GRID_PRECISION))
        i += 1
    if points:
        end_rounded = round(end, DB_GRID_PRECISION)
        if points[-1] != end_rounded:
            points.append(end_rounded)
    return points


def _fit_exponential_baseline(points: list[tuple[float, float]]) -> dict | None:
    """Fit ln(y)=intercept+slope*x on positive y points."""
    fit_points = [
        (float(x), float(y))
        for x, y in points
        if isinstance(x, (int, float))
        and isinstance(y, (int, float))
        and math.isfinite(x)
        and math.isfinite(y)
        and y > 0
    ]
    n = len(fit_points)
    if n < 2:
        return None
    xs = [p[0] for p in fit_points]
    lny = [math.log(p[1]) for p in fit_points]
    x_mean = sum(xs) / n
    y_mean = sum(lny) / n
    ss_xx = sum((x - x_mean) ** 2 for x in xs)
    if ss_xx <= 0:
        return None
    ss_xy = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, lny))
    slope = ss_xy / ss_xx
    intercept = y_mean - slope * x_mean
    return {
        'intercept': float(intercept),
        'slope': float(slope),
        'point_count': n,
    }


def _baseline_airflow_at_db(fit: dict, db: float) -> float | None:
    try:
        val = math.exp(float(fit['intercept']) + float(fit['slope']) * float(db))
        return float(val) if math.isfinite(val) and val > 0 else None
    except Exception:
        return None


def _baseline_db_at_airflow(fit: dict, airflow: float) -> float | None:
    """Invert the exponential baseline to recover the dB value for a given airflow.

    The baseline is airflow = exp(intercept + slope * db), so:
        db = (ln(airflow) - intercept) / slope
    Returns None if the inversion is undefined (non-positive airflow, zero or near-zero
    slope, non-finite result).
    """
    try:
        if airflow <= 0:
            return None
        slope = float(fit['slope'])
        intercept = float(fit['intercept'])
        slope_epsilon = 1e-12
        if not math.isfinite(slope) or abs(slope) < slope_epsilon:
            return None
        db = (math.log(float(airflow)) - intercept) / slope
        return float(db) if math.isfinite(db) else None
    except Exception:
        return None


def _build_equal_airflow_db_grid(
    baseline_fit: dict, valid_db_min: float, valid_db_max: float, n: int
) -> list[float]:
    """Build a shared dB grid by equal-airflow spacing over the baseline valid interval.

    Steps:
      1. Evaluate baseline airflow at valid_db_min and valid_db_max.
      2. Generate *n* equally spaced airflow values between those two bounds.
      3. Map each airflow value back to its corresponding dB via the inverted exponential baseline.

    Because db = (ln(airflow) - intercept) / slope, equal airflow steps map to larger
    dB steps at low airflow and smaller dB steps at high airflow.  This makes the shared
    dB grid sparser in the low-airflow / idle region and denser in the high-airflow /
    high-load region, naturally de-emphasising the idle zone without any manual segment
    weights.

    Returns a list of *n* dB values, or an empty list if the construction fails.
    """
    if n < 2:
        return []
    af_min = _baseline_airflow_at_db(baseline_fit, valid_db_min)
    af_max = _baseline_airflow_at_db(baseline_fit, valid_db_max)
    if af_min is None or af_max is None or af_min <= 0 or af_max <= 0:
        return []
    result: list[float] = []
    for i in range(n):
        af = af_min + (af_max - af_min) * i / (n - 1)
        db = _baseline_db_at_airflow(baseline_fit, af)
        if db is None or not math.isfinite(db):
            return []
        result.append(db)
    return result if len(result) == n else []


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



def _aggregate_ratios(ratios: list[float], weights: list[float]) -> float:
    """Aggregate a list of per-sample airflow ratios into a single raw score.

    When RAW_SCORE_ADAPTIVE_WEIGHTS is enabled and the weights list is non-empty,
    computes a standard weighted mean (sum(ratio * weight) / sum(weight)).
    Falls back to an equal-weight arithmetic mean when adaptive weighting is off,
    when the weights list is empty, or when the weight sum is zero.

    ``ratios`` and ``weights`` must have the same length.
    """
    if not ratios:
        return 0.0
    if RAW_SCORE_ADAPTIVE_WEIGHTS and weights:
        if len(weights) != len(ratios):
            raise ValueError("ratios and weights must be the same length")
        total_w = sum(weights)
        if total_w > 0:
            return sum(r * w for r, w in zip(ratios, weights)) / total_w
    return sum(ratios) / len(ratios)


def _denom_cache_fingerprint() -> str:
    parts = [
        f"code={CODE_VERSION}",
        f"interp={perf_interp_contract()}",
        f"cids={sorted(SCORE_CONDITION_IDS)}",
        # Lower-bound (valid_db_min) threshold: MAX(count, ceil(ratio * N)).
        f"lower_min_count={BASELINE_VALID_LOWER_MIN_COUNT}",
        f"lower_min_ratio={BASELINE_VALID_LOWER_MIN_RATIO}",
        # Upper-bound (valid_db_max) threshold: MAX(count, ceil(ratio * N)).
        f"upper_min_count={BASELINE_VALID_UPPER_MIN_COUNT}",
        f"upper_min_ratio={BASELINE_VALID_UPPER_MIN_RATIO}",
        f"baseline_step={BASELINE_DB_STEP}",
        # Equal-airflow sampling: N equally spaced airflow points over the valid interval,
        # mapped back to dB via the inverted exponential baseline.
        f"raw_score_airflow_samples={RAW_SCORE_AIRFLOW_SAMPLE_COUNT}",
        f"raw_score_adaptive_weights={RAW_SCORE_ADAPTIVE_WEIGHTS}",
        "raw_score_sampling=equal_airflow",
        "norm=ratio_to_best",
    ]
    parts.append(_CANONICAL_SCORE_KEY)
    return "|".join(parts)


def _model_score_cache_fingerprint() -> str:
    base = _denom_cache_fingerprint()
    cw_str = ','.join(f'{k}:{v:.6g}' for k, v in sorted(COMPOSITE_WEIGHTS.items()))
    return f"{base}|cw={cw_str}"


def _save_denom_cache_to_disk() -> None:
    try:
        with _cond_denom_lock:
            snapshot = {cid: dict(entry) for cid, entry in _cond_denom_cache.items()}
        payload = {
            'cond_denom_cache': snapshot,
            'written_at': time.time(),
            'fingerprint': _denom_cache_fingerprint(),
        }
        _atomic_json_write(_denom_cache_path(), payload)
    except Exception as e:
        _logger.warning('[denom_cache] save to disk failed: %s', e)


def _normalize_and_validate_denom_entry_model_id_maps(entry: dict) -> bool:
    raw_score_by_model = entry.get('raw_score_by_model')
    sampled_points = entry.get('sampled_points_used_by_model')
    if not isinstance(raw_score_by_model, dict) or not isinstance(sampled_points, dict):
        return False
    try:
        entry['raw_score_by_model'] = {str(int(mid)): float(val) for mid, val in raw_score_by_model.items()}
        entry['sampled_points_used_by_model'] = {str(int(mid)): int(val) for mid, val in sampled_points.items()}
    except (TypeError, ValueError):
        return False
    return True


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
        if not isinstance(raw, dict):
            return False
        loaded: Dict[int, dict] = {}
        for k, v in raw.items():
            entry = dict(v or {})
            # Strict cache schema for curve-relative baseline cache (ratio-to-best normalization).
            if (
                'valid_db_min' not in entry
                or 'valid_db_max' not in entry
                or 'baseline_fit' not in entry
                or 'cond_db_grid' not in entry
                or 'normalization_mode' not in entry
                or 'raw_score_sampling' not in entry
                or 'raw_score_best' not in entry
                or 'raw_score_by_model' not in entry
                or 'sampled_points_used_by_model' not in entry
            ):
                return False
            if entry.get('normalization_mode') != 'ratio_to_best':
                return False
            if entry.get('raw_score_sampling') != 'equal_airflow':
                return False
            if not isinstance(entry.get('cond_db_grid'), list):
                return False
            # Validate cond_point_weights: it must be present as a list with the same
            # length as cond_db_grid, containing only finite values in [0.0, 1.0].
            # When adaptive weighting is enabled this list is semantically required; if
            # it is absent or malformed we reject the cache so it is rebuilt correctly.
            cond_db_grid_len = len(entry['cond_db_grid'])
            cond_point_weights = entry.get('cond_point_weights')
            if not isinstance(cond_point_weights, list):
                return False
            if len(cond_point_weights) != cond_db_grid_len:
                return False
            if cond_db_grid_len > 0:
                try:
                    for w in cond_point_weights:
                        wf = float(w)
                        if not (math.isfinite(wf) and 0.0 <= wf <= 1.0):
                            return False
                except (TypeError, ValueError):
                    return False
            if not _normalize_and_validate_denom_entry_model_id_maps(entry):
                return False
            raw_score_by_model = entry['raw_score_by_model']
            if raw_score_by_model:
                try:
                    raw_score_best = float(entry.get('raw_score_best'))
                except (TypeError, ValueError):
                    return False
                if not math.isfinite(raw_score_best) or raw_score_best <= 0:
                    return False
            loaded[int(k)] = entry
        # Ensure the payload covers every currently configured radar condition.
        if not all(cid in loaded for cid in SCORE_CONDITION_IDS):
            return False
        with _cond_denom_lock:
            _cond_denom_cache.clear()
            _cond_denom_cache.update(loaded)
        if _app_debug:
            _logger.debug('[denom_cache] loaded from disk (pid=%s)', os.getpid())
        return True
    except Exception as e:
        _logger.warning('[denom_cache] load from disk failed: %s', e)
        return False


def _save_rankings_cache(result: dict) -> None:
    try:
        payload = {'result': result, 'written_at': time.time()}
        _atomic_json_write(_rankings_cache_path(), payload)
    except Exception as e:
        _logger.warning('[rankings_v2] save to disk failed: %s', e)


def _normalize_rankings_item(item: dict) -> None:
    """Normalize condition_id (CID) keys back to int in a rankings item after JSON round-trip."""
    for key in ('condition_scores', 'condition_heat', 'condition_likes'):
        val = item.get(key)
        if isinstance(val, dict):
            item[key] = {int(k): v for k, v in val.items()}


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


def _load_rankings_cache_from_disk():
    """Return (written_at, result_dict) from disk if fresh, or None if stale/missing."""
    try:
        p = _rankings_cache_path()
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
        _logger.warning('[rankings_v2] load from disk failed: %s', e)
        return None


def _normalize_model_score_entry(entry: dict) -> None:
    """Normalize condition_id (CID) keys back to int in a model-score cache entry."""
    conds = entry.get('conditions')
    if isinstance(conds, dict):
        entry['conditions'] = {int(k): v for k, v in conds.items()}


def _validate_model_score_cache_entry(entry: dict) -> dict:
    if not isinstance(entry, dict):
        raise ValueError("model_score cache entry is not a dict")
    if 'cached_at' not in entry:
        raise KeyError("model_score cache entry missing 'cached_at'")
    try:
        entry['cached_at'] = float(entry['cached_at'])
    except (TypeError, ValueError):
        raise ValueError(f"model_score cache entry 'cached_at' is not numeric: {entry['cached_at']!r}")
    if 'conditions' not in entry:
        raise KeyError("model_score cache entry missing 'conditions'")
    if not isinstance(entry['conditions'], dict):
        raise ValueError("model_score cache entry 'conditions' is not a dict")
    if 'composite_score' in entry:
        cs = entry['composite_score']
        if cs is not None and not isinstance(cs, int):
            try:
                entry['composite_score'] = int(cs)
            except (TypeError, ValueError):
                raise ValueError(f"model_score cache entry 'composite_score' is not int/None: {cs!r}")
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


def _save_model_score_cache_to_disk() -> None:
    try:
        with _model_score_cache_lock:
            snapshot = {str(mid): dict(entry) for mid, entry in _model_score_cache.items()}
        if not snapshot:
            return
        payload = {
            'entries': snapshot,
            'written_at': time.time(),
            'fingerprint': _model_score_cache_fingerprint(),
            'denom_written_at': _get_current_denom_written_at(),
        }
        _atomic_json_write(_model_score_cache_path(), payload)
        if _app_debug:
            _logger.debug('[model_score_cache] saved %d entries to disk', len(snapshot))
    except Exception as e:
        _logger.warning('[model_score_cache] save to disk failed: %s', e)


def _invalidate_model_score_disk_cache() -> None:
    global _model_score_disk_loaded, _model_score_disk_last_attempt_at
    with _model_score_disk_loaded_lock:
        _model_score_disk_loaded = False
        _model_score_disk_last_attempt_at = 0.0
    try:
        path = _model_score_cache_path()
        if os.path.isfile(path):
            os.remove(path)
    except Exception as e:
        _logger.warning('[model_score_cache] invalidate disk failed: %s', e)


def _try_load_model_score_from_disk() -> None:
    global _model_score_disk_loaded, _model_score_disk_last_attempt_at
    now = time.time()
    with _model_score_disk_loaded_lock:
        if _model_score_disk_loaded:
            return
        last_attempt = _model_score_disk_last_attempt_at
        if (now - last_attempt) < _MODEL_SCORE_DISK_LOAD_RETRY_INTERVAL_SEC:
            return
        _model_score_disk_last_attempt_at = now

    try:
        loaded = 0
        payload_accepted = False
        path = _model_score_cache_path()
        if not os.path.isfile(path):
            return
        with open(path, 'r', encoding='utf-8') as f:
            payload = json.load(f)
        if payload.get('fingerprint') != _model_score_cache_fingerprint():
            _logger.debug('[model_score_cache] fingerprint mismatch, skipping disk load')
            return
        written_at = float(payload.get('written_at') or 0)
        if (time.time() - written_at) > _MODEL_SCORE_CACHE_FILE_TTL_SEC:
            return
        model_score_denom_ts = float(payload.get('denom_written_at') or 0)
        current_denom_ts = _get_current_denom_written_at()
        if current_denom_ts > model_score_denom_ts:
            _logger.debug('[model_score_cache] denom generation mismatch (cache_denom_ts=%.0f < '
                          'current_denom_ts=%.0f), skipping disk load',
                          model_score_denom_ts, current_denom_ts)
            return
        entries = payload.get('entries')
        if not entries:
            return
        payload_accepted = True
        with _model_score_cache_lock:
            for str_mid, entry in entries.items():
                try:
                    mid = int(str_mid)
                    _normalize_model_score_entry(entry)
                    _validate_model_score_cache_entry(entry)
                    if mid not in _model_score_cache:
                        _model_score_cache[mid] = entry
                        loaded += 1
                except Exception as _entry_exc:
                    _logger.debug('[model_score_cache] skipping entry str_mid=%r: %s', str_mid, _entry_exc)
                    continue
        if payload_accepted:
            with _model_score_disk_loaded_lock:
                _model_score_disk_loaded = True
        if _app_debug and loaded > 0:
            _logger.debug('[model_score_cache] loaded %d entries from disk', loaded)
    except Exception as e:
        _logger.warning('[model_score_cache] load from disk failed: %s', e)


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
    """Compute the canonical score-independent facts snapshot from DB."""
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

    lrc_snapshot: dict | None = None
    try:
        like_rank_cache.ensure_cache(_fetch_all)
    except Exception as e:
        _logger.warning('[canonical_facts] like cache refresh failed: %s', e)
    try:
        # Keep heat/like rendering resilient by reusing any in-proc snapshot
        # even when refresh fails.
        lrc_snapshot = like_rank_cache.get_all_like_counts()
    except Exception as e:
        _logger.warning('[canonical_facts] like cache snapshot read failed: %s', e)

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
        meta = model_meta.get(mid)
        # Guard against stale IDs present in query/like snapshots but no longer
        # displayable in available_models_info_view.
        if not isinstance(meta, dict):
            continue
        if not (meta.get('brand_name_zh') or meta.get('model_name')):
            continue

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


def _invalidate_canonical_facts_cache(purge_disk: bool = True) -> None:
    with _canonical_facts_cache_lock:
        _canonical_facts_cache_data.pop('entry', None)
    if not purge_disk:
        return
    try:
        os.remove(_canonical_facts_path())
    except FileNotFoundError:
        pass
    except Exception as e:
        _logger.warning('[canonical_facts] invalidate disk failed: %s', e)


def get_canonical_facts() -> dict:
    """Return the canonical score-independent facts snapshot.

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
# Model score computation
# =========================================

def _compute_model_score_for_model(model_id: int) -> dict | None:
    """Compute per-condition curve-relative scores for model_id."""
    result: Dict[int, dict] = {}
    for cid in SCORE_CONDITION_IDS:
        with _cond_denom_lock:
            denom_entry = _cond_denom_cache.get(cid)
        if not denom_entry:
            continue

        raw_map = denom_entry.get('raw_score_by_model') or {}
        raw_score = raw_map.get(str(model_id))
        if raw_score is None:
            continue

        raw_best = denom_entry.get('raw_score_best')
        score_total: int | None = None
        try:
            raw_score_f = float(raw_score)
            raw_best_f = float(raw_best)
            # raw_score is avg(fan_pchip_airflow / baseline_airflow) over valid
            # per-fan 1 dB sampled points (no extrapolation), so both numerator
            # and denominator are positive and raw ratios should be > 0.
            # raw_best_f==0 (or non-finite) means this condition has no usable positive baseline ratio.
            if math.isfinite(raw_score_f) and math.isfinite(raw_best_f) and raw_best_f > 0:
                # Ratio-to-best normalization: best raw score => 100.
                norm = raw_score_f / raw_best_f
                score_total = round(max(0.0, min(1.0, norm)) * SCORE_MAX)
        except (TypeError, ValueError):
            score_total = None

        if score_total is None:
            continue

        points_map = denom_entry.get('sampled_points_used_by_model') or {}
        used_points = points_map.get(str(model_id), 0)

        result[cid] = {
            'score_total': score_total,
            'curve_raw_score': raw_score,
            'curve_valid_points': int(used_points or 0),
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


def _sync_compute_and_cache(model_id: int) -> dict | None:
    conditions = _compute_model_score_for_model(model_id)
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
    with _model_score_cache_lock:
        _model_score_cache[model_id] = entry
    return entry


def _extract_n2a_db_range(n2a: dict | None) -> tuple[float, float] | None:
    if not isinstance(n2a, dict):
        return None
    xs = n2a.get('x') or []
    if len(xs) < 2:
        return None
    try:
        min_db = float(xs[0])
        max_db = float(xs[-1])
    except Exception:
        return None
    # Reject degenerate single-point spans here because the downstream equal-airflow
    # sampling path expects a real interval with at least one valid coverage range.
    if not (math.isfinite(min_db) and math.isfinite(max_db) and min_db < max_db):
        return None
    return (min_db, max_db)


def _compute_curve_raw_score_for_n2a(denom_entry: dict, n2a: dict | None) -> tuple[float, int] | None:
    """Compute the raw score for a single model curve using the shared equal-airflow dB grid.

    The shared condition dB grid (cond_db_grid) was built by equal-airflow spacing over the
    baseline valid interval.  This model is only scored at grid points that fall within its
    own PCHIP coverage range (condition valid interval ∩ model's own coverage interval).

    When RAW_SCORE_ADAPTIVE_WEIGHTS is enabled each sample point is weighted by its per-point
    coverage ratio (cond_point_weights from the denom_entry), yielding a weighted mean.
    When disabled, an equal-weight mean is used instead.
    """
    baseline_fit = denom_entry.get('baseline_fit') or {}
    valid_db_min = denom_entry.get('valid_db_min')
    valid_db_max = denom_entry.get('valid_db_max')
    cond_db_grid = denom_entry.get('cond_db_grid') or []
    cond_point_weights = denom_entry.get('cond_point_weights') or []
    # Defensive: if weights are present they must be parallel to the grid; fall back to
    # equal weighting if lengths don't match (e.g. unexpected cache format).
    if cond_point_weights and len(cond_point_weights) != len(cond_db_grid):
        cond_point_weights = []
    db_range = _extract_n2a_db_range(n2a)
    if not (baseline_fit and cond_db_grid and db_range and valid_db_min is not None and valid_db_max is not None):
        return None

    fan_min_db, fan_max_db = db_range

    ratios = []
    weights: list[float] = []
    for i, db in enumerate(cond_db_grid):
        # Only score within the model's own coverage interval.
        if db < fan_min_db or db > fan_max_db:
            continue
        try:
            airflow = eval_pchip(n2a, db)
        except Exception:
            airflow = None
        if airflow is None or not math.isfinite(airflow) or airflow <= 0:
            continue
        base_af = _baseline_airflow_at_db(baseline_fit, db)
        if base_af is None:
            continue
        ratios.append(airflow / base_af)
        weights.append(cond_point_weights[i] if cond_point_weights else 1.0)

    if not ratios:
        return None

    raw_avg = _aggregate_ratios(ratios, weights)

    if not math.isfinite(raw_avg):
        return None
    return (float(raw_avg), len(ratios))


def _trigger_model_score_soft_refresh(model_id: int) -> None:
    key = model_id
    with _model_score_inflight_lock:
        if key in _model_score_inflight:
            return
        _model_score_inflight.add(key)

    def _refresh():
        try:
            _sync_compute_and_cache(model_id)
        finally:
            with _model_score_inflight_lock:
                _model_score_inflight.discard(key)

    threading.Thread(target=_refresh, daemon=True).start()


def _get_model_score_cached(model_id: int) -> dict | None:
    now = time.time()
    with _model_score_cache_lock:
        entry = _model_score_cache.get(model_id)

    if entry is not None:
        age = now - entry['cached_at']
        if age < MODEL_SCORE_CACHE_SOFT_TTL_SEC:
            return entry
        if age < MODEL_SCORE_CACHE_HARD_TTL_SEC:
            _trigger_model_score_soft_refresh(model_id)
            return entry

    _try_load_model_score_from_disk()
    with _model_score_cache_lock:
        entry = _model_score_cache.get(model_id)
    if entry is not None:
        age = now - entry['cached_at']
        if age < MODEL_SCORE_CACHE_SOFT_TTL_SEC:
            return entry
        if age < MODEL_SCORE_CACHE_HARD_TTL_SEC:
            _trigger_model_score_soft_refresh(model_id)
            return entry

    return _sync_compute_and_cache(model_id)


def _get_canonical_model_score(model_id: int) -> dict | None:
    return _get_model_score_cached(model_id)


def composite_score_sort_key(composite_score, model_id):
    """Unified composite-score ordering: score DESC, model_id DESC."""
    score = composite_score if composite_score is not None else float('-inf')
    return (
        -score,
        -int(model_id),
    )


# =========================================
# Rankings v2
# =========================================

def _build_rankings_v2() -> dict:
    """Compute model-centric rankings for Right Panel v2."""
    RADAR_CIDS = _RADAR_CIDS

    cf = get_canonical_facts()
    facts_lookup: dict[int, dict] = cf.get('model_lookup') or {}
    items = []
    for mid, facts in facts_lookup.items():
        if not _is_displayable_ranking_item(facts):
            continue
        condition_heat:  dict[int, int] = facts.get('condition_heat')  or {cid: 0 for cid in RADAR_CIDS}
        condition_likes: dict[int, int] = facts.get('condition_likes') or {cid: 0 for cid in RADAR_CIDS}
        total_query = facts.get('query_count', 0)
        total_like  = facts.get('like_count',  0)
        heat_score  = facts.get('heat_score',  0)

        model_score_entry = _get_canonical_model_score(mid)
        composite_score = None
        condition_scores: dict = {}
        if model_score_entry:
            composite_score = model_score_entry.get('composite_score')
            cond_scores = model_score_entry.get('conditions') or {}
            for cid in RADAR_CIDS:
                cd = cond_scores.get(cid)
                raw = cd.get('score_total') if cd else None
                try:
                    condition_scores[cid] = int(raw) if raw is not None else None
                except (TypeError, ValueError):
                    condition_scores[cid] = None
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
            'condition_heat':  condition_heat,
            'condition_likes': condition_likes,
        })

    limit = _RANKINGS_V2_DISPLAY_LIMIT

    return {
        'heat_board': _build_heat_board(items, limit),
        'performance_board': _build_performance_board(items, limit),
        'model_lookup': {item['model_id']: item for item in items},
    }


def _is_displayable_ranking_item(item: dict | None) -> bool:
    """Return True when a rankings item has enough display metadata for UI rendering."""
    if not isinstance(item, dict):
        return False
    brand = str(item.get('brand_name_zh') or '').strip()
    model = str(item.get('model_name') or '').strip()
    return bool(brand or model)


def _build_heat_board(items: list[dict], limit: int) -> list[dict]:
    heat_board = []
    for i, row in enumerate(
        sorted(items, key=lambda x: (-x['heat_score'], -x['query_count'], -x['model_id']))[:limit], 1
    ):
        heat_board.append({**row, 'rank': i})
    return heat_board


def _performance_sort_key_for_item(row: dict):
    return composite_score_sort_key(row.get('composite_score'), row['model_id'])


def _build_performance_board(items: list[dict], limit: int) -> list[dict]:
    performance_board = []
    prev_score = object()
    prev_rank = 0
    for i, row in enumerate(
        sorted(items, key=lambda x: _performance_sort_key_for_item(x))[:limit], 1
    ):
        score = row.get('composite_score')
        rank = prev_rank if (score is not None and prev_score is not None and score == prev_score) else i
        performance_board.append({**row, 'rank': rank})
        prev_score = score
        prev_rank = rank
    return performance_board


def _filter_non_displayable_rankings_result(
    result: dict,
) -> dict:
    """Strip non-displayable models from rankings boards/model_lookup to avoid blank rows."""
    model_lookup = (result or {}).get('model_lookup')
    if not isinstance(model_lookup, dict):
        return result

    filtered_lookup = {
        mid: item
        for mid, item in model_lookup.items()
        if _is_displayable_ranking_item(item)
    }
    if len(filtered_lookup) == len(model_lookup):
        return result

    filtered_items = list(filtered_lookup.values())
    limit = _RANKINGS_V2_DISPLAY_LIMIT

    return {
        'heat_board': _build_heat_board(filtered_items, limit),
        'performance_board': _build_performance_board(filtered_items, limit),
        'model_lookup': filtered_lookup,
    }


def _finalize_rankings_result(
    result: dict,
    trigger_source: str = '',
) -> dict:
    """Apply displayability filtering, then queue visibility-sync hints for missing scores."""
    filtered = _filter_non_displayable_rankings_result(result)
    queue_missing_score_visibility_sync_for_items(
        list((filtered.get('model_lookup') or {}).values()),
        trigger_source=trigger_source,
    )
    return filtered


def get_rankings_v2() -> dict:
    """Return model-centric rankings for Right Panel v2, with a 10-minute cache."""
    now = time.time()
    cache_key = 'data:canonical'

    with _rankings_v2_cache_lock:
        cached = _rankings_v2_cache.get(cache_key)
        if cached and (now - cached[0]) < _RANKINGS_V2_CACHE_TTL_SEC:
            result = cached[1]
            return _finalize_rankings_result(result, trigger_source='rankings_v2:mem')

    disk = _load_rankings_cache_from_disk()
    if disk is not None:
        disk_written_at, disk_result = disk
        with _rankings_v2_cache_lock:
            existing = _rankings_v2_cache.get(cache_key)
            if not existing or disk_written_at > existing[0]:
                _rankings_v2_cache[cache_key] = (disk_written_at, disk_result)
        _try_load_model_score_from_disk()
        return _finalize_rankings_result(disk_result, trigger_source='rankings_v2:disk')

    build_lock = _rankings_v2_build_lock
    with build_lock:
        with _rankings_v2_cache_lock:
            cached = _rankings_v2_cache.get(cache_key)
            if cached and (time.time() - cached[0]) < _RANKINGS_V2_CACHE_TTL_SEC:
                result = cached[1]
                return _finalize_rankings_result(result, trigger_source='rankings_v2:mem_locked')

        disk = _load_rankings_cache_from_disk()
        if disk is not None:
            disk_written_at, disk_result = disk
            with _rankings_v2_cache_lock:
                existing = _rankings_v2_cache.get(cache_key)
                if not existing or disk_written_at > existing[0]:
                    _rankings_v2_cache[cache_key] = (disk_written_at, disk_result)
            _try_load_model_score_from_disk()
            return _finalize_rankings_result(
                disk_result,
                trigger_source='rankings_v2:disk_locked',
            )

        result = _build_rankings_v2()
        computed_at = time.time()

        with _rankings_v2_cache_lock:
            existing = _rankings_v2_cache.get(cache_key)
            if not existing or computed_at > existing[0]:
                _rankings_v2_cache[cache_key] = (computed_at, result)

        _save_model_score_cache_to_disk()

        if result.get('model_lookup'):
            _save_rankings_cache(result)
        else:
            _logger.warning(
                '[rankings_v2] skipping disk persist: build returned empty '
                'model_lookup')

    return _finalize_rankings_result(result, trigger_source='rankings_v2:fresh')


def _invalidate_rankings_v2_cache(purge_disk: bool = True) -> None:
    with _rankings_v2_cache_lock:
        _rankings_v2_cache.pop('data:canonical', None)
    if not purge_disk:
        return
    try:
        os.remove(_rankings_cache_path())
    except FileNotFoundError:
        pass
    except Exception as e:
        _logger.warning('[rankings_v2] invalidate disk failed: %s', e)


def _get_ranked_lookup_with_fallback() -> dict:
    """Return model_lookup from the unified rankings cache."""
    try:
        result = get_rankings_v2()
        lookup = result.get('model_lookup') or {}
        if lookup:
            return lookup
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
    items = []
    for r in rows:
        try:
            mid = int(r['model_id'])
        except (TypeError, ValueError, KeyError):
            continue

        facts = facts_lookup.get(mid)
        heat_score     = facts.get('heat_score',     0) if facts else 0
        condition_heat = facts.get('condition_heat', {cid: 0 for cid in RADAR_CIDS}) if facts else {cid: 0 for cid in RADAR_CIDS}

        model_score_entry = _get_canonical_model_score(mid)
        composite_score  = None
        condition_scores = {cid: None for cid in RADAR_CIDS}
        if model_score_entry:
            cs = model_score_entry.get('composite_score')
            try:
                composite_score = int(cs) if cs is not None else None
            except (TypeError, ValueError):
                composite_score = None
            cond_scores = model_score_entry.get('conditions') or {}
            for cid in RADAR_CIDS:
                cd  = cond_scores.get(cid)
                raw = cd.get('score_total') if cd else None
                try:
                    condition_scores[cid] = int(raw) if raw is not None else None
                except (TypeError, ValueError):
                    condition_scores[cid] = None
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
            'condition_heat':   condition_heat,
            'update_date':     r.get('update_date') or '',
        })

    queue_missing_score_visibility_sync_for_items(
        items,
        trigger_source='recent_updates',
    )
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


def _do_denom_refresh():
    """Rebuild per-condition curve-relative baseline/scoring cache."""
    try:
        cid_list = list(SCORE_CONDITION_IDS)
        if not cid_list:
            return
        parts = ', '.join([f':c{i}' for i in range(len(cid_list))])
        params = {f'c{i}': cid for i, cid in enumerate(cid_list)}

        pair_rows = _fetch_all(
            f"SELECT DISTINCT model_id, condition_id FROM meta_view "
            f"WHERE condition_id IN ({parts})",
            params
        )
        pairs = [(int(r['model_id']), int(r['condition_id'])) for r in pair_rows]

        per_cid_n2a: Dict[int, list[tuple[int, dict, float, float]]] = defaultdict(list)
        if pairs:
            perf_map: Dict[str, Any] = {}
            try:
                perf_map = spectrum_reader.get_perf_models(pairs)
            except Exception as e:
                _logger.warning('[denom_refresh] pchip load/rebuild failed: %s', e)
            for mid, cid in pairs:
                mdl = perf_map.get(f'{mid}_{cid}')
                pchip_data = (mdl or {}).get('pchip') or {}
                n2a = pchip_data.get('noise_to_airflow')
                if not (n2a and isinstance(n2a, dict)):
                    continue
                xs = n2a.get('x') or []
                if len(xs) < 2:
                    continue
                try:
                    min_db = float(xs[0])
                    max_db = float(xs[-1])
                except Exception:
                    continue
                if not (math.isfinite(min_db) and math.isfinite(max_db) and min_db < max_db):
                    continue
                per_cid_n2a[cid].append((mid, n2a, min_db, max_db))

        now_t = time.time()
        new_cache: Dict[int, dict] = {}
        for cid in cid_list:
            models = per_cid_n2a.get(cid, [])
            baseline_fit = None
            sampled_median_points: list[tuple[float, float, int]] = []
            valid_db_min = None
            valid_db_max = None

            # Total curves participating in scoring for this condition.
            total_curve_count = len(models)

            # Dual-threshold MAX rule for both bounds.
            # required_coverage = max(min_count_threshold, ceil(min_ratio_threshold * N))
            # Lower bound: first dB point in the best segment with coverage >= lower_required.
            lower_required = max(
                BASELINE_VALID_LOWER_MIN_COUNT,
                math.ceil(BASELINE_VALID_LOWER_MIN_RATIO * total_curve_count)
            ) if total_curve_count > 0 else BASELINE_VALID_LOWER_MIN_COUNT
            # Upper bound: last dB point in the best segment with coverage >= upper_required.
            upper_required = max(
                BASELINE_VALID_UPPER_MIN_COUNT,
                math.ceil(BASELINE_VALID_UPPER_MIN_RATIO * total_curve_count)
            ) if total_curve_count > 0 else BASELINE_VALID_UPPER_MIN_COUNT

            # Candidate pool: include any point covered by at least min(lower, upper) curves so
            # that both bound scans can find valid points. Guard against an empty/zero
            # coverage threshold by clamping to >= 1.
            candidate_min = max(1, min(lower_required, upper_required))

            if models:
                global_min = min(m[2] for m in models)
                global_max = max(m[3] for m in models)
                db_grid = _build_unified_db_grid(global_min, global_max, BASELINE_DB_STEP)
                for db in db_grid:
                    vals = []
                    for _, n2a, min_db, max_db in models:
                        if db < min_db or db > max_db:
                            continue
                        try:
                            val = eval_pchip(n2a, db)
                        except Exception:
                            val = None
                        if val is not None and math.isfinite(val) and val > 0:
                            vals.append(float(val))
                    valid_value_count = len(vals)
                    if valid_value_count >= candidate_min:
                        med = _safe_median(vals)
                        if med is not None and med > 0:
                            sampled_median_points.append((db, med, valid_value_count))

            # Longest contiguous valid region.
            best_segment: list[tuple[float, float, int]] = []
            if sampled_median_points:
                cur_segment = [sampled_median_points[0]]
                for point in sampled_median_points[1:]:
                    prev_db = cur_segment[-1][0]
                    if abs(point[0] - prev_db) <= (BASELINE_DB_STEP * BASELINE_SEGMENT_CONTIGUITY_FACTOR):
                        cur_segment.append(point)
                    else:
                        if len(cur_segment) > len(best_segment):
                            best_segment = cur_segment
                        cur_segment = [point]
                if len(cur_segment) > len(best_segment):
                    best_segment = cur_segment

            if len(best_segment) >= BASELINE_MIN_SEGMENT_POINTS:
                # Upper bound (valid_db_max): scan from the END of the best segment for the
                # last dB point covered by >= upper_required curves.
                for db_pt, _med_pt, cov_pt in reversed(best_segment):
                    if cov_pt >= upper_required:
                        valid_db_max = float(db_pt)
                        break
                # Lower bound (valid_db_min): scan from the START of the best segment for
                # the first dB point covered by >= lower_required curves.
                for db_pt, _med_pt, cov_pt in best_segment:
                    if cov_pt >= lower_required:
                        valid_db_min = float(db_pt)
                        break
                if valid_db_min is None or valid_db_max is None or valid_db_min >= valid_db_max:
                    # No point satisfies one or both bound thresholds, or the resulting
                    # interval has zero/negative width; the entire interval is invalid for
                    # this condition.
                    valid_db_min = None
                    valid_db_max = None
                baseline_fit = _fit_exponential_baseline([(db, med) for db, med, _ in best_segment])
                if baseline_fit is None:
                    valid_db_min = None
                    valid_db_max = None

            # Build the per-condition equal-airflow dB grid.
            # 1. Compute baseline airflow at valid_db_min and valid_db_max.
            # 2. Generate RAW_SCORE_AIRFLOW_SAMPLE_COUNT equally spaced airflow values.
            # 3. Map each airflow value back to dB via the inverted exponential baseline.
            # This is the shared sampling grid used for every model in this condition.
            cond_db_grid: list[float] = []
            if baseline_fit and valid_db_min is not None and valid_db_max is not None:
                cond_db_grid = _build_equal_airflow_db_grid(
                    baseline_fit, valid_db_min, valid_db_max, RAW_SCORE_AIRFLOW_SAMPLE_COUNT
                )
                if not cond_db_grid:
                    # Grid construction failed; invalidate the interval for this condition.
                    valid_db_min = None
                    valid_db_max = None
                    baseline_fit = None

            # Compute per-point coverage weights for adaptive weighting.
            # weight[i] = (# models whose coverage range includes cond_db_grid[i]) / N
            cond_point_weights: list[float] = []
            if cond_db_grid and total_curve_count > 0:
                for db_pt in cond_db_grid:
                    count = sum(
                        1 for _, _, fan_min_db, fan_max_db in models
                        if fan_min_db <= db_pt <= fan_max_db
                    )
                    cond_point_weights.append(count / total_curve_count)

            raw_score_by_model: Dict[str, float] = {}
            sampled_points_used_by_model: Dict[str, int] = {}
            if baseline_fit and cond_db_grid:
                # Sample every model's PCHIP curve on the shared equal-airflow dB grid and
                # compute per-point airflow ratios relative to the baseline.
                # A model scores only at grid points within its own coverage interval
                # (condition valid interval ∩ model's own coverage interval).
                for mid, n2a, fan_min_db, fan_max_db in models:
                    ratios: list[float] = []
                    pt_weights: list[float] = []
                    for i, db in enumerate(cond_db_grid):
                        # Restrict to the model's own coverage interval.
                        if db < fan_min_db or db > fan_max_db:
                            continue
                        try:
                            airflow = eval_pchip(n2a, db)
                        except Exception:
                            airflow = None
                        if airflow is None or not math.isfinite(airflow) or airflow <= 0:
                            continue
                        base_af = _baseline_airflow_at_db(baseline_fit, db)
                        if base_af is None:
                            continue
                        ratios.append(airflow / base_af)
                        pt_weights.append(cond_point_weights[i] if cond_point_weights else 1.0)
                    if ratios:
                        raw_avg = _aggregate_ratios(ratios, pt_weights)
                        if math.isfinite(raw_avg):
                            raw_score_by_model[str(mid)] = float(raw_avg)
                            sampled_points_used_by_model[str(mid)] = len(ratios)

            raw_values = [v for v in raw_score_by_model.values() if math.isfinite(v)]
            # None means no model had valid equal-airflow sampled PCHIP points for this condition.
            raw_score_best = max(raw_values) if raw_values else None

            entry = {
                'valid_db_min': valid_db_min,
                'valid_db_max': valid_db_max,
                'baseline_fit': baseline_fit or {},
                'cond_db_grid': cond_db_grid,
                'cond_point_weights': cond_point_weights,
                'median_sample_count': len(best_segment),
                'normalization_mode': 'ratio_to_best',
                'raw_score_sampling': 'equal_airflow',
                'raw_score_best': raw_score_best,
                'raw_score_by_model': raw_score_by_model,
                'sampled_points_used_by_model': sampled_points_used_by_model,
                'cached_at': now_t,
            }
            new_cache[cid] = entry

        # Swap in the fully-built snapshot under a short lock.
        with _cond_denom_lock:
            _cond_denom_cache.clear()
            _cond_denom_cache.update(new_cache)

        if _app_debug:
            _logger.debug(
                '[denom_refresh] curve-relative refresh completed for %d conditions',
                len(cid_list)
            )

        with _model_score_cache_lock:
            _model_score_cache.clear()

        _invalidate_model_score_disk_cache()

        _save_denom_cache_to_disk()
    except Exception as e:
        _logger.warning('[denom_refresh] error: %s', e)


_refresh_visibility_scoring_caches_inflight = threading.Lock()
_refresh_visibility_scoring_caches_pending = threading.Event()
_visible_model_set_watch_wakeup = threading.Event()
_visible_model_set_snapshot: set[int] | None = None
_visible_model_set_snapshot_lock = threading.Lock()
_visible_model_set_sync_lock = threading.Lock()
# Rate-limit repeated read-path visibility-sync hints per model so repeated misses
# do not spawn redundant visibility-sync hint requests on every request.
_SINGLE_MODEL_SCORE_VISIBILITY_SYNC_HINT_RETRY_SEC = 60
_single_model_score_visibility_sync_hint_inflight: set[int] = set()
_single_model_score_visibility_sync_hint_lock = threading.Lock()
_single_model_score_visibility_sync_hint_last_attempt: dict[int, float] = {}
_visibility_sync_hint_inflight = threading.Lock()
_visibility_sync_hint_pending = threading.Event()
_visibility_sync_hint_forced_model_ids: set[int] = set()
_visibility_sync_hint_forced_model_ids_lock = threading.Lock()


def _run_heavy_visibility_refresh() -> None:
    _do_denom_refresh()
    _invalidate_canonical_facts_cache(purge_disk=True)
    _invalidate_rankings_v2_cache(purge_disk=True)
    _warmup_rankings_async()


def _fetch_current_visible_model_ids() -> set[int]:
    rows = _fetch_all("SELECT model_id FROM available_models_info_view")
    visible: set[int] = set()
    for row in rows or []:
        try:
            mid = int(row.get('model_id') if isinstance(row, dict) else row['model_id'])
        except Exception:
            continue
        if mid > 0:
            visible.add(mid)
    return visible


def _recompute_raw_score_best_for_entry(denom_entry: dict) -> bool:
    raw_map = denom_entry.setdefault('raw_score_by_model', {})
    best_val = None
    for raw_score in raw_map.values():
        try:
            score_f = float(raw_score)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(score_f):
            continue
        if best_val is None or score_f > best_val:
            best_val = score_f

    before_best = denom_entry.get('raw_score_best')
    denom_entry['raw_score_best'] = best_val
    return before_best != best_val


def _collect_model_condition_pairs(model_ids: list[int]) -> list[tuple[int, int]]:
    mids = []
    for raw_mid in model_ids:
        try:
            mid = int(raw_mid)
        except (TypeError, ValueError):
            continue
        if mid > 0:
            mids.append(mid)
    if not mids:
        return []

    mid_placeholders = ', '.join(f':m{i}' for i in range(len(mids)))
    cid_list = list(SCORE_CONDITION_IDS)
    cid_placeholders = ', '.join(f':c{i}' for i in range(len(cid_list)))
    params = {f'm{i}': mid for i, mid in enumerate(mids)}
    params.update({f'c{i}': cid for i, cid in enumerate(cid_list)})
    rows = _fetch_all(
        f"""
            SELECT DISTINCT model_id, condition_id
            FROM meta_view
            WHERE model_id IN ({mid_placeholders})
              AND condition_id IN ({cid_placeholders})
        """,
        params,
    )
    pairs: list[tuple[int, int]] = []
    for row in rows or []:
        try:
            mid = int(row['model_id'])
            cid = int(row['condition_id'])
        except Exception:
            continue
        if cid in SCORE_CONDITION_IDS:
            pairs.append((mid, cid))
    return pairs


def _apply_incremental_visibility_diff(added_model_ids: list[int], removed_model_ids: list[int]) -> bool:
    changed_model_ids: set[int] = set()
    denom_changed = False
    raw_best_changed = False

    updates_by_pair: dict[tuple[int, int], tuple[float, int]] = {}
    if added_model_ids:
        pairs = _collect_model_condition_pairs(added_model_ids)
        perf_map: dict[str, Any] = {}
        if pairs:
            try:
                perf_map = spectrum_reader.build_performance_pchips(pairs)
            except Exception as e:
                _logger.warning('[visibility_sync] pchip rebuild failed for incremental add: %s', e)
            for mid, cid in pairs:
                perf = perf_map.get(f'{mid}_{cid}') or {}
                n2a = ((perf.get('pchip') or {}).get('noise_to_airflow'))
                with _cond_denom_lock:
                    denom_entry = _cond_denom_cache.get(cid)
                if not denom_entry:
                    continue
                score_data = _compute_curve_raw_score_for_n2a(denom_entry, n2a)
                if score_data is not None:
                    updates_by_pair[(mid, cid)] = score_data

    with _cond_denom_lock:
        for (mid, cid), (raw_avg, used_points) in updates_by_pair.items():
            denom_entry = _cond_denom_cache.get(cid)
            if not denom_entry:
                continue
            raw_map = denom_entry.setdefault('raw_score_by_model', {})
            points_map = denom_entry.setdefault('sampled_points_used_by_model', {})
            str_mid = str(mid)
            prev_raw = raw_map.get(str_mid)
            prev_points = points_map.get(str_mid)
            if prev_raw != raw_avg or prev_points != used_points:
                denom_changed = True
                changed_model_ids.add(mid)
            raw_map[str_mid] = float(raw_avg)
            points_map[str_mid] = int(used_points)

            current_best = denom_entry.get('raw_score_best')
            try:
                current_best_f = float(current_best) if current_best is not None else None
            except (TypeError, ValueError):
                current_best_f = None
            should_update_best = current_best_f is None or raw_avg > current_best_f
            if should_update_best:
                denom_entry['raw_score_best'] = float(raw_avg)
                raw_best_changed = True

        for raw_mid in removed_model_ids:
            try:
                mid = int(raw_mid)
            except (TypeError, ValueError):
                continue
            if mid <= 0:
                continue
            str_mid = str(mid)
            for cid, denom_entry in _cond_denom_cache.items():
                raw_map = denom_entry.setdefault('raw_score_by_model', {})
                points_map = denom_entry.setdefault('sampled_points_used_by_model', {})
                removed = False
                if str_mid in raw_map:
                    removed_raw = raw_map.pop(str_mid, None)
                    removed = True
                else:
                    removed_raw = None
                if str_mid in points_map:
                    points_map.pop(str_mid, None)
                if removed:
                    denom_changed = True
                    changed_model_ids.add(mid)
                    need_recompute_best = False
                    if removed_raw is not None:
                        raw_best = denom_entry.get('raw_score_best')
                        try:
                            removed_raw_f = float(removed_raw)
                            raw_best_f = float(raw_best) if raw_best is not None else None
                        except (TypeError, ValueError):
                            removed_raw_f = None
                            raw_best_f = None
                        if (
                            removed_raw_f is not None
                            and raw_best_f is not None
                            and math.isfinite(removed_raw_f)
                            and math.isfinite(raw_best_f)
                            and removed_raw_f == raw_best_f
                        ):
                            need_recompute_best = True
                    if need_recompute_best and _recompute_raw_score_best_for_entry(denom_entry):
                        raw_best_changed = True

    if not denom_changed:
        return False

    try:
        _save_denom_cache_to_disk()
    except Exception as e:
        _logger.warning('[visibility_sync] failed to persist denom cache after incremental diff: %s', e)

    if raw_best_changed:
        with _model_score_cache_lock:
            _model_score_cache.clear()
        _invalidate_model_score_disk_cache()
    else:
        _invalidate_model_score_cache_entries(list(changed_model_ids))

    models_to_sync = changed_model_ids.intersection(set(added_model_ids))
    for mid in models_to_sync:
        try:
            _sync_compute_and_cache(mid)
        except Exception as e:
            _logger.warning('[visibility_sync] model score recompute failed for model_id=%s: %s', mid, e)

    _invalidate_canonical_facts_cache(purge_disk=True)
    _invalidate_rankings_v2_cache(purge_disk=True)
    _warmup_rankings_async()
    return True


def _sync_visible_model_set_once(
    source: str = 'watcher',
    allow_bootstrap_without_sync: bool = True,
    forced_model_ids: list[int] | None = None,
) -> None:
    global _visible_model_set_snapshot
    with _visible_model_set_sync_lock:
        try:
            current_visible_ids = _fetch_current_visible_model_ids()
        except Exception as e:
            _logger.warning('[visibility_sync] failed to query visible model ids (%s): %s', source, e)
            return

        with _visible_model_set_snapshot_lock:
            previous_visible_ids = _visible_model_set_snapshot
            if previous_visible_ids is None:
                _visible_model_set_snapshot = set(current_visible_ids)
                previous_visible_ids = None
            else:
                previous_visible_ids = set(previous_visible_ids)
                _visible_model_set_snapshot = set(current_visible_ids)

        if previous_visible_ids is None:
            if allow_bootstrap_without_sync:
                if _app_debug:
                    _logger.debug('[visibility_sync] initialized visible-set snapshot (%s): %d models',
                                  source, len(current_visible_ids))
                return
            _run_heavy_visibility_refresh()
            return

        added = sorted(current_visible_ids - previous_visible_ids)
        removed = sorted(previous_visible_ids - current_visible_ids)
        if forced_model_ids:
            forced_set = set()
            for raw_mid in forced_model_ids:
                try:
                    mid = int(raw_mid)
                except (TypeError, ValueError):
                    continue
                if mid > 0:
                    forced_set.add(mid)
            forced_only = sorted(forced_set.difference(added).difference(removed))
            if forced_only:
                added.extend(forced_only)
        changed_count = len(added) + len(removed)
        if changed_count == 0:
            if forced_model_ids:
                try:
                    _apply_incremental_visibility_diff(forced_model_ids, [])
                except Exception as e:
                    _logger.warning(
                        '[visibility_sync] forced-model incremental sync failed (%s): %s',
                        source,
                        e,
                    )
            return

        if changed_count > _VISIBILITY_DIFF_INCREMENTAL_MAX:
            if _app_debug:
                _logger.debug(
                    '[visibility_sync] large diff (%s): added=%d removed=%d -> heavy refresh',
                    source,
                    len(added),
                    len(removed),
                )
            _run_heavy_visibility_refresh()
            return

        try:
            applied = _apply_incremental_visibility_diff(added, removed)
        except Exception as e:
            _logger.warning('[visibility_sync] incremental diff failed (%s): %s - falling back to heavy refresh', source, e)
            _run_heavy_visibility_refresh()
            return

        if not applied:
            _invalidate_canonical_facts_cache(purge_disk=True)
            _invalidate_rankings_v2_cache(purge_disk=True)
            _warmup_rankings_async()


def refresh_visibility_scoring_caches() -> None:
    """Request immediate visible-set synchronization via the watcher/coordinator path."""
    _visible_model_set_watch_wakeup.set()
    if not _refresh_visibility_scoring_caches_inflight.acquire(blocking=False):
        _refresh_visibility_scoring_caches_pending.set()
        if _app_debug:
            _logger.debug('[visibility_sync] sync already in flight; coalescing request')
        return

    try:
        while True:
            _refresh_visibility_scoring_caches_pending.clear()
            with _visibility_sync_hint_forced_model_ids_lock:
                forced_model_ids = sorted(_visibility_sync_hint_forced_model_ids)
                _visibility_sync_hint_forced_model_ids.clear()
            _sync_visible_model_set_once(
                source='manual',
                allow_bootstrap_without_sync=False,
                forced_model_ids=forced_model_ids,
            )

            if not _refresh_visibility_scoring_caches_pending.is_set():
                break

            if _app_debug:
                _logger.debug('[visibility_sync] processing coalesced sync request')
    finally:
        _refresh_visibility_scoring_caches_inflight.release()


def _invalidate_model_score_cache_entries(model_ids: list[int] | tuple[int, ...]) -> None:
    mids = set()
    for raw_mid in model_ids:
        try:
            mid = int(raw_mid)
        except (TypeError, ValueError):
            continue
        if mid > 0:
            mids.add(mid)
    if not mids:
        return
    with _model_score_cache_lock:
        for mid in mids:
            _model_score_cache.pop(mid, None)
    _invalidate_model_score_disk_cache()


def queue_visibility_sync_hint(model_id: int, trigger_source: str = '') -> bool:
    try:
        model_id = int(model_id)
    except (TypeError, ValueError):
        return False
    if model_id <= 0:
        return False

    now = time.time()
    with _single_model_score_visibility_sync_hint_lock:
        last_attempt = _single_model_score_visibility_sync_hint_last_attempt.get(model_id)
        if last_attempt is not None and (now - last_attempt) < _SINGLE_MODEL_SCORE_VISIBILITY_SYNC_HINT_RETRY_SEC:
            return False
        if model_id in _single_model_score_visibility_sync_hint_inflight:
            return False
        _single_model_score_visibility_sync_hint_inflight.add(model_id)
        _single_model_score_visibility_sync_hint_last_attempt[model_id] = now

    with _visibility_sync_hint_forced_model_ids_lock:
        _visibility_sync_hint_forced_model_ids.add(model_id)

    _visibility_sync_hint_pending.set()
    if not _visibility_sync_hint_inflight.acquire(blocking=False):
        with _single_model_score_visibility_sync_hint_lock:
            _single_model_score_visibility_sync_hint_inflight.discard(model_id)
        return True

    def _run():
        try:
            while True:
                _visibility_sync_hint_pending.clear()
                refresh_visibility_scoring_caches()
                if _app_debug:
                    _logger.debug(
                        '[visibility_sync] queued by read-path miss model_id=%s source=%s',
                        model_id,
                        trigger_source,
                    )
                if not _visibility_sync_hint_pending.is_set():
                    break
        finally:
            _visibility_sync_hint_inflight.release()
            with _single_model_score_visibility_sync_hint_lock:
                _single_model_score_visibility_sync_hint_inflight.discard(model_id)

    threading.Thread(target=_run, daemon=True, name=f'visibility-sync-hint-{model_id}').start()
    return True


def _item_missing_requested_score(item: dict) -> bool:
    if not isinstance(item, dict):
        return False
    return item.get('composite_score') is None


def queue_missing_score_visibility_sync_for_items(
    items: list[dict],
    trigger_source: str = '',
) -> int:
    queued = 0
    for item in items or []:
        try:
            model_id = int(item.get('model_id') or 0)
        except Exception:
            continue
        if not model_id or not _item_missing_requested_score(item):
            continue
        if queue_visibility_sync_hint(model_id, trigger_source=trigger_source):
            queued += 1
    return queued

def _warmup_rankings_async() -> None:
    """Launch a background thread that pre-builds canonical score rankings."""
    if os.getenv('DISABLE_COND_STATS_CRON'):
        if _app_debug:
            _logger.debug('[warmup] rankings warm-up skipped because DISABLE_COND_STATS_CRON is set')
        return
    if not _warmup_rankings_inflight.acquire(blocking=False):
        return

    def _run():
        try:
            try:
                _invalidate_canonical_facts_cache(purge_disk=True)
                get_canonical_facts()
                if _app_debug:
                    _logger.debug('[warmup] canonical facts warmed (pid=%s)', os.getpid())
            except Exception as _cfe:
                _logger.warning('[warmup] canonical facts warm-up failed: %s', _cfe)

            try:
                _invalidate_rankings_v2_cache(purge_disk=True)
                get_rankings_v2()
                if _app_debug:
                    _logger.debug('[warmup] rankings warmed (pid=%s)', os.getpid())
            except Exception as _e:
                _logger.warning('[warmup] rankings warm-up failed: %s', _e)
        finally:
            _warmup_rankings_inflight.release()

    threading.Thread(target=_run, daemon=True, name='rankings-warmup').start()


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
        loaded_from_disk = _load_denom_cache_from_disk()
        if not loaded_from_disk:
            for retry_sleep in _DENOM_STARTUP_CACHE_RETRY_INTERVALS_SEC:
                time.sleep(retry_sleep)
                if _app_debug:
                    _logger.debug('[denom_refresh] startup cache re-check after %ss wait (pid=%s)',
                                  retry_sleep, os.getpid())
                loaded_from_disk = _load_denom_cache_from_disk()
                if loaded_from_disk:
                    break
        if not loaded_from_disk:
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


def _visible_model_set_watch_loop():
    """Periodically poll visible-model IDs and sync scoring/rankings for visibility diffs."""
    while True:
        _visible_model_set_watch_wakeup.clear()
        try:
            _sync_visible_model_set_once(source='watcher', allow_bootstrap_without_sync=True)
        except Exception as e:
            _logger.warning('[visibility_sync] watcher tick failed: %s', e)
        _visible_model_set_watch_wakeup.wait(_VISIBLE_MODEL_SET_WATCH_INTERVAL_SEC)


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
    """Start scoring-system background daemon threads.

    Call once during app startup after ``setup()`` has been called.
    """
    if _fetch_all is None or _exec_write is None or _logger is None:
        raise RuntimeError(
            "start_background_threads() called before setup(); "
            "missing injected dependencies"
        )
    if not os.getenv('DISABLE_COND_STATS_CRON'):
        threading.Thread(target=_denom_refresh_loop, daemon=True).start()
        threading.Thread(target=_visible_model_set_watch_loop, daemon=True).start()
