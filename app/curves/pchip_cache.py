import os
import json
import math
import hashlib
import threading
import tempfile
import logging
from collections import OrderedDict
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

# Setup logger for this module
_logger = logging.getLogger(__name__)

# =========================
# Interpolation contract / tuning controls
# =========================

_CODE_VERSION = os.getenv("CODE_VERSION", "")  # 纳入统一 env-key 用于失效
_INTERP_CONTRACT = "joint_strict_anchors_conservative_pchip_v5"
_PERF_SCHEMA_VERSION = 3  # v3: per-row DB noise_sone is part of raw/hash/models
_SONE_SELECTION_VERSION = 1
_ANCHOR_SELECTION_VERSION = 1

# Centralized interpolation tuning (in-code only; not env driven):
# - sharp-step detection based on adjacent secant slope ratio
# - local derivative damping near abrupt slope changes
# - per-segment derivative caps (tighter for sharp-turn regions) to prevent post-step overshoot
_NUMERIC_STABILITY_EPS = 1e-12
_SHARP_STEP_SLOPE_RATIO_TRIGGER = 2.0
_SHARP_STEP_DERIVATIVE_DAMPING = 1.0
_SEGMENT_DERIVATIVE_CAP_SMOOTH = 3.5
_SEGMENT_DERIVATIVE_CAP_SHARP = 1.1


def perf_interp_contract() -> str:
    return _INTERP_CONTRACT

def reload_curve_params_from_env():
    # Legacy compatibility hook.
    # Global alpha/tau smoothing knobs are retired from the interpolation contract.
    # Runtime env toggles that remain effective (monotone/node-lock/cache knobs) are read
    # on demand and do not require explicit reload here.
    _logger.warning(
        "reload_curve_params_from_env() is a legacy no-op: interpolation tuning "
        "knobs are no longer reloaded from env at runtime; remaining env-driven "
        "controls are read on demand and do not require explicit reload"
    )
    return None

def _axis_norm(axis: str) -> str:
    return "noise_db" if axis == "noise" else axis

def _env_bool(name: str, default: str = "1") -> bool:
    return (os.getenv(name, default) or "").strip() in ("1", "true", "True", "YES", "yes")

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

def _env_monotone_enable(axis: str) -> bool:
    ax = _axis_norm(axis)
    key = "CURVE_MONOTONE_ENABLE_RPM" if ax == "rpm" else "CURVE_MONOTONE_ENABLE_NOISE"
    default = "1"
    return _env_bool(key, default)

def _env_node_lock(axis: str) -> bool:
    ax = _axis_norm(axis)
    key = "CURVE_NODE_LOCK_RPM" if ax == "rpm" else "CURVE_NODE_LOCK_NOISE"
    return _env_bool(key, "0")

def curve_cache_dir() -> str:
    d = os.getenv("CURVE_CACHE_DIR", "./curve_cache")
    os.makedirs(d, exist_ok=True)
    return d

def _check_spectrum_supports_audio(model_id: int, condition_id: int) -> bool:
    """
    检查对应的频谱模型缓存是否支持音频生成。
    Check if the corresponding spectrum model cache supports audio generation.
    
    通过检查 spectrum cache 中是否同时存在 sweep_frame_index 和 sweep_audio_meta 来判断。
    Returns True if both sweep_frame_index and sweep_audio_meta are present in the cached spectrum model.
    
    Args:
        model_id: 模型 ID
        condition_id: 工况 ID
        
    Returns:
        bool: True if audio is supported, False otherwise (including when cache is missing/malformed)
    """
    try:
        # Import spectrum_cache here to avoid circular dependency
        # This is acceptable as the function is not called in hot paths
        from app.audio_services import spectrum_cache
        
        # Try to load the spectrum cache
        cached = spectrum_cache.load(model_id, condition_id)
        if not cached or not isinstance(cached, dict):
            return False
        
        # Get the model from the cache
        model = cached.get('model')
        if not model or not isinstance(model, dict):
            return False
        
        # Check if both sweep_frame_index and sweep_audio_meta are present
        sweep_frame_index = model.get('sweep_frame_index')
        sweep_audio_meta = model.get('sweep_audio_meta')
        
        has_frame_index = (
            sweep_frame_index is not None 
            and isinstance(sweep_frame_index, list) 
            and len(sweep_frame_index) > 0
        )
        has_audio_meta = (
            sweep_audio_meta is not None 
            and isinstance(sweep_audio_meta, dict) 
            and len(sweep_audio_meta) > 0
        )
        
        return has_frame_index and has_audio_meta
        
    except (ImportError, FileNotFoundError, KeyError, TypeError, AttributeError) as e:
        # Expected errors when spectrum cache is missing or malformed
        _logger.debug("Spectrum cache check failed for model=%s, condition=%s: %s", 
                     model_id, condition_id, str(e))
        return False
    except Exception as e:
        # Unexpected errors should be logged for debugging
        _logger.warning("Unexpected error checking spectrum cache for model=%s, condition=%s: %s", 
                       model_id, condition_id, str(e), exc_info=True)
        return False

def _env_inmem_enable() -> bool:
    return _env_bool("CURVE_CACHE_INMEM_ENABLE", "1")

def _env_inmem_max_models() -> int:
    return max(0, _env_int("CURVE_CACHE_INMEM_MAX_MODELS", 2000))

def _env_inmem_max_points() -> int:
    return max(0, _env_int("CURVE_CACHE_INMEM_MAX_POINTS", 200000))

def _env_inmem_admit_hits() -> int:
    return max(1, _env_int("CURVE_CACHE_INMEM_ADMIT_HITS", 2))

def _env_inmem_hits_window() -> int:
    return max(512, _env_int("CURVE_CACHE_INMEM_HITS_WINDOW", 4096))

# =========================
# In-Mem LRU（兼容旧逻辑）
# =========================

class _InMemLRU:
    def __init__(self, max_models: int, max_points: int):
        self.max_models = int(max_models)
        self.max_points = int(max_points)
        self._lock = threading.Lock()
        self._map: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        self._points_sum = 0

    def _weight(self, model: Dict[str, Any]) -> int:
        """按样条节点数估重；四合一模型统计 4 条曲线的点数总和。"""
        try:
            if not model: return 0
            if model.get("type") == "perf_pchip_v1":
                p = (model.get("pchip") or {})
                total = 0
                for k in ("rpm_to_airflow","rpm_to_noise_db","noise_to_rpm","noise_to_airflow",
                          "rpm_to_sone","sone_to_rpm","sone_to_airflow"):
                    m = p.get(k)
                    if m and isinstance(m, dict):
                        total += int(len(m.get("x") or []))
                return total
            # 兜底：当存入的是单条 pchip（不推荐），按其 x 长度估重
            return int(len(model.get("x", []) or []))
        except Exception:
            return 0

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            m = self._map.get(key)
            if m is None:
                return None
            self._map.move_to_end(key, last=True)
            return m

    def put(self, key: str, model: Dict[str, Any]):
        if self.max_models <= 0 or self.max_points <= 0:
            return
        w = self._weight(model)
        with self._lock:
            old = self._map.pop(key, None)
            if old is not None:
                self._points_sum -= self._weight(old)
            self._map[key] = model
            self._points_sum += w
            while (len(self._map) > self.max_models) or (self._points_sum > self.max_points):
                k, v = self._map.popitem(last=False)
                self._points_sum -= self._weight(v)

_INMEM = _InMemLRU(_env_inmem_max_models(), _env_inmem_max_points()) if _env_inmem_enable() else None
_ADMIT_HITS = _env_inmem_admit_hits()
_HITS_WINDOW = _env_inmem_hits_window()
_HITS: Dict[str, int] = {}
_HITS_LOCK = threading.Lock()

def _note_hit(key: str) -> int:
    if not _INMEM or _ADMIT_HITS <= 1:
        return _ADMIT_HITS
    with _HITS_LOCK:
        cnt = _HITS.get(key, 0) + 1
        _HITS[key] = cnt
        if len(_HITS) > _HITS_WINDOW:
            n_purge = max(1, _HITS_WINDOW // 10)
            for i, k in enumerate(list(_HITS.keys())):
                _HITS.pop(k, None)
                if i + 1 >= n_purge:
                    break
        return cnt

# =========================
# 通用散列与轴向 PCHIP 构建
# =========================

def raw_points_hash(xs: List[float], ys: List[float]) -> str:
    """仍保留（内部用），对 (x,y) 对的顺序无关散列。"""
    pairs = sorted([(float(x), float(y)) for x, y in zip(xs, ys)])
    buf = ";".join(f"{x:.6f}|{y:.6f}" for x, y in pairs)
    return hashlib.sha1(buf.encode("utf-8")).hexdigest()

def raw_triples_hash(rpm: List[float], airflow: List[float], noise: List[float]) -> str:
    """对三轴点的顺序无关散列，None 以 'null' 表示，统一到 6 位小数。"""
    triples: List[Tuple[str,str,str]] = []
    n = min(len(airflow or []), max(len(rpm or []), len(noise or [])))
    for i in range(n):
        def norm(v):
            if v is None: return "null"
            try:
                f = float(v)
                if not math.isfinite(f): return "null"
                return f"{f:.6f}"
            except Exception:
                return "null"
        triples.append((norm(rpm[i] if i < len(rpm) else None),
                        norm(airflow[i] if i < len(airflow) else None),
                        norm(noise[i] if i < len(noise) else None)))
    triples.sort()
    buf = ";".join("|".join(t) for t in triples)
    return hashlib.sha1(buf.encode("utf-8")).hexdigest()

def raw_quads_hash(rpm: List[float], airflow: List[float], noise: List[float], pressure: List[float],
                   sone: Optional[List[float]] = None) -> str:
    """Hash original precision, row order, missing values and array lengths.

    The per-row DB ``noise_sone`` array is appended whenever any row has a
    non-NULL value, so a sone-only DB change is detected by TTL / admin
    rebuilds; all-NULL sone hashes like the base rows (no sone models either way).
    """
    arrays = [rpm or [], airflow or [], noise or [], pressure or []]
    if any(v is not None for v in (sone or [])):
        arrays.append(list(sone))
    buf = json.dumps(arrays, ensure_ascii=False, separators=(",", ":"), default=str)
    return hashlib.sha1(buf.encode("utf-8")).hexdigest()

def select_joint_anchors(rpm: List, airflow: List, noise: List,
                         pressure: Optional[List] = None) -> Dict[str, Any]:
    """Select the longest chain, then span, reversed RPMs, and stable indices."""
    arrays = (rpm or [], airflow or [], noise or [])
    records = []
    groups: Dict[float, List[Tuple[int, Tuple[float, ...]]]] = {}
    for i in range(max(*(len(a) for a in arrays), len(pressure or []))):
        if any(i >= len(a) or a[i] is None for a in arrays):
            records.append({"included": False, "reason": "missing"})
            continue
        try:
            triple = tuple(float(a[i]) for a in arrays)
            valid = (all(math.isfinite(v) for v in triple)
                     and triple[0] > 0 and triple[1] > 0)
        except (TypeError, ValueError, OverflowError):
            valid = False
        records.append({"included": False, "reason": "monotonic_conflict" if valid else "invalid"})
        if valid:
            groups.setdefault(triple[0], []).append((i, triple))

    candidates = []
    for group in groups.values():
        if any(triple != group[0][1] for _, triple in group):
            for i, _ in group:
                records[i]["reason"] = "rpm_conflict"
        else:
            candidates.append(group[0])
            for i, _ in group[1:]:
                records[i]["reason"] = "duplicate"
    candidates.sort(key=lambda row: (row[1][0], row[0]))

    def quality(chain):
        rpms = tuple(candidates[j][1][0] for j in chain)
        return (len(chain), rpms[-1] - rpms[0], tuple(reversed(rpms)),
                tuple(-candidates[j][0] for j in chain))

    chains = []
    extension_keys = []
    for j, (_, triple) in enumerate(candidates):
        predecessor = None
        for k in range(j):
            if all(a < b for a, b in zip(candidates[k][1], triple)):
                if predecessor is None or extension_keys[k] > extension_keys[predecessor]:
                    predecessor = k
        best = chains[predecessor] + (j,) if predecessor is not None else (j,)
        chains.append(best)
        key = quality(best)
        extension_keys.append((key[0], -candidates[best[0]][1][0], key[2], key[3]))
    best = max(chains, key=quality) if chains else ()
    indices = [candidates[j][0] for j in best]
    for i in indices:
        records[i] = {"included": True, "reason": "included"}
    return {"version": _ANCHOR_SELECTION_VERSION, "indices": indices, "records": records}

def _pava_isotonic_non_decreasing(ys: List[float]) -> List[float]:
    n = len(ys)
    if n <= 1:
        return ys[:]
    y = [float(v) for v in ys]
    level = y[:]
    weight = [1.0] * n
    i = 0
    curr_n = n
    while i < curr_n - 1:
        if level[i] > level[i + 1]:
            w = weight[i] + weight[i + 1]
            v = (level[i] * weight[i] + level[i + 1] * weight[i + 1]) / w
            level[i] = v
            weight[i] = w
            j = i
            while j > 0 and level[j - 1] > level[j]:
                w2 = weight[j - 1] + weight[j]
                v2 = (level[j - 1] * weight[j - 1] + level[j] * weight[j]) / w2
                level[j - 1] = v2
                weight[j - 1] = w2
                for k in range(j, curr_n - 1):
                    level[k] = level[k + 1]
                    weight[k] = weight[k + 1]
                curr_n -= 1
                j -= 1
            for k in range(i + 1, curr_n - 1):
                level[k] = level[k + 1]
                weight[k] = weight[k + 1]
            curr_n -= 1
        else:
            i += 1
    out: List[float] = []
    for w, v in zip(weight[:curr_n], level[:curr_n]):
        cnt = int(round(w))
        for _ in range(max(1, cnt)):
            out.append(v)
    if len(out) >= n:
        return out[:n]
    else:
        out.extend([out[-1]] * (n - len(out)))
        return out

def _is_sharp_slope_jump(a: float, b: float, eps: float) -> bool:
    a_abs = abs(a)
    b_abs = abs(b)
    max_abs = max(a_abs, b_abs)
    min_abs = min(a_abs, b_abs)
    if max_abs <= eps:
        return False
    if min_abs <= eps:
        return max_abs >= (_SHARP_STEP_SLOPE_RATIO_TRIGGER * eps)
    return (max_abs / min_abs) >= _SHARP_STEP_SLOPE_RATIO_TRIGGER

def _pchip_slopes_fritsch_carlson(
    xs: List[float],
    ys: List[float],
    axis: str,
    *,
    allow_tail_virtual: bool = True,
    monotone_enabled: Optional[bool] = None,
) -> List[float]:
    """Compute conservative shape-preserving PCHIP slopes.

    Uses weighted-harmonic interior derivatives with endpoint limiting
    (Fritsch–Carlson family), which is more conservative around sharp
    slope changes than simple centered averaging.

    Tail handling:
    If the last real segment forms a sharp slope jump with the previous segment,
    append one virtual point on the extension of the last segment for slope
    calculation only. This makes the last real point behave like an interior
    point without changing the persisted model domain or forcing the shared
    derivative at the penultimate point.
    """
    n = len(xs)
    ax = _axis_norm(axis)
    if monotone_enabled is None:
        monotone_enabled = _env_monotone_enable(ax)
    eps = _NUMERIC_STABILITY_EPS

    if n < 2:
        return [0.0] * n

    h = [xs[i + 1] - xs[i] for i in range(n - 1)]
    delta = [(ys[i + 1] - ys[i]) / h[i] if h[i] != 0 else 0.0 for i in range(n - 1)]

    # Tail virtual extension:
    # Only used for derivative computation; the returned derivative list is
    # truncated back to the original real point count.
    if allow_tail_virtual and n >= 3:
        d_prev = delta[-2]
        d_last = delta[-1]
        h_last = h[-1]

        if abs(h_last) > eps and _is_sharp_slope_jump(d_prev, d_last, eps):
            x_virtual = xs[-1] + h_last
            y_virtual = ys[-1] + d_last * h_last

            # Guard against accidental non-increasing x due to pathological input.
            if x_virtual > xs[-1] + eps:
                m_ext = _pchip_slopes_fritsch_carlson(
                    xs + [x_virtual],
                    ys + [y_virtual],
                    axis,
                    allow_tail_virtual=False,
                    monotone_enabled=monotone_enabled,
                )
                return m_ext[:n]

    if n == 2:
        m = [delta[0], delta[0]]
        if monotone_enabled:
            m = [max(0.0, v) for v in m]
        return m

    m = [0.0] * n

    # Interior points: weighted harmonic mean for monotone neighboring slopes;
    # otherwise set derivative to zero to avoid local overshoot around turns.
    for i in range(1, n - 1):
        d_prev = delta[i - 1]
        d_next = delta[i]
        if (
            d_prev == 0.0
            or d_next == 0.0
            or abs(d_prev) <= eps
            or abs(d_next) <= eps
            or d_prev * d_next <= 0
        ):
            m[i] = 0.0
        else:
            w1 = 2.0 * h[i] + h[i - 1]
            w2 = h[i] + 2.0 * h[i - 1]
            denom = (w1 / d_prev) + (w2 / d_next)
            if abs(denom) <= eps:
                m[i] = 0.0
            else:
                m[i] = (w1 + w2) / denom

    # Endpoints with one-sided estimate and monotonicity limiter.
    d0 = h[0] + h[1]
    if n > 2 and abs(d0) > eps:
        m0_num = (2.0 * h[0] + h[1]) * delta[0] - h[0] * delta[1]
        m0 = m0_num / d0
    else:
        m0 = delta[0]

    if m0 * delta[0] <= 0:
        m0 = 0.0
    elif delta[0] * delta[1] < 0 and abs(m0) > abs(3.0 * delta[0]):
        m0 = 3.0 * delta[0]

    m[0] = m0

    dn = h[-1] + h[-2]
    if n > 2 and abs(dn) > eps:
        mn_num = (2.0 * h[-1] + h[-2]) * delta[-1] - h[-1] * delta[-2]
        mn = mn_num / dn
    else:
        mn = delta[-1]

    if mn * delta[-1] <= 0:
        mn = 0.0
    elif delta[-1] * delta[-2] < 0 and abs(mn) > abs(3.0 * delta[-1]):
        mn = 3.0 * delta[-1]

    m[-1] = mn

    if monotone_enabled:
        # 单调模式不允许负斜率
        m = [max(0.0, v) for v in m]

    # Additional local guards for sharp-turn regions and post-step overshoot suppression.
    for i in range(1, n - 1):
        d_prev = delta[i - 1]
        d_next = delta[i]
        min_abs = min(abs(d_prev), abs(d_next))

        if min_abs <= eps:
            continue

        if _is_sharp_slope_jump(d_prev, d_next, eps):
            keep = _SHARP_STEP_DERIVATIVE_DAMPING * min_abs
            if abs(m[i]) > keep:
                m[i] = math.copysign(keep, m[i])

    for i in range(n - 1):
        di = delta[i]

        if abs(di) <= eps:
            m[i] = 0.0
            m[i + 1] = 0.0
            continue

        seg_mag = abs(di)
        is_sharp = (
            (i > 0 and _is_sharp_slope_jump(di, delta[i - 1], eps))
            or (i < n - 2 and _is_sharp_slope_jump(di, delta[i + 1], eps))
        )

        cap = _SEGMENT_DERIVATIVE_CAP_SHARP if is_sharp else _SEGMENT_DERIVATIVE_CAP_SMOOTH
        lim = cap * seg_mag

        # Preserve segment direction: derivatives that point against the secant
        # are zeroed to avoid local reversal and post-step overshoot.
        if m[i] * di < 0:
            m[i] = 0.0
        elif abs(m[i]) > lim:
            m[i] = math.copysign(lim, m[i])

        if m[i + 1] * di < 0:
            m[i + 1] = 0.0
        elif abs(m[i + 1]) > lim:
            m[i + 1] = math.copysign(lim, m[i + 1])

    return m

def build_pchip_model_with_opts(xs_in: List[float], ys_in: List[float], axis: str) -> Optional[Dict[str, Any]]:
    """Legacy configurable builder for spectrum/sone; base perf uses strict anchors."""
    pairs = []
    for x, y in zip(xs_in, ys_in):
        try:
            xf = float(x); yf = float(y)
            if math.isfinite(xf) and math.isfinite(yf):
                pairs.append((xf, yf))
        except Exception:
            continue
    if not pairs:
        return None
    pairs.sort(key=lambda t: t[0])
    xs: List[float] = []
    ys: List[float] = []
    for x, y in pairs:
        if xs and abs(x - xs[-1]) < 1e-9:
            ys[-1] = (ys[-1] + y) / 2.0
        else:
            xs.append(x); ys.append(y)
    if len(xs) == 1:
        return {"x": xs, "y": ys, "m": [0.0], "x0": xs[0], "x1": xs[0]}

    ax = _axis_norm(axis)

    if _env_node_lock(ax):
        ys_target = ys[:]
        m = _pchip_slopes_fritsch_carlson(xs, ys_target, ax)
        return {"x": xs, "y": ys_target, "m": m, "x0": xs[0], "x1": xs[-1]}

    if _env_monotone_enable(ax):
        ys_mono = _pava_isotonic_non_decreasing(ys)
    else:
        ys_mono = ys[:]
    ys_target = ys_mono
    m = _pchip_slopes_fritsch_carlson(xs, ys_target, ax)

    return {"x": xs, "y": ys_target, "m": m, "x0": xs[0], "x1": xs[-1]}

def _build_strict_perf_pchip(xs: List[float], ys: List[float], axis: str) -> Optional[Dict[str, Any]]:
    """Build exact selected nodes without shared-builder repair or deduplication."""
    if len(xs) < 2 or len(xs) != len(ys):
        return None
    if not all(math.isfinite(v) for v in xs + ys):
        return None
    if not all(xs[i] < xs[i + 1] and ys[i] < ys[i + 1] for i in range(len(xs) - 1)):
        return None
    slopes = _pchip_slopes_fritsch_carlson(xs, ys, axis, monotone_enabled=True)
    # Enforce the shape-preserving bound regardless of legacy environment flags.
    for i in range(len(xs) - 1):
        secant = (ys[i + 1] - ys[i]) / (xs[i + 1] - xs[i])
        slopes[i] = max(0.0, slopes[i])
        slopes[i + 1] = max(0.0, slopes[i + 1])
        radius = math.hypot(slopes[i] / secant, slopes[i + 1] / secant)
        if radius > 3.0:
            scale = 3.0 / radius
            slopes[i] *= scale
            slopes[i + 1] *= scale
    return {"x": xs[:], "y": ys[:], "m": slopes, "x0": xs[0], "x1": xs[-1]}

def eval_pchip(model: Dict[str, Any], x: float) -> float:
    xs = model["x"]; ys = model["y"]; ms = model["m"]
    n = len(xs)
    if n == 0:
        return float("nan")
    if n == 1:
        return ys[0]
    if x <= xs[0]:
        x = xs[0]
    if x >= xs[-1]:
        x = xs[-1]
    lo, hi = 0, n - 2
    i = 0
    while lo <= hi:
        mid = (lo + hi) // 2
        if xs[mid] <= x <= xs[mid + 1]:
            i = mid; break
        if x < xs[mid]:
            hi = mid - 1
        else:
            lo = mid + 1
    else:
        i = max(0, min(n - 2, lo))
    x0 = xs[i]; x1 = xs[i + 1]
    h = x1 - x0
    t = (x - x0) / h if h != 0 else 0.0
    y0 = ys[i]; y1 = ys[i + 1]
    m0 = ms[i] * h; m1 = ms[i + 1] * h
    h00 = (2 * t**3 - 3 * t**2 + 1)
    h10 = (t**3 - 2 * t**2 + t)
    h01 = (-2 * t**3 + 3 * t**2)
    h11 = (t**3 - t**2)
    return h00 * y0 + h10 * m0 + h01 * y1 + h11 * m1

# =========================
# 四合一模型：落盘/加载/构建/失效
# =========================

def unified_perf_path(model_id: int, condition_id: int) -> str:
    """Return the on-disk path for the unified performance model cache file."""
    return os.path.join(curve_cache_dir(), f"perf_{int(model_id)}_{int(condition_id)}.json")

# Internal alias — within this module we keep the original private name so that
# the many existing callers in this file do not need to change.
_unified_path = unified_perf_path

def env_key_for_perf() -> str:
    """Return the env-key string capturing all curve-fit parameters.

    Changes to interpolation contract/tuning constants or CODE_VERSION invalidate
    base performance models. Legacy monotone/node-lock flags affect only the
    shared spectrum/sone builder, never the fixed performance contract.
    """
    # 将影响拟合的参数和代码版本纳入统一 env-key
    ek = "|".join([
        f"interp={_INTERP_CONTRACT}",
        f"sharp_step_ratio={_SHARP_STEP_SLOPE_RATIO_TRIGGER:.6f}",
        f"step_damping={_SHARP_STEP_DERIVATIVE_DAMPING:.6f}",
        f"cap_smooth={_SEGMENT_DERIVATIVE_CAP_SMOOTH:.6f}",
        f"cap_sharp={_SEGMENT_DERIVATIVE_CAP_SHARP:.6f}",
        f"code={_CODE_VERSION}",
    ])
    return ek

# Internal alias — existing code within this module calls the original private name.
_env_key_for_perf = env_key_for_perf

# =========================
# Sone 段（由 DB 行级 noise_sone 构建）
# =========================
#
# Per-row ``fan_performance_data.noise_sone`` (NULL = missing, 0 = valid) is
# the only sone source.  Sone models are rebuilt from DB rows exactly like the
# base maps; nothing is preserved from older cache files and no audio
# calibration is run on reads / TTL / cache misses.
#
# Deterministic sone anchor rule (``select_sone_anchors``):
#   1. Eligible row: RPM finite and > 0, sone finite and >= 0 (and, for the
#      airflow chain, airflow finite and > 0).  Otherwise ``missing``/``invalid``.
#   2. Same RPM: identical values keep the first row (others ``duplicate``);
#      differing values exclude every row at that RPM (``rpm_conflict``).
#   3. Equal sone at different RPMs (plateau, e.g. several 0-sone rows): keep
#      the highest RPM, matching the calibration inverse plateau rule; the
#      others are ``plateau``.
#   4. Keep the longest chain strictly increasing in every axis, ties broken
#      like the base joint selection (span, higher RPMs, lower indices);
#      remaining rows are ``monotonic_conflict``.
# Source values are never averaged or altered.  Fewer than two anchors means
# no model for that map.  Missing sone never affects base anchors or scores.

SONE_PCHIP_KEYS = ("rpm_to_sone", "sone_to_rpm", "sone_to_airflow")
# Legacy cache-only sone fields (pre-DB lifecycle); only read by migration.
LEGACY_SONE_FIELDS = ("rpm_nodes_for_sone", "sone_nodes", "sone_version", "sone_source",
                      "sone_airflow_dependency")


def _finite_or_none(v) -> Optional[float]:
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError, OverflowError):
        return None
    return f if math.isfinite(f) else None


def _longest_strict_chain(candidates: List[Tuple[int, Tuple[float, ...]]]) -> List[int]:
    """Indices of the longest chain strictly increasing in every coordinate."""
    candidates = sorted(candidates, key=lambda row: (row[1][0], row[0]))

    def quality(chain):
        rpms = tuple(candidates[j][1][0] for j in chain)
        return (len(chain), rpms[-1] - rpms[0], tuple(reversed(rpms)),
                tuple(-candidates[j][0] for j in chain))

    chains: List[Tuple[int, ...]] = []
    for j, (_, vals) in enumerate(candidates):
        best = (j,)
        for k in range(j):
            if all(a < b for a, b in zip(candidates[k][1], vals)):
                cand = chains[k] + (j,)
                if quality(cand) > quality(best):
                    best = cand
        chains.append(best)
    best_chain = max(chains, key=quality) if chains else ()
    return [candidates[j][0] for j in best_chain]


def select_sone_anchors(rpm: List, sone: List, airflow: Optional[List] = None) -> Dict[str, Any]:
    """Select sone anchors per the documented rule (see section comment)."""
    rpm = rpm or []
    sone = sone or []
    n = max(len(rpm), len(sone), len(airflow or []))
    records: List[Dict[str, Any]] = []
    groups: Dict[float, List[Tuple[int, Tuple[float, ...]]]] = {}
    for i in range(n):
        raw = [rpm[i] if i < len(rpm) else None, sone[i] if i < len(sone) else None]
        if airflow is not None:
            raw.append(airflow[i] if i < len(airflow) else None)
        if any(v is None for v in raw):
            records.append({"included": False, "reason": "missing"})
            continue
        vals = [_finite_or_none(v) for v in raw]
        ok = (all(v is not None for v in vals) and vals[0] > 0 and vals[1] >= 0
              and (airflow is None or vals[2] > 0))
        records.append({"included": False, "reason": "invalid"})
        if not ok:
            continue
        records[i]["reason"] = "monotonic_conflict"
        # Chain order: rpm, sone[, airflow]
        groups.setdefault(vals[0], []).append((i, tuple(vals)))

    by_rpm: List[Tuple[int, Tuple[float, ...]]] = []
    for group in groups.values():
        if any(v[1] != group[0][1][1] for _, v in group):
            for i, _ in group:
                records[i]["reason"] = "rpm_conflict"
        else:
            by_rpm.append(group[0])
            for i, _ in group[1:]:
                records[i]["reason"] = "duplicate"

    by_sone: Dict[float, Tuple[int, Tuple[float, ...]]] = {}
    for row in sorted(by_rpm, key=lambda r: (r[1][0], r[0])):
        prev = by_sone.get(row[1][1])
        if prev is not None:
            records[prev[0]]["reason"] = "plateau"
        by_sone[row[1][1]] = row  # highest RPM wins

    indices = sorted(_longest_strict_chain(list(by_sone.values())),
                     key=lambda i: float(rpm[i]))
    for i in indices:
        records[i] = {"included": True, "reason": "included"}
    return {"version": _SONE_SELECTION_VERSION, "indices": indices, "records": records}


def build_sone_models(rpm: List, airflow: List, sone: List) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Build rpm_to_sone / sone_to_rpm / sone_to_airflow from DB rows.

    Returns ``(pchip_maps, selection)``; maps with < 2 anchors are omitted.
    """
    pack: Dict[str, Any] = {}
    base = select_sone_anchors(rpm, sone)
    xs = [float(rpm[i]) for i in base["indices"]]
    ss = [float(sone[i]) for i in base["indices"]]
    m = _build_strict_perf_pchip(xs, ss, "rpm")
    if m:
        pack["rpm_to_sone"] = m
        pack["sone_to_rpm"] = _build_strict_perf_pchip(ss, xs, "noise_db")
    air = select_sone_anchors(rpm, sone, airflow)
    sa = [float(sone[i]) for i in air["indices"]]
    aa = [float(airflow[i]) for i in air["indices"]]
    m2 = _build_strict_perf_pchip(sa, aa, "noise_db")
    if m2:
        pack["sone_to_airflow"] = m2
    pack = {k: v for k, v in pack.items() if v}
    return pack, {"version": _SONE_SELECTION_VERSION,
                  "indices": base["indices"], "airflow_indices": air["indices"]}


def legacy_sone_snapshot_dir() -> str:
    """Fixed server-side directory holding pre-DB sone cache snapshots."""
    return os.path.join(curve_cache_dir(), "legacy_sone_snapshot")


def has_legacy_sone(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    meta = payload.get("meta") or {}
    if isinstance(meta, dict) and meta.get("schema_version") == _PERF_SCHEMA_VERSION:
        return False
    return isinstance(payload.get("sone_nodes"), list) and bool(payload.get("sone_nodes"))


def snapshot_legacy_sone(model_id: int, condition_id: int, payload: Any) -> Optional[str]:
    """Copy a legacy (pre-v3) cache carrying raw sone nodes before overwrite.

    The first snapshot is kept (never overwritten) so later rebuilds cannot
    erase the migration source.  Returns the snapshot path when present.
    """
    if not has_legacy_sone(payload):
        return None
    d = legacy_sone_snapshot_dir()
    p = os.path.join(d, f"perf_{int(model_id)}_{int(condition_id)}.json")
    if os.path.isfile(p):
        return p
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix="snap_", suffix=".json", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        os.replace(tmp, p)
    except Exception:
        try:
            os.remove(tmp)
        except Exception:
            pass
        raise
    return p


def _write_perf_payload_atomic(model_id: int, condition_id: int, payload: Dict[str, Any]) -> str:
    p = _unified_path(model_id, condition_id)
    # Any writer replacing a legacy cache with raw sone nodes snapshots it first;
    # if the snapshot fails the legacy file is left in place (not overwritten).
    try:
        with open(p, "r", encoding="utf-8") as f:
            existing = json.load(f)
    except Exception:
        existing = None
    if has_legacy_sone(existing):
        try:
            snapshot_legacy_sone(model_id, condition_id, existing)
        except Exception as e:
            _logger.warning("legacy sone snapshot failed for %s_%s; cache not overwritten: %s",
                            model_id, condition_id, e)
            return p
    # 原子替换写入，避免并发读到半成品
    d = os.path.dirname(p)
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix="perf_", suffix=".json", dir=d)
    replaced = False
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        os.replace(tmp, p)
        replaced = True
    finally:
        if not replaced:
            try:
                os.remove(tmp)
            except Exception:
                pass
    return p


def save_unified_perf_model(model_id: int, condition_id: int, models: dict, *, data_hash: str, env_key: str,
                            supports_audio: bool = False,
                            raw: Optional[Dict[str, Any]] = None,
                            anchor_selection: Optional[Dict[str, Any]] = None) -> str:
    payload = {
        "type": "perf_pchip_v1",
        "model_id": int(model_id),
        "condition_id": int(condition_id),
        "pchip": {
            "rpm_to_airflow": models.get("rpm_to_airflow"),
            "rpm_to_noise_db": models.get("rpm_to_noise_db"),
            "noise_to_rpm": models.get("noise_to_rpm"),
            "noise_to_airflow": models.get("noise_to_airflow"),
        },
        "supports_audio": bool(supports_audio),
        "meta": {
            "data_hash": data_hash,
            "env_key": env_key,
            "interp_contract": _INTERP_CONTRACT,
            "schema_version": _PERF_SCHEMA_VERSION,
            "code_version": _CODE_VERSION,
            "created_at": datetime.now().isoformat(timespec='seconds'),
        }
    }
    if raw is not None:
        raw = dict(raw)
        raw.setdefault("noise_sone", [])
        payload["raw"] = raw
        payload["anchor_selection"] = anchor_selection or select_joint_anchors(
            raw.get("rpm"), raw.get("airflow"), raw.get("noise_db"), raw.get("pressure_mmh2o"))
        sone_pack, sone_sel = build_sone_models(raw.get("rpm") or [], raw.get("airflow") or [],
                                                raw.get("noise_sone") or [])
        payload["pchip"].update(sone_pack)
        payload["sone_selection"] = sone_sel
    return _write_perf_payload_atomic(model_id, condition_id, payload)


def _has_current_anchor_schema(data: Dict[str, Any]) -> bool:
    selection = data.get("anchor_selection")
    meta = data.get("meta") or {}
    raw = data.get("raw") or {}
    if not isinstance(selection, dict) or not isinstance(meta, dict) or not isinstance(raw, dict):
        return False
    if not all(isinstance(raw.get(k), list) for k in ("rpm", "airflow", "noise_db", "noise_sone")):
        return False
    if meta.get("schema_version") != _PERF_SCHEMA_VERSION or meta.get("interp_contract") != _INTERP_CONTRACT:
        return False
    if selection.get("version") != _ANCHOR_SELECTION_VERSION:
        return False
    records, indices = selection.get("records"), selection.get("indices")
    if not isinstance(records, list) or not isinstance(indices, list):
        return False
    if not isinstance(raw.get("pressure_mmh2o", []), list):
        return False
    if len(records) != max(len(raw.get(k) or []) for k in ("rpm", "airflow", "noise_db", "pressure_mmh2o")):
        return False
    if raw["noise_sone"] and len(raw["noise_sone"]) != len(records):
        return False
    if any(type(i) is not int or i < 0 or i >= len(records) for i in indices):
        return False
    selected = set(indices)
    if len(selected) != len(indices):
        return False
    return all(isinstance(r, dict) and r.get("included") is (i in selected)
               and (r.get("reason") == "included") is (i in selected)
               and r.get("reason") in ("included", "missing", "invalid", "duplicate",
                                       "rpm_conflict", "monotonic_conflict")
               for i, r in enumerate(records))


def _read_unified_perf_model(model_id: int, condition_id: int) -> dict | None:
    p = _unified_path(model_id, condition_id)
    if not os.path.isfile(p):
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return None
        if data.get("type") != "perf_pchip_v1":
            return None
        if not isinstance(data.get("pchip"), dict) or not isinstance(data.get("meta"), dict):
            return None
        if "raw" in data and not isinstance(data["raw"], dict):
            return None
        return data
    except Exception:
        return None

def is_current_perf_payload(payload: Any, env_key: Optional[str] = None) -> bool:
    """Validate current anchors and contract against the current or supplied env key."""
    expected_env = env_key_for_perf() if env_key is None else env_key
    return (isinstance(payload, dict) and payload.get("type") == "perf_pchip_v1"
            and isinstance(payload.get("pchip"), dict)
            and _has_current_anchor_schema(payload)
            and payload["meta"].get("env_key") == expected_env)


def load_unified_perf_model(model_id: int, condition_id: int) -> dict | None:
    data = _read_unified_perf_model(model_id, condition_id)
    if not is_current_perf_payload(data):
        return None
    return data

def update_perf_cache_supports_audio(model_id: int, condition_id: int) -> bool:
    """Update the ``supports_audio`` field of an existing perf cache file in-place.

    Reads the current perf cache, recomputes ``supports_audio`` by calling
    ``_check_spectrum_supports_audio``, and writes the file back only when the
    value has changed.  Returns the new ``supports_audio`` value, or ``False``
    when no perf cache file exists.

    This is the **write-path** counterpart of removing the synchronous check
    from the hot read path: call it after every spectrum cache update so the
    stored value stays accurate without burdening queries.
    """
    cached = load_unified_perf_model(model_id, condition_id)
    if cached is None:
        return False
    expected = _check_spectrum_supports_audio(model_id, condition_id)
    if bool(cached.get("supports_audio")) == expected:
        return expected
    cached["supports_audio"] = expected
    _write_perf_payload_atomic(model_id, condition_id, cached)

    return expected


def _inmem_key_unified(model_id: int, condition_id: int, data_hash: str, env_key: str) -> str:
    return f"{int(model_id)}|{int(condition_id)}|perf|{data_hash}|{env_key}"

def _collect_valid_xy(xs: List[float], ys: List[float]) -> Tuple[List[float], List[float]]:
    outx: List[float] = []
    outy: List[float] = []
    for x, y in zip(xs or [], ys or []):
        try:
            xf = float(x) if x is not None else None
            yf = float(y) if y is not None else None
        except Exception:
            continue
        if xf is None or yf is None:
            continue
        if not (math.isfinite(xf) and math.isfinite(yf)):
            continue
        outx.append(xf); outy.append(yf)
    return outx, outy

def get_or_build_unified_perf_model(model_id: int, condition_id: int,
                                    rpm: List[float], airflow: List[float], noise: List[float],
                                    pressure: Optional[List] = None,
                                    sone: Optional[List] = None) -> Optional[Dict[str, Any]]:
    """
    四合一模型唯一入口：
      - 依据原始行计算 data_hash（含 pressure_mmh2o 与 DB 行级 noise_sone）
      - 组成 env_key（含固定插值契约/局部保守调参/代码版本）
      - 先查内存 LRU；再查磁盘；任一命中且 meta 匹配则直接返回
      - 否则重建四条曲线并落盘 + 进入 LRU
    """
    data_hash = raw_quads_hash(rpm or [], airflow or [], noise or [], pressure or [], sone or [])
    env_key = _env_key_for_perf()
    ikey = _inmem_key_unified(model_id, condition_id, data_hash, env_key)

    if _INMEM:
        m = _INMEM.get(ikey)
        if is_current_perf_payload(m, env_key):
            _note_hit(ikey)
            return m

    cached = _read_unified_perf_model(model_id, condition_id)
    if is_current_perf_payload(cached, env_key):
        meta = cached.get("meta") or {}
        if meta.get("data_hash") == data_hash and meta.get("env_key") == env_key:
            if _INMEM and _note_hit(ikey) >= _ADMIT_HITS:
                _INMEM.put(ikey, cached)
            return cached

    selection = select_joint_anchors(rpm, airflow, noise, pressure)
    indices = selection["indices"]
    xs = [float(rpm[i]) for i in indices]
    air = [float(airflow[i]) for i in indices]
    nz = [float(noise[i]) for i in indices]
    pack = {
        "rpm_to_airflow": _build_strict_perf_pchip(xs, air, "rpm"),
        "rpm_to_noise_db": _build_strict_perf_pchip(xs, nz, "rpm"),
        "noise_to_rpm": _build_strict_perf_pchip(nz, xs, "noise_db"),
        "noise_to_airflow": _build_strict_perf_pchip(nz, air, "noise_db"),
    }

    # 检查对应的 spectrum cache 是否支持音频生成
    supports_audio = _check_spectrum_supports_audio(model_id, condition_id)

    sone_pack, sone_selection = build_sone_models(rpm or [], airflow or [], sone or [])
    pack.update(sone_pack)

    # 原始锚点存入 raw 块，供 api_curves 直接读取（不再走 DB 查询）
    preserve_extras = cached and cached["meta"].get("data_hash") == data_hash
    raw_block = dict(cached.get("raw") or {}) if preserve_extras else {}
    raw_block.update({
        "rpm": list(rpm or []),
        "airflow": list(airflow or []),
        "noise_db": list(noise or []),
        "pressure_mmh2o": list(pressure or []),
        "noise_sone": list(sone or []),
    })

    out = {
        "type": "perf_pchip_v1",
        "model_id": int(model_id),
        "condition_id": int(condition_id),
        "pchip": pack,
        "supports_audio": supports_audio,
        "raw": raw_block,
        "anchor_selection": selection,
        "sone_selection": sone_selection,
        "meta": {
            "data_hash": data_hash,
            "env_key": env_key,
            "interp_contract": _INTERP_CONTRACT,
            "schema_version": _PERF_SCHEMA_VERSION,
            "code_version": _CODE_VERSION,
            "created_at": datetime.now().isoformat(timespec='seconds'),
        }
    }
    _write_perf_payload_atomic(model_id, condition_id, out)
    if _INMEM and _note_hit(ikey) >= _ADMIT_HITS:
        _INMEM.put(ikey, out)
    return out
