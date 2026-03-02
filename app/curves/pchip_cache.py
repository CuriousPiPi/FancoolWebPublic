import os
import json
import math
import hashlib
import threading
import logging
from collections import OrderedDict
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

# Setup logger for this module
_logger = logging.getLogger(__name__)

# =========================
# 环境参数（兼容原逻辑）
# =========================

_ALPHA = {
    "rpm": float(os.getenv("CURVE_SMOOTH_ALPHA_RPM", "0.0")),
    "noise_db": float(os.getenv("CURVE_SMOOTH_ALPHA_NOISE", "0.0")),
}
_TAU = {
    "rpm": float(os.getenv("CURVE_TENSION_TAU_RPM", "0.0")),
    "noise_db": float(os.getenv("CURVE_TENSION_TAU_NOISE", "0.0")),
}
_CODE_VERSION = os.getenv("CODE_VERSION", "")  # 纳入统一 env-key 用于失效

def reload_curve_params_from_env():
    # 仅支持运行中刷新平滑/张力（与旧逻辑一致）；内存 LRU 门限须重启生效
    _ALPHA["rpm"]         = float(os.getenv("CURVE_SMOOTH_ALPHA_RPM",         str(_ALPHA["rpm"])))
    _ALPHA["noise_db"]    = float(os.getenv("CURVE_SMOOTH_ALPHA_NOISE",       str(_ALPHA["noise_db"])))
    _TAU["rpm"]           = float(os.getenv("CURVE_TENSION_TAU_RPM",          str(_TAU["rpm"])))
    _TAU["noise_db"]      = float(os.getenv("CURVE_TENSION_TAU_NOISE",        str(_TAU["noise_db"])))

def _axis_norm(axis: str) -> str:
    return "noise_db" if axis == "noise" else axis

def _env_bool(name: str, default: str = "1") -> bool:
    return (os.getenv(name, default) or "").strip() in ("1", "true", "True", "YES", "yes")

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

def _env_alpha_for_axis(axis: str) -> float:
    ax = _axis_norm(axis)
    val = float(_ALPHA.get(ax, 0.0))
    return max(0.0, min(1.0, val))

def _env_tau_for_axis(axis: str) -> float:
    ax = _axis_norm(axis)
    val = float(_TAU.get(ax, 0.0))
    return max(0.0, min(1.0, val))

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
                for k in ("rpm_to_airflow","rpm_to_noise_db","noise_to_rpm","noise_to_airflow"):
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

def _ols_linear(xs: List[float], ys: List[float]) -> tuple[float, float]:
    n = len(xs)
    if n == 0:
        return (0.0, 0.0)
    sx = sum(xs); sy = sum(ys)
    sxx = sum(x*x for x in xs)
    sxy = sum(x*y for x, y in zip(xs, ys))
    denom = n * sxx - sx * sx
    if abs(denom) < 1e-12:
        return (sy / n if n else 0.0, 0.0)
    b = (n * sxy - sx * sy) / denom
    a = (sy - b * sx) / n
    return (a, b)

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

def _pchip_slopes_fritsch_carlson(xs: List[float], ys: List[float]) -> List[float]:
    n = len(xs)
    if n < 2:
        return [0.0] * n
    h = [xs[i + 1] - xs[i] for i in range(n - 1)]
    delta = [(ys[i + 1] - ys[i]) / h[i] if h[i] != 0 else 0.0 for i in range(n - 1)]
    m = [0.0] * n
    m[0] = delta[0]
    m[-1] = delta[-1]
    for i in range(1, n - 1):
        if delta[i - 1] * delta[i] > 0:
            m[i] = (delta[i - 1] + delta[i]) / 2.0
        else:
            m[i] = 0.0
    for i in range(n - 1):
        if delta[i] == 0.0:
            m[i] = 0.0
            m[i + 1] = 0.0
        else:
            a = m[i] / delta[i]
            b = m[i + 1] / delta[i]
            s = a * a + b * b
            if s > 9.0:
                t = 3.0 / math.sqrt(s)
                m[i] = t * a * delta[i]
                m[i + 1] = t * b * delta[i]
        if _env_monotone_enable("rpm") or _env_monotone_enable("noise_db"):
            # 单调模式不允许负斜率
            if m[i] < 0:
                m[i] = 0.0
            if m[i + 1] < 0:
                m[i + 1] = 0.0
    return m

def _blend_nodes_with_trend(xs: List[float], ys_mono: List[float], axis: str) -> List[float]:
    alpha = _env_alpha_for_axis(axis)
    if alpha <= 1e-9:
        return ys_mono[:]
    a, b = _ols_linear(xs, ys_mono)
    ys_lin = [a + b * x for x in xs]
    return [(1.0 - alpha) * ym + alpha * yl for ym, yl in zip(ys_mono, ys_lin)]

def _scale_slopes(m: List[float], axis: str) -> List[float]:
    tau = _env_tau_for_axis(axis)
    if tau <= 1e-9:
        return m
    return [(1.0 - tau) * v for v in m]

def build_pchip_model_with_opts(xs_in: List[float], ys_in: List[float], axis: str) -> Optional[Dict[str, Any]]:
    """统一轴向 PCHIP 构建：保留旧的平滑/张力/单调/节点锁定语义。"""
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
        m = _pchip_slopes_fritsch_carlson(xs, ys_target)
        return {"x": xs, "y": ys_target, "m": m, "x0": xs[0], "x1": xs[-1]}

    if _env_monotone_enable(ax):
        ys_mono = _pava_isotonic_non_decreasing(ys)
    else:
        ys_mono = ys[:]
    ys_target = _blend_nodes_with_trend(xs, ys_mono, ax)
    m = _pchip_slopes_fritsch_carlson(xs, ys_target)
    m = _scale_slopes(m, ax)

    return {"x": xs, "y": ys_target, "m": m, "x0": xs[0], "x1": xs[-1]}

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

def _unified_path(model_id: int, condition_id: int) -> str:
    return os.path.join(curve_cache_dir(), f"perf_{int(model_id)}_{int(condition_id)}.json")

def _env_key_for_perf() -> str:
    # 将影响拟合的环境参数和代码版本纳入统一 env-key
    ek = "|".join([
        f"alpha_rpm={_env_alpha_for_axis('rpm'):.6f}",
        f"alpha_noise={_env_alpha_for_axis('noise_db'):.6f}",
        f"tau_rpm={_env_tau_for_axis('rpm'):.6f}",
        f"tau_noise={_env_tau_for_axis('noise_db'):.6f}",
        f"mono_rpm={int(_env_monotone_enable('rpm'))}",
        f"mono_noise={int(_env_monotone_enable('noise_db'))}",
        f"lock_rpm={int(_env_node_lock('rpm'))}",
        f"lock_noise={int(_env_node_lock('noise_db'))}",
        f"code={_CODE_VERSION}",
    ])
    return ek

def save_unified_perf_model(model_id: int, condition_id: int, models: dict, *, data_hash: str, env_key: str, supports_audio: bool = False) -> str:
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
            "code_version": _CODE_VERSION,
            "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        }
    }
    p = _unified_path(model_id, condition_id)
    # 原子替换写入，避免并发读到半成品
    import tempfile
    d = os.path.dirname(p)
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix="perf_", suffix=".json", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        os.replace(tmp, p)
    finally:
        try:
            os.remove(tmp)
        except Exception:
            pass
    return p

def load_unified_perf_model(model_id: int, condition_id: int) -> dict | None:
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
        if "pchip" not in data or "meta" not in data:
            return None
        return data
    except Exception:
        return None

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
                                    rpm: List[float], airflow: List[float], noise: List[float]) -> Optional[Dict[str, Any]]:
    """
    四合一模型唯一入口：
      - 依据三轴原始点计算 data_hash
      - 组成 env_key（含平滑/张力/单调/节点锁定/代码版本）
      - 先查内存 LRU；再查磁盘；任一命中且 meta 匹配则直接返回
      - 否则重建四条曲线并落盘 + 进入 LRU
    """
    data_hash = raw_triples_hash(rpm or [], airflow or [], noise or [])
    env_key = _env_key_for_perf()
    ikey = _inmem_key_unified(model_id, condition_id, data_hash, env_key)

    if _INMEM:
        m = _INMEM.get(ikey)
        if m is not None:
            _note_hit(ikey)
            return m

    cached = load_unified_perf_model(model_id, condition_id)
    if cached:
        meta = cached.get("meta") or {}
        if meta.get("data_hash") == data_hash and meta.get("env_key") == env_key:
            # Ensure cached model has supports_audio flag
            # If not present, check spectrum cache and add it
            # Note: cached is a fresh dict from disk (not shared), so modification is safe
            if "supports_audio" not in cached:
                cached["supports_audio"] = _check_spectrum_supports_audio(model_id, condition_id)
            
            if _INMEM and _note_hit(ikey) >= _ADMIT_HITS:
                _INMEM.put(ikey, cached)
            return cached

    # 现算
    x_rpm_air, y_rpm_air = _collect_valid_xy(rpm, airflow)
    x_rpm_nz,  y_rpm_nz  = _collect_valid_xy(rpm, noise)
    x_nz_rpm,  y_nz_rpm  = _collect_valid_xy(noise, rpm)
    x_nz_air,  y_nz_air  = _collect_valid_xy(noise, airflow)

    pack = {
        "rpm_to_airflow":   build_pchip_model_with_opts(x_rpm_air, y_rpm_air, axis="rpm")       if x_rpm_air and y_rpm_air else None,
        "rpm_to_noise_db":  build_pchip_model_with_opts(x_rpm_nz,  y_rpm_nz,  axis="rpm")       if x_rpm_nz and y_rpm_nz   else None,
        "noise_to_rpm":     build_pchip_model_with_opts(x_nz_rpm,  y_nz_rpm,  axis="noise_db")  if x_nz_rpm and y_nz_rpm   else None,
        "noise_to_airflow": build_pchip_model_with_opts(x_nz_air,  y_nz_air,  axis="noise_db")  if x_nz_air and y_nz_air   else None,
    }

    # 检查对应的 spectrum cache 是否支持音频生成
    # Check if the corresponding spectrum cache supports audio generation
    supports_audio = _check_spectrum_supports_audio(model_id, condition_id)

    # 落盘
    save_unified_perf_model(model_id, condition_id, pack, data_hash=data_hash, env_key=env_key, supports_audio=supports_audio)
    out = {
        "type": "perf_pchip_v1",
        "model_id": int(model_id),
        "condition_id": int(condition_id),
        "pchip": pack,
        "supports_audio": supports_audio,
        "meta": {
            "data_hash": data_hash,
            "env_key": env_key,
            "code_version": _CODE_VERSION,
            "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        }
    }
    if _INMEM and _note_hit(ikey) >= _ADMIT_HITS:
        _INMEM.put(ikey, out)
    return out