# -*- coding: utf-8 -*-
"""
synth.py - 一键从项目的 pipeline.py 构模并按任意转速合成拟合音频（命令行）

用法示例：
  - 固定转速从根目录构模并合成 5 秒音频：
      python synth.py --root /path/to/data --rpm 1800 --seconds 5 --out R1800.wav
  - 直接加载已保存的模型 JSON 合成：
      python synth.py --load-model model.json --rpm 1500 --seconds 4 --out R1500.wav
  - 从转速轨迹 CSV 合成（每行 "rpm[,sec]"；如无 sec 列，需配合 --step-sec）：
      python synth.py --root /path/to/data --rpm-track track.csv --step-sec 0.1 --out sweep.wav

依赖：
  - 同目录的 pipeline.py
  - numpy, scipy, soundfile
"""
import os
import sys
import json
import math
import argparse
from typing import Dict, Any, List, Optional, Tuple

import numpy as np

# 优先把当前脚本目录加入 sys.path，保证可以 import pipeline.py
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
if _THIS_DIR not in sys.path:
    sys.path.insert(0, _THIS_DIR)

# 导入项目中的 pipeline 工具
try:
    from pipeline import (
        P0,
        a_weight_db,
        predict_spectrum_db_with_harmonics,
        band_edges_from_centers,
        laeq_full_via_bands_filterbank,
        _design_cpb_filterbank_iir,
        _design_cpb_filterbank_fir,
        run_calibration_and_model,
        pchip_eval,  # from app.curves.pchip_cache.eval_pchip
    )
except Exception as e:
    print("ERROR: 无法导入 pipeline.py，请将 synth.py 放在与 pipeline.py 同一目录。")
    raise

from scipy import signal
# ---------- 小工具 ----------

def _as_bool(v, d=False):
    try:
        return bool(v)
    except Exception:
        return d

def _as_float(v, d=0.0):
    try:
        return float(v)
    except Exception:
        return d

def _as_int(v, d=0):
    try:
        return int(v)
    except Exception:
        return d

def _ensure_soundfile():
    try:
        import soundfile as sf  # noqa
        return True
    except Exception:
        return False

def _write_wav(path: str, y: np.ndarray, fs: int):
    """
    优先使用 soundfile 写出；如缺失，回退到 16-bit PCM。
    """
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    if _ensure_soundfile():
        import soundfile as sf
        sf.write(path, y.astype(np.float32, copy=False), fs)
        return
    # 回退：使用标准库 wave 写 16-bit PCM
    import wave, struct
    y16 = np.clip(y, -1.0, 1.0)
    y16 = (y16 * 32767.0).astype(np.int16, copy=False)
    with wave.open(path, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(fs)
        wf.writeframes(y16.tobytes())

def _get_centers_from_model(model: Dict[str, Any], default_npo: int = 12) -> Tuple[np.ndarray, int]:
    centers = np.array(model.get("centers_hz") or [], dtype=float)
    if centers.size == 0:
        raise RuntimeError("model 中缺少 centers_hz")
    npo = int((model.get("calibration") or {}).get("n_per_oct", default_npo))
    return centers, npo

def _pick_filterbank(centers: np.ndarray,
                     fs: int,
                     n_per_oct: int,
                     use_fir_cpb: bool = False,
                     bands_filter_order: int = 6,
                     fir_base_taps: int = 256) -> Tuple[List[Optional[np.ndarray]], bool]:
    if use_fir_cpb:
        return _design_cpb_filterbank_fir(centers, fs, n_per_oct, base_taps=int(fir_base_taps)), True
    else:
        return _design_cpb_filterbank_iir(centers, fs, n_per_oct, order=int(bands_filter_order)), False

def _gauss_logf_weights(f_line: float,
                        centers: np.ndarray,
                        f1: np.ndarray,
                        f2: np.ndarray,
                        sigma_bands: float = 0.25,
                        topk: int = 3) -> List[Tuple[int, float]]:
    if not np.isfinite(f_line) or f_line <= 0.0:
        return []
    logc = np.log(np.maximum(centers, 1e-30))
    w = np.exp(-0.5 * ((np.log(max(f_line, 1e-30)) - logc) / max(1e-6, sigma_bands))**2)
    inside = ((f_line >= f1) & (f_line <= f2)).astype(float)
    w = w * inside
    if np.all(w <= 0):
        return []
    idx = np.argsort(-w)[:max(1, int(topk))]
    ww = w[idx]
    ww = ww / max(1e-30, np.sum(ww))
    return [(int(i), float(v)) for i, v in zip(idx.tolist(), ww.tolist())]

def _distribute_harmonics_energy_A(model: Dict[str, Any],
                                   rpm: float,
                                   centers: np.ndarray,
                                   n_per_oct: int) -> Tuple[np.ndarray, List[Tuple[float, float]]]:
    """
    返回：
      - per-band 谐波分配后的 A 计权能量向量 E_harm_A (长度=K)
      - 线谱列表 lines: [(f_line_hz, Eh_A), ...]
    """
    calib = model.get("calibration") or {}
    harm = calib.get("harmonics") or {}
    enabled = bool(calib.get("harmonics_enabled", False))
    K = int(centers.size)
    E_harm_A = np.zeros((K,), dtype=float)
    lines: List[Tuple[float, float]] = []

    if not (enabled and isinstance(harm, dict) and int(harm.get("n_blade", 0)) > 0):
        return E_harm_A, lines

    n_blade = int(harm["n_blade"])
    kernel = harm.get("kernel") or {}
    sigma_b = float(kernel.get("sigma_bands", 0.25))
    topk = int(kernel.get("topk", 3))

    f1, f2 = band_edges_from_centers(centers, n_per_oct, grid="iec-decimal")
    bpf = n_blade * (float(rpm) / 60.0)

    for item in (harm.get("models") or []):
        mdl = item.get("amp_pchip_db")
        h = int(item.get("h", 0) or 0)
        if h <= 0 or not mdl or not isinstance(mdl, dict):
            continue
        Lh_db = float(pchip_eval(mdl, float(rpm)))
        if not np.isfinite(Lh_db):
            continue
        f_line = float(h) * float(bpf)
        if not (np.isfinite(f_line) and f_line > 0.0):
            continue
        Eh_A = (P0**2) * (10.0 ** (Lh_db / 10.0))
        lines.append((f_line, Eh_A))
        # 采用与建模一致的分配核
        for k, w in _gauss_logf_weights(f_line, centers, f1, f2, sigma_bands=sigma_b, topk=topk):
            E_harm_A[k] += Eh_A * w

    return E_harm_A, lines

# ---------- 合成核心 ----------

def synthesize_stationary_audio_from_model(model: Dict[str, Any],
                                           rpm: float,
                                           seconds: float = 5.0,
                                           fs: int = 48000,
                                           *,
                                           tones: bool = True,
                                           use_fir_cpb: bool = False,
                                           bands_filter_order: int = 6,
                                           fir_base_taps: int = 256,
                                           random_seed: Optional[int] = None) -> np.ndarray:
    """
    基于模型合成“稳态（固定转速）”音频。
      - tones=True：单独注入谐波正弦，并将其能量从频带噪声里扣除，避免双计
      - tones=False：所有能量均用带限噪声呈现
    返回：float 波形，范围约 [-1,1]
    """
    centers, npo = _get_centers_from_model(model)

    # 1) 取得“包含谐波注入”的频带 dB（保持与模型推理一致）
    band_db_list, _la_model_db = predict_spectrum_db_with_harmonics(model, rpm)
    if not band_db_list or all(v is None for v in band_db_list):
        raise RuntimeError("给定转速下模型未返回有效频带谱")

    # 总目标 A 计权能量（含谐波）
    Es_total_A = np.zeros((centers.size,), dtype=float)
    for i, v in enumerate(band_db_list):
        if v is None or not np.isfinite(float(v)):
            Es_total_A[i] = 0.0
        else:
            Es_total_A[i] = (P0**2) * (10.0 ** (float(v) / 10.0))

    # 2) 如需将谐波做成“线谱”，则把谐波能量分配到频带，随后从噪声目标中扣除
    harmonic_lines: List[Tuple[float, float]] = []
    if tones:
        E_harm_A_band, harmonic_lines = _distribute_harmonics_energy_A(model, rpm, centers, npo)
        # 防止因数值近似导致负值
        Es_noise_A = np.maximum(Es_total_A - E_harm_A_band, 0.0)
    else:
        Es_noise_A = Es_total_A

    # 3) 噪声：把每带 A 计权能量换算为“未计权均方”，再用带通滤波白噪声并按能量缩放
    W_A_cent = 10.0 ** (a_weight_db(centers) / 10.0)
    target_preA_E = np.divide(Es_noise_A, np.maximum(W_A_cent, 1e-30))

    fb, is_fir = _pick_filterbank(centers, fs, npo,
                                  use_fir_cpb=use_fir_cpb,
                                  bands_filter_order=bands_filter_order,
                                  fir_base_taps=fir_base_taps)
    N = int(max(1, round(seconds * fs)))
    rng = np.random.default_rng(int(random_seed) if random_seed is not None else None)
    white = rng.standard_normal(N, dtype=np.float64)
    y = np.zeros(N, dtype=np.float64)

    for k, filt in enumerate(fb):
        if filt is None or target_preA_E[k] <= 0.0:
            continue
        if is_fir:
            yk = signal.oaconvolve(white, np.asarray(filt, dtype=float), mode="same")
        else:
            yk = signal.sosfilt(np.asarray(filt), white)
        var0 = float(np.mean(yk * yk)) if yk.size else 0.0
        if var0 <= 0.0:
            continue
        gain = math.sqrt(max(0.0, float(target_preA_E[k])) / var0)
        y += yk * gain

    # 4) 线谱：对每个谐波，按 Eh_A 反算未计权均方，再以随机初相生成正弦并叠加
    if tones and harmonic_lines:
        t = np.arange(N, dtype=np.float64) / float(fs)
        for f_line, Eh_A in harmonic_lines:
            if not (np.isfinite(f_line) and 0 < f_line < fs * 0.49 and Eh_A > 0.0):
                continue
            W_line = 10.0 ** (a_weight_db(np.array([f_line], float))[0] / 10.0)
            var_pre = float(Eh_A) / max(1e-30, float(W_line))
            amp_rms = math.sqrt(max(0.0, var_pre))
            amp_peak = amp_rms * math.sqrt(2.0)
            phase = rng.uniform(0.0, 2.0*np.pi)
            y += amp_peak * np.sin(2.0 * np.pi * float(f_line) * t + phase)

    # 5) 防削波 + 用同一滤波器组回测 LAeq 并全局对齐（保证与模型一致）
    peak = float(np.max(np.abs(y))) if y.size else 0.0
    if peak > 0.99:
        y = y / peak * 0.99

    LA_synth_db, _ = laeq_full_via_bands_filterbank(
        y, fs, centers, npo,
        bands_filter_order=bands_filter_order,
        use_fir_cpb=use_fir_cpb,
        fir_base_taps=fir_base_taps
    )
    LA_target_db = 10.0 * math.log10(max(float(np.sum(Es_total_A)) / (P0**2), 1e-30)) if np.any(Es_total_A > 0) else float("nan")
    if np.isfinite(LA_synth_db) and np.isfinite(LA_target_db):
        delta_db = float(LA_target_db - LA_synth_db)
        g = 10.0 ** (delta_db / 20.0)
        y *= g
        peak = float(np.max(np.abs(y))) if y.size else 0.0
        if peak > 0.99:
            y = y / max(1e-12, peak) * 0.99

    return y

def synthesize_from_rpm_track(model: Dict[str, Any],
                              track: List[Tuple[float, float]],
                              fs: int = 48000,
                              tones: bool = True,
                              use_fir_cpb: bool = False,
                              bands_filter_order: int = 6,
                              fir_base_taps: int = 256) -> np.ndarray:
    """
    轨迹合成：track 为 [(rpm, seconds), ...]
    片段间做 50 ms 线性交叉淡化。
    """
    pieces: List[np.ndarray] = []
    for rpm, sec in track:
        sec = max(0.01, float(sec))
        seg = synthesize_stationary_audio_from_model(
            model, float(rpm), seconds=sec, fs=fs,
            tones=tones,
            use_fir_cpb=use_fir_cpb,
            bands_filter_order=bands_filter_order,
            fir_base_taps=fir_base_taps,
        )
        pieces.append(seg.astype(np.float64, copy=False))

    if not pieces:
        return np.zeros(0, dtype=np.float64)

    x = pieces[0]
    xf = int(min(len(x), 0.05 * fs))  # 50 ms crossfade
    for i in range(1, len(pieces)):
        y = pieces[i]
        n = min(xf, len(x), len(y))
        if n > 0:
            w = np.linspace(0.0, 1.0, n, dtype=np.float64)
            tail = x[-n:] * (1.0 - w) + y[:n] * w
            x = np.concatenate([x[:-n], tail, y[n:]], axis=0)
        else:
            x = np.concatenate([x, y], axis=0)
    # 轻微限幅
    peak = float(np.max(np.abs(x)))
    if peak > 0.99:
        x = x / peak * 0.99
    return x

# ---------- 构模/加载 ----------

def build_or_load_model(args) -> Dict[str, Any]:
    """
    优先 --load-model 加载；否则从 --root 调用 pipeline.run_calibration_and_model 构建模型。
    """
    if args.load_model:
        with open(args.load_model, "r", encoding="utf-8") as f:
            return json.load(f)

    if not args.root:
        raise RuntimeError("缺少 --root 或 --load-model")

    params: Dict[str, Any] = {
        # 关键参数采用 pipeline 缺省或常用值
        "fs": int(args.fs),
        "n_per_oct": int(args.n_per_oct),
        "fmin_hz": float(args.fmin),
        "fmax_hz": float(args.fmax),
        "frame_sec": float(args.frame_sec),
        "hop_sec": float(args.hop_sec),
        "bands_filter_order": int(args.order),
        "use_fir_cpb": bool(args.use_fir),
        "fir_base_taps": int(args.fir_taps),
        # 可选导出诊断 CSV
        "dump_anchor_fit_csv": bool(args.dump_anchor_csv),
        "dump_anchor_fit_with_harmonics": True,
        "dump_anchor_fit_dir": str(args.dump_anchor_dir),
        # 谐波建模开关
        "harmonics_enable": True,
    }

    model, _rows = run_calibration_and_model(
        os.path.abspath(args.root),
        params=params,
        out_dir=os.path.abspath(args.workdir) if args.workdir else None,
        model_id=None,
        condition_id=None
    )

    if args.save_model:
        os.makedirs(os.path.dirname(os.path.abspath(args.save_model)) or ".", exist_ok=True)
        with open(args.save_model, "w", encoding="utf-8") as f:
            json.dump(model, f, ensure_ascii=False, indent=2)
        print(f"已保存模型到: {args.save_model}")

    return model

# ---------- CLI ----------

def _parse_track_csv(path: str, default_step: float) -> List[Tuple[float, float]]:
    """
    解析 CSV/文本轨迹：
      - 支持每行 "rpm,sec" 或 "rpm"
      - 有表头也可，无表头则按位置解析
    """
    import csv
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        # 尝试用 csv 读取；若失败退化为逐行 split
        try:
            sniffer = csv.Sniffer()
            sample = f.read(2048)
            f.seek(0)
            dialect = sniffer.sniff(sample)
            has_header = sniffer.has_header(sample)
            reader = csv.reader(f, dialect)
            if has_header:
                header = next(reader, None) or []
                header = [h.strip().lower() for h in header]
                def idx(name): 
                    return header.index(name) if name in header else -1
                ir = idx("rpm")
                isec = idx("sec") if idx("sec") >= 0 else (idx("secs") if idx("secs") >= 0 else idx("duration") if idx("duration") >= 0 else -1)
                for r in reader:
                    if not r: continue
                    try:
                        rpm = float(r[ir]) if ir >= 0 and ir < len(r) else float(r[0])
                        sec = float(r[isec]) if isec >= 0 and isec < len(r) else float(default_step)
                        rows.append((rpm, max(0.01, sec)))
                    except Exception:
                        continue
            else:
                for r in reader:
                    if not r: continue
                    try:
                        if len(r) >= 2:
                            rpm = float(r[0]); sec = float(r[1])
                        else:
                            rpm = float(r[0]); sec = float(default_step)
                        rows.append((rpm, max(0.01, sec)))
                    except Exception:
                        continue
        except Exception:
            # 简单逐行
            f.seek(0)
            for line in f:
                line = line.strip()
                if not line: continue
                parts = [p.strip() for p in line.replace(",", " ").split()]
                try:
                    if len(parts) >= 2:
                        rpm = float(parts[0]); sec = float(parts[1])
                    else:
                        rpm = float(parts[0]); sec = float(default_step)
                    rows.append((rpm, max(0.01, sec)))
                except Exception:
                    continue
    return rows

def main():
    ap = argparse.ArgumentParser(description="按任意转速合成拟合音频（基于 pipeline.py 模型）")
    ap.add_argument("--root", type=str, default="", help="音频数据根目录（需包含 env/ 与 sweep/）")
    ap.add_argument("--workdir", type=str, default="", help="可选：输出校准产物（calib.json 等）的目录")
    ap.add_argument("--load-model", type=str, default="", help="直接加载已保存的模型 JSON")
    ap.add_argument("--save-model", type=str, default="", help="可选：将构建出的模型保存到该 JSON 路径")

    # 合成目标
    ap.add_argument("--rpm", type=float, help="固定转速（与 --rpm-track 互斥）")
    ap.add_argument("--seconds", type=float, default=5.0, help="固定转速时的合成时长（秒）")
    ap.add_argument("--rpm-track", type=str, default="", help="转速轨迹 CSV：每行 rpm[,sec]；如无 sec 列，配合 --step-sec")
    ap.add_argument("--step-sec", type=float, default=0.1, help="当轨迹只给 rpm 时，每步的时长（秒）")
    ap.add_argument("--out", type=str, required=True, help="输出 WAV 路径")

    # 声音控制
    ap.add_argument("--fs", type=int, default=48000, help="采样率")
    ap.add_argument("--tones", dest="tones", action="store_true", help="将谐波做成正弦线谱并从噪声中扣除（默认开启）")
    ap.add_argument("--no-tones", dest="tones", action="store_false", help="禁用谐波线谱，全部用带限噪声合成")
    ap.set_defaults(tones=True)

    # 滤波器/频带网格（与 pipeline 对齐）
    ap.add_argument("--n-per-oct", type=int, default=12, help="每倍频程带数（建议与建模一致）")
    ap.add_argument("--fmin", type=float, default=20.0, help="最低中心频（Hz）")
    ap.add_argument("--fmax", type=float, default=20000.0, help="最高中心频（Hz）")
    ap.add_argument("--use-fir", action="store_true", help="使用 FIR 线性相位带通（默认使用 IIR Butterworth）")
    ap.add_argument("--order", type=int, default=6, help="IIR 带通阶数")
    ap.add_argument("--fir-taps", type=int, default=256, help="FIR 基准 taps")

    # 分帧参数（参与构模/诊断；轨迹模式非必须）
    ap.add_argument("--frame-sec", type=float, default=1.0, help="建模/诊断的帧长（秒）")
    ap.add_argument("--hop-sec", type=float, default=0.5, help="建模/诊断的帧移（秒）")

    # 诊断导出
    ap.add_argument("--dump-anchor-csv", action="store_true", help="导出锚点拟合对比 CSV（用于检查模型）")
    ap.add_argument("--dump-anchor-dir", type=str, default="anchor_fit_csv", help="锚点 CSV 输出子目录名")

    args = ap.parse_args()

    # 1) 构建或加载模型
    model = build_or_load_model(args)

    # 2) 合成
    fs = int(args.fs)
    if args.rpm_track:
        track = _parse_track_csv(args.rpm_track, default_step=float(args.step_sec))
        if not track:
            raise RuntimeError("轨迹文件为空或无法解析")
        y = synthesize_from_rpm_track(
            model, track, fs=fs,
            tones=bool(args.tones),
            use_fir_cpb=bool(args.use_fir),
            bands_filter_order=int(args.order),
            fir_base_taps=int(args.fir_taps),
        )
    else:
        if args.rpm is None:
            raise RuntimeError("请提供 --rpm 或 --rpm-track")
        y = synthesize_stationary_audio_from_model(
            model, float(args.rpm), seconds=float(args.seconds), fs=fs,
            tones=bool(args.tones),
            use_fir_cpb=bool(args.use_fir),
            bands_filter_order=int(args.order),
            fir_base_taps=int(args.fir_taps),
        )

    # 3) 写出 WAV
    _write_wav(args.out, y, fs)
    print(f"已写出: {args.out}  采样率={fs}Hz  时长={len(y)/fs:.3f}s")

if __name__ == "__main__":
    main()