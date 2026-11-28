"""Audio segment generation for sweep and fixed-speed recordings."""

import io
import logging
import numpy as np
import soundfile as sf
from scipy.signal import correlate
from typing import Dict, Any, Optional, List, Tuple

# 设置模块日志记录器 / Set up module logger
logger = logging.getLogger(__name__)

# 常量定义 / Constants
# 新的选择策略常量 / New selection strategy constants
SELECTION_TOL_RPM = 30.0      # 固定 RPM 容差（绝对值）/ Fixed RPM tolerance (absolute)
SELECTION_TOL_RATIO = 0.01    # 固定 RPM 容差（比例，1%）/ Fixed RPM tolerance (ratio, 1%)
TARGET_DURATION_SEC = 5.0     # 目标音频时长（秒）/ Target audio duration in seconds

LOOP_CROSSFADE_MS = 50.0     # 循环交叉淡化时长（毫秒）/ Loop crossfade duration in milliseconds
TRANSITION_CLIP_DURATION_SEC = 0.1
TRANSITION_SOURCE_INTERVAL_SEC = 5.0
TRANSITION_CROSSFADE_SEC = 0.05
TRANSITION_SOURCE_MATCH_MARGIN_RPM = 1.0
VALIDATION_SAMPLE_SIZE = 10   # 验证时检查的帧数 / Number of frames to check during validation
CORR_OFFSET_NONNEGATIVE_ONLY = True # loop 对齐策略开关 / Loop alignment switches

MIN_RELIABILITY = 0.01         # reliability 硬阈值 / Hard reliability threshold
MIN_CONTIGUOUS_FRAMES = 3     # 最少连续帧数 n / Minimum contiguous frames (n)

# 音频质量相关常量 / Audio quality related constants
EPSILON = 1e-6  # 极小值，用于避免除零 / Epsilon value to avoid division by zero


def detect_frame_format(frame_index: List[List], sweep_audio_meta: Optional[Dict[str, Any]] = None) -> str:
    """
    检测帧索引的格式类型。
    Detect the format type of frame index.
    
    支持的格式 / Supported formats:
    - "meta": [file_idx, frame_index, rpm, la, reliability] (需要 sweep_audio_meta)
    
    Args:
        frame_index: 帧索引列表 / Frame index list
        sweep_audio_meta: 音频元数据（必需）/ Audio metadata (required)
        
    Returns:
        格式类型字符串（始终为 "meta"）/ Format type string (always "meta")
        
    Raises:
        ValueError: 如果格式无法识别 / If format cannot be recognized
    """
    if not frame_index or not isinstance(frame_index, list) or len(frame_index) == 0:
        raise ValueError("帧索引为空或无效 / Frame index is empty or invalid")
    
    if sweep_audio_meta is None:
        raise ValueError(
            "缺少 sweep_audio_meta，仅支持元数据格式 [file_idx, frame_index, rpm, la, reliability] / "
            "Missing sweep_audio_meta, only meta format [file_idx, frame_index, rpm, la, reliability] is supported."
        )
    
    # 检查第一帧以确定格式 / Check first frame to determine format
    first_frame = frame_index[0]
    if not isinstance(first_frame, (list, tuple)):
        raise ValueError(f"帧格式无效：期望列表或元组，得到 {type(first_frame)} / Invalid frame format: expected list or tuple, got {type(first_frame)}")
    
    frame_len = len(first_frame)
    
    # 格式检测逻辑：仅支持元数据格式 / Format detection logic: only meta format supported
    if frame_len == 5:
        # 元数据格式 [file_idx, frame_index, rpm, la, reliability]
        # Meta format [file_idx, frame_index, rpm, la, reliability]
        return "meta"
    else:
        raise ValueError(
            f"帧格式不支持：期望 5 字段的元数据格式 [file_idx, frame_index, rpm, la, reliability]，得到 {frame_len} 字段 / "
            f"Frame format not supported: expected 5-field meta format [file_idx, frame_index, rpm, la, reliability], got {frame_len} fields."
        )


def parse_frame_data(frame: List, format_type: str, sweep_audio_meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    根据格式类型解析单个帧数据（仅支持 meta）。
    Parse single frame data according to meta format.

    Meta frame format:
      [file_idx, frame_index, rpm, la, reliability]

    注意：la 已不再解析/返回（当前播放器逻辑不使用）。
    Returns keys:
      - rpm
      - reliability
      - file_idx
      - start_sample
      - end_sample
    """
    if format_type != "meta":
        raise ValueError(f"不支持的格式类型: {format_type}，仅支持 'meta' / Unsupported format type: {format_type}, only 'meta' supported")

    if len(frame) < 5:
        raise ValueError(f"元数据格式需要至少 5 个字段，当前 {len(frame)} / Meta format needs at least 5 fields, got {len(frame)}")

    if not sweep_audio_meta:
        raise ValueError("元数据格式需要 sweep_audio_meta / Meta format requires sweep_audio_meta")

    file_idx = int(frame[0])
    frame_idx = int(frame[1])

    frame_len_samples = sweep_audio_meta.get('frame_len_samples')
    hop_samples = sweep_audio_meta.get('hop_samples')
    if not frame_len_samples or not hop_samples:
        raise ValueError("sweep_audio_meta 缺少 frame_len_samples 或 hop_samples / sweep_audio_meta missing frame_len_samples or hop_samples")

    start_sample = frame_idx * hop_samples
    end_sample = start_sample + frame_len_samples

    return {
        "file_idx": file_idx,
        "rpm": float(frame[2]),
        "reliability": float(frame[4]),
        "start_sample": int(start_sample),
        "end_sample": int(end_sample),
    }


def load_audio_segment(file_path: str, start_sample: int, end_sample: int, fs: int) -> np.ndarray:
    """
    从音频文件中读取指定采样范围的音频数据。
    Load audio samples from file within the specified range.
    
    Args:
        file_path: 音频文件路径 / Path to audio file
        start_sample: 起始采样点 / Start sample index
        end_sample: 结束采样点 / End sample index
        fs: 采样率 / Sample rate
        
    Returns:
        音频数据数组 (float32) / Audio data array (float32)
    """
    # 读取指定范围的音频数据
    # Read audio data in the specified range
    data, sr = sf.read(file_path, start=start_sample, stop=end_sample, dtype='float32')
    
    if sr != fs:
        raise ValueError(f"采样率不匹配：文件为 {sr} Hz，期望 {fs} Hz / Sample rate mismatch: file is {sr} Hz, expected {fs} Hz")
    
    # 提取第一声道，不做均值混合以保留原始电平
    # Extract first channel (no averaging) to preserve original level
    if data.ndim > 1:
        data = data[:, 0]
    
    return data.astype(np.float32)


def filter_frames_by_rpm_fixed_tolerance(
    frame_index: List[List[float]],
    target_rpm: float,
    sweep_audio_meta: Optional[Dict[str, Any]] = None,
) -> List[int]:
    """
    使用固定 RPM 容差过滤帧索引，并将 reliability 作为硬条件（固定使用 MIN_RELIABILITY）。
    Filter frame index using fixed RPM tolerance, with reliability as a hard constraint (fixed MIN_RELIABILITY).
    """
    if sweep_audio_meta is None:
        raise ValueError("缺少 sweep_audio_meta / Missing sweep_audio_meta")

    tol_absolute = SELECTION_TOL_RPM
    tol_relative = SELECTION_TOL_RATIO * target_rpm
    tolerance = max(tol_absolute, tol_relative)

    rpm_min = target_rpm - tolerance
    rpm_max = target_rpm + tolerance

    tolerance_percent = (tolerance / target_rpm * 100) if target_rpm > 0 else 0
    logger.info(
        "[sweep-audio] Fixed tolerance filtering: target=%.1f, tolerance=±%.1f RPM (±%.2f%%), min_reliability=%.2f / "
        "固定容差过滤: 目标=%.1f, 容差=±%.1f RPM (±%.2f%%), 最小可靠性=%.2f",
        target_rpm, tolerance, tolerance_percent, MIN_RELIABILITY,
        target_rpm, tolerance, tolerance_percent, MIN_RELIABILITY
    )

    filtered_indices: List[int] = []
    for idx, frame_data in enumerate(frame_index):
        if not isinstance(frame_data, (list, tuple)) or len(frame_data) < 5:
            continue

        try:
            # rpm/reliability 直接从 meta frame 里取，避免 parse_frame_data 冗余解析
            rpm = float(frame_data[2])
            reliability = float(frame_data[4])

            if not np.isfinite(rpm) or rpm <= 0:
                continue
            if not np.isfinite(reliability) or reliability < MIN_RELIABILITY:
                continue

            if rpm_min <= rpm <= rpm_max:
                filtered_indices.append(idx)

        except (ValueError, TypeError):
            continue

    logger.info(
        "[sweep-audio] Fixed tolerance: found %d frames in range [%.1f, %.1f] / "
        "固定容差: 在范围 [%.1f, %.1f] 内找到 %d 帧",
        len(filtered_indices), rpm_min, rpm_max,
        rpm_min, rpm_max, len(filtered_indices),
    )
    return filtered_indices

def group_filtered_frames_into_contiguous_runs(
    frame_index: List[List[float]],
    filtered_list_indices: List[int],
    sweep_audio_meta: Dict[str, Any]
) -> List[List[int]]:
    """
    将“容差命中”的帧（sweep_frame_index 下标）分组成连续段：
    - 同一 file_idx（直接取 meta frame[0]）
    - frame_index 字段连续（直接取 meta frame[1]，要求 +1）
    """
    if not filtered_list_indices:
        return []

    items: List[Tuple[int, int, int]] = []
    for li in filtered_list_indices:
        if li < 0 or li >= len(frame_index):
            continue
        fr = frame_index[li]
        if not isinstance(fr, (list, tuple)) or len(fr) < 2:
            continue
        try:
            file_idx = int(fr[0])
            frame_field = int(fr[1])
        except (ValueError, TypeError):
            continue
        items.append((file_idx, frame_field, li))

    items.sort(key=lambda x: (x[0], x[1]))

    runs: List[List[int]] = []
    current: List[int] = []
    prev_file: Optional[int] = None
    prev_frame_field: Optional[int] = None

    for file_idx, frame_field, li in items:
        if not current:
            current = [li]
            prev_file = file_idx
            prev_frame_field = frame_field
            continue

        is_same_file = (file_idx == prev_file)
        is_consecutive = (frame_field == (prev_frame_field + 1 if prev_frame_field is not None else frame_field))

        if is_same_file and is_consecutive:
            current.append(li)
        else:
            runs.append(current)
            current = [li]

        prev_file = file_idx
        prev_frame_field = frame_field

    if current:
        runs.append(current)

    logger.info(
        "[sweep-audio] Grouped filtered frames into %d contiguous runs / "
        "将过滤帧分组成 %d 个连续段",
        len(runs), len(runs)
    )
    return runs

def select_best_subclip_by_min_endpoint_rpm_diff(
    frame_index: List[List[float]],
    run_list_indices: List[int],
    sweep_audio_meta: Dict[str, Any],
    n: int,
    alpha_len: float = 2.0,
) -> Tuple[int, int, float]:
    """
    在单个连续段（run）中选择子片段 i..j（run 内位置），长度至少 n。
    评分函数：score = diff + alpha_len / length
      - diff = |rpm[j] - rpm[i]|
      - length = j - i + 1
      - alpha_len 控制“偏好长片段”的力度（alpha 越大，越惩罚短片段）
    返回 (best_i_pos, best_j_pos, best_diff)，i/j 为 run 内位置。
    """
    if len(run_list_indices) < n:
        raise ValueError("run length < n")

    rpms: List[float] = []
    for li in run_list_indices:
        p = parse_frame_data(frame_index[li], "meta", sweep_audio_meta)
        rpms.append(float(p["rpm"]))

    m = len(rpms)
    best_i = 0
    best_j = n - 1
    best_diff = float("inf")
    best_score = float("inf")
    best_len = 0

    for i in range(m):
        j_start = i + n - 1
        if j_start >= m:
            break
        for j in range(j_start, m):
            diff = abs(rpms[j] - rpms[i])
            length = j - i + 1
            score = diff + alpha_len / length

            if (score < best_score) or (score == best_score and length > best_len):
                best_score = score
                best_len = length
                best_diff = diff
                best_i, best_j = i, j

    return best_i, best_j, float(best_diff)

def find_best_clip_segments(
    frame_index: List[List[float]],
    filtered_list_indices: List[int],
    sweep_audio_meta: Dict[str, Any],
    n: int
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    在容差命中帧里，按“同文件 + frame_index字段连续”分段；
    仅保留长度>=n的段；在每段内找端点RPM差最小的子片段；
    最终在所有段的候选子片段中选择全局最优（端点差最小）。

    与旧版本不同：
    - 不再返回“每帧一个 segment 列表”，而是把选中的子片段合并为一个连续音频区间 segment。
    """
    runs = group_filtered_frames_into_contiguous_runs(frame_index, filtered_list_indices, sweep_audio_meta)
    runs = [r for r in runs if len(r) >= n]
    if not runs:
        raise ValueError(f"没有找到长度>= {n} 的连续容差命中段 / No contiguous run length >= {n} found")

    best = None

    for run in runs:
        bi, bj, diff = select_best_subclip_by_min_endpoint_rpm_diff(frame_index, run, sweep_audio_meta, n)
        chosen_list_indices = run[bi:bj + 1]
        if not chosen_list_indices:
            continue

        try:
            merged = build_merged_segment_from_list_indices(frame_index, chosen_list_indices, sweep_audio_meta)
        except Exception as e:
            logger.warning("[sweep-audio] Failed to build merged segment: %s", e)
            continue

        cand = {
            "diff": float(diff),
            "run_len": int(len(run)),
            "clip_len_frames": int(len(chosen_list_indices)),
            "file_idx": int(merged["file_idx"]),
            "start_list_index": int(merged["list_index_start"]),
            "end_list_index": int(merged["list_index_end"]),
            "start_frame_index_field": int(merged["frame_index_field_start"]),
            "end_frame_index_field": int(merged["frame_index_field_end"]),
            "start_rpm": float(merged["rpm_start"]),
            "end_rpm": float(merged["rpm_end"]),
            "segment": merged,
        }

        if best is None or cand["diff"] < best["diff"]:
            best = cand

    if best is None:
        raise ValueError("无法从连续段中选出有效片段 / Failed to select valid clip from runs")

    merged_segment = best["segment"]
    debug_info = {k: v for k, v in best.items() if k != "segment"}
    debug_info.update({
        "merged_start_sample": int(merged_segment["start_sample"]),
        "merged_end_sample": int(merged_segment["end_sample"]),
        "reliability_min": float(merged_segment.get("reliability_min", float("nan"))),
    })
    return merged_segment, debug_info

def _find_best_offset_by_correlation(
    tail: np.ndarray,
    head: np.ndarray,
    max_shift: int,
    fade_samples: int,
    nonnegative_only: bool = False,
) -> int:
    """Find the best normalized correlation over the exact overlap to be mixed."""
    fade = int(min(fade_samples, len(tail), len(head)))
    if max_shift <= 0 or fade < 16:
        return 0

    def scores(reference: np.ndarray, candidates: np.ndarray) -> np.ndarray:
        reference = np.asarray(reference, dtype=np.float64)
        candidates = np.asarray(candidates, dtype=np.float64)
        reference = reference - np.mean(reference)
        reference_energy = np.dot(reference, reference)
        count = len(candidates) - fade + 1
        if reference_energy <= fade * EPSILON ** 2:
            return np.zeros(count)

        candidates = candidates - np.mean(candidates)
        sums = np.concatenate(([0.0], np.cumsum(candidates)))
        squares = np.concatenate(([0.0], np.cumsum(candidates ** 2)))
        window_sums = sums[fade:] - sums[:-fade]
        energies = np.maximum(
            squares[fade:] - squares[:-fade] - window_sums ** 2 / fade, 0.0
        )
        result = np.full(count, -np.inf)
        np.divide(
            correlate(candidates, reference, mode='valid', method='auto'),
            np.sqrt(reference_energy * energies),
            out=result,
            where=energies > fade * EPSILON ** 2,
        )
        return result

    positive_shift = min(int(max_shift), len(head) - fade)
    positive_scores = scores(tail[-fade:], head[:fade + positive_shift])
    best_off = int(np.argmax(positive_scores))
    best_score = positive_scores[best_off]
    if not nonnegative_only:
        negative_shift = min(int(max_shift), len(tail) - fade)
        negative_scores = scores(head[:fade], tail[-fade - negative_shift:])[::-1]
        cut = int(np.argmax(negative_scores))
        if negative_scores[cut] > best_score:
            best_off = -cut
            best_score = negative_scores[cut]
    return best_off if np.isfinite(best_score) else 0

def apply_loop_crossfade_with_alignment(
    audio: np.ndarray,
    loop_clip: np.ndarray,
    fade_samples: int,
    use_correlation_alignment: bool = True,
    corr_max_shift_samples: Optional[int] = None
) -> np.ndarray:
    """
    将 loop_clip 追加到 audio 的末尾，使用 loop crossfade（fade_samples），并可选相关性对齐。

    对齐策略：
    - CORR_OFFSET_NONNEGATIVE_ONLY：
        True  -> offset 搜索限制为 >=0（只裁 loop_clip 头部，不裁 audio 尾部）
        False -> offset 允许为负；当 offset<0 时，裁 audio 尾部（不再给 loop_clip 头部补零）
    """
    if len(audio) == 0:
        return loop_clip
    if len(loop_clip) == 0:
        return audio

    # Keep an untouched tail for the next join and guarantee forward progress.
    actual_fade = min(int(fade_samples), len(audio), len(loop_clip) // 2)
    max_shift = 0
    if use_correlation_alignment and actual_fade >= 2:
        requested_shift = corr_max_shift_samples
        if requested_shift is None:
            requested_shift = max(1, actual_fade // 4)
        max_shift = max(0, min(int(requested_shift), len(loop_clip) // 4))
        # Reserve alignment room even when the requested fade exceeds the clip.
        reserved_shift = max_shift * (1 if CORR_OFFSET_NONNEGATIVE_ONLY else 2)
        actual_fade = min(actual_fade, (len(loop_clip) - reserved_shift) // 2)
        if not CORR_OFFSET_NONNEGATIVE_ONLY:
            max_shift = min(max_shift, max(0, len(audio) - 2 * actual_fade))
    logger.info("[sweep-audio] actual_fade=%d (fade_samples=%d, audio_len=%d, loop_len=%d)",
            int(actual_fade), int(fade_samples), len(audio), len(loop_clip))
    if actual_fade < 2:
        return np.concatenate([audio, loop_clip])

    # correlation alignment
    offset = 0
    if use_correlation_alignment:
        offset = _find_best_offset_by_correlation(
            audio[-actual_fade - max_shift:],
            loop_clip[:actual_fade + max_shift],
            max_shift,
            actual_fade,
            nonnegative_only=CORR_OFFSET_NONNEGATIVE_ONLY,
        )

    # 应用 offset：
    # offset > 0: 裁掉 loop_clip 头部 offset 个采样，使 head 延后对齐
    if offset > 0:
        shifted_loop = loop_clip[offset:]
        return apply_crossfade(audio, shifted_loop, actual_fade)

    # offset < 0: 不再给 loop_clip 补零，而是裁 audio 尾部（让 audio 更“提前结束”去匹配 loop_clip）
    if offset < 0:
        cut = -offset
        if cut >= len(audio):
            # 极端情况，audio 被裁没了，直接退化为拼接
            return apply_crossfade(audio, loop_clip, actual_fade)

        trimmed_audio = audio[:-cut]
        # 裁剪后重新计算可用 fade
        new_fade = min(actual_fade, len(trimmed_audio), len(loop_clip))
        if new_fade < 2:
            return np.concatenate([trimmed_audio, loop_clip])
        return apply_crossfade(trimmed_audio, loop_clip, new_fade)

    # offset == 0
    return apply_crossfade(audio, loop_clip, actual_fade)

def loop_stitch_to_min_duration(
    loop_clip: np.ndarray,
    fs: int,
    min_duration_sec: float,
    loop_crossfade_ms: float = LOOP_CROSSFADE_MS,
    use_correlation_alignment: bool = True,
    corr_max_shift_samples: Optional[int] = None, 
) -> np.ndarray:
    """
    将 loop_clip 循环拼接到总时长 >= min_duration_sec，使用 loop_crossfade_ms 做循环边界淡化。
    不裁剪超过部分。
    """
    if len(loop_clip) == 0:
        raise ValueError("loop_clip is empty")

    if len(loop_clip) / fs >= min_duration_sec:
        return loop_clip

    fade_samples = int(loop_crossfade_ms * fs / 1000.0)
    fade_samples = max(2, fade_samples)

    target_samples = int(min_duration_sec * fs)

    out = loop_clip.copy()
    while len(out) < target_samples:
        out = apply_loop_crossfade_with_alignment(
            out,
            loop_clip,
            fade_samples=fade_samples,
            use_correlation_alignment=use_correlation_alignment,
            corr_max_shift_samples=corr_max_shift_samples,  # 透传
        )
    return out

def apply_crossfade(audio1: np.ndarray, audio2: np.ndarray, fade_samples: int) -> np.ndarray:
    """
    应用相关性补偿的平滑交叉淡化，不改变重叠区以外的录音电平。
    Use a smooth, correlation-aware crossfade without changing the source level
    outside the overlap. Uncorrelated signals use equal-power gains; correlated
    signals approach complementary gains to avoid a 3 dB level bump.

    Args:
        audio1: 第一个音频段 / First audio segment
        audio2: 第二个音频段 / Second audio segment
        fade_samples: 交叉淡化长度（采样点数）/ Crossfade length in samples

    Returns:
        拼接后的音频 / Stitched audio
    """
    if audio1 is None or len(audio1) == 0:
        return audio2
    if audio2 is None or len(audio2) == 0:
        return audio1

    # 实际淡化长度不能超过任一段长度
    fade = int(min(fade_samples, len(audio1), len(audio2)))
    if fade < 2:
        return np.concatenate([audio1, audio2])

    a = np.asarray(audio1[-fade:], dtype=np.float64)
    b = np.asarray(audio2[:fade], dtype=np.float64)
    energy = np.sqrt(np.dot(a, a) * np.dot(b, b))
    correlation = float(np.clip(np.dot(a, b) / energy, 0.0, 1.0)) if energy > 0 else 0.0

    t = np.linspace(0.0, np.pi, fade, dtype=np.float32)
    fade_in = (0.5 - 0.5 * np.cos(t)).astype(np.float32)
    fade_out = 1.0 - fade_in
    # Do not boost anti-correlated signals to compensate for cancellation.
    normalization = np.sqrt(
        fade_out ** 2 + fade_in ** 2 + 2.0 * correlation * fade_out * fade_in
    )
    fade_out /= normalization
    fade_in /= normalization

    # 重叠区
    a_pre = audio1[:-fade]
    a_ov = audio1[-fade:] * fade_out
    b_ov = audio2[:fade] * fade_in
    b_post = audio2[fade:]

    return np.concatenate([a_pre, a_ov + b_ov, b_post])

def create_wav_bytes(audio_data: np.ndarray, fs: int) -> bytes:
    """
    将音频数据转换为 WAV 格式的字节流。
    Convert audio data to WAV format bytes.

    Output format: 32-bit float WAV, 2 channels (stereo).
    The mono source buffer is duplicated to both L and R channels so that
    output is stereo with identical left/right content.  No gain change or
    clipping is applied; the float32 pipeline is preserved end-to-end.

    Args:
        audio_data: 音频数据数组 (float32, -1.0 到 1.0) / Audio data array (float32, -1.0 to 1.0)
        fs: 采样率 / Sample rate

    Returns:
        WAV 格式的字节流 / WAV format bytes
    """
    # 将单声道缓冲区复制为立体声（L/R 相同），输出 32-bit float WAV
    # Duplicate mono buffer to stereo (identical L/R), write as 32-bit float WAV
    mono = audio_data.astype(np.float32)
    stereo = np.stack([mono, mono], axis=1)  # shape: (N, 2)

    wav_buffer = io.BytesIO()
    sf.write(wav_buffer, stereo, fs, format='WAV', subtype='FLOAT')
    wav_buffer.seek(0)
    return wav_buffer.read()


def generate_short_audio(file_path: str) -> Tuple[bytes, Dict[str, Any]]:
    info = sf.info(file_path)
    if info.frames <= 0 or info.samplerate <= 0:
        raise ValueError("Recording contains no audio samples")
    count = min(info.frames, int(TARGET_DURATION_SEC * info.samplerate))
    start = (info.frames - count) // 2
    audio = load_audio_segment(file_path, start, start + count, info.samplerate)
    if len(audio) != count:
        raise ValueError("Recording ended before the selected segment")
    return create_wav_bytes(audio, info.samplerate), {
        "duration_sec": count / info.samplerate,
        "sample_rate": info.samplerate,
        "start_sample": start,
        "num_samples": count,
    }


def generate_sweep_audio(
    model_json: Dict[str, Any],
    target_rpm: float,
    duration_sec: Optional[float] = None,
    corr_max_shift_samples: Optional[int] = None, 
) -> Tuple[bytes, Dict[str, Any]]:
    if not model_json:
        raise ValueError("模型 JSON 为空 / Model JSON is empty")
    if target_rpm <= 0:
        raise ValueError(f"目标 RPM 必须为正数，当前值: {target_rpm} / Target RPM must be positive, got: {target_rpm}")

    if duration_sec is not None:
        logger.warning(
            "[sweep-audio] duration_sec parameter is deprecated and will be ignored. Using fixed TARGET_DURATION_SEC=%.1fs. / "
            "duration_sec 参数已弃用且将被忽略。使用固定 TARGET_DURATION_SEC=%.1f秒。",
            TARGET_DURATION_SEC, TARGET_DURATION_SEC
        )

    sweep_frame_index = model_json.get('sweep_frame_index')
    if not sweep_frame_index or not isinstance(sweep_frame_index, list):
        raise ValueError("模型 JSON 中缺少或无效的 sweep_frame_index / sweep_frame_index missing or invalid")

    sweep_audio_meta = model_json.get('sweep_audio_meta')
    if not sweep_audio_meta:
        raise ValueError("模型 JSON 中缺少 sweep_audio_meta / sweep_audio_meta missing in model JSON")

    frame_format = detect_frame_format(sweep_frame_index, sweep_audio_meta)

    if 'files' not in sweep_audio_meta or not sweep_audio_meta['files']:
        raise ValueError("元数据格式需要 sweep_audio_meta.files / Meta format requires sweep_audio_meta.files")

    files = sweep_audio_meta['files']
    fs = (files[0].get('fs') if files else None) or sweep_audio_meta.get('fs')
    fallback_file_path = files[0].get('file_path') if files else None
    if not fs or fs <= 0:
        raise ValueError("sweep_audio_meta 中缺少有效的 fs / sweep_audio_meta missing valid fs")
    if not fallback_file_path:
        raise ValueError("sweep_audio_meta.files 中没有有效的文件路径 / No valid file path in sweep_audio_meta.files")

    # 根据目标转速自动设定最大对齐移位（默认 1.5 个周期的样本数）
    if corr_max_shift_samples is None:
        samples_per_rev = int(fs * 60.0 / target_rpm)
        corr_max_shift_samples = max(1, int(samples_per_rev * 1.5))

    filtered_list_indices = filter_frames_by_rpm_fixed_tolerance(
        sweep_frame_index,
        target_rpm,
        sweep_audio_meta=sweep_audio_meta,
    )
    if not filtered_list_indices:
        raise ValueError(
            "无法匹配到有效的音频数据（容差/可靠性过滤后为空），请尝试其他转速或噪音值。/ "
            "Cannot match valid audio data after tolerance/reliability filtering."
        )

    merged_segment, best_debug = find_best_clip_segments(
        sweep_frame_index,
        filtered_list_indices,
        sweep_audio_meta,
        n=MIN_CONTIGUOUS_FRAMES
    )

    loop_clip_audio = load_audio_segment(
        merged_segment["file_path"],
        int(merged_segment["start_sample"]),
        int(merged_segment["end_sample"]),
        fs
    )

    loop_clip_duration = len(loop_clip_audio) / fs
    logger.info(
        "[sweep-audio] Selected merged clip: file_idx=%s, frames=%d, duration=%.3fs, endpoint_rpm_diff=%.3f / "
        "选择合并片段: file_idx=%s, 帧数=%d, 时长=%.3f秒, 首尾RPM差=%.3f",
        best_debug.get("file_idx"), best_debug.get("clip_len_frames"), loop_clip_duration, best_debug.get("diff"),
        best_debug.get("file_idx"), best_debug.get("clip_len_frames"), loop_clip_duration, best_debug.get("diff")
    )

    if loop_clip_duration >= TARGET_DURATION_SEC:
        final_audio = loop_clip_audio
        did_loop = False
    else:
        final_audio = loop_stitch_to_min_duration(
            loop_clip_audio,
            fs=fs,
            min_duration_sec=TARGET_DURATION_SEC,
            loop_crossfade_ms=LOOP_CROSSFADE_MS,
            use_correlation_alignment=True,
            corr_max_shift_samples=corr_max_shift_samples,
        )
        did_loop = True

    final_duration_sec = len(final_audio) / fs
    wav_bytes = create_wav_bytes(final_audio, fs)

    metadata = {
        "target_rpm": float(target_rpm),
        "duration_sec": float(final_duration_sec),
        "sample_rate": int(fs),
        "frame_format": frame_format,
        "tolerance_used": float(max(SELECTION_TOL_RPM, SELECTION_TOL_RATIO * target_rpm)),
        "min_reliability": float(MIN_RELIABILITY),
        "n_min_contiguous": int(MIN_CONTIGUOUS_FRAMES),
        "num_frames_filtered": int(len(filtered_list_indices)),
        "corr_max_shift_samples_used": int(corr_max_shift_samples),
        "selected_clip": {
            **best_debug,
            "clip_duration_sec": float(loop_clip_duration),
            "loop_crossfade_ms": float(LOOP_CROSSFADE_MS),
            "did_loop_stitch": bool(did_loop),
        }
    }

    return wav_bytes, metadata


def _transition_clip_centers(
    valid_frames: Dict[int, float], source: int, target: int, hop: int,
    frame_len: int, file_samples: int, clip_length: int, interval: int,
) -> List[int]:
    """Choose centers no farther apart than interval, avoiding invalid frame windows."""
    last_frame = (file_samples - frame_len) // hop
    indices = sorted(i for i in valid_frames if 0 <= i <= last_frame)
    runs = []
    for index in indices:
        if runs and index == runs[-1][1] + 1:
            runs[-1][1] = index
        else:
            runs.append([index, index])

    spans = []
    before = clip_length // 2
    after = clip_length - before
    for first, last in runs:
        start = first * hop
        end = last * hop + frame_len
        if first > 0:
            start = max(start, (first - 1) * hop + frame_len)
        if last < last_frame:
            end = min(end, (last + 1) * hop)
        left, right = start + before, end - after
        if left <= right:
            spans.append((left, right))

    direction = 1 if target >= source else -1
    current = source
    centers = []
    while abs(target - current) > interval:
        if len(centers) >= 2000:
            raise ValueError("Sweep transition is too long")
        candidates = []
        for left, right in spans:
            if direction > 0:
                low = max(left, current + 1)
                high = min(right, current + interval, target - 1)
                if low <= high:
                    candidates.append(high)
            else:
                low = max(left, current - interval, target + 1)
                high = min(right, current - 1)
                if low <= high:
                    candidates.append(low)
        if not candidates:
            raise ValueError(
                "无法在最大时间间隔内找到有效的过渡片段 / "
                "No reliable transition clip within the maximum source interval"
            )
        current = max(candidates) if direction > 0 else min(candidates)
        centers.append(current)
    return centers


def generate_sweep_transition(
    model_json: Dict[str, Any], source_rpm: float, target_rpm: float,
) -> Tuple[bytes, Dict[str, Any]]:
    """Generate an ordered sweep transition followed by the target loop."""
    if not np.isfinite(source_rpm) or source_rpm <= 0 or not np.isfinite(target_rpm) or target_rpm <= 0:
        raise ValueError("Source and target RPM must be finite and positive")
    if not (np.isfinite(TRANSITION_CLIP_DURATION_SEC) and TRANSITION_CLIP_DURATION_SEC > 0 and
            np.isfinite(TRANSITION_SOURCE_INTERVAL_SEC) and TRANSITION_SOURCE_INTERVAL_SEC > 0):
        raise ValueError("Invalid sweep transition configuration")

    target_wav, metadata = generate_sweep_audio(model_json, target_rpm)
    target, fs = sf.read(io.BytesIO(target_wav), dtype="float32", always_2d=True)
    target = target[:, 0]
    meta = model_json["sweep_audio_meta"]
    files = meta["files"]
    selected = metadata["selected_clip"]
    file_idx = selected["file_idx"]
    if file_idx < 0 or file_idx >= len(files):
        raise ValueError("Invalid sweep recording index")
    path = files[file_idx]["file_path"]
    hop = int(meta["hop_samples"])
    frame_len = int(meta["frame_len_samples"])
    if hop <= 0 or frame_len <= 0 or hop > frame_len:
        raise ValueError("Invalid sweep frame timing")
    info = sf.info(path)
    if info.samplerate != fs:
        raise ValueError("Sweep recording sample rate changed")

    frames = {}
    for frame in model_json["sweep_frame_index"]:
        try:
            if int(frame[0]) != file_idx:
                continue
            index, rpm, reliability = int(frame[1]), float(frame[2]), float(frame[4])
            if (index >= 0 and index * hop + frame_len <= info.frames and
                    np.isfinite(rpm) and rpm > 0 and np.isfinite(reliability) and reliability >= MIN_RELIABILITY):
                frames[index] = rpm
        except (ValueError, TypeError, IndexError):
            continue
    if not frames:
        raise ValueError("No reliable frames for transition")
    tolerance = max(SELECTION_TOL_RPM, SELECTION_TOL_RATIO * source_rpm)
    candidates = [i for i, rpm in frames.items() if abs(rpm - source_rpm) <= tolerance]
    if not candidates:
        raise ValueError("Cannot match the current RPM in the target recording")
    target_index = round((selected["start_frame_index_field"] + selected["end_frame_index_field"]) / 2)
    best_error = min(abs(frames[i] - source_rpm) for i in candidates)
    source_index = min(
        (i for i in candidates if abs(frames[i] - source_rpm) <= best_error + TRANSITION_SOURCE_MATCH_MARGIN_RPM),
        key=lambda i: (abs(i - target_index), abs(frames[i] - source_rpm), i),
    )

    interval = int(TRANSITION_SOURCE_INTERVAL_SEC * fs)
    clip_length = round(TRANSITION_CLIP_DURATION_SEC * fs)
    if interval < 1 or clip_length < 2:
        raise ValueError("Sweep transition configuration is shorter than the audio sample resolution")
    source_sample = source_index * hop + frame_len // 2
    target_sample = target_index * hop + frame_len // 2
    centers = _transition_clip_centers(
        frames, source_sample, target_sample, hop, frame_len, info.frames, clip_length, interval,
    )
    clips = []
    for center in centers:
        start = center - clip_length // 2
        if start < 0 or start + clip_length > info.frames:
            raise ValueError("Sweep transition clip exceeds the recording")
        clip = load_audio_segment(path, start, start + clip_length, fs)
        if len(clip) != clip_length:
            raise ValueError("Sweep recording ended during transition")
        clips.append(clip)
    fade = min(round(TRANSITION_CROSSFADE_SEC * fs), clip_length // 2)
    if clips and fade >= 2:
        parts = [clips[0][:-fade]]
        tail = clips[0][-fade:]
        for clip in clips[1:]:
            joined = apply_crossfade(tail, clip, fade)
            parts.append(joined[:-fade])
            tail = joined[-fade:]
        parts.append(tail)
        transition = np.concatenate(parts)
    else:
        transition = np.concatenate(clips) if clips else np.empty(0, dtype=np.float32)
    combined = np.concatenate((transition, target))
    metadata["transition_duration_sec"] = len(transition) / fs
    metadata["duration_sec"] = len(combined) / fs
    return create_wav_bytes(combined, fs), metadata

def validate_model_has_frame_index(model_json: Dict[str, Any]) -> bool:
    """
    验证模型 JSON 是否包含有效的 sweep_frame_index（帧级索引）。
    Validate if model JSON contains valid sweep_frame_index (frame-level index).
    
    仅支持元数据格式 / Only supports meta format:
    - Meta format: [file_idx, frame_idx, rpm, la, reliability] (5 fields) + sweep_audio_meta required
    
    Args:
        model_json: 模型 JSON / Model JSON
        
    Returns:
        是否有效 / Whether valid
    """
    if not model_json or not isinstance(model_json, dict):
        logger.warning("[sweep-audio] validate: model_json is empty or not dict")
        return False
    
    # 检查新格式：sweep_frame_index / Check new format: sweep_frame_index
    sweep_frame_index = model_json.get('sweep_frame_index')
    if not sweep_frame_index or not isinstance(sweep_frame_index, list):
        logger.warning("[sweep-audio] validate: sweep_frame_index is missing or not a list")
        return False
    
    if len(sweep_frame_index) == 0:
        logger.warning("[sweep-audio] validate: sweep_frame_index is empty")
        return False
    
    # Get sweep_audio_meta (required for meta format)
    sweep_audio_meta = model_json.get('sweep_audio_meta')
    if not sweep_audio_meta:
        logger.warning("[sweep-audio] validate: sweep_audio_meta is missing (required for meta format)")
        return False
    
    # 检查至少有一帧有效数据 / Check at least one frame has valid data
    # Only meta format is supported: [file_idx, frame_idx, rpm, la, reliability]
    for frame_data in sweep_frame_index[:min(VALIDATION_SAMPLE_SIZE, len(sweep_frame_index))]:
        if not isinstance(frame_data, (list, tuple)):
            continue
        
        frame_len = len(frame_data)
        
        # Meta format: [file_idx, frame_idx, rpm, la, reliability]
        if frame_len == 5:
            rpm_idx = 2  # RPM is at index 2 in meta format
            
            # Check if RPM is valid
            if rpm_idx < len(frame_data):
                rpm = frame_data[rpm_idx]
                if isinstance(rpm, (int, float)) and rpm > 0:
                    logger.info("[sweep-audio] validate: valid meta format frame detected, rpm=%.1f", rpm)
                    return True
        else:
            logger.debug("[sweep-audio] validate: frame with %d fields detected, expected 5 for meta format", frame_len)
            continue
    
    logger.warning("[sweep-audio] validate: no valid meta format frames found in sweep_frame_index")
    return False

def build_merged_segment_from_list_indices(
    frame_index: List[List],
    list_indices: List[int],
    sweep_audio_meta: Dict[str, Any]
) -> Dict[str, Any]:
    """
    将“连续帧片段”（同 file_idx + frame_index_field 连续）合并为一个连续的音频区间，
    直接截取 [first.start_sample, last.end_sample]，避免 frame_len_samples > hop_samples 时的重叠重复播放，
    从而减少响度调制与拼接痕迹。

    Returns:
        segment dict:
          - start_sample
          - end_sample
          - file_path
          - file_idx
          - list_index_start / list_index_end
          - frame_index_field_start / frame_index_field_end
          - rpm_start / rpm_end
          - reliability_min
    """
    if not list_indices:
        raise ValueError("list_indices is empty")

    files = sweep_audio_meta.get('files') or []
    if not files:
        raise ValueError("sweep_audio_meta.files is required")

    # list_indices 可能未排序；这里按 sweep_frame_index 下标排序后再取两端
    lis = sorted(int(i) for i in list_indices)
    first_li = lis[0]
    last_li = lis[-1]

    # 取 meta frame 直接字段（避免重复 parse）
    first_fr = frame_index[first_li]
    last_fr = frame_index[last_li]

    if not (isinstance(first_fr, (list, tuple)) and len(first_fr) >= 5):
        raise ValueError(f"invalid first frame at {first_li}")
    if not (isinstance(last_fr, (list, tuple)) and len(last_fr) >= 5):
        raise ValueError(f"invalid last frame at {last_li}")

    file_idx_first = int(first_fr[0])
    file_idx_last = int(last_fr[0])
    if file_idx_first != file_idx_last:
        raise ValueError("list_indices span multiple file_idx; expected single file run")

    file_idx = file_idx_first
    if not (0 <= file_idx < len(files)):
        raise ValueError(f"file_idx out of range: {file_idx}")

    file_path = files[file_idx].get('file_path')
    if not file_path:
        raise ValueError(f"missing file_path for file_idx={file_idx}")

    # 用 parse_frame_data 计算 start/end_sample（依赖 hop/frame_len），这部分保留以保证一致性
    first_parsed = parse_frame_data(first_fr, "meta", sweep_audio_meta)
    last_parsed = parse_frame_data(last_fr, "meta", sweep_audio_meta)

    start_sample = int(first_parsed["start_sample"])
    end_sample = int(last_parsed["end_sample"])
    if end_sample <= start_sample:
        raise ValueError("merged segment has non-positive length")

    # 汇总一些 debug 信息（rpm/reliability 直接从 meta frame 取）
    reliability_vals = []
    for li in lis:
        fr = frame_index[li]
        if isinstance(fr, (list, tuple)) and len(fr) >= 5:
            try:
                reliability_vals.append(float(fr[4]))
            except (ValueError, TypeError):
                # Ignore frames with non-numeric reliability; they are simply excluded
                # from the aggregated reliability statistics.
                pass

    reliability_min = float(min(reliability_vals)) if reliability_vals else float("nan")

    return {
        "start_sample": start_sample,
        "end_sample": end_sample,
        "file_path": file_path,
        "file_idx": file_idx,
        "list_index_start": first_li,
        "list_index_end": last_li,
        "frame_index_field_start": int(first_fr[1]),
        "frame_index_field_end": int(last_fr[1]),
        "rpm_start": float(first_fr[2]),
        "rpm_end": float(last_fr[2]),
        "reliability_min": reliability_min,
    }