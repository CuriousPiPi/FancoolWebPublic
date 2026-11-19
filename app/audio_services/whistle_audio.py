"""Resolve public whistle measurements to their bound original recordings."""

import hashlib
import logging
import os

import soundfile as sf

from app import whistle_value_service
from app.audio_services.audio_player import generate_short_audio

logger = logging.getLogger(__name__)


class WhistleAudioUnavailable(ValueError):
    pass


def _recording_path(batch_path, relative_path):
    root = os.path.realpath(batch_path)
    if os.path.isabs(relative_path):
        raise ValueError("Recording path must be relative")
    # Upload indexes paths relative to the logical root, stripping a sole ZIP wrapper folder.
    with os.scandir(root) as entries:
        children = list(entries)
    logical_root = root
    if len(children) == 1 and children[0].is_dir():
        logical_root = children[0].path
    path = os.path.realpath(os.path.join(logical_root, relative_path))
    if os.path.commonpath([root, path]) != root:
        raise ValueError("Recording path escapes its batch")
    return path


def generate_whistle_audio(fetch_all, model_id):
    _, _, point_map = whistle_value_service.derive_whistle_values_for_models(
        fetch_all, [model_id], include_sources=True, include_baseline=False, logger=logger,
    )
    points = point_map.get(model_id, [])
    _, condition_id = whistle_value_service._get_condition_config()
    for point in points:
        if not point.get('selected') or not point.get('batch_id'):
            continue
        files = fetch_all("""
            SELECT ab.fs_path, af.rel_path, af.sha256
            FROM audio_perf_binding b
            JOIN audio_batch ab ON ab.audio_batch_id = b.audio_batch_id
            JOIN audio_file af ON af.audio_batch_id = ab.audio_batch_id
            WHERE b.perf_batch_id = :batch_id
              AND b.model_id = :model_id AND b.condition_id = :condition_id
              AND ab.fs_state = 'present'
              AND af.file_type = 'audio' AND af.rpm = :rpm
            ORDER BY ab.audio_batch_id, af.rel_path
        """, {
            'batch_id': point['batch_id'], 'model_id': model_id,
            'condition_id': condition_id, 'rpm': point['source_rpm'],
        })
        for item in files:
            try:
                path = _recording_path(item['fs_path'], item['rel_path'])
                wav, metadata = generate_short_audio(path)
            except (OSError, sf.LibsndfileError, ValueError) as exc:
                logger.warning("Unavailable whistle recording model=%s rpm=%s: %s",
                               model_id, point['source_rpm'], exc)
                continue
            metadata.update({
                'rpm': point['source_rpm'],
                'condition_id': condition_id,
                'tonality_tu_HMS': point['tonality'],
                'noise_db': point.get('noise_db'),
                'airflow_cfm': point.get('airflow_cfm'),
                'tonality_is_multi_file_median': True,
                'source_version': hashlib.sha256(wav).hexdigest(),
            })
            return wav, metadata
    raise WhistleAudioUnavailable("No original whistle recording is available")
