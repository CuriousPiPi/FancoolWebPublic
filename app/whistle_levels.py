import logging
import math
import os
from collections.abc import Mapping


_logger = logging.getLogger(__name__)


def load_whistle_color_metric(environ: Mapping[str, str] | None = None) -> str | None:
    source = os.environ if environ is None else environ
    metric = source.get('WHISTLE_COLOR_METRIC', 'dba').strip().lower()
    if metric not in ('dba', 'sone'):
        _logger.warning('Invalid WHISTLE_COLOR_METRIC=%r; whistle colors remain neutral', metric)
        return None
    return metric


WHISTLE_COLOR_METRIC = load_whistle_color_metric()


def load_whistle_low_tonality_threshold(environ: Mapping[str, str] | None = None) -> float | None:
    source = os.environ if environ is None else environ
    raw = source.get('WHISTLE_LOW_TONALITY_THRESHOLD')
    if raw is None:
        return 0.15
    try:
        value = float(raw)
    except (TypeError, ValueError):
        value = math.nan
    if not math.isfinite(value) or value < 0:
        _logger.warning('Invalid WHISTLE_LOW_TONALITY_THRESHOLD=%r; low whistle level disabled', raw)
        return None
    return value


def load_whistle_levels(environ: Mapping[str, str] | None = None) -> dict[str, float] | None:
    source = os.environ if environ is None else environ
    raw = (source.get('WHISTLE_LEVELS') or '').strip()
    if not raw:
        return {'low_mid': 2.0, 'mid_high': 4.0} if load_whistle_color_metric(source) == 'dba' else None

    try:
        values = [float(value.strip()) for value in raw.split(',')]
    except (TypeError, ValueError):
        _logger.warning('Invalid WHISTLE_LEVELS=%r; whistle levels remain neutral', raw)
        return None

    if len(values) != 2 or any(not math.isfinite(value) or value < 0 for value in values):
        _logger.warning('Invalid WHISTLE_LEVELS=%r; whistle levels remain neutral', raw)
        return None

    low, high = values
    if low >= high:
        _logger.warning('Invalid WHISTLE_LEVELS=%r; whistle levels remain neutral', raw)
        return None

    return {'low_mid': low, 'mid_high': high}
