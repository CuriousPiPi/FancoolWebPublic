# -*- coding: utf-8 -*-
"""
spectrum_reader: App-side helpers for the spectrum / audio track.

This module contains utilities used when generating or evaluating audio
spectrum data.  It is separate from:

* ``app/curves/perf_model_service`` — the authoritative entry point for
  fetching PCHIP performance-curve models (use that instead of this module
  for anything related to rpm / airflow / noise curves).
* ``admin`` — the admin-side spectrum_builder which contains calibration and
  import functions.

Backward-compat shims
---------------------
``get_perf_model``, ``get_perf_models``, and ``build_performance_pchips``
are re-exported here as thin wrappers around
``app.curves.perf_model_service`` so that call-sites that were updated to
use this module in a previous iteration continue to work.  New code should
import from ``perf_model_service`` directly.

Engine Injection
----------------
``set_engine(engine)`` propagates to ``perf_model_service`` / ``perf_raw_source``
so that a single call at startup wires up the full stack.
"""

from __future__ import annotations

from typing import Dict, Any, Optional, Tuple, List

from app.curves.pchip_cache import eval_pchip as _pchip_eval
from app.curves import perf_model_service as _perf_svc


# ---------------------------------------------------------------------------
# Engine injection — propagates to the perf-curve stack
# ---------------------------------------------------------------------------

def set_engine(engine) -> None:
    """Set the SQLAlchemy engine for the performance-curve stack.

    This call is forwarded to :mod:`app.curves.perf_model_service` (which in
    turn forwards it to :mod:`app.curves.perf_raw_source`).  A single call
    at application startup is sufficient.

    Args:
        engine: SQLAlchemy engine instance.
    """
    _perf_svc.set_engine(engine)


# ---------------------------------------------------------------------------
# Spectrum / audio helpers
# ---------------------------------------------------------------------------

def eval_pchip_model(model: Optional[Dict[str, Any]], x: float) -> float:
    """Evaluate a PCHIP model at *x*.

    Used by the audio / spectrum pipeline to interpolate values from a
    previously built PCHIP curve.

    Args:
        model: PCHIP model dictionary (``{"x": [...], "y": [...], "m": [...]}``)
        x: X value to evaluate.

    Returns:
        Interpolated y value, or ``float('nan')`` if *model* is invalid.
    """
    if not model or not isinstance(model, dict):
        return float("nan")
    try:
        return float(_pchip_eval(model, float(x)))
    except Exception:
        return float("nan")


# ---------------------------------------------------------------------------
# Backward-compat shims — delegate to perf_model_service
# ---------------------------------------------------------------------------

def get_perf_model(
    model_id: int, condition_id: int
) -> Optional[Dict[str, Any]]:
    """Load or rebuild the unified PCHIP model for one (model_id, condition_id).

    Deprecated: prefer importing from :mod:`app.curves.perf_model_service`
    directly.  This shim delegates to :func:`perf_model_service.get_perf_model`.
    """
    return _perf_svc.get_perf_model(model_id, condition_id)


def get_perf_models(
    pairs: List[Tuple[int, int]],
) -> Dict[str, Dict[str, Any]]:
    """Batch load / rebuild unified PCHIP models.

    Deprecated: prefer importing from :mod:`app.curves.perf_model_service`
    directly.  This shim delegates to :func:`perf_model_service.get_perf_models`.
    """
    return _perf_svc.get_perf_models(pairs)


def build_performance_pchips(
    pairs: List[Tuple[int, int]],
) -> Dict[str, Dict[str, Any]]:
    """Force-rebuild PCHIP performance models from the DB.

    Deprecated: prefer importing from :mod:`app.curves.perf_model_service`
    directly.  This shim delegates to :func:`perf_model_service.build_perf_models`.
    """
    return _perf_svc.build_perf_models(pairs)
