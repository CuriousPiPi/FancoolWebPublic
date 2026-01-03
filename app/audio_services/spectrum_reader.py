# -*- coding: utf-8 -*-
"""
spectrum_reader: App-side functions for reading and building performance PCHIP models.

This module contains functions that are used by the application to read performance
data from the database and build PCHIP interpolation models. It is separate from the
admin-side spectrum_builder which contains calibration-specific functions.

Engine Injection:
    This module does NOT create its own SQLAlchemy engine. Instead, the engine must
    be injected by the caller using set_engine(engine) before any database operations.
    
    Example:
        from app.audio_services import spectrum_reader
        spectrum_reader.set_engine(my_engine)
        models = spectrum_reader.build_performance_pchips(pairs)
"""

from __future__ import annotations

import math
import logging
from typing import Dict, Any, Optional, Tuple, List

from sqlalchemy import text

from app.curves.pchip_cache import (
    eval_pchip as _pchip_eval,
    get_or_build_unified_perf_model,
)

log = logging.getLogger('app.spectrum_reader')

# Module-level engine (must be set by caller before use)
_engine = None

def set_engine(engine):
    """
    Set the SQLAlchemy engine for this module.
    
    This must be called before any database operations are performed.
    
    Args:
        engine: SQLAlchemy engine instance
    """
    global _engine
    _engine = engine

# =========================
# 公用：PCHIP 评估薄封装
# =========================

def eval_pchip_model(model: Optional[Dict[str, Any]], x: float) -> float:
    """
    Evaluate a PCHIP model at a given x value.
    
    Args:
        model: PCHIP model dictionary
        x: X value to evaluate at
        
    Returns:
        float: Interpolated y value, or NaN if model is invalid
    """
    if not model or not isinstance(model, dict):
        return float('nan')
    try:
        return float(_pchip_eval(model, float(x)))
    except Exception:
        return float('nan')

# =========================
# 曲线侧：从 general_view 三元组构建四条轴向 PCHIP
# =========================

def _collect_perf_rows(pairs: List[Tuple[int, int]]) -> Dict[str, dict]:
    """
    Collect performance data rows from the database for given model/condition pairs.
    
    Args:
        pairs: List of (model_id, condition_id) tuples
        
    Returns:
        dict: Dictionary keyed by "model_id_condition_id" with rpm, airflow, noise data
    """
    if _engine is None:
        raise RuntimeError(
            "Engine not set. Call set_engine(engine) before using this function."
        )
    
    out: Dict[str, dict] = {}
    if not pairs:
        return out

    conds: List[str] = []
    params: Dict[str, Any] = {}
    for i, (m, c) in enumerate(pairs, start=1):
        conds.append(f"(:m{i}, :c{i})")
        params[f"m{i}"] = int(m)
        params[f"c{i}"] = int(c)

    sql = f"""
      SELECT model_id,
             condition_id,
             rpm,
             airflow_cfm AS airflow,
             noise_db
      FROM general_view
      WHERE (model_id, condition_id) IN ({",".join(conds)})
      ORDER BY model_id, condition_id, rpm
    """

    with _engine.begin() as conn:
        rows = conn.execute(text(sql), params).fetchall()

    for r in rows or []:
        mp = r._mapping
        mid = int(mp['model_id'])
        cid = int(mp['condition_id'])
        k = f"{mid}_{cid}"

        bucket = out.setdefault(k, {
            'model_id': mid,
            'condition_id': cid,
            'rpm': [],
            'airflow': [],
            'noise': []
        })

        rpm = mp.get('rpm')
        airflow = mp.get('airflow')
        noise = mp.get('noise_db')

        try:
            af = float(airflow) if airflow is not None else None
            nz = float(noise) if noise is not None else None
            rp = float(rpm) if rpm is not None else None
        except Exception:
            continue

        # 过滤 airflow 为 NaN 的点
        if af is None or math.isnan(af):
            continue

        # rpm 与 noise 同时缺失的点无意义
        if (rp is None) and (nz is None):
            continue

        bucket['rpm'].append(rp)
        bucket['airflow'].append(af)
        bucket['noise'].append(nz)

    return out


def build_performance_pchips(pairs: List[Tuple[int, int]]) -> Dict[str, Dict[str, Any]]:
    """
    Build unified performance PCHIP models for given model/condition pairs.
    
    This function fetches performance data from the database and builds four-in-one
    PCHIP interpolation models (rpm→airflow, rpm→noise, noise→rpm, noise→airflow).
    
    Args:
        pairs: List of (model_id, condition_id) tuples
        
    Returns:
        dict: Dictionary keyed by "model_id_condition_id" containing PCHIP models and metadata.
              Each entry has:
              - model_id: Model ID
              - condition_id: Condition ID
              - pchip: Dictionary with four PCHIP models
              - supports_audio: Whether audio generation is supported
    """
    out: Dict[str, Dict[str, Any]] = {}

    if not pairs:
        return out

    bucket = _collect_perf_rows(pairs)

    for key, b in bucket.items():
        mid = int(b['model_id'])
        cid = int(b['condition_id'])

        rpm_list: List[float] = [
            float(v) for v in (b.get('rpm') or []) if v is not None
        ]
        air_list: List[float] = [
            float(v) for v in (b.get('airflow') or []) if v is not None
        ]
        noi_list: List[float] = [
            float(v) for v in (b.get('noise') or []) if v is not None
        ]

        unified = get_or_build_unified_perf_model(
            mid,
            cid,
            rpm_list,
            air_list,
            noi_list
        ) or {}

        pset = (unified.get('pchip') or {})
        supports_audio = unified.get('supports_audio', False)

        out[key] = {
            'model_id': mid,
            'condition_id': cid,
            'pchip': {
                'rpm_to_airflow':   pset.get('rpm_to_airflow'),
                'rpm_to_noise_db':  pset.get('rpm_to_noise_db'),
                'noise_to_rpm':     pset.get('noise_to_rpm'),
                'noise_to_airflow': pset.get('noise_to_airflow'),
            },
            'supports_audio': supports_audio
        }

    return out
