# -*- coding: utf-8 -*-
"""
perf_raw_source: Raw performance data layer.

Responsible for fetching raw rpm / airflow / noise_db data points from the
database for a list of (model_id, condition_id) pairs.  This layer is
internal to the performance-curve subsystem and must NOT be called directly
by the business layer (fancoolserver.py, API handlers, etc.).

Engine Injection
----------------
The SQLAlchemy engine is injected via :func:`set_engine` before any DB call.
This keeps the module side-effect-free at import time, which is important for
Windows / Jupyter local testing where a real database may not be available.
"""

from __future__ import annotations

import logging
from typing import Dict, Any, List, Tuple

from sqlalchemy import text

log = logging.getLogger(__name__)


def _raw_number(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError, OverflowError):
        return value

# Module-level engine — must be injected by the caller before any DB access.
_engine = None


def set_engine(engine) -> None:
    """Inject the SQLAlchemy engine used for raw-point queries.

    This must be called once (e.g. at application startup) before any
    database operations are performed by this module.
    """
    global _engine
    _engine = engine


def fetch_raw_perf_rows(
    pairs: List[Tuple[int, int]],
) -> Dict[str, Dict[str, Any]]:
    """Fetch raw performance data from canonical tables for the given pairs.

    Args:
        pairs: List of ``(model_id, condition_id)`` tuples.

    Returns:
        Dict keyed by ``"model_id_condition_id"`` containing::

            {
                "model_id": int,
                "condition_id": int,
                "rpm":     [float | None, ...],
                "airflow": [float | None, ...],
                "noise":   [float | None, ...],
                "pressure_mmh2o": [float | None, ...],
                "sone":    [float | None, ...],   # fan_performance_data.noise_sone
            }

    Raises:
        RuntimeError: If the engine has not been set via :func:`set_engine`.
    """
    if _engine is None:
        raise RuntimeError(
            "Engine not set.  Call perf_raw_source.set_engine(engine) "
            "before performing database operations."
        )

    out: Dict[str, Dict[str, Any]] = {}
    if not pairs:
        return out

    conds: List[str] = []
    params: Dict[str, Any] = {}
    for i, (m, c) in enumerate(pairs, start=1):
        conds.append(f"(:m{i}, :c{i})")
        params[f"m{i}"] = int(m)
        params[f"c{i}"] = int(c)

    sql = (
        "SELECT p.model_id, p.condition_id, p.rpm, p.airflow_cfm AS airflow, p.noise_db, p.pressure_mmh2o, p.noise_sone "
        "FROM fan_performance_data p "
        "JOIN fan_model m "
        "  ON m.model_id = p.model_id "
        "JOIN fan_brand b "
        "  ON b.brand_id = m.brand_id "
        "JOIN working_condition c "
        "  ON c.condition_id = p.condition_id "
        "WHERE p.is_valid = 1 "
        "AND m.is_valid = 1 "
        "AND b.is_valid = 1 "
        "AND c.is_valid = 1 "
        f"AND (p.model_id, p.condition_id) IN ({','.join(conds)}) "
        "ORDER BY p.model_id, p.condition_id, p.rpm, p.data_id"
    )

    with _engine.begin() as conn:
        rows = conn.execute(text(sql), params).fetchall()

    for r in rows or []:
        mp = r._mapping
        mid = int(mp["model_id"])
        cid = int(mp["condition_id"])
        key = f"{mid}_{cid}"

        bucket = out.setdefault(
            key,
            {
                "model_id": mid,
                "condition_id": cid,
                "rpm": [],
                "airflow": [],
                "noise": [],
                "pressure_mmh2o": [],
                "sone": [],
            },
        )

        rpm_val = mp.get("rpm")
        airflow_val = mp.get("airflow")
        noise_val = mp.get("noise_db")
        pressure_val = mp.get("pressure_mmh2o")

        # Keep complete rows aligned; fitting eligibility belongs to joint selection.
        bucket["rpm"].append(_raw_number(rpm_val))
        bucket["airflow"].append(_raw_number(airflow_val))
        bucket["noise"].append(_raw_number(noise_val))
        bucket["pressure_mmh2o"].append(_raw_number(pressure_val))
        # Per-row calibrated sone (NULL = missing; 0 is a valid value).
        bucket["sone"].append(_raw_number(mp.get("noise_sone")))

    return out
