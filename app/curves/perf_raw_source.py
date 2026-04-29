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

import math
import logging
from typing import Dict, Any, List, Tuple

from sqlalchemy import text

log = logging.getLogger(__name__)

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
    """Fetch raw performance data from *general_view* for the given pairs.

    Args:
        pairs: List of ``(model_id, condition_id)`` tuples.

    Returns:
        Dict keyed by ``"model_id_condition_id"`` containing::

            {
                "model_id": int,
                "condition_id": int,
                "rpm":     [float | None, ...],
                "airflow": [float, ...],       # NaN rows are dropped
                "noise":   [float | None, ...],
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
        "SELECT model_id, condition_id, rpm, airflow_cfm AS airflow, noise_db "
        f"FROM general_view "
        f"WHERE (model_id, condition_id) IN ({','.join(conds)}) "
        "ORDER BY model_id, condition_id, rpm"
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
            },
        )

        rpm_val = mp.get("rpm")
        airflow_val = mp.get("airflow")
        noise_val = mp.get("noise_db")

        try:
            af = float(airflow_val) if airflow_val is not None else None
            nz = float(noise_val) if noise_val is not None else None
            rp = float(rpm_val) if rpm_val is not None else None
        except Exception:
            continue

        # Drop rows where airflow is missing or NaN — they are unusable for fitting.
        if af is None or math.isnan(af):
            continue

        # Rows where both rpm and noise are absent carry no useful signal.
        if rp is None and nz is None:
            continue

        bucket["rpm"].append(rp)
        bucket["airflow"].append(af)
        bucket["noise"].append(nz)

    return out
