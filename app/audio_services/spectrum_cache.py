# -*- coding: utf-8 -*-
"""
spectrum_cache: 频谱模型缓存的统一管理（仅一份按 ID 命名的文件）
文件命名：{model_id}_{condition_id}_spectrum.json
可供前端对外服务与后台管理端复用。
"""
from __future__ import annotations
import os
import json
from datetime import datetime
from typing import Dict, Any, Optional

from app.curves.pchip_cache import curve_cache_dir

def path(model_id: int, condition_id: int) -> str:
    base = os.path.abspath(curve_cache_dir())
    os.makedirs(base, exist_ok=True)
    return os.path.join(base, f"{int(model_id)}_{int(condition_id)}_spectrum.json")

def exists(model_id: int, condition_id: int) -> bool:
    return os.path.isfile(path(model_id, condition_id))

def load(model_id: int, condition_id: int) -> Optional[Dict[str, Any]]:
    p = path(model_id, condition_id)
    if not os.path.isfile(p):
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def save(model_json: Dict[str, Any], *, model_id: int, condition_id: int,
         extra_meta: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
    """
    v2：支持写入自定义 meta（例如：perf_batch_id、audio_batch_id、audio_data_hash、param_hash、code_version、built_at 等）
    """
    out = {
        "type": "spectrum_v2",
        "model": model_json or {},
        "meta": {
            "model_id": int(model_id),
            "condition_id": int(condition_id),
            "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        }
    }
    if extra_meta and isinstance(extra_meta, dict):
        out["meta"].update(extra_meta)

    p = path(model_id, condition_id)
    # 原子覆盖写入（避免并发读到半成品）
    import tempfile
    d = os.path.dirname(p)
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix="sp_", suffix=".json", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False)
        os.replace(tmp, p)
    finally:
        try:
            os.remove(tmp)
        except Exception:
            # Ignore errors during temp file cleanup; leftover temp files are not critical.
            pass
    return {"path": p}

def delete(model_id: int, condition_id: int) -> bool:
    p = path(model_id, condition_id)
    try:
        if os.path.isfile(p):
            os.remove(p)
            return True
        return False
    except Exception:
        return False

def validate(model_id: int, condition_id: int) -> Dict[str, Any]:
    p = path(model_id, condition_id)
    if not os.path.isfile(p):
        return {"exists": False, "valid": False, "reason": "not-found", "path": p, "meta": {}}
    try:
        with open(p, "r", encoding="utf-8") as f:
            j = json.load(f)
        t = (j.get("type") if isinstance(j, dict) else None) or ""
        ok_type = t in ("spectrum_v1", "spectrum_v2")
        ok = isinstance(j, dict) and ok_type and isinstance(j.get("model"), dict)
        meta = (j.get("meta") or {}) if isinstance(j, dict) else {}
        return {"exists": True, "valid": bool(ok), "reason": None if ok else "bad-structure", "path": p, "meta": meta}
    except Exception:
        return {"exists": True, "valid": False, "reason": "read-error", "path": p, "meta": {}}