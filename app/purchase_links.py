"""
Purchase link helpers and redirect route for FancoolWeb.

Provides:
  - get_active_purchase_links_by_model_ids(model_ids): safe frontend payloads
  - GET /go/purchase/<id>: event-log + counter + temporary redirect

Call ``setup(fetch_all_fn, exec_write_fn, logger_obj)`` once during app startup
before registering the Blueprint.
"""

import logging
from urllib.parse import urlparse

from flask import Blueprint, abort, redirect, request

_fetch_all = None
_exec_write = None
_logger: logging.Logger = logging.getLogger(__name__)

bp = Blueprint('purchase_links', __name__)


def setup(fetch_all_fn, exec_write_fn, logger_obj):
    global _fetch_all, _exec_write, _logger
    _fetch_all = fetch_all_fn
    _exec_write = exec_write_fn
    _logger = logger_obj


def _normalize_model_ids(model_ids) -> list[int]:
    out = []
    seen = set()
    for model_id in model_ids or []:
        try:
            mid = int(model_id)
        except (TypeError, ValueError):
            continue
        if mid <= 0 or mid in seen:
            continue
        seen.add(mid)
        out.append(mid)
    return out


def _build_in_params(values: list[int], prefix: str) -> tuple[str, dict]:
    placeholders = []
    params = {}
    for i, value in enumerate(values, start=1):
        key = f'{prefix}{i}'
        placeholders.append(f':{key}')
        params[key] = int(value)
    return ', '.join(placeholders), params


def get_active_purchase_links_by_model_ids(model_ids) -> dict[int, list[dict]]:
    wanted = _normalize_model_ids(model_ids)
    if not wanted:
        return {}
    placeholders, params = _build_in_params(wanted, 'mid')
    rows = _fetch_all(f"""
        SELECT
            id,
            model_id,
            display_text_zh,
            display_text_en,
            platform_code,
            region_code
        FROM fan_model_purchase_link
        WHERE enabled = 1
          AND model_id IN ({placeholders})
        ORDER BY model_id ASC, sort_order ASC, id ASC
    """, params)
    out = {}
    for row in rows:
        try:
            model_id = int(row['model_id'])
            link_id = int(row['id'])
        except (KeyError, TypeError, ValueError):
            continue
        out.setdefault(model_id, []).append({
            'id': link_id,
            'platform_code': (row.get('platform_code') or '').strip(),
            'display_text_zh': row.get('display_text_zh') or '',
            'display_text_en': row.get('display_text_en') or '',
            'region_code': (row.get('region_code') or '').strip(),
        })
    return out


def _is_allowed_external_url(url: str) -> bool:
    try:
        parsed = urlparse(url or '')
    except Exception:
        return False
    return parsed.scheme in ('http', 'https') and bool(parsed.netloc)


_VALID_PURCHASE_SOURCES: frozenset[str] = frozenset({
    'ranking_lighting_board',
    'ranking_heat_board',
    'ranking_perf_board',
    'search_results',
    'recent_updates',
    'fc_model_detail',
})
_DEFAULT_PURCHASE_SOURCE = 'purchase_redirect'


def _resolve_purchase_source() -> str:
    """Read and validate the ``from`` query parameter.

    Returns the normalised source string if it is in the allowlist,
    otherwise the default fallback value.
    """
    raw = (request.args.get('from') or '').strip()
    return raw if raw in _VALID_PURCHASE_SOURCES else _DEFAULT_PURCHASE_SOURCE


@bp.get('/go/purchase/<int:link_id>')
def go_purchase(link_id: int):
    rows = _fetch_all("""
        SELECT
            id,
            model_id,
            url,
            display_text_zh,
            display_text_en,
            platform_code,
            enabled
        FROM fan_model_purchase_link
        WHERE id = :id
        LIMIT 1
    """, {'id': int(link_id)})
    row = rows[0] if rows else None
    if not row or int(row.get('enabled') or 0) != 1:
        abort(404)

    target = (row.get('url') or '').strip()
    if not _is_allowed_external_url(target):
        abort(404)

    route_url = f'/go/purchase/{int(row["id"])}'
    payload = {
        'purchase_link_id': int(row['id']),
        'platform_code': (row.get('platform_code') or '').strip(),
    }
    display_text_zh = (row.get('display_text_zh') or '').strip()
    display_text_en = (row.get('display_text_en') or '').strip()
    if display_text_zh:
        payload['display_text_zh'] = display_text_zh
    if display_text_en:
        payload['display_text_en'] = display_text_en

    try:
        from app import user_activity
        user_activity.write_event_log(
            'click_purchase_link',
            page_key=_resolve_purchase_source(),
            target_url=route_url,
            model_id=int(row['model_id']) if row.get('model_id') is not None else None,
            payload_json=payload,
        )
    except Exception as exc:
        _logger.exception('purchase redirect log failed for link_id=%s: %s', link_id, exc)

    try:
        _exec_write("""
            UPDATE fan_model_purchase_link
            SET total_click = COALESCE(total_click, 0) + 1
            WHERE id = :id
        """, {'id': int(row['id'])})
    except Exception as exc:
        _logger.exception('purchase redirect counter update failed for link_id=%s: %s', link_id, exc)

    return redirect(target, code=302)
