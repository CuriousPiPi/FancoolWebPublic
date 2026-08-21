"""
File share helpers and routes for FancoolWeb.

Provides:
  - get_active_file_share_items(): grouped by category, safe frontend payloads
  - GET /go/file-share/<file_id>: event-log + counter + temporary redirect
  - GET /api/file-share/thumb/<filename>: serve thumbnail
  - GET /api/file-share/original/<filename>: serve original image

Call ``setup(fetch_all_fn, exec_write_fn, logger_obj)`` once during app startup
before registering the Blueprint.
"""

import logging
from urllib.parse import urlparse

from flask import Blueprint, abort, redirect, send_from_directory

from app import file_share_services as _fs

_fetch_all = None
_exec_write = None
_logger: logging.Logger = logging.getLogger(__name__)

bp = Blueprint('file_share', __name__)


def setup(fetch_all_fn, exec_write_fn, logger_obj):
    global _fetch_all, _exec_write, _logger
    _fetch_all = fetch_all_fn
    _exec_write = exec_write_fn
    _logger = logger_obj


def _is_allowed_external_url(url: str) -> bool:
    try:
        parsed = urlparse(url or '')
    except Exception:
        return False
    return parsed.scheme in ('http', 'https') and bool(parsed.netloc)


def get_active_file_share_items() -> list[dict]:
    """
    Return all enabled file-share items grouped by category.

    Each group: {category_id, category_name_zh, category_name_en, sort_order, items: [...]}
    Each item: {file_id, title_zh, title_en, description_zh, description_en,
                thumb_url, sort_order, total_click}
    URL for download: /go/file-share/<file_id>
    """
    rows = _fetch_all("""
        SELECT
            i.file_id,
            i.category_id,
            i.thumb_filename,
            i.title_zh,
            i.title_en,
            i.description_zh,
            i.description_en,
            i.sort_order,
            i.total_click,
            c.category_name_zh,
            c.category_name_en,
            c.sort_order AS category_sort_order
        FROM file_share_item i
        JOIN file_share_category c ON c.category_id = i.category_id
        WHERE i.enabled = 1
        ORDER BY c.sort_order ASC, c.category_id ASC, i.sort_order ASC, i.file_id ASC
    """, {})

    categories: dict[int, dict] = {}
    for row in rows:
        cat_id = int(row['category_id'])
        if cat_id not in categories:
            categories[cat_id] = {
                'category_id': cat_id,
                'category_name_zh': row.get('category_name_zh') or '',
                'category_name_en': row.get('category_name_en') or '',
                'sort_order': int(row.get('category_sort_order') or 0),
                'items': [],
            }
        thumb_filename = (row.get('thumb_filename') or '').strip()
        thumb_url = (
            f'/api/file-share/thumb/{thumb_filename}' if thumb_filename else None
        )
        categories[cat_id]['items'].append({
            'file_id': int(row['file_id']),
            'title_zh': row.get('title_zh') or '',
            'title_en': row.get('title_en') or '',
            'description_zh': row.get('description_zh') or '',
            'description_en': row.get('description_en') or '',
            'thumb_url': thumb_url,
            'sort_order': int(row.get('sort_order') or 0),
            'total_click': int(row.get('total_click') or 0),
        })

    return sorted(categories.values(), key=lambda c: (c['sort_order'], c['category_id']))


@bp.get('/go/file-share/<int:file_id>')
def go_file_share(file_id: int):
    rows = _fetch_all("""
        SELECT
            i.file_id,
            i.category_id,
            i.file_url,
            i.title_zh,
            i.enabled
        FROM file_share_item i
        WHERE i.file_id = :id
        LIMIT 1
    """, {'id': int(file_id)})
    row = rows[0] if rows else None
    if not row or int(row.get('enabled') or 0) != 1:
        abort(404)

    target = (row.get('file_url') or '').strip()
    if not _is_allowed_external_url(target):
        abort(404)

    route_url = f'/go/file-share/{int(row["file_id"])}'
    payload = {
        'file_id': int(row['file_id']),
        'category_id': int(row['category_id']),
        'title_zh': (row.get('title_zh') or '').strip(),
    }

    try:
        from app import user_activity
        user_activity.write_event_log(
            'click_file_share_download',
            page_key='sidebar',
            target_url=route_url,
            payload_json=payload,
        )
    except Exception as exc:
        _logger.exception('file-share redirect log failed for file_id=%s: %s', file_id, exc)

    try:
        _exec_write("""
            UPDATE file_share_item
            SET total_click = COALESCE(total_click, 0) + 1
            WHERE file_id = :id
        """, {'id': int(row['file_id'])})
    except Exception as exc:
        _logger.exception('file-share counter update failed for file_id=%s: %s', file_id, exc)

    return redirect(target, code=302)


@bp.get('/api/file-share/thumb/<path:filename>')
def api_file_share_thumb(filename):
    """Serve a file-share thumbnail."""
    if not _fs.is_valid_filename(filename):
        abort(404)

    import re
    m = re.match(r'^file_(\d+)\.', filename, flags=re.IGNORECASE)
    file_id = int(m.group(1)) if m else 0
    rows = _fetch_all('SELECT enabled FROM file_share_item WHERE file_id = :id LIMIT 1', {'id': file_id})
    if not rows or int(rows[0].get('enabled') or 0) != 1:
        abort(404)

    if not _fs.ensure_thumbnail(filename):
        abort(404)
    return send_from_directory(_fs.thumbs_dir(), filename)


@bp.get('/api/file-share/original/<path:filename>')
def api_file_share_original(filename):
    """Serve a file-share original image."""
    if not _fs.is_valid_filename(filename):
        abort(404)

    import os
    import re
    m = re.match(r'^file_(\d+)\.', filename, flags=re.IGNORECASE)
    file_id = int(m.group(1)) if m else 0
    rows = _fetch_all('SELECT enabled FROM file_share_item WHERE file_id = :id LIMIT 1', {'id': file_id})
    if not rows or int(rows[0].get('enabled') or 0) != 1:
        abort(404)

    originals = _fs.originals_dir()
    if not os.path.isfile(os.path.join(originals, filename)):
        abort(404)
    return send_from_directory(originals, filename)


@bp.get('/api/file-share')
def api_file_share_list():
    """Return active file-share items grouped by category (public endpoint)."""
    from flask import jsonify, make_response
    try:
        groups = get_active_file_share_items()
        return make_response(jsonify({'success': True, 'data': {'groups': groups}, 'message': None, 'meta': {}}), 200)
    except Exception as exc:
        _logger.exception('api_file_share_list failed: %s', exc)
        return make_response(jsonify({'success': False, 'error_code': 'INTERNAL_ERROR', 'error_message': '接口异常', 'data': None, 'meta': {}}), 500)
