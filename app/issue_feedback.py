"""
Issue Feedback module for FancoolWeb.

Provides API routes for collecting user feedback on fan models:
  GET  /api/feedback/options   — list active feedback options from static table
  POST /api/feedback/submit    — submit feedback items for a model
  GET  /api/feedback/my        — get current user's valid feedbacks (grouped by model)
  POST /api/feedback/revoke    — revoke (invalidate) a feedback item

Call ``setup(fetch_all_fn, exec_write_fn, logger_obj)`` once during app startup
before registering the Blueprint.
"""

import hashlib
import logging
import re
from collections import OrderedDict

from flask import Blueprint, request

from app.common_utils import make_success_response, make_error_response
from app.user_activity import get_or_create_user_identifier

# =========================================
# Module-level state — injected via setup()
# =========================================
_fetch_all = None
_exec_write = None
_logger: logging.Logger = logging.getLogger(__name__)

OTHER_TEXT_MAX_LEN = 40  # backend safety cap (40 chars)


def setup(fetch_all_fn, exec_write_fn, logger_obj):
    """Inject shared dependencies. Call once during app startup."""
    global _fetch_all, _exec_write, _logger
    _fetch_all = fetch_all_fn
    _exec_write = exec_write_fn
    _logger = logger_obj


# =========================================
# Flask Blueprint
# =========================================
bp = Blueprint('issue_feedback', __name__)


# =========================================
# Response helpers
# =========================================
def _resp_ok(data=None, message=None, meta=None, http_status=200):
    return make_success_response(data, message, meta, http_status)


def _resp_err(error_code, error_message, http_status=400, *, meta=None):
    return make_error_response(error_code, error_message, http_status, meta)


# =========================================
# Internal helpers
# =========================================
def _normalize_text(text: str) -> str:
    """Lightweight normalization: strip, lowercase, collapse whitespace."""
    t = text.strip().lower()
    t = re.sub(r'\s+', ' ', t)
    return t


# =========================================
# Routes
# =========================================

@bp.get('/api/feedback/options')
def api_feedback_options():
    """Return active feedback options from the static table."""
    try:
        rows = _fetch_all(
            "SELECT option_id, option_name_zh, option_name_en, is_other, sort_order "
            "FROM fan_issue_feedback_option "
            "WHERE is_valid=1 "
            "ORDER BY sort_order, option_id",
            {}
        )
        options = [
            {
                'option_id': int(r['option_id']),
                'option_name_zh': r['option_name_zh'] or '',
                'option_name_en': r['option_name_en'] or '',
                'is_other': bool(r['is_other']),
                'sort_order': int(r['sort_order']),
            }
            for r in rows
        ]
        return _resp_ok({'options': options})
    except Exception as e:
        _logger.exception(e)
        return _resp_err('INTERNAL_ERROR', str(e), 500)


@bp.post('/api/feedback/submit')
def api_feedback_submit():
    """Submit one or more feedback items for a model."""
    try:
        user_id = get_or_create_user_identifier()
        data = request.get_json(force=True, silent=True) or {}

        try:
            model_id = int(data.get('model_id') or 0)
        except (ValueError, TypeError):
            model_id = 0
        if model_id <= 0:
            return _resp_err('INVALID_MODEL', 'model_id 无效')

        items = data.get('items')
        if not isinstance(items, list) or not items:
            return _resp_err('INVALID_ITEMS', 'items 不能为空')
        if len(items) > 20:
            return _resp_err('TOO_MANY_ITEMS', '单次最多提交 20 条')

        source_page = (data.get('source_page') or '').strip()[:64] or None
        client_ip = request.remote_addr or ''
        client_ip_hash = hashlib.sha256(client_ip.encode()).hexdigest() if client_ip else None

        # Fetch all active options once
        valid_options = {
            int(r['option_id']): r
            for r in _fetch_all(
                "SELECT option_id, is_other FROM fan_issue_feedback_option WHERE is_valid=1",
                {}
            )
        }

        created_count = 0
        restored_count = 0
        duplicate_count = 0

        for item in items:
            try:
                option_id = int(item.get('option_id') or 0)
            except (ValueError, TypeError):
                option_id = 0
            if option_id not in valid_options:
                continue

            opt = valid_options[option_id]
            is_other = bool(opt.get('is_other'))
            other_text = None
            normalized_other_text = ''

            if is_other:
                raw_text = (item.get('other_text') or '').strip()[:OTHER_TEXT_MAX_LEN]
                if not raw_text:
                    continue  # "其他" with no text — skip
                other_text = raw_text
                normalized_other_text = _normalize_text(raw_text)

            # Dedup check: unified on normalized_other_text ('' for non-"其他", text for "其他")
            existing = _fetch_all(
                "SELECT feedback_id, is_valid FROM fan_issue_feedback "
                "WHERE model_id=:mid AND user_identifier=:uid AND option_id=:oid "
                "AND normalized_other_text=:ntxt "
                "LIMIT 1",
                {'mid': model_id, 'uid': user_id, 'oid': option_id, 'ntxt': normalized_other_text}
            )

            if existing:
                row = existing[0]
                if int(row['is_valid']) == 1:
                    duplicate_count += 1
                else:
                    # Restore invalidated record
                    _exec_write(
                        "UPDATE fan_issue_feedback "
                        "SET is_valid=1, update_date=NOW() "
                        "WHERE feedback_id=:fid",
                        {'fid': row['feedback_id']}
                    )
                    restored_count += 1
            else:
                _exec_write(
                    "INSERT INTO fan_issue_feedback "
                    "(model_id, user_identifier, option_id, other_text, normalized_other_text, "
                    "submit_date, update_date, is_valid, source_page, client_ip_hash) "
                    "VALUES "
                    "(:mid, :uid, :oid, :otxt, :ntxt, NOW(), NOW(), 1, :src, :iph)",
                    {
                        'mid': model_id,
                        'uid': user_id,
                        'oid': option_id,
                        'otxt': other_text,
                        'ntxt': normalized_other_text,
                        'src': source_page,
                        'iph': client_ip_hash,
                    }
                )
                created_count += 1

        total = created_count + restored_count + duplicate_count
        if total == 0:
            return _resp_err('NO_VALID_ITEMS', '没有有效的反馈项可提交')

        return _resp_ok({
            'created_count': created_count,
            'restored_count': restored_count,
            'duplicate_count': duplicate_count,
        })
    except Exception as e:
        _logger.exception(e)
        return _resp_err('INTERNAL_ERROR', str(e), 500)


@bp.get('/api/feedback/my')
def api_feedback_my():
    """Return current user's valid feedbacks, grouped by model (most-recently-updated first)."""
    try:
        user_id = get_or_create_user_identifier()

        rows = _fetch_all(
            """
            SELECT
                f.feedback_id,
                f.model_id,
                f.option_id,
                f.other_text,
                f.submit_date,
                f.update_date,
                o.option_name_zh,
                o.is_other,
                fm.brand_name_zh,
                fm.brand_name_en,
                fm.model_name
            FROM fan_issue_feedback f
            JOIN fan_issue_feedback_option o ON o.option_id = f.option_id
            LEFT JOIN available_models_info_view fm ON fm.model_id = f.model_id
            WHERE f.user_identifier = :uid AND f.is_valid = 1
            ORDER BY f.update_date DESC, f.feedback_id DESC
            """,
            {'uid': user_id}
        )

        # Group by model; row order (update_date DESC) already gives correct intra-group order
        groups: dict = OrderedDict()
        for r in rows:
            mid = int(r['model_id'])
            if mid not in groups:
                groups[mid] = {
                    'model_id': mid,
                    'brand_name_zh': r['brand_name_zh'] or '',
                    'brand_name_en': r['brand_name_en'] or '',
                    'model_name': r['model_name'] or '',
                    'feedbacks': [],
                    '_latest_date': str(r['update_date']) if r['update_date'] else '',
                }
            groups[mid]['feedbacks'].append({
                'feedback_id': r['feedback_id'],
                'option_id': int(r['option_id']),
                'option_name_zh': r['option_name_zh'] or '',
                'is_other': bool(r['is_other']),
                'other_text': r['other_text'] or None,
                'submit_date': str(r['submit_date']) if r['submit_date'] else None,
                'update_date': str(r['update_date']) if r['update_date'] else None,
            })

        # Sort groups by most-recent update_date descending
        sorted_groups = sorted(
            groups.values(),
            key=lambda g: g['_latest_date'],
            reverse=True
        )
        for g in sorted_groups:
            del g['_latest_date']

        return _resp_ok({'groups': sorted_groups})
    except Exception as e:
        _logger.exception(e)
        return _resp_err('INTERNAL_ERROR', str(e), 500)


@bp.post('/api/feedback/revoke')
def api_feedback_revoke():
    """Revoke (set is_valid=0) a feedback item owned by the current user."""
    try:
        user_id = get_or_create_user_identifier()
        data = request.get_json(force=True, silent=True) or {}

        try:
            feedback_id = int(data.get('feedback_id') or 0)
        except (ValueError, TypeError):
            feedback_id = 0
        if feedback_id <= 0:
            return _resp_err('INVALID_ID', 'feedback_id 无效')

        existing = _fetch_all(
            "SELECT feedback_id, is_valid FROM fan_issue_feedback "
            "WHERE feedback_id=:fid AND user_identifier=:uid LIMIT 1",
            {'fid': feedback_id, 'uid': user_id}
        )
        if not existing:
            return _resp_err('NOT_FOUND', '未找到该反馈记录或无权撤回', 404)

        if int(existing[0]['is_valid']) == 0:
            return _resp_ok({'revoked': False}, message='该记录已撤回')

        _exec_write(
            "UPDATE fan_issue_feedback SET is_valid=0, update_date=NOW() "
            "WHERE feedback_id=:fid AND user_identifier=:uid",
            {'fid': feedback_id, 'uid': user_id}
        )
        return _resp_ok({'revoked': True})
    except Exception as e:
        _logger.exception(e)
        return _resp_err('INTERNAL_ERROR', str(e), 500)
