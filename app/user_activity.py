"""
User activity module for FancoolWeb.

Provides UID/cookie management, device parsing, like/fingerprint helpers,
curve-set logging helpers, and user-activity API routes (visit_start, log_event,
like/unlike, curve_set, radar_models) extracted from fancoolserver.py.

Call ``setup(fetch_all_fn, exec_write_fn, engine_obj, logger_obj)`` once during
app startup before registering the Blueprint.
"""

import os
import uuid
import json
import time
import logging
from typing import List

from flask import Blueprint, request, g, session
from sqlalchemy import text
from user_agents import parse as parse_ua

from app.common_utils import (
    sign_uid, unsign_uid,
    make_success_response, make_error_response,
)

# =========================================
# Module-level state — injected via setup()
# =========================================
_fetch_all = None
_exec_write = None
_engine = None
_logger: logging.Logger = logging.getLogger(__name__)

# UID cookie config (read from env at import time, same as fancoolserver.py)
UID_COOKIE_NAME = os.getenv('UID_COOKIE_NAME', 'fc_uid')
UID_COOKIE_MAX_AGE = int(os.getenv('UID_COOKIE_MAX_AGE_SECONDS', str(60 * 60 * 24 * 365 * 2)))
UID_COOKIE_SAMESITE = os.getenv('UID_COOKIE_SAMESITE', 'Lax')
UID_COOKIE_SECURE = os.getenv('UID_COOKIE_SECURE', '0') == '1'
UID_COOKIE_HTTPONLY = os.getenv('UID_COOKIE_HTTPONLY', '0') == '1'
UID_COOKIE_REFRESH_INTERVAL = int(os.getenv('UID_COOKIE_REFRESH_INTERVAL_SECONDS', str(60 * 60 * 24 * 7)))
UID_COOKIE_REFRESH_TS_NAME = os.getenv('UID_COOKIE_REFRESH_TS_NAME', 'fc_uid_refreshed_at')
UID_COOKIE_DOMAIN = os.getenv('UID_COOKIE_DOMAIN', '.fancool.cc')

RECENT_LIKES_LIMIT = 100
LIKE_STATUS_PAIRS_MAX = 200


def setup(fetch_all_fn, exec_write_fn, engine_obj, logger_obj):
    """Inject shared dependencies.  Call once during app startup."""
    global _fetch_all, _exec_write, _engine, _logger
    _fetch_all = fetch_all_fn
    _exec_write = exec_write_fn
    _engine = engine_obj
    _logger = logger_obj


# =========================================
# Flask Blueprint
# =========================================
bp = Blueprint('user_activity', __name__)


# =========================================
# UID Signing helpers
# =========================================
def _sign_uid(value: str) -> str:
    from flask import current_app
    return sign_uid(value, current_app.secret_key)


def _unsign_uid(token: str) -> str | None:
    from flask import current_app
    return unsign_uid(token, current_app.secret_key)


# =========================================
# Flask hooks (app-wide)
# =========================================
@bp.before_app_request
def _init_g_defaults():
    if not hasattr(g, '_uid_source'):
        g._uid_source = None


@bp.after_app_request
def ensure_uid_cookie(resp):
    now = int(time.time())
    token_to_set = getattr(g, '_set_uid_cookie', None)
    if token_to_set:
        resp.set_cookie(
            UID_COOKIE_NAME, token_to_set,
            max_age=UID_COOKIE_MAX_AGE,
            samesite=UID_COOKIE_SAMESITE,
            secure=UID_COOKIE_SECURE,
            httponly=UID_COOKIE_HTTPONLY,
            path='/',
            domain=UID_COOKIE_DOMAIN
        )
        if getattr(g, '_set_uid_refresh_now', False):
            resp.set_cookie(
                UID_COOKIE_REFRESH_TS_NAME, str(now),
                max_age=UID_COOKIE_MAX_AGE,
                samesite=UID_COOKIE_SAMESITE,
                secure=UID_COOKIE_SECURE,
                httponly=UID_COOKIE_HTTPONLY,
                path='/',
                domain=UID_COOKIE_DOMAIN
            )
        return resp

    last_ts_raw = request.cookies.get(UID_COOKIE_REFRESH_TS_NAME)
    try:
        last_ts = int(last_ts_raw or '0')
    except ValueError:
        last_ts = 0

    if now - last_ts >= UID_COOKIE_REFRESH_INTERVAL:
        existing_token = request.cookies.get(UID_COOKIE_NAME)
        uid = _unsign_uid(existing_token) if existing_token else None
        if uid:
            resp.set_cookie(
                UID_COOKIE_NAME, existing_token,
                max_age=UID_COOKIE_MAX_AGE,
                samesite=UID_COOKIE_SAMESITE,
                secure=UID_COOKIE_SECURE,
                httponly=UID_COOKIE_HTTPONLY,
                path='/',
                domain=UID_COOKIE_DOMAIN
            )
        elif getattr(g, '_active_uid', None):
            resp.set_cookie(
                UID_COOKIE_NAME, _sign_uid(g._active_uid),
                max_age=UID_COOKIE_MAX_AGE,
                samesite=UID_COOKIE_SAMESITE,
                secure=UID_COOKIE_SECURE,
                httponly=UID_COOKIE_HTTPONLY,
                path='/',
                domain=UID_COOKIE_DOMAIN
            )
        resp.set_cookie(
            UID_COOKIE_REFRESH_TS_NAME, str(now),
            max_age=UID_COOKIE_MAX_AGE,
            samesite=UID_COOKIE_SAMESITE,
            secure=UID_COOKIE_SECURE,
            httponly=UID_COOKIE_HTTPONLY,
            path='/',
            domain=UID_COOKIE_DOMAIN
        )
    return resp


# =========================================
# UID / session helpers
# =========================================
def get_or_create_user_identifier() -> str:
    token = request.cookies.get(UID_COOKIE_NAME)
    uid_from_cookie = _unsign_uid(token) if token else None
    if uid_from_cookie:
        uid = uid_from_cookie
        g._uid_source = 'cookie'
    else:
        if token:
            g._uid_source = 'cookie_invalid'
        uid = session.get('user_identifier')
        if uid:
            g._uid_source = g._uid_source or 'session'
        else:
            uid = str(uuid.uuid4())
            g._uid_source = g._uid_source or 'generated'
    g._active_uid = uid
    if not token:
        g._set_uid_cookie = _sign_uid(uid)
        g._set_uid_refresh_now = True
    session['user_identifier'] = uid
    session.permanent = True
    return uid


# =========================================
# Device / visit helpers
# =========================================
def _parse_device_basic(ua_string: str) -> dict:
    try:
        ua = parse_ua(ua_string or '')
        if ua.is_bot:
            dtype = 'bot'
        elif ua.is_mobile:
            dtype = 'mobile'
        elif ua.is_tablet:
            dtype = 'tablet'
        elif ua.is_pc:
            dtype = 'desktop'
        else:
            dtype = 'other'
        return dict(os_name=ua.os.family or None, device_type=dtype)
    except Exception:
        return dict(os_name=None, device_type='other')


def _get_latest_visit_id_for_user(uid: str) -> int | None:
    try:
        rows = _fetch_all(
            "SELECT id FROM visit_logs WHERE user_identifier=:u ORDER BY id DESC LIMIT 1",
            {'u': uid}
        )
        if rows:
            return int(rows[0]['id'])
    except Exception:
        pass
    return None


def _coerce_to_int_or_none(value) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


# =========================================
# FNV fingerprint / likes helpers
# =========================================
FNV_OFFSET_64 = 0xCBF29CE484222325
FNV_PRIME_64 = 0x100000001B3
MASK_64 = 0xFFFFFFFFFFFFFFFF


def _fnv1a_64(s: str) -> int:
    h = FNV_OFFSET_64
    for ch in s:
        h ^= ord(ch)
        h = (h * FNV_PRIME_64) & MASK_64
    return h


def get_user_likes_full(user_identifier: str, limit: int | None = None) -> List[dict]:
    sql = """
        SELECT u.user_identifier, u.model_id, u.condition_id,
               u.brand_name_zh, u.model_name,
               u.condition_name_zh, u.resistance_type_zh, u.resistance_location_zh,
               u.max_speed, u.size, u.thickness,
               u.reference_price,
               COALESCE(u.rgb_light, '') AS rgb_light
        FROM user_likes_view u
        WHERE u.user_identifier = :u
        ORDER BY u.create_date DESC
    """
    params: dict = {'u': user_identifier}
    if limit is not None:
        sql += " LIMIT :lim"
        params['lim'] = limit
    return _fetch_all(sql, params)


def get_user_like_keys(user_identifier: str) -> List[str]:
    return [f"{int(r['model_id'])}_{int(r['condition_id'])}" for r in get_user_likes_full(user_identifier)]


def compute_like_fingerprint(user_id: str) -> dict:
    keys = get_user_like_keys(user_id)
    xor_v = 0
    sum_v = 0
    for k in keys:
        hv = _fnv1a_64(k)
        xor_v ^= hv
        sum_v = (sum_v + hv) & MASK_64
    return {'c': len(keys), 'x': f"{xor_v:016x}", 's': f"{sum_v:016x}"}


# =========================================
# Curve-set helpers
# =========================================
_VALID_CURVE_EVENT_TYPES = frozenset({
    'condition_activate', 'restore', 'model_show', 'model_add',
    'condition_inactivate', 'model_remove', 'model_hide',
    'radar_clear_all', 'reset_condition',
})
_CURVE_ENTER_EVENT_TYPES = frozenset({'condition_activate', 'restore', 'model_show', 'model_add'})
_CURVE_LEAVE_EVENT_TYPES = frozenset({
    'condition_inactivate', 'model_remove', 'model_hide', 'radar_clear_all', 'reset_condition',
})
_CURVE_RESTORE_EVENT_TYPES = frozenset({'restore'})


def _parse_curve_pairs(raw_pairs):
    """Deduplicate and validate (model_id, condition_id) pairs from request body."""
    cleaned, seen = [], set()
    for p in (raw_pairs or []):
        try:
            mid = int(p.get('model_id'))
            cid = int(p.get('condition_id'))
        except Exception:
            continue
        t = (mid, cid)
        if t in seen:
            continue
        seen.add(t)
        cleaned.append({'model_id': mid, 'condition_id': cid})
    return cleaned


def _write_curve_logs(user_id, visit_id, pairs, event_type, effect_type, source, action_id):
    """Insert curve-set rows into user_query_logs. Returns count written."""
    if not pairs:
        return 0
    sql = """INSERT INTO user_query_logs
               (user_identifier, visit_id, model_id, condition_id, batch_id, source, event_type, effect_type)
             VALUES (:u,:vid,:m,:c,:b,:s,:etype,:efftype)"""
    batch = action_id or str(uuid.uuid4())
    params = [{
        'u': user_id,
        'vid': visit_id,
        'm': pair['model_id'],
        'c': pair['condition_id'],
        'b': batch,
        's': source,
        'etype': event_type,
        'efftype': effect_type,
    } for pair in pairs]
    with _engine.begin() as conn:
        conn.execute(text(sql), params)
    return len(pairs)


# =========================================
# Response helpers (local aliases)
# =========================================
def _resp_ok(data=None, message=None, meta=None, http_status=200):
    return make_success_response(data, message, meta, http_status)


def _resp_err(error_code, error_message, http_status=400, *, meta=None):
    return make_error_response(error_code, error_message, http_status, meta)


# =========================================
# Routes
# =========================================

@bp.route('/api/visit_start', methods=['POST'])
def api_visit_start():
    try:
        _ = get_or_create_user_identifier()
        uid = g._active_uid
        uid_source = getattr(g, '_uid_source', None)
        row = _fetch_all("SELECT COUNT(*) AS c FROM visit_logs WHERE user_identifier=:u", {'u': uid})
        visit_index = int(row[0]['c']) + 1 if row else 1
        is_new_user = (visit_index == 1)

        data = request.get_json(force=True, silent=True) or {}
        screen_w = int(data.get('screen_w') or 0) or None
        screen_h = int(data.get('screen_h') or 0) or None
        dpr = float(data.get('device_pixel_ratio') or 0) or None
        language = (data.get('language') or '').strip() or None
        is_touch = 1 if data.get('is_touch') else 0
        ui_theme = (data.get('theme') or '').strip() or None

        ua_raw = request.headers.get('User-Agent', '') or None
        dev = _parse_device_basic(ua_raw or '')

        sql = """
        INSERT INTO visit_logs
        (user_identifier, uid_source, visit_index, is_new_user,
         user_agent_raw, os_name, device_type,
         screen_w, screen_h, device_pixel_ratio, language, is_touch,
         ui_theme)
        VALUES
        (:uid, :usrc, :vidx, :isnew,
         :ua, :osn, :dtype,
         :sw, :sh, :dpr, :lang, :touch,
         :theme)
        """
        _exec_write(sql, {
            'uid': uid,
            'usrc': uid_source,
            'vidx': visit_index,
            'isnew': 1 if is_new_user else 0,
            'ua': ua_raw,
            'osn': dev['os_name'],
            'dtype': dev['device_type'],
            'sw': screen_w,
            'sh': screen_h,
            'dpr': dpr,
            'lang': language,
            'touch': is_touch,
            'theme': ui_theme,
        })
        return _resp_ok({'visit_index': visit_index, 'is_new_user': is_new_user})
    except Exception as e:
        _logger.exception(e)
        return _resp_err('INTERNAL_ERROR', str(e), 500)


@bp.post('/api/log_event')
def api_log_event():
    try:
        user_id = get_or_create_user_identifier()
        data = request.get_json(force=True, silent=True) or {}

        event_type_code = (data.get('event_type_code') or '').strip()
        if not event_type_code:
            return _resp_err('INVALID_EVENT', '缺少 event_type_code')

        if len(event_type_code) > 64:
            event_type_code = event_type_code[:64]
        page_key = (data.get('page_key') or 'home').strip() or 'home'
        if len(page_key) > 64:
            page_key = page_key[:64]
        target_url = (data.get('target_url') or '').strip() or None
        if target_url and len(target_url) > 512:
            target_url = target_url[:512]

        model_id = _coerce_to_int_or_none(data.get('model_id'))
        condition_id = _coerce_to_int_or_none(data.get('condition_id'))
        raw_payload = data.get('payload_json')
        if raw_payload is not None:
            if isinstance(raw_payload, (dict, list)):
                payload_json = json.dumps(raw_payload, ensure_ascii=False)
            else:
                payload_json = str(raw_payload)
            if len(payload_json) > 2048:
                payload_json = payload_json[:2048]
        else:
            payload_json = None

        visit_id = _get_latest_visit_id_for_user(user_id)

        sql = """
        INSERT INTO event_logs
          (user_identifier, visit_id, event_type_code, occurred_at, page_key, target_url,
           model_id, condition_id, payload_json)
        VALUES
          (:u, :vid, :type, NOW(), :page_key, :target_url,
           :model_id, :condition_id, :payload_json)
        """
        _exec_write(sql, {
            'u': user_id,
            'vid': visit_id,
            'type': event_type_code,
            'page_key': page_key,
            'target_url': target_url,
            'model_id': model_id,
            'condition_id': condition_id,
            'payload_json': payload_json,
        })
        return _resp_ok({'logged': 1})
    except Exception as e:
        _logger.exception(e)
        return _resp_err('INTERNAL_ERROR', str(e), 500)


@bp.route('/api/like_status', methods=['POST'])
def api_like_status():
    try:
        user_id = get_or_create_user_identifier()
        data = request.get_json(force=True, silent=True) or {}
        raw_pairs = data.get('pairs') or []
        cleaned, seen = [], set()
        for p in raw_pairs:
            try:
                mid = int(p.get('model_id'))
                cid = int(p.get('condition_id'))
            except Exception:
                continue
            t = (mid, cid)
            if t in seen:
                continue
            seen.add(t)
            cleaned.append(t)
        if len(cleaned) > LIKE_STATUS_PAIRS_MAX:
            return _resp_err('TOO_MANY_PAIRS', f'number of pairs exceeds maximum of {LIKE_STATUS_PAIRS_MAX}', 400)
        if not cleaned:
            fp = compute_like_fingerprint(user_id)
            return _resp_ok({'like_keys': [], 'fp': fp})
        conds, params = [], {'u': user_id}
        for i, (m, c) in enumerate(cleaned, start=1):
            conds.append(f"(:m{i}, :c{i})")
            params[f"m{i}"] = m
            params[f"c{i}"] = c
        sql = f"""
          SELECT model_id, condition_id
          FROM user_likes_view
          WHERE user_identifier=:u AND (model_id, condition_id) IN ({",".join(conds)})
        """
        rows = _fetch_all(sql, params)
        like_keys = [f"{int(r['model_id'])}_{int(r['condition_id'])}" for r in rows]
        fp = compute_like_fingerprint(user_id)
        return _resp_ok({'like_keys': like_keys, 'fp': fp})
    except Exception as e:
        _logger.exception(e)
        return _resp_err('INTERNAL_ERROR', str(e), 500)


@bp.route('/api/like_keys', methods=['GET'])
def api_like_keys():
    try:
        user_id = get_or_create_user_identifier()
        keys = get_user_like_keys(user_id)
        fp = compute_like_fingerprint(user_id)
        return _resp_ok({'like_keys': keys, 'fp': fp})
    except Exception as e:
        _logger.exception(e)
        return _resp_err('INTERNAL_ERROR', str(e), 500)


@bp.route('/api/like', methods=['POST'])
def api_like():
    data = request.get_json(force=True, silent=True) or {}
    model_id = data.get('model_id')
    condition_id = data.get('condition_id')
    user_id = get_or_create_user_identifier()
    if not model_id or not condition_id:
        return _resp_err('LIKE_MISSING_IDS', '缺少 model_id 或 condition_id', 400)
    try:
        _exec_write("""INSERT INTO rate_logs (user_identifier, model_id, condition_id, is_valid, rate_id)
                      VALUES (:u,:m,:c,1,1)
                      ON DUPLICATE KEY UPDATE is_valid=1, update_date=NOW()""",
                    {'u': user_id, 'm': model_id, 'c': condition_id})
        fp = compute_like_fingerprint(user_id)
        return _resp_ok({'fp': fp})
    except Exception as e:
        _logger.exception(e)
        return _resp_err('LIKE_DB_WRITE_FAIL', str(e), 500)


@bp.route('/api/unlike', methods=['POST'])
def api_unlike():
    data = request.get_json(force=True, silent=True) or {}
    model_id = data.get('model_id')
    condition_id = data.get('condition_id')
    user_id = get_or_create_user_identifier()
    if not model_id or not condition_id:
        return _resp_err('LIKE_MISSING_IDS', '缺少 model_id 或 condition_id', 400)
    try:
        _exec_write("""UPDATE rate_logs
                      SET is_valid=0, update_date=NOW()
                      WHERE rate_id=1 AND user_identifier=:u AND model_id=:m AND condition_id=:c""",
                    {'u': user_id, 'm': model_id, 'c': condition_id})
        fp = compute_like_fingerprint(user_id)
        return _resp_ok({'fp': fp})
    except Exception as e:
        _logger.exception(e)
        return _resp_err('UNLIKE_DB_WRITE_FAIL', str(e), 500)


@bp.route('/api/recent_likes', methods=['GET'])
def api_recent_likes():
    try:
        user_id = get_or_create_user_identifier()
        items = get_user_likes_full(user_id, limit=RECENT_LIKES_LIMIT)
        fp = compute_like_fingerprint(user_id)
        return _resp_ok({'items': items, 'fp': fp})
    except Exception as e:
        _logger.exception(e)
        return _resp_err('INTERNAL_ERROR', str(e), 500)


@bp.post('/api/curve_set')
def api_curve_set():
    """Unified curve-set logging endpoint."""
    try:
        user_id = get_or_create_user_identifier()
        data = request.get_json(force=True, silent=True) or {}

        event_type = (data.get('event_type') or '').strip()[:32] or None
        if not event_type or event_type not in _VALID_CURVE_EVENT_TYPES:
            return _resp_err('INVALID_EVENT_TYPE',
                             f'event_type must be one of {sorted(_VALID_CURVE_EVENT_TYPES)}', 400)

        effect_type = 'enter' if event_type in _CURVE_ENTER_EVENT_TYPES else 'leave'

        raw_pairs = data.get('pairs') or []
        cleaned = _parse_curve_pairs(raw_pairs)
        if not cleaned:
            return _resp_ok({'logged': 0})

        source = (data.get('source') or '').strip()[:64] or None
        action_id = (data.get('action_id') or '').strip()[:64] or None

        visit_id = _get_latest_visit_id_for_user(user_id)

        if event_type in _CURVE_RESTORE_EVENT_TYPES and visit_id is not None:
            existing = _fetch_all(
                "SELECT 1 FROM user_query_logs WHERE visit_id=:vid LIMIT 1",
                {'vid': visit_id}
            )
            if existing:
                _logger.debug(
                    '[curve_set] restore suppressed for visit_id=%s (same-visit refresh)', visit_id
                )
                return _resp_ok({'logged': 0, 'suppressed': True})

        logged = _write_curve_logs(user_id, visit_id, cleaned, event_type, effect_type, source, action_id)
        return _resp_ok({'logged': logged})
    except Exception as e:
        _logger.exception(e)
        return _resp_err('INTERNAL_ERROR', str(e), 500)


_VALID_RADAR_ACTIONS = frozenset({'add', 'remove', 'restore', 'clear_all'})


@bp.post('/api/radar_models')
def api_radar_models():
    """Unified radar model-set logging endpoint."""
    try:
        user_id = get_or_create_user_identifier()
        data = request.get_json(force=True, silent=True) or {}

        action = (data.get('action') or '').strip()
        if action not in _VALID_RADAR_ACTIONS:
            return _resp_err('INVALID_ACTION', f'action must be one of {sorted(_VALID_RADAR_ACTIONS)}', 400)

        model_id = _coerce_to_int_or_none(data.get('model_id'))
        if action in ('add', 'remove') and model_id is None:
            return _resp_err('MISSING_MODEL_ID', 'model_id is required for add/remove actions', 400)

        source = (data.get('source') or '').strip()[:64] or None
        action_id = (data.get('action_id') or '').strip()[:64] or None

        effect_type = 'enter' if action in ('add', 'restore') else 'leave'
        visit_id = _get_latest_visit_id_for_user(user_id)

        if action == 'restore' and visit_id is not None:
            existing = _fetch_all(
                "SELECT 1 FROM user_radar_logs WHERE visit_id=:vid LIMIT 1",
                {'vid': visit_id}
            )
            if existing:
                _logger.debug(
                    '[radar_models] restore suppressed for visit_id=%s (same-visit refresh)', visit_id
                )
                return _resp_ok({'logged': 0, 'suppressed': True})

        sql = """
        INSERT INTO user_radar_logs
          (user_identifier, visit_id, model_id, event_type, effect_type, source, action_id)
        VALUES
          (:u, :vid, :mid, :etype, :efftype, :src, :aid)
        """
        _exec_write(sql, {
            'u': user_id,
            'vid': visit_id,
            'mid': model_id,
            'etype': action,
            'efftype': effect_type,
            'src': source,
            'aid': action_id,
        })
        return _resp_ok({'logged': 1})
    except Exception as e:
        _logger.exception(e)
        return _resp_err('INTERNAL_ERROR', str(e), 500)
