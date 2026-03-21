import os
import uuid
import logging
import time
import threading
import math
import signal
import json
import hashlib
import requests  # Added for HTTP client
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Tuple, Any
from collections import defaultdict

from flask import Flask, request, render_template, session, jsonify, g, make_response, send_from_directory
from sqlalchemy import create_engine, text
from sqlalchemy import exc as sa_exc
from user_agents import parse as parse_ua
from werkzeug.middleware.proxy_fix import ProxyFix

from app.curves.pchip_cache import get_or_build_unified_perf_model, eval_pchip, load_unified_perf_model
from app.audio_services import spectrum_cache
from app.audio_services import spectrum_reader
from app.audio_services import sweep_audio_player
from app.common_utils import (
    sign_uid, unsign_uid, make_success_response, make_error_response,
    db_fetch_all, db_exec_write
)
from app.asset_manifest import ManifestLoader, create_asset_url_helper

CODE_VERSION = os.getenv('CODE_VERSION', '')

# =========================================
# App / Config
# =========================================
from logging.config import dictConfig

dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': { 'default': { 'format': '[%(asctime)s] %(levelname)s in %(name)s: %(message)s' } },
    'handlers': { 'wsgi': { 'class': 'logging.StreamHandler', 'stream': 'ext://sys.stdout', 'formatter': 'default' } },
    'root': { 'level': 'WARNING', 'handlers': ['wsgi'] },
    'loggers': {
        'werkzeug': {'level': 'WARNING', 'propagate': True},
        'sqlalchemy.engine': {'level': 'WARNING', 'propagate': False},
        'sqlalchemy.pool': {'level': 'WARNING', 'propagate': False},
        'curves.spectrum_builder': {'level': 'WARNING', 'propagate': True},
        'fancoolserver.spectrum': {'level': 'WARNING', 'propagate': True},
        'app.audio_services.sweep_audio_player': {'level': 'WARNING', 'propagate': True}
    }
})

app = Flask(__name__)
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=30)
app.secret_key = os.getenv('APP_SECRET', 'replace-me-in-prod')
app.config['SESSION_COOKIE_SECURE'] = os.getenv('SESSION_COOKIE_SECURE', '0') == '1'

# Setup asset manifest loader for webpack bundles
_manifest_path = os.path.join(os.path.dirname(__file__), 'static', 'dist', 'manifest.json')
_manifest_loader = ManifestLoader(_manifest_path)
_asset_url_fn = create_asset_url_helper(_manifest_loader, static_prefix='/static/app-dist')
_manifest_loader.get_asset('app.js')

@app.context_processor
def inject_asset_url():
    """Make asset_url function available in all templates."""
    return {'asset_url': _asset_url_fn}

# Local development static route mapping for webpack dist assets
# In production, nginx maps /static/app-dist/ to the dist directory
@app.route('/static/app-dist/<path:filename>')
def serve_app_dist(filename):
    """
    Serve webpack dist assets from app/static/dist/ when running locally without nginx.
    
    Note: Flask's send_from_directory() automatically:
    - Prevents directory traversal attacks
    - Returns 404 for missing files
    - Sets appropriate content-type headers
    """
    dist_dir = os.path.join(os.path.dirname(__file__), 'static', 'dist')
    return send_from_directory(dist_dir, filename)

slog = logging.getLogger('fancoolserver.spectrum')



def _on_sighup(signum, frame):
    try:
        pchip_cache.reload_curve_params_from_env()
        app.logger.info("Reloaded curve params from env via SIGHUP")
    except Exception as e:
        app.logger.exception("Reload curve params failed: %s", e)

try:
    signal.signal(signal.SIGHUP, _on_sighup)
except Exception:
    # Windows / 一些环境不支持 SIGHUP，可忽略
    pass



DB_DSN = os.getenv(
    'FANDB_DSN',
    'mysql+pymysql://appuser:12345678@127.0.0.1/FANDB?charset=utf8mb4'
)
engine = create_engine(
    DB_DSN,
    pool_pre_ping=True,
    pool_recycle=1800,
    future=True
)
# Inject engine into modules that need database access
spectrum_reader.set_engine(engine)

# Admin internal API configuration (for autofix service)
ADMIN_INTERNAL_BASE_URL = os.getenv('ADMIN_INTERNAL_BASE_URL', 'http://127.0.0.1:6001')
ADMIN_INTERNAL_TIMEOUT_SEC = int(os.getenv('ADMIN_INTERNAL_TIMEOUT_SEC', '10'))

SIZE_OPTIONS = ["不限", "120"] #, "140"]
TOP_QUERIES_LIMIT = 100
RECENT_LIKES_LIMIT = 100
CLICK_COOLDOWN_SECONDS = 0.5
RECENT_UPDATES_LIMIT = 100
SPECTRUM_DOCK_ENABLED = os.getenv('SPECTRUM_DOCK_ENABLED', '') == '1'
PLAY_AUDIO_ENABLED = os.getenv('PLAY_AUDIO_ENABLED', '') == '1'
query_count_cache = 0
announcement_cache: List[dict] | None = None
# Announcement state: active items, upcoming scheduled items, fingerprint, next_change_at
_announcement_state: dict = {
    'active': [],
    'upcoming': [],
    'fingerprint': '',
    'next_change_at': None,
}

# =========================================
# Scoring config helpers
# =========================================

def _parse_json_float_map(env_var: str, key_type=str, clamp_negative: bool = False) -> dict | None:
    """Parse a JSON-encoded float mapping from an environment variable.

    Returns a dict on success, or None if the env var is unset/empty.
    Raises ValueError with a clear message on invalid JSON or non-numeric values.
    When clamp_negative is True, negative values are silently clamped to 0.0.
    """
    raw = os.getenv(env_var, '').strip()
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Environment variable {env_var!r} contains invalid JSON: {exc}"
        ) from exc
    if not isinstance(parsed, dict):
        raise ValueError(
            f"Environment variable {env_var!r} must be a JSON object (got {type(parsed).__name__})"
        )
    result = {}
    for k, v in parsed.items():
        try:
            fv = float(v)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Environment variable {env_var!r}: value for key {k!r} is not numeric: {v!r}"
            ) from exc
        if not math.isfinite(fv):
            raise ValueError(
                f"Environment variable {env_var!r}: value for key {k!r} must be finite, got {v!r}"
            )
        if clamp_negative and fv < 0:
            fv = 0.0
        try:
            typed_key = key_type(k)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Environment variable {env_var!r}: key {k!r} cannot be converted to {key_type.__name__}: {exc}"
            ) from exc
        result[typed_key] = fv
    return result


# =========================================
# ABC Score Config
# =========================================

# --- Condition-score ALPHA values ---
# Single concentrated env var: CONDITION_SCORE_ALPHA_VALUES_JSON={"A":1.0,"B":0.6,"D":0.8}
# Overrides the local defaults below; omitted keys fall back to the local defaults.
_DEFAULT_SCORE_ALPHA = {'A': 1.0, 'B': 0.6, 'D': 1.0}
_env_score_alpha = _parse_json_float_map('CONDITION_SCORE_ALPHA_VALUES_JSON')
_score_alpha = dict(_DEFAULT_SCORE_ALPHA)
if _env_score_alpha is not None:
    _score_alpha.update(_env_score_alpha)

SCORE_ALPHA_A: float = _score_alpha.get('A', _DEFAULT_SCORE_ALPHA['A'])
SCORE_ALPHA_B: float = _score_alpha.get('B', _DEFAULT_SCORE_ALPHA['B'])
SCORE_ALPHA_D: float = _score_alpha.get('D', _DEFAULT_SCORE_ALPHA['D'])

# --- WA2 anchor model ID ---
# Controls WA2 reference dB clamping.  Source priority: env var > local default (21).
#
# WA2_ANCHOR_MODEL_ID == 0  →  no-clamp mode (original WA2 behavior):
#   wa2_ref_db = condition-wide high_db  (no upward adjustment)
#
# WA2_ANCHOR_MODEL_ID > 0  →  clamp mode:
#   wa2_ref_db = max(condition-wide high_db, anchor_model_max_db_for_condition)
#   If the anchor model has no usable max dB data for a condition, that condition
#   falls back to wa2_ref_db = high_db (same as no-clamp) for that condition only.
#
# In both modes, models whose max dB is below wa2_ref_db fall back to their own
# max airflow for the WA2 raw value.
_DEFAULT_WA2_ANCHOR_MODEL_ID = 21
_env_wa2_anchor_raw = os.getenv('WA2_ANCHOR_MODEL_ID', '').strip()
if _env_wa2_anchor_raw:
    try:
        WA2_ANCHOR_MODEL_ID: int = int(_env_wa2_anchor_raw)
    except ValueError as _exc:
        raise ValueError(
            f"WA2_ANCHOR_MODEL_ID must be an integer, got {_env_wa2_anchor_raw!r}"
        ) from _exc
else:
    WA2_ANCHOR_MODEL_ID: int = _DEFAULT_WA2_ANCHOR_MODEL_ID

# --- Condition-score A/B/C/D dimension weights ---
# Single concentrated env var:
# CONDITION_SCORE_DIMENSION_WEIGHTS_JSON={"WA1":0.25,"WA2":0.10,"WA3":0.40,"WB":0.00,"WC":0.05,"WD":0.20}
# Overrides the local defaults below; omitted keys fall back to the local defaults.
# Note: WA2 defaults to 0.10 so WA2_ANCHOR_MODEL_ID has an observable effect on score_total.
#       Set WA2 to 0 to disable the WA2 dimension (and make WA2_ANCHOR_MODEL_ID inert).
_DEFAULT_SCORE_DIM_WEIGHTS = {
    'WA1': 0.20, 'WA2': 0.30, 'WA3': 0.25,
    'WB': 0.05, 'WC': 0.05, 'WD': 0.15,
}
_env_score_dim_weights = _parse_json_float_map('CONDITION_SCORE_DIMENSION_WEIGHTS_JSON')
_score_dim_weights = dict(_DEFAULT_SCORE_DIM_WEIGHTS)
if _env_score_dim_weights is not None:
    _score_dim_weights.update(_env_score_dim_weights)

SCORE_WA1: float = _score_dim_weights.get('WA1', _DEFAULT_SCORE_DIM_WEIGHTS['WA1'])
SCORE_WA2: float = _score_dim_weights.get('WA2', _DEFAULT_SCORE_DIM_WEIGHTS['WA2'])
SCORE_WA3: float = _score_dim_weights.get('WA3', _DEFAULT_SCORE_DIM_WEIGHTS['WA3'])
SCORE_WB: float = _score_dim_weights.get('WB',  _DEFAULT_SCORE_DIM_WEIGHTS['WB'])
SCORE_WC: float = _score_dim_weights.get('WC',  _DEFAULT_SCORE_DIM_WEIGHTS['WC'])
SCORE_WD: float = _score_dim_weights.get('WD',  _DEFAULT_SCORE_DIM_WEIGHTS['WD'])

# All ABC scores and totals are on a 0–SCORE_MAX scale (100-point)
SCORE_MAX = 100
# Fixed condition IDs eligible for ABC scoring
SCORE_CONDITION_IDS: frozenset = frozenset({1, 2, 3, 7, 8, 10})

# --- Composite score condition weights ---
# Single concentrated env var (highest priority):
#   COMPOSITE_SCORE_CONDITION_WEIGHTS_JSON={"1":1.2,"2":1.0,"3":0.7,"7":1.0,"8":1.0,"10":1.0}
# Overrides the local defaults below; omitted condition keys fall back to the local defaults.
# Missing conditions still default to 1.0.  Negative values are clamped to 0.0.  0 is allowed.
_DEFAULT_COMPOSITE_WEIGHTS: Dict[int, float] = {
    1: 0.0, 2: 2.0, 3: 2.0, 7: 3.0, 8: 2.0, 10: 3.0,
}
_env_composite_weights = _parse_json_float_map('COMPOSITE_SCORE_CONDITION_WEIGHTS_JSON',
                                                key_type=int, clamp_negative=True)
_composite_weights_merged = dict(_DEFAULT_COMPOSITE_WEIGHTS)
if _env_composite_weights is not None:
    _composite_weights_merged.update(_env_composite_weights)

COMPOSITE_WEIGHTS: Dict[int, float] = {
    cid: max(0.0, _composite_weights_merged.get(cid, 1.0))
    for cid in SCORE_CONDITION_IDS
}

# =========================================
# Scoring profiles: LOW / MED / HI
# =========================================
# Each profile is an independent combination of:
#   - WA2 anchor model ID
#   - alpha values (A, B, D exponents)
#   - dimension weights (WA1, WA2, WA3, WB, WC, WD)
#
# Environment variable naming convention:
#   WA2_ANCHOR_MODEL_ID_LOW / _MED / _HI
#   CONDITION_SCORE_ALPHA_VALUES_JSON_LOW / _MED / _HI
#   CONDITION_SCORE_DIMENSION_WEIGHTS_JSON_LOW / _MED / _HI
#
# Backward-compatibility rules:
#   - MED falls back to the legacy single-profile env vars (WA2_ANCHOR_MODEL_ID,
#     CONDITION_SCORE_ALPHA_VALUES_JSON, CONDITION_SCORE_DIMENSION_WEIGHTS_JSON).
#   - LOW and HI fall back to MED when their profile-specific vars are absent.

SCORING_PROFILES = ('low', 'med', 'hi')
_DEFAULT_SCORE_PROFILE = 'med'


def _parse_wa2_anchor(env_suffix: str, fallback: int) -> int:
    """Parse WA2_ANCHOR_MODEL_ID{env_suffix} with integer validation; fallback on missing."""
    raw = os.getenv(f'WA2_ANCHOR_MODEL_ID{env_suffix}', '').strip()
    if not raw:
        return fallback
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(
            f"Environment variable WA2_ANCHOR_MODEL_ID{env_suffix} must be an integer, got {raw!r}"
        ) from exc


def _resolve_alpha(env_suffix: str, fallback: dict) -> dict:
    """Parse CONDITION_SCORE_ALPHA_VALUES_JSON{env_suffix}; fall back to fallback dict."""
    parsed = _parse_json_float_map(f'CONDITION_SCORE_ALPHA_VALUES_JSON{env_suffix}')
    if parsed is None:
        return dict(fallback)
    result = dict(fallback)
    result.update(parsed)
    return result


def _resolve_dim_weights(env_suffix: str, fallback: dict) -> dict:
    """Parse CONDITION_SCORE_DIMENSION_WEIGHTS_JSON{env_suffix}; fall back to fallback dict."""
    parsed = _parse_json_float_map(f'CONDITION_SCORE_DIMENSION_WEIGHTS_JSON{env_suffix}')
    if parsed is None:
        return dict(fallback)
    result = dict(fallback)
    result.update(parsed)
    return result


def _build_profile_cfg(wa2_anchor: int, alpha: dict, dim_weights: dict) -> dict:
    """Pack a profile's scoring parameters into a single config dict."""
    return {
        'wa2_anchor': wa2_anchor,
        'alpha_a': alpha.get('A', _DEFAULT_SCORE_ALPHA['A']),
        'alpha_b': alpha.get('B', _DEFAULT_SCORE_ALPHA['B']),
        'alpha_d': alpha.get('D', _DEFAULT_SCORE_ALPHA['D']),
        'score_wa1': dim_weights.get('WA1', _DEFAULT_SCORE_DIM_WEIGHTS['WA1']),
        'score_wa2': dim_weights.get('WA2', _DEFAULT_SCORE_DIM_WEIGHTS['WA2']),
        'score_wa3': dim_weights.get('WA3', _DEFAULT_SCORE_DIM_WEIGHTS['WA3']),
        'score_wb':  dim_weights.get('WB',  _DEFAULT_SCORE_DIM_WEIGHTS['WB']),
        'score_wc':  dim_weights.get('WC',  _DEFAULT_SCORE_DIM_WEIGHTS['WC']),
        'score_wd':  dim_weights.get('WD',  _DEFAULT_SCORE_DIM_WEIGHTS['WD']),
    }


# MED profile: uses legacy env vars as primary, _MED suffix as override.
_med_alpha   = _resolve_alpha('_MED', _score_alpha)
_med_weights = _resolve_dim_weights('_MED', _score_dim_weights)
_med_wa2     = _parse_wa2_anchor('_MED', WA2_ANCHOR_MODEL_ID)
_med_cfg     = _build_profile_cfg(_med_wa2, _med_alpha, _med_weights)

# LOW profile: falls back to MED when _LOW vars are absent.
_low_alpha   = _resolve_alpha('_LOW', _med_alpha)
_low_weights = _resolve_dim_weights('_LOW', _med_weights)
_low_wa2     = _parse_wa2_anchor('_LOW', _med_wa2)
_low_cfg     = _build_profile_cfg(_low_wa2, _low_alpha, _low_weights)

# HI profile: falls back to MED when _HI vars are absent.
_hi_alpha    = _resolve_alpha('_HI', _med_alpha)
_hi_weights  = _resolve_dim_weights('_HI', _med_weights)
_hi_wa2      = _parse_wa2_anchor('_HI', _med_wa2)
_hi_cfg      = _build_profile_cfg(_hi_wa2, _hi_alpha, _hi_weights)

PROFILE_CONFIGS: Dict[str, dict] = {
    'low': _low_cfg,
    'med': _med_cfg,
    'hi':  _hi_cfg,
}

# Soft/Hard TTL for per-model ABC in-memory cache
ABC_SOFT_TTL_SEC = int(os.getenv('ABC_SOFT_TTL_SEC', str(10 * 60)))   # 10 min
ABC_HARD_TTL_SEC = int(os.getenv('ABC_HARD_TTL_SEC', str(120 * 60)))   # 120 min

# Per-condition denominator caches — one dict per scoring profile.
# Structure: {profile_key: {cid: {'low_db', 'high_db', 'wa2_ref_db', 'mid_db',
#                                  'max_a1', 'max_a2', 'max_a3',
#                                  'max_b', 'max_c', 'max_d', 'cached_at'}}}
# wa2_ref_db and max_a2 differ between profiles (driven by each profile's wa2_anchor).
_cond_denom_cache: Dict[str, Dict[int, dict]] = {p: {} for p in SCORING_PROFILES}
_cond_denom_lock = threading.Lock()

# Per-model ABC cache with soft/hard TTL — one dict per scoring profile.
# Structure: {profile_key: {model_id: {'conditions', 'composite_score',
#                                       'updated_at', 'cached_at'}}}
_abc_cache: Dict[str, Dict[int, dict]] = {p: {} for p in SCORING_PROFILES}
_abc_cache_lock = threading.Lock()

# Inflight soft-refresh deduplication per (profile_key, model_id)
_abc_inflight: set = set()
_abc_inflight_lock = threading.Lock()

# UID cookie config
UID_COOKIE_NAME = os.getenv('UID_COOKIE_NAME', 'fc_uid')
UID_COOKIE_MAX_AGE = int(os.getenv('UID_COOKIE_MAX_AGE_SECONDS', str(60 * 60 * 24 * 365 * 2)))
UID_COOKIE_SAMESITE = os.getenv('UID_COOKIE_SAMESITE', 'Lax')
UID_COOKIE_SECURE = os.getenv('UID_COOKIE_SECURE', '0') == '1'
UID_COOKIE_HTTPONLY = os.getenv('UID_COOKIE_HTTPONLY', '0') == '1'
UID_COOKIE_REFRESH_INTERVAL = int(os.getenv('UID_COOKIE_REFRESH_INTERVAL_SECONDS', str(60 * 60 * 24 * 7)))
UID_COOKIE_REFRESH_TS_NAME = os.getenv('UID_COOKIE_REFRESH_TS_NAME', 'fc_uid_refreshed_at')
UID_COOKIE_DOMAIN = os.getenv('UID_COOKIE_DOMAIN', '.fancool.cc')

# =========================================
# Middleware / Headers
# =========================================
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_for=1)
app.config['SESSION_COOKIE_HTTPONLY'] = os.getenv('SESSION_COOKIE_HTTPONLY', '1') == '1'
app.config['SESSION_COOKIE_SAMESITE'] = os.getenv('SESSION_COOKIE_SAMESITE', 'Lax')
app.config['SESSION_COOKIE_PATH'] = '/'


@app.after_request
def add_security_headers(resp):
    try:
        if request.is_secure:
            resp.headers.setdefault(
                'Strict-Transport-Security',
                'max-age=31536000; includeSubDomains; preload'
            )
        resp.headers.setdefault('X-Frame-Options', 'SAMEORIGIN')
        resp.headers.setdefault('X-Content-Type-Options', 'nosniff')
        resp.headers.setdefault('Referrer-Policy', 'strict-origin-when-cross-origin')
    except Exception:
        pass
    return resp

# =========================================
# Unified Response Helpers
# =========================================
def resp_ok(data: Any = None, message: str | None = None,
            meta: dict | None = None, http_status: int = 200):
    """Wrapper for common response helper."""
    return make_success_response(data, message, meta, http_status)


def resp_err(error_code: str, error_message: str,
             http_status: int = 400, *,
             meta: dict | None = None):
    """Wrapper for common response helper."""
    return make_error_response(error_code, error_message, http_status, meta)



# =========================================
# UID Signing
# =========================================
def _sign_uid(value: str) -> str:
    """Wrapper for common UID signing."""
    return sign_uid(value, app.secret_key)


def _unsign_uid(token: str) -> str | None:
    """Wrapper for common UID unsigning."""
    return unsign_uid(token, app.secret_key)



@app.before_request
def _init_g_defaults():
    if not hasattr(g, '_uid_source'):
        g._uid_source = None


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


@app.after_request
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
# Announcement meta piggyback hook
# =========================================
@app.after_request
def _inject_announcement_meta(resp):
    """Inject lightweight announcement fingerprint metadata into all successful JSON API responses.
    This lets the frontend detect announcement state changes via normal business requests.
    """
    try:
         if (
            resp.status_code == 200
            and resp.content_type
            and resp.content_type.startswith("application/json")
        ):
            # Let Flask determine when JSON is appropriate; avoid forcing JSON parsing.
            body = resp.get_json(silent=True)
            # Only proceed if the body is a JSON object (dict) with success == True.
            if isinstance(body, dict) and body.get("success") is True:
                if "meta" not in body or not isinstance(body.get("meta"), dict):
                    body["meta"] = {}
                body["meta"]["announcement_meta"] = get_announcement_meta()
                # Use Flask's JSON provider to keep encoding behavior consistent.
                resp.set_data(app.json.dumps(body))
    except Exception:
        pass
    return resp

# =========================================
# DB Helpers
# =========================================
def fetch_all(sql: str, params: dict = None) -> List[dict]:
    """Wrapper for common database fetch helper."""
    return db_fetch_all(engine, sql, params)


def exec_write(sql: str, params: dict = None):
    """Wrapper for common database write helper."""
    db_exec_write(engine, sql, params)




# =========================================
# Admin Internal API Client
# =========================================
def call_admin_autofix_api(audio_batch_id: str, 
                           model_id: int, 
                           condition_id: int,
                           params: dict | None = None,
                           param_hash: str | None = None) -> dict | None:
    """
    Call the admin internal autofix API to request a spectrum model rebuild.
    
    Returns:
        dict with job_id, status, reused fields on success
        None on failure (logged but not raised)
    """
    try:
        url = f"{ADMIN_INTERNAL_BASE_URL}/admin/api/internal/autofix"
        headers = {
            'Content-Type': 'application/json'
        }
        payload = {
            'audio_batch_id': audio_batch_id,
            'model_id': model_id,
            'condition_id': condition_id
        }
        if params is not None:
            payload['params'] = params
        if param_hash:
            payload['param_hash'] = param_hash
        
        app.logger.info('[admin-autofix-client] Calling %s for pair (%s, %s)', 
                       url, model_id, condition_id)
        
        response = requests.post(
            url,
            json=payload,
            headers=headers,
            timeout=ADMIN_INTERNAL_TIMEOUT_SEC
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('success'):
                data = result.get('data', {})
                app.logger.info('[admin-autofix-client] Success: job_id=%s, reused=%s', 
                              data.get('job_id'), data.get('reused'))
                return data
            else:
                app.logger.error('[admin-autofix-client] API returned success=false: %s', 
                               result.get('error', {}).get('message'))
                return None
        else:
            app.logger.error('[admin-autofix-client] HTTP %s from admin API: %s', 
                           response.status_code, response.text[:200])
            return None
            
    except requests.exceptions.Timeout:
        app.logger.error('[admin-autofix-client] Timeout calling admin API after %ss', 
                        ADMIN_INTERNAL_TIMEOUT_SEC)
        return None
    except requests.exceptions.RequestException as e:
        app.logger.error('[admin-autofix-client] Request error: %s', e)
        return None
    except Exception as e:
        app.logger.exception('[admin-autofix-client] Unexpected error: %s', e)
        return None

# =========================================
# Utilities
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



# =========================================
# FNV Fingerprint for Likes (保持 c/x/s)
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
    rows = fetch_all("""
        SELECT u.user_identifier, u.model_id, u.condition_id,
               u.brand_name_zh, u.model_name,
               u.condition_name_zh, u.resistance_type_zh, u.resistance_location_zh,
               u.max_speed, u.size, u.thickness,
               fm.reference_price,
               COALESCE(fm.rgb_light, '') AS rgb_light
        FROM user_likes_view u
        LEFT JOIN available_models_info_view fm ON fm.model_id = u.model_id
        WHERE u.user_identifier = :u
    """, {'u': user_identifier})
    return rows if limit is None else rows[:limit]


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
# ABC Score Helpers
# =========================================

def _compute_abc_for_model(model_id: int, profile_key: str = _DEFAULT_SCORE_PROFILE) -> dict | None:
    """
    Compute A1/A2/A3/B/C/D scores for model_id across all SCORE_CONDITION_IDS.

    Pure computation function: queries DB + PCHIP for raw values and uses
    in-memory _cond_denom_cache for normalization denominators.

    A1 uses low_db (intersection lower bound).
    A2 uses wa2_ref_db (high_db clamped upward by the WA2 anchor model's max dB);
      if a model's max dB is below wa2_ref_db its a2_raw falls back to that model's max airflow.
    A3 uses mid_db; if a model's max dB is below mid_db its a3_raw falls back to max airflow.

    If a condition's common dB range is invalid (low_db > high_db) or not yet
    populated in the denominator cache, that condition is omitted from the result
    so all models receive a null/missing score for it.

    D dimension: d_raw = max_airflow / noise_dB_at_max_airflow (simple engineering
    metric from the noise_to_airflow PCHIP curve). Disabled by default (SCORE_WD=0).

    Returns {cid: {score_a1, score_a2, score_a3, score_a, score_b, score_c, score_d,
                   score_total, a1_raw, a2_raw, a3_raw, b_raw, c_raw, d_raw}}
    or None if no conditions could be computed.
    """
    result: Dict[int, dict] = {}
    for cid in SCORE_CONDITION_IDS:
        # Load or rebuild PCHIP
        mdl = load_unified_perf_model(model_id, cid)
        if mdl is None:
            try:
                spectrum_reader.build_performance_pchips([(model_id, cid)])
                mdl = load_unified_perf_model(model_id, cid)
            except Exception as e:
                app.logger.warning('[abc] pchip (%s,%s): %s', model_id, cid, e)
        if mdl is None:
            continue

        pchip_data = mdl.get('pchip') or {}

        # Get denominators from in-memory cache (set by _do_denom_refresh)
        with _cond_denom_lock:
            denom_entry = _cond_denom_cache[profile_key].get(cid)
        low_db     = denom_entry.get('low_db')     if denom_entry else None
        high_db    = denom_entry.get('high_db')    if denom_entry else None
        wa2_ref_db = denom_entry.get('wa2_ref_db') if denom_entry else None
        mid_db     = denom_entry.get('mid_db')     if denom_entry else None
        max_a1  = float(denom_entry.get('max_a1') or 0.0) if denom_entry else 0.0
        max_a2  = float(denom_entry.get('max_a2') or 0.0) if denom_entry else 0.0
        max_a3  = float(denom_entry.get('max_a3') or 0.0) if denom_entry else 0.0
        max_b   = float(denom_entry.get('max_b')  or 0.0) if denom_entry else 0.0
        max_c   = float(denom_entry.get('max_c')  or 0.0) if denom_entry else 0.0
        max_d   = float(denom_entry.get('max_d')  or 0.0) if denom_entry else 0.0

        # Validate common dB range; skip this condition if invalid so all models
        # get a null score rather than a silent fallback.
        if low_db is None or high_db is None or low_db > high_db:
            app.logger.warning(
                '[abc] cid=%s model=%s: invalid/missing dB range (low_db=%s, high_db=%s), '
                'skipping condition', cid, model_id, low_db, high_db
            )
            continue

        # A1/A2/A3: evaluate noise_to_airflow PCHIP at reference dB points.
        # WA2 uses wa2_ref_db from cache (high_db when no-clamp, clamped upward otherwise);
        # WA2 and WA3 fall back to model max airflow when the model cannot reach the reference dB.
        a1_raw = a2_raw = a3_raw = 0.0
        n2a = pchip_data.get('noise_to_airflow')
        if n2a and isinstance(n2a, dict) and n2a.get('x'):
            n2a_xs = n2a.get('x') or []
            n2a_ys = n2a.get('y') or []
            model_max_db = float(n2a_xs[-1]) if n2a_xs else None
            model_max_airflow = float(max(n2a_ys)) if n2a_ys else 0.0

            # A1: always eval at low_db (all models' max dB >= high_db >= low_db for valid conditions)
            try:
                val = eval_pchip(n2a, low_db)
                if val is not None and math.isfinite(val) and val > 0:
                    a1_raw = float(val)
            except Exception:
                pass

            # A2: use effective WA2 ref dB; fall back to model max airflow if model can't reach it.
            # eff_wa2 falls back to high_db which is guaranteed non-None at this point.
            eff_wa2 = wa2_ref_db if wa2_ref_db is not None else high_db
            if eff_wa2 is None:
                pass  # no WA2 reference available for this condition
            elif model_max_db is not None and model_max_db < eff_wa2:
                a2_raw = model_max_airflow
            else:
                try:
                    val = eval_pchip(n2a, eff_wa2)
                    if val is not None and math.isfinite(val) and val > 0:
                        a2_raw = float(val)
                except Exception:
                    pass

            # A3: eval at mid_db; fall back to model max airflow if model can't reach mid_db
            if model_max_db is not None and mid_db is not None and model_max_db < mid_db:
                a3_raw = model_max_airflow
            else:
                try:
                    val = eval_pchip(n2a, mid_db)
                    if val is not None and math.isfinite(val) and val > 0:
                        a3_raw = float(val)
                except Exception:
                    pass

        # B: max airflow (highest y value in rpm_to_airflow PCHIP)
        b_raw = 0.0
        r2a = pchip_data.get('rpm_to_airflow')
        if r2a and isinstance(r2a, dict):
            ys = r2a.get('y') or []
            if ys:
                try:
                    b_raw = float(max(ys))
                except Exception:
                    pass

        # C: like count for this model/condition
        lc = 0
        try:
            like_rows = fetch_all(
                "SELECT COALESCE(MAX(like_count), 0) AS like_count "
                "FROM general_view WHERE model_id=:m AND condition_id=:c",
                {'m': model_id, 'c': cid}
            )
            lc = max(0, int((like_rows[0].get('like_count') or 0))) if like_rows else 0
        except Exception as e:
            app.logger.warning('[abc] like_count (%s,%s): %s', model_id, cid, e)
        c_raw = math.log1p(lc)

        # D: max-airflow-to-noise ratio at the max airflow operating point.
        # Uses noise_to_airflow PCHIP: x=noise_db (sorted asc), y=airflow.
        # The point with maximum airflow gives both max_airflow and its noise_dB.
        d_raw = 0.0
        if n2a and isinstance(n2a, dict):
            n2a_x = n2a.get('x') or []
            n2a_y = n2a.get('y') or []
            if n2a_x and n2a_y and len(n2a_x) == len(n2a_y):
                try:
                    max_af = float(max(n2a_y))
                    max_af_idx = n2a_y.index(max_af)
                    noise_at_max = float(n2a_x[max_af_idx])
                    if max_af > 0 and noise_at_max > 0:
                        d_raw = max_af / noise_at_max
                except Exception:
                    pass

        # Scale A1/A2/A3 using existing A scoring function at each dB point
        pcfg = PROFILE_CONFIGS[profile_key]
        alpha_a = pcfg['alpha_a']
        alpha_b = pcfg['alpha_b']
        alpha_d = pcfg['alpha_d']
        p_wa1 = pcfg['score_wa1']
        p_wa2 = pcfg['score_wa2']
        p_wa3 = pcfg['score_wa3']
        p_wb  = pcfg['score_wb']
        p_wc  = pcfg['score_wc']
        p_wd  = pcfg['score_wd']
        a1_scaled = pow(a1_raw, alpha_a) if a1_raw > 0 else 0.0
        a2_scaled = pow(a2_raw, alpha_a) if a2_raw > 0 else 0.0
        a3_scaled = pow(a3_raw, alpha_a) if a3_raw > 0 else 0.0
        b_scaled  = pow(b_raw,  alpha_b) if b_raw  > 0 else 0.0
        d_scaled  = pow(d_raw,  alpha_d) if d_raw  > 0 else 0.0

        # Denominators (pow of raw max to match scaling)
        denom_a1 = pow(max_a1, alpha_a) if max_a1 > 0 else 1.0
        denom_a2 = pow(max_a2, alpha_a) if max_a2 > 0 else 1.0
        denom_a3 = pow(max_a3, alpha_a) if max_a3 > 0 else 1.0
        denom_b  = pow(max_b,  alpha_b) if max_b  > 0 else 1.0
        denom_c  = max_c if max_c > 0 else 1.0
        denom_d  = pow(max_d,  alpha_d) if max_d  > 0 else 1.0

        na1 = min(1.0, a1_scaled / denom_a1)
        na2 = min(1.0, a2_scaled / denom_a2)
        na3 = min(1.0, a3_scaled / denom_a3)
        nb  = min(1.0, b_scaled  / denom_b)
        nc  = min(1.0, c_raw     / denom_c)
        nd  = min(1.0, d_scaled  / denom_d)
        # min(1.0, ...) caps scores at 100 when denominators are temporarily stale.

        score_a1_f = na1 * SCORE_MAX
        score_a2_f = na2 * SCORE_MAX
        score_a3_f = na3 * SCORE_MAX
        score_b_f  = nb  * SCORE_MAX
        score_c_f  = nc  * SCORE_MAX
        score_d_f  = nd  * SCORE_MAX
        wa_sum = p_wa1 + p_wa2 + p_wa3
        score_total = round(
            p_wa1 * score_a1_f +
            p_wa2 * score_a2_f +
            p_wa3 * score_a3_f +
            p_wb  * score_b_f  +
            p_wc  * score_c_f  +
            p_wd  * score_d_f
        )

        result[cid] = {
            'score_a1': round(score_a1_f),
            'score_a2': round(score_a2_f),
            'score_a3': round(score_a3_f),
            # score_a is a backward-compatible summary using the same A weights as score_total
            'score_a': round((p_wa1 * score_a1_f + p_wa2 * score_a2_f + p_wa3 * score_a3_f)
                             / wa_sum if wa_sum > 0 else 0),
            'score_b': round(score_b_f),
            'score_c': round(score_c_f),
            'score_d': round(score_d_f),
            'score_total': score_total,
            'a1_raw': a1_raw,
            'a2_raw': a2_raw,
            'a3_raw': a3_raw,
            'b_raw': b_raw,
            'c_raw': c_raw,
            'd_raw': d_raw,
        }

    return result if result else None


def _compute_composite_score(conditions: Dict[int, dict]) -> int | None:
    """Weighted average of score_total across present conditions.

    Normalises over the weights of present conditions only.
    Returns None when conditions is empty (all-missing).
    """
    weighted_sum = 0.0
    total_w = 0.0
    for cid, sc in conditions.items():
        w = COMPOSITE_WEIGHTS.get(cid, 1.0)
        weighted_sum += w * sc['score_total']
        total_w += w
    return round(weighted_sum / total_w) if total_w > 0 else None


def _sync_compute_and_cache(model_id: int, profile_key: str = _DEFAULT_SCORE_PROFILE) -> dict | None:
    """Compute ABC for model_id under profile_key, store in _abc_cache, and return entry."""
    conditions = _compute_abc_for_model(model_id, profile_key)
    if conditions is None:
        return None
    composite_score = _compute_composite_score(conditions)
    now_str = datetime.now(timezone.utc).isoformat(timespec='seconds')
    entry = {
        'conditions': conditions,
        'composite_score': composite_score,
        'updated_at': now_str,
        'cached_at': time.time(),
    }
    with _abc_cache_lock:
        _abc_cache[profile_key][model_id] = entry
    return entry


def _trigger_soft_refresh(model_id: int, profile_key: str = _DEFAULT_SCORE_PROFILE) -> None:
    """Trigger an async background recompute for (model_id, profile_key), skipping if inflight."""
    key = (profile_key, model_id)
    with _abc_inflight_lock:
        if key in _abc_inflight:
            return
        _abc_inflight.add(key)

    def _refresh():
        try:
            _sync_compute_and_cache(model_id, profile_key)
        finally:
            with _abc_inflight_lock:
                _abc_inflight.discard(key)

    threading.Thread(target=_refresh, daemon=True).start()


def _get_abc_cached(model_id: int, profile_key: str = _DEFAULT_SCORE_PROFILE) -> dict | None:
    """
    Return ABC cache entry for model_id under profile_key, applying soft/hard TTL logic.

    - Fresh (age < ABC_SOFT_TTL_SEC): return cached entry as-is.
    - Soft-expired (soft ≤ age < hard): return stale entry + trigger async refresh.
    - Hard-expired or missing: compute synchronously, cache, and return.
    """
    now = time.time()
    with _abc_cache_lock:
        entry = _abc_cache[profile_key].get(model_id)

    if entry is not None:
        age = now - entry['cached_at']
        if age < ABC_SOFT_TTL_SEC:
            return entry
        if age < ABC_HARD_TTL_SEC:
            _trigger_soft_refresh(model_id, profile_key)
            return entry

    # Hard-expired or missing: synchronous recompute
    return _sync_compute_and_cache(model_id, profile_key)


def _get_abc_all_profiles(model_id: int) -> Dict[str, dict | None]:
    """Return ABC cache entries for all scoring profiles for model_id."""
    return {p: _get_abc_cached(model_id, p) for p in SCORING_PROFILES}


def _build_score_profiles_payload(all_profiles: Dict[str, dict | None],
                                  radar_cids: list) -> dict:
    """Build the score_profiles response dict from all-profile ABC entries.

    Returns: {
        'low': {'composite_score': int|None, 'condition_scores': {cid: int|None}},
        'med': { ... },
        'hi':  { ... },
    }
    """
    result = {}
    for pkey, abc in all_profiles.items():
        composite = None
        cond_scores: dict = {}
        if abc:
            composite = abc.get('composite_score')
            conds = abc.get('conditions') or {}
            for cid in radar_cids:
                cd = conds.get(cid)
                raw = cd.get('score_total') if cd else None
                try:
                    cond_scores[cid] = int(raw) if raw is not None else None
                except (TypeError, ValueError):
                    cond_scores[cid] = None
        else:
            cond_scores = {cid: None for cid in radar_cids}
        result[pkey] = {
            'composite_score': int(composite) if composite is not None else None,
            'condition_scores': cond_scores,
        }
    return result



# Display limit for each board (shown rows)
_RANKINGS_V2_DISPLAY_LIMIT = 10

# Radar condition IDs used for per-condition scores and heat/likes breakdown.
# Must stay in sync with RADAR_CIDS in right-panel-v2.js.
_RADAR_CIDS = [1, 2, 3, 7, 8, 10]

# Simple time-based cache for rankings v2 to avoid full-table scans on every request.
# Cache refresh interval: 10 minutes.  Stores (timestamp, result_dict).
_rankings_v2_cache: dict = {}
_rankings_v2_cache_lock = threading.Lock()
_RANKINGS_V2_CACHE_TTL_SEC = 600  # 10 minutes


def _build_rankings_v2(score_profile: str = _DEFAULT_SCORE_PROFILE) -> dict:
    """
    Compute model-centric rankings for Right Panel v2.

    Data approach (per requirements):
    - Fetch ALL (model_id, condition_id) rows from query_rank_d30_view /
      like_rank_d30_view WITHOUT any rank pre-filtering, so every model that
      has ever appeared in either view is considered as a candidate.
    - Aggregate per-(model, condition) → per-model totals from this full data.
    - This ensures both the heat board and performance board draw from the full
      candidate set, preventing models that are high in one metric but not the
      other from being incorrectly excluded.

    score_profile: one of SCORING_PROFILES ('low'|'med'|'hi').  The performance
    board is sorted by composite_score from this profile.  Heat board ordering
    is always by heat_score (profile-independent).

    Returns a dict:
      {
        'heat_board':        [...],   # sorted by heat_score DESC (top _RANKINGS_V2_DISPLAY_LIMIT)
        'performance_board': [...],   # sorted by composite_score DESC (same limit)
      }

    Each item shape:
      {
        model_id, brand_name_zh, model_name, size, thickness, max_speed,
        reference_price, rgb_light,
        query_count,      # sum of per-condition query counts (30d) for this model
        like_count,       # sum of per-condition like counts (30d) for this model
        heat_score,       # query_count + like_count * 10 + model_add_count * 2
        composite_score,  # overall percentage score (0-100), None if unavailable
        condition_scores, # {cid: score_total} for 6 radar conditions; None values for missing
        condition_heat,   # {cid: heat} where heat = cond_query + cond_like * 10 (unchanged)
      }

    Note: model_add_count (event_type='add' in user_radar_logs, last 30 days) contributes
    only to model-level heat_score, NOT to condition_heat.
    """
    RADAR_CIDS = _RADAR_CIDS

    # Build named placeholders for condition IDs (safe for SQLAlchemy text())
    cid_placeholders = ','.join(f':c{i}' for i in range(len(RADAR_CIDS)))
    cid_params: dict = {f'c{i}': cid for i, cid in enumerate(RADAR_CIDS)}

    # --- Step 1: Fetch full (model, condition) data — no rank pre-filter ---
    # Fetch per-(model_id, condition_id) query counts for all 6 radar conditions.
    # Aggregate MAX per (model_id, condition_id) to collapse any duplicates from the view.
    sql_cond_query = f"""
        SELECT
            v.model_id,
            v.brand_name_zh, v.model_name,
            v.size, v.thickness, v.max_speed, v.reference_price,
            COALESCE(fm.rgb_light, '') AS rgb_light,
            v.condition_id,
            MAX(v.query_by_model_condition_d30) AS cond_query_count
        FROM query_rank_d30_view v
        LEFT JOIN available_models_info_view fm ON fm.model_id = v.model_id
        WHERE v.condition_id IN ({cid_placeholders})
        GROUP BY v.model_id, v.brand_name_zh, v.model_name,
                 v.size, v.thickness, v.max_speed, v.reference_price, fm.rgb_light,
                 v.condition_id
    """
    sql_cond_like = f"""
        SELECT
            v.model_id,
            v.condition_id,
            MAX(v.like_by_model_condition_d30) AS cond_like_count
        FROM like_rank_d30_view v
        WHERE v.condition_id IN ({cid_placeholders})
        GROUP BY v.model_id, v.condition_id
    """

    # Fetch per-model radar-add counts (event_type='add') for the last 30 days.
    # Only intentional add-to-radar actions contribute to model heat.
    sql_radar_add = """
        SELECT model_id, COUNT(1) AS model_add_count
        FROM user_radar_logs
        WHERE event_time > (NOW() - INTERVAL 30 DAY)
          AND event_type = 'add'
        GROUP BY model_id
    """

    try:
        cq_rows = fetch_all(sql_cond_query, cid_params)
    except Exception:
        cq_rows = []
    try:
        lq_rows = fetch_all(sql_cond_like, cid_params)
    except Exception:
        lq_rows = []
    try:
        radar_add_rows = fetch_all(sql_radar_add)
    except Exception:
        radar_add_rows = []

    model_add_counts: dict[int, int] = {}
    for r in radar_add_rows:
        try:
            mid = int(r['model_id'])
            model_add_counts[mid] = int(r.get('model_add_count') or 0)
        except Exception:
            pass

    # --- Step 2: Build per-(model, condition) heat map and collect model metadata ---
    # cond_data[model_id][condition_id] = {'query': int, 'like': int}
    cond_data: dict[int, dict[int, dict[str, int]]] = {}
    model_meta: dict[int, dict] = {}

    for r in cq_rows:
        try:
            mid = int(r['model_id']); cid = int(r['condition_id'])
        except Exception:
            continue
        cond_data.setdefault(mid, {}).setdefault(cid, {'query': 0, 'like': 0})
        try:
            cond_data[mid][cid]['query'] = int(r['cond_query_count'] or 0)
        except Exception:
            pass
        if mid not in model_meta:
            model_meta[mid] = {
                'brand_name_zh': r.get('brand_name_zh') or '',
                'model_name':    r.get('model_name')    or '',
                'size':          r.get('size'),
                'thickness':     r.get('thickness'),
                'max_speed':     r.get('max_speed'),
                'reference_price': r.get('reference_price'),
                'rgb_light':     r.get('rgb_light') or '',
            }

    for r in lq_rows:
        try:
            mid = int(r['model_id']); cid = int(r['condition_id'])
        except Exception:
            continue
        cond_data.setdefault(mid, {}).setdefault(cid, {'query': 0, 'like': 0})
        try:
            cond_data[mid][cid]['like'] = int(r['cond_like_count'] or 0)
        except Exception:
            pass

    # --- Step 2b: Hydrate metadata for models that only appear in like_rank_d30_view ---
    # Models that appear ONLY in like_rank_d30_view (never queried in 30 days) will have
    # cond_data entries but no model_meta entry, causing brand/model/size/etc. to render
    # blank in the rankings table.  Fetch their metadata from available_models_info_view.
    missing_mids = [mid for mid in cond_data if mid not in model_meta]
    if missing_mids:
        mp_placeholders = ','.join(f':mm{i}' for i in range(len(missing_mids)))
        mp_params = {f'mm{i}': mid for i, mid in enumerate(missing_mids)}
        sql_meta = f"""
            SELECT model_id, brand_name_zh, model_name,
                   size, thickness, max_speed, reference_price,
                   COALESCE(rgb_light, '') AS rgb_light
            FROM available_models_info_view
            WHERE model_id IN ({mp_placeholders})
        """
        try:
            meta_rows = fetch_all(sql_meta, mp_params)
            for r in meta_rows:
                try:
                    rmid = int(r['model_id'])
                except Exception:
                    continue
                if rmid not in model_meta:
                    model_meta[rmid] = {
                        'brand_name_zh': r.get('brand_name_zh') or '',
                        'model_name':    r.get('model_name')    or '',
                        'size':          r.get('size'),
                        'thickness':     r.get('thickness'),
                        'max_speed':     r.get('max_speed'),
                        'reference_price': r.get('reference_price'),
                        'rgb_light':     r.get('rgb_light') or '',
                    }
        except Exception:
            pass

    # --- Step 3: Derive model totals and per-condition heat from the full (model, cond) data ---
    items = []
    for mid, cid_map in cond_data.items():
        meta = model_meta.get(mid, {})

        # Model total query / like = sum across all radar conditions
        total_query = sum(v['query'] for v in cid_map.values())
        total_like  = sum(v['like']  for v in cid_map.values())
        model_add   = model_add_counts.get(mid, 0)
        heat_score  = total_query + total_like * 10 + model_add * 2

        # Per-condition heat and like counts
        condition_heat: dict[int, int] = {}
        condition_likes: dict[int, int] = {}
        for cid in RADAR_CIDS:
            entry = cid_map.get(cid)
            if entry:
                condition_heat[cid] = entry['query'] + entry['like'] * 10
                condition_likes[cid] = entry['like']
            else:
                condition_heat[cid] = 0
                condition_likes[cid] = 0

        # Attach composite_score and per-condition scores from ABC cache (med = default profile)
        all_profiles = _get_abc_all_profiles(mid)
        abc_entry = all_profiles.get(_DEFAULT_SCORE_PROFILE)
        composite_score = None
        condition_scores: dict = {}
        if abc_entry:
            composite_score = abc_entry.get('composite_score')
            conds_abc = abc_entry.get('conditions') or {}
            for cid in RADAR_CIDS:
                cd = conds_abc.get(cid)
                raw = cd.get('score_total') if cd else None
                try:
                    condition_scores[cid] = int(raw) if raw is not None else None
                except (TypeError, ValueError):
                    condition_scores[cid] = None
        score_profiles = _build_score_profiles_payload(all_profiles, RADAR_CIDS)

        items.append({
            'model_id':       mid,
            'brand_name_zh':  meta.get('brand_name_zh', ''),
            'model_name':     meta.get('model_name',    ''),
            'size':           meta.get('size'),
            'thickness':      meta.get('thickness'),
            'max_speed':      meta.get('max_speed'),
            'reference_price': meta.get('reference_price'),
            'rgb_light':      meta.get('rgb_light', ''),
            'query_count':    total_query,
            'like_count':     total_like,
            'heat_score':     heat_score,
            'composite_score': int(composite_score) if composite_score is not None else None,
            'condition_scores': condition_scores,
            'score_profiles': score_profiles,
            'condition_heat':   condition_heat,
            'condition_likes':  condition_likes,
        })

    limit = _RANKINGS_V2_DISPLAY_LIMIT

    # 热度榜: sort by heat_score DESC, then query_count DESC as tiebreak
    # Use dict copies so rank assignments don't mutate shared objects between boards.
    heat_board = []
    for i, row in enumerate(
        sorted(items, key=lambda x: (-x['heat_score'], -x['query_count']))[:limit], 1
    ):
        heat_board.append({**row, 'rank': i})

    # 性能榜: sort by the requested profile's composite_score DESC, then heat_score DESC.
    # Using score_profiles[profile]['composite_score'] ensures the ordering matches the
    # active score profile rather than always defaulting to _DEFAULT_SCORE_PROFILE.
    def _perf_sort_key(row):
        sp = row.get('score_profiles', {}).get(score_profile)
        cs = (sp.get('composite_score') if sp else None)
        return (-(cs if cs is not None else -1), -row['heat_score'])

    perf_board = []
    for i, row in enumerate(
        sorted(items, key=_perf_sort_key)[:limit], 1
    ):
        perf_board.append({**row, 'rank': i})

    return {
        'heat_board': heat_board,
        'performance_board': perf_board,
        'model_lookup': {item['model_id']: item for item in items},
    }


def get_rankings_v2(score_profile: str = _DEFAULT_SCORE_PROFILE) -> dict:
    """
    Return model-centric rankings for Right Panel v2, with a 10-minute cache.

    score_profile: one of SCORING_PROFILES ('low'|'med'|'hi').  Each profile
    gets its own cache entry so profile-specific ordering is cached independently.

    The cache avoids repeated full-table scans while still reflecting reasonably
    fresh data.  A forced refresh can be triggered by clearing _rankings_v2_cache.

    Uses a double-check pattern: first a read-under-lock to avoid unnecessary
    recomputation when the cache is warm; computation happens outside the lock
    to keep contention low; a second check-under-lock before storing ensures no
    stale result overwrites a fresher one written by a concurrent thread.
    """
    if score_profile not in SCORING_PROFILES:
        score_profile = _DEFAULT_SCORE_PROFILE

    import time as _time
    now = _time.time()

    cache_key = f'data:{score_profile}'

    # Fast path: return cached result if still fresh (hold lock only briefly)
    with _rankings_v2_cache_lock:
        cached = _rankings_v2_cache.get(cache_key)
        if cached and (now - cached[0]) < _RANKINGS_V2_CACHE_TTL_SEC:
            return cached[1]

    # Slow path: compute outside the lock to avoid blocking other threads
    result = _build_rankings_v2(score_profile)
    computed_at = _time.time()

    # Second check: only store if no fresher result was written while we computed
    with _rankings_v2_cache_lock:
        existing = _rankings_v2_cache.get(cache_key)
        if not existing or computed_at > existing[0]:
            _rankings_v2_cache[cache_key] = (computed_at, result)

    return result


def _effective_value_for_series(series_rows: list, model_id: int, condition_id: int,
                                axis: str, limit_value: float | None):
    """
    输入：某个 (model_id, condition_id) 的所有行记录（含 rpm, noise_db, airflow）
    输出：effective_x, effective_airflow, source ('raw'|'fit'), axis ('rpm'|'noise_db'),
          effective_rpm, effective_noise_db（同时返回转速和分贝，用于搜索结果联合展示）
    新版：拟合一律使用四合一模型（噪音轴用 noise_to_airflow；转速轴用 rpm_to_airflow）
    """
    ax = 'noise_db' if axis == 'noise' else axis
    rpm, noise, airflow = [], [], []
    for r in series_rows:
        rpm.append(r.get('rpm'))
        noise.append(r.get('noise_db'))
        airflow.append(r.get('airflow'))
    # 统一模型（含缓存/失效/重建）
    unified = get_or_build_unified_perf_model(model_id, condition_id, rpm, airflow, noise) or {}
    p = (unified.get('pchip') or {})
    mdl_fit = p.get('noise_to_airflow') if ax == 'noise_db' else p.get('rpm_to_airflow')
    # 伴随轴模型：rpm轴→rpm_to_noise_db；noise轴→noise_to_rpm
    mdl_companion = p.get('rpm_to_noise_db') if ax == 'rpm' else p.get('noise_to_rpm')

    def _companion_from_fit(x_val):
        """用 PCHIP 模型计算伴随轴值（rpm↔noise_db），失败返回 None。"""
        if mdl_companion and isinstance(mdl_companion, dict):
            try:
                v = eval_pchip(mdl_companion, x_val)
                return float(v) if v is not None and math.isfinite(float(v)) else None
            except Exception:
                pass
        return None

    def _make_result(eff_x, eff_y, source, raw_companion=None):
        """构建返回 dict，同时填充 effective_rpm / effective_noise_db。"""
        comp = raw_companion
        if comp is None and source == 'fit':
            comp = _companion_from_fit(eff_x)
        if ax == 'rpm':
            eff_rpm = eff_x
            eff_noise_db = comp
        else:
            eff_rpm = comp
            eff_noise_db = eff_x
        return {
            'effective_x': eff_x, 'effective_airflow': eff_y,
            'effective_source': source, 'effective_axis': ax,
            'effective_rpm_at_point': None,
            'effective_rpm': eff_rpm,
            'effective_noise_db': eff_noise_db,
        }

    # 抽取有效原始点（用于"原始优先"和边界/落在原始点判断）
    # 同时保留伴随轴数据（noise when ax=rpm, rpm when ax=noise_db）
    xs, ys, comps = [], [], []
    src_x_arr = noise if ax == 'noise_db' else rpm
    comp_arr = rpm if ax == 'noise_db' else noise
    for x, y, c in zip(src_x_arr or [], airflow or [], comp_arr or []):
        try:
            xf = float(x) if x is not None else None
            yf = float(y) if y is not None else None
        except Exception:
            continue
        if xf is None or yf is None: continue
        if not (math.isfinite(xf) and math.isfinite(yf)): continue
        try:
            cf = float(c) if c is not None else None
            if cf is not None and not math.isfinite(cf):
                cf = None
        except Exception:
            cf = None
        xs.append(xf); ys.append(yf); comps.append(cf)
    if not xs:
        return None
    x_min, x_max = min(xs), max(xs)

    # 未限制：取原始最大风量的点
    if limit_value is None:
        idx = max(range(len(ys)), key=lambda i: ys[i])
        return _make_result(xs[idx], ys[idx], 'raw', comps[idx])

    lv = float(limit_value)
    if lv < x_min - 1e-9:
        return None
    if lv >= x_max - 1e-9:
        idxs = [i for i, x in enumerate(xs) if abs(x - x_max) < 1e-9]
        best = max(idxs, key=lambda i: ys[i])
        return _make_result(xs[best], ys[best], 'raw', comps[best])

    # 若存在原始点恰好等于 limit（噪音允许 0.05 容差）
    tol = 0.05 if ax == 'noise_db' else 0.0
    for i, x in enumerate(xs):
        if (tol == 0.0 and x == lv) or (tol > 0.0 and abs(x - lv) <= tol):
            return _make_result(x, ys[i], 'raw', comps[i])

    # 位于域内且无原始点 → 使用四合一 PCHIP 拟合
    if not (mdl_fit and isinstance(mdl_fit, dict)):
        # 回退：取最接近 limit 的原始点
        j = min(range(len(xs)), key=lambda i: abs(xs[i] - lv))
        return _make_result(xs[j], ys[j], 'raw', comps[j])
    lx = max(float(mdl_fit.get('x0') or lv), min(lv, float(mdl_fit.get('x1') or lv)))
    eff_y = eval_pchip(mdl_fit, lx)
    return _make_result(lx, float(eff_y), 'fit')


def search_fans_by_condition_with_fit(condition_id=None, condition_name=None, sort_by='none', sort_value=None,
                         size_filter=None, thickness_min=None, thickness_max=None,
                         price_min=None, price_max=None,
                         rgb_light=None,
                         max_speed_min=None, max_speed_max=None,
                         limit=200) -> list[dict]:
    where = []
    params = {}

    # 工况过滤
    if condition_id is not None:
        where.append("g.condition_id=:cid"); params['cid'] = int(condition_id)
    elif (condition_name or '').strip() and condition_name != '全部':
        where.append("g.condition_name_zh=:cn"); params['cn'] = condition_name.strip()

    # 尺寸/厚度
    if size_filter and size_filter != '不限':
        where.append("g.size=:sz"); params['sz'] = int(size_filter)
    if thickness_min is not None and thickness_max is not None:
        where.append("g.thickness BETWEEN :tmin AND :tmax")
        params.update(tmin=int(thickness_min), tmax=int(thickness_max))

    # 参考价格（元），来自 fan_model.reference_price
    if price_min is not None and price_max is not None:
        where.append("reference_price BETWEEN :pmin AND :pmax")
        params.update(pmin=int(price_min), pmax=int(price_max))

    # RGB灯光
    if rgb_light and rgb_light != '不限':
        where.append("g.model_id IN (SELECT model_id FROM available_models_info_view WHERE rgb_light=:rgb)")
        params['rgb'] = rgb_light

    # 最大转速（RPM）
    if max_speed_min is not None and max_speed_max is not None:
        where.append("g.model_id IN (SELECT model_id FROM available_models_info_view WHERE max_speed BETWEEN :msmin AND :msmax)")
        params.update(msmin=int(max_speed_min), msmax=int(max_speed_max))

    sql = f"""
      SELECT g.model_id, g.condition_id,
             g.brand_name_zh, g.model_name, g.condition_name_zh,
             g.size, g.thickness, g.rpm, g.noise_db, g.airflow_cfm AS airflow,
             COALESCE(g.like_count,0) AS like_count,
             reference_price,
             (SELECT fm2.max_speed FROM available_models_info_view fm2 WHERE fm2.model_id = g.model_id LIMIT 1) AS max_speed,
             (SELECT fm2.rgb_light FROM available_models_info_view fm2 WHERE fm2.model_id = g.model_id LIMIT 1) AS rgb_light
      FROM general_view g
      {"WHERE " + " AND ".join(where) if where else ""}
      ORDER BY g.model_id, g.condition_id, g.rpm
    """
    rows = fetch_all(sql, params)

    # 后续分组/拟合逻辑不变
    groups = {}
    for r in rows:
        mid = int(r['model_id']); cid = int(r['condition_id'])
        key = (mid, cid)
        g = groups.setdefault(key, {
            'rows': [], 'brand': r['brand_name_zh'], 'model': r['model_name'],
            'condition_name': r['condition_name_zh'], 'size': r['size'], 'thickness': r['thickness'],
            'like_count': 0, 'max_speed': r.get('max_speed'), 'max_noise_db': None, 'reference_price': r['reference_price'],
            'rgb_light': r.get('rgb_light') or ''
        })
        g['rows'].append({'rpm': r['rpm'], 'noise_db': r['noise_db'], 'airflow': r['airflow']})
        try:
            g['like_count'] = max(g['like_count'], int(r['like_count']))
        except Exception:
            pass
        try:
            if r['rpm'] is not None:
                rpm_int = int(r['rpm'])
                if g['max_noise_db'] is None or g.get('_max_test_rpm', 0) < rpm_int:
                    g['_max_test_rpm'] = rpm_int
                    g['max_noise_db'] = r.get('noise_db')
        except Exception:
            pass

    axis = 'rpm' if sort_by in ('rpm', 'none', 'condition_score') else 'noise_db'
    lv = None if sort_by in ('none', 'condition_score') else float(sort_value)

    items = []
    for (mid, cid), g in groups.items():
        eff = _effective_value_for_series(g['rows'], mid, cid, axis, lv)
        if not eff:
            continue
        items.append({
            'model_id': mid, 'condition_id': cid,
            'brand_name_zh': g['brand'], 'model_name': g['model'], 'condition_name_zh': g['condition_name'],
            'size': g['size'], 'thickness': g['thickness'], 'like_count': g['like_count'],
            'effective_airflow': eff['effective_airflow'], 'effective_x': eff['effective_x'],
            'effective_axis': eff['effective_axis'], 'effective_source': eff['effective_source'],
            'effective_rpm': eff.get('effective_rpm'),
            'effective_noise_db': eff.get('effective_noise_db'),
            'max_airflow': eff['effective_airflow'], 'max_speed': g['max_speed'],
            'max_noise_db': g.get('max_noise_db'),
            'reference_price': g['reference_price'],
            'rgb_light': g['rgb_light'] 
        })

    items.sort(key=lambda r: (r['effective_airflow'] if r['effective_airflow'] is not None else -1e9), reverse=True)
    return items[:limit]

def search_fans_composite(size_filter=None, thickness_min=None, thickness_max=None,
                           price_min=None, price_max=None, rgb_light=None,
                           max_speed_min=None, max_speed_max=None,
                           limit=200) -> list[dict]:
    """
    Search fans in 综合评分 (composite score) mode.
    No condition filter — returns one row per distinct model, enriched with
    composite_score, condition_scores, and condition_likes by the caller.
    """
    where = []
    params: dict = {}

    if size_filter and size_filter != '不限':
        where.append("g.size=:sz"); params['sz'] = int(size_filter)
    if thickness_min is not None and thickness_max is not None:
        where.append("g.thickness BETWEEN :tmin AND :tmax")
        params.update(tmin=int(thickness_min), tmax=int(thickness_max))
    if price_min is not None and price_max is not None:
        where.append("reference_price BETWEEN :pmin AND :pmax")
        params.update(pmin=int(price_min), pmax=int(price_max))
    if rgb_light and rgb_light != '不限':
        where.append("g.model_id IN (SELECT model_id FROM available_models_info_view WHERE rgb_light=:rgb)")
        params['rgb'] = rgb_light
    if max_speed_min is not None and max_speed_max is not None:
        where.append("g.model_id IN (SELECT model_id FROM available_models_info_view WHERE max_speed BETWEEN :msmin AND :msmax)")
        params.update(msmin=int(max_speed_min), msmax=int(max_speed_max))

    sql = f"""
      SELECT g.model_id, g.brand_name_zh, g.model_name, g.size, g.thickness,
             (SELECT fm2.max_speed FROM available_models_info_view fm2
              WHERE fm2.model_id = g.model_id LIMIT 1) AS max_speed,
             reference_price,
             (SELECT fm2.rgb_light FROM available_models_info_view fm2
              WHERE fm2.model_id = g.model_id LIMIT 1) AS rgb_light
      FROM general_view g
      {"WHERE " + " AND ".join(where) if where else ""}
      GROUP BY g.model_id, g.brand_name_zh, g.model_name, g.size, g.thickness, reference_price
      LIMIT :lim
    """
    params['lim'] = limit
    rows = fetch_all(sql, params)

    items = []
    for r in rows:
        items.append({
            'model_id': int(r['model_id']),
            'brand_name_zh': r['brand_name_zh'],
            'model_name': r['model_name'],
            'size': r['size'],
            'thickness': r['thickness'],
            'max_speed': r.get('max_speed'),
            'reference_price': r.get('reference_price'),
            'rgb_light': r.get('rgb_light') or '',
            'condition_id': None,
            'like_count': 0,
            'effective_airflow': None,
            'effective_x': None,
            'effective_axis': None,
            'effective_source': None,
            'max_airflow': None,
            'max_noise_db': None,
        })
    return items


def get_recent_updates(limit: int = RECENT_UPDATES_LIMIT) -> List[dict]:
    """
    Model-centric recent-updates list for Right Panel v2.

    Deduplicates upload-log entries by model: each model appears once,
    with its latest update_date across all upload records (day precision).
    Enriched with the same composite_score / heat_score / condition fields
    used by the rankings boards so the front-end can reuse the same
    expand-panel layout.  Sorted by latest update_date DESC.
    """
    RADAR_CIDS = _RADAR_CIDS

    # Step 1: unique models with their latest update date
    sql = """
      SELECT
        u.model_id,
        u.brand_name_zh, u.model_name,
        u.size, u.thickness, u.max_speed,
        COALESCE(fm.rgb_light, '')  AS rgb_light,
        fm.reference_price,
        DATE_FORMAT(MAX(u.update_date), '%Y-%m-%d') AS update_date
      FROM update_notice_d30_view AS u
      LEFT JOIN available_models_info_view fm ON fm.model_id = u.model_id
      GROUP BY u.model_id, u.brand_name_zh, u.model_name,
               u.size, u.thickness, u.max_speed,
               fm.rgb_light, fm.reference_price
      ORDER BY MAX(u.update_date) DESC
      LIMIT :l
    """
    rows = fetch_all(sql, {'l': limit})
    if not rows:
        return []

    # Step 2: build lookup from the rankings cache (already enriched with
    # heat_score, composite_score, condition_heat, condition_scores)
    try:
        rankings = get_rankings_v2()
    except Exception:
        rankings = {}
    ranked_lookup: dict = rankings.get('model_lookup') or {}

    # Step 3: assemble result list
    items = []
    for r in rows:
        try:
            mid = int(r['model_id'])
        except (TypeError, ValueError, KeyError):
            continue

        ranked = ranked_lookup.get(mid)
        if ranked:
            composite_score  = ranked.get('composite_score')
            condition_scores = ranked.get('condition_scores', {cid: None for cid in RADAR_CIDS})
            score_profiles   = ranked.get('score_profiles', {p: {'composite_score': None, 'condition_scores': {cid: None for cid in RADAR_CIDS}} for p in SCORING_PROFILES})
            heat_score       = ranked.get('heat_score', 0)
            condition_heat   = ranked.get('condition_heat', {cid: 0 for cid in RADAR_CIDS})
        else:
            # Model not in rankings (no heat data); fall back to ABC cache
            composite_score  = None
            condition_scores = {cid: None for cid in RADAR_CIDS}
            heat_score       = 0
            condition_heat   = {cid: 0 for cid in RADAR_CIDS}
            all_profiles_abc = _get_abc_all_profiles(mid)
            abc = all_profiles_abc.get(_DEFAULT_SCORE_PROFILE)
            if abc:
                cs = abc.get('composite_score')
                try:
                    composite_score = int(cs) if cs is not None else None
                except (TypeError, ValueError):
                    composite_score = None
                conds_abc = abc.get('conditions') or {}
                for cid in RADAR_CIDS:
                    cd  = conds_abc.get(cid)
                    raw = cd.get('score_total') if cd else None
                    try:
                        condition_scores[cid] = int(raw) if raw is not None else None
                    except (TypeError, ValueError):
                        condition_scores[cid] = None
            score_profiles = _build_score_profiles_payload(all_profiles_abc, RADAR_CIDS)

        items.append({
            'model_id':        mid,
            'brand_name_zh':   r.get('brand_name_zh') or '',
            'model_name':      r.get('model_name')    or '',
            'size':            r.get('size'),
            'thickness':       r.get('thickness'),
            'max_speed':       r.get('max_speed'),
            'reference_price': r.get('reference_price'),
            'rgb_light':       r.get('rgb_light') or '',
            'heat_score':      heat_score,
            'composite_score': composite_score,
            'condition_scores': condition_scores,
            'score_profiles':  score_profiles,
            'condition_heat':   condition_heat,
            'update_date':     r.get('update_date') or '',
        })

    return items

# =========================================
# Visit Start
# =========================================
@app.route('/api/visit_start', methods=['POST'])
def api_visit_start():
    try:
        _ = get_or_create_user_identifier()
        uid = g._active_uid
        uid_source = getattr(g, '_uid_source', None)
        row = fetch_all("SELECT COUNT(*) AS c FROM visit_logs WHERE user_identifier=:u", {'u': uid})
        visit_index = int(row[0]['c']) + 1 if row else 1
        is_new_user = (visit_index == 1)

        data = request.get_json(force=True, silent=True) or {}
        screen_w = int(data.get('screen_w') or 0) or None
        screen_h = int(data.get('screen_h') or 0) or None
        dpr = float(data.get('device_pixel_ratio') or 0) or None
        language = (data.get('language') or '').strip() or None
        is_touch = 1 if data.get('is_touch') else 0
        ui_theme = (data.get('theme') or '').strip() or None   # NEW: 加载时主题

        ua_raw = request.headers.get('User-Agent', '') or None
        dev = _parse_device_basic(ua_raw or '')

        sql = """
        INSERT INTO visit_logs
        (user_identifier, uid_source, visit_index, is_new_user,
         user_agent_raw, os_name, device_type,
         screen_w, screen_h, device_pixel_ratio, language, is_touch,
         ui_theme)  -- NEW
        VALUES
        (:uid, :usrc, :vidx, :isnew,
         :ua, :osn, :dtype,
         :sw, :sh, :dpr, :lang, :touch,
         :theme)  -- NEW
        """
        exec_write(sql, {
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
            'theme': ui_theme     # NEW
        })
        return resp_ok({'visit_index': visit_index, 'is_new_user': is_new_user})
    except Exception as e:
        app.logger.exception(e)
        return resp_err('INTERNAL_ERROR', str(e), 500)

# =========================================
# Helpers for events (NEW)
# =========================================
def _get_latest_visit_id_for_user(uid: str) -> int | None:
    try:
        rows = fetch_all("SELECT id FROM visit_logs WHERE user_identifier=:u ORDER BY id DESC LIMIT 1", {'u': uid})
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
# Event Logging API (NEW)
# =========================================
@app.post('/api/log_event')
def api_log_event():
    try:
        user_id = get_or_create_user_identifier()
        data = request.get_json(force=True, silent=True) or {}

        event_type_code = (data.get('event_type_code') or '').strip()
        if not event_type_code:
            return resp_err('INVALID_EVENT', '缺少 event_type_code')

        # 轻度清洗与长度保护
        if len(event_type_code) > 64:
            event_type_code = event_type_code[:64]
        page_key = (data.get('page_key') or 'home').strip() or 'home'
        if len(page_key) > 64:
            page_key = page_key[:64]
        target_url = (data.get('target_url') or '').strip() or None
        if target_url and len(target_url) > 512:
            target_url = target_url[:512]

        # Optional fields: model_id, condition_id, payload_json
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
        exec_write(sql, {
            'u': user_id,
            'vid': visit_id,
            'type': event_type_code,
            'page_key': page_key,
            'target_url': target_url,
            'model_id': model_id,
            'condition_id': condition_id,
            'payload_json': payload_json
        })
        return resp_ok({'logged': 1})
    except Exception as e:
        app.logger.exception(e)
        return resp_err('INTERNAL_ERROR', str(e), 500)
    
# =========================================
# Like APIs
# =========================================
@app.route('/api/like_status', methods=['POST'])
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
        if not cleaned:
            fp = compute_like_fingerprint(user_id)
            return resp_ok({'like_keys': [], 'fp': fp})
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
        rows = fetch_all(sql, params)
        like_keys = [f"{int(r['model_id'])}_{int(r['condition_id'])}" for r in rows]
        fp = compute_like_fingerprint(user_id)
        return resp_ok({'like_keys': like_keys, 'fp': fp})
    except Exception as e:
        app.logger.exception(e)
        return resp_err('INTERNAL_ERROR', str(e), 500)


@app.route('/api/like_keys', methods=['GET'])
def api_like_keys():
    try:
        user_id = get_or_create_user_identifier()
        keys = get_user_like_keys(user_id)
        fp = compute_like_fingerprint(user_id)
        return resp_ok({'like_keys': keys, 'fp': fp})
    except Exception as e:
        app.logger.exception(e)
        return resp_err('INTERNAL_ERROR', str(e), 500)


@app.route('/api/like', methods=['POST'])
def api_like():
    data = request.get_json(force=True, silent=True) or {}
    model_id = data.get('model_id')
    condition_id = data.get('condition_id')
    user_id = get_or_create_user_identifier()
    if not model_id or not condition_id:
        return resp_err('LIKE_MISSING_IDS', '缺少 model_id 或 condition_id', 400)
    try:
        exec_write("""INSERT INTO rate_logs (user_identifier, model_id, condition_id, is_valid, rate_id)
                      VALUES (:u,:m,:c,1,1)
                      ON DUPLICATE KEY UPDATE is_valid=1, update_date=NOW()""",
                   {'u': user_id, 'm': model_id, 'c': condition_id})
        fp = compute_like_fingerprint(user_id)
        return resp_ok({'fp': fp})
    except Exception as e:
        app.logger.exception(e)
        return resp_err('LIKE_DB_WRITE_FAIL', str(e), 500)


@app.route('/api/unlike', methods=['POST'])
def api_unlike():
    data = request.get_json(force=True, silent=True) or {}
    model_id = data.get('model_id')
    condition_id = data.get('condition_id')
    user_id = get_or_create_user_identifier()
    if not model_id or not condition_id:
        return resp_err('LIKE_MISSING_IDS', '缺少 model_id 或 condition_id', 400)
    try:
        exec_write("""UPDATE rate_logs
                      SET is_valid=0, update_date=NOW()
                      WHERE rate_id=1 AND user_identifier=:u AND model_id=:m AND condition_id=:c""",
                   {'u': user_id, 'm': model_id, 'c': condition_id})
        fp = compute_like_fingerprint(user_id)
        return resp_ok({'fp': fp})
    except Exception as e:
        app.logger.exception(e)
        return resp_err('UNLIKE_DB_WRITE_FAIL', str(e), 500)

# =========================================
# Recent Likes
# =========================================
@app.route('/api/recent_likes', methods=['GET'])
def api_recent_likes():
    try:
        user_id = get_or_create_user_identifier()
        items = get_user_likes_full(user_id, limit=RECENT_LIKES_LIMIT)
        fp = compute_like_fingerprint(user_id)
        return resp_ok({'items': items, 'fp': fp})
    except Exception as e:
        app.logger.exception(e)
        return resp_err('INTERNAL_ERROR', str(e), 500)

# =========================================
# Curves
# =========================================
def get_curves_for_pairs(pairs: List[Tuple[int, int]]) -> Dict[str, dict]:
    if not pairs:
        return {}
    conds, params = [], {}
    for i, (m, c) in enumerate(pairs, start=1):
        conds.append(f"(:m{i}, :c{i})")
        params[f"m{i}"] = int(m)
        params[f"c{i}"] = int(c)
    sql = f"""
      SELECT model_id, condition_id, brand_name_zh, model_name,
             condition_name_zh, resistance_type_zh, resistance_location_zh,
             rpm, airflow_cfm AS airflow, noise_db
      FROM general_view
      WHERE (model_id, condition_id) IN ({",".join(conds)})
      ORDER BY model_id, condition_id, rpm
    """
    rows = fetch_all(sql, params)
    bucket = {}
    for r in rows:
        key = f"{int(r['model_id'])}_{int(r['condition_id'])}"
        b = bucket.setdefault(key, {
            'rpm': [], 'airflow': [], 'noise_db': [],
            'info': {
                'brand': r['brand_name_zh'],
                'model': r['model_name'],
                'condition': r['condition_name_zh'],
                'resistance_type': r['resistance_type_zh'],
                'resistance_location': r['resistance_location_zh'],
                'model_id': int(r['model_id']),
                'condition_id': int(r['condition_id'])
            }   
        })
        rpm = r.get('rpm')
        airflow = r.get('airflow')
        noise = r.get('noise_db')
        try:
            airflow_f = float(airflow)
            if airflow_f != airflow_f:  # NaN
                continue
        except Exception:
            continue
        if rpm is None and noise is None:
            continue
        b['rpm'].append(rpm)
        b['airflow'].append(airflow_f)
        b['noise_db'].append(noise)
    return bucket


@app.post('/api/curves')
def api_curves():
    """
    返回 canonicalSeries：
      series: [
        {
          key, name, brand, model, condition,
          model_id, condition_id,
          resistance_type, resistance_location,
          data: { rpm:[], noise_db:[], airflow:[] },
          pchip: { rpm_to_airflow, rpm_to_noise_db, noise_to_rpm, noise_to_airflow }
        }, ...
      ]
      missing: [ {model_id, condition_id}, ... ]
    说明：
      - 不再返回顶层 rpm/noise_db/airflow，也不使用 -1 作为占位。
      - data.* 数组中允许出现 None（例如缺失的噪音或转速），前端会在渲染前清洗。
    """
    try:
      data = request.get_json(force=True, silent=True) or {}
      raw_pairs = data.get('pairs') or []
      uniq, seen = [], set()
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
          uniq.append(t)

      # 空集合：直接返回空 series
      if not uniq:
          return resp_ok({'series': [], 'missing': []})

      # 读取三轴点并按 (m,c) 聚合
      bucket = get_curves_for_pairs(uniq)  # { "m_c": { rpm:[], airflow:[], noise_db:[], info:{...} } }

      # 统一构建四合一拟合模型（含缓存/失效处理）
      perf_map = spectrum_reader.build_performance_pchips(uniq)  # { "m_c": { pchip:{...} } }

      # 计算缺失集合
      wanted_keys = {f"{m}_{c}": (m, c) for (m, c) in uniq}
      existing_keys = set(bucket.keys())
      missing = []
      for key, (mid, cid) in wanted_keys.items():
          if key not in existing_keys:
              missing.append({'model_id': mid, 'condition_id': cid})

      series = []
      for mid, cid in uniq:
          k = f"{mid}_{cid}"
          b = bucket.get(k)
          if not b:
              continue
          info = b['info']  # 含品牌/型号/工况/风阻等

          # 四合一 PCHIP
          perf = perf_map.get(k) or {}
          pset = (perf.get('pchip') or {})
          # Extract supports_audio from performance model (default to False if not present)
          supports_audio = perf.get('supports_audio', False)

          # 直接使用原始数组；不再填充 -1，占位留给前端清洗
          rpm_arr   = b.get('rpm') or []
          noise_arr = b.get('noise_db') or []
          air_arr   = b.get('airflow') or []

          series.append(dict(
              key=k,
              name=f"{info['brand']} {info['model']} - {info['condition']}",
              brand=info['brand'],
              model=info['model'],
              condition=info['condition'],
              model_id=info['model_id'],
              condition_id=info['condition_id'],
              resistance_type=info.get('resistance_type'),
              resistance_location=info.get('resistance_location'),
              data={
                  'rpm': rpm_arr,
                  'noise_db': noise_arr,
                  'airflow': air_arr
              },
              pchip={
                  'rpm_to_airflow':   pset.get('rpm_to_airflow'),
                  'rpm_to_noise_db':  pset.get('rpm_to_noise_db'),
                  'noise_to_rpm':     pset.get('noise_to_rpm'),
                  'noise_to_airflow': pset.get('noise_to_airflow')
              },
              supports_audio=supports_audio
          ))

      return resp_ok({'series': series, 'missing': missing})
    except Exception as e:
      app.logger.exception(e)
      return resp_err('INTERNAL_ERROR', f'后端异常: {e}', 500)

# =========================================
# Unified Curve Set API
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
    with engine.begin() as conn:
        for pair in pairs:
            conn.execute(text(sql), {
                'u': user_id,
                'vid': visit_id,
                'm': pair['model_id'],
                'c': pair['condition_id'],
                'b': batch,
                's': source,
                'etype': event_type,
                'efftype': effect_type,
            })
    return len(pairs)


@app.post('/api/curve_set')
def api_curve_set():
    """
    Unified curve-set logging endpoint.

    Body:
      event_type  (str, required) – one of: condition_activate / restore / model_show /
                                    model_add / condition_inactivate / model_remove /
                                    model_hide / radar_clear_all / reset_condition
      pairs       (list, required) – [{model_id, condition_id}, ...]
      source      (str, optional) – e.g. 'radar', 'search_add'
      action_id   (str, optional) – shared token for batch operations

    effect_type is derived automatically from event_type:
      enter → condition_activate / restore / model_show / model_add
      leave → condition_inactivate / model_remove / model_hide / radar_clear_all / reset_condition

    Restore suppression:
      If event_type is 'restore' and the current visit already has curve log entries,
      the restore is treated as a same-visit page-refresh and is NOT re-logged.
    """
    try:
        user_id = get_or_create_user_identifier()
        data = request.get_json(force=True, silent=True) or {}

        event_type = (data.get('event_type') or '').strip()[:32] or None
        if not event_type or event_type not in _VALID_CURVE_EVENT_TYPES:
            return resp_err('INVALID_EVENT_TYPE',
                            f'event_type must be one of {sorted(_VALID_CURVE_EVENT_TYPES)}', 400)

        effect_type = 'enter' if event_type in _CURVE_ENTER_EVENT_TYPES else 'leave'

        raw_pairs = data.get('pairs') or []
        cleaned = _parse_curve_pairs(raw_pairs)
        if not cleaned:
            return resp_ok({'logged': 0})

        source = (data.get('source') or '').strip()[:64] or None
        action_id = (data.get('action_id') or '').strip()[:64] or None

        visit_id = _get_latest_visit_id_for_user(user_id)

        # Restore suppression: skip logging if same visit already has curve entries
        if event_type in _CURVE_RESTORE_EVENT_TYPES and visit_id is not None:
            existing = fetch_all(
                "SELECT 1 FROM user_query_logs WHERE visit_id=:vid LIMIT 1",
                {'vid': visit_id}
            )
            if existing:
                app.logger.debug(
                    '[curve_set] restore suppressed for visit_id=%s (same-visit refresh)', visit_id
                )
                return resp_ok({'logged': 0, 'suppressed': True})

        logged = _write_curve_logs(user_id, visit_id, cleaned, event_type, effect_type, source, action_id)
        return resp_ok({'logged': logged})
    except Exception as e:
        app.logger.exception(e)
        return resp_err('INTERNAL_ERROR', str(e), 500)


# =========================================
# Unified Radar Model-Set API
# =========================================
_VALID_RADAR_ACTIONS = frozenset({'add', 'remove', 'restore', 'clear_all'})

@app.post('/api/radar_models')
def api_radar_models():
    """
    Unified radar model-set logging endpoint.

    Body:
      action      (str, required) – one of: add / remove / restore / clear_all
      model_id    (int, optional) – required for add / remove; omit for clear_all / restore
      source      (str, optional) – e.g. 'radar_panel', 'search_add', 'history_restore'
      action_id   (str, optional) – shared token for batch operations (e.g. clear_all / restore)

    Logs the event to user_radar_logs using visit_id as the canonical visit linkage.
    effect_type is derived automatically: add/restore → enter; remove/clear_all → leave.
    """
    try:
        user_id = get_or_create_user_identifier()
        data = request.get_json(force=True, silent=True) or {}

        action = (data.get('action') or '').strip()
        if action not in _VALID_RADAR_ACTIONS:
            return resp_err('INVALID_ACTION', f'action must be one of {sorted(_VALID_RADAR_ACTIONS)}', 400)

        model_id = _coerce_to_int_or_none(data.get('model_id'))
        if action in ('add', 'remove') and model_id is None:
            return resp_err('MISSING_MODEL_ID', 'model_id is required for add/remove actions', 400)

        source = (data.get('source') or '').strip()[:64] or None
        action_id = (data.get('action_id') or '').strip()[:64] or None

        effect_type = 'enter' if action in ('add', 'restore') else 'leave'
        visit_id = _get_latest_visit_id_for_user(user_id)

        # Restore suppression: skip logging if same visit already has radar log entries
        if action == 'restore' and visit_id is not None:
            existing = fetch_all(
                "SELECT 1 FROM user_radar_logs WHERE visit_id=:vid LIMIT 1",
                {'vid': visit_id}
            )
            if existing:
                app.logger.debug(
                    '[radar_models] restore suppressed for visit_id=%s (same-visit refresh)', visit_id
                )
                return resp_ok({'logged': 0, 'suppressed': True})

        sql = """
        INSERT INTO user_radar_logs
          (user_identifier, visit_id, model_id, event_type, effect_type, source, action_id)
        VALUES
          (:u, :vid, :mid, :etype, :efftype, :src, :aid)
        """
        exec_write(sql, {
            'u': user_id,
            'vid': visit_id,
            'mid': model_id,
            'etype': action,
            'efftype': effect_type,
            'src': source,
            'aid': action_id
        })
        return resp_ok({'logged': 1})
    except Exception as e:
        app.logger.exception(e)
        return resp_err('INTERNAL_ERROR', str(e), 500)

# =========================================
# Cascade / Simple Lists (raw=1 保留)
# =========================================
def _maybe_raw_array(data):
    if request.args.get('raw') == '1':
        return jsonify(data)
    return resp_ok(data)


@app.route('/search_models/<query>')
def search_models(query):
    rows = fetch_all(
        "SELECT DISTINCT brand_name_zh, model_name FROM general_view WHERE model_name LIKE :q LIMIT 20",
        {'q': f"%{query}%"}
    )
    data = [f"{r['brand_name_zh']} {r['model_name']}" for r in rows]
    return _maybe_raw_array(data)


@app.route('/get_models/<brand>')
def get_models(brand):
    rows = fetch_all(
        "SELECT DISTINCT model_name FROM available_models_info_view WHERE brand_name_zh=:b",
        {'b': brand}
    )
    return _maybe_raw_array([r['model_name'] for r in rows])


@app.route('/get_conditions', defaults={'brand': None, 'model': None})
@app.route('/get_conditions/<brand>/<model>')
def get_conditions(brand=None, model=None):
    if brand and model:
        rows = fetch_all(
            "SELECT DISTINCT condition_id, condition_name_zh, resistance_type_zh, resistance_location_zh "
            "FROM general_view WHERE brand_name_zh=:b AND model_name=:m "
            "ORDER BY condition_name_zh",
            {'b': brand, 'm': model}
        )
        # 返回 [{condition_id, condition_name_zh}, ...]
        return _maybe_raw_array(rows)
    else:
        rows = fetch_all(
            "SELECT condition_id, condition_name_zh, resistance_type_zh, resistance_location_zh "
            "FROM working_condition WHERE is_valid=1 "
            "ORDER BY condition_name_zh"
        )
        # 返回 [{condition_id, condition_name_zh}, ...]
        return _maybe_raw_array(rows)


# NEW: 通过 (model_id, condition_id) 批量获取显示所需元信息（品牌名、型号名、工况名等）
@app.post('/api/meta_by_ids')
def api_meta_by_ids():
    try:
        data = request.get_json(force=True, silent=True) or {}
        raw_pairs = data.get('pairs') or []
        uniq, seen = [], set()
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
            uniq.append(t)
        if not uniq:
            return resp_ok({'items': []})

        conds, params = [], {}
        for i, (m, c) in enumerate(uniq, start=1):
            conds.append(f"(:m{i}, :c{i})")
            params[f"m{i}"] = m
            params[f"c{i}"] = c

        sql = f"""
          SELECT
            model_id, condition_id,
            brand_name_zh, model_name, condition_name_zh,
            resistance_type_zh, resistance_location_zh,
            size, thickness, max_speed
          FROM meta_view
          WHERE (model_id, condition_id) IN ({",".join(conds)})
          ORDER BY model_id, condition_id
        """
        rows = fetch_all(sql, params)
        return resp_ok({'items': rows})
    except Exception as e:
        app.logger.exception(e)
        return resp_err('INTERNAL_ERROR', f'meta_by_ids 异常: {e}', 500)

# =========================================
# Search
# =========================================
@app.route('/api/search_fans', methods=['POST'])
def api_search_fans():
    try:
        data = request.get_json(force=True, silent=True) or {}
        # Filter mode（优先 condition_id）
        composite_mode = bool(data.get('composite_mode'))
        condition_id = data.get('condition_id')
        condition = (data.get('condition') or '').strip()
        size_filter = (data.get('size_filter') or '').strip()
        thickness_min = (data.get('thickness_min') or '').strip()
        thickness_max = (data.get('thickness_max') or '').strip()
        sort_by = (data.get('sort_by') or 'none').strip()
        sort_value_raw = (data.get('sort_value') or '').strip()

        # Active score profile — drives server-side sorting for composite / condition score
        score_profile = (data.get('score_profile') or _DEFAULT_SCORE_PROFILE).strip()
        if score_profile not in SCORING_PROFILES:
            score_profile = _DEFAULT_SCORE_PROFILE

        # 参考价格区间（默认 0-999），仅 0~999 的整数
        price_min_raw = (data.get('price_min') or '0').strip()
        price_max_raw = (data.get('price_max') or '999').strip()
        rgb_light = (data.get('rgb_light') or '').strip()

        # 最大转速区间（默认 1-9999）
        max_speed_min_raw = (data.get('max_speed_min') or '1').strip()
        max_speed_max_raw = (data.get('max_speed_max') or '9999').strip()

        if not composite_mode and not condition_id and not condition:
            return resp_err('SEARCH_MISSING_CONDITION', '请选择工况名称')

        try:
            tmin = int(thickness_min); tmax = int(thickness_max)
        except ValueError:
            return resp_err('SEARCH_INVALID_THICKNESS_FORMAT', '厚度必须为整数')
        if tmin < 1 or tmax < 1 or tmin > 99 or tmin > tmax:
            return resp_err('SEARCH_INVALID_THICKNESS_RANGE', '厚度区间不合法 (1~99 且最小不大于最大)')

        # 验证价格
        try:
            pmin = int(price_min_raw); pmax = int(price_max_raw)
        except ValueError:
            return resp_err('SEARCH_INVALID_PRICE_FORMAT', '参考价格必须为整数')
        if pmin < 0 or pmax < 0 or pmin > 999 or pmax > 999 or pmin > pmax:
            return resp_err('SEARCH_INVALID_PRICE_RANGE', '参考价格区间不合法 (0~999 且最小不大于最大)')

        # 验证最大转速
        try:
            msmin = int(max_speed_min_raw); msmax = int(max_speed_max_raw)
        except ValueError:
            return resp_err('SEARCH_INVALID_MAXSPEED_FORMAT', '最大转速必须为正整数')
        if msmin < 1 or msmax < 1 or msmin > 9999 or msmax > 9999 or msmin > msmax:
            return resp_err('SEARCH_INVALID_MAXSPEED_RANGE', '最大转速区间不合法 (1~9999 且最小不大于最大)')

        if composite_mode:
            # 综合评分 mode: no condition filter, sort by composite_score
            results = search_fans_composite(
                size_filter=size_filter if size_filter else None,
                thickness_min=tmin, thickness_max=tmax,
                price_min=pmin, price_max=pmax,
                rgb_light=rgb_light if rgb_light and rgb_light != '不限' else None,
                max_speed_min=msmin, max_speed_max=msmax,
                limit=200
            )
            label = '测试工况：综合评分，排序依据：综合评分'
            searched_cid = None
        else:
            sort_value = None
            if sort_by not in ('none', 'condition_score'):
                if not sort_value_raw: return resp_err('SEARCH_MISSING_SORT_VALUE', '请输入限制值')
                try: sort_value = float(sort_value_raw)
                except ValueError: return resp_err('SEARCH_INVALID_SORT_VALUE', '限制值必须是数字')

            results = search_fans_by_condition_with_fit(
                condition_id= int(condition_id) if condition_id else None,
                condition_name= condition if (not condition_id) else None,
                sort_by=sort_by, sort_value=sort_value,
                size_filter=size_filter, thickness_min=tmin, thickness_max=tmax,
                price_min=pmin, price_max=pmax,
                rgb_light=rgb_light if rgb_light and rgb_light != '不限' else None,
                max_speed_min=msmin, max_speed_max=msmax,
                limit=200
            )

            # Resolve condition name from backend-verified data
            cond_name = None
            if condition_id:
                cond_rows = fetch_all(
                    "SELECT condition_name_zh FROM working_condition WHERE condition_id=:cid LIMIT 1",
                    {'cid': int(condition_id)}
                )
                if cond_rows:
                    cond_name = cond_rows[0]['condition_name_zh']
            if not cond_name:
                cond_name = condition or ''

            cond_prefix = f'工况：{cond_name}，' if cond_name else ''

            if sort_by == 'rpm':
                label = f'{cond_prefix}排序依据：同转速风量（转速 ≤ {sort_value_raw} RPM，原始优先，无原始则拟合）'
            elif sort_by == 'noise':
                label = f'{cond_prefix}排序依据：同分贝风量（噪音 ≤ {sort_value_raw} dB，原始优先，无原始则拟合）'
            elif sort_by == 'condition_score':
                label = f'{cond_prefix}排序依据：工况评分'
            else:
                label = f'{cond_prefix}排序依据：全速风量'

            searched_cid = int(condition_id) if condition_id else None

        # Enrich results with ABC-cache data (composite_score, condition_scores, condition_score)
        # and condition heat / like / query counts. Reuse the rankings-v2 cache (same data
        # as the heat board and performance board) to avoid redundant DB reads.
        RADAR_CIDS_LIST = _RADAR_CIDS  # shared constant; synced with right-panel-v2.js

        try:
            rankings = get_rankings_v2()
        except Exception:
            rankings = {}
        ranked_lookup: dict = rankings.get('model_lookup') or {}

        for item in results:
            mid = item['model_id']
            ranked = ranked_lookup.get(mid)

            # Heat / like / query counts from rankings cache
            if ranked:
                heat_score      = ranked.get('heat_score', 0)
                condition_heat  = ranked.get('condition_heat', {cid: 0 for cid in RADAR_CIDS_LIST})
                condition_likes = ranked.get('condition_likes', {cid: 0 for cid in RADAR_CIDS_LIST})
                total_queries   = ranked.get('query_count', 0)
                total_likes     = ranked.get('like_count', 0)
            else:
                heat_score      = 0
                condition_heat  = {cid: 0 for cid in RADAR_CIDS_LIST}
                condition_likes = {cid: 0 for cid in RADAR_CIDS_LIST}
                total_queries   = 0
                total_likes     = 0

            # Score data from ABC cache (all profiles).
            # Use the requested score_profile so that server-side sorting by
            # composite_score / condition_score reflects the active profile.
            all_profiles_abc = _get_abc_all_profiles(mid)
            abc = all_profiles_abc.get(score_profile)
            composite_score = None
            condition_scores: dict = {}
            item_condition_score = None
            if abc:
                composite_score = abc.get('composite_score')
                conds_abc = abc.get('conditions') or {}
                for cid_r in RADAR_CIDS_LIST:
                    cd = conds_abc.get(cid_r)
                    raw = cd.get('score_total') if cd else None
                    try:
                        condition_scores[cid_r] = int(raw) if raw is not None else None
                    except (TypeError, ValueError):
                        condition_scores[cid_r] = None
                # Score for the searched condition (not used in composite mode)
                s_cid = searched_cid if searched_cid is not None else item.get('condition_id')
                if s_cid is not None:
                    cd_s = conds_abc.get(s_cid)
                    raw_s = cd_s.get('score_total') if cd_s else None
                    try:
                        item_condition_score = int(raw_s) if raw_s is not None else None
                    except (TypeError, ValueError):
                        item_condition_score = None
            item['composite_score'] = int(composite_score) if composite_score is not None else None
            item['condition_scores'] = condition_scores
            item['score_profiles'] = _build_score_profiles_payload(all_profiles_abc, RADAR_CIDS_LIST)
            item['condition_likes'] = condition_likes
            item['condition_score'] = item_condition_score
            item['condition_heat'] = condition_heat
            item['heat_score'] = heat_score
            item['query_count'] = total_queries
            item['like_count'] = total_likes

        # Re-sort by condition_score when requested (None treated as -1)
        if not composite_mode and sort_by == 'condition_score':
            results.sort(
                key=lambda x: (x['condition_score'] if x['condition_score'] is not None else -1),
                reverse=True
            )

        # Composite mode: sort by composite_score DESC
        if composite_mode:
            results.sort(
                key=lambda x: (x['composite_score'] if x['composite_score'] is not None else -1),
                reverse=True
            )

        return resp_ok({'search_results': results, 'condition_label': label})
    except Exception as e:
        app.logger.exception(e)
        return resp_err('INTERNAL_ERROR', f'搜索异常: {e}', 500)

@app.get('/api/brands')
def api_brands():
    try:
        rows = fetch_all("SELECT brand_id, brand_name_zh FROM fan_brand WHERE is_valid=1 ORDER BY brand_name_zh")
        return resp_ok({'items': rows})
    except Exception as e:
        app.logger.exception(e); return resp_err('INTERNAL_ERROR', str(e), 500)

@app.get('/api/rgb_options')
def api_rgb_options():
    try:
        rows = fetch_all("SELECT DISTINCT rgb_light FROM available_models_info_view WHERE rgb_light IS NOT NULL AND rgb_light != '' ORDER BY rgb_light")
        return resp_ok({'items': [r['rgb_light'] for r in rows]})
    except Exception as e:
        app.logger.exception(e); return resp_err('INTERNAL_ERROR', str(e), 500)

@app.get('/api/models_by_brand')
def api_models_by_brand():
    try:
        bid = int(request.args.get('brand_id') or '0')
        if not bid: return resp_err('BAD_REQUEST', 'brand_id 缺失或非法')
        rows = fetch_all("SELECT model_id, model_name FROM available_models_info_view WHERE brand_id=:b ORDER BY model_name", {'b': bid})
        items = []
        for row in rows:
            mid = int(row['model_id'])
            item = dict(row)
            all_profiles = _get_abc_all_profiles(mid)
            cache_entry = all_profiles.get(_DEFAULT_SCORE_PROFILE)
            if cache_entry:
                item['radar'] = {
                    'conditions': cache_entry['conditions'],
                    'composite_score': cache_entry.get('composite_score'),
                    'updated_at': cache_entry['updated_at'],
                    'score_profiles': _build_score_profiles_payload(all_profiles, _RADAR_CIDS),
                }
            else:
                item['radar'] = None
            items.append(item)
        return resp_ok({'items': items})
    except Exception as e:
        app.logger.exception(e); return resp_err('INTERNAL_ERROR', str(e), 500)

@app.get('/api/conditions_by_model')
def api_conditions_by_model():
    try:
        mid = int(request.args.get('model_id') or '0')
        if not mid: return resp_err('BAD_REQUEST', 'model_id 缺失或非法')
        rows = fetch_all(
            "SELECT DISTINCT condition_id, condition_name_zh, resistance_type_zh, "
            "resistance_location_zh FROM general_view WHERE model_id=:m ORDER BY condition_name_zh",
            {'m': mid}
        )
        # Attach ABC scores from in-memory cache (soft/hard TTL, no DB table dependency)
        all_profiles = _get_abc_all_profiles(mid)
        cache_entry = all_profiles.get(_DEFAULT_SCORE_PROFILE)
        scores: Dict[int, dict] = cache_entry['conditions'] if cache_entry else {}
        items = []
        for r in rows:
            cid = int(r['condition_id'])
            item = dict(r)
            sc = scores.get(cid)
            item['score_total'] = sc['score_total'] if sc else None
            item['score_a'] = sc['score_a'] if sc else None
            item['score_b'] = sc['score_b'] if sc else None
            item['score_c'] = sc['score_c'] if sc else None
            items.append(item)
        return resp_ok({'items': items})
    except Exception as e:
        app.logger.exception(e); return resp_err('INTERNAL_ERROR', str(e), 500)


# =========================================
# =========================================
# Model-level metadata batch endpoint
# =========================================
@app.get('/api/model_meta')
def api_model_meta():
    """
    Batch endpoint: GET /api/model_meta?model_ids=1,2,3

    Returns model-level metadata (brand, model name, price, max_speed, size,
    thickness, rgb_light, review) for the requested model_ids.  Used by the frontend
    to populate the persistent model-metadata cache so Browsing History cards
    can display correct header information.

    Response shape:
      {
        "models": {
          "<model_id>": {
            "brand": "...", "model": "...",
            "reference_price": <float|null>,
            "max_speed": <int|null>,
            "size": "...", "thickness": "...", "rgb_light": "...", "review": "..."
          }, ...
        }
      }
    """
    try:
        raw = request.args.get('model_ids', '')
        model_ids: List[int] = []
        for part in raw.split(','):
            part = part.strip()
            if part:
                try:
                    model_ids.append(int(part))
                except ValueError:
                    pass
        if not model_ids:
            return resp_ok({'models': {}})

        placeholders = ','.join(f':m{i}' for i in range(len(model_ids)))
        params = {f'm{i}': mid for i, mid in enumerate(model_ids)}
        sql = f"""
          SELECT v.model_id, v.brand_name_zh, v.model_name,
                 v.reference_price,
                 v.max_speed, v.size, v.thickness,
                 COALESCE(v.rgb_light, '') AS rgb_light,
                 v.review
          FROM available_models_info_view v
          WHERE v.model_id IN ({placeholders})
        """
        rows = fetch_all(sql, params)
        models_out = {}
        for r in rows:
            mid_str = str(int(r['model_id']))
            models_out[mid_str] = {
                'brand': r['brand_name_zh'] or '',
                'model': r['model_name'] or '',
                'reference_price': r.get('reference_price'),
                'max_speed': r.get('max_speed'),
                'size': r.get('size') or '',
                'thickness': r.get('thickness') or '',
                'rgb_light': r.get('rgb_light') or '',
                'review': r.get('review') or '',
            }
        return resp_ok({'models': models_out})
    except Exception as e:
        app.logger.exception(e)
        return resp_err('INTERNAL_ERROR', str(e), 500)


# =========================================
# Validate model IDs against current backend data
# =========================================
@app.get('/api/validate_models')
def api_validate_models():
    """
    Validate model IDs against the current backend data.
    Returns valid models with fresh metadata (including review) and a list of invalid (unavailable) IDs.

    GET /api/validate_models?model_ids=1,2,3

    Response shape:
      {
        "valid": {
          "<model_id>": {
            "brand": "...", "model": "...",
            "reference_price": <float|null>,
            "max_speed": <int|null>,
            "size": "...", "thickness": "...", "rgb_light": "...", "review": "..."
          }, ...
        },
        "invalid": [<model_id>, ...]
      }
    """
    try:
        raw = request.args.get('model_ids', '')
        model_ids: List[int] = []
        for part in raw.split(','):
            part = part.strip()
            if part:
                try:
                    model_ids.append(int(part))
                except ValueError:
                    pass
        if not model_ids:
            return resp_ok({'valid': {}, 'invalid': []})

        placeholders = ','.join(f':m{i}' for i in range(len(model_ids)))
        params = {f'm{i}': mid for i, mid in enumerate(model_ids)}
        sql = f"""
          SELECT v.model_id, v.brand_name_zh, v.model_name,
                 v.reference_price,
                 v.max_speed, v.size, v.thickness,
                 COALESCE(v.rgb_light, '') AS rgb_light,
                 COALESCE(v.review, '') AS review
          FROM available_models_info_view v
          WHERE v.model_id IN ({placeholders})
        """
        rows = fetch_all(sql, params)
        valid_set: set = set()
        valid_out: dict = {}
        for r in rows:
            try:
                mid = int(r['model_id'])
            except (TypeError, ValueError):
                continue
            valid_set.add(mid)
            valid_out[str(mid)] = {
                'brand': r['brand_name_zh'] or '',
                'model': r['model_name'] or '',
                'reference_price': r.get('reference_price'),
                'max_speed': r.get('max_speed'),
                'size': r.get('size') or '',
                'thickness': r.get('thickness') or '',
                'rgb_light': r.get('rgb_light') or '',
                'review': r.get('review') or '',
            }
        invalid = [mid for mid in model_ids if mid not in valid_set]
        return resp_ok({'valid': valid_out, 'invalid': invalid})
    except Exception as e:
        app.logger.exception(e)
        return resp_err('INTERNAL_ERROR', str(e), 500)


# =========================================
# Batch radar metrics API
# =========================================
@app.get('/api/radar_metrics')
def api_radar_metrics():
    """
    Batch endpoint: GET /api/radar_metrics?model_ids=1,2,3

    Returns ABC metrics for multiple model_ids in one call.
    Uses the in-memory _abc_cache with soft/hard TTL logic.

    Response shape:
      {
        "models": {
          "<model_id>": {
            "conditions": { "<cid>": {score_a, score_b, score_c, score_total, ...} },
            "updated_at": "<ISO-8601>"
          }, ...
        },
        "soft_refreshed": [<model_id>, ...],
        "hard_refreshed": [<model_id>, ...]
      }
    """
    try:
        raw = request.args.get('model_ids', '')
        model_ids: List[int] = []
        for part in raw.split(','):
            part = part.strip()
            if part:
                try:
                    model_ids.append(int(part))
                except ValueError:
                    pass
        if not model_ids:
            return resp_err('BAD_REQUEST', 'model_ids required (comma-separated integers)')

        models_out: dict = {}
        soft_refreshed: List[int] = []
        hard_refreshed: List[int] = []

        now = time.time()
        for model_id in model_ids:
            all_profiles = _get_abc_all_profiles(model_id)
            entry = all_profiles.get(_DEFAULT_SCORE_PROFILE)

            if entry is not None:
                age = now - entry['cached_at']
                if age < ABC_SOFT_TTL_SEC:
                    # Fresh: return as-is
                    pass
                elif age < ABC_HARD_TTL_SEC:
                    # Soft-expired: return stale + trigger async refresh for all profiles
                    soft_refreshed.append(model_id)
                    for pkey in SCORING_PROFILES:
                        _trigger_soft_refresh(model_id, pkey)
                else:
                    # Hard-expired: synchronous recompute for all profiles
                    hard_refreshed.append(model_id)
                    for pkey in SCORING_PROFILES:
                        _sync_compute_and_cache(model_id, pkey)
                    all_profiles = _get_abc_all_profiles(model_id)
                    entry = all_profiles.get(_DEFAULT_SCORE_PROFILE)
                if entry:
                    models_out[str(model_id)] = {
                        'conditions': entry['conditions'],
                        'composite_score': entry.get('composite_score'),
                        'updated_at': entry['updated_at'],
                        'score_profiles': _build_score_profiles_payload(all_profiles, _RADAR_CIDS),
                    }
            else:
                # Missing: synchronous compute for all profiles
                hard_refreshed.append(model_id)
                for pkey in SCORING_PROFILES:
                    _sync_compute_and_cache(model_id, pkey)
                all_profiles = _get_abc_all_profiles(model_id)
                entry = all_profiles.get(_DEFAULT_SCORE_PROFILE)
                if entry:
                    models_out[str(model_id)] = {
                        'conditions': entry['conditions'],
                        'composite_score': entry.get('composite_score'),
                        'updated_at': entry['updated_at'],
                        'score_profiles': _build_score_profiles_payload(all_profiles, _RADAR_CIDS),
                    }

        return resp_ok({
            'models': models_out,
            'soft_refreshed': soft_refreshed,
            'hard_refreshed': hard_refreshed,
        })
    except Exception as e:
        app.logger.exception(e)
        return resp_err('INTERNAL_ERROR', str(e), 500)


# =========================================
# Internal: warm score cache (called by admin after data import)
# =========================================
@app.post('/api/internal/warm_scores')
def api_internal_warm_scores():
    """
    Internal endpoint to rebuild PCHIP and warm ABC score cache for a
    (model_id, condition_id) pair after admin data import.
    Runs the work in a background thread so the response is instant.
    """
    try:
        data = request.get_json(force=True, silent=True) or {}
        model_id = int(data.get('model_id') or 0)
        condition_id = int(data.get('condition_id') or 0)
        if not model_id or not condition_id:
            return resp_err('BAD_REQUEST', 'model_id and condition_id required')

        def _warm():
            try:
                spectrum_reader.build_performance_pchips([(model_id, condition_id)])
            except Exception as e:
                app.logger.warning('[warm_scores] pchip rebuild (%s,%s): %s', model_id, condition_id, e)
            # Recompute ABC for this model under all profiles and refresh in-memory cache
            for profile_key in SCORING_PROFILES:
                try:
                    _sync_compute_and_cache(model_id, profile_key)
                except Exception as e:
                    app.logger.warning('[warm_scores] abc recompute (%s, %s): %s', model_id, profile_key, e)

        threading.Thread(target=_warm, daemon=True).start()
        return resp_ok({'queued': True})
    except Exception as e:
        app.logger.exception(e)
        return resp_err('INTERNAL_ERROR', str(e), 500)

# =========================================
# Query Count (去除顶层旧兼容字段)
# =========================================
@app.route('/api/query_count')
def get_query_count():
    return resp_ok({'count': query_count_cache})


def update_query_count():
    global query_count_cache
    while True:
        try:
            result = fetch_all("SELECT COUNT(DISTINCT batch_id) AS c FROM user_query_logs WHERE effect_type = 'enter'")
            query_count = result[0]['c'] if result else 0
            radar_result = fetch_all(
                "SELECT COUNT(1) AS radar_add FROM FANDB.user_radar_logs WHERE effect_type = 'enter'"
            )
            radar_add = radar_result[0]['radar_add'] if radar_result else 0
            query_count_cache = query_count + radar_add
        except Exception as e:
            print(f"更新查询次数失败: {e}")
        time.sleep(60)


threading.Thread(target=update_query_count, daemon=True).start()


# =========================================
# Denominator refresh scheduled jobs (replaces condition_stats scheduled jobs)
# =========================================

def _like_max_refresh_loop():
    """Every 5 minutes: update max_c in _cond_denom_cache from DB max(like_count).

    max_c (like count popularity score) is profile-independent; update all profiles.
    """
    while True:
        time.sleep(5 * 60)
        try:
            cid_list = list(SCORE_CONDITION_IDS)
            parts = ', '.join([f':c{i}' for i in range(len(cid_list))])
            params = {f'c{i}': cid for i, cid in enumerate(cid_list)}
            rows = fetch_all(
                f"SELECT condition_id, COALESCE(MAX(like_count), 0) AS max_like "
                f"FROM general_view WHERE condition_id IN ({parts}) GROUP BY condition_id",
                params
            )
            with _cond_denom_lock:
                for r in rows:
                    cid = int(r['condition_id'])
                    max_like = max(0, int(r.get('max_like') or 0))
                    new_c = math.log1p(max_like)
                    for pkey in SCORING_PROFILES:
                        entry = _cond_denom_cache[pkey].get(cid)
                        if entry is not None and new_c > entry.get('max_c', 0.0):
                            entry['max_c'] = new_c
        except Exception as e:
            app.logger.warning('[like_max_refresh] error: %s', e)


def _update_max_with_model(
    per_val: dict, per_mid: dict, cid: int, new_val, new_mid: int
) -> None:
    """Update per_val[cid] if new_val is strictly greater, or equal with smaller model_id.

    Implements tie-breaking by smallest model_id: when new_val equals the current
    maximum, the model with the smallest model_id wins.
    """
    curr = per_val.get(cid, 0.0)
    if new_val > curr or (new_val == curr and new_mid < per_mid.get(cid, float('inf'))):
        per_val[cid] = new_val
        per_mid[cid] = new_mid


def _persist_denom_audit(
    cid_list,
    per_cid_low_db, per_cid_high_db, per_cid_wa2_ref_db, per_cid_mid_db,
    per_cid_a1, per_cid_a1_mid,
    per_cid_a2, per_cid_a2_mid,
    per_cid_a3, per_cid_a3_mid,
    per_cid_b,  per_cid_b_mid,
    per_cid_wc_raw, per_cid_wc_mid,
    per_cid_d,  per_cid_d_mid,
):
    """Persist per-condition ABC normalization reference/max audit rows.

    Writes one row per condition_id to condition_score_ref_stats containing
    the raw (unnormalized) reference dB values and maxima used for scoring.
    Silently skips if the table does not exist.

    The required CREATE TABLE statement is provided in the PR description for
    manual execution; this function never creates or alters the table.
    """
    sql = """
        INSERT INTO condition_score_ref_stats
            (condition_id,
             wa1_ref_db, wa1_max_raw, wa1_model_id,
             wa2_ref_db, wa2_max_raw, wa2_model_id,
             wa3_ref_db, wa3_max_raw, wa3_model_id,
             wb_ref_db,  wb_max_raw,  wb_model_id,
             wc_ref_db,  wc_max_raw,  wc_model_id,
             wd_ref_db,  wd_max_raw,  wd_model_id,
             updated_at)
        VALUES
            (:condition_id,
             :wa1_ref_db, :wa1_max_raw, :wa1_model_id,
             :wa2_ref_db, :wa2_max_raw, :wa2_model_id,
             :wa3_ref_db, :wa3_max_raw, :wa3_model_id,
             NULL, :wb_max_raw, :wb_model_id,
             NULL, :wc_max_raw, :wc_model_id,
             NULL, :wd_max_raw, :wd_model_id,
             NOW())
        ON DUPLICATE KEY UPDATE
            wa1_ref_db   = VALUES(wa1_ref_db),
            wa1_max_raw  = VALUES(wa1_max_raw),
            wa1_model_id = VALUES(wa1_model_id),
            wa2_ref_db   = VALUES(wa2_ref_db),
            wa2_max_raw  = VALUES(wa2_max_raw),
            wa2_model_id = VALUES(wa2_model_id),
            wa3_ref_db   = VALUES(wa3_ref_db),
            wa3_max_raw  = VALUES(wa3_max_raw),
            wa3_model_id = VALUES(wa3_model_id),
            wb_ref_db    = NULL,
            wb_max_raw   = VALUES(wb_max_raw),
            wb_model_id  = VALUES(wb_model_id),
            wc_ref_db    = NULL,
            wc_max_raw   = VALUES(wc_max_raw),
            wc_model_id  = VALUES(wc_model_id),
            wd_ref_db    = NULL,
            wd_max_raw   = VALUES(wd_max_raw),
            wd_model_id  = VALUES(wd_model_id),
            updated_at   = NOW()
    """
    for cid in cid_list:
        params = {
            'condition_id': cid,
            'wa1_ref_db':   per_cid_low_db.get(cid),
            'wa1_max_raw':  per_cid_a1.get(cid) or None,
            'wa1_model_id': per_cid_a1_mid.get(cid),
            'wa2_ref_db':   per_cid_wa2_ref_db.get(cid),
            'wa2_max_raw':  per_cid_a2.get(cid) or None,
            'wa2_model_id': per_cid_a2_mid.get(cid),
            'wa3_ref_db':   per_cid_mid_db.get(cid),
            'wa3_max_raw':  per_cid_a3.get(cid) or None,
            'wa3_model_id': per_cid_a3_mid.get(cid),
            'wb_max_raw':   per_cid_b.get(cid) or None,
            'wb_model_id':  per_cid_b_mid.get(cid),
            'wc_max_raw':   per_cid_wc_raw.get(cid) or None,
            'wc_model_id':  per_cid_wc_mid.get(cid),
            'wd_max_raw':   per_cid_d.get(cid) or None,
            'wd_model_id':  per_cid_d_mid.get(cid),
        }
        try:
            exec_write(sql, params)
        except sa_exc.OperationalError as e:
            orig = getattr(e, 'orig', None)
            code = getattr(orig, 'args', [None])[0] if orig else None
            if code == 1146:
                app.logger.debug('[denom_refresh] audit table not found, skipping persist')
                return
            app.logger.warning('[denom_refresh] audit persist error for cid=%s: %s', cid, e)
        except Exception as e:
            app.logger.warning('[denom_refresh] audit persist error for cid=%s: %s', cid, e)


def _do_denom_refresh():
    """Rebuild per-condition denominators for all SCORE_CONDITION_IDS across all profiles.

    For each condition, determines the common valid dB range across all models:
      low_db  = max(each model's min noise dB) for that condition
      high_db = min(each model's max noise dB) for that condition
      mid_db  = (low_db + high_db) / 2
      wa2_ref_db = max(high_db, WA2_ANCHOR_MODEL_ID's max dB for that condition)

    Profile-specific computation:
      wa2_ref_db = max(high_db, profile's WA2_ANCHOR's max dB for that condition)
      max_a2     = max airflow at wa2_ref_db across all models per condition

    Shared computation (same for all profiles):
      max_a1: max airflow at low_db     across all models per condition
      max_a3: max airflow at mid_db     across all models per condition
      max_b: max(airflow_cfm) per condition from general_view
      max_c: log1p(max(like_count)) per condition from general_view
      max_d: max(max_airflow / noise_dB_at_max_airflow) per condition

    Results are stored in _cond_denom_cache[profile_key] (in-memory).
    After updating, also persists audit rows for the med profile (same as legacy).
    """
    try:
        cid_list = list(SCORE_CONDITION_IDS)
        parts = ', '.join([f':c{i}' for i in range(len(cid_list))])
        params = {f'c{i}': cid for i, cid in enumerate(cid_list)}
        pair_rows = fetch_all(
            f"SELECT DISTINCT model_id, condition_id FROM general_view "
            f"WHERE condition_id IN ({parts})",
            params
        )
        pairs = [(int(r['model_id']), int(r['condition_id'])) for r in pair_rows]

        # Step 1: rebuild PCHIP for all pairs and collect per-model dB bounds + n2a
        # per_cid_min_dbs[cid] → list of each model's minimum noise dB for that condition
        # per_cid_max_dbs[cid] → list of each model's maximum noise dB for that condition
        # per_cid_n2a[cid]     → list of (mid, n2a) for models that have a valid n2a curve
        # per_model_max_db     → (cid, model_id) → that model's maximum noise dB
        per_cid_min_dbs: Dict[int, list] = defaultdict(list)
        per_cid_max_dbs: Dict[int, list] = defaultdict(list)
        per_cid_n2a: Dict[int, list] = defaultdict(list)
        per_model_max_db: Dict[tuple, float] = {}
        if pairs:
            try:
                spectrum_reader.build_performance_pchips(pairs)
            except Exception as e:
                app.logger.warning('[denom_refresh] pchip rebuild failed: %s', e)
            for mid, cid in pairs:
                mdl = load_unified_perf_model(mid, cid)
                if mdl:
                    n2a = (mdl.get('pchip') or {}).get('noise_to_airflow')
                    if n2a and isinstance(n2a, dict):
                        xs = n2a.get('x') or []
                        if xs:
                            # PCHIP x-coordinates are guaranteed sorted ascending by construction
                            per_cid_min_dbs[cid].append(float(xs[0]))
                            per_cid_max_dbs[cid].append(float(xs[-1]))
                            per_cid_n2a[cid].append((mid, n2a))
                            per_model_max_db[(cid, mid)] = float(xs[-1])

        # Step 2: determine per-condition common dB range (intersection across all models)
        per_cid_low_db: Dict[int, float | None] = {}
        per_cid_high_db: Dict[int, float | None] = {}
        per_cid_mid_db: Dict[int, float | None] = {}
        for cid in cid_list:
            min_dbs = per_cid_min_dbs.get(cid)
            max_dbs = per_cid_max_dbs.get(cid)
            if not min_dbs or not max_dbs:
                per_cid_low_db[cid] = None
                per_cid_high_db[cid] = None
                per_cid_mid_db[cid] = None
                continue
            low_db  = max(min_dbs)   # highest lower bound = intersection start
            high_db = min(max_dbs)   # lowest upper bound  = intersection end
            mid_db  = (low_db + high_db) / 2
            per_cid_low_db[cid]  = low_db
            per_cid_high_db[cid] = high_db
            per_cid_mid_db[cid]  = mid_db
            if low_db > high_db:
                app.logger.warning(
                    '[denom_refresh] cid=%s: invalid common dB range '
                    'low_db=%.2f > high_db=%.2f; all models will get null score for this condition',
                    cid, low_db, high_db
                )

        # Step 2b: compute effective WA2 reference dB per condition — once per profile.
        # Each profile may have a different wa2_anchor which changes wa2_ref_db and max_a2.
        per_profile_wa2_ref_db: Dict[str, Dict[int, float | None]] = {}
        for pkey, pcfg in PROFILE_CONFIGS.items():
            anchor_id = pcfg['wa2_anchor']
            per_cid_wa2_ref_db: Dict[int, float | None] = {}
            for cid in cid_list:
                orig_high_db = per_cid_high_db.get(cid)
                if orig_high_db is None:
                    per_cid_wa2_ref_db[cid] = None
                    continue
                if anchor_id == 0:
                    per_cid_wa2_ref_db[cid] = orig_high_db
                else:
                    anchor_max = per_model_max_db.get((cid, anchor_id))
                    per_cid_wa2_ref_db[cid] = (
                        max(orig_high_db, anchor_max) if anchor_max is not None else orig_high_db
                    )
            per_profile_wa2_ref_db[pkey] = per_cid_wa2_ref_db

        # Step 3: compute per-condition max airflow at WA1 (low_db) and WA3 (mid_db).
        # These are profile-independent (all profiles share the same dB geometry).
        per_cid_a1: Dict[int, float] = defaultdict(float)
        per_cid_a3: Dict[int, float] = defaultdict(float)
        per_cid_a1_mid: Dict[int, int] = {}
        per_cid_a3_mid: Dict[int, int] = {}
        for cid in cid_list:
            low_db  = per_cid_low_db.get(cid)
            high_db = per_cid_high_db.get(cid)
            wa2_ref = per_cid_wa2_ref_db.get(cid)
            mid_db  = per_cid_mid_db.get(cid)
            if low_db is None or high_db is None or low_db > high_db:
                continue
            for mid, n2a in per_cid_n2a.get(cid, []):
                model_max_db = per_model_max_db.get((cid, mid))
                n2a_y = n2a.get('y') or []
                model_max_airflow = float(max(n2a_y)) if n2a_y else 0.0

                # WA1: always eval at low_db
                try:
                    val = eval_pchip(n2a, low_db)
                    if val is not None and math.isfinite(val) and val > 0:
                        _update_max_with_model(per_cid_a1, per_cid_a1_mid, cid, float(val), mid)
                except Exception:
                    pass

                # WA3: fall back to model max airflow if model can't reach mid_db
                wa3_val = None
                if model_max_db is not None and mid_db is not None and model_max_db < mid_db:
                    if model_max_airflow > 0:
                        wa3_val = model_max_airflow
                else:
                    try:
                        val = eval_pchip(n2a, mid_db)
                        if val is not None and math.isfinite(val) and val > 0:
                            wa3_val = float(val)
                    except Exception:
                        pass
                if wa3_val is not None and wa3_val > 0:
                    _update_max_with_model(per_cid_a3, per_cid_a3_mid, cid, wa3_val, mid)

        # Step 3b: compute WA2 max airflow per profile (profile-specific due to wa2_ref_db).
        per_profile_a2: Dict[str, Dict[int, float]] = {}
        per_profile_a2_mid: Dict[str, Dict[int, int]] = {}
        for pkey in SCORING_PROFILES:
            wa2_ref_map = per_profile_wa2_ref_db[pkey]
            pp_a2: Dict[int, float] = defaultdict(float)
            pp_a2_mid: Dict[int, int] = {}
            for cid in cid_list:
                low_db  = per_cid_low_db.get(cid)
                high_db = per_cid_high_db.get(cid)
                wa2_ref = wa2_ref_map.get(cid)
                if low_db is None or high_db is None or low_db > high_db:
                    continue
                for mid, n2a in per_cid_n2a.get(cid, []):
                    model_max_db = per_model_max_db.get((cid, mid))
                    n2a_y = n2a.get('y') or []
                    model_max_airflow = float(max(n2a_y)) if n2a_y else 0.0
                    eff_wa2 = wa2_ref if wa2_ref is not None else high_db
                    wa2_val = None
                    if eff_wa2 is None:
                        pass
                    elif model_max_db is not None and model_max_db < eff_wa2:
                        if model_max_airflow > 0:
                            wa2_val = model_max_airflow
                    else:
                        try:
                            val = eval_pchip(n2a, eff_wa2)
                            if val is not None and math.isfinite(val) and val > 0:
                                wa2_val = float(val)
                        except Exception:
                            pass
                    if wa2_val is not None and wa2_val > 0:
                        _update_max_with_model(pp_a2, pp_a2_mid, cid, wa2_val, mid)
            per_profile_a2[pkey] = pp_a2
            per_profile_a2_mid[pkey] = pp_a2_mid

        # B maxima: max(airflow_cfm) per condition, plus model_id (tie: smallest).
        b_parts = ', '.join([f':c{i}' for i in range(len(cid_list))])
        b_params = {f'c{i}': cid for i, cid in enumerate(cid_list)}
        b_rows = fetch_all(
            f"SELECT condition_id, COALESCE(MAX(airflow_cfm), 0) AS max_b "
            f"FROM general_view WHERE condition_id IN ({b_parts}) GROUP BY condition_id",
            b_params
        )
        per_cid_b: Dict[int, float] = {
            int(r['condition_id']): float(r.get('max_b') or 0.0) for r in b_rows
        }
        # Per-(condition, model) max airflow to resolve which model achieved max_b.
        bm_rows = fetch_all(
            f"SELECT condition_id, model_id, MAX(airflow_cfm) AS model_max_b "
            f"FROM general_view WHERE condition_id IN ({b_parts}) GROUP BY condition_id, model_id",
            b_params
        )
        per_cid_b_mid: Dict[int, int] = {}
        for r in bm_rows:
            cid = int(r['condition_id'])
            mid = int(r['model_id'])
            val = float(r.get('model_max_b') or 0.0)
            if val == per_cid_b.get(cid, 0.0):
                if cid not in per_cid_b_mid or mid < per_cid_b_mid[cid]:
                    per_cid_b_mid[cid] = mid

        # C maxima: log1p(max(like_count)) per condition (used for scoring denominator).
        # Also collect raw max like_count and its model_id for the audit table.
        c_parts = ', '.join([f':c{i}' for i in range(len(cid_list))])
        c_params = {f'c{i}': cid for i, cid in enumerate(cid_list)}
        c_rows = fetch_all(
            f"SELECT condition_id, COALESCE(MAX(like_count), 0) AS max_lc "
            f"FROM general_view WHERE condition_id IN ({c_parts}) GROUP BY condition_id",
            c_params
        )
        per_cid_c: Dict[int, float] = {
            int(r['condition_id']): math.log1p(max(0, int(r.get('max_lc') or 0)))
            for r in c_rows
        }
        # Per-(condition, model) max like_count for WC audit (raw value, not log-transformed).
        cm_rows = fetch_all(
            f"SELECT condition_id, model_id, MAX(like_count) AS model_max_lc "
            f"FROM general_view WHERE condition_id IN ({c_parts}) GROUP BY condition_id, model_id",
            c_params
        )
        per_cid_wc_raw: Dict[int, int] = defaultdict(int)
        per_cid_wc_mid: Dict[int, int] = {}
        for r in cm_rows:
            cid = int(r['condition_id'])
            mid = int(r['model_id'])
            val = max(0, int(r.get('model_max_lc') or 0))
            _update_max_with_model(per_cid_wc_raw, per_cid_wc_mid, cid, val, mid)

        # D maxima: max(max_airflow / noise_dB_at_max_airflow) per condition from noise_to_airflow PCHIP.
        # Uses the same per_cid_n2a data already collected in Step 1.
        # Also tracks model_id for the audit table (tie: smallest model_id).
        per_cid_d: Dict[int, float] = defaultdict(float)
        per_cid_d_mid: Dict[int, int] = {}
        for cid in cid_list:
            for mid, n2a in per_cid_n2a.get(cid, []):
                n2a_x = n2a.get('x') or []
                n2a_y = n2a.get('y') or []
                if n2a_x and n2a_y and len(n2a_x) == len(n2a_y):
                    try:
                        max_af = float(max(n2a_y))
                        max_af_idx = n2a_y.index(max_af)
                        noise_at_max = float(n2a_x[max_af_idx])
                        if max_af > 0 and noise_at_max > 0:
                            d_raw = max_af / noise_at_max
                            _update_max_with_model(per_cid_d, per_cid_d_mid, cid, d_raw, mid)
                    except Exception:
                        pass

        now_t = time.time()
        with _cond_denom_lock:
            for pkey in SCORING_PROFILES:
                wa2_ref_map = per_profile_wa2_ref_db[pkey]
                pp_a2 = per_profile_a2[pkey]
                for cid in cid_list:
                    _cond_denom_cache[pkey][cid] = {
                        'low_db':     per_cid_low_db.get(cid),
                        'high_db':    per_cid_high_db.get(cid),
                        'mid_db':     per_cid_mid_db.get(cid),
                        'wa2_ref_db': wa2_ref_map.get(cid),
                        'max_a1':     per_cid_a1.get(cid, 0.0),
                        'max_a2':     pp_a2.get(cid, 0.0),
                        'max_a3':     per_cid_a3.get(cid, 0.0),
                        'max_b':      per_cid_b.get(cid, 0.0),
                        'max_c':      per_cid_c.get(cid, 0.0),
                        'max_d':      per_cid_d.get(cid, 0.0),
                        'cached_at':  now_t,
                    }
        if app.debug:
            app.logger.debug('[denom_refresh] completed for %d conditions × %d profiles',
                             len(cid_list), len(SCORING_PROFILES))

        # Invalidate per-model ABC cache across all profiles so scores are recomputed
        # against the new denominators (including the updated wa2_ref_db per profile).
        with _abc_cache_lock:
            for pkey in SCORING_PROFILES:
                _abc_cache[pkey].clear()

        # Persist per-condition audit rows for the med profile (same as legacy behaviour).
        _persist_denom_audit(
            cid_list,
            per_cid_low_db, per_cid_high_db, per_profile_wa2_ref_db['med'], per_cid_mid_db,
            per_cid_a1, per_cid_a1_mid,
            per_profile_a2['med'], per_profile_a2_mid['med'],
            per_cid_a3, per_cid_a3_mid,
            per_cid_b,  per_cid_b_mid,
            per_cid_wc_raw, per_cid_wc_mid,
            per_cid_d,  per_cid_d_mid,
        )
    except Exception as e:
        app.logger.warning('[denom_refresh] error: %s', e)


def _denom_refresh_loop():
    """Run _do_denom_refresh at startup and then daily at 06:00 CST (UTC+8).

    China Standard Time is UTC+8 with no DST, so a fixed +08:00 offset is correct.
    The initial run at startup populates _cond_denom_cache before any requests arrive.
    This runs in a daemon thread and does not block app startup.
    """
    if app.debug:
        app.logger.debug('[denom_refresh] startup refresh triggered')
    _do_denom_refresh()
    while True:
        try:
            cst = timezone(timedelta(hours=8))
            now = datetime.now(cst)
            target = now.replace(hour=6, minute=0, second=0, microsecond=0)
            if now >= target:
                target = target + timedelta(days=1)
            sleep_secs = (target - now).total_seconds()
        except Exception:
            sleep_secs = 86400
        time.sleep(max(sleep_secs, 1))
        _do_denom_refresh()


if not os.getenv('DISABLE_COND_STATS_CRON'):
    threading.Thread(target=_denom_refresh_loop, daemon=True).start()

# =========================================
# Theme & Config (去除 extra)
# =========================================
@app.route('/api/theme', methods=['POST'])
def api_theme():
    data = request.get_json(force=True, silent=True) or {}
    theme = data.get('theme', 'light')
    session['theme'] = theme
    session.modified = True
    return resp_ok({'theme': theme})


@app.route('/api/config')
def api_config():
    cfg = {
        'click_cooldown_ms': CLICK_COOLDOWN_SECONDS * 1000,
        'recent_likes_limit': RECENT_LIKES_LIMIT,
        'spectrum_dock_enabled': SPECTRUM_DOCK_ENABLED,
        'play_audio_enabled': PLAY_AUDIO_ENABLED,
        'score_profiles': list(SCORING_PROFILES),
        'default_score_profile': _DEFAULT_SCORE_PROFILE,
    }
    return resp_ok(cfg)

@app.route('/source-info')
def source_info():
    return render_template('source-info.html')

@app.route('/legal')
def legal():
    return render_template(
        'legal.html',
        current_year=datetime.now().year,
        update_date='2025-10-08'
    )

# =========================================
# Index
# =========================================
@app.route('/')
def index():
    brands_rows = fetch_all("SELECT DISTINCT brand_name_zh FROM fan_brand WHERE is_valid=1")
    brands = [r['brand_name_zh'] for r in brands_rows]
    conditions_rows = fetch_all("SELECT DISTINCT condition_name_zh FROM working_condition WHERE is_valid=1")
    
    # v2 rankings data (model-centric, with heat_score + composite_score)
    try:
        rankings_v2 = get_rankings_v2()
    except Exception:
        rankings_v2 = {'heat_board': [], 'performance_board': []}

    html_content = render_template(
        'fancoolindex.html',
        brands=brands,
        all_conditions=[r['condition_name_zh'] for r in conditions_rows],
        rankings_v2=rankings_v2,
        size_options=SIZE_OPTIONS,
        current_year=datetime.now().year
    )
    response = make_response(html_content)
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, proxy-revalidate'
    response.headers['Expires'] = '0'
    response.headers['Pragma'] = 'no-cache'
    return response

# 4) 排行榜 API — 支持 score_profile 参数，用于前端切换评分策略后重新拉取
@app.get('/api/rankings_v2')
def api_rankings_v2():
    """Return rankings sorted by the requested score_profile.

    Query param:
      score_profile=low|med|hi  (default: med)

    Each profile has its own 10-minute cache entry so switching profiles is fast
    on subsequent requests.

    Response (HTTP 200):
      { "success": true, "data": {
          "heat_board":        [...],   # sorted by heat_score DESC
          "performance_board": [...],   # sorted by profile composite_score DESC
          "model_lookup":      {...}    # model_id -> item dict
      }}
    Error (HTTP 500):
      { "success": false, "error_code": "INTERNAL_ERROR", "error_message": "..." }
    """
    try:
        profile = (request.args.get('score_profile') or _DEFAULT_SCORE_PROFILE).strip()
        if profile not in SCORING_PROFILES:
            profile = _DEFAULT_SCORE_PROFILE
        data = get_rankings_v2(profile)
        return resp_ok(data)
    except Exception as e:
        app.logger.exception(e)
        return resp_err('INTERNAL_ERROR', str(e), 500)


# 3) 近期更新 API（懒加载页签调用）
@app.route('/api/recent_updates', methods=['GET'])
def api_recent_updates():
    try:
        items = get_recent_updates(limit=RECENT_UPDATES_LIMIT)
        # 直接返回标准结构，前端用 normalizeApiResponse 解析
        return resp_ok({'items': items})
    except Exception as e:
        app.logger.exception(e)
        return resp_err('INTERNAL_ERROR', str(e), 500)

@app.post('/api/spectrum-models')
def api_spectrum_models():
    """
    用户频谱模型接口：只依赖 spectrum_cache + audio_calib_job，不直接跑 pipeline。
      - 缓存命中且 meta 一致：返回瘦身 model；
      - 无绑定：missing；
      - 有绑定但无有效模型：创建标定任务（purpose='autofix'）提交后台 worker，当前请求标记 rebuilding。

    新版：
      - 默认 param_hash 仍来自 audio_calibration_params.is_default=1；
      - 若 audio_perf_binding.params_json 存在，则优先使用该 params：
          * param_hash 基于 binding.params_json 计算；
          * spectrum_cache / audio_spectrum_model / audio_calib_job 一致性都基于该 hash；
      - 这样不同 perf_batch 可以绑定各自参数，同时 spectrum 模型仍由 (mid,cid) 缓存。
    """
    try:
        data = request.get_json(force=True, silent=True) or {}
        raw_pairs = data.get('pairs') or []
        uniq, seen = [], set()
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
            uniq.append(t)
        if not uniq:
            return resp_ok({'models': [], 'missing': [], 'rebuilding': []})

        # 读取默认参数（用于无绑定自定义参数时的兜底）
        with engine.begin() as conn:
            row = conn.execute(text("""
                SELECT params_json
                FROM audio_calibration_params
                WHERE is_default = 1
                ORDER BY updated_at DESC
                LIMIT 1
            """)).fetchone()
        if not row:
            return resp_err('NO_DEFAULT_PARAMS', '未配置默认标定参数', 500)

        mp = row._mapping
        params_json = mp.get('params_json')
        if isinstance(params_json, str):
            default_params = json.loads(params_json)
        else:
            default_params = params_json or {}

        code_ver = CODE_VERSION or ''

        models, missing, rebuilding = [], [], []

        for mid, cid in uniq:
            # 1) 尝试读取绑定，拿 audio_batch_id + 可能存在的 params_json
            with engine.begin() as conn:
                row_bind = conn.execute(text("""
                    SELECT audio_batch_id, perf_batch_id, params_json
                    FROM audio_perf_binding
                    WHERE model_id=:m AND condition_id=:c
                    ORDER BY created_at DESC
                    LIMIT 1
                """), {'m': mid, 'c': cid}).fetchone()
                binding = row_bind._mapping if row_bind else None

            if not binding:
                slog.info(
                    "[/api/spectrum-models] pair=(%s,%s) no binding → missing(no_audio_bound)",
                    mid, cid
                )
                missing.append({'model_id': mid, 'condition_id': cid, 'reason': 'no_audio_bound'})
                continue

            audio_batch_id = (binding.get('audio_batch_id') or '').strip()
            if not audio_batch_id:
                missing.append({'model_id': mid, 'condition_id': cid, 'reason': 'no_audio_batch_id'})
                continue

            # 1.1) 解析绑定里的 params_json（若有）
            raw_bind_params = binding.get('params_json')
            bind_params = None
            if isinstance(raw_bind_params, str) and raw_bind_params.strip():
                try:
                    bind_params = json.loads(raw_bind_params)
                except Exception:
                    bind_params = None
            elif isinstance(raw_bind_params, dict):
                bind_params = raw_bind_params

            # 1.2) 决定本对 (mid,cid) 所使用的参数
            params_for_pair = bind_params if isinstance(bind_params, dict) and bind_params else default_params
            try:
                param_hash = hashlib.sha1(
                    json.dumps(params_for_pair, sort_keys=True, separators=(',', ':')).encode('utf-8')
                ).hexdigest()
            except Exception as e:
                return resp_err('PARAM_HASH_FAIL', f'参数哈希计算失败: {e}', 500)

            # 2) 先尝试读取缓存
            j = spectrum_cache.load(mid, cid)
            cur_meta = (j.get('meta') if isinstance(j, dict) else {}) or {}
            cur_model_raw = (j.get('model') if isinstance(j, dict) else {}) or {}
            slog.info(
                "[/api/spectrum-models] pair=(%s,%s) cache_exists=%s",
                mid, cid, bool(j)
            )

            # 3) 一致性校验：仅依赖 cache meta 的 param_hash / code_version / audio_data_hash 是否存在
            cached_ok = False
            if cur_meta:
                meta_param = str(cur_meta.get('param_hash') or '')
                meta_code = str(cur_meta.get('code_version') or '')
                meta_audio = str(cur_meta.get('audio_data_hash') or '')
                cached_ok = (
                    meta_param == param_hash and
                    meta_code == code_ver and
                    bool(meta_audio) and
                    bool(cur_model_raw)
                )
                slog.info(
                    "  check cache: meta_param=%s cur_param=%s meta_code=%s cur_code=%s meta_audio=%s -> ok=%s",
                    meta_param, param_hash, meta_code, code_ver, meta_audio, cached_ok
                )

            if cached_ok:
                # 4) 缓存一致，返回瘦身后的模型
                m = cur_model_raw
                calib = m.get('calibration') or {}
                calib_model = calib.get('calib_model') or {}
                
                # Check if model has audio capability before stripping sensitive keys
                # supports_audio is true when sweep_frame_index and sweep_audio_meta exist
                supports_audio = sweep_audio_player.validate_model_has_frame_index(m)
                
                slim = {
                    'version': m.get('version'),
                    'centers_hz': m.get('centers_hz') or m.get('freq_hz') or m.get('freq') or [],
                    'band_models_pchip': m.get('band_models_pchip') or [],
                    'rpm_min': m.get('rpm_min') or calib_model.get('x0'),
                    'rpm_max': m.get('rpm_max') or calib_model.get('x1'),
                    'calibration': {
                        'rpm_peak': calib.get('rpm_peak'),
                        'rpm_peak_tol': calib.get('rpm_peak_tol'),
                        'session_delta_db': calib.get('session_delta_db'),
                    },
                    'anchor_presence': m.get('anchor_presence') or {},
                    'supports_audio': supports_audio  # Audio generation support flag
                    # Note: sweep_frame_index and sweep_audio_meta are intentionally excluded for security
                }
                models.append({
                    'key': f'{mid}_{cid}',
                    'model_id': mid,
                    'condition_id': cid,
                    'model': slim,
                    'type': j.get('type') or 'spectrum_v2'
                })
                continue

            # 5) 缓存不满足要求 → 调用 admin 内部 autofix API 请求重建
            try:
                result = call_admin_autofix_api(
                    audio_batch_id=audio_batch_id,
                    model_id=mid,
                    condition_id=cid,
                    params=params_for_pair,
                    param_hash=param_hash
                )
                
                if result:
                    # Successfully requested rebuild from admin
                    job_id = result.get('job_id')
                    if job_id:
                        rebuilding.append({
                            'model_id': mid,
                            'condition_id': cid,
                            'job_id': int(job_id)
                        })
                    else:
                        # No job_id returned, but call succeeded
                        rebuilding.append({'model_id': mid, 'condition_id': cid})
                else:
                    # Failed to call admin API, still mark as rebuilding but without job_id
                    slog.warning("  call_admin_autofix_api failed for pair (%s, %s)", mid, cid)
                    rebuilding.append({'model_id': mid, 'condition_id': cid})
                    
            except Exception as e:
                slog.exception("  call_admin_autofix_api exception: %s", e)
                rebuilding.append({'model_id': mid, 'condition_id': cid})

        return resp_ok({'models': models, 'missing': missing, 'rebuilding': rebuilding})
    except Exception as e:
        app.logger.exception(e)
        return resp_err('INTERNAL_ERROR', f'频谱模型接口异常: {e}', 500)

def refresh_announcement_cache():
    """
    每 60 秒轮询一次公告表，缓存当前可展示的公告以及即将生效的预定公告。
    同时计算指纹（fingerprint），用于前端检测公告状态变化，包括 starts_at 到期自动生效。
    """
    global announcement_cache, _announcement_state
    while True:
        try:
            # Currently active announcements
            active_rows = fetch_all("""
              SELECT id, content_text, starts_at, ends_at
              FROM announcements
              WHERE is_valid=1
                AND starts_at <= NOW()
                AND NOW() < IFNULL(ends_at, '9999-12-31')
              ORDER BY priority DESC, created_at DESC, id ASC
              LIMIT 20
            """)
            active_items = active_rows or []

            # Upcoming scheduled announcements (starts_at > NOW())
            upcoming_rows = fetch_all("""
              SELECT id, content_text, starts_at, ends_at
              FROM announcements
              WHERE is_valid=1
                AND starts_at > NOW()
              ORDER BY starts_at ASC
              LIMIT 10
            """)
            upcoming_items = upcoming_rows or []

            # Derive next_change_at: the earliest future time the announcement state changes
            # Could be: an active announcement's ends_at, or an upcoming announcement's starts_at
            next_change_at: datetime | None = None
            for row in active_items:
                ends_at = row.get('ends_at')
                if ends_at:
                    dt = ends_at if isinstance(ends_at, datetime) else None
                    if dt is None:
                        try: dt = datetime.fromisoformat(str(ends_at))
                        except Exception: pass
                    if dt and (next_change_at is None or dt < next_change_at):
                        next_change_at = dt
            for row in upcoming_items:
                starts_at = row.get('starts_at')
                if starts_at:
                    dt = starts_at if isinstance(starts_at, datetime) else None
                    if dt is None:
                        try: dt = datetime.fromisoformat(str(starts_at))
                        except Exception: pass
                    if dt and (next_change_at is None or dt < next_change_at):
                        next_change_at = dt

            # Fingerprint derived from effective visible state (active IDs+content + upcoming IDs+starts_at)
            # This changes when: new row inserted, row expires, future row becomes active, ordering changes
            active_sig = '|'.join(
                f"{r['id']}:{r['content_text']}"
                for r in active_items
            )
            upcoming_sig = '|'.join(
                f"{r['id']}:{r.get('starts_at', '')}"
                for r in upcoming_items
            )
            fp_raw = active_sig + '||' + upcoming_sig
            fingerprint = hashlib.md5(fp_raw.encode('utf-8')).hexdigest()[:16]

            # Prepare serializable active/upcoming lists (convert datetime to str)
            def _serialize_rows(rows):
                out = []
                for r in rows:
                    item = {'id': r['id'], 'content_text': r['content_text']}
                    if r.get('starts_at') is not None:
                        item['starts_at'] = str(r['starts_at'])
                    if r.get('ends_at') is not None:
                        item['ends_at'] = str(r['ends_at'])
                    out.append(item)
                return out

            _announcement_state = {
                'active': _serialize_rows(active_items),
                'upcoming': _serialize_rows(upcoming_items),
                'fingerprint': fingerprint,
                'next_change_at': next_change_at.isoformat() if next_change_at else None,
            }
            # Keep legacy cache for backward compat
            announcement_cache = _serialize_rows(active_items)
        except Exception as e:
            app.logger.warning("更新公告缓存失败: %s", e)
        time.sleep(60)


def get_announcement_meta() -> dict:
    """Return lightweight announcement fingerprint metadata for piggyback inclusion in API responses."""
    return {
        'fingerprint': _announcement_state.get('fingerprint', ''),
        'next_change_at': _announcement_state.get('next_change_at'),
    }

threading.Thread(target=refresh_announcement_cache, daemon=True).start()

@app.get('/api/announcement')
def api_announcement():
    """
    返回完整公告状态，供前端在指纹变化时刷新用。
      active: 当前可展示的公告列表
      upcoming: 预定未来生效的公告列表（含 starts_at）
      fingerprint: 有效公告状态的指纹，前端用于检测变化
      next_change_at: 下次状态预期变化时间（用于前端精准定时重取）
      item / items: 兼容旧字段
    """
    state = _announcement_state
    active = state.get('active') or []
    upcoming = state.get('upcoming') or []
    primary = active[0] if active else None
    return resp_ok({
        'items': active,
        'item': primary,
        'active': active,
        'upcoming': upcoming,
        'fingerprint': state.get('fingerprint', ''),
        'next_change_at': state.get('next_change_at'),
    })


# =========================================
# Sweep Audio Playback API
# =========================================
@app.post('/api/sweep-audio')
def api_sweep_audio():
    try:
        from app.audio_services import sweep_audio_player
        
        data = request.get_json(force=True, silent=True) or {}
        
        # 解析参数 / Parse parameters
        try:
            model_id = int(data.get('model_id') or 0)
            condition_id = int(data.get('condition_id') or 0)
            target_rpm = float(data.get('target_rpm') or 0)
        except (ValueError, TypeError) as e:
            return resp_err('INVALID_INPUT', f'参数格式错误: {e}', 400)
        
        # 验证参数 / Validate parameters
        if model_id <= 0 or condition_id <= 0:
            return resp_err('INVALID_INPUT', '缺少 model_id 或 condition_id', 400)
        if target_rpm <= 0:
            return resp_err('INVALID_INPUT', f'target_rpm 必须为正数: {target_rpm}', 400)
        
        # 1) 从缓存加载模型
        cached_model = spectrum_cache.load(model_id, condition_id)
        if not cached_model or not isinstance(cached_model, dict):
            return resp_err('MODEL_NOT_FOUND', f'未找到 model_id={model_id}, condition_id={condition_id} 的缓存模型', 404)
        
        model_json = cached_model.get('model')
        if not model_json or not isinstance(model_json, dict):
            return resp_err('MODEL_INVALID', '缓存模型数据无效', 500)
        
        # 2) 验证是否包含 sweep_frame_index
        if not sweep_audio_player.validate_model_has_frame_index(model_json):
            return resp_err('SWEEP_INDEX_MISSING', '模型中缺少 sweep_frame_index 或数据无效', 404)
        
        app.logger.info('[sweep-audio] Using sweep_frame_index for model_id=%s, condition_id=%s, target_rpm=%s', 
                       model_id, condition_id, target_rpm)
        
        # 3) 生成音频（duration_sec 已废弃，固定使用内部 TARGET_DURATION_SEC）
        try:
            wav_bytes, metadata = sweep_audio_player.generate_sweep_audio(
                model_json, target_rpm, duration_sec=None
            )
        except ValueError as e:
            return resp_err('AUDIO_GENERATION_FAILED', f'音频生成失败: {e}', 400)
        except Exception as e:
            app.logger.exception('sweep_audio_player.generate_sweep_audio error: %s', e)
            return resp_err('AUDIO_GENERATION_ERROR', f'音频生成异常: {e}', 500)
        
        # 4) 返回音频
        response = make_response(wav_bytes)
        response.headers['Content-Type'] = 'audio/wav'
        response.headers['Content-Disposition'] = f'attachment; filename="sweep_audio_rpm{int(target_rpm)}.wav"'
        
        # 元数据
        response.headers['X-Target-RPM'] = str(metadata.get('target_rpm'))
        response.headers['X-Duration-Sec'] = str(metadata.get('duration_sec'))
        response.headers['X-Sample-Rate'] = str(metadata.get('sample_rate'))
        
        return response
        
    except Exception as e:
        app.logger.exception('api_sweep_audio error: %s', e)
        return resp_err('INTERNAL_ERROR', f'接口异常: {e}', 500)
    
# =========================================
# Entrypoint
# =========================================
if __name__ == '__main__':
    app.logger.setLevel(logging.WARNING)
    app.run(host='0.0.0.0', port=5001, debug=False, use_reloader=False)