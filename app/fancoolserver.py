import os
import logging
import time
import threading
import math
import signal
import json
import hashlib
import hmac
import urllib.parse
import requests  # Added for HTTP client
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Tuple, Any

from flask import Flask, request, render_template, session, jsonify, make_response, send_from_directory, Response, abort
from sqlalchemy import create_engine, text
from sqlalchemy import exc as sa_exc
from werkzeug.middleware.proxy_fix import ProxyFix

from app.curves import pchip_cache
from app.curves.pchip_cache import eval_pchip
from app.curves import perf_model_service
from app.audio_services import spectrum_cache
from app.audio_services import spectrum_reader
from app.audio_services import sweep_audio_player
from app.common_utils import (
    sign_uid, unsign_uid, make_success_response, make_error_response,
    db_fetch_all, db_exec_write
)
from app.asset_manifest import ManifestLoader, create_asset_url_helper

CODE_VERSION = os.getenv('CODE_VERSION', '')
from app import scoring_system, user_activity

SCORE_CONDITION_IDS = scoring_system.SCORE_CONDITION_IDS
COMPOSITE_WEIGHTS = scoring_system.COMPOSITE_WEIGHTS
_cond_denom_cache = scoring_system._cond_denom_cache
_cond_denom_lock = scoring_system._cond_denom_lock
_RADAR_CIDS = scoring_system._RADAR_CIDS
get_canonical_facts = scoring_system.get_canonical_facts
get_rankings_v2 = scoring_system.get_rankings_v2
get_recent_updates = scoring_system.get_recent_updates
refresh_visibility_scoring_caches = scoring_system.refresh_visibility_scoring_caches
_get_canonical_model_score = scoring_system._get_canonical_model_score
_sync_compute_and_cache = scoring_system._sync_compute_and_cache
_trigger_model_score_soft_refresh = scoring_system._trigger_model_score_soft_refresh
_get_ranked_lookup_with_fallback = scoring_system._get_ranked_lookup_with_fallback
_warmup_rankings_async = scoring_system._warmup_rankings_async
_build_condition_weights = scoring_system._build_condition_weights
BASELINE_VALID_LOWER_MIN_COUNT = scoring_system.BASELINE_VALID_LOWER_MIN_COUNT
BASELINE_VALID_LOWER_MIN_RATIO = scoring_system.BASELINE_VALID_LOWER_MIN_RATIO
BASELINE_VALID_UPPER_MIN_COUNT = scoring_system.BASELINE_VALID_UPPER_MIN_COUNT
BASELINE_VALID_UPPER_MIN_RATIO = scoring_system.BASELINE_VALID_UPPER_MIN_RATIO
BASELINE_DB_STEP = scoring_system.BASELINE_DB_STEP
RAW_SCORE_AIRFLOW_SAMPLE_COUNT = scoring_system.RAW_SCORE_AIRFLOW_SAMPLE_COUNT
RAW_SCORE_ADAPTIVE_WEIGHTS = scoring_system.RAW_SCORE_ADAPTIVE_WEIGHTS
MODEL_SCORE_CACHE_SOFT_TTL_SEC = scoring_system.MODEL_SCORE_CACHE_SOFT_TTL_SEC
MODEL_SCORE_CACHE_HARD_TTL_SEC = scoring_system.MODEL_SCORE_CACHE_HARD_TTL_SEC
from app.user_activity import bp as user_activity_bp

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

# Serve webpack dist assets directly when running without nginx.
@app.route('/static/app-dist/<path:filename>')
def serve_app_dist(filename):
    """Serve app/static/dist assets during local development."""
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
# Inject engine into modules that need database access.
# spectrum_reader.set_engine() forwards to perf_model_service which forwards
# to perf_raw_source, so a single call wires up the full performance-curve stack.
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


def _require_internal_warmup_token():
    expected_token = (os.getenv('INTERNAL_WARMUP_TOKEN') or '').strip()
    if not expected_token:
        return None
    supplied = request.headers.get('X-Warmup-Token', '').strip()
    if not hmac.compare_digest(supplied, expected_token):
        return resp_err('UNAUTHORIZED', 'Invalid or missing X-Warmup-Token', 401)
    return None




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
# Module initialization
#
# scoring_system and user_activity are extracted modules.  Both require
# fetch_all / exec_write to be defined before they can operate.  Blueprint
# registration must happen before the first request is served.
# =========================================
scoring_system.setup(fetch_all, exec_write, app.logger, app.debug)
user_activity.setup(fetch_all, exec_write, engine, app.logger)
app.register_blueprint(user_activity_bp)
scoring_system.start_background_threads()


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
    # 统一模型（含缓存/TTL/失效/重建）——原始点校验职责由服务层统一管理
    unified = perf_model_service.get_perf_model(model_id, condition_id) or {}
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
        where.append("g.reference_price BETWEEN :pmin AND :pmax")
        params.update(pmin=int(price_min), pmax=int(price_max))

    # RGB灯光
    if rgb_light and rgb_light != '不限':
        where.append("g.rgb_light=:rgb")
        params['rgb'] = rgb_light

    # 最大转速（RPM）
    if max_speed_min is not None and max_speed_max is not None:
        where.append("g.max_speed BETWEEN :msmin AND :msmax")
        params.update(msmin=int(max_speed_min), msmax=int(max_speed_max))

    sql = f"""
      SELECT g.model_id, g.condition_id,
             g.brand_name_zh, g.model_name, g.condition_name_zh,
             g.size, g.thickness, g.rpm, g.noise_db, g.airflow_cfm AS airflow,
             COALESCE(g.like_count,0) AS like_count,
             g.reference_price,
             g.max_speed,
             COALESCE(g.rgb_light, '') AS rgb_light
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

    items.sort(key=lambda r: (r['effective_airflow'] if r['effective_airflow'] is not None else -1e9, r['model_id']), reverse=True)
    return items[:limit]

def search_fans_composite(size_filter=None, thickness_min=None, thickness_max=None,
                           price_min=None, price_max=None, rgb_light=None,
                           max_speed_min=None, max_speed_max=None,
                           limit=200) -> list[dict]:
    """
    Search fans in 综合评分 (composite score) mode.
    No condition filter — returns one row per distinct model, enriched with
    composite_score, condition_scores, and condition_likes by the caller.
    Queries available_models_info_view directly (lighter than general_view)
    since no per-measurement data (rpm/noise/airflow) is needed here.
    """
    where = []
    params: dict = {}

    if size_filter and size_filter != '不限':
        where.append("g.size=:sz"); params['sz'] = int(size_filter)
    if thickness_min is not None and thickness_max is not None:
        where.append("g.thickness BETWEEN :tmin AND :tmax")
        params.update(tmin=int(thickness_min), tmax=int(thickness_max))
    if price_min is not None and price_max is not None:
        where.append("g.reference_price BETWEEN :pmin AND :pmax")
        params.update(pmin=int(price_min), pmax=int(price_max))
    if rgb_light and rgb_light != '不限':
        where.append("g.rgb_light=:rgb")
        params['rgb'] = rgb_light
    if max_speed_min is not None and max_speed_max is not None:
        where.append("g.max_speed BETWEEN :msmin AND :msmax")
        params.update(msmin=int(max_speed_min), msmax=int(max_speed_max))

    sql = f"""
      SELECT g.model_id, g.brand_name_zh, g.model_name, g.size, g.thickness,
             g.max_speed, g.reference_price,
             COALESCE(g.rgb_light, '') AS rgb_light
      FROM available_models_info_view g
      {"WHERE " + " AND ".join(where) if where else ""}
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


def _safe_finite_float(value):
    try:
        num = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return num if math.isfinite(num) else None


def _build_baseline_overlays_for_conditions(
    condition_ids: List[int],
) -> Dict[str, dict]:
    """Return per-condition baseline overlay metadata (formula + valid dB interval only)."""
    wanted = {int(cid) for cid in (condition_ids or [])}
    if not wanted:
        return {}

    with _cond_denom_lock:
        snapshot = {cid: dict(_cond_denom_cache.get(cid) or {}) for cid in wanted}

    overlays: Dict[str, dict] = {}
    for cid in sorted(wanted):
        entry = snapshot.get(cid) or {}
        fit = entry.get('baseline_fit') if isinstance(entry.get('baseline_fit'), dict) else {}
        intercept = _safe_finite_float(fit.get('intercept'))
        slope = _safe_finite_float(fit.get('slope'))
        valid_db_min = _safe_finite_float(entry.get('valid_db_min'))
        valid_db_max = _safe_finite_float(entry.get('valid_db_max'))

        available = (
            intercept is not None
            and slope is not None
            and valid_db_min is not None
            and valid_db_max is not None
            and valid_db_max > valid_db_min
        )

        overlays[str(cid)] = {
            'available': bool(available),
            'fit_type': 'exp_ln' if available else None,
            'parameters': {
                'intercept': intercept,
                'slope': slope,
            } if available else None,
            'valid_db_min': valid_db_min,
            'valid_db_max': valid_db_max,
            'target_chart': 'airflow_vs_db_main',
            'interactive': False,
            'exclude_from_fit_panel': True,
        }
    return overlays


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
      baseline_overlays: {
        "<condition_id>": {
          available, fit_type, parameters, valid_db_min, valid_db_max,
          target_chart, interactive, exclude_from_fit_panel
        }, ...
      }
    说明：
      - 不再返回顶层 rpm/noise_db/airflow，也不使用 -1 作为占位。
      - data.* 数组中允许出现 None（例如缺失的噪音或转速），前端会在渲染前清洗。
      - baseline_overlays 仅返回基线公式参数与有效评分区间，不返回采样点。
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
          return resp_ok({'series': [], 'missing': [], 'baseline_overlays': {}})

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

      baseline_overlays = _build_baseline_overlays_for_conditions(
          [cid for _, cid in uniq],
      )

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

      return resp_ok({'series': series, 'missing': missing, 'baseline_overlays': baseline_overlays})
    except Exception as e:
      app.logger.exception(e)
      return resp_err('INTERNAL_ERROR', f'后端异常: {e}', 500)


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
        "SELECT DISTINCT brand_name_zh, model_name FROM available_models_info_view WHERE model_name LIKE :q LIMIT 20",
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
            "FROM meta_view WHERE brand_name_zh=:b AND model_name=:m "
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

        # Enrich results with model-score cache data (composite_score, condition_scores, condition_score)
        # and condition heat / like / query counts from the canonical shared facts cache.
        RADAR_CIDS_LIST = _RADAR_CIDS  # shared constant; synced with right-panel-v2.js

        # Heat data comes directly from the canonical facts cache (profile-independent).
        # This avoids borrowing from rankings cache and makes search resilient to
        # cold-start windows where rankings/model-score caches are still warming.
        _cf = get_canonical_facts()
        facts_lookup: dict = _cf.get('model_lookup') or {}
        for item in results:
            mid = item['model_id']
            facts = facts_lookup.get(mid)

            # Heat / like / query counts from canonical facts (profile-independent)
            if facts:
                heat_score      = facts.get('heat_score', 0)
                condition_heat  = facts.get('condition_heat',  {cid: 0 for cid in RADAR_CIDS_LIST})
                condition_likes = facts.get('condition_likes', {cid: 0 for cid in RADAR_CIDS_LIST})
                total_queries   = facts.get('query_count', 0)
                total_likes     = facts.get('like_count',  0)
            else:
                heat_score      = 0
                condition_heat  = {cid: 0 for cid in RADAR_CIDS_LIST}
                condition_likes = {cid: 0 for cid in RADAR_CIDS_LIST}
                total_queries   = 0
                total_likes     = 0

            # Score data from canonical model-score cache.
            model_score_entry = _get_canonical_model_score(mid)
            composite_score = None
            condition_scores: dict = {}
            item_condition_score = None
            if model_score_entry:
                composite_score = model_score_entry.get('composite_score')
                cond_scores = model_score_entry.get('conditions') or {}
                for cid_r in RADAR_CIDS_LIST:
                    cd = cond_scores.get(cid_r)
                    raw = cd.get('score_total') if cd else None
                    try:
                        condition_scores[cid_r] = int(raw) if raw is not None else None
                    except (TypeError, ValueError):
                        condition_scores[cid_r] = None
                # Score for the searched condition (not used in composite mode)
                s_cid = searched_cid if searched_cid is not None else item.get('condition_id')
                if s_cid is not None:
                    cd_s = cond_scores.get(s_cid)
                    raw_s = cd_s.get('score_total') if cd_s else None
                    try:
                        item_condition_score = int(raw_s) if raw_s is not None else None
                    except (TypeError, ValueError):
                        item_condition_score = None
            item['composite_score'] = int(composite_score) if composite_score is not None else None
            item['condition_scores'] = condition_scores
            item['condition_likes'] = condition_likes
            item['condition_score'] = item_condition_score
            item['condition_heat'] = condition_heat
            item['heat_score'] = heat_score
            item['query_count'] = total_queries
            item['like_count'] = total_likes

        scoring_system.queue_missing_score_visibility_sync_for_items(
            results,
            trigger_source='search_fans',
        )

        # Re-sort by condition_score when requested (None treated as -1)
        if not composite_mode and sort_by == 'condition_score':
            results.sort(
                key=lambda x: (x['condition_score'] if x['condition_score'] is not None else -1, x['model_id']),
                reverse=True
            )

        # Composite mode: sort by composite_score DESC
        if composite_mode:
            results.sort(
                key=lambda x: scoring_system.composite_score_sort_key(
                    x.get('composite_score'),
                    x.get('model_id', 0),
                )
            )

        return resp_ok({'search_results': results, 'condition_label': label})
    except Exception as e:
        app.logger.exception(e)
        return resp_err('INTERNAL_ERROR', f'搜索异常: {e}', 500)

@app.get('/api/brands')
def api_brands():
    try:
        rows = fetch_all("""
            SELECT
                brand_id,
                brand_name_zh,
                brand_name_en,
                COUNT(DISTINCT model_id) AS model_count
            FROM available_models_info_view
            GROUP BY brand_id, brand_name_zh, brand_name_en
            ORDER BY brand_name_zh, brand_name_en, brand_id
        """)
        items = []
        for row in rows:
            item = dict(row)
            item['brand_name'] = item.get('brand_name_zh')  # backward compatibility
            raw_count = item.get('model_count')
            try:
                item['model_count'] = int(raw_count or 0)
            except (TypeError, ValueError):
                app.logger.warning("Invalid model_count for brand_id=%s: %r", item.get('brand_id'), raw_count)
                item['model_count'] = 0
            items.append(item)
        return resp_ok({'items': items})
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
            cache_entry = _get_canonical_model_score(mid)
            if cache_entry:
                item['radar'] = {
                    'conditions': cache_entry['conditions'],
                    'composite_score': cache_entry.get('composite_score'),
                    'updated_at': cache_entry['updated_at'],
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
            "resistance_location_zh FROM meta_view WHERE model_id=:m ORDER BY condition_name_zh",
            {'m': mid}
        )
        # Attach model scores from in-memory cache (soft/hard TTL, no DB table dependency)
        cache_entry = _get_canonical_model_score(mid)
        scores: Dict[int, dict] = cache_entry['conditions'] if cache_entry else {}
        items = []
        for r in rows:
            cid = int(r['condition_id'])
            item = dict(r)
            sc = scores.get(cid)
            item['score_total'] = sc['score_total'] if sc else None
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

    Returns model-score metrics for multiple model_ids in one call.
    Uses the in-memory model-score cache
    with soft/hard TTL logic.

    Response shape:
      {
        "models": {
          "<model_id>": {
            "conditions": { "<cid>": {"score_total": 0, "curve_raw_score": 0.0, "curve_valid_points": 0} },
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
        visible_displayable_model_ids: set[int] = set()
        if model_ids:
            placeholders = ','.join(f':m{i}' for i in range(len(model_ids)))
            params = {f'm{i}': mid for i, mid in enumerate(model_ids)}
            visible_rows = fetch_all(
                f"""
                    SELECT model_id, brand_name_zh, model_name
                    FROM available_models_info_view
                    WHERE model_id IN ({placeholders})
                """,
                params,
            )
            for row in visible_rows or []:
                try:
                    mid = int(row['model_id'])
                except (TypeError, ValueError, KeyError):
                    continue
                brand = str(row.get('brand_name_zh') or '').strip()
                model = str(row.get('model_name') or '').strip()
                if brand or model:
                    visible_displayable_model_ids.add(mid)

        now = time.time()
        for model_id in model_ids:
            entry = _get_canonical_model_score(model_id)

            if entry is not None:
                age = now - entry['cached_at']
                if age < MODEL_SCORE_CACHE_SOFT_TTL_SEC:
                    # Fresh: return as-is
                    pass
                elif age < MODEL_SCORE_CACHE_HARD_TTL_SEC:
                    # Soft-expired: return stale + trigger async refresh
                    soft_refreshed.append(model_id)
                    _trigger_model_score_soft_refresh(model_id)
                else:
                    # Hard-expired: synchronous recompute
                    hard_refreshed.append(model_id)
                    _sync_compute_and_cache(model_id)
                    entry = _get_canonical_model_score(model_id)
                if entry:
                    models_out[str(model_id)] = {
                        'conditions': entry['conditions'],
                        'composite_score': entry.get('composite_score'),
                        'updated_at': entry['updated_at'],
                    }
                else:
                    if model_id in visible_displayable_model_ids:
                        scoring_system.queue_visibility_sync_hint(
                            model_id,
                            trigger_source='radar_metrics:cache_miss',
                        )
                    else:
                        app.logger.info(
                            "Skipping radar_metrics visibility-sync hint for model_id=%s: "
                            "model is not currently visible/displayable.",
                            model_id,
                        )
            else:
                # Missing: synchronous compute
                hard_refreshed.append(model_id)
                _sync_compute_and_cache(model_id)
                entry = _get_canonical_model_score(model_id)
                if entry:
                    models_out[str(model_id)] = {
                        'conditions': entry['conditions'],
                        'composite_score': entry.get('composite_score'),
                        'updated_at': entry['updated_at'],
                    }
                else:
                    if model_id in visible_displayable_model_ids:
                        scoring_system.queue_visibility_sync_hint(
                            model_id,
                            trigger_source='radar_metrics:missing_after_sync',
                        )
                    else:
                        app.logger.info(
                            "Skipping radar_metrics visibility-sync hint for model_id=%s: "
                            "model remained unavailable after sync recompute and "
                            "has not been verified as visible/displayable.",
                            model_id,
                        )

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
    Internal endpoint to rebuild PCHIP and warm the model-score cache for a
    (model_id, condition_id) pair after admin data import.
    Runs the work in a background thread so the response is instant.
    """
    try:
        auth_err = _require_internal_warmup_token()
        if auth_err is not None:
            return auth_err
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
            # Recompute canonical model scores for this model and refresh in-memory cache.
            try:
                _sync_compute_and_cache(model_id)
            except Exception as e:
                app.logger.warning('[warm_scores] model_score recompute (%s): %s', model_id, e)

        threading.Thread(target=_warm, daemon=True).start()
        return resp_ok({'queued': True})
    except Exception as e:
        app.logger.exception(e)
        return resp_err('INTERNAL_ERROR', str(e), 500)


@app.post('/api/internal/refresh_scoring_visibility')
def api_internal_refresh_scoring_visibility():
    """Refresh scoring caches affected by model visibility transitions."""
    try:
        auth_err = _require_internal_warmup_token()
        if auth_err is not None:
            return auth_err
        data = request.get_json(force=True, silent=True) or {}
        model_id = int(data.get('model_id') or 0)
        old_is_valid = data.get('old_is_valid')
        new_is_valid = data.get('new_is_valid')

        def _refresh():
            try:
                refresh_visibility_scoring_caches()
                app.logger.info(
                    '[refresh_scoring_visibility] refreshed caches for model_id=%s (%s -> %s)',
                    model_id or 'unknown',
                    old_is_valid,
                    new_is_valid,
                )
            except Exception as e:
                app.logger.warning(
                    '[refresh_scoring_visibility] refresh failed for model_id=%s (%s -> %s): %s',
                    model_id or 'unknown',
                    old_is_valid,
                    new_is_valid,
                    e,
                )

        threading.Thread(target=_refresh, daemon=True, name='visibility-refresh').start()
        return resp_ok({'queued': True})
    except Exception as e:
        app.logger.exception(e)
        return resp_err('INTERNAL_ERROR', str(e), 500)


# =========================================
# Internal: full cache warm-up (for post-deploy multi-worker startup)
# =========================================
@app.post('/api/internal/warmup')
def api_internal_warmup():
    """
    Internal endpoint to proactively warm all critical caches after deployment.

    Triggers in a background thread:
      1. rankings_v2 for canonical scoring profile
      2. model-score cache flush to shared disk

    Intended for use in deployment scripts / Gunicorn post-start hooks to
    reduce the cold-start window where multi-worker environments return
    incomplete heat data or empty radar charts.

    Example usage (run once after systemctl start fancoolweb):

        curl -s -X POST http://localhost:5000/api/internal/warmup

    Returns immediately; actual warm-up runs in the background.
    Concurrent requests are coalesced: if a warm-up is already in progress
    the new request is acknowledged but no additional thread is spawned.

    Security: if the env var INTERNAL_WARMUP_TOKEN is set, callers must
    supply it in the ``X-Warmup-Token`` request header.  Without the env
    var the endpoint is unprotected — only deploy behind a firewall or
    restrict with Nginx/systemd socket activation to loopback.
    """
    try:
        auth_err = _require_internal_warmup_token()
        if auth_err is not None:
            return auth_err
        _warmup_rankings_async()
        return resp_ok({'status': 'warming_up'})
    except Exception as e:
        app.logger.exception(e)
        return resp_err('INTERNAL_ERROR', str(e), 500)


# =========================================
# Score rule explanation (read-only public API)
# =========================================


@app.get('/api/score_rule_explain')
def api_score_rule_explain():
    """Return a concise explanation of the current scoring algorithm."""
    try:
        lower_min_count = int(BASELINE_VALID_LOWER_MIN_COUNT)
        lower_min_ratio = float(BASELINE_VALID_LOWER_MIN_RATIO)
        upper_min_count = int(BASELINE_VALID_UPPER_MIN_COUNT)
        upper_min_ratio = float(BASELINE_VALID_UPPER_MIN_RATIO)
        baseline_step = float(BASELINE_DB_STEP)
        airflow_samples = int(RAW_SCORE_AIRFLOW_SAMPLE_COUNT)
        adaptive_weights = bool(RAW_SCORE_ADAPTIVE_WEIGHTS)
        cid_list: list[int] = []
        for cid in _RADAR_CIDS:
            try:
                c = int(cid)
            except Exception:
                continue
            if c > 0 and c not in cid_list:
                cid_list.append(c)
        weight_pct_map = {
            int(e.get('condition_id')): float(e.get('weight_pct') or 0.0)
            for e in _build_condition_weights(cid_list)
            if e.get('condition_id') is not None
        }

        cid_name_map: dict[int, str] = {}
        if cid_list:
            placeholders = ','.join(f':c{i}' for i in range(len(cid_list)))
            params = {f'c{i}': cid for i, cid in enumerate(cid_list)}
            rows = fetch_all(
                f"SELECT condition_id, condition_name_zh FROM working_condition "
                f"WHERE is_valid=1 AND condition_id IN ({placeholders})",
                params
            )
            for r in rows:
                try:
                    row_cid = int(r['condition_id'])
                except Exception:
                    continue
                cid_name_map[row_cid] = (r.get('condition_name_zh') or '').strip()

        composite_weights = []
        for cid in cid_list:
            composite_weights.append({
                'condition_id': cid,
                'condition_name': cid_name_map.get(cid) or f'工况 {cid}',
                'weight': float(COMPOSITE_WEIGHTS.get(cid, 1.0)),
                'weight_pct': float(weight_pct_map.get(cid, 0.0)),
            })

        # Build human-readable threshold descriptions.
        lower_desc = (
            f'max({lower_min_count} 条, {lower_min_ratio * 100:g}% × 该工况总曲线数)'
        )
        upper_desc = (
            f'max({upper_min_count} 条, {upper_min_ratio * 100:g}% × 该工况总曲线数)'
        )
        weighting_desc = (
            '启用自适应权重：每个采样点权重 = 该点的曲线覆盖比例（覆盖该点的曲线数 / 该工况总曲线数），'
            '原始分为各点「风量比 × 覆盖权重」的加权均值。'
            if adaptive_weights else
            '等权重均值：有效采样点的风量比做简单算术平均。'
        )

        return resp_ok({
            'items': [
                {
                    'title': '1) 工况基线：群体中位数拟合',
                    'content': (
                        f'每个工况在所有型号的噪音-风量PCHIP曲线范围内按统一 dB 网格（步长 {baseline_step:g} dB）'
                        f'采样，取各点群体中位数风量，再对最长连续有效区段用指数回归拟合基线（airflow = exp(intercept + slope × dB)）。'
                    )
                },
                {
                    'title': '2) 有效评分区间',
                    'content': (
                        f'• 下界：区段中首个覆盖数 ≥ {lower_desc} 的 dB 点；\n'
                        f'• 上界：区段中最后一个覆盖数 ≥ {upper_desc} 的 dB 点。\n'
                        f'单个风扇参与评分的实际区间为「工况有效区间 ∩ 该风扇自身覆盖区间」。'
                    )
                },
                {
                    'title': f'3) 等风量间隔采样',
                    'content': (
                        f'基线为单调指数函数，可解析求逆（dB = (ln(airflow) − intercept) / slope）。'
                        f'在有效区间 [valid_db_min, valid_db_max] 对应的风量范围内取等间距风量值，'
                        f'再经逆变换映射回 dB 轴，作为所有型号共享的采样网格。'
                        f'每个型号在该共享 dB 网格与自身覆盖区间的交集上采样并计算风量比（model airflow / baseline airflow）。'
                    )
                },
                {
                    'title': '4) 原始分聚合',
                    'content': weighting_desc
                },
                {
                    'title': '5) ratio-to-best 归一化',
                    'content': '工况内以「该工况所有型号中最高原始分」为分母做比值归一化，得到 0–100 的工况分数。'
                },
                {
                    'title': '6) 综合分计算',
                    'content': '综合分按当前配置权重对各工况分做加权平均（权重=0 的工况不参与综合评分）；同一分值按 model_id 降序作为统一并列排序次序。'
                },
            ],
            'composite_weighting': {
                'title': '综合分权重（实时配置）',
                'content': '下表展示后端当前生效的各工况综合分占比。',
                'conditions': composite_weights,
            },
            'effective_thresholds': {
                'valid_interval_lower_min_count': lower_min_count,
                'valid_interval_lower_min_ratio': lower_min_ratio,
                'valid_interval_upper_min_count': upper_min_count,
                'valid_interval_upper_min_ratio': upper_min_ratio,
                'baseline_db_step': baseline_step,
                'raw_score_airflow_sample_count': airflow_samples,
                'raw_score_adaptive_weights': adaptive_weights,
            },
        })
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
    # Stagger startup across Gunicorn workers: sleep a random amount (0–30 s) before
    # the first fetch so all workers don't hit the DB simultaneously at boot time.
    import random as _rand
    time.sleep(_rand.uniform(0, 30))
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
        # Canonical radar condition ID order: CCW from UL (UL→L→LL→LR→R→UR for 6 slots).
        # Driven by SCORE_CONDITION_IDS env var; matches the frontend label layout.
        'radar_cids': list(_RADAR_CIDS),
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
        current_year=datetime.now().year,
        radar_cids=list(_RADAR_CIDS),
    )
    response = make_response(html_content)
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, proxy-revalidate'
    response.headers['Expires'] = '0'
    response.headers['Pragma'] = 'no-cache'
    return response

# 4) 排行榜 API（canonical model-score profile）
@app.get('/api/rankings_v2')
def api_rankings_v2():
    """Return rankings from the canonical score cache snapshot.

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
        data = get_rankings_v2()
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
    # Stagger startup across Gunicorn workers so multiple workers don't all query
    # the announcements table at the exact same moment when the app boots.
    import random as _rand
    time.sleep(_rand.uniform(0, 15))
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
# Gallery API (public – read-only)
# =========================================
from app import gallery_services as _gallery

@app.route('/api/gallery/<int:model_id>')
def api_gallery_list(model_id):
    """Return gallery metadata for a model, including video_BV if set."""
    try:
        items = _gallery.list_gallery(model_id)
        video_bv = None
        try:
            rows = fetch_all(
                "SELECT video_BV FROM fan_model WHERE model_id = :mid LIMIT 1",
                {'mid': model_id}
            )
            if rows:
                raw = (rows[0].get('video_BV') or '').strip()
                video_bv = raw if raw else None
        except sa_exc.SQLAlchemyError as e:
            # Only degrade gracefully if the column is missing; propagate other DB errors.
            msg = str(getattr(e, "orig", e))
            lower_msg = msg.lower()
            if "unknown column" in lower_msg or "no such column" in lower_msg:
                video_bv = None
            else:
                raise
        return resp_ok({'items': items, 'video_BV': video_bv})
    except Exception as e:
        app.logger.exception('api_gallery_list error: %s', e)
        return resp_err('INTERNAL_ERROR', f'接口异常: {e}', 500)


@app.route('/api/gallery/thumb/<path:filename>')
def api_gallery_thumb(filename):
    """Serve a gallery thumbnail."""
    if not _gallery.is_valid_filename(filename):
        abort(404)
    thumbs = _gallery.thumbs_dir()
    return send_from_directory(thumbs, filename)


@app.route('/api/gallery/original/<path:filename>')
def api_gallery_original(filename):
    """Serve a gallery original image."""
    if not _gallery.is_valid_filename(filename):
        abort(404)
    originals = _gallery.originals_dir()
    return send_from_directory(originals, filename)


# =========================================
# Bilibili metadata + poster proxy
# =========================================

_BILIBILI_VIEW_API = "https://api.bilibili.com/x/web-interface/view"
_BILIBILI_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.bilibili.com/",
}
# Maximum number of bytes accepted from the upstream poster-proxy response.
# Bilibili thumbnails are typically well under 1 MB; 5 MB is a generous cap.
_MAX_POSTER_BYTES = 5 * 1024 * 1024  # 5 MB

# In-process TTL cache for Bilibili metadata: bvid -> (fetched_at, meta_dict).
# Deduplicates back-to-back calls from /meta and /poster-proxy within the TTL window.
_BILIBILI_META_CACHE_TTL = 300  # seconds
_bilibili_meta_cache: dict = {}  # bvid -> (fetched_at, meta_dict)
_bilibili_meta_cache_lock = threading.Lock()


def _fetch_bilibili_meta(bvid: str) -> dict:
    """
    Query Bilibili open API for video metadata (title, poster URL, cid).
    Returns a dict with keys: bvid, title, poster, cid; or raises on failure.
    """
    resp = requests.get(
        _BILIBILI_VIEW_API,
        params={"bvid": bvid},
        headers=_BILIBILI_HEADERS,
        timeout=8,
    )
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("code") != 0 or not payload.get("data"):
        raise ValueError(payload.get("message") or "Bilibili API error")
    data = payload["data"]
    pages = data.get("pages") or []
    cid = pages[0].get("cid") if pages else None
    return {
        "bvid": data.get("bvid") or bvid,
        "title": data.get("title") or "",
        "poster": data.get("pic") or "",
        "cid": cid,
    }


def _fetch_bilibili_meta_cached(bvid: str) -> dict:
    """Return Bilibili metadata for *bvid*, using a short in-process TTL cache
    so that back-to-back calls from /meta and /poster-proxy for the same video
    hit the Bilibili API only once within *_BILIBILI_META_CACHE_TTL* seconds.
    """
    now = time.monotonic()
    with _bilibili_meta_cache_lock:
        entry = _bilibili_meta_cache.get(bvid)
        if entry is not None:
            fetched_at, cached_meta = entry
            if now - fetched_at < _BILIBILI_META_CACHE_TTL:
                return cached_meta
    # Cache miss or stale — fetch outside the lock to avoid blocking.
    meta = _fetch_bilibili_meta(bvid)
    with _bilibili_meta_cache_lock:
        _bilibili_meta_cache[bvid] = (time.monotonic(), meta)
    return meta


@app.route('/api/bilibili/meta')
def api_bilibili_meta():
    """
    GET /api/bilibili/meta?bvid=BVxxxxxxxxx
    Returns video title, poster proxy URL, and cid for the given BV number.
    The poster URL is returned as a same-origin proxy path so the browser
    does not have to load it directly from Bilibili CDN (which can fail due
    to hotlink protection or CSP restrictions).
    """
    bvid = (request.args.get('bvid') or '').strip()
    if not bvid:
        return resp_err('MISSING_PARAM', 'bvid 参数缺失', 400)
    try:
        meta = _fetch_bilibili_meta_cached(bvid)
    except Exception as e:
        app.logger.warning('api_bilibili_meta error bvid=%s: %s', bvid, e)
        return resp_err('BILIBILI_API_ERROR', f'Bilibili 接口异常: {e}', 502)

    poster_proxy_url = (
        f'/api/bilibili/poster-proxy?bvid={urllib.parse.quote(meta["bvid"])}'
        if meta.get("poster") else None
    )
    return resp_ok({
        "bvid": meta["bvid"],
        "title": meta["title"],
        "poster_proxy_url": poster_proxy_url,
        "cid": meta["cid"],
    })


@app.route('/api/bilibili/poster-proxy')
def api_bilibili_poster_proxy():
    """
    GET /api/bilibili/poster-proxy?bvid=BVxxxxxxxxx
    Fetches the Bilibili video thumbnail on the server side and returns it to
    the browser.  This is necessary because browsers often cannot load Bilibili
    CDN image URLs directly (hotlink protection, missing CORS headers, or CSP).
    The upstream response is buffered in memory (posters are small) before being
    returned, so the upstream connection is always closed promptly.
    """
    bvid = (request.args.get('bvid') or '').strip()
    if not bvid:
        return resp_err('MISSING_PARAM', 'bvid 参数缺失', 400)
    try:
        meta = _fetch_bilibili_meta_cached(bvid)
        poster_url = meta.get("poster") or ""
        if not poster_url:
            return resp_err('NO_POSTER', '该视频没有封面图', 404)
        # Normalise protocol-relative URLs
        if poster_url.startswith('//'):
            poster_url = 'https:' + poster_url
        # SSRF guard: only allow fetching from known Bilibili CDN domains.
        # The URL originates from the Bilibili API response (based on user-provided bvid),
        # so we must restrict it to trusted Bilibili hosts before proxying.
        _allowed_poster_hosts = {
            'i0.hdslb.com', 'i1.hdslb.com', 'i2.hdslb.com',
            'archive.biliimg.com', 'pic.bstarstatic.com',
        }
        try:
            parsed_poster = urllib.parse.urlparse(poster_url)
            if parsed_poster.hostname not in _allowed_poster_hosts:
                app.logger.warning(
                    'api_bilibili_poster_proxy: untrusted host %s for bvid=%s',
                    parsed_poster.hostname, bvid
                )
                return resp_err('UNTRUSTED_HOST', '封面图地址不在允许的域名列表内', 400)
            # Reconstruct the URL from validated components to avoid using
            # the raw user-influenced string directly (SSRF mitigation).
            safe_poster_url = urllib.parse.urlunparse((
                'https',
                parsed_poster.hostname,
                parsed_poster.path,
                '',
                parsed_poster.query,
                '',
            ))
        except Exception:
            return resp_err('INVALID_URL', '封面图地址解析失败', 400)
        with requests.get(
            safe_poster_url,
            headers=_BILIBILI_HEADERS,
            timeout=10,
            stream=True,
        ) as img_resp:
            img_resp.raise_for_status()
            content_type = img_resp.headers.get('Content-Type', 'image/jpeg')
            # Guard against unexpectedly large upstream payloads.
            # Reject early if Content-Length is reported and exceeds the cap,
            # then enforce the same cap while reading to handle chunked responses.
            cl = img_resp.headers.get('Content-Length')
            try:
                if cl is not None and int(cl) > _MAX_POSTER_BYTES:
                    return resp_err('PAYLOAD_TOO_LARGE', '封面图超过大小限制', 502)
            except ValueError:
                pass  # Malformed Content-Length — proceed and rely on read-time cap
            chunks = []
            received = 0
            for chunk in img_resp.iter_content(chunk_size=65536):
                received += len(chunk)
                if received > _MAX_POSTER_BYTES:
                    return resp_err('PAYLOAD_TOO_LARGE', '封面图超过大小限制', 502)
                chunks.append(chunk)
            data = b''.join(chunks)
        return Response(
            data,
            status=200,
            headers={
                'Content-Type': content_type,
                'Cache-Control': 'public, max-age=3600',
            },
        )
    except Exception as e:
        app.logger.warning('api_bilibili_poster_proxy error bvid=%s: %s', bvid, e)
        return resp_err('PROXY_ERROR', f'封面图代理异常: {e}', 502)


# =========================================
# Entrypoint
# =========================================
if __name__ == '__main__':
    app.logger.setLevel(logging.WARNING)
    app.run(host='0.0.0.0', port=5001, debug=False, use_reloader=False)
