"""Server-owned fan challenge wallet and equipment.

Register ``bp`` after ``setup(engine_obj, logger_obj)``. Schema creation is
deliberately manual: docs/sql/V010__mini_game_tables.sql.

Reports are observations, not proof of human input or a physics simulation.
The 30,000 RPM safety envelope deliberately permits hard-edge impulses;
friction-only acceleration limits would reject real demo collisions. Forward
income is limited to 3,000 RPM and *server elapsed time*, not a daily cap.
Within this envelope forged observations remain possible; isTrusted is only
a client deterrent. Rejections and timing anomalies are ledger risks, not bans.
"""

import hashlib
import hmac
import json
import logging
import math
import secrets
import threading
import time
import uuid
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path
from urllib.parse import urlsplit

from flask import Blueprint, current_app, request, session
from sqlalchemy import (
    BigInteger, Column, Float, ForeignKey, Integer, MetaData, String, Table,
    Text, UniqueConstraint, func, insert, select, update,
)
from sqlalchemy.exc import SQLAlchemyError

from app import user_activity
from app.common_utils import make_error_response, make_success_response

bp = Blueprint("fan_game", __name__)
_engine = None
_logger = logging.getLogger(__name__)
CATALOG = json.loads(Path(__file__).with_name("fan_game_catalog.json").read_text(encoding="utf-8"))
_priced_catalogs = {}
_records_cache = OrderedDict()
_records_lock = threading.Lock()
RECORDS_CACHE_SECONDS = 5
RECORDS_CACHE_CAPACITY = 256
SHANGHAI = timezone(timedelta(hours=8))
MAX_BODY = 32768
MAX_RPM = 30000
IDLE_MS = 60000
MAX_OBSERVATION_DELAY_MS = 5000
metadata = MetaData()

players = Table(
    "mini_game_player", metadata,
    Column("uid", String(64), primary_key=True),
    Column("site_host", String(253), nullable=False),
    Column("user_identifier", String(64), nullable=False),
    Column("balance_milli", BigInteger, nullable=False),
    Column("income_milli", BigInteger, nullable=False),
    Column("income_carry", String(64), nullable=False),
    Column("total_turns", Float, nullable=False),
    Column("best_rpm", Float, nullable=False),
    Column("best_airflow", Float, nullable=False),
    Column("revision", BigInteger, nullable=False),
    Column("state_json", Text, nullable=False),
    Column("created_ms", BigInteger, nullable=False),
    Column("updated_ms", BigInteger, nullable=False),
    UniqueConstraint("site_host", "user_identifier"),
)
inventory = Table(
    "mini_game_inventory", metadata,
    Column("uid", String(64), ForeignKey(players.c.uid), primary_key=True),
    Column("kind", String(16), primary_key=True),
    Column("item_id", String(32), primary_key=True),
    Column("state_json", Text, nullable=False),
    Column("updated_ms", BigInteger, nullable=False),
)
play_sessions = Table(
    "mini_game_session", metadata,
    Column("session_id", String(36), primary_key=True),
    Column("uid", String(64), ForeignKey(players.c.uid), nullable=False, index=True),
    Column("revision", BigInteger, nullable=False),
    Column("catalog_version", String(64), nullable=False),
    Column("seq", Integer, nullable=False),
    Column("active", Integer, nullable=False),
    Column("started_ms", BigInteger, nullable=False),
    Column("last_report_ms", BigInteger, nullable=False),
    Column("reported_ms", Float, nullable=False),
    Column("credited_ms", Float, nullable=False),
    Column("state_json", Text, nullable=False),
)
events = Table(
    "mini_game_event", metadata,
    Column("event_id", String(36), primary_key=True),
    Column("uid", String(64), ForeignKey(players.c.uid), nullable=False),
    Column("idempotency_key", String(36), nullable=False),
    Column("session_id", String(36), ForeignKey(play_sessions.c.session_id)),
    Column("seq", Integer),
    Column("action", String(32), nullable=False),
    Column("fingerprint", String(64), nullable=False),
    Column("delta_milli", BigInteger, nullable=False),
    Column("balance_milli", BigInteger, nullable=False),
    Column("status_code", Integer, nullable=False),
    Column("result_json", Text, nullable=False),
    Column("request_json", Text, nullable=False),
    Column("risk_json", Text, nullable=False),
    Column("created_ms", BigInteger, nullable=False),
    UniqueConstraint("uid", "idempotency_key"),
    UniqueConstraint("session_id", "seq"),
)
daily_stats = Table(
    "mini_game_daily", metadata,
    Column("uid", String(64), ForeignKey(players.c.uid), primary_key=True),
    Column("day", String(10), primary_key=True),
    Column("income_milli", BigInteger, nullable=False),
    Column("income_carry", String(64), nullable=False),
    Column("turns", Float, nullable=False),
    Column("max_rpm", Float, nullable=False),
    Column("max_airflow", Float, nullable=False),
    Column("updated_ms", BigInteger, nullable=False),
)


def setup(engine_obj, logger_obj):
    global _engine, _logger
    _engine, _logger = engine_obj, logger_obj
    with _records_lock:
        _records_cache.clear()


def _now_ms():
    return int(time.time() * 1000)


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _date(ms):
    return datetime.fromtimestamp(ms / 1000, SHANGHAI).date().isoformat()


def _scoped_uid(site_host, uid):
    return hashlib.sha256(_json([site_host, uid]).encode("utf-8")).hexdigest()


class GameError(Exception):
    def __init__(self, code, message, status=400):
        self.code, self.message, self.status = code, message, status


class RateLimiter:
    """Fixed-size UID/IP windows; saturation rejects instead of evicting live keys."""

    def __init__(self, capacity=8192):
        self.capacity = capacity
        self.entries = OrderedDict()
        self.lock = threading.Lock()

    def allow(self, keys, now):
        with self.lock:
            expired = [key for key, (start, _) in self.entries.items() if now - start >= 60]
            for key in expired:
                del self.entries[key]
            new = sum(key not in self.entries for key, _ in keys)
            if len(self.entries) + new > self.capacity:
                return False
            if any(self.entries.get(key, (now, 0))[1] >= limit for key, limit in keys):
                return False
            for key, _ in keys:
                start, count = self.entries.get(key, (now, 0))
                self.entries[key] = (start, count + 1)
            return True


_limiter = RateLimiter()
# MySQL row locks are authoritative across workers. SQLite has no FOR UPDATE;
# bounded stripes serialize tests/local requests without unbounded lock allocation.
_stripes = [threading.RLock() for _ in range(128)]


def _price_multiplier():
    try:
        value = Decimal(str(current_app.config.get("FAN_GAME_PRICE_MULTIPLIER", 1)))
    except (ArithmeticError, TypeError, ValueError):
        return Decimal(1)
    return value if value.is_finite() and value > 0 else Decimal(1)


def _catalog():
    """Catalog with every ft³ cost scaled and rounded up to a whole coin."""
    factor = _price_multiplier()
    cached = _priced_catalogs.get(factor)
    if cached is not None:
        return cached

    def scale(cost):
        return int((Decimal(str(cost)) * factor).to_integral_value(rounding=ROUND_CEILING))

    priced = json.loads(json.dumps(CATALOG))
    if factor != 1:
        priced["version"] = f"{CATALOG['version']}+price-{format(factor.normalize(), 'f')}"
    for group in ("materials", "bearings", "hands", "consumables"):
        for item in priced[group].values():
            if "price" in item:
                item["price"] = scale(item["price"])
            if "replaceCost" in item:
                item["replaceCost"] = scale(item["replaceCost"])
            if "service" in item:
                item["service"]["cost"] = scale(item["service"]["cost"])
    _priced_catalogs[factor] = priced
    return priced


def _origin(url):
    try:
        parts = urlsplit(url)
        if parts.scheme not in ("http", "https") or not parts.hostname or parts.username or parts.password:
            return None
        return parts.scheme, parts.hostname.lower(), parts.port or (443 if parts.scheme == "https" else 80)
    except (TypeError, ValueError):
        return None


@bp.before_request
def _gate():
    if not current_app.config.get("FAN_GAME_ENABLED", False):
        return make_error_response("NOT_FOUND", "Fan challenge is disabled", 404)
    if request.method == "POST":
        origin = request.headers.get("Origin")
        source = origin if origin is not None else request.headers.get("Referer")
        if _origin(source) is None or _origin(source) != _origin(request.host_url):
            return make_error_response("INVALID_ORIGIN", "Same-origin request required", 403)
        token = session.get("fan_game_csrf")
        supplied = request.headers.get("X-CSRF-Token", "")
        if not token or not supplied.isascii() or not hmac.compare_digest(token, supplied):
            return make_error_response("INVALID_CSRF", "Refresh challenge state first", 403)
        if not request.is_json:
            return make_error_response("INVALID_JSON", "JSON object required", 400)
        if request.content_length is not None and request.content_length > MAX_BODY:
            return make_error_response("BODY_TOO_LARGE", "Maximum body is 32 KiB", 413)
        # Bounded read also covers requests lacking a Content-Length.
        raw = request.stream.read(MAX_BODY + 1)
        if len(raw) > MAX_BODY:
            return make_error_response("BODY_TOO_LARGE", "Maximum body is 32 KiB", 413)
        try:
            payload = json.loads(raw)
        except (ValueError, UnicodeDecodeError, RecursionError):
            return make_error_response("INVALID_JSON", "JSON object required", 400)
        if not isinstance(payload, dict):
            return make_error_response("INVALID_JSON", "JSON object required", 400)
        pending = [(payload, 0)]
        while pending:
            value, depth = pending.pop()
            if depth > 16:
                return make_error_response("INVALID_JSON", "Maximum JSON nesting is 16", 400)
            if isinstance(value, dict):
                pending.extend((child, depth + 1) for child in value.values())
            elif isinstance(value, list):
                pending.extend((child, depth + 1) for child in value)
        request.fan_game_payload = payload
    uid = user_activity.get_or_create_user_identifier()
    host = user_activity._visit_host()
    if len(host) > 253:
        return make_error_response("INVALID_HOST", "Host is too long", 400)
    request.fan_game_uid = _scoped_uid(host, uid)
    request.fan_game_user_identifier = uid
    request.fan_game_site_host = host
    if not _limiter.allow(
        [("uid:" + uid, 90), ("ip:" + (request.remote_addr or "unknown"), 300)],
        time.monotonic(),
    ):
        return make_error_response("RATE_LIMITED", "Try again later", 429)


def _locked(conn, statement):
    if conn.dialect.name != "sqlite":
        statement = statement.with_for_update()
    return conn.execute(statement).mappings().first()


def _item_default(kind):
    if kind == "material":
        return {"broken": []}
    if kind == "bearing":
        return {"turns": 0}
    return {"durability": 100, "contact_seconds": 0}


def _player(conn, uid, now, site_host, user_identifier):
    defaults = dict(
        uid=uid, site_host=site_host, user_identifier=user_identifier,
        balance_milli=0, income_milli=0, income_carry="0",
        total_turns=0, best_rpm=0, best_airflow=0, revision=0, created_ms=now, updated_ms=now,
        state_json=_json(dict(material="print3d", bearing="sleeve", hand="hand",
                              sweat=0, sweatRestAt=now, powderCount=0,
                              powderSeconds=0,
                              printReadyAt=now + CATALOG["materials"]["print3d"]["printSeconds"] * 1000)),
    )
    # An upsert locks/initializes the parent before any session or inventory row.
    if conn.dialect.name == "mysql":
        from sqlalchemy.dialects.mysql import insert as mysql_insert
        stmt = mysql_insert(players).values(**defaults)
        conn.execute(stmt.on_duplicate_key_update(uid=stmt.inserted.uid))
    elif conn.dialect.name == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert
        conn.execute(sqlite_insert(players).values(**defaults).on_conflict_do_nothing())
    else:
        raise RuntimeError("Fan challenge supports MySQL and SQLite only")
    p = dict(_locked(conn, select(players).where(players.c.uid == uid)))
    p["extra"] = json.loads(p["state_json"])
    if p["extra"]["material"] == "print3d" and not p["extra"].get("printReadyAt"):
        p["extra"]["printReadyAt"] = now + CATALOG["materials"]["print3d"]["printSeconds"] * 1000
        conn.execute(update(players).where(players.c.uid == uid).values(
            state_json=_json(p["extra"]), updated_ms=now,
        ))
    for kind, item_id in (("material", "print3d"), ("bearing", "sleeve"), ("hand", "hand")):
        where = (inventory.c.uid == uid) & (inventory.c.kind == kind) & (inventory.c.item_id == item_id)
        if conn.execute(select(inventory.c.uid).where(where)).first() is None:
            conn.execute(insert(inventory).values(
                uid=uid, kind=kind, item_id=item_id,
                state_json=_json(_item_default(kind)), updated_ms=now,
            ))
    return p


def _items(conn, uid):
    stmt = select(inventory).where(inventory.c.uid == uid).order_by(inventory.c.kind, inventory.c.item_id)
    if conn.dialect.name != "sqlite":
        stmt = stmt.with_for_update()
    return {(r["kind"], r["item_id"]): json.loads(r["state_json"])
            for r in conn.execute(stmt).mappings()}


def _save_player(conn, p, now):
    values = {key: p[key] for key in (
        "balance_milli", "income_milli", "income_carry", "total_turns", "best_rpm", "best_airflow", "revision",
    )}
    conn.execute(update(players).where(players.c.uid == p["uid"]).values(
        **values, state_json=_json(p["extra"]), updated_ms=now,
    ))


def _save_items(conn, uid, items, now):
    for (kind, item_id), state in sorted(items.items()):
        where = (inventory.c.uid == uid) & (inventory.c.kind == kind) & (inventory.c.item_id == item_id)
        if conn.execute(select(inventory.c.uid).where(where)).first():
            conn.execute(update(inventory).where(where).values(state_json=_json(state), updated_ms=now))
        else:
            conn.execute(insert(inventory).values(
                uid=uid, kind=kind, item_id=item_id, state_json=_json(state), updated_ms=now,
            ))


def _recover(p, now, observed_ms=0):
    x = p["extra"]
    idle = max(0, now - x["sweatRestAt"] - observed_ms)
    x["sweat"] = max(0, x["sweat"] - idle / 1000 / CATALOG["sweat"]["recoverSeconds"])
    x["sweatRestAt"] = now


def _snapshot(conn, p, now, items=None):
    items = _items(conn, p["uid"]) if items is None else items
    x = dict(p["extra"])
    live = conn.execute(select(play_sessions.c.session_id).where(
        (play_sessions.c.uid == p["uid"]) & (play_sessions.c.active == 1)
        & (play_sessions.c.last_report_ms >= now - IDLE_MS)
    )).first()
    if not live:
        x["sweat"] = max(0, x["sweat"] - max(0, now - x["sweatRestAt"]) / 60000)
        x["sweatRestAt"] = now
    day = conn.execute(select(daily_stats).where(
        (daily_stats.c.uid == p["uid"]) & (daily_stats.c.day == _date(now))
    )).mappings().first()
    campaign = dict(
        x, balance=p["balance_milli"] / 1000,
        totalAirflow=float((Decimal(p["income_milli"]) + Decimal(p["income_carry"])) / 1000),
        todayAirflow=float((Decimal(day["income_milli"]) + Decimal(day["income_carry"])) / 1000) if day else 0,
        todayMaxRpm=day["max_rpm"] if day else 0,
        bestRpm=p["best_rpm"], todayMaxAirflow=day["max_airflow"] if day else 0,
        bestAirflow=p["best_airflow"], date=_date(now),
        ownedMaterials=[id for kind, id in items if kind == "material"],
        ownedBearings=[id for kind, id in items if kind == "bearing"],
        ownedHands=[id for kind, id in items if kind == "hand"],
        bearingTurns={id: items.get(("bearing", id), {"turns": 0})["turns"] for id in CATALOG["bearings"]},
        handDurability={id: items.get(("hand", id), {"durability": 100})["durability"]
                        for id in CATALOG["hands"] if id != "hand"},
        handContactSeconds={id: items.get(("hand", id), {"contact_seconds": 0})["contact_seconds"]
                            for id in CATALOG["hands"] if id != "hand"},
        brokenBlades=items[("material", x["material"])]["broken"],
        storedBrokenBlades={id: state["broken"] for (kind, id), state in items.items() if kind == "material"},
    )
    session.setdefault("fan_game_csrf", secrets.token_urlsafe(32))
    return dict(campaign=campaign, total_turns=p["total_turns"], revision=p["revision"],
                catalog=_catalog(), csrf_token=session["fan_game_csrf"], server_time_ms=now,
                server_records=request.fan_game_server_records)


def _cached_server_records(site_host, now):
    day = _date(now)
    key = (site_host, day)
    with _records_lock:
        timestamp = time.monotonic()
        cached = _records_cache.get(key)
        if cached and 0 <= timestamp - cached[0] < RECORDS_CACHE_SECONDS:
            _records_cache.move_to_end(key)
            return cached[1]
        with _engine.connect() as conn:
            records = _server_records(conn, site_host, now)
        _records_cache[key] = (time.monotonic(), records)
        _records_cache.move_to_end(key)
        if len(_records_cache) > RECORDS_CACHE_CAPACITY:
            _records_cache.popitem(last=False)
        return records


def _server_records(conn, site_host, now):
    today = datetime.fromtimestamp(now / 1000, SHANGHAI).date()
    week_start = today - timedelta(days=today.weekday())
    totals = (
        select(daily_stats.c.uid,
               func.sum(daily_stats.c.turns).label("turns"),
               func.max(daily_stats.c.max_rpm).label("max_rpm"),
               func.max(daily_stats.c.max_airflow).label("max_airflow"))
        .select_from(daily_stats.join(players, daily_stats.c.uid == players.c.uid))
        .where(players.c.site_host == site_host,
               daily_stats.c.day >= week_start.isoformat(),
               daily_stats.c.day <= today.isoformat())
        .group_by(daily_stats.c.uid)
    ).subquery()
    weekly = conn.execute(select(func.max(totals.c.turns), func.max(totals.c.max_rpm),
                                 func.max(totals.c.max_airflow))).one()
    daily = conn.execute(
        select(func.max(daily_stats.c.turns), func.max(daily_stats.c.max_rpm), func.max(daily_stats.c.max_airflow))
        .select_from(daily_stats.join(players, daily_stats.c.uid == players.c.uid))
        .where(players.c.site_host == site_host, daily_stats.c.day == today.isoformat())
    ).one()
    return dict(today_turns=daily[0] or 0, today_rpm=daily[1] or 0, today_airflow=daily[2] or 0,
                week_turns=weekly[0] or 0, week_rpm=weekly[1] or 0, week_airflow=weekly[2] or 0)


def _key(payload):
    key = payload.get("idempotency_key")
    try:
        if not isinstance(key, str) or str(uuid.UUID(key)) != key:
            raise ValueError
    except (ValueError, AttributeError):
        raise GameError("INVALID_KEY", "Canonical UUID idempotency_key required")
    return key


def _valid_uuid(value):
    if not isinstance(value, str) or len(value) != 36:
        return False
    try:
        return str(uuid.UUID(value)) == value
    except ValueError:
        return False


def _report_session_id(payload):
    sid = payload.get("play_session_id")
    if not _valid_uuid(sid):
        raise GameError("INVALID_SESSION_ID", "Canonical UUID play_session_id required")
    return sid


def _integer(value, name, minimum=0, maximum=2147483647):
    if type(value) is not int or not minimum <= value <= maximum:
        raise GameError("INVALID_REPORT", f"{name} must be an integer in [{minimum}, {maximum}]")
    return value


def _number(value, name, maximum=None, signed=False):
    try:
        finite = type(value) in (int, float) and math.isfinite(value)
    except OverflowError:
        finite = False
    if not finite:
        raise GameError("INVALID_REPORT", f"{name} must be finite")
    if (not signed and value < 0) or (maximum is not None and abs(value) > maximum):
        raise GameError("INVALID_REPORT", f"{name} out of bounds")
    return float(value)


def _response(result, status):
    if status == 200:
        return make_success_response(result)
    return make_error_response(result["code"], result["message"], status)


def _conflict_risk(conn, event_id, code):
    row = conn.execute(select(events.c.risk_json).where(events.c.event_id == event_id)).first()
    risks = json.loads(row[0])
    if code not in risks:
        risks.append(code)
        conn.execute(update(events).where(events.c.event_id == event_id).values(risk_json=_json(risks)))


def _execute(action):
    uid, payload = request.fan_game_uid, getattr(request, "fan_game_payload", None)
    try:
        key = _key(payload) if payload is not None else None
        canonical = _json(payload)
        fingerprint = hashlib.sha256((action + ":" + canonical).encode()).hexdigest()
        with _stripes[hash(uid) % len(_stripes)]:
            request.fan_game_server_records = _cached_server_records(request.fan_game_site_host, _now_ms())
            with _engine.begin() as conn:
                now = _now_ms()
                p = _player(conn, uid, now, request.fan_game_site_host,
                            request.fan_game_user_identifier)
                if payload is None:
                    return make_success_response(_snapshot(conn, p, now))
                prior = conn.execute(select(events).where(
                    (events.c.uid == uid) & (events.c.idempotency_key == key)
                )).mappings().first()
                if prior:
                    if prior["fingerprint"] != fingerprint:
                        _conflict_risk(conn, prior["event_id"], "IDEMPOTENCY_CONFLICT")
                        return make_error_response(
                            "IDEMPOTENCY_CONFLICT", "Key was used with a different payload", 409,
                        )
                    result = json.loads(prior["result_json"])
                    if prior["status_code"] == 200:
                        session.setdefault("fan_game_csrf", secrets.token_urlsafe(32))
                        result["csrf_token"] = session["fan_game_csrf"]
                    return _response(result, prior["status_code"])
                # A reused sequence never creates a second ledger event, even under another key.
                sid, seq = payload.get("play_session_id"), payload.get("seq")
                if action == "report" and _valid_uuid(sid) and type(seq) is int:
                    prior_seq = conn.execute(select(events.c.event_id).where(
                        (events.c.uid == uid) & (events.c.session_id == sid) & (events.c.seq == seq)
                    )).first()
                    if prior_seq:
                        _conflict_risk(conn, prior_seq[0], "SEQUENCE_CONFLICT")
                        return make_error_response(
                            "SEQUENCE_CONFLICT", "Sequence already recorded; replay original key", 409,
                        )
                before = p["balance_milli"]
                risk = []
                linked_sid, linked_seq = None, None
                # Rejections persist outside a savepoint; failed operations cannot partially mutate.
                try:
                    with conn.begin_nested():
                        if action == "start":
                            result, linked_sid = _start(conn, p, now)
                        elif action == "report":
                            sid = _report_session_id(payload)
                            owned = conn.execute(select(play_sessions.c.session_id).where(
                                (play_sessions.c.uid == uid) & (play_sessions.c.session_id == sid)
                            )).first()
                            if owned:
                                linked_sid = sid
                                if type(seq) is int and 1 <= seq <= 2147483647:
                                    linked_seq = seq
                            result = _report(conn, p, payload, now, risk)
                        else:
                            result = _mutate(conn, p, payload, now)
                    status = 200
                except GameError as exc:
                    status = exc.status
                    result = {"code": exc.code, "message": exc.message}
                    risk.append(exc.code)
                    # A rejected observation has consumed its ledger sequence;
                    # restart rather than leave an ambiguous partially usable stream.
                    if action == "report" and linked_sid:
                        conn.execute(update(play_sessions).where(
                            play_sessions.c.session_id == linked_sid
                        ).values(active=0))
                if status != 200:
                    balance = before
                else:
                    balance = p["balance_milli"]
                conn.execute(insert(events).values(
                    event_id=str(uuid.uuid4()), uid=uid, idempotency_key=key,
                    session_id=linked_sid, seq=linked_seq, action=action, fingerprint=fingerprint,
                    delta_milli=balance - before, balance_milli=balance,
                    status_code=status, result_json=_json(result), request_json=canonical,
                    risk_json=_json(risk), created_ms=now,
                ))
                return _response(result, status)
    except GameError as exc:
        return make_error_response(exc.code, exc.message, exc.status)
    except (SQLAlchemyError, RuntimeError, AttributeError):
        _logger.exception("fan-game database unavailable; verify manual V010/V011 game schema")
        return make_error_response("FAN_GAME_UNAVAILABLE", "Database unavailable; verify game schema", 503)


def _start(conn, p, now):
    # Player lock is already held; session locks always precede inventory locks.
    _lock_sessions(conn, p["uid"])
    _recover(p, now)
    conn.execute(update(play_sessions).where(play_sessions.c.uid == p["uid"]).values(active=0))
    items = _items(conn, p["uid"])
    sid = str(uuid.uuid4())
    baseline = {kind: p["extra"][kind] for kind in ("material", "bearing", "hand")}
    baseline["broken"] = items[("material", baseline["material"])]["broken"]
    baseline["observed_ms"] = 0
    conn.execute(insert(play_sessions).values(
        session_id=sid, uid=p["uid"], revision=p["revision"], catalog_version=_catalog()["version"],
        seq=0, active=1, started_ms=now, last_report_ms=now,
        reported_ms=0, credited_ms=0, state_json=_json(baseline),
    ))
    _save_player(conn, p, now)
    return dict(_snapshot(conn, p, now, items), play_session_id=sid, seq=0), sid


def _lock_sessions(conn, uid, sid=None):
    where = (play_sessions.c.uid == uid) & (
        (play_sessions.c.active == 1) | (play_sessions.c.session_id == sid)
    )
    stmt = select(play_sessions).where(where).order_by(play_sessions.c.session_id)
    if conn.dialect.name != "sqlite":
        stmt = stmt.with_for_update()
    return list(conn.execute(stmt).mappings())


def _validate_slices(payload):
    slices = payload.get("slices")
    if not isinstance(slices, list) or not 1 <= len(slices) <= 20:
        raise GameError("INVALID_REPORT", "Between 1 and 20 slices required")
    clean = []
    for raw in slices:
        if not isinstance(raw, dict):
            raise GameError("INVALID_REPORT", "Slice must be an object")
        s = {name: _number(raw.get(name), name) for name in (
            "duration_ms", "turns", "forward_turns", "peak_rpm", "peak_forward_rpm", "peak_airflow", "press_ms",
            "contact_ms", "effort_ms", "impact_wear",
        )}
        s["end_rpm"] = _number(raw.get("end_rpm"), "end_rpm", MAX_RPM, signed=True)
        d = s["duration_ms"]
        if not 0 < d <= 10000 or s["peak_rpm"] > MAX_RPM or abs(s["end_rpm"]) > s["peak_rpm"] + 1e-6:
            raise GameError("INVALID_REPORT", "Duration or RPM outside safety envelope")
        if s["peak_forward_rpm"] > s["peak_rpm"]:
            raise GameError("INVALID_REPORT", "Forward peak RPM exceeds absolute peak RPM")
        if (s["forward_turns"] > s["turns"] + 1e-7
                or s["forward_turns"] > min(s["peak_forward_rpm"], CATALOG["airflow"]["earningRpmCap"]) * d / 60000 + 1e-7
                or s["turns"] > s["peak_rpm"] * d / 60000 + 1e-7):
            raise GameError("INVALID_REPORT", "Revolutions exceed duration/RPM bounds")
        if (s["press_ms"] > d or s["contact_ms"] > s["press_ms"]
                or s["effort_ms"] > s["press_ms"] + 1e-6
                or s["effort_ms"] < .6 * s["press_ms"] - 1e-6
                or s["impact_wear"] > 100):
            raise GameError("INVALID_REPORT", "Contact, effort or impact outside bounds")
        if s["impact_wear"] and not s["contact_ms"]:
            raise GameError("INVALID_REPORT", "Impact requires contact")
        broken = raw.get("broken_blades")
        if (not isinstance(broken, list) or len(broken) > 7
                or any(type(i) is not int or not 0 <= i < 7 for i in broken)
                or len(set(broken)) != len(broken)):
            raise GameError("INVALID_REPORT", "Unique blade slots 0..6 required")
        s["broken_blades"] = sorted(broken)
        clean.append(s)
    if sum(s["duration_ms"] for s in clean) > 15000:
        raise GameError("INVALID_REPORT", "Maximum report duration is 15 seconds")
    return clean


def _broken_factor(broken):
    magnitude = math.hypot(sum(math.cos(i * math.tau / 7) for i in broken),
                           sum(math.sin(i * math.tau / 7) for i in broken))
    airflow = CATALOG["airflow"]
    return ((7 - len(broken)) / 7) ** airflow["missingBladeExponent"] * (
        1 - airflow["imbalancePenalty"] * magnitude / 7
    )


def _daily_add(conn, uid, start, duration, income, turns, rpm, peak_airflow, now):
    """Prorate a chronological slice at Shanghai midnight, never client dates."""
    end = start + duration
    cursor = start
    remaining_income = income
    remaining_turns = turns
    while cursor < end:
        local = datetime.fromtimestamp(cursor / 1000, SHANGHAI)
        midnight = datetime.combine(local.date() + timedelta(days=1), datetime.min.time(), SHANGHAI)
        boundary = min(end, midnight.timestamp() * 1000)
        fraction = Decimal(str(boundary - cursor)) / Decimal(str(duration))
        part_income = remaining_income if boundary == end else income * fraction
        part_turns = remaining_turns if boundary == end else turns * float(fraction)
        remaining_income -= part_income
        remaining_turns -= part_turns
        day = local.date().isoformat()
        where = (daily_stats.c.uid == uid) & (daily_stats.c.day == day)
        row = conn.execute(select(daily_stats).where(where)).mappings().first()
        amount = (Decimal(row["income_milli"]) + Decimal(row["income_carry"]) if row else Decimal(0))
        amount += part_income
        whole = int(amount.to_integral_value(rounding=ROUND_FLOOR))
        values = dict(income_milli=whole, income_carry=str(amount - whole),
                      turns=(row["turns"] if row else 0) + part_turns,
                      max_rpm=max(row["max_rpm"] if row else 0, rpm),
                      max_airflow=max(row["max_airflow"] if row else 0, peak_airflow), updated_ms=now)
        if row:
            conn.execute(update(daily_stats).where(where).values(**values))
        else:
            conn.execute(insert(daily_stats).values(uid=uid, day=day, **values))
        cursor = boundary


def _report(conn, p, payload, now, risk):
    sid = _report_session_id(payload)
    sessions = _lock_sessions(conn, p["uid"], sid)
    play = next((dict(s) for s in sessions if s["session_id"] == sid), None)
    if not play or not play["active"]:
        raise GameError("SESSION_INVALID", "Start a new play session", 409)
    seq = _integer(payload.get("seq"), "seq", 1)
    if seq != play["seq"] + 1:
        raise GameError("SEQUENCE_CONFLICT", "Reports must be sequential", 409)
    revision = _integer(payload.get("revision"), "revision", maximum=9223372036854775807)
    if revision != p["revision"] or revision != play["revision"]:
        raise GameError("REVISION_CONFLICT", "Equipment changed; restart session", 409)
    version = _catalog()["version"]
    if payload.get("catalog_version") != version or play["catalog_version"] != version:
        raise GameError("CATALOG_CONFLICT", "Refresh the catalog and restart session", 409)
    if now - play["last_report_ms"] > IDLE_MS or now < play["started_ms"]:
        raise GameError("SESSION_EXPIRED", "Session idle limit is 60 seconds", 409)
    slices = _validate_slices(payload)
    duration = sum(s["duration_ms"] for s in slices)
    elapsed = now - play["started_ms"]
    observed_ms = _number(payload.get("observed_ms"), "observed_ms")
    if observed_ms > elapsed * 1.1 + 1:
        raise GameError("TIME_INFLATION", "Observation timestamp exceeds server elapsed time")
    if elapsed - observed_ms > MAX_OBSERVATION_DELAY_MS:
        raise GameError("OBSERVATION_STALE", "Observation timestamp is too far behind server time", 409)
    if play["reported_ms"] + duration > elapsed * 1.1 + 1:
        raise GameError("TIME_INFLATION", "Reported duration exceeds server elapsed time")
    if play["reported_ms"] + duration > elapsed:
        risk.append("ELAPSED_TOLERANCE")
    items = _items(conn, p["uid"])
    x = p["extra"]
    baseline = json.loads(play["state_json"])
    if observed_ms < baseline["observed_ms"]:
        raise GameError("INVALID_REPORT", "Observation timestamps must be sequential")
    observed_ms = min(observed_ms, elapsed)
    if any(baseline[k] != x[k] for k in ("material", "bearing", "hand")):
        raise GameError("REVISION_CONFLICT", "Loadout changed", 409)
    broken = set(items[("material", x["material"])]["broken"])
    for s in slices:
        if not broken.issubset(s["broken_blades"]):
            raise GameError("DAMAGE_HEALING", "Reports cannot restore broken blades")
        airflow_limit = (s["peak_forward_rpm"] / CATALOG["airflow"]["referenceRpm"]
                         * CATALOG["airflow"]["referenceCfm"]
                         * CATALOG["materials"][x["material"]]["airflow"]
                         * _broken_factor(broken))
        if s["peak_airflow"] > airflow_limit + 1e-6:
            raise GameError("INVALID_REPORT", "Peak airflow exceeds RPM and blade condition bounds")
        broken = set(s["broken_blades"])
    _recover(p, now, duration)
    # Each sent report advances the observation cursor, discarding unobserved gaps.
    budget = min(duration, max(0, observed_ms - baseline["observed_ms"]),
                 max(0, elapsed - play["credited_ms"]))
    baseline["observed_ms"] = observed_ms
    time_ratio = budget / duration
    observed_cursor = play["started_ms"] + observed_ms - budget
    amount = Decimal(0)
    new_turns = 0
    for index, s in enumerate(slices):
        d = s["duration_ms"]
        credited = budget if index == len(slices) - 1 else min(d * time_ratio, budget)
        budget -= credited
        play["credited_ms"] += credited
        # Prorating removes tolerance income; no money can be minted for future time.
        ratio = Decimal(str(credited)) / Decimal(str(d))
        airflow = (Decimal(str(s["forward_turns"])) * ratio
                   * Decimal(str(CATALOG["airflow"]["referenceCfm"]))
                   / Decimal(str(CATALOG["airflow"]["referenceRpm"]))
                   * Decimal(str(CATALOG["materials"][x["material"]]["airflow"]))
                   * Decimal(str(_broken_factor(s["broken_blades"]))) * 1000)
        amount += airflow
        items[("bearing", x["bearing"])]["turns"] += s["turns"]
        new_turns += s["turns"]
        if x["hand"] != "hand":
            gear = items[("hand", x["hand"])]
            lifetime = CATALOG["hands"][x["hand"]]["contactMinutes"] * 60
            contact = s["contact_ms"] / 1000
            gear["contact_seconds"] += contact
            gear["durability"] = max(0, gear["durability"] - contact / lifetime * 100 - s["impact_wear"])
        x["sweat"] = min(1, max(
            0, x["sweat"] + s["effort_ms"] / 1000 / CATALOG["sweat"]["fullPressSeconds"]
            - (d - s["press_ms"]) / 1000 / CATALOG["sweat"]["recoverSeconds"],
        ))
        x["powderSeconds"] = max(0, x["powderSeconds"] - s["press_ms"] / 1000)
        if credited:
            _daily_add(conn, p["uid"], observed_cursor, credited, airflow, s["turns"],
                       s["peak_forward_rpm"], s["peak_airflow"], now)
        observed_cursor += credited
    items[("material", x["material"])]["broken"] = sorted(broken)
    carry = Decimal(p["income_carry"]) + amount
    earned = int(carry.to_integral_value(rounding=ROUND_FLOOR))
    if p["balance_milli"] + earned > 9223372036854775807:
        raise GameError("WALLET_OVERFLOW", "Wallet storage limit reached", 409)
    p["balance_milli"] += earned
    p["income_milli"] += earned
    p["income_carry"] = str(carry - earned)
    p["total_turns"] += new_turns
    p["best_rpm"] = max(p["best_rpm"], *(s["peak_forward_rpm"] for s in slices))
    p["best_airflow"] = max(p["best_airflow"], *(s["peak_airflow"] for s in slices))
    conn.execute(update(play_sessions).where(play_sessions.c.session_id == sid).values(
        seq=seq, reported_ms=play["reported_ms"] + duration,
        credited_ms=play["credited_ms"], last_report_ms=now, state_json=_json(baseline),
    ))
    _save_items(conn, p["uid"], items, now)
    _save_player(conn, p, now)
    return dict(_snapshot(conn, p, now, items), play_session_id=sid, seq=seq,
                earned_milli=earned, verdict="accepted")


def _mutate(conn, p, payload, now):
    action, item_id = payload.get("action"), payload.get("id")
    if not isinstance(action, str):
        raise GameError("INVALID_ACTION", "action must be a string")
    if "id" in payload and not isinstance(item_id, str):
        raise GameError("INVALID_ITEM", "id must be a string when provided")
    revision = _integer(payload.get("revision"), "revision", maximum=9223372036854775807)
    if revision != p["revision"]:
        raise GameError("REVISION_CONFLICT", "Refresh state before changing equipment", 409)
    _lock_sessions(conn, p["uid"])
    items = _items(conn, p["uid"])
    x = p["extra"]
    cost = 0
    groups = {"Material": ("material", "materials"), "Bearing": ("bearing", "bearings"), "Hand": ("hand", "hands")}
    if isinstance(action, str) and action in {
        "buyMaterial", "equipMaterial", "buyBearing", "equipBearing", "buyHand", "equipHand",
    }:
        match = next((suffix for suffix in groups if action in ("buy" + suffix, "equip" + suffix)), None)
        if not match:
            raise GameError("INVALID_ACTION", "Unknown equipment action")
        kind, group = groups[match]
        if not isinstance(item_id, str) or item_id not in CATALOG[group]:
            raise GameError("INVALID_ITEM", "Unknown catalog item")
        owned = (kind, item_id) in items
        if action.startswith("equip") and not owned:
            raise GameError("NOT_OWNED", "Purchase equipment before equipping", 409)
        if action == "buyMaterial" and item_id == "print3d":
            raise GameError("PRINT_REQUIRED", "Install an automatically printed impeller")
        if action.startswith("buy") and (not owned or kind == "material"):
            cost = _catalog()[group][item_id]["price"]
            items[(kind, item_id)] = _item_default(kind)
        x[kind] = item_id
        if kind == "material" and item_id == "print3d" and not x["printReadyAt"]:
            x["printReadyAt"] = now + CATALOG["materials"]["print3d"]["printSeconds"] * 1000
    elif action == "serviceBearing":
        item = CATALOG["bearings"][x["bearing"]]
        cost = _catalog()["bearings"][x["bearing"]]["service"]["cost"]
        items[("bearing", x["bearing"])]["turns"] *= item["service"]["keepWearFraction"] ** (1 / item["wearP"])
    elif action == "replaceHand":
        item_id = item_id or x["hand"]
        if not isinstance(item_id, str) or ("hand", item_id) not in items or item_id == "hand":
            raise GameError("NOT_OWNED", "Owned hand equipment required")
        cost = _catalog()["hands"][item_id]["replaceCost"]
        items[("hand", item_id)] = _item_default("hand")
    elif action == "buyPowder":
        cost = _catalog()["consumables"]["powder"]["price"]
        x["powderCount"] += 1
    elif action == "usePowder":
        if x["powderCount"] < 1 or x["powderSeconds"] > 0:
            raise GameError("POWDER_UNAVAILABLE", "Powder must be owned and not already active", 409)
        x["powderCount"] -= 1
        x["powderSeconds"] = CATALOG["consumables"]["powder"]["pressSeconds"]
        x["sweat"] = 0
    elif action == "installPrint":
        if not x["printReadyAt"] or now < x["printReadyAt"]:
            raise GameError("PRINT_NOT_READY", "Server print deadline has not been reached", 409)
        x["printReadyAt"] = now + CATALOG["materials"]["print3d"]["printSeconds"] * 1000
        x["material"] = "print3d"
        items[("material", "print3d")] = _item_default("material")
    else:
        raise GameError("INVALID_ACTION", "Unknown workshop action")
    price = int(Decimal(str(cost)) * 1000)
    if p["balance_milli"] < price:
        raise GameError("INSUFFICIENT_FUNDS", "Not enough fan coins", 409)
    p["balance_milli"] -= price
    p["revision"] += 1
    _recover(p, now)
    conn.execute(update(play_sessions).where(play_sessions.c.uid == p["uid"]).values(active=0))
    _save_items(conn, p["uid"], items, now)
    _save_player(conn, p, now)
    return _snapshot(conn, p, now, items)


@bp.get("/api/fan-game/state")
@bp.get("/api/fan-game/catalog")
def state():
    return _execute("state")


@bp.post("/api/fan-game/session/start")
def start():
    return _execute("start")


@bp.post("/api/fan-game/session/report")
def report():
    return _execute("report")


@bp.post("/api/fan-game/purchase")
@bp.post("/api/fan-game/service")
def workshop():
    return _execute("workshop")
