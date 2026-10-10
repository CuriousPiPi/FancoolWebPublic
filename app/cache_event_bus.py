from __future__ import annotations

import json
import logging
import os
import threading
from typing import Callable

from sqlalchemy import BigInteger, Column, DateTime, Integer, MetaData, String, Table, Text, func, insert, select

EVENT_WARM_SCORES = 'warm_scores'
EVENT_REFRESH_SCORING_VISIBILITY = 'refresh_scoring_visibility'

_metadata = MetaData()
_id_type = BigInteger().with_variant(Integer, 'sqlite')
_events_table = Table(
    'app_cache_events',
    _metadata,
    Column('id', _id_type, primary_key=True, autoincrement=True),
    Column('event_type', String(64), nullable=False),
    Column('payload_json', Text, nullable=False),
    Column('created_at', DateTime(), nullable=False, server_default=func.now()),
)

_engine = None
_logger = logging.getLogger(__name__)
_table_ready_engines: set[int] = set()
_table_ready_lock = threading.Lock()
_consumer_started = False
_consumer_started_lock = threading.Lock()
_consumer_wakeup = threading.Event()
_DEFAULT_POLL_INTERVAL_SEC = 2.0
# Clamp the consumer sleep floor so a bad env var cannot drive a tight polling
# loop that hammers the shared database.
_MIN_POLL_INTERVAL_SEC = 0.2
_poll_interval_sec_value = _DEFAULT_POLL_INTERVAL_SEC


def setup(engine, logger=None) -> None:
    global _engine, _logger, _poll_interval_sec_value
    _engine = engine
    if logger is not None:
        _logger = logger
    _poll_interval_sec_value = _load_poll_interval_sec()


def _resolve_engine(engine=None):
    eng = engine or _engine
    if eng is None:
        raise RuntimeError('cache_event_bus engine is not configured')
    return eng


def _resolve_logger(logger=None):
    return logger or _logger


def _load_poll_interval_sec() -> float:
    raw = (os.getenv('APP_CACHE_EVENT_POLL_INTERVAL_SEC') or '').strip()
    try:
        value = float(raw) if raw else _DEFAULT_POLL_INTERVAL_SEC
    except (TypeError, ValueError):
        value = _DEFAULT_POLL_INTERVAL_SEC
    return max(_MIN_POLL_INTERVAL_SEC, value)


def ensure_table(engine=None) -> None:
    eng = _resolve_engine(engine)
    engine_key = id(eng)
    if engine_key in _table_ready_engines:
        return
    with _table_ready_lock:
        if engine_key in _table_ready_engines:
            return
        _metadata.create_all(bind=eng, tables=[_events_table], checkfirst=True)
        _table_ready_engines.add(engine_key)


def get_latest_event_id(*, engine=None) -> int:
    eng = _resolve_engine(engine)
    ensure_table(eng)
    with eng.begin() as conn:
        value = conn.execute(select(func.max(_events_table.c.id))).scalar()
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def publish(event_type: str, payload: dict | None = None, *, engine=None, logger=None) -> int:
    eng = _resolve_engine(engine)
    ensure_table(eng)
    event_type = str(event_type or '').strip()
    if not event_type:
        raise ValueError('event_type is required')
    payload_json = json.dumps(payload or {}, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    with eng.begin() as conn:
        result = conn.execute(
            insert(_events_table).values(
                event_type=event_type,
                payload_json=payload_json,
            )
        )
        inserted_pk = result.inserted_primary_key[0] if result.inserted_primary_key else None
    if inserted_pk is None:
        raise RuntimeError(f'cache_event_bus publish failed to return a primary key for event_type={event_type!r}')
    event_id = int(inserted_pk)
    _resolve_logger(logger).info('[cache_event_bus] published event_id=%s type=%s', event_id, event_type)
    return event_id


def fetch_events_after(last_event_id: int, *, limit: int = 100, engine=None) -> list[dict]:
    eng = _resolve_engine(engine)
    ensure_table(eng)
    stmt = (
        select(
            _events_table.c.id,
            _events_table.c.event_type,
            _events_table.c.payload_json,
            _events_table.c.created_at,
        )
        .where(_events_table.c.id > int(last_event_id or 0))
        .order_by(_events_table.c.id.asc())
        .limit(max(1, int(limit or 100)))
    )
    with eng.begin() as conn:
        rows = conn.execute(stmt).mappings().all()
    events = []
    for row in rows:
        try:
            payload = json.loads(row['payload_json'] or '{}')
        except (TypeError, json.JSONDecodeError) as exc:
            _logger.warning('[cache_event_bus] invalid payload_json for event_id=%s: %s', row['id'], exc)
            payload = {}
        if not isinstance(payload, dict):
            payload = {'value': payload}
        events.append({
            'id': int(row['id']),
            'event_type': row['event_type'],
            'payload': payload,
            'created_at': row['created_at'],
        })
    return events


def consume_available(process_event: Callable[[dict], None], last_event_id: int, *, limit: int = 100, engine=None) -> int:
    current = int(last_event_id or 0)
    for event in fetch_events_after(current, limit=limit, engine=engine):
        try:
            process_event(event)
        except Exception as exc:
            _logger.warning(
                '[cache_event_bus] process_event failed for event_id=%s type=%s: %s',
                event.get('id'),
                event.get('event_type'),
                exc,
            )
        current = int(event['id'])
    return current


def wake_consumer() -> None:
    _consumer_wakeup.set()


def start_background_consumer(process_event: Callable[[dict], None], *, engine=None, logger=None, thread_name: str = 'cache-event-consumer') -> None:
    eng = _resolve_engine(engine)
    ensure_table(eng)
    log = _resolve_logger(logger)
    global _consumer_started
    with _consumer_started_lock:
        if _consumer_started:
            return
        _consumer_started = True

    def _run():
        last_event_id = get_latest_event_id(engine=eng)
        while True:
            try:
                last_event_id = consume_available(process_event, last_event_id, engine=eng)
            except Exception as exc:
                log.warning('[cache_event_bus] consumer poll failed: %s', exc)
            _consumer_wakeup.wait(timeout=max(_MIN_POLL_INTERVAL_SEC, _poll_interval_sec_value))
            _consumer_wakeup.clear()

    threading.Thread(target=_run, daemon=True, name=thread_name).start()
