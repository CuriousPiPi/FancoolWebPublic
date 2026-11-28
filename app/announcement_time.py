import hashlib
import json
from datetime import datetime
from zoneinfo import ZoneInfo

ANNOUNCEMENT_TIMEZONE = ZoneInfo('Asia/Shanghai')


def announcement_now():
    return datetime.now(ANNOUNCEMENT_TIMEZONE).replace(tzinfo=None)


def announcement_iso(value):
    if value is None:
        return None
    if not isinstance(value, datetime):
        value = datetime.fromisoformat(str(value))
    if value.tzinfo is None:
        value = value.replace(tzinfo=ANNOUNCEMENT_TIMEZONE)
    return value.isoformat()


def announcement_database_time(value):
    if not isinstance(value, str) or not value.strip():
        raise ValueError('时间必须包含时区')
    try:
        parsed = datetime.fromisoformat(value.strip().replace('Z', '+00:00'))
    except ValueError as exc:
        raise ValueError('时间格式无效') from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError('时间必须包含时区')
    if parsed.microsecond:
        raise ValueError('时间精度不能小于一秒')
    try:
        result = parsed.astimezone(ANNOUNCEMENT_TIMEZONE).replace(tzinfo=None)
    except (OverflowError, ValueError) as exc:
        raise ValueError('时间超出支持范围') from exc
    if result.year < 1000:
        raise ValueError('时间超出数据库支持范围')
    return result


def build_announcement_state(active_rows, upcoming_rows):
    def serialize(row):
        return {
            'id': row['id'],
            'content_text_zh': row.get('content_text_zh'),
            'content_text_en': row.get('content_text_en'),
            'starts_at': announcement_iso(row.get('starts_at')),
            'ends_at': announcement_iso(row.get('ends_at')),
        }

    active = [serialize(row) for row in active_rows]
    upcoming = [serialize(row) for row in upcoming_rows]
    changes = [row['ends_at'] for row in active if row['ends_at']]
    changes.extend(row['starts_at'] for row in upcoming if row['starts_at'])
    fingerprint = hashlib.sha256(
        json.dumps([active, upcoming], ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    ).hexdigest()[:16]
    return {
        'active': active,
        'upcoming': upcoming,
        'fingerprint': fingerprint,
        'next_change_at': min(changes, key=datetime.fromisoformat) if changes else None,
    }
