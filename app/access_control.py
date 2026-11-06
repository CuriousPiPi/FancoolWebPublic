from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable


PASSWORD_BINDING_REQUIRED_MESSAGE = (
    '当前页面需要用户名和密码验证，但未检测到您的设备已绑定相关账号信息，请联系管理员。'
)


@dataclass
class AccessDecision:
    allowed: bool
    requires_password: bool
    reason_code: str | None = None
    message: str | None = None
    whitelist_row: dict | None = None


def _to_bool_env(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or '').strip().lower()
    if not raw:
        return default
    return raw in ('1', 'true', 'yes', 'y', 'on')


def get_app_policy_from_env() -> tuple[bool, bool]:
    require_uid = _to_bool_env('APP_REQUIRE_UID_WHITELIST', False)
    require_password = _to_bool_env('APP_REQUIRE_PASSWORD', False)
    return require_uid, require_password


def _parse_dt(v) -> datetime | None:
    if not v:
        return None
    if isinstance(v, datetime):
        return v
    if isinstance(v, str):
        txt = v.strip()
        if txt.endswith('Z'):
            txt = f'{txt[:-1]}+00:00'
        normalized = txt.replace(' ', 'T')
        try:
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
            return parsed
        except ValueError:
            pass
        txt = txt.replace('T', ' ')
        for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f'):
            try:
                return datetime.strptime(txt, fmt)
            except ValueError:
                continue
    return None


def _is_allow_until_valid(allow_until) -> bool:
    at = _parse_dt(allow_until)
    if at is None:
        return allow_until in (None, '', 0, '0')
    return datetime.now(timezone.utc).replace(tzinfo=None) <= at


def fetch_whitelist_row(fetch_all: Callable[[str, dict | None], list[dict]], uid: str) -> dict | None:
    rows = fetch_all(
        """
        SELECT
            id,
            user_identifier,
            label,
            is_active,
            allow_until,
            can_access_app,
            can_access_admin,
            app_password_bypass,
            admin_password_bypass,
            auth_user_id
        FROM user_access_whitelist
        WHERE user_identifier=:u
        LIMIT 1
        """,
        {'u': uid},
    )
    return rows[0] if rows else None


def evaluate_uid_access(
    fetch_all: Callable[[str, dict | None], list[dict]],
    uid: str,
    *,
    service: str,
    require_uid: bool,
    require_password: bool,
) -> AccessDecision:
    if not require_uid:
        # Password-only mode: no whitelist check; credential gate handled downstream.
        return AccessDecision(allowed=True, requires_password=require_password)

    row = fetch_whitelist_row(fetch_all, uid)
    if not row:
        return AccessDecision(False, require_password, 'UID_NOT_WHITELISTED', '当前设备不在访问白名单中。')
    if int(row.get('is_active') or 0) != 1:
        return AccessDecision(False, require_password, 'UID_INACTIVE', '当前设备白名单状态不可用。', row)
    if not _is_allow_until_valid(row.get('allow_until')):
        return AccessDecision(False, require_password, 'UID_EXPIRED', '当前设备访问权限已过期。', row)

    if service == 'admin':
        if int(row.get('can_access_admin') or 0) != 1:
            return AccessDecision(False, require_password, 'ADMIN_ACCESS_DENIED', '当前设备无后台访问权限。', row)
        row_password_bypass = int(row.get('admin_password_bypass') or 0) == 1
    else:
        if int(row.get('can_access_app') or 0) != 1:
            return AccessDecision(False, require_password, 'APP_ACCESS_DENIED', '当前设备无应用访问权限。', row)
        row_password_bypass = int(row.get('app_password_bypass') or 0) == 1

    if require_password:
        if row_password_bypass:
            # Device is marked to bypass credential verification even when the service gate is on.
            return AccessDecision(True, False, whitelist_row=row)
        if row.get('auth_user_id') in (None, ''):
            return AccessDecision(False, True, 'DEVICE_AUTH_BINDING_REQUIRED', PASSWORD_BINDING_REQUIRED_MESSAGE, row)
        return AccessDecision(True, True, whitelist_row=row)

    return AccessDecision(True, False, whitelist_row=row)


def fetch_auth_user_by_login(fetch_all: Callable[[str, dict | None], list[dict]], login_name: str) -> dict | None:
    rows = fetch_all(
        """
        SELECT id, login_name, password_hash, is_active, failed_attempts, locked_until
        FROM auth_user
        WHERE login_name=:ln
        LIMIT 1
        """,
        {'ln': login_name},
    )
    return rows[0] if rows else None


def is_auth_user_locked(row: dict) -> tuple[bool, int]:
    locked_until = _parse_dt((row or {}).get('locked_until'))
    if not locked_until:
        return False, 0
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    if now >= locked_until:
        return False, 0
    left = int((locked_until - now).total_seconds() // 60) + 1
    return True, max(left, 1)


def is_authenticated_user_allowed(decision: AccessDecision, auth_user_id: int | None) -> bool:
    if not decision.requires_password:
        return True
    if not decision.whitelist_row:
        # Password-only mode (no UID gate): any successfully authenticated user is allowed.
        return auth_user_id is not None
    bound = decision.whitelist_row.get('auth_user_id')
    if bound in (None, ''):
        return False
    try:
        return int(bound) == int(auth_user_id or 0)
    except (TypeError, ValueError):
        return False
