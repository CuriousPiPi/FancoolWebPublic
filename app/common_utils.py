"""
Common utility functions shared across app and admin servers.
This module consolidates duplicate code to improve maintainability.
"""
import hmac
import hashlib
from typing import Any, List
from flask import make_response, jsonify
from sqlalchemy import text


def sign_uid(value: str, secret_key: bytes | str) -> str:
    """
    Sign a user identifier with HMAC-SHA256.
    
    Args:
        value: The value to sign
        secret_key: The secret key (will be converted to bytes if string)
        
    Returns:
        Signed value in format "value.signature"
    """
    key = secret_key.encode() if not isinstance(secret_key, (bytes, bytearray)) else secret_key
    sig = hmac.new(key, value.encode('utf-8'), hashlib.sha256).hexdigest()[:16]
    return f"{value}.{sig}"


def unsign_uid(token: str, secret_key: bytes | str) -> str | None:
    """
    Verify and extract a signed user identifier.
    
    Args:
        token: The signed token
        secret_key: The secret key used for signing
        
    Returns:
        The original value if signature is valid, None otherwise
    """
    if not token:
        return None
    parts = token.split('.', 1)
    if len(parts) != 2:
        return None
    raw, sig = parts
    expect = sign_uid(raw, secret_key).split('.', 1)[1]
    if hmac.compare_digest(sig, expect):
        return raw
    return None


def make_success_response(data: Any = None, message: str | None = None,
                          meta: dict | None = None, http_status: int = 200):
    """
    Create a standardized success response.
    
    Args:
        data: Response data
        message: Optional message
        meta: Optional metadata
        http_status: HTTP status code (default 200)
        
    Returns:
        Flask response object
    """
    payload = {
        'success': True,
        'data': data,
        'message': message,
        'meta': meta or {}
    }
    return make_response(jsonify(payload), http_status)


def make_error_response(error_code: str, error_message: str,
                        http_status: int = 400, meta: dict | None = None):
    """
    Create a standardized error response.
    
    Args:
        error_code: Error code identifier
        error_message: Human-readable error message
        http_status: HTTP status code (default 400)
        meta: Optional metadata
        
    Returns:
        Flask response object
    """
    payload = {
        'success': False,
        'error_code': error_code,
        'error_message': error_message,
        'data': None,
        'meta': meta or {}
    }
    return make_response(jsonify(payload), http_status)


def db_fetch_all(engine, sql: str, params: dict = None) -> List[dict]:
    """
    Execute a SELECT query and return all results as dicts.
    
    Args:
        engine: SQLAlchemy engine
        sql: SQL query string
        params: Query parameters (default None)
        
    Returns:
        List of result rows as dictionaries
    """
    with engine.begin() as conn:
        rows = conn.execute(text(sql), params or {})
        return [dict(r._mapping) for r in rows]


def db_exec_write(engine, sql: str, params: dict = None):
    """
    Execute a write query (INSERT, UPDATE, DELETE).
    
    Args:
        engine: SQLAlchemy engine
        sql: SQL query string
        params: Query parameters (default None)
    """
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})
