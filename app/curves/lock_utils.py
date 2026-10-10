# -*- coding: utf-8 -*-
"""
lock_utils: Cross-platform file-locking helpers.

On Linux / macOS (Gunicorn production), this uses fcntl.flock for genuine
advisory exclusive locking so that only one worker at a time runs expensive
startup work.

On Windows / Jupyter (local testing), fcntl is not available.  The module
falls back to a no-op stub so that import never fails and the main code path
continues to run without real multi-process protection (which is acceptable
because local testing is single-process).

Usage
-----
    from app.curves.lock_utils import startup_lock

    with startup_lock('/tmp/my_startup.lock') as acquired:
        if acquired:
            # This process won the lock race — do the work.
            do_expensive_startup()
        else:
            # Another process beat us to it — skip or wait.
            pass
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Generator

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Safe fcntl import — no-op fallback on Windows / environments without fcntl
# ---------------------------------------------------------------------------
try:
    import fcntl as _fcntl
    _FCNTL_AVAILABLE = True
except ImportError:
    _fcntl = None  # type: ignore[assignment]
    _FCNTL_AVAILABLE = False
    log.debug(
        "fcntl not available (Windows / Jupyter environment); "
        "file-locking will use no-op fallback."
    )


def try_lock_exclusive_nb(fileobj) -> bool:
    """Attempt to acquire a non-blocking exclusive lock on *fileobj*.

    Returns True if the lock was acquired, False if it is already held by
    another process.  Always returns True on platforms without fcntl.
    """
    if not _FCNTL_AVAILABLE:
        return True
    try:
        _fcntl.flock(fileobj, _fcntl.LOCK_EX | _fcntl.LOCK_NB)
        return True
    except BlockingIOError:
        return False
    except Exception as exc:
        log.warning("try_lock_exclusive_nb: unexpected error: %s", exc)
        return True  # fail-open so the caller is not blocked forever


def unlock(fileobj) -> None:
    """Release the lock held on *fileobj*.  No-op when fcntl is unavailable."""
    if not _FCNTL_AVAILABLE:
        return
    try:
        _fcntl.flock(fileobj, _fcntl.LOCK_UN)
    except Exception as exc:
        log.debug("unlock: %s", exc)


@contextmanager
def startup_lock(lock_path: str) -> Generator[bool, None, None]:
    """Context manager that tries to acquire an exclusive startup lock.

    Yields True if this process won the lock (i.e. should do the work),
    False if another process is already doing it.

    The lock file is created at *lock_path* and the file handle is released
    (but not deleted) on exit so other processes can acquire it later.

    Example::

        with startup_lock('/tmp/fancool_startup.lock') as acquired:
            if acquired:
                do_expensive_startup_work()
            else:
                # Another process is handling it; sleep and check shared cache.
                pass
    """
    lf = None
    acquired = False
    try:
        lf = open(lock_path, "a")
        acquired = try_lock_exclusive_nb(lf)
        yield acquired
    except Exception as exc:
        log.warning("startup_lock(%s): error opening/locking: %s", lock_path, exc)
        yield True  # fail-open: let the caller proceed
    finally:
        if lf is not None:
            if acquired:
                unlock(lf)
            try:
                lf.close()
            except Exception:
                pass
