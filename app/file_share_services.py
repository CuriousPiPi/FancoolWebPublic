"""
File share services: storage helpers for file-share thumbnails.

Storage layout (all under FILE_SHARE_ROOT):
  {FILE_SHARE_ROOT}/
    originals/
      file_<file_id>.<ext>
    thumbs/
      file_<file_id>.<ext>

FILE_SHARE_ROOT defaults to the CWD-relative path ``./file_share``,
overridable via the ``FILE_SHARE_ROOT`` environment variable.
"""

from __future__ import annotations

import os
import re
import logging
from typing import Optional, Dict

# Re-use allowed extensions and thumbnail logic from gallery_services
from app.gallery_services import (
    ALLOWED_EXTENSIONS,
    _make_thumbnail,
    _safe_remove,
)

_LOGGER = logging.getLogger(__name__)

_FILENAME_RE = re.compile(
    r'^file_(\d+)\.(jpg|jpeg|png|gif|webp|avif)$', re.IGNORECASE
)


def file_share_root() -> str:
    """Return the absolute file-share root directory."""
    root_path = os.getenv('FILE_SHARE_ROOT', './file_share')
    return os.path.abspath(root_path)


def originals_dir() -> str:
    return os.path.join(file_share_root(), 'originals')


def thumbs_dir() -> str:
    return os.path.join(file_share_root(), 'thumbs')


def _ensure_dirs():
    os.makedirs(originals_dir(), exist_ok=True)
    os.makedirs(thumbs_dir(), exist_ok=True)


def allowed_ext(filename: str) -> Optional[str]:
    """Return the normalised (lower-case) extension if allowed, else None."""
    if '.' not in filename:
        return None
    ext = filename.rsplit('.', 1)[1].lower()
    return ext if ext in ALLOWED_EXTENSIONS else None


def is_valid_filename(filename: str) -> bool:
    """Return True if *filename* matches the file-share naming pattern file_<id>.<ext>."""
    return bool(_FILENAME_RE.match(filename))


def filename_for(file_id: int, ext: str) -> str:
    """Return the canonical filename for a given file_id and extension."""
    return f'file_{file_id}.{ext.lower()}'


def save_thumbnail(file_id: int, file_stream, original_filename: str) -> Dict:
    """
    Save *file_stream* as the thumbnail for *file_id*.  Generates a
    resized thumbnail automatically.

    Returns a dict with 'thumb_filename', 'thumb_url', 'original_url'.
    Raises ValueError for unsupported formats.
    """
    ext = allowed_ext(original_filename)
    if ext is None:
        raise ValueError(f'Unsupported image format: {original_filename!r}')

    _ensure_dirs()
    fname = filename_for(file_id, ext)

    orig_path = os.path.join(originals_dir(), fname)
    thumb_path = os.path.join(thumbs_dir(), fname)

    # Remove any existing thumbnail for this file_id (different extension)
    _remove_existing_for_file_id(file_id, skip_fname=fname)

    import shutil
    with open(orig_path, 'wb') as f:
        shutil.copyfileobj(file_stream, f)

    try:
        _make_thumbnail(orig_path, thumb_path, ext)
    except Exception:
        _safe_remove(thumb_path)
        _safe_remove(orig_path)
        raise

    return {
        'thumb_filename': fname,
        'thumb_url': f'/api/file-share/thumb/{fname}',
        'original_url': f'/api/file-share/original/{fname}',
    }


def _remove_existing_for_file_id(file_id: int, *, skip_fname: str | None = None):
    """Remove all existing original/thumb files for file_id (to replace old thumbnails)."""
    for directory in (originals_dir(), thumbs_dir()):
        try:
            for entry in os.listdir(directory):
                m = _FILENAME_RE.match(entry)
                if m and int(m.group(1)) == file_id and entry != skip_fname:
                    _safe_remove(os.path.join(directory, entry))
        except FileNotFoundError:
            pass


def delete_thumbnail(file_id: int, thumb_filename: str) -> bool:
    """
    Delete the original and thumbnail for *thumb_filename*.
    Returns True if at least one file was deleted.
    """
    m = _FILENAME_RE.match(thumb_filename)
    if not m or int(m.group(1)) != file_id:
        raise ValueError(
            f'Filename {thumb_filename!r} does not belong to file_id {file_id}'
        )

    orig_path = os.path.join(originals_dir(), thumb_filename)
    thumb_path = os.path.join(thumbs_dir(), thumb_filename)

    found = False
    if os.path.isfile(orig_path):
        os.remove(orig_path)
        found = True
    if os.path.isfile(thumb_path):
        os.remove(thumb_path)
        found = True
    return found


def ensure_thumbnail(filename: str) -> bool:
    """
    Ensure thumbnail exists and is valid for *filename*.
    Returns True when a valid thumbnail is ready, otherwise False.
    """
    if not is_valid_filename(filename):
        return False

    orig_path = os.path.join(originals_dir(), filename)
    thumb_path = os.path.join(thumbs_dir(), filename)

    if os.path.isfile(thumb_path):
        return True

    if not os.path.isfile(orig_path):
        return False

    ext = allowed_ext(filename)
    if ext is None:
        return False

    try:
        _make_thumbnail(orig_path, thumb_path, ext)
        return os.path.isfile(thumb_path)
    except Exception as e:
        _safe_remove(thumb_path)
        _LOGGER.warning('File-share thumbnail self-heal failed for %s: %s', filename, e)
        return False
