"""
Gallery services: storage helpers for per-model image gallery.

Storage layout (all under GALLERY_ROOT):
  {GALLERY_ROOT}/
    originals/
      {model_id}_{index}.{ext}
    thumbs/
      {model_id}_{index}.{ext}

Gallery root follows the same positioning strategy as ``curve_cache_dir()``:
the default is the CWD-relative path ``./gallery``, overridable via the
``GALLERY_ROOT`` environment variable.  The resolved path is always
absolutised (via ``os.path.abspath``) so that ``send_from_directory`` and
other callers receive a stable absolute path regardless of how the server
was started (Jupyter notebook, gunicorn, etc.).

Filenames are standardised to {model_id}_{index}.{ext} by the backend; the
original uploaded filename is ignored.  _1 is the primary image.  New uploads
receive max_index + 1.  Deleting an image does NOT re-pack the remaining
indices.
"""

from __future__ import annotations

import os
import re
import logging
from typing import List, Dict, Optional

ALLOWED_EXTENSIONS = {'jpg', 'jpeg', 'png', 'gif', 'webp', 'avif'}
_DEFAULT_THUMB_SIZE = (480, 480)
_LOGGER = logging.getLogger(__name__)

_FILENAME_RE = re.compile(r'^(\d+)_(\d+)\.(jpg|jpeg|png|gif|webp|avif)$', re.IGNORECASE)

# Allowed extensions for BV poster cache (order matters: checked in this order when looking up cached files).
BV_POSTER_ALLOWED_EXTENSIONS: tuple = ('jpg', 'jpeg', 'png', 'webp', 'gif', 'avif')


def _parse_thumb_size(raw: Optional[str]) -> Optional[tuple[int, int]]:
    """Parse GALLERY_THUMB_SIZE from '320', '320x320', or '480x360'."""
    if raw is None:
        return None
    text = raw.strip().lower()
    if not text:
        return None
    if 'x' in text:
        parts = text.split('x', 1)
        if len(parts) != 2:
            return None
        w_str, h_str = parts[0].strip(), parts[1].strip()
    else:
        w_str = text
        h_str = text
    try:
        width = int(w_str)
        height = int(h_str)
    except ValueError:
        return None
    if width <= 0 or height <= 0:
        return None
    return width, height


def _load_thumb_size() -> tuple[int, int]:
    raw = os.getenv('GALLERY_THUMB_SIZE', '')
    parsed = _parse_thumb_size(raw)
    if parsed is not None:
        return parsed
    if raw.strip():
        _LOGGER.warning(
            "Invalid GALLERY_THUMB_SIZE=%r; fallback to default %sx%s",
            raw, _DEFAULT_THUMB_SIZE[0], _DEFAULT_THUMB_SIZE[1]
        )
    return _DEFAULT_THUMB_SIZE


THUMB_SIZE = _load_thumb_size()  # max bounding box for thumbnails


def gallery_root() -> str:
    """Return the absolute gallery root directory (matching curve_cache_dir style)."""
    root_path = os.getenv("GALLERY_ROOT", "./gallery")
    return os.path.abspath(root_path)


def originals_dir() -> str:
    return os.path.join(gallery_root(), 'originals')


def thumbs_dir() -> str:
    return os.path.join(gallery_root(), 'thumbs')


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
    """Return True if *filename* matches the gallery naming pattern {model_id}_{index}.{ext}."""
    return bool(_FILENAME_RE.match(filename))


def _list_originals_for_model(model_id: int) -> List[Dict]:
    """
    Scan originals_dir for files matching {model_id}_{index}.{ext} and return
    a list sorted by index ascending.
    """
    prefix = f'{model_id}_'
    items: List[Dict] = []
    try:
        for fname in os.listdir(originals_dir()):
            m = _FILENAME_RE.match(fname)
            if m and int(m.group(1)) == model_id:
                index = int(m.group(2))
                ext = m.group(3).lower()
                items.append({
                    'filename': fname,
                    'index': index,
                    'ext': ext,
                    'is_primary': index == 1,
                })
    except FileNotFoundError:
        pass
    items.sort(key=lambda x: x['index'])
    return items


def list_gallery(model_id: int) -> List[Dict]:
    """
    Return gallery metadata for *model_id*, sorted by index ascending.

    Each entry: {filename, index, ext, is_primary, original_url, thumb_url}
    URL fields are relative paths intended to be exposed via API.
    """
    items = _list_originals_for_model(model_id)
    for item in items:
        fn = item['filename']
        item['original_url'] = f'/api/gallery/original/{fn}'
        item['thumb_url'] = f'/api/gallery/thumb/{fn}'
    return items


def next_index(model_id: int) -> int:
    """Return current_max_index + 1 for model_id.  Returns 1 if no images exist."""
    items = _list_originals_for_model(model_id)
    if not items:
        return 1
    return max(i['index'] for i in items) + 1


def save_image(model_id: int, file_stream, original_filename: str) -> Dict:
    """
    Save *file_stream* as the next image for *model_id*.  Generates a
    thumbnail automatically.

    Returns the new item dict (same shape as list_gallery entries).
    Raises ValueError for unsupported formats.
    """
    ext = allowed_ext(original_filename)
    if ext is None:
        raise ValueError(f'Unsupported image format: {original_filename!r}')

    _ensure_dirs()
    index = next_index(model_id)
    fname = f'{model_id}_{index}.{ext}'

    orig_path = os.path.join(originals_dir(), fname)
    thumb_path = os.path.join(thumbs_dir(), fname)

    # Save original
    data = file_stream.read()
    with open(orig_path, 'wb') as f:
        f.write(data)

    # Generate thumbnail
    try:
        _make_thumbnail(orig_path, thumb_path, ext)
    except Exception:
        _safe_remove(thumb_path)
        _safe_remove(orig_path)
        raise

    return {
        'filename': fname,
        'index': index,
        'ext': ext,
        'is_primary': index == 1,
        'original_url': f'/api/gallery/original/{fname}',
        'thumb_url': f'/api/gallery/thumb/{fname}',
    }


def _make_thumbnail(src: str, dst: str, ext: str):
    """
    Create a thumbnail at *dst* from *src*.
    Raises RuntimeError with details if thumbnail generation fails.
    """
    try:
        from PIL import Image
    except Exception as e:
        raise RuntimeError(f'Pillow is required for thumbnail generation: {e}') from e

    try:
        with Image.open(src) as img:
            # Preserve alpha channel for formats that support it; convert
            # JPEG-destined images to RGB to avoid black-background artifacts.
            if ext.lower() in ('jpg', 'jpeg'):
                if img.mode in ('RGBA', 'LA', 'P'):
                    bg = Image.new('RGB', img.size, (255, 255, 255))
                    alpha = img.convert('RGBA').split()[3]
                    bg.paste(img.convert('RGBA'), mask=alpha)
                    img = bg
                else:
                    img = img.convert('RGB')
            img.thumbnail(THUMB_SIZE, Image.LANCZOS)
            save_fmt = _pil_format(ext)
            # Keep save args format-specific to avoid codec-specific keyword errors.
            save_kwargs = {'format': save_fmt}
            if save_fmt in ('JPEG', 'WEBP'):
                save_kwargs.update({'quality': 82, 'optimize': True})
            elif save_fmt == 'PNG':
                save_kwargs.update({'optimize': True})
            img.save(dst, **save_kwargs)
    except Exception as e:
        _safe_remove(dst)
        _LOGGER.exception('Failed to generate thumbnail src=%s dst=%s ext=%s', src, dst, ext)
        raise RuntimeError(f'Failed to generate thumbnail for {os.path.basename(src)}: {e}') from e


def _safe_remove(path: str):
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        _LOGGER.warning('Failed to remove file during gallery cleanup: %s', path, exc_info=True)


def _pil_format(ext: str) -> str:
    mapping = {
        'jpg': 'JPEG', 'jpeg': 'JPEG',
        'png': 'PNG',
        'gif': 'GIF',
        'webp': 'WEBP',
        'avif': 'AVIF',
    }
    return mapping.get(ext.lower(), 'JPEG')


def _thumbnail_state(path: str) -> tuple[bool, str]:
    if not os.path.isfile(path):
        return False, 'missing'
    try:
        from PIL import Image
        with Image.open(path) as img:
            width, height = img.size
            img.verify()
    except Exception as e:
        return False, f'invalid: {e}'

    max_w, max_h = THUMB_SIZE
    if width > max_w or height > max_h:
        return False, f'oversize: {width}x{height} > {max_w}x{max_h}'
    return True, 'ok'


def ensure_thumbnail(filename: str) -> bool:
    """
    Ensure thumbnail exists and is valid for *filename*.
    Returns True when a valid thumbnail is ready, otherwise False.
    """
    if not is_valid_filename(filename):
        return False

    orig_path = os.path.join(originals_dir(), filename)
    thumb_path = os.path.join(thumbs_dir(), filename)

    ok, reason = _thumbnail_state(thumb_path)
    if ok:
        return True
    if not os.path.isfile(orig_path):
        _LOGGER.warning(
            'Thumbnail unavailable and original missing for %s (thumb_state=%s)',
            filename, reason
        )
        return False

    _safe_remove(thumb_path)
    os.makedirs(os.path.dirname(thumb_path), exist_ok=True)

    ext = allowed_ext(filename)
    if ext is None:
        _LOGGER.warning('Thumbnail self-heal skipped due to invalid extension for %s', filename)
        return False
    try:
        _make_thumbnail(orig_path, thumb_path, ext)
    except Exception as e:
        _safe_remove(thumb_path)
        _LOGGER.warning('Thumbnail self-heal failed for %s: %s', filename, e)
        return False

    ok, reason = _thumbnail_state(thumb_path)
    if ok:
        return True
    _safe_remove(thumb_path)
    _LOGGER.warning('Thumbnail self-heal produced invalid thumb for %s: %s', filename, reason)
    return False


def delete_image(model_id: int, filename: str) -> bool:
    """
    Delete the original and thumbnail for *filename*, if they belong to
    *model_id*.  Returns True if the original was found and deleted.

    Raises ValueError for filenames that don't match the expected pattern or
    don't belong to model_id.
    """
    m = _FILENAME_RE.match(filename)
    if not m or int(m.group(1)) != model_id:
        raise ValueError(f'Filename {filename!r} does not belong to model {model_id}')

    orig_path = os.path.join(originals_dir(), filename)
    thumb_path = os.path.join(thumbs_dir(), filename)

    found = False
    if os.path.isfile(orig_path):
        os.remove(orig_path)
        found = True
    if os.path.isfile(thumb_path):
        os.remove(thumb_path)

    return found


# =========================================
# BV Poster Cache (bilibili_posters/)
# =========================================

def bilibili_posters_dir() -> str:
    """Return the absolute path to the BV poster cache directory."""
    return os.path.join(gallery_root(), 'bilibili_posters')


def get_cached_bv_poster_url(bvid: str) -> Optional[str]:
    """
    Return the local API URL for a cached BV poster, or None if not cached.
    Scans the bilibili_posters directory for a file named {bvid}.{ext}
    using the allowed extension list in order.
    """
    if not bvid or not bvid.upper().startswith('BV') or len(bvid) < 3 or not all(c.isalnum() for c in bvid):
        return None
    poster_dir = bilibili_posters_dir()
    for ext in BV_POSTER_ALLOWED_EXTENSIONS:
        fname = f'{bvid}.{ext}'
        if os.path.isfile(os.path.join(poster_dir, fname)):
            return f'/api/gallery/bilibili-poster/{fname}'
    return None


def cache_bv_poster(bvid: str, data: bytes, ext: str) -> Optional[str]:
    """
    Save BV poster *data* as ``{bvid}.{ext}`` in the bilibili_posters directory.

    Returns the local API URL on success, or None if the extension is not
    allowed or the write fails.  Does not transcode; the caller is responsible
    for providing already-downloaded image bytes in an allowed format.
    """
    if not bvid or not data:
        return None
    if not bvid.upper().startswith('BV') or len(bvid) < 3 or not all(c.isalnum() for c in bvid):
        _LOGGER.warning('cache_bv_poster: invalid bvid=%r', bvid)
        return None
    ext = ext.lower()
    if ext not in BV_POSTER_ALLOWED_EXTENSIONS:
        _LOGGER.warning('cache_bv_poster: unsupported extension %r for bvid=%s', ext, bvid)
        return None
    poster_dir = bilibili_posters_dir()
    try:
        os.makedirs(poster_dir, exist_ok=True)
    except Exception:
        _LOGGER.warning('cache_bv_poster: failed to create dir %s', poster_dir, exc_info=True)
        return None
    fname = f'{bvid}.{ext}'
    path = os.path.join(poster_dir, fname)
    try:
        import tempfile
        fd, tmp_path = tempfile.mkstemp(prefix=f'{bvid}.', suffix=f'.{ext}.tmp', dir=poster_dir)
        try:
            with os.fdopen(fd, 'wb') as f:
                f.write(data)
            os.replace(tmp_path, path)
        finally:
            _safe_remove(tmp_path)
        return f'/api/gallery/bilibili-poster/{fname}'
    except Exception:
        _LOGGER.warning('cache_bv_poster: failed to write %s', path, exc_info=True)
        _safe_remove(path)
        return None


def cleanup_orphan_bv_posters(active_bvids: set) -> int:
    """
    Delete files in the bilibili_posters directory whose BV ID is not in
    *active_bvids*.  Files whose extension is not in BV_POSTER_ALLOWED_EXTENSIONS
    are ignored (neither deleted nor counted).

    Returns the number of poster files deleted.
    """
    poster_dir = bilibili_posters_dir()
    if not os.path.isdir(poster_dir):
        return 0
    deleted = 0
    try:
        entries = os.listdir(poster_dir)
    except Exception:
        _LOGGER.warning('cleanup_orphan_bv_posters: failed to list %s', poster_dir, exc_info=True)
        return 0
    for fname in entries:
        parts = fname.rsplit('.', 1)
        if len(parts) != 2:
            continue
        bvid_part, ext = parts[0], parts[1].lower()
        if ext not in BV_POSTER_ALLOWED_EXTENSIONS:
            continue
        if bvid_part not in active_bvids:
            _safe_remove(os.path.join(poster_dir, fname))
            deleted += 1
    return deleted


# =========================================
# BV Poster URL / filename helpers
# =========================================

def ext_from_content_type(content_type: Optional[str]) -> str:
    """Return a normalised image extension from a Content-Type header, or ''."""
    ct = ((content_type or '').split(';')[0]).strip().lower()
    mapping = {
        'image/jpeg': 'jpg',
        'image/jpg': 'jpg',
        'image/png': 'png',
        'image/gif': 'gif',
        'image/webp': 'webp',
        'image/avif': 'avif',
    }
    return mapping.get(ct, '')


def ext_from_url(url: str) -> str:
    """Return a normalised image extension from a URL path, or ''."""
    import urllib.parse
    path = urllib.parse.urlparse(url or '').path
    if '.' not in path:
        return ''
    ext = path.rsplit('.', 1)[1].lower().split('?')[0]
    return ext if ext in BV_POSTER_ALLOWED_EXTENSIONS else ''


def is_valid_bv_poster_filename(filename: str) -> bool:
    """Return True if *filename* is a safe BV poster filename: {BVxxx}.{allowed_ext}.

    Rejects path-traversal characters and filenames with unsupported extensions.
    """
    if not filename or '/' in filename or '\\' in filename:
        return False
    parts = filename.rsplit('.', 1)
    if len(parts) != 2:
        return False
    bvid_part, ext = parts[0], parts[1].lower()
    if ext not in BV_POSTER_ALLOWED_EXTENSIONS:
        return False
    # BV IDs must start with 'BV' (case-insensitive) followed by at least one alphanumeric char.
    if not bvid_part.upper().startswith('BV') or len(bvid_part) < 3:
        return False
    return all(c.isalnum() for c in bvid_part)
