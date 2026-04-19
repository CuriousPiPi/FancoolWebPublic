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
from typing import List, Dict, Optional

ALLOWED_EXTENSIONS = {'jpg', 'jpeg', 'png', 'gif', 'webp', 'avif'}
THUMB_SIZE = (320, 320)  # max bounding box for thumbnails

_FILENAME_RE = re.compile(r'^(\d+)_(\d+)\.(jpg|jpeg|png|gif|webp|avif)$', re.IGNORECASE)


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
    _make_thumbnail(orig_path, thumb_path, ext)

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
    Create a thumbnail at *dst* from *src*.  Falls back to copying the original
    if Pillow is unavailable.
    """
    try:
        from PIL import Image
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
            img.save(dst, format=save_fmt, quality=82, optimize=True)
    except Exception:
        # Pillow not available or image corrupt – fall back to copying
        import shutil
        shutil.copy2(src, dst)


def _pil_format(ext: str) -> str:
    mapping = {
        'jpg': 'JPEG', 'jpeg': 'JPEG',
        'png': 'PNG',
        'gif': 'GIF',
        'webp': 'WEBP',
        'avif': 'AVIF',
    }
    return mapping.get(ext.lower(), 'JPEG')


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
