"""Bilibili metadata and explicitly requested poster downloads."""

import urllib.parse

import requests

from app import gallery_services as gallery

VIEW_API = 'https://api.bilibili.com/x/web-interface/view'
HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/123.0.0.0 Safari/537.36'
    ),
    'Referer': 'https://www.bilibili.com/',
}
MAX_POSTER_BYTES = 5 * 1024 * 1024
ALLOWED_POSTER_HOSTS = frozenset({
    'i0.hdslb.com', 'i1.hdslb.com', 'i2.hdslb.com',
    'archive.biliimg.com', 'pic.bstarstatic.com',
})


def fetch_meta(bvid):
    with requests.get(VIEW_API, params={'bvid': bvid}, headers=HEADERS, timeout=8) as resp:
        resp.raise_for_status()
        payload = resp.json()
    if payload.get('code') != 0 or not payload.get('data'):
        raise ValueError(payload.get('message') or 'Bilibili API error')
    data = payload['data']
    pages = data.get('pages') or []
    return {
        'bvid': data.get('bvid') or bvid,
        'title': data.get('title') or '',
        'poster': data.get('pic') or '',
        'cid': pages[0].get('cid') if pages else None,
    }


def download_poster(bvid):
    poster_url = fetch_meta(bvid).get('poster') or ''
    if not poster_url:
        raise ValueError('B站未返回封面地址')
    if poster_url.startswith('//'):
        poster_url = 'https:' + poster_url
    parsed = urllib.parse.urlparse(poster_url)
    if parsed.hostname not in ALLOWED_POSTER_HOSTS:
        raise ValueError('B站封面地址不在允许的域名列表内')
    safe_url = urllib.parse.urlunparse(('https', parsed.hostname, parsed.path, '', parsed.query, ''))
    with requests.get(safe_url, headers=HEADERS, timeout=10, stream=True, allow_redirects=False) as resp:
        if resp.is_redirect or resp.is_permanent_redirect:
            raise ValueError('B站封面地址发生重定向')
        resp.raise_for_status()
        length = resp.headers.get('Content-Length', '')
        if length.isdigit() and int(length) > MAX_POSTER_BYTES:
            raise ValueError('封面图片超过 5 MiB')
        chunks = []
        size = 0
        for chunk in resp.iter_content(chunk_size=65536):
            size += len(chunk)
            if size > MAX_POSTER_BYTES:
                raise ValueError('封面图片超过 5 MiB')
            chunks.append(chunk)
    data = b''.join(chunks)
    return data, gallery.validate_bv_poster_image(data)
