"""Local, process-scoped DB-IP City Lite snapshots; never retain client IPs."""

import atexit
import ipaddress
import logging
import math
import os
import re
import threading
import time
from collections.abc import Mapping
from numbers import Real


GEO_FIELDS = (
    'geo_country_code', 'geo_country_name', 'geo_region', 'geo_city',
    'geo_continent_code', 'geo_latitude', 'geo_longitude',
    'geo_db_build_epoch', 'geo_status',
)
_logger = logging.getLogger(__name__)


def _snapshot(status, epoch=None):
    result = dict.fromkeys(GEO_FIELDS)
    result.update(geo_status=status, geo_db_build_epoch=epoch)
    return result


def _mapping(value):
    return value if isinstance(value, Mapping) else {}


def _name(value):
    names = _mapping(value)
    keys = sorted(key for key in names if isinstance(key, str) and key != 'en')
    for key in ['en', *keys]:
        name = names.get(key)
        if isinstance(name, str) and name.strip():
            return name.strip()[:255]
    return None


def _code(value, *, continent=False):
    if not isinstance(value, str):
        return None
    code = value.strip().upper()
    if not re.fullmatch(r'[A-Z]{2}', code):
        return None
    if continent and code not in {'AF', 'AN', 'AS', 'EU', 'NA', 'OC', 'SA'}:
        return None
    return code


def _coordinate(value, limit):
    if isinstance(value, bool) or not isinstance(value, Real):
        return None
    try:
        value = float(value)
    except (OverflowError, ValueError):
        return None
    return value if math.isfinite(value) and -limit <= value <= limit else None


def _record_snapshot(record, epoch):
    if not isinstance(record, Mapping):
        raise ValueError('Invalid MMDB record shape')
    result = _snapshot('ok', epoch)
    country = _mapping(record.get('country'))
    result['geo_country_code'] = _code(country.get('iso_code'))
    result['geo_country_name'] = _name(country.get('names'))
    result['geo_continent_code'] = _code(
        _mapping(record.get('continent')).get('code'), continent=True
    )
    result['geo_city'] = _name(_mapping(record.get('city')).get('names'))
    subdivisions = record.get('subdivisions')
    # Support both the supplied mapping shape and DB-IP-compatible arrays.
    if isinstance(subdivisions, list):
        subdivisions = subdivisions[0] if subdivisions else None
    result['geo_region'] = _name(_mapping(subdivisions).get('names'))
    location = _mapping(record.get('location'))
    latitude = _coordinate(location.get('latitude'), 90)
    longitude = _coordinate(location.get('longitude'), 180)
    if latitude is not None and longitude is not None:
        result.update(geo_latitude=latitude, geo_longitude=longitude)
    return result


class GeoIPLookup:
    """One lazy reader per process, with serialized lookup/close and cached failure."""

    def __init__(self, path):
        self.path = path
        self._pid = os.getpid()
        self._lock = threading.Lock()
        self._reader = None
        self._initialized = False
        self._epoch = None
        self._last_warning = None

    def _warn(self, message):
        now = time.monotonic()
        if self._last_warning is None or now - self._last_warning >= 300:
            _logger.warning(message)
            self._last_warning = now

    def _after_fork(self):
        # The inherited lock may have been held by a thread absent in the child.
        self._lock = threading.Lock()
        if self._reader is not None:
            try:
                self._reader.close()
            except Exception:
                pass
        self._reader = None
        self._initialized = False
        self._epoch = None
        self._last_warning = None
        self._pid = os.getpid()

    def _initialize(self):
        self._initialized = True
        if not self.path:
            return
        reader = None
        try:
            import maxminddb

            reader = maxminddb.open_database(self.path)
            epoch = reader.metadata().build_epoch
            if type(epoch) is int and 0 <= epoch <= 2**64 - 1:
                self._epoch = epoch
            self._reader = reader
        except Exception:
            if reader is not None:
                try:
                    reader.close()
                except Exception:
                    pass
            self._warn('GeoIP database unavailable; geolocation disabled until worker restart')

    def lookup(self, client_ip):
        try:
            if not isinstance(client_ip, str) or '%' in client_ip:
                raise ValueError()
            address = ipaddress.ip_address(client_ip)
            if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
                address = address.ipv4_mapped
        except ValueError:
            return _snapshot('invalid_ip')
        if not address.is_global or address.is_multicast or address.is_reserved:
            return _snapshot('non_public')
        if self._pid != os.getpid():
            self._after_fork()
        with self._lock:
            if not self._initialized:
                self._initialize()
            if not self.path:
                return _snapshot('disabled')
            if self._reader is None:
                return _snapshot('unavailable')
            try:
                record = self._reader.get(str(address))
                if record is None:
                    return _snapshot('not_found', self._epoch)
                return _record_snapshot(record, self._epoch)
            except Exception:
                # Do not log exceptions: third-party error text can contain the IP.
                self._warn('GeoIP lookup failed; visit will retain an empty geolocation snapshot')
                return _snapshot('lookup_error', self._epoch)

    def close(self):
        if self._pid != os.getpid():
            self._after_fork()
        with self._lock:
            if self._reader is not None:
                try:
                    self._reader.close()
                except Exception:
                    self._warn('GeoIP reader close failed')
                self._reader = None
            # Closing does not trigger a per-request reopen.
            self._initialized = True


_service = GeoIPLookup(os.getenv('GEOIP_MMDB_PATH', ''))
if hasattr(os, 'register_at_fork'):
    os.register_at_fork(after_in_child=_service._after_fork)
atexit.register(_service.close)


def lookup(client_ip):
    return _service.lookup(client_ip)
