"""Bounded Loop manifests backed by immutable, deduplicated local archive objects."""
from __future__ import annotations

from collections import OrderedDict
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import zlib

from file_lock import advisory_file_lock
from secure_files import atomic_write_private_bytes, ensure_private_directory, secure_private_file

SERVICE = 'PB8LoopStorage'
FORMAT = 1
FANOUT = 64
MAX_MANIFEST_BYTES = 64 * 1024
MAX_OBJECT_BYTES = 4 * 1024 * 1024
CACHE_BYTES = 4 * 1024 * 1024
HOT_FIELDS = ('id', 'owner', 'status', 'phase', 'round', 'control_generation',
              'created_at', 'updated_at', 'started_at', 'deadline', 'cleanup_done')
CONFIG_FIELDS = {'config', 'executed_config', 'initial_config', 'overrides', 'initial_overrides'}


class LoopStorageError(OSError):
    """Local persistence failed; asking the model for another answer cannot repair it."""


def encoded(value):
    """Keep stable exact JSON bytes, rejecting nonfinite numbers before publication."""
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode()


class LoopArchive:
    """One owned SQLite archive; immutable objects commit before the manifest changes."""

    def __init__(self, owner_dir, loop_id):
        if not re.fullmatch(r'[a-f0-9]{32}', loop_id):
            raise ValueError('Invalid Loop archive identity')
        self.directory = Path(owner_dir) / 'run_archives'
        if self.directory.is_symlink():
            raise LoopStorageError('Loop archive directory must not be a symlink')
        self.path = self.directory / f'{loop_id}.sqlite3'

    @contextmanager
    def connection(self, *, write=False):
        """Own and deterministically close each local connection; never follow archive links."""
        connection = None
        try:
            if write:
                ensure_private_directory(self.directory)
                secure_private_file(self.path)
                if not self.path.exists():
                    atomic_write_private_bytes(self.path, b'')
            if self.path.is_symlink() or not self.path.is_file():
                raise LoopStorageError('Loop archive is missing or unsafe')
            connection = sqlite3.connect(self.path.absolute().as_uri() + ('?mode=rw' if write else '?mode=ro'),
                                         uri=True, timeout=10)
            if write:
                connection.execute('CREATE TABLE IF NOT EXISTS objects (hash TEXT PRIMARY KEY, payload BLOB NOT NULL)')
                connection.execute('CREATE TABLE IF NOT EXISTS legacy (hash TEXT PRIMARY KEY, payload BLOB NOT NULL)')
            yield ObjectSession(connection)
            if write:
                connection.commit()
        except (sqlite3.Error, OSError, ValueError, TypeError, IndexError, KeyError, RuntimeError) as exc:
            if connection is not None:
                connection.rollback()
            if isinstance(exc, LoopStorageError):
                raise
            raise LoopStorageError(f'Loop archive operation failed: {type(exc).__name__}') from exc
        finally:
            if connection is not None:
                connection.close()

    def save(self, record, legacy=None):
        """Preserve legacy bytes and the new immutable root in the same archive transaction."""
        ensure_private_directory(self.directory)
        with advisory_file_lock(self.path):
            with self.connection(write=True) as session:
                if legacy is not None:
                    session.connection.execute('INSERT OR IGNORE INTO legacy VALUES (?, ?)',
                        (hashlib.sha256(legacy).hexdigest(), zlib.compress(legacy)))
                root = session.pack(record)
        return root

    def load(self, root):
        """Compatibility read of a complete run; scan/paging callers need not hydrate it."""
        with self.connection() as session:
            return session.unpack(root)

    def page(self, root, field, offset, limit):
        """Read at most one bounded collection page without loading earlier rounds/configs."""
        if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 200:
            raise ValueError('Choose a nonnegative offset and a page size from 1 to 200')
        with self.connection() as session:
            value = session.field(root, field)
            node = session.object(value)
            kind, count = node[0], node[1]
            if kind not in {'list', 'dict'}:
                raise ValueError('Requested Loop field is not a collection')
            entries = session.entries(value, offset, limit)
            items = ([session.unpack(item) for item in entries] if kind == 'list' else
                     [{'key': key, 'value': session.unpack(item)} for key, item in entries])
            return {'items': items, 'total': count, 'offset': offset,
                    'next_offset': offset + len(items) if offset + len(items) < count else None}

    def fields(self, root, names):
        """Load explicitly requested small fields without expanding historical collections."""
        with self.connection() as session:
            return {key: session.unpack(value) for key, value in session.entries(root) if key in names}


class ObjectSession:
    """Content-addressed chunk tree with a fixed byte cache and no mutable-object aliasing."""

    def __init__(self, connection):
        self.connection = connection
        self.cache = OrderedDict()
        self.cache_size = 0
        self.written = set()

    def put(self, value):
        """Deduplicate unchanged nodes/configurations rather than copying run snapshots."""
        raw = encoded(value)
        if len(raw) > MAX_OBJECT_BYTES:
            raise LoopStorageError('An individual Loop archive object exceeds its safe size')
        key = hashlib.sha256(raw).hexdigest()
        if key not in self.written:
            self.connection.execute('INSERT OR IGNORE INTO objects VALUES (?, ?)', (key, zlib.compress(raw)))
            self.written.add(key)
        return ['ref', key]

    def object(self, reference):
        """Verify immutable bytes before decoding; bound decompression and cached data."""
        if (not isinstance(reference, list) or len(reference) != 2 or reference[0] != 'ref'
                or not isinstance(reference[1], str) or not re.fullmatch(r'[a-f0-9]{64}', reference[1])):
            raise LoopStorageError('Invalid Loop archive reference')
        key = reference[1]
        raw = self.cache.get(key)
        if raw is None:
            row = self.connection.execute('SELECT payload FROM objects WHERE hash = ?', (key,)).fetchone()
            if row is None:
                raise LoopStorageError('Referenced Loop archive object is missing')
            inflater = zlib.decompressobj()
            try:
                raw = inflater.decompress(row[0], MAX_OBJECT_BYTES + 1)
            except zlib.error as exc:
                raise LoopStorageError('Loop archive object is corrupt') from exc
            if (len(raw) > MAX_OBJECT_BYTES or not inflater.eof or inflater.unused_data
                    or hashlib.sha256(raw).hexdigest() != key):
                raise LoopStorageError('Loop archive object failed integrity verification')
            while self.cache and self.cache_size + len(raw) > CACHE_BYTES:
                _, removed = self.cache.popitem(last=False)
                self.cache_size -= len(removed)
            self.cache[key] = raw
            self.cache_size += len(raw)
        else:
            self.cache.move_to_end(key)
        return json.loads(raw)

    def sequence(self, kind, entries):
        """Each tree node has fixed fanout; the active root contains no growing ref array."""
        if len(entries) <= FANOUT:
            return self.put([kind, len(entries), entries])
        level = [(self.put([kind, len(part), part]), len(part))
                 for start in range(0, len(entries), FANOUT) if (part := entries[start:start + FANOUT])]
        while len(level) > FANOUT:
            level = [(self.put([kind, sum(count for _, count in part), {'children': part}]),
                      sum(count for _, count in part))
                     for start in range(0, len(level), FANOUT) if (part := level[start:start + FANOUT])]
        return self.put([kind, len(entries), {'children': level}])

    def pack(self, value, name=None):
        """Archive complete configurations once, and mutable collections as chunked trees."""
        if name in CONFIG_FIELDS and isinstance(value, dict):
            return self.put(['json', value])
        if isinstance(value, dict):
            return self.sequence('dict', [[key, self.pack(item, key)] for key, item in value.items()])
        if isinstance(value, list):
            return self.sequence('list', [self.pack(item) for item in value])
        if isinstance(value, str) and len(value) > 4096:
            return self.put(['text', self.sequence('list', [['value', value[i:i + 4096]]
                                                           for i in range(0, len(value), 4096)])])
        return ['value', value]

    def entries(self, reference, offset=0, limit=None):
        """Skip whole archived branches when serving a history page."""
        node = self.object(reference)
        kind, count, payload = node
        if kind not in {'dict', 'list'}:
            raise LoopStorageError('Invalid archived collection')
        if limit is None:
            limit = count
        if isinstance(payload, list):
            yield from payload[offset:offset + limit]
            return
        for child, size in payload['children']:
            if offset >= size:
                offset -= size
                continue
            take = min(limit, size - offset)
            yield from self.entries(child, offset, take)
            limit -= take
            if limit <= 0:
                break
            offset = 0

    def field(self, root, name):
        """Resolve a single top-level collection without expanding the run."""
        for key, reference in self.entries(root):
            if key == name:
                return reference
        raise KeyError(name)

    def unpack(self, reference):
        """Return independent mutable values even when configurations share archived bytes."""
        if reference[0] == 'value':
            return reference[1]
        node = self.object(reference)
        if node[0] == 'json':
            return node[1]
        if node[0] == 'text':
            return ''.join(self.unpack(item) for item in self.entries(node[1]))
        if node[0] == 'dict':
            return {key: self.unpack(item) for key, item in self.entries(reference)}
        if node[0] == 'list':
            return [self.unpack(item) for item in self.entries(reference)]
        raise LoopStorageError('Unknown Loop archive object type')


def manifest(record, root):
    """The active JSON has fixed fields and one archive root, never historical arrays."""
    from pb8_loop_observer import ObserverJobs
    result = {key: record[key] for key in HOT_FIELDS if key in record}
    result['_loop_storage'] = {'version': FORMAT, 'root': root}
    # Retained legacy records can have incomplete job metadata. Never make
    # persistence depend on reconstructing such a report target; a controller
    # scan must inspect it rather than incorrectly skipping pending reports.
    incomplete = (record.get('status') not in {'completed', 'unconfirmed', 'stopped', 'failed'}
                  or not record.get('cleanup_done')
                  or any(not {'kind', 'operation'}.issubset(job)
                         or job.get('kind') == 'validation' and not job.get('candidate')
                         for job in record.get('jobs', []))
                  or any(not {'kind', 'status', 'round'}.issubset(job)
                         for job in record.get('observer_jobs', [])))
    result['observer_pending'] = True if incomplete else ObserverJobs.pending(record)
    return result
