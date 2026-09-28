"""Durable, private per-account credential revisions and PBRun acknowledgements."""
from __future__ import annotations

from copy import deepcopy
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import time
import uuid

from file_lock import advisory_file_lock
from secure_files import atomic_write_private_text, ensure_private_directory, read_regular_file_nofollow

SERVICE = "CredentialRuntime"
_FIELDS = ("exchange", "key", "secret", "passphrase", "wallet_address", "private_key", "is_vault", "quote", "options")
_STATUSES = {"key_arrived", "pending", "restarting", "waiting", "applied", "error"}


def _name(value: str) -> str:
    """Validate persisted account and instance names before using them."""
    if not isinstance(value, str) or not value or value in {".", ".."} or len(value) > 256:
        raise ValueError("Invalid credential runtime name")
    if any(char in value for char in "/\\") or any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError("Invalid credential runtime name")
    return value


class CredentialRuntimeSourceChanged(ValueError):
    """A journal from a previous runtime path is not evidence for the current path."""


def _hex_value(value, length: int) -> bool:
    """Validate opaque revisions and private digests without interpreting them."""
    return isinstance(value, str) and len(value) == length and all(char in '0123456789abcdef' for char in value)


def _timestamp(value) -> bool:
    """Reject corrupt timestamps instead of treating them as an adopted baseline."""
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def _validate_users(users) -> None:
    """Fail closed when any persisted account intent is malformed."""
    if not isinstance(users, dict):
        raise ValueError('Invalid credential user state')
    for name, row in users.items():
        _name(name)
        if (not isinstance(row, dict) or not _hex_value(row.get('revision'), 32)
                or type(row.get('present')) is not bool or not _timestamp(row.get('ready_at'))
                or (row['present'] and not _hex_value(row.get('fingerprint'), 64))
                or (not row['present'] and row.get('fingerprint') is not None)):
            raise ValueError('Invalid credential user state')


class CredentialRuntimeJournal:
    """Separate file arrival from credential adoption by each exact bot process."""

    def __init__(self, api_path: Path, status_path: Path):
        """Bind one runtime's journal; no filesystem writes happen on construction."""
        self.api_path = Path(os.path.abspath(Path(api_path).expanduser()))
        self.path = Path(os.path.abspath(Path(status_path).expanduser())).with_suffix(".runtime.json")

    def _safe(self) -> None:
        """Reject symlinked roots, files and locks before every access."""
        from pb7_api_keys import _reject_symlink_components
        for path in (self.path, self.path.with_name(self.path.name + '.lock')):
            _reject_symlink_components(path)

    def _lock(self):
        """Serialize journal read-modify-write across PBRun and key writers."""
        self._safe()
        return advisory_file_lock(self.path)

    def _fresh(self) -> dict:
        """Build an untracked baseline for one configured runtime path."""
        return {"version": 1, "source": str(self.api_path), "salt": uuid.uuid4().hex,
                "users": {}, "instances": {}}

    def _read(self) -> dict:
        """Read the private journal, failing closed on corrupt state."""
        self._safe()
        if not self.path.exists():
            return self._fresh()
        value = json.loads(read_regular_file_nofollow(self.path, self.path.parent))
        if (not isinstance(value, dict) or value.get("version") != 1
                or not isinstance(value.get("source"), str)
                or not _hex_value(value.get("salt"), 32)
                or not isinstance(value.get("users"), dict)
                or not isinstance(value.get("instances"), dict)):
            raise ValueError("Invalid credential runtime journal")
        _validate_users(value['users'])
        if 'pending' in value:
            _validate_users(value['pending'])
        for instance, row in value['instances'].items():
            _name(instance)
            if not isinstance(row, dict):
                raise ValueError('Invalid credential process state')
            _name(row.get('user'))
            if (row.get('status') not in _STATUSES or not _timestamp(row.get('observed_at'))
                    or not _timestamp(row.get('retry_at')) or not isinstance(row.get('reason'), str)):
                raise ValueError('Invalid credential process state')
            if 'applied_revision' in row and (not _hex_value(row['applied_revision'], 32)
                    or type(row.get('pid')) is not int or row['pid'] <= 0 or not _timestamp(row.get('created'))):
                raise ValueError('Invalid credential process identity')
        if value['source'] != str(self.api_path):
            raise CredentialRuntimeSourceChanged('Configured runtime path changed')
        return value

    def _write(self, value: dict) -> None:
        """Persist private state atomically; never publish credential fingerprints."""
        self._safe()
        ensure_private_directory(self.path.parent)
        atomic_write_private_text(self.path, json.dumps(value, indent=4))

    @staticmethod
    def _fingerprints(payload: dict, salt: str) -> dict:
        """Hash only authentication/account fields, excluding UI/serial metadata."""
        result = {}
        for name, item in payload.items():
            if name.startswith('_') or name == 'tradfi' or not isinstance(item, dict):
                continue
            _name(name)
            fields = {key: item.get(key, {} if key == 'options' else False if key == 'is_vault' else '') for key in _FIELDS}
            raw = json.dumps(fields, sort_keys=True, separators=(',', ':')).encode()
            result[name] = hmac.new(salt.encode(), raw, hashlib.sha256).hexdigest()
        return result

    @staticmethod
    def _matches(users: dict, fingerprints: dict) -> bool:
        """Compare a pending projection with verified live file contents."""
        return {name: row['fingerprint'] for name, row in users.items() if row.get('present')} == fingerprints

    @staticmethod
    def _next(users: dict, fingerprints: dict, timestamp: float) -> dict:
        """Retain unchanged revisions and rotate only changed account credentials."""
        result = deepcopy(users)
        for name in users.keys() | fingerprints.keys():
            _name(name)
            previous = users.get(name, {})
            fingerprint = fingerprints.get(name)
            if previous and previous.get('fingerprint') == fingerprint:
                continue
            result[name] = {'revision': uuid.uuid4().hex, 'fingerprint': fingerprint,
                            'present': name in fingerprints, 'ready_at': timestamp}
        return result

    def _recover(self, value: dict, actual: dict) -> dict:
        """Resolve a crash on either side of atomic key replacement under the merge lock."""
        fingerprints = self._fingerprints(actual, value['salt'])
        pending = value.pop('pending', None)
        if pending and self._matches(pending, fingerprints):
            # A process started before crash recovery may already have the new keys;
            # conservatively require a restart unless a launch acknowledgement proves it.
            for name, row in pending.items():
                if row['revision'] != value['users'].get(name, {}).get('revision'):
                    row['ready_at'] = time.time()
            value['users'] = pending
        elif not self._matches(value['users'], fingerprints):
            value['users'] = self._next(value['users'], fingerprints, time.time())
        return value

    def prepare(self, before: dict, after: dict) -> None:
        """Persist intent before the key file changes; caller holds its merge lock."""
        with self._lock():
            existed = self.path.exists()
            try:
                value = self._read()
            except CredentialRuntimeSourceChanged:
                value = self._fresh()
                existed = False
            if not existed:
                value['users'] = self._next({}, self._fingerprints(before, value['salt']), 0.0)
            value = self._recover(value, before)
            value['pending'] = self._next(value['users'], self._fingerprints(after, value['salt']), time.time())
            self._write(value)

    def recover(self, actual: dict) -> dict:
        """Verify the written file before releasing a revision to PBRun."""
        with self._lock():
            value = self._read()
            before = deepcopy(value)
            value = self._recover(value, actual)
            if value != before:
                self._write(value)
            return deepcopy(value)

    def record(self, instance: str, user: str, revision: str, status: str, *, pid: int = 0,
               created: float = 0.0, reason: str = '', retry_at: float = 0.0) -> None:
        """Persist an exact-process acknowledgement without acknowledging a newer revision."""
        _name(instance)
        _name(user)
        if status not in _STATUSES:
            raise ValueError('Invalid credential runtime status')
        with self._lock():
            value = self._read()
            previous = value['instances'].get(instance, {})
            if previous.get('user') != user:
                previous = {}
            row = {**previous, 'user': user, 'status': status, 'reason': reason,
                   'retry_at': retry_at, 'observed_at': time.time()}
            if status == 'applied':
                row.update(applied_revision=revision, pid=pid, created=created)
                if value['users'].get(user, {}).get('revision') != revision:
                    row['status'] = 'pending'
            if all(row.get(k) == previous.get(k) for k in row if k != 'observed_at') and time.time() - previous.get('observed_at', 0) < 30:
                return
            value['instances'][instance] = row
            self._write(value)

    def public_status(self) -> dict:
        """Return only explicit non-secret fields, without mutating recovery state."""
        self._safe()
        if not self.path.exists():
            return {'status': 'not_tracked', 'items': []}
        # Atomic replacement makes this read consistent without creating a lock file.
        try:
            value = self._read()
        except CredentialRuntimeSourceChanged:
            return {'status': 'not_tracked', 'items': []}
        items = []
        for instance, row in sorted(value['instances'].items()):
            _name(instance)
            user = _name(row['user'])
            desired = value['users'].get(user, {})
            status = row.get('status', 'waiting')
            if row.get('applied_revision') != desired.get('revision') and status == 'applied':
                status = 'pending'
            if time.time() - float(row.get('observed_at', 0)) > 90:
                status = 'stale'
            items.append({'instance': instance, 'user': user, 'status': status,
                          'desired_revision': desired.get('revision', ''),
                          'applied_revision': row.get('applied_revision', ''),
                          'reason': row.get('reason', ''), 'observed_at': row.get('observed_at', 0)})
        status = 'key_arrived'
        if items:
            states = {item['status'] for item in items}
            status = 'applied' if states == {'applied'} else next((s for s in ('error', 'stale', 'restarting', 'pending', 'waiting') if s in states), 'key_arrived')
        return {'status': 'projecting' if value.get('pending') else status, 'items': items}
