"""Read-only PB8 bundle snapshots and short-lived reviewed PB9 conversion proposals."""
from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import threading
import time
import uuid

from fastapi import HTTPException
from file_lock import advisory_file_lock
from pb9_config import migrate_pb8_config_to_pb9
from pb9_config_helper import _HelperError, check_migration_input, migration_file_references

SERVICE = 'PB9Config'  # Shared Tier-3 config log owner.
_LIMIT = 1024 * 1024 - 8192
_LOCK = threading.RLock()
_reviews = {}
_METADATA = {'pbgui', '_raw', '_raw_effective', '_transform_log', '_coins_sources', '_original_backtest_config'}


def _read(path, root, *, decode=True, limit=_LIMIT):
    """Read bounded JSON through no-follow descriptors for every path component."""
    descriptors = []
    try:
        root = root.resolve()
        relative = path.absolute().relative_to(root)
        descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        descriptors.append(descriptor)
        for part in relative.parts[:-1]:
            descriptor = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor)
            descriptors.append(descriptor)
        descriptor = os.open(relative.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=descriptor)
        descriptors.append(descriptor)
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError('not regular')
        raw = bytearray()
        while len(raw) <= limit:
            chunk = os.read(descriptor, min(65536, limit + 1 - len(raw)))
            if not chunk:
                break
            raw.extend(chunk)
        if len(raw) > limit:
            raise HTTPException(413, 'PB8 migration file exceeds its size limit')
        if not decode:
            return bytes(raw)
        value = json.loads(raw, parse_constant=lambda _: (_ for _ in ()).throw(ValueError('nonfinite')))
        return value, bytes(raw)
    except HTTPException:
        raise
    except (OSError, ValueError, UnicodeError, RecursionError) as exc:
        raise HTTPException(422, 'PB8 source must contain regular JSON files without symbolic links') from exc
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _clean(value, depth=0):
    """Remove GUI/result helper metadata from the disposable migration copy only."""
    if depth > 64:
        raise HTTPException(422, 'PB8 migration source nesting exceeds 64 levels')
    if isinstance(value, dict):
        return {key: _clean(item, depth + 1) for key, item in value.items() if key not in _METADATA and key != 'base_config_path'}
    if isinstance(value, list):
        return [_clean(item, depth + 1) for item in value]
    return value


def source_snapshot(selection, *, with_preferences=False):
    """Resolve one saved config or local PB8 result through the existing PB8 boundaries."""
    from api import backtest_v8 as pb8
    if not isinstance(selection, dict) or set(selection) != {'source_type', 'source'} or not isinstance(selection['source'], str):
        raise HTTPException(422, 'Select a saved PB8 config or local PB8 result')
    kind, value = selection['source_type'], selection['source']
    if kind == 'config':
        name = pb8._validate_name(value)
        path = pb8._config_file(name)
        root = pb8._configs_dir()
    elif kind == 'result':
        directory = pb8._resolve_result_dir(value, allow_archives=False, allow_legacy=False)
        root = pb8._results_root()
        path = next((directory / name for name in ('config.original.json', 'config.json', 'backtest.json')
                     if (directory / name).exists()), directory / 'config.json')
        name = directory.relative_to(root.resolve()).parts[0]
    else:
        raise HTTPException(422, 'Select a saved PB8 config or local PB8 result')
    if not root.is_dir():
        raise HTTPException(404, 'PB8 source not found')
    # Same writer lock as PB8 config persistence, without its recovery/write path.
    with advisory_file_lock(root / '.write'):
        document, raw = _read(path, root)
        preferences = {}
        if with_preferences:
            from pb9_gui_preferences import backtest_preferences
            try:
                preferences = backtest_preferences(document)
            except ValueError as exc:
                raise HTTPException(422, str(exc)) from None
        config = _clean(document)
        if not isinstance(config, dict):
            raise HTTPException(422, 'Expected a PB8 config object')
        files, originals = {}, {path.name: raw}
        try:
            pending = migration_file_references(config)
            while pending:
                filename = pending.pop()
                if filename in files:
                    continue
                if len(files) >= 32:
                    raise HTTPException(413, 'Too many PB8 migration source files')
                payload, original = _read(path.parent / filename, root)
                originals[filename] = original
                if sum(map(len, originals.values())) > _LIMIT:
                    raise HTTPException(413, 'PB8 migration bundle exceeds 1 MiB')
                files[filename] = _clean(payload)
                pending.update(migration_file_references(files[filename]) - files.keys())
            bundle = {'config': config, 'files': files}
            check_migration_input(bundle)
        except _HelperError as exc:
            code = str(exc)
            message = ('Supported source schemas: v8.0.0, v8.1.0, v8.6.0' if code == 'config_version_mismatch' else
                       'Use inline coin lists and local JSON overrides; external paths, legacy coin flags and credentials cannot be migrated here')
            raise HTTPException(422, message) from None
    fingerprint = hashlib.sha256(json.dumps({name: hashlib.sha256(raw).hexdigest() for name, raw in sorted(originals.items())}).encode()).hexdigest()
    name = re.sub(r'[^A-Za-z0-9_. -]', '_', str(name)).strip(' ._') or 'backtest'
    result = (bundle, fingerprint, name[:112] + '_v9')
    return (*result, preferences) if with_preferences else result


def preview(body, owner, paths, cancel):
    """Create a bounded owner-bound proposal, never a saved config or queue job."""
    bundle, fingerprint, name, preferences = source_snapshot(body, with_preferences=True)
    native = migrate_pb8_config_to_pb9(bundle, paths['pb9dir'], paths['pb9venv'],
              {key: value for key, value in paths.items() if not key.startswith('pb9')}, cancel_event=cancel)
    proposal = native['migration']
    if proposal['status'] != 'ready':
        return proposal
    if preferences:
        proposal['config']['pbgui'] = preferences
    if cancel.is_set():
        from pb9_config import PB9ConfigError
        raise PB9ConfigError('helper_cancelled')
    reference = None
    try:
        reference = make_comparison_reference(body, fingerprint)
    except HTTPException:
        proposal['report'].setdefault('warnings', []).append('The PB8 comparison reference could not be captured; use manual Compare for this migration.')
    token = str(uuid.uuid4())
    with _LOCK:
        for key, item in list(_reviews.items()):
            if time.monotonic() - item['created'] > 600:
                del _reviews[key]
        owned = [key for key, item in _reviews.items() if item['owner'] == owner]
        if owned and (len(owned) >= 3 or len(_reviews) >= 20):
            del _reviews[owned[0]]
        if len(_reviews) >= 20:
            raise HTTPException(429, 'Migration review capacity is busy; try again later')
        _reviews[token] = {'created': time.monotonic(), 'owner': owner, 'selection': copy.deepcopy(body),
                          'fingerprint': fingerprint, 'name': name, 'config': proposal['config'],
                          'report': proposal['report'], 'source': copy.deepcopy(native['source']),
                          'comparison_reference': reference}
    return {'status': 'ready', 'review_id': token, 'suggested_name': name, 'report': proposal['report']}


def reviewed(body, owner, runtime):
    """Recheck both sources before adopting a proposal; source edits require a new preview."""
    if not isinstance(body, dict) or set(body) != {'review_id'} or not isinstance(body['review_id'], str):
        raise HTTPException(422, 'Invalid PB9 migration review')
    with _LOCK:
        item = copy.deepcopy(_reviews.get(body['review_id']))
    if not item or item['owner'] != owner or time.monotonic() - item['created'] > 600:
        raise HTTPException(404, 'Migration review expired or not owned by this user')
    _, fingerprint, _ = source_snapshot(item['selection'])
    if fingerprint != item['fingerprint']:
        raise HTTPException(409, 'PB8 source changed; create a new migration preview')
    if any(item['source'].get(key) != runtime.get(key) for key in ('commit', 'config_schema', 'package_version')):
        raise HTTPException(409, 'PB9 converter changed; create a new migration preview')
    if item.get('comparison_reference'):
        resolve_comparison_reference(item['comparison_reference'])
    return item


def shutdown():
    """Release API-owned review memory deterministically."""
    with _LOCK:
        _reviews.clear()


def validate_comparison_reference(value):
    """Validate persisted non-secret navigation metadata before any source lookup."""
    if value is None:
        return None
    keys={'version','path','config_fingerprint','analysis_sha256'}
    if (not isinstance(value,dict) or set(value)!=keys or value.get('version')!='v8'
            or not isinstance(value.get('path'),str) or not 0<len(value['path'])<=4096
            or not Path(value['path']).is_absolute() or '..' in Path(value['path']).parts
            or '\\' in value['path'] or any(ord(char)<32 or ord(char)==127 for char in value['path'])
            or any(not isinstance(value.get(key),str) or not re.fullmatch('[0-9a-f]{64}',value[key])
                   for key in ('config_fingerprint','analysis_sha256'))):
        raise HTTPException(422,'Invalid PB8 comparison reference')
    return copy.deepcopy(value)


def config_comparison_reference(config):
    """Keep comparison metadata out of native Passivbot config validation."""
    gui=config.get('pbgui') if isinstance(config,dict) else None
    return validate_comparison_reference(gui.get('comparison_reference')) if isinstance(gui,dict) else None


def carry_comparison_reference(source, document):
    """Preserve safe editor preferences and the reviewed comparison reference."""
    from pb9_gui_preferences import backtest_preferences
    try:
        gui = backtest_preferences(document)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from None
    reference=config_comparison_reference(document)
    if reference:
        gui['comparison_reference']=reference
    if gui:
        source['pbgui']=gui
    return source


def make_comparison_reference(selection, fingerprint):
    """Bind a migration to its exact local PB8 result, never to a config name guess."""
    if selection.get('source_type')!='result':
        return None
    from api import backtest_v8 as pb8
    directory=pb8._resolve_result_dir(selection['source'],allow_archives=False,allow_legacy=False)
    raw=_read(directory/'analysis.json',pb8._results_root(),decode=False,limit=8*1024*1024)
    return {'version':'v8','path':str(directory),'config_fingerprint':fingerprint,
            'analysis_sha256':hashlib.sha256(raw).hexdigest()}


def resolve_comparison_reference(value):
    """Reject missing/replaced source results instead of silently matching another run."""
    reference=validate_comparison_reference(value)
    if not reference:
        raise HTTPException(404,'This PB9 result has no PB8 source reference; select results with Compare')
    selection={'source_type':'result','source':reference['path']}
    _,fingerprint,_=source_snapshot(selection)
    current=make_comparison_reference(selection,fingerprint)
    if current!=reference:
        raise HTTPException(409,'The PB8 reference result changed; use manual Compare to select a reference')
    from api import backtest_v8 as pb8
    return pb8._resolve_result_dir(reference['path'],allow_archives=False,allow_legacy=False)
