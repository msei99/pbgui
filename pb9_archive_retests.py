"""Native PB9 evidence and recovery adapter for the shared archive retest engine."""
from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import re
import stat

from fastapi import HTTPException
from secure_files import ensure_private_directory

SERVICE = 'ArchiveRetest'
KIND = 'pb9_archive_retest_v1'


class RetestFinalizePending(RuntimeError):
    """A verified replacement needs another owned finalization attempt."""


def _modules():
    """Resolve the existing engines lazily without startup import cycles."""
    from api import backtest_v7 as shared, backtest_v9 as native
    return shared, native


def source_digest(config):
    """Bind authored native input independently of GUI-only preferences."""
    value = copy.deepcopy(config)
    value.pop('pbgui', None)
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def tree_digest(root):
    """Hash every result artifact without executing or following repository links."""
    from api.archive_helpers import _validate_directory_tree_no_symlinks
    _, native = _modules()
    root = _validate_directory_tree_no_symlinks(Path(root))
    digest = hashlib.sha256()
    for path in sorted(root.rglob('*')):
        relative = path.relative_to(root).as_posix()
        native._safe(root, relative)
        if path.is_dir():
            continue
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, 'rb') as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise RuntimeError('Retest artifact must be a regular file')
            digest.update(relative.encode()); digest.update(b'\0')
            digest.update(str(os.fstat(stream.fileno()).st_size).encode()); digest.update(b'\0')
            for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                digest.update(chunk)
    return digest.hexdigest()


def _validate_run(run):
    """Reject persisted identifiers before constructing managed result or journal paths."""
    _, native = _modules()
    if (not isinstance(run, dict) or run.get('kind') != KIND
            or not re.fullmatch(r'[0-9a-f]{32}', str(run.get('id', '')))
            or run.get('backtest_version') != 'v9'):
        raise RuntimeError('Invalid native archive retest record')
    from passivbot_runtime import generation_from_schema
    schema=run.get('native_schema')
    if not isinstance(schema,str) or generation_from_schema(schema)!='pb9' or not isinstance(run.get('native_commit'),str):
        raise RuntimeError('Invalid native archive runtime evidence')
    native._job_path(run.get('queue_filename'))
    native._name(run.get('queue_name'))
    for key in ('snapshot_sha256', 'source_content_sha256', 'original_tree_sha256'):
        if not re.fullmatch(r'[0-9a-f]{64}', str(run.get(key, ''))):
            raise RuntimeError('Invalid native archive retest evidence')
    return run


def queue_retest(run, config, archive_dir, result_dir, cancel):
    """Use the native immutable queue; never attach control metadata to bot input."""
    if cancel is None:
        raise HTTPException(422, 'PB9 retests require the owned native endpoint')
    shared, native = _modules()
    recover_runs()
    if any(item.get('backtest_version') == 'v9' and item.get('archive_name') == run['archive_name']
           and item.get('source_relative_path') == run['source_relative_path']
           and item.get('status') in {'queued', 'processing'} for item in shared._load_archive_retest_runs()):
        raise HTTPException(409, 'A native retest for this archive result is already pending')
    run = copy.deepcopy(run)
    safe_name = re.sub(r'[^A-Za-z0-9_. -]', '_', run['archive_config_name'])[:90]
    run.update(kind=KIND, queue_name='archive_retest_' + safe_name + '_' + run['id'][:8],
               original_tree_sha256=tree_digest(result_dir), config_family='pb9')
    native._archive_data_paths(config)
    queued = native._enqueue({'name': run['queue_name'], 'config': config,
                              'operation_id': 'archive-retest-' + run['id']}, cancel, archive_retest=run)
    record = native._read(native._job_path(queued['filename']))
    return _validate_run(record['archive_retest'])


def recover_runs():
    """Recover an enqueue acknowledged on disk before its shared state was saved."""
    shared, native = _modules()
    candidates = []
    root = native._root('queue')
    if root.is_dir():
        for path in root.glob('*.json'):
            if re.fullmatch(r'[0-9a-f-]{36}', path.stem):
                candidates.append((path, False))
    results = native._root('results')
    if results.is_dir():
        candidates.extend((path, True) for path in results.glob('archive_retest_*/*/pbgui.completion.json'))
    runs = shared._load_archive_retest_runs()
    seen = {item.get('id') for item in runs}
    changed = False
    for path, completion in candidates:
        try:
            native._safe(results if completion else root, str(path))
            record = native._read(path)
            run = record.get('archive_retest')
            if not isinstance(run, dict) or run.get('kind') != KIND:
                continue
            _validate_run(run)
            if run['id'] in seen or record.get('snapshot_sha256') != run['snapshot_sha256']:
                continue
            if completion:
                expected = native._safe(results, run['queue_name'], run['queue_filename'], 'pbgui.completion.json')
                if path != expected or record.get('returncode') != 0:
                    continue
            elif path != native._job_path(run['queue_filename']):
                continue
            runs.append(run); seen.add(run['id']); changed = True
        except (HTTPException, OSError, ValueError, RuntimeError):
            from logging_helpers import human_log
            human_log(SERVICE, 'Ignoring invalid native retest recovery metadata', level='WARNING')
            continue
    if changed:
        shared._save_archive_retest_runs(runs)


def _job_root(run):
    """Return only this exact native job's result namespace."""
    _, native = _modules()
    _validate_run(run)
    return native._safe(native._root('results'), run['queue_name'], run['queue_filename'])


def _check_result(directory, run):
    """Verify result-owned source, native metrics and provenance before replacement."""
    _, native = _modules()
    evidence = native._read(native._safe(directory, 'pbgui.provenance.json'))
    metadata = evidence.get('archive_retest') or {}
    if (evidence.get('runtime') != 'pb9' or evidence.get('filename') != run['queue_filename']
            or evidence.get('snapshot_sha256') != run['snapshot_sha256'] or metadata.get('id') != run['id']
            or evidence.get('schema') != run['native_schema'] or evidence.get('commit') != run['native_commit']):
        raise RuntimeError('Native retest result provenance does not match its queued snapshot')
    config = native._read(native._safe(directory, 'pbgui.source.json'))
    if source_digest(config) != run['source_content_sha256']:
        raise RuntimeError('Native retest source changed after execution')
    times, _, equity = native._equity_arrays(directory)
    if not len(times):
        raise RuntimeError('Native retest has no equity samples')
    analysis = native._read(native._safe(directory, 'analysis.json'), 8 * 1024 * 1024)
    if run.get('options', {}).get('skip_liquidated', True):
        start = float(config.get('backtest', {}).get('starting_balance') or 0)
        value = config.get('backtest', {}).get('equity_floor_fraction')
        floor = float(.05 if value is None else value)
        if analysis.get('liquidated') is True or start <= 0 or float(equity[-1]) < start * floor:
            raise RuntimeError('Native retest is liquidated; original archive result is unchanged')


def find_result(run):
    """Accept one unambiguous successful result; never choose an unrelated latest run."""
    _, native = _modules()
    root = _job_root(run)
    marker = native._safe(root, 'pbgui.completion.json')
    if not marker.exists():
        return None
    complete = native._read(marker)
    if (complete.get('returncode') != 0 or complete.get('filename') != run['queue_filename']
            or complete.get('snapshot_sha256') != run['snapshot_sha256']
            or (complete.get('archive_retest') or {}).get('id') != run['id']):
        raise RuntimeError('Native retest completion evidence does not match')
    candidates = []
    for path in root.glob('**/analysis.json'):
        directory = native._safe(root, str(path.parent))
        _check_result(directory, run)
        candidates.append(directory)
    if len(candidates) != 1:
        raise RuntimeError('Native archive retest requires one unambiguous result')
    return candidates[0]


def _journal_path(run):
    """Keep recovery records outside the Git archive and below controlled storage."""
    shared, native = _modules()
    _validate_run(run)
    return native._safe(shared._archive_retests_dir(), 'native_v9', run['id'] + '.json')


def has_replacement(run):
    """Allow a controller restart to finish a replacement after native cleanup."""
    return _journal_path(run).is_file()


def cleanup(run, local_result=None):
    """Retire exactly one finished native job, retaining its result in managed trash."""
    _, native = _modules()
    root = _job_root(run)
    removed = []
    with native._job_lock(run['queue_filename']):
        record = native._job_path(run['queue_filename'])
        if record.exists() and native._job_status(run['queue_filename'])[0] not in {'complete', 'error', 'stopped'}:
            return {'removed': [], 'errors': ['Native retest cleanup is waiting for process exit'], 'complete': False}
        if root.exists():
            trash = native._root('trash'); ensure_private_directory(trash)
            dest = native._safe(trash, 'archive-retest-' + run['id'])
            if dest.exists():
                raise RuntimeError('Native retest recovery destination already exists')
            os.replace(root, dest); removed.append('local_result_to_trash')
        if record.exists():
            native._remove_job_files(run['queue_filename']); removed.append('queue_snapshot')
    return {'removed': removed, 'errors': [], 'complete': True}


def replace_result(run):
    """Reuse archive copy/manifest handling with a durable native replacement checkpoint."""
    from api.archive_helpers import archive_transaction, copy_backtest_result_to_archive, rebuild_archive_manifest
    shared, native = _modules()
    _validate_run(run)
    shared._validate_name(run['archive_name'])
    archive = native._safe(shared._archives_dir(), run['archive_name'])
    with archive_transaction(archive):
        shared._require_own_archive(run['archive_name'], 'Retest & Replace')
        old = native._safe(archive, run['source_relative_path'])
        journal_path = _journal_path(run)
        backup = native._safe(shared._archive_retests_dir(), 'native_v9', run['id'] + '.original')
        if journal_path.exists():
            journal = native._read(journal_path)
            new = native._safe(archive, journal['new_relative_path'])
            if journal.get('run_id') != run['id'] or not new.is_dir() or tree_digest(new) != journal.get('new_tree_sha256'):
                raise RuntimeError('Native archive replacement checkpoint is invalid')
            _check_result(new, run)
        else:
            if not old.is_dir() or tree_digest(old) != run['original_tree_sha256']:
                raise RuntimeError('Original archive result changed while its native retest was pending')
            ensure_private_directory(backup.parent)
            if backup.exists():
                raise RuntimeError('Native archive backup destination already exists')
            if old.stat().st_dev != backup.parent.stat().st_dev:
                raise RuntimeError('Archive and native retest recovery backup must use the same filesystem')
            local = find_result(run)
            if local is None:
                raise RuntimeError('Successful native retest result is not available')
            copied = copy_backtest_result_to_archive(local, archive, config_name=run['archive_config_name'])
            new = native._safe(archive, copied['relative_path'])
            if new == old:
                raise RuntimeError('Native replacement would overwrite its original result path')
            journal = {'run_id': run['id'], 'new_relative_path': copied['relative_path'], 'new_tree_sha256': tree_digest(new)}
            native._write(journal_path, journal)
        try:
            if old.exists():
                if tree_digest(old) != run['original_tree_sha256']:
                    raise RuntimeError('Original archive result changed; native replacement is blocked')
                ensure_private_directory(backup.parent)
                if backup.exists():
                    raise RuntimeError('Native archive backup destination already exists')
                os.replace(old, backup)
            elif not backup.is_dir() or tree_digest(backup) != run['original_tree_sha256']:
                raise RuntimeError('Original native archive backup cannot be verified')
        except Exception:
            # Before retiring the original, roll back only this exact verified copy.
            if old.is_dir() and not backup.exists() and tree_digest(new) == journal['new_tree_sha256']:
                import shutil
                shutil.rmtree(new)
                journal_path.unlink()
                shared._invalidate_archive_cache(run['archive_name'])
            raise
        try:
            shared._invalidate_archive_cache(run['archive_name'])
            manifest = rebuild_archive_manifest(archive)
            cleaned = cleanup(run)
        except Exception as exc:
            raise RetestFinalizePending('Native replacement is copied; finalization will retry') from exc
        return {'old_relative_path': run['source_relative_path'], 'new_relative_path': journal['new_relative_path'],
                'new_path': str(new), 'manifest': manifest, 'cleanup': cleaned, 'backup': str(backup)}
