"""Publish rsync-completed data as immutable inputs without rehashing the dataset.

Sent by PBGui over its authenticated SSH channel. Compatible with existing
queue-v3 workers; neither Passivbot nor the installed worker needs replacement.
"""

import importlib.util
import gzip
import io
import json
import os
import re
import tempfile
from pathlib import Path

SERVICE = 'VastWorker'
MAX_JOB_METADATA_BYTES = 1024**3


def prepare(worker, metadata):
    """Create only the owned receiver directories, rejecting symlink redirection."""
    for relative in ('rsync-data', 'rsync-data/.rsync-partial',
                     'jobs/' + worker.ROOT.name + '/incoming',
                     'jobs/' + worker.ROOT.name + '/incoming/.rsync-partial'):
        path = worker.safe_path(worker.GUARD_ROOT, relative)
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if set(metadata) not in (
        {'manifest.json', 'optimize.json'},
        {'manifest.json', 'optimize.json', '_reuse_job_id'},
    ):
        raise ValueError('Invalid job metadata')
    reuse_job_id = metadata.pop('_reuse_job_id', None)
    for name, value in metadata.items():
        if not isinstance(value, str) or len(value.encode()) > MAX_JOB_METADATA_BYTES:
            raise ValueError('Invalid job metadata size')
        target = worker.safe_path(worker.ROOT, 'incoming/' + name)
        fd, temporary = tempfile.mkstemp(dir=target.parent)
        try:
            with os.fdopen(fd, 'w') as stream:
                stream.write(value)
            os.replace(temporary, target)
        finally:
            Path(temporary).unlink(missing_ok=True)
    if reuse_job_id is not None:
        if not isinstance(reuse_job_id, str) or not re.fullmatch(r'[0-9a-f]{32}', reuse_job_id):
            raise ValueError('Invalid reusable job identity')
        worker.write_record(
            worker.safe_path(worker.ROOT, 'incoming/reuse.json'),
            {'job_id': reuse_job_id},
        )


def reusable_input(worker):
    """Return one previously verified immutable input tree with identical data."""
    reuse_path = worker.safe_path(worker.ROOT, 'incoming/reuse.json')
    if not reuse_path.is_file() or reuse_path.is_symlink():
        return None
    reuse = json.loads(reuse_path.read_text())
    source_id = reuse.get('job_id')
    if not isinstance(source_id, str) or not re.fullmatch(r'[0-9a-f]{32}', source_id):
        raise ValueError('Invalid reusable job identity')
    if source_id == worker.ROOT.name:
        return None
    source_root = worker.safe_path(worker.GUARD_ROOT, 'jobs/' + source_id)
    source_input = worker.safe_path(source_root, 'input')
    source_ohlcv = worker.safe_path(source_input, 'ohlcv')
    source_marker = worker.safe_path(source_root, 'input-ready.json')
    if (
        not source_input.is_dir()
        or source_input.is_symlink()
        or not source_ohlcv.is_dir()
        or source_ohlcv.is_symlink()
        or not source_marker.is_file()
        or source_marker.is_symlink()
    ):
        return None
    marker = json.loads(source_marker.read_text())
    if marker.get('ready') is not True:
        return None
    current_manifest = json.loads(
        worker.safe_path(worker.ROOT, 'incoming/manifest.json').read_text()
    )
    source_manifest = json.loads(
        worker.safe_path(source_input, 'manifest.json').read_text()
    )
    if (
        current_manifest.get('pb8_revision') != source_manifest.get('pb8_revision')
        or current_manifest.get('files') != source_manifest.get('files')
    ):
        return None
    roots = {
        item.get('path', '').split('/', 1)[0]
        for item in current_manifest.get('files', [])
        if isinstance(item, dict)
    }
    if roots != {'ohlcv'}:
        return None
    return source_input


def publish(worker):
    """Link completed immutable blobs into a private job input and publish readiness."""
    root = worker.ROOT
    incoming = worker.safe_path(root, 'incoming')
    manifest_path = worker.safe_path(incoming, 'manifest.json')
    config_path = worker.safe_path(incoming, 'optimize.json')
    manifest = json.loads(manifest_path.read_text())
    if manifest['pb8_revision'] != Path('/opt/pb8-revision').read_text().strip():
        raise ValueError('PB8 image revision mismatch')
    if worker.file_hash(config_path) != manifest['config_sha256']:
        raise ValueError('Config checksum mismatch')
    marker = worker.safe_path(root, 'input-ready.json')
    if marker.exists():
        result = json.loads(marker.read_text())
        if (result.get('config_sha256') != manifest['config_sha256']
                or worker.file_hash(worker.safe_path(root, 'input/manifest.json')) != worker.file_hash(manifest_path)):
            raise ValueError('Prepared job configuration changed')
        return result
    reuse = None
    destination = worker.safe_path(root, 'input')
    # Input is renamed before its ready marker. Recover a crash between the two
    # by checking the small published manifest, never re-reading all data blobs.
    if destination.exists():
        if (worker.file_hash(worker.safe_path(destination, 'manifest.json')) != worker.file_hash(manifest_path)
                or worker.file_hash(worker.safe_path(destination, 'optimize.json')) != manifest['config_sha256']):
            raise ValueError('Unverified input directory already exists')
    else:
        reuse = reusable_input(worker)
        with tempfile.TemporaryDirectory(prefix='rsync-install-', dir=root) as temporary:
            stage = Path(temporary) / 'input'
            stage.mkdir(mode=0o700)
            if reuse is not None:
                worker.safe_path(stage, 'ohlcv').symlink_to(
                    worker.safe_path(reuse, 'ohlcv'), target_is_directory=True
                )
            else:
                seen = {'manifest.json', 'optimize.json'}
                total = 0
                for item in manifest['files']:
                    key, size = item['sha256'], item['bytes']
                    if not isinstance(key, str) or not re.fullmatch(r'[0-9a-f]{64}', key):
                        raise ValueError('Invalid data identity')
                    if type(size) is not int or size < 0:
                        raise ValueError('Invalid data size')
                    total += size
                    if total > 12 * 1024**3 or item['path'] in seen:
                        raise ValueError('Invalid input manifest')
                    seen.add(item['path'])
                    path = worker.safe_path(stage, item['path'])
                    blob = worker.safe_path(worker.GUARD_ROOT, 'rsync-data/' + key)
                    if not blob.is_file() or blob.stat().st_size != size:
                        raise ValueError('Synchronized data file is missing or incomplete')
                    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                    os.link(blob, path)
            for name in ('manifest.json', 'optimize.json'):
                # Small metadata stays job-owned; no shared mutable config links.
                target = stage / name
                target.write_bytes(worker.safe_path(incoming, name).read_bytes())
                target.chmod(0o600)
            os.replace(stage, destination)
    result = {
        'ready': True,
        'config_sha256': manifest['config_sha256'],
        'transport': 'rsync-files',
    }
    if reuse is not None:
        result['reused_input_job'] = reuse.parent.name
    worker.write_record(marker, result)
    return result


def cache_complete(worker):
    """Confirm that every immutable blob from the prepared manifest is cached."""
    if reusable_input(worker) is not None:
        return True
    manifest = json.loads(worker.safe_path(worker.ROOT, 'incoming/manifest.json').read_text())
    seen = {'manifest.json', 'optimize.json'}
    total = 0
    for item in manifest['files']:
        key, size = item['sha256'], item['bytes']
        if (not isinstance(key, str) or not re.fullmatch(r'[0-9a-f]{64}', key)
                or type(size) is not int or size < 0 or item['path'] in seen):
            raise ValueError('Invalid input manifest')
        seen.add(item['path'])
        total += size
        if total > 12 * 1024**3:
            raise ValueError('Invalid input manifest')
        blob = worker.safe_path(worker.GUARD_ROOT, 'rsync-data/' + key)
        if not blob.is_file() or blob.is_symlink() or blob.stat().st_size != size:
            return False
    return True


def read_metadata(stream):
    """Read one bounded length-prefixed message without waiting for SSH stdin EOF."""
    header = stream.read(8)
    if len(header) != 8:
        raise ValueError('Incomplete job metadata header')
    size = int.from_bytes(header, 'big')
    if not 0 < size <= MAX_JOB_METADATA_BYTES:
        raise ValueError('Invalid job metadata size')
    payload = stream.read(size)
    if len(payload) != size:
        raise ValueError('Incomplete job metadata')
    # Accept the initial uncompressed framed protocol during rolling updates.
    if payload.startswith(b'\x1f\x8b'):
        with gzip.GzipFile(fileobj=io.BytesIO(payload)) as compressed:
            payload = compressed.read(MAX_JOB_METADATA_BYTES + 1)
        if len(payload) > MAX_JOB_METADATA_BYTES:
            raise ValueError('Job metadata exceeds limit')
    return json.loads(payload)


def main():
    """Use the installed worker's path validation and job-specific environment."""
    import sys
    spec = importlib.util.spec_from_file_location('pbgui_worker', '/work/pbgui/worker.py')
    worker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(worker)
    if not re.fullmatch(r'[0-9a-f]{32}', worker.ROOT.name):
        raise ValueError('Invalid job identity')
    if sys.argv[1:] == ['prepare']:
        prepare(worker, read_metadata(sys.stdin.buffer))
    elif sys.argv[1:] == ['cache-complete']:
        print(json.dumps({'complete': cache_complete(worker)}))
    elif sys.argv[1:] == ['publish']:
        print(json.dumps(publish(worker)))
    else:
        raise ValueError('Invalid input sync operation')


if __name__ == '__main__':
    main()
