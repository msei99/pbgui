"""Publish rsync-completed data as immutable inputs without rehashing the dataset.

Sent by PBGui over its authenticated SSH channel. Compatible with existing
queue-v3 workers; neither Passivbot nor the installed worker needs replacement.
"""

import importlib.util
import json
import os
import re
import tempfile
from pathlib import Path

SERVICE = 'VastWorker'


def prepare(worker, metadata):
    """Create only the owned receiver directories, rejecting symlink redirection."""
    for relative in ('rsync-data', 'rsync-data/.rsync-partial',
                     'jobs/' + worker.ROOT.name + '/incoming',
                     'jobs/' + worker.ROOT.name + '/incoming/.rsync-partial'):
        path = worker.safe_path(worker.GUARD_ROOT, relative)
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if set(metadata) != {'manifest.json', 'optimize.json'}:
        raise ValueError('Invalid job metadata')
    for name, value in metadata.items():
        if not isinstance(value, str) or len(value.encode()) > 64 * 1024**2:
            raise ValueError('Invalid job metadata size')
        target = worker.safe_path(worker.ROOT, 'incoming/' + name)
        fd, temporary = tempfile.mkstemp(dir=target.parent)
        try:
            with os.fdopen(fd, 'w') as stream:
                stream.write(value)
            os.replace(temporary, target)
        finally:
            Path(temporary).unlink(missing_ok=True)


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
    destination = worker.safe_path(root, 'input')
    # Input is renamed before its ready marker. Recover a crash between the two
    # by checking the small published manifest, never re-reading all data blobs.
    if destination.exists():
        if (worker.file_hash(worker.safe_path(destination, 'manifest.json')) != worker.file_hash(manifest_path)
                or worker.file_hash(worker.safe_path(destination, 'optimize.json')) != manifest['config_sha256']):
            raise ValueError('Unverified input directory already exists')
    else:
        with tempfile.TemporaryDirectory(prefix='rsync-install-', dir=root) as temporary:
            stage = Path(temporary) / 'input'
            stage.mkdir(mode=0o700)
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
    result = {'ready': True, 'config_sha256': manifest['config_sha256'], 'transport': 'rsync-files'}
    worker.write_record(marker, result)
    return result


def main():
    """Use the installed worker's path validation and job-specific environment."""
    import sys
    spec = importlib.util.spec_from_file_location('pbgui_worker', '/work/pbgui/worker.py')
    worker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(worker)
    if not re.fullmatch(r'[0-9a-f]{32}', worker.ROOT.name):
        raise ValueError('Invalid job identity')
    if sys.argv[1:] == ['prepare']:
        payload = sys.stdin.buffer.read(128 * 1024**2 + 1)
        if len(payload) > 128 * 1024**2:
            raise ValueError('Job metadata exceeds limit')
        prepare(worker, json.loads(payload))
    elif sys.argv[1:] == ['publish']:
        print(json.dumps(publish(worker)))
    else:
        raise ValueError('Invalid input sync operation')


if __name__ == '__main__':
    main()
