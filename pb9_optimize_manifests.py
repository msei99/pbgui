"""Bounded, persistent, content-addressed PB9 user attachments, not candle caches."""
from pathlib import Path

from file_lock import advisory_file_lock
from pb9_optimize_options import MAX_MANIFEST_BYTES, validate_options
from secure_files import atomic_write_private_text, ensure_private_directory, read_regular_file_nofollow

SERVICE = 'OptimizeV9'
MAX_ARTIFACTS = 256
MAX_TOTAL_BYTES = 256 * 1024 * 1024


def store_manifest(root, manifest):
    """Deduplicate imported reports without putting large text into config/draft JSON."""
    checked = validate_options({'preparation_manifest':manifest})['preparation_manifest']
    if 'text' not in checked:
        raise ValueError('Import preparation manifest content, not an existing reference')
    root = Path(root)
    if root.is_symlink():
        raise ValueError('Invalid native manifest directory')
    ensure_private_directory(root)
    target = root/(checked['sha256'] + '.json')
    with advisory_file_lock(root/'.write'):
        if target.exists() or target.is_symlink():
            if read_regular_file_nofollow(target, root) != checked['text'].encode('utf-8'):
                raise ValueError('Stored preparation manifest changed')
        else:
            entries = list(root.glob('*.json'))
            if any(path.is_symlink() or not path.is_file() for path in entries):
                raise ValueError('Invalid preparation manifest attachment storage')
            if (len(entries) >= MAX_ARTIFACTS
                    or sum(path.stat().st_size for path in entries) + len(checked['text'].encode('utf-8')) > MAX_TOTAL_BYTES):
                raise ValueError('Native preparation manifest attachment storage is full')
            atomic_write_private_text(target, checked['text'])
    return {'name':checked['name'], 'artifact':checked['sha256'], 'sha256':checked['sha256']}


def resolve_manifest(root, manifest):
    """Read only a checked hash-named attachment; freeze bytes in launch options."""
    if not isinstance(manifest, dict) or 'text' in manifest:
        return manifest
    checked = validate_options({'preparation_manifest':manifest})['preparation_manifest']
    root = Path(root)
    if root.is_symlink():
        raise ValueError('Invalid native manifest directory')
    path = root/(checked['artifact'] + '.json')
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_MANIFEST_BYTES:
            raise ValueError('Managed preparation manifest is missing or invalid; import it again')
        text = read_regular_file_nofollow(path, root).decode('utf-8')
    except (OSError, RuntimeError, UnicodeError) as exc:
        raise ValueError('Managed preparation manifest cannot be read; import it again') from exc
    return validate_options({'preparation_manifest':{**checked, 'text':text}})['preparation_manifest']
