"""Freeze installed PB8 documentation as bounded, read-only decision evidence."""
from __future__ import annotations

import hashlib
from pathlib import Path

from logging_helpers import human_log as _log
from secure_files import read_regular_file_nofollow

SERVICE = 'AILoopManual'
MAX_DOCUMENT_BYTES = 512 * 1024
MAX_MANUAL_BYTES = 4 * 1024 * 1024
MAX_DOCUMENTS = 64


def installed_manual():
    """Read only current top-level Markdown manuals from the configured checkout."""
    from pbgui_purefunc import pb8dir
    from pb8_loop_evidence import LoopAIPause, sha
    raw_root = pb8dir()
    if not raw_root:
        raise LoopAIPause('manual_unavailable', 'Configure the installed PB8 documentation before starting this Loop')
    root = Path(raw_root).expanduser().absolute()
    docs = root / 'docs'
    if root.is_symlink() or docs.is_symlink() or not docs.is_dir():
        raise LoopAIPause('manual_unavailable', 'The installed PB8 documentation is unavailable or unsafe')
    documents, omitted, used = [], [], 0
    try:
        for path in sorted(docs.glob('*.md')):
            # Historical releases and development plans are not the current user manual.
            if path.name.startswith(('release_notes_', 'development_', 'candlestick_')) or path.name in {
                    'CONTRIBUTING.md', 'releases.md'} or 'session_summary' in path.name or path.stem.endswith('_spec'):
                continue
            relative = path.relative_to(root).as_posix()
            if (path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_DOCUMENT_BYTES
                    or used + path.stat().st_size > MAX_MANUAL_BYTES or len(documents) >= MAX_DOCUMENTS):
                omitted.append(relative)
                continue
            content = read_regular_file_nofollow(path, root)
            if len(content) > MAX_DOCUMENT_BYTES or used + len(content) > MAX_MANUAL_BYTES:
                omitted.append(relative)
                continue
            text = content.decode('utf-8')
            used += len(content)
            documents.append({'path': relative, 'sha256': hashlib.sha256(content).hexdigest(),
                              'text': text})
    except (OSError, RuntimeError, UnicodeError) as exc:
        _log(SERVICE, 'Installed PB8 manual could not be read safely', level='ERROR',
             meta={'error_type': type(exc).__name__})
        raise LoopAIPause('manual_unavailable', 'The installed PB8 manual could not be read safely') from exc
    required = {'docs/optimizing.md', 'docs/metrics.md', 'docs/config.bot.md'}
    if not required <= {doc['path'] for doc in documents}:
        raise LoopAIPause('manual_unavailable', 'Installed PB8 optimizing, metrics and bot manuals are required')
    identity = [{'path': doc['path'], 'sha256': doc['sha256']} for doc in documents]
    index = []
    for number, doc in enumerate(documents):
        index.append({**identity[number], 'evidence_path': '/data/passivbot_manual/documents/' + str(number) + '/text',
                      'headings': [line[:180] for line in doc['text'].splitlines() if line.startswith('#')][:100]})
    return {'source': 'installed_pb8_checkout', 'digest': sha(identity), 'index': index,
            'documents': documents, 'omitted': omitted}


def manual_summary(manual):
    """Put the complete document index in context without duplicating full manuals."""
    return {key: value for key, value in manual.items() if key != 'documents'}
