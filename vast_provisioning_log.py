"""Bounded downloads of Vast's image-pull log during worker provisioning."""
import re
import time
import urllib.error
import urllib.request
from pathlib import Path

from logging_helpers import human_log as _log
from secure_files import atomic_write_private_text, ensure_private_directory
from vast_provider import NoRedirect, VastError, positive_id
from vast_jobs import job_id

LOG_ROOT = Path(__file__).resolve().parent / 'data/logs/optimizes_v8'


def collect_provisioning_log(store, identifier, client, instance_id):
    """Fetch at most once a minute; errors cannot interrupt rental supervision."""
    identifier = job_id(identifier)
    state = store.read(identifier)
    now = time.time()
    if now - state.get('provider_log_checked_at', 0) < 60:
        return
    store.update(identifier, provider_log_checked_at=now)
    try:
        text = _download_log(client, instance_id, daemon=True)
        if _missing_log(text):
            text = _download_log(client, instance_id, daemon=False)
        path = ensure_private_directory(LOG_ROOT) / f'vast_{identifier}_provider.log'
        if _missing_log(text):
            # Keep the last useful snapshot, including snapshots from older code.
            if path.is_file() and not path.is_symlink() and not _missing_log(path.read_text()):
                return
            text = (time.strftime('%Y-%m-%dT%H:%M:%S') +
                    f' [INFO] Instance {positive_id(instance_id)}: waiting for provider logs; automatic retry in 60 seconds.\n')
        atomic_write_private_text(path, text)
        progress = image_layer_progress(text, state.get('image_progress'))
        if progress:
            store.update(identifier, image_progress=progress)
    except urllib.error.HTTPError as exc:
        if exc.code != 404:  # A newly requested log may not yet be uploaded.
            _log('VastRunner', f'{identifier}: provisioning log unavailable (HTTP {exc.code})', level='WARNING')
    except VastError as exc:
        _log('VastRunner', f'{identifier}: provisioning log: {exc}', level='WARNING')
    except (OSError, ValueError):
        _log('VastRunner', f'{identifier}: provisioning log temporarily unavailable', level='WARNING')


def _download_log(client, instance_id, *, daemon):
    """Read one bounded provider log without redirects or forwarded credentials."""
    response = client.request('PUT', f'/instances/request_logs/{positive_id(instance_id)}/',
                              {'tail':'1000', **({'daemon_logs':'true'} if daemon else {})})
    url = response.get('result_url', '')
    # Only the documented log bucket, no redirects or forwarded API key.
    if not isinstance(url, str) or not re.fullmatch(r'https://s3\.amazonaws\.com/(?:public\.)?vast\.ai/instance_logs/[A-Za-z0-9_.-]+\.log', url):
        raise VastError('Vast returned an unsupported provisioning-log location')
    for attempt in range(8):
        try:
            with urllib.request.build_opener(NoRedirect()).open(url, timeout=5) as handle:
                raw = handle.read(1024 * 1024 + 1)
            break
        except urllib.error.HTTPError as exc:
            if exc.code not in (403, 404) or attempt == 7:
                raise
            time.sleep(1)
    if len(raw) > 1024 * 1024:
        raise VastError('Vast provisioning log exceeds the size limit')
    return raw.decode('utf-8', errors='replace')


def _missing_log(text):
    """Recognize exact unavailable-log diagnostics while a container is loading."""
    return not text.strip() or bool(re.fullmatch(
        r"(?:cat: /var/lib/vastai_kaalia/data/instance(?:_extra)?_logs/[^\r\n]+: No such file or directory"
        r"|Error response from daemon: No such container: C\.\d+)", text.strip()))


def image_layer_progress(text, previous=None):
    """Count observed Docker layers, preserving completion across rolling log tails."""
    layers = dict((previous or {}).get('layers') or {})
    ranks = {'Pulling fs layer': 0, 'Waiting': 0, 'Verifying Checksum': 1,
             'Download complete': 2, 'Pull complete': 3, 'Already exists': 3}
    for line in text.splitlines():
        match = re.search(r'\b([0-9a-f]{12,64}):\s*(?:\d{4}-\d\d-\d\d \d\d:\d\d:\d\d UTC:\s*)?'
                          r'(Pulling fs layer|Waiting|Verifying Checksum|Download complete|Pull complete|Already exists)\s*$', line)
        if match and (match[1] in layers or len(layers) < 512):
            layers[match[1]] = max(layers.get(match[1], 0), ranks[match[2]])
    if not layers:
        return None
    ready = sum(rank == 3 for rank in layers.values())
    return {'layers': layers, 'total': len(layers), 'ready': ready,
            'downloaded': sum(rank >= 2 for rank in layers.values()),
            'percent': 100 * ready / len(layers)}
