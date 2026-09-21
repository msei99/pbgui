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


def collect_provisioning_log(store, identifier, client, instance_id, *, retry_waiting=False):
    """Fetch at most once a minute, except once when a waiting worker becomes ready."""
    identifier = job_id(identifier)
    state = store.read(identifier)
    now = time.time()
    path = ensure_private_directory(LOG_ROOT) / f'vast_{identifier}_provider.log'
    waiting_snapshot = False
    if retry_waiting and path.is_file() and not path.is_symlink():
        try:
            waiting_snapshot = 'waiting for provider logs' in path.read_text()
        except OSError:
            waiting_snapshot = False
    if now - state.get('provider_log_checked_at', 0) < 60 and not waiting_snapshot:
        return
    store.update(identifier, provider_log_checked_at=now)
    try:
        text = _download_log(client, instance_id, daemon=True)
        if _missing_log(text):
            text = _download_log(client, instance_id, daemon=False)
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


def image_layer_progress(text, previous=None, *, image=None):
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
    result = {'layers': layers, 'total': len(layers), 'ready': ready,
            'downloaded': sum(rank >= 2 for rank in layers.values()),
            'percent': 100 * ready / len(layers)}
    from vast_image_layers import IMAGE_LAYERS
    sizes = IMAGE_LAYERS.get(image)
    if sizes:
        # Wrapper layers in host logs are excluded from the pinned image totals.
        known = {}
        for digest, rank in layers.items():
            prefix = digest[:12]
            if prefix in sizes:
                known[prefix] = max(rank, known.get(prefix, 0))
        total_bytes = sum(sizes.values())
        completed_bytes = sum(sizes[key] for key, rank in known.items() if rank >= 2)
        result.update(total=len(sizes), ready=sum(rank == 3 for rank in known.values()),
                      downloaded=sum(rank >= 2 for rank in known.values()),
                      total_bytes=total_bytes, completed_bytes=completed_bytes,
                      download_percent=100 * completed_bytes / total_bytes,
                      bytes_basis='completed_layers_including_cache')
        result['percent'] = 100 * result['ready'] / result['total']
    return result
