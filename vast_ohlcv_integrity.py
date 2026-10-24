"""Read-only, pinned-worker OHLCV admission before cloud packaging and rental."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import io
import json
import math
import time
from pathlib import Path
from functools import lru_cache
from zipfile import BadZipFile
from bisect import bisect_left, bisect_right

import numpy as np

from vast_provider import VastError

SERVICE = 'Vast'
CONTRACT = 'pb8-source-dir-v1'
PROBE_REVISION = CONTRACT + ':2'
MINUTE = 60_000


def _runs(timestamps):
    """Compact sorted timestamp facts while retaining non-minute intervals."""
    ts = np.unique(timestamps[np.isfinite(timestamps)])
    if not len(ts):
        return []
    breaks = np.flatnonzero(np.diff(ts) != MINUTE) + 1
    return [[float(part[0]), float(part[-1])] for part in np.split(ts, breaks)]


def signature(path: Path) -> tuple:
    """Include ctime/inode so replacing bytes with a restored mtime invalidates facts."""
    item = path.stat()
    return item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns, item.st_ctime_ns


def _probe(raw: bytes, suffix: str) -> dict:
    """Mirror pinned load_ohlcv_data normalization, without its NPY rewrite.

    Only timestamps affect admission. Validate numeric OHLCV columns as the
    worker reader does, but do not impose the stricter daily repair policy.
    Retain runs, rather than complete historical price DataFrames.
    """
    try:
        loaded = np.load(io.BytesIO(raw), allow_pickle=False)
        if suffix == '.npz':
            with loaded as data:
                arr = data['candles']
                if arr.dtype.names is None or any(k not in arr.dtype.names for k in ('ts', 'o', 'h', 'l', 'c', 'bv')):
                    raise ValueError('Invalid structured candles')
                ts = arr['ts'].astype(np.int64).astype(float)
                arr['o'].astype(float)
                finite_rows = np.isfinite(np.column_stack([arr[field].astype(float) for field in ('h', 'l', 'c', 'bv')])).all(axis=1)
                _, reverse_indices = np.unique(ts[::-1], return_index=True)
                kept = len(ts) - 1 - reverse_indices  # Original NPZ keeps the last duplicate timestamp.
        else:
            arr = loaded
            if arr.ndim != 2 or arr.shape[1] != 6 or arr.dtype.kind not in 'iufc':
                raise ValueError('Invalid NPY columns')
            arr[:, 1:].astype(float)
            ts = arr[:, 0].astype(float)
            finite_rows = np.isfinite(arr[:, 2:].astype(float)).all(axis=1)
            _, kept = np.unique(ts, return_index=True)  # Source merge keeps the first NPY timestamp.
        finite = ts[np.isfinite(ts) & (ts != 0)]
        uniq = np.unique(finite)
        if uniq.size > 1:
            median = float(np.median(np.diff(uniq)))
            if abs(median - round(median / 1000) * 1000) >= 1e-6:
                ts = ts * 1000
        elif uniq.size:
            representative = float(np.abs(finite).max())
            if representative >= 1e15:
                ts = ts / 1000
            elif 1e9 <= representative < 1e12:
                ts = ts * 1000
        return {'readable': True, 'nonempty': bool(len(ts)), 'runs': _runs(ts),
                'finite_runs': _runs(ts[kept][finite_rows[kept]])}
    except (OSError, ValueError, TypeError, KeyError, EOFError, OverflowError, AttributeError, BadZipFile):
        # The source loader tries NPY when NPZ cannot be decoded or is empty.
        return {'readable': False, 'nonempty': False, 'runs': [], 'finite_runs': []}


def inspect_file(path: Path, db_path: Path) -> dict:
    """Read at most three times, binding cached facts to exactly the hashed bytes."""
    from market_data_integrity import cached_file_probe

    for _attempt in range(3):
        before = signature(path)
        raw = path.read_bytes()
        sha = hashlib.sha256(raw).hexdigest()
        facts = cached_file_probe(db_path, path, PROBE_REVISION, before, sha, lambda: _probe(raw, path.suffix))
        if before == signature(path):
            return dict(facts, signature=list(before), sha256=sha)
    raise VastError('Market data changed during integrity validation; prepare the job again. No instance rented.', 409)


def _issue(exchange, coin, start, end, context, kind='missing candles'):
    """Keep exact affected UTC days and missing-minute counts in actionable errors."""
    parts = []
    cursor = start
    while cursor <= end:
        day = datetime.fromtimestamp(cursor / 1000, timezone.utc).strftime('%Y-%m-%d')
        boundary = (int(cursor) // 86_400_000 + 1) * 86_400_000
        last = min(end, boundary - MINUTE)
        minutes = max(1, math.floor((last - cursor) / MINUTE) + 1)
        parts.append(f'{exchange}/{coin} {day}: {minutes} missing minutes ({kind}; {context})')
        cursor = boundary
    return parts


def validate_dataset(facts, start, end, tolerance, listing, *, exchange, coin, context):
    """Apply the worker's gap, listing-prefix and exact-end decisions to file runs."""
    runs = []
    for item in facts:
        for first, last in item['runs']:
            first = first + max(0, math.ceil((start - first) / MINUTE)) * MINUTE
            last = last - max(0, math.ceil((last - end) / MINUTE)) * MINUTE
            if first <= last:
                runs.append((int(first), int(last)))
    runs.sort()
    if not runs:
        return _issue(exchange, coin, start, end, context)
    merged = []
    irregular = False
    for first, last in runs:
        if merged and first <= merged[-1][1] + MINUTE:
            if (first - merged[-1][0]) % MINUTE or (last - merged[-1][0]) % MINUTE:
                irregular = True
            merged[-1][1] = max(merged[-1][1], last)
        else:
            merged.append([first, last])
    errors = []
    first, last = merged[0][0], merged[-1][1]
    if first > start and (listing is None or abs(first - int(listing)) > MINUTE):
        errors += _issue(exchange, coin, start, first - MINUTE, context, 'unverified listing prefix')
    if last < end:
        errors += _issue(exchange, coin, last + MINUTE, end, context, 'incomplete range end')
    limit = int(tolerance * MINUTE)
    if coin.startswith('xyz:'):
        limit = max(limit, 4 * 24 * 60 * MINUTE)
    if irregular and limit < MINUTE:
        errors.append(f'{exchange}/{coin} {datetime.fromtimestamp(first / 1000, timezone.utc):%Y-%m-%d}: '
                      f'0 missing minutes (non-minute timestamp spacing; {context})')
    # The original checks tolerance only when at least one interval differs
    # from one minute, with a minimum greatest interval of one minute.
    for left, right in zip(merged, merged[1:]):
        if max(MINUTE, right[0] - left[1]) > limit:
            errors += _issue(exchange, coin, left[1] + MINUTE, right[0] - MINUTE, context, 'internal gap')
    return errors


def _has_finite_rows(files, start, end):
    """Respect first-loaded duplicate timestamps across days without collecting frames."""
    seen = {}
    for item in files:
        for first, last in item['finite_runs']:
            candidate = first + max(0, math.ceil((start - first) / MINUTE)) * MINUTE
            intervals = seen.get(first % MINUTE, [])
            index = max(0, bisect_right(intervals, (candidate, float('inf'))) - 1)
            for lower, upper in intervals[index:]:
                if lower > candidate:
                    break
                if candidate <= upper:
                    candidate = upper + MINUTE
            if candidate <= min(last, end):
                return True
        for first, last in item['runs']:
            intervals = seen.setdefault(first % MINUTE, [])
            index = bisect_left(intervals, (first, last))
            if index and intervals[index - 1][1] + MINUTE >= first:
                index -= 1
            finish = index
            while finish < len(intervals) and intervals[finish][0] <= last + MINUTE:
                first = min(first, intervals[finish][0])
                last = max(last, intervals[finish][1])
                finish += 1
            intervals[index:finish] = [(first, last)]
    return False


def validate_export(shards, requirements, markets, inception, db_path):
    """Check all selected datasets offline; return byte-bound facts for copying."""
    from vast_inception import CACHE_FILES

    checked = {str(relative): inspect_file(path, db_path) for path, relative in shards}
    selected = {}
    for _path, relative in shards:
        selected.setdefault(tuple(relative.parts[:3]), {}).setdefault(relative.stem, []).append(relative)
    datasets = []
    for (exchange, _timeframe, dataset), days in sorted(selected.items()):
        coin = next(coin for coin, symbol in markets[exchange].items() if symbol.replace('/', '_') == dataset)
        venue = 'binanceusdm' if exchange == 'binance' else exchange
        listing = inception['files'][CACHE_FILES[1]].get(coin, {}).get(venue)
        datasets.append((exchange, coin, days, listing))

    def usable_files(days, start, end):
        """Use only the days and NPZ/NPY precedence the source reader sees."""
        usable = []
        first_day = datetime.fromtimestamp(start / 1000, timezone.utc).strftime('%Y-%m-%d')
        last_day = datetime.fromtimestamp(end / 1000, timezone.utc).strftime('%Y-%m-%d')
        for day, candidates in sorted(days.items()):
            if first_day <= day <= last_day:
                for relative in sorted(candidates, key=lambda p: (p.suffix != '.npz', str(p))):
                    item = checked[str(relative)]
                    if item['readable'] and item['nonempty']:
                        usable.append(item)
                        break
        return usable

    issues = []
    for request in requirements:
        starts = []
        for exchange, coin, days, listing in datasets:
            forced = request.get('coin_sources', {}).get(coin)
            if (coin not in request['coins'] or (forced is not None and forced != exchange)
                    or (exchange not in request['exchanges'] and forced != exchange)):
                continue
            start = request['start_ts']
            age = request.get('minimum_coin_age_days', 0)
            if not coin.startswith('xyz:') and (request['label'] == 'combined' or age > 0):
                unified = (inception['files'][CACHE_FILES[0]][coin]
                           if request['label'] == 'combined' else listing)
                if request['label'] != 'combined' and age > 0 and (
                        listing >= request['end_ts'] or int(round(time.time() * 1000 - unified) / 86_400_000) < age):
                    continue
                start = max(start, int(unified + age * 86_400_000))
                if request['label'] != 'combined':
                    start = max(start, int(listing))
            if start > request['end_ts']:
                continue  # The worker skips markets not old enough in this window.
            usable = usable_files(days, start, request['end_ts'])
            issues += validate_dataset(usable, start, request['end_ts'], request['tolerance'], listing,
                                       exchange=exchange, coin=coin, context=request['label'])
            if not _has_finite_rows(usable, start, request['end_ts']):
                continue
            first_rows = [first + max(0, math.ceil((start - first) / MINUTE)) * MINUTE
                          for item in usable for first, last in item['runs']
                          if last >= start and first <= request['end_ts']]
            if first_rows:
                starts.append(min(first_rows))
        if not starts:
            issues.append(f"{request['label']}: no usable selected OHLCV markets in the requested window "
                          '(including minimum coin age, warmup and finite OHLCV rows)')
        # The loader fetches BTC over the resulting shared payload, which can
        # begin after the requested warmup when all selected coins listed later.
        reference_start = min(starts) if starts else request['start_ts']
        for exchange, coin, days, listing in datasets:
            if coin != 'BTC' or exchange not in request.get('reference_exchanges', request['exchanges']):
                continue
            issues += validate_dataset(usable_files(days, reference_start, request['end_ts']), reference_start,
                                       request['end_ts'], request['tolerance'], listing,
                                       exchange=exchange, coin=coin, context=request['label'] + ' BTC reference')
    if issues:
        raise VastError('OHLCV integrity preflight failed before rental: ' + '; '.join(dict.fromkeys(issues))
                        + '. Download the affected 1m history in Market Data and prepare the job again. No instance rented.', 422)
    return checked


def proof_payload(manifest):
    """Canonicalize every decision input, metadata snapshot and exported file hash."""
    return {key: value for key, value in manifest.items() if key != 'ohlcv_integrity'}


def seal_manifest(manifest):
    """Bind the versioned proof to the immutable manifest, not a bare success flag."""
    payload = json.dumps(proof_payload(manifest), sort_keys=True, allow_nan=False).encode()
    return {'contract': CONTRACT, 'sha256': hashlib.sha256(payload).hexdigest()}


@lru_cache(maxsize=32768)
def _stable_digest(path: str, stamp: tuple) -> str:
    """Bound immutable-input hash caching by inode, size, mtime and ctime."""
    with Path(path).open('rb') as stream:
        sha = hashlib.file_digest(stream, 'sha256').hexdigest()
    if signature(Path(path)) != stamp:
        raise VastError('Prepared cloud input changed while checking; prepare this job again', 409)
    return sha


def _frozen_digest(path: Path, root: Path) -> str:
    """Reject symlinks and traversal before hashing loose or archived input."""
    if path.is_symlink() or not path.resolve(strict=True).is_relative_to(root.resolve(strict=True)) or not path.is_file():
        raise VastError('Prepared cloud input path changed; prepare this job again', 409)
    return _stable_digest(str(path), signature(path))


def require_manifest_proof(store, identifier):
    """Reject old or changed archives before reuse, direct start, rental or dispatch."""
    try:
        return _require_manifest_proof(store, identifier)
    except VastError:
        raise
    except (OSError, ValueError, TypeError, KeyError, AttributeError) as exc:
        from logging_helpers import human_log
        human_log(SERVICE, 'Prepared OHLCV proof cannot be read', level='WARNING',
                  meta={'error_type': type(exc).__name__})
        raise VastError('OHLCV integrity proof cannot be read; prepare this job again before rental or dispatch.', 422) from None


def _require_manifest_proof(store, identifier):
    """Validate the full persisted proof, converting malformed input at the boundary."""
    from vast_jobs import REVISION

    manifest = store.read(identifier, 'input/manifest.json')
    intent = store.read(identifier, 'intent.json')
    folder = store.directory(identifier)
    markets = (manifest.get('metadata_snapshot') or {}).get('markets')
    if (not isinstance(markets, dict) or not markets or any(
            not isinstance(item, dict) or type(item.get('fetched_at')) not in (int, float)
            or not 0 <= time.time() - item['fetched_at'] < 86400 for item in markets.values())):
        raise VastError('Prepared market metadata is missing or expired; prepare fresh input before renting.', 422)
    if (manifest.get('pb8_revision') != REVISION
            or not manifest.get('ohlcv_requirements') or not manifest.get('metadata_snapshot')
            or manifest.get('ohlcv_integrity') != seal_manifest(manifest)
            or _frozen_digest(folder / 'input/optimize.json', store.root) != manifest.get('config_sha256')
            or _frozen_digest(folder / 'input.tar.gz', store.root) != intent.get('bundle_sha256')
            or _frozen_digest(folder / 'input/manifest.json', store.root) != intent.get('manifest_sha256')):
        raise VastError('OHLCV integrity proof is missing or changed; prepare this job again before rental or dispatch.', 422)
    source = store.prepared_input_directory(identifier)
    for entry in manifest.get('files', []):
        relative = Path(entry['path'])
        if relative.is_absolute() or '..' in relative.parts or '\\' in str(relative) or any(ord(c) < 32 for c in str(relative)):
            raise VastError('Invalid prepared cloud input path', 422)
        if _frozen_digest(source / relative, source) != entry.get('sha256'):
            raise VastError('Prepared OHLCV bytes changed; prepare this job again before rental or dispatch', 422)
    return manifest
