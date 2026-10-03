"""Measured optimizer throughput from bounded, timestamped native GPU log tails."""

from datetime import datetime, timezone
import json
import math
import re

SERVICE = "VastThroughput"
LOG_TAIL_BYTES = 512 * 1024
_STAMP = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.\d+)?Z\b")
_COUNTERS = re.compile(
    r"GPU (?:optimize\s*\|\s*gen=\d+\s+proxy=(\d+)\s+\([0-9.]+/s\)\s+exact=(\d+)"
    r"|optimization complete\s*\|\s*generations=\d+\s+proxy=(\d+)\s+exact=(\d+)"
    r"|optimizer progress\s*\|[^\n]*?\bevolution_proxy_completed_run=(\d+)(?=\s|$)"
    r"[^\n]*?\bevolution_exact=(\d+)/\d+(?=\s|$))"
)
_REPORTED_PROXY_RATE = re.compile(r"GPU optimize\s*\|[^\n]*?\bproxy=\d+\s+\(([0-9.]+)/s\)")


def parse_throughput(raw_log: bytes, previous: dict | None = None) -> dict | None:
    """Keep the last measured proxy rate and measure native Exact progress."""
    samples = []
    reset = False
    reported_proxy_rate = None
    for line in raw_log[-LOG_TAIL_BYTES:].decode("utf-8", errors="replace").splitlines():
        stamp = _STAMP.match(line)
        if not stamp:
            continue
        counts = _COUNTERS.search(line)
        progress = None
        if counts is None and '[gpu-profile] ' in line:
            try:
                progress = json.loads(line.split('[gpu-profile] ', 1)[1])
            except json.JSONDecodeError:
                continue
            if not isinstance(progress, dict) or progress.get('event') != 'exact_progress':
                continue
        elif counts is None:
            continue
        try:
            sampled_at = datetime.strptime(stamp[1], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
            if counts is not None:
                pair = next((counts[index], counts[index + 1]) for index in (1, 3, 5) if counts[index] is not None)
                proxy, exact = (int(value) for value in pair)
            else:
                exact = progress.get('exact_completed')
                if type(exact) is not int or exact < 0:
                    continue
                proxy = samples[-1]['proxy_total'] if samples else (previous or {}).get('proxy_total')
                if type(proxy) is not int:
                    continue
        except (ValueError, OverflowError):
            continue
        if max(proxy, exact) > 2**53 - 1:
            continue
        if counts is not None:
            rate_match = _REPORTED_PROXY_RATE.search(line)
            if rate_match:
                native_rate = float(rate_match[1]) * 60
                if math.isfinite(native_rate):
                    reported_proxy_rate = native_rate
        sample = dict(sampled_at=sampled_at, proxy_total=proxy, exact_total=exact)
        if samples:
            last = samples[-1]
            if (sampled_at < last['sampled_at'] or proxy < last['proxy_total']
                    or exact < last['exact_total']):
                samples = []  # A restarted counter/clock must not create a cross-run rate.
                reset = True
            elif sampled_at == last['sampled_at']:
                samples.pop()  # One-second log precision: retain the last counters in that second.
        samples.append(sample)
        samples = samples[-128:]
    if not samples:
        return previous
    latest = samples[-1]
    if previous and isinstance(previous.get('samples'), list) and previous['samples']:
        old = previous['samples'][-1]
        if latest['sampled_at'] < old['sampled_at']:
            return previous  # A delayed HTTP snapshot must not roll telemetry backwards.
        if latest == old:
            return previous
        if (not reset and latest['proxy_total'] >= old['proxy_total']
                and latest['exact_total'] >= old['exact_total']):
            prefix = [sample for sample in previous['samples'][-128:]
                      if sample['sampled_at'] < samples[0]['sampled_at']
                      and sample['proxy_total'] <= samples[0]['proxy_total']
                      and sample['exact_total'] <= samples[0]['exact_total']]
            samples = (prefix + samples)[-128:]
        elif latest['proxy_total'] < old['proxy_total'] or latest['exact_total'] < old['exact_total']:
            samples = [latest]
            reset = True
    latest = samples[-1]
    baseline = samples[0]
    for sample in samples[:-1]:
        if sample['sampled_at'] <= latest['sampled_at'] - 60:
            baseline = sample
    seconds = latest['sampled_at'] - baseline['sampled_at']
    proxy_delta = latest['proxy_total'] - baseline['proxy_total']
    exact_delta = latest['exact_total'] - baseline['exact_total']
    last_proxy_rate = (previous or {}).get('proxy_per_minute') if not reset else None
    last_exact_rate = (previous or {}).get('exact_per_minute') if not reset else None
    proxy_rate = (proxy_delta * 60 / seconds if seconds >= 10 and proxy_delta > 0
                  else reported_proxy_rate if reported_proxy_rate is not None
                  else last_proxy_rate if last_proxy_rate is not None
                  else 0 if seconds >= 10 else None)
    exact_rate = (exact_delta * 60 / seconds if seconds >= 10 and exact_delta > 0
                  else last_exact_rate if last_exact_rate is not None
                  else 0 if seconds >= 10 else None)
    return dict(latest, samples=samples, window_seconds=seconds,
                proxy_per_minute=proxy_rate, exact_per_minute=exact_rate,
                proxy_per_exact=latest['proxy_total'] / latest['exact_total'] if latest['exact_total'] else None)


def observe_throughput(store, identifier: str, raw_log: bytes) -> dict | None:
    """Persist only new observations under the job's reentrant cross-process state lock."""
    from file_lock import advisory_file_lock

    with advisory_file_lock(store.directory(identifier) / '.state-lock'):
        state = store.read(identifier)
        previous = state.get('throughput')
        result = parse_throughput(raw_log, previous)
        if result != previous:
            store.update(identifier, throughput=result)
    return {key: value for key, value in result.items() if key != 'samples'} if result else None
