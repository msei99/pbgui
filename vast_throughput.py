"""Measured optimizer throughput from bounded, timestamped native GPU log tails."""

from datetime import datetime, timezone
import re

SERVICE = "VastThroughput"
_STAMP = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.\d+)?Z\b")
_COUNTERS = re.compile(
    r"GPU (?:optimize\s*\|\s*gen=\d+\s+proxy=(\d+)\s+\([0-9.]+/s\)\s+exact=(\d+)"
    r"|optimization complete\s*\|\s*generations=\d+\s+proxy=(\d+)\s+exact=(\d+))"
)


def parse_throughput(raw_log: bytes, previous: dict | None = None) -> dict | None:
    """Measure counter deltas over roughly a minute; never infer work from population size."""
    samples = []
    reset = False
    for line in raw_log[-65536:].decode("utf-8", errors="replace").splitlines():
        stamp, counts = _STAMP.match(line), _COUNTERS.search(line)
        if not stamp or not counts:
            continue
        try:
            sampled_at = datetime.strptime(stamp[1], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
            proxy, exact = (int(value) for value in ((counts[1], counts[2]) if counts[1] is not None else (counts[3], counts[4])))
        except (ValueError, OverflowError):
            continue
        if max(proxy, exact) > 2**53 - 1:
            continue
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
    latest = samples[-1]
    baseline = samples[0]
    for sample in samples[:-1]:
        if sample['sampled_at'] <= latest['sampled_at'] - 60:
            baseline = sample
    seconds = latest['sampled_at'] - baseline['sampled_at']
    return dict(latest, samples=samples, window_seconds=seconds,
                proxy_per_minute=(latest['proxy_total'] - baseline['proxy_total']) * 60 / seconds if seconds >= 10 else None,
                exact_per_minute=(latest['exact_total'] - baseline['exact_total']) * 60 / seconds if seconds >= 10 else None,
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
