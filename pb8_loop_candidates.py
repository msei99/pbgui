"""Bounded nominations from the complete native optimizer overlay stream."""
from __future__ import annotations

import copy
import heapq
import math

import msgpack

from pb8_loop_store import digest

SERVICE = 'PB8LoopCandidates'


def result_records(path):
    """Reconstruct every complete record, including deletions at periodic boundaries."""
    if not path.exists():
        return
    if path.is_symlink() or not path.is_file():
        raise ValueError('Optimizer result stream must be a regular non-symlink file')

    def overlay(base, change):
        """Apply PB8's recursive overlay without retaining previous configurations."""
        out = dict(base)
        for key, value in change.items():
            if isinstance(value, dict) and value == {'__passivbot_diff_delete__': True}:
                out.pop(key, None)
            elif isinstance(value, dict) and isinstance(out.get(key), dict):
                out[key] = overlay(out[key], value)
            else:
                out[key] = value
        return out

    current = {}
    try:
        with path.open('rb') as handle:
            unpacker = msgpack.Unpacker(handle, raw=False, strict_map_key=False,
                                        max_buffer_size=64 * 1024 * 1024)
            complete_offset = 0
            for index, change in enumerate(unpacker):
                if not isinstance(change, dict):
                    raise ValueError('Optimizer result stream contains a non-object record')
                current = (change if current.get('optimize', {}).get('compress_results_file') is False
                           else overlay(current, change))
                complete_offset = unpacker.tell()
                yield index, current
            # A quota stop can interrupt the final write. Never silently call
            # that a complete scan or use its incomplete candidate.
            if complete_offset < path.stat().st_size:
                raise ValueError('Optimizer result stream ends with an incomplete record')
    except (OSError, msgpack.UnpackException) as exc:
        raise ValueError('Optimizer result stream could not be decoded completely') from exc


def candidate_config(data, initial):
    """Use the same complete native configuration for identity and CPU validation."""
    return {key: copy.deepcopy(data.get(key, value)) for key, value in initial.items()}


def ranking_criteria(record):
    """Use the AI's latest explained CPU criteria, falling back to its interpreted rubric."""
    for entry in reversed(record.get('history', [])):
        criteria = (entry.get('evaluation_comparison') or {}).get('criteria')
        if criteria:
            return criteria
    return [{'metric': metric, 'direction': rule['direction'],
             'reducer': 'max' if rule['direction'] == 'min' else 'mean'}
            for rule in record.get('rubric', []) for metric in rule.get('metrics', [])]


def metric_value(data, metric, reducer, summary):
    """Read the requested training reducer; never substitute a mean for worst-window risk."""
    block = (data.get('suite_metrics') or {}).get('metrics', {}).get(metric)
    if isinstance(block, dict):
        value = block.get('stats', {}).get(reducer)
    else:
        value = summary.get(metric)
    return value if type(value) in (int, float) and math.isfinite(value) else None


def supplemental_candidates(record, job, directory, artifacts, summarize):
    """Merge native tradeoffs and training-criterion leaders within the existing model budget.

    This only nominates candidates. Native checks cannot replace independent CPU
    qualification. Historical advice is handled by the existing provenance gate.
    """
    path = directory / 'all_results.bin'
    if not path.exists():
        return artifacts, None
    maximum = (job.get('optimizer_archive') or {}).get('candidate_capacity_per_job')
    if type(maximum) is not int or maximum < 1:
        maximum = max(len(artifacts), job['config'].get('optimize', {}).get('pareto_max_size', 1000))
    criteria = ranking_criteria(record)
    heaps = [[] for _ in criteria]
    heap_identities = [set() for _ in criteria]
    seen_records = 0
    eligible = 0
    for index, data in result_records(path):
        seen_records += 1
        summary = summarize(data, 'mean')
        if not summary or (data.get('metrics') or {}).get('liquidated') is True:
            continue
        safe = True
        for rule in record.get('rubric', []):
            if rule.get('target') is None:
                continue
            for metric in rule.get('metrics', []):
                value = metric_value(data, metric, 'max' if rule['direction'] == 'min' else 'min', summary)
                if value is None or (value > rule['target'] if rule['direction'] == 'min' else value < rule['target']):
                    safe = False
        if not safe:
            continue
        eligible += 1
        # Overlay records are immutable after yielding; heaps share the same
        # object rather than copying it for each selection criterion.
        identity = digest(candidate_config(data, job['config']))
        for rule, heap, identities in zip(criteria, heaps, heap_identities):
            if identity in identities:
                continue
            value = metric_value(data, rule['metric'], rule['reducer'], summary)
            if value is None:
                continue
            entry = (value if rule['direction'] == 'max' else -value, -index, index, data, identity)
            if len(heap) < maximum:
                heapq.heappush(heap, entry)
                identities.add(identity)
            elif entry[:2] > heap[0][:2]:
                removed = heapq.heapreplace(heap, entry)
                identities.remove(removed[4])
                identities.add(identity)

    ranked = [[(f'all_results.bin#{item[2]}', item[3]) for item in sorted(heap, reverse=True)]
              for heap in heaps]
    # Put one leader from every available CPU criterion first, then interleave
    # native Pareto tradeoffs and further leaders. This is a visible context
    # bound, not a forced GPU objective or a CPU-validation quota.
    streams = [iter(items) for items in ranked]
    selected, identities = [], set()

    def add(item):
        """Deduplicate configurations, not just filenames or equal metric values."""
        identity = digest(candidate_config(item[1], job['config']))
        if identity not in identities and len(selected) < maximum:
            identities.add(identity)
            selected.append(item)

    for stream in streams:
        item = next(stream, None)
        if item is not None:
            add(item)
    streams.insert(0, iter(artifacts))
    while streams and len(selected) < maximum:
        active = []
        for stream in streams:
            item = next(stream, None)
            if item is not None:
                add(item)
                active.append(stream)
        streams = active
    return selected, {'records_scanned': seen_records, 'native_safety_screened_records': eligible,
                      'native_pareto_artifacts': len(artifacts), 'offered_distinct_candidates': len(selected),
                      'candidate_capacity': maximum, 'criteria': criteria,
                      'policy': 'CPU criterion leaders plus native Pareto tradeoffs; independent CPU validation required',
                      'scope': 'optimizer training only; no Holdout or Full Time Range reports'}
