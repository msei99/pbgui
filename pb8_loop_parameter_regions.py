"""Bounded descriptive parameter regions from verified native CPU nominations."""
from __future__ import annotations

import heapq
import math

from pb8_loop_store import digest

SERVICE = 'PB8LoopParameterRegions'
MAX_SOURCES = 4
MAX_PARAMETERS = 12
BINS = 8
MAX_ELITE = 32
MIN_CANDIDATES = 16


def regions(record, criteria):
    """Describe high-ranked regions, without proxy/observer data or a new search rule."""
    from pb8_loop_bounds import active_ranges, _leaf, _number
    unavailable = {'available': False, 'profiles': [], 'reason': 'No qualified native CPU parameter cohort'}
    if record.get('optimizer_research_version') != 1 or not criteria:
        return unavailable
    rule = criteria[0]
    if rule.get('direction') not in {'min', 'max'}:
        return unavailable
    jobs = {job['operation']: job for job in record.get('jobs', []) if job.get('kind') == 'optimizer'}
    groups, seen = {}, set()
    for candidate in record.get('candidates', []):
        source = candidate.get('optimizer_job')
        if (source not in jobs or candidate.get('native_cpu_qualified') is not True
                or candidate.get('observer_only') or candidate.get('overrides')):
            continue
        stats = (candidate.get('metric_statistics') or {}).get(rule.get('metric'))
        config = candidate.get('config')
        if not isinstance(stats, dict) or not isinstance(config, dict):
            continue
        value = stats.get(rule.get('reducer'))
        bot = config.get('bot')
        if not _number(value) or not isinstance(bot, dict):
            continue
        identity = (source, digest([bot, candidate['config'].get('live', {}).get('strategy_kind')]))
        if identity in seen:
            continue
        seen.add(identity)
        groups.setdefault(source, []).append((value, candidate))
    profiles = []
    sources = sorted(groups, key=lambda op: (jobs[op].get('round', -1), op), reverse=True)
    for operation in sources[:MAX_SOURCES]:
        cohort, source = groups[operation], jobs[operation]
        if len(cohort) < MIN_CANDIDATES:
            continue
        elite_size = min(MAX_ELITE, max(4, math.ceil(len(cohort) * .2)))
        quality = lambda value: value if rule['direction'] == 'max' else -value
        cutoff = min(heapq.nlargest(elite_size, (quality(value) for value, _ in cohort)))
        elite_count = sum(quality(value) >= cutoff for value, _ in cohort)
        ranges = active_ranges(source.get('executed_config') or source['config'], record.get('settings', {}).get('goals') or {})
        incumbent = ((record.get('best') or {}).get('candidate') or {}).get('config', {}).get('bot', {})
        rows = []
        for path, (low, high) in ranges.items():
            if high <= low:
                continue
            counts, elite, sums = [0] * BINS, [0] * BINS, [0.0] * BINS
            for value, candidate in cohort:
                parameter = _leaf(candidate['config']['bot'], path)
                if not _number(parameter) or not low <= parameter <= high:
                    continue
                index = min(BINS - 1, int(BINS * (parameter - low) / (high - low)))
                counts[index] += 1
                sums[index] += value
                elite[index] += quality(value) >= cutoff
            total, good = sum(counts), sum(elite)
            if total < MIN_CANDIDATES or not good:
                continue
            contrast = sum(abs(elite[i] / good - counts[i] / total) for i in range(BINS)) / 2
            point = _leaf(incumbent, path)
            rows.append({'parameter': path, 'bounds': [low, high], 'qualified_bins': counts,
                         'elite_bins': elite, 'mean_criterion_by_bin': [
                             sums[i] / counts[i] if counts[i] and math.isfinite(sums[i] / counts[i]) else None
                             for i in range(BINS)],
                         'distribution_contrast': contrast,
                         'incumbent_value': point if _number(point) else None,
                         'incumbent_inside_range': _number(point) and low <= point <= high})
        rows.sort(key=lambda row: (-row['distribution_contrast'], row['parameter']))
        profiles.append({'optimizer_operation': operation, 'round': source.get('round'),
                         'criterion': rule, 'qualified_distinct_count': len(cohort), 'elite_count': elite_count,
                         'requested_budget': (source.get('research') or {}).get('budget'),
                         'actual_proxy_evaluations': source.get('proxy_evaluations'),
                         'parameter_count': len(rows), 'parameters': rows[:MAX_PARAMETERS],
                         'other_parameter_names': [row['parameter'] for row in rows[MAX_PARAMETERS:]]})
    return {'available': bool(profiles), 'profiles': profiles, 'bin_count': BINS,
            'omitted_source_count': max(0, len(sources) - MAX_SOURCES),
            'scope': 'Distinct qualified loaded native CPU nominations, not the full optimizer stream or independent trials',
            'interpretation': 'Equal-width parameter bins; elite follows the first unchanged CPU criterion, including cutoff ties. '
                'Associations are confounded, not causal proof. Different scoring can change the sampled cohort. '
                'Do not automatically cut out the known incumbent or narrow around one bot. '
                'Requested and actual work may differ; preserve hard goals and every boundary-response gate.'}
