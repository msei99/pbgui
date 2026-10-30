"""Visible own-job archive budgets and explicit historical source selection."""
from __future__ import annotations

import copy
import math

import numpy as np

from pb8_loop_evidence import LoopAIPause, SAFE_METRICS, required_content, tokens, sha, EvidenceSession

SERVICE = 'PB8LoopArchive'


def archive_budget(record, config):
    """Derive a conservative candidate bound from this model and rubric, not a fixed 512."""
    profile = record.get('ai_model_profile')
    if not profile:
        raise LoopAIPause('context_budget', 'Interpretation must establish the effective model context before optimization')
    metrics = SAFE_METRICS | {metric for rule in record['rubric'] for metric in rule['metrics']}
    # Finite native float worst-width example plus bounded native provenance overhead.
    candidate = {'id': 'c_' + 'f' * 32, 'optimizer_job': 'f' * 32,
                 'metrics': {metric: -1.7976931348623157e308 for metric in sorted(metrics)},
                 'source': {'run': 'r' * 256, 'artifact': 'f' * 255, 'modified': 1.7976931348623157e308}}
    per_candidate = max(1, tokens(candidate))
    output = profile.get('output_tokens', 32768)
    ceiling = int(profile['context'] * (.75 * .5 if record['settings']['provider'] in {'chatgpt', 'claude-code'} else .9))
    overhead = tokens({'initial_config': record['initial_config'], 'goals': record['settings']['goals'],
                       'rubric': record['rubric']}) + profile.get('knowledge_tokens', 6000) + 8000
    if record['settings']['provider'] not in {'chatgpt', 'claude-code'}:
        overhead += profile.get('additional_context_tokens', 12000)
    capacity = math.floor(((ceiling - output - 512) / 1.25 - overhead) / per_candidate)
    parallel = record['settings']['parallel']
    cap = capacity // parallel
    objectives = config.get('optimize', {}).get('scoring') or []
    extremes = max(1, 2 * len(objectives))
    if cap < extremes:
        raise LoopAIPause('context_budget', 'The archive budget cannot preserve the native objective extremes',
                          capacity=cap, extrema_capacity=extremes)
    requested = config.get('optimize', {}).get('pareto_max_size', 1000)
    if type(requested) is not int or requested < 1:
        requested = 1000
    return {'effective': min(requested, cap), 'requested': requested,
            'candidate_capacity_per_job': cap, 'candidate_capacity_total': capacity,
            'extrema_capacity': extremes, 'candidate_proxy_tokens': per_candidate,
            'context': profile['context'], 'parallel_jobs': parallel,
            'reason': 'Measured-profile context reserve and current goal/safety schema', 'version': 1}


def thin_with_extremes(candidates, maximum):
    """PB8 b9d6692 crowding/extrema rule; never claim an equivalent incremental archive."""
    if type(maximum) is not int or maximum < 1:
        raise ValueError('Positive historical candidate capacity required')
    semantics = [item.get('native_semantics') for item in candidates]
    if not semantics or any(not isinstance(item, dict) for item in semantics):
        raise ValueError('Historical native semantics are unavailable')
    signature = semantics[0].get('scoring')
    # Only explicit modern goals are safe; unknown/legacy objective conversion is not guessed.
    if not signature or any(not isinstance(spec, dict) or spec.get('goal') not in {'min', 'max'}
                            or not isinstance(spec.get('metric'), str) for spec in signature):
        raise ValueError('Historical objective directions cannot be reconstructed safely')
    if any(item.get('scoring') != signature or type(item.get('constraint_violation')) not in (int, float)
           or not math.isfinite(item['constraint_violation']) for item in semantics):
        raise ValueError('Historical constraints or objective definitions differ')
    matrix = []
    for item in semantics:
        values = item.get('objectives') or {}
        row = [values.get(spec['metric']) for spec in signature]
        if any(type(value) not in (int, float) or not math.isfinite(value) for value in row):
            raise ValueError('Historical native objectives contain unknown or nonfinite values')
        matrix.append(row)
    arr = np.asarray(matrix, dtype=float)
    required = set()
    distances = np.zeros(len(candidates))
    for column in range(arr.shape[1]):
        order = np.argsort(arr[:, column])
        required.update((int(np.argmin(arr[:, column])), int(np.argmax(arr[:, column]))))
        if len(order) <= 2:
            distances[:] = np.inf
            continue
        distances[order[0]] = distances[order[-1]] = np.inf
        span = arr[order[-1], column] - arr[order[0], column]
        if span:
            distances[order[1:-1]] += (arr[order[2:], column] - arr[order[:-2], column]) / span
    remove = []
    for index in sorted(range(len(candidates)), key=lambda i: distances[i]):
        if index in required:
            continue
        if len(remove) >= len(candidates) - maximum:
            break
        remove.append(index)
    removed = set(remove)
    return [candidate for index, candidate in enumerate(candidates) if index not in removed]


def select_sources(record, content, profile, output_tokens, *, metadata=None):
    """Select whole source archives, with an explicit thinning exception, never filename slicing."""
    from pb8_loop_ai_policy import check_context
    from pb8_loop_ai import instructions_for_run
    from pb8_loop_ai_v2 import initial_payload, PROTOCOL
    metadata = metadata or {'protocol': 'chat'}
    instructions = instructions_for_run(record)['text'] + PROTOCOL
    candidates = content['evidence'].get('existing_results') or []
    if not record['settings'].get('historical_advice', True):
        return [], {'mode': 'off', 'sources': [], 'in_a': 0, 'in_b': len(candidates)}
    from ai_loop_windows import filter_historical_candidates
    candidates = filter_historical_candidates(record, candidates)
    preferred = (record['settings'].get('source') or {}).get('result_path')
    groups = {}
    for candidate in candidates:
        source = candidate.get('source') or {}
        groups.setdefault(source.get('path') or source.get('run') or candidate['optimizer_job'], []).append(candidate)
    def order(item):
        """Prefer the user's explicit source, then newest, then stable source identity."""
        key, rows = item
        stamps = [(row.get('source') or {}).get('modified') for row in rows]
        stamp = max((value for value in stamps if type(value) in (int, float) and math.isfinite(value)), default=-1)
        return (key != preferred, -stamp, key)
    chosen, diagnostics = [], []
    def fits(values):
        """Measure the complete projected mandatory request, not candidate count alone."""
        trial = copy.deepcopy(content)
        trial['evidence']['existing_results'] = values
        required = required_content(trial)
        try:
            pager = EvidenceSession(None, {'id': '0' * 32, 'sha256': '0' * 64, 'required': required}, profile.get('page_tokens', 5000))
            payload = initial_payload(record['settings'], metadata, profile, instructions, required, output_tokens)
            count = check_context(payload, profile,
                                  output_tokens, codex=record['settings']['provider'] in {'chatgpt', 'claude-code'},
                                  required_pages=len(pager.required_pages), initial=True)
            return count
        except LoopAIPause:
            return None
    for key, rows in sorted(groups.items(), key=order):
        selected, reason = rows, 'complete_source'
        if fits(chosen + rows) is None:
            selected = []
            reason = 'remaining_context_budget'
            if fits(rows) is None:
                reason = 'historical_source_too_large'
                try:
                    low, high = 1, len(rows) - 1
                    while low <= high:
                        maximum = (low + high) // 2
                        trial = thin_with_extremes(rows, maximum)
                        if fits(chosen + trial) is not None:
                            selected, reason = trial, 'post_hoc_historical_thinning'
                            low = maximum + 1
                        else:
                            high = maximum - 1
                except (ValueError, TypeError, OverflowError):
                    reason = 'native_semantics_not_reconstructable'
        chosen.extend(selected)
        diagnostics.append({'source': key, 'in_a': len(selected), 'in_b': len(rows) - len(selected),
                            'reason': reason, 'candidate_ids': [row['id'] for row in selected]})
    return chosen, {'mode': 'auto', 'sources': diagnostics, 'in_a': len(chosen),
                    'in_b': len(candidates) - len(chosen), 'fallback_fresh': not chosen}
