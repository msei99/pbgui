"""Controlled optimizer experiments using independent training evidence only."""
from __future__ import annotations

import copy
import math
import statistics

from logging_helpers import human_log as _log
from pb8_loop_store import apply_run_limit, configured_direction, digest, scenario_state
from pb8_loop_exploration import _limits, _scoring

SERVICE = 'PB8LoopResearch'
VERSION = 1

PROTOCOL = '''
Optimizer research v1 overrides broad novelty and incumbent-driven experiment planning.
There are TWO separate outputs: best qualified bot candidate, and recommended optimizer
settings for a later deep search. A short trial need not beat the bot incumbent to be useful.
optimizer_research contains controller-verified exact CPU training outcomes only:
native Vast CPU reports or independent local CPU reports, never proxy estimates.
Choose optimizer_assessment={criteria:[{metric,direction,reducer,reason}],
recommendations:[{operation,reason}],reason} at EVERY evaluate. Choose 1-3 measured
optimizer operations when qualified trials exist, otherwise an empty list and explain.
Criteria describe search quality for the explicit user goal, not arbitrary auxiliary GPU
objectives. Distinguish total gain from daily growth and justify priorities. The controller
recomputes ALL trials with the same criteria, including earlier losing settings.
Rank settings using per-trial best qualified candidate results, qualification
rate, validation coverage, actual work and progress. Missing evidence is unavailable.
Technical validation failures are counted separately from complete but unsafe/poor candidates.
metric_coverage and each group's measurement_coverage expose missing/nonfinite observations.
Compare coverage before ranking; partial coverage cannot establish robust superiority.
Full native training reports are recovered locally when criteria change; do not invent cached values.
Do not compare different work budgets as equal or treat a single lucky result as robust.
Recommendations reference actual optimizer operations, never validation/bot operations.
Return variant.experiment={purpose:"explore",source_operation:null,budget_fraction:1}
with changed settings (complete config or compact patch below) and an explained hypothesis.
Every new run uses EXACTLY the
user-configured per-run budget. Never shorten or lengthen it; 100000 means 100000
requested proxy evaluations (native batch overshoot is an execution detail).
Prefer a compact variant with base_optimizer and optimizer_patch instead of copying a
complete config. optimizer_patch accepts ONLY bounds, limits and scoring. The controller
restores all other source inputs and overrides, then runs the SAME complete-config,
hard-limit, fixed-worker, fixed-budget, novelty and boundary checks. This is a changed
Explore experiment, never a repetition. Use the supplied optimizer_patch_contract.
Its planning reference is the best measured qualified fixed-budget settings under the
existing research criteria, NOT automatically the newest attempt. A lower short-run
result is uncertain, but must not become the next default parent. Branch from the
reference with a coherent new hypothesis; do not accumulate unsuccessful expansions.
The recent-trial comparison and unchanged-scoring count are controller-computed.
When scoring_change_required is true, change effective scoring: choose a supported
single objective or meaningful Pareto combination. Reordering objectives is not a change.
Read the source controls you change, not unrelated fixed config fields. The full-config
format remains available for explicitly opened Strategy/Scenario/HSL changes; its complete
schema is available at /data/variant_config_schema. Never include config or overrides in a
compact patch variant. Do not select, inspect or evaluate the source's seed.
Replicate and confirm are FORBIDDEN, including unchanged settings relabelled as explore.
Every new trial must change effective optimizer settings compared with all previous
trials. RNG, worker settings, recorder settings and reference values inside variable
bounds do not establish a new experiment. Historical advice about repetition, seed
variance, screening fractions or longer confirmation is obsolete and overridden here.
Read original settings via /data/optimizer_sources/{operation} and bounded input_path,
config_path and overrides_path references, then propose a changed full config.
The controller alone assigns fresh randomness; omit optimize.seed or return null.
Never choose, compare, rank or tune seeds or reuse populations/checkpoints.
Explore coherent alternatives in scoring, bounds or permitted search filters at equal
user-defined work budgets. Bot-incumbent stagnation alone never stops research;
round/run/time/cost/validation limits still apply. Never expand these limits.
At evaluate, inspect /evaluation/bounds before planning a follow-up. Boundary diagnostics
compare qualified CPU nominations with their actual executed bounds. Near-bound values
are hypotheses, not proof that expansion helps; top-quartile ties and limited-trial evidence
are explicit. Respond to EVERY signal in each next variant.boundary_response with
{id,action:"adjust"|"retain",reason}. Adjust moves the pressured endpoint by at least
10% of the reference range OUTWARD: raise an upper edge or lower a lower edge. For a
positive quantized lower edge where that would cross zero, use the diagnosed
minimum_adjustment/suggested_endpoint toward the positive step floor instead.
At that floor retain is allowed; never invent negative spans/sizes to meet a gate. Inward
contraction does not address boundary pressure. Retain preserves the affected endpoint and explains the next distinct settings
experiment. Verify by repeating the same settings is forbidden, including at a larger
budget. Test an outward bound adjustment or another genuinely changed configuration.
Never demand repeated trials as a prerequisite for exploring an alternative.
Boundary diagnostics refer to the successful planning reference, allowing a return
from worse expanded ranges without falsely calling that return an inward pressure test.
When adjusting bounds, address ALL batch_required endpoints together, not one knob
per round. After two stagnant evaluations this batch is mandatory when such signals
exist. Previously measured outward tests are listed: retaining those endpoints can
avoid another blind expansion; their short results do not prove causality. After
three measured trials with unchanged scoring and at least two without improvement,
a different scoring is also required; combine it with the bounds batch when needed.
When native parameter-region evidence is available, inspect its linked profile before
planning another follow-up, especially after unsuccessful new scoring objectives.
It compares high-ranked distinct qualified nominations with the loaded cohort in
equal-width parameter bins, under the unchanged first CPU criterion. Use it to form
coherent evidence-based bounds hypotheses, not an endless catalogue of unrelated
objectives. Retaining a pressured endpoint can still move its opposite endpoint when
all boundary gates permit it. Do not automatically narrow around one incumbent or
cut out known strong configurations. These within-trial associations are confounded,
not causal proof; different scoring changes the sampled cohort. Preserve all required
outward tests, hard user ceilings, fixed work and controller-owned randomness.
After two stagnant evaluations, three unchanged-bounds trials and repeated qualified
incumbent pressure, address the untested affected unprotected edges outward. Zero lower
edges may be retained as deliberate nonnegative domains. Preserve hard user limits
and immutable worker settings. Protected edges may only be retained, never expanded.
Coin-override exclusions are reported as unavailable coverage, never as absence of pressure.
At the final evaluation recommend measured settings with uncertainty; the user performs
the later deep search outside this fixed-budget research loop. Short-run rankings do not prove deep-search
performance. Holdout and Full Time Range reports are never research evidence.
'''


def enabled(record):
    """Existing persisted loops keep their original decision contract."""
    return record.get('optimizer_research_version') == VERSION


def search_settings(config, overrides):
    """Identify effective search controls without RNG, budget or recorder differences."""
    optimize = copy.deepcopy(config.get('optimize') or {})
    for key in ('seed', 'iters', 'pareto_max_size', 'write_all_results', 'compress_results_file',
                'gpu', 'n_cpus', 'population_size'):
        optimize.pop(key, None)
    optimize['scoring'], optimize['limits'] = _scoring(config), _limits(config)
    bot = copy.deepcopy(config.get('bot') or {})

    def remove_variables(bounds, values):
        """Reference bot values under genuine search bounds are not starting populations."""
        for key, value in bounds.items():
            if isinstance(value, dict) and isinstance(values.get(key), dict):
                remove_variables(value, values[key])
            elif isinstance(value, list) and len(value) >= 2 and value[0] != value[1]:
                values.pop(key, None)

    remove_variables(optimize.get('bounds') or {}, bot)
    return {'optimize': optimize, 'fixed_bot': bot, 'overrides': overrides,
            'strategy': (config.get('live') or {}).get('strategy_kind')}


def signature(config, overrides):
    """Group settings by actual normalized input, including quantization and fixed values."""
    return digest(search_settings(config, overrides))


def experiment_signature(record, config):
    """Count active Bounds/Limits/Scoring and only explicitly opened user freedoms."""
    settings = record['settings']
    goals = settings.get('goals') or {}
    direction = configured_direction(config)
    sides = {'long', 'short'} if direction == 'both' else {direction}
    strategy = config.get('live', {}).get('strategy_kind', 'trailing_martingale')
    optimize = config.get('optimize') or {}
    fixed = {str(path).removeprefix('bot.') for path in optimize.get('fixed_params') or []}
    fixed.update(str(path).removeprefix('bot.') for path in optimize.get('fixed_runtime_overrides') or {})
    bounds = {}

    def visit(node, path=''):
        """Ignore quantization metadata and knobs outside the active search space."""
        for key, value in node.items():
            name = path + '.' + key if path else key
            if isinstance(value, dict):
                visit(value, name)
            elif isinstance(value, list) and len(value) >= 2:
                if not path and '_' in name:
                    side_name, leaf = name.split('_', 1)
                    def matches(node, prefix=''):
                        found = []
                        for field, child in node.items():
                            nested = prefix + '.' + field if prefix else field
                            if isinstance(child, dict):
                                found.extend(matches(child, nested))
                            elif field == leaf:
                                found.append(nested)
                        return found
                    paths = matches(config.get('bot', {}).get(side_name, {}))
                    name = side_name + '.' + (paths[0] if len(paths) == 1 else leaf)
                side = name.split('.')[0]
                if side not in sides:
                    continue
                if any(name == prefix or name.startswith(prefix + '.') for prefix in fixed):
                    continue
                if '.strategy.' in name and '.strategy.' + strategy + '.' not in name:
                    continue
                if '.hsl.' in name and not config.get('bot', {}).get(side, {}).get('hsl', {}).get('enabled'):
                    continue
                if '.forager.' in name and len(goals.get('coins') or []) == 1:
                    continue
                bounds[name] = value[:2]
    visit(config.get('optimize', {}).get('bounds') or {})
    state = {'bounds': bounds, 'scoring': _scoring(config), 'limits': _limits(config)}
    if settings.get('strategy_enabled'):
        state['strategy'] = strategy
    if settings.get('scenario_enabled'):
        state['scenarios'] = scenario_state(config)['backtest']
    if settings.get('hsl_mode') == 'auto':
        state['hsl'] = {side: bool(config.get('bot', {}).get(side, {}).get('hsl', {}).get('enabled')) for side in sides}
    def canonical(value):
        """Equivalent numeric spellings cannot disguise an unchanged experiment."""
        if isinstance(value, dict):
            return {key: canonical(child) for key, child in value.items()}
        if isinstance(value, list):
            return [canonical(child) for child in value]
        return float(value) if type(value) in (int, float) else value
    return digest(canonical(state))


def budget(record, fraction=1):
    """Each new trial uses exactly the user's configured per-run work allowance."""
    if type(fraction) not in (int, float) or not math.isfinite(fraction) or fraction != 1:
        raise ValueError('Experiment budget_fraction must be 1; the user-configured per-run budget is fixed')
    settings = record['settings']
    mode = settings.get('run_limit_mode', 'config')
    value = (record['initial_config'].get('optimize', {}).get('iters') if mode == 'config' else
             settings.get({'proxy': 'run_proxy', 'hours': 'run_hours', 'iters': 'run_iters'}.get(mode)))
    return {'mode': mode, 'value': value,
            'fraction': 1}


def source_job(record, operation):
    """Resolve only optimizer inputs owned by this Loop, never arbitrary file references."""
    source = next((job for job in record.get('jobs', []) if job.get('kind') == 'optimizer'
                   and job.get('operation') == operation and job.get('research')), None)
    if source is None:
        raise ValueError('Research repetition requires an owned optimizer source_operation')
    return source


def source_config(record, source):
    """Replay executed search inputs with planned transport paths and reviewed workers."""
    from pb8_loop_config_contract import copy_worker_settings, worker_reference
    planned = source['config']
    executed = source.get('executed_config') or planned
    config = copy.deepcopy(planned)
    for key in ('optimize', 'bot'):
        config[key] = copy.deepcopy(executed[key])
    if 'strategy_kind' in executed.get('live', {}):
        config.setdefault('live', {})['strategy_kind'] = executed['live']['strategy_kind']
    config = copy_worker_settings(worker_reference(record), config)
    config.setdefault('optimize', {})['seed'] = None
    return config


def resolve_variant(record, variant):
    """Historical inputs remain readable, but cannot authorize another unchanged trial."""
    if enabled(record) and isinstance(variant, dict):
        experiment = variant.get('experiment') or {}
        if isinstance(experiment, dict) and experiment.get('purpose') in {'replicate', 'confirm'}:
            raise ValueError('Replicate and confirm experiments are forbidden; provide a changed complete optimizer config at the fixed user budget')
        if 'optimizer_patch' in variant or 'base_optimizer' in variant:
            patch = variant.get('optimizer_patch')
            if ('config' in variant or 'overrides' in variant or not isinstance(patch, dict) or not patch
                    or set(patch) - {'bounds', 'limits', 'scoring'}):
                raise ValueError('Optimizer patch accepts only bounds, limits and scoring; omit config and overrides')
            base = variant.get('base_optimizer')
            if base == 'initial':
                config, overrides = copy.deepcopy(record['initial_config']), copy.deepcopy(record['initial_overrides'])
                config.setdefault('optimize', {})['seed'] = None
            else:
                if not isinstance(base, str):
                    raise ValueError('Optimizer patch requires an owned base_optimizer or initial')
                source = source_job(record, base)
                config, overrides = source_config(record, source), copy.deepcopy(source['overrides'])
            optimize = config.setdefault('optimize', {})
            for key, value in patch.items():
                if key == 'bounds':
                    if not isinstance(value, dict):
                        raise ValueError('Optimizer bounds patch must be an object')
                    optimize['bounds'] = _merge_bounds(optimize.get('bounds', {}), value)
                else:
                    if not isinstance(value, list):
                        raise ValueError('Optimizer limits/scoring patch must replace the complete list')
                    optimize[key] = copy.deepcopy(value)
            variant = {key: copy.deepcopy(value) for key, value in variant.items()
                       if key not in {'base_optimizer', 'optimizer_patch'}}
            variant.update(config=config, overrides=overrides)
        if isinstance(variant.get('config'), dict):
            config = variant['config']
            bounds = config.get('optimize', {}).get('bounds')
            if isinstance(bounds, dict):
                config = copy.deepcopy(config)
                config['optimize']['bounds'] = _merge_bounds({}, bounds)
                variant = {**variant, 'config': config}
    return variant


def _merge_bounds(original, patch):
    """Resolve dotted/nested sparse paths to native nested bounds without shadow keys."""
    result = copy.deepcopy(original) if isinstance(original, dict) else {}
    changes = {}

    def collect(node, prefix=()):
        """Reject duplicate/overlapping assignments instead of depending on key order."""
        for key, value in node.items():
            if not isinstance(key, str) or any(not part for part in key.split('.')):
                raise ValueError('Optimizer bounds patch contains an invalid parameter path')
            path = prefix + tuple(key.split('.'))
            if isinstance(value, dict) and value:
                collect(value, path)
                continue
            if any(path[:len(other)] == other or other[:len(path)] == path for other in changes):
                raise ValueError('Optimizer bounds patch contains conflicting parameter paths')
            changes[path] = value

    collect(patch)
    for path, value in changes.items():
        target = result
        for part in path[:-1]:
            if part not in target:
                target[part] = {}
            if not isinstance(target[part], dict):
                raise ValueError('Optimizer bounds patch traverses a parameter instead of an object')
            target = target[part]
        key = path[-1]
        if value is None:
            target.pop(key, None)
        elif isinstance(value, dict) and not value and isinstance(target.get(key), dict):
            continue
        else:
            target[key] = copy.deepcopy(value)
    return result


def patch_contract(record, references, research=None):
    """Offer one bounded source identity and exact control paths, never seed/worker data."""
    from pb8_loop_planning import planning_context
    planning = planning_context(record, research)
    base = planning.get('reference_operation', 'initial')
    path = '/data/initial_config' if base == 'initial' else references[base]['config_path']
    return {'version': 2, 'planning': planning, 'preferred_variant': {'base_optimizer': base,
            'optimizer_patch': {'bounds': 'sparse nested object or dotted parameter paths; null deletes a bound; '
                                         'dotted paths update native nested bounds, never add shadow keys; '
                                         'duplicate/overlapping paths are rejected',
                                'limits': 'complete replacement list when changed',
                                'scoring': 'complete replacement list when changed'}},
            'source_controls': {key: path + '/optimize/' + key for key in ('bounds', 'limits', 'scoring')},
            'rules': 'Omit unchanged controls. No config/overrides. Every effective change is fully validated; '
                     'fixed work, workers, random-seed ownership and user safety limits remain binding.',
            'full_config_schema_path': '/data/variant_config_schema'}


def source_inputs(record):
    """Bind shared immutable input archives, leaving only pageable references in each snapshot."""
    references, archives = {}, []
    for source in record.get('jobs', []):
        if source.get('kind') != 'optimizer' or not source.get('research'):
            continue
        config = source_config(record, source)
        payload = {'operation': source['operation'], 'config': config,
                   'overrides': copy.deepcopy(source['overrides']),
                   'budget': copy.deepcopy(source['research']['budget']),
                   'signature': signature(source.get('executed_config') or source['config'], source['overrides'])}
        identity = 'native_source:' + record['id'] + ':optimizer:' + digest(payload)
        path = '/history/' + identity.replace('~', '~0').replace('/', '~1')
        references[source['operation']] = {'input_path': path, 'config_path': path + '/config',
                                           'overrides_path': path + '/overrides'}
        archives.append({'id': identity, 'loop': record['id'], 'created_at': source.get('created_at', 0), **payload})
    return references, archives


def experiment_plan(record, config, overrides, experiment=None):
    """Validate experiment settings without drawing or evaluating execution seeds."""
    if (record['settings'].get('run_limit_mode', 'config') == 'config'
            and config.get('optimize', {}).get('iters') != record['initial_config'].get('optimize', {}).get('iters')):
        raise ValueError('The user-configured per-run iteration budget is fixed')
    config = apply_run_limit(record['settings'], config)
    metrics = [entry['metric'] for entry in _scoring(config)]
    if len(metrics) != len(set(metrics)):
        raise ValueError('PB8 removes repeated scoring metrics even with different reducers; use distinct metrics')
    experiment = experiment or {'purpose': 'explore', 'budget_fraction': 1}
    if isinstance(experiment, dict) and set(experiment) - {'purpose', 'source_operation', 'budget_fraction'}:
        raise ValueError('Experiment accepts only purpose, source_operation and budget_fraction; RNG is controller-owned')
    if (not isinstance(experiment, dict) or not isinstance(experiment.get('purpose'), str)
            or experiment['purpose'] not in {'explore', 'replicate', 'confirm'}):
        raise ValueError('Explain an explore experiment at the fixed user budget')
    purpose = experiment['purpose']
    if purpose != 'explore':
        raise ValueError('Replicate and confirm experiments are forbidden; change effective optimizer settings at the fixed user budget')
    work = budget(record, experiment.get('budget_fraction', 1))
    key = signature(config, overrides)
    experiment_key = experiment_signature(record, config)
    source = experiment.get('source_operation')
    if source is not None:
        raise ValueError('Explore has no source_operation; provide a changed complete optimizer config')
    for previous in record.get('jobs', []):
        if previous.get('kind') != 'optimizer':
            continue
        previous_config = apply_run_limit(record['settings'], previous.get('executed_config') or previous['config'])
        if experiment_key == experiment_signature(record, previous_config):
            raise ValueError('Unchanged optimizer settings are forbidden, including seed-only repetitions labelled explore; change bounds, scoring or permitted search controls')
    return {'purpose': purpose, 'source_operation': source, 'signature': key, 'budget': work}


def execution_check(proposed, executed, overrides):
    """Reject semantic normalization changes instead of crediting an unexecuted experiment."""
    before, after = search_settings(proposed, overrides), search_settings(executed, overrides)
    changed = [key for key in before if before[key] != after[key]]
    if changed:
        raise ValueError('Optimizer experiment changed during normalization (' + ', '.join(changed)
                         + '); revise the proposal to match supported effective settings')
    return {'status': 'verified', 'signature': signature(executed, overrides)}


def finite(value):
    """Booleans and absent/nonfinite metrics never become measurements."""
    return type(value) in (int, float) and math.isfinite(value)


def qualified(item):
    """Independent complete comparable hard-target-qualified training reports only."""
    assessment = item.get('assessment') or {}
    return (item.get('exact') is True and not item.get('observer_only')
            and assessment.get('comparable') is True
            and assessment.get('simulation_complete') is True
            and assessment.get('hard_targets_met') is True)


def trial_records(record, observations=(), criteria=None, report_loader=None, refresh_current=True):
    """Store report identities/catalogues and a small cache for the current criteria."""
    criteria = criteria if criteria is not None else (record.get('optimizer_assessment') or {}).get('criteria') or []
    wanted = {c['metric'] for c in criteria}
    trials = copy.deepcopy(record.get('optimizer_trials') or {})
    jobs = record.get('jobs', [])
    checks = {j['operation']: j for j in jobs if j['kind'] == 'validation' and not j.get('observer_only') and not j.get('diagnostic_only')}
    exact = {o['operation']: o for o in observations if not o.get('observer_only')}
    for operation, observation in (record.get('training_candidates') or {}).items():
        if operation in checks and not observation.get('observer_only'):
            exact.setdefault(operation, observation)

    def compact(outcome, catalogue):
        """Keep finite availability separately from the selected metric cache."""
        original = exact.get(outcome['operation'])
        source = original or outcome
        reports = source.get('per_report') or []
        known = set(source.get('available_metrics') or [])
        if original is not None or 'available_metric_indices' not in outcome:
            known.update(k for report in reports for k in report)
            available = {k for k in known if reports and all(finite(r.get(k)) for r in reports)}
        else:
            available = {catalogue[i] for i in outcome['available_metric_indices'] if 0 <= i < len(catalogue)}
        # Native reports remain the source of truth when criteria change. Never
        # re-run a backtest merely to recover an un-cached metric.
        if report_loader is not None and any(k in available and (not reports or any(not finite(r.get(k)) for r in reports)) for k in wanted):
            try:
                source = report_loader(outcome['operation'])
                reports = source.get('per_report') or []
            except (OSError, ValueError) as exc:
                _log(SERVICE, 'Native training report unavailable; retaining cached measurements',
                     level='WARNING', meta={'operation': outcome['operation'], 'error_type': type(exc).__name__})
                # Persist the lost source capability so later decisions cannot
                # repeatedly pay to request the same inaccessible measurements.
                outcome = dict(outcome, report_unavailable=type(exc).__name__)
                available = {k for r in reports for k in r if all(finite(v.get(k)) for v in reports)}
        new = {k: copy.deepcopy(v) for k, v in outcome.items()
               if k not in {'metrics', 'per_report', 'available_metrics', 'available_metric_indices'}}
        cache_keys = ({k for r in reports for k in r if all(finite(v.get(k)) for v in reports)}
                      if outcome.get('report_unavailable') else wanted)
        new['per_report'] = [{k: r.get(k) if finite(r.get(k)) else None for k in cache_keys} for r in reports]
        new['_known'], new['_available'] = known | set(catalogue), available
        return new

    # Current results replace only their own physical trials, preserving previous rounds.
    for job in jobs:
        if not refresh_current or job['kind'] != 'optimizer' or job.get('round') != record['round'] or not job.get('research'):
            continue
        outcomes = []
        for check in checks.values():
            if (check.get('candidate') or {}).get('optimizer_job') != job['operation']:
                continue
            observation = exact.get(check['operation'])
            if observation is None and check['status'] not in {'failed', 'cancelled', 'stopped', 'complete', 'completed'}:
                continue
            technical = observation is None or not observation.get('exact') or not observation.get('per_report')
            outcomes.append({'operation': check['operation'], 'qualified': qualified(observation or {}),
                'technical_failure': bool(technical), 'per_report': (observation or {}).get('per_report') or []})
        config = job.get('executed_config') or job['config']
        from pb8_loop_gpu_replacement import replaced_predecessor
        trials[job['operation']] = {
            'operation': job['operation'], 'round': job['round'], **copy.deepcopy(job['research']),
            'signature': signature(config, job['overrides']),
            'execution_verified': job.get('research_verification', {}).get('status') == 'verified',
            'status': job['status'], 'error': job.get('error'), 'limit_reached': job.get('limit_reached'),
            'infrastructure_interruption': bool(replaced_predecessor(record, job)),
            'completed_proxies': job.get('proxy_evaluations'), 'offered_candidates': job.get('result_count'),
            **({'native_cpu_population': copy.deepcopy(record['native_training_summary']['populations'][job['operation']])}
               if record.get('native_training_round') == job.get('round')
               and job['operation'] in (record.get('native_training_summary') or {}).get('populations', {}) else {}),
            'progress': copy.deepcopy(job.get('research_progress') or []), 'outcomes': outcomes}
    for trial in trials.values():
        old_catalogue = trial.get('metric_catalogue') or []
        compacted = [compact(o, old_catalogue) for o in trial['outcomes']]
        catalogue = sorted({k for o in compacted for k in o['_known']})
        indices = {k: i for i, k in enumerate(catalogue)}
        for outcome in compacted:
            outcome.pop('_known')
            outcome['available_metric_indices'] = sorted(indices[k] for k in outcome.pop('_available'))
            outcome.setdefault('technical_failure', not outcome.get('per_report'))
        trial['metric_catalogue'], trial['outcomes'] = catalogue, compacted
    return trials


def metric_available(trial, outcome, metric):
    """A nonfinite outlier removes only its own measurement, not a run-wide criterion."""
    catalogue = trial.get('metric_catalogue') or []
    if 'available_metric_indices' not in outcome:
        return bool(outcome.get('per_report')) and all(finite(r.get(metric)) for r in outcome['per_report'])
    return metric in catalogue and catalogue.index(metric) in outcome['available_metric_indices']


def metric_value(outcome, criterion):
    """Always derive a requested reducer from independent per-report measurements."""
    values = [r.get(criterion['metric']) for r in outcome.get('per_report') or []]
    if not values or not all(finite(v) for v in values):
        return None
    return {'mean': statistics.fmean, 'min': min, 'max': max, 'median': statistics.median}[criterion['reducer']](values)


def trial_completed(trial):
    """Only this physical trial's verified completed budget can support recommendations."""
    return (trial['execution_verified'] and not trial.get('error') and not trial.get('infrastructure_interruption')
                 and trial['status'] in {'complete', 'completed', 'cancelled', 'stopped'}
                 and (trial['status'] in {'complete', 'completed'} or trial.get('limit_reached') == trial['budget']['mode'])
                 and (trial['budget']['mode'] != 'proxy' or finite(trial.get('completed_proxies'))
                      and trial['completed_proxies'] >= trial['budget']['value']))


def summarize(trials, criteria):
    """Recompute every setting/seed under one common explained evaluation method."""
    groups = {}
    for trial in trials.values():
        # Interrupted/replaced/unverified trials remain visible but cannot support a ranking.
        valid = trial_completed(trial)
        group_key = digest([trial['signature'], trial['budget']])
        group = groups.setdefault(group_key, {'id': group_key, 'signature': trial['signature'],
            'budget': trial['budget'], 'operations': [], 'seeds': [], 'trial_count': 0,
            'qualified_count': 0, 'validated_count': 0, 'assessed_trial_count': 0, 'technical_failure_count': 0, 'measured_candidate_count': 0, 'retained_qualified_count': 0, 'per_seed_best': [], 'actual_proxies': [], 'criteria': criteria})
        group['operations'].append(trial['operation'])
        if not valid:
            continue
        qualified_outcomes = [o for o in trial['outcomes'] if o['qualified']]
        group['trial_count'] += 1
        if finite(trial.get('completed_proxies')):
            group['actual_proxies'].append(trial['completed_proxies'])
        group['seeds'].append(trial.get('seed_index', trial['operation']))
        group['technical_failure_count'] += sum(bool(o.get('technical_failure')) for o in trial['outcomes'])
        population = trial.get('native_cpu_population')
        group['validated_count'] += population['verified'] if population else sum(not o.get('technical_failure') for o in trial['outcomes'])
        group['qualified_count'] += population['qualified'] if population else len(qualified_outcomes)
        group['assessed_trial_count'] += bool(population['verified']) if population else any(not o.get('technical_failure') for o in trial['outcomes'])
        measured = [(o, [metric_value(o, c) for c in criteria]) for o in qualified_outcomes]
        measured = [(o, values) for o, values in measured if all(v is not None for v in values)]
        group['measured_candidate_count'] += len(measured)
        group['retained_qualified_count'] += len(qualified_outcomes)
        if measured:
            # Lexicographic user-goal priorities, never a fabricated weighted score.
            outcome, values = min(measured, key=lambda pair: tuple(
                v if c['direction'] == 'min' else -v for c, v in zip(criteria, pair[1])))
            group['per_seed_best'].append({'operation': trial['operation'], 'candidate': outcome['operation'],
                                          'seed_index': trial.get('seed_index', trial['operation']), 'values': values})
    for group in groups.values():
        values = group['per_seed_best']
        group['distribution'] = [{'mean': statistics.fmean(column), 'min': min(column), 'max': max(column),
                                  'stdev': statistics.stdev(column) if len(column) > 1 else None}
                                 for index in range(len(criteria))
                                 if (column := [row['values'][index] for row in values])]
        group['successful_seed_rate'] = len(values) / group['assessed_trial_count'] if group['assessed_trial_count'] else None
        group['measured_seed_count'] = len({row['seed_index'] for row in values})
        group['measurement_coverage'] = (group['measured_candidate_count'] / group['retained_qualified_count']
                                         if group['retained_qualified_count'] else None)
        group['measurement_coverage_basis'] = 'retained qualified outcomes'
        group['selection_coverage'] = (group['retained_qualified_count'] / group['qualified_count']
                                       if group['qualified_count'] else None)
        group['qualification_rate'] = (group['qualified_count'] / group['validated_count']
                                       if group['validated_count'] else None)
        group['uncertainty'] = ('No qualified measured trial' if not values else
                                'One completed trial with a measured qualified result; limited settings evidence'
                                if group['measured_seed_count'] < 2 else
                                'Repeated short trials; deep-search performance remains unverified')
        if group['measurement_coverage'] is not None and group['measurement_coverage'] < 1:
            group['uncertainty'] += '; partial metric coverage, compare coverage before ranking'
    return list(groups.values())


def evidence(record, observations=()):
    """Send groups and compact trial summaries, never raw report metrics or duplicate groups."""
    trials = trial_records(record, observations, refresh_current=bool(observations) or record.get('phase') == 'evaluate')
    previous = record.get('optimizer_assessment') or {}
    criteria = previous.get('criteria') or []
    coverage = {}
    total_qualified = sum(o['qualified'] for t in trials.values() for o in t['outcomes'])
    for trial in trials.values():
        for metric in trial.get('metric_catalogue') or []:
            row = coverage.setdefault(metric, {'available': 0, 'qualified': total_qualified})
            for outcome in trial['outcomes']:
                if outcome['qualified']:
                    row['available'] += metric_available(trial, outcome, metric)
    summaries = {key: {k: copy.deepcopy(v) for k, v in trial.items()
                      if k not in {'outcomes', 'metric_catalogue', 'progress', 'seed', 'seed_index'}} | {
        'validated_count': trial['native_cpu_population']['verified'] if trial.get('native_cpu_population') else sum(not o.get('technical_failure') for o in trial['outcomes']),
        'technical_failure_count': sum(bool(o.get('technical_failure')) for o in trial['outcomes']),
        'qualified_count': trial['native_cpu_population']['qualified'] if trial.get('native_cpu_population') else sum(o['qualified'] for o in trial['outcomes']),
        'unavailable_report_count': sum(bool(o.get('report_unavailable')) for o in trial['outcomes']),
        'progress': trial.get('progress', [])}
        for key, trial in trials.items()}
    groups = []
    for original in summarize(trials, criteria):
        group = {k: copy.deepcopy(v) for k, v in original.items()
                 if k not in {'seeds', 'per_seed_best', 'measured_seed_count', 'successful_seed_rate'}}
        group['per_trial_best'] = [{k: v for k, v in row.items() if k != 'seed_index'}
                                   for row in original['per_seed_best']]
        group['measured_trial_count'] = original['measured_seed_count']
        group['successful_trial_rate'] = original['successful_seed_rate']
        groups.append(group)
    return {'version': VERSION, 'trials': summaries, 'groups': groups,
            'available_metrics': sorted(k for k, v in coverage.items() if v['available']),
            'metric_coverage': coverage,
            'previous_assessment': {k: copy.deepcopy(v) for k, v in previous.items() if k != 'groups'},
            'seed_policy': 'Fresh random controller-owned execution detail; never an AI evaluation axis', 'fixed_budget': budget(record),
            'experiment_policy': 'Changed effective settings only: Bounds/Limits/Scoring plus explicitly opened user freedoms',
            'confirmation': 'Replicate, confirm, shorter screening and longer runs are forbidden; use the exact user budget',
            'selection_bias': 'CPU outcomes cover selected nominations, not the entire proxy population'}


def validate_assessment(record, observations, assessment, report_loader=None, allow_uncached=False, include_trials=False):
    """Recommendations must cite verified measured settings and a common finite comparison."""
    if not isinstance(assessment, dict) or not isinstance(assessment.get('reason'), str) or not assessment['reason'].strip():
        raise ValueError('Explain optimizer_assessment separately from the bot winner')
    criteria = assessment.get('criteria')
    if not isinstance(criteria, list) or not 1 <= len(criteria) <= 6:
        raise ValueError('Optimizer research requires 1-6 ordered measurable criteria')
    for criterion in criteria:
        if (not isinstance(criterion, dict) or not isinstance(criterion.get('metric'), str)
                or not 0 < len(criterion['metric']) <= 100 or not isinstance(criterion.get('direction'), str)
                or criterion['direction'] not in {'min', 'max'} or not isinstance(criterion.get('reducer'), str)
                or criterion['reducer'] not in {'mean', 'min', 'max', 'median'}
                or not isinstance(criterion.get('reason'), str) or not criterion['reason'].strip()):
            raise ValueError('Research criteria require metric, direction, reducer and reason')
    trials = trial_records(record, observations, criteria, report_loader,
                          refresh_current=bool(observations) or record.get('phase') == 'evaluate')
    unavailable_sources = {metric for t in trials.values() for o in t['outcomes']
                           if o.get('report_unavailable') for metric in t.get('metric_catalogue', [])}
    if (any(c['metric'] not in unavailable_sources and not any(o['qualified'] and metric_available(trial, o, c['metric'])
                    for trial in trials.values() for o in trial['outcomes']) for c in criteria)
            and any(o['qualified'] for t in trials.values() for o in t['outcomes'])):
        raise ValueError('Research criterion is unavailable in all qualified training candidates')
    groups = summarize(trials, criteria)
    available = {best['operation'] for g in groups for best in g['per_seed_best']}
    if allow_uncached:
        available.update(t['operation'] for t in trials.values() if trial_completed(t)
            and any(o['qualified'] and all(metric_available(t, o, c['metric']) for c in criteria) for o in t['outcomes']))
    recommendations = assessment.get('recommendations')
    if (not isinstance(recommendations, list) or len(recommendations) > 3
            or available and not recommendations):
        raise ValueError('Recommend 1-3 measured optimizer settings, or explain no qualified trials')
    lost_recommendations = []
    if unavailable_sources and isinstance(recommendations, list):
        lost_recommendations = [r for r in recommendations if isinstance(r, dict)
                                and isinstance(r.get('operation'), str) and r['operation'] in trials
                                and r['operation'] not in available
                                and trial_completed(trials[r['operation']])
                                and any(o['qualified'] and o.get('report_unavailable')
                                        and any(not metric_available(trials[r['operation']], o, c['metric'])
                                                for c in criteria) for o in trials[r['operation']]['outcomes'])]
        recommendations = [r for r in recommendations if r not in lost_recommendations]
    seen = set()
    group_ids = {operation: group['id'] for group in groups for operation in group['operations']}
    for recommendation in recommendations:
        if (not isinstance(recommendation, dict) or not isinstance(recommendation.get('operation'), str)
                or recommendation['operation'] not in available
                or not isinstance(recommendation.get('reason'), str) or not recommendation['reason'].strip()):
            raise ValueError('Recommend only verified optimizer operations with independent training measurements')
        key = group_ids[recommendation['operation']]
        if key in seen:
            raise ValueError('Recommend distinct optimizer setting/budget groups')
        seen.add(key)
    return {'criteria': copy.deepcopy(criteria), 'recommendations': copy.deepcopy(recommendations),
            'reason': assessment['reason'] + (' Native reports unavailable; unmeasurable recommendations withheld.'
                                             if lost_recommendations else ''), 'groups': groups,
            **({'_trials': trials} if include_trials else {})}


def progress_sample(job, progress):
    """Retain at most 32 native progress checkpoints, explicitly separate from CPU evidence."""
    count = progress.get('proxy_evaluations')
    if not finite(count):
        return
    samples = job.setdefault('research_progress', [])
    if samples and count <= samples[-1]['proxies']:
        return
    sample = {'proxies': count, 'generation': progress.get('generation'),
              'exact_evaluations': progress.get('exact_evaluations'),
              'native_objectives': {k: v for k, v in (progress.get('objectives') or {}).items() if finite(v)}}
    samples.append(sample)
    if len(samples) > 32:
        # Keep the initial checkpoint and thin the oldest interior samples.
        samples.pop(1)


def recommendation_configs(record):
    """Export exact recorded search inputs, never the best bot as an optimizer substitute."""
    from fastapi import HTTPException
    assessment = record.get('optimizer_assessment') or {}
    recommendations = assessment.get('recommendations') or []
    if not recommendations:
        raise HTTPException(409, 'No measured optimizer recommendation is available yet')
    configs = []
    for recommendation in recommendations:
        job = next((j for j in record['jobs'] if j['kind'] == 'optimizer'
                    and j['operation'] == recommendation['operation']), None)
        if job is None or job.get('research_verification', {}).get('status') != 'verified':
            raise HTTPException(409, 'Recommended optimizer evidence is no longer available')
        config = copy.deepcopy(job.get('executed_config') or job['config'])
        config.setdefault('optimize', {})['seed'] = None
        configs.append({'operation': job['operation'], 'config': config,
                        'override_configs': copy.deepcopy(job['overrides']),
                        'experiment': {k: copy.deepcopy(v) for k, v in job['research'].items()
                                       if k not in {'seed', 'seed_index'}}, 'reason': recommendation['reason']})
    return {'version': VERSION, 'loop_id': record['id'], 'recommendations': configs,
            'assessment': copy.deepcopy(assessment), 'status': record['status'],
            'provisional': record['status'] not in {'completed', 'unconfirmed'},
            'deep_search_verified': False}
