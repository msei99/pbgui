"""Bounded research evaluation views over exact immutable CPU training evidence."""
from __future__ import annotations

import copy
from collections.abc import Sequence
import heapq
import statistics

from pb8_loop_selection import SelectionIndex, numeric, overview

SERVICE = 'PB8LoopEvaluationEvidence'
PROJECTION_VERSION = 9


class EvaluationOptions(Sequence):
    """Expose option identities without duplicating reports or configurations."""

    def __init__(self, options):
        self.options, self.keys = options, tuple(options)

    def __len__(self):
        return len(self.keys)

    def __getitem__(self, index):
        key = self.keys[index]
        return {**self.options[key], 'id': key}


class ResearchGroups(Sequence):
    """Expose measured group means without copying seed histories into rank records."""

    def __init__(self, groups):
        self.groups = groups

    def __len__(self):
        return len(self.groups)

    def __getitem__(self, index):
        group = self.groups[index]
        return {'id': group.get('id'), 'metrics': {
            str(i): value.get('mean') for i, value in enumerate(group.get('distribution') or [])}}


def report_value(candidate, metric, reducer):
    """Use the same exact finite per-report reducers as the final comparison."""
    values = [report.get(metric) for report in candidate.get('per_report') or []]
    if not values or any(not numeric(value) for value in values):
        return None
    return {'mean': statistics.fmean, 'min': min, 'max': max, 'median': statistics.median}[reducer](values)


class EvaluationIndex(SelectionIndex):
    """Own reusable CPU-report ranking indexes under the evidence session lifecycle."""

    def __init__(self, evidence):
        self.research_groups = (evidence.get('optimizer_research') or {}).get('groups') or []
        self.research_index = None
        super().__init__(EvaluationOptions(evidence.get('winner_options') or {}), [])

    @staticmethod
    def value(candidate, metric, reducer):
        """Rank actual independent reports, never optimizer proxy statistics."""
        return report_value(candidate, metric, reducer)

    def rank_research(self, criterion, direction, offset):
        """Query a whole research programme without a model-side scan of every group."""
        with self.lock:
            if self.research_index is None:
                self.research_index = SelectionIndex(ResearchGroups(self.research_groups), [])
            return self.research_index.ranked(criterion, 'value', direction, False, offset)

    def close(self):
        """Release both ranking owners even when one cleanup reports an error."""
        try:
            if self.research_index is not None:
                try:
                    self.research_index.close()
                finally:
                    self.research_index = None
        finally:
            super().close()


def evaluation_overview(evidence, metrics):
    """Summarize every qualified option without growing context with prior rounds."""
    options = EvaluationOptions(evidence.get('winner_options') or {})
    research = evidence.get('optimizer_research') or {}
    summary = overview(options, metrics)
    return {'option_count': len(options), 'aggregate_metrics': summary['metrics'],
            'incumbent_operation': (evidence.get('best') or {}).get('operation'),
            'baseline_operation': (evidence.get('baseline') or {}).get('operation'),
            'current_observation_count': len(evidence.get('observations') or []),
            'research_trial_count': len(research.get('trials') or {}),
            'research_group_count': len(research.get('groups') or []),
            'options_path': '/evaluation/options', 'metrics_path': '/evaluation/metrics',
            'rank_path': '/evaluation/rank/{metric}/{mean|min|max|median}/{min|max}',
            'option_path': '/evaluation/id/{operation}', 'research_path': '/evaluation/research',
            'research_rank_path': '/evaluation/research_rank/{criterion_index}/{min|max}',
            'research_metrics_path': '/evaluation/research_metrics',
            'bounds': {key: (evidence.get('boundary_diagnostics') or {}).get(key)
                       for key in ('version', 'signal_count', 'candidate_count', 'reference_operation', 'unchanged_bounds_trials',
                                   'excluded_override_count', 'override_coverage', 'parameter_regions_summary')},
            'bounds_path': '/evaluation/bounds',
            'assessment_contract': {'candidate_assessment_policy': {
                'scope': 'all_training_candidates', 'reason': 'Explain the common criteria; controller recomputes every option'}},
            'coverage': 'all qualified independent CPU training options; not Holdout or proxy evidence'}


def decision_brief(content):
    """Preview exact leaders with fixed cardinality, without copying reports or choosing winners."""
    evidence = content['evidence']
    options = evidence.get('winner_options') or {}
    research = evidence.get('optimizer_research') or {}
    criteria = (evidence.get('evaluation_policy') or {}).get('previous') or []
    criteria = [{key: item[key] for key in ('metric', 'direction', 'reducer')}
                for item in criteria[:6]]
    groups = research.get('groups') or []

    def rank(values, rules):
        """Missing observations sort last and never turn into favorable zeroes."""
        return tuple((0, -value if rule['direction'] == 'max' else value)
                     if numeric(value) else (1, 0) for value, rule in zip(values, rules))

    def option_values(item):
        """Use exact reducers over every report, including arbitrarily many windows."""
        return [report_value(item, rule['metric'], rule['reducer']) for rule in criteria]

    def option_row(operation, values=None):
        """Show aggregate facts only; full reports remain under their existing exact query."""
        item = options[operation]
        return {'operation': operation, 'values': option_values(item) if values is None else values,
                'report_count': len(item.get('per_report') or []),
                'hard_limit_observations': {name: report_value(item, name, reducer) for name, reducer in (
                    ('drawdown_worst_strategy_eq', 'max'), ('drawdown_worst', 'max'),
                    ('backtest_completion_ratio', 'min'), ('liquidated', 'max'))},
                'details_path': '/evaluation/id/' + operation.replace('~', '~0').replace('/', '~1')}

    leaders = heapq.nsmallest(3, ((rank(values, criteria), operation, values)
        for operation, item in options.items() if criteria
        for values in [option_values(item)] if all(numeric(value) for value in values)))
    references = {key: option_row(operation) for key in ('best', 'baseline')
                  if (operation := (evidence.get(key) or {}).get('operation')) in options}
    group_criteria = (groups[0].get('criteria') or [])[:6] if groups else []
    group_rules = [{key: item[key] for key in ('metric', 'direction', 'reducer')} for item in group_criteria]
    ranked_groups = heapq.nsmallest(3, ((rank(values, group_rules), position, values)
        for position, group in enumerate(groups) if group_rules
        if [{key: item[key] for key in ('metric', 'direction', 'reducer')}
            for item in (group.get('criteria') or [])[:6]] == group_rules
        for values in [[item.get('mean') for item in (group.get('distribution') or [])[:len(group_rules)]]]
        if len(values) == len(group_rules) and all(numeric(value) for value in values)))
    summaries = []
    for _, position, values in ranked_groups:
        group = groups[position]
        operation = next((item['operation'] for item in group.get('per_trial_best') or []), None)
        source = (content.get('optimizer_sources') or {}).get(operation) or {}
        summaries.append({key: copy.deepcopy(group.get(key)) for key in (
            'id', 'budget', 'trial_count', 'qualified_count', 'validated_count', 'technical_failure_count',
            'measurement_coverage', 'selection_coverage', 'qualification_rate', 'uncertainty')} | {
            'values': values, 'representative_operation': operation,
            'source_controls': {key: source['config_path'] + '/optimize/' + key
                                for key in ('bounds', 'limits', 'scoring')} if source else None,
            'details_path': '/data/evidence/optimizer_research/groups/' + str(position)})
    return {'ranking_basis': 'lexicographic preview under previously explained criteria; not a new decision',
            'criteria': criteria, 'option_count': len(options),
            'leaders': [option_row(operation, values) for _, operation, values in leaders],
            'references': references, 'research_criteria': group_rules, 'research_group_count': len(groups),
            'research_leaders': summaries,
            'coverage': 'At most three fully measured previews, not exhaustive inspection. All options/groups remain queryable; '
                        'the controller still recomputes every candidate and trial under the final criteria.'}


def option_summary(candidate, relevant):
    """Keep exact CPU values and missingness; large records retain their exact read path."""
    key = candidate['id']
    path = '/data/evidence/winner_options/' + key.replace('~', '~0').replace('/', '~1')
    assessment = candidate.get('assessment') or {}
    return {'operation': key, 'candidate_id': candidate.get('candidate_id'), 'details_path': path,
            'metrics': {k: v for k, v in (candidate.get('metrics') or {}).items() if k in relevant},
            'per_report': [{k: v for k, v in report.items() if k in relevant}
                           for report in candidate.get('per_report') or []],
            'assessment': {k: assessment.get(k) for k in ('hard_targets_met', 'comparable', 'simulation_complete')}}


def query(snapshot, path, offset, page_tokens, index=None):
    """Return bounded exact queries with stable cursors over the complete training dataset."""
    from pb8_loop_evidence import SAFE_METRICS, tokens
    if type(offset) is not int or offset < 0 or type(page_tokens) is not int or not 512 <= page_tokens <= 6144:
        raise ValueError('Invalid evaluation query limits')
    content = snapshot['data']
    if content.get('stage') != 'evaluate' or 'optimizer_research' not in content.get('evidence', {}):
        raise ValueError('Evaluation queries require research training evidence')
    evidence = content['evidence']
    owned = index is None
    if owned:
        index = EvaluationIndex(evidence)
    try:
        if index.candidates.options is not evidence.get('winner_options') and evidence.get('winner_options'):
            raise ValueError('Evaluation index belongs to another dataset')
        options = index.candidates
        relevant = SAFE_METRICS | {metric for rule in content.get('rubric') or [] for metric in rule['metrics']}
        parts = [part.replace('~1', '/').replace('~0', '~') for part in path.split('/')]
        result = {'snapshot': snapshot['id'], 'sha256': snapshot['sha256'], 'path': path,
                  'offset': offset, 'dataset_count': len(options), 'entries': [], 'next_offset': None,
                  'coverage': 'queried_subset_of_exact_CPU_training_evidence'}
        if parts == ['', 'evaluation', 'options']:
            count = len(options)
            positions = range(offset, count)
            entry = lambda i: option_summary(options[i], relevant)
        elif parts[:3] == ['', 'evaluation', 'id'] and len(parts) == 4:
            key = parts[3]
            count = int(key in options.options)
            positions = [key] if count and offset == 0 else []
            entry = lambda key: option_summary({**options.options[key], 'id': key}, relevant)
        elif parts[:3] == ['', 'evaluation', 'rank'] and len(parts) == 6:
            metric, reducer, direction = parts[3:]
            if reducer not in {'mean', 'min', 'max', 'median'} or direction not in {'min', 'max'}:
                raise ValueError('Unsupported CPU ranking')
            if offset > len(options):
                raise ValueError('Evaluation offset exceeds dataset')
            positions, count = index.ranked(metric, reducer, direction, False, offset)
            def entry(i):
                """Show the requested reducer together with its exact report values."""
                candidate = options[i]
                return option_summary(candidate, relevant | {metric}) | {
                    'comparison': {'metric': metric, 'reducer': reducer, 'value': report_value(candidate, metric, reducer)}}
        elif parts == ['', 'evaluation', 'metrics']:
            common = None
            for candidate in options:
                for report in candidate.get('per_report') or []:
                    available = {key for key, value in report.items() if numeric(value)}
                    common = available if common is None else common & available
            names = sorted(common or [])
            count, positions = len(names), range(offset, len(names))
            entry = lambda i: names[i]
        elif parts == ['', 'evaluation', 'bounds']:
            rows = (evidence.get('boundary_diagnostics') or {}).get('parameters') or []
            count, positions = len(rows), range(offset, len(rows))
            entry = lambda i: {**rows[i], 'details_path': '/data/evidence/boundary_diagnostics/parameters/' + str(i)}
        elif parts == ['', 'evaluation', 'research_metrics']:
            coverage = (evidence.get('optimizer_research') or {}).get('metric_coverage') or {}
            names = sorted(coverage)
            count, positions = len(names), range(offset, len(names))
            entry = lambda i: {'metric': names[i], **coverage[names[i]]}
        elif parts == ['', 'evaluation', 'research'] or parts[:3] == ['', 'evaluation', 'research_rank']:
            groups = (evidence.get('optimizer_research') or {}).get('groups') or []
            if parts == ['', 'evaluation', 'research']:
                count, positions = len(groups), range(offset, len(groups))
            else:
                if (len(parts) != 5 or parts[3] not in {str(i) for i in range(6)}
                        or parts[4] not in {'min', 'max'} or offset > len(groups)):
                    raise ValueError('Unsupported research ranking')
                positions, count = index.rank_research(parts[3], parts[4], offset)
            def entry(i):
                """Large seed histories stay optional, without dropping any settings group."""
                group = groups[i]
                representative = next((v['operation'] for v in (group.get('per_trial_best') or group.get('per_seed_best') or [])), None)
                return {k: copy.deepcopy(group.get(k)) for k in (
                    'id', 'signature', 'budget', 'trial_count', 'qualified_count', 'validated_count',
                    'technical_failure_count', 'measurement_coverage', 'measurement_coverage_basis',
                    'retained_qualified_count', 'selection_coverage', 'distribution', 'criteria', 'uncertainty')} | {
                    'representative_operation': representative,
                    'source_input_path': '/data/optimizer_sources/' + representative.replace('~', '~0').replace('/', '~1') if representative else None,
                    'details_path': '/data/evidence/optimizer_research/groups/' + str(i)}
        else:
            raise ValueError('Unsupported evaluation query')
        result['matched_count'] = count
        if offset > count:
            raise ValueError('Evaluation offset exceeds dataset')
        for position in positions:
            value = entry(position)
            trial = {**result, 'entries': result['entries'] + [value], 'next_offset': offset + len(result['entries']) + 1}
            if tokens(trial) > page_tokens:
                if result['entries']:
                    break
                if not isinstance(value, dict) or not value.get('details_path'):
                    raise ValueError('Evaluation schema entry exceeds page budget')
                value = {key: value[key] for key in ('operation', 'id', 'details_path') if key in value}
                value['requires_detail_read'] = True
            result['entries'].append(value)
            if len(result['entries']) >= 32:
                break
        end = offset + len(result['entries'])
        result['next_offset'] = end if end < count else None
        if tokens(result) > page_tokens:
            raise ValueError('Evaluation query envelope exceeds page budget')
        return result
    finally:
        if owned:
            index.close()
