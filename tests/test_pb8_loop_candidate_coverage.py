"""Offline regressions for native-stream nominations and labeled CPU evidence."""
import json

import msgpack
import pytest

from pb8_loop_backend import LoopBackend
from pb8_loop_candidates import result_records, supplemental_candidates
from pb8_loop_evidence import required_content
from pb8_loop_reports import report_contexts
from tests.test_pb8_loop_optimizer import config, record


def native(gain, adg, exposure, dd=.4):
    """Build a complete native observation whose CPU metric differs from GPU ranking."""
    value = config()
    value['bot']['long']['risk']['total_wallet_exposure_limit'] = exposure
    value['suite_metrics'] = {'metrics': {
        'gain': {'stats': {'mean': gain, 'min': gain, 'max': gain}},
        'adg': {'stats': {'mean': adg}},
        'dd': {'stats': {'mean': dd / 2, 'max': dd}},
        'backtest_completion_ratio': {'stats': {'min': 1, 'mean': 1}}}}
    value['metrics'] = {'liquidated': False}
    return value


def summarize(data, reducer):
    """Read fixture suite means through the same callback boundary as production."""
    return {key: row['stats'].get(reducer) for key, row in data['suite_metrics']['metrics'].items()}


def write_stream(path, rows):
    """Write native MessagePack objects only inside the test directory."""
    path.write_bytes(b''.join(msgpack.packb(row, use_bin_type=True) for row in rows))


def test_overlay_stream_retains_changes_and_deletions_past_record_100(tmp_path):
    """A former periodic full-record assumption cannot lose later configs or deletions."""
    first = native(1, .1, 1)
    first['removed'] = {'old': 2}
    diffs = [{'bot': {'long': {'risk': {'total_wallet_exposure_limit': index}}}}
             for index in range(2, 103)]
    diffs[-1]['removed'] = {'__passivbot_diff_delete__': True}
    path = tmp_path / 'all_results.bin'
    write_stream(path, [first, *diffs])
    rows = list(result_records(path))
    assert len(rows) == 102
    assert rows[-1][1]['bot']['long']['risk']['total_wallet_exposure_limit'] == 102
    assert rows[-1][1]['suite_metrics'] == first['suite_metrics']
    assert 'removed' not in rows[-1][1]
    assert rows[0][1]['removed'] == {'old': 2}


def test_uncompressed_records_replace_previous_objects(tmp_path):
    """Uncompressed complete records do not inherit removed top-level values."""
    first = native(1, .1, 1)
    first['optimize']['compress_results_file'] = False
    first['previous_only'] = 7
    second = native(2, .2, 2)
    second['optimize']['compress_results_file'] = False
    path = tmp_path / 'all_results.bin'
    write_stream(path, [first, second])
    assert 'previous_only' not in list(result_records(path))[-1][1]


@pytest.mark.parametrize('tail', [b'\x81', b'\xc1'])
def test_truncated_or_corrupt_stream_is_not_silently_complete(tmp_path, tail):
    """Never claim full coverage when a native record is incomplete or malformed."""
    path = tmp_path / 'all_results.bin'
    write_stream(path, [native(1, .1, 1)])
    path.write_bytes(path.read_bytes() + tail)
    with pytest.raises(ValueError, match='stream'):
        list(result_records(path))


def test_nonpareto_gain_leader_reaches_backend_selection(record, tmp_path, monkeypatch):
    """A CPU-gain leader dominated on ADG remains available for independent validation."""
    from api import optimize_v8 as opt
    store, row = record
    row['rubric'] = [{'goal': 'gain', 'metrics': ['gain'], 'target': None, 'direction': 'max'}]
    front, leader = native(6.5, .02, 2), native(6.64, .019, 3)
    write_stream(tmp_path / 'all_results.bin', [front, leader])
    monkeypatch.setattr(opt, '_resolve_result_path', lambda path: tmp_path)
    monkeypatch.setattr(opt, '_pareto_files_for_listing', lambda directory: (directory, [tmp_path / 'front.json']))
    monkeypatch.setattr(opt, '_read_json', lambda path: front)
    monkeypatch.setattr(opt, '_pareto_summary', summarize)
    candidates = LoopBackend(tmp_path, store)._result_candidates(row,
        {'path': 'isolated', 'result': 'isolated', 'modified': 1},
        {'operation': 'optimizer', 'config': config(), 'overrides': {},
         'optimizer_archive': {'candidate_capacity_per_job': 4}})
    assert sorted(item['metrics']['gain'] for item in candidates) == [6.5, 6.64]
    assert len({item['id'] for item in candidates}) == 2
    coverage = candidates[0]['source']['training_candidate_coverage']
    assert coverage['records_scanned'] == 2
    assert coverage['offered_distinct_candidates'] == 2
    assert any(item['source']['artifact'] == 'all_results.bin#1' for item in candidates)


def test_adaptive_criteria_budget_dedup_and_worst_window_screen(tmp_path):
    """Respect current CPU criteria, preserve native tradeoffs, and screen maximum risk."""
    row = {'rubric': [{'metrics': ['dd'], 'target': .5, 'direction': 'min'}],
           'history': [{'evaluation_comparison': {'criteria': [
               {'metric': 'gain', 'direction': 'max', 'reducer': 'mean'},
               {'metric': 'dd', 'direction': 'min', 'reducer': 'max'}]}}]}
    good = [native(6 + i / 10, .01, 1 + i / 10) for i in range(12)]
    low_risk = native(3, .01, 4, dd=.1)
    unsafe = native(99, .01, 5, dd=.7)  # mean .35 would hide the violation
    write_stream(tmp_path / 'all_results.bin', [*good, good[-1], low_risk, unsafe])
    selected, coverage = supplemental_candidates(row,
        {'config': config(), 'optimizer_archive': {'candidate_capacity_per_job': 4}}, tmp_path,
        [('native.json', good[0])], summarize)
    values = [summarize(data, 'mean')['gain'] for name, data in selected]
    assert len(values) == 4 and len(set(values)) == 4
    assert 7.1 in values and 3 in values and 6 in values
    assert 99 not in values
    assert coverage['records_scanned'] == 15
    assert coverage['candidate_capacity'] == 4


@pytest.mark.parametrize('bad', ['unknown', 'duplicate', 'missing'])
def test_ambiguous_suite_reports_never_get_positional_window_names(tmp_path, bad):
    """Fail closed for unidentified, repeated, or absent training windows."""
    scenarios = [{'label': name, 'start_date': '2020-01-01', 'end_date': '2020-02-01'}
                 for name in ('train_01', 'train_02')]
    paths = [tmp_path / name / 'analysis.json' for name in ('train_01', 'train_02')]
    if bad == 'unknown':
        paths[0] = tmp_path / 'unidentified' / 'analysis.json'
    elif bad == 'duplicate':
        paths[1] = paths[0]
    else:
        paths.pop()
    job = {'config': {'backtest': {'suite_enabled': True, 'scenarios': scenarios}}}
    with pytest.raises(ValueError, match='CPU report'):
        report_contexts(paths, [{} for _ in paths], job, lambda path: {})


def test_reverse_file_order_keeps_exact_window_identity_in_ai_payload(record, tmp_path, monkeypatch):
    """Newest-first reports retain labels through both mandatory AI projections."""
    from api import backtest_v8 as bt
    from pb8_loop_ai import compact_evaluation_content
    store, row = record
    scenarios = [{'label': f'train_0{i}', 'start_date': f'2020-0{i}-01',
                  'end_date': f'2020-0{i}-20'} for i in range(1, 4)]
    cfg = config()
    cfg['backtest'].update(suite_enabled=True, scenarios=scenarios)
    paths = []
    for i in (3, 2, 1):
        path = tmp_path / f'train_0{i}' / 'analysis.json'
        path.parent.mkdir()
        path.write_text(json.dumps({'gain': i, 'backtest_completion_ratio': 1}))
        paths.append(path)
    monkeypatch.setattr(bt, '_result_analysis_paths', lambda name: paths)
    monkeypatch.setattr(bt, '_read_json', lambda path: json.loads(path.read_text()))
    observation = LoopBackend(tmp_path, store).observations(row, {'name': 'isolated', 'config': cfg})
    assert [item['gain'] for item in observation['per_report']] == [3, 2, 1]
    assert [item['label'] for item in observation['per_report_context']] == ['train_03', 'train_02', 'train_01']
    for context in observation['per_report_context']:
        i = int(context['label'][-1])
        assert observation['per_report'][context['report_index']]['gain'] == i
        assert context['requested_start_date'] == f'2020-0{i}-01'
    content = {'stage': 'evaluate', 'initial_config': cfg, 'rubric': row['rubric'],
               'evidence': {'observations': [observation]}}
    for projected in (required_content(content), compact_evaluation_content(content)):
        assert projected['evidence']['observations'][0]['per_report_context'] == observation['per_report_context']


def test_native_summary_binds_artifact_and_effective_dates(tmp_path):
    """Explicit native artifact mapping wins over any directory ordering assumption."""
    path = tmp_path / 'native_directory' / 'analysis.json'
    summary = tmp_path / 'suite_summary.json'
    summary.write_text('{}')
    data = {'per_scenario': {'train_A': {'artifacts': {'analysis.json': 'native_directory/analysis.json'}}},
            'suite_metrics': {'scenarios': {'train_A': {'effective_start_date': '2020-01-02T00:01:00Z'}}}}
    job = {'config': {'backtest': {'suite_enabled': True, 'scenarios': [
        {'label': 'train_A', 'start_date': '2020-01-01', 'end_date': '2020-01-20'}]}}}
    contexts = report_contexts([path], [{}], job, lambda path: data)
    assert contexts[0]['label'] == 'train_A'
    assert contexts[0]['effective_start_date'] == '2020-01-02T00:01:00Z'


def test_historical_advice_does_not_scan_unreviewed_stream(record, tmp_path, monkeypatch):
    """Historical provenance remains artifact-scoped; a foreign stream cannot leak in."""
    from api import optimize_v8 as opt
    store, row = record
    path = tmp_path / 'all_results.bin'
    path.write_bytes(b'not a valid stream and must not be read')
    front = native(2, .01, 2)
    monkeypatch.setattr(opt, '_resolve_result_path', lambda path: tmp_path)
    monkeypatch.setattr(opt, '_pareto_files_for_listing', lambda directory: (directory, [tmp_path / 'front.json']))
    monkeypatch.setattr(opt, '_read_json', lambda path: front)
    monkeypatch.setattr(opt, '_pareto_summary', summarize)
    result = LoopBackend(tmp_path, store)._result_candidates(row,
        {'path': 'isolated', 'result': 'isolated', 'modified': 1},
        {'operation': 'historical', 'config': config(), 'overrides': {}}, historical=True)
    assert len(result) == 1
    assert result[0]['source']['artifact'] == 'front.json'


def test_unlabeled_legacy_report_is_explicitly_unknown(tmp_path):
    """A retained old report never acquires a guessed training-window identity."""
    context = report_contexts([tmp_path / 'analysis.json'], [{}], {}, lambda path: {})[0]
    assert context['label'] is None
    assert context['identity'] == 'unavailable_legacy_context'
