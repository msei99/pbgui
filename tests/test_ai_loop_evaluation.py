"""Adaptive criteria require comparable complete exact training evidence."""
import copy

import pytest

from ai_loop_evaluation import evaluation_comparison


def example():
    """Use two synthetic candidates where growth and risk rank differently."""
    options = {'baseline': {'per_report': [{'gain': 2, 'dd': .2}, {'gain': 4, 'dd': .3}]},
               'candidate': {'per_report': [{'gain': 4, 'dd': .4}, {'gain': 5, 'dd': .45}]}}
    decision = {'evaluation_criteria': [{'metric': 'gain', 'direction': 'max', 'reducer': 'mean', 'reason': 'return'}],
                'candidate_assessments': {'baseline': 'lower gain, lower risk', 'candidate': 'higher gain and risk'}}
    return decision, options


def test_changed_criteria_recompute_every_candidate_without_rewriting_observations():
    """Changing the method recomputes baseline and challenger with identical reducers."""
    decision, options = example()
    before = copy.deepcopy(options)
    assert evaluation_comparison(decision, options)['candidates']['candidate']['values'] == [4.5]
    decision['evaluation_criteria'] = [{'metric': 'dd', 'direction': 'min', 'reducer': 'max', 'reason': 'worst risk'}]
    result = evaluation_comparison(decision, options)
    assert result['candidates']['baseline']['values'] == [.3]
    assert result['candidates']['candidate']['values'] == [.45]
    assert options == before


def test_selective_reassessment_is_rejected():
    """The AI cannot revise criteria only for its favored candidate."""
    decision, options = example()
    decision['candidate_assessments'].pop('baseline')
    with pytest.raises(ValueError, match='every supplied training candidate'):
        evaluation_comparison(decision, options)


@pytest.mark.parametrize('value', [None, True, float('nan'), float('inf')])
def test_missing_or_nonfinite_evidence_blocks_new_metric(value):
    """An absent metric cannot be invented or replaced by an aggregate proxy."""
    decision, options = example()
    options['baseline']['per_report'][0]['gain'] = value
    with pytest.raises(ValueError, match='unavailable'):
        evaluation_comparison(decision, options)


def test_controller_reassesses_prior_training_candidates_but_never_observers(tmp_path):
    """A changed ranking can select an earlier candidate using exact training only."""
    import asyncio
    from types import SimpleNamespace
    from pb8_loop_controller import LoopController
    from tests.test_ai_loop_objectives import reviewed_run, RUBRIC
    from tests.test_pb8_loop_optimizer import _warm_start_observation
    store, row, _ = reviewed_run(tmp_path)

    def observation(operation, gain, risk, number):
        """Build a complete synthetic CPU observation, never a real backtest."""
        item = _warm_start_observation(row, gain, number)
        metrics = {'adg_strategy_eq': gain, 'gain_strategy_eq': gain,
                   'drawdown_worst_strategy_eq': risk, 'backtest_completion_ratio': 1}
        item.update(operation=operation, metrics=metrics, per_report=[metrics], reports=1)
        item['assessment'].update(comparable=True, simulation_complete=True, hard_targets_met=True)
        return item

    baseline = observation('baseline', 1, .2, 1)
    old = observation('old', 2, .1, 2)
    fresh = observation('fresh', 3, .4, 3)
    row.update(round=1, phase='evaluate', rubric=RUBRIC, baseline=baseline, best=baseline,
               observations=[fresh], jobs=[
                   {'kind': 'validation', 'operation': 'old', 'round': 0, 'status': 'completed', 'candidate': old['candidate']},
                   {'kind': 'validation', 'operation': 'fresh', 'round': 1, 'status': 'completed'},
                   {'kind': 'observer_holdout', 'operation': 'private', 'round': 0, 'status': 'completed'}])
    row['settings']['max_rounds'] = 1
    row = store.update(row['owner'], row['id'], lambda current: current.update(row))
    recovered = []

    def reports(current, job):
        """Only the earlier training validation may be recovered."""
        recovered.append(job['operation'])
        assert job['operation'] == 'old'
        return copy.deepcopy(old)

    async def decide(current, stage, evidence):
        """Compare all options under the same newly selected risk criterion."""
        assert set(evidence['winner_options']) == {'baseline', 'old', 'fresh'}
        return {'winner_operation': 'old', 'reason': 'Lower measured risk at higher gain than baseline',
                'finish': True, 'evaluation_criteria': [
                    {'metric': 'drawdown_worst_strategy_eq', 'direction': 'min', 'reducer': 'max',
                     'reason': 'Compare worst training risk'}],
                'candidate_assessments': {'baseline': 'DD .2', 'old': 'DD .1', 'fresh': 'DD .4'}}

    async def finish(current, reason):
        """Keep lifecycle effects inside the test-owned store."""
        return store.update(current['owner'], current['id'], lambda saved: saved.update(status='completed'))

    controller = LoopController(tmp_path, store=store,
                                backend=SimpleNamespace(observations=reports, capacity=lambda _: 1), ai=SimpleNamespace())
    controller.decision, controller.finish = decide, finish
    asyncio.run(controller._tick(row))
    saved = store.read(row['owner'], row['id'])
    assert recovered == ['old'] and saved['best']['operation'] == 'old'
    assert set(saved['training_candidates']) == {'old', 'fresh'}
    comparison = saved['history'][-1]['evaluation_comparison']
    assert comparison['candidates']['old']['values'] == [.1]
    assert comparison['candidates']['fresh']['values'] == [.4]
