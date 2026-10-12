"""Exact liquidation flags and measurable overview rows retain their meaning."""
import json
from pathlib import Path
import shutil
import subprocess

import pytest

from pb8_loop_backend import LoopBackend
from pb8_loop_store import LoopStore, evaluate


@pytest.mark.parametrize('reports,expected,qualified', [
    ([{'liquidated': False}, {'liquidated': False}], 0, True),
    ([{'liquidated': False}, {'liquidated': True}], 1, False),
    ([{'liquidated': False}, {}], None, False),
    ([{'liquidated': 'false'}], None, False),
    ([{'liquidated_usd': False}], 0, True),
    ([{'liquidated_btc': False}], None, False),
])
def test_exact_liquidation_requires_explicit_native_flag(tmp_path, monkeypatch, reports, expected, qualified):
    """A missing/string/BTC-only flag cannot prove quote-currency safety."""
    from api import backtest_v8 as bt
    paths = []
    for index, report in enumerate(reports):
        path = tmp_path / f'{index}.json'
        path.write_text(json.dumps({'backtest_completion_ratio': 1, 'unrelated': False, **report}))
        paths.append(path)
    monkeypatch.setattr(bt, '_result_analysis_paths', lambda name: paths)
    monkeypatch.setattr(bt, '_read_json', lambda path: json.loads(path.read_text()))
    record = {'rubric': [{'goal': 'custom', 'metrics': ['liquidated_usd'], 'target': 0, 'direction': 'min'}]}
    result = LoopBackend(tmp_path, LoopStore(tmp_path / 'loops')).observations(record, {'name': 'isolated'})
    assert result['metrics'].get('liquidated_usd') == expected
    assert result['assessment']['hard_targets_met'] is qualified
    assert result['assessment']['comparable'] is (expected is not None)
    assert 'unrelated' not in result['metrics']
    assert result['assessment']['goals'][0]['metric'] == 'liquidated_usd'


def test_missing_goal_retains_requested_metric_name():
    """Unknown observations still identify the requirement that lacks evidence."""
    assessment = evaluate({}, [{'goal': 'custom', 'metrics': ['liquidated_usd'], 'target': 0, 'direction': 'min'}])
    assert assessment['goals'][0]['metric'] == 'liquidated_usd'
    assert assessment['goals'][0]['value'] is None
    assert not assessment['hard_targets_met']


def test_overview_hides_instruction_rows_but_keeps_missing_requirements():
    """Execute the actual overview renderer with minimal DOM boundary doubles."""
    node = shutil.which('node')
    if not node:
        pytest.skip('Node is unavailable')
    source = (Path(__file__).resolve().parents[1] / 'frontend/js/pb8_loop_optimizer.js').read_text()
    function = source[source.index('  function overviewGoals('):source.index('  function overviewObserver(')]
    program = r'''
    function element(tag,text){return {textContent:text,children:[],appendChild(c){this.children.push(c);}};}
    function overviewValue(value){return value==null?'—':String(value);}
    function overviewChange(){}
    const cell={children:[],classList:{add(){}},replaceChildren(){this.children=[];},appendChild(c){this.children.push(c);}};
    ''' + function + r'''
    overviewGoals(cell,[
      {goal:'custom',metrics:[],value:null},
      {goal:'custom',metric:'liquidated_usd',target:0,value:null},
      {goal:'custom',metrics:['exponential_fit_error'],value:0.01},
      {goal:'gain',metrics:['gain_strategy_eq'],value:2}
    ],{goals:[]},false);
    console.log(JSON.stringify(cell.children.map(row=>row.children[0].textContent)));
    '''
    result = subprocess.run([node, '-e', program], capture_output=True, text=True, check=True, timeout=10)
    assert json.loads(result.stdout) == ['Liquidated —', 'Fit error 0.01', 'Gain 2']
