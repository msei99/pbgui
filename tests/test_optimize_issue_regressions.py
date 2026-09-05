"""Offline regressions for optimize, Pareto, AI and rebacktest issue fixes."""

import asyncio
import copy
import json
import re
import subprocess
import threading
from collections import OrderedDict
from pathlib import Path
from types import SimpleNamespace

import msgpack
import pytest
from fastapi import HTTPException

from ai_capabilities import AICapabilityService
from ai_chat import AIChatService
from api import backtest_v8, optimize_v8
from ParetoDataLoader import ParetoDataLoader


@pytest.mark.parametrize("ascending, expected", [(False, [1, 2, 0]), (True, [2, 1, 0])])
@pytest.mark.parametrize("pareto_only", [False, True])
def test_missing_pareto_metric_ranks_last(ascending, expected, pareto_only):
    """#149: Missing metrics cannot win either ordering or a top-N selection."""
    loader = ParetoDataLoader("unused")
    loader.configs = [
        SimpleNamespace(config_index=0, suite_metrics={}, is_pareto=True),
        SimpleNamespace(config_index=1, suite_metrics={"score": 2}, is_pareto=True),
        SimpleNamespace(config_index=2, suite_metrics={"score": 1}, is_pareto=True),
    ]
    ranked = loader.get_top_configs("score", n=3, ascending=ascending, pareto_only=pareto_only)
    assert [item.config_index for item in ranked] == expected
    assert loader.get_top_configs("score", n=1, ascending=ascending)[0] is ranked[0]


@pytest.mark.parametrize("skipped", ["malformed", "rejected"])
def test_pareto_snapshot_matches_selected_index_after_skipped_file(tmp_path, monkeypatch, skipped):
    """#147: Compact indices retain the parsed snapshot, including after a reload."""
    from test_pareto_preset_generator import _loader_config

    pareto = tmp_path / "pareto"
    pareto.mkdir()
    first = _loader_config(1, {"score": 1})
    second = _loader_config(2, {"score": 2})
    (pareto / "000.json").write_text(json.dumps(first), encoding="utf-8")
    (pareto / "001.json").write_text("{" if skipped == "malformed" else '{"reject":true}', encoding="utf-8")
    (pareto / "002.json").write_text(json.dumps(second), encoding="utf-8")
    loader = ParetoDataLoader(str(tmp_path))
    parse = loader._parse_json_config
    monkeypatch.setattr(loader, "_parse_json_config", lambda data, index: None if data.get("reject") else parse(data, index))

    assert loader.load_pareto_jsons_only()
    assert [item.config_index for item in loader.configs] == [0, 1]
    assert loader.get_full_config(0) == first
    assert loader.get_full_config(1) == second
    assert loader.get_full_config(2) is None
    assert loader.get_full_config(-1) is None
    # Directory changes must not silently change the selected strategy.
    (pareto / "000.json").unlink()
    assert loader.get_full_config(0) == first
    assert loader.load_pareto_jsons_only()
    assert loader.get_full_config(0) == second
    assert loader.get_full_config(1) is None


@pytest.fixture
def result_roots(tmp_path, monkeypatch):
    """Keep every resolver root inside temporary storage without runtime discovery."""
    local, legacy, archive = [tmp_path / name for name in ("results", "legacy", "archives")]
    for root in (local, legacy, archive):
        root.mkdir()
    monkeypatch.setattr(optimize_v8, "_results_root", lambda: local)
    monkeypatch.setattr(backtest_v8, "_results_root", lambda: local)
    monkeypatch.setattr(backtest_v8, "_legacy_results_roots", lambda: [legacy])
    monkeypatch.setattr(backtest_v8, "_data_dir", lambda: tmp_path)
    monkeypatch.setattr(backtest_v8, "_result_config", lambda _path: {"config_version": "v8"})
    monkeypatch.chdir(tmp_path)
    return local, legacy, archive


@pytest.mark.parametrize("absolute", [False, True])
def test_optimize_result_and_artifact_paths_are_root_relative(result_roots, absolute):
    """#143: Both directory and nested Pareto artifact selectors accept relative paths."""
    root = result_roots[0]
    result = root / "run"
    pareto = result / "pareto"
    pareto.mkdir(parents=True)
    artifact = pareto / "candidate.json"
    artifact.write_text("{}", encoding="utf-8")
    select = lambda path: str(path if absolute else path.relative_to(root))
    assert optimize_v8._resolve_result_path(select(result)) == result
    assert optimize_v8._resolve_result_path(select(artifact), require_directory=False) == artifact


@pytest.mark.parametrize("root_index", [0, 1, 2])
@pytest.mark.parametrize("absolute", [False, True])
def test_backtest_result_paths_search_allowed_roots(result_roots, root_index, absolute):
    """#143: Relative names find local, legacy and archive results without using cwd."""
    root = result_roots[root_index]
    result = root / "config" / "exchange" / "run"
    result.mkdir(parents=True)
    (result / "analysis.json").write_text("{}", encoding="utf-8")
    (result / "config.json").write_text('{"config_version":"v8"}', encoding="utf-8")
    selected = str(result if absolute else result.relative_to(root))
    assert backtest_v8._resolve_result_dir(selected) == result
    if root_index:
        with pytest.raises(HTTPException):
            backtest_v8._resolve_result_dir(selected, allow_legacy=False, allow_archives=False)


@pytest.mark.parametrize("resolver", [optimize_v8._resolve_result_path, backtest_v8._resolve_result_dir])
@pytest.mark.parametrize("selected", ["", ".", "../outside", "run/../../outside", "run\x00bad", "run\nbad", "..\\outside", "linked"])
def test_relative_result_paths_keep_containment_checks(result_roots, tmp_path, resolver, selected):
    """#143: Root, traversal, control characters and symlink selectors remain rejected."""
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "analysis.json").write_text("{}", encoding="utf-8")
    (result_roots[0] / "linked").symlink_to(outside, target_is_directory=True)
    with pytest.raises(HTTPException) as exc:
        resolver(selected)
    assert exc.value.status_code in (400, 404)
    with pytest.raises(HTTPException) as exc:
        resolver(str(outside))
    assert exc.value.status_code == 400


def test_evaluation_scanner_stops_at_partial_tail_and_resumes_after_append(tmp_path, monkeypatch):
    """#145: A truncated final record causes one scan, not an unbounded polling loop."""
    monkeypatch.setattr(optimize_v8, "_active_eval_count_cache", OrderedDict())
    monkeypatch.setattr(optimize_v8, "_active_eval_scan_threads", {})
    monkeypatch.setattr(optimize_v8, "_active_eval_scan_stops", {})
    path = tmp_path / "all_results.bin"
    first = msgpack.packb({"score": 1})
    second = msgpack.packb({"score": 2})
    path.write_bytes(first + second[:-1])
    count = optimize_v8._all_results_evaluation_count
    stop = threading.Event()
    calls = []

    def bounded_count(path):
        """Bound even the buggy implementation so the regression never spins forever."""
        result = count(path)
        calls.append(result)
        if len(calls) >= 5:
            stop.set()
        return result

    monkeypatch.setattr(optimize_v8, "_all_results_evaluation_count", bounded_count)
    key = str(path)
    optimize_v8._active_eval_scan_threads[key] = object()
    optimize_v8._active_eval_scan_stops[key] = stop
    optimize_v8._run_active_evaluation_scan(path, key, stop)
    assert len(calls) == 1
    assert calls[0]["evaluations"] == 1
    assert calls[0]["trailing_partial_entry"]
    assert key not in optimize_v8._active_eval_scan_threads
    assert key not in optimize_v8._active_eval_scan_stops
    with path.open("ab") as handle:
        handle.write(second[-1:])
    optimize_v8._run_active_evaluation_scan(path, key, stop)
    assert calls[-1]["evaluations"] == 2
    assert calls[-1]["scan_complete"]


def test_evaluation_scanner_stops_when_budget_cannot_complete_record(tmp_path, monkeypatch):
    """#145: A record larger than the per-scan byte budget cannot spin either."""
    stop = threading.Event()
    calls = []

    def stalled_count(_path):
        """Return bounded incomplete progress and cap the old buggy loop."""
        calls.append(True)
        if len(calls) == 5:
            stop.set()
        return {"bytes_scanned": 0, "scan_complete": False, "trailing_partial_entry": False}

    monkeypatch.setattr(optimize_v8, "_all_results_evaluation_count", stalled_count)
    optimize_v8._run_active_evaluation_scan(tmp_path / "unused", "isolated", stop)
    assert len(calls) == 2


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_ai_nonfinite_metrics_are_null_before_dataset_encoding(tmp_path, monkeypatch, value):
    """#139: Run summaries and candidate matrices stay strict JSON through persistence."""
    async def scenario():
        """Create a proposal using only temporary storage and mocked result discovery."""
        service = AICapabilityService(tmp_path / "capabilities")
        raw = {"path": "managed/run", "name": "run", "sharpe_ratio": value}
        original = copy.deepcopy(raw)
        monkeypatch.setattr(service, "_resolve_listed_resource", lambda *_args: raw)
        monkeypatch.setattr(optimize_v8, "list_paretos", lambda *_args, **_kwargs: {
            "paretos": [{"path": "managed/run/pareto/a.json", "name": "a", "summary": {"gain": value}}]
        })
        created = await service._propose_optimizer_run_python_analysis("a" * 32, "b" * 32, {
            "version": "v8", "run_resource": service._virtual_uri("optimizer-run", "v8", "managed/run"),
            "metrics": ["gain"], "code": "print('ok')",
        })
        dataset = service.proposals[created["proposal_id"]].config["input_data"]
        assert dataset["run"]["sharpe_ratio"] is None
        assert dataset["candidates"][0]["values"] == [[None]]
        assert service._sanitize_config({"metrics": [value, {"score": value}]}) == {"metrics": [None, {"score": None}]}
        json.dumps(dataset, allow_nan=False)
        assert service._digest(dataset) == created["preview"]["input_resource"]["digest"]
        assert raw["path"] == original["path"]
        assert raw["sharpe_ratio"] is value
        # Approval hashes must still reject non-finite executable payloads.
        with pytest.raises(ValueError):
            service._digest({"parameter": value})

    asyncio.run(scenario())


@pytest.mark.parametrize("stdout", [b'{"score":NaN}', b'{"score":Infinity}', b'{"score":-Infinity}', b'{"score":[1e999,-1e999]}'])
def test_ai_nonfinite_analysis_output_survives_chat_continuation(tmp_path, stdout):
    """#139: Preserve non-finite stdout as text rather than failing strict JSON follow-up."""
    async def scenario():
        """Persist and reattach the same analysis result without any provider access."""
        service = AIChatService(tmp_path / "ai")
        owner = "a" * 32
        conversation = await service._conversation(owner, "chatgpt", "model", None)
        output = AICapabilityService._analysis_output(stdout)
        assert output == {"format": "text", "text": stdout.decode()}
        result = {"proposal_id": "b" * 32, "action": "python_analysis", "status": "executed", "output": output, "exit_code": 0}
        first = await service.record_approved_action_result(owner, conversation.id, result)
        assert await service.record_approved_action_result(owner, conversation.id, result) == first
        json.dumps(first, allow_nan=False)
        assert len(conversation.messages) == 1
        await service.shutdown()

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", ["results", "draft", "preserved", "single_exchange"])
def test_multiexchange_rebacktest_queue_names_are_distinct(mode):
    """#138: Execute real modal callbacks with a fake DOM and no HTTP requests."""
    source = (Path(__file__).resolve().parents[1] / "frontend/v7_backtest.html").read_text(encoding="utf-8")
    functions = "\n".join(re.search(r"^function " + name + r"\(.*?^}", source, re.M | re.S).group()
                          for name in ("showInitialBacktestQueueDraftModal", "getQueueDraftItemExchanges", "rebacktestSelected"))
    script = r"""
const assert = require('node:assert/strict');
const mode = MODE;
const queued = [], errors = [];
const cfg = {backtest: {exchanges: ['binance', 'bybit'], start_date:'2020-01-01', end_date:'2021-01-01'}};
const original = JSON.stringify(cfg);
const results = [{path:'a', config_name:'alpha'}, {path:'b', config_name:'beta'}];
const inputs = {
  'rbt-start': {value:'2022-01-01'}, 'rbt-end': {value:'2023-01-01'},
  'rbt-balance': {value:'1000'}, 'rbt-pbgui-data': {checked:false},
  'rbt-exchanges': {options: [{selected:true,value:'binance'}, {selected:mode !== 'single_exchange',value:'bybit'}]}
};
const document = {getElementById: id => inputs[id]};
function getSelectedResults() { return ['a','b']; }
function apiFetch(path, options) {
  if (path === '/queue') { queued.push(JSON.parse(options.body)); return Promise.resolve({}); }
  return Promise.resolve(JSON.parse(original));
}
function toast(message, kind) { if (kind === 'err') errors.push(message); }
function showModal(title, body, actions) { actions[0].action(); }
function closeModal() {}
function selectPanel() {}
function wsRefresh() {}
function backtestDialogDateInputHtml() { return ''; }
function pbguiMarketDataDefaultCheckedAttr() { return ''; }
FUNCTIONS
if (mode === 'results') rebacktestSelected();
else showInitialBacktestQueueDraftModal([{name:'alpha', config:cfg, preserve_exchanges:mode === 'preserved', preserve_timerange:mode === 'preserved'}]);
setImmediate(() => {
  assert.deepEqual(errors, []);
  const expected = mode === 'results' ? ['alpha_binance','alpha_bybit','beta_binance','beta_bybit']
    : mode === 'draft' ? ['alpha_binance','alpha_bybit'] : ['alpha'];
  assert.deepEqual(queued.map(x => x.name), expected);
  assert.equal(new Set(queued.map(x => x.name)).size, queued.length);
  if (mode === 'preserved') assert.deepEqual(queued[0].config.backtest, {...cfg.backtest,starting_balance:1000});
  else queued.forEach(x => assert.equal(x.config.backtest.exchanges.length, 1));
  assert.equal(JSON.stringify(cfg), original);
});
""".replace("MODE", json.dumps(mode)).replace("FUNCTIONS", functions)
    completed = subprocess.run(["node", "-e", script], capture_output=True, text=True, timeout=10, check=False)
    assert completed.returncode == 0, completed.stderr
