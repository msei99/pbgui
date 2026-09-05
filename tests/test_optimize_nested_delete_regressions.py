"""Offline destructive-selector boundary tests for PB8 optimize results."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi import HTTPException

from api import optimize_v8


@pytest.fixture
def owned_results(tmp_path, monkeypatch):
    """Create only temporary results and mock every process/ownership observation."""
    root = tmp_path / "results"
    queue = tmp_path / "queue"
    queue.mkdir()
    active = root / "active"
    nested = active / "pareto"
    nested.mkdir(parents=True)
    (nested / "candidate.json").write_text("{}", encoding="utf-8")
    old = root / "old"
    old.mkdir()
    (queue / "worker.pid").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(optimize_v8, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(optimize_v8, "_results_root", lambda: root)
    monkeypatch.setattr(optimize_v8, "_queue_dir", lambda: queue)
    monkeypatch.setattr(optimize_v8, "_read_process_record", lambda _name: {"pid": 77, "create_time": 1, "owned_results": [str(active)]})
    monkeypatch.setattr(optimize_v8, "_process_matches", lambda *_args: True)
    monkeypatch.setattr(optimize_v8.psutil, "Process", lambda _pid: SimpleNamespace(children=lambda **_kw: [], open_files=lambda: []))
    monkeypatch.setattr(optimize_v8, "_read_dash_registry", lambda: {})
    monkeypatch.setattr(optimize_v8, "_dash_sessions", {})
    monkeypatch.setattr(optimize_v8, "_dash_pending_sessions", {})
    return root, active, nested, old


@pytest.mark.parametrize("batch", [False, True])
@pytest.mark.parametrize("absolute", [False, True])
@pytest.mark.parametrize("selector", ["active/pareto", "active", ".", "old/child", "active/pareto/missing"])
def test_delete_only_accepts_inactive_top_level_runs(owned_results, monkeypatch, batch, absolute, selector):
    """Both routes reject nested children, active runs, and the parent before staging."""
    root, active, nested, old = owned_results
    (old / "child").mkdir()
    selected = str(root / selector) if absolute else selector
    stage = Mock(side_effect=AssertionError("No rename before guard"))
    remove = Mock(side_effect=AssertionError("No removal before guard"))
    monkeypatch.setattr(optimize_v8, "_stage_delete_result", stage)
    monkeypatch.setattr(optimize_v8, "rmtree", remove)
    with pytest.raises(HTTPException) as error:
        if batch:
            optimize_v8.delete_results({"paths": [str(old), selected]}, None)
        else:
            optimize_v8.delete_result(selected, None)
    assert error.value.status_code == (409 if selector == "active" else 400)
    stage.assert_not_called()
    remove.assert_not_called()
    assert (nested / "candidate.json").read_text(encoding="utf-8") == "{}"


@pytest.mark.parametrize("absolute", [False, True])
def test_nested_artifact_reads_remain_supported(owned_results, absolute):
    """Deletion restrictions must not narrow the shipped nested read/seed resolver."""
    root, active, nested, old = owned_results
    artifact = nested / "candidate.json"
    selected = str(artifact if absolute else artifact.relative_to(root))
    assert optimize_v8._resolve_result_path(selected, require_directory=False) == artifact
    assert optimize_v8._resolve_result_path(str(nested)) == nested
    assert optimize_v8._validate_pareto_seed_source(selected) == str(artifact)


def test_verified_unrelated_top_level_run_deletes_with_known_owner(owned_results):
    """Positive ownership still permits deletion of an unrelated inactive canonical run."""
    root, active, nested, old = owned_results
    assert optimize_v8.delete_result("old", None)["removed"] == 1
    assert not old.exists()
    assert active.is_dir()
