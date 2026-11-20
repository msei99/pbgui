"""Regression tests for shared PB7/PB8 automatic backtest slots."""

from pathlib import Path
from types import SimpleNamespace

import pytest

import backtest_autostart as arbitration


@pytest.mark.parametrize('version', ['pb8', 'pb9'])
@pytest.mark.parametrize('relative', [False, True])
def test_runner_from_another_gui_does_not_consume_capacity(tmp_path, monkeypatch, version, relative):
    """Native runners belong to their PBGui checkout even when bot paths are shared."""
    root = tmp_path / 'pbgui'
    other = tmp_path / 'pbgui-share'
    monkeypatch.setattr(arbitration, 'PBGDIR', str(root))
    filename = f'{version}_backtest_runner.py'

    def process(pid, owner):
        """Build one process without inspecting the real host."""
        script = filename if relative else str(owner / filename)
        return SimpleNamespace(info={'pid': pid, 'create_time': float(pid),
                                      'cmdline': ['python', script, 'backtest']},
                               cwd=lambda: str(owner))

    monkeypatch.setattr(arbitration.psutil, 'process_iter', lambda _fields: [process(101, root), process(102, other)])
    assert arbitration._managed_processes() == {101: 101.0}


@pytest.mark.parametrize('own_config,own_parent,expected', [
    (True, False, True), (False, True, True), (False, False, False),
])
def test_pb7_checkout_does_not_determine_gui_ownership(tmp_path, monkeypatch, own_config, own_parent, expected):
    """Separate GUI daemons may use the same native PB7 checkout."""
    root = tmp_path / 'pbgui'
    other = tmp_path / 'pbgui-share'
    monkeypatch.setattr(arbitration, 'PBGDIR', str(root))
    config = (root if own_config else other) / 'data/bt_v7_queue/job.json'
    parent_root = root if own_parent else other
    parent = SimpleNamespace(cwd=lambda: str(parent_root), cmdline=lambda: ['python', 'PBGBacktests.py'])
    process = SimpleNamespace(info={'pid': 101, 'create_time': 10.0,
                                    'cmdline': ['python', '/shared/pb7/src/backtest.py', str(config)]},
                              parents=lambda: [parent])
    monkeypatch.setattr(arbitration.psutil, 'process_iter', lambda _fields: [process])
    assert arbitration._managed_processes() == ({101: 10.0} if expected else {})


def test_unrelated_backtest_does_not_block_first_local_slot(tmp_path, monkeypatch):
    """The actual slot claim ignores a different GUI's legacy PB7 process."""
    root = tmp_path / 'pbgui'
    other = tmp_path / 'pbgui-share'
    monkeypatch.setattr(arbitration, 'PBGDIR', str(root))
    monkeypatch.setattr(arbitration, '_state_root', lambda: root / 'data/locks')
    parent = SimpleNamespace(cwd=lambda: str(other), cmdline=lambda: ['python', 'PBGBacktests.py'])
    process = SimpleNamespace(info={'pid': 101, 'create_time': 10.0,
                                    'cmdline': ['python', '/shared/pb7/src/backtest.py', str(other / 'job.json')]},
                              parents=lambda: [parent])
    monkeypatch.setattr(arbitration.psutil, 'process_iter', lambda _fields: [process])
    assert arbitration.claim_backtest_slot('v9', 'local-job', 1)


def test_pb7_and_pb8_claim_one_shared_backtest_capacity(tmp_path: Path, monkeypatch) -> None:
    """Claims from either version consume the same configured CPU capacity."""
    monkeypatch.setattr(arbitration, "_state_root", lambda: tmp_path)
    monkeypatch.setattr(arbitration, "_managed_processes", lambda: {})
    monkeypatch.setattr(arbitration, "_process_matches", lambda _payload: True)

    assert arbitration.claim_backtest_slot("v7", "v7-a", 2) is True
    assert arbitration.claim_backtest_slot("v8", "v8-a", 2) is True
    assert arbitration.claim_backtest_slot("v7", "v7-b", 2) is False

    arbitration.publish_backtest_process("v7", "v7-a", 101, 10.0, ["backtest.py", "v7-a.json"])
    arbitration.publish_backtest_process("v8", "v8-a", 102, 11.0, ["pb8_backtest_runner.py", "v8-a.json"])
    assert arbitration.claim_backtest_slot("v8", "v8-b", 2) is False

    arbitration.release_backtest_slot("v7", "v7-a")
    assert arbitration.claim_backtest_slot("v8", "v8-b", 2) is True


def test_untracked_manual_backtests_consume_shared_capacity(tmp_path: Path, monkeypatch) -> None:
    """Automatic launches respect PBGui-compatible backtests started manually."""
    monkeypatch.setattr(arbitration, "_state_root", lambda: tmp_path)
    monkeypatch.setattr(arbitration, "_managed_processes", lambda: {201: 20.0})

    assert arbitration.claim_backtest_slot("v8", "queued", 1) is False


def test_pending_native_runner_counts_once_with_exact_command_identity(tmp_path, monkeypatch):
    """A real pending runner consumes one slot; unrelated untracked jobs still count."""
    monkeypatch.setattr(arbitration, '_state_root', lambda: tmp_path)
    monkeypatch.setattr(arbitration, '_managed_processes', lambda: {})
    assert arbitration.claim_backtest_slot('v9', 'first', 2,
        command_markers=['pb9_backtest_runner.py', 'unique-first.config.json'])
    monkeypatch.setattr(arbitration, '_managed_processes', lambda: {201: 20.0})
    monkeypatch.setattr(arbitration, '_process_matches', lambda payload:
        payload.get('pid') == 201 and payload.get('command_markers') == ['pb9_backtest_runner.py', 'unique-first.config.json'])
    assert arbitration.claim_backtest_slot('v8', 'second', 2)
    assert not arbitration.claim_backtest_slot('v7', 'third', 2)
    arbitration.release_backtest_slot('v8', 'second')
    monkeypatch.setattr(arbitration, '_managed_processes', lambda: {201: 20.0, 202: 21.0})
    assert not arbitration.claim_backtest_slot('v8', 'second', 2)
