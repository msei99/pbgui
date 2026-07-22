"""Regression tests for shared PB7/PB8 automatic backtest slots."""

from pathlib import Path

import backtest_autostart as arbitration


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
