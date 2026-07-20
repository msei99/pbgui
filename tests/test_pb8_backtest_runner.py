"""Tests for the detached PB8 backtest status runner."""

from __future__ import annotations

import json
import os
import sys
from types import SimpleNamespace

import pb8_backtest_runner


def test_runner_persists_pb8_cli_exit_status(tmp_path, monkeypatch) -> None:
    """The runner should publish the exact CLI return code for queue recovery."""
    state = tmp_path / "state" / "job.json"
    ownership = tmp_path / "job.pid"
    ready = tmp_path / "state" / "job.ready"
    ownership.write_text(json.dumps({"pid": os.getpid()}), encoding="utf-8")
    captured = {}

    async def fake_main():
        captured["argv"] = list(sys.argv)
        return 7

    class Lease:
        def release(self):
            captured["released"] = True

    monkeypatch.setattr(pb8_backtest_runner.importlib, "import_module", lambda name: SimpleNamespace(main=fake_main) if name == "backtest" else None)
    monkeypatch.setattr(pb8_backtest_runner, "acquire_master_runtime_lock", lambda _path: Lease())

    returncode = pb8_backtest_runner.main(
        [
            "backtest",
            str(state),
            str(ownership),
            str(ready),
            "/venv_pb8/bin/passivbot",
            "/pb8",
            "/queue/backtest.json",
        ]
    )

    payload = json.loads(state.read_text(encoding="utf-8"))
    assert returncode == 7
    assert payload["returncode"] == 7
    assert captured == {
        "argv": ["/venv_pb8/bin/passivbot", "/queue/backtest.json"],
        "released": True,
    }
    assert ready.read_text(encoding="utf-8") == f"{os.getpid()}\n"
