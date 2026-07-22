"""Tests for the detached PB8 optimize status runner."""

from __future__ import annotations

import json
import os
import sys
from types import SimpleNamespace

import pytest

import pb8_optimize_runner


@pytest.mark.parametrize(
    ("options", "expected_tail"),
    [
        ({"mode": "fresh"}, []),
        ({"mode": "pareto_seed", "source": "/results/pareto/one.json"}, ["--start", "/results/pareto/one.json"]),
        ({"mode": "checkpoint_resume", "source": "/results/run"}, ["--resume", "/results/run"]),
    ],
)
def test_runner_builds_distinct_pb8_launch_modes(tmp_path, monkeypatch, options, expected_tail) -> None:
    """Fresh, Pareto seed, and checkpoint resume must map to different PB8 flags."""
    state = tmp_path / "state.json"
    ownership = tmp_path / "ownership.json"
    ready = tmp_path / "ready"
    options_path = tmp_path / "options.json"
    ownership.write_text(json.dumps({"pid": os.getpid()}), encoding="utf-8")
    options_path.write_text(json.dumps(options), encoding="utf-8")
    captured = {}

    async def fake_main() -> None:
        captured["argv"] = list(sys.argv)

    class Lease:
        """Minimal runtime lock lease."""

        def release(self) -> None:
            captured["released"] = True

    monkeypatch.setattr(pb8_optimize_runner.importlib, "import_module", lambda name: SimpleNamespace(main=fake_main))
    monkeypatch.setattr(pb8_optimize_runner, "acquire_master_runtime_lock", lambda _path: Lease())

    returncode = pb8_optimize_runner.main(
        [
            "optimize",
            str(state),
            str(ownership),
            str(ready),
            "/venv_pb8/bin/passivbot",
            "/pb8",
            "/queue/optimize.json",
            str(options_path),
        ]
    )

    assert returncode == 0
    assert captured["argv"] == ["/venv_pb8/bin/passivbot", "/queue/optimize.json", *expected_tail]
    assert captured["released"] is True
    assert json.loads(state.read_text(encoding="utf-8"))["returncode"] == 0
    assert json.loads(ownership.read_text(encoding="utf-8"))["pid"] == os.getpid()
    assert json.loads(ownership.read_text(encoding="utf-8"))["create_time"] > 0
    assert ready.read_text(encoding="utf-8") == f"{os.getpid()}\n"


def test_runner_passes_fine_tune_and_polish_options(tmp_path, monkeypatch) -> None:
    """PB8 expert launch controls must survive the detached runner boundary."""
    state = tmp_path / "state.json"
    ownership = tmp_path / "ownership.json"
    ready = tmp_path / "ready"
    options_path = tmp_path / "options.json"
    ownership.write_text(json.dumps({"pid": os.getpid()}), encoding="utf-8")
    options_path.write_text(
        json.dumps(
            {
                "mode": "pareto_seed",
                "source": "/results/pareto/one.json",
                "fine_tune_params": ["bot.long.risk.n_positions", "bot.short.risk.n_positions"],
                "polish_percentage": 0.2,
                "polish_bounds_mode": "override-tunable",
            }
        ),
        encoding="utf-8",
    )
    captured = {}

    async def fake_main() -> None:
        captured["argv"] = list(sys.argv)

    class Lease:
        """Minimal runtime lock lease."""

        def release(self) -> None:
            return None

    monkeypatch.setattr(pb8_optimize_runner.importlib, "import_module", lambda name: SimpleNamespace(main=fake_main))
    monkeypatch.setattr(pb8_optimize_runner, "acquire_master_runtime_lock", lambda _path: Lease())

    assert pb8_optimize_runner.main(
        ["optimize", str(state), str(ownership), str(ready), "passivbot", "/pb8", "/queue/config.json", str(options_path)]
    ) == 0
    assert captured["argv"] == [
        "passivbot",
        "/queue/config.json",
        "--start",
        "/results/pareto/one.json",
        "--fine-tune-params",
        "bot.long.risk.n_positions,bot.short.risk.n_positions",
        "--polish-pct",
        "0.2",
        "--polish-bounds-mode",
        "override-tunable",
    ]
