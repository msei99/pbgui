"""Offline checks for the Vast worker's optimizer progress parser."""

import json

from setup.vast_gpu_benchmark import cloud_worker


def test_exact_progress_keeps_completed_proxy_generation(tmp_path, monkeypatch):
    """An exact-only event must not erase the last GPU generation count."""
    monkeypatch.setattr(cloud_worker, "ROOT", tmp_path)
    output = tmp_path / "output"
    output.mkdir()
    events = [
        {"event": "generation", "generation": 2, "population_size": 8192, "exact_completed": 64},
        {"event": "exact_progress", "generation": 2, "exact_completed": 80, "exact_inflight": 0},
    ]
    (output / "optimizer.log").write_text(
        "".join("[gpu-profile] " + json.dumps(event) + "\n" for event in events)
    )

    progress = cloud_worker.status()

    assert progress["gpu_candidates"] == 16384
    assert progress["exact_completed"] == 80
