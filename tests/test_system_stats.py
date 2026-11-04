"""Tests for local system statistics used by Storage and Welcome."""

from __future__ import annotations

import threading
from collections import namedtuple

import system_stats

Times = namedtuple("Times", "user nice system idle iowait irq softirq steal guest guest_nice")


def _times(busy: float, idle: float) -> Times:
    return Times(busy, 0.0, 0.0, idle, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)


def test_cpu_percent_is_independent_of_the_calling_thread(monkeypatch):
    """Calls from changing executor threads measure the window since the previous call."""
    samples = iter([_times(100, 900), _times(160, 940), _times(160, 1040)])
    clock = iter([10.0, 15.0, 20.0])
    monkeypatch.setattr(system_stats.psutil, "cpu_times", lambda: next(samples))
    monkeypatch.setattr(system_stats.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(system_stats, "_cpu_sample", None)
    monkeypatch.setattr(system_stats, "_cpu_value", 0.0)
    results = []

    def call():
        results.append(system_stats.cpu_percent())

    for _ in range(3):
        thread = threading.Thread(target=call)
        thread.start()
        thread.join()
    assert results == [0.0, 60.0, 0.0]


def test_cpu_percent_keeps_value_for_short_windows(monkeypatch):
    """Back-to-back calls reuse the last value instead of measuring an empty window."""
    samples = iter([_times(100, 900), _times(150, 950), _times(150, 950)])
    clock = iter([10.0, 12.0, 12.1])
    monkeypatch.setattr(system_stats.psutil, "cpu_times", lambda: next(samples))
    monkeypatch.setattr(system_stats.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(system_stats, "_cpu_sample", None)
    monkeypatch.setattr(system_stats, "_cpu_value", 0.0)
    assert [system_stats.cpu_percent() for _ in range(3)] == [0.0, 50.0, 50.0]


def test_accounting_matches_psutil():
    """Total and busy time follow psutil's own rules, including guest time."""
    sample = Times(150.0, 5.0, 20.0, 950.0, 7.0, 1.0, 2.0, 3.0, 40.0, 4.0)
    total, busy = system_stats._cpu_total_busy(sample)
    assert total == system_stats.psutil._cpu_tot_time(sample)
    assert busy == system_stats.psutil._cpu_busy_time(sample)


class _Proc:
    """Minimal psutil.Process stand-in for process detection."""

    def __init__(self, pid, ppid, cmdline, rss=1000):
        self.info = {"pid": pid, "ppid": ppid, "cmdline": cmdline, "memory_info": type("M", (), {"rss": rss})()}

    def cpu_percent(self, interval=None):
        return 0.0


def test_process_detection_uses_scripts_not_working_directory(tmp_path, monkeypatch):
    """PBGui scripts, modules and their multiprocessing helpers are listed; other tools are not."""
    pbgui = tmp_path / "pbgui"
    (pbgui / "master").mkdir(parents=True)
    (pbgui / "PBApiServer.py").write_text("")
    (pbgui / "master" / "__init__.py").write_text("")
    (pbgui / "master" / "daemon.py").write_text("")
    py = "/venv/bin/python"
    procs = [
        _Proc(10, 1, [py, "-u", str(pbgui / "PBApiServer.py")]),
        _Proc(11, 10, [py, "-c", "from multiprocessing.spawn import spawn_main; spawn_main(tracker_fd=3)", "--multiprocessing-fork"]),
        _Proc(12, 10, [py, "-c", "from multiprocessing.resource_tracker import main;main(33)"]),
        _Proc(13, 11, [py, "-c", "from multiprocessing.spawn import spawn_main", "--multiprocessing-fork"]),
        _Proc(20, 1, [py, "-m", "master.daemon"]),
        _Proc(30, 1, [py, "/other/project/run_server.py"]),
        _Proc(31, 1, [py, "-c", "from multiprocessing.spawn import spawn_main"]),
        _Proc(32, 1, ["/usr/bin/python3", "/usr/share/unattended-upgrades/unattended-upgrade-shutdown"]),
        _Proc(33, 1, []),
    ]
    monkeypatch.setattr(system_stats.psutil, "process_iter", lambda attrs=None: iter(procs))
    monkeypatch.setattr(system_stats, "_process_cache", {})
    monkeypatch.chdir(pbgui)
    result = system_stats.collect_processes(pbgui)
    names = {item["pid"]: item["name"] for item in result["items"]}
    assert names == {
        10: "PBApiServer",
        11: "PBApiServer · worker",
        12: "PBApiServer · resource tracker",
        13: "PBApiServer · worker",
        20: "master.daemon",
    }
    assert result["count"] == 5 and result["rss"] == 5000
