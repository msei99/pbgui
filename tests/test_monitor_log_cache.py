"""Isolated correctness and I/O regressions for incremental monitor log caching."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
import json
import os
import sqlite3
import subprocess
import sys

import pytest

from monitor_log_cache import LogHistoryCache


HOUR = int(datetime(2026, 10, 15, 20, tzinfo=timezone.utc).timestamp()) // 3600
ERROR = "2026-10-15T20:00:00Z ERROR [bot] failed\n"
SUMMARY = "2026-10-15T20:01:00Z INFO [fill] 10 fills, pnl=2 USDT\n"
FILL = "2026-10-15T20:02:00Z INFO [fill] 2026-10-15T20:01:59Z HYPE long close -1 @ 80, pnl=+3 USDT, fee=-0.1 USDT\n"


@pytest.fixture
def cache(tmp_path):
    """Use only temporary cache and log files, never runtime state."""
    return LogHistoryCache(tmp_path / "cache" / "history.sqlite3", [tmp_path / "logs"])


def _write(tmp_path, name, text):
    """Create one isolated log fixture."""
    path = tmp_path / "logs" / name
    path.parent.mkdir(exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _collect(cache, paths, stderr=(), hour=HOUR, scope="8:bot"):
    """Collect a rolling 28-day window from fixture files."""
    return cache.collect(scope, list(map(str, paths)), list(map(str, stderr)), hour - 671, hour)


def test_initial_summary_counts_and_net_fees(cache, tmp_path):
    """The cold cache preserves ERROR/traceback buckets and canonical net PNL."""
    native = _write(tmp_path, "native.log", ERROR + SUMMARY + FILL)
    stderr = _write(tmp_path, "stderr.log", "2026-10-15T20:03:00Z Traceback (most recent call last):\n")
    result = _collect(cache, [native], [stderr])
    assert result["errors"] == {str(HOUR): 1}
    assert result["tracebacks"] == {str(HOUR): 1}
    assert result["pnl"] == {
        "total_fills": 11, "total_pnl": pytest.approx(4.9),
        "last_fill_ts": HOUR * 3600 + 119, "source": "PB8 fill batch summary",
    }


def test_restart_and_unchanged_logs_read_no_contents(cache, tmp_path, monkeypatch):
    """A new collector instance uses durable results without opening log files."""
    native = _write(tmp_path, "native.log", ERROR + SUMMARY + FILL)
    expected = _collect(cache, [native])
    restarted = LogHistoryCache(cache.path, cache.roots)
    original = Path.open

    def guarded_open(path, *args, **kwargs):
        """Fail if a cache hit reads any log content."""
        if path == native:
            pytest.fail("unchanged log was reopened")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)
    assert _collect(restarted, [native]) == expected


def test_append_reads_only_new_lines_and_bounded_guards(cache, tmp_path, monkeypatch):
    """Appending does not reparse the old multi-megabyte history."""
    native = _write(tmp_path, "native.log", "unrelated diagnostic\n" * 100000 + ERROR + SUMMARY)
    _collect(cache, [native])
    with native.open("a") as handle:
        handle.write(FILL + ERROR)
    import monitor_log_cache as module
    original = module._timestamp
    calls = []

    def tracked(value):
        """Count parsed timestamps rather than elapsed-time-sensitive performance."""
        calls.append(value)
        return original(value)

    monkeypatch.setattr(module, "_timestamp", tracked)
    result = _collect(cache, [native])
    assert len(calls) == 3
    assert result["errors"] == {str(HOUR): 2}
    assert result["pnl"]["total_fills"] == 11
    assert _collect(cache, [native]) == result


def test_partial_line_is_not_lost_or_double_counted(cache, tmp_path):
    """An incomplete write is consumed once after its newline arrives."""
    native = _write(tmp_path, "native.log", SUMMARY + ERROR[:-1])
    assert _collect(cache, [native])["errors"] == {}
    with native.open("a") as handle:
        handle.write("\n")
    assert _collect(cache, [native])["errors"] == {str(HOUR): 1}
    assert _collect(cache, [native])["errors"] == {str(HOUR): 1}


@pytest.mark.parametrize("replacement", [ERROR, ERROR + "new content\n" * 40])
def test_truncate_and_rewrite_rebuilds_file(cache, tmp_path, replacement):
    """Truncation and a rewrite beyond the old offset both replace cached facts."""
    native = _write(tmp_path, "native.log", SUMMARY + FILL)
    _collect(cache, [native])
    native.write_text(replacement)
    result = _collect(cache, [native])
    assert result["errors"] == {str(HOUR): 1}
    assert result["pnl"] is None


def test_rotation_reuses_inode_and_deduplicates_aliases(cache, tmp_path, monkeypatch):
    """Renamed rotations retain cached facts without rereading or duplication."""
    native = _write(tmp_path, "native.log", ERROR + SUMMARY)
    _collect(cache, [native])
    rotated = native.with_suffix(".log.1")
    native.rename(rotated)
    native.write_text(FILL + ERROR)
    alias = native.with_name("alias.log")
    alias.symlink_to(native)
    original = Path.open

    def guarded_open(path, *args, **kwargs):
        """A pure rename must not invalidate the inode cache."""
        if path == rotated:
            pytest.fail("rotated file was reread")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)
    result = _collect(cache, [rotated, native, alias])
    assert result["errors"] == {str(HOUR): 2}
    assert result["pnl"]["total_fills"] == 11
    assert result["pnl"]["total_pnl"] == pytest.approx(4.9)


def test_day_rollover_expires_buckets_without_reading_logs(cache, tmp_path, monkeypatch):
    """The rolling window advances even when the files remain unchanged."""
    native = _write(tmp_path, "native.log", ERROR + SUMMARY)
    _collect(cache, [native])
    original = Path.open

    def guarded_open(path, *args, **kwargs):
        """Window expiry reads cached aggregates only."""
        if path == native:
            pytest.fail("rollover reopened log")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)
    assert _collect(cache, [native], hour=HOUR + 672)["errors"] == {}


def test_latest_summary_across_files_supersedes_prior_fills(cache, tmp_path):
    """Global ordering retains only events later than the newest summary."""
    a = _write(tmp_path, "a.log", SUMMARY + FILL)
    b = _write(tmp_path, "b.log", "2026-10-15T20:03:00Z INFO [fill] 99 fills, pnl=-5 USDT\n" + FILL.replace("20:02:00", "20:04:00"))
    result = _collect(cache, [a, b])
    assert result["pnl"]["total_fills"] == 100
    assert result["pnl"]["total_pnl"] == pytest.approx(-2.1)


def test_equal_timestamps_preserve_source_alias_ordering(cache, tmp_path):
    """Equal-time summaries use source names, not resolved symlink names."""
    a = _write(tmp_path, "a.log", SUMMARY)
    b = _write(tmp_path, "b.log", SUMMARY.replace("10 fills", "20 fills"))
    os.utime(a, ns=(1000000000, 1000000000))
    os.utime(b, ns=(1000000000, 1000000000))
    alias = a.with_name("z.log")
    alias.symlink_to(a)
    result = _collect(cache, [alias, b])
    assert result["pnl"]["total_fills"] == 10


def test_removed_files_are_pruned_without_affecting_other_scopes(cache, tmp_path):
    """Cache cleanup releases removed files while preserving other bots."""
    a = _write(tmp_path, "a.log", ERROR)
    b = _write(tmp_path, "b.log", ERROR)
    _collect(cache, [a], scope="8:a")
    _collect(cache, [b], scope="8:b")
    _collect(cache, [], scope="8:a")
    with sqlite3.connect(cache.path) as db:
        assert db.execute("SELECT COUNT(*) FROM files").fetchone()[0] == 1
    assert _collect(cache, [b], scope="8:b")["errors"] == {str(HOUR): 1}


def test_failed_collection_rolls_back_offsets_and_counts(cache, tmp_path, monkeypatch):
    """A failed batch must not consume data or publish partial aggregates."""
    a = _write(tmp_path, "a.log", ERROR)
    b = _write(tmp_path, "b.log", ERROR)
    _collect(cache, [a, b])
    with a.open("a") as handle:
        handle.write(ERROR)
    original = cache._sync

    def fail_second(db, path, stat, identity, from_hour):
        """Simulate a rotation/read failure after another file advanced."""
        if path == b:
            raise OSError("simulated failure")
        return original(db, path, stat, identity, from_hour)

    monkeypatch.setattr(cache, "_sync", fail_second)
    with pytest.raises(OSError, match="simulated"):
        _collect(cache, [a, b])
    monkeypatch.setattr(cache, "_sync", original)
    assert _collect(cache, [a, b])["errors"] == {str(HOUR): 3}


def test_parallel_collectors_do_not_lose_or_double_counts(cache, tmp_path):
    """Independent collectors serialize cache transactions for the same logs."""
    native = _write(tmp_path, "native.log", ERROR + SUMMARY + FILL)

    def collect(_):
        """Use independent connections as real collector invocations do."""
        return _collect(LogHistoryCache(cache.path, cache.roots), [native])

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(collect, range(8)))
    assert all(result == results[0] for result in results)
    assert results[0]["errors"] == {str(HOUR): 1}


def test_rejects_paths_outside_roots_and_symlink_escape(cache, tmp_path):
    """Untrusted paths cannot make the collector read outside approved log roots."""
    outside = tmp_path / "outside.log"
    outside.write_text(ERROR)
    link = _write(tmp_path, "link.log", "")
    link.unlink()
    link.symlink_to(outside)
    for path in [outside, link]:
        with pytest.raises(ValueError, match="approved roots"):
            _collect(cache, [path])


def test_independent_processes_share_the_transaction_lock(cache, tmp_path):
    """Real OS processes must not race schema creation, offsets or aggregates."""
    native = _write(tmp_path, "native.log", ERROR + SUMMARY + FILL)
    source = """
import json, sys
from pathlib import Path
from monitor_log_cache import LogHistoryCache
cache = LogHistoryCache(Path(sys.argv[1]), [Path(sys.argv[2])])
print(json.dumps(cache.collect('8:bot', [sys.argv[3]], [], int(sys.argv[4])-671, int(sys.argv[4]))))
"""

    def run(_):
        """Launch an independent collector against temporary data."""
        result = subprocess.run([sys.executable, "-c", source, str(cache.path), str(cache.roots[0]), str(native), str(HOUR)],
                                capture_output=True, text=True, check=True, timeout=20)
        return json.loads(result.stdout)

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(run, range(4)))
    assert all(row == results[0] for row in results)
    assert results[0]["errors"] == {str(HOUR): 1}


def test_embedded_collector_uses_persistent_history_cache(tmp_path):
    """The real embedded command consumes the cache across isolated invocations."""
    from master.async_monitor import INSTANCE_COLLECT_SCRIPT

    pbgui, pb8 = tmp_path / "pbgui", tmp_path / "pb8"
    config_dir = pbgui / "data" / "run_v8" / "bot"
    config_dir.mkdir(parents=True)
    (pb8 / "logs").mkdir(parents=True)
    (pbgui / "pbgui.ini").write_text(f"[main]\npb8dir = {pb8}\n")
    (config_dir / "config.json").write_text(json.dumps({
        "pbgui": {"runtime": "pb8", "version": 1, "enabled_on": "test"},
        "live": {"user": "bot"},
    }))
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:00:00Z")
    (pb8 / "logs" / "bot.log").write_text(
        f"{stamp} ERROR [bot] failure\n{stamp} INFO [fill] 10 fills, pnl=2 USDT\n")
    prefix = 'python3 -u -c "\n'
    source = INSTANCE_COLLECT_SCRIPT[len(prefix):-2]
    # Never enumerate or operate on production processes in this integration test.
    source = source.replace("os.listdir('/proc')", "[]")
    marker = "for name, process_info in sorted(running_v8.items()):"
    source = source.replace(marker, f"running_v8 = {{'bot': {{'dir': {str(config_dir)!r}, 'pid': 0}}}}\n" + marker)
    env = {**os.environ, "PBGUI_PBGDIR": str(pbgui), "PBGUI_PB7DIR": str(tmp_path / "pb7"),
           "PBGUI_CACHE_VERSION": "2", "PBGUI_CACHE": "{}"}
    result = subprocess.run([sys.executable, "-c", source], env=env,
                            capture_output=True, text=True, timeout=20, check=True)
    first = json.loads(result.stdout.splitlines()[-1])
    env["PBGUI_CACHE"] = json.dumps(first["cache"])
    result = subprocess.run([sys.executable, "-c", source], env=env,
                            capture_output=True, text=True, timeout=20, check=True)
    second = json.loads(result.stdout.splitlines()[-1])
    assert second["history"] == first["history"]
    assert sum(first["history"]["8:bot"]["errors"].values()) == 1
    assert first["history"]["8:bot"]["pnl"]["total_fills"] == 10
    assert (pbgui / "data" / "monitor_agent" / "log_history.sqlite3").exists()
