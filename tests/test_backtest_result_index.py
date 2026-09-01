"""Regression tests for the persistent Backtest result summary index."""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

import backtest_result_index


def _result(tmp_path: Path, name: str, gain: float) -> Path:
    """Create one compact result and return its analysis path."""
    result_dir = tmp_path / "results" / name
    result_dir.mkdir(parents=True)
    analysis_path = result_dir / "analysis.json"
    analysis_path.write_text(json.dumps({"gain": gain}), encoding="utf-8")
    (result_dir / "config.json").write_text(json.dumps({"name": name}), encoding="utf-8")
    return analysis_path


def _builder(calls: list[list[str]]):
    """Return a deterministic summary builder that records cache misses."""
    def build(paths: list[Path]) -> list[dict]:
        """Build summaries from the supplied analysis files."""
        calls.append([path.parent.name for path in paths])
        return [
            {
                "path": str(path.parent),
                "result_name": path.parent.name,
                "gain": json.loads(path.read_text(encoding="utf-8"))["gain"],
            }
            for path in paths
        ]

    return build


def test_index_rebuilds_only_new_or_changed_results(tmp_path: Path, monkeypatch) -> None:
    """Warm reads use SQLite while changed summary inputs rebuild only their own row."""
    monkeypatch.setattr(backtest_result_index, "PBGDIR", str(tmp_path))
    older = _result(tmp_path, "older", 1.1)
    newer = _result(tmp_path, "newer", 1.2)
    calls: list[list[str]] = []
    builder = _builder(calls)

    first = backtest_result_index.load_indexed_results("v8", [newer, older], builder)
    second = backtest_result_index.load_indexed_results("v8", [newer, older], builder)
    newer.write_text(json.dumps({"gain": 1.3}), encoding="utf-8")
    os.utime(newer, ns=(newer.stat().st_atime_ns, newer.stat().st_mtime_ns + 1_000_000))
    third = backtest_result_index.load_indexed_results("v8", [newer, older], builder)
    monkeypatch.setattr(backtest_result_index, "PBGUI_VERSION", "next-release")
    backtest_result_index.load_indexed_results("v8", [newer, older], builder)

    assert calls == [["newer", "older"], ["newer"], ["newer", "older"]]
    assert first == second
    assert [item["gain"] for item in third] == [1.3, 1.1]
    assert oct(backtest_result_index._db_path().stat().st_mode & 0o777) == "0o600"


def test_index_prunes_missing_rows_and_supports_explicit_invalidation(tmp_path: Path, monkeypatch) -> None:
    """Full scans and managed deletes remove summaries that no longer have a result."""
    monkeypatch.setattr(backtest_result_index, "PBGDIR", str(tmp_path))
    first = _result(tmp_path, "first", 1.1)
    second = _result(tmp_path, "second", 1.2)
    builder = _builder([])
    backtest_result_index.load_indexed_results("v8", [first, second], builder)

    backtest_result_index.load_indexed_results("v8", [first], builder, prune_missing=True)
    with sqlite3.connect(backtest_result_index._db_path()) as connection:
        assert connection.execute("SELECT result_path FROM result_summaries").fetchall() == [(str(first.parent),)]

    backtest_result_index.invalidate_result("v8", first.parent)
    with sqlite3.connect(backtest_result_index._db_path()) as connection:
        assert connection.execute("SELECT result_path FROM result_summaries").fetchall() == []


def test_index_failure_falls_back_to_filesystem_builder(tmp_path: Path, monkeypatch) -> None:
    """An unavailable reconstructable cache must never make result listing unavailable."""
    monkeypatch.setattr(backtest_result_index, "PBGDIR", str(tmp_path))
    analysis_path = _result(tmp_path, "fallback", 1.4)
    calls: list[list[str]] = []
    builder = _builder(calls)
    monkeypatch.setattr(
        backtest_result_index,
        "_connect",
        lambda: (_ for _ in ()).throw(sqlite3.OperationalError("database unavailable")),
    )
    monkeypatch.setattr(backtest_result_index, "_log", lambda *_args, **_kwargs: None)

    results = backtest_result_index.load_indexed_results("v8", [analysis_path], builder)

    assert calls == [["fallback"]]
    assert results[0]["gain"] == 1.4
