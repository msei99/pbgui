"""Regression tests for the persistent Backtest result summary index."""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
import backtest_result_index


def test_cold_pb8_builder_does_not_block_pb9_or_invalidation(tmp_path, monkeypatch):
    """A slow source reader owns no shared database lock while unrelated work proceeds."""
    monkeypatch.setattr(backtest_result_index, 'PBGDIR', tmp_path)
    older, native = _result(tmp_path, 'pb8', 1.2), _result(tmp_path, 'pb9', 1.3)
    entered, release = threading.Event(), threading.Event()
    def slow(paths):
        """Hold only PB8 build ownership until the other generation has completed."""
        entered.set()
        assert release.wait(5)
        return _builder([])(paths)
    with ThreadPoolExecutor(max_workers=3) as pool:
        pending = pool.submit(backtest_result_index.load_indexed_results, 'v8', [older], slow)
        try:
            assert entered.wait(2)
            result = pool.submit(backtest_result_index.load_indexed_results, 'v9', [native], _builder([])).result(timeout=2)
            assert result[0]['gain'] == 1.3
            pool.submit(backtest_result_index.invalidate_result, 'v8', older.parent).result(timeout=2)
        finally:
            release.set()
        assert pending.result(timeout=2)[0]['gain'] == 1.2


def test_concurrent_cold_reads_reuse_one_generation_build(tmp_path, monkeypatch):
    """A waiting reader rechecks the cache instead of rebuilding the same artifacts."""
    monkeypatch.setattr(backtest_result_index, 'PBGDIR', tmp_path)
    path = _result(tmp_path, 'native', 1.3)
    entered, release = threading.Event(), threading.Event()
    calls = []
    def slow(paths):
        """Count source reads while another caller waits on version ownership."""
        calls.append(list(paths)); entered.set()
        assert release.wait(5)
        return _builder([])(paths)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(backtest_result_index.load_indexed_results, 'v9', [path], slow)
        try:
            assert entered.wait(2)
            second = pool.submit(backtest_result_index.load_indexed_results, 'v9', [path], slow)
        finally:
            release.set()
        assert first.result(timeout=2) == second.result(timeout=2)
    assert len(calls) == 1


def test_interrupted_build_keeps_completed_chunks_and_retries_remainder(tmp_path, monkeypatch):
    """A source failure cannot discard previously committed summary progress."""
    monkeypatch.setattr(backtest_result_index, 'PBGDIR', tmp_path)
    monkeypatch.setattr(backtest_result_index, '_BUILD_CHUNK_SIZE', 2)
    paths = [_result(tmp_path, str(i), 1.2) for i in range(3)]
    def incomplete(chunk):
        """Fail the second source chunk without touching the first committed chunk."""
        if chunk == paths[2:]:
            raise ValueError('synthetic source failure')
        return _builder([])(chunk)
    with pytest.raises(ValueError, match='synthetic source failure'):
        backtest_result_index.load_indexed_results('v8', paths, incomplete)
    calls = []
    assert len(backtest_result_index.load_indexed_results('v8', paths, _builder(calls))) == 3
    assert calls == [['2']]


def test_changed_source_during_build_is_not_cached_or_returned(tmp_path, monkeypatch):
    """Metadata replacement during a source read cannot publish an obsolete summary."""
    monkeypatch.setattr(backtest_result_index, 'PBGDIR', tmp_path)
    path = _result(tmp_path, 'changed', 1.2)
    def replace_source(paths):
        """Replace the analysis after reading it, like concurrent result replacement."""
        rows = _builder([])(paths)
        replacement = path.with_suffix('.new')
        replacement.write_text('{"gain":1.8}')
        os.replace(replacement, path)
        backtest_result_index.invalidate_result('v9', path.parent)
        return rows
    assert backtest_result_index.load_indexed_results('v9', [path], replace_source) == []
    calls = []
    assert backtest_result_index.load_indexed_results('v9', [path], _builder(calls))[0]['gain'] == 1.8
    assert calls == [['changed']]


def test_cancelled_build_releases_ownership_and_keeps_previous_progress(tmp_path, monkeypatch):
    """Cancellation between chunks drains promptly and retains valid cached chunks."""
    monkeypatch.setattr(backtest_result_index, 'PBGDIR', tmp_path)
    monkeypatch.setattr(backtest_result_index, '_BUILD_CHUNK_SIZE', 1)
    paths = [_result(tmp_path, str(i), 1.2) for i in range(3)]
    cancelled = threading.Event()
    def check():
        """Use a caller-owned exception, without swallowing it as a cache failure."""
        if cancelled.is_set():
            raise RuntimeError('synthetic cancellation')
    def build(chunk):
        """Cancel the second build before its pending rows can be published."""
        rows = _builder([])(chunk)
        if chunk == paths[1:2]:
            cancelled.set()
        return rows
    with pytest.raises(RuntimeError, match='synthetic cancellation'):
        backtest_result_index.load_indexed_results('v9', paths, build, cancel_check=check)
    calls = []
    assert len(backtest_result_index.load_indexed_results('v9', paths, _builder(calls))) == 3
    assert calls == [['1'], ['2']]


@pytest.mark.parametrize('value', [float('nan'), float('inf'), -float('inf')])
def test_uncacheable_scalar_keeps_other_results_and_only_retries_that_row(tmp_path, monkeypatch, value):
    """Optional cache serialization must not fail the owner's whole result list."""
    monkeypatch.setattr(backtest_result_index, 'PBGDIR', tmp_path)
    notices = []
    monkeypatch.setattr(backtest_result_index, '_log', lambda *args, **kwargs: notices.append(args))
    paths = [_result(tmp_path, 'bad', 1.2), _result(tmp_path, 'good', 1.3)]
    calls = []
    def build(chunk):
        """Keep a malformed source scalar isolated from the valid cache row."""
        rows = _builder(calls)(chunk)
        for row in rows:
            if row['path'] == str(paths[0].parent):
                row['gain'] = value
        return rows
    rows = backtest_result_index.load_indexed_results('v8', paths, build)
    assert len(rows) == 2 and rows[1]['gain'] == 1.3
    assert notices
    calls.clear()
    assert len(backtest_result_index.load_indexed_results('v8', paths, build)) == 2
    assert calls == [['bad']]
    with sqlite3.connect(backtest_result_index._db_path()) as db:
        assert db.execute('SELECT result_path FROM result_summaries').fetchall() == [(str(paths[1].parent),)]


def test_cache_upsert_names_columns_and_preserves_extension_columns(tmp_path, monkeypatch):
    """A cache with additional optional metadata still accepts and reuses summaries."""
    monkeypatch.setattr(backtest_result_index, 'PBGDIR', tmp_path)
    connection = backtest_result_index._connect()
    with connection:
        connection.execute("ALTER TABLE result_summaries ADD COLUMN extension TEXT DEFAULT 'retained'")
    connection.close()
    path = _result(tmp_path, 'source', 1.2)
    calls = []
    for _ in range(2):
        assert backtest_result_index.load_indexed_results('v9', [path], _builder(calls))[0]['gain'] == 1.2
    assert calls == [['source']]
    with sqlite3.connect(backtest_result_index._db_path()) as db:
        assert db.execute('SELECT extension FROM result_summaries').fetchone()[0] == 'retained'


def test_pb9_envelope_projection_invalidates_only_native_cached_summaries(tmp_path, monkeypatch):
    """Old native summaries rebuild automatically while PB7/PB8 cached rows stay warm."""
    monkeypatch.setattr(backtest_result_index, 'PBGDIR', tmp_path)
    path = _result(tmp_path, 'native', 1.3)
    signature = backtest_result_index.result_signature(path)
    assert backtest_result_index.result_signature(path, version='v7') == signature
    assert backtest_result_index.result_signature(path, version='v8') == signature
    assert backtest_result_index.result_signature(path, version='v9') != signature
    connection = backtest_result_index._connect()
    with connection:
        for version in ('v7', 'v8', 'v9'):
            connection.execute('INSERT INTO result_summaries VALUES (?, ?, ?, ?, ?)',
                (version, str(path.parent), signature, 0, json.dumps({'path': str(path.parent), 'gain': 0})))
    connection.close()
    calls = []
    for version in ('v7', 'v8', 'v9'):
        rows = backtest_result_index.load_indexed_results(version, [path], _builder(calls))
        assert rows[0]['gain'] == (1.3 if version == 'v9' else 0)
    assert calls == [['native']]


@pytest.mark.parametrize('kind', ['file', 'directory', 'symlink', 'missing'])
def test_file_signature_uses_one_regular_file_identity(tmp_path, kind):
    """Signature checks retain the no-symlink boundary without repeated filesystem stats."""
    target = tmp_path / 'target'
    target.write_text('fixture')
    path = tmp_path / 'entry'
    if kind == 'file':
        path.write_text('result')
    elif kind == 'directory':
        path.mkdir()
    elif kind == 'symlink':
        path.symlink_to(target)
    signature = backtest_result_index._file_signature(path)
    assert bool(signature != (0, 0, 0)) is (kind == 'file')


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


@pytest.mark.parametrize('version', ['v7', 'v8'])
def test_result_resources_match_managed_ai_identity_on_every_read(tmp_path, monkeypatch, version):
    """Cold, warm and fallback summaries identify exact results without disclosing paths."""
    from ai_capabilities import AICapabilityService
    monkeypatch.setattr(backtest_result_index, 'PBGDIR', str(tmp_path))
    analysis = _result(tmp_path, 'same-display-name', 1.4)
    expected = AICapabilityService._virtual_uri('backtest', version, str(analysis.parent))
    builder = _builder([])
    for _ in range(2):
        rows = backtest_result_index.load_indexed_results(version, [analysis], builder)
        assert rows[0]['resource'] == expected
        assert str(tmp_path) not in rows[0]['resource']
    monkeypatch.setattr(backtest_result_index, '_connect', lambda: (_ for _ in ()).throw(sqlite3.OperationalError('fixture')))
    monkeypatch.setattr(backtest_result_index, '_log', lambda *args, **kwargs: None)
    assert backtest_result_index.load_indexed_results(version, [analysis], builder)[0]['resource'] == expected


def test_page_reads_only_its_index_rows(tmp_path, monkeypatch):
    """A small page must not deserialize payloads for every unrelated result."""
    monkeypatch.setattr(backtest_result_index, 'PBGDIR', str(tmp_path))
    paths = [_result(tmp_path, str(index), 1.1) for index in range(250)]
    builder = _builder([])
    backtest_result_index.load_indexed_results('v8', paths, builder)
    queries = []
    connect = backtest_result_index._connect

    def tracked_connection():
        """Observe actual SQLite statements while keeping all data isolated."""
        connection = connect()
        connection.set_trace_callback(queries.append)
        return connection

    monkeypatch.setattr(backtest_result_index, '_connect', tracked_connection)
    selected = backtest_result_index.load_indexed_results('v8', paths[:2], builder)
    assert len(selected) == 2
    payload_queries = [query for query in queries if query.startswith('SELECT result_path, signature, payload')]
    assert len(payload_queries) == 1 and 'result_path IN (' in payload_queries[0]
    assert str(paths[2].parent) not in payload_queries[0]


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


def test_old_index_rebuilds_group_metadata_after_schema_change(tmp_path: Path, monkeypatch) -> None:
    """Unchanged result files must gain grouping after upgrading a persisted index."""
    monkeypatch.setattr(backtest_result_index, 'PBGDIR', str(tmp_path))
    analysis = _result(tmp_path, 'backtests_full_timerange', 1.2)
    current_schema = backtest_result_index._SUMMARY_SCHEMA_VERSION
    monkeypatch.setattr(backtest_result_index, '_SUMMARY_SCHEMA_VERSION', 2)
    backtest_result_index.load_indexed_results('v8', [analysis], _builder([]))
    monkeypatch.setattr(backtest_result_index, '_SUMMARY_SCHEMA_VERSION', current_schema)
    calls = []

    def grouped(paths):
        """Simulate the current parser adding group metadata on a cache miss."""
        calls.append(paths)
        return [{'path':str(path.parent), 'result_group':{'kind':'optimize_validate','id':'same-candidate'}} for path in paths]

    result = backtest_result_index.load_indexed_results('v8', [analysis], grouped)
    assert result[0]['result_group']['id'] == 'same-candidate'
    assert backtest_result_index.load_indexed_results('v8', [analysis], grouped) == result
    assert len(calls) == 1
