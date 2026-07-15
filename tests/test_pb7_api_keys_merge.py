"""Tests for process-safe PB7 exchange and TradFi subtree ownership."""

from __future__ import annotations

import json
import multiprocessing
import os
from pathlib import Path
import stat

import pytest

from pb7_api_keys import PB7ApiKeysConflictError, PB7ApiKeysMergeWriter


def _write_exchange(path: str, status_path: str, start, queue) -> None:
    """Write one exchange snapshot in a child process."""

    try:
        start.wait(timeout=10)
        PB7ApiKeysMergeWriter(path, status_path).write_exchange_payload(
            {"_api_serial": 1, "alice": {"exchange": "binance", "secret": "exchange-secret"}},
            expected_generation=0,
        )
        queue.put(None)
    except Exception as exc:  # pragma: no cover - surfaced by the parent assertion
        queue.put(repr(exc))


def _write_projection(path: str, status_path: str, start, queue) -> None:
    """Write one TradFi projection in a child process."""

    try:
        start.wait(timeout=10)
        PB7ApiKeysMergeWriter(path, status_path).project_tradfi(
            {"provider": "tiingo", "api_key": "tradfi-secret", "profiles": {}},
            source_fingerprint="a" * 64,
        )
        queue.put(None)
    except Exception as exc:  # pragma: no cover - surfaced by the parent assertion
        queue.put(repr(exc))


def test_concurrent_exchange_and_projection_merge_preserves_both_owners(tmp_path: Path) -> None:
    """Concurrent process writers retain both the exchange and TradFi owners."""

    path = tmp_path / "pb7" / "api-keys.json"
    path.parent.mkdir()
    status_path = tmp_path / "credentials" / "projection.json"
    context = multiprocessing.get_context("spawn")
    start = context.Event()
    queue = context.Queue()
    processes = [
        context.Process(target=_write_exchange, args=(str(path), str(status_path), start, queue)),
        context.Process(target=_write_projection, args=(str(path), str(status_path), start, queue)),
    ]
    for process in processes:
        process.start()
    start.set()
    errors = [queue.get(timeout=20) for _ in processes]
    for process in processes:
        process.join(timeout=20)
        assert process.exitcode == 0

    assert errors == [None, None]
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["alice"]["secret"] == "exchange-secret"
    assert payload["tradfi"]["api_key"] == "tradfi-secret"
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_exchange_cas_and_projection_retry_state(tmp_path: Path, monkeypatch) -> None:
    """Stale exchange CAS fails and a failed projection remains retryable."""

    path = tmp_path / "pb7" / "api-keys.json"
    path.parent.mkdir()
    status_path = tmp_path / "credentials" / "projection.json"
    writer = PB7ApiKeysMergeWriter(path, status_path)
    writer.write_exchange_payload({"_api_serial": 1, "alice": {"exchange": "okx"}})

    with pytest.raises(PB7ApiKeysConflictError):
        writer.write_exchange_payload(
            {"_api_serial": 2, "bob": {"exchange": "bybit"}},
            expected_generation=0,
        )

    original_write = writer._write_api_keys_unlocked
    calls = 0

    def fail_once(payload: dict) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("simulated projection failure")
        original_write(payload)

    monkeypatch.setattr(writer, "_write_api_keys_unlocked", fail_once)
    with pytest.raises(OSError, match="simulated projection failure"):
        writer.project_tradfi(
            {"provider": "tiingo", "api_key": "retry-secret"},
            source_fingerprint="b" * 64,
        )

    failed = writer.projection_status()
    assert failed["status"] == "error"
    assert failed["desired_generation"] == 1
    assert "retry-secret" not in status_path.read_text(encoding="utf-8")

    current = writer.project_tradfi(
        {"provider": "tiingo", "api_key": "retry-secret"},
        source_fingerprint="b" * 64,
    )
    assert current["status"] == "current"
    assert current["desired_generation"] == 1
    assert current["attempts"] == 2
    assert json.loads(path.read_text(encoding="utf-8"))["tradfi"]["api_key"] == "retry-secret"
    if os.name == "posix":
        assert stat.S_IMODE(status_path.stat().st_mode) == 0o600


def test_writer_rejects_symlinked_pb7_root_without_touching_target(tmp_path: Path) -> None:
    """A configured PB7 root symlink cannot redirect reads or writes outside the lexical root."""

    outside = tmp_path / "outside"
    outside.mkdir()
    linked = tmp_path / "pb7"
    linked.symlink_to(outside, target_is_directory=True)

    with pytest.raises(RuntimeError, match="symlink"):
        PB7ApiKeysMergeWriter(linked / "api-keys.json", tmp_path / "status.json")
    assert list(outside.iterdir()) == []


def test_writer_revalidates_target_symlink_before_every_access(tmp_path: Path) -> None:
    """A target replaced by a symlink after construction is rejected before reading."""

    pb7 = tmp_path / "pb7"
    pb7.mkdir()
    path = pb7 / "api-keys.json"
    outside = tmp_path / "outside.json"
    outside.write_text('{"secret":"outside"}', encoding="utf-8")
    writer = PB7ApiKeysMergeWriter(path, tmp_path / "status.json")
    path.symlink_to(outside)

    with pytest.raises(RuntimeError, match="symlink"):
        writer.read()
    assert json.loads(outside.read_text(encoding="utf-8")) == {"secret": "outside"}


def test_restore_merges_exchange_owner_and_reprojects_vault(tmp_path: Path) -> None:
    """Backup TradFi is ignored while exchange content and vault projection are restored together."""

    from credential_store import CredentialStore

    pb7 = tmp_path / "pb7"
    pb7.mkdir()
    path = pb7 / "api-keys.json"
    path.write_text('{"old":{"exchange":"okx"},"tradfi":{"api_key":"old-runtime"}}', encoding="utf-8")
    store = CredentialStore(tmp_path / "credentials")
    profile = store.create_tradfi("tiingo", {"api_key": "vault-token"})
    writer = PB7ApiKeysMergeWriter(path, store.root / "projection.json")

    result = writer.restore_exchange_and_project(
        {
            "_api_serial": 7,
            "alice": {"exchange": "binance", "secret": "exchange-secret"},
            "tradfi": {"api_key": "backup-must-not-win"},
        },
        store,
        backup_path=tmp_path / "pre-restore.json",
    )

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["alice"]["secret"] == "exchange-secret"
    assert payload["tradfi"]["active_profile_id"] == profile["id"]
    assert payload["tradfi"]["api_key"] == "vault-token"
    assert "backup-must-not-win" not in json.dumps(payload)
    assert result["projection"]["status"] == "current"
