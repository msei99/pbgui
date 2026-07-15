"""Focused tests for canonical INI snapshots and atomic transactions."""

from __future__ import annotations

import configparser
import multiprocessing
import os
from pathlib import Path
import stat
import threading
import time

import pytest

import pbgui_purefunc


def _process_write(path: str, key: str, value: str) -> None:
    """Write one key from a spawned process."""
    def mutate(parser: configparser.ConfigParser) -> None:
        if not parser.has_section("processes"):
            parser.add_section("processes")
        parser.set("processes", key, value)

    pbgui_purefunc.update_ini(mutate, path)


def test_snapshot_is_canonical_consistent_and_read_only(tmp_path: Path) -> None:
    """A snapshot identifies one generation and does not expose mutable state."""
    path = tmp_path / "nested" / ".." / "settings.ini"
    canonical = path.resolve()
    canonical.write_text("[main]\ncount = 7\n", encoding="utf-8")

    snapshot = pbgui_purefunc.load_ini_snapshot(path)
    exposed = snapshot.parser
    exposed.set("main", "count", "9")

    assert snapshot.path == canonical
    assert snapshot.signature.exists is True
    assert snapshot.signature.size == canonical.stat().st_size
    assert snapshot.signature.mtime_ns == canonical.stat().st_mtime_ns
    assert snapshot.signature.inode == getattr(canonical.stat(), "st_ino", None)
    assert snapshot.get_typed("main", "count", int) == 7


def test_snapshot_distinguishes_missing_invalid_and_parse_errors(tmp_path: Path) -> None:
    """Missing, invalid typed, and malformed INI values remain distinct."""
    missing = pbgui_purefunc.load_ini_snapshot(tmp_path / "missing.ini")
    assert missing.signature == pbgui_purefunc.IniSignature(False, None, None, None)
    with pytest.raises(pbgui_purefunc.IniMissingValueError):
        missing.get_typed("main", "count", int)

    path = tmp_path / "settings.ini"
    path.write_text("[main]\ncount = no\n", encoding="utf-8")
    with pytest.raises(pbgui_purefunc.IniInvalidValueError):
        pbgui_purefunc.load_ini_snapshot(path).get_typed("main", "count", int)
    path.write_text("not an ini", encoding="utf-8")
    with pytest.raises(configparser.Error):
        pbgui_purefunc.load_ini_snapshot(path)


def test_update_publishes_with_replace_and_private_permissions(tmp_path: Path, monkeypatch) -> None:
    """Transactions publish a new inode with owner-only permissions."""
    path = tmp_path / "settings.ini"
    path.write_text("[main]\nvalue = old\n", encoding="utf-8")
    old_inode = path.stat().st_ino
    replacements: list[tuple[object, object]] = []
    real_replace = os.replace

    def recording_replace(source, destination) -> None:
        replacements.append((source, destination))
        real_replace(source, destination)

    monkeypatch.setattr(os, "replace", recording_replace)
    pbgui_purefunc.update_ini(lambda parser: parser.set("main", "value", "new"), path)

    assert replacements
    assert Path(replacements[-1][1]).resolve() == path.resolve()
    assert path.stat().st_ino != old_inode
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_concurrent_threads_do_not_lose_different_keys(tmp_path: Path) -> None:
    """Thread transactions serialize independent key updates."""
    path = tmp_path / "settings.ini"

    def write(index: int) -> None:
        def mutate(parser: configparser.ConfigParser) -> None:
            if not parser.has_section("threads"):
                parser.add_section("threads")
            parser.set("threads", f"key_{index}", str(index))
        pbgui_purefunc.update_ini(mutate, path)

    threads = [threading.Thread(target=write, args=(index,)) for index in range(20)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    snapshot = pbgui_purefunc.load_ini_snapshot(path)
    assert {snapshot.get("threads", f"key_{index}") for index in range(20)} == {str(index) for index in range(20)}


def test_concurrent_processes_do_not_lose_different_keys(tmp_path: Path) -> None:
    """Process transactions preserve every independent key update."""
    path = tmp_path / "settings.ini"
    context = multiprocessing.get_context("spawn")
    processes = [context.Process(target=_process_write, args=(str(path), f"key_{index}", str(index))) for index in range(8)]
    for process in processes:
        process.start()
    for process in processes:
        process.join(15)
        assert process.exitcode == 0

    snapshot = pbgui_purefunc.load_ini_snapshot(path)
    assert {snapshot.get("processes", f"key_{index}") for index in range(8)} == {str(index) for index in range(8)}


def test_same_key_concurrency_is_valid_and_uncorrupted(tmp_path: Path) -> None:
    """Same-key races resolve to one complete writer value."""
    path = tmp_path / "settings.ini"
    values = [f"value-{index}-" + "x" * 200 for index in range(16)]
    threads = [threading.Thread(target=_process_write, args=(str(path), "shared", value)) for value in values]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert pbgui_purefunc.load_ini_snapshot(path).get("processes", "shared") in values


@pytest.mark.parametrize("failure", ["mutator", "writer"])
def test_transaction_failure_leaves_original_unchanged(tmp_path: Path, monkeypatch, failure: str) -> None:
    """Mutation and publication failures roll back without touching the original."""
    path = tmp_path / "settings.ini"
    original = b"[main]\nvalue = original\n"
    path.write_bytes(original)

    def mutate(parser: configparser.ConfigParser) -> None:
        parser.set("main", "value", "changed")
        if failure == "mutator":
            raise RuntimeError("mutation failed")

    if failure == "writer":
        monkeypatch.setattr(pbgui_purefunc, "atomic_write_private_text", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("write failed")))
    with pytest.raises((RuntimeError, OSError)):
        pbgui_purefunc.update_ini(mutate, path)
    assert path.read_bytes() == original


def test_batch_is_never_observed_partially(tmp_path: Path) -> None:
    """Unlocked readers see either complete old or complete new batches."""
    path = tmp_path / "settings.ini"
    pbgui_purefunc.update_ini(lambda parser: (parser.add_section("batch"), parser.set("batch", "left", "old"), parser.set("batch", "right", "old")), path)
    stop = threading.Event()
    observed: set[tuple[str, str]] = set()

    def read_loop() -> None:
        while not stop.is_set():
            snapshot = pbgui_purefunc.load_ini_snapshot(path)
            observed.add((snapshot.get("batch", "left"), snapshot.get("batch", "right")))

    reader = threading.Thread(target=read_loop)
    reader.start()
    try:
        for index in range(30):
            value = str(index)
            def mutate(parser: configparser.ConfigParser, value: str = value) -> None:
                parser.set("batch", "left", value)
                time.sleep(0.001)
                parser.set("batch", "right", value)
            pbgui_purefunc.update_ini(mutate, path)
    finally:
        stop.set()
        reader.join()

    assert observed
    assert all(left == right for left, right in observed)


def test_legacy_load_save_behavior_and_normalization(tmp_path: Path, monkeypatch) -> None:
    """Legacy wrappers retain empty-missing returns and path normalization."""
    path = tmp_path / "settings.ini"
    monkeypatch.setattr(pbgui_purefunc, "pbgui_ini_path", lambda: path)

    assert pbgui_purefunc.load_ini("main", "missing") == ""
    assert pbgui_purefunc.save_ini("main", "plain", 3) is None
    pbgui_purefunc.save_ini_section("main", {"other": 4, "pb7dir": "relative/path"})

    assert pbgui_purefunc.load_ini("main", "plain") == "3"
    assert pbgui_purefunc.load_ini_section("main")["other"] == "4"
    assert pbgui_purefunc.load_ini("main", "pb7dir") == str(Path("relative/path").resolve())
