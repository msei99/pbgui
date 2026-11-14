"""Read-only v9 file and shared-loader dispatch tests, using temporary files only."""

import builtins
import copy
from dataclasses import replace
import json
import os
from pathlib import Path
import stat
import traceback
from types import SimpleNamespace

import pytest

import pb7_config
from pb9_config import PB9ConfigError
import pb9_config_storage as storage
import secure_files


@pytest.fixture
def library(tmp_path, monkeypatch):
    """Provide a temporary approved library with a recording, in-memory validator."""
    root = tmp_path / "documents"
    root.mkdir()
    context = storage.PB9FileContext(root, tmp_path / "pb9", tmp_path / "env" / "bin" / "python")
    calls = []

    def validate(document, pb9dir, pb9venv, other_slots, *, timeout):
        """Record exactly what the parser hands to the established document validator."""
        calls.append((copy.deepcopy(document), pb9dir, pb9venv, other_slots, timeout))
        config = copy.deepcopy(document)
        metadata = config.pop("pbgui", None)
        return {"config": config, "pbgui": metadata, "source": {"commit": "fixture-inspected-source"}}

    monkeypatch.setattr(storage, "validate_pb9_document", validate)
    return SimpleNamespace(root=root, context=context, calls=calls)


@pytest.mark.parametrize("metadata", [None, {}, {"name": "Änderung", "source": {"commit": "authored-note"}}])
def test_explicit_v9_load_preserves_sparse_source_and_gui_metadata(library, monkeypatch, metadata):
    """The common loader selects v9 without PB7 preparation, cleanup or writes."""
    document = {"config_version": "v9.0.0", "bot": {"long": {}, "short": {}}, "optimize": {"bounds": {}}}
    if metadata is not None:
        document["pbgui"] = metadata
    path = library.root / "example.json"
    path.write_text(json.dumps(document, ensure_ascii=False, indent=4), encoding="utf-8")
    before = path.read_bytes()

    def forbidden(*args, **kwargs):
        """A selected v9 source must never enter the PB7 pipeline."""
        pytest.fail("v9 load entered PB7")

    monkeypatch.setattr(pb7_config, "_get_pb7_config_api", forbidden)
    monkeypatch.setattr(pb7_config, "_strip_legacy_unsupported_optimize_bounds", forbidden)
    result = pb7_config.load_pb7_config(Path("example.json"), pb9_context=library.context)
    assert result == document
    assert result is not document
    assert path.read_bytes() == before
    assert list(library.root.iterdir()) == [path]
    assert library.calls == [(document, library.context.pb9dir, library.context.pb9venv, None, 60)]


@pytest.mark.parametrize("name", ["../example.json", "sub/example.json", "sub\\example.json", ".hidden.json",
    "example.json\x00", "bad\n.json", "example.json\x7f", "", "..", "example.txt", " example.json", "C:example.json", None])
def test_invalid_names_are_rejected_before_open(library, monkeypatch, name):
    """Only a JSON basename can cross the file boundary."""
    def forbidden(*args, **kwargs):
        """No path opening is allowed for an invalid name."""
        pytest.fail("invalid name opened a file")

    monkeypatch.setattr(storage.os, "open", forbidden)
    with pytest.raises(PB9ConfigError) as caught:
        pb7_config.load_pb7_config(name, pb9_context=library.context)
    assert caught.value.error_codes == ("invalid_config_name",)
    assert not library.calls


@pytest.mark.parametrize("root", ["", " ", ".", Path(""), "relative", "/tmp/../tmp", "/tmp/bad\x00",
                                  "~", "~/documents", None])
def test_invalid_roots_are_rejected_before_open(library, monkeypatch, root):
    """An unset or relative root cannot fall back to the working directory."""
    def forbidden(*args, **kwargs):
        """Context rejection must precede opening root descriptors."""
        pytest.fail("invalid root opened a file")

    monkeypatch.setattr(storage.os, "open", forbidden)
    with pytest.raises(PB9ConfigError) as caught:
        pb7_config.load_pb7_config("example.json", pb9_context=replace(library.context, approved_root=root))
    assert caught.value.error_codes == ("invalid_storage_context",)
    assert not library.calls


@pytest.mark.parametrize("context", [True, {}, "not a context"])
def test_invalid_context_type_has_a_fixed_error(context):
    """Wrong explicit context types never enter the legacy path."""
    with pytest.raises(PB9ConfigError) as caught:
        pb7_config.load_pb7_config("example.json", pb9_context=context)
    assert caught.value.error_codes == ("invalid_storage_context",)


@pytest.mark.parametrize("option", ["verbose", "neutralize_added"])
def test_pb7_only_options_are_rejected_in_v9_mode(library, option):
    """Legacy transformations cannot silently alter a v9 authored document."""
    with pytest.raises(PB9ConfigError) as caught:
        pb7_config.load_pb7_config("example.json", pb9_context=library.context, **{option: True})
    assert caught.value.error_codes == ("invalid_request",)
    assert not library.calls


@pytest.mark.parametrize("kind", ["file", "root", "ancestor", "fifo", "directory", "missing"])
def test_nonregular_or_symlink_sources_never_reach_validator(library, tmp_path, kind):
    """No-follow descriptor traversal rejects root/file symlinks and FIFOs safely."""
    context = library.context
    path = library.root / "example.json"
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "example.json").write_text('{"config_version":"v9.0.0"}', encoding="utf-8")
    if kind == "file":
        path.symlink_to(outside / "example.json")
    elif kind == "root":
        link = tmp_path / "root-link"
        link.symlink_to(outside, target_is_directory=True)
        context = replace(context, approved_root=link)
    elif kind == "ancestor":
        link = tmp_path / "ancestor-link"
        link.symlink_to(tmp_path, target_is_directory=True)
        context = replace(context, approved_root=link / "outside")
    elif kind == "fifo":
        os.mkfifo(path)
    elif kind == "directory":
        path.mkdir()
    with pytest.raises(PB9ConfigError) as caught:
        pb7_config.load_pb7_config("example.json", pb9_context=context)
    assert caught.value.error_codes == ("config_file_unavailable",)
    assert not library.calls


@pytest.mark.parametrize("hide_initial_size", [False, True])
def test_file_limit_applies_even_if_size_changes_after_stat(library, monkeypatch, hide_initial_size):
    """Both initial size checks and bounded reads stop oversized file contents."""
    (library.root / "example.json").write_bytes(b" " * (storage.MAX_FILE_BYTES + 1))
    if hide_initial_size:
        monkeypatch.setattr(storage.os, "fstat", lambda _fd: SimpleNamespace(st_mode=stat.S_IFREG, st_size=0))
    with pytest.raises(PB9ConfigError) as caught:
        pb7_config.load_pb7_config("example.json", pb9_context=library.context)
    assert caught.value.error_codes == ("config_file_too_large",)
    assert not library.calls


@pytest.mark.parametrize("data", [b'{"config_version":"v9.0.0","config_version":"v9.1.0"}',
    b'{"pbgui":{"name":"a","name":"b"}}', b'{"value":NaN}', b'{"value":Infinity}',
    b'{"value":1e999}', b'{"value":-1e999}', b'{broken', b'[]', b'null', b'{"bad":"\xff"}'])
def test_invalid_file_json_is_rejected_before_validator(library, data):
    """Duplicate members and malformed/nonfinite JSON never gain native meaning."""
    (library.root / "example.json").write_bytes(data)
    with pytest.raises(PB9ConfigError) as caught:
        pb7_config.load_pb7_config("example.json", pb9_context=library.context)
    assert caught.value.error_codes == ("invalid_document",)
    assert not library.calls


def test_native_validation_failure_is_not_repaired_or_written(library, monkeypatch):
    """Runtime rejection is propagated without fallback, mutation or persistence."""
    path = library.root / "example.json"
    path.write_text('{"config_version":"v8.6.0"}', encoding="utf-8")
    before = path.read_bytes()

    def reject(*args, **kwargs):
        """Represent the existing validator's schema refusal."""
        raise PB9ConfigError("config_version_mismatch")

    monkeypatch.setattr(storage, "validate_pb9_document", reject)
    with pytest.raises(PB9ConfigError) as caught:
        pb7_config.load_pb7_config("example.json", pb9_context=library.context)
    assert caught.value.error_codes == ("config_version_mismatch",)
    assert path.read_bytes() == before


@pytest.mark.parametrize("malformed", [False, True])
def test_rejection_logs_do_not_expose_path_or_document(library, monkeypatch, malformed):
    """File/parse failure diagnostics contain codes rather than private input text."""
    path = library.root / "private-name.json"
    if malformed:
        path.write_text('{"fixture-private-value":', encoding="utf-8")
    events = []
    monkeypatch.setattr(storage, "_log", lambda *args, **kwargs: events.append((args, kwargs)))
    with pytest.raises(PB9ConfigError) as caught:
        pb7_config.load_pb7_config(path.name, pb9_context=library.context)
    displayed = str(caught.value) + repr(events) + "".join(traceback.format_exception(caught.value))
    for private in (str(library.root), path.name, "fixture-private-value"):
        assert private not in displayed
    code = "invalid_document" if malformed else "config_file_unavailable"
    assert events[0][1]["meta"] == {"error_codes": [code]}
    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None


@pytest.mark.parametrize("fail_one_close", [False, True])
def test_failed_reads_release_every_owned_descriptor(library, monkeypatch, fail_one_close):
    """Descriptor cleanup continues even if one close reports an error."""
    if not Path("/proc/self/fd").is_dir():
        pytest.skip("Descriptor-count check requires Linux procfs")
    (library.root / "example.json").write_bytes(b"malformed JSON")
    monkeypatch.setattr(storage, "_log", lambda *args, **kwargs: None)
    actual_close = os.close
    closed = []

    def close_with_one_error(descriptor):
        """Model an error reported after the descriptor has already been released."""
        actual_close(descriptor)
        closed.append(descriptor)
        if len(closed) == 1:
            raise OSError("fixture-only-close-error")

    if fail_one_close:
        monkeypatch.setattr(storage.os, "close", close_with_one_error)
    before = len(os.listdir("/proc/self/fd"))
    with pytest.raises(PB9ConfigError) as caught:
        pb7_config.load_pb7_config("example.json", pb9_context=library.context)
    assert len(os.listdir("/proc/self/fd")) == before
    expected = "config_file_unavailable" if fail_one_close else "invalid_document"
    assert caught.value.error_codes == (expected,)
    assert caught.value.__context__ is None
    assert not library.calls


def test_legacy_loader_does_not_import_v9_storage(tmp_path, monkeypatch):
    """Default calls retain the PB7 pipeline without touching the v9 dispatch."""
    document = {"config_version": "v7.12.0", "bot": {"long": {}, "short": {}}, "pbgui": {"name": "Legacy"}}
    path = tmp_path / "legacy.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    calls = []

    def load(source, **kwargs):
        """Record use of the legacy native loader without a real bot checkout."""
        calls.append((source, kwargs))
        return copy.deepcopy(document)

    monkeypatch.setattr(pb7_config, "_get_pb7_config_api", lambda: (load, copy.deepcopy))
    original_import = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        """Fail if a default invocation evaluates the lazy v9 import."""
        if name == "pb9_config_storage":
            pytest.fail("legacy path imported v9 storage")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    assert pb7_config.load_pb7_config(path) == document
    assert calls == [(str(path), {"verbose": False, "log_info": False})]


@pytest.fixture
def private_library(library):
    """Provision only the temporary config root with the required private mode."""
    library.root.chmod(0o700)
    return library


@pytest.mark.parametrize("metadata", [None, {}, {"name": "Änderung", "source": {"note": "keep"}}])
def test_create_v9_source_preserves_values_and_private_permissions(private_library, monkeypatch, metadata):
    """The shared save entry creates one complete sparse source without PB7 cleanup."""
    library = private_library
    document = {"config_version": "v9.0.0", "bot": {"long": {}, "short": {}}, "optimize": {"bounds": {}}}
    if metadata is not None:
        document["pbgui"] = metadata
    before = copy.deepcopy(document)

    def forbidden(*args, **kwargs):
        """Explicit v9 saves cannot strip PB7 bounds or call its native pipeline."""
        pytest.fail("v9 creation entered PB7")

    monkeypatch.setattr(pb7_config, "_get_pb7_config_api", forbidden)
    monkeypatch.setattr(pb7_config, "_strip_legacy_unsupported_optimize_bounds", forbidden)
    assert pb7_config.save_pb7_config(document, "example.json", pb9_context=library.context) is None
    target = library.root / "example.json"
    saved = target.read_bytes()
    assert saved.endswith(b"\n")
    assert b'    "config_version"' in saved
    assert json.loads(saved) == before
    assert pb7_config.load_pb7_config(target.name, pb9_context=library.context) == before
    assert "source" not in json.loads(saved)
    assert document == before
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    assert list(library.root.iterdir()) == [target]


@pytest.mark.parametrize("kind", ["regular", "symlink", "dangling", "fifo", "directory"])
def test_create_rejects_existing_entries_before_opening_temporary(private_library, tmp_path, monkeypatch, kind):
    """Existing targets are neither followed, replaced nor temporarily modified."""
    library = private_library
    target = library.root / "example.json"
    outside = tmp_path / "outside.json"
    outside.write_bytes(b"private outside original")
    if kind == "regular":
        target.write_bytes(b"original")
    elif kind == "symlink":
        target.symlink_to(outside)
    elif kind == "dangling":
        target.symlink_to(tmp_path / "missing.json")
    elif kind == "fifo":
        os.mkfifo(target)
    else:
        target.mkdir()
    before_inode = target.lstat().st_ino

    def forbidden(*args, **kwargs):
        """The existing-target check should reject before creating a temporary."""
        pytest.fail("existing target reached private creator")

    monkeypatch.setattr(storage, "create_private_file_at", forbidden)
    with pytest.raises(PB9ConfigError) as caught:
        pb7_config.save_pb7_config({"config_version": "v9.0.0"}, target.name, pb9_context=library.context)
    assert caught.value.error_codes == ("config_file_exists",)
    assert target.lstat().st_ino == before_inode
    assert list(library.root.iterdir()) == [target]
    assert outside.read_bytes() == b"private outside original"
    if kind == "regular":
        assert target.read_bytes() == b"original"


@pytest.mark.parametrize("failure", ["validation", "formatted_size", "nested_metadata_size"])
def test_creation_rejects_invalid_source_before_library_open(library, monkeypatch, failure):
    """Native rejection or excessive formatted output cannot start filesystem publication."""
    def forbidden(*args, **kwargs):
        """No library descriptor can be opened before validation and output limits pass."""
        pytest.fail("rejected source opened the library")

    document = {"config_version": "v9.0.0"}
    expected = "native_config_rejected"
    if failure == "validation":
        def reject(*args, **kwargs):
            """Represent the established native validator rejecting this source."""
            raise PB9ConfigError("native_config_rejected")

        monkeypatch.setattr(storage, "validate_pb9_document", reject)
    elif failure == "formatted_size":
        document["bot"] = {"global": {"universe": {"approved_coins": ["BTC"] * 100000}}}
        assert len(json.dumps(document)) < storage.MAX_DOCUMENT_BYTES
        expected = "config_file_too_large"
    else:
        node = document["pbgui"] = {}
        for _ in range(100):
            node["nested"] = {}
            node = node["nested"]
        node["notes"] = ["small"] * 5000
        assert len(json.dumps(document)) < storage.MAX_DOCUMENT_BYTES
        expected = "config_file_too_large"
    monkeypatch.setattr(storage, "_log", lambda *args, **kwargs: None)
    monkeypatch.setattr(storage.os, "open", forbidden)
    with pytest.raises(PB9ConfigError) as caught:
        pb7_config.save_pb7_config(document, "example.json", pb9_context=library.context)
    assert caught.value.error_codes == (expected,)


@pytest.mark.parametrize("kind", ["missing", "symlink", "ancestor", "shared_mode"])
def test_creation_requires_an_existing_private_nofollow_root(library, tmp_path, kind):
    """Creating a config cannot provision, chmod or follow an unapproved root."""
    context = library.context
    outside = tmp_path / "outside"
    outside.mkdir(mode=0o700)
    outside.chmod(0o700)
    if kind == "missing":
        context = replace(context, approved_root=tmp_path / "missing")
    elif kind in {"symlink", "ancestor"}:
        link = tmp_path / "root-link"
        link.symlink_to(outside if kind == "symlink" else tmp_path, target_is_directory=True)
        context = replace(context, approved_root=link if kind == "symlink" else link / "outside")
    else:
        library.root.chmod(0o755)
    before_mode = library.root.stat().st_mode
    with pytest.raises(PB9ConfigError) as caught:
        pb7_config.save_pb7_config({"config_version": "v9.0.0"}, "example.json", pb9_context=context)
    expected = "config_directory_not_private" if kind == "shared_mode" else "config_file_unavailable"
    assert caught.value.error_codes == (expected,)
    assert not list(library.root.iterdir())
    assert not list(outside.iterdir())
    assert library.root.stat().st_mode == before_mode
    assert not (tmp_path / "missing").exists()


def test_concurrent_target_arrival_is_a_conflict_without_overwrite(private_library, monkeypatch):
    """The kernel exclusive bind remains authoritative after the preliminary stat."""
    library = private_library
    actual_create = storage.create_private_file_at

    def competing_create(directory_fd, name, content):
        """Insert another complete file between the precheck and atomic publication."""
        descriptor = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=directory_fd)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(b"another complete candidate")
        return actual_create(directory_fd, name, content)

    monkeypatch.setattr(storage, "create_private_file_at", competing_create)
    with pytest.raises(PB9ConfigError) as caught:
        pb7_config.save_pb7_config({"config_version": "v9.0.0"}, "example.json", pb9_context=library.context)
    assert caught.value.error_codes == ("config_file_exists",)
    assert (library.root / "example.json").read_bytes() == b"another complete candidate"
    assert list(library.root.iterdir()) == [library.root / "example.json"]


def test_publication_acknowledgement_failure_has_a_distinct_error(private_library, monkeypatch):
    """A committed file is preserved and reported honestly when directory sync fails."""
    library = private_library
    actual_sync = os.fsync
    events = []
    monkeypatch.setattr(storage, "_log", lambda *args, **kwargs: events.append((args, kwargs)))

    def fail_directory_sync(descriptor):
        """Reject directory sync after the file data was fully synced and published."""
        if stat.S_ISDIR(os.fstat(descriptor).st_mode):
            raise OSError("fixture-private-directory-sync-failure")
        return actual_sync(descriptor)

    monkeypatch.setattr(secure_files.os, "fsync", fail_directory_sync)
    with pytest.raises(PB9ConfigError) as caught:
        pb7_config.save_pb7_config({"config_version": "v9.0.0"}, "example.json", pb9_context=library.context)
    assert caught.value.error_codes == ("config_file_created_io_failed",)
    assert caught.value.__context__ is None
    assert events[-1][0][1] == "PB9 config created; I/O acknowledgement failed"
    assert json.loads((library.root / "example.json").read_bytes()) == {"config_version": "v9.0.0"}
    assert list(library.root.iterdir()) == [library.root / "example.json"]


def test_root_close_failure_after_creation_preserves_publication_state(private_library, monkeypatch):
    """A root cleanup error after the commit point cannot be reported as rejected."""
    library = private_library
    actual_close = os.close
    failed = []
    events = []
    monkeypatch.setattr(storage, "_log", lambda *args, **kwargs: events.append((args, kwargs)))

    def close_directory_with_one_error(descriptor):
        """Release each FD, but fail the first directory close after publication."""
        directory = stat.S_ISDIR(os.fstat(descriptor).st_mode)
        actual_close(descriptor)
        if directory and not failed:
            failed.append(True)
            raise OSError("fixture-private-root-close-error")

    with monkeypatch.context() as patch:
        patch.setattr(storage.os, "close", close_directory_with_one_error)
        with pytest.raises(PB9ConfigError) as caught:
            pb7_config.save_pb7_config({"config_version": "v9.0.0"}, "example.json", pb9_context=library.context)
    assert caught.value.error_codes == ("config_file_created_io_failed",)
    assert caught.value.__context__ is None
    assert events[-1][0][1] == "PB9 config created; I/O acknowledgement failed"
    assert json.loads((library.root / "example.json").read_bytes()) == {"config_version": "v9.0.0"}
    assert list(library.root.iterdir()) == [library.root / "example.json"]


def test_legacy_save_output_and_lazy_import_remain_unchanged(tmp_path, monkeypatch):
    """Default save keeps its exact format and cleanup without importing v9 code."""
    document = {"config_version": "v7.12.0", "bot": {"long": {}, "short": {}},
                "optimize": {"bounds": {"long_close_grid_markup_range": [0, 1], "kept": [1, 2]}},
                "pbgui": {"name": "Legacy"}, "_pbgui_param_status": {"long": {}}}
    before = copy.deepcopy(document)
    expected = copy.deepcopy(document)
    expected.pop("_pbgui_param_status")
    expected["optimize"]["bounds"].pop("long_close_grid_markup_range")
    original_import = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        """Fail if a legacy save evaluates the new lazy dispatch import."""
        if name == "pb9_config_storage":
            pytest.fail("legacy save imported v9 storage")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    destination = tmp_path / "legacy" / "example.json"
    assert pb7_config.save_pb7_config(document, destination) is None
    assert destination.read_text() == json.dumps(expected, indent=4) + "\n"
    assert document == before
