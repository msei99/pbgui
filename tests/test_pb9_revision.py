"""PB9 revision contracts with temporary source files and an in-memory validator."""

import copy
from dataclasses import replace
import hashlib
import json
import os
import threading

import pytest

import pb7_config
from pb9_config import PB9ConfigError
import pb9_config_storage as storage
from tests.test_pb9_config_storage import library, private_library


def _document(name="Original"):
    """Return a sparse source with metadata deliberately separate from revision state."""
    return {"config_version": "v9.0.0", "bot": {"long": {}, "short": {}},
            "optimize": {"bounds": {}}, "pbgui": {"name": name}}


def test_loaded_revision_matches_exact_noncanonical_bytes(private_library):
    """Whitespace and key order belong to the read revision, not a reconstructed dump."""
    library = private_library
    data = b'{ "pbgui": {"name":"Original"}, "config_version": "v9.0.0", "bot":{"long":{},"short":{}}, "optimize":{"bounds":{}} }\n\n'
    path = library.root / "example.json"
    path.write_bytes(data)
    loaded = pb7_config.load_pb7_config(path.name, pb9_context=library.context, pb9_with_revision=True)
    assert set(loaded) == {"config", "revision"}
    assert loaded["config"] == _document()
    assert loaded["revision"] == hashlib.sha256(data).hexdigest()
    assert loaded["revision"] != hashlib.sha256(json.dumps(loaded["config"]).encode()).hexdigest()
    assert pb7_config.load_pb7_config(path.name, pb9_context=library.context) == _document()


def test_update_matching_revision_preserves_values_and_changes_revision(private_library):
    """An explicit expected revision replaces only that exact loaded snapshot."""
    library = private_library
    pb7_config.save_pb7_config(_document(), "example.json", pb9_context=library.context)
    loaded = pb7_config.load_pb7_config("example.json", pb9_context=library.context, pb9_with_revision=True)
    updated = _document("Edited")
    before = copy.deepcopy(updated)
    assert pb7_config.save_pb7_config(updated, "example.json", pb9_context=library.context,
                                     pb9_expected_revision=loaded["revision"]) is None
    result = pb7_config.load_pb7_config("example.json", pb9_context=library.context, pb9_with_revision=True)
    assert result["config"] == before
    assert result["revision"] != loaded["revision"]
    assert result["revision"] == hashlib.sha256((library.root / "example.json").read_bytes()).hexdigest()
    assert updated == before
    assert (library.root / "example.json").stat().st_mode & 0o777 == 0o600
    assert list(library.root.iterdir()) == [library.root / "example.json"]
    assert "revision" not in result["config"]
    with pytest.raises(PB9ConfigError) as caught:
        pb7_config.save_pb7_config(_document("No overwrite"), "example.json", pb9_context=library.context)
    assert caught.value.error_codes == ("config_file_exists",)


def test_revision_remains_bound_to_bytes_read_before_validation(private_library, monkeypatch):
    """A later file change cannot relabel the config snapshot returned by this read."""
    library = private_library
    path = library.root / "example.json"
    before = json.dumps(_document()).encode()
    after = json.dumps(_document("Changed during read")).encode()
    path.write_bytes(before)
    actual_validate = storage.validate_pb9_document

    def change_during_validation(*args, **kwargs):
        """Model another completed save while this snapshot is being validated."""
        path.write_bytes(after)
        return actual_validate(*args, **kwargs)

    monkeypatch.setattr(storage, "validate_pb9_document", change_during_validation)
    loaded = pb7_config.load_pb7_config(path.name, pb9_context=library.context, pb9_with_revision=True)
    assert loaded == {"config": _document(), "revision": hashlib.sha256(before).hexdigest()}
    assert hashlib.sha256(path.read_bytes()).hexdigest() != loaded["revision"]


@pytest.mark.parametrize("missing", [False, True])
def test_stale_or_missing_target_conflicts_before_native_work(private_library, monkeypatch, missing):
    """The fast check rejects stale data without a subprocess or an implicit create."""
    library = private_library
    path = library.root / "example.json"
    before = json.dumps(_document()).encode()
    if not missing:
        path.write_bytes(before)

    def forbidden(*args, **kwargs):
        """A known revision conflict must precede native validation."""
        pytest.fail("stale revision reached native work")

    monkeypatch.setattr(storage, "validate_pb9_document", forbidden)
    with pytest.raises(PB9ConfigError) as caught:
        pb7_config.save_pb7_config(_document("Edited"), path.name, pb9_context=library.context,
                                  pb9_expected_revision="0" * 64)
    assert caught.value.error_codes == ("config_revision_conflict",)
    assert caught.value.__context__ is None
    if missing:
        assert not path.exists()
    else:
        assert path.read_bytes() == before


@pytest.mark.parametrize("kind", ["symlink", "fifo", "shared_mode", "missing_root"])
def test_update_rejects_unsafe_or_missing_library_targets(private_library, tmp_path, kind):
    """Unsafe targets and unavailable roots remain distinct from stale file revisions."""
    library = private_library
    path = library.root / "example.json"
    before = json.dumps(_document()).encode()
    outside = tmp_path / "outside.json"
    outside.write_bytes(before)
    context = library.context
    if kind == "symlink":
        path.symlink_to(outside)
    elif kind == "fifo":
        os.mkfifo(path)
    else:
        path.write_bytes(before)
        if kind == "shared_mode":
            library.root.chmod(0o755)
        else:
            context = replace(context, approved_root=library.root / "missing")
    with pytest.raises(PB9ConfigError) as caught:
        pb7_config.save_pb7_config(_document("Edited"), path.name, pb9_context=context,
                                  pb9_expected_revision=hashlib.sha256(before).hexdigest())
    expected = "config_directory_not_private" if kind == "shared_mode" else "config_file_unavailable"
    assert caught.value.error_codes == (expected,)
    assert caught.value.__context__ is None
    assert outside.read_bytes() == before
    if kind not in {"symlink", "fifo"}:
        assert path.read_bytes() == before
    if kind != "shared_mode":
        assert not library.calls


@pytest.mark.parametrize("revision", ["", "a" * 63, "a" * 65, "A" * 64, "g" * 64, True, 1, []])
def test_invalid_revision_has_no_file_or_native_access(private_library, monkeypatch, revision):
    """Only an explicit lowercase SHA-256 token can select an update."""
    library = private_library

    def forbidden(*args, **kwargs):
        """Malformed revisions must be rejected before library or runtime work."""
        pytest.fail("invalid revision crossed the boundary")

    monkeypatch.setattr(storage, "_log", lambda *args, **kwargs: None)
    monkeypatch.setattr(storage.os, "open", forbidden)
    monkeypatch.setattr(storage, "validate_pb9_document", forbidden)
    with pytest.raises(PB9ConfigError) as caught:
        pb7_config.save_pb7_config(_document(), "example.json", pb9_context=library.context,
                                  pb9_expected_revision=revision)
    assert caught.value.error_codes == ("invalid_revision",)


@pytest.mark.parametrize("operation", ["load", "save"])
def test_revision_flags_without_context_never_fall_back_to_pb7(monkeypatch, operation):
    """A v9-only request cannot accidentally run a legacy pipeline."""
    def forbidden(*args, **kwargs):
        """No legacy pipeline or filesystem open may follow the bad request."""
        pytest.fail("revision flag without context fell back")

    monkeypatch.setattr(storage, "_log", lambda *args, **kwargs: None)
    monkeypatch.setattr(storage.os, "open", forbidden)
    monkeypatch.setattr(pb7_config, "_get_pb7_config_api", forbidden)
    with pytest.raises(PB9ConfigError) as caught:
        if operation == "load":
            pb7_config.load_pb7_config("example.json", pb9_with_revision=True)
        else:
            pb7_config.save_pb7_config(_document(), "example.json", pb9_expected_revision="0" * 64)
    assert caught.value.error_codes == ("invalid_request",)


@pytest.mark.parametrize("flag", [None, 0, 1, "true"])
def test_revision_read_flag_requires_a_boolean(private_library, monkeypatch, flag):
    """Ambiguous read modes are refused before filesystem or native work."""
    def forbidden(*args, **kwargs):
        """No file descriptor is opened for an invalid mode."""
        pytest.fail("invalid revision-read flag opened a file")

    monkeypatch.setattr(storage, "_log", lambda *args, **kwargs: None)
    monkeypatch.setattr(storage.os, "open", forbidden)
    with pytest.raises(PB9ConfigError) as caught:
        pb7_config.load_pb7_config("example.json", pb9_context=private_library.context, pb9_with_revision=flag)
    assert caught.value.error_codes == ("invalid_request",)


@pytest.mark.parametrize("deleted", [False, True])
def test_change_during_validation_is_rechecked_under_lock(private_library, monkeypatch, deleted):
    """An earlier fast check cannot authorize replacing a newer complete file."""
    library = private_library
    path = library.root / "example.json"
    original = json.dumps(_document()).encode()
    path.write_bytes(original)
    winner = json.dumps(_document("Another writer")).encode()
    actual_validate = storage.validate_pb9_document

    def edit_during_validation(*args, **kwargs):
        """Represent another save between the fast check and locked comparison."""
        if deleted:
            path.unlink()
        else:
            path.write_bytes(winner)
        return actual_validate(*args, **kwargs)

    monkeypatch.setattr(storage, "validate_pb9_document", edit_during_validation)
    with pytest.raises(PB9ConfigError) as caught:
        pb7_config.save_pb7_config(_document("Stale edit"), path.name, pb9_context=library.context,
                                  pb9_expected_revision=hashlib.sha256(original).hexdigest())
    assert caught.value.error_codes == ("config_revision_conflict",)
    if deleted:
        assert not path.exists()
        assert not list(library.root.iterdir())
    else:
        assert path.read_bytes() == winner
        assert list(library.root.iterdir()) == [path]


def test_update_acknowledgement_failure_reports_the_committed_update(private_library, monkeypatch):
    """After replacement, an I/O acknowledgement error never becomes a rejected creation."""
    library = private_library
    path = library.root / "example.json"
    before = json.dumps(_document()).encode()
    path.write_bytes(before)
    actual_sync = os.fsync
    events = []

    def fail_directory_sync(descriptor):
        """Allow the private file data sync, then fail the directory acknowledgement."""
        import stat
        if stat.S_ISDIR(os.fstat(descriptor).st_mode):
            raise OSError("fixture-private-update-sync-error")
        return actual_sync(descriptor)

    monkeypatch.setattr(storage, "_log", lambda *args, **kwargs: events.append((args, kwargs)))
    monkeypatch.setattr(storage.os, "fsync", fail_directory_sync)
    with pytest.raises(PB9ConfigError) as caught:
        pb7_config.save_pb7_config(_document("Committed"), path.name, pb9_context=library.context,
                                  pb9_expected_revision=hashlib.sha256(before).hexdigest())
    assert caught.value.error_codes == ("config_file_updated_io_failed",)
    assert caught.value.__cause__ is None and caught.value.__context__ is None
    assert events[-1][0][1] == "PB9 config updated; I/O acknowledgement failed"
    assert json.loads(path.read_bytes()) == _document("Committed")
    assert list(library.root.iterdir()) == [path]


def test_two_updates_from_one_revision_have_one_winner(private_library, monkeypatch):
    """The locked compare/replace transaction prevents concurrent lost updates."""
    library = private_library
    path = library.root / "example.json"
    original = json.dumps(_document()).encode()
    path.write_bytes(original)
    revision = hashlib.sha256(original).hexdigest()
    barrier = threading.Barrier(2, timeout=5)
    actual_validate = storage.validate_pb9_document
    results, failures = [], []

    def synchronize_validators(*args, **kwargs):
        """Both requests must pass the fast check before either tries to commit."""
        result = actual_validate(*args, **kwargs)
        barrier.wait()
        return result

    def update(label):
        """Capture both outcomes so the test cannot miss a worker failure."""
        try:
            pb7_config.save_pb7_config(_document(label), path.name, pb9_context=library.context,
                                      pb9_expected_revision=revision)
            results.append("updated")
        except PB9ConfigError as exc:
            results.append(exc.error_codes[0])
        except BaseException as exc:
            failures.append(exc)

    monkeypatch.setattr(storage, "validate_pb9_document", synchronize_validators)
    workers = [threading.Thread(target=update, args=(label,), daemon=True) for label in ("First", "Second")]
    try:
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(10)
    finally:
        barrier.abort()
    assert not any(worker.is_alive() for worker in workers)
    assert not failures
    assert sorted(results) == ["config_revision_conflict", "updated"]
    assert json.loads(path.read_bytes())["pbgui"]["name"] in {"First", "Second"}
    assert list(library.root.iterdir()) == [path]
