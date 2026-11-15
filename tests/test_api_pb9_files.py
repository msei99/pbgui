"""Owned PB9 config-library API tests with temporary storage and native doubles."""

import copy
import hashlib
import os
from pathlib import Path
import stat

import pytest

from api import pb9
import pb9_config
from tests.test_api_pb9 import endpoint


@pytest.fixture
def library(endpoint, monkeypatch, tmp_path):
    """Keep every storage operation below this temporary server application root."""
    root = tmp_path / "application"
    root.mkdir()
    monkeypatch.setattr(pb9, "PBGDIR", root)
    calls = []

    def native(config, source, python, other_slots, *, timeout, cancel_event):
        """Record isolated native-bound input while retaining the real document boundary."""
        assert not cancel_event.is_set()
        calls.append(copy.deepcopy(config))
        return {"valid": True, "source": {"repo_dir": str(source)}}

    monkeypatch.setattr(pb9_config, "validate_pb9_config", native)
    endpoint.root = root / "data" / "configs_v9"
    endpoint.native = calls
    return endpoint


def _post(library, path, body):
    """Use explicit Bearer auth without touching production authentication state."""
    return library.client.post("/api/pb9/configs/" + path, json=body,
                               headers={"Authorization": "Bearer fixture-auth"})


def _document(name="Example"):
    """Return a sparse authored document with independent GUI metadata."""
    return {"config_version": "v9.0.0", "bot": {"long": {}, "short": {}}, "pbgui": {"name": name}}


def test_file_api_create_load_update_conflict_and_private_library(library):
    """A complete revision round trip cannot overwrite an existing or newer document."""
    response = library.client.get("/api/pb9/configs", headers={"Authorization": "Bearer fixture-auth"})
    assert response.status_code == 200
    assert response.json() == {"configs": []}
    assert not library.root.exists()
    assert _post(library, "save", {"name": "example.json", "config": _document(), "revision": None}).json() == {
        "saved": True, "name": "example.json"}
    target = library.root / "example.json"
    original = target.read_bytes()
    assert stat.S_IMODE(library.root.stat().st_mode) == 0o700
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    response = _post(library, "load", {"name": "example.json"})
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    loaded = response.json()
    assert loaded == {"config": _document(), "revision": hashlib.sha256(original).hexdigest()}
    for revision in (None, "0" * 64):
        response = _post(library, "save", {"name": "example.json", "config": _document("Rejected"), "revision": revision})
        assert response.status_code == 409
        assert target.read_bytes() == original
    response = _post(library, "save", {"name": "example.json", "config": _document("Edited"),
                                      "revision": loaded["revision"]})
    assert response.status_code == 200
    assert _post(library, "load", {"name": "example.json"}).json()["config"] == _document("Edited")
    assert all("pbgui" not in native for native in library.native)
    response = library.client.get("/api/pb9/configs", headers={"Authorization": "Bearer fixture-auth"})
    assert response.json() == {"configs": ["example.json"]}
    assert library.app.state.pb9_requests.active == 0


@pytest.mark.parametrize("name", ["../escape.json", "/escape.json", "sub/name.json", "sub\\name.json", ".hidden.json",
                                 "bad\n.json", "bad\x00.json", "", "..", "bad.txt", None, 12])
def test_file_api_rejects_names_before_settings_and_creation(library, name):
    """Both file paths reject traversal, control characters and non-basename types."""
    for path, body in (("load", {"name": name}),
                       ("save", {"name": name, "config": _document(), "revision": None})):
        response = _post(library, path, body)
        assert response.status_code == 422
        assert response.headers["x-pbgui-error-codes"] == "invalid_config_name"
        assert response.headers["cache-control"] == "no-store"
    assert not library.root.exists()
    assert not library.snapshots
    assert not library.native


@pytest.mark.parametrize("path,body", [("load", {"name": "test.json", "root": "/private/override"}),
    ("save", {"name": "test.json", "config": _document()}),
    ("save", {"name": "test.json", "config": _document(), "revision": None, "pb9dir": "/private/override"})])
def test_file_envelope_requires_explicit_revision_and_no_runtime_overrides(library, path, body):
    """Only the exact endpoint envelope reaches storage; create is an explicit null."""
    response = _post(library, path, body)
    assert response.status_code == 422
    assert "/private/override" not in response.text
    assert not library.root.exists()
    assert not library.snapshots


def test_missing_config_is_404_without_implicit_directory_creation(library):
    """Reading missing files neither creates storage nor starts native validation."""
    response = _post(library, "load", {"name": "missing.json"})
    assert response.status_code == 404
    assert not library.root.exists()
    assert not library.native


@pytest.mark.parametrize("link", ["data", "configs_v9"])
def test_file_api_never_creates_or_reads_through_symlink_parent(library, tmp_path, link):
    """Even library provisioning cannot write outside the selected application root."""
    outside = tmp_path / "outside"
    outside.mkdir()
    data = library.root.parent
    if link == "data":
        data.symlink_to(outside, target_is_directory=True)
    else:
        data.mkdir()
        library.root.symlink_to(outside, target_is_directory=True)
    response = _post(library, "save", {"name": "test.json", "config": _document(), "revision": None})
    assert response.status_code == 503
    assert not list(outside.iterdir())
    assert not library.native


def test_config_list_skips_nonregular_hidden_and_invalid_entries(library, tmp_path):
    """The sidebar inventory never follows symlinks or scans nested directories."""
    library.root.mkdir(parents=True, mode=0o700)
    (library.root / "b.json").write_text("{}")
    (library.root / "A.json").write_text("{}")
    (library.root / ".hidden.json").write_text("{}")
    (library.root / "directory.json").mkdir()
    (library.root / "link.json").symlink_to(tmp_path / "unapproved.json")
    os.mkfifo(library.root / "fifo.json")
    response = library.client.get("/api/pb9/configs", headers={"Authorization": "Bearer fixture-auth"})
    assert response.status_code == 200
    assert response.json() == {"configs": ["A.json", "b.json"]}
    assert not library.native


@pytest.mark.parametrize("method,path", [("get", "/configs"), ("post", "/configs/load"), ("post", "/configs/save")])
def test_file_routes_require_auth_before_any_storage(library, method, path):
    """No private filename listing or file mutation is possible without shared auth."""
    response = getattr(library.client, method)("/api/pb9" + path)
    assert response.status_code == 401
    assert response.headers["cache-control"] == "no-store"
    assert not library.root.exists()
    assert not library.snapshots


@pytest.mark.parametrize("revision", [False, 1, "", "a" * 63, "A" * 64, "g" * 64])
def test_file_update_requires_exact_lowercase_revision_format(library, revision):
    """Malformed update preconditions never create a target or reach native code."""
    response = _post(library, "save", {"name": "test.json", "config": _document(), "revision": revision})
    assert response.status_code == 422
    assert response.headers["x-pbgui-error-codes"] == "invalid_revision"
    assert not (library.root / "test.json").exists()
    assert not library.native


@pytest.mark.parametrize("origin", [None, "https://other.example", "http://testserver"])
def test_cookie_file_save_uses_shared_origin_protection(library, origin):
    """Cookie-authenticated writes need same-origin evidence before private storage access."""
    from api import auth

    library.client.cookies.set(auth.SESSION_COOKIE_NAME, "fixture-auth")
    response = library.client.post("/api/pb9/configs/save", json={"name": "test.json", "config": _document(), "revision": None},
                                    headers={"Origin": origin} if origin else {})
    assert response.status_code == (200 if origin == "http://testserver" else 403)
    assert response.headers["cache-control"] == "no-store"
    assert (library.root / "test.json").exists() == (origin == "http://testserver")


def test_file_inventory_limit_is_explicit_and_does_not_return_partial_names(library, monkeypatch):
    """A directory scan is bounded even when unrelated entries fill the library."""
    import pb9_config_library

    library.root.mkdir(parents=True, mode=0o700)
    for name in ("a.json", "b.json", "c.json"):
        (library.root / name).write_text("{}")
    monkeypatch.setattr(pb9_config_library, "MAX_LIBRARY_ENTRIES", 2)
    response = library.client.get("/api/pb9/configs", headers={"Authorization": "Bearer fixture-auth"})
    assert response.status_code == 503
    assert response.headers["x-pbgui-error-codes"] == "config_library_limit"
    assert "a.json" not in response.text


def test_existing_shared_library_mode_is_refused_without_chmod(library):
    """The API never changes permissions of an existing user-selected directory."""
    library.root.mkdir(parents=True, mode=0o755)
    library.root.chmod(0o755)
    response = _post(library, "save", {"name": "test.json", "config": _document(), "revision": None})
    assert response.status_code == 503
    assert stat.S_IMODE(library.root.stat().st_mode) == 0o755
    assert not list(library.root.iterdir())


def test_missing_update_does_not_provision_library(library):
    """An exact-revision update cannot implicitly create its missing directory."""
    response = _post(library, "save", {"name": "test.json", "config": _document(), "revision": "0" * 64})
    assert response.status_code == 409
    assert response.headers["x-pbgui-error-codes"] == "config_revision_conflict"
    assert not library.root.exists()
    assert not library.native


@pytest.mark.parametrize("name", ["x" * 251 + ".json", "é" * 128 + ".json"])
def test_overlong_utf8_name_is_rejected_without_provisioning(library, name):
    """The byte-oriented filename cap applies before any directory creation."""
    response = _post(library, "save", {"name": name, "config": _document(), "revision": None})
    assert response.status_code == 422
    assert response.headers["x-pbgui-error-codes"] == "invalid_config_name"
    assert not library.root.exists()
    assert not library.snapshots


def test_file_api_stop_after_publication_returns_success_and_keeps_file(library, monkeypatch):
    """The HTTP boundary reports a committed save honestly despite a simultaneous stop."""
    import secure_files

    original = secure_files.os.link

    def commit_then_stop(*args, **kwargs):
        """Request owner shutdown immediately after its file commit point."""
        result = original(*args, **kwargs)
        library.app.state.pb9_requests.cancel_signal.set()
        return result

    monkeypatch.setattr(secure_files.os, "link", commit_then_stop)
    response = _post(library, "save", {"name": "test.json", "config": _document(), "revision": None})
    assert response.status_code == 200
    assert response.json() == {"saved": True, "name": "test.json"}
    assert (library.root / "test.json").exists()
    assert library.app.state.pb9_requests.active == 0
