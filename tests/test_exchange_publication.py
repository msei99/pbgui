"""Isolated end-to-end exchange persistence, publication, and PB8 recovery tests."""

import json
import stat
from unittest.mock import Mock

import pytest

import User
import api_key_state
import cluster_sync_command as sync
from api import api_keys
from master import cluster_state
from pb7_api_keys import PB7ApiKeysMergeWriter, PB7ApiKeysConflictError


@pytest.fixture
def storage(tmp_path, monkeypatch):
    """Bind all credential, cluster, config, state, and runtime paths to temp data."""
    gui, pb7, pb8 = (tmp_path / name for name in ("pbgui", "pb7", "pb8"))
    for path in (gui, pb7, pb8):
        path.mkdir()
    (pb8 / ".git").mkdir()
    for module in (User, sync):
        monkeypatch.setattr(module, "PBGDIR", str(gui))
        monkeypatch.setattr(module, "pb7dir", lambda: str(pb7))
    monkeypatch.setattr(sync, "pb8dir", lambda: str(pb8))
    monkeypatch.setattr(User, "is_pb7_installed", lambda: True)
    monkeypatch.setattr(User, "_cluster_pbname", lambda: "isolated")
    monkeypatch.setattr(api_keys, "_PBGDIR", str(gui))
    monkeypatch.setattr(api_keys, "_get_users", User.Users)
    monkeypatch.setattr(api_keys, "_is_user_in_use", lambda _name: False)
    state = gui / "data" / "state"
    monkeypatch.setattr(api_key_state, "_STATE_FILE", state / "state.json")
    monkeypatch.setattr(api_key_state, "_LEGACY_STATE_FILE", state / "legacy.json")
    monkeypatch.setattr(api_key_state, "_TRANSACTION_FILE", state / "transaction.json")
    monkeypatch.setattr(api_key_state, "_API_KEY_WRITE_TARGET", gui / "data" / "api-keys" / ".write")
    for runtime in (pb7, pb8):
        (runtime / "api-keys.json").write_text(json.dumps({
            "_api_serial": 1,
            "alice": {"exchange": "binance", "key": "old-key", "secret": "old-secret"},
            "tradfi": {"api_key": runtime.name + "-tradfi"},
        }))
    return gui, pb7 / "api-keys.json", pb8 / "api-keys.json", cluster_state.default_cluster_root(gui)


def test_save_projects_pb8_preserving_tradfi_and_permissions(storage):
    """A successful legacy save publishes and securely merges both local runtimes."""
    gui, pb7, pb8, root = storage
    users = User.Users()
    users.find_user("alice").secret = "new-secret"
    users.save()
    for path in (pb7, pb8):
        payload = json.loads(path.read_text())
        assert payload["alice"]["secret"] == "new-secret"
        assert payload["tradfi"]["api_key"] == path.parent.name + "-tradfi"
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert sync._materialize_api_keys(root, write=False)["status"] == "current"
    assert not (gui / "data/credentials/exchange_publication.pending").exists()


@pytest.mark.parametrize("stage", ["blob", "append", "rebuild"])
def test_publication_failure_retries_authoritative_file(storage, monkeypatch, stage):
    """Every publication boundary retains intent and cannot revert to old desired keys."""
    gui, pb7, pb8, root = storage
    users = User.Users()
    users.save()
    with monkeypatch.context() as patch:
        module, name = {
            "blob": (User, "_write_cluster_blob"),
            "append": (cluster_state, "append_operation"),
            "rebuild": (cluster_state, "rebuild_materialized_state"),
        }[stage]
        patch.setattr(module, name, Mock(side_effect=OSError("secret-must-not-leak")))
        users.find_user("alice").secret = "new-secret"
        with pytest.raises(User.ApiKeysPublicationError) as error:
            users.save()
        assert "secret-must-not-leak" not in str(error.value)
        assert json.loads(pb7.read_text())["alice"]["secret"] == "new-secret"
        assert sync._materialize_api_keys(root, write=False)["status"] == "pending"
        with pytest.raises(User.ApiKeysPublicationError):
            sync._materialize_api_keys(root, write=True)
        assert json.loads(pb7.read_text())["alice"]["secret"] == "new-secret"
    marker = gui / "data/credentials/exchange_publication.pending"
    intent = json.loads(marker.read_text())
    assert intent["after_hash"] == User._exchange_publication_hash(json.loads(pb7.read_text()))
    assert "new-secret" not in marker.read_text()
    assert stat.S_IMODE(marker.stat().st_mode) == 0o600
    sync._materialize_api_keys(root, write=True)
    assert json.loads(pb8.read_text())["alice"]["secret"] == "new-secret"
    assert not marker.exists()


def test_pb8_failure_is_visible_and_retriable(storage, monkeypatch):
    """A failed PB8 merge reports partial persistence and PBCluster can retry it."""
    _gui, pb7, pb8, root = storage
    original = PB7ApiKeysMergeWriter.write_exchange_payload

    def fail_pb8(self, *args, **kwargs):
        """Inject only the secondary runtime write failure."""
        if self.api_keys_path == pb8:
            raise OSError("injected")
        return original(self, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(PB7ApiKeysMergeWriter, "write_exchange_payload", fail_pb8)
        users = User.Users()
        users.find_user("alice").key = "replacement"
        with pytest.raises(User.ApiKeysPublicationError, match="local projection"):
            users.save()
    assert json.loads(pb7.read_text())["alice"]["key"] == "replacement"
    sync._materialize_api_keys(root, write=True)
    assert json.loads(pb8.read_text())["alice"]["key"] == "replacement"


def test_pb7_unavailable_rejects_without_persistence(storage, monkeypatch):
    """PB8 availability does not turn a missing PB7 store into a successful save."""
    gui, pb7, pb8, _root = storage
    before = (pb7.read_bytes(), pb8.read_bytes())
    monkeypatch.setattr(User, "is_pb7_installed", lambda: False)
    with pytest.raises(User.ApiKeysPersistenceUnavailableError):
        User.Users().save()
    with pytest.raises(api_keys.HTTPException) as error:
        api_keys.update_user(name="alice", data=api_keys.UserCreateUpdate(exchange="binance"), session=None)
    assert error.value.status_code == 503
    assert (pb7.read_bytes(), pb8.read_bytes()) == before
    assert not (gui / "data/credentials/exchange_publication.pending").exists()
    assert api_key_state.get_pending_user_state_transaction() is None


def test_rename_publication_error_commits_state_but_returns_error(storage, monkeypatch):
    """Journal recovery must not swallow publication failures after credential commit."""
    _gui, pb7, pb8, root = storage
    api_key_state.update_user_state("alice", custom="keep")
    with monkeypatch.context() as patch:
        patch.setattr(cluster_state, "append_operation", Mock(side_effect=OSError("injected")))
        with pytest.raises(api_keys.HTTPException) as error:
            api_keys.update_user(name="alice", data=api_keys.UserCreateUpdate(
                exchange="binance", new_name="bob", key="new-key"), session=None)
    assert error.value.status_code == 503
    assert "publication is pending" in error.value.detail
    assert "bob" in json.loads(pb7.read_text())
    assert api_key_state.get_pending_user_state_transaction() is None
    assert api_key_state.get_user_state("bob")["custom"] == "keep"
    sync._materialize_api_keys(root, write=True)
    assert "bob" in json.loads(pb8.read_text())


def test_stale_legacy_save_cannot_revert_newer_credentials(storage):
    """A rejected stale snapshot leaves retry based on the newer authoritative file."""
    _gui, pb7, pb8, root = storage
    stale, fresh = User.Users(), User.Users()
    fresh.find_user("alice").key = "newer"
    fresh.save()
    with pytest.raises(PB7ApiKeysConflictError):
        stale.save()
    sync._materialize_api_keys(root, write=True)
    assert json.loads(pb7.read_text())["alice"]["key"] == "newer"
    assert json.loads(pb8.read_text())["alice"]["key"] == "newer"


def test_pb8_symlink_fails_closed(storage, tmp_path):
    """Secondary projection never follows a secret-file symlink."""
    _gui, _pb7, pb8, root = storage
    outside = tmp_path / "outside.json"
    outside.write_text("{}")
    pb8.unlink()
    pb8.symlink_to(outside)
    with pytest.raises(User.ApiKeysPublicationError):
        User.Users().save()
    assert outside.read_text() == "{}"
    pb8.unlink()
    sync._materialize_api_keys(root, write=True)
    assert pb8.is_file() and not pb8.is_symlink()


@pytest.mark.parametrize("publication_fails", [False, True])
def test_after_replace_error_cannot_hide_pending_publication(storage, monkeypatch, publication_fails):
    """Recovery of an ambiguous PB7 write also completes or reports publication."""
    _gui, pb7, pb8, root = storage
    original = PB7ApiKeysMergeWriter.write_exchange_payload

    def fail_after_replace(self, *args, **kwargs):
        """Raise after the authoritative file was durably replaced."""
        result = original(self, *args, **kwargs)
        if self.api_keys_path == pb7:
            raise OSError("after replace")
        return result

    with monkeypatch.context() as patch:
        patch.setattr(PB7ApiKeysMergeWriter, "write_exchange_payload", fail_after_replace)
        if publication_fails:
            patch.setattr(cluster_state, "append_operation", Mock(side_effect=OSError("injected")))
        data = api_keys.UserCreateUpdate(exchange="binance", new_name="bob")
        if publication_fails:
            with pytest.raises(api_keys.HTTPException) as error:
                api_keys.update_user(name="alice", data=data, session=None)
            assert error.value.status_code == 503
        else:
            assert api_keys.update_user(name="alice", data=data, session=None).name == "bob"
    assert api_key_state.get_pending_user_state_transaction() is None
    sync._materialize_api_keys(root, write=True)
    assert "bob" in json.loads(pb8.read_text())


def test_hl_rename_projects_pb8_without_conflicting_with_state_journal(storage):
    """Secondary HL projection must not mutate runtime state during a rename journal."""
    _gui, pb7, pb8, _root = storage
    for path in (pb7, pb8):
        path.write_text(json.dumps({
            "_api_serial": 1,
            "alice": {"exchange": "hyperliquid", "wallet_address": "0x1", "private_key": "old"},
        }))
    api_key_state.update_user_state("alice", hl_valid_until=123, custom="keep")
    result = api_keys.update_user(name="alice", data=api_keys.UserCreateUpdate(
        exchange="hyperliquid", wallet_address="0x1", new_name="bob"), session=None)
    assert result.name == "bob"
    assert api_key_state.get_user_state("bob")["hl_valid_until"] == 123
    assert "bob" in json.loads(pb8.read_text())


def test_materializer_waits_for_save_publication(storage, monkeypatch):
    """A materializer racing a save cannot apply the preceding desired snapshot."""
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    _gui, pb7, pb8, root = storage
    users = User.Users()
    users.save()
    entered, release, started = Event(), Event(), Event()
    original = User._record_cluster_api_keys_update

    def paused_publication(payload, *args):
        """Pause after PB7 replacement while the shared transaction lock is held."""
        entered.set()
        assert release.wait(5)
        return original(payload, *args)

    def materialize():
        """Signal a racing materializer before trying to acquire the transaction lock."""
        started.set()
        return sync._materialize_api_keys(root, write=True)

    monkeypatch.setattr(User, "_record_cluster_api_keys_update", paused_publication)
    users.find_user("alice").key = "newest"
    with ThreadPoolExecutor(max_workers=2) as pool:
        save = pool.submit(users.save)
        try:
            assert entered.wait(5)
            projection = pool.submit(materialize)
            assert started.wait(5)
            assert not projection.done()
        finally:
            release.set()
        save.result(timeout=5)
        projection.result(timeout=5)
    assert json.loads(pb7.read_text())["alice"]["key"] == "newest"
    assert json.loads(pb8.read_text())["alice"]["key"] == "newest"


@pytest.mark.parametrize("failure", ["cas", "backup", "interrupted_backup"])
def test_rejected_save_does_not_republish_old_pb7_over_newer_desired(storage, monkeypatch, failure):
    """Rejected local writes must leave an unapplied remote desired snapshot authoritative."""
    import pb7_api_keys

    gui, pb7, pb8, root = storage
    stale = User.Users()
    User.Users().save()
    users = stale if failure == "cas" else User.Users()
    desired = {"_api_serial": 10, "alice": {"exchange": "binance", "key": "remote-newer"}}
    User._record_cluster_api_keys_update(desired, root)
    operations = cluster_state.load_operations(root)
    original = pb7_api_keys.atomic_write_private_bytes

    def fail_backup(path, content):
        """Fail the backup before the authoritative file can be replaced."""
        if path.parent == gui / "data/api-keys":
            raise (KeyboardInterrupt if failure == "interrupted_backup" else OSError)("backup failed")
        return original(path, content)

    with monkeypatch.context() as patch:
        if failure != "cas":
            patch.setattr(pb7_api_keys, "atomic_write_private_bytes", fail_backup)
        error = {"cas": PB7ApiKeysConflictError, "backup": OSError, "interrupted_backup": KeyboardInterrupt}[failure]
        with pytest.raises(error):
            users.save()
    marker = gui / "data/credentials/exchange_publication.pending"
    assert marker.exists() is (failure == "interrupted_backup")
    sync._materialize_api_keys(root, write=True)
    assert not marker.exists()
    assert cluster_state.load_operations(root) == operations
    for path in (pb7, pb8):
        assert json.loads(path.read_text())["alice"]["key"] == "remote-newer"


@pytest.mark.parametrize("crash", [False, True])
def test_failed_first_save_does_not_block_missing_pb7_recovery(storage, monkeypatch, crash):
    """A failed or interrupted first write is not evidence of committed local credentials."""
    gui, pb7, pb8, root = storage
    pb7.unlink()
    User._record_cluster_api_keys_update({
        "_api_serial": 10, "alice": {"exchange": "binance", "key": "remote-newer"},
    }, root)
    operations = cluster_state.load_operations(root)
    error = KeyboardInterrupt if crash else OSError
    with monkeypatch.context() as patch:
        patch.setattr(PB7ApiKeysMergeWriter, "_write_api_keys_unlocked", Mock(side_effect=error("before replace")))
        with pytest.raises(error):
            User.Users().save()
    marker = gui / "data/credentials/exchange_publication.pending"
    assert marker.exists() is crash
    assert not pb7.exists()
    sync._materialize_api_keys(root, write=True)
    assert not marker.exists()
    assert cluster_state.load_operations(root) == operations
    assert json.loads(pb8.read_text())["alice"]["key"] == "remote-newer"


@pytest.mark.parametrize("failure", ["cas", "backup", "crash"])
def test_rejected_save_preserves_previous_committed_publication(storage, monkeypatch, failure):
    """A second failed write preserves the first committed save's publication evidence."""
    import pb7_api_keys

    gui, _pb7, pb8, root = storage
    users = User.Users()
    users.find_user("alice").key = "committed-pending"
    with monkeypatch.context() as patch:
        patch.setattr(cluster_state, "append_operation", Mock(side_effect=OSError("publication failed")))
        with pytest.raises(User.ApiKeysPublicationError):
            users.save()
    marker = gui / "data/credentials/exchange_publication.pending"
    previous = marker.read_bytes()
    users.find_user("alice").key = "must-not-publish"
    error = {"cas": PB7ApiKeysConflictError, "backup": OSError, "crash": KeyboardInterrupt}[failure]
    with monkeypatch.context() as patch:
        if failure == "cas":
            users._loaded_api_serial -= 1
        elif failure == "backup":
            patch.setattr(pb7_api_keys, "atomic_write_private_bytes", Mock(side_effect=OSError("backup failed")))
        else:
            patch.setattr(PB7ApiKeysMergeWriter, "_write_api_keys_unlocked", Mock(side_effect=KeyboardInterrupt()))
        with pytest.raises(error):
            users.save()
    if failure != "crash":
        assert marker.read_bytes() == previous
    else:
        assert json.loads(marker.read_text())["before_pending"] is True
    sync._materialize_api_keys(root, write=True)
    assert json.loads(pb8.read_text())["alice"]["key"] == "committed-pending"
    assert not marker.exists()


@pytest.mark.parametrize("first_save", [False, True])
def test_atomic_postreplace_failure_retains_committed_intent(storage, monkeypatch, first_save):
    """An error after the actual atomic replacement retains hash-proven recovery."""
    import pb7_api_keys

    gui, pb7, pb8, root = storage
    if first_save:
        pb7.unlink()
    users = User.Users()
    if first_save:
        user = User.User()
        user.name, user.exchange, user.key = "alice", "binance", "committed"
        users.users.append(user)
    else:
        users.find_user("alice").key = "committed"
    original = pb7_api_keys.atomic_write_private_bytes

    def fail_after_replace(path, content):
        """Complete the real file replacement, then raise before returning to the writer."""
        original(path, content)
        if path == pb7:
            raise OSError("after atomic replace")

    with monkeypatch.context() as patch:
        patch.setattr(pb7_api_keys, "atomic_write_private_bytes", fail_after_replace)
        with pytest.raises(OSError, match="after atomic replace"):
            users.save()
    marker = gui / "data/credentials/exchange_publication.pending"
    assert User._exchange_publication_committed(json.loads(marker.read_text()), json.loads(pb7.read_text()))
    sync._materialize_api_keys(root, write=True)
    assert json.loads(pb8.read_text())["alice"]["key"] == "committed"
    assert not marker.exists()


def test_recovery_refuses_unrelated_snapshot_at_same_generation(storage, monkeypatch):
    """Generation alone must not authorize publication of a different credential snapshot."""
    gui, pb7, _pb8, root = storage
    with monkeypatch.context() as patch:
        patch.setattr(cluster_state, "append_operation", Mock(side_effect=OSError("publication failed")))
        with pytest.raises(User.ApiKeysPublicationError):
            User.Users().save()
    payload = json.loads(pb7.read_text())
    payload["alice"]["key"] = "unrelated"
    pb7.write_text(json.dumps(payload))
    operations = cluster_state.load_operations(root)
    with pytest.raises(PB7ApiKeysConflictError, match="intent conflicts"):
        sync._materialize_api_keys(root, write=True)
    assert (gui / "data/credentials/exchange_publication.pending").exists()
    assert cluster_state.load_operations(root) == operations
    assert json.loads(pb7.read_text()) == payload
