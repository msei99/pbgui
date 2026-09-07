"""Tests for API-key persistence, Cluster Sync, and credential previews."""

from __future__ import annotations

import json
import os
import stat
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import User as user_module
import pytest
from fastapi import HTTPException
from master.cluster_state import default_cluster_root, load_operations, read_local_identity, rebuild_materialized_state


def test_weex_requires_passphrase_server_side(monkeypatch) -> None:
    """WEEX credentials must fail closed even when a client bypasses the browser form."""
    from api import api_keys

    monkeypatch.setattr(api_keys, "_get_users", lambda: SimpleNamespace(find_user=lambda _name: None))
    data = api_keys.UserCreateUpdate(exchange="weex", key="key", secret="secret")

    with pytest.raises(HTTPException) as exc_info:
        api_keys.create_user(name="weex-user", data=data, session=None)

    assert exc_info.value.status_code == 400
    assert "Passphrase is required for weex" in str(exc_info.value.detail)


def test_bitunix_credentials_do_not_require_passphrase(monkeypatch) -> None:
    """Bitunix uses key and secret only."""
    from api import api_keys

    stored = SimpleNamespace(users=[], find_user=lambda _name: None, save=lambda: None)
    monkeypatch.setattr(api_keys, "_get_users", lambda: stored)
    monkeypatch.setattr(api_keys, "delete_user_state", lambda _name: None)
    data = api_keys.UserCreateUpdate(exchange="bitunix", key="key", secret="secret")

    result = api_keys.create_user(name="bitunix-user", data=data, session=None)

    assert result.exchange == "bitunix"
    assert stored.users[0].passphrase is None


def test_user_detail_never_contains_credential_values() -> None:
    """Bulk and detail responses may expose presence, never stored credential material."""
    from api import api_keys

    user = SimpleNamespace(
        name="alice",
        exchange="weex",
        key="public-key",
        secret="secret-value",
        passphrase="passphrase-value",
        wallet_address=None,
        private_key="private-value",
        is_vault=False,
        quote="USDT",
        options=None,
        extra=None,
    )

    detail = api_keys._user_to_detail(user, False)

    assert detail.key is None
    assert detail.key_masked == "********"
    assert detail.secret_masked == "********"
    assert detail.passphrase_masked == "********"
    assert detail.private_key_masked == "********"
    assert not any(value in str(detail) for value in (user.key, user.secret, user.passphrase, user.private_key))


def test_api_key_meta_advertises_combined_user_update(monkeypatch) -> None:
    """The running API explicitly advertises atomic rename/update support."""
    from api import api_keys

    monkeypatch.setattr(api_keys, "_get_users", lambda: SimpleNamespace(api_meta={"api_serial": 7}))

    result = api_keys.get_meta(session=None)

    assert result["api_serial"] == 7
    assert result["capabilities"] == {"combined_user_update": True}


def test_api_key_reveal_is_post_only_no_store_and_private_fields_are_unavailable(monkeypatch) -> None:
    """Only one explicitly selected API key may be revealed through a POST body."""
    from api import api_keys

    user = SimpleNamespace(key="public-key")
    monkeypatch.setattr(api_keys, "_get_users", lambda: SimpleNamespace(find_user=lambda name: user if name == "alice" else None))
    response = api_keys.Response()

    result = api_keys.reveal_user_key(api_keys.UserKeyRevealRequest(name="alice"), response, session=None)

    assert result == {"value": "public-key"}
    assert response.headers["Cache-Control"] == "no-store"
    reveal_routes = [route for route in api_keys.router.routes if "reveal" in route.path and route.path != "/tradfi/reveal"]
    assert [(route.path, route.methods) for route in reveal_routes] == [("/reveal-key", {"POST"})]


def test_api_key_backup_diff_redacts_all_credentials(monkeypatch, tmp_path: Path) -> None:
    """Backup comparison may expose structure, never credential values."""
    from api import api_keys

    backup_dir = tmp_path / "data" / "api-keys"
    backup_dir.mkdir(parents=True)
    first = {
        "alice": {"key": "first-public", "secret": "first-secret", "passphrase": "first-pass"},
        "wallet": {"private_key": "first-private"},
    }
    second = {
        "alice": {"key": "second-public", "secret": "second-secret", "passphrase": "second-pass"},
        "wallet": {"private_key": "second-private"},
    }
    (backup_dir / "api-keys7_first.json").write_text(json.dumps(first), encoding="utf-8")
    (backup_dir / "api-keys7_second.json").write_text(json.dumps(second), encoding="utf-8")
    monkeypatch.setattr(api_keys, "_PBGDIR", str(tmp_path))
    response = api_keys.Response()

    result = api_keys.diff_backups(
        api_keys.DiffRequest(filename1="api-keys7_first.json", filename2="api-keys7_second.json"),
        response,
        session=None,
    )

    rendered = "\n".join(result["lines1"] + result["lines2"])
    for secret in (
        "first-public", "first-secret", "first-pass", "first-private",
        "second-public", "second-secret", "second-pass", "second-private",
    ):
        assert secret not in rendered
    assert rendered.count("<redacted>") == 8
    assert response.headers["Cache-Control"] == "no-store"


def test_update_keeps_stored_api_key_when_detail_field_is_blank(monkeypatch) -> None:
    """Masked detail editing must not erase an unchanged API key."""
    from api import api_keys

    user = SimpleNamespace(
        name="alice", exchange="weex", key="stored-key", secret="stored-secret",
        passphrase="stored-pass", wallet_address=None, private_key=None, is_vault=False,
        quote="USDT", options=None, extra=None,
    )
    users = SimpleNamespace(find_user=lambda name: user if name == "alice" else None, save=lambda: None)
    monkeypatch.setattr(api_keys, "_get_users", lambda: users)
    monkeypatch.setattr(api_keys, "delete_user_state", lambda _name: None)
    monkeypatch.setattr(api_keys, "_is_user_in_use", lambda _name: False)
    _stub_api_key_transaction(monkeypatch, api_keys)
    data = api_keys.UserCreateUpdate(exchange="weex", key=None, secret=None, passphrase=None, quote="USDT")

    api_keys.update_user(name="alice", data=data, session=None)

    assert user.key == "stored-key"
    assert user.secret == "stored-secret"
    assert user.passphrase == "stored-pass"


def test_exchange_change_requires_replacements_and_clears_irrelevant_credentials(monkeypatch) -> None:
    """Credentials from one venue must never silently migrate to another venue."""
    from api import api_keys

    user = SimpleNamespace(
        name="alice", exchange="weex", key="old-key", secret="old-secret",
        passphrase="old-pass", wallet_address=None, private_key=None, is_vault=False,
        quote="USDT", options=None, extra=None,
    )
    users = SimpleNamespace(find_user=lambda name: user if name == "alice" else None, save=lambda: None)
    monkeypatch.setattr(api_keys, "_get_users", lambda: users)
    monkeypatch.setattr(api_keys, "delete_user_state", lambda _name: None)
    monkeypatch.setattr(api_keys, "_is_user_in_use", lambda _name: False)
    _stub_api_key_transaction(monkeypatch, api_keys)

    with pytest.raises(HTTPException, match="API Key"):
        api_keys.update_user(
            name="alice",
            data=api_keys.UserCreateUpdate(exchange="bitunix", key=None, secret=None),
            session=None,
        )

    api_keys.update_user(
        name="alice",
        data=api_keys.UserCreateUpdate(exchange="bitunix", key="new-key", secret="new-secret"),
        session=None,
    )

    assert user.key == "new-key"
    assert user.secret == "new-secret"
    assert user.passphrase is None
    assert user.private_key is None


def _api_key_user(**overrides):
    """Return a complete mutable API-key user test double."""
    values = {
        "name": "alice",
        "exchange": "binance",
        "key": "old-key",
        "secret": "old-secret",
        "passphrase": None,
        "wallet_address": None,
        "private_key": None,
        "is_vault": False,
        "quote": "USDT",
        "options": None,
        "extra": {"label": "old"},
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _stub_api_key_transaction(monkeypatch, api_keys):
    """Keep narrow route unit tests away from the real runtime-state journal."""
    begin = Mock(return_value="transaction-id")
    finish = Mock()
    monkeypatch.setattr(api_keys, "_resolve_pending_user_update", Mock(return_value=None))
    monkeypatch.setattr(api_keys, "begin_user_state_transaction", begin)
    monkeypatch.setattr(api_keys, "finish_user_state_transaction", finish)
    return begin, finish


def test_update_user_combines_rename_credentials_and_state_in_one_save(monkeypatch) -> None:
    """One PUT atomically applies the final identity, credentials, and runtime state."""
    from api import api_keys

    user = _api_key_user(exchange="bybit")
    users = SimpleNamespace(
        find_user=lambda name: user if name == "alice" else None,
        save=Mock(),
    )
    monkeypatch.setattr(api_keys, "_get_users", lambda: users)
    monkeypatch.setattr(api_keys, "_is_user_in_use", lambda _name: False)
    begin, finish = _stub_api_key_transaction(monkeypatch, api_keys)
    api_keys._hl_expiry_cache.update({"alice": Mock(), "bob": Mock()})
    api_keys._bybit_expiry_cache.update({"alice": Mock(), "bob": Mock()})

    result = api_keys.update_user(
        name="alice",
        data=api_keys.UserCreateUpdate(
            new_name="bob",
            exchange="bybit",
            key="new-key",
            secret="new-secret",
            quote="USDC",
            extra={"label": "new", "bybit_ips": ["must-not-persist"]},
        ),
        session=None,
    )

    assert user.name == "bob"
    assert user.key == "new-key"
    assert user.secret == "new-secret"
    assert user.quote == "USDC"
    assert user.extra == {"label": "new"}
    users.save.assert_called_once_with()
    begin.assert_called_once()
    assert begin.call_args.args == ("alice", "bob")
    assert begin.call_args.kwargs["clear_all"] is False
    assert begin.call_args.kwargs["clear_keys"] == (
        "bybit_expires_at", "bybit_ips", "bybit_credential_fingerprint",
    )
    finish.assert_called_once_with("transaction-id", commit=True)
    assert result.name == "bob"
    assert result.key is None
    assert "new-key" not in str(result)
    assert "new-secret" not in str(result)
    assert "alice" not in api_keys._hl_expiry_cache
    assert "bob" not in api_keys._hl_expiry_cache
    assert "alice" not in api_keys._bybit_expiry_cache
    assert "bob" not in api_keys._bybit_expiry_cache


def test_update_user_rejects_collision_before_state_or_record_mutation(monkeypatch) -> None:
    """A final-name collision leaves the original record and runtime state untouched."""
    from api import api_keys

    alice = _api_key_user()
    bob = _api_key_user(name="bob")
    users = SimpleNamespace(
        find_user=lambda name: {"alice": alice, "bob": bob}.get(name),
        save=Mock(),
    )
    monkeypatch.setattr(api_keys, "_get_users", lambda: users)
    monkeypatch.setattr(api_keys, "_is_user_in_use", Mock(return_value=False))
    begin, _finish = _stub_api_key_transaction(monkeypatch, api_keys)

    with pytest.raises(HTTPException) as exc_info:
        api_keys.update_user(
            name="alice",
            data=api_keys.UserCreateUpdate(new_name="bob", exchange="binance"),
            session=None,
        )

    assert exc_info.value.status_code == 409
    assert alice.name == "alice"
    users.save.assert_not_called()
    begin.assert_not_called()


def test_update_user_journal_failure_prevents_any_record_mutation(monkeypatch) -> None:
    """A failed durable journal write cannot persist or retain credential mutations."""
    from api import api_keys

    user = _api_key_user()
    users = SimpleNamespace(
        find_user=lambda name: user if name == "alice" else None,
        save=Mock(),
    )
    monkeypatch.setattr(api_keys, "_get_users", lambda: users)
    monkeypatch.setattr(api_keys, "_is_user_in_use", lambda _name: False)
    monkeypatch.setattr(api_keys, "_resolve_pending_user_update", Mock(return_value=None))
    monkeypatch.setattr(api_keys, "begin_user_state_transaction", Mock(side_effect=OSError("journal write failed")))

    with pytest.raises(HTTPException) as exc_info:
        api_keys.update_user(
            name="alice",
            data=api_keys.UserCreateUpdate(
                new_name="bob", exchange="binance", key="new-key", secret="new-secret",
            ),
            session=None,
        )

    assert exc_info.value.status_code == 500
    assert user.name == "alice"
    assert user.key == "old-key"
    assert user.secret == "old-secret"
    users.save.assert_not_called()


def test_update_user_pre_replace_failure_restores_record_and_runtime_intent(monkeypatch) -> None:
    """A pre-replace credential failure resolves the journal as an exact rollback."""
    from api import api_keys

    user = _api_key_user()
    users = SimpleNamespace(
        find_user=lambda name: user if name == "alice" else None,
        save=Mock(side_effect=OSError("credential write failed")),
    )
    monkeypatch.setattr(api_keys, "_get_users", lambda: users)
    monkeypatch.setattr(api_keys, "_is_user_in_use", lambda _name: False)
    begin, _finish = _stub_api_key_transaction(monkeypatch, api_keys)
    api_keys._resolve_pending_user_update.side_effect = [None, None, "rolled_back"]

    with pytest.raises(HTTPException) as exc_info:
        api_keys.update_user(
            name="alice",
            data=api_keys.UserCreateUpdate(
                new_name="bob", exchange="binance", key="new-key", secret="new-secret",
            ),
            session=None,
        )

    assert exc_info.value.status_code == 500
    assert user == _api_key_user()
    users.save.assert_called_once_with()
    begin.assert_called_once()


def test_api_key_rename_patch_route_is_authenticated_compatibility_wrapper(monkeypatch) -> None:
    """PATCH remains available and delegates to the combined transaction implementation."""
    from api import api_keys

    user = _api_key_user()
    monkeypatch.setattr(api_keys, "_resolve_pending_user_update", Mock(return_value=None))
    monkeypatch.setattr(api_keys, "_get_users", lambda: SimpleNamespace(find_user=lambda name: user if name == "alice" else None))
    combined = Mock(return_value="renamed")
    monkeypatch.setattr(api_keys, "_update_user_transaction", combined)

    result = api_keys.rename_user(api_keys.RenameRequest(new_name="bob"), name="alice", session=None)

    assert result == "renamed"
    assert combined.call_count == 1
    assert combined.call_args.args[0] == "alice"
    assert combined.call_args.args[1].new_name == "bob"
    routes = [route for route in api_keys.router.routes if route.path == "/{name}/rename"]
    assert len(routes) == 1
    assert routes[0].methods == {"PATCH"}
    assert routes[0].dependant.dependencies


def test_user_state_journal_commits_and_rolls_back_exact_preimages(monkeypatch, tmp_path: Path) -> None:
    """The durable state journal applies its target or exact prior entries."""
    import api_key_state

    state_file = tmp_path / "state" / "api_key_state.json"
    state_file.parent.mkdir(parents=True)
    original = {
        "version": 1,
        "users": {
            "alice": {
                "bybit_expires_at": "2030-01-01T00:00:00Z",
                "bybit_ips": ["127.0.0.1"],
                "other": "keep",
            },
            "bob": {"stale": "do-not-adopt"},
        },
    }
    state_file.write_text(json.dumps(original), encoding="utf-8")
    monkeypatch.setattr(api_key_state, "_STATE_FILE", state_file)
    monkeypatch.setattr(api_key_state, "_LEGACY_STATE_FILE", tmp_path / "legacy.json")
    monkeypatch.setattr(api_key_state, "_TRANSACTION_FILE", state_file.with_name("transaction.json"))
    monkeypatch.setattr(api_key_state, "_API_KEY_WRITE_TARGET", tmp_path / "locks" / ".write")

    transaction_id = api_key_state.begin_user_state_transaction(
        "alice",
        "bob",
        before_marker="before",
        after_marker="after",
        clear_keys=("bybit_expires_at", "bybit_ips"),
    )
    assert json.loads(state_file.read_text(encoding="utf-8")) == original
    api_key_state.finish_user_state_transaction(transaction_id, commit=True)

    transitioned = json.loads(state_file.read_text(encoding="utf-8"))
    assert transitioned["users"] == {"bob": {"other": "keep"}}

    state_file.write_text(json.dumps(original), encoding="utf-8")
    transaction_id = api_key_state.begin_user_state_transaction(
        "alice", "bob", before_marker="before-2", after_marker="after-2", clear_all=True,
    )
    api_key_state.finish_user_state_transaction(transaction_id, commit=False)

    restored = json.loads(state_file.read_text(encoding="utf-8"))
    assert restored == original


class _FileUsers:
    """Minimal generation-CAS user store for isolated transaction failure tests."""

    def __init__(self, path: Path, failure: dict[str, str | None]) -> None:
        self.path = path
        self.failure = failure
        payload = json.loads(path.read_text(encoding="utf-8"))
        self._loaded_api_serial = int(payload["generation"])
        self.users = [_api_key_user(**record) for record in payload["users"]]

    def find_user(self, name: str):
        """Return one loaded user by canonical name."""
        return next((user for user in self.users if user.name == name), None)

    def save(self) -> None:
        """Atomically replace the fixture or inject a before/after-replace failure."""
        if self.failure["mode"] == "before":
            raise OSError("injected before replace")
        generation = self._loaded_api_serial + 1
        payload = {
            "generation": generation,
            "users": [{key: getattr(user, key) for key in (
                "name", "exchange", "key", "secret", "passphrase", "wallet_address",
                "private_key", "is_vault", "quote", "options", "extra",
            )} for user in self.users],
        }
        if self.failure["mode"] == "conflict":
            payload["users"][0]["name"] = "concurrent"
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload), encoding="utf-8")
        os.replace(tmp, self.path)
        self._loaded_api_serial = generation
        if self.failure["mode"] in {"after", "conflict"}:
            raise OSError("injected after replace")


def _install_file_transaction_fixtures(monkeypatch, tmp_path: Path, failure_mode: str | None):
    """Install isolated credential, state, journal, and lock files for one route call."""
    from api import api_keys
    import api_key_state

    credentials = tmp_path / "credentials.json"
    credentials.write_text(json.dumps({
        "generation": 1,
        "users": [{key: getattr(_api_key_user(), key) for key in (
            "name", "exchange", "key", "secret", "passphrase", "wallet_address",
            "private_key", "is_vault", "quote", "options", "extra",
        )}],
    }), encoding="utf-8")
    state_file = tmp_path / "state" / "api_key_state.json"
    state_file.parent.mkdir()
    state_file.write_text(json.dumps({
        "version": 1,
        "users": {"alice": {"hl_valid_until": 123, "custom": "keep"}},
    }), encoding="utf-8")
    transaction_file = state_file.with_name("user_update_transaction.json")
    failure = {"mode": failure_mode}
    monkeypatch.setattr(api_key_state, "_STATE_FILE", state_file)
    monkeypatch.setattr(api_key_state, "_LEGACY_STATE_FILE", tmp_path / "legacy.json")
    monkeypatch.setattr(api_key_state, "_TRANSACTION_FILE", transaction_file)
    monkeypatch.setattr(api_key_state, "_API_KEY_WRITE_TARGET", tmp_path / "locks" / ".write")
    monkeypatch.setattr(api_keys, "_PBGDIR", str(tmp_path))
    monkeypatch.setattr(api_keys, "_get_users", lambda: _FileUsers(credentials, failure))
    monkeypatch.setattr(api_keys, "_is_user_in_use", lambda _name: False)
    return api_keys, api_key_state, credentials, state_file, transaction_file, failure


def _renamed_update(api_keys):
    """Return one combined rename and credential replacement request."""
    return api_keys.UserCreateUpdate(
        new_name="bob",
        exchange="binance",
        key="new-key",
        secret="new-secret",
        quote="USDC",
        extra={"label": "new"},
    )


def test_user_update_recovers_interruption_after_credential_replace(monkeypatch, tmp_path: Path) -> None:
    """A durable journal completes runtime migration after an injected process interruption."""
    api_keys, api_key_state, credentials, state_file, journal, _failure = _install_file_transaction_fixtures(
        monkeypatch, tmp_path, None,
    )
    real_finish = api_key_state.finish_user_state_transaction
    monkeypatch.setattr(api_keys, "finish_user_state_transaction", Mock(side_effect=OSError("injected interruption")))

    with pytest.raises(HTTPException) as exc_info:
        api_keys.update_user(name="alice", data=_renamed_update(api_keys), session=None)

    assert exc_info.value.status_code == 500
    assert json.loads(credentials.read_text(encoding="utf-8"))["users"][0]["name"] == "bob"
    assert "alice" in json.loads(state_file.read_text(encoding="utf-8"))["users"]
    journal_text = journal.read_text(encoding="utf-8")
    assert "old-secret" not in journal_text
    assert "new-secret" not in journal_text

    monkeypatch.setattr(api_keys, "finish_user_state_transaction", real_finish)
    assert api_keys._resolve_pending_user_update() == "committed"
    recovered_state = json.loads(state_file.read_text(encoding="utf-8"))["users"]
    assert recovered_state == {"bob": {"hl_valid_until": 123, "custom": "keep"}}
    assert not journal.exists()


@pytest.mark.parametrize(
    ("failure_mode", "expected_status", "expected_name", "journal_exists"),
    [
        ("before", 500, "alice", False),
        ("after", None, "bob", False),
        ("conflict", 409, "concurrent", True),
    ],
)
def test_user_update_resolves_before_after_and_conflicting_replace_failures(
    monkeypatch,
    tmp_path: Path,
    failure_mode: str,
    expected_status: int | None,
    expected_name: str,
    journal_exists: bool,
) -> None:
    """Failure injection rolls back, commits, or reports a CAS conflict deterministically."""
    api_keys, _state_module, credentials, state_file, journal, _failure = _install_file_transaction_fixtures(
        monkeypatch, tmp_path, failure_mode,
    )

    if expected_status is None:
        result = api_keys.update_user(name="alice", data=_renamed_update(api_keys), session=None)
        assert result.name == "bob"
    else:
        with pytest.raises(HTTPException) as exc_info:
            api_keys.update_user(name="alice", data=_renamed_update(api_keys), session=None)
        assert exc_info.value.status_code == expected_status

    assert json.loads(credentials.read_text(encoding="utf-8"))["users"][0]["name"] == expected_name
    state_users = json.loads(state_file.read_text(encoding="utf-8"))["users"]
    if failure_mode == "before":
        assert "alice" in state_users and "bob" not in state_users
    elif failure_mode == "after":
        assert "bob" in state_users and "alice" not in state_users
    assert journal.exists() is journal_exists


@pytest.mark.parametrize("name", [".", "..", "bad/name", "bad\\name", "bad\x00name", "bad\nname"])
def test_create_user_rejects_unsafe_names_before_loading_storage(monkeypatch, name: str) -> None:
    """Create applies the same persisted-identifier validation as update and rename."""
    from api import api_keys

    monkeypatch.setattr(api_keys, "_resolve_pending_user_update", Mock(return_value=None))
    get_users = Mock()
    monkeypatch.setattr(api_keys, "_get_users", get_users)
    with pytest.raises(HTTPException) as exc_info:
        api_keys.create_user(
            name=name,
            data=api_keys.UserCreateUpdate(exchange="binance", key="key", secret="secret"),
            session=None,
        )
    assert exc_info.value.status_code == 400
    get_users.assert_not_called()


def _blob_path(root: Path, base: str, blob_hash: str) -> Path:
    """Return one content-addressed cluster blob path."""

    digest = blob_hash.removeprefix("sha256:")
    return root / base / "sha256" / digest[:2] / f"{digest}.json"


def test_users_save_records_api_key_cluster_secret_blob(monkeypatch, tmp_path: Path) -> None:
    """Saving api-keys.json writes redacted payload and restricted secret blobs."""

    pb7 = tmp_path / "pb7"
    pb7.mkdir()
    (pb7 / "api-keys.json").write_text(
        '{"tradfi":{"provider":"tiingo","api_key":"tradfi-vault-secret"}}',
        encoding="utf-8",
    )
    (tmp_path / "pbgui.ini").write_text("[main]\npbname = local-pbgui\n", encoding="utf-8")
    monkeypatch.setattr(user_module, "PBGDIR", str(tmp_path))
    monkeypatch.setattr(user_module.pbgui_purefunc, "pbgui_ini_path", lambda: tmp_path / "pbgui.ini")
    monkeypatch.setattr(user_module, "pb7dir", lambda: str(pb7))
    monkeypatch.setattr(user_module, "is_pb7_installed", lambda: True)

    users = user_module.Users()
    assert "tradfi" not in users.list()
    assert "tradfi" not in users._top_level_extras
    user = user_module.User()
    user.name = "api_user"
    user.exchange = "binance"
    user.key = "public-key"
    user.secret = "super-secret"
    users.users.append(user)

    users.save()

    cluster_root = default_cluster_root(tmp_path)
    identity = read_local_identity(cluster_root)
    operations = load_operations(cluster_root)
    api_op = next(item for item in operations if item["op"] == "UPSERT_API_KEYS")
    desired = rebuild_materialized_state(cluster_root, write=False)["desired_state"]["api_keys"]
    payload_blob = _blob_path(cluster_root, "config_blobs", api_op["payload_hash"])
    secret_blob = _blob_path(cluster_root, "secret_blobs", api_op["secret_blob_hash"])

    assert desired["serial"] == 1
    assert identity["created_from_pbname"] == "local-pbgui"
    assert desired["secret_blob_hash"] == api_op["secret_blob_hash"]
    assert payload_blob.is_file()
    assert secret_blob.is_file()
    assert b"super-secret" not in payload_blob.read_bytes()
    assert b"super-secret" in secret_blob.read_bytes()
    assert b"tradfi-vault-secret" not in secret_blob.read_bytes()
    assert stat.S_IMODE(secret_blob.stat().st_mode) == 0o600
    assert stat.S_IMODE(secret_blob.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE((cluster_root / "secret_blobs").stat().st_mode) == 0o700
    assert stat.S_IMODE((tmp_path / "data" / "api-keys").stat().st_mode) == 0o700
    assert stat.S_IMODE((pb7 / "api-keys.json").stat().st_mode) == 0o600
    assert all(
        stat.S_IMODE(path.stat().st_mode) == 0o600
        for path in (tmp_path / "data" / "api-keys").glob("*.json")
    )
    saved_payload = json.loads((pb7 / "api-keys.json").read_text(encoding="utf-8"))
    assert saved_payload["_api_serial"] == 1
    assert saved_payload["tradfi"]["api_key"] == "tradfi-vault-secret"


def test_hl_expiry_preview_uses_copy_without_persisting_override(monkeypatch) -> None:
    """Unsaved private keys are checked on a copy without cache persistence."""
    from api import api_keys

    user = SimpleNamespace(
        name="alice",
        exchange="hyperliquid",
        private_key="stored-key",
        extra={"existing": True},
    )
    users = SimpleNamespace(find_user=lambda name: user if name == "alice" else None)
    calls = []

    def fake_check(checked_user, users_obj=None):
        calls.append((checked_user, users_obj))
        return api_keys.HLExpiryInfo(name=checked_user.name, status="ok")

    monkeypatch.setattr(api_keys, "_get_users", lambda: users)
    monkeypatch.setattr(api_keys, "_check_hl_expiry_single", fake_check)

    saved_result = api_keys.get_hl_expiry_single(name="alice", session=None)
    preview_result = api_keys.preview_hl_expiry_single(
        name="alice",
        override=api_keys.HLExpiryOverride(private_key="unsaved-key"),
        session=None,
    )

    assert saved_result.status == "ok"
    assert preview_result.status == "ok"
    assert calls[0] == (user, users)
    preview_user, preview_users_obj = calls[1]
    assert preview_user is not user
    assert preview_user.private_key == "unsaved-key"
    assert preview_user.extra == user.extra
    assert preview_user.extra is not user.extra
    assert preview_users_obj is None
    assert user.private_key == "stored-key"


def test_hl_expiry_state_records_identity_only_for_saved_key(monkeypatch) -> None:
    """Bind persisted expiry metadata to saved credentials and never to a preview."""
    from api import api_keys

    valid_until = 1_900_000_000_000
    user = SimpleNamespace(
        name="alice",
        exchange="hyperliquid",
        private_key="stored-key",
        wallet_address="0xwallet",
        is_vault=False,
    )
    updates = []
    monkeypatch.setattr(api_keys, "_get_agent_address", lambda _key: "0xagent")
    monkeypatch.setattr(
        api_keys,
        "_query_hl_info",
        lambda _payload: [{"address": "0xagent", "validUntil": valid_until}],
    )
    monkeypatch.setattr(
        api_keys,
        "_commit_expiry_result",
        lambda context, info, **fields: updates.append((context, info, fields)),
    )

    fingerprint = api_keys._hl_credential_fingerprint(user, "0xagent")
    saved = api_keys._check_hl_expiry_single(
        user,
        users_obj=SimpleNamespace(_loaded_api_serial=42),
    )
    preview = api_keys._check_hl_expiry_single(user, users_obj=None)

    assert saved.valid_until == valid_until
    assert preview.valid_until == valid_until
    assert len(updates) == 1
    context, persisted_info, fields = updates[0]
    assert context[:2] == ("alice", "hyperliquid")
    assert context[3] == 42
    assert persisted_info is saved
    assert fields == {"hl_valid_until": valid_until, "hl_credential_fingerprint": fingerprint}
    assert context[2] not in str(saved)


def test_hl_expiry_routes_separate_saved_get_from_preview_post() -> None:
    """Only POST accepts an unsaved private key body for the expiry preview."""
    from api import api_keys

    routes = [route for route in api_keys.router.routes if route.path == "/{name}/hl-expiry"]
    methods = {method for route in routes for method in route.methods}

    assert methods == {"GET", "POST"}
    assert "private_key" not in api_keys.get_hl_expiry_single.__annotations__
    assert api_keys.HLExpiryOverride(private_key="preview-key").private_key == "preview-key"


@pytest.mark.parametrize("exchange", ["hyperliquid", "bybit"])
def test_expiry_persistence_rejects_changed_credentials_without_state_or_cache(
    monkeypatch,
    tmp_path: Path,
    exchange: str,
) -> None:
    """Network results cannot populate state or cache after credential replacement."""
    from api import api_keys

    original = _api_key_user(
        exchange=exchange,
        key="old-key",
        secret="old-secret",
        private_key="old-private" if exchange == "hyperliquid" else None,
        wallet_address="0xwallet" if exchange == "hyperliquid" else None,
    )
    replacement = _api_key_user(
        exchange=exchange,
        key="new-key",
        secret="new-secret",
        private_key="new-private" if exchange == "hyperliquid" else None,
        wallet_address="0xwallet" if exchange == "hyperliquid" else None,
    )
    loaded = SimpleNamespace(_loaded_api_serial=5)
    context = api_keys._capture_expiry_context(original, loaded)
    current_users = SimpleNamespace(
        _loaded_api_serial=6,
        find_user=lambda name: replacement if name == "alice" else None,
    )
    monkeypatch.setattr(api_keys, "_get_users", lambda: current_users)
    monkeypatch.setattr(api_keys, "_PBGDIR", str(tmp_path))
    monkeypatch.setattr(api_keys, "_resolve_pending_user_update", Mock(return_value=None))
    update_state = Mock()
    monkeypatch.setattr(api_keys, "update_user_state", update_state)
    api_keys._hl_expiry_cache.clear()
    api_keys._bybit_expiry_cache.clear()
    info = (
        api_keys.HLExpiryInfo(name="alice", status="ok")
        if exchange == "hyperliquid"
        else api_keys.BybitExpiryInfo(name="alice", status="ok")
    )

    with pytest.raises(HTTPException) as exc_info:
        api_keys._commit_expiry_result(context, info, expires_at="later")

    assert exc_info.value.status_code == 409
    update_state.assert_not_called()
    assert "alice" not in api_keys._hl_expiry_cache
    assert "alice" not in api_keys._bybit_expiry_cache


@pytest.mark.parametrize("exchange", ["hyperliquid", "bybit"])
def test_expiry_state_rejects_fingerprint_from_replaced_credentials(monkeypatch, exchange) -> None:
    """Persisted expiry metadata is visible only to the credential identity that produced it."""
    from api import api_keys

    original = _api_key_user(
        exchange=exchange,
        key="old-key",
        secret="old-secret",
        private_key="old-private" if exchange == "hyperliquid" else None,
        wallet_address="0xwallet" if exchange == "hyperliquid" else None,
    )
    replacement = _api_key_user(
        exchange=exchange,
        key="new-key",
        secret="new-secret",
        private_key="new-private" if exchange == "hyperliquid" else None,
        wallet_address="0xwallet" if exchange == "hyperliquid" else None,
    )
    monkeypatch.setattr(
        api_keys,
        "_get_agent_address",
        lambda private_key: "0xold" if private_key == "old-private" else "0xnew",
    )
    if exchange == "hyperliquid":
        state = {
            "hl_valid_until": 1_900_000_000_000,
            "hl_credential_fingerprint": api_keys._hl_credential_fingerprint(original),
        }
        read_state = api_keys._hl_expiry_from_state
        status_key = "hl_expiry_status"
    else:
        state = {
            "bybit_expires_at": "2030-01-01T00:00:00Z",
            "bybit_credential_fingerprint": api_keys._expiry_credential_fingerprint(original),
        }
        read_state = api_keys._bybit_expiry_from_state
        status_key = "bybit_expiry_status"
    monkeypatch.setattr(api_keys, "get_user_state", lambda _name: state)

    assert read_state(original)[status_key] is not None
    assert all(value is None for value in read_state(replacement).values())


@pytest.mark.parametrize("exchange", ["hyperliquid", "bybit"])
def test_expiry_cache_is_bypassed_after_external_credential_replacement(monkeypatch, exchange) -> None:
    """A fresh TTL cannot serve an expiry result from an older credential generation."""
    from api import api_keys

    old_user = _api_key_user(
        exchange=exchange,
        key="old-key",
        secret="old-secret",
        private_key="old-private" if exchange == "hyperliquid" else None,
    )
    new_user = _api_key_user(
        exchange=exchange,
        key="new-key",
        secret="new-secret",
        private_key="new-private" if exchange == "hyperliquid" else None,
    )

    class Users:
        """Iterable credential snapshot with the serial used by expiry checks."""

        _loaded_api_serial = 2

        def __iter__(self):
            return iter([new_user])

    users = Users()
    old_fingerprint = api_keys._expiry_credential_fingerprint(old_user)
    refreshed = []
    if exchange == "hyperliquid":
        old_info = api_keys.HLExpiryInfo(name="alice", status="expired")
        new_info = api_keys.HLExpiryInfo(name="alice", status="ok")
        monkeypatch.setattr(api_keys, "_hl_expiry_cache", {"alice": old_info})
        monkeypatch.setattr(api_keys, "_hl_expiry_cache_fingerprints", {"alice": old_fingerprint})
        monkeypatch.setattr(api_keys, "_hl_expiry_cache_ts", api_keys.time.time())
        monkeypatch.setattr(
            api_keys,
            "_check_hl_expiry_single",
            lambda user, users_obj=None: refreshed.append((user, users_obj)) or new_info,
        )
        refresh = api_keys._refresh_hl_expiry_cache
    else:
        old_info = api_keys.BybitExpiryInfo(name="alice", status="expired")
        new_info = api_keys.BybitExpiryInfo(name="alice", status="ok")
        monkeypatch.setattr(api_keys, "_bybit_expiry_cache", {"alice": old_info})
        monkeypatch.setattr(api_keys, "_bybit_expiry_cache_fingerprints", {"alice": old_fingerprint})
        monkeypatch.setattr(api_keys, "_bybit_expiry_cache_ts", api_keys.time.time())
        monkeypatch.setattr(
            api_keys,
            "_check_bybit_expiry_single",
            lambda user, users_obj=None: refreshed.append((user, users_obj)) or new_info,
        )
        refresh = api_keys._refresh_bybit_expiry_cache
    monkeypatch.setattr(api_keys, "_publish_expiry_cache", lambda *_args, **_kwargs: None)

    result = refresh(users)

    assert result == {"alice": new_info}
    assert refreshed == [(new_user, users)]
