"""Offline atomic Hyperliquid shared-key rotation, conflicts, and crash recovery."""
from copy import deepcopy
import json
from pathlib import Path
from unittest.mock import Mock

from fastapi import FastAPI, HTTPException, Response
from fastapi.testclient import TestClient
import pytest

from api import api_keys
import api_key_state
from test_user_cluster_api_keys import _install_file_transaction_fixtures, _api_key_user

OLD_KEY = 'ab' * 32
NEW_KEY = 'cd' * 32
OTHER_KEY = 'ef' * 32


@pytest.fixture
def group(tmp_path, monkeypatch):
    """Use only synthetic keys and isolated files, locks, cache and log sinks."""
    _, state, credentials, state_file, journal, failure = _install_file_transaction_fixtures(monkeypatch, tmp_path, None)
    records = []
    for name, key, vault, exchange in (
        ('vault-a', OLD_KEY, True, 'hyperliquid'),
        ('vault-b', '0x' + OLD_KEY.upper(), True, 'hyperliquid'),
        ('main', OLD_KEY, False, 'hyperliquid'),
        ('other', OTHER_KEY, True, 'hyperliquid'),
        ('empty', None, True, 'hyperliquid'),
        ('bybit', OLD_KEY, False, 'bybit'),
    ):
        record = vars(_api_key_user(name=name, exchange=exchange, private_key=key,
                    wallet_address='0x' + name, is_vault=vault, quote='USDC', options={'custom': name}))
        records.append(record)
    credentials.write_text(json.dumps({'generation': 1, 'users': records}))
    state_file.write_text(json.dumps({'version': 1, 'users': {
        item['name']: {'hl_valid_until': 123, 'hl_credential_fingerprint': 'old-fingerprint', 'custom': item['name']}
        for item in records}}))
    monkeypatch.setattr(api_keys, '_hl_expiry_cache', {item['name']: {'old': True} for item in records})
    monkeypatch.setattr(api_keys, '_bybit_expiry_cache', {})
    monkeypatch.setattr(api_keys, '_log', Mock())
    return credentials, state_file, journal, failure


def preview(name='vault-a'):
    """Request a secret-free group snapshot using the actual authenticated route body."""
    response = Response()
    result = api_keys.preview_shared_hl_private_key(api_keys.SharedPrivateKeyPreviewRequest(name=name), response, session=None)
    assert response.headers['Cache-Control'] == 'no-store'
    return result


def update(token=None, **overrides):
    """Save one source record, optionally extending to its confirmed shared group."""
    args = dict(exchange='hyperliquid', private_key=NEW_KEY, wallet_address='0xvault-a',
                is_vault=True, quote='USDC', options={'custom': 'vault-a'}, extra={'label': 'old'})
    args.update(overrides)
    return api_keys.update_user(name='vault-a', data=api_keys.UserCreateUpdate(**args, shared_private_key_preview=token), session=None)


def read_records(path):
    """Read isolated fixture records by account name."""
    return {item['name']: item for item in json.loads(path.read_text())['users']}


def test_preview_has_all_matching_accounts_no_keys_and_requires_auth(group):
    """Include main accounts and normalized hex keys, excluding empty and other exchanges."""
    result = preview()
    assert [item['name'] for item in result['accounts']] == ['main', 'vault-a', 'vault-b']
    assert result['accounts'][0]['is_vault'] is False
    assert not any(key in json.dumps(result).lower() for key in (OLD_KEY, NEW_KEY, OTHER_KEY))
    assert preview('empty') == {'accounts': [], 'preview_token': None}
    with pytest.raises(HTTPException) as exc:
        preview('bybit')
    assert exc.value.status_code == 400
    app = FastAPI()
    app.include_router(api_keys.router)
    with TestClient(app) as client:
        response = client.post('/hyperliquid/shared-key/preview', json={'name': 'vault-a'})
    assert response.status_code == 401


def test_shared_update_is_one_save_preserves_settings_and_clears_all_expiry(group):
    """One generation replaces the whole group, preserving unrelated records and state."""
    path, state, journal, _ = group
    before = read_records(path)
    result = update(preview()['preview_token'])
    assert result.shared_key_updated_users == ['main', 'vault-a', 'vault-b']
    after = read_records(path)
    for name in before:
        expected = deepcopy(before[name])
        if name in result.shared_key_updated_users:
            expected['private_key'] = NEW_KEY
            assert api_key_state.get_user_state(name) == {'custom': name}
            assert name not in api_keys._hl_expiry_cache
        else:
            assert api_key_state.get_user_state(name)['hl_valid_until'] == 123
        assert after[name] == expected
    assert json.loads(path.read_text())['generation'] == 2
    assert not journal.exists()
    assert NEW_KEY not in str(result)


def test_opt_out_only_updates_selected_user(group):
    """The default single-account save never expands the group automatically."""
    result = update()
    records = read_records(group[0])
    assert records['vault-a']['private_key'] == NEW_KEY
    assert records['main']['private_key'] == OLD_KEY
    assert records['vault-b']['private_key'] == '0x' + OLD_KEY.upper()
    assert result.shared_key_updated_users == []


@pytest.mark.parametrize('mutation', ['add', 'remove', 'key', 'wallet', 'generation', 'source'])
def test_stale_group_is_rejected_without_writing(group, mutation):
    """No client can apply an earlier group after credentials or identities change."""
    token = preview()['preview_token']
    path = group[0]
    payload = json.loads(path.read_text())
    if mutation == 'add':
        extra = deepcopy(payload['users'][0]); extra['name'] = 'new-vault'; payload['users'].append(extra)
    elif mutation == 'remove':
        payload['users'].pop(1)
    elif mutation == 'generation':
        payload['generation'] += 1
    else:
        member = payload['users'][0 if mutation == 'source' else 1]
        member['wallet_address' if mutation == 'wallet' else 'private_key'] = OTHER_KEY
    path.write_text(json.dumps(payload))
    before = path.read_bytes()
    with pytest.raises(HTTPException) as exc:
        update(token)
    assert exc.value.status_code == 409
    assert path.read_bytes() == before
    assert not group[2].exists()


@pytest.mark.parametrize('token', ['', 'bad', 'é', '0:' + 'f'*64, '999999999999:' + 'a'*64])
def test_invalid_confirmation_never_falls_back_to_single_save(group, token):
    """Malformed, expired and future confirmations fail closed without persisting keys."""
    before = group[0].read_bytes()
    with pytest.raises(HTTPException) as exc:
        update(token)
    assert exc.value.status_code == 409
    assert group[0].read_bytes() == before


@pytest.mark.parametrize('key', ['', 'invalid', '0x' + OLD_KEY.upper()])
def test_invalid_or_unchanged_replacement_does_not_write(group, key):
    """A bulk operation requires an actual syntactically valid new signing key."""
    before = group[0].read_bytes()
    with pytest.raises(HTTPException) as exc:
        update(preview()['preview_token'], private_key=key)
    assert exc.value.status_code == 400
    assert group[0].read_bytes() == before


@pytest.mark.parametrize('failure', ['before', 'after'])
def test_save_interruption_recovers_all_accounts_together(group, failure):
    """Recovery chooses the complete preimage or postimage, including every expiry entry."""
    token = preview()['preview_token']
    before = group[0].read_bytes()
    state_before = group[1].read_bytes()
    group[3]['mode'] = failure
    if failure == 'before':
        with pytest.raises(HTTPException) as exc:
            update(token)
        assert exc.value.status_code == 500
        assert group[0].read_bytes() == before
        assert group[1].read_bytes() == state_before
    else:
        result = update(token)
        assert len(result.shared_key_updated_users) == 3
        for name in result.shared_key_updated_users:
            assert read_records(group[0])[name]['private_key'] == NEW_KEY
            assert api_key_state.get_user_state(name) == {'custom': name}
    assert not group[2].exists()


def test_pending_state_recovers_after_api_restart_and_detects_partial_group(group, monkeypatch):
    """A durable journal retains every member; recovery must verify the entire group."""
    token = preview()['preview_token']
    finish = api_keys.finish_user_state_transaction
    monkeypatch.setattr(api_keys, 'finish_user_state_transaction', Mock(side_effect=OSError('injected failure')))
    with pytest.raises(HTTPException):
        update(token)
    assert group[2].exists()
    assert OLD_KEY not in group[2].read_text() and NEW_KEY not in group[2].read_text()
    monkeypatch.setattr(api_keys, 'finish_user_state_transaction', finish)
    committed = group[0].read_bytes()
    payload = json.loads(committed)
    payload['users'][1]['private_key'] = OTHER_KEY
    group[0].write_text(json.dumps(payload))
    with pytest.raises(api_key_state.ApiKeyStateTransactionConflictError):
        api_keys._resolve_pending_user_update()
    assert group[2].exists()
    group[0].write_bytes(committed)
    assert api_keys._resolve_pending_user_update() == 'committed'
    for name in ('vault-a', 'vault-b', 'main'):
        assert api_key_state.get_user_state(name) == {'custom': name}
    assert not group[2].exists()


def test_shared_update_can_atomically_rename_source(group):
    """The edited source can be renamed while related vault identities remain intact."""
    result = update(preview()['preview_token'], new_name='renamed')
    assert result.shared_key_updated_users == ['main', 'renamed', 'vault-b']
    assert 'vault-a' not in read_records(group[0])
    assert api_key_state.get_user_state('renamed') == {'custom': 'vault-a'}
    assert api_key_state.get_user_state('vault-a') == {}


def test_real_writer_publishes_one_group_and_rotates_only_affected_runtime_revisions(group, tmp_path, monkeypatch):
    """Exercise real User.save, private writer, cluster publication and restart journal."""
    import User as user_module
    import cluster_sync_command
    from credential_runtime import CredentialRuntimeJournal
    from pb7_api_keys import PB7ApiKeysMergeWriter
    from master.cluster_state import load_operations, default_cluster_root

    pb7 = tmp_path / 'pb7'; pb7.mkdir()
    monkeypatch.setattr(user_module, 'PBGDIR', str(tmp_path))
    monkeypatch.setattr(user_module, 'pb7dir', lambda: str(pb7))
    monkeypatch.setattr(user_module, 'is_pb7_installed', lambda: True)
    monkeypatch.setattr(cluster_sync_command, 'PBGDIR', str(tmp_path))
    monkeypatch.setattr(cluster_sync_command, 'pb7dir', lambda: str(pb7))
    monkeypatch.setattr(cluster_sync_command, 'pb8dir', lambda: '')
    ini = tmp_path / 'pbgui.ini'; ini.write_text('[main]\npbname = isolated\n')
    monkeypatch.setattr(user_module.pbgui_purefunc, 'pbgui_ini_path', lambda: ini)
    payload = {'_api_serial': 1}
    for name, record in read_records(group[0]).items():
        payload[name] = {k: v for k, v in record.items() if k not in ('name', 'extra', 'is_vault') and v is not None}
        payload[name].update(record['extra'])
        if record['exchange'] == 'hyperliquid':
            payload[name]['is_vault'] = record['is_vault']
    path = pb7 / 'api-keys.json'
    status = tmp_path / 'data/credentials/pb7_projection.json'
    writer = PB7ApiKeysMergeWriter(path, status)
    writer.write_exchange_payload(payload)
    journal = CredentialRuntimeJournal(path, status)
    before = json.loads(journal.path.read_text())['users']
    monkeypatch.setattr(api_keys, '_get_users', user_module.Users)
    result = update(preview()['preview_token'])
    after = json.loads(journal.path.read_text())['users']
    for name in before:
        assert (before[name]['revision'] != after[name]['revision']) == (name in result.shared_key_updated_users)
    operations = [item for item in load_operations(default_cluster_root(tmp_path)) if item['op'] == 'UPSERT_API_KEYS']
    assert len(operations) == 1
    assert json.loads(path.read_text())['_api_serial'] == 2


def test_used_confirmation_cannot_repeat_rotation(group):
    """Reusing a successful confirmation cannot write a new generation or rotate again."""
    token = preview()['preview_token']
    update(token)
    saved = group[0].read_bytes()
    with pytest.raises(HTTPException) as exc:
        update(token)
    assert exc.value.status_code == 409
    assert group[0].read_bytes() == saved


def test_preview_is_bound_to_source_and_expires(group, monkeypatch):
    """A confirmation is neither transferable across accounts nor valid indefinitely."""
    token = preview('main')['preview_token']
    with pytest.raises(HTTPException) as exc:
        update(token)
    assert exc.value.status_code == 409
    token = preview()['preview_token']
    issued_at = int(token.split(':')[0])
    monkeypatch.setattr(api_keys.time, 'time', lambda: issued_at + 601)
    with pytest.raises(HTTPException) as exc:
        update(token)
    assert exc.value.status_code == 409
    assert read_records(group[0])['vault-a']['private_key'] == OLD_KEY


def test_journal_preparation_failure_restores_all_buffered_users(group, monkeypatch):
    """No mutated in-memory member escapes when creating the durable intent fails."""
    token = preview()['preview_token']
    users = api_keys._get_users()
    original = {user.name: user.private_key for user in users.users}
    monkeypatch.setattr(api_keys, '_get_users', lambda: users)
    monkeypatch.setattr(api_keys, 'begin_user_state_transaction', Mock(side_effect=OSError('injected preparation failure')))
    with pytest.raises(HTTPException) as exc:
        update(token)
    assert exc.value.status_code == 500
    assert {user.name: user.private_key for user in users.users} == original
    assert not group[2].exists()


def test_publication_failure_preserves_whole_saved_group_and_returns_pending(group, monkeypatch):
    """A post-save publication error is reported honestly without rolling back stored keys."""
    from User import ApiKeysPublicationError
    from test_user_cluster_api_keys import _FileUsers

    save = _FileUsers.save

    def save_then_fail_publication(self):
        """Inject a publication outage only after the complete atomic replacement."""
        save(self)
        raise ApiKeysPublicationError('Credentials saved; publication is pending')

    token = preview()['preview_token']
    monkeypatch.setattr(_FileUsers, 'save', save_then_fail_publication)
    with pytest.raises(HTTPException) as exc:
        update(token)
    assert exc.value.status_code == 503
    assert 'publication is pending' in exc.value.detail
    for name in ('vault-a', 'vault-b', 'main'):
        assert read_records(group[0])[name]['private_key'] == NEW_KEY
        assert api_key_state.get_user_state(name) == {'custom': name}
    assert not group[2].exists()
