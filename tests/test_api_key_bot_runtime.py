"""Read-only account runtime projections using isolated PB7/PB8 observations."""
from types import SimpleNamespace

from api import api_keys, v7_instances, v8_instances


def test_runtime_projection_maps_accounts_and_omits_config(monkeypatch):
    """Both versions may use one account; only runtime fields leave the projection."""
    monkeypatch.setattr(v7_instances, '_load_local_instances', lambda: [
        dict(name='bot', user='account', running_on=['host-b', 'host-a', 'host-a'],
             status='synced', enabled_on='host-a', secret='must-not-leak')])
    monkeypatch.setattr(v7_instances, '_enrich_with_vps_data', lambda rows: rows)
    monkeypatch.setattr(v8_instances, '_list_instances', lambda: [
        dict(name='account', running_on=[], enabled_on='disabled', status='disabled')])
    result = api_keys._get_bot_runtime()
    assert result == {'account': [
        dict(pb_version=7, running_on=['host-a', 'host-b'], status='synced', enabled_on='host-a'),
        dict(pb_version=8, running_on=[], status='disabled', enabled_on='disabled')]}


def test_runtime_failure_preserves_other_version(monkeypatch):
    """An unavailable provider must not hide healthy runtime observations."""
    def fail():
        """Simulate unavailable monitoring without touching runtime files."""
        raise OSError('unavailable')
    monkeypatch.setattr(v7_instances, '_load_local_instances', fail)
    monkeypatch.setattr(api_keys, '_log', lambda *args, **kwargs: None)
    monkeypatch.setattr(v8_instances, '_list_instances', lambda: [
        dict(name='account', running_on=['host'], status='synced', enabled_on='host')])
    assert api_keys._get_bot_runtime()['account'][0]['running_on'] == ['host']


def test_list_preserves_disabled_account_protection(monkeypatch):
    """Usage remains independent of whether a bot currently runs."""
    monkeypatch.setattr(api_keys, '_recover_user_update_or_409', lambda: None)
    monkeypatch.setattr(api_keys, '_get_users', lambda: [SimpleNamespace(name='account')])
    monkeypatch.setattr(api_keys, '_get_in_use_names', lambda: {'account'})
    monkeypatch.setattr(api_keys, '_get_bot_runtime', lambda: {'account': [dict(status='disabled')]})
    monkeypatch.setattr(api_keys, '_user_to_summary', lambda user, in_use: api_keys.UserSummary(
        name=user.name, exchange='hyperliquid', in_use=in_use))
    summary = api_keys.list_users(session=None)[0]
    assert summary.in_use is True
    assert summary.bot_runtime == [dict(status='disabled')]
