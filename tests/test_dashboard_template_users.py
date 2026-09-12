"""Template instantiation remaps only persisted widget user-selection keys."""

import json

import pytest
from fastapi import HTTPException
from api import dashboards
import pbgui_purefunc


@pytest.mark.parametrize('target', ['', 'alice', 'bob'])
def test_named_template_targets_selected_user(tmp_path, monkeypatch, target):
    """Named dashboards retain layout and source template while remapping users."""
    monkeypatch.setattr(pbgui_purefunc, 'PBGDIR', tmp_path)
    template = dashboards._template_file('source')
    config = {'rows': 1, 'cols': 2, 'dashboard_balance_users_1_1': ['author'],
              'dashboard_pnl_users_1_2': ['ALL'], 'dashboard_type_1_1': 'BALANCE'}
    template.write_text(json.dumps(config))
    result = dashboards.dashboards_from_template({'template': 'source', 'name': 'copy', 'user': target}, session=None)
    assert result['created'] == ['copy']
    saved = json.loads(dashboards._dashboard_file('copy').read_text())
    assert saved['dashboard_balance_users_1_1'] == ([target] if target else ['author'])
    assert saved['dashboard_pnl_users_1_2'] == ([target] if target else ['ALL'])
    assert saved['dashboard_type_1_1'] == 'BALANCE'
    assert json.loads(template.read_text()) == config


@pytest.mark.parametrize('target', [[], None, '../alice', 'a\x00b'])
def test_invalid_target_user_is_rejected(tmp_path, monkeypatch, target):
    """Invalid target identifiers cannot reach file creation."""
    monkeypatch.setattr(pbgui_purefunc, 'PBGDIR', tmp_path)
    with pytest.raises(HTTPException) as error:
        dashboards.dashboards_from_template({'template': 'source', 'name': 'copy', 'user': target}, session=None)
    assert error.value.status_code == 400
    assert not dashboards._dashboard_file('copy').exists()
