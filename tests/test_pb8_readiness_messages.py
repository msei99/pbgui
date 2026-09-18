"""Cluster publication failures explain the observed cause without guessing upgrades."""
import json
import time

import pytest

from api import v8_instances


@pytest.mark.parametrize('timestamp,peer,expected', [
    (None, {}, 'older than two minutes'),
    ('invalid', {}, 'older than two minutes'),
    ('fresh', {'status': 'error', 'ok': False}, 'last Cluster sync failed'),
    ('fresh', {'status': 'backoff', 'pb8_capability': False}, 'last Cluster sync failed'),
    ('fresh', {}, 'no PB8 capability confirmation'),
    ('fresh', {'ok': True, 'pb8_capability': False}, 'handshake reports no PB8 support'),
])
def test_readiness_failure_explains_observed_cause(tmp_path, monkeypatch, timestamp, peer, expected):
    """Unknown support is distinct from a successful negative capability handshake."""
    monkeypatch.setattr(v8_instances, '_cluster_root', lambda: tmp_path)
    monkeypatch.setattr(v8_instances, '_cluster_nodes', lambda: {'remote': {'pbname': 'test-host'}})
    (tmp_path / 'sync_status.json').write_text(json.dumps({
        'finished_at': int(time.time()) if timestamp == 'fresh' else timestamp,
        'peers': [{'node_id': 'remote', **peer}],
    }))
    with pytest.raises(v8_instances.HTTPException) as exc:
        v8_instances._ensure_pb8_cluster_rollout_ready({'node_id': 'local'})
    assert exc.value.status_code == 409
    assert expected in exc.value.detail
    assert 'test-host' in exc.value.detail
    assert ('Update PBGui' in exc.value.detail) == (expected == 'handshake reports no PB8 support')
