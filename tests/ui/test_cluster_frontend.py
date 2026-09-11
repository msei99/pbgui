"""Static regression checks for Cluster Sync polling and audit controls."""

from pathlib import Path


HTML = Path("frontend/cluster.html").read_text(encoding="utf-8")


def test_oplog_targets_cover_cluster_v2_identifiers() -> None:
    """Credential and policy operations retain an actionable target label."""

    assert "function operationTarget(op)" in HTML
    for field in (
        "op.secret_id",
        "op.key_id",
        "op.quota_domain_id",
        "op.authority_node_id",
        "op.profile_id",
        "op.provider",
        "op.mode",
    ):
        assert field in HTML
    assert "target: operationTarget" in HTML
    assert "var target = operationTarget(op);" in HTML


def test_oplog_failure_isolated_from_primary_cluster_poll() -> None:
    """A failed audit-history request does not suppress status or node updates."""

    assert "fetchJson('/oplog?limit=50').catch(function (err)" in HTML
    assert "oplog_error" in HTML
    assert "Local cluster history is temporarily unavailable" in HTML


def test_bootstrap_apply_uses_preview_generation() -> None:
    """Bootstrap apply remains bound to the exact preview the operator confirmed."""

    assert "var expectedGeneration = Number(lastBootstrapPlan && lastBootstrapPlan.generation);" in HTML
    assert "runBootstrapApply(expectedGeneration);" in HTML
    assert "'/bootstrap?expected_generation=' + encodeURIComponent(expectedGeneration)" in HTML


def test_cluster_projects_pb8_state_and_node_capability() -> None:
    """Cluster UI renders PB8 desired state separately and exposes rollout readiness."""

    assert 'id="pb8-instances-table"' in HTML
    assert 'id="pb8-tombstones-table"' in HTML
    assert "renderInstanceTable(payload, 'pb8_instances'" in HTML
    assert "renderTombstoneTable(payload, 'pb8_tombstones'" in HTML
    assert "function pb8NodeCapability(node)" in HTML
    assert "typeof peer.pb8_capability === 'boolean'" in HTML
    assert "label: 'Unknown'" in HTML
    assert "PB8 Instances" in HTML


def test_cluster_keeps_v7_and_pb8_table_sort_state_separate() -> None:
    """Sorting either runtime table must not mutate the other runtime's ordering."""

    table_sort = HTML.split("var tableSort = {", 1)[1].split("};", 1)[0]
    assert "'pb8-instances': { key: 'instance', dir: 'asc' }" in table_sort
    assert "'pb8-tombstones': { key: 'instance', dir: 'asc' }" in table_sort
    assert "renderInstanceTable(payload, 'pb8_instances', 'pb8-instances-table', 'pb8-instances', 'PB8')" in HTML
    assert "renderTombstoneTable(payload, 'pb8_tombstones', 'pb8-tombstones-table', 'pb8-tombstones', 'PB8')" in HTML


def test_cluster_pb8_lifecycle_controls_use_shared_dialogs_and_generation() -> None:
    """PB8 lifecycle controls remain explicit, current-state bound, and capability filtered."""

    assert 'data-pb8-action="' in HTML
    assert "function pb8ActionRequest(name, action, targetNodeId)" in HTML
    assert "expected_generation: generation" in HTML
    assert "window.PBGuiDialogs.choose" in HTML
    assert "window.PBGuiDialogs.confirm" in HTML
    assert "pb8NodeCapability(node).supported === true" in HTML
