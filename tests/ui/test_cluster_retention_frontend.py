"""Static regression checks for Cluster history-retention controls."""

from pathlib import Path


HTML = Path("frontend/cluster.html").read_text(encoding="utf-8")


def test_cluster_retention_ui_is_explicit_and_fail_safe() -> None:
    """The page defaults to report-only and confirms destructive policy changes."""

    assert 'data-cluster-section="retention"' in HTML
    assert 'value="report_only"' in HTML
    assert 'value="oplog_and_blobs">Enabled (automatic)' in HTML
    assert 'value="oplog">' not in HTML
    assert 'id="retention-history-days"' in HTML
    assert "fetchJson('/retention/report')" in HTML
    assert "fetchJson('/retention/settings'" in HTML
    assert "Required Blob Set" in HTML
    assert "Remaining Local Garbage" in HTML
    assert "Remaining Garbage Size" in HTML
    assert "blobGc.reachable_digest" in HTML
    assert "blobGc.reachable_blobs" in HTML
    assert "Blob GC Result" in HTML
    assert "blobGc.deleted_blobs" in HTML
    assert "blobGc.eligible_blobs || 0) - Number(blobGc.deleted_blobs" in HTML
    assert "window.PBGuiConfirm" in HTML
    assert "blob_gc_enabled" not in HTML
    assert "window.confirm(" not in HTML
    assert "window.alert(" not in HTML


def test_cluster_page_uses_cookie_authentication_without_rendered_token() -> None:
    """Cluster browser requests never expose or manually forward a session token."""

    assert "%%TOKEN%%" not in HTML
    assert "window.TOKEN" not in HTML
    assert "headers.Authorization" not in HTML
    assert "Bearer " not in HTML


def test_retention_report_ignores_stale_async_responses() -> None:
    """A prior report response cannot overwrite a newer requested report."""

    assert "var retentionReportGeneration = 0;" in HTML
    assert "var requestGeneration = ++retentionReportGeneration;" in HTML
    assert "requestGeneration !== retentionReportGeneration" in HTML


def test_automatic_retention_status_uses_existing_five_second_feed() -> None:
    """Cleanup lifecycle status renders automatically without remote diagnostics."""

    assert 'id="retention-runtime"' in HTML
    assert "function renderRetentionRuntime(status)" in HTML
    assert "sync.history_retention" in HTML
    assert "renderRetentionRuntime(parts[0]);" in HTML
    assert "Cluster health is collected automatically during normal PBCluster peer sync" in HTML
    assert "cleanup runs at least hourly" in HTML
    assert "Refresh Node Details" in HTML
    assert "Detailed node diagnostics" in HTML
    assert "Automatic cluster cleanup" in HTML
    assert "Cluster retention healthy" in HTML
    assert "Cluster retention blocked" in HTML
    assert "clusterRetention.nodes_healthy" in HTML
    assert "oplog.retained_operations" in HTML
    assert "nodes_cleanup_verified" in HTML
    assert "automatic_cleanup" in HTML
    assert "Safety delay" not in HTML
