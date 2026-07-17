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
    assert "Local Garbage Blobs" in HTML
    assert "Local Garbage Size" in HTML
    assert "blobGc.reachable_digest" in HTML
    assert "blobGc.reachable_blobs" in HTML
    assert "Blob GC Projection" in HTML
    assert "blobGc.deleted_blobs" in HTML
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
    assert "Status refreshes every 5 seconds" in HTML
    assert "retention maintenance runs automatically at least hourly" in HTML
    assert "Verify All Nodes" in HTML
    assert "Cluster-wide retention verification" in HTML
    assert "Automatic local cleanup" in HTML
    assert "Local retention healthy" in HTML
    assert "nodes_cleanup_verified" in HTML
    assert "automatic_cleanup" in HTML
    assert "Safety delay" not in HTML
