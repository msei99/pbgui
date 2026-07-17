"""Static regression checks for Cluster history-retention controls."""

from pathlib import Path


HTML = Path("frontend/cluster.html").read_text(encoding="utf-8")


def test_cluster_retention_ui_is_explicit_and_fail_safe() -> None:
    """The page defaults to report-only and confirms destructive policy changes."""

    assert 'data-cluster-section="retention"' in HTML
    assert 'value="report_only"' in HTML
    assert 'id="retention-history-days"' in HTML
    assert "fetchJson('/retention/report')" in HTML
    assert "fetchJson('/retention/settings'" in HTML
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
