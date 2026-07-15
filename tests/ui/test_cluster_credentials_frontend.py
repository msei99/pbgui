"""Static checks for Cluster credential lifecycle UI behavior."""

from __future__ import annotations

from pathlib import Path


def test_cluster_credentials_ui_has_guarded_explicit_actions() -> None:
    """Credential controls use shared dialogs and stale-request generation guards."""

    source = (Path(__file__).parents[2] / "frontend" / "cluster.html").read_text(
        encoding="utf-8"
    )

    assert 'data-cluster-section="credentials"' in source
    assert 'id="credential-rewrap-btn"' in source
    assert 'id="credential-rotate-btn"' in source
    assert "credentialActionGeneration" in source
    assert "clusterLoadGeneration" in source
    assert "window.PBGuiConfirm" in source
    assert "window.confirm(" not in source
    assert "/credentials/rewrap" in source
    assert "/credentials/rotate-local-key" in source
    assert "/remote-materialize-credentials/" in source
    assert 'id="credential-ack-matrix"' in source
    assert "Acknowledged / Desired Secret Generation" in source
    assert "missing_node_ids" in source
    assert "unfreeze_status" in source
    assert "Assigned Authority" in source
    assert "Migration Candidate" in source
    assert "Scan ACK" in source
    assert "TradFi Projection ACK" in source
    assert "migration_blockers" in source
    assert "may retain learned CMC or TradFi plaintext" in source
    assert "rotation is optional and is not automatic" in source
    assert 'href="/api/services/main_page"' in source
    assert 'href="/api/api-keys/main_page#tradfi"' in source
    assert "affected_credentials" in source
    assert "credentialActionBusy" in source
    assert "credentialPendingAction" in source
    assert "resolvePendingCredentialAction" in source
    assert "'/credentials/operations/' + encodeURIComponent(pending.requestId)" in source
    assert "record.status === 'complete'" in source
    assert "if (same) return { context: pending, payload: null };" in source
    assert "A different mutation cannot start until it completes" in source
    assert "request_id=' + encodeURIComponent(context.requestId)" in source
    assert "JSON.stringify({ request_id: context.requestId })" in source
    assert "cluster_remote_materialize_credentials" in source
    assert "cluster_credential_rewrap" in source
    assert "cluster_rotate_local_credential_key" in source
    assert "JSON.stringify(credentialPendingAction)" not in source
    assert "sessionStorage" not in source
    assert "setCredentialMutationBusy(true)" in source
    assert "@media (max-width: 560px)" in source
    assert ".credential-grid { grid-template-columns:1fr; }" in source
    assert "ciphertext" in source  # The panel explicitly states that ciphertext is never shown.
