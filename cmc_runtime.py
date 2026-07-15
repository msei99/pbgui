"""Production construction for desired-state-aware CMC pooling and leases."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Mapping

from cmc_leases import CmcLeaseAuthority, CmcLeaseProvider, ClusterMailbox
from cmc_pool import CmcPoolClient
from credential_rolling_bootstrap import bootstrap_local_legacy_credentials
from credential_store import CredentialStore
from master.cluster_state import default_cluster_root, read_local_identity, rebuild_materialized_state


SERVICE = "CmcRuntime"


def read_cmc_cluster_snapshot(project_root: Path | str) -> dict[str, Any] | None:
    """Read one local materialized cluster snapshot without requiring cluster mode."""

    cluster_root = default_cluster_root(Path(project_root))
    try:
        materialized = rebuild_materialized_state(cluster_root, write=False)
    except Exception:
        return None
    desired = materialized.get("desired_state") if isinstance(materialized, Mapping) else None
    pool = desired.get("cmc_pool") if isinstance(desired, Mapping) else None
    if not isinstance(pool, Mapping):
        return None
    return dict(materialized)


def build_cmc_pool_client(
    project_root: Path | str | None = None,
    *,
    credential_store: CredentialStore | None = None,
) -> CmcPoolClient:
    """Build the canonical local pool and attach cluster leasing when usable."""

    root = Path(
        os.path.abspath(
            Path(project_root or os.environ.get("PBGUI_DIR") or Path(__file__).resolve().parent)
            .expanduser()
        )
    )
    bootstrap_local_legacy_credentials(root)
    store = credential_store or CredentialStore(root / "data" / "credentials")
    snapshot_loader = lambda: read_cmc_cluster_snapshot(root)
    initial = snapshot_loader()
    lease_provider = None

    try:
        desired = initial.get("desired_state") if isinstance(initial, Mapping) else None
        pool = desired.get("cmc_pool") if isinstance(desired, Mapping) else None
        raw_routes = pool.get("authorities") if isinstance(pool, Mapping) else None
        routes = {
            str(domain_id): dict(route)
            for domain_id, route in (raw_routes or {}).items()
            if isinstance(route, Mapping)
            and not route.get("conflicted")
            and route.get("authority_node_id")
            and int(route.get("authority_epoch") or 0) > 0
        }
        if routes:
            cluster_root = default_cluster_root(root)
            local_node_id = str(read_local_identity(cluster_root)["node_id"])
            mailbox = ClusterMailbox(cluster_root)
            authority = CmcLeaseAuthority(
                store.root / "cmc_pool" / "leases",
                authority_epochs={
                    domain_id: int(route["authority_epoch"])
                    for domain_id, route in routes.items()
                },
            )
            first_domain = sorted(routes)[0]
            first = routes[first_domain]
            lease_provider = CmcLeaseProvider(
                mailbox,
                str(first["authority_node_id"]),
                authority=authority,
                authority_epoch=int(first["authority_epoch"]),
                quota_domain_id=first_domain,
                authority_routes=routes,
                snapshot_loader=snapshot_loader,
            )
    except Exception:
        lease_provider = None

    return CmcPoolClient(
        credential_store=store,
        state_root=store.root / "cmc_pool",
        lease_provider=lease_provider,
        desired_state_provider=snapshot_loader,
    )


__all__ = ["build_cmc_pool_client", "read_cmc_cluster_snapshot"]
