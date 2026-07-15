"""Tests for PBCluster SSH key bootstrap helpers."""

from __future__ import annotations

import shutil
import stat
from pathlib import Path

import pytest

from master.cluster_ssh_keys import (
    ensure_cluster_ssh_key,
    install_authorized_cluster_key,
    remove_authorized_cluster_key,
)


NODE_ID = "pbgui-node-00000000-0000-4000-8000-000000000010"
PUBLIC_KEY = "ssh-ed25519 aGVsbG8= pbgui-cluster:test"


def test_ensure_cluster_ssh_key_creates_dedicated_files(tmp_path: Path) -> None:
    """Cluster SSH keys are generated under data/cluster/ssh with safe modes."""

    if not shutil.which("ssh-keygen"):
        pytest.skip("ssh-keygen is not available")

    payload = ensure_cluster_ssh_key(tmp_path / "data" / "cluster", node_id=NODE_ID)

    private_key = Path(payload["private_key_path"])
    public_key = Path(payload["public_key_path"])
    assert payload["created"] is True
    assert private_key.is_file()
    assert public_key.is_file()
    assert stat.S_IMODE(private_key.stat().st_mode) == 0o600
    assert stat.S_IMODE(private_key.parent.stat().st_mode) == 0o700
    assert public_key.read_text(encoding="utf-8").strip().endswith(f"pbgui-cluster:{NODE_ID}")


def test_install_authorized_cluster_key_is_idempotent_and_forced(tmp_path: Path) -> None:
    """Installing a peer key writes one forced-command authorized_keys line."""

    pbgdir = tmp_path / "software" / "pbgui"
    authorized_keys = tmp_path / ".ssh" / "authorized_keys"

    first = install_authorized_cluster_key(
        pbgdir=pbgdir,
        source_node_id=NODE_ID,
        source_public_key=PUBLIC_KEY,
        authorized_keys_path=authorized_keys,
    )
    second = install_authorized_cluster_key(
        pbgdir=pbgdir,
        source_node_id=NODE_ID,
        source_public_key=PUBLIC_KEY,
        authorized_keys_path=authorized_keys,
    )
    lines = authorized_keys.read_text(encoding="utf-8").splitlines()

    assert first["changed"] is True
    assert second["changed"] is False
    assert len(lines) == 1
    assert lines[0].startswith("restrict,no-pty,no-agent-forwarding")
    assert "cluster_sync_forced_command.sh" in lines[0]
    assert f"pbgui-cluster:{NODE_ID}" in lines[0]


def test_remove_authorized_cluster_key_removes_only_exact_forced_entry(tmp_path: Path) -> None:
    """Revocation preserves similar comments and unrelated authorized keys."""

    pbgdir = tmp_path / "software" / "pbgui"
    authorized_keys = tmp_path / ".ssh" / "authorized_keys"
    install_authorized_cluster_key(
        pbgdir=pbgdir,
        source_node_id=NODE_ID,
        source_public_key=PUBLIC_KEY,
        authorized_keys_path=authorized_keys,
    )
    exact = authorized_keys.read_text(encoding="utf-8").strip()
    similar = f"ssh-ed25519 aGVsbG8= pbgui-cluster:{NODE_ID}"
    unrelated = "ssh-ed25519 d29ybGQ= personal-key"
    authorized_keys.write_text(f"{exact}\n{similar}\n{unrelated}\n", encoding="utf-8")

    result = remove_authorized_cluster_key(
        pbgdir=pbgdir,
        source_node_id=NODE_ID,
        source_public_key=PUBLIC_KEY,
        authorized_keys_path=authorized_keys,
    )

    assert result["changed"] is True
    assert authorized_keys.read_text(encoding="utf-8").splitlines() == [similar, unrelated]
    assert stat.S_IMODE(authorized_keys.stat().st_mode) == 0o600
