#!/usr/bin/env python3
"""CLI helper for PBCluster SSH key bootstrap."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from master.cluster_ssh_keys import (
    ClusterSSHKeyError,
    ensure_cluster_ssh_key,
    install_authorized_cluster_key,
    to_json,
)
from master.cluster_state import default_cluster_root
from pbgui_purefunc import PBGDIR


def main(argv: list[str] | None = None) -> int:
    """Run the Cluster SSH setup helper."""

    parser = argparse.ArgumentParser(description="PBGui Cluster Sync SSH key setup")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ensure_parser = subparsers.add_parser("ensure-local", help="Ensure the local Cluster Sync key exists")
    ensure_parser.add_argument("--pbgdir", default=str(PBGDIR))
    ensure_parser.add_argument("--node-id", default="")

    install_parser = subparsers.add_parser("install-authorized-key", help="Install one source node key into authorized_keys")
    install_parser.add_argument("--pbgdir", default=str(PBGDIR))
    install_parser.add_argument("--source-node", required=True)
    install_parser.add_argument("--source-public-key", required=True)
    install_parser.add_argument("--authorized-keys", default="")

    args = parser.parse_args(argv)
    try:
        if args.command == "ensure-local":
            root = default_cluster_root(Path(args.pbgdir))
            payload = ensure_cluster_ssh_key(root, node_id=str(args.node_id or "") or None)
        else:
            payload = install_authorized_cluster_key(
                pbgdir=Path(args.pbgdir),
                source_node_id=str(args.source_node),
                source_public_key=str(args.source_public_key),
                authorized_keys_path=Path(args.authorized_keys) if str(args.authorized_keys or "").strip() else None,
            )
    except Exception as exc:
        error = str(exc) if not isinstance(exc, ClusterSSHKeyError) else str(exc)
        sys.stderr.write(to_json({"ok": False, "error": error}) + "\n")
        return 1
    sys.stdout.write(to_json({"ok": True, **payload}) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
