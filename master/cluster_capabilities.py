"""Short-lived, self-signed public capability evidence for allowed Cluster routes."""

from __future__ import annotations

import json
import re
import time
from pathlib import Path

from cluster_credentials import ClusterCredentialError, load_node_key_material, sign_operation, verify_operation
from file_lock import advisory_file_lock
from logging_helpers import human_log as _log
from master.cluster_state import (
    PB8_OPERATION_CAPABILITY, PB9_OPERATION_CAPABILITY,
    read_local_identity, read_materialized_state,
)
from secure_files import atomic_write_private_text, ensure_private_directory

SERVICE = "PBCluster"
DOMAIN = "pbcluster-capabilities-v1"
MAX_AGE = 120
MAX_PROOFS = 256
MAX_BYTES = 512 * 1024
_FIELDS = frozenset({
    "domain", "cluster_id", "node_id", "issued_at", "expires_at", "capabilities",
    "signer_id", "signing_key_id", "signature_version", "signature_algorithm", "signature",
})


def _context(root: Path) -> tuple[dict, dict]:
    """Use the existing authenticated materialized membership as the trust anchor."""
    identity = read_local_identity(root)
    state = read_materialized_state(root)
    return identity, (state.get("cluster_nodes") or {}).get("nodes") or {}


def _verified(proof, identity: dict, nodes: dict, now: int) -> bool:
    """Reject foreign, stale, future, removed, rotated or forged attestations."""
    if not isinstance(proof, dict) or set(proof) != _FIELDS:
        return False
    node_id = proof.get("node_id")
    if not isinstance(node_id, str) or proof.get("signer_id") != node_id:
        return False
    node = nodes.get(node_id)
    key_id = proof.get("signing_key_id")
    if (not isinstance(key_id, str) or not key_id or len(key_id) > 128
            or type(proof.get("signature_version")) is not int or proof["signature_version"] != 1
            or proof.get("signature_algorithm") != "Ed25519"
            or not isinstance(proof.get("signature"), str) or len(proof["signature"]) != 88):
        return False
    if (not isinstance(node, dict) or node.get("enabled") is False
            or node.get("state_replica") is False
            or proof.get("domain") != DOMAIN
            or proof.get("cluster_id") != identity.get("cluster_id")
            or not node.get("signing_public_key")
            or proof.get("signing_key_id") != node.get("signing_key_id")):
        return False
    issued, expires = proof.get("issued_at"), proof.get("expires_at")
    if (type(issued) is not int or type(expires) is not int or issued <= 0
            or issued > now + 5 or not issued < expires <= issued + MAX_AGE or now >= expires):
        return False
    capabilities = proof.get("capabilities")
    if (not isinstance(capabilities, list) or len(capabilities) > 16
            or any(not isinstance(c, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", c) for c in capabilities)
            or len(set(capabilities)) != len(capabilities)):
        return False
    try:
        verify_operation(proof, node["signing_public_key"])
    except (ClusterCredentialError, ValueError, TypeError):
        return False
    return True


def _cache_path(root: Path) -> Path:
    """Keep the cache and its transaction lock below the owned Cluster directory."""
    path = root / "capability_evidence.json"
    if root.is_symlink() or path.is_symlink() or path.with_name(path.name + ".lock").is_symlink():
        raise ValueError("Capability cache must not contain symlinks")
    return path


def _read_cache(root: Path, identity: dict, nodes: dict, now: int) -> dict:
    """Read a bounded atomic cache; invalid cache state never establishes support."""
    path = _cache_path(root)
    if not path.exists():
        return {}
    try:
        with path.open("rb") as handle:
            raw = handle.read(MAX_BYTES + 1)
        if len(raw) > MAX_BYTES:
            raise ValueError("Capability cache exceeds limit")
        rows = json.loads(raw)
        if not isinstance(rows, list) or len(rows) > MAX_PROOFS:
            raise ValueError("Invalid capability cache")
    except (OSError, ValueError, RecursionError) as exc:
        _log(SERVICE, f"Capability evidence cache unavailable: {type(exc).__name__}", level="WARNING")
        return {}
    return {p["node_id"]: p for p in rows if _verified(p, identity, nodes, now)}


def capability_evidence(root: Path, *, now: int | None = None) -> dict:
    """Export verified public proofs and a fresh local self-attestation, without SSH."""
    root = Path(root)
    at = int(time.time()) if now is None else now
    identity, nodes = _context(root)
    proofs = _read_cache(root, identity, nodes, at)
    keys = load_node_key_material(root)
    own = sign_operation({
        "domain": DOMAIN, "cluster_id": identity["cluster_id"], "node_id": identity["node_id"],
        "issued_at": at, "expires_at": at + MAX_AGE,
        "capabilities": ["sealed_credentials_v2", PB8_OPERATION_CAPABILITY, PB9_OPERATION_CAPABILITY],
    }, keys.signing_private_key, signer_id=identity["node_id"])
    if _verified(own, identity, nodes, at):
        proofs[identity["node_id"]] = own
    return {"version": 1, "proofs": _bounded_rows(proofs, local_node_id=identity["node_id"])}


def _bounded_rows(proofs: dict, *, local_node_id: str = "") -> list:
    """Keep recent evidence within both pretty-cache and wire-envelope limits."""
    rows = []
    used = 2  # JSON array brackets; reserve extra room for the wire envelope/newline.
    ordered = sorted(proofs, key=lambda node: (node != local_node_id, -proofs[node]["issued_at"], node))
    for node in ordered:
        document = json.dumps(proofs[node], indent=4)
        size = len(document.encode("utf-8")) + 4 * (document.count("\n") + 1) + 2
        if used + size > MAX_BYTES - 128:
            continue
        rows.append(proofs[node])
        used += size
        if len(rows) >= MAX_PROOFS:
            break
    return rows


def merge_capability_evidence(root: Path, envelope, *, now: int | None = None) -> int:
    """Merge authenticated evidence under one reentrant cross-process transaction."""
    if (not isinstance(envelope, dict) or type(envelope.get("version")) is not int or envelope.get("version") != 1
            or not isinstance(envelope.get("proofs"), list) or len(envelope["proofs"]) > MAX_PROOFS
            or len(json.dumps(envelope, allow_nan=False).encode("utf-8")) > MAX_BYTES):
        raise ValueError("Invalid capability evidence envelope")
    root = Path(root)
    at = int(time.time()) if now is None else now
    ensure_private_directory(root)
    path = _cache_path(root)
    with advisory_file_lock(path):
        # Re-read membership and cache under the lock; never extend signed timestamps.
        identity, nodes = _context(root)
        proofs = _read_cache(root, identity, nodes, at)
        accepted = 0
        for proof in envelope["proofs"]:
            if not _verified(proof, identity, nodes, at):
                continue
            old = proofs.get(proof["node_id"])
            if old and old["issued_at"] > proof["issued_at"]:
                continue
            if old and old["issued_at"] == proof["issued_at"]:
                # A same-second negative statement must not be replaced by a positive one.
                if not set(proof["capabilities"]).issubset(old["capabilities"]):
                    continue
            proofs[proof["node_id"]] = proof
            accepted += 1
        rows = _bounded_rows(proofs)
        if rows or path.exists():
            atomic_write_private_text(path, json.dumps(rows, indent=4) + "\n")
    return accepted


def verified_capability_flags(root: Path, *, now: int | None = None) -> dict:
    """Reverify current signer, membership and expiry at the point of use."""
    root = Path(root)
    at = int(time.time()) if now is None else now
    identity, nodes = _context(root)
    return {node_id: {
        "pb8_capability": PB8_OPERATION_CAPABILITY in proof["capabilities"],
        "pb9_capability": PB9_OPERATION_CAPABILITY in proof["capabilities"],
        "capability_expires_at": proof["expires_at"], "capability_source": "signed_relay",
    } for node_id, proof in _read_cache(root, identity, nodes, at).items()}
