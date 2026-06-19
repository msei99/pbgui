"""Cluster Sync SSH key helpers.

PBCluster uses a dedicated key pair so regular user SSH keys are not part of
the sync contract. The public key is installed on peers with an OpenSSH forced
command that only runs cluster_sync_command.py.
"""

from __future__ import annotations

import base64
import configparser
import hashlib
import json
import os
import platform
import re
import shlex
import subprocess
from pathlib import Path
from typing import Any

from master.cluster_state import default_cluster_root, ensure_local_identity, read_local_identity
from pbgui_purefunc import PBGDIR

KEY_DIR_NAME = "ssh"
KEY_BASENAME = "pbgui_cluster_ed25519"
KEY_COMMENT_PREFIX = "pbgui-cluster:"
SUPPORTED_KEY_TYPES = frozenset({"ssh-ed25519"})
NODE_ID_RE = re.compile(r"^pbgui-node-[0-9a-fA-F-]{36}$")


class ClusterSSHKeyError(RuntimeError):
    """Raised when Cluster Sync SSH key setup fails."""


def ensure_local_cluster_ssh_material(
    pbgdir: Path | str | None = None,
    *,
    role: str = "master",
    pbname: str | None = None,
) -> dict[str, Any]:
    """Ensure local cluster identity and SSH key material exist."""

    root = default_cluster_root(Path(pbgdir or PBGDIR))
    identity = ensure_local_identity(root, role=role, pbname=pbname or _pbname_from_ini(Path(pbgdir or PBGDIR)))
    key = ensure_cluster_ssh_key(root, node_id=str(identity.get("node_id") or ""))
    return {
        "cluster_id": str(identity.get("cluster_id") or ""),
        "node_id": str(identity.get("node_id") or ""),
        "role": str(identity.get("role") or role),
        **key,
    }


def ensure_cluster_ssh_key(cluster_root: Path | str, *, node_id: str | None = None) -> dict[str, Any]:
    """Ensure one local PBCluster SSH key pair exists and return metadata."""

    root = Path(cluster_root)
    key_dir = root / KEY_DIR_NAME
    private_key = key_dir / KEY_BASENAME
    public_key = key_dir / f"{KEY_BASENAME}.pub"
    key_dir.mkdir(parents=True, exist_ok=True)
    _chmod_best_effort(key_dir, 0o700)

    effective_node_id = _effective_node_id(root, node_id)
    comment = f"{KEY_COMMENT_PREFIX}{effective_node_id or 'unjoined'}"
    created = False
    if not private_key.exists():
        _generate_key_pair(private_key, comment)
        created = True
    elif not public_key.exists():
        _write_public_key_from_private(private_key, public_key)

    _chmod_best_effort(private_key, 0o600)
    _chmod_best_effort(public_key, 0o644)
    public = _normalize_public_key(public_key.read_text(encoding="utf-8"), effective_node_id or "unjoined")
    if public_key.read_text(encoding="utf-8").strip() != public:
        _atomic_write_text(public_key, public + "\n", mode=0o644)
    return {
        "created": created,
        "key_dir": str(key_dir),
        "private_key_path": str(private_key),
        "public_key_path": str(public_key),
        "public_key": public,
        "fingerprint": public_key_fingerprint(public),
    }


def install_authorized_cluster_key(
    *,
    pbgdir: Path | str,
    source_node_id: str,
    source_public_key: str,
    authorized_keys_path: Path | str | None = None,
) -> dict[str, Any]:
    """Install one source node public key with a forced Cluster Sync command."""

    source_node = _validate_source_node_id(source_node_id)
    public_key = _normalize_public_key(source_public_key, source_node)
    path = Path(authorized_keys_path) if authorized_keys_path else Path.home() / ".ssh" / "authorized_keys"
    line = build_authorized_key_line(Path(pbgdir), source_node, public_key)
    marker = f"{KEY_COMMENT_PREFIX}{source_node}"

    path.parent.mkdir(parents=True, exist_ok=True)
    _chmod_best_effort(path.parent, 0o700)
    existing = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    matching = [item for item in existing if marker in item]
    if matching == [line]:
        kept = existing
        changed = False
    else:
        kept = [item for item in existing if marker not in item]
        kept.append(line)
        changed = True
    if changed:
        _atomic_write_text(path, "\n".join(kept).rstrip() + "\n", mode=0o600)
    else:
        _chmod_best_effort(path, 0o600)
    return {
        "ok": True,
        "changed": changed,
        "authorized_keys_path": str(path),
        "source_node_id": source_node,
        "fingerprint": public_key_fingerprint(public_key),
    }


def build_authorized_key_line(pbgdir: Path, source_node_id: str, public_key: str) -> str:
    """Build an OpenSSH authorized_keys line for a Cluster Sync peer key."""

    source_node = _validate_source_node_id(source_node_id)
    forced = forced_command(str(Path(pbgdir).resolve()), source_node)
    if '"' in forced or "\n" in forced:
        raise ClusterSSHKeyError("forced command contains unsupported characters")
    options = (
        "restrict,no-pty,no-agent-forwarding,no-X11-forwarding,"
        "no-port-forwarding,no-user-rc,"
        f"command=\"{forced}\""
    )
    return f"{options} {_normalize_public_key(public_key, source_node)}"


def forced_command(pbgdir: str, source_node_id: str) -> str:
    """Return the forced command installed in authorized_keys."""

    source_node = _validate_source_node_id(source_node_id)
    return f"{shlex.quote(str(Path(pbgdir).resolve() / 'cluster_sync_forced_command.sh'))} {source_node}"


def public_key_fingerprint(public_key: str) -> str:
    """Return the OpenSSH SHA256 fingerprint for a public key."""

    parts = str(public_key or "").strip().split()
    if len(parts) < 2:
        raise ClusterSSHKeyError("invalid public key")
    try:
        raw = base64.b64decode(parts[1].encode("ascii"), validate=True)
    except Exception as exc:
        raise ClusterSSHKeyError("invalid public key blob") from exc
    digest = base64.b64encode(hashlib.sha256(raw).digest()).decode("ascii").rstrip("=")
    return f"SHA256:{digest}"


def _generate_key_pair(private_key: Path, comment: str) -> None:
    try:
        subprocess.run(
            ["ssh-keygen", "-t", "ed25519", "-C", comment, "-f", str(private_key), "-N", ""],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except FileNotFoundError as exc:
        raise ClusterSSHKeyError("ssh-keygen is required to create the Cluster Sync SSH key") from exc
    except subprocess.CalledProcessError as exc:
        raise ClusterSSHKeyError((exc.stderr or exc.stdout or "ssh-keygen failed").strip()) from exc


def _write_public_key_from_private(private_key: Path, public_key: Path) -> None:
    try:
        result = subprocess.run(
            ["ssh-keygen", "-y", "-f", str(private_key)],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except FileNotFoundError as exc:
        raise ClusterSSHKeyError("ssh-keygen is required to derive the Cluster Sync public key") from exc
    except subprocess.CalledProcessError as exc:
        raise ClusterSSHKeyError((exc.stderr or exc.stdout or "ssh-keygen failed").strip()) from exc
    _atomic_write_text(public_key, result.stdout.strip() + "\n", mode=0o644)


def _normalize_public_key(public_key: str, node_id: str) -> str:
    parts = str(public_key or "").strip().split()
    if len(parts) < 2:
        raise ClusterSSHKeyError("invalid public key")
    key_type, blob = parts[0], parts[1]
    if key_type not in SUPPORTED_KEY_TYPES:
        raise ClusterSSHKeyError("Cluster Sync keys must be ssh-ed25519")
    public_key_fingerprint(f"{key_type} {blob}")
    comment = f"{KEY_COMMENT_PREFIX}{node_id or 'unjoined'}"
    return f"{key_type} {blob} {comment}"


def _effective_node_id(cluster_root: Path, node_id: str | None) -> str:
    raw = str(node_id or "").strip()
    if raw and NODE_ID_RE.match(raw):
        return raw
    try:
        identity = read_local_identity(cluster_root)
        raw = str(identity.get("node_id") or "").strip()
        if NODE_ID_RE.match(raw):
            return raw
    except Exception:
        pass
    return ""


def _validate_source_node_id(node_id: str) -> str:
    raw = str(node_id or "").strip()
    if not NODE_ID_RE.match(raw):
        raise ClusterSSHKeyError("source_node_id must be a PBGui cluster node id")
    return raw


def _pbname_from_ini(pbgdir: Path) -> str:
    cfg = configparser.ConfigParser()
    try:
        cfg.read(pbgdir / "pbgui.ini")
        value = cfg.get("main", "pbname", fallback="").strip()
        if value:
            return value
    except Exception:
        pass
    return platform.node()


def _atomic_write_text(path: Path, value: str, *, mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(value, encoding="utf-8")
    os.chmod(tmp, mode)
    os.replace(tmp, path)
    _chmod_best_effort(path, mode)


def _chmod_best_effort(path: Path, mode: int) -> None:
    try:
        os.chmod(path, mode)
    except OSError:
        pass


def to_json(payload: dict[str, Any]) -> str:
    """Serialize helper payloads for CLI output."""

    return json.dumps(payload, sort_keys=True)
