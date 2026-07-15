"""Cluster operation signing and RFC 9180 sealed-secret cryptography.

This module is intentionally independent from Cluster Sync transport. Relays can
validate and forward a signed envelope without possessing an encryption key.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import os
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

from cryptography.exceptions import InvalidSignature, InvalidTag
from cryptography.hazmat.primitives import hashes, hmac, serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, x25519
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


PRIVATE_DIRECTORY_MODE = 0o700
PRIVATE_FILE_MODE = 0o600
CRYPTO_DIRECTORY = "crypto"
SIGNING_PRIVATE_FILE = "operation_signing_ed25519"
SIGNING_PUBLIC_FILE = "operation_signing_ed25519.pub"
ENCRYPTION_PRIVATE_FILE = "secret_encryption_x25519"
ENCRYPTION_PUBLIC_FILE = "secret_encryption_x25519.pub"
KEY_HISTORY_DIRECTORY = "history"
KEY_ROTATION_FILE = "rotation_pending.json"

ENVELOPE_FORMAT = "pbgui-sealed-secret"
ENVELOPE_VERSION = 1
SIGNATURE_VERSION = 1
KEM_ID = 0x0020
KDF_ID = 0x0001
AEAD_ID = 0x0001
HPKE_SUITE = {
    "kem_id": KEM_ID,
    "kdf_id": KDF_ID,
    "aead_id": AEAD_ID,
}
SUPPORTED_ROLES = frozenset({"master", "vps"})
SUPPORTED_AUDIENCES = frozenset({"cluster", "masters"})
MAX_SAFE_INTEGER = (1 << 53) - 1
MAX_SECRET_BYTES = 1024 * 1024
MAX_ENVELOPE_BYTES = 16 * 1024 * 1024

_HASH_LENGTH = 32
_HPKE_KEY_LENGTH = 16
_HPKE_NONCE_LENGTH = 12
_KEM_SUITE_ID = b"KEM" + KEM_ID.to_bytes(2, "big")
_HPKE_SUITE_ID = (
    b"HPKE"
    + KEM_ID.to_bytes(2, "big")
    + KDF_ID.to_bytes(2, "big")
    + AEAD_ID.to_bytes(2, "big")
)
_VERSION_LABEL = b"HPKE-v1"
_ENVELOPE_KEYS = frozenset({
    "format",
    "version",
    "suite",
    "cluster_id",
    "secret_id",
    "kind",
    "generation",
    "audience",
    "recipients",
    "signer_id",
    "signing_key_id",
    "signature_version",
    "signature_algorithm",
    "signature",
})
_RECIPIENT_KEYS = frozenset({"node_id", "role", "enc", "ciphertext"})


class ClusterCredentialError(RuntimeError):
    """Base error for cluster credential cryptography."""


class SignatureVerificationError(ClusterCredentialError):
    """Raised when a signed operation or envelope is not authentic."""


class EnvelopeValidationError(ClusterCredentialError):
    """Raised when a sealed-secret envelope is malformed or out of context."""


class NotRecipientError(ClusterCredentialError):
    """Raised when a node is not an authorized envelope recipient."""


class SecretDecryptionError(ClusterCredentialError):
    """Raised when HPKE authentication or decryption fails."""


@dataclass(frozen=True)
class NodeKeyMaterial:
    """A node's local signing and encryption key pairs."""

    crypto_root: Path
    signing_private_key: ed25519.Ed25519PrivateKey
    signing_public_key: ed25519.Ed25519PublicKey
    encryption_private_key: x25519.X25519PrivateKey
    encryption_public_key: x25519.X25519PublicKey

    def public_bundle(self, node_id: str, role: str) -> dict[str, str]:
        """Return transport-safe public key metadata for cluster membership."""

        node_id = _validated_text(node_id, "node_id")
        role = _validated_role(role)
        signing_raw = _public_bytes(self.signing_public_key)
        encryption_raw = _public_bytes(self.encryption_public_key)
        return {
            "node_id": node_id,
            "role": role,
            "signing_public_key": _b64encode(signing_raw),
            "signing_key_id": _key_id("ed25519", signing_raw),
            "encryption_public_key": _b64encode(encryption_raw),
            "encryption_key_id": _key_id("x25519", encryption_raw),
        }


@dataclass(frozen=True)
class SecretRecipient:
    """An active cluster member eligible for a sealed secret."""

    node_id: str
    role: str
    public_key: x25519.X25519PublicKey | bytes | str


@dataclass(frozen=True)
class SecretContext:
    """Authenticated identity and lifecycle metadata for one secret."""

    cluster_id: str
    secret_id: str
    kind: str
    generation: int
    audience: str

    def as_dict(self) -> dict[str, Any]:
        """Return the exact metadata bound as envelope AAD."""

        return _validate_context_fields(
            self.cluster_id,
            self.secret_id,
            self.kind,
            self.generation,
            self.audience,
        )


def ensure_node_key_material(cluster_root: Path | str) -> NodeKeyMaterial:
    """Create or load owner-only node keys below ``cluster_root/crypto``."""

    crypto_root = _ensure_private_directory(Path(cluster_root) / CRYPTO_DIRECTORY)
    with _key_lock(crypto_root):
        _recover_activated_rotation(crypto_root)
        signing_private = _ensure_private_key(
            crypto_root / SIGNING_PRIVATE_FILE,
            crypto_root / SIGNING_PUBLIC_FILE,
            ed25519.Ed25519PrivateKey.generate,
            ed25519.Ed25519PrivateKey.from_private_bytes,
        )
        encryption_private = _ensure_private_key(
            crypto_root / ENCRYPTION_PRIVATE_FILE,
            crypto_root / ENCRYPTION_PUBLIC_FILE,
            x25519.X25519PrivateKey.generate,
            x25519.X25519PrivateKey.from_private_bytes,
        )
    return NodeKeyMaterial(
        crypto_root=crypto_root,
        signing_private_key=signing_private,
        signing_public_key=signing_private.public_key(),
        encryption_private_key=encryption_private,
        encryption_public_key=encryption_private.public_key(),
    )


def load_node_key_material(cluster_root: Path | str) -> NodeKeyMaterial:
    """Load existing node keys and reject missing, mismatched, or unsafe files."""

    crypto_root = _ensure_private_directory(Path(cluster_root) / CRYPTO_DIRECTORY)
    with _key_lock(crypto_root):
        _recover_activated_rotation(crypto_root)
        signing_private = _load_private_key_pair(
            crypto_root / SIGNING_PRIVATE_FILE,
            crypto_root / SIGNING_PUBLIC_FILE,
            ed25519.Ed25519PrivateKey.from_private_bytes,
        )
        encryption_private = _load_private_key_pair(
            crypto_root / ENCRYPTION_PRIVATE_FILE,
            crypto_root / ENCRYPTION_PUBLIC_FILE,
            x25519.X25519PrivateKey.from_private_bytes,
        )
    return NodeKeyMaterial(
        crypto_root=crypto_root,
        signing_private_key=signing_private,
        signing_public_key=signing_private.public_key(),
        encryption_private_key=encryption_private,
        encryption_public_key=encryption_private.public_key(),
    )


def prepare_node_key_rotation(cluster_root: Path | str) -> tuple[NodeKeyMaterial, NodeKeyMaterial]:
    """Durably stage new node keys while leaving the active pairs unchanged."""

    root = Path(cluster_root)
    crypto_root = ensure_node_key_material(root).crypto_root
    with _key_lock(crypto_root):
        _recover_activated_rotation(crypto_root)
        signing_private = _load_private_key_pair(
            crypto_root / SIGNING_PRIVATE_FILE,
            crypto_root / SIGNING_PUBLIC_FILE,
            ed25519.Ed25519PrivateKey.from_private_bytes,
        )
        encryption_private = _load_private_key_pair(
            crypto_root / ENCRYPTION_PRIVATE_FILE,
            crypto_root / ENCRYPTION_PUBLIC_FILE,
            x25519.X25519PrivateKey.from_private_bytes,
        )
        current = NodeKeyMaterial(
            crypto_root=crypto_root,
            signing_private_key=signing_private,
            signing_public_key=signing_private.public_key(),
            encryption_private_key=encryption_private,
            encryption_public_key=encryption_private.public_key(),
        )
        pending = _read_rotation_journal(crypto_root)
        if pending is not None:
            return _rotation_material(crypto_root, pending, "old"), _rotation_material(
                crypto_root, pending, "new"
            )

        signing_private = ed25519.Ed25519PrivateKey.generate()
        encryption_private = x25519.X25519PrivateKey.generate()
        staged = {
            "version": 1,
            "activate": False,
            "old_signing_private_key": _b64encode(_private_bytes(current.signing_private_key)),
            "old_encryption_private_key": _b64encode(_private_bytes(current.encryption_private_key)),
            "new_signing_private_key": _b64encode(_private_bytes(signing_private)),
            "new_encryption_private_key": _b64encode(_private_bytes(encryption_private)),
        }
        _archive_private_key(
            crypto_root,
            "secret_encryption_x25519",
            current.encryption_private_key,
        )
        _atomic_write_private(
            crypto_root / KEY_ROTATION_FILE,
            json.dumps(staged, indent=4, sort_keys=True).encode("utf-8") + b"\n",
        )
        return current, _rotation_material(crypto_root, staged, "new")


def activate_prepared_node_key_rotation(cluster_root: Path | str) -> NodeKeyMaterial:
    """Activate a staged rotation; interrupted file swaps recover on next load."""

    crypto_root = _ensure_private_directory(Path(cluster_root) / CRYPTO_DIRECTORY)
    with _key_lock(crypto_root):
        pending = _read_rotation_journal(crypto_root)
        if pending is None:
            raise ClusterCredentialError("no node key rotation is pending")
        pending["activate"] = True
        _atomic_write_private(
            crypto_root / KEY_ROTATION_FILE,
            json.dumps(pending, indent=4, sort_keys=True).encode("utf-8") + b"\n",
        )
        _recover_activated_rotation(crypto_root)
        return _rotation_material(crypto_root, pending, "new")


def complete_node_key_rotation(cluster_root: Path | str) -> None:
    """Remove a completed rotation journal after recipient rewrap succeeds."""

    crypto_root = _ensure_private_directory(Path(cluster_root) / CRYPTO_DIRECTORY)
    with _key_lock(crypto_root):
        pending = _read_rotation_journal(crypto_root)
        if pending is None:
            return
        if pending.get("activate") is not True:
            raise ClusterCredentialError("node key rotation has not been activated")
        _recover_activated_rotation(crypto_root)
        (crypto_root / KEY_ROTATION_FILE).unlink(missing_ok=True)


def load_node_encryption_private_keys(
    cluster_root: Path | str,
) -> list[x25519.X25519PrivateKey]:
    """Load the active and archived local decryption keys, newest first."""

    current = load_node_key_material(cluster_root)
    result = [current.encryption_private_key]
    seen = {_public_bytes(current.encryption_public_key)}
    history_root = current.crypto_root / KEY_HISTORY_DIRECTORY
    if not history_root.is_dir() or history_root.is_symlink():
        return result
    for path in sorted(history_root.glob("secret_encryption_x25519.*"), reverse=True):
        raw = _read_private_key_file(path)
        try:
            private_key = x25519.X25519PrivateKey.from_private_bytes(raw)
        except ValueError as exc:
            raise ClusterCredentialError(f"invalid archived private key: {path}") from exc
        public_raw = _public_bytes(private_key.public_key())
        if public_raw not in seen:
            seen.add(public_raw)
            result.append(private_key)
    return result


def canonical_json_bytes(value: Any) -> bytes:
    """Encode the supported RFC 8785 canonical JSON subset.

    Floating-point values are deliberately rejected. Cluster operations use
    integers, and rejecting floats avoids platform-dependent number rendering.
    """

    return _canonical_json(value).encode("utf-8")


def canonical_operation_bytes(operation: Mapping[str, Any]) -> bytes:
    """Return deterministic signing bytes, excluding only ``signature``."""

    if not isinstance(operation, Mapping):
        raise ClusterCredentialError("operation must be a mapping")
    unsigned = dict(operation)
    unsigned.pop("signature", None)
    return canonical_json_bytes(unsigned)


def sign_operation(
    operation: Mapping[str, Any],
    signing_private_key: ed25519.Ed25519PrivateKey | bytes,
    *,
    signer_id: str,
) -> dict[str, Any]:
    """Return an Ed25519-signed copy of an operation."""

    private_key = _coerce_ed25519_private_key(signing_private_key)
    public_raw = _public_bytes(private_key.public_key())
    signed = dict(operation)
    signed.pop("signature", None)
    key_id_field = "signer_key_id" if "signing_key_id" in signed else "signing_key_id"
    signed.update({
        "signer_id": _validated_text(signer_id, "signer_id"),
        key_id_field: _key_id("ed25519", public_raw),
        "signature_version": SIGNATURE_VERSION,
        "signature_algorithm": "Ed25519",
    })
    signed["signature"] = _b64encode(private_key.sign(canonical_operation_bytes(signed)))
    return signed


def verify_operation(
    operation: Mapping[str, Any],
    signing_public_key: ed25519.Ed25519PublicKey | bytes | str,
) -> bool:
    """Verify a canonical operation signature and key identifier."""

    public_key = _coerce_ed25519_public_key(signing_public_key)
    _validate_signature_metadata(operation, _public_bytes(public_key))
    try:
        signature = _b64decode(operation.get("signature"), "signature", expected_length=64)
        public_key.verify(signature, canonical_operation_bytes(operation))
    except (InvalidSignature, ValueError) as exc:
        raise SignatureVerificationError("operation signature is invalid") from exc
    return True


def hpke_seal(
    recipient_public_key: x25519.X25519PublicKey | bytes | str,
    plaintext: bytes,
    *,
    info: bytes = b"",
    aad: bytes = b"",
    ephemeral_private_key: x25519.X25519PrivateKey | bytes | None = None,
) -> tuple[bytes, bytes]:
    """Apply RFC 9180 Base mode with the module's fixed ciphersuite."""

    public_key = _coerce_x25519_public_key(recipient_public_key)
    ephemeral_key = (
        x25519.X25519PrivateKey.generate()
        if ephemeral_private_key is None
        else _coerce_x25519_private_key(ephemeral_private_key)
    )
    enc = _public_bytes(ephemeral_key.public_key())
    recipient_raw = _public_bytes(public_key)
    try:
        dh = ephemeral_key.exchange(public_key)
    except ValueError as exc:
        raise SecretDecryptionError("invalid X25519 recipient public key") from exc
    shared_secret = _extract_and_expand(dh, enc + recipient_raw)
    key, base_nonce = _hpke_key_schedule(shared_secret, _as_bytes(info, "info"))
    ciphertext = AESGCM(key).encrypt(base_nonce, _as_bytes(plaintext, "plaintext"), _as_bytes(aad, "aad"))
    return enc, ciphertext


def hpke_open(
    recipient_private_key: x25519.X25519PrivateKey | bytes,
    enc: bytes,
    ciphertext: bytes,
    *,
    info: bytes = b"",
    aad: bytes = b"",
) -> bytes:
    """Open one RFC 9180 Base-mode ciphertext from sequence number zero."""

    private_key = _coerce_x25519_private_key(recipient_private_key)
    enc = _as_bytes(enc, "enc")
    if len(enc) != 32:
        raise SecretDecryptionError("HPKE encapsulated key must be 32 bytes")
    try:
        ephemeral_public = x25519.X25519PublicKey.from_public_bytes(enc)
        dh = private_key.exchange(ephemeral_public)
        recipient_raw = _public_bytes(private_key.public_key())
        shared_secret = _extract_and_expand(dh, enc + recipient_raw)
        key, base_nonce = _hpke_key_schedule(shared_secret, _as_bytes(info, "info"))
        return AESGCM(key).decrypt(
            base_nonce,
            _as_bytes(ciphertext, "ciphertext"),
            _as_bytes(aad, "aad"),
        )
    except (InvalidTag, ValueError) as exc:
        raise SecretDecryptionError("HPKE ciphertext authentication failed") from exc


def sealed_secret_aad(context: SecretContext) -> bytes:
    """Return canonical AAD binding all required secret context fields."""

    return canonical_json_bytes({"version": ENVELOPE_VERSION, **context.as_dict()})


def seal_secret(
    plaintext: bytes,
    context: SecretContext,
    recipients: Sequence[SecretRecipient],
    signing_private_key: ed25519.Ed25519PrivateKey | bytes,
    *,
    signer_id: str,
) -> dict[str, Any]:
    """Seal and sign a secret for its audience's eligible recipients."""

    plaintext = _as_bytes(plaintext, "plaintext")
    if len(plaintext) > MAX_SECRET_BYTES:
        raise EnvelopeValidationError("secret exceeds maximum size")
    context_fields = context.as_dict()
    normalized = _normalize_recipients(recipients)
    if context.audience == "masters":
        normalized = [recipient for recipient in normalized if recipient.role == "master"]
    if not normalized:
        raise EnvelopeValidationError("sealed secret has no eligible recipients")

    aad = sealed_secret_aad(context)
    recipient_entries: list[dict[str, str]] = []
    for recipient in normalized:
        info = _recipient_info(context, recipient.node_id)
        enc, ciphertext = hpke_seal(recipient.public_key, plaintext, info=info, aad=aad)
        recipient_entries.append({
            "node_id": recipient.node_id,
            "role": recipient.role,
            "enc": _b64encode(enc),
            "ciphertext": _b64encode(ciphertext),
        })

    private_key = _coerce_ed25519_private_key(signing_private_key)
    signing_public_raw = _public_bytes(private_key.public_key())
    envelope: dict[str, Any] = {
        "format": ENVELOPE_FORMAT,
        "version": ENVELOPE_VERSION,
        "suite": dict(HPKE_SUITE),
        **context_fields,
        "recipients": recipient_entries,
        "signer_id": _validated_text(signer_id, "signer_id"),
        "signing_key_id": _key_id("ed25519", signing_public_raw),
        "signature_version": SIGNATURE_VERSION,
        "signature_algorithm": "Ed25519",
    }
    envelope["signature"] = _b64encode(private_key.sign(_unsigned_envelope_bytes(envelope)))
    return envelope


def validate_sealed_secret(
    envelope: Mapping[str, Any],
    signing_public_key: ed25519.Ed25519PublicKey | bytes | str,
    *,
    expected_context: SecretContext | None = None,
    membership_roles: Mapping[str, str] | None = None,
) -> bool:
    """Validate an opaque envelope without decrypting its secret."""

    _validate_envelope_structure(envelope)
    public_key = _coerce_ed25519_public_key(signing_public_key)
    _validate_signature_metadata(envelope, _public_bytes(public_key))
    try:
        signature = _b64decode(envelope.get("signature"), "signature", expected_length=64)
        public_key.verify(signature, _unsigned_envelope_bytes(envelope))
    except (InvalidSignature, ValueError) as exc:
        raise SignatureVerificationError("sealed-secret signature is invalid") from exc

    context = _context_from_envelope(envelope)
    if expected_context is not None and context != expected_context:
        raise EnvelopeValidationError("sealed-secret context does not match expectation")
    _validate_recipient_audience(envelope, membership_roles)
    return True


def open_sealed_secret(
    envelope: Mapping[str, Any],
    recipient_id: str,
    recipient_private_key: x25519.X25519PrivateKey | bytes,
    signing_public_key: ed25519.Ed25519PublicKey | bytes | str,
    *,
    expected_context: SecretContext,
    membership_roles: Mapping[str, str] | None = None,
) -> bytes:
    """Validate and decrypt a sealed secret for one intended recipient."""

    validate_sealed_secret(
        envelope,
        signing_public_key,
        expected_context=expected_context,
        membership_roles=membership_roles,
    )
    recipient_id = _validated_text(recipient_id, "recipient_id")
    entry = next(
        (item for item in envelope["recipients"] if item["node_id"] == recipient_id),
        None,
    )
    if entry is None:
        raise NotRecipientError(f"node is not a sealed-secret recipient: {recipient_id}")
    enc = _b64decode(entry["enc"], "enc", expected_length=32)
    ciphertext = _b64decode(entry["ciphertext"], "ciphertext")
    return hpke_open(
        recipient_private_key,
        enc,
        ciphertext,
        info=_recipient_info(expected_context, recipient_id),
        aad=sealed_secret_aad(expected_context),
    )


def serialize_sealed_secret(envelope: Mapping[str, Any]) -> bytes:
    """Serialize an envelope canonically for opaque storage and forwarding."""

    _validate_envelope_structure(envelope)
    encoded = canonical_json_bytes(dict(envelope))
    if len(encoded) > MAX_ENVELOPE_BYTES:
        raise EnvelopeValidationError("sealed-secret envelope exceeds maximum size")
    return encoded


def deserialize_sealed_secret(data: bytes | str) -> dict[str, Any]:
    """Parse an envelope while rejecting duplicate JSON object keys."""

    raw = data.encode("utf-8") if isinstance(data, str) else _as_bytes(data, "data")
    if len(raw) > MAX_ENVELOPE_BYTES:
        raise EnvelopeValidationError("sealed-secret envelope exceeds maximum size")
    try:
        value = json.loads(raw, object_pairs_hook=_object_without_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError, EnvelopeValidationError) as exc:
        if isinstance(exc, EnvelopeValidationError):
            raise
        raise EnvelopeValidationError("sealed-secret envelope is not valid JSON") from exc
    if not isinstance(value, dict):
        raise EnvelopeValidationError("sealed-secret envelope must be a JSON object")
    _validate_envelope_structure(value)
    return value


def _canonical_json(value: Any) -> str:
    """Recursively encode the supported canonical JSON subset."""

    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, int):
        if not -MAX_SAFE_INTEGER <= value <= MAX_SAFE_INTEGER:
            raise ClusterCredentialError("integer is outside the interoperable JSON range")
        return str(value)
    if isinstance(value, float):
        raise ClusterCredentialError("floating-point values are not allowed in signed JSON")
    if isinstance(value, str):
        try:
            value.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise ClusterCredentialError("JSON strings must contain valid Unicode") from exc
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_canonical_json(item) for item in value) + "]"
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise ClusterCredentialError("JSON object keys must be strings")
        ordered_keys = sorted(value, key=lambda key: key.encode("utf-16be"))
        return "{" + ",".join(
            f"{_canonical_json(key)}:{_canonical_json(value[key])}" for key in ordered_keys
        ) + "}"
    raise ClusterCredentialError(f"unsupported canonical JSON value: {type(value).__name__}")


def _ensure_private_directory(path: Path) -> Path:
    """Create and secure a non-symlink owner-only directory."""

    if path.is_symlink():
        raise ClusterCredentialError(f"refusing crypto directory symlink: {path}")
    path.mkdir(parents=True, mode=PRIVATE_DIRECTORY_MODE, exist_ok=True)
    if path.is_symlink() or not path.is_dir():
        raise ClusterCredentialError(f"crypto path is not a private directory: {path}")
    if os.name == "posix":
        path.chmod(PRIVATE_DIRECTORY_MODE)
    return path


@contextmanager
def _key_lock(crypto_root: Path) -> Iterator[None]:
    """Serialize key-pair creation across local processes."""

    lock_path = crypto_root / ".keys.lock"
    if lock_path.is_symlink():
        raise ClusterCredentialError(f"refusing key lock symlink: {lock_path}")
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(lock_path, flags, PRIVATE_FILE_MODE)
    try:
        if os.name == "posix":
            import fcntl

            os.fchmod(fd, PRIVATE_FILE_MODE)
            fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        os.close(fd)


def _ensure_private_key(
    private_path: Path,
    public_path: Path,
    generate: Any,
    load_private: Any,
) -> Any:
    """Create one key pair or validate its existing on-disk representation."""

    if private_path.exists():
        return _load_private_key_pair(private_path, public_path, load_private, repair_public=True)
    if private_path.is_symlink() or public_path.is_symlink() or public_path.exists():
        raise ClusterCredentialError(f"incomplete or unsafe key pair: {private_path.name}")
    private_key = generate()
    _atomic_write_private(private_path, _b64encode(_private_bytes(private_key)).encode("ascii") + b"\n")
    _atomic_write_private(public_path, _b64encode(_public_bytes(private_key.public_key())).encode("ascii") + b"\n")
    return private_key


def _load_private_key_pair(
    private_path: Path,
    public_path: Path,
    load_private: Any,
    *,
    repair_public: bool = False,
) -> Any:
    """Load one private key and require its public companion to match."""

    private_raw = _read_private_key_file(private_path)
    try:
        private_key = load_private(private_raw)
    except ValueError as exc:
        raise ClusterCredentialError(f"invalid private key: {private_path}") from exc
    expected_public = _public_bytes(private_key.public_key())
    if not public_path.exists() and repair_public:
        _atomic_write_private(public_path, _b64encode(expected_public).encode("ascii") + b"\n")
    public_raw = _read_private_key_file(public_path)
    if public_raw != expected_public:
        raise ClusterCredentialError(f"public key does not match private key: {public_path}")
    return private_key


def _read_private_key_file(path: Path) -> bytes:
    """Read and secure one raw-base64 key file."""

    if path.is_symlink() or not path.is_file():
        raise ClusterCredentialError(f"missing or unsafe key file: {path}")
    if os.name == "posix":
        path.chmod(PRIVATE_FILE_MODE)
    try:
        return base64.b64decode(path.read_bytes().strip(), validate=True)
    except (ValueError, binascii.Error) as exc:
        raise ClusterCredentialError(f"invalid base64 key file: {path}") from exc


def _atomic_write_private(path: Path, content: bytes) -> None:
    """Atomically publish an owner-only file in an already-private directory."""

    if path.is_symlink():
        raise ClusterCredentialError(f"refusing key file symlink: {path}")
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    fd = os.open(temporary, flags, PRIVATE_FILE_MODE)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        if os.name == "posix":
            path.chmod(PRIVATE_FILE_MODE)
    finally:
        temporary.unlink(missing_ok=True)


def _read_rotation_journal(crypto_root: Path) -> dict[str, Any] | None:
    """Read and validate the owner-only pending rotation journal."""

    path = crypto_root / KEY_ROTATION_FILE
    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file():
        raise ClusterCredentialError("unsafe node key rotation journal")
    if os.name == "posix":
        path.chmod(PRIVATE_FILE_MODE)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ClusterCredentialError("invalid node key rotation journal") from exc
    required = {
        "version",
        "activate",
        "old_signing_private_key",
        "old_encryption_private_key",
        "new_signing_private_key",
        "new_encryption_private_key",
    }
    if not isinstance(value, dict) or set(value) != required or value.get("version") != 1:
        raise ClusterCredentialError("invalid node key rotation journal")
    if not isinstance(value.get("activate"), bool):
        raise ClusterCredentialError("invalid node key rotation journal")
    for field in required - {"version", "activate"}:
        _b64decode(value.get(field), field, expected_length=32)
    return value


def _rotation_material(
    crypto_root: Path,
    pending: Mapping[str, Any],
    prefix: str,
) -> NodeKeyMaterial:
    """Construct one old/new key set from a validated rotation journal."""

    signing_private = ed25519.Ed25519PrivateKey.from_private_bytes(
        _b64decode(pending[f"{prefix}_signing_private_key"], "signing key", expected_length=32)
    )
    encryption_private = x25519.X25519PrivateKey.from_private_bytes(
        _b64decode(
            pending[f"{prefix}_encryption_private_key"],
            "encryption key",
            expected_length=32,
        )
    )
    return NodeKeyMaterial(
        crypto_root=crypto_root,
        signing_private_key=signing_private,
        signing_public_key=signing_private.public_key(),
        encryption_private_key=encryption_private,
        encryption_public_key=encryption_private.public_key(),
    )


def _recover_activated_rotation(crypto_root: Path) -> None:
    """Finish an activated multi-file key swap after any interrupted write."""

    pending = _read_rotation_journal(crypto_root)
    if pending is None or pending.get("activate") is not True:
        return
    material = _rotation_material(crypto_root, pending, "new")
    for path, raw in (
        (crypto_root / SIGNING_PRIVATE_FILE, _private_bytes(material.signing_private_key)),
        (crypto_root / SIGNING_PUBLIC_FILE, _public_bytes(material.signing_public_key)),
        (crypto_root / ENCRYPTION_PRIVATE_FILE, _private_bytes(material.encryption_private_key)),
        (crypto_root / ENCRYPTION_PUBLIC_FILE, _public_bytes(material.encryption_public_key)),
    ):
        expected = _b64encode(raw).encode("ascii") + b"\n"
        if not path.is_file() or path.is_symlink() or path.read_bytes() != expected:
            _atomic_write_private(path, expected)


def _archive_private_key(crypto_root: Path, name: str, private_key: Any) -> None:
    """Archive one private key under its public-key fingerprint exactly once."""

    history_root = _ensure_private_directory(crypto_root / KEY_HISTORY_DIRECTORY)
    public_raw = _public_bytes(private_key.public_key())
    digest = hashlib.sha256(public_raw).hexdigest()
    path = history_root / f"{name}.{digest}"
    expected = _b64encode(_private_bytes(private_key)).encode("ascii") + b"\n"
    if path.exists():
        if path.is_symlink() or path.read_bytes() != expected:
            raise ClusterCredentialError("archived node key does not match rotation state")
        if os.name == "posix":
            path.chmod(PRIVATE_FILE_MODE)
        return
    _atomic_write_private(path, expected)


def _private_bytes(private_key: Any) -> bytes:
    """Serialize an Ed25519 or X25519 private key in raw form."""

    return private_key.private_bytes(
        serialization.Encoding.Raw,
        serialization.PrivateFormat.Raw,
        serialization.NoEncryption(),
    )


def _public_bytes(public_key: Any) -> bytes:
    """Serialize an Ed25519 or X25519 public key in raw form."""

    return public_key.public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)


def _key_id(algorithm: str, public_raw: bytes) -> str:
    """Return a stable identifier for a public key."""

    return f"{algorithm}:{hashlib.sha256(public_raw).hexdigest()}"


def _b64encode(value: bytes) -> str:
    """Encode binary envelope fields with canonical padded base64."""

    return base64.b64encode(value).decode("ascii")


def _b64decode(value: Any, field: str, *, expected_length: int | None = None) -> bytes:
    """Strictly decode one canonical base64 field."""

    if not isinstance(value, str):
        raise EnvelopeValidationError(f"{field} must be a base64 string")
    try:
        decoded = base64.b64decode(value, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise EnvelopeValidationError(f"{field} is not valid base64") from exc
    if _b64encode(decoded) != value:
        raise EnvelopeValidationError(f"{field} is not canonical base64")
    if expected_length is not None and len(decoded) != expected_length:
        raise EnvelopeValidationError(f"{field} must decode to {expected_length} bytes")
    return decoded


def _coerce_ed25519_private_key(value: ed25519.Ed25519PrivateKey | bytes) -> ed25519.Ed25519PrivateKey:
    """Normalize an Ed25519 private key input."""

    if isinstance(value, ed25519.Ed25519PrivateKey):
        return value
    try:
        return ed25519.Ed25519PrivateKey.from_private_bytes(_as_bytes(value, "signing_private_key"))
    except ValueError as exc:
        raise ClusterCredentialError("Ed25519 private key must be 32 bytes") from exc


def _coerce_ed25519_public_key(value: ed25519.Ed25519PublicKey | bytes | str) -> ed25519.Ed25519PublicKey:
    """Normalize an Ed25519 public key input."""

    if isinstance(value, ed25519.Ed25519PublicKey):
        return value
    raw = _b64decode(value, "signing_public_key", expected_length=32) if isinstance(value, str) else _as_bytes(value, "signing_public_key")
    try:
        return ed25519.Ed25519PublicKey.from_public_bytes(raw)
    except ValueError as exc:
        raise ClusterCredentialError("Ed25519 public key must be 32 bytes") from exc


def _coerce_x25519_private_key(value: x25519.X25519PrivateKey | bytes) -> x25519.X25519PrivateKey:
    """Normalize an X25519 private key input."""

    if isinstance(value, x25519.X25519PrivateKey):
        return value
    try:
        return x25519.X25519PrivateKey.from_private_bytes(_as_bytes(value, "recipient_private_key"))
    except ValueError as exc:
        raise ClusterCredentialError("X25519 private key must be 32 bytes") from exc


def _coerce_x25519_public_key(value: x25519.X25519PublicKey | bytes | str) -> x25519.X25519PublicKey:
    """Normalize an X25519 public key input."""

    if isinstance(value, x25519.X25519PublicKey):
        return value
    raw = _b64decode(value, "recipient_public_key", expected_length=32) if isinstance(value, str) else _as_bytes(value, "recipient_public_key")
    try:
        return x25519.X25519PublicKey.from_public_bytes(raw)
    except ValueError as exc:
        raise ClusterCredentialError("X25519 public key must be 32 bytes") from exc


def _as_bytes(value: Any, field: str) -> bytes:
    """Require an immutable bytes-like cryptographic input."""

    if not isinstance(value, (bytes, bytearray, memoryview)):
        raise ClusterCredentialError(f"{field} must be bytes")
    return bytes(value)


def _hkdf_extract(salt: bytes, ikm: bytes) -> bytes:
    """Apply RFC 5869 HKDF-Extract with SHA-256."""

    effective_salt = salt or (b"\x00" * _HASH_LENGTH)
    mac = hmac.HMAC(effective_salt, hashes.SHA256())
    mac.update(ikm)
    return mac.finalize()


def _hkdf_expand(prk: bytes, info: bytes, length: int) -> bytes:
    """Apply RFC 5869 HKDF-Expand with SHA-256."""

    if not 0 <= length <= 255 * _HASH_LENGTH:
        raise ClusterCredentialError("HKDF output length is invalid")
    output = bytearray()
    previous = b""
    for counter in range(1, (length + _HASH_LENGTH - 1) // _HASH_LENGTH + 1):
        mac = hmac.HMAC(prk, hashes.SHA256())
        mac.update(previous + info + bytes((counter,)))
        previous = mac.finalize()
        output.extend(previous)
    return bytes(output[:length])


def _labeled_extract(salt: bytes, label: bytes, ikm: bytes, suite_id: bytes) -> bytes:
    """Apply the RFC 9180 domain-separated extract function."""

    return _hkdf_extract(salt, _VERSION_LABEL + suite_id + label + ikm)


def _labeled_expand(prk: bytes, label: bytes, info: bytes, length: int, suite_id: bytes) -> bytes:
    """Apply the RFC 9180 domain-separated expand function."""

    labeled_info = length.to_bytes(2, "big") + _VERSION_LABEL + suite_id + label + info
    return _hkdf_expand(prk, labeled_info, length)


def _extract_and_expand(dh: bytes, kem_context: bytes) -> bytes:
    """Derive the DHKEM shared secret from X25519 output."""

    eae_prk = _labeled_extract(b"", b"eae_prk", dh, _KEM_SUITE_ID)
    return _labeled_expand(eae_prk, b"shared_secret", kem_context, _HASH_LENGTH, _KEM_SUITE_ID)


def _hpke_key_schedule(shared_secret: bytes, info: bytes) -> tuple[bytes, bytes]:
    """Derive RFC 9180 Base-mode AEAD key and initial nonce."""

    psk_id_hash = _labeled_extract(b"", b"psk_id_hash", b"", _HPKE_SUITE_ID)
    info_hash = _labeled_extract(b"", b"info_hash", info, _HPKE_SUITE_ID)
    key_schedule_context = b"\x00" + psk_id_hash + info_hash
    secret = _labeled_extract(shared_secret, b"secret", b"", _HPKE_SUITE_ID)
    key = _labeled_expand(secret, b"key", key_schedule_context, _HPKE_KEY_LENGTH, _HPKE_SUITE_ID)
    nonce = _labeled_expand(secret, b"base_nonce", key_schedule_context, _HPKE_NONCE_LENGTH, _HPKE_SUITE_ID)
    return key, nonce


def _validated_text(value: Any, field: str, *, maximum: int = 255) -> str:
    """Validate bounded text used in signed contexts."""

    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > maximum:
        raise EnvelopeValidationError(f"{field} must be non-empty text up to {maximum} bytes")
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
        raise EnvelopeValidationError(f"{field} contains a control character")
    return value


def _validated_role(value: Any) -> str:
    """Validate a cluster member role used by audience policy."""

    if value not in SUPPORTED_ROLES:
        raise EnvelopeValidationError(f"unsupported cluster role: {value}")
    return str(value)


def _validate_context_fields(
    cluster_id: Any,
    secret_id: Any,
    kind: Any,
    generation: Any,
    audience: Any,
) -> dict[str, Any]:
    """Validate and normalize envelope AAD fields."""

    if not isinstance(generation, int) or isinstance(generation, bool) or not 1 <= generation <= MAX_SAFE_INTEGER:
        raise EnvelopeValidationError("generation must be a positive interoperable integer")
    if audience not in SUPPORTED_AUDIENCES:
        raise EnvelopeValidationError(f"unsupported secret audience: {audience}")
    return {
        "cluster_id": _validated_text(cluster_id, "cluster_id"),
        "secret_id": _validated_text(secret_id, "secret_id"),
        "kind": _validated_text(kind, "kind"),
        "generation": generation,
        "audience": str(audience),
    }


def _normalize_recipients(recipients: Sequence[SecretRecipient]) -> list[SecretRecipient]:
    """Validate, deduplicate, and deterministically order recipients."""

    normalized: list[SecretRecipient] = []
    seen: set[str] = set()
    for recipient in recipients:
        if not isinstance(recipient, SecretRecipient):
            raise EnvelopeValidationError("recipients must be SecretRecipient values")
        node_id = _validated_text(recipient.node_id, "recipient node_id")
        if node_id in seen:
            raise EnvelopeValidationError(f"duplicate sealed-secret recipient: {node_id}")
        seen.add(node_id)
        normalized.append(SecretRecipient(
            node_id=node_id,
            role=_validated_role(recipient.role),
            public_key=_coerce_x25519_public_key(recipient.public_key),
        ))
    return sorted(normalized, key=lambda recipient: recipient.node_id.encode("utf-8"))


def _recipient_info(context: SecretContext, recipient_id: str) -> bytes:
    """Bind HPKE key derivation to the application and recipient identity."""

    return canonical_json_bytes({
        "domain": "PBGui cluster sealed secret",
        "recipient_id": _validated_text(recipient_id, "recipient_id"),
        "context": {"version": ENVELOPE_VERSION, **context.as_dict()},
    })


def _unsigned_envelope_bytes(envelope: Mapping[str, Any]) -> bytes:
    """Return canonical signature bytes for an envelope."""

    unsigned = dict(envelope)
    unsigned.pop("signature", None)
    return canonical_json_bytes(unsigned)


def _validate_signature_metadata(document: Mapping[str, Any], public_raw: bytes) -> None:
    """Require the supported signature version and matching key ID."""

    if document.get("signature_version") != SIGNATURE_VERSION:
        raise SignatureVerificationError("unsupported signature version")
    if document.get("signature_algorithm") != "Ed25519":
        raise SignatureVerificationError("unsupported signature algorithm")
    key_id = document.get("signer_key_id", document.get("signing_key_id"))
    if key_id != _key_id("ed25519", public_raw):
        raise SignatureVerificationError("signing key identifier does not match public key")
    _validated_text(document.get("signer_id"), "signer_id")


def _validate_envelope_structure(envelope: Mapping[str, Any]) -> None:
    """Validate the complete version-one sealed-secret schema."""

    if not isinstance(envelope, Mapping):
        raise EnvelopeValidationError("sealed-secret envelope must be a mapping")
    if frozenset(envelope) != _ENVELOPE_KEYS:
        missing = sorted(_ENVELOPE_KEYS - frozenset(envelope))
        extra = sorted(frozenset(envelope) - _ENVELOPE_KEYS)
        raise EnvelopeValidationError(f"sealed-secret fields mismatch; missing={missing}, extra={extra}")
    if envelope.get("format") != ENVELOPE_FORMAT or envelope.get("version") != ENVELOPE_VERSION:
        raise EnvelopeValidationError("unsupported sealed-secret envelope version")
    if envelope.get("suite") != HPKE_SUITE:
        raise EnvelopeValidationError("unsupported sealed-secret ciphersuite")
    _context_from_envelope(envelope)
    _validated_text(envelope.get("signer_id"), "signer_id")
    if envelope.get("signature_version") != SIGNATURE_VERSION or envelope.get("signature_algorithm") != "Ed25519":
        raise EnvelopeValidationError("unsupported sealed-secret signature scheme")
    _validated_text(envelope.get("signing_key_id"), "signing_key_id")
    _b64decode(envelope.get("signature"), "signature", expected_length=64)
    entries = envelope.get("recipients")
    if not isinstance(entries, list) or not entries:
        raise EnvelopeValidationError("sealed-secret recipients must be a non-empty list")
    previous_node_id: str | None = None
    for entry in entries:
        if not isinstance(entry, Mapping) or frozenset(entry) != _RECIPIENT_KEYS:
            raise EnvelopeValidationError("sealed-secret recipient has invalid fields")
        node_id = _validated_text(entry.get("node_id"), "recipient node_id")
        if previous_node_id is not None and node_id <= previous_node_id:
            raise EnvelopeValidationError("sealed-secret recipients must be unique and sorted")
        previous_node_id = node_id
        _validated_role(entry.get("role"))
        _b64decode(entry.get("enc"), "enc", expected_length=32)
        ciphertext = _b64decode(entry.get("ciphertext"), "ciphertext")
        if len(ciphertext) < 16 or len(ciphertext) > MAX_SECRET_BYTES + 16:
            raise EnvelopeValidationError("sealed-secret ciphertext size is invalid")


def _context_from_envelope(envelope: Mapping[str, Any]) -> SecretContext:
    """Build validated context metadata from an envelope."""

    fields = _validate_context_fields(
        envelope.get("cluster_id"),
        envelope.get("secret_id"),
        envelope.get("kind"),
        envelope.get("generation"),
        envelope.get("audience"),
    )
    return SecretContext(**fields)


def _validate_recipient_audience(
    envelope: Mapping[str, Any],
    membership_roles: Mapping[str, str] | None,
) -> None:
    """Enforce audience role policy and optionally exact active membership."""

    audience = str(envelope["audience"])
    actual = {str(entry["node_id"]): str(entry["role"]) for entry in envelope["recipients"]}
    if audience == "masters" and any(role != "master" for role in actual.values()):
        raise EnvelopeValidationError("masters audience contains a non-master recipient")
    if membership_roles is None:
        return
    normalized_membership = {
        _validated_text(node_id, "membership node_id"): _validated_role(role)
        for node_id, role in membership_roles.items()
    }
    expected = (
        normalized_membership
        if audience == "cluster"
        else {node_id: role for node_id, role in normalized_membership.items() if role == "master"}
    )
    if actual != expected:
        raise EnvelopeValidationError("sealed-secret recipients do not match active audience membership")


def _object_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Build a JSON object while rejecting duplicate keys."""

    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise EnvelopeValidationError(f"duplicate JSON object key: {key}")
        result[key] = value
    return result


__all__ = [
    "AEAD_ID",
    "ClusterCredentialError",
    "ENVELOPE_VERSION",
    "EnvelopeValidationError",
    "KDF_ID",
    "KEM_ID",
    "NodeKeyMaterial",
    "NotRecipientError",
    "SecretContext",
    "SecretDecryptionError",
    "SecretRecipient",
    "SignatureVerificationError",
    "canonical_json_bytes",
    "canonical_operation_bytes",
    "activate_prepared_node_key_rotation",
    "complete_node_key_rotation",
    "deserialize_sealed_secret",
    "ensure_node_key_material",
    "hpke_open",
    "hpke_seal",
    "load_node_key_material",
    "load_node_encryption_private_keys",
    "open_sealed_secret",
    "prepare_node_key_rotation",
    "seal_secret",
    "sealed_secret_aad",
    "serialize_sealed_secret",
    "sign_operation",
    "validate_sealed_secret",
    "verify_operation",
]
