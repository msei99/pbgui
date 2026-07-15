"""Tests for cluster operation signing and sealed-secret cryptography."""

from __future__ import annotations

import base64
import copy
import json
import stat
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric import ed25519, x25519

from cluster_credentials import (
    EnvelopeValidationError,
    NotRecipientError,
    SecretContext,
    SecretDecryptionError,
    SecretRecipient,
    SignatureVerificationError,
    canonical_operation_bytes,
    deserialize_sealed_secret,
    ensure_node_key_material,
    hpke_open,
    hpke_seal,
    load_node_key_material,
    open_sealed_secret,
    seal_secret,
    serialize_sealed_secret,
    sign_operation,
    validate_sealed_secret,
    verify_operation,
)


CLUSTER_ID = "cluster-7f0f88b0"
SIGNER_ID = "master-a"
MEMBERSHIP = {"master-a": "master", "master-b": "master", "vps-a": "vps"}


def _keys() -> dict[str, x25519.X25519PrivateKey]:
    """Create one X25519 private key for each test cluster member."""

    return {node_id: x25519.X25519PrivateKey.generate() for node_id in MEMBERSHIP}


def _recipients(keys: dict[str, x25519.X25519PrivateKey]) -> list[SecretRecipient]:
    """Build active recipient records in deliberately noncanonical order."""

    return [
        SecretRecipient(node_id, MEMBERSHIP[node_id], keys[node_id].public_key())
        for node_id in ("vps-a", "master-b", "master-a")
    ]


def _context(audience: str = "cluster") -> SecretContext:
    """Build the standard test secret context."""

    return SecretContext(
        cluster_id=CLUSTER_ID,
        secret_id="cmc-primary",
        kind="cmc_api_key",
        generation=4,
        audience=audience,
    )


def _sealed_fixture(
    audience: str = "cluster",
) -> tuple[dict, dict[str, x25519.X25519PrivateKey], ed25519.Ed25519PrivateKey]:
    """Create a signed envelope and all local private keys used by it."""

    keys = _keys()
    signing_key = ed25519.Ed25519PrivateKey.generate()
    envelope = seal_secret(
        b"super-secret-provider-key",
        _context(audience),
        _recipients(keys),
        signing_key,
        signer_id=SIGNER_ID,
    )
    return envelope, keys, signing_key


def test_node_key_material_is_stable_and_owner_only(tmp_path: Path) -> None:
    """Generated key directories and every key artifact remain owner-only."""

    first = ensure_node_key_material(tmp_path / "cluster")
    second = ensure_node_key_material(tmp_path / "cluster")
    loaded = load_node_key_material(tmp_path / "cluster")

    assert stat.S_IMODE(first.crypto_root.stat().st_mode) == 0o700
    assert {stat.S_IMODE(path.stat().st_mode) for path in first.crypto_root.iterdir()} == {0o600}
    assert first.public_bundle("master-a", "master") == second.public_bundle("master-a", "master")
    assert first.public_bundle("master-a", "master") == loaded.public_bundle("master-a", "master")
    assert sorted(path.name for path in first.crypto_root.iterdir()) == [
        ".keys.lock",
        "operation_signing_ed25519",
        "operation_signing_ed25519.pub",
        "secret_encryption_x25519",
        "secret_encryption_x25519.pub",
    ]


def test_operation_signing_is_canonical_deterministic_and_tamper_evident() -> None:
    """Map insertion order does not alter bytes or deterministic Ed25519 signatures."""

    private_key = ed25519.Ed25519PrivateKey.from_private_bytes(bytes(range(32)))
    left = {"z": 3, "a": [1, True, None], "nested": {"b": "two", "a": "one"}}
    right = {"nested": {"a": "one", "b": "two"}, "a": [1, True, None], "z": 3}

    assert canonical_operation_bytes(left) == b'{"a":[1,true,null],"nested":{"a":"one","b":"two"},"z":3}'
    signed_left = sign_operation(left, private_key, signer_id=SIGNER_ID)
    signed_right = sign_operation(right, private_key, signer_id=SIGNER_ID)

    assert signed_left["signature"] == signed_right["signature"]
    assert verify_operation(signed_left, private_key.public_key()) is True

    tampered = copy.deepcopy(signed_left)
    tampered["nested"]["a"] = "changed"
    with pytest.raises(SignatureVerificationError):
        verify_operation(tampered, private_key.public_key())


def test_rfc_9180_x25519_hkdf_sha256_aes128gcm_base_vector() -> None:
    """The one-shot HPKE API matches RFC 9180 Appendix A.1.1 exactly."""

    recipient_private = bytes.fromhex(
        "4612c550263fc8ad58375df3f557aac531d26850903e55a9f23f21d8534e8ac8"
    )
    recipient_public = bytes.fromhex(
        "3948cfe0ad1ddb695d780e59077195da6c56506b027329794ab02bca80815c4d"
    )
    ephemeral_private = bytes.fromhex(
        "52c4a758a802cd8b936eceea314432798d5baf2d7e9235dc084ab1b9cfa2f736"
    )
    info = bytes.fromhex("4f6465206f6e2061204772656369616e2055726e")
    aad = bytes.fromhex("436f756e742d30")
    plaintext = bytes.fromhex("4265617574792069732074727574682c20747275746820626561757479")
    expected_enc = bytes.fromhex(
        "37fda3567bdbd628e88668c3c8d7e97d1d1253b6d4ea6d44c150f741f1bf4431"
    )
    expected_ciphertext = bytes.fromhex(
        "f938558b5d72f1a23810b4be2ab4f84331acc02fc97babc53a52ae8218a355a9"
        "6d8770ac83d07bea87e13c512a"
    )

    enc, ciphertext = hpke_seal(
        recipient_public,
        plaintext,
        info=info,
        aad=aad,
        ephemeral_private_key=ephemeral_private,
    )

    assert enc == expected_enc
    assert ciphertext == expected_ciphertext
    assert hpke_open(recipient_private, enc, ciphertext, info=info, aad=aad) == plaintext


def test_cluster_audience_roundtrip_includes_every_active_recipient() -> None:
    """Cluster secrets decrypt for every master and VPS in active membership."""

    envelope, keys, signing_key = _sealed_fixture()

    assert [entry["node_id"] for entry in envelope["recipients"]] == sorted(MEMBERSHIP)
    assert validate_sealed_secret(
        envelope,
        signing_key.public_key(),
        expected_context=_context(),
        membership_roles=MEMBERSHIP,
    )
    for node_id, private_key in keys.items():
        assert open_sealed_secret(
            envelope,
            node_id,
            private_key,
            signing_key.public_key(),
            expected_context=_context(),
            membership_roles=MEMBERSHIP,
        ) == b"super-secret-provider-key"


def test_masters_audience_excludes_vps_but_survives_vps_relay() -> None:
    """A VPS can validate and forward a masters envelope but cannot decrypt it."""

    envelope, keys, signing_key = _sealed_fixture("masters")
    serialized = serialize_sealed_secret(envelope)

    relay_copy = deserialize_sealed_secret(serialized)
    assert validate_sealed_secret(
        relay_copy,
        signing_key.public_key(),
        expected_context=_context("masters"),
        membership_roles=MEMBERSHIP,
    )
    assert serialize_sealed_secret(relay_copy) == serialized
    assert [entry["node_id"] for entry in relay_copy["recipients"]] == ["master-a", "master-b"]
    with pytest.raises(NotRecipientError):
        open_sealed_secret(
            relay_copy,
            "vps-a",
            keys["vps-a"],
            signing_key.public_key(),
            expected_context=_context("masters"),
            membership_roles=MEMBERSHIP,
        )


@pytest.mark.parametrize("field", ["cluster_id", "secret_id", "kind", "generation", "audience"])
def test_envelope_aad_context_tampering_is_rejected(field: str) -> None:
    """Every required AAD context field is covered by the envelope signature."""

    envelope, _, signing_key = _sealed_fixture()
    tampered = copy.deepcopy(envelope)
    tampered[field] = 5 if field == "generation" else f"changed-{field}"

    with pytest.raises((EnvelopeValidationError, SignatureVerificationError)):
        validate_sealed_secret(tampered, signing_key.public_key())


def test_ciphertext_and_recipient_tampering_are_rejected() -> None:
    """Relay-visible recipient metadata and opaque ciphertext are signed."""

    envelope, _, signing_key = _sealed_fixture()
    for mutator in (
        lambda value: value["recipients"][0].update({"role": "vps"}),
        lambda value: value["recipients"][0].update({
            "ciphertext": base64.b64encode(
                bytes([base64.b64decode(value["recipients"][0]["ciphertext"])[0] ^ 1])
                + base64.b64decode(value["recipients"][0]["ciphertext"])[1:]
            ).decode("ascii")
        }),
    ):
        tampered = copy.deepcopy(envelope)
        mutator(tampered)
        with pytest.raises(SignatureVerificationError):
            validate_sealed_secret(tampered, signing_key.public_key())


def test_wrong_private_key_and_wrong_expected_context_are_rejected() -> None:
    """A listed identity still needs its key and the caller's intended context."""

    envelope, _, signing_key = _sealed_fixture()
    with pytest.raises(SecretDecryptionError):
        open_sealed_secret(
            envelope,
            "master-a",
            x25519.X25519PrivateKey.generate(),
            signing_key.public_key(),
            expected_context=_context(),
            membership_roles=MEMBERSHIP,
        )

    wrong_context = SecretContext(CLUSTER_ID, "other-secret", "cmc_api_key", 4, "cluster")
    with pytest.raises(EnvelopeValidationError, match="context"):
        open_sealed_secret(
            envelope,
            "master-a",
            x25519.X25519PrivateKey.generate(),
            signing_key.public_key(),
            expected_context=wrong_context,
            membership_roles=MEMBERSHIP,
        )


def test_membership_validation_rejects_incomplete_cluster_audience() -> None:
    """A relay with membership state rejects omitted active cluster recipients."""

    keys = _keys()
    signing_key = ed25519.Ed25519PrivateKey.generate()
    envelope = seal_secret(
        b"secret",
        _context(),
        _recipients(keys)[:-1],
        signing_key,
        signer_id=SIGNER_ID,
    )

    with pytest.raises(EnvelopeValidationError, match="membership"):
        validate_sealed_secret(
            envelope,
            signing_key.public_key(),
            expected_context=_context(),
            membership_roles=MEMBERSHIP,
        )


def test_envelope_contains_no_plaintext_hash_or_plaintext() -> None:
    """Opaque envelopes expose neither plaintext nor a plaintext digest field."""

    plaintext = b"uniquely-searchable-secret-value"
    keys = _keys()
    signing_key = ed25519.Ed25519PrivateKey.generate()
    envelope = seal_secret(
        plaintext,
        _context(),
        _recipients(keys),
        signing_key,
        signer_id=SIGNER_ID,
    )
    serialized = serialize_sealed_secret(envelope)

    assert plaintext not in serialized
    assert b"plaintext" not in serialized.lower()
    assert "hash" not in json.dumps(envelope).lower()


def test_deserializer_rejects_duplicate_fields() -> None:
    """Duplicate JSON keys cannot create parser-dependent signed semantics."""

    with pytest.raises(EnvelopeValidationError, match="duplicate"):
        deserialize_sealed_secret('{"version":1,"version":2}')
