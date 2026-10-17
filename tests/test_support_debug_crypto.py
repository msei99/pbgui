"""Support attachments are authenticated, bounded and decryptable only by their recipient."""
import base64
import io
import json
import zipfile
from types import SimpleNamespace

import pytest
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import FastAPI
from fastapi.testclient import TestClient

import support_debug_crypto as crypto
from api import support_debug as api
from api.auth import require_auth


@pytest.fixture(scope='module')
def recipient():
    """Use ephemeral key material, never the real operator private key."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=3072)
    return key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                             serialization.NoEncryption()), key.public_key().public_bytes(
                                 serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)


@pytest.fixture
def encrypted_setup(tmp_path, monkeypatch, recipient):
    """Isolate the recipient key and return an exact reviewed report."""
    public = tmp_path / 'public.pem'; public.write_bytes(recipient[1])
    monkeypatch.setattr(crypto, 'PUBLIC_KEY_PATH', public)
    return json.dumps({'format': 'pbgui-support-debug-v1', 'logs': {'job': 'PRIVATE_DIAGNOSTICS_ä'}}, indent=2)


def rewrite_envelope(attachment, change):
    """Alter the untrusted archive without changing ZIP validity."""
    with zipfile.ZipFile(io.BytesIO(attachment)) as archive:
        value = json.loads(archive.read('encrypted-report.json'))
    change(value)
    output = io.BytesIO()
    with zipfile.ZipFile(output, 'w') as archive:
        archive.writestr('encrypted-report.json', json.dumps(value))
    return output.getvalue()


def test_roundtrip_randomized_and_no_plaintext(encrypted_setup, recipient):
    """The exact snapshot survives, while identical reports produce different ciphertexts."""
    first = crypto.encrypt_report(encrypted_setup)
    second = crypto.encrypt_report(encrypted_setup)
    assert first != second
    assert crypto.decrypt_report(first, recipient[0]).decode() == encrypted_setup
    with zipfile.ZipFile(io.BytesIO(first)) as archive:
        assert archive.namelist() == ['encrypted-report.json']
        envelope = archive.read('encrypted-report.json')
    assert b'PRIVATE_DIAGNOSTICS' not in envelope and b'logs' not in envelope
    assert len(first) < 25_000_000


@pytest.mark.parametrize('part', ['ciphertext', 'nonce', 'wrapped_key', 'key_id', 'compression'])
def test_modified_attachments_are_rejected(encrypted_setup, recipient, part):
    """Ciphertext, key and authenticated metadata modifications fail closed."""
    def alter(value):
        """Corrupt metadata or one decoded byte without invalidating base64."""
        if part in ('key_id', 'compression'):
            value[part] = 'modified'
        else:
            raw = bytearray(base64.b64decode(value[part])); raw[0] ^= 1
            value[part] = base64.b64encode(raw).decode()
    damaged = rewrite_envelope(crypto.encrypt_report(encrypted_setup), alter)
    with pytest.raises((ValueError, InvalidTag)):
        crypto.decrypt_report(damaged, recipient[0])


def test_invalid_reports_wrong_key_and_expansion_limit(encrypted_setup, recipient, monkeypatch):
    """Reject foreign snapshots, recipients and oversized decompressed payloads."""
    for invalid in ('[]', '{}', 'not json'):
        with pytest.raises(ValueError):
            crypto.encrypt_report(invalid)
    attachment = crypto.encrypt_report(encrypted_setup)
    wrong = rsa.generate_private_key(public_exponent=65537, key_size=3072)
    wrong_pem = wrong.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                   serialization.NoEncryption())
    with pytest.raises(ValueError, match='different support key'):
        crypto.decrypt_report(attachment, wrong_pem)
    monkeypatch.setattr(crypto, 'MAX_REPORT_BYTES', 16)
    with pytest.raises(ValueError, match='input limit'):
        crypto.encrypt_report(encrypted_setup)
    with pytest.raises(ValueError, match='compressed report'):
        crypto.decrypt_report(attachment, recipient[0])


def test_authenticated_bounded_attachment_api(encrypted_setup, recipient, monkeypatch):
    """The endpoint returns only a ZIP, checks auth/size and does not persist reports."""
    monkeypatch.setattr(api, '_log', lambda *args, **kwargs: None)
    app = FastAPI(); app.include_router(api.router, prefix='/api/debug')
    with TestClient(app) as client:
        assert client.post('/api/debug/issue-attachment', content=encrypted_setup).status_code == 401
        app.dependency_overrides[require_auth] = lambda: SimpleNamespace(user_id='fixture')
        response = client.post('/api/debug/issue-attachment', content=encrypted_setup)
        assert response.status_code == 200
        assert response.headers['cache-control'] == 'no-store'
        assert response.headers['content-type'] == 'application/zip'
        assert crypto.decrypt_report(response.content, recipient[0]).decode() == encrypted_setup
        assert client.post('/api/debug/issue-attachment', content='{"format":"wrong"}').status_code == 422
        assert client.post('/api/debug/issue-attachment', content=b'\xff').status_code == 422
        monkeypatch.setattr(api, 'MAX_REPORT_BYTES', 16)
        assert client.post('/api/debug/issue-attachment', content=encrypted_setup).status_code == 413
        monkeypatch.setattr(api, 'MAX_REPORT_BYTES', crypto.MAX_REPORT_BYTES)
        monkeypatch.setattr(api, 'encrypt_report', lambda value: (_ for _ in ()).throw(FileNotFoundError('PRIVATE_KEY_PATH')))
        response = client.post('/api/debug/issue-attachment', content=encrypted_setup)
        assert response.status_code == 500 and 'PRIVATE_KEY_PATH' not in response.text


def test_screenshot_roundtrip_and_safe_owner_only_extraction(encrypted_setup, recipient, tmp_path):
    """Image bytes survive encryption; supplied filenames cannot escape the private output folder."""
    png = b'\x89PNG\r\n\x1a\n' + b'fixture image bytes'
    report = json.loads(encrypted_setup)
    report['screenshots'] = [{'name':'../../pbgui.ini','media_type':'image/png',
                              'data_base64':base64.b64encode(png).decode()}]
    raw = json.dumps(report)
    decrypted = crypto.decrypt_report(crypto.encrypt_report(raw), recipient[0])
    assert decrypted.decode() == raw
    directory = tmp_path/'images'
    files = crypto.extract_screenshots(decrypted, directory)
    assert files == [directory/'screenshot-001.png']
    assert files[0].read_bytes() == png
    assert files[0].stat().st_mode & 0o777 == 0o600
    assert directory.stat().st_mode & 0o777 == 0o700
    with pytest.raises(ValueError, match='already exists'):
        crypto.extract_screenshots(decrypted, directory)


@pytest.mark.parametrize('media,data', [('image/svg+xml',b'<svg/>'),('image/png',b'<svg/>'),
                                      ('image/jpeg',b'not jpeg'),('image/webp',b'not webp')])
def test_invalid_screenshot_content_is_rejected(encrypted_setup, media, data):
    """Reject unsupported and mislabeled images before encrypting or extracting them."""
    report = json.loads(encrypted_setup)
    report['screenshots'] = [{'media_type':media,'data_base64':base64.b64encode(data).decode()}]
    with pytest.raises(ValueError):
        crypto.encrypt_report(json.dumps(report))
