"""Encrypt support attachments; offline operator CLI decrypts with a separate key."""
from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import os
import zipfile
import zlib
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

FORMAT = 'pbgui-encrypted-support-v1'
MAX_REPORT_BYTES = 12 * 1024 * 1024
MAX_ATTACHMENT_BYTES = 24 * 1024 * 1024
PUBLIC_KEY_PATH = Path(__file__).resolve().parent / 'support/debug_public_key.pem'


def decoded_screenshots(report):
    """Validate bounded raster attachments; never trust supplied names as output paths."""
    screenshots = report.get('screenshots', [])
    if not isinstance(screenshots, list) or len(screenshots) > 5:
        raise ValueError('At most five screenshots are allowed')
    result = []
    for item in screenshots:
        if not isinstance(item, dict):
            raise ValueError('Invalid screenshot')
        media_type = item.get('media_type')
        suffix = {'image/png': '.png', 'image/jpeg': '.jpg', 'image/webp': '.webp'}.get(media_type)
        value = item.get('data_base64')
        if not suffix or not isinstance(value, str) or len(value) > (4 * 1024 * 1024 + 2) // 3 * 4:
            raise ValueError('Unsupported or oversized screenshot')
        data = base64.b64decode(value, validate=True)
        if not data or len(data) > 4 * 1024 * 1024:
            raise ValueError('Unsupported or oversized screenshot')
        matches = (media_type == 'image/png' and data.startswith(b'\x89PNG\r\n\x1a\n') or
                   media_type == 'image/jpeg' and data.startswith(b'\xff\xd8\xff') or
                   media_type == 'image/webp' and data.startswith(b'RIFF') and data[8:12] == b'WEBP')
        if not matches:
            raise ValueError('Screenshot media type does not match its content')
        result.append((suffix, data))
    return result


def extract_screenshots(report: bytes, directory: Path):
    """Save authenticated images under generated names in a new owner-only directory."""
    from secure_files import atomic_write_private_bytes, ensure_private_directory
    images = decoded_screenshots(json.loads(report))
    if not images:
        return []
    directory = Path(directory)
    if directory.exists() or directory.is_symlink():
        raise ValueError('Screenshot directory already exists')
    ensure_private_directory(directory)
    paths = []
    for index, (suffix, data) in enumerate(images, 1):
        path = directory / f'screenshot-{index:03d}{suffix}'
        atomic_write_private_bytes(path, data)
        paths.append(path)
    return paths


def _oaep():
    """Use RSA-OAEP with SHA-256 for the ephemeral AES key."""
    return padding.OAEP(mgf=padding.MGF1(hashes.SHA256()), algorithm=hashes.SHA256(), label=None)


def encrypt_report(report: str) -> bytes:
    """Compress then authenticate/encrypt the exact reviewed UTF-8 report in a ZIP."""
    raw = report.encode('utf-8')
    if len(raw) > MAX_REPORT_BYTES:
        raise ValueError('Report exceeds the 12 MiB input limit')
    parsed = json.loads(raw)
    if not isinstance(parsed, dict) or parsed.get('format') != 'pbgui-support-debug-v1':
        raise ValueError('Not a PBGui debug report')
    decoded_screenshots(parsed)
    public = serialization.load_pem_public_key(PUBLIC_KEY_PATH.read_bytes())
    if not isinstance(public, rsa.RSAPublicKey) or public.key_size < 3072:
        raise ValueError('Invalid support encryption key')
    fingerprint = hashlib.sha256(public.public_bytes(serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo)).hexdigest()
    header = {'format': FORMAT, 'key_id': fingerprint,
              'algorithm': 'RSA-OAEP-SHA256+AES-256-GCM', 'compression': 'zlib'}
    aad = json.dumps(header, sort_keys=True).encode('ascii')
    key, nonce = AESGCM.generate_key(bit_length=256), os.urandom(12)
    envelope = dict(header, wrapped_key=base64.b64encode(public.encrypt(key, _oaep())).decode('ascii'),
                    nonce=base64.b64encode(nonce).decode('ascii'),
                    ciphertext=base64.b64encode(AESGCM(key).encrypt(nonce, zlib.compress(raw), aad)).decode('ascii'))
    output = io.BytesIO()
    with zipfile.ZipFile(output, 'w', compression=zipfile.ZIP_STORED) as archive:
        archive.writestr('encrypted-report.json', json.dumps(envelope, indent=4))
    result = output.getvalue()
    if len(result) > MAX_ATTACHMENT_BYTES:
        raise ValueError('Encrypted attachment exceeds the 24 MiB upload limit')
    return result


def decrypt_report(attachment: bytes, private_pem: bytes) -> bytes:
    """Validate/authenticate an untrusted attachment before bounded decompression."""
    if len(attachment) > MAX_ATTACHMENT_BYTES:
        raise ValueError('Attachment too large')
    with zipfile.ZipFile(io.BytesIO(attachment)) as archive:
        entries = archive.infolist()
        if len(entries) != 1 or entries[0].filename != 'encrypted-report.json' or entries[0].file_size > MAX_ATTACHMENT_BYTES:
            raise ValueError('Invalid attachment archive')
        envelope = json.loads(archive.read(entries[0]))
    header = {name: envelope[name] for name in ('format', 'key_id', 'algorithm', 'compression')}
    if header['format'] != FORMAT or header['algorithm'] != 'RSA-OAEP-SHA256+AES-256-GCM' or header['compression'] != 'zlib':
        raise ValueError('Unsupported attachment format')
    private = serialization.load_pem_private_key(private_pem, password=None)
    if not isinstance(private, rsa.RSAPrivateKey):
        raise ValueError('Expected RSA private key')
    fingerprint = hashlib.sha256(private.public_key().public_bytes(serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo)).hexdigest()
    if header['key_id'] != fingerprint:
        raise ValueError('This attachment requires a different support key')
    key = private.decrypt(base64.b64decode(envelope['wrapped_key'], validate=True), _oaep())
    compressed = AESGCM(key).decrypt(base64.b64decode(envelope['nonce'], validate=True),
        base64.b64decode(envelope['ciphertext'], validate=True), json.dumps(header, sort_keys=True).encode('ascii'))
    decoder = zlib.decompressobj()
    raw = decoder.decompress(compressed, MAX_REPORT_BYTES + 1)
    if len(raw) > MAX_REPORT_BYTES or not decoder.eof or decoder.unused_data:
        raise ValueError('Invalid or oversized compressed report')
    parsed = json.loads(raw)
    if not isinstance(parsed, dict) or parsed.get('format') != 'pbgui-support-debug-v1':
        raise ValueError('Not a PBGui debug report')
    decoded_screenshots(parsed)
    return raw


def main():
    """Decrypt locally without printing report contents or exposing private key values."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('attachment', type=Path)
    parser.add_argument('--private-key', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--screenshots-dir', type=Path, help='Optional new directory for decrypted images')
    args = parser.parse_args()
    from secure_files import atomic_write_private_bytes
    if args.output.exists() or args.output.is_symlink():
        parser.error('Output already exists; choose a new path')
    try:
        if args.attachment.stat().st_size > MAX_ATTACHMENT_BYTES:
            raise ValueError('Attachment too large')
        result = decrypt_report(args.attachment.read_bytes(), args.private_key.read_bytes())
        if args.screenshots_dir:
            extract_screenshots(result, args.screenshots_dir)
        atomic_write_private_bytes(args.output, result)
    except Exception as exc:
        parser.exit(1, f'Decryption/export failed ({type(exc).__name__}).\n')
    print('Report decrypted to the requested owner-only output file.')


if __name__ == '__main__':
    main()
