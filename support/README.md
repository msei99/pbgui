# Support attachment recipient key

`debug_public_key.pem` is the public recipient key distributed with PBGui. It
cannot decrypt reports. The API never needs or loads the private key.

The matching initial private key was generated locally at
`.local-work/support-keys/pbgui-support-private.pem` (Git-ignored, directory 0700,
file 0600). Before distributing the public key, the maintainer must back up the
private key to a secure location. Never commit, attach, log or distribute it to
user installations. Keep old private keys after rotation: existing attachments
require the original key. Their public key fingerprint is included in the
authenticated envelope.

To decrypt a downloaded attachment on the maintainer machine:

```sh
/home/mani/software/venv_pbgui/bin/python support_debug_crypto.py report.zip \
    --private-key .local-work/support-keys/pbgui-support-private.pem \
    --output /tmp/pbgui-support-report.json \
    --screenshots-dir /tmp/pbgui-support-screenshots
```

Use your PBGui Python environment on other machines. The output must not already
exist and is written owner-only. The CLI verifies authentication before bounded
decompression and never extracts archive paths or prints diagnostic contents.
It accepts only the versioned PBGui envelope. The format uses zlib compression,
AES-256-GCM (random key and 96-bit nonce per report), RSA-OAEP-SHA256 with a
3072-bit recipient key, and SHA-256 public-key identification. Envelope format,
key ID, algorithm and compression metadata are authenticated as associated data.

The optional `--screenshots-dir` exports included screenshots into a new
owner-only directory (0700), with owner-only files (0600). It must not already
exist. Files receive generated names such as `screenshot-001.png`; supplied
filenames are never used as filesystem paths. Without this option, images remain
base64-encoded in the decrypted JSON. No directory is created when there are no
screenshots. Treat decrypted screenshots as private diagnostic data.

Do not replace the public key without retaining the matching private key and
arranging a new secure backup. Do not generate recipient keys automatically on
end-user installations. Maximum plaintext is 12 MiB; maximum ZIP is 24 MiB.

## GitHub connection and publication receipts

Direct reporting uses each authenticated user's own GitHub token, never a
maintainer token. Local account records and immutable publication reviews are
encrypted with an installation-local Fernet key in
`data/support/github/vault.key`. Back up this key and the encrypted `.vault`
records together privately; neither belongs in Git or a support attachment.
Losing the key requires restoring its backup, and PBGui will not silently replace
it while encrypted records exist. Files are 0600 and the directory is 0700.
The server key is unrelated to the RSA support-recipient key above.

No remote write occurs during preparation. Final approval uploads an encrypted
ZIP and separately approved public images as release assets in the reporting
user's public `pbgui-support-assets` repository, then creates the issue in
`msei99/pbgui`. Public filenames and release tags are generated; original local
paths/names are not published. Existing private repositories are not exposed.
These use GitHub's official [issue API](https://docs.github.com/en/rest/issues/issues#create-an-issue)
and [release asset API](https://docs.github.com/en/rest/releases/assets).

Publication checkpoints are durable and cross-process locked. Repeated approval
of the same review returns the existing receipt. An ambiguous issue POST is only
reconciled using its unique public marker; it is never blindly reposted. Approved
publication is API-owned and survives browser disconnection; API shutdown leaves
a resumable/interrupted receipt. A dead publisher PID is detected on review
restoration. Partial remote releases are retained for the reporting user to
inspect; PBGui does not automatically delete repositories or assets.

The AI investigation has no access to these tokens or vault records. Its bounded
virtual tools can only inspect permitted installed source and explicitly selected
redacted diagnostic snapshots. It cannot execute a patch, tests or publication.
