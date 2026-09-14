"""Anonymous GHCR image preflight before authorizing a paid cloud rental."""
import hashlib
import json
import re
import urllib.request
from urllib.error import URLError
from vast_provider import NoRedirect, VastError

SERVICE = 'Vast'


def require_public_image(image: str) -> None:
    """Verify the pinned public manifest without using any saved GitHub credential."""
    match = re.fullmatch(r'ghcr.io/([a-z0-9][a-z0-9_/-]*)@sha256:([0-9a-f]{64})', image)
    if not match:
        raise VastError('Invalid public worker image identity', 422)
    repository, digest = match.groups()
    opener = urllib.request.build_opener(NoRedirect())
    try:
        url = 'https://ghcr.io/token?service=ghcr.io&scope=repository:' + repository + ':pull'
        with opener.open(url, timeout=10) as response:
            raw = response.read(131073)
        if len(raw) > 131072:
            raise ValueError('oversized')
        token = json.loads(raw)['token']
        if not isinstance(token, str) or not token or any(c in token for c in '\r\n'):
            raise ValueError('invalid')
        request = urllib.request.Request('https://ghcr.io/v2/' + repository + '/manifests/sha256:' + digest,
            headers={'Authorization': 'Bearer ' + token,
                     'Accept': 'application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.manifest.v1+json'})
        with opener.open(request, timeout=10) as response:
            manifest = response.read(4 * 1024 * 1024 + 1)
        if len(manifest) > 4 * 1024 * 1024 or hashlib.sha256(manifest).hexdigest() != digest:
            raise ValueError('manifest identity mismatch')
    except (OSError, URLError, ValueError, KeyError):
        raise VastError('The public worker image cannot be verified anonymously. No GPU was rented; check image publication or retry the connection.', 409) from None
