"""Local-only Vast credentials, separate from cluster-distributed credentials."""

from __future__ import annotations

import json
import time
from pathlib import Path

from file_lock import advisory_file_lock
from secure_files import atomic_write_private_text, ensure_private_directory, read_regular_file_nofollow
from vast_provider import VastError

SERVICE = "Vast"
ROOT = Path(__file__).resolve().parent / "data" / "vast"


class VastCredentialStore:
    """Atomically store one local account with no import-time filesystem access."""

    def __init__(self, root: Path | None = None) -> None:
        """Accept an isolated root for tests and use no production reads at import."""
        self.root = Path(root) if root is not None else ROOT

    def _read(self) -> dict:
        """Read only the known credentials record without following symlinks."""
        path = self.root / "credentials.json"
        if not path.exists() and not path.is_symlink():
            return {}
        try:
            payload = json.loads(read_regular_file_nofollow(path, self.root))
        except (OSError, ValueError, RuntimeError):
            raise VastError("Vast credential storage could not be read", 500) from None
        if not isinstance(payload, dict):
            raise VastError("Vast credential storage is invalid", 500)
        return payload

    def metadata(self) -> dict:
        """Return only configured flags and generation, never partial key values."""
        row = self._read()
        return {"configured": bool(row.get("api_key")),
                "registry_configured": bool(row.get("registry_token")),
                "generation": row.get("generation", 0), "updated_at": row.get("updated_at")}

    def secrets(self) -> dict:
        """Load credentials for server-side provider calls; never return via API."""
        row = self._read()
        if not row.get("api_key"):
            raise VastError("Save a Vast API key first", 409)
        return row

    def save(self, *, api_key: str | None = None, registry_token: str | None = None) -> dict:
        """Merge explicit fields under the same lock used for rental creation."""
        for value in (api_key, registry_token):
            if value is not None and (not isinstance(value, str) or len(value) > 4096 or any(ord(c) < 33 or ord(c) > 126 for c in value)):
                raise VastError("Credential must contain only printable characters without spaces", 422)
        ensure_private_directory(self.root)
        with advisory_file_lock(self.root / ".lock"):
            jobs = self.root / "jobs"
            if jobs.exists():
                for path in jobs.glob("*/state.json"):
                    try:
                        job = json.loads(read_regular_file_nofollow(path, self.root))
                    except (OSError, ValueError, RuntimeError):
                        raise VastError("Cannot change credentials while job state is unreadable", 409) from None
                    if job.get("rental_state") not in ("none", "deletion_verified"):
                        raise VastError("Finish active rentals and cleanup before changing credentials", 409)
            row = self._read()
            for name, value in (("api_key", api_key), ("registry_token", registry_token)):
                if value is not None:
                    row[name] = value
            row.update(generation=int(row.get("generation", 0)) + 1, updated_at=time.time())
            atomic_write_private_text(self.root / "credentials.json", json.dumps(row, indent=4) + "\n")
        return self.metadata()
