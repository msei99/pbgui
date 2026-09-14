"""Independent deadline guard for one explicitly authorized Vast benchmark rental."""

from __future__ import annotations

import argparse
import fcntl
import json
import math
import os
from pathlib import Path
import re
import sys
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from secure_files import atomic_write_private_text, secure_private_file

SERVICE = "VastGpuCleanup"
WORK_ROOT = ROOT / ".local-work/vast-gpu-test"


class PermanentCleanupError(ValueError):
    """A permission or schema failure requiring intervention rather than retries."""


class RejectRedirects(urllib.request.HTTPRedirectHandler):
    """Never forward the account credential to a redirect destination."""

    def redirect_request(self, request, fp, code, message, headers, new_url):
        """Reject every redirect; the client uses canonical fixed endpoints."""
        return None


class VastCleanupClient:
    """Use only account-local instance listing and exact-ID destruction."""

    def __init__(self, key: str):
        """Keep the credential in memory, never in command arguments or output."""
        self._key = key

    def _request(self, method: str, path: str) -> dict:
        """Return JSON while replacing potentially sensitive transport errors."""
        request = urllib.request.Request(
            "https://console.vast.ai" + path,
            method=method,
            headers={"Authorization": "Bearer " + self._key},
        )
        try:
            with urllib.request.build_opener(RejectRedirects()).open(request, timeout=20) as response:
                result = json.load(response)
        except urllib.error.HTTPError as error:
            error_type = RuntimeError if error.code in (408, 429) or error.code >= 500 else PermanentCleanupError
            raise error_type(f"Vast HTTP {error.code}") from None
        except (urllib.error.URLError, TimeoutError, OSError):
            raise RuntimeError("Vast transport failure") from None
        except (ValueError, UnicodeError):
            raise PermanentCleanupError("Invalid Vast JSON response") from None
        if not isinstance(result, dict):
            raise PermanentCleanupError("Unexpected Vast response schema")
        return result

    def instances(self) -> list[dict]:
        """Reject unknown response shapes instead of mistaking them for deletion."""
        result = self._request("GET", "/api/v0/instances/")
        instances = result.get("instances")
        if not isinstance(instances, list) or any(not isinstance(row, dict) or type(row.get("id")) is not int for row in instances):
            raise PermanentCleanupError("Unexpected instance-list schema")
        return instances

    def destroy(self, instance_id: int) -> None:
        """Request destruction, without treating acknowledgement as verification."""
        if type(instance_id) is not int or instance_id <= 0:
            raise ValueError("Invalid instance ID")
        result = self._request("DELETE", f"/api/v0/instances/{instance_id}")
        if result.get("success") is not True:
            raise PermanentCleanupError("Vast did not confirm the destroy request")


def validate_cleanup_job(job: dict) -> dict:
    """Require exact ownership and a bounded, explicitly authorized lease."""
    if not isinstance(job, dict):
        raise ValueError("Expected a cleanup job object")
    if type(job.get("instance_id")) is not int or job["instance_id"] <= 0:
        raise ValueError("Invalid instance ID")
    if not isinstance(job.get("label"), str) or not re.fullmatch(r"pbgui-gpu-test-[0-9a-f]{32}", job["label"]):
        raise ValueError("Invalid benchmark label")
    if job.get("destroy_authorized") is not True:
        raise ValueError("Explicit rental cleanup authorization is missing")
    for name in ("created_at", "deadline"):
        if type(job.get(name)) not in (int, float) or not math.isfinite(job[name]) or job[name] <= 0:
            raise ValueError("Invalid cleanup time")
    maximum = 14400 if job.get("extended_four_hour_lease_authorized") is True else 5400
    if job.get("extended_twelve_hour_lease_authorized") is True:
        maximum = 43200
    if not 0 < job["deadline"] - job["created_at"] <= maximum:
        raise ValueError("Cleanup deadline exceeds authorized lease")
    return job


def cleanup_step(job: dict, client: VastCleanupClient, now: float, force: bool = False) -> str:
    """Never delete before the deadline or touch an instance with another label."""
    validate_cleanup_job(job)
    if not force and now < job["deadline"]:
        return "waiting"
    matches = [row for row in client.instances() if row["id"] == job["instance_id"]]
    if not matches:
        return "absent"
    if len(matches) != 1 or matches[0].get("label") != job["label"]:
        raise ValueError("Instance ownership label mismatch; destruction refused")
    client.destroy(job["instance_id"])
    return "destroy_requested"


def vast_cleanup_main() -> int:
    """Run independently of PBGui until deletion is confirmed twice."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job", type=Path, required=True)
    parser.add_argument("--now", action="store_true", help="Destroy early after test/result retrieval")
    args = parser.parse_args()
    os.umask(0o077)
    job_file = args.job.resolve(strict=True)
    job_file.relative_to(WORK_ROOT.resolve(strict=True))
    job = validate_cleanup_job(json.loads(job_file.read_text()))
    key_file = WORK_ROOT / "credentials/vast_api_key"
    client = VastCleanupClient(key_file.read_text().strip())
    status_file = job_file.with_suffix(".status.json")
    lock_file = job_file.with_suffix(".lock")
    with lock_file.open("a") as lock:
        secure_private_file(lock_file)
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit("A cleanup guard already owns this job") from None
        consecutive_absent = 0
        # A second monotonic limit prevents a backwards wall-clock jump from
        # extending the deadline while this process remains alive.
        monotonic_deadline = time.monotonic() + max(0, job["deadline"] - time.time())
        while True:
            try:
                state = cleanup_step(job, client, time.time(), args.now or time.monotonic() >= monotonic_deadline)
                consecutive_absent = consecutive_absent + 1 if state == "absent" else 0
                if consecutive_absent >= 2:
                    state = "deletion_verified"
                error = None
            except RuntimeError as exc:
                state, error, consecutive_absent = "retrying", str(exc), 0
            except ValueError as exc:
                state, error = "refused", str(exc)
            record = {"instance_id": job["instance_id"], "state": state, "updated_at": time.time(), "error": error}
            atomic_write_private_text(status_file, json.dumps(record, indent=4) + "\n")
            if state == "deletion_verified":
                return 0
            if state == "refused":
                return 2
            time.sleep(min(30, max(1, job["deadline"] - time.time())) if state == "waiting" else 10)


if __name__ == "__main__":
    raise SystemExit(vast_cleanup_main())
