"""Supervise one pre-authorized rental from before creation through destruction.

Run under a persistent user systemd service. A durable create-attempt marker
prevents a service restart from renting twice, even after an ambiguous response.
Only the immutable intent's unique label is recovered or destroyed.
"""

import argparse
import base64
import fcntl
import json
import os
from pathlib import Path
import re
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parent))
from cleanup import ROOT, WORK_ROOT, RejectRedirects, VastCleanupClient, validate_cleanup_job
from secure_files import atomic_write_private_text, secure_private_file

SERVICE = "VastGpuRental"

# Only the provider's per-container credential is read inside the worker.
# No account or registry credential is embedded in this public startup source.
WORKER_SOURCE = r'''
import json, os, time, urllib.request
from pathlib import Path
deadline = float(os.environ["PBGUI_BENCHMARK_DEADLINE"])
instance_id = int(os.environ["CONTAINER_ID"])
key = os.environ["CONTAINER_API_KEY"]
class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args):
        return None
opener = urllib.request.build_opener(NoRedirect())
Path("/work/worker-guard-ready.json").write_text(json.dumps({"instance_id": instance_id, "deadline": deadline}))
limit = time.monotonic() + max(0, deadline - time.time())
while time.time() < deadline and time.monotonic() < limit:
    time.sleep(min(10, max(0, deadline - time.time())))
while True:
    try:
        request = urllib.request.Request("https://console.vast.ai/api/v0/instances/" + str(instance_id), method="DELETE", headers={"Authorization": "Bearer " + key})
        with opener.open(request, timeout=20) as response:
            json.load(response)
    except Exception:
        Path("/work/worker-guard-error.txt").write_text("Destroy request failed; retrying. Local guard independently verifies deletion.\n")
    time.sleep(10)
'''


def rental_request(key, method, path, body):
    """Send an authenticated body without logging credentials or provider echoes."""
    request = urllib.request.Request("https://console.vast.ai" + path, method=method,
        data=json.dumps(body).encode(), headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"})
    try:
        with urllib.request.build_opener(RejectRedirects()).open(request, timeout=30) as response:
            result = json.load(response)
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Vast HTTP {exc.code}") from None
    except (OSError, ValueError):
        raise RuntimeError("Vast request failed; response suppressed") from None
    if not isinstance(result, dict):
        raise RuntimeError("Unexpected Vast response schema")
    return result


def validate_rental_intent(intent):
    """Validate a narrow one-off benchmark authorization before any network call."""
    validate_cleanup_job({**intent, "instance_id": 1})
    if type(intent.get("offer_id")) is not int or intent["offer_id"] <= 0:
        raise ValueError("Invalid offer ID")
    authorized_budget = 3 if intent.get("extended_twelve_hour_lease_authorized") is True else 1
    if intent.get("disk_gb") != 40 or intent.get("budget_usd") != authorized_budget:
        raise ValueError("Unexpected authorized budget or storage")
    if not re.fullmatch(r"ghcr.io/msei99/pbgui-pb8-gpu@sha256:[0-9a-f]{64}", intent.get("image", "")):
        raise ValueError("Unexpected pinned image")
    return intent


def rental_payload(intent, pull_token):
    """Keep private registry login exclusively in the authenticated API body."""
    if not re.fullmatch(r"[A-Za-z0-9_]+", pull_token):
        raise ValueError("Invalid registry credential format")
    encoded = base64.b64encode(WORKER_SOURCE.encode()).decode()
    startup = "umask 077\ntouch /root/.no_auto_tmux\nmkdir -p /work\n/usr/local/bin/python -c \"import base64,pathlib; pathlib.Path('/work/worker_guard.py').write_bytes(base64.b64decode('" + encoded + "'))\"\nnohup /usr/local/bin/python /work/worker_guard.py >/work/worker-guard.log 2>&1 </dev/null &\n"
    return {"client_id": "me", "image": intent["image"], "disk": 40,
        "label": intent["label"], "runtype": "ssh_direct", "target_state": "running",
        "cancel_unavail": True, "onstart": startup,
        "env": {"PBGUI_BENCHMARK_DEADLINE": str(intent["deadline"])},
        "image_login": "-u msei99 -p " + pull_token + " ghcr.io"}


def vast_rental_main():
    """Persist intent before creation and keep reconciling until confirmed absent."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--intent", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    intent_path = args.intent.resolve(strict=True)
    intent_path.relative_to(WORK_ROOT.resolve(strict=True))
    intent = validate_rental_intent(json.loads(intent_path.read_text()))
    prefix = intent_path.with_suffix("")
    marker = prefix.with_suffix(".attempt.json")
    status_path = prefix.with_suffix(".status.json")
    finish = prefix.with_suffix(".finish")
    lock_path = prefix.with_suffix(".lock")
    with lock_path.open("a") as lock:
        secure_private_file(lock_path)
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        key = (WORK_ROOT / "credentials/vast_api_key").read_text().strip()
        client = VastCleanupClient(key)
        if not marker.exists():
            if time.time() >= intent["deadline"] or finish.exists():
                raise ValueError("Rental authorization has expired or been cancelled")
            # This must be durable BEFORE the only create request. Never retry it.
            atomic_write_private_text(marker, json.dumps({"attempted_at": time.time()}, indent=4))
            pull_token = (WORK_ROOT / "credentials/ghcr_pull_token").read_text().strip()
            try:
                response = rental_request(key, "PUT", f"/api/v0/asks/{intent['offer_id']}/", rental_payload(intent, pull_token))
                created_id = response.get("new_contract")
                if response.get("success") is not True or type(created_id) is not int:
                    raise RuntimeError("Rental was not confirmed; recovering by unique label")
                atomic_write_private_text(prefix.with_suffix(".created.json"), json.dumps({"instance_id": created_id}, indent=4))
            except RuntimeError as exc:
                atomic_write_private_text(status_path, json.dumps({"state": "creation_uncertain", "error": str(exc)}, indent=4))
            del pull_token
        absent = 0
        monotonic_deadline = time.monotonic() + max(0, intent["deadline"] - time.time())
        while True:
            ending = finish.exists() or time.time() >= intent["deadline"] or time.monotonic() >= monotonic_deadline
            try:
                matches = [row for row in client.instances() if row.get("label") == intent["label"]]
                if len(matches) > 1:
                    raise ValueError("Duplicate unique rental label; manual inspection required")
                row = matches[0] if matches else None
                absent = absent + 1 if row is None and ending else 0
                state = "absent" if row is None else "active"
                record = {"state": state, "updated_at": time.time(), "deadline": intent["deadline"]}
                if row:
                    record.update({name: row.get(name) for name in ("id", "actual_status", "cur_state", "ssh_host", "ssh_port", "public_ipaddr", "ports", "dph_total")})
                    if ending:
                        client.destroy(row["id"])
                        record["state"] = "destroy_requested"
                if absent >= 2:
                    record["state"] = "deletion_verified"
                atomic_write_private_text(status_path, json.dumps(record, indent=4))
                if absent >= 2:
                    return 0
            except (RuntimeError, ValueError) as exc:
                absent = 0
                atomic_write_private_text(status_path, json.dumps({"state": "retrying", "error": str(exc), "updated_at": time.time()}, indent=4))
            time.sleep(10)


if __name__ == "__main__":
    raise SystemExit(vast_rental_main())
