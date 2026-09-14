"""Bounded server-side Vast REST access with explicit public response projections."""

from __future__ import annotations

import hashlib
from pathlib import Path
from email.utils import parsedate_to_datetime

from file_lock import advisory_file_lock
from secure_files import ensure_private_directory, read_regular_file_nofollow, atomic_write_private_text

import json
import math
import time
import urllib.error
import urllib.parse
import urllib.request

SERVICE = "Vast"
BASE_URL = "https://console.vast.ai/api/v0"
COORDINATION_ROOT = Path(__file__).resolve().parent / "data/vast/provider"
INSTANCE_TTL = 15
REFERRAL_URL = "https://cloud.vast.ai/?ref_id=522435"


class VastError(RuntimeError):
    """A safe provider error which never includes response bodies or credentials."""

    def __init__(self, message: str, status: int = 502) -> None:
        """Retain only the public explanation and appropriate HTTP status."""
        super().__init__(message)
        self.status = status


class VastRateLimit(VastError):
    """Distinguish a provider refusal from a locally deferred, unsent request."""
    def __init__(self, retry_after: float, *, request_sent: bool):
        super().__init__('Vast API rate limit; automatic retry is waiting', 429)
        self.retry_after = retry_after
        self.request_sent = request_sent


def retry_after_seconds(value: str | None) -> float:
    """Parse Retry-After seconds or an HTTP date without exposing provider data."""
    try:
        seconds = float(value)
        return max(0, seconds) if math.isfinite(seconds) else 0
    except (ValueError, TypeError):
        try:
            return max(0, parsedate_to_datetime(value).timestamp() - time.time())
        except (ValueError, TypeError, OverflowError):
            return 0


class NoRedirect(urllib.request.HTTPRedirectHandler):
    """Reject redirects instead of forwarding a provider credential."""

    def redirect_request(self, request, fp, code, message, headers, new_url):
        """Refuse even same-origin redirects; endpoints are canonical."""
        return None


def number(value: object, *, minimum: float | None = 0) -> float | None:
    """Return a finite provider number, preserving unknown values as null."""
    if isinstance(value, bool) or value is None:
        return None
    try:
        result = float(value)
    except (ValueError, TypeError, OverflowError):
        return None
    return result if math.isfinite(result) and (minimum is None or result >= minimum) else None


def positive_id(value: object) -> int:
    """Validate a provider identifier without accepting floats or booleans."""
    if type(value) is not int or value <= 0:
        raise VastError("Invalid Vast identifier", 422)
    return value


def public_text(value: object, limit: int = 100) -> str:
    """Bound provider strings and remove control characters."""
    return "".join(c for c in str(value or "") if ord(c) >= 32 and ord(c) != 127)[:limit]


def gpu_name_matches(name: str, search: str) -> bool:
    """Match model fragments regardless of case, spaces, underscores or hyphens."""
    normalize = lambda value: ''.join(char for char in value.casefold() if char.isalnum())
    return normalize(search) in normalize(name)


class VastClient:
    """Own one credential and use short-lived, bounded HTTP connections."""

    def __init__(self, api_key: str) -> None:
        """Keep keys in memory, outside URLs, command arguments and diagnostics."""
        if not isinstance(api_key, str) or not api_key.strip() or len(api_key) > 4096 or any(ord(c) < 33 or ord(c) > 126 for c in api_key):
            raise VastError("Enter a valid Vast API key", 422)
        self._key = api_key

    def request(self, method: str, path: str, body: dict | None = None, *, fresh: bool = False) -> dict:
        """Serialize callers, share cooldown and cache only safe instance metadata."""
        root = ensure_private_directory(COORDINATION_ROOT)
        state_path = root / 'state.json'
        fingerprint = hashlib.sha256(self._key.encode()).hexdigest()
        with advisory_file_lock(root / '.http-lock'):
            state = {}
            if state_path.exists():
                try:
                    state = json.loads(read_regular_file_nofollow(state_path, root))
                    if not isinstance(state, dict):
                        raise ValueError('invalid')
                except (OSError, ValueError, RuntimeError):
                    raise VastError('Vast API coordination state cannot be read') from None
            if state.get('account') != fingerprint:
                state = {'account': fingerprint}
            now = time.time()
            remaining = state.get('retry_at', 0) - now
            if remaining > 0:
                raise VastRateLimit(remaining, request_sent=False)
            listing = method == 'GET' and path == '/instances/'
            if listing and not fresh and 0 <= now - state.get('instances_at', 0) < INSTANCE_TTL and 'instances' in state:
                return {'instances': state['instances']}
            try:
                result = self._request(method, path, body)
            except VastRateLimit as exc:
                failures = min(int(state.get('failures', 0)) + 1, 5)
                delay = max(exc.retry_after, min(300, 30 * 2 ** (failures - 1)))
                state.update(failures=failures, retry_at=time.time() + delay)
                atomic_write_private_text(state_path, json.dumps(state, indent=4))
                raise VastRateLimit(delay, request_sent=True) from None
            state.update(failures=0, retry_at=0)
            if listing and isinstance(result.get('instances'), list):
                fields = ('id', 'label', 'actual_status', 'public_ipaddr', 'ports')
                rows = result['instances']
                if all(isinstance(row, dict) and type(row.get('id')) is int for row in rows):
                    state.update(instances=[{key: row[key] for key in fields if key in row} for row in rows], instances_at=time.time())
            elif method != 'GET':
                state.pop('instances', None)
                state.pop('instances_at', None)
            atomic_write_private_text(state_path, json.dumps(state, indent=4))
            return result

    def _request(self, method: str, path: str, body: dict | None = None) -> dict:
        """Call a fixed provider path, masking all upstream error payloads."""
        if not path.startswith("/") or ".." in path or any(c in path for c in "?#\\\r\n"):
            raise VastError("Invalid Vast endpoint", 422)
        endpoint = BASE_URL + path
        if method == 'GET' and path == '/charges/' and body is not None:
            endpoint += '?' + urllib.parse.urlencode(body)
            body = None
        req = urllib.request.Request(
            endpoint, method=method,
            data=json.dumps(body, allow_nan=False).encode() if body is not None else None,
            headers={"Authorization": "Bearer " + self._key, "Content-Type": "application/json"},
        )
        try:
            with urllib.request.build_opener(NoRedirect()).open(req, timeout=20) as response:
                raw = response.read(4 * 1024 * 1024 + 1)
            if len(raw) > 4 * 1024 * 1024:
                raise VastError("Vast response exceeds the size limit")
            result = json.loads(raw)
        except urllib.error.HTTPError as exc:
            if exc.code == 429:
                raise VastRateLimit(retry_after_seconds(exc.headers.get("Retry-After") if exc.headers else None), request_sent=True) from None
            messages = {
                401: "Vast rejected the API key",
                403: "Vast key lacks permission for this operation",
                404: "Vast resource is no longer available",
                410: "Vast offer is no longer available",
                429: "Vast rate limit reached; try again shortly",
            }
            message = messages.get(exc.code, "Vast request failed")
            raise VastError(f"{message} (HTTP {exc.code})", 502) from None
        except (OSError, ValueError, urllib.error.URLError):
            raise VastError("Vast could not be reached or returned invalid data") from None
        if not isinstance(result, dict) or result.get("success") is False:
            raise VastError("Vast did not confirm this operation")
        return result

    def account(self) -> dict:
        """Expose balance and account ID only, never email, sid or SSH material."""
        row = self.request("GET", "/users/current/")
        # Vast's prepaid rental funds are `credit`; `balance` is a separate
        # ledger field and can be zero while spendable credit remains.
        return {
            "account_id": row.get("id") if type(row.get("id")) is int else None,
            "balance_usd": number(row.get("credit"), minimum=None),
            "checked_at": time.time(),
        }

    def offers(self, *, max_price: float = 1, min_vram: float = 12,
               min_ram: float = 16, min_cpu: float = 4, disk_gb: int = 40,
               verified_only: bool = True, gpu_name: str = "", offer_id: int | None = None,
               min_cuda: float = 0, min_duration: float = 0) -> list[dict]:
        """Search a bounded page of single-GPU on-demand offers."""
        values = [number(x) for x in (max_price, min_vram, min_ram, min_cpu, disk_gb, min_cuda, min_duration)]
        if any(x is None for x in values) or not (0 < max_price <= 100 and 1 <= disk_gb <= 2000):
            raise VastError("Invalid GPU search limits", 422)
        query = {"rentable": {"eq": True}, "rented": {"eq": False}, "gpu_arch": {"eq": "nvidia"},
                 "num_gpus": {"eq": 1}, "gpu_ram": {"gte": min_vram * 1024},
                 "cpu_ram": {"gte": min_ram * 1024}, "cpu_cores_effective": {"gte": min_cpu},
                 "dph_total": {"lte": max_price}, "disk_space": {"gte": disk_gb},
                 "type": "on-demand", "allocated_storage": disk_gb,
                 "order": [["dph_total", "asc"]], "limit": 100}
        if min_cuda:
            query["cuda_max_good"] = {"gte": min_cuda}
        if min_duration:
            query["duration"] = {"gte": min_duration}
        if verified_only:
            query["verified"] = {"eq": True}
        if offer_id is not None:
            query["ask_contract_id"] = {"eq": positive_id(offer_id)}
        matching_names = None
        if gpu_name.strip():
            names = self.request("GET", "/gpu_names/unique/").get("gpu_names")
            if not isinstance(names, list) or len(names) > 2000 or any(not isinstance(name, str) for name in names):
                raise VastError("Vast returned an invalid GPU model list")
            matching_names = [name for name in names if gpu_name_matches(name, gpu_name)]
            exact = [name for name in matching_names if gpu_name_matches(name, gpu_name) and gpu_name_matches(gpu_name, name)]
            matching_names = exact or matching_names
            if not matching_names:
                return []
            query["gpu_name"] = {"in": matching_names}
        payload = self.request("POST", "/bundles", query)
        rows = payload.get("offers")
        if not isinstance(rows, list):
            raise VastError("Vast returned an invalid offer list")
        output = []
        for row in rows[:100]:
            if not isinstance(row, dict) or type(row.get("id")) is not int:
                continue
            price = number(row.get("dph_total"))
            if price is None or price > max_price:
                continue
            if matching_names is not None and row.get("gpu_name") not in matching_names:
                continue
            cuda = number(row.get("cuda_max_good"))
            duration = number(row.get("duration"))
            if min_cuda and (cuda is None or cuda < min_cuda):
                continue
            if min_duration and duration is not None and duration < min_duration:
                continue
            output.append({
                "id": positive_id(row["id"]), "gpu_name": public_text(row.get("gpu_name")),
                "num_gpus": number(row.get("num_gpus")),
                "vram_gb": (number(row.get("gpu_ram")) or 0) / 1024,
                "ram_gb": (number(row.get("cpu_ram")) or 0) / 1024,
                "cpu_cores": number(row.get("cpu_cores_effective")),
                "cpu_name": public_text(row.get("cpu_name")),
                "gpu_mem_bw_gbps": number(row.get("gpu_mem_bw")),
                "tflops": number(row.get("total_flops")),
                "pci_gen": number(row.get("pci_gen")),
                "gpu_lanes": number(row.get("gpu_lanes")),
                "pcie_bw_gbps": number(row.get("pcie_bw")),
                "disk_name": public_text(row.get("disk_name")),
                "disk_bw_mbps": number(row.get("disk_bw")),
                "duration_seconds": duration,
                "price_hour_usd": price, "disk_gb": disk_gb,
                "storage_month_gb_usd": number(row.get("storage_cost")),
                "upload_gb_usd": number(row.get("inet_up_cost")),
                "download_gb_usd": number(row.get("inet_down_cost")),
                "inet_down_mbps": number(row.get("inet_down")),
                "inet_up_mbps": number(row.get("inet_up")),
                "reliability": number(row.get("reliability2", row.get("reliability"))),
                "verified": row.get("verification") == "verified" or row.get("verified") is True,
                "location": public_text(row.get("geolocation")),
                "cuda_max_good": number(row.get("cuda_max_good")),
            })
        return sorted(output, key=lambda row: row["price_hour_usd"])

    def instances(self, *, fresh: bool = False) -> list[dict]:
        """Return validated instance records to server-side ownership checks only."""
        rows = (self.request("GET", "/instances/", fresh=True) if fresh else self.request("GET", "/instances/")).get("instances")
        if not isinstance(rows, list) or any(not isinstance(r, dict) or type(r.get("id")) is not int for r in rows):
            raise VastError("Vast returned an invalid instance list")
        return rows
