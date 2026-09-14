"""Durable single-rental jobs and immutable local PB8 input snapshots."""

from __future__ import annotations

import copy
import hashlib
import json
import re
import shutil
import subprocess
import sys
import tarfile
import time
import uuid
from pathlib import Path

from file_lock import advisory_file_lock
from logging_helpers import human_log as _log
from secure_files import atomic_write_private_text, ensure_private_directory, read_regular_file_nofollow
from vast_credentials import VastCredentialStore
from vast_provider import VastClient, VastError, number, positive_id

SERVICE = "Vast"
PROJECT = Path(__file__).resolve().parent
IMAGE = "ghcr.io/msei99/pbgui-pb8-worker@sha256:0d827eb097a9d26c9088421097b4a9f0eacf660f08613871c48946ddf71a0a88"
REVISION = "ee2b7d49fd53ef790a66a28e2c85f2a6c8faebe8"
from vast_config_validation import METRICS, validate_cloud_config

TERMINAL = {"completed", "cancelled", "failed"}


def job_id(value: str) -> str:
    """Allow only generated IDs in filesystem paths and service names."""
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{32}", value):
        raise VastError("Invalid cloud job ID", 422)
    return value


def config_name(value: str) -> str:
    """Validate a saved optimizer name without permitting path traversal."""
    if not isinstance(value, str) or not value or len(value) > 160 or value in (".", "..") or any(c in value for c in "/\\\x00") or any(ord(c) < 32 or ord(c) == 127 for c in value):
        raise VastError("Invalid optimizer config name", 422)
    return value


def digest(path: Path) -> str:
    """Hash a file in bounded chunks."""
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def write_json(path: Path, value: dict) -> None:
    """Atomically publish a private non-secret state or manifest."""
    atomic_write_private_text(path, json.dumps(value, indent=4, allow_nan=False) + "\n")


def native_job_config(source: dict, iterations: int, workers: int, use_adg: bool) -> dict:
    """Prepare a supported fresh GPU job without local CUDA or source edits."""
    if type(iterations) is not int or not 256 <= iterations <= 10_000_000 or type(workers) is not int or not 1 <= workers <= 64:
        raise VastError("Choose 256–10,000,000 iterations and 1–64 CPU workers", 422)
    errors = validate_cloud_config(source, iterations, workers, use_adg=use_adg, image=IMAGE, revision=REVISION)
    if errors:
        raise VastError("Cloud configuration is invalid: " + "; ".join(item['path'] + ": " + item['message'] for item in errors), 422)
    config = copy.deepcopy(source)
    live, bt, opt = config["live"], config["backtest"], config["optimize"]
    if live.get("strategy_kind") not in ("trailing_martingale", "ema_anchor"):
        raise VastError("This image supports trailing_martingale and ema_anchor", 422)
    if config.get("coin_overrides") or opt.get("enable_overrides") or bt.get("coin_sources") or bt.get("market_settings_sources"):
        raise VastError("Cloud export does not yet support coin or optimizer overrides", 422)
    if bt.get("btc_collateral_cap") or (opt.get("gpu", {}).get("successive_halving") or {}).get("enabled"):
        raise VastError("Cloud jobs currently require no BTC collateral and no successive halving", 422)
    coins = live.get("approved_coins")
    if not isinstance(coins, dict) or any(not isinstance(coins.get(side), list) for side in ("long", "short")):
        raise VastError("Explicit approved coin lists are required", 422)
    for side in ("long", "short"):
        if config["bot"][side].get("hsl", {}).get("enabled"):
            raise VastError("The tested cloud profile currently requires HSL disabled", 422)
    if use_adg:
        for objective in opt["scoring"]:
            if objective.get("metric") == "gain_strategy_eq":
                objective["metric"] = "adg_strategy_eq"
    requested = {item.get("metric") for item in opt.get("scoring", []) + opt.get("limits", [])}
    unsupported = requested - METRICS
    if unsupported:
        raise VastError("Unsupported cloud metrics: " + ", ".join(sorted(str(x) for x in unsupported)) + ". For gain_strategy_eq, explicitly select the ADG job-copy option.", 422)
    config.pop("pbgui", None)
    live["user"] = "vast_optimizer"
    bt.update(ohlcv_source_dir="/work/pbgui/input/ohlcv", hlcvs_data_dir=None, base_dir="backtests")
    opt.update(backend="gpu", iters=iterations, n_cpus=workers)
    opt.setdefault("gpu", {}).update(exact_workers=workers, auto_lean_parallelism=False)
    return config


def services_available() -> bool:
    """Check local supervision support without starting a service or rental."""
    if not shutil.which("systemd-run") or not shutil.which("ssh") or not shutil.which("ssh-keygen"):
        return False
    try:
        return subprocess.run(["systemctl", "--user", "show-environment"], stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL, timeout=5).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


class JobStore:
    """Own the local cloud job namespace and cross-process state transitions."""

    def __init__(self, root: Path | None = None) -> None:
        """Avoid filesystem work until an explicit operation is requested."""
        self.root = Path(root) if root is not None else PROJECT / "data/vast"

    def directory(self, identifier: str) -> Path:
        """Resolve only existing owned job directories without following links."""
        path = self.root / "jobs" / job_id(identifier)
        if not path.is_dir() or path.is_symlink() or (self.root / "jobs").is_symlink():
            raise VastError("Cloud job not found", 404)
        return path

    def read(self, identifier: str, filename: str = "state.json") -> dict:
        """Read a bounded structured job record."""
        if filename not in {"state.json", "intent.json", "control.json", "attempt.json", "input/manifest.json"}:
            raise VastError("Invalid job record", 422)
        path = self.directory(identifier) / filename
        try:
            if path.stat().st_size > 16 * 1024 * 1024:
                raise ValueError("oversized")
            value = json.loads(read_regular_file_nofollow(path, self.root))
            if not isinstance(value, dict):
                raise ValueError("invalid")
            return value
        except (OSError, ValueError, RuntimeError):
            raise VastError("Cloud job record cannot be read", 500) from None

    def update(self, identifier: str, **changes) -> dict:
        """Serialize each read-modify-write and increment its state generation."""
        directory = self.directory(identifier)
        with advisory_file_lock(directory / ".state-lock"):
            state = self.read(identifier)
            events = []
            for key in ('status', 'rental_state', 'provider_status'):
                value = changes.get(key)
                if value != state.get(key) and value in {
                    'ready','preparing','provisioning','uploading','running','collecting','completed','failed','cancelled',
                    'active','creation_pending','destroy_pending','deletion_verified','loading','starting','stopped','exited','paused'}:
                    events.append(f'{key}: {value}')
            if type(changes.get('instance_id')) is int and changes['instance_id'] != state.get('instance_id'):
                events.append(f"Instance confirmed: {changes['instance_id']}")
            state.update(changes)
            state["updated_at"] = time.time()
            state["generation"] = int(state.get("generation", 0)) + 1
            write_json(directory / "state.json", state)
            if events:
                _log('VastRunner', f"{identifier}: " + ' · '.join(events), level='INFO')
            return state

    def list(self) -> list[dict]:
        """Read saved jobs, newest first, with no remote or service mutations."""
        folder = self.root / "jobs"
        if not folder.exists():
            return []
        if folder.is_symlink():
            raise VastError("Invalid cloud jobs directory", 500)
        rows = [self.read(p.name) for p in folder.iterdir() if p.is_dir() and re.fullmatch(r"[0-9a-f]{32}", p.name)]
        return sorted(rows, key=lambda row: row.get("created_at", 0), reverse=True)

    def exchanges(self, row: dict) -> list[str]:
        """Read frozen exchange metadata, with an export-manifest fallback for older jobs."""
        values = row.get('exchanges')
        if isinstance(values, list):
            return [value for value in values if isinstance(value, str)]
        identifier = job_id(row['id'])
        manifest_path = self.directory(identifier) / 'input/manifest.json'
        if not manifest_path.exists():
            return []
        manifest = self.read(identifier, 'input/manifest.json')
        result = []
        for entry in manifest.get('files', []):
            path = entry.get('path', '') if isinstance(entry, dict) else ''
            parts = Path(path).parts if isinstance(path, str) else ()
            if len(parts) > 1 and parts[0] == 'ohlcv':
                exchange = parts[1]
                if exchange in ('binance', 'bybit') and exchange not in result:
                    result.append(exchange)
        return result

    def control(self, identifier: str, action: str) -> dict:
        """Request stop or cleanup without directly signalling unrelated processes."""
        if action not in ("stop", "cleanup"):
            raise VastError("Invalid job action", 422)
        directory = self.directory(identifier)
        with advisory_file_lock(directory / ".control-lock"):
            current = self.read(identifier, "control.json")
            current[action] = True
            write_json(directory / "control.json", current)
        state = self.read(identifier)
        if state.get("rental_state") == "none" and not state.get("lease_id"):
            self.update(identifier, status="cancelled")
        return self.read(identifier)

    def prepare(self, name: str, source: dict, source_sha256: str, market_root: Path,
                mapping_root: Path, results_root: Path, iterations: int, workers: int, use_adg: bool,
                *, requeue_from: str | None = None) -> dict:
        """Freeze a config/data bundle locally before requesting any paid resource."""
        from pb8_config import save_prepared_pb8_config
        from setup.vast_gpu_benchmark.prepare import select_shards
        from sweep_cycles import build_sweep_plan

        config = native_job_config(source, iterations, workers, use_adg)
        try:
            shards = select_shards(config, market_root, mapping_root)
        except (ValueError, OSError) as exc:
            raise VastError("Market data export failed: " + str(exc), 422) from None
        if sum(p.stat().st_size for p, _ in shards) > 10 * 1024**3:
            raise VastError("This cloud profile supports input bundles up to 10 GB", 422)
        ensure_private_directory(self.root)
        ensure_private_directory(self.root / "jobs")
        identifier = uuid.uuid4().hex
        config["backtest"]["ohlcv_source_dir"] = "/work/pbgui/jobs/" + identifier + "/input/ohlcv"
        directory = ensure_private_directory(self.root / "jobs" / identifier)
        state = {"id": identifier, "config_name": config_name(name), "status": "preparing", "rental_state": "none",
                 "iterations": iterations, "workers": workers, "auto_cpu_workers": True, "created_at": time.time(), "generation": 0,
                 "exact_completed": 0, "gpu_candidates": 0, "error": None,
                 "exchanges": list(config["backtest"].get("exchanges", []))}
        if requeue_from is not None:
            state['requeue_from'] = job_id(requeue_from)
        write_json(directory / "state.json", state)
        write_json(directory / "control.json", {"stop": False, "cleanup": False})
        try:
            folder = ensure_private_directory(directory / "input")
            save_prepared_pb8_config(config, folder / "optimize.json")
            from scenario_windows import build_validation_plan
            sweep = build_sweep_plan(source)
            validation_plan = build_validation_plan(source)
            manifest = {"schema_version": 1, "pb8_revision": REVISION, "config_sha256": digest(folder / "optimize.json"),
                        "source_config_sha256": source_sha256, "sweep_plan": sweep, "validation_plan": validation_plan, "files": []}
            for original, relative in shards:
                destination = folder / "ohlcv" / relative
                ensure_private_directory(destination.parent)
                before = original.stat()
                with original.open("rb") as src, destination.open("xb") as dest:
                    shutil.copyfileobj(src, dest, 1024 * 1024)
                destination.chmod(0o600)
                after = original.stat()
                if (before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns):
                    raise VastError("Market data changed during preparation; prepare the job again", 409)
                manifest["files"].append({"path": str(Path("ohlcv") / relative), "bytes": after.st_size, "sha256": digest(destination)})
            write_json(folder / "manifest.json", manifest)
            with tarfile.open(directory / "input.tar.gz", "w:gz") as archive:
                archive.add(folder, arcname="input", recursive=True)
            (directory / "input.tar.gz").chmod(0o600)
            write_json(directory / "intent.json", {"id": identifier, "image": IMAGE, "pb8_revision": REVISION,
                       "results_root": str(results_root.resolve()), "bundle_sha256": digest(directory / "input.tar.gz"),
                       "bundle_bytes": (directory / "input.tar.gz").stat().st_size, "source_config_sha256": source_sha256})
            return self.update(identifier, status="ready", input_bytes=(directory / "input.tar.gz").stat().st_size,
                               use_adg=use_adg, sweep_enabled=sweep is not None)
        except Exception:
            self.update(identifier, status="failed", error="Input preparation failed; no instance rented")
            raise

    def start(self, identifier: str, offer: dict, hours: float, budget: float) -> dict:
        """Persist authorization before starting independently supervised processes."""
        if not services_available():
            raise VastError("A working user systemd service manager and OpenSSH are required", 409)
        if number(hours) is None or not .25 <= hours <= 24 or number(budget) is None or not .1 <= budget <= 100:
            raise VastError("Choose 0.25–24 hours and a budget of $0.10–$100", 422)
        positive_id(offer.get("id"))
        rate = number(offer.get("price_hour_usd"))
        if rate is None or rate <= 0 or (number(offer.get("cuda_max_good")) or 0) < 13:
            raise VastError("This worker image needs a CUDA 13 compatible offer with a known price", 422)
        if offer.get("disk_gb", 0) < 40:
            raise VastError("This image requires at least 40 GB disk", 422)
        directory = self.directory(identifier)
        with advisory_file_lock(self.root / ".lock"):
            state = self.read(identifier)
            if state["rental_state"] != "none":
                return state  # Start retries never create another rental.
            if (number(offer.get("cpu_cores")) or 0) < state["workers"]:
                raise VastError("Offer CPU allocation is below the requested worker count", 422)
            if state["status"] != "ready" or self.read(identifier, "control.json")["stop"]:
                raise VastError("Prepare a fresh job before starting", 409)
            if any(row["id"] != identifier and row["rental_state"] not in ("none", "deletion_verified") for row in self.list()):
                raise VastError("Finish the existing rental and cleanup first", 409)
            from vast_image import require_public_image
            require_public_image(IMAGE)
            secrets = VastCredentialStore(self.root).secrets()
            account = VastClient(secrets["api_key"]).account()
            if account["balance_usd"] is None or account["balance_usd"] < budget:
                raise VastError("The account needs at least the selected job budget in rental credit", 409)
            inbound = number(offer.get("download_gb_usd"))
            outbound = number(offer.get("upload_gb_usd"))
            if inbound is None or outbound is None:
                raise VastError("Choose an offer with known transfer prices", 422)
            intent = self.read(identifier, "intent.json")
            reserve = max(.05, 2 * intent["bundle_bytes"] / 1e9 * inbound + 3 * outbound * max(1, int(intent.get("job_count", 1))))
            duration = min(hours * 3600, (budget - reserve) / rate * 3600)
            if duration < 900:
                raise VastError("Budget must cover at least 15 minutes plus transfer reserve", 422)
            now = time.time()
            intent.update(offer=offer, budget_usd=budget, transfer_reserve_usd=max(reserve, budget - duration / 3600 * rate),
                          deadline=now + duration, accepted_at=now, label="pbgui-vast-" + identifier,
                          credential_generation=secrets["generation"])
            write_json(directory / "intent.json", intent)
            self.update(identifier, status="provisioning", rental_state="creation_pending", deadline=intent["deadline"],
                        budget_usd=budget, price_hour_usd=rate, offer_id=offer["id"], gpu_name=offer.get("gpu_name"),
                        started_at=now, allocated_cpus=min(64, int(offer["cpu_cores"])))
            try:
                self.launch_service(identifier, "run")
                self.launch_service(identifier, "guard")
            except VastError:
                self.control(identifier, "cleanup")
                self.update(identifier, error="Service startup failed; use Retry cleanup to reconcile")
                raise
        return self.read(identifier)

    def launch_service(self, identifier: str, mode: str) -> None:
        """Create one restartable local supervisor; no secrets in argv or output."""
        job_id(identifier)
        if mode not in ("run", "guard"):
            raise VastError("Invalid supervisor mode", 422)
        log_root = ensure_private_directory(PROJECT / "data/logs/vast")
        unit = f"pbgui-vast-{identifier}-{mode}"
        command = ["systemd-run", "--user", "--collect", "--unit", unit,
                   "--property=Restart=on-failure", "--property=RestartSec=10", "--property=UMask=0077",
                   "--property=StandardOutput=append:" + str(log_root / (identifier + "-" + mode + ".log")),
                   "--property=StandardError=inherit", "--working-directory=" + str(PROJECT),
                   sys.executable, str(PROJECT / "vast_job_runner.py"), mode, identifier]
        try:
            active = subprocess.run(["systemctl", "--user", "is-active", "--quiet", unit],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
            if active.returncode == 0:
                return
            result = subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)
        except (OSError, subprocess.TimeoutExpired):
            raise VastError("Local cloud supervisor could not be started", 503) from None
        if result.returncode:
            raise VastError("Local cloud supervisor could not be started", 503)
