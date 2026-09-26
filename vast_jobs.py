"""Durable single-rental jobs and immutable local PB8 input snapshots."""

from __future__ import annotations

import copy
import os
import psutil
import hashlib
import json
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import time
import uuid
from pathlib import Path

from file_lock import advisory_file_lock
from logging_helpers import human_log as _log
from secure_files import atomic_write_private_bytes, atomic_write_private_text, ensure_private_directory, read_regular_file_nofollow
from vast_credentials import VastCredentialStore
from vast_provider import VastClient, VastError, number, positive_id

from vast_exchanges import SUPPORTED_EXCHANGES

SERVICE = "Vast"
PROJECT = Path(__file__).resolve().parent
IMAGE = "ghcr.io/msei99/pbgui-pb8-worker@sha256:09cb0f9ba004db44f3ca02a7b3b03ea3211fd9e6c3c9ea33794cd4c6d24dee30"
REVISION = "903ed11153ce82d1b6760604eaa3a553309a752a"
PREVIOUS_REVISION = "69227b75e808f8ce1f4b8949350b3311916bd377"
ORIGINAL_REVISION = "ee2b7d49fd53ef790a66a28e2c85f2a6c8faebe8"
# Persisted rental intents keep their own immutable image/revision pair.
SUPPORTED_RENTAL_IMAGE_REVISIONS = {
    IMAGE: REVISION,
    "ghcr.io/msei99/pbgui-pb8-worker@sha256:8ad62f43decae47fe670f3ac15ba7e4d7c648f0bb030fa511cd4771321ff4327": PREVIOUS_REVISION,
    "ghcr.io/msei99/pbgui-pb8-worker@sha256:70366b9989a12528245d4c263e0e3dc4350271427afd76f9568dcf29cf33878f": PREVIOUS_REVISION,
    "ghcr.io/msei99/pbgui-pb8-worker@sha256:bee2e513d77e2c22f5b0671392063c51bb04bd547c7334e614fe918ada2d49e2": PREVIOUS_REVISION,
    "ghcr.io/msei99/pbgui-pb8-worker@sha256:f078b47466f53e3039b13e41905531d9504ca6caca7b57469499fae20b77ec0e": PREVIOUS_REVISION,
    "ghcr.io/msei99/pbgui-pb8-worker@sha256:8a85444f341a447564fc23e955b34f8a68b1970afc54026334aefa204c6cbc56": PREVIOUS_REVISION,
    "ghcr.io/msei99/pbgui-pb8-worker@sha256:bc330893bef1864065dc21835faac725e551383c3455ad4c5908a6713417268e": PREVIOUS_REVISION,
    "ghcr.io/msei99/pbgui-pb8-worker@sha256:b67111ebcc0d0c55c57c0b27ad8d2017c8577061a47a4b7909936bd4215c0fc4": PREVIOUS_REVISION,
    "ghcr.io/msei99/pbgui-pb8-worker@sha256:b6f61c54b546640f5f00e386c10a27380e0ed8715788bcc8c4b597eedff58dbc": ORIGINAL_REVISION,
    "ghcr.io/msei99/pbgui-pb8-worker@sha256:ea52a9ea51f1945b5c3c5133246fd125ee7793faa288754b21e095b95e138743": ORIGINAL_REVISION,
    "ghcr.io/msei99/pbgui-pb8-worker@sha256:0d827eb097a9d26c9088421097b4a9f0eacf660f08613871c48946ddf71a0a88": ORIGINAL_REVISION,
}
SUPPORTED_RENTAL_IMAGES = tuple(SUPPORTED_RENTAL_IMAGE_REVISIONS)
from vast_config_validation import METRICS, validate_cloud_config

TERMINAL = {"completed", "cancelled", "failed"}
MAX_JOB_RECORD_BYTES = 16 * 1024 * 1024
MAX_INPUT_MANIFEST_BYTES = 1024**3


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


class _ProgressReader:
    """Count source bytes consumed by tarfile without buffering whole inputs."""

    def __init__(self, stream, advance) -> None:
        self.stream = stream
        self.advance = advance

    def read(self, size: int = -1) -> bytes:
        """Forward one bounded read and publish its consumed byte count."""
        value = self.stream.read(size)
        if value:
            self.advance(len(value))
        return value


def _write_input_archive(folder: Path, relative_files: list[Path], target: Path, progress) -> None:
    """Write the existing gzip-tar format while reporting actual source consumption."""
    members = [Path('optimize.json'), Path('manifest.json'),
               *(Path('ohlcv') / relative for relative in relative_files)]
    sizes = [safe.stat().st_size for relative in members
             for safe in [folder / relative]]
    total = sum(sizes)
    completed = 0
    files_completed = 0
    started = time.monotonic()
    last_report = started

    def report(*, force: bool = False) -> None:
        """Rate-limit persistent progress while retaining a final exact sample."""
        nonlocal last_report
        now = time.monotonic()
        if not force and now - last_report < 0.5:
            return
        elapsed = max(0, now - started)
        rate = completed / elapsed if completed > 0 and elapsed > 0 else 0
        progress({'stage': 'compressing', 'files_completed': files_completed,
                  'files_total': len(members), 'bytes_completed': completed,
                  'bytes_total': total, 'bytes_per_second': rate,
                  'elapsed_seconds': elapsed,
                  'eta_seconds': (total - completed) / rate if rate > 0 else None})
        last_report = now

    def advance(count: int) -> None:
        """Account for bytes only after tarfile has consumed them."""
        nonlocal completed
        completed += count
        report()

    report(force=True)
    with tarfile.open(target, 'w:gz') as archive:
        for relative, size in zip(members, sizes):
            source = folder / relative
            info = archive.gettarinfo(str(source), arcname=str(Path('input') / relative))
            if not info.isfile() or info.size != size:
                raise VastError('Prepared input changed before compression', 409)
            with source.open('rb') as stream:
                archive.addfile(info, _ProgressReader(stream, advance))
            files_completed += 1
            report()
    report(force=True)


def native_job_config(source: dict, iterations: int, workers: int, use_adg: bool) -> dict:
    """Prepare a supported fresh GPU job without local CUDA or source edits."""
    if type(iterations) is not int or not 256 <= iterations <= 10_000_000 or type(workers) is not int or not 1 <= workers <= 64:
        raise VastError("Choose 256–10,000,000 iterations and 1–64 CPU workers", 422)
    errors = validate_cloud_config(source, iterations, workers, use_adg=use_adg, image=IMAGE, revision=REVISION)
    if errors:
        raise VastError("Cloud configuration is invalid: " + "; ".join(item['path'] + ": " + item['message'] for item in errors), 422)
    config = copy.deepcopy(source)
    live, bt, opt = config["live"], config["backtest"], config["optimize"]
    if bt.get("suite_enabled"):
        from vast_scenarios import flatten_overrides
        for scenario in bt.get("scenarios", []):
            if scenario.get("overrides"):
                # PB8's GPU preflight expects dotted paths before suite materialization.
                scenario["overrides"] = flatten_overrides(scenario["overrides"])

    if live.get("strategy_kind") not in ("trailing_martingale", "ema_anchor"):
        raise VastError("This image supports trailing_martingale and ema_anchor", 422)
    if config.get("coin_overrides") or opt.get("enable_overrides") or bt.get("market_settings_sources"):
        raise VastError("Cloud export does not yet support coin or optimizer overrides", 422)
    if bt.get("btc_collateral_cap") or (opt.get("gpu", {}).get("successive_halving") or {}).get("enabled"):
        raise VastError("Cloud jobs currently require no BTC collateral and no successive halving", 422)
    coins = live.get("approved_coins")
    if not isinstance(coins, dict) or any(not isinstance(coins.get(side), list) for side in ("long", "short")):
        raise VastError("Explicit approved coin lists are required", 422)
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
    opt.setdefault("gpu", {}).update(exact_workers=workers)
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
            maximum = MAX_INPUT_MANIFEST_BYTES if filename == "input/manifest.json" else MAX_JOB_RECORD_BYTES
            if path.stat().st_size > maximum:
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
        rows = []
        for path in folder.iterdir():
            if not path.is_dir() or not re.fullmatch(r"[0-9a-f]{32}", path.name):
                continue
            try:
                rows.append(self.read(path.name))
            except VastError:
                if not path.exists():
                    continue
                raise
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
                if exchange in SUPPORTED_EXCHANGES and exchange not in result:
                    result.append(exchange)
        return result

    def prepared_input_directory(self, identifier: str) -> Path:
        """Resolve an immutable snapshot reference without duplicating its file tree."""
        current = job_id(identifier)
        seen = set()
        for _ in range(8):
            if current in seen:
                raise VastError("Prepared input snapshot reference is cyclic", 422)
            seen.add(current)
            state = self.read(current)
            source = state.get("snapshot_source_id")
            if source is None:
                path = self.directory(current) / "input"
                if not path.is_dir() or path.is_symlink():
                    raise VastError("Prepared input snapshot is unavailable", 409)
                return path
            current = job_id(source)
        raise VastError("Prepared input snapshot reference is too deep", 422)

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

    def reset_unstarted_for_replacement(self, identifier: str, lease_id: str) -> dict:
        """Release an unstarted immutable snapshot after its old lease is gone."""
        identifier = job_id(identifier)
        lease_id = job_id(lease_id)
        directory = self.directory(identifier)
        with advisory_file_lock(directory / ".state-lock"):
            state = self.read(identifier)
            if state.get('lease_id') != lease_id:
                raise VastError('Replacement lease no longer owns this job', 409)
            if (state.get('final_collected') or state.get('result_path') or
                    int(state.get('exact_completed') or 0) > 0 or int(state.get('downloaded_bytes') or 0) > 0):
                raise VastError('Optimizer results already exist; requeue this job explicitly', 409)
            for key in (
                'lease_id', 'dispatch_at', 'setup_started_at', 'worker_ready', 'hardware',
                'uploaded', 'upload_progress', 'upload_attempts', 'upload_recovery_since',
                'upload_retry_at', 'upload_retry_failures', 'upload_retry_bytes', 'started_at',
                'stop_reason', 'completion_reason', 'throughput', 'runtime_metrics',
                'exact_queue', 'convergence', 'gpu_tuning', 'finished_at', 'elapsed_seconds', 'exit_code',
            ):
                state.pop(key, None)
            state.update(status='ready', rental_state='none', exact_completed=0,
                         gpu_candidates=0, error=None, generation=int(state.get('generation', 0)) + 1,
                         updated_at=time.time())
            write_json(directory / 'state.json', state)
        with advisory_file_lock(directory / '.control-lock'):
            write_json(directory / 'control.json', {'stop': False, 'cleanup': False})
        _log(SERVICE, f"{identifier}: released for replacement GPU after verified rental cleanup", level='INFO')
        return state

    def recover_interrupted_preparation(self, identifier: str) -> dict:
        """Expose retry when the process owning an unfinished snapshot has exited."""
        with advisory_file_lock(self.directory(identifier) / ".state-lock"):
            state = self.read(identifier)
            owner = state.get("preparation_owner")
            if state.get("status") != "preparing" or not isinstance(owner, dict):
                return state
            try:
                process = psutil.Process(owner["pid"])
                if process.create_time() == owner["created_at"] and process.is_running():
                    return state
            except psutil.NoSuchProcess:
                pass
            except (psutil.AccessDenied, KeyError, TypeError, ValueError):
                return state
            _log(SERVICE, "Input preparation interrupted; job can be requeued", level="WARNING")
            return self.update(identifier, status="failed",
                               error="Input preparation interrupted by process shutdown. Requeue to prepare again.")

    def create_preparation(self, name: str, iterations: int, workers: int, use_adg: bool,
                           *, requeue_from: str | None = None) -> dict:
        """Persist a pending input snapshot before its slower file work starts."""
        ensure_private_directory(self.root)
        ensure_private_directory(self.root / "jobs")
        identifier = uuid.uuid4().hex
        directory = ensure_private_directory(self.root / "jobs" / identifier)
        state = {"id": identifier, "config_name": config_name(name), "status": "preparing", "rental_state": "none",
                 "iterations": iterations, "workers": workers, "auto_cpu_workers": True, "created_at": time.time(), "generation": 0,
                 "preparation_owner": {"pid": os.getpid(), "created_at": psutil.Process().create_time()},
                 "exact_completed": 0, "gpu_candidates": 0, "error": None,
                 "input_progress": {"stage": "selecting", "files_completed": 0, "files_total": 0,
                                    "bytes_completed": 0, "bytes_total": 0}}
        if requeue_from is not None:
            state['requeue_from'] = job_id(requeue_from)
        write_json(directory / "state.json", state)
        write_json(directory / "control.json", {"stop": False, "cleanup": False})
        return state

    def prepare(self, name: str, source: dict, source_sha256: str, market_root: Path,
                mapping_root: Path, results_root: Path, iterations: int, workers: int, use_adg: bool,
                *, requeue_from: str | None = None, identifier: str | None = None) -> dict:
        """Freeze a config/data bundle locally before requesting any paid resource."""
        from pb8_config import save_prepared_pb8_config
        from setup.vast_gpu_benchmark.prepare import select_shards
        from sweep_cycles import build_sweep_plan

        config = native_job_config(source, iterations, workers, use_adg)
        try:
            shards = select_shards(config, market_root, mapping_root)
        except (ValueError, OSError) as exc:
            raise VastError("Market data export failed: " + str(exc), 422) from None
        total_bytes = sum(p.stat().st_size for p, _ in shards)
        if total_bytes > 10 * 1024**3:
            raise VastError("This cloud profile supports input bundles up to 10 GB", 422)
        if identifier is None:
            identifier = self.create_preparation(name, iterations, workers, use_adg, requeue_from=requeue_from)['id']
        else:
            identifier = job_id(identifier)
            if self.read(identifier).get('status') != 'preparing':
                raise VastError('Cloud job is no longer awaiting input preparation', 409)
        config["backtest"]["ohlcv_source_dir"] = "/work/pbgui/jobs/" + identifier + "/input/ohlcv"
        directory = self.directory(identifier)
        self.update(identifier, exchanges=sorted({relative.parts[0] for _, relative in shards}), input_progress={
            "stage": "copying", "files_completed": 0, "files_total": len(shards),
            "bytes_completed": 0, "bytes_total": total_bytes,
        })
        try:
            folder = ensure_private_directory(directory / "input")
            save_prepared_pb8_config(config, folder / "optimize.json")
            from scenario_windows import build_validation_plan
            sweep = build_sweep_plan(source)
            validation_plan = build_validation_plan(source)
            manifest = {"schema_version": 1, "pb8_revision": REVISION, "config_sha256": digest(folder / "optimize.json"),
                        "source_config_sha256": source_sha256, "sweep_plan": sweep, "validation_plan": validation_plan, "files": []}
            copied_bytes = 0
            last_progress_update = time.monotonic()
            for index, (original, relative) in enumerate(shards, start=1):
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
                copied_bytes += after.st_size
                if index == len(shards) or time.monotonic() - last_progress_update >= 0.25:
                    self.update(identifier, input_progress={
                        "stage": "copying", "files_completed": index, "files_total": len(shards),
                        "bytes_completed": copied_bytes, "bytes_total": total_bytes,
                    })
                    last_progress_update = time.monotonic()
            write_json(folder / "manifest.json", manifest)
            def compression_progress(value: dict) -> None:
                """Persist bounded archive progress for automatic Queue polling."""
                self.update(identifier, input_progress=value)

            _write_input_archive(folder, [relative for _, relative in shards],
                                 directory / 'input.tar.gz', compression_progress)
            (directory / "input.tar.gz").chmod(0o600)
            write_json(directory / "intent.json", {"id": identifier, "image": IMAGE, "pb8_revision": REVISION,
                       "results_root": str(results_root.resolve()), "bundle_sha256": digest(directory / "input.tar.gz"),
                       "bundle_bytes": (directory / "input.tar.gz").stat().st_size, "source_config_sha256": source_sha256})
            return self.update(identifier, status="ready", input_bytes=(directory / "input.tar.gz").stat().st_size,
                               use_adg=use_adg, sweep_enabled=sweep is not None, input_progress={
                                   "stage": "complete", "files_completed": len(shards), "files_total": len(shards),
                                   "bytes_completed": total_bytes, "bytes_total": total_bytes,
                               })
        except Exception:
            self.update(identifier, status="failed", error="Input preparation failed; no instance rented")
            raise

    def clone_prepared(self, source_id: str, name: str, source_sha256: str, results_root: Path,
                       iterations: int, workers: int, use_adg: bool) -> dict | None:
        """Reference an unchanged failed snapshot without walking its market-data files."""
        from pb8_config import load_pb8_config

        source_id = job_id(source_id)
        source = self.read(source_id)
        try:
            intent = self.read(source_id, "intent.json")
        except VastError:
            return None
        source_directory = self.directory(source_id)
        archive = source_directory / "input.tar.gz"
        optimize_path = source_directory / "input/optimize.json"
        manifest_path = source_directory / "input/manifest.json"
        if (source.get("status") not in {"failed", "cancelled"}
                or (source.get("input_progress") or {}).get("stage") != "complete"
                or intent.get("image") != IMAGE or intent.get("pb8_revision") != REVISION
                or intent.get("source_config_sha256") != source_sha256
                or source.get("iterations") != iterations or bool(source.get("use_adg")) != bool(use_adg)
                or any(not path.is_file() or path.is_symlink() for path in (archive, optimize_path, manifest_path))):
            return None
        try:
            prepared = load_pb8_config(optimize_path)
            optimize = prepared.get("optimize") if isinstance(prepared, dict) else None
            if not isinstance(optimize, dict) or optimize.get("iters") != iterations or optimize.get("n_cpus") != workers:
                return None
            expected_bytes = int(intent["bundle_bytes"])
            expected_digest = str(intent["bundle_sha256"])
            if expected_bytes <= 0 or not re.fullmatch(r"[0-9a-f]{64}", expected_digest):
                return None
        except (OSError, ValueError, TypeError, KeyError):
            return None

        identifier = self.create_preparation(name, iterations, workers, use_adg, requeue_from=source_id)["id"]
        target_directory = self.directory(identifier)
        try:
            input_directory = ensure_private_directory(target_directory / "input")
            manifest = self.read(source_id, "input/manifest.json")
            atomic_write_private_bytes(input_directory / "optimize.json", read_regular_file_nofollow(optimize_path, self.root))
            write_json(input_directory / "manifest.json", manifest)

            files = manifest.get("files")
            if not isinstance(files, list):
                raise VastError("Prepared cloud input manifest is invalid", 422)
            source_stat = archive.stat(follow_symlinks=False)
            if not stat.S_ISREG(source_stat.st_mode) or source_stat.st_size != expected_bytes:
                raise VastError("Prepared cloud input snapshot changed; rebuilding is required", 409)
            os.link(archive, target_directory / "input.tar.gz")
            (target_directory / "input.tar.gz").chmod(0o600)
            write_json(target_directory / "intent.json", {
                "id": identifier, "image": IMAGE, "pb8_revision": REVISION,
                "results_root": str(results_root.resolve()), "bundle_sha256": expected_digest,
                "bundle_bytes": expected_bytes, "source_config_sha256": source_sha256,
            })
            progress = source.get("input_progress") or {}
            return self.update(
                identifier, status="ready", input_bytes=expected_bytes, use_adg=use_adg,
                snapshot_source_id=source.get("snapshot_source_id") or source_id,
                exchanges=list(source.get("exchanges") or []), sweep_enabled=bool(source.get("sweep_enabled")),
                input_progress={
                    "stage": "complete", "files_completed": int(progress.get("files_total", 0)),
                    "files_total": int(progress.get("files_total", 0)),
                    "bytes_completed": int(progress.get("bytes_total", 0)),
                    "bytes_total": int(progress.get("bytes_total", 0)),
                },
            )
        except Exception:
            self.update(identifier, status="failed", error="Prepared input snapshot reuse failed; requeue to rebuild")
            raise

    def create_calibration(self, source_id: str, plan: dict) -> dict:
        """Create a disk-efficient immutable calibration copy of one prepared input."""
        source_id = job_id(source_id)
        source = self.read(source_id)
        if (source.get('kind') in {'worker', 'calibration'} or source.get('status') != 'ready'
                or source.get('lease_id') or source.get('deleted_at')):
            raise VastError('Choose an inactive prepared PB8 queue item for calibration', 409)
        if not isinstance(plan, dict):
            raise VastError('Invalid GPU calibration plan', 422)
        from vast_calibration import configurable_calibration_plan
        try:
            expected = configurable_calibration_plan(
                plan.get('populations', [None])[0], plan.get('population_step'),
                plan.get('max_population'), plan.get('min_scale_gain'),
                plan.get('sample_seconds'), plan.get('case_timeout_seconds'))
        except (AttributeError, IndexError, TypeError, ValueError) as exc:
            raise VastError('Invalid GPU calibration plan', 422) from exc
        if plan != expected:
            raise VastError('Invalid GPU calibration plan', 422)
        source_directory = self.directory(source_id)
        source_input = source_directory / 'input'
        source_archive = source_directory / 'input.tar.gz'
        source_intent = self.read(source_id, 'intent.json')
        manifest = self.read(source_id, 'input/manifest.json')
        if (source_intent.get('image') != IMAGE
                or source_intent.get('pb8_revision') != REVISION
                or any(not path.is_file() or path.is_symlink()
                       for path in (source_archive, source_input / 'optimize.json', source_input / 'manifest.json'))
                or digest(source_archive) != source_intent.get('bundle_sha256')):
            raise VastError('Prepared PB8 input is no longer reusable; prepare it again', 409)
        calibration_name = ('GPU calibration · ' + config_name(source.get('config_name', 'PB8')))[:160]
        identifier = self.create_preparation(
            calibration_name,
            source['iterations'], source['workers'], bool(source.get('use_adg')),
        )['id']
        target_directory = self.directory(identifier)
        try:
            target_input = ensure_private_directory(target_directory / 'input')
            files = manifest.get('files')
            if not isinstance(files, list):
                raise VastError('Prepared cloud input manifest is invalid', 422)
            relative_paths = [Path('optimize.json'), Path('manifest.json')]
            for entry in files:
                relative = Path(str(entry.get('path') or '')) if isinstance(entry, dict) else Path()
                if (relative.is_absolute() or not relative.parts or '..' in relative.parts
                        or '\\' in str(relative) or any(ord(char) < 32 for char in str(relative))):
                    raise VastError('Prepared cloud input manifest is invalid', 422)
                relative_paths.append(relative)
            for relative in relative_paths:
                source_path = source_input / relative
                target_path = target_input / relative
                source_path.resolve(strict=True).relative_to(source_input.resolve(strict=True))
                if source_path.is_symlink() or not source_path.is_file():
                    raise VastError('Prepared cloud input changed; prepare it again', 409)
                ensure_private_directory(target_path.parent)
                os.link(source_path, target_path)
                target_path.chmod(0o600)
            os.link(source_archive, target_directory / 'input.tar.gz')
            (target_directory / 'input.tar.gz').chmod(0o600)
            write_json(target_directory / 'intent.json', {
                'id': identifier, 'image': IMAGE, 'pb8_revision': REVISION,
                'results_root': source_intent.get('results_root'),
                'bundle_sha256': source_intent['bundle_sha256'],
                'bundle_bytes': source_intent['bundle_bytes'],
                'source_config_sha256': source_intent.get('source_config_sha256'),
            })
            progress = source.get('input_progress') or {}
            return self.update(
                identifier, kind='calibration', status='ready', calibration_plan=copy.deepcopy(plan),
                calibration_source_id=source_id, snapshot_source_id=source.get('snapshot_source_id') or source_id,
                input_bytes=source_intent['bundle_bytes'], use_adg=bool(source.get('use_adg')),
                exchanges=list(source.get('exchanges') or []), sweep_enabled=bool(source.get('sweep_enabled')),
                input_progress={
                    'stage': 'complete', 'files_completed': int(progress.get('files_total', 0)),
                    'files_total': int(progress.get('files_total', 0)),
                    'bytes_completed': int(progress.get('bytes_total', 0)),
                    'bytes_total': int(progress.get('bytes_total', 0)),
                },
            )
        except Exception:
            self.update(identifier, status='failed', error='Calibration snapshot creation failed; no instance rented')
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
            active = [row for row in self.list() if row['id'] != identifier
                      and row['rental_state'] not in ('none', 'deletion_verified')]
            from vast_queue import CloudQueue
            from vast_pool import pool_limit
            queue_state = CloudQueue(self).read()
            limit = pool_limit(queue_state) if queue_state.get('pool_enabled') and state.get('kind') == 'worker' else 1
            if len(active) >= limit:
                raise VastError("Finish the existing rental and cleanup first", 409)
            from vast_image import require_public_image
            intent = self.read(identifier, "intent.json")
            image = intent.get("image")
            authorized_revision = SUPPORTED_RENTAL_IMAGE_REVISIONS.get(image) if isinstance(image, str) else None
            if not authorized_revision or intent.get("pb8_revision") != authorized_revision:
                raise VastError("Invalid cloud rental image/revision", 422)
            require_public_image(intent["image"])
            secrets = VastCredentialStore(self.root).secrets()
            if (queue_state.get('pool_enabled') and state.get('kind') == 'worker'
                    and (queue_state.get('pool_authorization') or {}).get('credential_generation') != secrets['generation']):
                raise VastError('Vast credentials changed; start the GPU pool again to authorize this account', 422)
            account = VastClient(secrets["api_key"]).account()
            committed = sum(row.get('budget_usd', 0) for row in active)
            if account["balance_usd"] is None or account["balance_usd"] < budget + committed:
                raise VastError("The account needs at least the selected job budget in rental credit", 409)
            inbound = number(offer.get("download_gb_usd"))
            outbound = number(offer.get("upload_gb_usd"))
            if inbound is None or outbound is None:
                raise VastError("Choose an offer with known transfer prices", 422)
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
