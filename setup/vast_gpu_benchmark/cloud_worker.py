"""Self-contained PBGui worker protocol for the pinned PB8 CUDA image.

Only the guard consumes Vast's per-container credential. No PBGui, registry
write, exchange or Vast account credentials belong in this worker.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
from pathlib import Path, PurePosixPath
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.request

SERVICE = "VastWorker"
ROOT = Path(os.environ.get("PBGUI_WORKDIR", "/work/pbgui"))
GUARD_ROOT = Path("/work/pbgui")


def write_record(path: Path, value: dict) -> None:
    """Publish a private JSON record atomically."""
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=4, allow_nan=False) + "\n")
    os.replace(temporary, path)


def file_hash(path: Path) -> str:
    """Hash immutable transfer content in bounded chunks."""
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def safe_path(root: Path, value: str) -> Path:
    """Reject absolute, traversal, symlink and noncanonical archive paths."""
    if not isinstance(value, str) or not value or value.startswith("/") or "\\" in value or any(ord(c) < 32 or ord(c) == 127 for c in value):
        raise ValueError("Invalid transfer path")
    parts = value.split("/")
    if any(part in ("", ".", "..") for part in parts):
        raise ValueError("Invalid transfer path")
    path = root.joinpath(*parts)
    for ancestor in [path, *path.parents]:
        if ancestor == root.parent:
            break
        if ancestor.is_symlink():
            raise ValueError("Symlink in transfer path")
    path.resolve().relative_to(root.resolve())
    return path


def guard() -> None:
    """Independently destroy this container at the immutable authorized deadline."""
    deadline = float(os.environ["PBGUI_DEADLINE"])
    instance = int(os.environ["CONTAINER_ID"])
    key = os.environ["CONTAINER_API_KEY"]
    if not 0 < deadline - time.time() <= 86400 or instance <= 0:
        raise ValueError("Invalid guard authorization")
    write_record(ROOT / "guard.json", {"instance_id": instance, "deadline": deadline, "job_id": os.environ["PBGUI_JOB_ID"]})
    monotonic_deadline = time.monotonic() + max(0, deadline - time.time())
    while time.time() < deadline and time.monotonic() < monotonic_deadline:
        time.sleep(2)
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        """Never forward a container credential to another endpoint."""
        def redirect_request(self, *args):
            """Refuse redirects."""
            return None
    while True:
        try:
            req = urllib.request.Request("https://console.vast.ai/api/v0/instances/" + str(instance),
                method="DELETE", headers={"Authorization": "Bearer " + key})
            with urllib.request.build_opener(NoRedirect()).open(req, timeout=15) as response:
                response.read(1024)
        except Exception:
            write_record(ROOT / "guard-error.json", {"error": "Container deletion failed; retrying", "at": time.time()})
        time.sleep(10)


def cache_missing() -> dict:
    """Identify immutable data blobs not already verified in this worker cache."""
    manifest = json.loads((ROOT / 'cache-request.json').read_text())
    missing = []
    for item in manifest['files']:
        key = item['sha256']
        if not re.fullmatch(r'[0-9a-f]{64}', key):
            raise ValueError('Invalid data cache key')
        target = safe_path(GUARD_ROOT / 'cache', key)
        if not target.is_file() or target.stat().st_size != item['bytes'] or file_hash(target) != key:
            missing.append(key)
    return {'missing': sorted(set(missing))}


def install() -> dict:
    """Unpack a bounded input archive into a fresh staging directory."""
    marker = ROOT / "input-ready.json"
    if marker.exists():
        return json.loads(marker.read_text())
    total = 0
    with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
        stage = Path(temporary)
        with tarfile.open(ROOT / "input.tar.gz", "r:gz") as archive:
            for entry in archive:
                if not (entry.isdir() or entry.isfile()) or not (entry.name == "input" or entry.name.startswith("input/")):
                    raise ValueError("Unexpected input archive entry")
                path = safe_path(stage, entry.name)
                total += entry.size
                if total > 12 * 1024**3:
                    raise ValueError("Input archive is too large")
                if entry.isdir():
                    path.mkdir(parents=True, exist_ok=True)
                else:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    with archive.extractfile(entry) as source, path.open("xb") as target:
                        shutil.copyfileobj(source, target, 1024 * 1024)
        manifest = json.loads((stage / "input/manifest.json").read_text())
        if manifest["pb8_revision"] != Path("/opt/pb8-revision").read_text().strip():
            raise ValueError("PB8 image revision mismatch")
        if file_hash(stage / "input/optimize.json") != manifest["config_sha256"]:
            raise ValueError("Config checksum mismatch")
        for item in manifest["files"]:
            path = safe_path(stage / "input", item["path"])
            key = item['sha256']
            if not re.fullmatch(r'[0-9a-f]{64}', key):
                raise ValueError('Invalid cache hash')
            cache_root = GUARD_ROOT / 'cache'
            cache_root.mkdir(exist_ok=True)
            cached = safe_path(cache_root, key)
            if not path.exists():
                if not cached.is_file():
                    raise ValueError('Required data cache entry is missing')
                path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(cached, path)
            if path.stat().st_size != item["bytes"] or file_hash(path) != key:
                raise ValueError("Data checksum mismatch")
            if not cached.exists():
                temporary_blob = cache_root / (key + '.tmp')
                shutil.copyfile(path, temporary_blob)
                os.replace(temporary_blob, cached)
        if (ROOT / "input").exists():
            raise ValueError("Unverified input directory already exists")
        os.replace(stage / "input", ROOT / "input")
    result = {"ready": True, "config_sha256": manifest["config_sha256"]}
    write_record(marker, result)
    return result


def health() -> dict:
    """Check the actual GPU, image identity and effective container allocation."""
    import torch
    if not torch.cuda.is_available():
        raise ValueError("CUDA is unavailable")
    sys.path.insert(0, "/opt/passivbot/src")
    from rust_utils import verify_loaded_runtime_extension
    import passivbot_rust
    identity = verify_loaded_runtime_extension()
    if not identity.get("runtime_compiled_source_stamp") or identity["runtime_compiled_source_stamp"] != identity["expected_source_fingerprint"]:
        raise ValueError("Rust image stamp mismatch")
    cpus = float(len(os.sched_getaffinity(0)))
    quota_path = Path("/sys/fs/cgroup/cpu.max")
    if quota_path.exists():
        quota, period = quota_path.read_text().split()
        if quota != "max":
            cpus = min(cpus, int(quota) / int(period))
    return {"protocol": 1, "revision": Path("/opt/pb8-revision").read_text().strip(),
            "gpu": torch.cuda.get_device_name(0), "cpu_cores": cpus,
            "vram_bytes": torch.cuda.get_device_properties(0).total_memory,
            "guard": json.loads((GUARD_ROOT / "guard.json").read_text())}


def stop_process(process: subprocess.Popen) -> None:
    """Reap only the optimizer group owned by this worker."""
    if process.poll() is not None:
        process.wait()
        return
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            break
        try:
            process.wait(timeout=10)
            break
        except subprocess.TimeoutExpired:
            continue
    process.wait()


def run() -> None:
    """Run at most one optimizer; duplicate starts cannot reset its state."""
    with (ROOT / "run.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        if (ROOT / "finished.json").exists():
            return
        if (ROOT / "started.json").exists():
            # An earlier wrapper disappeared: do not launch a second optimizer.
            return
        install()
        hardware = health()
        config = json.loads((ROOT / "input/optimize.json").read_text())
        workers = config["optimize"]["n_cpus"]
        if workers > max(1, int(hardware["cpu_cores"])):
            raise ValueError("Requested exact workers exceed the container CPU quota")
        output = ROOT / "output"
        output.mkdir(exist_ok=True)
        write_record(ROOT / "started.json", {"started_at": time.time()})
        environment = {k: v for k, v in os.environ.items() if k not in {"CONTAINER_API_KEY"}}
        environment.update(PASSIVBOT_GPU_PROFILE="1", PYTHONUNBUFFERED="1", OMP_NUM_THREADS="1", OPENBLAS_NUM_THREADS="1", MKL_NUM_THREADS="1")
        started = time.monotonic()
        cancelled = False
        with (output / "optimizer.log").open("ab") as log:
            process = subprocess.Popen([sys.executable, "/opt/passivbot/src/optimize.py", str(ROOT / "input/optimize.json")],
                cwd=output, env=environment, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                while process.poll() is None:
                    cancelled = (ROOT / "stop").exists()
                    if cancelled or time.time() >= hardware["guard"]["deadline"] - 180:
                        cancelled = True
                        break
                    time.sleep(2)
            finally:
                stop_process(process)
        write_record(ROOT / "finished.json", {"exit_code": process.returncode, "cancelled": cancelled,
                     "wall_seconds": time.monotonic() - started, "finished_at": time.time()})


def status() -> dict:
    """Return bounded progress without credential-bearing environment or files."""
    result = {"started": (ROOT / "started.json").exists(), "finished": None, "exact_completed": 0, "gpu_candidates": 0}
    if (ROOT / "finished.json").exists():
        result["finished"] = json.loads((ROOT / "finished.json").read_text())
    log = ROOT / "output/optimizer.log"
    if log.exists():
        with log.open("rb") as stream:
            stream.seek(max(0, log.stat().st_size - 512 * 1024))
            lines = stream.read().decode(errors="replace").splitlines()
        import re
        for line in lines:
            match = re.search(r"Iter: (\d+)", line)
            if match:
                result["exact_completed"] = max(result["exact_completed"], int(match[1]))
            if "[gpu-profile] " in line:
                try:
                    event = json.loads(line.split("[gpu-profile] ", 1)[1])
                except ValueError:
                    continue
                result["exact_completed"] = max(result["exact_completed"], int(event.get("exact_completed") or 0))
                if event.get("generation"):
                    result["gpu_candidates"] = int(event["generation"]) * int(event.get("population_size") or 0)
    return result


def snapshot(final: bool) -> dict:
    """Archive immutable Pareto copies; include complete native files only at finish."""
    if final and not (ROOT / "finished.json").exists():
        raise ValueError("Optimizer must finish before final collection")
    with tempfile.TemporaryDirectory(dir=ROOT) as temporary:
        stage = Path(temporary)
        files = []
        source_root = ROOT / "output"
        candidates = list(source_root.glob("optimize_results/*/pareto/*.json"))
        if final:
            candidates += list(source_root.glob("optimize_results/*/all_results.bin"))
            candidates += list(source_root.glob("optimize_results/*/checkpoint.pkl"))
        log = source_root / "optimizer.log"
        if log.exists():
            candidates.append(log)
        total = 0
        for source in candidates:
            relative = source.relative_to(source_root)
            if source.is_symlink():
                raise ValueError("Symlink in results")
            target = stage / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            try:
                before = source.stat()
                total += before.st_size
                if total > 2 * 1024**3:
                    raise ValueError("Result collection exceeds 2 GB limit")
                shutil.copyfile(source, target)
            except FileNotFoundError:
                if final:
                    raise
                continue
            files.append({"path": str(relative), "bytes": target.stat().st_size, "sha256": file_hash(target)})
        if final:
            shutil.copyfile(ROOT / "finished.json", stage / "finished.json")
            files.append({"path": "finished.json", "bytes": (stage / "finished.json").stat().st_size,
                          "sha256": file_hash(stage / "finished.json")})
        write_record(stage / "manifest.json", {"files": files, "final": final})
        destination = ROOT / ("final.tar.gz" if final else "partial.tar.gz")
        temporary_archive = destination.with_suffix(".tmp")
        with tarfile.open(temporary_archive, "w:gz") as archive:
            for path in stage.rglob("*"):
                if path.is_file():
                    archive.add(path, arcname=str(path.relative_to(stage)), recursive=False)
        os.replace(temporary_archive, destination)
        return {"bytes": destination.stat().st_size, "sha256": file_hash(destination), "final": final}


def protocol_call(operation) -> dict:
    """Return actionable preflight errors without environment or traceback dumps."""
    try:
        return operation()
    except (ValueError, RuntimeError, ImportError, OSError) as exc:
        return {"error": type(exc).__name__ + ": " + str(exc)[:400]}


def main() -> None:
    """Expose only fixed operations used by the local supervisor."""
    os.umask(0o077)
    ROOT.mkdir(parents=True, exist_ok=True)
    action = sys.argv[1]
    if action == "guard":
        guard()
    elif action == "run":
        try:
            run()
        except Exception as exc:
            write_record(ROOT / "finished.json", {"exit_code": 1, "cancelled": False, "error": type(exc).__name__, "finished_at": time.time()})
    elif action == "health":
        print(json.dumps(protocol_call(health)))
    elif action == "cache-missing":
        print(json.dumps(protocol_call(cache_missing)))
    elif action == "install":
        print(json.dumps(protocol_call(install)))
    elif action == "status":
        print(json.dumps(status()))
    elif action == "stop":
        (ROOT / "stop").touch()
        print('{"stop_requested":true}')
    elif action in ("partial", "final"):
        print(json.dumps(snapshot(action == "final")))
    else:
        raise ValueError("Unknown operation")


if __name__ == "__main__":
    main()
