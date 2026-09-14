"""Strict SSH transport and verified result import for owned Vast jobs."""

from __future__ import annotations

import gzip
import ipaddress
import json
import os
from pathlib import Path
import re
import selectors
import shlex
import shutil
import subprocess
import tarfile
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

from file_lock import advisory_file_lock
from secure_files import atomic_write_private_text, ensure_private_directory
from vast_jobs import PROJECT, JobStore, digest, write_json, job_id
from vast_provider import NoRedirect, VastClient, VastError, positive_id
from setup.vast_gpu_benchmark.cloud_worker import safe_path

SERVICE = "VastRunner"
WORKER = "/usr/local/bin/python /work/pbgui/worker.py "


def host_key_from_output(value: str) -> str:
    """Accept only an ED25519 public host key obtained via provider execution."""
    for line in value.splitlines():
        match = re.fullmatch(r"ssh-ed25519 ([A-Za-z0-9+/]{40,100}={0,2})(?: .*)?", line.strip())
        if match:
            import base64
            decoded = base64.b64decode(match[1], validate=True)
            if len(decoded) == 51 and decoded[:19] == b"\x00\x00\x00\x0bssh-ed25519\x00\x00\x00\x20":
                return "ssh-ed25519 " + match[1]
    raise VastError("The provider has not returned a valid SSH host identity")


def fetch_host_key_result(url: str, *, lease_id: str | None = None) -> str:
    """Read only the provider's bounded S3 result, without forwarding API auth."""
    parsed = urllib.parse.urlsplit(url)
    valid = (parsed.hostname == "s3.amazonaws.com" and parsed.path.startswith(("/vast.ai/instance_logs/", "/public.vast.ai/instance_logs/"))) or (
        parsed.hostname == "vast.ai.s3.amazonaws.com" and parsed.path.startswith("/instance_logs/"))
    if not valid or parsed.scheme != "https" or parsed.username or parsed.password or parsed.port not in (None, 443) or parsed.fragment or "/../" in parsed.path:
        raise VastError("Provider returned an unsupported host-identity URL")
    try:
        with urllib.request.build_opener(NoRedirect()).open(url, timeout=10) as response:
            raw = response.read(1024 * 1024 + 1)
        if len(raw) > 1024 * 1024:
            raise VastError("Host-identity response exceeds limit")
        output = raw.decode("utf-8")
        if lease_id is not None:
            prefix = "PBGUI_HOST_KEY " + job_id(lease_id) + " "
            keys = set()
            lines = output.splitlines()
            for index, line in enumerate(lines):
                if not line.startswith(prefix):
                    continue
                value = line[len(prefix):]
                if re.fullmatch(r'(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun) [A-Z][a-z]{2} +\d{1,2} \d\d:\d\d:\d\d UTC \d{4}', value):
                    value = lines[index + 1] if index + 1 < len(lines) else ''
                keys.add(host_key_from_output(value))
            if len(keys) != 1:
                raise VastError("Waiting for this rental's SSH identity in the container log")
            return keys.pop()
        return host_key_from_output(output)
    except (OSError, ValueError, urllib.error.URLError):
        raise VastError("SSH host identity is not available yet") from None


class WorkerConnection:
    """Own one job's SSH identity and short-lived bounded commands."""

    def __init__(self, store: JobStore, identifier: str, row: dict, *, lease_id: str | None = None) -> None:
        """Bind the direct endpoint to a previously matched provider instance."""
        self.store, self.identifier = store, identifier
        self.directory = store.directory(identifier)
        self.lease_id = lease_id or identifier
        self.identity_directory = store.directory(self.lease_id)
        self.remote_root = "/work/pbgui/jobs/" + identifier
        self.worker_command = "PBGUI_WORKDIR=" + self.remote_root + " " + WORKER
        self.row = row
        host = str(row.get("public_ipaddr") or "")
        try:
            if not ipaddress.ip_address(host).is_global:
                raise ValueError("not public")
            ports = row.get("ports") or {}
            port = int(ports["22/tcp"][0]["HostPort"])
            if not 1 <= port <= 65535:
                raise ValueError("port")
        except (ValueError, KeyError, IndexError, TypeError):
            raise VastError("Waiting for a direct public SSH endpoint") from None
        self.host, self.port = host, port

    def bootstrap(self, client: VastClient) -> None:
        """Attach a job-specific client key and authenticate the server via Vast."""
        key = self.identity_directory / "ssh-key"
        if not key.exists():
            result = subprocess.run(["ssh-keygen", "-t", "ed25519", "-N", "", "-C", "pbgui-vast", "-f", str(key)],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)
            if result.returncode:
                raise VastError("Could not generate the job SSH key")
        if key.is_symlink() or (self.identity_directory / "ssh-key.pub").is_symlink():
            raise VastError("Invalid SSH key files")
        key.chmod(0o600)
        identifier = positive_id(self.row["id"])
        marker = self.identity_directory / "ssh-attached.json"
        if not marker.exists():
            client.request("POST", f"/instances/{identifier}/ssh/", {"ssh_key": (self.identity_directory / "ssh-key.pub").read_text().strip()})
            write_json(marker, {"instance_id": identifier})
        known_hosts = self.identity_directory / "known_hosts"
        if not known_hosts.exists():
            # Execute supports only constrained filesystem commands, not cat.
            # The startup script publishes the public host key to container stdout;
            # retrieve it through the authenticated provider log API instead.
            result_file = self.identity_directory / "host-identity-log.json"
            saved = json.loads(result_file.read_text()) if result_file.exists() else {}
            if not saved or time.time() - saved.get("requested_at", 0) >= 60:
                result = client.request("PUT", f"/instances/request_logs/{identifier}/",
                                        {"tail": "1000"})
                url = result.get("result_url")
                if not isinstance(url, str):
                    raise VastError("Vast did not return an SSH host-identity result")
                write_json(result_file, {"url": url, "requested_at": time.time()})
            url = json.loads(result_file.read_text())["url"]
            public_key = fetch_host_key_result(url, lease_id=self.lease_id)
            atomic_write_private_text(known_hosts, f"pbgui-{self.lease_id} {public_key}\n")

    def command(self, command: str, *, stdin=None, stdout=None, timeout: int = 30, max_output: int = 1024 * 1024, progress=None, idle_timeout: int | None = None) -> bytes:
        """Execute only fixed internal operations with strict host-key verification."""
        args = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=10",
                "-o", "ServerAliveCountMax=2", "-o", "StrictHostKeyChecking=yes", "-o", "IdentitiesOnly=yes",
                "-o", "HostKeyAlias=pbgui-" + self.lease_id, "-o", "UserKnownHostsFile=" + str(self.identity_directory / "known_hosts"),
                "-i", str(self.identity_directory / "ssh-key"), "-p", str(self.port), "root@" + self.host,
                "cd /opt/passivbot && " + command]
        captured = bytearray()
        received = 0
        process = None
        try:
            process = subprocess.Popen(args, stdin=stdin, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            deadline = time.monotonic() + max(1, timeout)
            last_progress = 0.0
            last_received = time.monotonic()
            diagnostic = bytearray()
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                selector.register(process.stderr, selectors.EVENT_READ)
                while True:
                    if progress is not None and time.monotonic() - last_progress >= 2:
                        progress()
                        last_progress = time.monotonic()
                    if time.monotonic() >= deadline:
                        raise VastError("SSH operation timed out; reconnecting")
                    if idle_timeout and time.monotonic() - last_received >= idle_timeout:
                        raise VastError("SSH upload stalled; no receiver progress, reconnecting")
                    events = selector.select(min(.25, max(.01, deadline - time.monotonic())))
                    ready = False
                    for key, _ in events:
                        if key.fileobj is process.stderr:
                            error_bytes = os.read(process.stderr.fileno(), 8192)
                            diagnostic.extend(error_bytes[:max(0, 8192-len(diagnostic))])
                            if not error_bytes:
                                selector.unregister(process.stderr)
                        else:
                            ready = True
                    if not ready:
                        continue
                    chunk = os.read(process.stdout.fileno(), 65536)
                    if not chunk:
                        break
                    last_received = time.monotonic()
                    received += len(chunk)
                    if received > max_output:
                        raise VastError("SSH output exceeds the permitted transfer size")
                    if stdout is None:
                        captured.extend(chunk)
                    elif stdout != subprocess.DEVNULL:
                        stdout.write(chunk)
                if process.wait(timeout=max(.01, deadline - time.monotonic())):
                    # Report only known categories, never raw remote output or credentials.
                    detail = diagnostic.decode('utf-8', errors='replace').lower()
                    category = next((label for token, label in (
                        ('permission denied', 'authentication or permission denied'),
                        ('host key verification failed', 'host identity verification failed'),
                        ('no space left', 'remote disk full'),
                        ('connection timed out', 'connection timed out'),
                        ('connection reset', 'connection reset by peer'),
                        ('broken pipe', 'connection interrupted'),
                        ('connection refused', 'connection refused'),
                        ('could not resolve', 'hostname resolution failed'),
                    ) if token in detail), 'remote command or connection failed')
                    raise VastError('SSH worker operation failed: ' + category)
                if progress is not None:
                    progress()
        except (OSError, subprocess.TimeoutExpired):
            raise VastError("SSH operation interrupted; reconnecting") from None
        finally:
            if process is not None:
                if process.poll() is None:
                    process.kill()
                process.wait()
                process.stdout.close()
                process.stderr.close()
        return bytes(captured)

    def operation(self, action: str, timeout: int = 30) -> dict:
        """Decode a bounded response from an allowlisted worker command."""
        if action not in {"health", "install", "status", "stop", "partial", "final", "cache-missing"}:
            raise VastError("Invalid worker operation", 422)
        raw = self.command(self.worker_command + action, timeout=timeout)
        if len(raw) > 1024 * 1024:
            raise VastError("Worker response exceeds limit")
        try:
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise ValueError("invalid")
            if value.get("error"):
                raise VastError("Worker preflight failed: " + str(value["error"])[:500], 422)
            return value
        except ValueError:
            raise VastError("Invalid worker response") from None

    def upload(self, timeout: int) -> None:
        """Transfer only data missing from the shared content-addressed worker cache."""
        upload_deadline = time.monotonic() + timeout
        def remaining():
            """Keep packaging, transfer and install inside the caller's deadline."""
            seconds = int(upload_deadline-time.monotonic())
            if seconds <= 0:
                raise VastError('Upload deadline reached; verified chunks retained')
            return seconds
        config_root = execution_input(self.store, self.identifier)
        manifest = json.loads((config_root / 'manifest.json').read_text())
        with (config_root / 'manifest.json').open('rb') as source:
            self.command('mkdir -p ' + self.remote_root + ' && cat > ' + self.remote_root + '/cache-request.json',
                         stdin=source, stdout=subprocess.DEVNULL, timeout=remaining())
        response = self.operation('cache-missing', timeout=remaining())
        missing = response.get('missing')
        expected = {item['sha256'] for item in manifest['files']}
        if not isinstance(missing, list) or any(not isinstance(key, str) or key not in expected for key in missing):
            raise VastError('Invalid remote cache response', 422)
        missing = set(missing)
        delta = self.directory / 'upload.tar.gz'
        # A stable gzip header preserves chunk hashes across retries.
        fd, temporary_name = tempfile.mkstemp(prefix='upload-', suffix='.tmp', dir=self.directory)
        temporary = Path(temporary_name)
        try:
            with os.fdopen(fd, 'wb') as target:
                with gzip.GzipFile(filename='', fileobj=target, mode='wb', mtime=0) as compressed:
                    with tarfile.open(fileobj=compressed, mode='w') as archive:
                        for name in ('manifest.json', 'optimize.json'):
                            archive.add(config_root / name, arcname='input/' + name, recursive=False)
                        for item in manifest['files']:
                            if item['sha256'] in missing:
                                source = safe_path(self.directory / 'input', item['path'])
                                archive.add(source, arcname='input/' + item['path'], recursive=False)
            temporary.chmod(0o600)
            os.replace(temporary, delta)
        finally:
            temporary.unlink(missing_ok=True)
        delta.chmod(0o600)
        self.store.update(self.identifier, transfer_input_bytes=delta.stat().st_size,
                          cached_files=sum(item['sha256'] not in missing for item in manifest['files']))
        self.upload_archive(delta, timeout=remaining())
        self.operation('install', timeout=remaining())

    def upload_archive(self, archive: Path, *, timeout: int, chunk_bytes: int = 2 * 1024**2) -> None:
        """Resume verified content-addressed chunks after interrupted SSH transfers."""
        import hashlib
        started = time.monotonic()
        total = archive.stat().st_size
        chunks = []
        with archive.open('rb') as source:
            while data := source.read(chunk_bytes):
                chunks.append([hashlib.sha256(data).hexdigest(), len(data)])
        root = self.remote_root + '/upload-chunks'
        def remaining():
            """Bound the whole upload, including all short SSH connections."""
            seconds = int(timeout - (time.monotonic() - started))
            if seconds <= 0:
                raise VastError('SSH upload timed out; verified chunks will be reused')
            return seconds
        script = ('import pathlib,hashlib,json; root=pathlib.Path(' + repr(root) + '); '
                  'root.mkdir(parents=True,exist_ok=True); chunks=' + repr(chunks) + '; '
                  'print(json.dumps([h for h,n in chunks if (root/h).is_file() and not (root/h).is_symlink() '
                  'and (root/h).stat().st_size==n and hashlib.sha256((root/h).read_bytes()).hexdigest()==h]))')
        present = json.loads(self.command('python3 -c ' + shlex.quote(script), timeout=remaining()))
        expected = {h for h, _ in chunks}
        if not isinstance(present, list) or any(h not in expected for h in present):
            raise VastError('Invalid upload chunk inventory', 422)
        present = set(present)
        confirmed = sum(size for checksum, size in chunks if checksum in present)
        initial_confirmed = confirmed
        received_current = 0
        retry_bytes = 0
        def report(sent, stage='sending'):
            """Report verified progress, including chunks retained from earlier attempts."""
            self.store.update(self.identifier, upload_progress={'bytes': sent, 'total': total,
                'bytes_per_second': max(0, confirmed-initial_confirmed) / max(.001, time.monotonic()-started),
                'in_flight_bytes': received_current, 'stage': stage})
        report(confirmed)
        with archive.open('rb') as source:
            for checksum, size in chunks:
                data = source.read(size)
                if hashlib.sha256(data).hexdigest() != checksum:
                    raise VastError('Local upload archive changed', 422)
                if checksum not in present:
                    destination = root + '/' + checksum
                    verify = ('import pathlib,hashlib,os; p=pathlib.Path(' + repr(destination + '.tmp') + '); '
                              'assert p.stat().st_size==' + str(size) + '; '
                              'assert hashlib.sha256(p.read_bytes()).hexdigest()==' + repr(checksum) + '; '
                              'os.replace(p,' + repr(destination) + ')')
                    for attempt in range(3):
                        try:
                            received_current = 0
                            class ReceiverProgress:
                                """Parse bounded byte acknowledgements emitted by our receiver."""
                                pending = b''
                                last_report = 0.0
                                def write(self, value):
                                    nonlocal received_current
                                    self.pending += value
                                    lines = self.pending.split(b'\n')
                                    self.pending = lines.pop()
                                    for line in lines:
                                        if not line.isdigit() or not received_current <= int(line) <= size:
                                            raise VastError('Invalid upload progress acknowledgement', 422)
                                        received_current = int(line)
                                    if time.monotonic()-self.last_report >= 2:
                                        report(confirmed)
                                        self.last_report = time.monotonic()
                            receiver = ('import sys,os; p=' + repr(destination + '.tmp') + '; n=0\n'
                                        'with open(p,"wb") as out:\n'
                                        ' while True:\n'
                                        '  data=sys.stdin.buffer.read1(65536)\n'
                                        '  if not data: break\n'
                                        '  out.write(data); out.flush(); n+=len(data); print(n,flush=True)\n')
                            with tempfile.TemporaryFile() as chunk:
                                chunk.write(data); chunk.seek(0)
                                self.command('python3 -c ' + shlex.quote(receiver) + ' && python3 -c ' + shlex.quote(verify),
                                             stdin=chunk, stdout=ReceiverProgress(), timeout=remaining(),
                                             idle_timeout=120, max_output=max(1024*1024, size*8))
                            break
                        except VastError as exc:
                            retry_bytes += size
                            if exc.status != 502 or attempt == 2 or retry_bytes > total:
                                raise
                            received_current = 0
                            report(confirmed, 'reconnecting')
                            time.sleep(min(2 * (attempt + 1), remaining()))
                    confirmed += size
                received_current = 0
                report(confirmed)
        report(total, 'verifying')
        script = ('import pathlib,hashlib,os; root=pathlib.Path(' + repr(root) + '); '
                  'target=pathlib.Path(' + repr(self.remote_root + '/input.tar.gz') + '); '
                  'tmp=target.with_suffix(".tmp"); h=hashlib.sha256(); chunks=' + repr(chunks) + '\n'
                  'with tmp.open("wb") as out:\n'
                  ' for key,size in chunks:\n'
                  '  p=root/key; assert not p.is_symlink(); data=p.read_bytes(); '
                  'assert len(data)==size and hashlib.sha256(data).hexdigest()==key; out.write(data); h.update(data)\n'
                  'assert h.hexdigest()==' + repr(digest(archive)) + '\n'
                  'os.replace(tmp,target)')
        self.command('python3 -c ' + shlex.quote(script), timeout=remaining())

    def start(self) -> None:
        """Idempotently start the remote wrapper, which owns its optimizer group."""
        manifest = self.store.read(self.identifier, 'input/manifest.json')
        cache_times = manifest.get('public_market_cache_mtimes', {})
        if cache_times:
            if not isinstance(cache_times, dict) or set(cache_times) - {'binance', 'bybit'}:
                raise VastError('Invalid public market cache metadata', 422)
            for exchange, stamp in cache_times.items():
                if (type(stamp) not in (int, float) or not 0 <= time.time() - stamp < 86400):
                    raise VastError('Public market cache expired; prepare fresh input before starting', 422)
                relative = 'caches/' + exchange + '/markets.json'
                expected = next((entry for entry in manifest['files'] if entry['path'] == relative), None)
                if expected is None or digest(self.directory / 'input' / relative) != expected['sha256']:
                    raise VastError('Public market cache is missing or changed', 422)
            # The immutable image starts PB8 from output/. Keep source freshness:
            # extraction/copy time must never make an old market snapshot fresh.
            script = (
                'import pathlib,shutil,os,time; '
                'root=pathlib.Path(' + repr(self.remote_root) + '); '
                'stamps=' + repr(cache_times) + '; '
                'assert all(0 <= time.time()-t < 86400 for t in stamps.values()); '
                '\nfor exchange,stamp in stamps.items():\n'
                ' source=root/"input"/"caches"/exchange/"markets.json"\n'
                ' target=root/"output"/"caches"/exchange/"markets.json"\n'
                ' target.parent.mkdir(parents=True,exist_ok=True)\n'
                ' shutil.copyfile(source,target)\n'
                ' os.utime(target,(stamp,stamp))\n'
            )
            self.command('/usr/local/bin/python -c ' + shlex.quote(script), timeout=30)
        self.command("mkdir -p " + self.remote_root + " && " + "PBGUI_WORKDIR=" + self.remote_root + " nohup " + WORKER + "run > " + self.remote_root + "/runner.log 2>&1 < /dev/null &", timeout=15)

    def collect(self, final: bool, timeout: int) -> Path:
        """Download and verify a bounded snapshot before replacing the last copy."""
        action = "final" if final else "partial"
        metadata = self.operation(action, timeout=timeout)
        size = metadata.get("bytes")
        if type(size) is not int or not 0 < size <= 2 * 1024**3 or not re.fullmatch(r"[0-9a-f]{64}", str(metadata.get("sha256"))):
            raise VastError("Invalid result snapshot metadata")
        state = self.store.read(self.identifier)
        if int(state.get("downloaded_bytes", 0)) + size > 2 * 1024**3:
            raise VastError("Job transfer limit reached; collection cannot exceed 2 GB")
        destination = self.directory / (action + ".tar.gz")
        temporary = destination.with_suffix(".tmp")
        try:
            with temporary.open("wb") as stream:
                self.command(f"head -c {size + 1} {self.remote_root}/{action}.tar.gz", stdout=stream,
                             timeout=timeout, max_output=size + 1)
        finally:
            if temporary.exists():
                temporary.chmod(0o600)
                self.store.update(self.identifier, downloaded_bytes=int(state.get("downloaded_bytes", 0)) + temporary.stat().st_size)
        if temporary.stat().st_size != size or digest(temporary) != metadata["sha256"]:
            raise VastError("Result archive checksum mismatch")
        os.replace(temporary, destination)
        extracted = extract_results(destination, self.directory, action)
        log = extracted / "optimizer.log"
        if log.is_file():
            log_root = ensure_private_directory(PROJECT / "data/logs/optimizes_v8")
            with log.open("rb") as stream:
                stream.seek(max(0, log.stat().st_size - 1024 * 1024))
                text = stream.read(1024 * 1024).decode("utf-8", errors="replace")
            atomic_write_private_text(log_root / ("vast_" + self.identifier + ".log"), text)
        self.store.update(self.identifier, last_backup_at=time.time(), final_collected=final or state.get("final_collected", False))
        return extracted


def extract_results(archive_path: Path, directory: Path, name: str) -> Path:
    """Extract only regular expected result files and validate their manifest."""
    destination = directory / (name + "-results")
    with tempfile.TemporaryDirectory(dir=directory) as temporary:
        stage = Path(temporary)
        names = set()
        total = 0
        with tarfile.open(archive_path, "r:gz") as archive:
            for item in archive:
                path = safe_path(stage, item.name)
                if not item.isfile() or item.name in names:
                    raise VastError("Invalid result archive entry")
                parts = Path(item.name).parts
                allowed = item.name in {"manifest.json", "finished.json", "optimizer.log"} or (
                    len(parts) in (3, 4) and parts[0] == "optimize_results" and (
                        len(parts) == 3 and parts[2] in {"all_results.bin", "checkpoint.pkl"} or
                        len(parts) == 4 and parts[2] == "pareto" and re.fullmatch(r"[A-Za-z0-9_-]+\.json", parts[3])))
                total += item.size
                if not allowed or total > 2 * 1024**3 or len(names) >= 100_000:
                    raise VastError("Unexpected or oversized result archive")
                names.add(item.name)
                path.parent.mkdir(parents=True, exist_ok=True)
                with archive.extractfile(item) as source, path.open("xb") as target:
                    shutil.copyfileobj(source, target, 1024 * 1024)
                path.chmod(0o600)
        manifest = json.loads((stage / "manifest.json").read_text())
        expected = {"manifest.json"}
        for entry in manifest["files"]:
            path = safe_path(stage, entry["path"])
            if entry["path"] in expected or path.stat().st_size != entry["bytes"] or digest(path) != entry["sha256"]:
                raise VastError("Result file checksum mismatch")
            expected.add(entry["path"])
        if expected != names:
            raise VastError("Result manifest does not match the archive")
        if destination.is_symlink():
            raise VastError("Invalid snapshot destination")
        if destination.exists():
            shutil.rmtree(destination)  # Only the owned replaceable snapshot cache.
        os.replace(stage, destination)
    return destination


def allocated_cpu_workers(quota) -> int:
    """Choose whole CPU workers from a positive finite container allocation."""
    import math
    if type(quota) not in (int, float) or not math.isfinite(quota) or quota <= 0:
        raise VastError('Invalid measured CPU allocation', 422)
    return max(1, math.floor(quota))


def execution_input(store: JobStore, identifier: str) -> Path:
    """Prepare an atomic CPU-adjusted execution copy, preserving the source snapshot."""
    from pb8_config import load_pb8_config, save_prepared_pb8_config
    directory = store.directory(identifier)
    row = store.read(identifier)
    source = directory / 'input'
    if not row.get('auto_cpu_workers'):
        return source
    if not row.get('cpu_allocation_resolved') or type(row.get('workers')) is not int or row['workers'] < 1:
        raise VastError('CPU allocation has not been measured', 422)
    target = directory / 'execution-input'
    with advisory_file_lock(directory / '.execution-lock'):
        if target.is_symlink():
            raise VastError('Invalid execution input', 422)
        if target.exists():
            manifest = json.loads((target / 'manifest.json').read_text())
            if manifest.get('execution_workers') != row['workers'] or digest(target / 'optimize.json') != manifest['config_sha256']:
                raise VastError('Execution input changed after preparation', 422)
            return target
        manifest = store.read(identifier, 'input/manifest.json')
        if digest(source / 'optimize.json') != manifest['config_sha256']:
            raise VastError('Source config changed before execution', 422)
        config = load_pb8_config(source / 'optimize.json')
        config['optimize']['n_cpus'] = row['workers']
        config['optimize'].setdefault('gpu', {})['exact_workers'] = row['workers']
        with tempfile.TemporaryDirectory(dir=directory) as temporary:
            stage = Path(temporary) / 'execution'
            stage.mkdir(mode=0o700)
            save_prepared_pb8_config(config, stage / 'optimize.json')
            manifest.update(config_sha256=digest(stage / 'optimize.json'), execution_workers=row['workers'])
            write_json(stage / 'manifest.json', manifest)
            os.replace(stage, target)
    return target


def import_results(store: JobStore, identifier: str, *, partial: bool = False) -> dict:
    """Import verified native results idempotently, never unpickle a checkpoint."""
    import msgpack
    directory = store.directory(identifier)
    intent = store.read(identifier, "intent.json")
    sources = list((directory / ("partial-results" if partial else "final-results") / "optimize_results").glob("*"))
    if len(sources) != 1:
        raise VastError("No single complete native result directory found")
    source = sources[0]
    binary = source / "all_results.bin"
    count, complete = 0, 0
    if binary.is_file():
        with binary.open("rb") as stream:
            unpacker = msgpack.Unpacker(stream, raw=False, strict_map_key=False, max_buffer_size=64 * 1024 * 1024)
            for record in unpacker:
                if not isinstance(record, dict):
                    raise VastError("Invalid native result record")
                count += 1
                complete = unpacker.tell()
        if not count or complete != binary.stat().st_size:
            raise VastError("Native result binary is empty or incomplete")
    elif partial and list((source / "pareto").glob("*.json")):
        # Lightweight worker backups contain the Pareto front, not the full
        # evaluation history. Keep the true counter rather than inventing rows.
        count = int(store.read(identifier).get("exact_completed", 0))
    else:
        raise VastError("Native result binary is empty or incomplete")
    root = Path(intent["results_root"])
    if not root.is_absolute() or root.is_symlink() or root.name != "optimize_results":
        raise VastError("Invalid configured optimizer result root")
    root.mkdir(parents=True, exist_ok=True)
    target = root / (source.name + "_vast_" + identifier[:12])
    with advisory_file_lock(root / ".vast-import"):
        if target.is_symlink():
            raise VastError("Invalid existing result destination")
        if target.exists():
            provenance = json.loads((target / ".pbgui_vast.json").read_text())
            if provenance.get("job_id") != identifier:
                raise VastError("Existing result belongs to another import")
            if provenance.get("partial"):
                if count < int(provenance.get("evaluations", 0)):
                    raise VastError("Result snapshot is older than the published results")
                # Replace individual files atomically so open readers retain a valid
                # previous file while new readers see the latest complete snapshot.
                for source_file in ([binary] if binary.is_file() else []) + sorted((source / "pareto").glob("*.json")):
                    destination = target / source_file.relative_to(source)
                    destination.parent.mkdir(exist_ok=True, mode=0o700)
                    if destination.is_symlink() or destination.parent.is_symlink():
                        raise VastError("Invalid result destination")
                    with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as stream:
                        temporary = Path(stream.name)
                        try:
                            with source_file.open("rb") as incoming:
                                shutil.copyfileobj(incoming, stream)
                            stream.close()
                            os.replace(temporary, destination)
                        finally:
                            temporary.unlink(missing_ok=True)
                for old in (target / "pareto").glob("*.json"):
                    if not (source / "pareto" / old.name).exists():
                        old.unlink()
                provenance.update(partial=partial, evaluations=count)
                write_json(target / ".pbgui_vast.json", provenance)
            elif digest(target / "all_results.bin") != digest(binary):
                raise VastError("Existing result belongs to another import")
        else:
            with tempfile.TemporaryDirectory(prefix=".vast-import-", dir=root) as temporary:
                stage = Path(temporary) / "result"
                shutil.copytree(source, stage, ignore=shutil.ignore_patterns("checkpoint.pkl"))
                manifest = store.read(identifier, "input/manifest.json")
                if manifest.get('validation_plan'):
                    from scenario_windows import build_validation_plan, VALIDATION_PLAN_FILENAME
                    plan = build_validation_plan({'pbgui': {'scenario_template': manifest['validation_plan']}})
                    if plan is not None:
                        write_json(stage / VALIDATION_PLAN_FILENAME, plan)
                if manifest.get("sweep_plan"):
                    write_json(stage / ".pbgui_sweep_cycles.json", manifest["sweep_plan"])
                write_json(stage / ".pbgui_vast.json", {"job_id": identifier, "image": intent["image"],
                           "source_config_sha256": intent["source_config_sha256"], "pb8_revision": intent["pb8_revision"],
                           "partial": partial, "evaluations": count})
                for path in [stage, *stage.rglob("*")]:
                    path.chmod(0o700 if path.is_dir() else 0o600)
                os.replace(stage, target)
    return {"result_path": str(target), "result_partial": partial, "evaluations": count, "pareto_count": len(list((target / "pareto").glob("*.json")))}
