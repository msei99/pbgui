"""Resume cloud input archives over one owned, strictly authenticated SSH stream."""

import ipaddress
import json
import os
from pathlib import Path
import re
import selectors
import shlex
import shutil
import signal
import subprocess
import tempfile
import time

from logging_helpers import human_log
from vast_jobs import digest, job_id
from vast_provider import VastError
from setup.vast_gpu_benchmark.cloud_worker import safe_path

SERVICE = "VastRunner"
_PROGRESS = re.compile(rb"^\s*([\d,]+)\s+\d+%\s")


def available(connection, timeout):
    """Retain the old transport only when either endpoint lacks rsync."""
    if shutil.which('rsync') is None:
        return False
    return connection.command(
        "if command -v rsync >/dev/null 2>&1; then printf rsync; fi",
        timeout=min(30, timeout)).strip() == b'rsync'


def ssh_arguments(connection):
    """Preserve the same pinned host identity used by the worker command channel."""
    job_id(connection.identifier)
    job_id(connection.lease_id)
    if not ipaddress.ip_address(connection.host).is_global or not 1 <= int(connection.port) <= 65535:
        raise VastError('Invalid rsync SSH endpoint', 422)
    return ['ssh', '-T', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
            '-o', 'ServerAliveInterval=10', '-o', 'ServerAliveCountMax=2',
            '-o', 'StrictHostKeyChecking=yes', '-o', 'IdentitiesOnly=yes',
            '-o', 'Compression=no', '-o', 'HostKeyAlias=pbgui-' + connection.lease_id,
            '-o', 'UserKnownHostsFile=' + str(connection.identity_directory / 'known_hosts'),
            '-i', str(connection.identity_directory / 'ssh-key'), '-p', str(connection.port)]


def _cancelled(connection):
    """Observe both job cancellation and rental cleanup while the child is running."""
    for identifier in {connection.identifier, connection.lease_id}:
        control = connection.store.read(identifier, 'control.json')
        if control.get('stop') or control.get('cleanup'):
            return True
    return False


def _attempt(connection, args, deadline, total, offset, report, *, statistics=None):
    """Drain bounded progress/error streams and always reap rsync plus its SSH child."""
    process = subprocess.Popen(args, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, start_new_session=True, env={**os.environ, 'LC_ALL': 'C'})
    pending = b''
    diagnostic = bytearray()
    started = time.monotonic()
    last_report = 0.0
    processed = offset
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            selector.register(process.stderr, selectors.EVENT_READ)
            while selector.get_map():
                now = time.monotonic()
                if now >= deadline:
                    raise VastError('Rsync upload deadline reached; partial data retained')
                if now - last_report >= 1:
                    if _cancelled(connection):
                        raise VastError('Rsync upload cancelled; partial data retained', 422)
                    report(processed, max(0, processed - offset) / max(.001, now - started))
                    last_report = now
                for key, _ in selector.select(.25):
                    value = os.read(key.fileobj.fileno(), 65536)
                    if not value:
                        selector.unregister(key.fileobj)
                    elif key.fileobj is process.stderr:
                        diagnostic.extend(value[:max(0, 8192 - len(diagnostic))])
                    else:
                        lines = re.split(rb'[\r\n]', pending + value)
                        pending = lines.pop()
                        if len(pending) > 8192:
                            raise VastError('Invalid rsync progress output', 422)
                        for line in lines:
                            if statistics is not None:
                                for title, field in ((b'Total transferred file size:', 'changed_bytes'),
                                                     (b'Literal data:', 'literal_bytes'),
                                                     (b'Total bytes sent:', 'sent_bytes')):
                                    if line.startswith(title):
                                        value = line[len(title):].strip().split()[0].replace(b',', b'')
                                        if value.isdigit():
                                            statistics[field] = int(value)
                            match = _PROGRESS.match(line)
                            if match:
                                count = int(match[1].replace(b',', b''))
                                # Append verification may resend a corrupt prefix;
                                # progress2 then includes both passes cumulatively.
                                if not 0 <= count <= 2 * total:
                                    raise VastError('Invalid rsync progress count', 422)
                                processed = min(total, count)
            code = process.wait(timeout=max(.01, deadline - time.monotonic()))
            report(processed, max(0, processed - offset) / max(.001, time.monotonic() - started))
            return code, diagnostic.decode(errors='replace').lower()
    finally:
        # The child owns a separate process group, including its SSH subprocess.
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
        process.stdout.close()
        process.stderr.close()


def upload(connection, archive: Path, *, timeout: int):
    """Resume a hash-named partial file, then publish only after full SHA256 verification."""
    archive = archive.resolve(strict=True)
    if not archive.is_relative_to(connection.directory.resolve()) or not archive.is_file():
        raise VastError('Invalid rsync input archive', 422)
    ssh = ssh_arguments(connection)
    total, checksum = archive.stat().st_size, digest(archive)
    root = '/work/pbgui/jobs/' + job_id(connection.identifier)
    if connection.remote_root != root:
        raise VastError('Invalid rsync destination', 422)
    destination = root + '/rsync-' + checksum + '.tar.gz'
    deadline = time.monotonic() + timeout

    def remaining():
        """Bound retries and verification by the rental's remaining upload time."""
        seconds = int(deadline - time.monotonic())
        if seconds <= 0:
            raise VastError('Rsync upload deadline reached; partial archive retained')
        return seconds

    def report(count, rate=0, stage='sending', verified=False):
        """Distinguish rsync progress from the final checksum-verified archive."""
        connection.store.update(connection.identifier, upload_progress={
            'transport': 'rsync', 'stage': stage, 'bytes': total if verified else 0,
            'transferred_bytes': count, 'total': total, 'bytes_per_second': rate})

    host = '[' + connection.host + ']' if ':' in connection.host else connection.host
    args = ['rsync', '--partial', '--append-verify', '--protect-args', '--no-compress',
            '--info=progress2', '--outbuf=L', '--timeout=60', '--chmod=F600',
            '-e', shlex.join(ssh), '--', str(archive), 'root@' + host + ':' + destination]
    for attempt in range(3):
        if _cancelled(connection):
            raise VastError('Rsync upload cancelled; partial archive retained', 422)
        script = ('import pathlib,hashlib; p=pathlib.Path(' + repr(destination) + '); '
                  'p.parent.mkdir(parents=True,exist_ok=True); '
                  'assert not p.is_symlink()\n'
                  'if p.exists() and p.stat().st_size >= ' + str(total) + ':\n'
                  ' with p.open("rb") as stream: valid=hashlib.file_digest(stream,"sha256").hexdigest()==' + repr(checksum) + '\n'
                  ' if not valid: p.unlink()\n'
                  'print(p.stat().st_size if p.exists() else 0)')
        raw = connection.command('python3 -c ' + shlex.quote(script), timeout=remaining())
        try:
            offset = int(raw)
            if not 0 <= offset <= total:
                raise ValueError('size')
        except ValueError:
            raise VastError('Invalid partial rsync archive size', 422) from None
        report(offset)
        code, diagnostic = _attempt(connection, args, deadline, total, offset, report)
        if code == 0:
            break
        permanent = any(token in diagnostic for token in (
            'permission denied', 'host key verification failed', 'no space left',
            'command not found', 'unknown option'))
        if permanent or code not in {10, 12, 30, 35, 255} or attempt == 2:
            raise VastError('Rsync upload failed (exit ' + str(code) + '); partial archive retained',
                            422 if permanent else 502)
        report(offset, stage='reconnecting')
        human_log(SERVICE, 'Rsync connection interrupted; resuming partial input archive', level='WARNING')
        remaining()
    report(total, stage='verifying')
    verify = (
        'import pathlib,hashlib,os; p=pathlib.Path(' + repr(destination) + '); '
        'assert p.is_file() and not p.is_symlink() and p.stat().st_size==' + str(total) + '; '
        'h=hashlib.sha256()\n'
        'with p.open("rb") as stream:\n'
        ' for block in iter(lambda:stream.read(1048576),b""): h.update(block)\n'
        'assert h.hexdigest()==' + repr(checksum) + '\n'
        'os.replace(p,' + repr(root + '/input.tar.gz') + ')')
    connection.command('python3 -c ' + shlex.quote(verify), timeout=remaining())
    report(total, stage='verified', verified=True)


def sync_files(connection, config_root: Path, *, timeout: int):
    """Synchronize immutable data files directly; rsync alone decides what to send."""
    identifier = job_id(connection.identifier)
    root = '/work/pbgui/jobs/' + identifier
    if connection.remote_root != root:
        raise VastError('Invalid rsync destination', 422)
    config_root = config_root.resolve(strict=True)
    if not config_root.is_relative_to(connection.directory.resolve()):
        raise VastError('Invalid rsync source', 422)
    manifest = json.loads(safe_path(config_root, 'manifest.json').read_text())
    deadline = time.monotonic() + timeout
    next_control_check = 0.0
    helper = (Path(__file__).parent / 'setup/vast_gpu_benchmark/sync_input.py').read_bytes()

    def remaining():
        """Bound preparation, synchronization and publication by one deadline."""
        nonlocal next_control_check
        now = time.monotonic()
        if now >= next_control_check:
            if _cancelled(connection):
                raise VastError('Rsync synchronization cancelled; partial files retained', 422)
            next_control_check = now + 1
        seconds = int(deadline - now)
        if seconds <= 0:
            raise VastError('Rsync synchronization deadline reached; partial files retained')
        return seconds

    def worker_step(action):
        """Execute the shipped compatibility helper over the pinned SSH channel."""
        with tempfile.TemporaryFile() as stream:
            if action == 'prepare':
                stream.write(json.dumps({name: safe_path(config_root, name).read_text()
                                         for name in ('manifest.json', 'optimize.json')}).encode())
            stream.seek(0)
            return connection.command('PBGUI_WORKDIR=' + root + ' /usr/local/bin/python -c '
                                      + shlex.quote(helper.decode()) + ' ' + action,
                                      stdin=stream, timeout=remaining())

    connection.store.update(identifier, upload_progress={
        'transport': 'rsync', 'mode': 'files', 'stage': 'preparing_files'})
    with tempfile.TemporaryDirectory(prefix='rsync-input-', dir=connection.directory) as temporary:
        source = Path(temporary)
        entries = {}
        seen = {'manifest.json', 'optimize.json'}
        for item in manifest['files']:
            remaining()
            key, size = item['sha256'], item['bytes']
            if (not isinstance(key, str) or not re.fullmatch(r'[0-9a-f]{64}', key)
                    or type(size) is not int or size < 0 or item['path'] in seen):
                raise VastError('Invalid rsync data manifest', 422)
            seen.add(item['path'])
            # execution-input contains only CPU-adjusted config/manifest;
            # frozen OHLCV files remain in the original prepared input tree.
            original = safe_path(connection.directory / 'input', item['path'])
            if not original.is_file() or original.stat().st_size != size:
                raise VastError('Prepared input file is missing or changed', 422)
            name = 'rsync-data/' + key
            if name in entries:
                if entries[name][1] != size:
                    raise VastError('Conflicting rsync data identity', 422)
            else:
                entries[name] = (original, size)
        total = sum(size for _, size in entries.values())
        if total > 12 * 1024**3:
            raise VastError('Input files exceed the transfer limit', 422)
        # Hardlinks create a cheap sender view, retaining timestamps without
        # copying or re-reading the large immutable prepared dataset.
        for name, (original, _) in entries.items():
            target = source / name
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            os.link(original, target)
        file_list = source / 'files.list'
        file_list.write_bytes(b''.join(name.encode() + b'\0' for name in entries))
        worker_step('prepare')
        connection.store.update(identifier, transfer_input_bytes=total)

        def report(count, rate=0, stage='sending'):
            """Persist sender work; skipped-file savings are known at completion."""
            connection.store.update(identifier, upload_progress={
                'transport': 'rsync', 'mode': 'files', 'stage': stage,
                'bytes': 0, 'transferred_bytes': count, 'total': total,
                'bytes_per_second': rate})

        host = '[' + connection.host + ']' if ':' in connection.host else connection.host
        args = ['rsync', '--recursive', '--times', '--size-only', '--protect-args', '--stats',
                '--partial-dir=.rsync-partial', '--compress', '--info=progress2',
                '--outbuf=L', '--timeout=60', '--chmod=D700,F600',
                '--from0', '--files-from=' + str(file_list),
                '-e', shlex.join(ssh_arguments(connection)), '--', str(source) + '/',
                'root@' + host + ':/work/pbgui/']
        # A content hash is the immutable filename, so same name+size is a safe
        # quick-check identity even when another prepared job has newer mtimes.
        # No --checksum, --inplace or --delete: rsync validates changed transfers
        # and atomically renames them; interrupted files never become cache hits.
        sent_bytes = 0
        for attempt in range(3):
            remaining()
            report(0)
            statistics = {}
            code, diagnostic = _attempt(connection, args, deadline, total, 0, report, statistics=statistics)
            sent_bytes += statistics.get('sent_bytes', statistics.get('literal_bytes', 0))
            if code == 0:
                break
            permanent = any(token in diagnostic for token in (
                'permission denied', 'host key verification failed', 'no space left',
                'command not found', 'unknown option'))
            if permanent or code not in {10, 12, 30, 35, 255} or attempt == 2:
                raise VastError('Rsync file synchronization failed (exit ' + str(code)
                                + '); partial files retained', 422 if permanent else 502)
            report(0, stage='reconnecting')
            human_log(SERVICE, 'Rsync connection interrupted; reusing completed and partial input files', level='WARNING')
        report(total, stage='installing')
        worker_step('publish')
        report(total, stage='synchronized')
        connection.store.update(identifier, transfer_input_bytes=sent_bytes, sync_statistics={
            'total_bytes': total, 'changed_bytes': statistics.get('changed_bytes'),
            'literal_bytes': statistics.get('literal_bytes'), 'sent_bytes': sent_bytes,
            'reused_bytes': max(0, total - statistics['changed_bytes']) if 'changed_bytes' in statistics else None})
