"""Identify this installation's live systemd Vast supervisors for explicit restarts."""
import json
from pathlib import Path
import re

import psutil

SERVICE = 'VastSupervisorRestart'
UNIT_PATTERN = re.compile(r'pbgui-vast-([0-9a-f]{32})-(run|guard)\.service')


def stale_supervisors(root, current_serial):
    """Read process identity and startup serial; never start, stop, or modify a job."""
    root = Path(root).resolve()
    script = str(root / 'vast_job_runner.py')
    result = []
    for process in psutil.process_iter(['pid', 'cmdline', 'create_time']):
        try:
            args = process.info.get('cmdline') or []
            if len(args) != 4 or args[1] != script:
                continue
            mode, identifier = args[2:]
            unit = f'pbgui-vast-{identifier}-{mode}.service'
            if not UNIT_PATTERN.fullmatch(unit):
                continue
            # Only the actual owning systemd unit is restartable, not an argv impersonator.
            cgroup = Path(f'/proc/{process.pid}/cgroup').read_text()
            if not any(line.endswith('/' + unit) for line in cgroup.splitlines()):
                continue
            path = root / 'data/vast/jobs' / identifier / 'state.json'
            if any(part.is_symlink() for part in (path, *path.parents)):
                continue
            record = {}
            if path.exists():
                try:
                    state = json.loads(path.read_text())
                    record = state.get('supervisor_' + mode) or {}
                except (ValueError, AttributeError):
                    record = {}
            if not isinstance(record, dict):
                record = {}
            matching = (record.get('pid') == process.pid and
                        record.get('created_at') == process.info['create_time'])
            running = str(record.get('code_serial', '')) if matching else ''
            if running == str(current_serial):
                continue
            result.append(dict(service='VastSupervisor', unit=unit,
                label=f'Vast {mode} supervisor {identifier[:8]}',
                running_serial=running, current_serial=str(current_serial),
                reason='outdated code serial' if running else 'startup serial not reported'))
        except (psutil.NoSuchProcess, psutil.AccessDenied, FileNotFoundError, ProcessLookupError):
            continue
    return sorted(result, key=lambda item: item['unit'])
