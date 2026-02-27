"""
Realtime Data Collector for PBMaster.

Maintains persistent SSH streams to each VPS that continuously report
system metrics (CPU, RAM, Disk, Swap) every second. Also periodically
collects bot instance monitor data (PnL, errors, tracebacks).

Architecture:
    - Per-VPS: A persistent SSH channel runs a small Python agent script
      that prints JSON every second. A StreamReader thread parses this
      and updates an in-memory dict.
    - UI reads directly from memory — no filesystem IPC, ~1-3s latency.
    - Instance data collected every 30s via a separate SSH command.
"""

import json
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from master.connection_pool import SSHConnectionPool
from logging_helpers import human_log as _log


SERVICE = "PBMaster"

# Remote PBGui directory on VPS (relative to home)
REMOTE_PBGUI_DIR = "software/pbgui"

# Agent script that runs on VPS — prints JSON per second via stdout.
# Pure-stdlib agent script — no psutil required.
# Reads /proc/stat, /proc/meminfo, os.statvfs for metrics.
MONITOR_AGENT_SCRIPT = r'''python3 -u -c "
import json, os, time
def rcpu():
    with open('/proc/stat') as f:
        p = f.readline().split()
    idle = int(p[4])
    return idle, sum(int(x) for x in p[1:])
def rmem():
    d = {}
    with open('/proc/meminfo') as f:
        for ln in f:
            k, v = ln.split(':')
            if k in ('MemTotal','MemAvailable','SwapTotal','SwapFree'):
                d[k] = int(v.split()[0]) * 1024
    mt = d.get('MemTotal', 0)
    ma = d.get('MemAvailable', 0)
    mu = mt - ma
    mp = round(mu / mt * 100, 1) if mt else 0
    st = d.get('SwapTotal', 0)
    sf = d.get('SwapFree', 0)
    su = st - sf
    sp = round(su / st * 100, 1) if st else 0
    return [mt, ma, mp, mu], [st, su, sf, sp]
pi, pt = rcpu()
time.sleep(1)
while True:
    try:
        ci, ct = rcpu()
        di, dt = ci - pi, ct - pt
        cpu = round((1 - di / dt) * 100, 1) if dt else 0
        pi, pt = ci, ct
        mem, swap = rmem()
        s = os.statvfs('/')
        dtot = s.f_frsize * s.f_blocks
        dused = s.f_frsize * (s.f_blocks - s.f_bfree)
        dfree = s.f_frsize * s.f_bavail
        dpct = round(dused / dtot * 100, 1) if dtot else 0
        print(json.dumps({'ts': time.time(), 'cpu': cpu, 'mem': mem, 'disk': [dtot, dused, dfree, dpct], 'swap': swap}), flush=True)
    except Exception:
        pass
    time.sleep(1)
"'''

# Script to collect all bot instance monitor data in one SSH call.
# Only includes instances whose process is actually running.
INSTANCE_COLLECT_SCRIPT = r'''python3 -u -c "
import json, glob, os, subprocess
HOME = os.path.expanduser('~')
PBGDIR = os.path.join(HOME, 'software/pbgui')

# Get list of running bot processes and extract their instance dirs
running_dirs = set()
try:
    out = subprocess.check_output(['ps', 'aux'], text=True)
    for line in out.splitlines():
        # V7: main.py with config_run.json
        if 'main.py' in line and 'config_run.json' in line:
            for part in line.split():
                if part.endswith('/config_run.json'):
                    running_dirs.add(os.path.dirname(part))
        # V6 Single: passivbot.py with config.json
        elif 'passivbot.py' in line and 'config.json' in line:
            for part in line.split():
                if part.endswith('/config.json'):
                    running_dirs.add(os.path.dirname(part))
        # V6 Multi: passivbot_multi.py with multi_run.hjson
        elif 'passivbot_multi.py' in line and 'multi_run.hjson' in line:
            for part in line.split():
                if part.endswith('/multi_run.hjson'):
                    running_dirs.add(os.path.dirname(part))
except Exception:
    pass

result = []
patterns = [
    os.path.join(PBGDIR, 'data/run_v7/*/monitor.json'),
    os.path.join(PBGDIR, 'data/multi/*/monitor.json'),
    os.path.join(PBGDIR, 'data/instances/*/monitor.json'),
]
for pat in patterns:
    for mf in glob.glob(pat):
        inst_dir = os.path.dirname(mf)
        if inst_dir not in running_dirs:
            continue
        try:
            with open(mf) as f:
                result.append(json.load(f))
        except Exception:
            pass
print(json.dumps(result))
"'''

# How often to collect instance data (seconds)
INSTANCE_COLLECT_INTERVAL = 30


@dataclass
class SystemMetrics:
    """Latest system metrics from a VPS."""
    timestamp: float = 0.0
    cpu: float = 0.0
    mem_total: int = 0
    mem_available: int = 0
    mem_percent: float = 0.0
    mem_used: int = 0
    disk_total: int = 0
    disk_used: int = 0
    disk_free: int = 0
    disk_percent: float = 0.0
    swap_total: int = 0
    swap_used: int = 0
    swap_free: int = 0
    swap_percent: float = 0.0

    @classmethod
    def from_json(cls, data: dict) -> "SystemMetrics":
        """Parse from agent JSON output."""
        mem = data.get("mem", [0, 0, 0, 0])
        disk = data.get("disk", [0, 0, 0, 0])
        swap = data.get("swap", [0, 0, 0, 0])
        return cls(
            timestamp=data.get("ts", 0.0),
            cpu=data.get("cpu", 0.0),
            mem_total=mem[0] if len(mem) > 0 else 0,
            mem_available=mem[1] if len(mem) > 1 else 0,
            mem_percent=mem[2] if len(mem) > 2 else 0.0,
            mem_used=mem[3] if len(mem) > 3 else 0,
            disk_total=disk[0] if len(disk) > 0 else 0,
            disk_used=disk[1] if len(disk) > 1 else 0,
            disk_free=disk[2] if len(disk) > 2 else 0,
            disk_percent=disk[3] if len(disk) > 3 else 0.0,
            swap_total=swap[0] if len(swap) > 0 else 0,
            swap_used=swap[1] if len(swap) > 1 else 0,
            swap_free=swap[2] if len(swap) > 2 else 0,
            swap_percent=swap[3] if len(swap) > 3 else 0.0,
        )


class _StreamReader(threading.Thread):
    """
    Background thread: reads JSON lines from a persistent SSH channel
    and updates the shared system_data dict for one VPS.
    """

    def __init__(self, hostname: str, channel, system_data: dict,
                 lock: threading.Lock):
        super().__init__(daemon=True, name=f"realtime-{hostname}")
        self.hostname = hostname
        self._channel = channel
        self._system_data = system_data
        self._lock = lock
        self._active = True
        self._error: Optional[str] = None

    @property
    def active(self) -> bool:
        return self._active

    @property
    def error(self) -> Optional[str]:
        return self._error

    def stop(self):
        self._active = False

    def run(self):
        """Read JSON lines from SSH channel stdout, update shared dict."""
        try:
            stdout = self._channel.makefile("r")
            for line in stdout:
                if not self._active:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    metrics = SystemMetrics.from_json(data)
                    with self._lock:
                        self._system_data[self.hostname] = metrics
                except json.JSONDecodeError:
                    continue
        except Exception as e:
            if self._active:
                self._error = str(e)
                _log(SERVICE, f"[realtime] Stream reader error for "
                     f"{self.hostname}: {e}", level="WARNING")
        finally:
            self._active = False
            # Capture stderr for diagnostics if stream died quickly
            try:
                if self._channel.recv_stderr_ready():
                    stderr = self._channel.makefile_stderr("r").read(4096)
                    if stderr.strip():
                        _log(SERVICE, f"[realtime] Stream stderr for "
                             f"{self.hostname}: {stderr.strip()}",
                             level="WARNING")
            except Exception:
                pass
            try:
                self._channel.close()
            except Exception:
                pass
            _log(SERVICE, f"[realtime] Stream reader ended for {self.hostname}")


class RealtimeCollector:
    """
    Manages persistent monitoring streams and periodic instance collection.

    System metrics: ~1s latency via persistent SSH streams.
    Instance data: ~30s latency via periodic SSH commands.

    Thread-safe — all reads from UI go through get_* methods.
    """

    def __init__(self, pool: SSHConnectionPool):
        self._pool = pool
        self._lock = threading.Lock()

        # Per-host latest system metrics (updated every ~1s by stream readers)
        self._system_data: dict[str, SystemMetrics] = {}

        # Per-host instance data (updated every ~30s by periodic collection)
        self._instance_data: dict[str, list[dict]] = {}

        # Active stream readers per hostname
        self._streams: dict[str, _StreamReader] = {}

        # Instance collection tracking
        self._last_instance_collect: float = 0.0
        self._instance_collect_threads: dict[str, threading.Thread] = {}

    # ── Stream management (system metrics, 1s) ──

    def start_stream(self, hostname: str) -> bool:
        """
        Start a persistent monitoring stream on a VPS.
        Called when SSH connection is established/reconnected.
        Returns True if stream started successfully.
        """
        # Stop existing stream if any
        self.stop_stream(hostname)

        client = self._pool.get(hostname)
        if not client:
            _log(SERVICE, f"[realtime] Cannot start stream for {hostname}: "
                 "no connection", level="WARNING")
            return False

        try:
            transport = client.get_transport()
            if not transport or not transport.is_active():
                _log(SERVICE, f"[realtime] Cannot start stream for {hostname}: "
                     "transport not active", level="WARNING")
                return False

            channel = transport.open_session()
            channel.exec_command(MONITOR_AGENT_SCRIPT)

            reader = _StreamReader(
                hostname=hostname,
                channel=channel,
                system_data=self._system_data,
                lock=self._lock,
            )
            reader.start()

            with self._lock:
                self._streams[hostname] = reader

            _log(SERVICE, f"[realtime] Started monitoring stream for {hostname}")
            return True

        except Exception as e:
            _log(SERVICE, f"[realtime] Failed to start stream for {hostname}: "
                 f"{e}", level="ERROR")
            return False

    def stop_stream(self, hostname: str):
        """Stop the monitoring stream for a VPS."""
        with self._lock:
            reader = self._streams.pop(hostname, None)
        if reader:
            reader.stop()
            _log(SERVICE, f"[realtime] Stopped stream for {hostname}")

    def stop_all_streams(self):
        """Stop all monitoring streams."""
        with self._lock:
            hostnames = list(self._streams.keys())
        for hostname in hostnames:
            self.stop_stream(hostname)

    def is_stream_alive(self, hostname: str) -> bool:
        """Check if the monitoring stream for a host is alive."""
        with self._lock:
            reader = self._streams.get(hostname)
        return reader is not None and reader.is_alive() and reader.active

    def restart_dead_streams(self):
        """Restart streams that have died (e.g. due to SSH reconnect)."""
        with self._lock:
            dead = [h for h, r in self._streams.items()
                    if not r.is_alive() or not r.active]
        for hostname in dead:
            _log(SERVICE, f"[realtime] Restarting dead stream for {hostname}")
            self.start_stream(hostname)

    # ── Instance data collection (30s) ──

    def collect_instances(self, hostname: str):
        """
        Collect bot instance monitor data from a single VPS.
        Runs the collection script and stores result in memory.
        """
        client = self._pool.get(hostname)
        if not client:
            return

        try:
            stdin, stdout, stderr = client.exec_command(
                INSTANCE_COLLECT_SCRIPT, timeout=15
            )
            output = stdout.read().decode("utf-8", errors="replace").strip()
            exit_code = stdout.channel.recv_exit_status()

            if exit_code == 0 and output:
                instances = json.loads(output)
                with self._lock:
                    self._instance_data[hostname] = instances
                _log(SERVICE, f"[realtime] Collected {len(instances)} instances "
                     f"from {hostname}", level="DEBUG")
            else:
                err = stderr.read().decode("utf-8", errors="replace").strip()
                _log(SERVICE, f"[realtime] Instance collection failed on "
                     f"{hostname}: exit={exit_code} err={err}", level="WARNING")

        except Exception as e:
            _log(SERVICE, f"[realtime] Instance collection error on "
                 f"{hostname}: {e}", level="ERROR")

    def collect_instances_all(self):
        """
        Collect instance data from all connected VPS in parallel.
        Called from PBMaster main loop every ~30s.
        Respects the interval to avoid over-polling.
        """
        now = time.time()
        if now - self._last_instance_collect < INSTANCE_COLLECT_INTERVAL:
            return
        self._last_instance_collect = now

        connected = self._pool.hostnames()
        # Only collect from hosts that have an active stream (= are connected)
        targets = [h for h in connected if self.is_stream_alive(h)]

        if not targets:
            return

        threads = []
        for hostname in targets:
            t = threading.Thread(
                target=self.collect_instances,
                args=(hostname,),
                daemon=True,
                name=f"inst-collect-{hostname}",
            )
            t.start()
            threads.append(t)

        # Wait max 15s for all threads
        for t in threads:
            t.join(timeout=15)

    # ── Read access (thread-safe, called from UI) ──

    def get_system(self, hostname: str) -> Optional[SystemMetrics]:
        """Get latest system metrics for a VPS (thread-safe)."""
        with self._lock:
            return self._system_data.get(hostname)

    def get_instances(self, hostname: str) -> list[dict]:
        """Get latest instance data for a VPS (thread-safe)."""
        with self._lock:
            return list(self._instance_data.get(hostname, []))

    def get_all_systems(self) -> dict[str, SystemMetrics]:
        """Get latest system metrics for all VPS (thread-safe)."""
        with self._lock:
            return dict(self._system_data)

    def get_all_instances(self) -> dict[str, list[dict]]:
        """Get latest instance data for all VPS (thread-safe)."""
        with self._lock:
            return {h: list(v) for h, v in self._instance_data.items()}

    def get_all(self) -> dict[str, dict]:
        """
        Get all data for all hosts — single call from UI.
        Returns: {hostname: {system: SystemMetrics|None, instances: [...],
                             stream_alive: bool, data_age_s: float}}
        """
        with self._lock:
            connected = self._pool.hostnames()
            now = time.time()
            result = {}
            for host in connected:
                metrics = self._system_data.get(host)
                age = (now - metrics.timestamp) if metrics else -1
                result[host] = {
                    "system": metrics,
                    "instances": list(self._instance_data.get(host, [])),
                    "stream_alive": (
                        host in self._streams
                        and self._streams[host].is_alive()
                        and self._streams[host].active
                    ),
                    "data_age_s": round(age, 1),
                }
            return result

    def get_stream_info(self) -> dict[str, dict]:
        """Get diagnostic info about all streams."""
        with self._lock:
            return {
                hostname: {
                    "alive": reader.is_alive(),
                    "active": reader.active,
                    "error": reader.error,
                }
                for hostname, reader in self._streams.items()
            }
