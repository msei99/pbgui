"""
Async VPS Monitor — system metrics, instances, services, alerts.

All coroutines run on the FastAPI event loop.  No threads, no paramiko.
Uses asyncssh via ``AsyncSSHPool`` for all SSH operations.
"""

from __future__ import annotations

import asyncio
import json
import time
import traceback
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

import asyncssh

from pbgui_purefunc import PBGDIR, load_ini, save_ini
from logging_helpers import human_log as _log
from ini_watcher import IniWatcher
from master.async_pool import AsyncSSHPool, ConnectionStatus
from master.async_store import VPSStore, SystemMetrics

SERVICE = "VPSMonitor"

# ── Constants ───────────────────────────────────────────────

LOOP_INTERVAL = 15          # seconds between main loop iterations
SERVICE_CHECK_EVERY = 4     # every N iterations (= 60s at 15s)
INSTANCE_COLLECT_INTERVAL = 30  # seconds
HOST_META_INTERVAL = 30     # seconds
PACKAGE_STATUS_INTERVAL = 3600  # seconds
MONITOR_CACHE_VERSION = 2

# ── Remote scripts (same as old realtime_collector) ─────────

MONITOR_AGENT_SCRIPT = r'''python3 -u -c "
import json, os, sys, time, threading, subprocess
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
def _ppid_watcher():
    while True:
        time.sleep(3)
        if os.getppid() == 1:
            os._exit(0)
t = threading.Thread(target=_ppid_watcher, daemon=True)
t.start()
_bots_cpu_prev = {}
_bots = {}
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
        bots = []
        try:
            now = time.time()
            out = subprocess.check_output(['ps', 'auxw'], text=True, timeout=2)
            for line in out.splitlines():
                if 'main.py' not in line or 'config_run.json' not in line:
                    continue
                parts = line.split()
                pid = int(parts[1])
                try:
                    with open(f'/proc/{pid}/stat') as sf:
                        sfp = sf.read().split()
                    ticks = int(sfp[13]) + int(sfp[14])
                except Exception:
                    continue
                prev = _bots_cpu_prev.get(pid)
                cpu_pct = 0.0
                if prev:
                    dt_sec = now - prev[1]
                    if dt_sec > 0:
                        cpu_pct = round((ticks - prev[0]) / (dt_sec * 100) * 100, 1)
                _bots_cpu_prev[pid] = (ticks, now)
                rss_mb = 0
                swap_mb = 0
                try:
                    with open(f'/proc/{pid}/status') as sf:
                        for sl in sf.read().splitlines():
                            if sl.startswith('VmRSS:'):
                                rss_mb = round(int(sl.split()[1]) / 1024, 1)
                            elif sl.startswith('VmSwap:'):
                                swap_mb = round(int(sl.split()[1]) / 1024, 1)
                except Exception:
                    pass
                name = _bots.get(pid, '')
                if not name:
                    for p in parts:
                        if p.endswith('/config_run.json'):
                            name = p.split('/')[-2]
                            _bots[pid] = name
                            break
                if name:
                    bots.append({'name': name, 'cpu': cpu_pct, 'rss_mb': rss_mb, 'swap_mb': swap_mb})
            alive = set()
            for pl in out.splitlines():
                if 'main.py' in pl and 'config_run.json' in pl:
                    ps = pl.split()
                    if len(ps) > 1:
                        alive.add(int(ps[1]))
            for dead in list(_bots_cpu_prev.keys()):
                if dead not in alive:
                    del _bots_cpu_prev[dead]
                    _bots.pop(dead, None)
        except Exception:
            pass
        print(json.dumps({'ts': time.time(), 'cpu': cpu, 'mem': mem, 'disk': [dtot, dused, dfree, dpct], 'swap': swap, 'bots': bots}), flush=True)
    except Exception:
        pass
    time.sleep(1)
"'''

INSTANCE_COLLECT_SCRIPT = r'''python3 -u -c "
import json, os, re, subprocess, time
from datetime import datetime

HOME = os.path.expanduser('~')
PBGDIR = os.path.join(HOME, 'software/pbgui')
PB7DIR = os.path.join(HOME, 'software/pb7')
TODAY = datetime.utcnow().strftime('%Y-%m-%d')
YESTERDAY = datetime.utcfromtimestamp(time.time() - 86400).strftime('%Y-%m-%d')

# PNL regex (matches PBRun patterns)
FILL_SUMMARY_RE = re.compile(r'\[fill\]\s+(\d+)\s+fills,\s+pnl=([+-]?(?:\d+\.?\d*|\d*\.\d+))\s+\w+')
FILL_PNL_RE = re.compile(r'\bpnl=([+-]?(?:\d+\.?\d*|\d*\.\d+))\b')

# shared helpers (used by both counting and dump mode)

def _process_pb7_line(line, mode, bc=None, lines_out=None, last_day=None):
    if ' ERROR ' in line:
        if mode == 'count' and bc is not None:
            if last_day == 'today': bc['et'] += 1
            elif last_day == 'yesterday': bc['ey'] += 1
        elif mode == 'dump' and lines_out is not None:
            lines_out.append(line.rstrip('\n'))
    if mode == 'count' and bc is not None:
        if '[fill]' not in line:
            return
        m = FILL_SUMMARY_RE.search(line)
        if m:
            c = int(m.group(1)); pnl = float(m.group(2))
            if last_day == 'today': bc['ct'] += c; bc['pt'] += pnl
            elif last_day == 'yesterday': bc['cy'] += c; bc['py'] += pnl
        else:
            m = FILL_PNL_RE.search(line)
            if m:
                pnl = float(m.group(1))
                if last_day == 'today': bc['ct'] += 1; bc['pt'] += pnl
                elif last_day == 'yesterday': bc['cy'] += 1; bc['py'] += pnl

def _read_pb7_tail(fp, offset, today_start, yesterday_start, bc):
    # Incrementally read one pb7 log file from offset to EOF.
    last_day = None
    try:
        size = os.path.getsize(fp)
        if offset > size:
            offset = 0
        with open(fp, 'r') as f:
            f.seek(offset)
            for line in f:
                mts = re.match(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)', line)
                if mts:
                    ts_str = mts.group(1).rstrip('Z')
                    try:
                        ts_val = int(datetime.strptime(ts_str, '%Y-%m-%dT%H:%M:%S').timestamp())
                        if ts_val >= today_start: last_day = 'today'
                        elif ts_val >= yesterday_start: last_day = 'yesterday'
                        else: last_day = None
                    except: pass
                _process_pb7_line(line, 'count', bc=bc, last_day=last_day)
            return f.tell()
    except Exception:
        pass
    return offset

def _read_err_tail(fp, offset, today_start, yesterday_start, bc):
    # Incrementally read one stderr traceback file from offset to EOF.
    last_day = None
    try:
        size = os.path.getsize(fp)
        if offset > size:
            offset = 0
        with open(fp, 'r') as f:
            f.seek(offset)
            for line in f:
                mts = re.match(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)', line)
                if mts:
                    ts_str = mts.group(1).rstrip('Z')
                    try:
                        ts_val = int(datetime.strptime(ts_str, '%Y-%m-%dT%H:%M:%S').timestamp())
                        if ts_val >= today_start: last_day = 'today'
                        elif ts_val >= yesterday_start: last_day = 'yesterday'
                        else: last_day = None
                    except: pass
                if 'Traceback' in line:
                    if last_day == 'today': bc['tt'] += 1
                    elif last_day == 'yesterday': bc['ty'] += 1
            return f.tell()
    except Exception:
        pass
    return offset

def _file_start_sig(fp):
    # Return a small signature of the current file start to detect truncate+rewrite.
    try:
        with open(fp, 'r') as f:
            return f.readline().rstrip('\n')[:200]
    except Exception:
        pass
    return ''

def _read_pb7_file(fp, mode, today_start, yesterday_start, bc=None, lines_out=None,
                   target_start=None, target_end=None):
    # Read one pb7 log file. Returns earliest_ts seen or None.
    last_day = None
    earliest = None
    in_target = False
    try:
        with open(fp, 'r') as f:
            for line in f:
                mts = re.match(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)', line)
                if mts:
                    ts_str = mts.group(1).rstrip('Z')
                    try:
                        ts_val = int(datetime.strptime(ts_str, '%Y-%m-%dT%H:%M:%S').timestamp())
                        if earliest is None or ts_val < earliest:
                            earliest = ts_val
                        if mode == 'dump' and ts_val >= (target_end or ts_val + 1):
                            break
                        if ts_val >= today_start: last_day = 'today'
                        elif ts_val >= yesterday_start: last_day = 'yesterday'
                        else: last_day = None
                        if mode == 'dump':
                            in_target = (last_day is not None) if target_start is None else (
                                ts_val >= target_start and (target_end is None or ts_val < target_end)
                            )
                    except: pass
                if mode == 'dump' and not in_target:
                    continue
                _process_pb7_line(line, mode, bc=bc, lines_out=lines_out, last_day=last_day)
    except Exception: pass
    return earliest

def _read_err_file(fp, mode, today_start, yesterday_start, bc=None):
    # Read one err_log file and count tracebacks.
    last_day = None
    try:
        with open(fp, 'r') as f:
            for line in f:
                mts = re.match(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)', line)
                if mts:
                    ts_str = mts.group(1).rstrip('Z')
                    try:
                        ts_val = int(datetime.strptime(ts_str, '%Y-%m-%dT%H:%M:%S').timestamp())
                        if ts_val >= today_start: last_day = 'today'
                        elif ts_val >= yesterday_start: last_day = 'yesterday'
                        else: last_day = None
                    except: pass
                if 'Traceback' in line and last_day:
                    if mode == 'count' and bc is not None:
                        if last_day == 'today': bc['tt'] += 1
                        else: bc['ty'] += 1
    except Exception: pass

# cache from master
EXPECTED_CACHE_VERSION = int(os.environ.get('PBGUI_CACHE_VERSION', '0') or 0)
cache_raw = os.environ.get('PBGUI_CACHE', '{}')
host_cache = {}
try:
    host_cache = json.loads(cache_raw)
except Exception:
    pass
if not isinstance(host_cache, dict):
    host_cache = {}
host_cache_version = int(host_cache.get('_version', 0) or 0) if isinstance(host_cache, dict) else 0
if host_cache_version != EXPECTED_CACHE_VERSION:
    host_cache = {}

# find running bots
running = {}
try:
    out = subprocess.check_output(['ps', 'aux'], text=True)
    for line in out.splitlines():
        if 'main.py' not in line or 'config_run.json' not in line:
            continue
        for part in line.split():
            if part.endswith('/config_run.json'):
                d = os.path.dirname(part)
                running[os.path.basename(d)] = d
                break
except Exception:
    pass

# ── dump mode: return matching log lines for bot-log popup ──
dump_mode = os.environ.get('PBGUI_DUMP')
if dump_mode:
    dump_bot = os.environ.get('PBGUI_DUMP_BOT', '')
    dump_kind = os.environ.get('PBGUI_DUMP_KIND', 'errors')
    dump_bucket = os.environ.get('PBGUI_DUMP_BUCKET', 'today')
    dump_lines = int(os.environ.get('PBGUI_DUMP_LINES', '5000'))

    # find cfg_dir for the requested bot
    cfg_dir = running.get(dump_bot)
    lines_out = []

    if cfg_dir:
        # file lists (same as counting loop)
        pb7_log = os.path.join(PB7DIR, 'logs', f'{dump_bot}.log')
        err_log = os.path.join(cfg_dir, 'passivbot_err.log')
        old_err = os.path.join(cfg_dir, 'passivbot_err.log.old')

        pb7_old_files = []
        try:
            import glob as _glob3
            log_real = os.path.realpath(pb7_log) if os.path.isfile(pb7_log) else ''
            for fp in sorted(
                _glob3.glob(os.path.join(PB7DIR, 'logs', '*' + dump_bot + '*.log')),
                key=os.path.getmtime, reverse=True
            ):
                if not os.path.isfile(fp): continue
                if os.path.islink(fp): continue
                if log_real and os.path.realpath(fp) == log_real: continue
                pb7_old_files.append(fp)
        except Exception: pass

        today_start = int(datetime.strptime(TODAY + 'T00:00:00', '%Y-%m-%dT%H:%M:%S').timestamp())
        yesterday_start = today_start - 86400
        if dump_bucket == 'today':
            target_start = today_start
            target_end = today_start + 86400
        elif dump_bucket == 'yesterday':
            target_start = yesterday_start
            target_end = today_start
        else:
            target_start = yesterday_start
            target_end = today_start + 86400

        if dump_kind == 'tracebacks':
            # read passivbot_err.log and its .old (same files as counting)
            err_files = [
                os.path.join(cfg_dir, 'passivbot_err.log.old'),
                os.path.join(cfg_dir, 'passivbot_err.log'),
            ]
            for fp in err_files:
                if not os.path.isfile(fp):
                    continue
                # group lines by wrapper timestamp into entries
                entry_lines = []
                last_ts = None

                def flush_tb_entry():
                    if entry_lines and any('Traceback' in l for l in entry_lines):
                        lines_out.extend(entry_lines)
                        if len(lines_out) > 0 and lines_out[-1] != '-----':
                            lines_out.extend(['', '-----', ''])

                try:
                    with open(fp, 'r') as f:
                        for line in f:
                            line = line.rstrip('\n')
                            mts = re.match(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)', line)
                            ts_val = None
                            if mts:
                                try:
                                    ts_val = int(datetime.strptime(mts.group(1).rstrip('Z'), '%Y-%m-%dT%H:%M:%S').timestamp())
                                except: pass
                            if ts_val is not None and ts_val != last_ts:
                                flush_tb_entry()
                                entry_lines = []
                                last_ts = ts_val
                            if ts_val is not None and ts_val >= target_start and ts_val < target_end:
                                entry_lines.append(line)
                            elif entry_lines and 'Traceback' not in line:
                                entry_lines.append(line)
                    flush_tb_entry()
                except Exception: pass
            # remove trailing separator
            while lines_out and lines_out[-1] == '-----':
                lines_out.pop()
                if lines_out and lines_out[-1] == '':
                    lines_out.pop()
        else:
            # errors: same file list + same read logic as counting
            for fp in pb7_old_files:
                if not os.path.isfile(fp):
                    continue
                earliest = _read_pb7_file(fp, 'dump', today_start, yesterday_start,
                                          lines_out=lines_out,
                                          target_start=target_start, target_end=target_end)
                if earliest is not None and earliest < yesterday_start:
                    break
            if os.path.isfile(pb7_log):
                _read_pb7_file(pb7_log, 'dump', today_start, yesterday_start,
                               lines_out=lines_out,
                               target_start=target_start, target_end=target_end)

        # trim to max lines
        if dump_lines > 0 and len(lines_out) > dump_lines:
            lines_out = lines_out[-dump_lines:]

    print(json.dumps({'lines': lines_out}))
    exit(0)

monitors = []
v7 = []
new_cache = {'_version': EXPECTED_CACHE_VERSION}

# collect old passivbot log files for sidebar selector
old_bot_logs = {}
try:
    log_dir = os.path.join(PB7DIR, 'logs')
    if os.path.isdir(log_dir):
        for f in sorted(os.listdir(log_dir)):
            if not f.endswith('.log'): continue
            # extract bot name: either name.log or 20260508_..._name_config_run.json.log
            if os.path.islink(os.path.join(log_dir, f)): continue
            # get the base name before _config_run
            base = f.rsplit('_config_run', 1)[0]
            # extract bot name from end of the full path
            parts = base.split('__')
            for p in parts:
                if p in running:
                    old_bot_logs.setdefault(p, []).append(f'pb7/logs/{f}')
                    break
except Exception: pass

for name, cfg_dir in sorted(running.items()):
    # config version + enabled_on
    version = 0; enabled_on = 'disabled'
    cf = os.path.join(cfg_dir, 'config.json')
    if os.path.isfile(cf):
        try:
            pbgui = json.load(open(cf)).get('pbgui', {})
            version = pbgui.get('version', 0)
            enabled_on = pbgui.get('enabled_on', 'disabled')
        except Exception: pass
    rv = 0
    rvf = os.path.join(cfg_dir, 'running_version.txt')
    if os.path.isfile(rvf):
        try: rv = int(open(rvf).read().strip())
        except Exception: pass
    v7.append({'name': name, 'running': True, 'cv': version, 'eo': enabled_on, 'rv': rv})

    # passivbot monitor dir (for start time)
    monitor_dir = None
    mroot = os.path.join(PB7DIR, 'monitor')
    if os.path.isdir(mroot):
        for ex in os.listdir(mroot):
            d = os.path.join(mroot, ex, name)
            if os.path.isdir(d) and os.path.isfile(os.path.join(d, 'state.latest.json')):
                monitor_dir = d; break

    # start time from state.latest.json (or default 0 if no monitor dir)
    start_ts = 0.0
    if monitor_dir:
        sf = os.path.join(monitor_dir, 'state.latest.json')
        if os.path.isfile(sf):
            try:
                meta = json.load(open(sf)).get('meta', {})
                start_ts = float(meta.get('bot_start_ts_ms', 0)) / 1000.0
            except Exception: pass

    # per-bot cache
    bc = dict(host_cache.get(name, {}))
    bc.setdefault('today', TODAY)
    bc.setdefault('et', 0); bc.setdefault('ey', 0)
    bc.setdefault('tt', 0); bc.setdefault('ty', 0)
    bc.setdefault('ct', 0); bc.setdefault('cy', 0)
    bc.setdefault('pt', 0.0); bc.setdefault('py', 0.0)
    bc.setdefault('log_off', 0)
    bc.setdefault('log_fp', '')
    bc.setdefault('log_sig', '')
    bc.setdefault('err_sig', '')

    # day change
    if bc['today'] != TODAY:
        bc['ey'] = bc['et']; bc['et'] = 0
        bc['ty'] = bc['tt']; bc['tt'] = 0
        bc['cy'] = bc['ct']; bc['ct'] = 0
        bc['py'] = bc['pt']; bc['pt'] = 0.0
        bc['today'] = TODAY

    # pb7 log (errors, PNL) — passivbot's own formatted output
    pb7_log = os.path.join(PB7DIR, 'logs', f'{name}.log')

    # collect old pb7 log files (non-symlink, newest-first by mtime)
    pb7_old_files = []
    try:
        import glob as _glob2
        log_real = os.path.realpath(pb7_log) if os.path.isfile(pb7_log) else ''
        for fp in sorted(
            _glob2.glob(os.path.join(PB7DIR, 'logs', '*' + name + '*.log')),
            key=os.path.getmtime, reverse=True
        ):
            if not os.path.isfile(fp):
                continue
            if os.path.islink(fp):
                continue
            if log_real and os.path.realpath(fp) == log_real:
                continue
            pb7_old_files.append(fp)
    except Exception:
        pass

    # stderr capture (traceback source, wrapper-timestamped)
    err_log = os.path.join(cfg_dir, 'passivbot_err.log')
    old_err = os.path.join(cfg_dir, 'passivbot_err.log.old')

    bc.setdefault('log_off', 0)
    bc.setdefault('err_off', 0)

    first_run = name not in host_cache
    today_start = int(datetime.strptime(TODAY + 'T00:00:00', '%Y-%m-%dT%H:%M:%S').timestamp())
    yesterday_start = today_start - 86400

    if first_run:
        # errors/PNL: read old files until yesterday covered, then current log
        for fp in pb7_old_files:
            if not os.path.isfile(fp):
                continue
            earliest = _read_pb7_file(fp, 'count', today_start, yesterday_start, bc=bc)
            if earliest is not None and earliest < yesterday_start:
                break
        if os.path.isfile(pb7_log):
            _read_pb7_file(pb7_log, 'count', today_start, yesterday_start, bc=bc)
        bc['log_off'] = os.path.getsize(pb7_log) if os.path.isfile(pb7_log) else 0
        bc['log_fp'] = os.path.realpath(pb7_log) if os.path.isfile(pb7_log) else ''
        bc['log_sig'] = _file_start_sig(pb7_log) if os.path.isfile(pb7_log) else ''
        # tracebacks: read err_log and its .old
        for fp in (old_err, err_log):
            if os.path.isfile(fp):
                _read_err_file(fp, 'count', today_start, yesterday_start, bc=bc)
        bc['err_off'] = os.path.getsize(err_log) if os.path.isfile(err_log) else 0
        bc['err_sig'] = _file_start_sig(err_log) if os.path.isfile(err_log) else ''
    else:
        # incremental read: pb7 log
        if os.path.isfile(pb7_log):
            try:
                current_log_fp = os.path.realpath(pb7_log)
                current_log_sig = _file_start_sig(pb7_log)
                prev_log_fp = bc.get('log_fp', '')
                prev_log_sig = bc.get('log_sig', '')
                offset = bc['log_off']
                size = os.path.getsize(pb7_log)
                rotated = bool(offset and prev_log_sig and current_log_sig and prev_log_sig != current_log_sig)
                if prev_log_fp and current_log_fp and prev_log_fp != current_log_fp:
                    if os.path.isfile(prev_log_fp):
                        _read_pb7_tail(prev_log_fp, offset, today_start, yesterday_start, bc)
                    offset = 0
                elif offset > size or rotated:
                    offset = 0
                bc['log_off'] = _read_pb7_tail(pb7_log, offset, today_start, yesterday_start, bc)
                bc['log_fp'] = current_log_fp
                bc['log_sig'] = current_log_sig
            except Exception: pass
        # incremental read: err_log
        if os.path.isfile(err_log):
            try:
                offset = bc['err_off']
                size = os.path.getsize(err_log)
                current_err_sig = _file_start_sig(err_log)
                prev_err_sig = bc.get('err_sig', '')
                rotated = bool(offset and prev_err_sig and current_err_sig and prev_err_sig != current_err_sig)
                if offset > size and os.path.isfile(old_err):
                    _read_err_tail(old_err, offset, today_start, yesterday_start, bc)
                    offset = 0
                elif rotated and os.path.isfile(old_err):
                    _read_err_tail(old_err, offset, today_start, yesterday_start, bc)
                    offset = 0
                bc['err_off'] = _read_err_tail(err_log, offset, today_start, yesterday_start, bc)
                bc['err_sig'] = current_err_sig
            except Exception: pass

    # build monitor dict
    monitors.append({
        'u': name, 'p': '7', 'v': version, 'st': start_ts,
        'm': [0]*10, 'c': 0.0,
        'i': '', 'it': 0, 'iy': 0, 'e': '', 't': '',
        'et': bc['et'], 'ey': bc['ey'],
        'tt': bc['tt'], 'ty': bc['ty'],
        'pt': bc['pt'], 'py': bc['py'],
        'ct': bc['ct'], 'cy': bc['cy'],
    })
    new_cache[name] = {
        'today': bc['today'],
        'et': bc['et'], 'ey': bc['ey'], 'tt': bc['tt'], 'ty': bc['ty'],
        'ct': bc['ct'], 'cy': bc['cy'], 'pt': bc['pt'], 'py': bc['py'],
        'log_off': bc['log_off'], 'err_off': bc['err_off'], 'log_fp': bc['log_fp'], 'log_sig': bc['log_sig'], 'err_sig': bc['err_sig'],
    }

print(json.dumps({'monitors': monitors, 'v7': v7, 'cache': new_cache,
    'bot_logs': old_bot_logs}))
"'''



HOST_META_SCRIPT = r'''python3 -u -c "
import configparser, hashlib, json, os, re, subprocess, sys
from pathlib import Path

HOME = os.path.expanduser('~')
PBGDIR = os.path.join(HOME, '__PBGDIR__')
INI_PATH = os.path.join(PBGDIR, 'pbgui.ini')


def run(cmd, timeout=10):
    try:
        res = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=timeout,
        )
        if res.returncode == 0:
            return (res.stdout or '').strip()
    except Exception:
        pass
    return ''


def read_pbgui_version(root):
    readme = Path(root) / 'README.md'
    if not readme.exists():
        return 'N/A'
    try:
        for line in readme.read_text(encoding='utf-8', errors='ignore').splitlines()[:20]:
            match = re.search(r'v[0-9.]+', line)
            if match:
                return match.group(0)
    except Exception:
        pass
    return 'N/A'


def read_pb7_version(pb7dir):
    if not pb7dir:
        return 'N/A'
    version_file = Path(pb7dir) / 'src' / 'passivbot_version.py'
    if not version_file.exists():
        return 'N/A'
    try:
        content = version_file.read_text(encoding='utf-8', errors='ignore')
        match = re.search(r'__version__\s*=\s*[\"\']([^\"\']+)[\"\']', content)
        if match:
            return 'v' + match.group(1)
    except Exception:
        pass
    return 'N/A'


def git_value(git_dir, args, default=''):
    if not git_dir or not Path(git_dir).exists():
        return default
    value = run(['git', '--git-dir', git_dir] + list(args), timeout=10)
    return value or default


def python_version(exe):
    if not exe or not Path(exe).exists():
        return ''
    return run([exe, '-c', 'import sys; print(f\"{sys.version_info.major}.{sys.version_info.minor}\")'], timeout=5)


cfg = configparser.ConfigParser()
try:
    cfg.read(INI_PATH)
except Exception:
    pass

role = cfg.get('main', 'role', fallback='slave')
pb7dir = cfg.get('main', 'pb7dir', fallback='')
pb7venv = cfg.get('main', 'pb7venv', fallback='')

result = {
    'role': role,
    'boot': 0,
    'api_md5': '',
    'reboot': os.path.exists('/var/run/reboot-required'),
    'pbgv': read_pbgui_version(PBGDIR),
    'pbgc': '',
    'pbgb': 'unknown',
    'pbgpy': 'N/A',
    'pb7v': read_pb7_version(pb7dir),
    'pb7c': '',
    'pb7b': 'unknown',
    'pb7py': 'N/A',
}

try:
    with open('/proc/stat', encoding='utf-8') as f:
        for line in f:
            if line.startswith('btime '):
                result['boot'] = int(line.split()[1])
                break
except Exception:
    pass

api_keys = Path(pb7dir) / 'api-keys.json' if pb7dir else None
if api_keys and api_keys.exists():
    try:
        result['api_md5'] = hashlib.md5(api_keys.read_bytes()).hexdigest()
    except Exception:
        pass

pbgui_git = str(Path(PBGDIR) / '.git')
result['pbgc'] = git_value(pbgui_git, ['log', '-n', '1', '--pretty=format:%H'])
result['pbgb'] = git_value(pbgui_git, ['rev-parse', '--abbrev-ref', 'HEAD'], 'unknown')

pb7_git = str(Path(pb7dir) / '.git') if pb7dir else ''
result['pb7c'] = git_value(pb7_git, ['log', '-n', '1', '--pretty=format:%H'])
result['pb7b'] = git_value(pb7_git, ['rev-parse', '--abbrev-ref', 'HEAD'], 'unknown')

for candidate in (
    str(Path(PBGDIR) / '.venv' / 'bin' / 'python'),
    str(Path(HOME) / 'software' / 'venv_pbgui' / 'bin' / 'python'),
):
    version = python_version(candidate)
    if version:
        result['pbgpy'] = version
        break
if result['pbgpy'] == 'N/A':
    result['pbgpy'] = f'{sys.version_info.major}.{sys.version_info.minor}'

pb7_python = python_version(pb7venv)
if pb7_python:
    result['pb7py'] = pb7_python

logs_dir = os.path.join(PBGDIR, 'data', 'logs')
available = []
if os.path.isdir(logs_dir):
    for f in sorted(os.listdir(logs_dir)):
        full = os.path.join(logs_dir, f)
        if os.path.isfile(full) and (f.endswith('.log') or f.endswith('.log.old')):
            available.append('data/logs/' + f)
result['available_logs'] = available

print(json.dumps(result))
"'''

PACKAGE_STATUS_SCRIPT = r'''python3 -u -c "
import json, os, re, subprocess

result = {
    'upgrades': 'N/A',
    'reboot': os.path.exists('/var/run/reboot-required'),
}
env = os.environ.copy()
env['LANG'] = 'C'
try:
    res = subprocess.run(
        ['apt-get', 'dist-upgrade', '-s'],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        timeout=20,
        env=env,
    )
    if res.returncode == 0:
        match = re.search(r'(\d+) upgraded', res.stdout or '')
        if match:
            result['upgrades'] = match.group(1)
except Exception:
    pass

print(json.dumps(result))
"'''


# ── Service definitions ─────────────────────────────────────

class ServiceStatus(Enum):
    RUNNING = "running"
    STOPPED = "stopped"
    UNKNOWN = "unknown"
    RESTARTING = "restarting"


@dataclass
class ServiceInfo:
    name: str
    pid_file: str       # relative to PBGUI dir
    script_file: str    # Python script to run
    process_match: str  # grep string in cmdline


MONITORED_SERVICES = {
    "PBRun": ServiceInfo("PBRun", "data/pid/pbrun.pid",
                         "PBRun.py", "pbrun.py"),
    "PBRemote": ServiceInfo("PBRemote", "data/pid/pbremote.pid",
                            "PBRemote.py", "pbremote.py"),
    "PBCoinData": ServiceInfo("PBCoinData", "data/pid/pbcoindata.pid",
                              "PBCoinData.py", "pbcoindata.py"),
}


# ── Main orchestrator ───────────────────────────────────────

class VPSMonitor:
    """
    Async VPS monitoring orchestrator.

    Lifecycle:
        monitor = VPSMonitor()
        await monitor.start()   # launches all background tasks
        ...
        await monitor.stop()    # cancels everything, disconnects
    """

    def __init__(self):
        self.pool = AsyncSSHPool()
        self.store = VPSStore()

        # Config
        self._auto_restart: Optional[bool] = None
        self._enabled_hosts: Optional[set[str]] = None

        # Telegram
        self._telegram_token = ""
        self._telegram_chat_id = ""

        # Alert dedup
        self._connection_alerts: set[str] = set()
        self._service_alerts: set[str] = set()
        self._connection_alerts_enabled_at: float = 0.0

        # Restart rate limiting
        self._restart_history: dict[str, dict[str, list[datetime]]] = {}
        self.max_restarts_per_hour = 3

        # Instance collection timing
        self._last_instance_collect: float = 0.0
        self._last_host_meta_collect: dict[str, float] = {}
        self._last_package_status_collect: dict[str, float] = {}

        # Monitor cache (persisted across restarts, per-host per-bot GZ state)
        self._cache_path = Path(PBGDIR) / 'data' / 'monitor_cache.json'
        self._monitor_cache: dict[str, dict[str, dict]] = {}

        # Debug logging
        self._debug_logging: Optional[bool] = None

        # ini watcher (thread-based, fine alongside asyncio)
        self._ini_watcher = IniWatcher()

        # Background tasks
        self._tasks: list[asyncio.Task] = []
        self._stream_tasks: dict[str, asyncio.Task] = {}
        self._running = False

    # ── Config ──────────────────────────────────────────────

    @property
    def auto_restart(self) -> bool:
        if self._auto_restart is None:
            val = load_ini("vps_monitor", "auto_restart")
            self._auto_restart = val.lower() == "true" if val else True
        return self._auto_restart

    @property
    def debug_logging(self) -> bool:
        if self._debug_logging is not None:
            return self._debug_logging
        val = load_ini("vps_monitor", "debug_logging")
        return val.lower() == "true" if val else False

    @debug_logging.setter
    def debug_logging(self, value: bool):
        self._debug_logging = bool(value)
        save_ini("vps_monitor", "debug_logging", "true" if value else "false")

    @property
    def enabled_hosts(self) -> set[str]:
        if self._enabled_hosts is None:
            val = load_ini("vps_monitor", "enabled_hosts")
            if val and val.strip():
                self._enabled_hosts = {
                    h.strip() for h in val.split(",") if h.strip()
                }
            else:
                self._enabled_hosts = set()
        return self._enabled_hosts

    @property
    def telegram_token(self):
        if not self._telegram_token:
            self._telegram_token = load_ini("main", "telegram_token") or ""
        return self._telegram_token

    @property
    def telegram_chat_id(self):
        if not self._telegram_chat_id:
            self._telegram_chat_id = load_ini("main", "telegram_chat_id") or ""
        return self._telegram_chat_id

    # ── Lifecycle ───────────────────────────────────────────

    async def start(self):
        """Initialize and start all monitoring tasks."""
        if self._running:
            return
        self._running = True
        _log(SERVICE, "Starting VPS monitor...")
        self._connection_alerts_enabled_at = time.time() + 90.0

        self.pool.load_vps_configs()
        self.store.load_ui_settings()
        self._ini_watcher.start()
        self._load_monitor_cache()

        enabled = self.enabled_hosts
        if not enabled:
            _log(SERVICE, "No VPS hosts enabled for monitoring. "
                 "Enable hosts in Services → API Server → Settings.")
        else:
            # Remove non-enabled hosts from pool
            for h in list(self.pool.hostnames()):
                if h not in enabled:
                    self.pool.remove_host(h)

            results = await self.pool.connect_enabled(enabled)
            connected = sum(1 for v in results.values() if v)
            _log(SERVICE, f"Connected to {connected}/{len(results)} VPS servers")

            # Start metric streams for connected hosts
            for hostname, success in results.items():
                if success:
                    self._start_metrics_stream(hostname)

        # Launch main loop as background task
        self._tasks.append(asyncio.create_task(
            self._main_loop(), name="vps-main-loop"
        ))

        _log(SERVICE, "VPS monitor started")

    async def stop(self):
        """Cancel all tasks and disconnect."""
        if not self._running:
            return
        self._running = False
        _log(SERVICE, "Stopping VPS monitor...")

        # Cancel stream tasks
        for task in self._stream_tasks.values():
            task.cancel()
        # Cancel main tasks
        for task in self._tasks:
            task.cancel()

        # Wait for cancellation
        all_tasks = list(self._tasks) + list(self._stream_tasks.values())
        if all_tasks:
            await asyncio.gather(*all_tasks, return_exceptions=True)

        self._tasks.clear()
        self._stream_tasks.clear()
        self._ini_watcher.stop()
        await self.pool.disconnect_all()
        _log(SERVICE, "VPS monitor stopped")

    # ── Main loop ───────────────────────────────────────────

    async def _main_loop(self):
        """Main monitoring loop — health checks, reconnects, services."""
        loop_count = 0
        while self._running:
            try:
                await self._loop_iteration(loop_count)
                loop_count += 1
            except asyncio.CancelledError:
                break
            except Exception as e:
                _log(SERVICE, f"Error in main loop: {e}", level="WARNING",
                     meta={'traceback': traceback.format_exc()})

            # Sleep but wake on ini change
            try:
                await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(
                        None, self._ini_watcher.changed.wait, LOOP_INTERVAL
                    ),
                    timeout=LOOP_INTERVAL + 1,
                )
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass

    async def _loop_iteration(self, loop_count: int):
        """Single iteration of the main loop."""
        # Config changes
        if self._ini_watcher.changed.is_set():
            self._ini_watcher.changed.clear()
            await self._apply_config_changes()

        enabled = self.enabled_hosts
        if not enabled:
            return

        # 1. Health check
        status = self.pool.health_check()
        enabled_status = {h: s for h, s in status.items() if h in enabled}
        self._handle_connection_changes(enabled_status)

        # 2. Reconnect lost
        reconnected = await self.pool.reconnect_lost(enabled)
        newly_reconnected: list[str] = []
        for hostname, success in reconnected.items():
            if success:
                _log(SERVICE, f"Reconnected to {hostname}")
                self._start_metrics_stream(hostname)
                self._last_host_meta_collect.pop(hostname, None)
                self._last_package_status_collect.pop(hostname, None)
                alert_key = f"conn:{hostname}"
                if alert_key in self._connection_alerts:
                    self._connection_alerts.discard(alert_key)
                    newly_reconnected.append(hostname)

        # Send reconnect alerts (batched if mass reconnect)
        if newly_reconnected and time.time() >= self._connection_alerts_enabled_at:
            if len(newly_reconnected) >= max(2, len(enabled) * 0.5):
                hosts_str = ", ".join(sorted(newly_reconnected))
                await self._send_alert(
                    f"✅ *VPSMonitor*: Network recovered — "
                    f"SSH reconnected to *{len(newly_reconnected)}* "
                    f"hosts ({hosts_str})"
                )
            else:
                for hostname in newly_reconnected:
                    await self._send_alert(
                        f"✅ *VPSMonitor*: SSH reconnected to "
                        f"*{hostname}*"
                    )

        # 3. Restart dead metric streams
        self._restart_dead_streams()

        # 4. Collect instances (every ~30s)
        await self._collect_instances_all()

        # 4b. Collect host metadata on the same SSH channel
        await self._collect_host_meta_all()

        # 5. Service monitoring (every N iterations)
        if loop_count % SERVICE_CHECK_EVERY == 0:
            connected = [
                h for h, s in enabled_status.items()
                if s == ConnectionStatus.CONNECTED
            ]
            if connected:
                results = await self._check_and_heal_services(connected)
                self.store.update_services(results)

    # ── Config reload ───────────────────────────────────────

    async def _apply_config_changes(self):
        """Re-read config and apply host enable/disable changes."""
        prev_enabled = self._enabled_hosts or set()
        self._enabled_hosts = None
        self._auto_restart = None
        self._debug_logging = None
        enabled = self.enabled_hosts

        newly_disabled = prev_enabled - enabled
        newly_enabled = enabled - prev_enabled

        if newly_disabled:
            _log(SERVICE, f"Hosts disabled: {', '.join(sorted(newly_disabled))}")
            for h in newly_disabled:
                self._stop_metrics_stream(h)
                await self.pool.disconnect(h)
                self.pool.remove_host(h)
                self.store.remove_host(h)

        if newly_enabled:
            _log(SERVICE, f"Hosts newly enabled: "
                 f"{', '.join(sorted(newly_enabled))}")
            self.pool.load_vps_configs()
            for h in list(self.pool.hostnames()):
                if h not in enabled:
                    self.pool.remove_host(h)
            for h in newly_enabled:
                if h in self.pool.hostnames():
                    if await self.pool.connect(h):
                        self._start_metrics_stream(h)

    # ── Metric streams ──────────────────────────────────────

    def _start_metrics_stream(self, hostname: str):
        """Launch an async task that reads system metrics from SSH."""
        self._stop_metrics_stream(hostname)
        task = asyncio.create_task(
            self._metrics_stream(hostname),
            name=f"metrics-{hostname}",
        )
        self._stream_tasks[hostname] = task

    def _stop_metrics_stream(self, hostname: str):
        """Cancel the metrics stream task for a host."""
        task = self._stream_tasks.pop(hostname, None)
        if task and not task.done():
            task.cancel()

    def _restart_dead_streams(self):
        """Restart metric streams that have ended."""
        for hostname in list(self._stream_tasks):
            task = self._stream_tasks[hostname]
            if task.done():
                if hostname in self.pool.connected_hosts():
                    _log(SERVICE, f"Restarting dead metrics stream for "
                         f"{hostname}")
                    self._start_metrics_stream(hostname)
                else:
                    self._stream_tasks.pop(hostname, None)

    async def _metrics_stream(self, hostname: str):
        """Read system metrics from SSH stdout (JSON per line, 1/s)."""
        proc = None
        try:
            proc = await self.pool.start_process(hostname, MONITOR_AGENT_SCRIPT)
            if not proc:
                _log(SERVICE, f"[metrics] Cannot start stream for {hostname}",
                     level="WARNING")
                return

            self.store.update_stream_info(hostname, {
                "alive": True, "active": True, "error": None, "last_update": 0,
            })

            async for line in proc.stdout:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    metrics = SystemMetrics.from_json(data)
                    self.store.update_system(hostname, metrics)
                    bots = data.get("bots")
                    if bots:
                        self.store.update_instances_live(hostname, bots)
                    self.store.update_stream_info(hostname, {
                        "alive": True,
                        "active": True,
                        "error": None,
                        "last_update": metrics.timestamp,
                    })
                except json.JSONDecodeError:
                    continue

        except asyncio.CancelledError:
            pass
        except Exception as e:
            _log(SERVICE, f"[metrics] Stream error for {hostname}: {e}",
                 level="WARNING")
            self.store.update_stream_info(hostname, {
                "alive": False, "active": False, "error": str(e),
            })
        finally:
            if proc is not None:
                try:
                    proc.close()
                except Exception:
                    pass
            self.store.update_stream_info(hostname, {
                "alive": False, "active": False, "error": None,
            })
            _log(SERVICE, f"[metrics] Stream ended for {hostname}")

    # ── Instance collection ─────────────────────────────────

    async def collect_instances_now(self, hostname: str):
        """Public: immediately collect instances from a single VPS.

        Unlike _collect_instances_all() this bypasses the interval gate
        so callers (e.g. V7ConfigSyncWorker) can trigger a refresh right
        after an activation signal.
        """
        entry = self.pool.get_connection(hostname)
        if not entry:
            _log(SERVICE, f"[instances] collect_instances_now: "
                 f"{hostname} not connected", level="WARNING")
            return
        try:
            await self._collect_instances(hostname)
            _log(SERVICE, f"[instances] Immediate collect for {hostname}",
                 level="DEBUG")
        except Exception as e:
            _log(SERVICE, f"[instances] Immediate collect error on "
                 f"{hostname}: {e}", level="WARNING")

    async def _collect_instances_all(self):
        """Collect bot instance data from all connected VPS."""
        now = time.time()
        if now - self._last_instance_collect < INSTANCE_COLLECT_INTERVAL:
            return
        self._last_instance_collect = now

        connected = self.pool.connected_hosts()
        targets = [
            h for h in connected
            if h in self._stream_tasks and not self._stream_tasks[h].done()
        ]
        if not targets:
            return

        results = await asyncio.gather(
            *(self._collect_instances(h) for h in targets),
            return_exceptions=True,
        )
        for hostname, result in zip(targets, results):
            if isinstance(result, Exception):
                _log(SERVICE, f"[instances] Error on {hostname}: {result}",
                     level="WARNING")

    async def _collect_instances(self, hostname: str):
        """Collect bot instances from a single VPS."""
        host_cache = self._monitor_cache.get(hostname, {})
        if not isinstance(host_cache, dict):
            host_cache = {}
        host_cache = dict(host_cache)
        host_cache['_version'] = MONITOR_CACHE_VERSION
        cache_json = json.dumps(host_cache)
        cmd = f"PBGUI_CACHE_VERSION={MONITOR_CACHE_VERSION} PBGUI_CACHE='{cache_json}' {INSTANCE_COLLECT_SCRIPT}"
        result = await self.pool.run(hostname, cmd, timeout=30)
        if result and result.exit_status == 0 and result.stdout:
            try:
                parsed = json.loads(result.stdout.strip())
                if isinstance(parsed, dict):
                    monitors = parsed.get('monitors', [])
                    v7_list = parsed.get('v7', [])
                    new_host_cache = parsed.get('cache', {})
                    bot_logs = parsed.get('bot_logs', {})
                    if isinstance(monitors, list) and isinstance(v7_list, list):
                        self.store.update_instances(hostname, monitors)
                        self.store.update_v7_instances(hostname, v7_list)
                        self.store.update_bot_logs(hostname, bot_logs if isinstance(bot_logs, dict) else {})
                        if isinstance(new_host_cache, dict):
                            self._monitor_cache[hostname] = new_host_cache
                            self._save_monitor_cache()
                        if self.debug_logging:
                            _log(SERVICE, f"[instances] Collected "
                                 f"{len(monitors)} monitors, "
                                 f"{len(v7_list)} v7 instances from "
                                 f"{hostname}", level="DEBUG")
                        return
                # fallback: old format
                if isinstance(parsed, list) and len(parsed) == 2:
                    self.store.update_instances(hostname, parsed[0])
                    self.store.update_v7_instances(hostname, parsed[1])
                else:
                    self.store.update_instances(hostname, parsed)
            except json.JSONDecodeError:
                pass

    def _load_monitor_cache(self) -> None:
        try:
            if self._cache_path.exists():
                loaded = json.loads(self._cache_path.read_text())
                if not isinstance(loaded, dict):
                    self._monitor_cache = {}
                    return
                cleaned: dict[str, dict[str, dict]] = {}
                for hostname, host_cache in loaded.items():
                    if not isinstance(host_cache, dict):
                        continue
                    cache_version = int(host_cache.get('_version', 0) or 0)
                    if cache_version != MONITOR_CACHE_VERSION:
                        continue
                    cleaned[hostname] = host_cache
                self._monitor_cache = cleaned
        except Exception:
            self._monitor_cache = {}

    def _save_monitor_cache(self) -> None:
        try:
            tmp = self._cache_path.with_suffix('.json.tmp')
            tmp.parent.mkdir(parents=True, exist_ok=True)
            tmp.write_text(json.dumps(self._monitor_cache))
            tmp.replace(self._cache_path)
        except Exception:
            pass

    async def collect_host_meta_now(self, hostname: str,
                                    *, include_package_status: bool = False):
        """Public: immediately collect host metadata from a single VPS."""
        entry = self.pool.get_connection(hostname)
        if not entry:
            _log(SERVICE, f"[host-meta] collect_host_meta_now: "
                 f"{hostname} not connected", level="WARNING")
            return
        try:
            await self._collect_host_meta(hostname,
                                          include_package_status=include_package_status,
                                          force=True)
            _log(SERVICE, f"[host-meta] Immediate collect for {hostname}",
                 level="DEBUG")
        except Exception as e:
            _log(SERVICE, f"[host-meta] Immediate collect error on "
                 f"{hostname}: {e}", level="WARNING")

    async def _collect_host_meta_all(self):
        """Collect host metadata from all connected VPS via the shared SSH pool."""
        now = time.time()
        connected = self.pool.connected_hosts()
        targets = [
            h for h in connected
            if h in self._stream_tasks and not self._stream_tasks[h].done()
        ]
        if not targets:
            return

        scheduled: list[tuple[str, bool]] = []
        for hostname in targets:
            needs_host_meta = now - self._last_host_meta_collect.get(hostname, 0.0) >= HOST_META_INTERVAL
            needs_package_status = now - self._last_package_status_collect.get(hostname, 0.0) >= PACKAGE_STATUS_INTERVAL
            if needs_host_meta or needs_package_status:
                scheduled.append((hostname, needs_package_status))

        if not scheduled:
            return

        results = await asyncio.gather(
            *(
                self._collect_host_meta(hostname, include_package_status=include_package_status)
                for hostname, include_package_status in scheduled
            ),
            return_exceptions=True,
        )
        for (hostname, include_package_status), result in zip(scheduled, results):
            if isinstance(result, Exception):
                label = "Package status" if include_package_status else "host-meta"
                _log(SERVICE, f"[{label}] Error on {hostname}: {result}",
                     level="WARNING")

    async def _collect_host_meta(self, hostname: str,
                                 *, include_package_status: bool = False,
                                 force: bool = False):
        """Collect SSH-derived host metadata for a single VPS."""
        now = time.time()
        collect_host_meta = force or (
            now - self._last_host_meta_collect.get(hostname, 0.0) >= HOST_META_INTERVAL
        )
        collect_package_status = include_package_status and (
            force or now - self._last_package_status_collect.get(hostname, 0.0) >= PACKAGE_STATUS_INTERVAL
        )

        if not collect_host_meta and not collect_package_status:
            return

        if collect_host_meta:
            pbgui_dir = self.pool.get_remote_pbgui_dir(hostname)
            script = HOST_META_SCRIPT.replace('__PBGDIR__', pbgui_dir)
            result = await self.pool.run(hostname, script, timeout=20)
            if result and result.exit_status == 0 and result.stdout:
                try:
                    parsed = json.loads(result.stdout.strip())
                    if isinstance(parsed, dict):
                        self.store.update_host_meta(hostname, parsed)
                        self._last_host_meta_collect[hostname] = now
                        if self.debug_logging:
                            _log(SERVICE, f"[host-meta] Collected metadata for {hostname}",
                                 level="DEBUG")
                except json.JSONDecodeError:
                    pass

        if collect_package_status:
            package_result = await self.pool.run(hostname, PACKAGE_STATUS_SCRIPT,
                                                 timeout=30)
            if package_result and package_result.exit_status == 0 and package_result.stdout:
                try:
                    package_data = json.loads(package_result.stdout.strip())
                    if isinstance(package_data, dict):
                        self.store.update_host_meta(hostname, package_data)
                        self._last_package_status_collect[hostname] = now
                except json.JSONDecodeError:
                    pass

    # ── Service monitoring ──────────────────────────────────

    async def _check_service(self, hostname: str, svc: ServiceInfo
                             ) -> dict:
        """Check if a service is running on a VPS."""
        result = None
        for base_dir in self.pool.get_remote_pbgui_dirs(hostname):
            pid_path = f"{base_dir}/{svc.pid_file}"
            result = await self.pool.run(hostname, f'cat {pid_path}', timeout=10)
            pid_str = (result.stdout or "").strip() if result else ""
            if pid_str.isdigit():
                break
        if result is None:
            return {
                "status": ServiceStatus.UNKNOWN.value,
                "pid": None,
                "error": "SSH connection error",
                "was_restarted": False,
            }
        pid_str = (result.stdout or "").strip()
        if not pid_str.isdigit():
            return {
                "status": ServiceStatus.STOPPED.value,
                "pid": None,
                "error": "No PID file or invalid PID",
                "was_restarted": False,
            }
        pid = int(pid_str)

        # Step 2: Check if process is running
        check = await self.pool.run(
            hostname,
            f'ps -p {pid} -o cmd= 2>/dev/null | grep -qi '
            f'"{svc.process_match}" && echo "yes" || echo "no"',
            timeout=10,
        )
        if check is None:
            return {
                "status": ServiceStatus.UNKNOWN.value,
                "pid": pid,
                "error": "SSH error during process check",
                "was_restarted": False,
            }
        running = (check.stdout or "").strip() == "yes"
        return {
            "status": (ServiceStatus.RUNNING.value if running
                       else ServiceStatus.STOPPED.value),
            "pid": pid if running else None,
            "error": (None if running
                      else f"PID {pid} not running"),
            "was_restarted": False,
        }

    async def _restart_service(self, hostname: str,
                               service_name: str) -> bool:
        """Restart a service on a VPS (same logic as old ServiceMonitor)."""
        svc = MONITORED_SERVICES.get(service_name)
        if not svc:
            return False

        if not self._can_restart(hostname, service_name):
            _log(SERVICE, f"[service] Restart limit reached for "
                 f"{service_name} on {hostname}", level="WARNING")
            return False

        _log(SERVICE, f"[service] Restarting {service_name} on {hostname}")

        start_cmd = ""
        for base_dir in self.pool.get_remote_pbgui_dirs(hostname):
            venv_check = await self.pool.run(
                hostname,
                f'test -d ~/{base_dir} || exit 1; '
                f'test -f ~/software/venv_pbgui/bin/activate && echo "venv_pbgui" '
                f'|| (test -f ~/{base_dir}/.venv/bin/activate '
                f'&& echo "dotvenv" || echo "system")',
                timeout=5,
            )
            if not venv_check or venv_check.exit_status != 0:
                continue
            venv_type = (venv_check.stdout or "").strip() if venv_check else "system"
            if venv_type == "venv_pbgui":
                start_cmd = (
                    f"cd ~/{base_dir} && "
                    f"source ~/software/venv_pbgui/bin/activate && "
                    f"nohup python -u starter.py -r {service_name} "
                    f"> /dev/null 2>&1 &"
                )
            elif venv_type == "dotvenv":
                start_cmd = (
                    f"cd ~/{base_dir} && "
                    f"source ~/{base_dir}/.venv/bin/activate && "
                    f"nohup python -u starter.py -r {service_name} "
                    f"> /dev/null 2>&1 &"
                )
            else:
                start_cmd = (
                    f"cd ~/{base_dir} && "
                    f"nohup python3 -u starter.py -r {service_name} "
                    f"> /dev/null 2>&1 &"
                )
            break
        if not start_cmd:
            return False

        result = await self.pool.run(hostname, start_cmd, timeout=15)
        if result and result.exit_status == 0:
            self._record_restart(hostname, service_name)
            _log(SERVICE, f"[service] {service_name} restart sent to "
                 f"{hostname}")
            return True
        _log(SERVICE, f"[service] Failed to restart {service_name} on "
             f"{hostname}", level="ERROR")
        return False

    async def _check_and_heal_services(self, hostnames: list[str]) -> dict:
        """Check + auto-heal all services on given hosts."""
        all_results: dict[str, dict] = {}
        for hostname in hostnames:
            host_svc: dict[str, dict] = {}
            for svc_name, svc_info in MONITORED_SERVICES.items():
                check = await self._check_service(hostname, svc_info)

                status_val = check["status"]
                alert_key = f"svc:{hostname}:{svc_name}"

                if status_val == ServiceStatus.STOPPED.value and self.auto_restart:
                    _log(SERVICE, f"[service] {svc_name} down on {hostname}, "
                         "attempting restart")
                    restarted = await self._restart_service(hostname, svc_name)
                    check["was_restarted"] = restarted
                    if restarted:
                        check["status"] = ServiceStatus.RESTARTING.value

                # Alerts
                if status_val == ServiceStatus.STOPPED.value:
                    if alert_key not in self._service_alerts:
                        self._service_alerts.add(alert_key)
                        if check.get("was_restarted"):
                            await self._send_alert(
                                f"🔄 *VPSMonitor*: {svc_name} was down on "
                                f"*{hostname}*, restart initiated"
                            )
                        else:
                            await self._send_alert(
                                f"❌ *VPSMonitor*: {svc_name} is down on "
                                f"*{hostname}*"
                            )
                elif status_val == ServiceStatus.RUNNING.value:
                    if alert_key in self._service_alerts:
                        self._service_alerts.discard(alert_key)
                        await self._send_alert(
                            f"✅ *VPSMonitor*: {svc_name} is running on "
                            f"*{hostname}*"
                        )

                host_svc[svc_name] = check
            all_results[hostname] = host_svc
        return all_results

    # ── Restart rate limiting ───────────────────────────────

    def _can_restart(self, hostname: str, service_name: str) -> bool:
        history = self._restart_history.get(hostname, {}).get(
            service_name, []
        )
        now = datetime.now()
        history = [ts for ts in history if (now - ts).total_seconds() < 3600]
        self._restart_history.setdefault(hostname, {})[service_name] = history
        return len(history) < self.max_restarts_per_hour

    def _record_restart(self, hostname: str, service_name: str):
        self._restart_history.setdefault(hostname, {}).setdefault(
            service_name, []
        ).append(datetime.now())

    # ── Connection alerts ───────────────────────────────────

    def _handle_connection_changes(self, status: dict[str, ConnectionStatus]):
        newly_disconnected: list[str] = []
        for hostname, conn_status in status.items():
            alert_key = f"conn:{hostname}"
            if conn_status == ConnectionStatus.DISCONNECTED:
                if alert_key not in self._connection_alerts:
                    self._connection_alerts.add(alert_key)
                    newly_disconnected.append(hostname)
            elif conn_status == ConnectionStatus.CONNECTED:
                self._connection_alerts.discard(alert_key)

        if not newly_disconnected:
            return

        if time.time() < self._connection_alerts_enabled_at:
            if self.debug_logging:
                _log(
                    SERVICE,
                    f"[conn] suppressing startup disconnect alerts for {len(newly_disconnected)} host(s)",
                    level="DEBUG",
                )
            return

        total_hosts = len(status)
        # Mass disconnect: ≥50% of monitored hosts lost simultaneously
        if len(newly_disconnected) >= max(2, total_hosts * 0.5):
            hosts_str = ", ".join(sorted(newly_disconnected))
            asyncio.create_task(self._send_alert(
                f"⚠️ *VPSMonitor*: Network blip — SSH lost to "
                f"*{len(newly_disconnected)}* hosts ({hosts_str})"
            ))
        else:
            for hostname in newly_disconnected:
                asyncio.create_task(self._send_alert(
                    f"⚠️ *VPSMonitor*: SSH connection lost to "
                    f"*{hostname}*"
                ))

    async def _send_alert(self, message: str):
        """Send Telegram alert."""
        if not self.telegram_token or not self.telegram_chat_id:
            _log(SERVICE, f"[alert] No Telegram config: {message}",
                 level="WARNING")
            return
        try:
            from telegram import Bot
            bot = Bot(token=self.telegram_token)
            async with bot:
                await bot.send_message(
                    chat_id=self.telegram_chat_id,
                    text=message,
                    parse_mode='Markdown',
                )
            _log(SERVICE, f"[alert] Sent: {message}")
        except Exception as e:
            _log(SERVICE, f"[alert] Failed: {e}", level="ERROR")

    # ── Kill instance (called by WebSocket command) ─────────

    async def kill_instance(self, hostname: str, name: str,
                            pb_version: str = "") -> dict:
        """Kill a bot instance on a VPS."""
        grep_pattern = f"main.py.*{name}"

        kill_cmd = (
            f"pid=$(ps aux | grep -E '{grep_pattern}' | grep -v grep "
            f"| awk '{{print $2}}' | head -1) && "
            f'[ -n "$pid" ] && kill $pid && echo "killed:$pid" '
            f'|| echo "not_found"'
        )

        result = await self.pool.run(hostname, kill_cmd, timeout=15)
        success = (result and result.exit_status == 0
                   and "killed:" in (result.stdout or ""))
        killed_pid = ""
        if success:
            killed_pid = result.stdout.split("killed:")[1].strip()

        _log(SERVICE,
             f"[cmd] Kill instance {name} on {hostname}: "
             f"{'OK pid=' + killed_pid if success else 'not found'}",
             level="INFO" if success else "WARNING")

        return {
            "success": success,
            "pid": killed_pid,
        }
