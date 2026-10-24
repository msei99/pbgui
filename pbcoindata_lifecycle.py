"""Conservative, local PBCoinData demand shared by daemon and controllers."""

from __future__ import annotations

from pathlib import Path
from dataclasses import dataclass
import json
import time

import psutil

from logging_helpers import human_log as _log
from pb7_config import load_pb7_config
from pbgui_purefunc import load_ini_snapshot

SERVICE = "PBCoinData"
POLICY_VERSION = 1
ACTIVE = "ACTIVE"
IDLE = "IDLE"
UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class ConsumerSnapshot:
    """Secret-free demand and exchange coverage from one local process scan."""

    demand: str
    complete: bool
    exchanges: frozenset[str]


def consumer_snapshot(root: Path, pb7_dir: str = "", *, process_iter=None, config_loader=None) -> ConsumerSnapshot:
    """Inspect every consumer; only complete coverage may restrict exchanges.

    PB8's live command does not consume the DynamicIgnore daemon. PB7 candidates
    must match the configured script and an exact local runtime config path.
    """
    try:
        root = Path(root).resolve()
        run_root = (root / "data" / "run_v7").resolve()
        if not run_root.is_relative_to(root):
            _log(SERVICE, "Consumer runtime directory is outside this checkout", level="WARNING")
            return ConsumerSnapshot(UNKNOWN, False, frozenset())
    except Exception as exc:
        _log(SERVICE, f"Consumer runtime path unavailable: {type(exc).__name__}", level="WARNING")
        return ConsumerSnapshot(UNKNOWN, False, frozenset())
    loader = config_loader or load_pb7_config
    unknown = False
    active = False
    exchanges = set()
    accounts = None
    accounts_attempted = False

    def resolved(argument, cwd):
        """Resolve argv paths without suffix matching or shell interpretation."""
        if not isinstance(argument, str) or any(ord(c) < 32 for c in argument):
            raise ValueError("Invalid process path")
        path = Path(argument)
        return (path if path.is_absolute() else cwd / path).resolve()

    try:
        processes = list((process_iter or psutil.process_iter)())
        script = resolved(str(Path(pb7_dir) / "src" / "main.py"), root) if pb7_dir else None
        for process in processes:
            try:
                argv = process.cmdline()
                if not argv:
                    continue
                main_args = [arg for arg in argv if isinstance(arg, str) and Path(arg).name == "main.py"]
                if not main_args:
                    continue
                cwd = Path(process.cwd()) if any(not Path(arg).is_absolute() for arg in main_args + [a for a in argv if str(a).endswith("config_run.json")]) else root
                if script is None:
                    unknown = True
                    continue
                if not any(resolved(arg, cwd) == script for arg in main_args):
                    continue
                configs = [resolved(arg, cwd) for arg in argv if isinstance(arg, str) and Path(arg).name == "config_run.json"]
                if not configs:
                    unknown = True
                    continue
                lexical_local = any(
                    str((Path(arg) if Path(arg).is_absolute() else cwd / arg).absolute()).startswith(str(run_root) + "/")
                    for arg in argv if isinstance(arg, str) and Path(arg).name == "config_run.json"
                )
                candidates = [path for path in configs if path.parent.parent == run_root]
                if not candidates:
                    if lexical_local:
                        unknown = True
                    # Other PBGui checkouts and standalone PB7 bots are not consumers.
                    continue
                if len(candidates) != 1:
                    unknown = True
                    continue
                runtime = candidates[0]
                instance = runtime.parent
                if instance.name in {".", ".."} or "\\" in instance.name or any(ord(c) < 32 for c in instance.name):
                    unknown = True
                    continue
                values = []
                users = []
                for path in (instance / "config.json", runtime):
                    try:
                        if not path.is_file() or path.resolve().parent != instance:
                            raise ValueError("Missing or external consumer configuration")
                        payload = loader(path)
                        metadata = payload.get("pbgui", {})
                        value = metadata.get("dynamic_ignore", False)
                        if type(value) is not bool:
                            raise ValueError("Invalid dynamic_ignore metadata")
                        values.append(value)
                        live = payload.get("live", {})
                        users.append(live.get("user") if isinstance(live, dict) else None)
                    except Exception as exc:
                        unknown = True
                        _log(SERVICE, f"Consumer configuration unavailable: {type(exc).__name__}", level="WARNING")
                if len(values) != 2 or len(set(values)) != 1:
                    unknown = True
                if any(values):
                    active = True
                    if not accounts_attempted:
                        accounts_attempted = True
                        try:
                            account_path = script.parent.parent / "api-keys.json"
                            if not account_path.is_file():
                                raise ValueError("Missing local account projection")
                            # Keep only the allowlisted exchange field; never return or log keys.
                            payload = json.loads(account_path.read_text(encoding="utf-8"))
                            accounts = {name: entry.get("exchange") for name, entry in payload.items()
                                        if isinstance(entry, dict)}
                        except Exception as exc:
                            unknown = True
                            _log(SERVICE, f"Consumer account mapping unavailable: {type(exc).__name__}", level="WARNING")
                    if (len(users) != 2 or not all(isinstance(user, str) and user for user in users)
                            or users[0] != users[1] or accounts is None):
                        unknown = True
                        continue
                    exchange = accounts.get(users[0])
                    if not isinstance(exchange, str) or not exchange or not exchange.isascii() or not exchange.isalpha():
                        unknown = True
                        continue
                    exchanges.add(exchange)
            except (psutil.NoSuchProcess, psutil.ZombieProcess):
                continue
            except Exception as exc:
                unknown = True
                _log(SERVICE, f"Consumer process unavailable: {type(exc).__name__}", level="WARNING")
    except Exception as exc:
        unknown = True
        _log(SERVICE, f"Consumer snapshot unavailable: {type(exc).__name__}", level="WARNING")
    return ConsumerSnapshot(ACTIVE if active else (UNKNOWN if unknown else IDLE), not unknown, frozenset(exchanges))


def consumer_demand(root: Path, pb7_dir: str = "", *, process_iter=None, config_loader=None) -> str:
    """Preserve the ACTIVE/IDLE/UNKNOWN contract for lifecycle controllers."""
    return consumer_snapshot(root, pb7_dir, process_iter=process_iter, config_loader=config_loader).demand


def lifecycle_decision(root: Path, credential_active: bool | None, *, config=None, **scan_options) -> dict:
    """Keep role, demand and credential readiness separate and versioned."""
    try:
        cfg = config if config is not None else load_ini_snapshot(Path(root) / "pbgui.ini").parser
        role = cfg.get("main", "role", fallback="").strip().lower()
        pb7_dir = cfg.get("main", "pb7dir", fallback="").strip()
    except Exception as exc:
        _log(SERVICE, f"Lifecycle configuration unavailable: {type(exc).__name__}", level="WARNING")
        role, pb7_dir = "", ""
    demand = consumer_demand(root, pb7_dir, **scan_options) if role == "slave" else ACTIVE
    expected = credential_active if type(credential_active) is bool else None
    reason = "CMC credential readiness"
    if role == "slave":
        if demand == IDLE:
            expected, reason = False, "No running Dynamic Ignore consumers on this slave"
        elif demand == UNKNOWN:
            expected, reason = None, "Dynamic Ignore consumer status unknown; observing only"
        else:
            reason = "Running Dynamic Ignore consumer; CMC credential readiness required"
    return {
        "policy_version": POLICY_VERSION,
        "generated_at": time.time(),
        "role": role,
        "demand": demand,
        "expected": expected,
        "reason": reason,
    }


def metadata_expected(metadata: dict, *, now: float | None = None) -> bool | None:
    """Read fresh lifecycle metadata without collapsing UNKNOWN into False."""
    if not isinstance(metadata, dict) or metadata.get("policy_version") != POLICY_VERSION:
        return None
    timestamp = metadata.get("generated_at")
    if type(timestamp) not in (int, float) or not 0 <= (time.time() if now is None else now) - timestamp <= 120:
        return None
    value = metadata.get("expected")
    return value if type(value) is bool else None
