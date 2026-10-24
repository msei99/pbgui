"""Lease-elected legacy PBCoinData recovery inside existing persistent owners."""

from __future__ import annotations

import json
from pathlib import Path

from logging_helpers import human_log as _log
from pbcoindata_lifecycle import ACTIVE, IDLE, lifecycle_decision
import pbcoindata_process as processes
from pbgui_purefunc import load_ini_snapshot
from process_identity import ProcessIdentity
from service_lifecycle_lock import ServiceLifecycleBusyError

SERVICE = "PBCoinData"


def enabled(config):
    """Use the existing automatic service-healing setting."""
    return config.get("vps_monitor", "auto_restart", fallback="true").strip().lower() in {"1", "true", "yes", "on"}


def controller_metadata(root):
    """Require both an OS-held election lock and exact live owner identity."""
    lease = None
    try:
        lease = processes.FileLease(root, "controller", "probe")
        return {"controller": "none", "recovery_ready": False, "recovery_reason": "No verified persistent recovery controller"}
    except BlockingIOError:
        try:
            path = Path(root).resolve() / "data/locks/pbcoindata-controller.lock"
            data = json.loads(path.read_text())
            owner = data["owner"]
            script = {"pbrun": "PBRun.py", "monitor-agent": "monitor_agent.py"}.get(owner)
            if script and data.get("protocol") == processes.guard_protocol.PROTOCOL:
                state = processes.inspect_pid(root, int(data["pid"]), ProcessIdentity(int(data["pid"]), float(data["create_time"])), script=script)
                if state["status"] == "running":
                    return {"controller": owner, "recovery_ready": True, "recovery_reason": f"Verified {owner} controller lease"}
        except Exception as exc:
            _log(SERVICE, f"Recovery owner unavailable: {type(exc).__name__}", level="WARNING")
        return {"controller": "unknown", "recovery_ready": False, "recovery_reason": "Recovery controller identity is unconfirmed"}
    except Exception as exc:
        _log(SERVICE, f"Recovery lease unavailable: {type(exc).__name__}", level="WARNING")
        return {"controller": "unknown", "recovery_ready": False, "recovery_reason": "Recovery lease unavailable"}
    finally:
        if lease is not None:
            lease.release()


class LegacyController:
    """PBRun retains election ownership; the agent acquires only one-cycle fallback."""

    def __init__(self, root, owner="pbrun"):
        self.root = Path(root).resolve()
        self.owner = owner
        self.lease = None

    def close(self):
        """Release election on shutdown, disabled control or manager/role change."""
        if self.lease is not None:
            self.lease.release()
            self.lease = None
        processes.reap_children()

    def reconcile(self, credential_ready, *, config=None):
        """Recheck manager, demand and exact PID under lease then ordered action locks."""
        try:
            cfg = config if config is not None else load_ini_snapshot(self.root / "pbgui.ini").parser
            if cfg.get("main", "role", fallback="").strip().lower() != "slave" or not enabled(cfg):
                self.close()
                return None
            manager = processes.manager_state(self.root)
            if manager["manager"] != "legacy":
                self.close()
                return None
            if self.lease is None:
                try:
                    self.lease = processes.FileLease(self.root, "controller", self.owner)
                except BlockingIOError:
                    return None
            recovery = controller_metadata(self.root)
            if not recovery["recovery_ready"] or recovery["controller"] != self.owner:
                self.close()
                return None
            with processes.actions(self.root, "auto-reconcile"):
                # Do not reuse pre-lock config, manager, consumer or PID snapshots.
                cfg = load_ini_snapshot(self.root / "pbgui.ini").parser
                if (cfg.get("main", "role", fallback="").strip().lower() != "slave"
                        or not enabled(cfg) or processes.manager_state(self.root)["manager"] != "legacy"):
                    self.close()
                    return None
                try:
                    ready = credential_ready() if callable(credential_ready) else credential_ready
                except Exception as exc:
                    _log(SERVICE, f"Credential readiness unavailable: {type(exc).__name__}", level="WARNING")
                    ready = None
                decision = lifecycle_decision(self.root, ready, config=cfg)
                state = processes.snapshot(self.root)
                decision.update(recovery)
                if decision["demand"] == IDLE and state["status"] == "running":
                    preflight = processes.unload_preflight(self.root, decision, state, manager="legacy", recovery=recovery)
                    if preflight["allowed"]:
                        processes.stop(self.root)
                        state = processes.snapshot(self.root)
                    else:
                        decision["reason"] += "; " + preflight["reason"]
                        state["lifecycle_error"] = preflight["reason"]
                elif decision["demand"] == ACTIVE and decision["expected"] is True and state["status"] == "stopped":
                    try:
                        processes.start(self.root, automatic=True)
                    except processes.RetryDeferred as exc:
                        decision["reason"] += "; " + str(exc)
                        state["lifecycle_error"] = str(exc)
                    except processes.ProcessStateError as exc:
                        decision["reason"] += "; " + str(exc)
                        state["lifecycle_error"] = str(exc)
                    else:
                        state = processes.snapshot(self.root)
                return decision, state
        except ServiceLifecycleBusyError:
            return None
        except Exception as exc:
            self.close()
            _log(SERVICE, f"Legacy reconciliation unconfirmed: {type(exc).__name__}", level="WARNING")
            return None


def agent_fallback(root, credential_ready, *, config=None):
    """Use the same election once; a live PBRun PID alone never blocks fallback."""
    controller = LegacyController(root, "monitor-agent")
    try:
        return controller.reconcile(credential_ready, config=config)
    finally:
        controller.close()
