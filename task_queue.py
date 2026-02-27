from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from market_data import get_market_data_root_dir


def _atomic_write_json(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


def get_tasks_root_dir() -> Path:
    return get_market_data_root_dir() / "_tasks"


def get_task_state_dir(state: str) -> Path:
    return get_tasks_root_dir() / str(state).strip().lower()


def ensure_task_dirs() -> None:
    for s in ("pending", "running", "done", "failed"):
        get_task_state_dir(s).mkdir(parents=True, exist_ok=True)


@dataclass
class EnqueueResult:
    job_id: str
    path: str


def enqueue_job(*, job_type: str, payload: dict[str, Any]) -> EnqueueResult:
    ensure_task_dirs()
    jid = f"{int(time.time())}-{uuid4().hex[:10]}"
    job = {
        "id": jid,
        "type": str(job_type).strip(),
        "created_ts": int(time.time()),
        "updated_ts": int(time.time()),
        "payload": payload or {},
        "status": "pending",
        "progress": {},
        "error": "",
    }
    path = get_task_state_dir("pending") / f"{jid}.json"
    _atomic_write_json(path, job)
    return EnqueueResult(job_id=jid, path=str(path))


def list_jobs(*, states: list[str] | None = None, limit: int = 50) -> list[dict[str, Any]]:
    ensure_task_dirs()
    if not states:
        states = ["pending", "running", "done", "failed"]
    out: list[dict[str, Any]] = []
    for s in states:
        d = get_task_state_dir(s)
        if not d.is_dir():
            continue
        for p in sorted(d.glob("*.json"), key=lambda x: x.name, reverse=True):
            try:
                obj = json.loads(p.read_text(encoding="utf-8"))
                if isinstance(obj, dict):
                    obj["_path"] = str(p)
                    out.append(obj)
            except Exception:
                continue
            if limit and len(out) >= int(limit):
                return out
    return out


def _iter_job_paths(states: list[str]) -> list[Path]:
    ensure_task_dirs()
    out: list[Path] = []
    for s in states:
        d = get_task_state_dir(s)
        if not d.is_dir():
            continue
        out.extend(sorted(d.glob("*.json"), key=lambda p: p.name, reverse=True))
    return out


def request_cancel_job(job_id: str, *, reason: str = "cancel requested") -> bool:
    """Mark a job for cancellation.

    Cooperative: the worker checks the flag between chunks.
    Returns True if a matching job file was found and updated.
    """

    jid = str(job_id or "").strip()
    if not jid:
        return False

    for p in _iter_job_paths(["pending", "running"]):
        if p.stem != jid:
            continue

        def mut(o: dict[str, Any]) -> None:
            o["cancel_requested"] = True
            if str(o.get("status") or "").strip().lower() in {"pending", "running"}:
                o["status"] = "cancelling"
            pr = o.get("progress")
            pr = pr if isinstance(pr, dict) else {}
            pr["cancel_reason"] = str(reason or "cancel requested")
            o["progress"] = pr

        update_job_file(p, mutate=mut)
        return True
    return False


def force_fail_job(job_id: str, *, error: str = "cancelled") -> bool:
    """Immediately mark job failed and move it to failed/.

    Use when you want an immediate UI effect and/or after killing the worker.
    Returns True if a matching job file was found and moved.
    """

    jid = str(job_id or "").strip()
    if not jid:
        return False

    for p in _iter_job_paths(["pending", "running"]):
        if p.stem != jid:
            continue

        update_job_file(
            p,
            mutate=lambda o: o.update(
                {
                    "status": "failed",
                    "error": str(error or "cancelled"),
                    "cancel_requested": True,
                }
            ),
        )
        try:
            move_job_file(p, "failed")
        except Exception:
            return False
        return True
    return False


def retry_failed_job(job_id: str) -> bool:
    """Move a failed job back to pending for retry.

    Returns True if a failed job with the given id was found and moved.
    """

    jid = str(job_id or "").strip()
    if not jid:
        return False

    for p in _iter_job_paths(["failed"]):
        if p.stem != jid:
            continue

        def mut(o: dict[str, Any]) -> None:
            o["status"] = "pending"
            o["error"] = ""
            o["cancel_requested"] = False
            o["progress"] = {}

        update_job_file(p, mutate=mut)
        try:
            move_job_file(p, "pending")
        except Exception:
            return False
        return True
    return False


def delete_job(job_id: str, *, states: list[str] | None = None) -> bool:
    """Delete a job file from selected states.

    By default, only non-running states are searched.
    Returns True if the job file was found and removed.
    """

    jid = str(job_id or "").strip()
    if not jid:
        return False

    search_states = states or ["pending", "done", "failed"]
    for p in _iter_job_paths(search_states):
        if p.stem != jid:
            continue
        try:
            p.unlink(missing_ok=True)  # type: ignore[arg-type]
            return True
        except Exception:
            try:
                if p.exists():
                    p.unlink()
                    return True
            except Exception:
                return False
    return False


def delete_jobs_by_ids(job_ids: list[str], *, states: list[str] | None = None) -> int:
    """Delete multiple jobs and return number of successfully deleted files."""

    ids = [str(x).strip() for x in (job_ids or []) if str(x).strip()]
    if not ids:
        return 0

    deleted = 0
    for jid in ids:
        if delete_job(jid, states=states):
            deleted += 1
    return deleted


def move_job_file(src: Path, dst_state: str) -> Path:
    ensure_task_dirs()
    dst = get_task_state_dir(dst_state) / src.name
    os.replace(src, dst)
    return dst


def update_job_file(path: Path, *, mutate: callable) -> None:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(obj, dict):
            return
    except Exception:
        return

    try:
        mutate(obj)
    except Exception:
        return

    obj["updated_ts"] = int(time.time())
    _atomic_write_json(path, obj)


def get_worker_pid_path() -> Path:
    return get_tasks_root_dir() / "worker.pid"


def is_pid_running(pid: int) -> bool:
    if pid <= 1:
        return False
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def read_worker_pid() -> int | None:
    p = get_worker_pid_path()
    if not p.exists():
        return None
    try:
        return int(p.read_text(encoding="utf-8").strip())
    except Exception:
        return None


def write_worker_pid(pid: int) -> None:
    get_worker_pid_path().parent.mkdir(parents=True, exist_ok=True)
    get_worker_pid_path().write_text(str(int(pid)), encoding="utf-8")


def clear_worker_pid() -> None:
    try:
        get_worker_pid_path().unlink(missing_ok=True)  # type: ignore[arg-type]
    except Exception:
        try:
            if get_worker_pid_path().exists():
                get_worker_pid_path().unlink()
        except Exception:
            pass
