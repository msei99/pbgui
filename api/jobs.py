"""
Job Queue API endpoints.

Provides REST API for job management (list, cancel, delete, retry).
"""

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional

from task_queue import (
    list_jobs,
    request_cancel_job,
    delete_job,
    retry_failed_job,
    requeue_done_job,
    read_worker_pid,
    is_pid_running,
)
from api.auth import require_auth, SessionToken, get_token_from_request

router = APIRouter()


class CancelJobRequest(BaseModel):
    """Request payload for canceling a job."""
    job_id: str
    reason: Optional[str] = "user cancel"


@router.get("/")
def get_jobs(
    states: str = "pending,running",
    limit: int = 50,
    session: SessionToken = Depends(require_auth)
):
    """List jobs by state.
    
    Args:
        states: Comma-separated list of states (pending,running,done,failed)
        limit: Maximum number of jobs to return
        session: Authenticated session (auto-injected)
        
    Returns:
        {"jobs": [...], "worker_running": bool}
    """
    state_list = [s.strip() for s in states.split(",") if s.strip()]
    jobs = list_jobs(states=state_list, limit=limit)
    
    # Include worker status
    worker_pid = read_worker_pid()
    worker_running = bool(worker_pid and is_pid_running(int(worker_pid)))
    
    return {
        "jobs": jobs,
        "worker_running": worker_running,
        "worker_pid": worker_pid
    }


@router.get("/{job_id}")
def get_job(job_id: str, session: SessionToken = Depends(require_auth)):
    """Get a single job by ID.
    
    Returns:
        Job dict or 404 if not found
    """
    jobs = list_jobs(states=["pending", "running", "done", "failed"], limit=1000)
    for j in jobs:
        if str(j.get("id", "")).strip() == str(job_id).strip():
            return j
    raise HTTPException(status_code=404, detail=f"Job {job_id} not found")


@router.post("/cancel")
def cancel_job(req: CancelJobRequest, session: SessionToken = Depends(require_auth)):
    """Request job cancellation.
    
    This is cooperative: worker checks the flag between chunks.
    """
    ok = request_cancel_job(req.job_id, reason=req.reason)
    if not ok:
        raise HTTPException(status_code=404, detail="Job not found or not cancelable")
    return {"success": True, "job_id": req.job_id}


@router.delete("/{job_id}")
def delete_job_endpoint(job_id: str, states: Optional[str] = None, session: SessionToken = Depends(require_auth)):
    """Delete a job.
    
    Args:
        job_id: Job ID to delete
        states: Optional comma-separated list of states to search (default: pending,done,failed)
    """
    search_states = None
    if states:
        search_states = [s.strip() for s in states.split(",") if s.strip()]
    
    ok = delete_job(job_id, states=search_states)
    if not ok:
        raise HTTPException(status_code=404, detail="Job not found or not deletable")
    return {"success": True, "job_id": job_id}


@router.post("/{job_id}/retry")
def retry_job(job_id: str, session: SessionToken = Depends(require_auth)):
    """Retry a failed job (moves it back to pending)."""
    ok = retry_failed_job(job_id)
    if not ok:
        raise HTTPException(
            status_code=404,
            detail="Job not found or not in failed state"
        )
    return {"success": True, "job_id": job_id}


@router.post("/{job_id}/requeue")
def requeue_job(job_id: str, session: SessionToken = Depends(require_auth)):
    """Create a new pending job with the same payload as a done job."""
    ok = requeue_done_job(job_id)
    if not ok:
        raise HTTPException(
            status_code=404,
            detail="Job not found or not in done state"
        )
    return {"success": True, "job_id": job_id}


@router.post("/bulk-delete")
def bulk_delete_jobs(request: dict, session: SessionToken = Depends(require_auth)):
    """Delete multiple jobs by IDs or all jobs in a state.
    
    Request body:
        {
            "job_ids": ["id1", "id2", ...],  // optional: specific job IDs to delete
            "state": "done",  // optional: state to delete from (used with delete_all)
            "delete_all": true,  // optional: delete all jobs in the state
            "exchange": "hyperliquid"  // optional: exchange filter (jobs without exchange field are also deleted)
        }
    """
    from task_queue import delete_jobs_by_ids, list_jobs
    
    job_ids = request.get("job_ids", [])
    state = request.get("state")
    delete_all = request.get("delete_all", False)
    exchange_filter = (request.get("exchange") or "").strip().lower()
    
    if delete_all and state:
        # Delete all jobs in the specified state (with optional exchange filter)
        all_jobs = list_jobs(states=[state], limit=1000)
        
        # Apply exchange filter if provided
        if exchange_filter:
            filtered_jobs = []
            for j in all_jobs:
                job_exchange = (j.get("exchange") or "").strip().lower()
                job_type = (j.get("type") or "").strip().lower()
                
                # Match explicit exchange field
                if job_exchange == exchange_filter:
                    filtered_jobs.append(j)
                # Fallback: derive from job_type for legacy jobs
                elif not job_exchange:
                    if exchange_filter == "hyperliquid" and (job_type.startswith("hl_") or "hyperliquid" in job_type):
                        filtered_jobs.append(j)
                    elif exchange_filter == "binanceusdm" and (job_type.startswith("binance_") or "binance" in job_type):
                        filtered_jobs.append(j)
                    elif exchange_filter == "bybit" and (job_type.startswith("bybit_") or "bybit" in job_type):
                        filtered_jobs.append(j)
            all_jobs = filtered_jobs
        
        job_ids = [j.get("id") for j in all_jobs if j.get("id")]
    
    if not job_ids:
        raise HTTPException(status_code=400, detail="No job IDs provided or no matching jobs found")
    
    states_list = [state] if state else None
    deleted_count = delete_jobs_by_ids(job_ids, states=states_list)
    
    return {
        "success": True,
        "deleted": deleted_count,
        "total": len(job_ids)
    }


@router.get("/{job_id}/log")
def get_job_log(job_id: str, lines: int = 500, session: SessionToken = Depends(require_auth)):
    """Get log file content for a job.
    
    Args:
        job_id: Job ID
        lines: Number of lines to return (default: 500)
    
    Returns:
        {"log": ["line1", "line2", ...], "exists": bool}
    """
    from pathlib import Path
    from pbgui_purefunc import PBGDIR
    
    log_file = Path(PBGDIR) / "data" / "logs" / "jobs" / f"{job_id}.log"
    
    if not log_file.exists():
        return {"log": [], "exists": False}
    
    try:
        # Read last N lines efficiently
        with open(log_file, 'r', encoding='utf-8', errors='replace') as f:
            all_lines = f.readlines()
            log_lines = all_lines[-lines:] if lines > 0 else all_lines
            log_lines = [line.rstrip('\n\r') for line in log_lines]
        
        return {"log": log_lines, "exists": True}
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read log: {str(e)}"
        )
