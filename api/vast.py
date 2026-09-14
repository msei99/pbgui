"""Authenticated Vast account, balance and GPU marketplace routes."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel, ConfigDict, Field

from api.auth import SessionToken, require_auth
from api.page_templates import render_page_urls
from logging_helpers import human_log as _log
from vast_credentials import VastCredentialStore
from vast_provider import REFERRAL_URL, VastClient, VastError, gpu_name_matches
from vast_jobs import JobStore, config_name, digest, services_available, job_id
from vast_queue import CloudQueue, can_remove_job
from file_lock import advisory_file_lock

SERVICE = "Vast"
router = APIRouter()
CLOUD_LOG_ROOT = Path(__file__).resolve().parents[1] / "data/logs/optimizes_v8"


class ValidateConfigRequest(BaseModel):
    """Accept an unsaved config for pure validation without runtime writes."""
    model_config = ConfigDict(extra="forbid")
    config: dict


@router.post("/validate-config")
def validate_config(body: ValidateConfigRequest, response: Response, session: SessionToken = Depends(require_auth)) -> dict:
    """Expose the same pinned-profile checks used before job preparation."""
    from vast_jobs import IMAGE, REVISION
    from vast_config_validation import METRICS, validate_cloud_config
    response.headers['Cache-Control'] = 'no-store'
    errors = validate_cloud_config(body.config, image=IMAGE, revision=REVISION)
    return {'valid': not errors, 'errors': errors, 'metrics': sorted(METRICS),
            'image': IMAGE, 'revision': REVISION}


class PrepareJobRequest(BaseModel):
    """A fresh job snapshot, with an explicit optional objective adjustment."""
    model_config = ConfigDict(extra="forbid")
    config_name: str = Field(min_length=1, max_length=160)
    iterations: int = Field(default=512, ge=256, le=10_000_000)
    workers: int = Field(default=4, ge=1, le=64)
    use_adg: bool = False


class GpuPreferences(BaseModel):
    """Provider-independent requirements saved for future queue rentals."""
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    gpu_name: str = Field(default="", max_length=80, pattern=r"^[^\x00-\x1f\x7f]*$")
    max_price: float = Field(default=.5, gt=0, le=100)
    min_vram: float = Field(default=12, ge=0, le=256)
    min_ram: float = Field(default=16, ge=0, le=4096)
    min_cpu: float = Field(default=4, ge=0, le=512)
    disk_gb: int = Field(default=40, ge=40, le=2000)
    verified_only: bool = True


class RentalPreferences(GpuPreferences):
    """Persist the shared rental limits alongside marketplace requirements."""
    hours: float = Field(default=1, ge=.25, le=24)
    budget: float = Field(default=1, ge=.1, le=100)
    idle_seconds: Literal[0, 300] = 300
    convergence_enabled: bool = False
    convergence_min_exact: int = Field(default=512, ge=256, le=10_000_000)
    convergence_patience: int = Field(default=512, ge=128, le=10_000_000)
    convergence_tolerance_pct: float = Field(default=0.1, gt=0, le=10)


class StartJobRequest(BaseModel):
    """Bind rental consent to a requirement snapshot and bounded spending policy."""
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    preferences: GpuPreferences | None = None
    use_saved_settings: bool = False
    rent_only: bool = False
    offer_id: int | None = Field(default=None, gt=0)
    hours: float = Field(default=1, ge=.25, le=24)
    budget: float = Field(default=1, ge=.1, le=100)
    accept_rental_and_cleanup: bool = False
    idle_seconds: int = Field(default=300, ge=0, le=300)


@router.get("/gpu-preferences")
def gpu_preferences(response: Response, session: SessionToken = Depends(require_auth)) -> dict:
    """Return durable GPU requirements without consulting the marketplace."""
    response.headers["Cache-Control"] = "no-store"
    try:
        return RentalPreferences.model_validate(CloudQueue().read().get("gpu_preferences", {})).model_dump()
    except VastError as exc:
        raise _error(exc) from None
    except ValueError:
        _log(SERVICE, "Invalid stored GPU preferences", level="ERROR")
        raise HTTPException(status_code=422, detail="Save valid GPU preferences in Settings.") from None


@router.post("/gpu-preferences")
def save_gpu_preferences(body: RentalPreferences, session: SessionToken = Depends(require_auth)) -> dict:
    """Save requirements atomically under the existing cross-process queue lock."""
    try:
        values = body.model_dump()
        values['gpu_name'] = values['gpu_name'].strip()
        CloudQueue().update(gpu_preferences=values)
        return values
    except VastError as exc:
        raise _error(exc) from None


def _error(exc: VastError) -> HTTPException:
    """Log only safe domain errors before displaying them in the UI."""
    _log(SERVICE, str(exc), level="WARNING")
    return HTTPException(status_code=exc.status, detail=str(exc),
                         headers={"X-PBGui-Error-Source": "vast"})


@router.get("/main_page")
def main_page(request: Request, session: SessionToken = Depends(require_auth)):
    """Keep old bookmarks pointing at the integrated optimizer queue."""
    return RedirectResponse(str(request.base_url).rstrip('/') + '/api/optimize-v8/main_page?view=queue', status_code=307)


@router.get("/fragment", response_class=HTMLResponse)
def fragment(session: SessionToken = Depends(require_auth)):
    """Serve a local authenticated component, not a standalone system page."""
    path = Path(__file__).resolve().parents[1] / 'frontend/vast.html'
    return HTMLResponse(path.read_text(), headers={'Cache-Control': 'no-store'})


@router.get("/settings")
def settings(response: Response, session: SessionToken = Depends(require_auth)) -> dict:
    """Expose flags and the explicit account-registration referral link."""
    response.headers["Cache-Control"] = "no-store"
    try:
        return {**VastCredentialStore().metadata(), "referral_url": REFERRAL_URL}
    except VastError as exc:
        raise _error(exc) from None


@router.post("/credentials")
async def save_credentials(request: Request, response: Response,
                           session: SessionToken = Depends(require_auth)) -> dict:
    """Accept secrets only in authenticated request bodies."""
    response.headers["Cache-Control"] = "no-store"
    try:
        raw = bytearray()
        async for chunk in request.stream():
            raw.extend(chunk)
            if len(raw) > 16384:
                raise VastError("Credential request is too large", 413)
        try:
            body = json.loads(raw)
        except (ValueError, UnicodeError):
            raise VastError("Invalid credential request", 422) from None
        if not isinstance(body, dict) or not body or set(body) - {"api_key", "registry_token"}:
            raise VastError("Invalid credential fields", 422)
        if any(not isinstance(value, str) for value in body.values()):
            raise VastError("Credentials must be strings", 422)
        return VastCredentialStore().save(
            api_key=body.get("api_key"), registry_token=body.get("registry_token"),
        )
    except VastError as exc:
        raise _error(exc) from None


@router.post("/account")
def account(response: Response, session: SessionToken = Depends(require_auth)) -> dict:
    """Refresh the current USD balance through the saved account key."""
    response.headers["Cache-Control"] = "no-store"
    try:
        return VastClient(VastCredentialStore().secrets()["api_key"]).account()
    except VastError as exc:
        raise _error(exc) from None


@router.get("/offers")
def offers(response: Response, max_price: float = Query(1, gt=0, le=100),
           min_vram: float = Query(12, ge=0, le=256), min_ram: float = Query(16, ge=0, le=4096),
           min_cpu: float = Query(4, ge=0, le=512), disk_gb: int = Query(40, ge=10, le=2000),
           verified_only: bool = True, gpu_name: str = Query("", max_length=80),
           include_incompatible: bool = False, rental_hours: float = Query(1, ge=.25, le=24),
           session: SessionToken = Depends(require_auth)) -> dict:
    """List live offers without renting or changing any provider resources."""
    response.headers["Cache-Control"] = "no-store"
    try:
        rows = VastClient(VastCredentialStore().secrets()["api_key"]).offers(
            max_price=max_price, min_vram=min_vram, min_ram=min_ram,
            min_cpu=min_cpu, disk_gb=disk_gb, verified_only=verified_only, gpu_name=gpu_name,
            min_cuda=0 if include_incompatible else 13,
            min_duration=0 if include_incompatible else rental_hours * 3600,
        )
        return {"offers": rows, "limit": 100}
    except VastError as exc:
        raise _error(exc) from None


def _rental_details(store, identifier):
    """Project only public offer metadata from the job's own immutable rental."""
    if not identifier:
        return None
    identifier = job_id(identifier)
    intent = store.read(identifier, 'intent.json')
    state_path = store.directory(identifier) / 'state.json'
    state = store.read(identifier) if state_path.is_file() else {}
    if state.get('deadline_confirmed'):
        from vast_deadline import effective_intent
        intent = effective_intent(store, identifier, intent)
    fields = ('gpu_name', 'vram_gb', 'ram_gb', 'cpu_cores', 'cpu_name',
              'gpu_mem_bw_gbps', 'pci_gen', 'gpu_lanes', 'pcie_bw_gbps',
              'disk_name', 'disk_bw_mbps', 'disk_gb', 'price_hour_usd',
              'download_gb_usd', 'upload_gb_usd', 'inet_down_mbps', 'inet_up_mbps',
              'location', 'verified', 'reliability')
    offer = intent.get('offer') or {}
    return {'id': identifier, 'deadline_protocol': state.get('deadline_protocol', 0),
            'deadline_pending': bool(state.get('deadline_request')), 'deadline_error': state.get('deadline_error'),
            'offer': {key: offer.get(key) for key in fields},
            'deadline': intent.get('deadline'), 'budget_usd': intent.get('budget_usd')}


def _run_cost_estimate(row, rental, now):
    """Estimate this job's running-container time and observed transfer charges."""
    offer = rental['offer']
    started = row.get('setup_started_at')
    if started is None:
        return None
    end = min(now, rental.get('deadline') or now)
    if row.get('status') in ('failed', 'cancelled', 'completed'):
        end = min(end, row.get('updated_at') or end)
    compute = max(0, end - started) / 3600 * (offer.get('price_hour_usd') or 0)
    incoming = row.get('transfer_input_bytes', 0) if row.get('uploaded') else (row.get('upload_progress') or {}).get('bytes', 0)
    transfer = incoming / 1e9 * (offer.get('download_gb_usd') or 0) + row.get('downloaded_bytes', 0) / 1e9 * (offer.get('upload_gb_usd') or 0)
    return {'compute_usd': compute, 'transfer_usd': transfer, 'total_usd': compute + transfer}


@router.get("/jobs")
def jobs(response: Response, session: SessionToken = Depends(require_auth)) -> dict:
    """List durable job state without starting any process or rental."""
    response.headers["Cache-Control"] = "no-store"
    try:
        queue = CloudQueue()
        worker = queue.worker()
        if worker:
            provider_log = CLOUD_LOG_ROOT / ('vast_' + job_id(worker['id']) + '_provider.log')
            worker = dict(worker, has_provider_log=provider_log.is_file() and not provider_log.is_symlink())
        rows = []
        rentals = {}
        stored = queue.store.list()
        replacements = {}
        for candidate in stored:  # Store order is newest first; keep the latest attempt.
            if candidate.get('requeue_from') and not candidate.get('deleted_at'):
                replacements.setdefault(candidate['requeue_from'], candidate)
        for row in stored:
            if row.get('kind') == 'worker' or row.get('deleted_at'):
                continue
            if row.get('status') == 'preparing' and row.get('preparation_owner'):
                row = queue.store.recover_interrupted_preparation(row['id'])
            replacement = replacements.get(row['id'])
            if replacement and replacement.get('status') != 'failed':
                continue
            if row.get('requeue_from') and row.get('status') == 'failed':
                latest = replacements.get(row['requeue_from'])
                if latest and latest['id'] != row['id'] and latest.get('status') != 'failed':
                    continue
                original = next((item for item in stored if item['id'] == row['requeue_from']), None)
                if original and not original.get('deleted_at'):
                    continue
            log = CLOUD_LOG_ROOT / ('vast_' + job_id(row['id']) + '.log')
            provider_log = CLOUD_LOG_ROOT / ('vast_' + row['id'] + '_provider.log')
            row = dict(row, exchange=', '.join(queue.store.exchanges(row)), has_log=log.is_file() and not log.is_symlink(), can_delete=can_remove_job(row, worker))
            control_exists = (queue.store.directory(row['id']) / 'control.json').exists()
            row['stop_requested'] = bool(queue.store.read(row['id'], 'control.json').get('stop')) if control_exists else False
            row['has_provider_log'] = provider_log.is_file() and not provider_log.is_symlink()
            if row['has_provider_log'] and row.get('status') == 'provisioning':
                from vast_provisioning_log import image_layer_progress
                try:
                    with provider_log.open('rb') as stream:
                        text = stream.read(1024 * 1024).decode('utf-8', errors='replace')
                    row['image_progress'] = image_layer_progress(text, row.get('image_progress'))
                except OSError:
                    _log(SERVICE, 'Provisioning progress snapshot unavailable', level='WARNING')
            lease = row.get('lease_id')
            if lease:
                if lease not in rentals:
                    rentals[lease] = _rental_details(queue.store, lease)
                row['rental'] = rentals[lease]
                row['cost_estimate'] = _run_cost_estimate(row, rentals[lease], time.time())
            rows.append(row)
        return {"jobs": rows[:100],
                "worker": worker, "queue": queue.read(), "supervision_available": services_available()}
    except VastError as exc:
        raise _error(exc) from None


@router.get('/jobs/{identifier}/charges')
def job_charges(identifier: str, response: Response, session: SessionToken = Depends(require_auth)) -> dict:
    """Read provider billing separately so job polling never waits for it."""
    from vast_billing import instance_charges
    response.headers['Cache-Control'] = 'no-store'
    try:
        store = JobStore()
        row = store.read(job_id(identifier))
        lease = row.get('lease_id')
        return {'billing': instance_charges(store, job_id(lease)) if lease else None}
    except VastError as exc:
        raise _error(exc) from None


@router.get("/configs")
def configs(session: SessionToken = Depends(require_auth)) -> dict:
    """Offer existing PB8 configurations without reading result or secret stores."""
    from api.optimize_v8 import list_configs
    return list_configs(session=session, include_result_summary=False)


@router.post("/jobs/prepare")
def prepare_job(body: PrepareJobRequest, session: SessionToken = Depends(require_auth)) -> dict:
    """Validate and freeze local data before permitting a paid instance request."""
    return _prepare_job(body)


def _prepare_job(body: PrepareJobRequest, *, requeue_from: str | None = None) -> dict:
    """Prepare a snapshot with an internal replacement relationship for queue display."""
    from api.optimize_v8 import _config_file, _config_lock, _results_root
    from pb8_config import load_pb8_config
    try:
        name = config_name(body.config_name)
        with _config_lock():
            path = _config_file(name)
            if not path.is_file() or path.is_symlink() or path.parent.is_symlink():
                raise VastError("Optimizer config not found", 404)
            original_hash = digest(path)
            config = load_pb8_config(path)
        root = Path(__file__).resolve().parents[1]
        return JobStore().prepare(name, config, original_hash, root / "data/ohlcv", root / "data/coindata",
                                  _results_root(), body.iterations, body.workers, body.use_adg, requeue_from=requeue_from)
    except HTTPException:
        raise
    except VastError as exc:
        raise _error(exc) from None
    except Exception as exc:
        _log(SERVICE, "Cloud input preparation failed: " + type(exc).__name__, level="ERROR")
        raise HTTPException(status_code=422, detail="Cloud input preparation failed. Check the saved config and local market data.") from None


@router.post("/jobs/{identifier}/requeue")
def requeue_job(identifier: str, session: SessionToken = Depends(require_auth)) -> dict:
    """Serialize replacement preparation and reuse its durable identity on retries."""
    try:
        identifier = job_id(identifier)
        queue = CloudQueue()
        with advisory_file_lock(queue.store.directory(identifier) / '.requeue-lock'):
            source = queue.store.read(identifier)
            replacement_id = source.get('requeued_as')
            if replacement_id:
                return queue.store.read(job_id(replacement_id))
            if source.get('deleted_at') or source.get('status') not in {'failed', 'cancelled'} or not can_remove_job(source, queue.worker()):
                raise VastError('Only inactive failed or cancelled jobs can be requeued', 409)
            replacement = _prepare_job(PrepareJobRequest(config_name=source['config_name'],
                iterations=source['iterations'], workers=4 if source.get('auto_cpu_workers') else source['workers'], use_adg=bool(source.get('use_adg'))), requeue_from=identifier)
            queue.store.update(identifier, requeued_as=replacement['id'])
            queue.remove_job(identifier)
            return replacement
    except VastError as exc:
        raise _error(exc) from None


@router.post("/queue/start", status_code=202)
def start_queue(body: StartJobRequest, session: SessionToken = Depends(require_auth)) -> dict:
    """Authorize one shared rental for queued jobs after refreshing the quote."""
    try:
        queue = CloudQueue()
        if not body.accept_rental_and_cleanup:
            raise VastError("Confirm the shared rental and automatic cleanup first", 422)
        current = queue.worker()
        if current and current['rental_state'] not in ('none', 'deletion_verified'):
            control = queue.store.read(current['id'], 'control.json')
            if control.get('cleanup') or control.get('stop') or current['rental_state'] == 'destroy_pending':
                raise VastError('Previous GPU rental is still being cleaned up. No new job has started; finish rental cleanup before starting again.', 409)
            if not body.rent_only and queue.read().get('paused'):
                queue.action('resume')
            return current
        if body.use_saved_settings:
            stored = queue.read().get('gpu_preferences')
            if not stored:
                raise VastError('Save GPU requirements and rental limits in Settings first', 422)
            try:
                saved = RentalPreferences.model_validate(stored)
            except ValueError:
                raise VastError('Save valid GPU requirements and rental limits in Settings', 422) from None
            body = StartJobRequest(preferences=GpuPreferences.model_validate(saved.model_dump(include=set(GpuPreferences.model_fields))),
                                   hours=saved.hours, budget=saved.budget, idle_seconds=saved.idle_seconds,
                                   accept_rental_and_cleanup=True, rent_only=body.rent_only, offer_id=body.offer_id)
        if body.preferences is None:
            raise VastError('Save GPU requirements in Settings first', 422)
        if body.rent_only != (body.offer_id is not None):
            raise VastError('Immediate rental requires one selected offer', 422)
        waiting = queue.waiting()
        if not waiting and not body.rent_only:
            raise VastError("Queue a cloud optimizer config first", 409)
        preferences = body.preferences.model_dump()
        preferences['gpu_name'] = preferences['gpu_name'].strip()
        preferences['min_cpu'] = max(preferences['min_cpu'], max((1 if row.get('auto_cpu_workers') else row['workers'] for row in waiting), default=1))
        client = VastClient(VastCredentialStore().secrets()['api_key'])
        rows = client.offers(**preferences, min_cuda=13, min_duration=body.hours * 3600,
                             **({'offer_id': body.offer_id} if body.rent_only else {}))
        # Recheck hard requirements even if the provider ignores a query filter.
        matches = [row for row in rows if
                   gpu_name_matches(row.get('gpu_name', ''), preferences['gpu_name'])
                   and row.get('num_gpus') == 1
                   and (row.get('cuda_max_good') or 0) >= 13
                   and (row.get('duration_seconds') is None or row['duration_seconds'] >= body.hours * 3600)
                   and 0 < (row.get('price_hour_usd') or 0) <= preferences['max_price']
                   and (row.get('vram_gb') or 0) >= preferences['min_vram']
                   and (row.get('ram_gb') or 0) >= preferences['min_ram']
                   and (row.get('cpu_cores') or 0) >= preferences['min_cpu']
                   and (row.get('disk_gb') or 0) >= preferences['disk_gb']
                   and (not preferences['verified_only'] or row.get('verified') is True)]
        if body.rent_only:
            matches = [row for row in matches if row['id'] == body.offer_id]
            if not matches:
                raise VastError('The selected GPU is no longer available or no longer meets the rental limits. Refresh offers and select again; no replacement was rented.', 409)
        if not matches:
            raise VastError("No available GPU matches your Settings requirements. No rental started; try again later or adjust the requirements.", 409)
        selected = min(matches, key=lambda row: (row['price_hour_usd'], row['id']))
        if body.rent_only:
            return queue.start(selected, body.hours, body.budget, body.idle_seconds, manual=True)
        return queue.start(selected, body.hours, body.budget, body.idle_seconds)
    except VastError as exc:
        raise _error(exc) from None


class DeadlineRequest(BaseModel):
    """Require explicit lease identity and compare-and-set deadline from the UI."""
    model_config = ConfigDict(extra='forbid')
    worker_id: str
    expected_deadline: float = Field(allow_inf_nan=False)
    minutes: Literal[-30, 30]


@router.post('/queue/deadline', status_code=202)
def adjust_deadline(body: DeadlineRequest, session: SessionToken = Depends(require_auth)) -> dict:
    """Queue a deadline change for acknowledgement by the independent worker guard."""
    from vast_deadline import request_deadline
    try:
        return request_deadline(CloudQueue(), body.worker_id, body.expected_deadline, body.minutes)
    except VastError as exc:
        raise _error(exc) from None



@router.post("/queue/{action}")
def queue_action(action: str, session: SessionToken = Depends(require_auth)) -> dict:
    """Pause, continue or end the existing shared worker, never rent implicitly."""
    try:
        return CloudQueue().action(action)
    except VastError as exc:
        raise _error(exc) from None


@router.post("/jobs/{identifier}/stop")
def stop_job(identifier: str, session: SessionToken = Depends(require_auth)) -> dict:
    """Request normal stop, collection and already authorized rental cleanup."""
    try:
        return JobStore().control(identifier, "stop")
    except VastError as exc:
        raise _error(exc) from None


@router.post("/jobs/{identifier}/recover")
def recover_job(identifier: str, session: SessionToken = Depends(require_auth)) -> dict:
    """Resume supervision of an existing authorization without a replacement rent."""
    try:
        store = JobStore()
        state = store.read(identifier)
        if state.get("final_collected"):
            from vast_job_runner import finalize_collected_results
            return finalize_collected_results(store, identifier)
        if state.get("lease_id"):
            return CloudQueue(store).action("recover")
        if state["rental_state"] == "none":
            raise VastError("This job has not been authorized for rental", 409)
        if state["rental_state"] != "deletion_verified":
            store.launch_service(identifier, "guard")
        store.launch_service(identifier, "run")
        return store.read(identifier)
    except VastError as exc:
        raise _error(exc) from None


@router.post("/jobs/{identifier}/cleanup")
def cleanup_job(identifier: str, session: SessionToken = Depends(require_auth)) -> dict:
    """Retry an existing cleanup request; never bypass pending result collection."""
    try:
        store = JobStore()
        state = store.read(identifier)
        if state["status"] not in {"failed", "cancelled", "completed"} and not state.get("final_collected"):
            raise VastError("Use Stop & collect for an active optimizer", 409)
        if state["rental_state"] not in {"none", "deletion_verified"}:
            store.control(identifier, "cleanup")
            store.launch_service(identifier, "guard")
        return store.read(identifier)
    except VastError as exc:
        raise _error(exc) from None


@router.delete("/jobs/{identifier}")
def delete_job(identifier: str, session: SessionToken = Depends(require_auth)) -> dict:
    """Remove an inactive cloud queue entry, preserving its stored artifacts."""
    try:
        return CloudQueue().remove_job(identifier)
    except VastError as exc:
        raise _error(exc) from None
