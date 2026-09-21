"""Authenticated Vast account, balance and GPU marketplace routes."""

from __future__ import annotations

import asyncio
import json
import os
import time

from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from threading import RLock
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator

from api.auth import SessionToken, require_auth
from api.page_templates import render_page_urls
from optimizer_workload import estimate_coin_candles, estimate_snapshot
from logging_helpers import human_log as _log
from vast_credentials import VastCredentialStore
from vast_provider import REFERRAL_URL, VastClient, VastError, gpu_name_matches, positive_id
from vast_jobs import JobStore, config_name, digest, services_available, job_id
from vast_queue import CloudQueue, can_remove_job, blocked_machine_ids, offer_host_allowed
from vast_hosts import host_history, set_host_preference, search_host_offers, offer_priority
from file_lock import advisory_file_lock
from secure_files import ensure_private_directory

SERVICE = "Vast"
router = APIRouter()
CLOUD_LOG_ROOT = Path(__file__).resolve().parents[1] / "data/logs/optimizes_v8"
_PREPARATION_LOCK = RLock()
_PREPARATION_EXECUTOR: ThreadPoolExecutor | None = None
_PREPARATION_TASKS: dict[str, Future] = {}
_PREPARATION_STOPPING = False
_PERFORMANCE_COLLECTOR = None


def startup() -> None:
    """Start the bounded local input-preparation owner for this API lifespan."""
    global _PREPARATION_EXECUTOR, _PREPARATION_STOPPING, _PERFORMANCE_COLLECTOR
    with _PREPARATION_LOCK:
        _PREPARATION_STOPPING = False
        if _PREPARATION_EXECUTOR is None:
            _PREPARATION_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="vast-input")
        if _PERFORMANCE_COLLECTOR is None:
            from vast_performance import PerformanceCollector
            collector = PerformanceCollector(JobStore(), CLOUD_LOG_ROOT)
            try:
                collector.start()
            except Exception:
                collector.stop()
                collector.join()
                _PREPARATION_EXECUTOR.shutdown(wait=True, cancel_futures=True)
                _PREPARATION_EXECUTOR = None
                raise
            _PERFORMANCE_COLLECTOR = collector
    queue = CloudQueue()
    from vast_job_runner import recover_cancelled_empty_results
    recover_cancelled_empty_results(queue.store)
    if queue.read().get('pool_enabled'):
        try:
            from vast_pool import launch_pool
            launch_pool(queue)
        except VastError as exc:
            queue.update(pool_error=str(exc), paused=True)
            _log(SERVICE, 'Automatic GPU pool could not be restored: ' + str(exc), level='WARNING')


async def shutdown() -> None:
    """Drain input preparation deterministically before the API process exits."""
    global _PREPARATION_EXECUTOR, _PREPARATION_STOPPING, _PERFORMANCE_COLLECTOR
    with _PREPARATION_LOCK:
        _PREPARATION_STOPPING = True
        executor, _PREPARATION_EXECUTOR = _PREPARATION_EXECUTOR, None
        collector, _PERFORMANCE_COLLECTOR = _PERFORMANCE_COLLECTOR, None
    pending = []
    if collector is not None:
        collector.stop()
        pending.append(asyncio.to_thread(collector.join))
    if executor is not None:
        pending.append(asyncio.to_thread(executor.shutdown, True, cancel_futures=True))
    for result in await asyncio.gather(*pending, return_exceptions=True):
        if isinstance(result, BaseException):
            _log(SERVICE, 'Cloud shutdown failed: ' + type(result).__name__, level='ERROR')
    with _PREPARATION_LOCK:
        _PREPARATION_TASKS.clear()


def _run_preparation(store: JobStore, identifier: str, name: str, config: dict, source_sha256: str,
                     iterations: int, workers: int, use_adg: bool) -> None:
    """Build one immutable cloud input snapshot outside the request lifetime."""
    try:
        with _PREPARATION_LOCK:
            stopping = _PREPARATION_STOPPING
        if stopping:
            store.update(identifier, status="failed", error="Input preparation interrupted by API shutdown")
            return
        root = Path(__file__).resolve().parents[1]
        from api.optimize_v8 import _results_root
        store.prepare(name, config, source_sha256, root / "data/ohlcv", root / "data/coindata", _results_root(),
                      iterations, workers, use_adg, identifier=identifier)
    except Exception as exc:
        try:
            store.update(identifier, status="failed", error="Input preparation failed; no instance rented")
        except Exception:
            pass
        _log(SERVICE, "Cloud input preparation failed: " + type(exc).__name__, level="ERROR")
    finally:
        with _PREPARATION_LOCK:
            _PREPARATION_TASKS.pop(identifier, None)


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
    min_tflops: float = Field(default=0, ge=0, le=100_000)
    disk_gb: int = Field(default=40, ge=40, le=2000)
    verified_only: bool = True


class RentalPreferences(GpuPreferences):
    """Persist the shared rental limits alongside marketplace requirements."""
    hours: float = Field(default=1, ge=.25, le=24)
    budget: float = Field(default=1, ge=.1, le=100)
    idle_seconds: Literal[-1, 0, 300, 1800, 3600] = -1
    max_rentals: int = Field(default=1, ge=1, le=16, strict=True)
    auto_rent: bool = False
    convergence_enabled: bool = False
    convergence_min_exact: int = Field(default=512, ge=256, le=10_000_000)
    convergence_patience: int = Field(default=512, ge=128, le=10_000_000)
    convergence_tolerance_pct: float = Field(default=0.25, gt=0, le=10)


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
    idle_seconds: Literal[-1, 0, 300, 1800, 3600] = -1


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
        _apply_gpu_preferences(CloudQueue(), values)
        return values
    except VastError as exc:
        raise _error(exc) from None


@router.patch("/gpu-preferences")
def patch_gpu_preferences(body: RentalPreferences, session: SessionToken = Depends(require_auth)) -> dict:
    """Merge only submitted preference fields under the reentrant queue lock."""
    try:
        queue = CloudQueue()
        ensure_private_directory(queue.root)
        with advisory_file_lock(queue.root / '.queue-lock'):
            stored = queue.read().get('gpu_preferences', {})
            values = RentalPreferences.model_validate({**stored, **body.model_dump(exclude_unset=True)}).model_dump()
            values['gpu_name'] = values['gpu_name'].strip()
            _apply_gpu_preferences(queue, values)
        return values
    except VastError as exc:
        raise _error(exc) from None
    except (ValueError, TypeError):
        _log(SERVICE, "Invalid stored GPU preferences during partial save", level="ERROR")
        raise HTTPException(status_code=422, detail="Stored GPU preferences are invalid; save valid settings before retrying.") from None


def _apply_gpu_preferences(queue: CloudQueue, values: dict) -> None:
    """Persist settings and explicitly enable or disable automatic paid rentals."""
    ensure_private_directory(queue.root)
    with advisory_file_lock(queue.root / '.queue-lock'):
        previous = queue.read().get('gpu_preferences', {})
        was_automatic = previous.get('auto_rent') is True
        if values.get('auto_rent'):
            from vast_pool import authorize_pool
            authorize_pool(queue, values, resume=not was_automatic)
        elif was_automatic:
            queue.update(pool_enabled=False, paused=True, pool_authorization=None, pool_error=None)
        queue.update(gpu_preferences=values)


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
           min_cpu: float = Query(4, ge=0, le=512), min_tflops: float = Query(0, ge=0, le=100_000),
           disk_gb: int = Query(40, ge=10, le=2000),
           verified_only: bool = True, gpu_name: str = Query("", max_length=80),
           include_incompatible: bool = False, rental_hours: float = Query(1, ge=.25, le=24),
           session: SessionToken = Depends(require_auth)) -> dict:
    """List live offers without renting or changing any provider resources."""
    response.headers["Cache-Control"] = "no-store"
    try:
        queue = CloudQueue()
        state = queue.read()
        blocked = blocked_machine_ids(state)
        rows = search_host_offers(VastClient(VastCredentialStore().secrets()["api_key"]), state,
            max_price=max_price, min_vram=min_vram, min_ram=min_ram,
            min_cpu=min_cpu, min_tflops=min_tflops, disk_gb=disk_gb, verified_only=verified_only, gpu_name=gpu_name,
            min_cuda=0 if include_incompatible else 13,
            min_duration=0 if include_incompatible else rental_hours * 3600,
            excluded_machine_ids=blocked,
        )
        history = host_history(queue.store, state)
        return {"offers": [dict(row, host_history=history.get(row.get('machine_id'), {}))
                           for row in rows if offer_host_allowed(row, blocked)][:100],
                "hosts": list(history.values()), "limit": 100}
    except VastError as exc:
        raise _error(exc) from None


class HostBlockRequest(BaseModel):
    """Explicit reversible exclusion of a physical GPU machine."""
    model_config = ConfigDict(extra='forbid')
    blocked: bool = Field(strict=True)


class MachineBlockRequest(HostBlockRequest):
    """Identify a host from the offer list or the stored exclusion list."""
    machine_id: int = Field(gt=0, strict=True)


@router.post('/blocked-hosts')
def set_host_block(body: MachineBlockRequest, session: SessionToken = Depends(require_auth)) -> dict:
    """Change only local selection policy; never stop or alter a rental."""
    try:
        return {'blocked_machine_ids': CloudQueue().set_host_block(body.machine_id, body.blocked)}
    except VastError as exc:
        raise _error(exc) from None


@router.post('/jobs/{identifier}/host-block')
def set_rental_host_block(identifier: str, body: HostBlockRequest,
                          session: SessionToken = Depends(require_auth)) -> dict:
    """Resolve legacy rentals by ownership before excluding their machine."""
    try:
        queue = CloudQueue()
        machine = _rental_machine(queue, identifier)
        return {'machine_id': machine, 'blocked_machine_ids': queue.set_host_block(machine, body.blocked)}
    except VastError as exc:
        raise _error(exc) from None


def _rental_machine(queue, identifier: str) -> int:
    """Resolve a saved rental identity for a local host mark, without remote writes."""
    row = queue.store.read(job_id(identifier))
    rental_id = job_id(row.get('lease_id') or identifier)
    state = queue.store.read(rental_id)
    intent = queue.store.read(rental_id, 'intent.json')
    machine = (intent.get('offer') or {}).get('machine_id') or state.get('host_machine_id')
    if machine is None:
        from vast_job_runner import owned_instance, validate_intent
        intent = validate_intent(intent, rental_id)
        client = VastClient(VastCredentialStore(queue.root).secrets()['api_key'])
        instance = owned_instance(client, intent, fresh=True)
        machine = (instance or {}).get('machine_id')
        if machine is None:
            raise VastError('Machine ID unavailable for this rental. Enter its Vast machine ID in host settings.', 409)
    machine = positive_id(machine)
    queue.store.update(rental_id, host_machine_id=machine)
    return machine


class HostPreferenceRequest(BaseModel):
    """Edit individual marks independently without losing concurrent changes."""
    model_config = ConfigDict(extra='forbid')
    preferred: bool | None = Field(default=None, strict=True)
    working: bool | None = Field(default=None, strict=True)


class MachinePreferenceRequest(HostPreferenceRequest):
    """Bind a preference to a physical machine rather than a changing offer ID."""
    machine_id: int = Field(gt=0, strict=True)


@router.get('/hosts')
def hosts(response: Response, session: SessionToken = Depends(require_auth)) -> dict:
    """Return local rental evidence and manual marks without provider queries."""
    response.headers['Cache-Control'] = 'no-store'
    try:
        queue = CloudQueue()
        return {'hosts': list(host_history(queue.store, queue.read()).values())}
    except VastError as exc:
        raise _error(exc) from None


@router.post('/host-preferences')
def save_host_preference(body: MachinePreferenceRequest, session: SessionToken = Depends(require_auth)) -> dict:
    """Persist optional priority or a manual working mark for a machine."""
    try:
        queue = CloudQueue()
        set_host_preference(queue, body.machine_id, preferred=body.preferred, working=body.working)
        return {'hosts': list(host_history(queue.store, queue.read()).values())}
    except VastError as exc:
        raise _error(exc) from None


@router.post('/jobs/{identifier}/host-preferences')
def save_rental_host_preference(identifier: str, body: HostPreferenceRequest,
                                session: SessionToken = Depends(require_auth)) -> dict:
    """Let legacy rentals be marked using the same ownership-bound resolution."""
    try:
        queue = CloudQueue()
        machine = _rental_machine(queue, identifier)
        set_host_preference(queue, machine, preferred=body.preferred, working=body.working)
        return {'machine_id': machine, 'hosts': list(host_history(queue.store, queue.read()).values())}
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
    if (state.get('deadline_confirmed') or state.get('transfer_reserve_confirmed') is not None
            or state.get('budget_confirmed') is not None):
        from vast_deadline import effective_intent
        intent = effective_intent(store, identifier, intent)
    fields = ('gpu_name', 'vram_gb', 'ram_gb', 'cpu_cores', 'cpu_name',
              'gpu_mem_bw_gbps', 'pci_gen', 'gpu_lanes', 'pcie_bw_gbps',
              'disk_name', 'disk_bw_mbps', 'disk_gb', 'price_hour_usd',
              'download_gb_usd', 'upload_gb_usd', 'inet_down_mbps', 'inet_up_mbps',
              'location', 'verified', 'reliability', 'machine_id')
    offer = intent.get('offer') or {}
    return {'id': identifier, 'deadline_protocol': state.get('deadline_protocol', 0),
            'deadline_pending': bool(state.get('deadline_request')), 'deadline_error': state.get('deadline_error'),
            'offer': {**{key: offer.get(key) for key in fields},
                      'machine_id': offer.get('machine_id') or state.get('host_machine_id')},
            'deadline': intent.get('deadline'), 'budget_usd': intent.get('budget_usd'),
            'transfer_reserve_usd': intent.get('transfer_reserve_usd')}


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
        workers = queue.workers()
        for item in workers:
            path = CLOUD_LOG_ROOT / ('vast_' + job_id(item['id']) + '_provider.log')
            item['has_provider_log'] = path.is_file() and not path.is_symlink()
        worker_by_id = {item['id']: item for item in workers}
        if worker:
            provider_log = CLOUD_LOG_ROOT / ('vast_' + job_id(worker['id']) + '_provider.log')
            worker = dict(worker, has_provider_log=provider_log.is_file() and not provider_log.is_symlink())
        rows = []
        rentals = {}
        stored = queue.store.list()
        active_job_ids = {
            item.get('active_job') for item in workers if item.get('active_job')
        }
        replacements = {}
        for candidate in stored:  # Store order is newest first; keep the latest attempt.
            if candidate.get('requeue_from') and not candidate.get('deleted_at'):
                replacements.setdefault(candidate['requeue_from'], candidate)
        for row in stored:
            # A running optimizer remains reachable even if stale local state
            # incorrectly carries a deletion timestamp.  The worker ownership is
            # authoritative until worker_step releases active_job.
            if row.get('kind') == 'worker' or (
                row.get('deleted_at') and row.get('id') not in active_job_ids
            ):
                continue
            if row.get('status') == 'preparing' and row.get('preparation_owner'):
                row = queue.store.recover_interrupted_preparation(row['id'])
            replacement = replacements.get(row['id'])
            if replacement and replacement.get('status') not in {'failed', 'cancelled'}:
                continue
            if row.get('requeue_from') and row.get('status') in {'failed', 'cancelled'}:
                latest = replacements.get(row['requeue_from'])
                if (latest and latest['id'] != row['id']
                        and latest.get('status') not in {'failed', 'cancelled'}):
                    continue
                original = next((item for item in stored if item['id'] == row['requeue_from']), None)
                if original and not original.get('deleted_at'):
                    continue
            log = CLOUD_LOG_ROOT / ('vast_' + job_id(row['id']) + '.log')
            provider_log = CLOUD_LOG_ROOT / ('vast_' + row['id'] + '_provider.log')
            estimate = row.get('estimated_coin_candles')
            if 'estimated_coin_candles' not in row:
                estimate = estimate_snapshot(queue.store.directory(row['id']) / 'input/optimize.json', queue.store.root)
            row = dict(row, estimated_coin_candles=estimate, exchange=', '.join(queue.store.exchanges(row)),
                       has_log=log.is_file() and not log.is_symlink(), can_delete=can_remove_job(row, worker_by_id.get(row.get('lease_id'), worker)))
            control_exists = (queue.store.directory(row['id']) / 'control.json').exists()
            row['stop_requested'] = bool(queue.store.read(row['id'], 'control.json').get('stop')) if control_exists else False
            row['has_provider_log'] = provider_log.is_file() and not provider_log.is_symlink()
            if row['has_provider_log'] and row.get('status') == 'provisioning':
                from vast_provisioning_log import image_layer_progress
                try:
                    with provider_log.open('rb') as stream:
                        row['provider_log_fetched_at'] = os.fstat(stream.fileno()).st_mtime
                        text = stream.read(1024 * 1024).decode('utf-8', errors='replace')
                    rental_id = job_id(row.get('lease_id') or row['id'])
                    intent_path = queue.store.directory(rental_id) / 'intent.json'
                    image = queue.store.read(rental_id, 'intent.json').get('image') if intent_path.is_file() else None
                    row['image_progress'] = image_layer_progress(text, row.get('image_progress'), image=image)
                except OSError:
                    _log(SERVICE, 'Provisioning progress snapshot unavailable', level='WARNING')
            lease = row.get('lease_id')
            if lease:
                if lease not in rentals:
                    rentals[lease] = _rental_details(queue.store, lease)
                row['rental'] = rentals[lease]
                row['cost_estimate'] = _run_cost_estimate(row, rentals[lease], time.time())
            if isinstance(row.get('throughput'), dict):
                row['throughput'] = {key: value for key, value in row['throughput'].items() if key != 'samples'}
            if row['has_log'] and len(rows) < 100:
                from vast_throughput import observe_throughput
                try:
                    with log.open('rb') as stream:
                        stream.seek(max(0, os.fstat(stream.fileno()).st_size - 65536))
                        row['throughput'] = observe_throughput(queue.store, row['id'], stream.read(65536))
                except OSError:
                    _log(SERVICE, 'Optimizer throughput snapshot unavailable', level='WARNING')
            rows.append(row)
        return {"jobs": rows[:100],
                "worker": worker, "workers": workers, "queue": queue.read(), "supervision_available": services_available()}
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
    """Create a pending cloud job and prepare its immutable data snapshot in the background."""
    return _queue_preparation_job(body)


def _queue_preparation_job(body: PrepareJobRequest) -> dict:
    """Return promptly after durable queue creation; never rent before preparation succeeds."""
    global _PREPARATION_EXECUTOR
    from api.optimize_v8 import _config_file, _config_lock
    from pb8_config import load_pb8_config
    try:
        name = config_name(body.config_name)
        with _config_lock():
            path = _config_file(name)
            if not path.is_file() or path.is_symlink() or path.parent.is_symlink():
                raise VastError("Optimizer config not found", 404)
            source_sha256 = digest(path)
            config = load_pb8_config(path)
        store = JobStore()
        with _PREPARATION_LOCK:
            if _PREPARATION_STOPPING:
                raise VastError("API is shutting down; retry after it has restarted", 409)
            if _PREPARATION_EXECUTOR is None:
                _PREPARATION_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="vast-input")
            job = store.create_preparation(name, body.iterations, body.workers, body.use_adg)
            job = store.update(job['id'], estimated_coin_candles=estimate_coin_candles(config))
            future = _PREPARATION_EXECUTOR.submit(
                _run_preparation, store, job['id'], name, config, source_sha256,
                body.iterations, body.workers, body.use_adg,
            )
            _PREPARATION_TASKS[job['id']] = future
        return job
    except HTTPException:
        raise
    except VastError as exc:
        raise _error(exc) from None
    except Exception as exc:
        _log(SERVICE, "Could not queue cloud input preparation: " + type(exc).__name__, level="ERROR")
        raise HTTPException(status_code=422, detail="Could not queue cloud input preparation. Check the saved config and local market data.") from None


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
        store = JobStore()
        if requeue_from is not None:
            reused = store.clone_prepared(requeue_from, name, original_hash, _results_root(),
                                          body.iterations, body.workers, body.use_adg)
            if reused is not None:
                return reused
        return store.prepare(name, config, original_hash, root / "data/ohlcv", root / "data/coindata",
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
                replacement = queue.store.read(job_id(replacement_id))
                _resume_auto_pool_after_requeue(queue)
                return replacement
            if source.get('deleted_at') or source.get('status') not in {'failed', 'cancelled'} or not can_remove_job(source, queue.worker_for(source.get('lease_id'))):
                raise VastError('Only inactive failed or cancelled jobs can be requeued', 409)
            replacement = _prepare_job(PrepareJobRequest(config_name=source['config_name'],
                iterations=source['iterations'], workers=4 if source.get('auto_cpu_workers') else source['workers'], use_adg=bool(source.get('use_adg'))), requeue_from=identifier)
            queue.store.update(identifier, requeued_as=replacement['id'])
            queue.remove_job(identifier)
            _resume_auto_pool_after_requeue(queue)
            return replacement
    except VastError as exc:
        raise _error(exc) from None


def _resume_auto_pool_after_requeue(queue: CloudQueue) -> None:
    """Treat explicit Requeue as resume consent for an authorized automatic pool."""
    state = queue.read()
    authorization = state.get('pool_authorization') or {}
    settings = authorization.get('settings') or {}
    if not (state.get('paused') and state.get('pool_enabled') and settings.get('auto_rent') is True):
        return
    try:
        queue.action('resume')
    except VastError as exc:
        queue.update(paused=True, pool_error=str(exc))
        _log(SERVICE, 'Requeued job is ready, but automatic GPU scheduling could not resume: ' + str(exc), level='WARNING')


@router.post("/queue/start", status_code=202)
def start_queue(body: StartJobRequest, session: SessionToken = Depends(require_auth)) -> dict:
    """Authorize one shared rental for queued jobs after refreshing the quote."""
    try:
        queue = CloudQueue()
        if not body.accept_rental_and_cleanup:
            raise VastError("Confirm the shared rental and automatic cleanup first", 422)
        if body.use_saved_settings and not body.rent_only:
            stored = queue.read().get('gpu_preferences', {})
            try:
                saved = RentalPreferences.model_validate(stored)
            except ValueError:
                raise VastError('Save valid GPU requirements and rental limits in Settings', 422) from None
            if saved.max_rentals > 1 or queue.read().get('pool_authorization'):
                from vast_pool import authorize_pool
                return authorize_pool(queue, saved.model_dump())
        current = queue.worker()
        if current and current['rental_state'] not in ('none', 'deletion_verified'):
            control = queue.store.read(current['id'], 'control.json')
            if control.get('cleanup') or control.get('stop') or current['rental_state'] == 'destroy_pending':
                raise VastError('Previous GPU rental is still being cleaned up. No new job has started; finish rental cleanup before starting again.', 409)
            # A manually rented worker stays reserved until the user starts the
            # queue.  Resume is intentionally idempotent: it also releases that
            # reservation when the queue had already been marked unpaused by an
            # earlier start request.
            if not body.rent_only:
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
        host_state = queue.read()
        blocked = blocked_machine_ids(host_state)
        rows = search_host_offers(client, host_state, **preferences, min_cuda=13, min_duration=body.hours * 3600,
                             excluded_machine_ids=blocked,
                             **({'offer_id': body.offer_id} if body.rent_only else {}))
        # Recheck hard requirements even if the provider ignores a query filter.
        matches = [row for row in rows if
                   offer_host_allowed(row, blocked)
                   and gpu_name_matches(row.get('gpu_name', ''), preferences['gpu_name'])
                   and row.get('num_gpus') == 1
                   and (row.get('cuda_max_good') or 0) >= 13
                   and (row.get('duration_seconds') is None or row['duration_seconds'] >= body.hours * 3600)
                   and 0 < (row.get('price_hour_usd') or 0) <= preferences['max_price']
                   and (row.get('vram_gb') or 0) >= preferences['min_vram']
                   and (row.get('ram_gb') or 0) >= preferences['min_ram']
                   and (row.get('cpu_cores') or 0) >= preferences['min_cpu']
                   and (row.get('tflops') or 0) >= preferences['min_tflops']
                   and (row.get('disk_gb') or 0) >= preferences['disk_gb']
                   and (not preferences['verified_only'] or row.get('verified') is True)]
        if body.rent_only:
            matches = [row for row in matches if row['id'] == body.offer_id]
            if not matches:
                raise VastError('The selected GPU is no longer available or no longer meets the rental limits. Refresh offers and select again; no replacement was rented.', 409)
        if not matches:
            raise VastError("No available GPU matches your Settings requirements. No rental started; try again later or adjust the requirements.", 409)
        selected = min(matches, key=lambda row: offer_priority(row, host_state))
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
    minutes: int = Field(strict=True, ge=-1440, le=1440)

    @field_validator('minutes')
    @classmethod
    def nonzero_minutes(cls, value: int) -> int:
        """Reject an empty adjustment before dispatching a worker request."""
        if value == 0:
            raise ValueError('Deadline adjustment must not be zero')
        return value


@router.post('/queue/deadline', status_code=202)
def adjust_deadline(body: DeadlineRequest, session: SessionToken = Depends(require_auth)) -> dict:
    """Queue a deadline change for acknowledgement by the independent worker guard."""
    from vast_deadline import request_deadline
    try:
        return request_deadline(CloudQueue(), body.worker_id, body.expected_deadline, body.minutes)
    except VastError as exc:
        raise _error(exc) from None


class TransferReserveRequest(BaseModel):
    """Raise the transfer allowance while retaining the existing rental budget."""
    model_config = ConfigDict(extra='forbid')
    worker_id: str
    expected_reserve_usd: float = Field(allow_inf_nan=False, ge=.05, le=100)
    reserve_usd: float = Field(allow_inf_nan=False, ge=.05, le=100)


@router.post('/queue/transfer-reserve', status_code=202)
def adjust_transfer_reserve(body: TransferReserveRequest, session: SessionToken = Depends(require_auth)) -> dict:
    """Reserve more of a live rental budget for input and result transfers."""
    from vast_deadline import request_transfer_reserve
    try:
        return request_transfer_reserve(CloudQueue(), body.worker_id, body.expected_reserve_usd, body.reserve_usd)
    except VastError as exc:
        raise _error(exc) from None


class BudgetRequest(BaseModel):
    """Change the user-approved budget target for one active rental."""
    model_config = ConfigDict(extra='forbid')
    worker_id: str
    expected_budget_usd: float = Field(allow_inf_nan=False, ge=.1, le=100)
    budget_usd: float = Field(allow_inf_nan=False, ge=.1, le=100)


@router.post('/queue/budget', status_code=202)
def adjust_budget(body: BudgetRequest, session: SessionToken = Depends(require_auth)) -> dict:
    """Update an active rental budget through its acknowledged deadline guard."""
    from vast_deadline import request_budget
    try:
        return request_budget(CloudQueue(), body.worker_id, body.expected_budget_usd, body.budget_usd)
    except VastError as exc:
        raise _error(exc) from None



@router.post('/queue/workers/{identifier}/{action}', status_code=202)
def worker_action(identifier: str, action: str, session: SessionToken = Depends(require_auth)) -> dict:
    """Control one durable rental while leaving the rest of the GPU pool running."""
    try:
        return CloudQueue().worker_action(identifier, action)
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


@router.get('/performance')
def performance_history(response: Response, limit: int = Query(default=100, ge=1, le=200),
                        offset: int = Query(default=0, ge=0, le=100000),
                        fingerprint: str | None = Query(default=None, pattern=r'^[a-f0-9]{64}$'),
                        session: SessionToken = Depends(require_auth)) -> dict:
    """Browse retained measurements without renting, modifying or deleting a job."""
    from vast_performance import PerformanceHistory
    response.headers['Cache-Control'] = 'no-store'
    return PerformanceHistory(CloudQueue().store.root).list_runs(limit, offset, fingerprint)


class PerformanceCompareRequest(BaseModel):
    """Compare only a bounded selection of real retained run IDs."""
    ids: list[str] = Field(min_length=1, max_length=4)
    mode: Literal['hardware', 'config'] = 'hardware'

    @field_validator('ids')
    @classmethod
    def valid_ids(cls, value):
        """Reject duplicate or invalid job identifiers at the API boundary."""
        try:
            result = [job_id(identifier) for identifier in value]
        except VastError as exc:
            raise ValueError('Invalid performance run ID') from exc
        if len(set(result)) != len(result):
            raise ValueError('Choose distinct runs')
        return result


@router.post('/performance/compare')
def compare_performance(body: PerformanceCompareRequest, response: Response,
                        session: SessionToken = Depends(require_auth)) -> dict:
    """Keep hardware comparisons strict; allow explicit descriptive config comparisons."""
    from vast_performance import PerformanceHistory
    response.headers['Cache-Control'] = 'no-store'
    history = PerformanceHistory(CloudQueue().store.root)
    rows = [history.get(identifier) for identifier in body.ids]
    if any(row is None for row in rows):
        raise HTTPException(status_code=404, detail='Performance run not found')
    if body.mode == 'hardware' and len(rows) > 1 and (not rows[0].get('fingerprint') or len({row.get('fingerprint') for row in rows}) != 1):
        raise HTTPException(status_code=409, detail='Select runs with the same verified workload fingerprint')
    return {'mode': body.mode, 'runs': [dict(row, series=history.series(row['id'])) for row in rows]}
