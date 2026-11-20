"""PB9 bindings over the existing authenticated Vast account and marketplace API."""
import inspect
from functools import wraps

from fastapi import APIRouter,Depends,HTTPException,Response,Request,Query
from fastapi.routing import APIRoute

from api.auth import SessionToken,require_auth
from api import vast as shared
from vast_jobs import JobStore,job_id
from vast_provider import VastError
from vast_worker_profiles import worker_profile

SERVICE='Vast'
router=APIRouter()


def _profile():
    """Expose a stable public publication state without account/provider access."""
    p=worker_profile('pb9')
    return {'runtime':p.runtime,'revision':p.revision,'image':p.image,'schema':p.schema,
            'published':p.published,'reason':'' if p.published else 'Native PB9 worker image has not been published. Cloud rental is disabled; local optimization is available.'}


@router.get('/profile')
def profile(session:SessionToken=Depends(require_auth)):
    """Explain why native cloud execution is available or safely blocked."""
    return _profile()


@router.get('/configs')
def configs(session:SessionToken=Depends(require_auth)):
    """List only native optimizer configs, sharing the original list implementation."""
    from api.optimize_v9 import engine
    return engine.list_configs(session=session,include_result_summary=False,include_backtest_counts=False)


@router.post('/validate-config')
async def validate_config(request:Request,session:SessionToken=Depends(require_auth)):
    """Keep slow native validation within the existing bounded shutdown-owned executor."""
    from api.v9_instances import _run
    return await _run(request,'opt9_cloud_validate',{},with_body=True)


@router.get('/jobs')
def jobs(response:Response,session:SessionToken=Depends(require_auth)):
    """Keep common account/queue supervision but display only native jobs and workers."""
    result=shared.jobs(response,session)
    for key in ('jobs','workers'):
        result[key]=[row for row in result.get(key,[]) if row.get('runtime','pb8')=='pb9']
    if (result.get('worker') or {}).get('runtime','pb8')!='pb9':result['worker']=None
    return {**result,'profile':_profile()}


@router.post('/jobs/prepare')
def prepare_job(body:shared.PrepareJobRequest,session:SessionToken=Depends(require_auth)):
    """Prepare through the shared registered export executor, using native config hooks."""
    return shared._queue_preparation_job(body,runtime='pb9')


@router.get('/performance')
def performance_history(response:Response,limit:int=Query(default=100,ge=1,le=200),
        offset:int=Query(default=0,ge=0,le=100000),
        fingerprint:str|None=Query(default=None,pattern=r'^[a-f0-9]{64}$'),
        session:SessionToken=Depends(require_auth)):
    """Page retained native measurements independently of surviving cloud job records."""
    from vast_performance import PerformanceHistory
    response.headers['Cache-Control']='no-store'
    return PerformanceHistory(JobStore().root).list_runs(limit,offset,fingerprint,runtime='pb9')


def _native_endpoint(route):
    """Reuse account/marketplace handlers with native ownership at action boundaries."""
    endpoint=route.endpoint
    signature=inspect.signature(endpoint,eval_str=True)
    @wraps(endpoint)
    def native(**kwargs):
        """Reject unpublished paid work and cross-generation job IDs before delegation."""
        identifier=kwargs.get('identifier') or kwargs.get('worker_id')
        if identifier is not None:
            try:
                row=JobStore().read(job_id(identifier))
                if row.get('runtime','pb8')!='pb9':raise HTTPException(404,'PB9 cloud job not found')
            except VastError as exc:
                raise shared._error(exc) from None
        body=kwargs.get('body')
        payload=body.model_dump() if hasattr(body,'model_dump') else body if isinstance(body,dict) else {}
        identities=list(payload.get('ids') or [])
        for key in ('job_id','source_job_id','worker_id','calibration_id'):
            if payload.get(key):identities.append(payload[key])
        identities.extend((payload.get('gpu_job_profile_overrides') or {}).keys())
        for identity in identities:
            try:
                if route.path=='/performance/compare':
                    from vast_performance import PerformanceHistory
                    row=PerformanceHistory(JobStore().root).get(job_id(identity)) or {}
                    owner=(row.get('workload') or {}).get('runtime','pb8')
                else:owner=JobStore().read(job_id(identity)).get('runtime','pb8')
                if owner!='pb9':
                    raise HTTPException(404,'PB9 cloud job not found')
            except VastError as exc:raise shared._error(exc) from None
        account_paths={'/gpu-preferences','/credentials','/account','/blocked-hosts','/host-preferences','/performance/compare'}
        if route.methods-{'GET','HEAD','OPTIONS'} and route.path not in account_paths:
            try:worker_profile('pb9').require_published()
            except VastError as exc:raise shared._error(exc) from None
        native_helpers={'start_queue','calibration_status','start_calibration','start_configurable_calibration','start_calibration_watch'}
        if endpoint.__name__ in native_helpers:
            return getattr(shared,'_'+endpoint.__name__)(**kwargs,runtime='pb9')
        return endpoint(**kwargs)
    if inspect.iscoroutinefunction(endpoint):
        sync_check=native
        @wraps(endpoint)
        async def native(**kwargs):
            """Keep asynchronous authenticated account handlers asynchronous."""
            return await sync_check(**kwargs)
    native.__signature__=signature
    return native


for _route in shared.router.routes:
    if isinstance(_route,APIRoute) and _route.path not in {'/configs','/validate-config','/jobs','/jobs/prepare','/performance'}:
        router.add_api_route(_route.path,_native_endpoint(_route),methods=_route.methods,
            status_code=_route.status_code,response_class=_route.response_class)
