"""Native PB9 policy hooks for the shared Vast export, transfer and result pipeline."""
import copy
from pathlib import Path

from vast_provider import VastError
from vast_worker_profiles import worker_profile

SERVICE = 'Vast'


def _adapter():
    """Use the same selected native source, cancellation and metadata owner as Optimize."""
    from api.optimize_v9 import engine
    return engine.native


def _launch_options(source):
    """Keep actionable native validation messages in detached cloud preparation."""
    from fastapi import HTTPException
    from api.optimize_v9 import engine
    try:
        options = engine._runtime_options_from_config(source)
        engine._validate_options_for_config(options, source)
        return options
    except HTTPException as exc:
        raise VastError(str(exc.detail), exc.status_code) from exc


def validate(config,iterations=None,workers=None,*,use_adg=False):
    """Validate native schema/search and cloud data boundaries without requiring local CUDA."""
    from pb9_config import PB9ConfigError
    errors=[]
    try:
        source=_adapter().prepare(config)
        metadata=_adapter().metadata()
    except PB9ConfigError as exc:
        return [{'path':'config','message':str(exc),'suggestions':[]}]
    bt=source.get('backtest',{})
    opt=source.get('optimize',{})
    if bt.get('hlcvs_data_dir'):
        errors.append({'path':'backtest.hlcvs_data_dir','message':'Cloud export requires PBGui daily OHLCV shards.'})
    if source.get('coin_overrides') or bt.get('market_settings_sources'):
        errors.append({'path':'coin_overrides','message':'The shared cloud profile does not support coin overrides.'})
    if use_adg:
        errors.append({'path':'optimize.scoring','message':'PB9 uses native metric names. Select its objective explicitly; PB8 ADG substitution does not apply.'})
    metrics=set(metadata['scoring']['metrics'])
    for key in ('scoring','limits'):
        for index,item in enumerate(opt.get(key,[])):
            if item.get('metric') not in metrics:
                errors.append({'path':f'optimize.{key}.{index}.metric','message':'Unsupported native PB9 metric.'})
    if iterations is not None and (type(iterations) is not int or not 256<=iterations<=20_000_000):
        errors.append({'path':'optimize.iters','message':'Choose 256–20,000,000 iterations.'})
    if workers is not None and (type(workers) is not int or not 1<=workers<=64):
        errors.append({'path':'optimize.n_cpus','message':'Choose 1–64 CPU workers.'})
    return errors


def prepare(source,iterations,workers,use_adg):
    """Produce native worker input plus an export-only projection, never PB8 execution JSON."""
    from pb9_config import get_pb9_optimize_cloud_plan
    from vast_history import freeze_relative_dates
    profile=worker_profile('pb9').require_published()
    errors=validate(source,iterations,workers,use_adg=use_adg)
    if errors:raise VastError('; '.join(item['path']+': '+item['message'] for item in errors),422)
    config=copy.deepcopy(source);config.pop('pbgui',None)
    opt=config.setdefault('optimize',{})
    opt.update(backend='gpu',iters=iterations,n_cpus=workers)
    config.setdefault('backtest',{})['offline']=True
    freeze_relative_dates(config)
    native=_adapter()
    launch_options = _launch_options(source)
    if launch_options['mode'] == 'checkpoint_resume':
        raise VastError('Use local checkpoint resume for local results; cloud Requeue creates a fresh job.', 422)
    plan_config = copy.deepcopy(config)
    selected = launch_options.get('scenarios') or []
    if selected:
        plan_config['backtest']['suite_enabled'] = True
        plan_config['backtest']['scenarios'] = [item for item in plan_config['backtest']['scenarios'] if isinstance(item, dict) and item.get('label') in selected]
    plan=get_pb9_optimize_cloud_plan(plan_config,*native.options(),cancel_event=native.stop_flag())
    if plan['source']['commit']!=profile.revision or plan['source']['config_schema']!=profile.schema:
        raise VastError('PB9 source differs from the pinned cloud worker; rebuild the worker before exporting.',409)
    plans=plan['data_plans']
    projection=copy.deepcopy(plans[0])
    # Native helper has already applied each scenario. Legacy helpers below
    # consume only the resulting data selections, never native policy patches.
    projection['backtest']['scenarios']=[]
    projection['backtest']['suite_enabled']=len(plans)>1
    for index,item in enumerate(plans[1:]):
        approved=item['live']['approved_coins']
        projection['backtest']['scenarios'].append({'label':f'native_{index}',
            'coins':sorted(set(approved['long']+approved['short'])),
            **{key:item['backtest'][key] for key in ('exchanges','start_date','end_date')}})
    return config,profile,projection,[item['warmup_minutes'] for item in plans]


def launch_bundle(source):
    """Freeze validated seed artifacts and native CLI options for a cloud job."""
    import json
    from secure_files import read_regular_file_nofollow
    options = _launch_options(source)
    artifacts = {}
    if options['mode'] == 'checkpoint_resume':
        raise VastError('Local checkpoints cannot be submitted as fresh cloud jobs', 422)
    if options['mode'] == 'pareto_seed':
        selected = options['source']
        if selected == '__self__':
            options['source'] = 'optimize.json'
        else:
            root = Path(selected)
            candidates = sorted(root.glob('*.json')) if root.is_dir() else [root]
            if not candidates or len(candidates) > 4096:
                raise VastError('Cloud seed requires 1–4096 managed JSON artifacts', 422)
            total = 0
            for index, path in enumerate(candidates):
                if path.stat().st_size > 4 * 1024 * 1024:
                    raise VastError('Cloud seed artifact exceeds 4 MiB', 422)
                content = read_regular_file_nofollow(path, root if root.is_dir() else root.parent)
                total += len(content)
                if total > 40 * 1024 * 1024:
                    raise VastError('Cloud seed artifacts exceed 40 MiB', 422)
                try:
                    if not isinstance(json.loads(content), dict):
                        raise ValueError('Expected a JSON object')
                except ValueError as exc:
                    raise VastError('Invalid managed JSON seed artifact', 422) from exc
                artifacts[f'starting_configs/{index:04d}.json'] = content
            options['source'] = 'starting_configs'
    artifacts['launch_options.json'] = (json.dumps(options, indent=4, allow_nan=False) + '\n').encode()
    return artifacts


def save(config,path):
    """Persist the native worker input through PBGui's checked config gateway."""
    _adapter().write(Path(path),config)


def snapshot_errors(config,revision):
    """Check the pinned native worker contract without importing a local bot runtime."""
    import json
    contract=json.loads((Path(__file__).parent/'setup/vast_gpu_benchmark/pb9_worker_contract.json').read_text())
    errors=[]
    def error(path,message):errors.append({'path':path,'message':message})
    if revision!=contract['commit'] or config.get('config_version')!=contract['schema']:
        error('config_version','Native worker source/schema mismatch.')
    opt=config.get('optimize') or {}
    if opt.get('backend')!='gpu':error('optimize.backend','Native cloud jobs require GPU.')
    if (config.get('backtest') or {}).get('offline') is not True:error('backtest.offline','Native cloud jobs require frozen offline inputs.')
    if set(opt.get('gpu') or {})-set(contract['gpu_keys']):error('optimize.gpu','Config contains fields outside the native worker contract.')
    for name in ('scoring','limits'):
        for index,item in enumerate(opt.get(name) or []):
            if not isinstance(item,dict) or item.get('metric') not in contract['metrics']:
                error(f'optimize.{name}.{index}.metric','Metric is outside the native worker contract.')
    return errors


def resolve_gpu_settings(config,state,offer,rental_override=None):
    """Keep native GPU resource policy; never insert retired PB8 tuning fields."""
    import hashlib
    import json
    from vast_gpu_tuning import _validated_rental_gpu_profile
    opt=config.setdefault('optimize',{});gpu=opt.setdefault('gpu',{})
    override=_validated_rental_gpu_profile(rental_override, runtime='pb9')
    automatic={}
    preserved=copy.deepcopy(gpu)
    calibrated = None
    if (override is None and opt.get('population_size') is None and gpu.get('batch_size') is None
            and str(gpu.get('tuning_mode', 'conservative')).strip().lower() != 'off'):
        from vast_calibration import (CalibrationProfiles, calibration_workload_fingerprint,
                                      gpu_variant_identity, NATIVE_CALIBRATION_IMAGES)
        from logging_helpers import human_log as _log
        hardware = dict(state.get('hardware') or {})
        measured_power = (state.get('runtime_metrics') or {}).get('gpu_power_limit_watts')
        if measured_power is not None:
            hardware['gpu_power_limit_watts'] = measured_power
        try:
            candidate = CalibrationProfiles(Path(__file__).resolve().parent/'data/vast', runtime='pb9').match(
                gpu_variant_identity(offer, hardware), calibration_workload_fingerprint(config))
            profile = worker_profile('pb9')
            if (candidate and candidate.get('protocol') == 5
                    and candidate.get('worker_image') == profile.image
                    and profile.image in NATIVE_CALIBRATION_IMAGES
                    and candidate.get('pb8_revision') == profile.revision):
                calibrated = candidate
                population = candidate['population_size']
                opt['population_size'] = population
                gpu.update(batch_size=population, tuning_mode='off')
                automatic.update(population_size=population, batch_size=population, tuning_mode='off')
        except (OSError, ValueError, TypeError, KeyError, AttributeError) as exc:
            _log(SERVICE, 'Native GPU calibration profile unavailable: ' + type(exc).__name__, level='WARNING')
    if override is not None and str(gpu.get('tuning_mode','conservative')).strip().lower()!='off':
        opt['population_size']=override['population_size']
        automatic['population_size']=override['population_size']
        for key in ('batch_size','max_dispatch_candidate_bars'):
            gpu[key]=override[key];automatic[key]=override[key]
        gpu['tuning_mode']='off';automatic['tuning_mode']='off'
    record={'schema_version':1,'runtime':'pb9','applied_by':('rental_override' if override and automatic else
            'native_calibration' if calibrated else 'native_pb9'),
        'calibration_profile_id':calibrated.get('id') if calibrated else None,
        'gpu_name':(state.get('hardware') or {}).get('gpu') or offer.get('gpu_name'),
        'automatic':automatic,'preserved':preserved,'rental_gpu_profile':override,
        'population_size':opt.get('population_size'),'batch_size':gpu.get('batch_size'),
        'max_dispatch_candidate_bars':gpu.get('max_dispatch_candidate_bars')}
    record['fingerprint']=hashlib.sha256(json.dumps(record,sort_keys=True,allow_nan=False).encode()).hexdigest()
    return record


def calibration_config(template,metadata=None):
    """Bind the shared performance-test protocol to native policy, bounds and metrics."""
    metadata=_adapter().metadata() if metadata is None else metadata
    config=copy.deepcopy(template)
    bot=config['bot'];bot['portfolio']['strategy_kind']='ema_anchor'
    universe=bot['global'].setdefault('universe',{})
    universe.update(approved_coins=['BTC','ETH'],ignored_coins=[])
    for side in ('long','short'):
        bot[side]['enabled']=side == 'long'
        bot[side].setdefault('risk', {}).update(max_positions=2 if side == 'long' else 0,
            total_wallet_exposure_limit_ratio=1.0 if side == 'long' else 0.0)
        bot[side]['universe']={'approved_coins':None,'ignored_coins':None}
        bot[side]['strategy']={'ema_anchor':copy.deepcopy(metadata['strategy_defaults'][side]['ema_anchor'])}
    config['backtest'].update(start_date='2024-01-01',end_date='2024-01-03',exchanges=['binance'],
        coin_sources={},scenarios=[{'label':'pbgui_calibration_v1'}],suite_enabled=False)
    opt=config['optimize'];opt.update(backend='gpu',iters=10_000_000,n_cpus=4,seed=7,
        scoring=[{'metric':'cagr_fraction_strategy_equity','goal':'max'}],
        bounds=copy.deepcopy(metadata['active_bounds']['ema_anchor']),enable_overrides=[],fixed_params=[])
    for side in ('long','short'):opt['bounds'].setdefault(side,{}).setdefault('risk',{})['max_positions']=[1,2,1] if side == 'long' else [0,0,1]
    opt['population_size']=128
    opt['gpu'].update(tuning_mode='off',batch_size=128)
    config['pbgui']={'calibration_workload':'pb9-gpu-calibration-v2-small'}
    return config


def calibration_preset(config, preset):
    """Scale native reference data independently of the legacy proxy workload."""
    days = {'small': 3, 'medium': 7, 'large': 14}[preset]
    coins = ['BTC', 'ETH'] if preset == 'small' else ['BTC', 'ETH', 'SOL']
    config['bot']['global']['universe']['approved_coins'] = coins
    config['backtest'].update(start_date='2024-01-01', end_date=f'2024-01-{days:02d}',
        exchanges=['binance'], suite_enabled=False,
        scenarios=[{'label': f'pbgui_native_{preset}'}])
    config['pbgui']['calibration_workload'] = 'pb9-gpu-calibration-v2-' + preset
    return config


def calibration_template():
    """Expose the native starter through the same performance-test builder seam."""
    return copy.deepcopy(_adapter().metadata()['template'])
