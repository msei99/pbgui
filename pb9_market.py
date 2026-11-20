"""Use PB8's market catalog protocol with the selected native PB9 resolver."""
from __future__ import annotations

import asyncio
import importlib.util
import os
from pathlib import Path
import re
from tempfile import TemporaryDirectory

SERVICE = "V9Instances"
ROOT = Path(__file__).resolve().parent


def _shared():
    """Load the existing PB8 implementation, without importing any bot modules."""
    spec = importlib.util.spec_from_file_location("_pbgui_shared_market_catalog", ROOT / "passivbot_market.py")
    shared = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(shared)
    return shared


def validate_selection(exchanges, identifiers):
    """Apply the shared request bounds before runtime inspection or cache paths."""
    if any(not isinstance(values,list) or any(not isinstance(value,str) or value != value.strip() for value in values)
           for values in (exchanges,identifiers)):
        raise ValueError('Market selections must contain trimmed strings')
    shared = _shared()
    exchanges = shared._validated_market_strings(exchanges, "exchanges", maximum=16, max_bytes=64)
    identifiers = shared._validated_market_strings(identifiers, "identifiers", maximum=1000, max_bytes=256)
    if not exchanges or any(not re.fullmatch(r"[a-z][a-z0-9_-]*", value) or value == "fake" for value in exchanges):
        raise ValueError("Invalid exchange selection")
    return exchanges, identifiers


def get_market_identifiers(paths, exchanges, identifiers, cancel_event, *, coindata=None):
    """Reuse PB9's bounded, cancellable helper with an owned temporary cache view."""
    from pb9_config import _config_operation
    from pb9_backtest_runtime import prepare_market_metadata

    exchanges, identifiers = validate_selection(exchanges, identifiers)
    selected = [value for value in identifiers if value.lower() != "all"]
    metadata_roots = [str(Path(paths[key]).expanduser().resolve()) for key in ('pb7dir','pb8dir') if paths.get(key)]
    with TemporaryDirectory(prefix="pbgui-pb9-markets-") as work:
        config = {'backtest':{'offline':True,'exchanges':exchanges,'ohlcv_source_dir':str(ROOT/'data/ohlcv')}}
        prepare_market_metadata(Path(work), config, Path(coindata) if coindata is not None else ROOT/'data/coindata',
                                Path(paths['pb9dir']), metadata_roots)
        response = _config_operation("market_identifiers", paths['pb9dir'], paths['pb9venv'],
            {key:value for key,value in paths.items() if not key.startswith('pb9')}, timeout=60,
            cancel_event=cancel_event, probe_context={"work_dir":work,
                "metadata_roots":metadata_roots,
                "market_request":{"exchanges":exchanges,"identifiers":selected,"quote":None}})
    result = response['market_catalog']
    for value in identifiers:
        if value.lower() == 'all':
            result['statuses'][value] = {'input':value,'normalized':'all','status':'valid',
                'reason':'all','detail':'','resolutions':[],'display':'all'}
    return result


def resolve(request, repo, work, metadata_roots):
    """Run the unchanged PB8 catalog algorithm against PB9's own native functions."""
    exchanges, identifiers = validate_selection(request.get('exchanges'), request.get('identifiers'))
    if set(request) != {'exchanges','identifiers','quote'} or request['quote'] is not None:
        raise ValueError("Invalid market request")
    shared = _shared()
    modules = shared._load_pb8_market_modules()
    from simulation_data import simulation_data_policy

    config = {'backtest':{'offline':True,'exchanges':exchanges,'ohlcv_source_dir':str(ROOT/'data/ohlcv')}}
    previous_cwd = Path.cwd()
    try:
        os.chdir(work)
        with simulation_data_policy(config):
            return asyncio.run(shared._market_identifiers(modules,
                {'exchanges':exchanges,'identifiers':identifiers,'quote':None}))
    finally:
        os.chdir(previous_cwd)
