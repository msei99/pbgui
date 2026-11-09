"""Offline coverage and isolation regressions for selected-candle Python analysis."""
import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path

import numpy as np
import pytest

import ai_capabilities
from ai_capabilities import AICapabilityError, AICapabilityService
from ai_ohlcv_analysis import candle_dataset


def _fixture(root: Path, count: int = 1440) -> dict:
    """Build one isolated market mapping and day of candles."""
    mapping = root / 'data/coindata/hyperliquid/mapping.json'
    mapping.parent.mkdir(parents=True)
    mapping.write_text(json.dumps([{'base': 'ADA', 'quote': 'USDC', 'swap': True, 'ccxt_symbol': 'ADA/USDC:USDC'}]))
    directory = root / 'data/ohlcv/hyperliquid/1m/ADA_USDC:USDC'
    directory.mkdir(parents=True)
    a = np.zeros(count, dtype=[('ts','i8'), ('o','f4'), ('h','f4'), ('l','f4'), ('c','f4'), ('bv','f4')])
    start = int(datetime(2026, 11, 8, tzinfo=timezone.utc).timestamp() * 1000)
    a['ts'] = start + np.arange(count) * 60000
    for k,v in [('o',1),('h',1.1),('l',.9),('c',1),('bv',10)]:a[k] = v
    np.savez_compressed(directory/'2026-11-08.npz', candles=a)
    return {'exchange':'hyperliquid','coins':['ADA'],'start_time':'2026-11-08','end_time':'2026-11-09'}


def test_exact_minutes_and_missing_coverage(tmp_path: Path) -> None:
    """No interpolation, sampling or 1000-row projection limit is applied."""
    args = _fixture(tmp_path, 1385)
    data = candle_dataset(tmp_path/'data', args)
    assert len(data['series'][0]['candles']) == 1385
    assert data['series'][0]['coverage']['missing_minutes'] == 55
    assert data['interpolated'] is False
    args['end_time'] = '2026-11-08T00:02:00Z'
    assert len(candle_dataset(tmp_path/'data',args)['series'][0]['candles']) == 2


@pytest.mark.parametrize('change',[
    {'coins':['../secret']}, {'exchange':'../data'}, {'dataset':'../logs'},
    {'coins':['ADA','ADA']}, {'start_time':'2026-11-09'},
    {'start_time':'2026-11-08T00:00:00'}, {'start_time':'2026-11-08T00:00:00+01:00'},
    {'start_time':'2026-01-01','end_time':'2027-01-01'},
])
def test_reject_invalid_selection_before_reading(tmp_path: Path, change: dict) -> None:
    """Unsafe identifiers, ambiguous dates and excessive work are rejected."""
    args = _fixture(tmp_path);args.update(change)
    with pytest.raises(ValueError):candle_dataset(tmp_path/'data',args)


@pytest.mark.parametrize('target',['mapping','directory','archive'])
def test_reject_symlink_sources(tmp_path: Path, target: str) -> None:
    """No source component may redirect the broker outside approved data."""
    args = _fixture(tmp_path)
    paths = {'mapping':tmp_path/'data/coindata/hyperliquid/mapping.json',
             'directory':tmp_path/'data/ohlcv/hyperliquid/1m/ADA_USDC:USDC',
             'archive':tmp_path/'data/ohlcv/hyperliquid/1m/ADA_USDC:USDC/2026-11-08.npz'}
    path=paths[target];outside=tmp_path/'outside';path.rename(outside);path.symlink_to(outside,target_is_directory=outside.is_dir())
    with pytest.raises(ValueError,match='unsafe'):candle_dataset(tmp_path/'data',args)


def test_corrupt_archive_and_absent_candles(tmp_path: Path) -> None:
    """Corruption cannot be silently treated as a market gap."""
    args=_fixture(tmp_path)
    p=tmp_path/'data/ohlcv/hyperliquid/1m/ADA_USDC:USDC/2026-11-08.npz'
    p.write_bytes(b'not npz')
    with pytest.raises(ValueError,match='Invalid local candles'):candle_dataset(tmp_path/'data',args)
    p.unlink()
    with pytest.raises(ValueError,match='No local candles'):candle_dataset(tmp_path/'data',args)


def test_frozen_candles_approval_without_bubblewrap(tmp_path: Path, monkeypatch) -> None:
    """Approval runs exact >1000-row input while host files and networking remain blocked."""
    async def scenario() -> None:
        args=_fixture(tmp_path,1385)
        secret=tmp_path/'private.txt';secret.write_text('must not be read')
        monkeypatch.setattr(ai_capabilities,'PBGDIR',tmp_path)
        monkeypatch.setattr(ai_capabilities,'_BWRAP_PATH',tmp_path/'no-bwrap')
        service=AICapabilityService(tmp_path/'capabilities')
        args['code']=f'''import json,sys,socket
x=json.load(sys.stdin)
print(len(x['series'][0]['candles']))
for operation in [lambda:open({str(secret)!r}).read(),lambda:socket.socket(),lambda:open('/tmp/analysis-write-test','w')]:
    try:operation();print('UNSAFE')
    except PermissionError:print('blocked')
'''
        created=await service._propose_ohlcv_python_analysis('a'*32,'c'*32,args)
        proposal=service.proposals[created['proposal_id']]
        assert len(proposal.config['input_data']['series'][0]['candles'])==1385
        assert 'input_data' not in created['preview']
        assert 'workspace_roots' not in proposal.config
        assert created['preview']['input_resource']['series'][0]['coverage']['missing_minutes']==55
        assert str(tmp_path) not in json.dumps(created['preview']['input_resource'])
        # A source change after review never changes the approved snapshot.
        (tmp_path/'data/ohlcv/hyperliquid/1m/ADA_USDC:USDC/2026-11-08.npz').unlink()
        reloaded=AICapabilityService(tmp_path/'capabilities')
        pending=(await reloaded.list_proposals('a'*32,'c'*32))[0]
        result=await reloaded.approve('a'*32,created['proposal_id'],pending['payload_digest'],'c'*32)
        assert result['analysis_status']=='completed'
        assert result['output']['text'].strip().splitlines()==['1385','blocked','blocked','blocked']
    asyncio.run(scenario())


@pytest.mark.parametrize('mutation', ['nan', 'duplicate', 'wrong_day', 'inverted_range'])
def test_reject_invalid_candle_content(tmp_path: Path, mutation: str) -> None:
    """Malformed market values must never reach approved numerical input."""
    args = _fixture(tmp_path)
    path = tmp_path/'data/ohlcv/hyperliquid/1m/ADA_USDC:USDC/2026-11-08.npz'
    with np.load(path, allow_pickle=False) as archive:
        candles = archive['candles'].copy()
    if mutation == 'nan':
        candles['c'][0] = float('nan')
    elif mutation == 'duplicate':
        candles['ts'][1] = candles['ts'][0]
    elif mutation == 'wrong_day':
        candles['ts'][0] -= 86400000
    else:
        candles['h'][0] = .1
    np.savez_compressed(path, candles=candles)
    with pytest.raises(ValueError, match='Invalid local candles'):
        candle_dataset(tmp_path/'data', args)


def test_optional_config_snapshot_and_byte_limit(tmp_path: Path, monkeypatch) -> None:
    """Config context stays frozen with candles and oversized requests fail before proposal creation."""
    async def scenario() -> None:
        import ai_ohlcv_analysis
        args = _fixture(tmp_path)
        args.update(config_name='selected', config_kind='run', code='print(1)')
        monkeypatch.setattr(ai_capabilities, 'PBGDIR', tmp_path)
        service = AICapabilityService(tmp_path/'capabilities')
        calls = []
        def config(kind: str, selection: dict) -> dict:
            """Emulate the existing sanitized native config reader."""
            calls.append((kind, selection))
            return {'config': {'bot': {'long': {'risk': {'total_wallet_exposure_limit': 8}}}}, 'override_configs': {}}
        monkeypatch.setattr(service, '_get_managed_config', config)
        created = await service._propose_ohlcv_python_analysis('a'*32, 'c'*32, args)
        assert calls == [('run', {'version':'v8','name':'selected'})]
        proposal = service.proposals[created['proposal_id']]
        assert proposal.config['input_data']['config']['bot']['long']['risk']['total_wallet_exposure_limit'] == 8
        monkeypatch.setattr(ai_ohlcv_analysis, 'MAX_INPUT_BYTES', 1)
        with pytest.raises(AICapabilityError, match='split'):
            await service._propose_ohlcv_python_analysis('b'*32, 'd'*32, args)
        assert len(service.proposals) == 1
    asyncio.run(scenario())
