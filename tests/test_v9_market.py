"""PB9 market routes share native PB8 catalog and filter semantics, fully offline."""
import json

import pytest
from fastapi import HTTPException

from api import pb9, v9_market
from pb9_config import PB9CancelSignal, PB9ConfigError, _checked_response


@pytest.fixture
def catalog():
    """Include normal names, a qualified collision and a namespaced contract."""
    return {'contract_version':1,'exchanges':['bybit','hyperliquid'],
        'symbols':['BTC','HYPE','hyperliquid::1234'],
        'catalog':[{'config_id':value,'coin':coin,'display':coin,
                    'resolutions':[{'exchange':exchange,'symbol':coin+'/USDC:USDC'}]}
                   for value,coin,exchange in [('BTC','BTC','bybit'),('HYPE','HYPE','bybit'),
                                                ('hyperliquid::1234','xyz:TSLA','hyperliquid')]],
        'statuses':{'hyperliquid::0':{'input':'hyperliquid::0','normalized':'hyperliquid::0','status':'valid'}}}


@pytest.mark.parametrize('operation,body',[
    ('status',{'exchanges':['bybit','hyperliquid'],'coins':['HYPE','hyperliquid::0']}),
    ('symbols',{'exchange':'bybit'}),
    ('filter',{'exchange':'hyperliquid','market_cap':0,'vol_mcap':10,'only_cpt':False,'notices_ignore':False,'tags':''}),
])
def test_market_routes_share_native_catalog(monkeypatch,catalog,operation,body):
    """All editor consumers get native names; filters retain exact IDs and HIP-3 aliases."""
    seen=[]
    monkeypatch.setattr(pb9,'_read_paths',lambda:{'pb9dir':'/fixture/pb9','pb9venv':'/fixture/venv/bin/python'})
    def resolve(paths,exchanges,identifiers,cancel):
        """Assert explicit PB9 runtime/cancellation ownership at the existing boundary."""
        seen.append((exchanges,identifiers))
        assert paths['pb9dir']=='/fixture/pb9'
        assert isinstance(cancel,PB9CancelSignal)
        return catalog
    monkeypatch.setattr(v9_market,'get_market_identifiers',resolve)
    monkeypatch.setattr(v9_market.markets,'filter_symbols',lambda *args:(['btc','XYZ-TSLA'],['HYPE']))
    result=v9_market._operation(operation,PB9CancelSignal(),json.dumps(body).encode())
    if operation=='filter':
        assert result=={'approved':['BTC','hyperliquid::1234'],'ignored':['HYPE'],'unresolved':[]}
    else:
        assert result is catalog
    assert len(seen)==1
    assert seen[0][1]==body.get('coins',[])


def test_invalid_market_selection_fails_before_runtime(monkeypatch):
    """An HTTP selection cannot become a metadata or filesystem location."""
    monkeypatch.setattr(pb9,'_read_paths',lambda:pytest.fail('runtime accessed'))
    with pytest.raises(HTTPException) as error:
        v9_market._operation('symbols',PB9CancelSignal(),b'{"exchange":"../bybit"}')
    assert error.value.status_code==422


@pytest.mark.parametrize('invalid',['resolver','symbols','catalog','statuses','version'])
def test_market_response_is_source_bound_and_shape_checked(tmp_path,catalog,invalid):
    """Reject foreign resolver modules and malformed catalog envelopes."""
    src=tmp_path/'src'
    status={'pb9dir':str(tmp_path),'config_schema':'v9.0.0','package_version':'8.2.0.dev0',
            'commit':None,'commit_available':False}
    result={'config_schema':'v9.0.0','package_version':'8.2.0.dev0',
        'schema_module_file':str(src/'config/schema.py'),'version_module_file':str(src/'passivbot_version.py'),
        'resolver_module_file':str(src/'utils.py'),'market_catalog':catalog}
    assert _checked_response(json.dumps({'ok':True,'result':result}).encode(),status,
                             operation='market_identifiers')['market_catalog']==catalog
    if invalid=='resolver':result['resolver_module_file']=str(tmp_path.parent/'foreign-utils.py')
    elif invalid=='version':catalog['contract_version']=2
    else:catalog[invalid]=None
    with pytest.raises(PB9ConfigError) as error:
        _checked_response(json.dumps({'ok':True,'result':result}).encode(),status,operation='market_identifiers')
    assert error.value.error_codes==(('runtime_identity_mismatch' if invalid=='resolver' else 'invalid_response'),)
