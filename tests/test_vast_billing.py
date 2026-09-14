"""Offline provider billing projection and refresh ownership tests."""
import json
from types import SimpleNamespace

from vast_jobs import JobStore, write_json
from secure_files import ensure_private_directory
from vast_provider import VastError
from vast_billing import instance_charges


def test_instance_billing_is_scoped_cached_and_preserved(tmp_path, monkeypatch):
    """Do not count foreign instances, double-count components, or erase on failure."""
    import vast_billing as module
    store=JobStore(tmp_path/'vast'); lease='a'*32
    directory=ensure_private_directory(store.root/'jobs'/lease)
    write_json(directory/'state.json',{'id':lease,'instance_id':123,'status':'running'})
    write_json(directory/'intent.json',{'accepted_at':1000})
    clock=[2000]; calls=[]; failing=[False]
    monkeypatch.setattr(module.time,'time',lambda:clock[0])
    monkeypatch.setattr(module,'VastCredentialStore',lambda root:SimpleNamespace(secrets=lambda:{'api_key':'fake'}))
    def request(method,path,body):
        """Return only synthetic account billing records."""
        calls.append((method,path,body))
        assert json.loads(body['select_filters'])['type']=={'in':['instance']}
        if failing[0]: raise VastError('permission unavailable')
        return {'results':[
            {'source':'instance-999','type':'instance','amount':999},
            {'source':'instance-123','type':'instance','amount':.044,
             'metadata':{'private':'must not expose'},'items':[{'type':'gpu','amount':.034},{'type':'disk','amount':.01}]}]}
    monkeypatch.setattr(module,'VastClient',lambda key:SimpleNamespace(request=request))
    result=instance_charges(store,lease)
    assert result['amount_usd']==.044 and result['breakdown']=={'gpu':.034,'disk':.01}
    assert 'metadata' not in str(result) and '999' not in str(result)
    assert instance_charges(store,lease)==result and len(calls)==1
    failing[0]=True; clock[0]=2400
    result=instance_charges(store,lease)
    assert result['amount_usd']==.044 and result['reported_at']==2000
    assert result['error']=='permission unavailable'
    assert len(calls)==2
