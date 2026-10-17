"""Read-only support investigation excludes runtime files and treats evidence as untrusted."""
import asyncio
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import ValidationError

import support_diagnosis as diagnosis
from ai_chat import AIChatError


@pytest.fixture
def source_root(tmp_path):
    """Use synthetic installed source and secret runtime files outside the allowlist."""
    (tmp_path/'frontend').mkdir(); (tmp_path/'api').mkdir(); (tmp_path/'data').mkdir()
    (tmp_path/'frontend/services_monitor.html').write_text('function fetchStatus() {\n  return response.json();\n}\n')
    (tmp_path/'api/services.py').write_text('def status():\n    return {}\n')
    (tmp_path/'data/passwords.json').write_text('RUNTIME_SECRET')
    (tmp_path/'pbgui.ini').write_text('CONFIG_SECRET')
    (tmp_path/'frontend/link.js').symlink_to(tmp_path/'data/passwords.json')
    return tmp_path


def final_result():
    """Return a synthetic English diagnosis with explicit unverified validation."""
    return {'draft':{'subject':'Handle failed service status polls','description':'A failed request shows stopped services.','steps':'Return HTTP 503.','expected':'Show status unavailable.'},
            'analysis_markdown':'### Observations\nReported stopped cards.\n### Evidence\nE2: frontend/services_monitor.html:2.\n### Root cause / hypotheses\nLikely unchecked HTTP status.\n### Proposed fix\nCheck response.ok.\n### Validation still required\nNo tests were executed.',
            'questions':['Which PBGui version is affected?'],'confidence':'likely'}


def test_source_boundary_numbered_evidence_and_redaction(source_root):
    """Only installed code and explicitly provided diagnostic data can be read."""
    workspace=diagnosis.EvidenceWorkspace(source_root,{'error_context':{'message':'HTTP 503','detail':'api_key=SECRET_VALUE'},'logs':{'PBGui.log':['status failed api_key=SECRET_VALUE']}})
    assert 'frontend/services_monitor.html' in workspace.files
    assert 'data/passwords.json' not in workspace.files and 'frontend/link.js' not in workspace.files
    assert 'CONFIG_SECRET' not in json.dumps(workspace.diagnostics)
    assert 'HTTP 503' in workspace.diagnostics['error_context']
    assert 'SECRET_VALUE' not in json.dumps(workspace.diagnostics)
    read=workspace.read({'tool':'read_source','path':'frontend/services_monitor.html','offset':2,'count':1})
    assert read['lines']==[{'line':2,'text':'  return response.json();'}]
    assert read['sha256']==hashlib.sha256((source_root/'frontend/services_monitor.html').read_bytes()).hexdigest()
    for path in ['../pbgui.ini','data/passwords.json','frontend/link.js','/etc/passwd']:
        assert 'error' in workspace.read({'tool':'read_source','path':path})
    assert 'error' in workspace.read({'tool':'read_diagnostic','path':'logs/missing'})
    with pytest.raises(ValidationError):
        workspace.read({'tool':'shell','command':'cat pbgui.ini'})


def test_source_permission_and_swapped_symlink(source_root):
    """Source opt-out and late symlink changes cannot expose non-code data."""
    disabled=diagnosis.EvidenceWorkspace(source_root,inspect_source=False)
    assert not disabled.files
    workspace=diagnosis.EvidenceWorkspace(source_root)
    path=source_root/'api/services.py'; path.unlink(); path.symlink_to(source_root/'data/passwords.json')
    assert 'error' in workspace.read({'tool':'read_source','path':'api/services.py'})


def test_iterative_diagnosis_pinned_model_and_concrete_evidence(source_root,monkeypatch):
    """The model can request real bounded reads, then produce a diagnosis without publishing."""
    replies=[json.dumps({'requests':[{'tool':'list_sources','query':'services'}]}),
             json.dumps({'requests':[{'tool':'read_source','path':'frontend/services_monitor.html','offset':1,'count':3}]}),json.dumps(final_result())]
    model=AsyncMock(side_effect=[(reply,'fixture','selected-model') for reply in replies])
    monkeypatch.setattr(diagnosis,'provider_reply',model)
    chat=SimpleNamespace(get_preferences=Mock(return_value={'selection':{'provider':'fixture','model':'selected-model'}}))
    body=diagnosis.DiagnosisRequest(description='Alle Dienste zeigen Stopped',diagnostics={'error_context':{'message':'503'}})
    result=asyncio.run(diagnosis.diagnose(chat,'owner',body,source_root))
    assert result['confidence']=='likely' and result['investigation']=={'steps':3,'read_only':True,'tests_executed':False,'source_files':2}
    assert result['evidence'][-1]['path']=='frontend/services_monitor.html'
    assert result['evidence'][-1]['sha256']
    assert all(call.kwargs['selection']=={'provider':'fixture','model':'selected-model'} for call in model.call_args_list)
    assert model.call_args_list[-1].kwargs['content']['evidence'][1]['lines'][1]['line']==2
    assert 'cannot run code, tests' in model.call_args.kwargs['instructions']
    assert (source_root/'frontend/services_monitor.html').read_text().startswith('function fetchStatus')


@pytest.mark.parametrize('reply', [json.dumps({'requests':[{'tool':'publish_issue'}]}),json.dumps({'requests':[]}), 'not JSON',json.dumps({'draft':{}})])
def test_invalid_model_actions_never_escape_read_only_boundary(source_root,monkeypatch,reply):
    """Unsupported actions and malformed replies leave files untouched and publish nothing."""
    monkeypatch.setattr(diagnosis,'provider_reply',AsyncMock(return_value=(reply,'fixture','fixture')))
    chat=SimpleNamespace(get_preferences=Mock(return_value={}))
    with pytest.raises(AIChatError):
        asyncio.run(diagnosis.diagnose(chat,'owner',diagnosis.DiagnosisRequest(description='Problem'),source_root))
    assert (source_root/'data/passwords.json').read_text()=='RUNTIME_SECRET'


def test_unsubstantiated_confirmation_and_maximum_output(source_root,monkeypatch):
    """An unsupported confirmation is downgraded without exceeding UI output limits."""
    reply=final_result()
    reply.update(confidence='confirmed',analysis_markdown='A'*24000)
    monkeypatch.setattr(diagnosis,'provider_reply',AsyncMock(return_value=(json.dumps(reply),'fixture','fixture')))
    chat=SimpleNamespace(get_preferences=Mock(return_value={}))
    result=asyncio.run(diagnosis.diagnose(chat,'owner',diagnosis.DiagnosisRequest(description='Problem'),source_root))
    assert result['confidence']=='unknown'
    assert len(result['analysis_markdown'])<=24000
