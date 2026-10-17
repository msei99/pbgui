"""Native chat support capabilities retain read boundaries and explicit publication approval."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from ai_capabilities import AICapabilityError, AICapabilityService
from ai_support_tools import ChatSupportTools
from support_github import SupportGitHubService, SupportGitHubStore

OWNER='a'*32
CHAT='b'*32


@pytest.fixture
def support(tmp_path,monkeypatch):
    """Use only synthetic source, diagnostics and an isolated encrypted GitHub vault."""
    root=tmp_path/'installed'; (root/'frontend').mkdir(parents=True)
    (root/'frontend/dashboard.html').write_text('function draw() {\n  return "dashboard";\n}\n')
    (root/'data').mkdir(); (root/'data/secret.json').write_text('NEVER_READ')
    capabilities=AICapabilityService(tmp_path/'capabilities')
    capabilities.support_tools=ChatSupportTools(capabilities,root)
    github=SupportGitHubService(SupportGitHubStore(tmp_path/'vault'))
    github.store.write(OWNER,{'login':'fixture-user','token':'ghp_FIXTURE_TOKEN_PRIVATE'})
    monkeypatch.setattr('ai_support_tools.get_support_github_service',lambda:github)
    monkeypatch.setattr('support_github.get_support_github_service',lambda:github)
    monkeypatch.setattr('ai_chat.get_ai_chat_service',lambda:SimpleNamespace(accepting_turns=True,active_tasks={}))
    monkeypatch.setattr(capabilities,'_runtime_fingerprints',lambda:{})
    return capabilities,github


def test_chat_reads_source_and_redacted_diagnostics_without_debug_ui(support,monkeypatch):
    """Direct native tool dispatch is bounded, owner-scoped and never executes commands."""
    capabilities,_=support
    seen=[]
    def snapshot(owner,loop_id=None):
        """Return a synthetic report while checking the trusted owner boundary."""
        seen.append((owner,loop_id))
        return {'created_at':123,'system':{'version':'fixture'},'logs':{'PBGui.log':['failed api_key=PRIVATE_VALUE']}}
    monkeypatch.setattr(capabilities.support_tools,'snapshot',snapshot)
    async def scenario():
        """Run the same model-visible tools used by all native chat providers."""
        source=await capabilities.dispatch(OWNER,CHAT,'read_pbgui_support_evidence',{'tool':'read_source','path':'frontend/dashboard.html','offset':2,'count':1})
        assert source['lines']==[{'line':2,'text':'  return "dashboard";'}]
        denied=await capabilities.dispatch(OWNER,CHAT,'read_pbgui_support_evidence',{'tool':'read_source','path':'data/secret.json'})
        assert 'error' in denied
        catalog=await capabilities.dispatch(OWNER,CHAT,'read_pbgui_support_evidence',{'tool':'read_diagnostic','query':'PBGui'})
        assert catalog['diagnostic_keys']==['logs/PBGui.log']
        record=await capabilities.dispatch(OWNER,CHAT,'read_pbgui_support_evidence',{'tool':'read_diagnostic','path':'logs/PBGui.log'})
        assert 'PRIVATE_VALUE' not in record['text'] and 'failed' in record['text']
        with pytest.raises(AICapabilityError):
            await capabilities.dispatch(OWNER,CHAT,'read_pbgui_support_evidence',{'tool':'shell','path':'/etc/passwd'})
    asyncio.run(scenario())
    assert seen==[(OWNER,None),(OWNER,None)]


def test_chat_prepares_exact_issue_and_publishes_only_after_owner_approval(support,monkeypatch):
    """Preparing and reviewing an English issue makes zero external writes."""
    capabilities,github=support
    publish=AsyncMock(return_value={'status':'published','issue_url':'https://github.com/msei99/pbgui/issues/123'})
    monkeypatch.setattr(github,'publish',publish)
    async def scenario():
        """Prepare, validate review identity, approve once and reuse the durable result."""
        result=await capabilities.dispatch(OWNER,CHAT,'propose_github_issue',{'title':'Add yearly dashboard ranges','body':'### Feature request\nSelect years and all-time in the dashboard.\nSee https://github.com/msei99/pbgui/issues/422\napi_key=SECRET_VALUE'})
        assert publish.await_count==0
        proposals=await capabilities.list_proposals(OWNER,CHAT)
        proposal=proposals[0]
        assert proposal['preview']['body'].startswith('### Feature request')
        assert 'https://github.com/msei99/pbgui/issues/422' in proposal['preview']['body']
        assert 'SECRET_VALUE' not in proposal['preview']['body']
        assert proposal['preview']['target']=='msei99/pbgui'
        assert 'ghp_FIXTURE' not in json.dumps(proposal)
        assert 'NEVER_READ' not in json.dumps(proposal)
        with pytest.raises(AICapabilityError):
            await capabilities.approve('c'*32,result['proposal_id'],proposal['payload_digest'],CHAT)
        with pytest.raises(AICapabilityError):
            await capabilities.approve(OWNER,result['proposal_id'],'tampered',CHAT)
        assert publish.await_count==0
        approved=await capabilities.approve(OWNER,result['proposal_id'],proposal['payload_digest'],CHAT)
        assert approved['status']=='executed' and approved['publication_status']=='published'
        assert approved['issue_url'].endswith('/123')
        assert not list(capabilities.journal_root.glob('*.json'))
        again=await capabilities.approve(OWNER,result['proposal_id'],proposal['payload_digest'],CHAT)
        assert again==approved and publish.await_count==1
        publish.assert_awaited_once_with(OWNER,proposal['preview']['review_id'])
    asyncio.run(scenario())


def test_disconnected_chat_never_requests_credentials_in_messages(support):
    """The assistant directs the user to the chat's password control rather than token text."""
    capabilities,github=support
    github.store.disconnect(OWNER)
    result=asyncio.run(capabilities.dispatch(OWNER,CHAT,'propose_github_issue',{'title':'Feature','body':'Description'}))
    assert result['status']=='github_connection_required'
    assert 'Never ask for a token in chat' in result['message']
    assert not list(github.store.root.glob(OWNER+'-????????????????????????????????.vault'))


def test_uncertain_publication_resumes_same_receipt(support,monkeypatch):
    """Recovery prepares another approval for the same frozen record, never a replacement."""
    capabilities,github=support
    async def scenario():
        """Exercise native proposal resumption after a lost GitHub response."""
        review=await github.prepare(OWNER,'Observed failure','Observed behavior and possible cause.')
        row=github.store.review(OWNER,review['review_id'])
        row.update(status='uncertain',phase='issue')
        github.store.write(OWNER,row,review['review_id'])
        result=await capabilities.dispatch(OWNER,CHAT,'propose_github_issue',{'review_id':review['review_id']})
        assert result['preview']['reconcile_only'] is True
        assert result['preview']['body']==review['body']
        assert len(list(github.store.root.glob(OWNER+'-????????????????????????????????.vault')))==1
        with pytest.raises(AICapabilityError):
            await capabilities.dispatch('c'*32,'d'*32,'propose_github_issue',{'review_id':review['review_id']})
    asyncio.run(scenario())


def test_catalog_advertises_support_on_all_native_protocols(support):
    """Support tools appear alongside existing capabilities without PB8 installation requirements."""
    capabilities,_=support
    names={item['name'] for item in capabilities.capability_registry()['capabilities']}
    assert {'read_pbgui_support_evidence','get_github_support_status','propose_github_issue'}<=names
    for tools in [capabilities.responses_tools(),capabilities.chat_completion_tools(),capabilities.messages_tools()]:
        assert 'propose_github_issue' in json.dumps(tools)


def test_interrupted_publication_is_not_replayed_at_startup(support,monkeypatch):
    """A restarted API records interruption and requires a new review of the same receipt."""
    capabilities,github=support
    async def scenario():
        """Simulate a lost process after approval without doing any network work."""
        result=await capabilities.dispatch(OWNER,CHAT,'propose_github_issue',{'title':'Failure','body':'Observed failure.'})
        proposal=capabilities.proposals[result['proposal_id']]
        proposal.status='executing';capabilities._persist_proposal(proposal)
        restored=AICapabilityService(capabilities.root)
        publish=AsyncMock();monkeypatch.setattr(github,'publish',publish)
        await restored.startup()
        restored._load_proposals_for_owner(OWNER)
        assert restored.proposals[proposal.id].status=='interrupted'
        assert publish.await_count==0
    asyncio.run(scenario())


def test_analysis_only_chat_cannot_publish_support_issues(support):
    """Adding support tools does not weaken the existing research-derived action boundary."""
    capabilities,_=support
    capabilities.restrict_to_analysis(OWNER,CHAT)
    with pytest.raises(AICapabilityError,match='analysis-only'):
        asyncio.run(capabilities.dispatch(OWNER,CHAT,'propose_github_issue',{'title':'Feature','body':'Description'}))


@pytest.mark.parametrize('extra',[{'token':'NEVER_SEND'},{'target':'foreign/repository'},{'attach_diagnostics':'true'}])
def test_model_cannot_supply_credentials_targets_or_coerced_attachment_permissions(support,extra):
    """Closed strict issue arguments reject credentials, foreign targets and string booleans."""
    capabilities,_=support
    with pytest.raises(AICapabilityError,match='Invalid GitHub issue proposal'):
        asyncio.run(capabilities.dispatch(OWNER,CHAT,'propose_github_issue',{'title':'Feature','body':'Description',**extra}))
