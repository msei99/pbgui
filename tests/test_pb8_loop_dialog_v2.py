"""Offline three-protocol evidence dialogs, recovery budgets and durable resumes."""
import asyncio
import copy
import json
import math
from types import SimpleNamespace

import pytest

from pb8_loop_ai_policy import DecisionLedger, ModelProfiles, check_context
from pb8_loop_dialog import OpenCodeDialog, read_sse
from pb8_loop_evidence import EvidenceStore, EvidenceSession, LoopAIPause, encode, tokens
from pb8_loop_archive import archive_budget, thin_with_extremes, select_sources
from tests.test_pb8_loop_optimizer import OWNER, RULE, record


def profile():
    """A measured-shaped offline fixture, never a production acceptance record."""
    return {'context': 256000, 'page_tokens': 5000, 'output_tokens': 1000,
            'streaming': False, 'call_timeout': .1}


def frozen(record, required=None):
    """Create only isolated temporary evidence."""
    store, row = record
    evidence = EvidenceStore(store)
    value = evidence.create(row, required or {'stage':'select', 'safety':{'liquidated':0}}, {'stage':'select'}, [])
    return EvidenceSession(evidence, value, 5000)


@pytest.mark.parametrize('protocol', ['chat', 'responses', 'messages'])
@pytest.mark.parametrize('needs_evidence', [False, True])
def test_requested_stream_can_return_complete_json_without_second_request(record, protocol, needs_evidence):
    """Use an ordinary JSON reply when a provider ignores streaming, without probes."""
    from pb8_loop_ai import LoopAI
    from pb8_loop_ai_v2 import _ACTIVE, provider_request

    store, row = record
    row['settings'].update(provider='opencode-go', effort='', service_tier='')
    metadata = {'protocol': protocol, 'context': 256000}
    runtime_settings = {**profile(), 'streaming': True,
                        'connect_timeout': 1, 'first_timeout': 1, 'idle_timeout': 1}
    session = frozen(record)
    ledger = DecisionLedger(store, row, 'select', session.snapshot['sha256'], metadata)
    answer = '{"candidate_ids":["selected"]}'
    replies = {
        'chat': {'choices': [{'finish_reason': 'stop', 'message': {'content': answer}}]},
        'responses': {'status': 'completed', 'output': [
            {'type': 'message', 'content': [{'type': 'output_text', 'text': answer}]}]},
        'messages': {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': answer}]},
    }
    sent, closed, decoded = [], [], []

    class Response:
        """A complete JSON response with no SSE byte stream."""
        status = 200
        headers = {'Content-Type': 'application/json; charset=utf-8'}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            closed.append(True)

    class HTTP:
        """Capture the actual request without any network access."""
        def post(self, url, **kwargs):
            sent.append(copy.deepcopy(kwargs['json']))
            return Response()

    class Chat:
        """Only provide deterministic test credentials and one ordinary reply."""
        credentials = SimpleNamespace(load_go_key=lambda owner: 'test-only-key')

        async def _http_session(self):
            return HTTP()

        async def _read_json_response(self, response, **kwargs):
            decoded.append(True)
            if needs_evidence and len(decoded) == 1:
                arguments = {'path': '/data', 'offset': 0}
                return {
                    'chat': {'choices': [{'finish_reason': 'tool_calls', 'message': {'role': 'assistant',
                        'tool_calls': [{'id': 'read-1', 'type': 'function', 'function': {
                            'name': 'loop_evidence', 'arguments': encode(arguments)}}]}}]},
                    'responses': {'status': 'completed', 'output': [{'type': 'function_call',
                        'call_id': 'read-1', 'name': 'loop_evidence', 'arguments': encode(arguments)}]},
                    'messages': {'stop_reason': 'tool_use', 'content': [{'type': 'tool_use',
                        'id': 'read-1', 'name': 'loop_evidence', 'input': arguments}]},
                }[protocol]
            return {**replies[protocol], 'usage': {'input_tokens': 40, 'output_tokens': 20}}

    ai = LoopAI(store, Chat())

    async def run():
        """Enter the real adapter with a temporary owning evidence decision."""
        token = _ACTIVE.set({'profile': runtime_settings, 'session': session, 'ledger': ledger,
                             'instructions': 'Select a candidate.', 'validate': None})
        try:
            return await provider_request(ai, row, metadata, {'stage': 'select'}, 1000)
        finally:
            _ACTIVE.reset(token)

    text, _ = asyncio.run(run())
    assert text == answer
    expected_calls = 2 if needs_evidence else 1
    assert len(sent) == len(closed) == len(decoded) == expected_calls
    assert sent[0]['stream'] is True and runtime_settings['streaming'] is False
    if needs_evidence:
        assert 'stream' not in sent[1] and 'stream_options' not in sent[1]
    cached = ai.profiles.load({**row['settings'], 'profile_owner': OWNER}, metadata)
    assert cached['streaming'] is False
    assert len(ledger.state()['calls']) == expected_calls


@pytest.mark.parametrize('protocol', ['chat','responses','messages'])
def test_complete_tool_dialog_preserves_ids_reasoning_and_usage(record, protocol):
    """The second request must contain the exact native call ID and retained thinking."""
    store, row = record
    row = store.update(OWNER, row['id'], lambda current: current['settings'].update(provider='opencode-go'))
    session = frozen((store,row))
    ledger = DecisionLedger(store,row,'select',session.snapshot['sha256'],{'protocol':protocol})
    calls = []
    arguments = {'path':'/required', 'offset':0}
    async def send(body, _profile, _remaining):
        """Return one exact read followed by a completed final decision."""
        calls.append(copy.deepcopy(body))
        usage = {'input_tokens':100*len(calls), 'output_tokens':20}
        if len(calls) == 1:
            if protocol == 'chat':
                return {'usage':usage,'choices':[{'finish_reason':'tool_calls','message':{'role':'assistant','content':None,
                    'reasoning_content':'retained thinking','tool_calls':[{'id':'native-7','type':'function',
                    'function':{'name':'loop_evidence','arguments':encode(arguments)}}]}}]}
            if protocol == 'responses':
                return {'usage':usage,'status':'completed','output':[{'type':'reasoning','id':'r1','summary':[]},
                    {'type':'function_call','call_id':'native-7','name':'loop_evidence','arguments':encode(arguments)}]}
            return {'usage':usage,'stop_reason':'tool_use','content':[{'type':'thinking','thinking':'retained thinking','signature':'sig'},
                    {'type':'tool_use','id':'native-7','name':'loop_evidence','input':arguments}]}
        if protocol == 'chat':
            return {'usage':usage,'choices':[{'finish_reason':'stop','message':{'role':'assistant','content':'{"ok":true}'}}]}
        if protocol == 'responses':
            return {'usage':usage,'status':'completed','output':[{'type':'message','content':[{'type':'output_text','text':'{"ok":true}'}]}]}
        return {'usage':usage,'stop_reason':'end_turn','content':[{'type':'text','text':'{"ok":true}'}]}
    result = asyncio.run(OpenCodeDialog(row['settings'], {'protocol':protocol}, profile(), session, ledger, send).run('Read evidence', session.snapshot['required'], 1000))
    assert json.loads(result) == {'ok':True}
    assert len(calls) == 2 and 'native-7' in encode(calls[1])
    assert ('retained thinking' if protocol != 'responses' else 'r1') in encode(calls[1])
    state = ledger.state()
    assert len(state['calls']) == 2 and state['tokens'] == 340
    assert all(call['usage_confirmed'] for call in state['calls'])
    from pb8_loop_evidence import sha
    assert [call['request_sha256'] for call in state['calls']] == [sha(body) for body in calls]


def test_optional_reads_reserve_exact_mandatory_recovery(record):
    """Optional budget notices preserve A recovery without aborting the decision."""
    session = frozen(record, {'stage':'evaluate','values':list(range(15000))})
    assert len(session.required_pages) > 1
    session.calls = 60 - len(session.required_pages)
    session.total_calls = session.calls
    notice = session.read({'path':'/data','offset':0}, 'optional-limit')
    assert notice['error'] == 'optional_evidence_budget_reserved'
    assert notice['can_finalize'] and notice['required_reads'] == []
    assert session.calls + len(session.required_pages) <= 64
    session.compacted()
    for page in session.required_pages:
        session.read({'path':'/required','offset':page['offset']}, 'recovery-'+str(page['offset']))
    assert session.missing() == []


def test_direct_subsection_reads_cover_all_required_pages(record):
    """Coverage tracks leaves, not only canonical /required page signatures."""
    session = frozen(record, {'stage':'select','a':{'value':7},'b':[None,0]})
    session.compacted()
    for index,path in enumerate(['/required/stage','/required/a','/required/b']):
        session.read({'path':path,'offset':0},str(index))
    assert session.missing() == []


def test_invalid_tool_calls_consume_quota(record):
    """An endless sequence of malformed arguments cannot bypass the hard allowance."""
    session = frozen(record)
    for index in range(64):
        with pytest.raises(ValueError):
            session.read({'offset':0},str(index))
    with pytest.raises(LoopAIPause):
        session.read({'offset':0},'65')


def test_findings_index_retains_archives_beyond_mutable_ui_index(record):
    """Historical originals are referenced once and remain tool-readable after UI eviction."""
    store,row = record
    evidence = EvidenceStore(store)
    entry = {'id':'old','loop':row['id'],'status':'observed_exact','created_at':1,
             'fingerprint':row['fingerprint'],'coins':row['settings']['goals']['coins'],
             'knowledge':'x'*1001,'observations':[{'exact':True,'metrics':{'gain':3}}]}
    evidence.create(row,{}, {'stage':'evaluate'}, [entry])
    entries = evidence.findings(row)
    assert len(entries) == 1 and 'observations' not in entries[0]
    assert 'knowledge' not in entries[0] and entries[0]['knowledge_reference']['characters'] == 1001
    snapshot = evidence.create(row,{}, {'stage':'evaluate'}, entries)
    assert evidence.resolve(snapshot,'/history/old/observations/0/metrics/gain') == 3
    assert evidence.findings(row) == entries


def test_native_archive_budget_is_visible_and_does_not_mutate_source(record):
    """Only the own-job effective limit derives from the verified model/rubric."""
    _,row = record
    row['ai_model_profile'] = profile()
    row['rubric'] = RULE
    row['initial_config']['optimize']['pareto_max_size'] = 100000
    before = copy.deepcopy(row)
    bound = archive_budget(row,row['initial_config'])
    assert 0 < bound['effective'] < 100000
    assert bound['effective'] >= bound['extrema_capacity'] and row == before


@pytest.mark.parametrize('bad', [None, float('nan'), float('inf')])
def test_historical_thinning_never_guesses_missing_or_nonfinite_semantics(bad):
    """Such a source belongs in optional B instead of failing bootstrap."""
    rows = [{'id':str(i),'native_semantics':{'scoring':[{'metric':'gain','goal':'max'}],
            'constraint_violation':0,'objectives':{'gain':bad}}} for i in range(3)]
    with pytest.raises(ValueError):
        thin_with_extremes(rows,1)


def test_extreme_points_may_exceed_requested_historical_limit():
    """Native minima and maxima remain protected even for requested limit one."""
    rows = [{'id':str(i),'native_semantics':{'scoring':[{'metric':'gain','goal':'max'}, {'metric':'risk','goal':'min'}],
            'constraint_violation':0,'objectives':{'gain':a,'risk':b}}} for i,(a,b) in enumerate([(0,2),(2,0),(3,2),(2,3),(2,2)])]
    assert {row['id'] for row in thin_with_extremes(rows,1)} == {'0','1','2','3'}


def test_runtime_settings_need_no_model_acceptance(record):
    """The model catalog is sufficient; automatic hang protection is finite."""
    store,row = record
    value = ModelProfiles(store.root).load({**row['settings'], 'provider':'opencode-go'},
                                         {'protocol':'chat','context':256000,'loop_tool_support':True})
    assert value['context'] == 256000 and value['streaming']
    assert all(math.isfinite(value[key]) and value[key]>0 for key in
               ['connect_timeout','first_timeout','idle_timeout','call_timeout'])


def test_recovery_budget_formula_uses_profile_reserves():
    """Page capacity is H minus reserves divided by measured recovery passes."""
    measured = {**profile(), 'optional_call_reserve':12, 'recovery_call_reserve':4, 'recovery_passes':2}
    check_context({},measured,1000,codex=True,required_pages=24)
    with pytest.raises(LoopAIPause):
        check_context({},measured,1000,codex=True,required_pages=25)


def test_sse_pings_do_not_extend_first_meaningful_progress_deadline():
    """Continuous keepalives cannot keep a dead model call alive."""
    class Pings:
        """Provide bytes but no meaningful model event."""
        async def readany(self):
            """Keep the connection busy without model progress."""
            await asyncio.sleep(.003)
            return b': ping\n\ndata: {"type":"ping"}\n\n'
    with pytest.raises(TimeoutError):
        asyncio.run(read_sse(Pings(),'messages',{'first_timeout':.015,'idle_timeout':.015}))


@pytest.mark.parametrize('reread',[False,True])
def test_codex_thread_compaction_requires_complete_native_tool_recovery(record,tmp_path,monkeypatch,reread):
    """A real CodexRuntime reader processes thread-only compaction and owned tool calls."""
    from tests.test_pb8_loop_gpt_timeout import native_ai, stop_feeders
    store,row = record
    async def feed(runtime):
        """Emit native-shaped notifications and optionally recover every required page."""
        runtime.notifications.put_nowait({'method':'thread/compacted','params':{'threadId':'thread'}})
        await asyncio.sleep(.01)
        if reread:
            offset = 0
            while offset is not None:
                reply = await runtime.tool_handler({'tool':'loop_evidence','threadId':'thread','turnId':'turn',
                    'callId':'recovery-'+str(offset),'arguments':{'path':'/required','offset':offset}})
                assert reply['success']
                offset = json.loads(reply['contentItems'][0]['text'])['next_offset']
        runtime.notifications.put_nowait({'method':'item/completed','params':{'turnId':'turn',
            'item':{'type':'agentMessage','text':'{"winner_operation":null,"finish":true}'}}})
        runtime.notifications.put_nowait({'method':'turn/completed','params':{'turnId':'turn',
            'turn':{'id':'turn','status':'completed'}}})
    ai,runtimes,feeders = native_ai(store,tmp_path,monkeypatch,feed)
    async def exercise():
        """Always join test-owned producers, also on a required recovery pause."""
        try:
            if reread:
                assert await ai.decide(row,'evaluate',{}) == {'winner_operation':None,'finish':True}
            else:
                with pytest.raises(LoopAIPause) as error:
                    await ai.decide(row,'evaluate',{})
                assert error.value.reason == 'evidence_recovery'
        finally:
            await stop_feeders(feeders)
    asyncio.run(exercise())
    saved = store.read(OWNER,row['id'])
    assert bool(saved.get('last_ai_decision')) == reread
    assert all(runtime.close.await_count == 1 for runtime in runtimes)


def test_recovery_resume_validates_actual_new_provider_payload_before_unpausing(record,monkeypatch):
    """The same overflowing request stays paused; no provider or rental action occurs."""
    from api import loop_optimizer_v8 as api
    from pb8_loop_controller import LoopController
    from tests.test_pb8_loop_optimizer import FakeBackend
    store,row = record
    evidence = EvidenceStore(store)
    snap = evidence.create(row,{'stage':'evaluate','large':'0123456789'*100000}, {'stage':'evaluate'}, [])
    row = store.update(OWNER,row['id'],lambda current:current.update(status='paused',ai_snapshot_id=snap['id'],
                      ai_pause={'reason':'context_budget','message':'too large'}))
    class AI:
        """Only return an explicit isolated model contract."""
        async def preflight(self,*args,**kwargs):
            """Return only fixture metadata without provider IO."""
            return {'loop_profile':profile(),'protocol':'chat','output_limit':1000}
    monkeypatch.setattr(api,'_controller',LoopController(store.root,store=store,backend=FakeBackend(),ai=AI()))
    with pytest.raises(Exception) as error:
        asyncio.run(api._loop_action_owned(OWNER,row['id'],api.LoopAction(action='resume')))
    assert error.value.status_code == 409
    assert error.value.detail['reason'] == 'context_budget'
    assert store.read(OWNER,row['id'])['status'] == 'paused'


def test_evicted_knowledge_id_never_overwrites_immutable_original(record):
    """Retrying an old ID after index eviction must preserve the original file bytes."""
    from secure_files import atomic_write_private_text
    from pb8_loop_store import digest
    store,row = record
    store.remember(row,'old-original',{'knowledge':'original'})
    path = store.owner_dir(OWNER)/'evidence'/(digest('old-original')+'.json')
    original = path.read_bytes()
    atomic_write_private_text(store.owner_dir(OWNER)/'knowledge.json',json.dumps({'entries':[],'used_holdouts':[]}))
    store.remember(row,'old-original',{'knowledge':'replacement'})
    assert path.read_bytes() == original


def test_context_telemetry_is_automatic_owner_bound_cache(record):
    """Normal runtime values replace the planning floor without a gate file."""
    store,row = record
    profiles = ModelProfiles(store.root)
    selection = {**row['settings'],'profile_owner':OWNER}
    profiles.observe_window(selection,{},400000)
    value = profiles.load(selection,{})
    assert value['context']==400000 and value['context_observed_at']>0
    assert profiles.load({**selection,'profile_owner':'b'*32},{})['context_pending']


@pytest.mark.parametrize('wire',[b'data: []\n\n',b'data: {broken}\n\n',b'data: \xff\n\n'])
def test_malformed_streams_are_incomplete_transport_not_partial_winners(wire):
    """Malformed Unicode/JSON cannot escape to tool execution or commit."""
    from pb8_loop_dialog import IncompleteResponse
    class Reader:
        """Deliver one bounded malformed response."""
        async def readany(self):
            """Return the malformed frame without a real stream."""
            return wire
    with pytest.raises(IncompleteResponse):
        asyncio.run(read_sse(Reader(),'chat',{'first_timeout':.1,'idle_timeout':.1}))


def test_tool_allowance_survives_logical_decision_reconstruction(record):
    """A fresh session/retry cannot replenish the 128-call persisted ceiling."""
    store,row = record
    first = DecisionLedger(store,row,'select','frozen',{})
    store.update(OWNER,row['id'],lambda current: current['ai_decisions'][first.key].update(tool_calls=127))
    rebuilt = DecisionLedger(store,store.read(OWNER,row['id']),'select','frozen',{})
    rebuilt.reserve_tool()
    with pytest.raises(LoopAIPause):
        first.reserve_tool()
    assert rebuilt.state()['tool_calls'] == 128


@pytest.mark.parametrize('protocol', ['chat', 'responses', 'messages'])
def test_json_reader_supports_models_without_native_tools(record, protocol):
    """The ordinary decision reads exact evidence without a model approval or native tools."""
    store,row = record
    row = store.update(OWNER,row['id'],lambda current: current['settings'].update(provider='opencode-go'))
    session = frozen((store,row))
    metadata = {'protocol':protocol,'context':256000,'loop_tool_support':False}
    runtime = ModelProfiles(store.root).load(row['settings'],metadata)
    ledger = DecisionLedger(store,row,'select',session.snapshot['sha256'],metadata)
    calls=[]
    def reply(text):
        """Return a normal text response in the selected native envelope."""
        if protocol=='chat':
            return {'choices':[{'message':{'role':'assistant','content':text}}]}
        if protocol=='responses':
            return {'output':[{'type':'message','content':[{'type':'output_text','text':text}]}]}
        return {'content':[{'type':'text','text':text}]}
    async def send(body,*_args):
        """One ordinary read request followed by the real decision answer."""
        calls.append(copy.deepcopy(body))
        assert 'tools' not in body
        if len(calls)==1:
            return reply('{"loop_evidence":{"path":"/required","offset":0}}')
        assert 'loop_evidence_results' in encode(body)
        assert session.snapshot['sha256'] in encode(body)
        return reply('{"ok":true}')
    answer=asyncio.run(OpenCodeDialog(row['settings'],metadata,runtime,session,ledger,send)
                       .run('Read exact data.',session.snapshot['required'],1000))
    assert json.loads(answer)=={'ok':True} and len(calls)==2 and session.total_calls==1


@pytest.mark.parametrize('unavailable', ['streaming', 'tools'])
def test_normal_request_negotiates_unsupported_transport_without_probe(record, unavailable):
    """A specific provider rejection adjusts the actual decision rather than blocking its model."""
    from pb8_loop_dialog import StreamingUnavailable,NativeToolsUnavailable
    store,row=record
    row=store.update(OWNER,row['id'],lambda current:current['settings'].update(provider='opencode-go'))
    session=frozen((store,row))
    metadata={'protocol':'chat','context':256000,'loop_tool_support':True}
    runtime=ModelProfiles(store.root).load(row['settings'],metadata)
    ledger=DecisionLedger(store,row,'select',session.snapshot['sha256'],metadata)
    calls=[]
    async def send(body,*_args):
        """Reject the unsupported parameter once on the ordinary decision request."""
        calls.append(copy.deepcopy(body))
        if len(calls)==1:
            raise StreamingUnavailable() if unavailable=='streaming' else NativeToolsUnavailable()
        assert ('stream' not in body) if unavailable=='streaming' else ('tools' not in body)
        return {'choices':[{'message':{'role':'assistant','content':'{"ok":true}'}}]}
    answer=asyncio.run(OpenCodeDialog(row['settings'],metadata,runtime,session,ledger,send)
                       .run('Normal decision',session.snapshot['required'],1000))
    assert json.loads(answer)=={'ok':True} and len(calls)==2
    assert ledger.state()['calls'][0]['tokens']>0
    assert not ledger.state()['calls'][0]['usage_confirmed']


def test_normal_transport_observations_are_optional_automatic_cache(record):
    """Future normal requests reuse actual transport negotiation, without repeated probes."""
    store,row=record
    settings={**row['settings'],'provider':'opencode-go','profile_owner':OWNER}
    metadata={'context':256000,'protocol':'chat','loop_tool_support':True}
    cache=ModelProfiles(store.root)
    cache.observe_transport(settings,metadata,streaming=False,tool_mode='json')
    value=cache.load(settings,metadata)
    assert value['tool_mode']=='json' and not value['streaming']


def test_transport_hints_expire_without_context_updates_extending_them(record, monkeypatch):
    """A stale fallback is retried normally; context telemetry never renews it."""
    import pb8_loop_ai_policy as policy
    store, row = record
    settings = {**row['settings'], 'provider': 'opencode-go', 'profile_owner': OWNER}
    metadata = {'context': 256000, 'protocol': 'chat', 'loop_tool_support': True}
    now = [100000.0]
    monkeypatch.setattr(policy.time, 'time', lambda: now[0])
    cache = ModelProfiles(store.root)
    cache.observe_transport(settings, metadata, streaming=False, tool_mode='json')
    now[0] += policy.TRANSPORT_CACHE_TTL_SECONDS - 1
    cache.observe_window(settings, metadata, 400000)
    recent = cache.load(settings, metadata)
    assert not recent['streaming'] and recent['tool_mode'] == 'json'
    now[0] += 1
    expired = cache.load(settings, metadata)
    assert expired['streaming'] and expired['tool_mode'] == 'native'

    cache.observe_transport(settings, metadata, streaming=False)
    separate = cache.load(settings, metadata)
    assert not separate['streaming'] and separate['tool_mode'] == 'native'


@pytest.mark.parametrize('timestamp', [None, 0, 'yesterday', 100001.0])
def test_transport_hints_without_valid_observation_time_are_ignored(record, monkeypatch, timestamp):
    """Legacy or invalid hints cannot permanently disable preferred transport."""
    import pb8_loop_ai_policy as policy
    store, row = record
    settings = {**row['settings'], 'provider': 'opencode-go', 'profile_owner': OWNER}
    metadata = {'context': 256000, 'protocol': 'chat', 'loop_tool_support': True}
    monkeypatch.setattr(policy.time, 'time', lambda: 100000.0)
    cache = ModelProfiles(store.root)
    cache._observe(settings, metadata, streaming=False, tool_mode='json',
                   streaming_observed_at=timestamp, tool_mode_observed_at=timestamp)
    current = cache.load(settings, metadata)
    assert current['streaming'] and current['tool_mode'] == 'native'


@pytest.mark.parametrize('protocol', ['chat', 'responses', 'messages'])
def test_json_transport_switch_preserves_fresh_contract_recovery(record, protocol):
    """Negotiation does not consume the fresh dialog needed after bad decisions."""
    from pb8_loop_dialog import NativeToolsUnavailable
    store, row = record
    row = store.update(OWNER, row['id'], lambda current: current['settings'].update(provider='opencode-go'))
    session = frozen((store, row))
    metadata = {'context': 256000, 'protocol': protocol, 'loop_tool_support': True}
    runtime = ModelProfiles(store.root).load(row['settings'], metadata)
    ledger = DecisionLedger(store, row, 'select', session.snapshot['sha256'], metadata)
    requests = []

    async def send(body, *_args):
        """Reject native tools, then require the full existing recovery allowance."""
        requests.append(copy.deepcopy(body))
        if len(requests) == 1:
            raise NativeToolsUnavailable()
        assert 'tools' not in body
        text = '{"ok":true}' if len(requests) == 5 else '{"invalid":true}'
        return {
            'chat': {'choices': [{'finish_reason': 'stop', 'message': {'role': 'assistant', 'content': text}}]},
            'responses': {'status': 'completed', 'output': [
                {'type': 'message', 'role': 'assistant', 'content': [{'type': 'output_text', 'text': text}]}]},
            'messages': {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': text}]},
        }[protocol]

    def validate(text):
        """Only the final candidate decision satisfies the isolated contract."""
        if json.loads(text) != {'ok': True}:
            raise ValueError('Use the required decision contract')

    result = asyncio.run(OpenCodeDialog(row['settings'], metadata, runtime, session, ledger, send, validate)
                         .run('Normal decision', session.snapshot['required'], 1000))
    assert json.loads(result) == {'ok': True}
    assert session.dialog == 1 and len(requests) == 5
    assert len(ledger.state()['calls']) == 5
    assert not ledger.state()['calls'][0]['usage_confirmed']


def test_transport_negotiation_after_retries_does_not_consume_recovery(record):
    """A late native rejection still leaves both dialogs and charges every call."""
    from pb8_loop_dialog import NativeToolsUnavailable
    store, row = record
    row = store.update(OWNER, row['id'], lambda current: current['settings'].update(provider='opencode-go'))
    session = frozen((store, row))
    metadata = {'context': 256000, 'protocol': 'chat'}
    runtime = ModelProfiles(store.root).load(row['settings'], metadata)
    ledger = DecisionLedger(store, row, 'select', session.snapshot['sha256'], metadata)
    requests = []

    async def send(body, *_args):
        """Simulate two interrupted transports followed by ordinary negotiation."""
        requests.append(copy.deepcopy(body))
        if len(requests) < 3:
            raise ConnectionError('Interrupted test connection')
        if len(requests) == 3:
            raise NativeToolsUnavailable()
        return {'choices': [{'message': {'content': '{"ok":true}'}}]}

    result = asyncio.run(OpenCodeDialog(row['settings'], metadata, runtime, session, ledger, send)
                         .run('Normal decision', session.snapshot['required'], 1000))
    assert json.loads(result) == {'ok': True} and session.dialog == 0
    assert len(ledger.state()['calls']) == len(requests) == 4
