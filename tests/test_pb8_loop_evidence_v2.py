"""Offline evidence privacy, exact values, bounded pages and provider contracts."""
import asyncio
import copy
import json
from types import SimpleNamespace

import pytest

from pb8_loop_evidence import (EvidenceStore, EvidenceSession, LoopAIPause, compact_finding,
                               required_content, select_findings, pointer, encode)
from pb8_loop_ai_policy import DecisionLedger, ModelProfiles, check_context
from pb8_loop_dialog import StreamAccumulator, read_sse, IncompleteResponse, OpenCodeDialog
from tests.test_pb8_loop_optimizer import record


def test_config_deltas_preserve_boolean_and_numeric_types():
    """Python equality must not hide False-to-zero or integer-to-float changes."""
    from pb8_loop_evidence import delta
    change = delta({'enabled':False,'bounds':[1]}, {'enabled':0,'bounds':[1.0]})
    values = {tuple(item['path']):item['value'] for item in change['set']}
    assert type(values[('enabled',)]) is int
    assert type(values[('bounds',)][0]) is float


def snapshot(record, required=None, history=()):
    """Use isolated production-shaped storage without any provider access."""
    store, row = record
    evidence = EvidenceStore(store)
    required = required if required is not None else {'stage': 'evaluate', 'reports': [{'gain': 1.2345678901234567}]}
    value = evidence.create(row, required, {'stage': 'evaluate', 'evidence': {}}, history)
    return evidence, value


def profile(**updates):
    """Create an explicit offline-tested profile, never a production gate record."""
    return {'context': 256000, 'page_tokens': 1500, 'streaming': True,
            'first_timeout': .2, 'idle_timeout': .2, 'connect_timeout': .2, **updates}


@pytest.mark.parametrize('key', ['liquidated', 'liquidated_usd', 'n_liquidations', 'backtest_completion_ratio'])
def test_current_safety_is_mandatory_not_hidden_in_extras(key):
    """Safety aliases and missing/null/zero retain distinct meanings."""
    source = {'stage': 'evaluate', 'initial_config': {}, 'rubric': [{'metrics': ['gain']}],
              'evidence': {'observations': [{'per_report': [{'gain': 1.2345678901234567, key: None, 'extra': 99},
                                                           {'gain': 2., key: 0}, {'gain': 3.}]}]}}
    required = required_content(source)
    reports = required['evidence']['observations'][0]['per_report']
    assert reports == [{'gain': 1.2345678901234567, key: None}, {'gain': 2., key: 0}, {'gain': 3.}]
    assert source['evidence']['observations'][0]['per_report'][0]['extra'] == 99


@pytest.mark.parametrize('path,expected', [('/a~1b/~0', 7), ('//0', 'zero'), ('/array/0', None)])
def test_json_pointer_is_data_not_a_filesystem_path(path, expected):
    """RFC escapes, empty keys and array indices are resolved without file access."""
    assert pointer({'a/b': {'~': 7}, '': ['zero'], 'array': [None]}, path) == expected


@pytest.mark.parametrize('path', ['../outside', '/a~2b', '/array/01', '/array/-1', '/bad\x00'])
def test_pointer_rejects_invalid_escape_and_array_boundary(path):
    """Unsafe or ambiguous pointer syntax is never converted into file paths."""
    with pytest.raises((ValueError, KeyError)):
        pointer({'array': [1]}, path)


def test_archive_filename_is_not_its_content_hash(record):
    """Two snapshots share one immutable archive and independently verify its bytes."""
    store, row = record
    entry = {'id': 'old:round:1', 'loop': row['id'], 'knowledge': 'exact original', 'created_at': 1}
    evidence, first = snapshot(record, history=[entry])
    _, second = snapshot(record, history=[entry])
    binding = first['history'][entry['id']]
    assert len(list((store.owner_dir(row['owner']) / 'evidence').glob('*.json'))) == 1
    assert second['history'] == first['history']
    assert binding['sha256'] != binding['key']
    assert evidence.resolve(first, '/history/old:round:1/knowledge') == 'exact original'
    assert EvidenceStore(store).load(row['owner'], first['id']) == first


@pytest.mark.parametrize('required', [False, True])
def test_mutated_archive_never_reaches_model(record, required):
    """Optional corruption is a tool error; mandatory corruption is resumably paused."""
    store, row = record
    entry = {'id': 'source', 'loop': row['id'], 'knowledge': 'original'}
    evidence, value = snapshot(record, history=[entry])
    binding = value['history']['source']
    binding['required'] = required
    path = store.owner_dir(row['owner']) / 'evidence' / (binding['key'] + '.json')
    path.write_text('{"knowledge":"changed"}')
    if required:
        with pytest.raises(LoopAIPause, match='integrity') as error:
            evidence.resolve(value, '/history/source')
        assert error.value.reason == 'evidence_recovery'
    else:
        result = evidence.resolve(value, '/history/source')
        assert result['error'] == 'evidence_integrity'
        assert 'changed' not in encode(result)


def test_pages_preserve_large_scalars_and_cover_after_compaction(record):
    """Every string fragment and exact numeric value survives bounded JSON delivery."""
    text = 'α"\\\n' * 2500
    evidence, value = snapshot(record, {'text': text, 'numbers': [1.2345678901234567, None, 0]})
    session = EvidenceSession(evidence, value, 1500)
    session.compacted()
    fragments = []
    for index, page in enumerate(session.required_pages):
        result = session.read({'path': '/required', 'offset': page['offset']}, 'call_' + str(index))
        assert result == page
        assert len(encode(result)) > 0
        for entry in result['entries']:
            if entry['path'] == '/text':
                fragments.append(entry['value'])
    assert ''.join(fragments) == text
    assert session.missing() == []
    session.start_dialog()
    assert session.dialog == 1 and session.total_calls == len(session.required_pages)


def test_rpc_retry_is_idempotent_but_new_duplicate_calls_count(record):
    """Transport retries do not consume quota; new identical reads are no progress."""
    evidence, value = snapshot(record)
    session = EvidenceSession(evidence, value, 1500)
    args = {'path': '/required', 'offset': 0}
    assert session.read(args, 'rpc') == session.read(args, 'rpc')
    assert session.calls == 1
    for index in range(31):
        session.read(args, 'duplicate_' + str(index))
    with pytest.raises(LoopAIPause):
        for index in range(5):
            session.read(args, 'stalled_' + str(index))


@pytest.mark.parametrize('legacy,status', [('rejected_exact', 'observed_not_qualified'),
                                          ('technical_repair', 'hypothesis'), ('unconfirmed', 'hypothesis')])
def test_legacy_findings_have_no_model_migration_or_silent_truncation(legacy, status):
    """Long prose is referenced; rejection is not a disproved trading hypothesis."""
    entry = {'id': 'old', 'status': legacy, 'knowledge': 'x' * 1001, 'hypothesis': 'y' * 601,
             'uncertainty': 'Incomplete comparison', 'improved': True}
    compact = compact_finding(entry)
    assert compact['status'] == status
    assert 'knowledge' not in compact and 'hypothesis' not in compact
    assert compact['knowledge_reference']['characters'] == 1001
    assert compact['incumbent_replaced'] is True
    assert compact['uncertainty'] == entry['uncertainty']


def test_findings_order_and_budget_stay_bounded(record):
    """Same-loop/contradiction/age/ID ordering remains stable as history grows."""
    _, row = record
    common = {'coins': row['settings']['goals'].get('coins', []), 'fingerprint': row['fingerprint'],
              'status': 'hypothesis', 'knowledge': 'measured result', 'created_at': 1}
    entries = [dict(common, id='b', loop=row['id']), dict(common, id='a', loop=row['id']),
               dict(common, id='older_conflict', loop=row['id'], contradicts=['a'], created_at=0),
               dict(common, id='other', loop='c' * 32, created_at=999)]
    small, _ = select_findings(entries, row, 1200)
    assert [entry['id'] for entry in small['findings']][:3] == ['older_conflict', 'a', 'b']
    many, _ = select_findings(entries + [dict(common, id='extra_' + str(i), loop='c' * 32) for i in range(100)], row, 1200)
    assert many['omitted'] > small['omitted']
    from pb8_loop_evidence import tokens
    assert tokens(many['findings']) <= 1200


@pytest.mark.parametrize('tool,mode', [(False, 'json'), (None, 'native')])
def test_native_tool_metadata_selects_transport_without_blocking_model(record, tool, mode):
    """A model lacking native calls can use the same reader as JSON messages."""
    store,row = record
    value = ModelProfiles(store.root).load({**row['settings'],'provider':'opencode-go'},
        {'protocol':'chat','context':256000,'tools':True,'loop_tool_support':tool})
    assert value['tool_mode'] == mode


def test_paid_ledger_counts_replayed_input_and_keeps_unknown_cost(record):
    """Every request reserves independently and a timeout never resets the total."""
    store, row = record
    def configure(value):
        """Apply explicit paid limits only to the temporary record."""
        value['settings'].update(provider='opencode-zen', ai_budget_confirmed=True,
                                 ai_decision_token_limit=3500, ai_decision_usd_limit=1.)
    row = store.update(row['owner'], row['id'], configure)
    metadata = {'free': False, 'cost': {'input': 1., 'output': 2.}}
    ledger = DecisionLedger(store, row, 'evaluate', 'frozen', metadata)
    first = ledger.reserve(1000, 500)
    ledger.timeout(first)
    second = ledger.reserve(1000, 500)
    ledger.settle(second, {'input_tokens': 1000, 'output_tokens': 100})
    ledger.settle(second, {'input_tokens': 1, 'output_tokens': 1})
    assert ledger.state()['tokens'] == 2600
    resumed = DecisionLedger(store, row, 'evaluate', 'frozen', metadata)
    with pytest.raises(LoopAIPause) as error:
        resumed.reserve(1000, 500)
    assert error.value.reason == 'ai_budget'
    assert len(resumed.state()['calls']) == 2


class Reader:
    """Deliver arbitrary in-memory SSE chunks without a socket or credentials."""
    def __init__(self, data):
        self.parts = iter(data)
    async def readany(self):
        """Finish at EOF instead of implicitly manufacturing a terminal event."""
        return next(self.parts, b'')


@pytest.mark.parametrize('protocol', ['chat', 'responses', 'messages'])
def test_sse_fragmentation_and_explicit_completion(protocol):
    """A complete native response is reconstructed even across UTF-8 packet splits."""
    if protocol == 'chat':
        events = [{'choices': [{'delta': {'content': 'α'}, 'finish_reason': None}]},
                  {'choices': [{'delta': {}, 'finish_reason': 'stop'}]}]
        terminal = 'data: [DONE]\n\n'
    elif protocol == 'responses':
        events = [{'type': 'response.output_text.delta', 'delta': 'α'}, {'type': 'response.completed',
                  'response': {'status': 'completed', 'output': [{'type': 'message', 'content': [{'type': 'output_text', 'text': 'α'}]}]}}]
        terminal = ''
    else:
        events = [{'type': 'content_block_start', 'index': 0, 'content_block': {'type': 'text', 'text': ''}},
                  {'type': 'content_block_delta', 'index': 0, 'delta': {'type': 'text_delta', 'text': 'α'}},
                  {'type': 'content_block_stop', 'index': 0},
                  {'type': 'message_delta', 'delta': {'stop_reason': 'end_turn'}},
                  {'type': 'message_stop'}]
        terminal = ''
    wire = (''.join('data: ' + json.dumps(event, ensure_ascii=False) + '\n\n' for event in events) + terminal).encode()
    payload = asyncio.run(read_sse(Reader([wire[i:i+1] for i in range(len(wire))]), protocol, profile()))
    assert 'α' in encode(payload)


@pytest.mark.parametrize('protocol', ['chat', 'responses', 'messages'])
def test_partial_sse_cannot_trigger_tools_or_a_winner(protocol):
    """EOF is not successful completion, even after a tool-looking payload."""
    with pytest.raises(IncompleteResponse):
        asyncio.run(read_sse(Reader([b': ping\n\n']), protocol, profile()))


def test_opencode_context_counts_whole_replayed_history():
    """A call-count allowance cannot admit an overflowing stateless context."""
    with pytest.raises(LoopAIPause) as error:
        check_context({'messages': [{'content': '0123456789' * 100000}]}, profile(), 32768)
    assert error.value.reason == 'context_budget'
