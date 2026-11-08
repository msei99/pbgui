"""Bounded initial context with exact current evidence and archive references."""
import copy
import json

import pytest

from pb8_loop_evidence import EvidenceStore, EvidenceSession, required_content, select_findings, encode, tokens
from pb8_loop_ai_policy import check_context
from tests.test_pb8_loop_optimizer import record, RULE


def test_one_hundred_rounds_do_not_reinject_full_historical_reports(record):
    """The findings budget stays fixed while immutable optional provenance grows."""
    store,row = record
    evidence = EvidenceStore(store)
    entries = []
    lengths = []
    for index in range(100):
        entries.append({'id':'round:'+str(index),'loop':row['id'],'created_at':index,
            'status':'observed_exact','coins':row['settings']['goals']['coins'],
            'fingerprint':row['fingerprint'],'knowledge':'Measured hypothesis '+str(index),
            'observations':[{'exact':True,'metrics':{'unused_'+str(key):key for key in range(240)}}]})
        package, originals = select_findings(entries,row,1000)
        content = {'stage':'evaluate','initial_config':{},'rubric':RULE,'knowledge':package,
                   'evidence':{'observations':[{'metrics':{'gain':2,'liquidated':0}}]}}
        required = required_content(content)
        lengths.append(tokens(required))
        assert 'observations' not in encode(package) and tokens(package['findings']) <= 1000
    assert max(lengths) - min(lengths) < 1200
    frozen = evidence.create(row,required,content,originals)
    assert len(frozen['history']) == 100
    assert evidence.resolve(frozen,'/history/round:0/observations/0/metrics/unused_239') == 239


@pytest.mark.parametrize('count',[100,512,1000])
def test_selection_columns_preserve_every_id_and_current_safety_value(count):
    """There is no reader-prefix or arbitrary shortlist hidden in the context projector."""
    candidates = [{'id':'candidate_'+str(index),'optimizer_job':'owned',
                   'config':{'distinct':index},'metrics':{'gain':index+.1234567890123456,'liquidated':0,
                   'drawdown_worst_strategy_eq':.49,'unrelated_extra':index}} for index in range(count)]
    content = {'stage':'select','rubric':RULE,'initial_config':{},'evidence':{'candidates':candidates}}
    before = copy.deepcopy(content)
    required = required_content(content)
    from pb8_loop_selection import query
    assert 'candidates' not in required['evidence']
    assert required['evidence']['selection']['candidate_count'] == count
    frozen = {'id': 'a' * 32, 'sha256': 'b' * 64, 'data': content, 'required': required}
    cursor, seen = 0, []
    while cursor is not None:
        page = query(frozen, '/selection/rows', cursor, 1500)
        assert tokens(page) <= 1500
        for item in page['entries']:
            index = int(item['id'].split('_')[-1])
            assert item['metrics']['gain'] == candidates[index]['metrics']['gain']
            assert item['metrics']['liquidated'] == 0 and 'unrelated_extra' not in item['metrics']
            seen.append(item['id'])
        cursor = page['next_offset']
    assert seen == [candidate['id'] for candidate in candidates]
    assert content == before


def test_page_measurement_includes_escaped_wrapper_and_scalar_fragments(record):
    """Nested quotes and Unicode are budgeted using the exact delivered response shape."""
    store,row = record
    required = {'text':'"\\\nα🙂' * 5000,'safety':{'liquidated':None}}
    archive = EvidenceStore(store)
    frozen = archive.create(row,required,{'stage':'select'},[])
    session = EvidenceSession(archive,frozen,1500)
    assert all(tokens(page) <= 1500 for page in session.required_pages)
    fragments = [entry for page in session.required_pages for entry in page['entries'] if entry['path']=='/text']
    assert ''.join(entry['value'] for entry in fragments) == required['text']
