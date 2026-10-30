"""New and Send must work with the real Optimize renderer and large Unicode pages."""
import importlib.util
import json
from pathlib import Path
from urllib.parse import urlparse

from browser_driver import expect

from ai_chat import AIChatError, AIChatService

ROOT = Path(__file__).resolve().parents[2]
_fixture_spec = importlib.util.spec_from_file_location(
    'new_chat_optimizer_fixture', ROOT / 'tests/ui/test_optimizer_log_layout_browser.py')
_fixture_module = importlib.util.module_from_spec(_fixture_spec)
_fixture_spec.loader.exec_module(_fixture_module)
log_page = _fixture_module.log_page


def test_new_and_send_with_large_unicode_optimize_context(log_page):
    """Keep the chosen Claude model and old history while creating and sending from a large real page."""
    page, _, _, errors, _, _, _ = log_page
    prefix = '/mounted'
    model = 'claude-sonnet-5-5'
    old_id, new_id = 'c' * 32, 'd' * 32
    selection = {'provider': 'claude-code', 'model': model, 'profile': 'default', 'effort': 'high', 'service_tier': ''}
    conversations = {old_id: {'conversation_id': old_id, 'title': 'Older fixture', 'provider': 'claude-code',
        'model': model, 'busy': False, 'analysis_only': False, 'effort': 'high',
        'messages': [{'role': 'assistant', 'content': 'Older synthetic answer'}]}}
    contexts, mutations = [], []

    def respond(route):
        """Validate captured page context with the real backend; never call a model or real PBGui."""
        path = urlparse(route.request.url).path.removeprefix(prefix)
        method = route.request.method
        if path == '/api/ai/status':
            route.fulfill(json={'providers': {'claude-code': {'available': True, 'connected': True, 'catalog': [model]}}})
        elif path == '/api/ai/models':
            route.fulfill(json={'models': [{'id': model, 'name': 'Sonnet 5.5', 'tools': True,
                'reasoning_variants': [{'id': 'high', 'label': 'High'}]}]})
        elif path == '/api/ai/preferences':
            route.fulfill(json={'selection': selection})
        elif method == 'POST' and (path == '/api/ai/conversations' or path.endswith('/turns')):
            body = route.request.post_data_json
            context = body['context']
            contexts.append(context)
            try:
                AIChatService._validate_page_context(context)
            except AIChatError as exc:
                route.fulfill(status=400, json={'detail': str(exc)})
                return
            mutations.append(path)
            assert body['provider'] == 'claude-code' and body['model'] == model and body['effort'] == 'high'
            if path == '/api/ai/conversations':
                conversations[new_id] = {**conversations[old_id], 'conversation_id': new_id, 'title': 'New fixture', 'messages': []}
                route.fulfill(json={'conversation_id': new_id})
            else:
                conversations[new_id]['messages'] = [{'role': 'user', 'content': body['message']},
                    {'role': 'assistant', 'content': 'Synthetic new conversation answer'}]
                route.fulfill(json={'turn_id': 'fixture'})
        elif path == '/api/ai/conversations':
            route.fulfill(json={'conversations': list(reversed(list(conversations.values())))})
        elif path.removeprefix('/api/ai/conversations/') in conversations:
            route.fulfill(json=conversations[path.removeprefix('/api/ai/conversations/')])
        else:
            route.fulfill(json={})

    page.route('**/api/ai/**', respond)
    page.locator('[data-panel="configs"]').click()
    page.evaluate('''() => {
      state.configs=Array.from({length:193},(_,index)=>({name:'Config_'+index+'_'+'測試'.repeat(30),
        exchange:'binance',strategy:'設定'.repeat(45),start_date:'2021-04-20',end_date:'2026-10-25'}));
      state.configsLoadedOnce=true;renderConfigs();
    }''')
    expect(page.locator('#configs-tbody tr[data-name]')).to_have_count(193)
    page.locator('#pbgui-ai-btn').click()
    expect(page.locator('#pai-model')).to_have_value(model)
    expect(page.locator('.pai-messages')).to_contain_text('Older synthetic answer')
    page.locator('.pai-compose textarea').fill('Synthetic question')
    page.locator('.pai-new').click()
    expect(page.locator('.pai-messages')).not_to_contain_text('Older synthetic answer')
    expect(page.locator('#pai-provider')).to_have_value('claude-code')
    expect(page.locator('#pai-model')).to_have_value(model)
    expect(page.locator('.pai-compose textarea')).to_have_value('Synthetic question')
    page.locator('.pai-compose button.primary').click()
    expect(page.locator('.pai-messages')).to_contain_text('Synthetic new conversation answer')
    assert mutations == ['/api/ai/conversations', f'/api/ai/conversations/{new_id}/turns']
    assert conversations[old_id]['messages'] == [{'role': 'assistant', 'content': 'Older synthetic answer'}]
    assert all(context['page_key'] == 'v8_optimize' and context['section'] == 'configs' for context in contexts)
    assert all(len(json.dumps(context, ensure_ascii=False, separators=(',', ':')).encode('utf-8')) <= 256 * 1024 for context in contexts)
    assert any(len(json.dumps(context).encode('utf-8')) > 300 * 1024 for context in contexts)
    assert not errors
