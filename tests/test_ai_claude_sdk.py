"""Exercise the installed SDK/native bridge against a local synthetic model server."""
import asyncio
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

import pytest

from ai_claude_code import ClaudeCodeError, ClaudeRuntime


@pytest.mark.skipif(not ClaudeRuntime.available(), reason="Optional pinned Claude SDK is not installed")
@pytest.mark.parametrize('loop_name', ['asyncio', 'uvloop'])
@pytest.mark.parametrize('output_kind', ['text', 'structured', 'structured_retry', 'structured_failure',
                                        'structured_missing', 'loop', 'loop_select', 'loop_bootstrap', 'loop_evaluate'])
def test_installed_sdk_routes_native_mcp_tool_without_real_account(tmp_path, monkeypatch, loop_name, output_kind):
    """The real native client must round-trip MCP into Python without user data or API billing."""
    requests = []
    structured_attempts = []
    structured_value = {'result': 'PBGui fixture completed'}
    output_schema = {'type': 'object', 'properties': {'result': {'type': 'string'}},
                     'required': ['result'], 'additionalProperties': False}
    if output_kind.startswith('loop'):
        from types import SimpleNamespace
        from pb8_loop_claude import _decision_schema
        from tests.test_pb8_loop_optimizer import RULE
        stage = 'interpret' if output_kind == 'loop' else output_kind.removeprefix('loop_')
        evidence = {}
        structured_value = {'reason': 'Synthetic Loop decision'}
        if stage == 'interpret':
            structured_value['rubric'] = RULE
        elif stage == 'select':
            evidence = {'candidates': [{'id': 'candidate-a'}, {'id': 'candidate-b'}], 'max_candidates': 2}
            structured_value['candidate_ids'] = ['candidate-a']
        else:
            structured_value['variants'] = [{'config': {key: {} for key in ('backtest', 'bot', 'live', 'optimize')},
                                             'overrides': {}, 'reason': 'Fixture variant'}]
            if stage == 'evaluate':
                evidence = {'winner_options': {'keep': {}}, 'evaluation_policy': {'mode': 'adaptive'}}
                structured_value.update(winner_operation='keep', finish=False,
                    evaluation_criteria=[{'metric': 'gain', 'direction': 'max', 'reducer': 'mean', 'reason': 'Fixture criterion'}],
                    candidate_assessments={'keep': 'Fixture assessment'})
        output_schema = _decision_schema({'session': SimpleNamespace(snapshot={'phase': stage, 'data': {'evidence': evidence}})})
    factory = asyncio.new_event_loop if loop_name == 'asyncio' else pytest.importorskip('uvloop').new_event_loop

    class ModelServer(BaseHTTPRequestHandler):
        """Respond to native requests locally with one synthetic tool call and a final answer."""

        def do_POST(self):
            """Return ordinary non-streaming API fixtures, including token counts."""
            body = json.loads(self.rfile.read(int(self.headers.get('Content-Length', 0))))
            if urlsplit(self.path).path.endswith('/count_tokens'):
                payload = {'input_tokens': 10}
            else:
                requests.append(body)
                used = any(block.get('type') == 'tool_result' for message in body.get('messages', [])
                           for block in (message.get('content', []) if isinstance(message.get('content'), list) else []))
                if used and output_kind == 'structured_missing':
                    content = [{'type': 'text', 'text': json.dumps(structured_value)}]
                elif used and output_kind != 'text':
                    structured_attempts.append(True)
                    invalid = output_kind == 'structured_failure' or output_kind == 'structured_retry' and len(structured_attempts) == 1
                    content = [{'type': 'tool_use', 'id': 'structured_fixture_'+str(len(structured_attempts)), 'name': 'StructuredOutput',
                                'input': {'result': 7} if invalid else structured_value}]
                else:
                    content = ([{'type': 'text', 'text': 'PBGui fixture completed'}] if used else
                               [{'type': 'tool_use', 'id': 'tool_fixture', 'name': 'mcp__pbgui__fixture_tool', 'input': {'value': 'fixture'}}])
                payload = {'id': 'msg_fixture', 'type': 'message', 'role': 'assistant', 'model': body.get('model'),
                           'content': content, 'stop_reason': 'tool_use' if content[0]['type'] == 'tool_use' else 'end_turn', 'stop_sequence': None,
                           'usage': {'input_tokens': 10, 'output_tokens': 10}}
            data = json.dumps(payload).encode()
            self.send_response(200)
            self.send_header('content-type', 'application/json')
            self.send_header('content-length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            """Keep unrelated metadata requests local and empty."""
            self.send_response(404)
            self.end_headers()

        def log_message(self, *args):
            """Never print request metadata from the synthetic provider."""

    server = ThreadingHTTPServer(('127.0.0.1', 0), ModelServer)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    async def scenario():
        """Use disposable native profiles and a synthetic token accepted only by this fixture."""
        runtime = ClaudeRuntime(tmp_path / 'claude')
        original = runtime.environment

        def environment(owner):
            """Override routing only inside this test; production explicitly discards these variables."""
            env = original(owner)
            env.update(CLAUDE_CODE_OAUTH_TOKEN='synthetic-not-a-real-token',
                       ANTHROPIC_BASE_URL=f'http://127.0.0.1:{server.server_port}')
            return env

        monkeypatch.setattr(runtime, 'environment', environment)
        handled = []

        async def handler(name, arguments, native_call_id):
            """Verify the real SDK's in-process MCP result crosses the owned Python bridge."""
            assert (name, arguments) == ('fixture_tool', {'value': 'fixture'})
            assert isinstance(native_call_id, str) and len(native_call_id) == 36
            handled.append(name)
            return {'success': True, 'result': 'Synthetic tool result'}

        try:
            metadata = await runtime._request('a' * 32, {'mode': 'models'})
            assert not requests, 'Model discovery must not submit a model prompt'
            model = next(row['resolvedModel'] for row in metadata if row.get('resolvedModel') and 'sonnet' in row['resolvedModel'])
            async def advertised(owner):
                """Keep metadata testing independent of real account credentials."""
                return [{'id': model}]
            monkeypatch.setattr(runtime, 'models', advertised)
            context = await runtime.model_context('a' * 32, model)
            assert context['context'] >= 256000
            assert not requests, 'Context discovery must not submit a model prompt'
            events = []

            async def event(value):
                """Collect only the SDK runtime telemetry exposed to the Loop adapter."""
                events.append(value)

            async def reply():
                """Exercise the production Python reply boundary and its native output-format option."""
                return await runtime.reply('a' * 32, model,
                    [{'role': 'user', 'content': 'Use fixture_tool once, then finish.'}], '', output_tokens=32768,
                    tools=[{'name': 'fixture_tool', 'description': 'Synthetic offline test',
                        'input_schema': {'type': 'object', 'properties': {'value': {'type': 'string'}}, 'required': ['value']}}],
                    handler=handler, on_event=event, handler_with_id=True, timeout_seconds=30,
                    output_schema=output_schema if output_kind != 'text' else None)

            if output_kind in {'structured_failure', 'structured_missing'}:
                with pytest.raises(ClaudeCodeError, match='required structured decision'):
                    await reply()
            else:
                answer = await reply()
                expected = json.dumps(structured_value, separators=(',', ':')) if output_kind != 'text' else 'PBGui fixture completed'
                assert answer == expected
            assert handled == ['fixture_tool']
            assert requests and not runtime.operations and not runtime.processes
            if output_kind != 'structured_failure':
                assert any(value['event'] == 'usage' and value['context_window'] >= 256000 for value in events)
            if output_kind == 'structured_retry':
                assert len(structured_attempts) >= 2
            if output_kind != 'text':
                assert any(tool.get('name') == 'StructuredOutput' for body in requests for tool in body.get('tools', []))
        finally:
            await runtime.close()
            assert not runtime.processes

    try:
        with asyncio.Runner(loop_factory=factory) as runner:
            runner.run(scenario())
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
