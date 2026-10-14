"""Loop-only provider adapters: one read tool, complete SSE and bounded dialogs."""
from __future__ import annotations

import asyncio
import copy
import json
import time

from pb8_loop_evidence import LoopAIPause, TOOL_DESCRIPTION, TOOL_SCHEMA, encode, sha
from pb8_loop_ai_policy import check_context

SERVICE = 'PB8LoopDialog'


class IncompleteResponse(ValueError):
    """A stream must finish explicitly before any tool or winner is accepted."""


class StreamingUnavailable(ValueError):
    """A normal request received an explicit unsupported-streaming response."""


class NativeToolsUnavailable(ValueError):
    """A normal request confirmed that its native tool transport is unsupported."""


class StreamAccumulator:
    """Reassemble protocol items without executing partially received tool arguments."""

    def __init__(self, protocol):
        self.protocol, self.done = protocol, False
        self.text, self.reasoning = [], []
        self.calls, self.items, self.blocks, self.usage = {}, [], {}, {}
        self.open_blocks = set()
        self.result = None

    def feed(self, payload):
        """Return true only for new meaningful data, never keepalives or repeats."""
        progress = False
        if self.protocol == 'chat':
            if payload.get('usage'):
                self.usage = payload['usage']
            choices = payload.get('choices') or []
            if choices:
                choice = choices[0]
                delta = choice.get('delta') or {}
                progress = bool(delta.get('content') or delta.get('reasoning_content'))
                self.text.append(delta.get('content') or '')
                self.reasoning.append(delta.get('reasoning_content') or '')
                for part in delta.get('tool_calls') or []:
                    index = part.get('index', 0)
                    target = self.calls.setdefault(index, {'id': '', 'type': 'function',
                                                          'function': {'name': '', 'arguments': ''}})
                    if part.get('id'):
                        progress |= target['id'] != part['id']
                        target['id'] = part['id']
                    function = part.get('function') or {}
                    progress |= bool(function.get('name') or function.get('arguments'))
                    target['function']['name'] += function.get('name') or ''
                    target['function']['arguments'] += function.get('arguments') or ''
                # A finish reason is not sufficient to retain trailing usage: [DONE] finishes SSE.
                if choice.get('finish_reason'):
                    self.finish_reason = choice['finish_reason']
        elif self.protocol == 'responses':
            kind = payload.get('type', '')
            index = payload.get('output_index', 0)
            if kind == 'response.output_item.added':
                while len(self.items) <= index:
                    self.items.append({})
                item = payload.get('item') or {}
                progress = bool(item) and self.items[index] != item
                self.items[index] = copy.deepcopy(item)
            elif kind == 'response.output_item.done':
                while len(self.items) <= index:
                    self.items.append({})
                item = payload.get('item') or {}
                progress = bool(item) and self.items[index] != item
                self.items[index] = copy.deepcopy(item)
            elif kind == 'response.function_call_arguments.delta':
                if index >= len(self.items):
                    raise IncompleteResponse('Tool argument delta has no parent item')
                self.items[index]['arguments'] = self.items[index].get('arguments', '') + (payload.get('delta') or '')
                progress = bool(payload.get('delta'))
            elif kind in {'response.output_text.delta', 'response.reasoning_text.delta', 'response.reasoning_summary_text.delta'}:
                progress = bool(payload.get('delta'))
                if 'reasoning' in kind:
                    self.reasoning.append(payload.get('delta') or '')
                else:
                    self.text.append(payload.get('delta') or '')
            elif kind == 'response.completed':
                self.result = payload.get('response') or {}
                self.done = self.result.get('status', 'completed') == 'completed'
            elif kind in {'response.failed', 'response.incomplete', 'error'}:
                raise IncompleteResponse('Provider did not complete its response')
        else:
            kind = payload.get('type', '')
            index = payload.get('index', 0)
            if kind == 'message_start':
                self.usage.update((payload.get('message') or {}).get('usage') or {})
            elif kind == 'content_block_start':
                self.open_blocks.add(index)
                block = payload.get('content_block') or {}
                progress = bool(block) and self.blocks.get(index) != block
                self.blocks[index] = copy.deepcopy(block)
                if self.blocks[index].get('type') == 'tool_use':
                    self.blocks[index]['_partial_json'] = ''
            elif kind == 'content_block_delta':
                if index not in self.blocks:
                    raise IncompleteResponse('Content delta has no parent block')
                delta = payload.get('delta') or {}
                block = self.blocks[index]
                for source, target in (('text', 'text'), ('thinking', 'thinking'), ('signature', 'signature'),
                                       ('partial_json', '_partial_json')):
                    if isinstance(delta.get(source), str):
                        progress |= bool(delta[source])
                        block[target] = block.get(target, '') + delta[source]
            elif kind == 'content_block_stop':
                self.open_blocks.discard(index)
                block = self.blocks.get(index) or {}
                if block.get('type') == 'tool_use':
                    raw = block.pop('_partial_json', '')
                    if raw:
                        # Complete malformed arguments are returned as a native tool error,
                        # distinct from an incomplete stream (which must never execute tools).
                        try:
                            block['input'] = json.loads(raw)
                        except ValueError:
                            block['input'] = raw
            elif kind == 'message_delta':
                self.usage.update(payload.get('usage') or {})
                if (payload.get('delta') or {}).get('stop_reason'):
                    self.stop_reason = payload['delta']['stop_reason']
            elif kind == 'message_stop':
                self.done = True
            elif kind == 'error':
                raise IncompleteResponse('Provider stream reported an error')
        return progress

    def finish(self):
        """Produce native complete responses, retaining reasoning and signature items."""
        if not self.done:
            raise IncompleteResponse('Provider stream ended without a terminal event')
        if self.protocol == 'responses':
            return self.result or {'status': 'completed', 'output': self.items, 'usage': self.usage}
        if self.protocol == 'chat':
            message = {'role': 'assistant', 'content': ''.join(self.text)}
            reasoning = ''.join(self.reasoning)
            if reasoning:
                message['reasoning_content'] = reasoning
            if self.calls:
                message['tool_calls'] = [self.calls[key] for key in sorted(self.calls)]
            return {'choices': [{'message': message, 'finish_reason': getattr(self, 'finish_reason', 'stop')}],
                    'usage': self.usage}
        if self.open_blocks:
            raise IncompleteResponse('Provider message ended with unfinished content blocks')
        return {'content': [self.blocks[key] for key in sorted(self.blocks)], 'usage': self.usage,
                'stop_reason': getattr(self, 'stop_reason', None)}


async def read_sse(reader, protocol, profile):
    """Bound first-progress/idle waits even if bytes or pings arrive continuously."""
    accumulator = StreamAccumulator(protocol)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + profile['first_timeout']
    buffer, event_lines, total = b'', [], 0
    event_id, delivered = None, {}
    while not accumulator.done:
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise TimeoutError('Provider produced no meaningful stream progress')
        chunk = await asyncio.wait_for(reader.readany(), timeout=remaining)
        if not chunk:
            break
        total += len(chunk)
        buffer += chunk
        if total > 4 * 1024 * 1024 or len(buffer) > 256 * 1024:
            raise IncompleteResponse('Provider stream exceeds its bounded size')
        while b'\n' in buffer:
            raw, buffer = buffer.split(b'\n', 1)
            try:
                line = raw.rstrip(b'\r').decode('utf-8')
            except UnicodeDecodeError as exc:
                raise IncompleteResponse('Provider sent malformed UTF-8') from exc
            if not line:
                if event_lines:
                    text = '\n'.join(event_lines)
                    event_lines = []
                    if event_id is not None:
                        if event_id in delivered:
                            if delivered[event_id] != text:
                                raise IncompleteResponse('Provider reused a stream event ID with different data')
                            event_id = None
                            continue
                        delivered[event_id] = text
                    event_id = None
                    if text == '[DONE]':
                        if protocol != 'chat' or not getattr(accumulator, 'finish_reason', None):
                            raise IncompleteResponse('Premature provider terminal marker')
                        accumulator.done = True
                    else:
                        try:
                            payload = json.loads(text)
                        except ValueError as exc:
                            raise IncompleteResponse('Provider sent malformed stream JSON') from exc
                        if not isinstance(payload, dict):
                            raise IncompleteResponse('Provider stream event is not an object')
                        try:
                            progress = accumulator.feed(payload)
                        except (ValueError, TypeError, KeyError) as exc:
                            raise IncompleteResponse('Provider sent an invalid native stream fragment') from exc
                        if progress:
                            deadline = loop.time() + profile['idle_timeout']
                continue
            if line.startswith('data:'):
                event_lines.append(line[5:].lstrip(' '))
                if sum(len(part) for part in event_lines) > 256 * 1024:
                    raise IncompleteResponse('Provider event exceeds its bounded size')
            elif line.startswith('id:'):
                event_id = line[3:].strip()
            # Comments, event names and pings do not extend the progress deadline.
    return accumulator.finish()


def response_parts(protocol, payload):
    """Separate complete native tool calls from a possible final answer."""
    def arguments(raw):
        """Keep invalid JSON as a non-object; the reader charges and rejects it safely."""
        try:
            return json.loads(raw) if isinstance(raw, str) else raw
        except ValueError:
            return raw
    if payload.get('error'):
        raise IncompleteResponse('Provider returned an error response')
    if protocol == 'chat':
        choices = payload.get('choices') or []
        if not choices:
            raise IncompleteResponse('Provider returned no assistant message')
        choice = choices[0]
        if choice.get('finish_reason') in {'length', 'content_filter'}:
            raise IncompleteResponse('Provider response was truncated')
        message = choice.get('message') or {}
        calls = [{'id': item['id'], 'name': item['function']['name'],
                  'arguments': arguments(item['function']['arguments'])}
                 for item in message.get('tool_calls') or []]
        return message.get('content') or '', calls, [message]
    if protocol == 'responses':
        if payload.get('status') not in {None, 'completed'}:
            raise IncompleteResponse('Provider response did not complete')
        items = payload.get('output') or []
        calls = [{'id': item['call_id'], 'name': item['name'], 'arguments': arguments(item['arguments'])}
                 for item in items if item.get('type') == 'function_call']
        text = ''.join(block.get('text', '') for item in items if item.get('type') == 'message'
                       for block in item.get('content') or [] if block.get('type') == 'output_text')
        return text, calls, items
    blocks = payload.get('content') or []
    if payload.get('stop_reason') in {'max_tokens', 'refusal'}:
        raise IncompleteResponse('Provider response was truncated')
    calls = [{'id': block['id'], 'name': block['name'], 'arguments': block['input']}
             for block in blocks if block.get('type') == 'tool_use']
    assistant = copy.deepcopy(blocks)
    for block in assistant:
        if block.get('type') == 'tool_use' and not isinstance(block.get('input'), dict):
            # Messages requires object input even when replaying a malformed call.
            # Keep its original arguments in `calls` for a charged native error;
            # use an empty object only in the legal assistant replay envelope.
            block['input'] = {}
    return ''.join(block.get('text', '') for block in blocks if block.get('type') == 'text'), calls, assistant


def validate_native_reply(protocol, payload, model, output_tokens):
    """Reject acknowledged invalid replies without executing incomplete tool arguments."""
    from pb8_loop_ai import LoopAIResponseError
    usage = {'tokens': payload.get('usage'), 'usd': None}
    def reject(message):
        """Preserve known usage for the controller's existing repair path."""
        raise LoopAIResponseError(message, usage)
    if payload.get('error'):
        reject('AI provider returned an error')
    if payload.get('model') and payload['model'] != model:
        reject('The provider did not honor the pinned model')
    if protocol == 'chat':
        choices = payload.get('choices') or []
        if any(item.get('finish_reason') == 'length' for item in choices):
            reject(f'AI response reached the output limit ({output_tokens} tokens)')
        if any(item.get('finish_reason') == 'content_filter' for item in choices):
            reject('AI response was stopped by the provider content filter')
        if any((item.get('message') or {}).get('function_call') for item in choices):
            reject('AI requested a tool outside the evidence contract')
    elif protocol == 'responses' and payload.get('status') not in {None, 'completed'}:
        reject('AI response reached the output limit' if (payload.get('incomplete_details') or {}).get('reason') == 'max_output_tokens'
               else 'AI provider stopped the response before completion')
    elif protocol == 'messages':
        if payload.get('stop_reason') == 'max_tokens':
            reject('AI response reached the output limit')
        if payload.get('stop_reason') == 'refusal':
            reject('AI provider declined the response')
    try:
        _text, calls, _assistant = response_parts(protocol, payload)
        if any(call['name'] != 'loop_evidence' or not isinstance(call['id'], str) or not call['id'] for call in calls) \
                or len({call['id'] for call in calls}) != len(calls):
            reject('AI requested a tool outside the evidence contract')
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, LoopAIResponseError):
            raise
        reject('AI requested a tool or response with an incomplete native contract')


class OpenCodeDialog:
    """One model/protocol adapter around the same evidence session and ledger."""

    def __init__(self, settings, metadata, profile, session, ledger, send, validate=None):
        self.settings, self.metadata, self.profile = settings, metadata, profile
        self.session, self.ledger, self.send = session, ledger, send
        self.validate = validate

    def initial(self, instructions, content, output_tokens):
        """Create a fresh native payload, registering only the evidence reader."""
        protocol = self.metadata['protocol']
        tool = {'name': 'loop_evidence', 'description': TOOL_DESCRIPTION}
        body = {'model': self.settings['model']}
        message = {'role': 'user', 'content': encode(content)}
        if protocol == 'responses':
            body.update(instructions=instructions, input=[message], store=False, max_output_tokens=output_tokens,
                        tools=[{'type': 'function', **tool, 'parameters': TOOL_SCHEMA}])
        elif protocol == 'chat':
            body.update(messages=[{'role': 'system', 'content': instructions}, message], max_tokens=output_tokens,
                        tools=[{'type': 'function', 'function': {**tool, 'parameters': TOOL_SCHEMA}}])
        else:
            body.update(system=instructions, messages=[message], max_tokens=output_tokens,
                        tools=[{**tool, 'input_schema': TOOL_SCHEMA}])
        if self.profile.get('tool_mode') == 'json':
            body.pop('tools', None)
            reader = ('\nFor this transport, request evidence by returning ONLY '
                      '{"loop_evidence":{"path":"/data/...","offset":0}}. '
                      'The next user message contains the exact reader result. '
                      'Otherwise return the complete decision JSON. No other actions are allowed.')
            if protocol == 'chat':
                body['messages'][0]['content'] += reader
            else:
                body['instructions' if protocol == 'responses' else 'system'] += reader
        from ai_chat import AIChatService
        variant = AIChatService._selected_reasoning_variant(self.metadata, self.settings.get('effort',''))
        AIChatService._apply_reasoning_variant(body, protocol, self.settings['model'], variant)
        if self.profile.get('streaming') is True:
            body['stream'] = True
            if protocol == 'chat':
                body['stream_options'] = {'include_usage':True}
        return body

    async def run(self, instructions, content, output_tokens):
        """Bound HTTP rounds, context, cost and retries independently of tool quota."""
        protocol = self.metadata['protocol']
        failure = ('evidence_recovery', 'The bounded provider dialog exhausted its correction or call allowance')
        for dialog in range(2):
            if dialog:
                self.session.start_dialog()
            body = self.initial(instructions, content, output_tokens)
            corrections = 0
            sent = 0
            for _round in range(67):
                try:
                    input_tokens = check_context(body, self.profile, output_tokens, initial=_round == 0)
                except LoopAIPause as exc:
                    failure = (exc.reason, str(exc))
                    break
                payload = None
                attempt = 0
                while attempt < 3 and sent < 67:
                    call_id = self.ledger.reserve(input_tokens, output_tokens, sha(body))
                    sent += 1
                    try:
                        payload = await self.send(body, self.profile, self.ledger.remaining())
                    except StreamingUnavailable:
                        self.ledger.timeout(call_id)
                        if self.profile.get('streaming') is not True:
                            raise LoopAIPause('evidence_recovery', 'Provider rejected the fallback transport')
                        self.profile['streaming'] = False
                        body.pop('stream', None)
                        body.pop('stream_options', None)
                        input_tokens = check_context(body, self.profile, output_tokens, initial=_round == 0)
                        continue
                    except NativeToolsUnavailable:
                        self.ledger.timeout(call_id)
                        if self.profile.get('tool_mode') == 'json':
                            raise LoopAIPause('evidence_recovery', 'Provider rejected the fallback reader transport')
                        self.profile['tool_mode'] = 'json'
                        # Rebuild the normal request in JSON mode, preserving
                        # call/cost/read counters and both recovery dialogs.
                        body = self.initial(instructions, content, output_tokens)
                        input_tokens = check_context(body, self.profile, output_tokens, initial=True)
                        continue
                    except (TimeoutError, ConnectionError, IncompleteResponse) as exc:
                        self.ledger.timeout(call_id)
                        attempt += 1
                        if attempt == 3:
                            raise LoopAIPause('evidence_recovery', 'Provider did not complete a response after bounded transport retries',
                                              error_type=type(exc).__name__) from exc
                        continue
                    self.ledger.settle(call_id, payload.get('usage'))
                    validate_native_reply(protocol, payload, self.settings['model'], output_tokens)
                    if self.profile.get('streaming') is False:
                        body.pop('stream', None)
                        body.pop('stream_options', None)
                    break
                if payload is None:
                    break
                text, calls, assistant = response_parts(protocol, payload)
                textual_reader = False
                if not calls and self.profile.get('tool_mode') == 'json':
                    from pb8_loop_ai import parse_object
                    try:
                        candidate = parse_object(text)
                    except ValueError:
                        candidate = None
                    if isinstance(candidate, dict) and set(candidate) == {'loop_evidence'}:
                        calls = [{'id': 'reader-' + call_id, 'name': 'loop_evidence',
                                  'arguments': candidate['loop_evidence']}]
                        textual_reader = True
                if calls:
                    results = []
                    for call in calls:
                        if call['name'] != 'loop_evidence':
                            raise LoopAIPause('evidence_recovery', 'An unregistered tool was requested')
                        try:
                            from pb8_loop_controller import owned_thread
                            result = await owned_thread(self.session.read, call['arguments'], call['id'])
                        except LoopAIPause as exc:
                            if exc.reason != 'evidence_recovery' or dialog == 1:
                                raise
                            break
                        except (ValueError, KeyError, IndexError):
                            result = {'error': 'invalid_evidence_request'}
                        results.append((call['id'], encode(result), 'error' in result))
                    if len(results) != len(calls):
                        break
                    if textual_reader:
                        # Preserve the exact assistant answer and a plain user
                        # reader result for models without native function calls.
                        if protocol == 'responses':
                            body['input'].extend(assistant)
                        elif protocol == 'chat':
                            body['messages'].extend(assistant)
                        else:
                            body['messages'].append({'role': 'assistant', 'content': assistant})
                        body['input' if protocol == 'responses' else 'messages'].append({
                            'role': 'user', 'content': encode({'loop_evidence_results': [
                                {'call_id': key, 'result': json.loads(value)} for key, value, _error in results]})})
                    elif protocol == 'chat':
                        body['messages'].extend(assistant)
                        body['messages'].extend({'role': 'tool', 'tool_call_id': key, 'content': value} for key, value, _error in results)
                    elif protocol == 'responses':
                        body['input'].extend(assistant)
                        body['input'].extend({'type': 'function_call_output', 'call_id': key, 'output': value} for key, value, _error in results)
                    else:
                        body['messages'].append({'role': 'assistant', 'content': assistant})
                        body['messages'].append({'role': 'user', 'content': [
                            {'type': 'tool_result', 'tool_use_id': key, 'content': value, 'is_error': error} for key, value, error in results]})
                    continue
                if not text.strip():
                    corrections += 1
                    if corrections > 2:
                        break
                    message = {'role': 'user', 'content': 'Return the complete decision JSON or use loop_evidence. Do not repeat empty answers.'}
                    body['input' if protocol == 'responses' else 'messages'].append(message)
                    continue
                if self.validate is not None:
                    try:
                        self.validate(text)
                    except (ValueError, KeyError, TypeError) as exc:
                        corrections += 1
                        if corrections > 2:
                            break
                        if protocol == 'responses':
                            body['input'].extend(assistant)
                        elif protocol == 'chat':
                            body['messages'].extend(assistant)
                        else:
                            body['messages'].append({'role': 'assistant', 'content': assistant})
                        body['input' if protocol == 'responses' else 'messages'].append({
                            'role': 'user', 'content': 'Correct the decision JSON contract using the same evidence: ' + str(exc)[:500]})
                        continue
                return text
        raise LoopAIPause(*failure)
