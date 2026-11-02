"""Bounded DeepSeek Chat Completions transport, including streamed tool calls."""
from __future__ import annotations

import copy
import json

import aiohttp

SERVICE = 'AIDeepSeek'
DEEPSEEK_PROVIDER = {'base_url': 'https://api.deepseek.com/v1', 'protocol': 'chat'}
MAX_REASONING_CHARS = 256_000
_MAX_BYTES = 2 * 1024 * 1024
MAX_OUTPUT_TOKENS = 32768
REQUEST_TIMEOUT = 900
MAX_CONTENT_CHARS = 512_000


def stream_limit(output_tokens=MAX_OUTPUT_TOKENS):
    """Allow bounded SSE framing overhead independently of decoded content limits."""
    tokens = output_tokens if type(output_tokens) is int and output_tokens > 0 else MAX_OUTPUT_TOKENS
    return max(4 * 1024 * 1024, min(tokens, MAX_OUTPUT_TOKENS) * 2048 + 256 * 1024)


def validate_finish(payload):
    """Require an explicitly successful native finish before accepting text or tools."""
    choices = payload.get('choices') if isinstance(payload, dict) else None
    if not isinstance(choices, list) or not choices:
        raise _error('DeepSeek returned no completed assistant response')
    for choice in choices:
        reason = choice.get('finish_reason') if isinstance(choice, dict) else None
        if reason not in {'stop', 'tool_calls'}:
            if reason == 'length':
                raise _error('DeepSeek response reached the output limit; incomplete output was not accepted')
            raise _error('DeepSeek response was interrupted before completion; incomplete output was not accepted')


def models(payload):
    """Project API-advertised metadata for the supported chat and image adapter."""
    import re
    data = payload.get('data') if isinstance(payload, dict) and not payload.get('error') else None
    if not isinstance(data, list) or len(data) > 256:
        raise _error('DeepSeek returned invalid model information')
    result, seen = [], set()
    for item in data:
        if not isinstance(item, dict):
            continue
        model_id = item.get('id')
        if not isinstance(model_id, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}', model_id) or model_id in seen:
            continue
        seen.add(model_id)
        name = item.get('name')
        if not isinstance(name, str) or not name.strip() or len(name) > 200 or any(ord(c) < 32 for c in name):
            name = model_id
        effort = item.get('effort')
        effort = effort if isinstance(effort, dict) else {}
        levels = effort.get('supported_levels')
        levels = levels if isinstance(levels, list) else []
        supported = list(dict.fromkeys(level for level in levels[:32]
                         if isinstance(level, str) and re.fullmatch(r'[a-z][a-z0-9_-]{0,31}', level)
                         and level != 'none'))
        variants = [{'id': level, 'label': 'Off' if level == 'none' else level,
                     'type': 'effort', 'value': level}
                    for level in (['none', *supported] if supported else [])]
        row = {'id': model_id, 'name': name, 'protocol': 'chat', 'provider': 'deepseek',
               'tools': True, 'loop_tool_support': True,
               'image_input': isinstance(item.get('input_modalities'), list) and 'image' in item['input_modalities'],
               'reasoning': bool(supported), 'reasoning_variants': variants,
               'default_effort': effort.get('default_level') if effort.get('default_level') in supported else '',
               'default': model_id == 'deepseek-flash'}
        for field, api_field in (('context', 'context_window'), ('output_limit', 'max_output_tokens')):
            value = item.get(api_field)
            if type(value) is int and 0 < value <= 100_000_000:
                row[field] = value
        result.append(row)
    if not result:
        raise _error('DeepSeek returned no usable models')
    if not any(row['default'] for row in result):
        result[0]['default'] = True
    return result


def _error(message):
    """Retain the shared sanitized error contract without an import cycle."""
    from ai_chat import AIChatError
    return AIChatError(message)



def balances(payload):
    """Return only finite nonnegative, provider-reported balance amounts."""
    from decimal import Decimal, InvalidOperation
    if not isinstance(payload, dict) or type(payload.get('is_available')) is not bool or not isinstance(payload.get('balance_infos'), list):
        raise _error('DeepSeek returned invalid balance information')
    rows = []
    for item in payload['balance_infos'][:8]:
        if not isinstance(item, dict) or item.get('currency') not in {'USD', 'CNY'}:
            continue
        row = {'currency': item['currency']}
        for key in ('total_balance', 'granted_balance', 'topped_up_balance'):
            value = item.get(key)
            try:
                if not isinstance(value, str) or len(value) > 32:
                    continue
                number = Decimal(value)
                if number.is_finite() and 0 <= number <= Decimal('1e15'):
                    row[key] = str(number)
            except InvalidOperation:
                continue
        if 'total_balance' in row:
            rows.append(row)
    if not rows:
        raise _error('DeepSeek returned no usable balance amounts')
    return {'is_available': payload['is_available'], 'balances': rows}


def input_reserve(body):
    """Budget text conservatively and images at the documented 1024-token maximum.

    Base64 bytes are transport overhead, not text tokens:
    https://api-docs.deepseek.com/guides/vision/#token-usage
    """
    projection = copy.deepcopy(body)
    image_tokens = 0
    for message in projection.get('messages', []):
        content = message.get('content')
        if isinstance(content, list):
            for part in content:
                if isinstance(part, dict) and part.get('type') == 'image_url':
                    image_tokens += 1024
                    part.clear()
                    part.update(type='text', text='')
    return len(json.dumps(projection, ensure_ascii=False, allow_nan=False).encode('utf-8')) + 1024 + image_tokens


async def _read_json(response):
    """Read a bounded JSON body, without exposing upstream error messages."""
    from ai_chat import AIChatService
    payload = await AIChatService._read_json_response(response, expected_status=200, max_bytes=_MAX_BYTES)
    if not isinstance(payload, dict) or payload.get('error'):
        raise _error('DeepSeek API returned an error or invalid response')
    return payload


async def _read_stream(reader, on_text=None, on_reasoning=None, *, max_bytes=None):
    """Require a terminal finish before exposing complete tool arguments to callers."""
    from pb8_loop_dialog import StreamAccumulator, IncompleteResponse
    accumulator = StreamAccumulator('chat')
    accumulator.max_content_chars = MAX_CONTENT_CHARS
    max_bytes = max_bytes or stream_limit()
    buffer, lines, total = b'', [], 0
    while not accumulator.done:
        chunk = await reader.readany()
        if not chunk:
            break
        total += len(chunk)
        buffer += chunk
        if total > max_bytes or len(buffer) > 256 * 1024:
            raise _error('DeepSeek response is too large')
        while b'\n' in buffer:
            raw, buffer = buffer.split(b'\n', 1)
            try:
                line = raw.rstrip(b'\r').decode('utf-8')
            except UnicodeDecodeError as exc:
                raise _error('DeepSeek returned invalid stream text') from exc
            if line.startswith('data:'):
                lines.append(line[5:].lstrip())
                if sum(len(part) for part in lines) > 256 * 1024:
                    raise _error('DeepSeek stream event is too large')
            elif not line and lines:
                event, lines = '\n'.join(lines), []
                if event == '[DONE]':
                    if not getattr(accumulator, 'finish_reason', None):
                        raise _error('DeepSeek stream ended before completing its response')
                    accumulator.done = True
                    break
                try:
                    payload = json.loads(event)
                    if not isinstance(payload, dict) or payload.get('error'):
                        raise ValueError('Invalid event')
                    accumulator.feed(payload)
                    for choice in (payload.get('choices') or [])[:1]:
                        delta = choice.get('delta') or {}
                        if on_text and delta.get('content'):
                            await on_text(delta['content'])
                        if on_reasoning and delta.get('reasoning_content'):
                            await on_reasoning(delta['reasoning_content'])
                except IncompleteResponse as exc:
                    if 'bounded size' in str(exc):
                        raise _error('DeepSeek decoded response is too large') from exc
                    raise _error('DeepSeek stream ended before completing its response') from exc
                except (ValueError, TypeError, KeyError, AttributeError) as exc:
                    raise _error('DeepSeek API returned an error or invalid stream event') from exc
    try:
        return accumulator.finish()
    except IncompleteResponse as exc:
        raise _error('DeepSeek stream ended before completing its response') from exc
    except (TypeError, AttributeError) as exc:
        raise _error('DeepSeek API returned invalid stream data') from exc


async def request(session, api_key, body, *, on_text=None, on_reasoning=None):
    """Send only to DeepSeek, refuse redirects and bound streaming/network failures."""
    from ai_chat import AIChatError, AIChatService
    payload = copy.deepcopy(body)
    payload['stream'] = True
    payload['stream_options'] = {'include_usage': True}
    try:
        async with session.post(
            DEEPSEEK_PROVIDER['base_url'] + '/chat/completions',
            headers={'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'},
            json=payload, allow_redirects=False,
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT, connect=15, sock_read=120),
        ) as response:
            if response.status != 200:
                # Shared reader classifies status safely; upstream bodies are never displayed.
                await AIChatService._read_json_response(response, expected_status=200, max_bytes=64 * 1024)
            if 'application/json' in response.headers.get('Content-Type', '').lower():
                result = await _read_json(response)
            else:
                result = await _read_stream(response.content, on_text, on_reasoning, max_bytes=stream_limit(payload.get('max_tokens')))
        if result.get('error'):
            raise _error('DeepSeek API returned an error')
        validate_finish(result)
        return result
    except AIChatError as exc:
        raise _error(str(exc).replace("OpenCode", "DeepSeek")) from exc
    except TimeoutError as exc:
        raise _error('DeepSeek request timed out') from exc
    except aiohttp.ClientError as exc:
        raise _error('DeepSeek connection failed') from exc
