"""Bounded DeepSeek Chat Completions transport, including streamed tool calls."""
from __future__ import annotations

import copy
import json

import aiohttp

SERVICE = 'AIDeepSeek'
DEEPSEEK_PROVIDER = {'base_url': 'https://api.deepseek.com/v1', 'protocol': 'chat'}
# https://api-docs.deepseek.com/quick_start/pricing/ (checked 2026-11-02).
# Keep the requested legacy IDs explicit; never silently substitute a newer model.
DEEPSEEK_MODELS = {
    'deepseek-flash': {'name': 'DeepSeek Flash', 'context': 1_000_000},
    'deepseek-v4-pro': {'name': 'DeepSeek V4 Pro', 'context': 1_000_000},
    'deepseek-chat': {'name': 'DeepSeek Chat (V3, legacy)', 'context': 128_000, 'output_limit': 8192},
    'deepseek-reasoner': {'name': 'DeepSeek Reasoner (R1, legacy)', 'context': 128_000},
}
MAX_REASONING_CHARS = 256_000
_MAX_BYTES = 2 * 1024 * 1024


def models():
    """Return independent, conservative metadata without a public catalog dependency."""
    return [{'id': key, 'protocol': 'chat', 'provider': 'deepseek', 'tools': True,
             'loop_tool_support': True, 'image_input': False, 'reasoning': key != 'deepseek-chat',
             'reasoning_variants': [], 'output_limit': 32768, 'default': key == 'deepseek-flash',
             **copy.deepcopy(metadata)} for key, metadata in DEEPSEEK_MODELS.items()]


def _error(message):
    """Retain the shared sanitized error contract without an import cycle."""
    from ai_chat import AIChatError
    return AIChatError(message)


async def _read_json(response):
    """Read a bounded JSON body, without exposing upstream error messages."""
    from ai_chat import AIChatService
    payload = await AIChatService._read_json_response(response, expected_status=200, max_bytes=_MAX_BYTES)
    if not isinstance(payload, dict) or payload.get('error'):
        raise _error('DeepSeek API returned an error or invalid response')
    return payload


async def _read_stream(reader, on_text=None, on_reasoning=None):
    """Require a terminal finish before exposing complete tool arguments to callers."""
    from pb8_loop_dialog import StreamAccumulator, IncompleteResponse
    accumulator = StreamAccumulator('chat')
    buffer, lines, total = b'', [], 0
    while not accumulator.done:
        chunk = await reader.readany()
        if not chunk:
            break
        total += len(chunk)
        buffer += chunk
        if total > _MAX_BYTES or len(buffer) > 256 * 1024:
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
            timeout=aiohttp.ClientTimeout(total=180, connect=15, sock_read=60),
        ) as response:
            if response.status != 200:
                # Shared reader classifies status safely; upstream bodies are never displayed.
                await AIChatService._read_json_response(response, expected_status=200, max_bytes=64 * 1024)
            if 'application/json' in response.headers.get('Content-Type', '').lower():
                result = await _read_json(response)
            else:
                result = await _read_stream(response.content, on_text, on_reasoning)
        if result.get('error'):
            raise _error('DeepSeek API returned an error')
        return result
    except AIChatError:
        raise
    except TimeoutError as exc:
        raise _error('DeepSeek request timed out') from exc
    except aiohttp.ClientError as exc:
        raise _error('DeepSeek connection failed') from exc
