"""Bounded Chat Completions SSE reading shared by the direct API adapters."""
from __future__ import annotations

import json

SERVICE = 'AIChatStream'


def provider_error(message):
    """Retain the shared sanitized error contract without an import cycle."""
    from ai_chat import AIChatError
    return AIChatError(message)


async def read_chat_stream(reader, accumulator, label, *, max_bytes, on_text=None, on_reasoning=None, prepare=None):
    """Require a terminal finish before exposing complete tool arguments to callers.

    An optional prepare callback normalizes or rejects each event before it is accumulated.
    """
    from pb8_loop_dialog import IncompleteResponse
    buffer, lines, total = b'', [], 0
    while not accumulator.done:
        chunk = await reader.readany()
        if not chunk:
            break
        total += len(chunk)
        buffer += chunk
        if total > max_bytes or len(buffer) > 256 * 1024:
            raise provider_error(f'{label} response is too large')
        while b'\n' in buffer:
            raw, buffer = buffer.split(b'\n', 1)
            try:
                line = raw.rstrip(b'\r').decode('utf-8')
            except UnicodeDecodeError as exc:
                raise provider_error(f'{label} returned invalid stream text') from exc
            if line.startswith('data:'):
                lines.append(line[5:].lstrip())
                if sum(len(part) for part in lines) > 256 * 1024:
                    raise provider_error(f'{label} stream event is too large')
            elif not line and lines:
                event, lines = '\n'.join(lines), []
                if event == '[DONE]':
                    if not getattr(accumulator, 'finish_reason', None):
                        raise provider_error(f'{label} stream ended before completing its response')
                    accumulator.done = True
                    break
                try:
                    payload = json.loads(event)
                    if not isinstance(payload, dict):
                        raise ValueError('Invalid event')
                    if prepare:
                        payload = prepare(payload)
                    if payload.get('error'):
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
                        raise provider_error(f'{label} decoded response is too large') from exc
                    raise provider_error(f'{label} stream ended before completing its response') from exc
                except (ValueError, TypeError, KeyError, AttributeError) as exc:
                    raise provider_error(f'{label} API returned an error or invalid stream event') from exc
    try:
        return accumulator.finish()
    except IncompleteResponse as exc:
        raise provider_error(f'{label} stream ended before completing its response') from exc
    except (TypeError, AttributeError) as exc:
        raise provider_error(f'{label} API returned invalid stream data') from exc
