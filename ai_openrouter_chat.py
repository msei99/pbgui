"""OpenRouter Chat Completions transport and model metadata, separate from Jev decisions."""
from __future__ import annotations

import asyncio
import copy
from datetime import datetime, timezone
import json
import math
import re
import time
from uuid import uuid4

import aiohttp

from ai_chat_stream import provider_error as _error, read_chat_stream
from ai_deepseek import MAX_CONTENT_CHARS, REQUEST_TIMEOUT, stream_limit
from ai_model_routing import is_decision_model
from ai_openrouter import OPENROUTER_API
from logging_helpers import human_log as _log
from pb8_loop_dialog import StreamAccumulator

SERVICE = 'AIOpenRouterChat'
CHAT_BASE_URL = f'{OPENROUTER_API}/v1'
CATALOG_TTL_SECONDS = 300
CATALOG_STALE_SECONDS = 3600
CATALOG_MAX_BYTES = 4 * 1024 * 1024
IMAGE_TOKEN_RESERVE = 4096
PROBE_TOOL = 'pbgui_connection_probe'
PROBE_OUTPUT_TOKENS = 8192
RETENTION_NOTE = 'Provider-dependent; data-collecting providers are excluded; not Zero Data Retention'
_GATEWAY_EFFORTS = ('max', 'xhigh', 'high', 'medium', 'low', 'minimal')
_MAX_ERROR_BYTES = 64 * 1024
_MAX_DETAILS = 256
_MAX_DETAIL_CHARS = 1_000_000
_MODEL_ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}')
_CANONICAL_SLUG = re.compile(r'[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}')
_STATUS_ERRORS = {
    400: 'OpenRouter rejected this request',
    401: 'OpenRouter rejected the API key; reconnect it in AI Setup',
    402: 'OpenRouter credits are insufficient for this request',
    403: 'OpenRouter refused this request (moderation, guardrail or key limit)',
    404: ('OpenRouter could not find an available endpoint for this model (HTTP 404). '
          'The model may be unavailable, or no provider may match the requested tools, '
          'reasoning settings and privacy policy. '
          'The exact cause was not confirmed. Try another model'),
    408: 'OpenRouter request timed out',
    413: 'This request is too large for the selected OpenRouter model',
    422: 'OpenRouter could not process this request',
    429: 'OpenRouter rate limit reached; try again later',
    502: "The selected model's provider is unavailable through OpenRouter",
    503: ('OpenRouter cannot serve this model right now (HTTP 503). '
          'Providers may be temporarily unavailable, or none may match the requested tools, '
          'reasoning settings and privacy policy. '
          'The exact cause was not confirmed. Try again later or choose another model'),
    504: 'OpenRouter request timed out',
}
_TYPE_ERRORS = {
    'rate_limit_exceeded': 'OpenRouter rate limit reached; try again later',
    'provider_unavailable': "The selected model's provider is unavailable through OpenRouter",
    'provider_overloaded': "The selected model's provider is overloaded; try again later",
    'content_policy_violation': 'The provider declined this request under its content policy',
    'refusal': 'The selected model refused this request',
    'authentication': 'The selected provider rejected its authentication',
}


class ModelCatalog:
    """Single-flight, bounded cache of provider-wide model metadata without owner state."""

    def __init__(self, ttl=CATALOG_TTL_SECONDS, stale_seconds=CATALOG_STALE_SECONDS):
        """Create an empty cache; rows never contain credentials or approvals."""
        self.ttl, self.stale_seconds = ttl, stale_seconds
        self.rows, self.fetched_at, self.generation = None, 0.0, 0
        self.lock = asyncio.Lock()

    def _age(self):
        """Return the cached age, or infinity while nothing is cached."""
        return math.inf if self.rows is None else time.monotonic() - self.fetched_at

    async def get(self, fetch):
        """Fetch once for concurrent readers; a cleared cache rejects older results."""
        if self._age() < self.ttl:
            return self.rows
        async with self.lock:
            if self._age() < self.ttl:
                return self.rows
            generation = self.generation
            try:
                rows = await fetch()
            except RuntimeError:
                if generation == self.generation and self._age() < self.stale_seconds:
                    return self.rows
                raise
            if generation == self.generation:
                self.rows, self.fetched_at = rows, time.monotonic()
            return rows

    def clear(self):
        """Drop cached rows and any fetch that started before this point."""
        self.generation += 1
        self.rows, self.fetched_at = None, 0.0


def _price(value):
    """Return one finite nonnegative token price; routers advertise negative prices."""
    try:
        price = float(value)
    except (TypeError, ValueError):
        return None
    return price if math.isfinite(price) and price >= 0 else None


def _string_list(value):
    """Keep only string members of an optional provider list."""
    return [item for item in value if isinstance(item, str)] if isinstance(value, list) else []


def _is_chat_model(item, parameters, today):
    """Accept fixed text models with bounded output; never routers, batches or aliases."""
    model_id = item.get('id')
    architecture = item.get('architecture')
    architecture = architecture if isinstance(architecture, dict) else {}
    endpoints = item.get('endpoints')
    expires = item.get('expiration_date')
    pricing = item.get('pricing')
    pricing = pricing if isinstance(pricing, dict) else {}
    if not isinstance(model_id, str) or not _MODEL_ID.fullmatch(model_id):
        return False
    if model_id.startswith('openrouter/') or model_id.endswith(':batch') or item.get('alias_target'):
        return False
    if isinstance(endpoints, list) and '/api/v1/chat/completions' not in endpoints:
        return False
    if 'text' not in _string_list(architecture.get('input_modalities')):
        return False
    if _string_list(architecture.get('output_modalities')) != ['text']:
        return False
    if isinstance(expires, str) and expires[:10] < today:
        return False
    if 'max_tokens' not in parameters:
        return False
    return _price(pricing.get('prompt')) is not None and _price(pricing.get('completion')) is not None


def _reasoning_fields(item, parameters):
    """Project reasoning metadata, keeping null and missing effort lists distinct."""
    metadata = item.get('reasoning')
    metadata = metadata if isinstance(metadata, dict) else {}
    mandatory = metadata.get('mandatory') is True
    if 'supported_efforts' not in metadata:
        efforts = []  # Effort support is not advertised, so PBGui offers only the model default.
    elif metadata['supported_efforts'] is None:
        efforts = [*_GATEWAY_EFFORTS, 'none']  # Documented meaning: no allowlist.
    else:
        known = (*_GATEWAY_EFFORTS, 'none')
        efforts = [level for level in dict.fromkeys(_string_list(metadata['supported_efforts'])) if level in known]
    if 'reasoning' not in parameters:
        efforts = []
    elif mandatory:
        efforts = [level for level in efforts if level != 'none']
    default_enabled = metadata.get('default_enabled')
    default_effort = metadata.get('default_effort')
    default_effort = default_effort if default_effort in (*_GATEWAY_EFFORTS, 'none') else ''
    if not mandatory and (default_enabled is False or default_effort == 'none'):
        display_default = 'off'
    else:
        display_default = '' if default_effort == 'none' else default_effort
    return {
        'reasoning': mandatory or 'reasoning' in parameters,
        'reasoning_mandatory': mandatory,
        'reasoning_default_enabled': default_enabled if isinstance(default_enabled, bool) else None,
        'default_effort': display_default,
        'reasoning_variants': [{'id': level, 'label': 'Off' if level == 'none' else level,
                                'type': 'effort', 'value': level}
                               for level in sorted(efforts, key=lambda level: level != 'none')],
    }


def models(payload, today=None):
    """Project catalog rows for ordinary chat; decision models are never derived here."""
    data = payload.get('data') if isinstance(payload, dict) else None
    if not isinstance(data, list) or len(data) > 4096:
        raise _error('OpenRouter returned invalid model information')
    today = today or datetime.now(timezone.utc).date().isoformat()
    result, seen = [], set()
    for item in data:
        if not isinstance(item, dict):
            continue
        parameters = _string_list(item.get('supported_parameters'))
        model_id = item.get('id')
        if not _is_chat_model(item, parameters, today) or model_id in seen or is_decision_model('openrouter', model_id):
            continue
        seen.add(model_id)
        name = item.get('name')
        if not isinstance(name, str) or not name.strip() or len(name) > 200 or any(ord(c) < 32 for c in name):
            name = model_id
        slug = item.get('canonical_slug')
        slug = slug if isinstance(slug, str) and _CANONICAL_SLUG.fullmatch(slug) else ''
        pricing = item['pricing']
        prompt, completion = _price(pricing.get('prompt')), _price(pricing.get('completion'))
        row = {'id': model_id, 'name': name, 'protocol': 'chat', 'provider': 'openrouter',
               'canonical_slug': slug, 'tools': False, 'loop_tool_support': False,
               'tool_capable': 'tools' in parameters and bool(slug),
               'tool_choice': 'tool_choice' in parameters,
               'image_input': 'image' in _string_list((item.get('architecture') or {}).get('input_modalities')),
               'free': model_id.endswith(':free') or prompt == completion == 0,
               'cost': {'input': round(prompt * 1_000_000, 6), 'output': round(completion * 1_000_000, 6)},
               'retention': RETENTION_NOTE, 'training': False, 'default': False,
               **_reasoning_fields(item, parameters)}
        description = item.get('description')
        if isinstance(description, str):
            row['description'] = ''.join(char for char in description[:12000]
                                         if ord(char) >= 32 or char in '\n\t')
        created = item.get('created')
        if type(created) is int and 0 < created <= 253402300799:
            row['created'] = created
        top_provider = item.get('top_provider')
        top_provider = top_provider if isinstance(top_provider, dict) else {}
        for field, value in (('context', item.get('context_length')), ('output_limit', top_provider.get('max_completion_tokens'))):
            if type(value) is int and 0 < value <= 100_000_000:
                row[field] = value
        result.append(row)
    return result


def with_tool_status(rows, approvals):
    """Overlay one owner's approvals; a new model version always loses its approval."""
    result = []
    for row in rows:
        row = copy.deepcopy(row)
        approval = approvals.get(row['id']) or {}
        row['tools'] = row['tool_capable'] and approval.get('canonical_slug') == row['canonical_slug']
        row['tool_status'] = 'validated' if row['tools'] else 'unvalidated' if row['tool_capable'] else 'unsupported'
        result.append(row)
    return sorted(result, key=lambda row: (not row['tools'], row['name'].casefold()))


def apply_reasoning(body, effort):
    """Send one validated effort in OpenRouter's documented request shape."""
    if effort:
        body['reasoning'] = {'effort': effort}


def apply_routing(body, data_collection='deny'):
    """Set the routing shared by chat and probes; settings are never silently dropped."""
    if data_collection not in ('allow', 'deny'):
        raise _error('Invalid OpenRouter data collection policy')
    body['provider'] = {'data_collection': data_collection,
                        'require_parameters': bool(body.get('tools') or body.get('reasoning'))}


def _routing_failure(message, metadata):
    """Classify gateway diagnostics without copying arbitrary provider text or prompts."""
    message = message.casefold().strip() if isinstance(message, str) else ''
    step = metadata.get('failed_routing_step')
    if (step == 'Filter by Data Policy'
            or message.startswith(('no endpoints found matching your data policy',
                                   'no endpoints available matching your guardrail restrictions and data policy'))):
        return ('OpenRouter reports that the privacy/data-policy filters exclude every endpoint '
                'for this model. Check the model\'s data collection permission in AI Setup and '
                'OpenRouter Settings > Privacy (https://openrouter.ai/settings/privacy). '
                'Account restrictions still apply when PBGui permission is enabled')
    if (step == 'Filter by Tool Compatibility'
            or message.startswith('no endpoints found that support tool use')):
        return ('OpenRouter reports that no available endpoint for this model supports the '
                'requested tool calls. Choose another tool-capable model; the catalog capability '
                'does not guarantee that a compatible endpoint is currently available')
    if message.startswith(('no endpoints found that support all the requested parameters',
                           'no endpoints found that support the requested parameters')):
        return ('OpenRouter reports that no endpoint supports all requested parameters. '
                'Try another reasoning setting or another model; PBGui does not silently drop parameters')
    if message.rstrip('.') in ('no endpoints found for this model', 'no endpoints for this model found'):
        return ('OpenRouter reports that this model has no available endpoints. '
                'Choose another model or try again later')
    return None


def _failure_reason(code, metadata, message=None):
    """Map provider classifications to fixed text; upstream messages can echo user data."""
    metadata = metadata if isinstance(metadata, dict) else {}
    kind = metadata.get('error_type')
    if isinstance(kind, str) and kind in _TYPE_ERRORS:
        return _TYPE_ERRORS[kind]
    if type(code) is int and code in (404, 503):
        reason = _routing_failure(message, metadata)
        if reason:
            return reason
    if type(code) is int and code in _STATUS_ERRORS:
        return _STATUS_ERRORS[code]
    return 'OpenRouter could not complete this request'


def _error_details(error):
    """Read code and metadata from the documented error object."""
    return (error.get('code'), error.get('metadata'), error.get('message')) if isinstance(error, dict) else (None, None, None)


def status_message(status, raw):
    """Build a safe message for a request rejected before its stream started."""
    try:
        document = json.loads(raw.decode('utf-8'))
    except (UnicodeDecodeError, ValueError):
        document = None
    error = document.get('error') if isinstance(document, dict) else None
    code, metadata, message = _error_details(error)
    return _failure_reason(status if status in _STATUS_ERRORS else code, metadata, message)


def merge_reasoning_details(target, parts):
    """Assemble stream fragments in arrival order, keeping IDs, indexes and signatures."""
    if not isinstance(parts, list):
        raise ValueError('Invalid reasoning details')
    for part in parts:
        if not isinstance(part, dict):
            raise ValueError('Invalid reasoning details')
        kind, index, identifier = part.get('type'), part.get('index'), part.get('id')
        slot = None
        if index is not None:
            slot = next((item for item in reversed(target) if item.get('type') == kind and item.get('index') == index), None)
        elif target and target[-1].get('type') == kind and target[-1].get('index') is None:
            slot = target[-1]
        if slot is not None and identifier and slot.get('id') and slot['id'] != identifier:
            slot = None
        if slot is None:
            if len(target) >= _MAX_DETAILS:
                raise _error('OpenRouter reasoning is too large')
            target.append(copy.deepcopy(part))
            continue
        for key, value in part.items():
            if key in {'text', 'summary', 'data'} and isinstance(value, str):
                slot[key] = str(slot.get(key) or '') + value
            elif value is not None:
                slot[key] = copy.deepcopy(value)
    if len(json.dumps(target, ensure_ascii=False, allow_nan=False)) > _MAX_DETAIL_CHARS:
        raise _error('OpenRouter reasoning is too large')
    return target


class _CompletionStream(StreamAccumulator):
    """Accumulate one stream until [DONE], rejecting failures after any terminal chunk."""

    def __init__(self):
        """Keep OpenRouter-only reasoning and model data outside the shared accumulator."""
        super().__init__('chat')
        self.max_content_chars = MAX_CONTENT_CHARS
        self.details, self.model, self.terminal = [], '', ''

    def _failure(self, code, metadata, message=None):
        """Describe a failed stream, naming discarded output and the unexecuted tools."""
        reason = _failure_reason(code, metadata, message)
        if any(self.text) or self.calls:
            return _error(f'{reason}. The stream failed after partial output; '
                          'the incomplete response was discarded and no PBGui tool was run')
        return _error(reason)

    def prepare(self, payload):
        """Normalize one event, or raise before its fragments reach accumulation."""
        if payload.get('error'):
            raise self._failure(*_error_details(payload['error']))
        if isinstance(payload.get('model'), str):
            self.model = payload['model'][:200]
        choices = payload.get('choices') or []
        if not choices:
            return payload
        choice = choices[0]
        delta = choice.get('delta') or {}
        reason = choice.get('finish_reason')
        details = delta.get('reasoning_details')
        reasoning = delta.get('reasoning') or delta.get('reasoning_content')
        if reason == 'error':
            raise self._failure(None, None)
        if self.terminal and (reason not in (None, self.terminal) or delta.get('content')
                              or delta.get('tool_calls') or reasoning or details):
            raise _error('OpenRouter stream continued after finishing; the incomplete response was discarded')
        if reason:
            self.terminal = reason
        if details:
            merge_reasoning_details(self.details, details)
        if not isinstance(reasoning, str) or not reasoning:
            reasoning = ''.join(str(item.get('text') or item.get('summary') or '') for item in details or [] if isinstance(item, dict))
        return {**payload, 'choices': [{**choice, 'delta': {**delta, 'reasoning_content': reasoning}}, *choices[1:]]}

    def finish(self):
        """Return the complete message only after the stream's final [DONE] event."""
        result = super().finish()
        if self.details:
            result['choices'][0]['message']['reasoning_details'] = self.details
        result['model'] = self.model
        return result


def _completion_from_json(payload):
    """Give a non-stream response the same normalized shape as a finished stream."""
    if not isinstance(payload, dict):
        raise _error('OpenRouter API returned an invalid response')
    if payload.get('error'):
        raise _error(_failure_reason(*_error_details(payload['error'])))
    for choice in payload.get('choices') or []:
        message = choice.get('message') if isinstance(choice, dict) else None
        if isinstance(message, dict):
            if isinstance(message.get('reasoning'), str):
                message['reasoning_content'] = message['reasoning']
            if message.get('reasoning_details'):
                try:
                    details = message['reasoning_details']
                    if not isinstance(details, list) or any(not isinstance(part, dict) for part in details):
                        raise ValueError('Invalid reasoning details')
                    if (len(details) > _MAX_DETAILS or
                            len(json.dumps(details, ensure_ascii=False, allow_nan=False)) > _MAX_DETAIL_CHARS):
                        raise _error('OpenRouter reasoning is too large')
                    # These are complete blocks, not deltas. In particular, concatenating
                    # separate encrypted blocks would invalidate their signatures/data.
                    message['reasoning_details'] = copy.deepcopy(details)
                except (TypeError, ValueError) as exc:
                    raise _error('OpenRouter API returned an invalid response') from exc
    return payload


def _base_model(model):
    """Ignore an endpoint variant such as :free when identifying a served model."""
    return model.split(':', 1)[0]


def validate_completion(payload, metadata):
    """Accept only a finished response from the selected model."""
    choices = payload.get('choices') if isinstance(payload, dict) else None
    choice = choices[0] if isinstance(choices, list) and choices else None
    if not isinstance(choice, dict):
        raise _error('OpenRouter returned no completed assistant response')
    reason = choice.get('finish_reason')
    message = choice.get('message')
    message = message if isinstance(message, dict) else {}
    if reason == 'length':
        raise _error('OpenRouter response reached the output limit; incomplete output was not accepted')
    if reason == 'content_filter':
        raise _error('OpenRouter response was stopped by the provider content filter; incomplete output was not accepted')
    if reason not in {'stop', 'tool_calls'}:
        raise _error('OpenRouter response was interrupted before completion; incomplete output was not accepted')
    if bool(message.get('tool_calls')) != (reason == 'tool_calls'):
        raise _error('OpenRouter returned an inconsistent tool response; no PBGui tool was run')
    served = payload.get('model')
    if isinstance(served, str) and served:
        expected = {_base_model(metadata['id']), _base_model(metadata.get('canonical_slug') or metadata['id'])}
        if _base_model(served) not in expected:
            raise _error('OpenRouter answered with a different model; the response was not accepted')


def validate_tool_calls(message, allowed, max_argument_bytes):
    """Parse every call before any executes; one invalid call rejects the whole set."""
    calls = message.get('tool_calls')
    if not isinstance(calls, list):
        raise _error('OpenRouter returned an invalid capability call; no PBGui tool was run')
    parsed, seen = [], set()
    for call in calls:
        function = call.get('function') if isinstance(call, dict) else None
        name = function.get('name') if isinstance(function, dict) else None
        raw = function.get('arguments') if isinstance(function, dict) else None
        identifier = call.get('id') if isinstance(call, dict) else None
        if (not isinstance(identifier, str) or not identifier or identifier in seen or name not in allowed
                or not isinstance(raw, str) or len(raw.encode('utf-8')) > max_argument_bytes):
            raise _error('OpenRouter returned an invalid capability call; no PBGui tool was run')
        try:
            arguments = json.loads(raw)
        except ValueError as exc:
            raise _error('OpenRouter returned invalid capability arguments; no PBGui tool was run') from exc
        if not isinstance(arguments, dict):
            raise _error('OpenRouter returned invalid capability arguments; no PBGui tool was run')
        seen.add(identifier)
        parsed.append((identifier, name, arguments))
    return parsed


def assistant_turn(message):
    """Replay one tool-calling turn, preserving reasoning for the required follow-up."""
    turn = {'role': 'assistant', 'content': message.get('content') or None, 'tool_calls': message['tool_calls']}
    if message.get('reasoning_details'):
        turn['reasoning_details'] = message['reasoning_details']
    elif message.get('reasoning_content'):
        turn['reasoning'] = message['reasoning_content']
    return turn


async def _read_error_body(response):
    """Read a bounded error body; the remainder is discarded with the connection."""
    chunks, total = [], 0
    async for chunk in response.content.iter_chunked(8 * 1024):
        chunks.append(chunk)
        total += len(chunk)
        if total >= _MAX_ERROR_BYTES:
            break
    return b''.join(chunks)[:_MAX_ERROR_BYTES]


async def request(session, api_key, body, metadata, *, on_text=None, on_reasoning=None, data_collection=None):
    """Send only to OpenRouter and return a validated, completely received response."""
    from ai_chat import AIChatError, AIChatService
    payload = copy.deepcopy(body)
    payload['stream'] = True
    apply_routing(payload, data_collection() if data_collection is not None else 'deny')
    try:
        async with session.post(
            f'{CHAT_BASE_URL}/chat/completions',
            headers={'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'},
            json=payload, allow_redirects=False,
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT, connect=15, sock_read=120),
        ) as response:
            if response.status != 200:
                _log(SERVICE, f'OpenRouter chat failed with HTTP {response.status}', level='WARNING')
                raise _error(status_message(response.status, await _read_error_body(response)))
            if 'application/json' in response.headers.get('Content-Type', '').lower():
                result = _completion_from_json(await AIChatService._read_json_response(
                    response, expected_status=200, max_bytes=2 * 1024 * 1024))
            else:
                stream = _CompletionStream()
                result = await read_chat_stream(
                    response.content, stream, 'OpenRouter', on_text=on_text, on_reasoning=on_reasoning,
                    prepare=stream.prepare, max_bytes=stream_limit(payload.get('max_tokens')))
    except AIChatError:
        raise
    except TimeoutError as exc:
        raise _error('OpenRouter request timed out') from exc
    except aiohttp.ClientError as exc:
        raise _error('OpenRouter connection failed') from exc
    validate_completion(result, metadata)
    return result


def _probe_body(metadata, messages, tools, effort, apply_budget):
    """Build one probe request with the same bounds and reasoning setting as chat."""
    body = {'model': metadata['id'], 'messages': messages, 'tools': tools}
    apply_reasoning(body, effort)
    apply_budget(body, 'chat', metadata)
    body['max_tokens'] = min(body['max_tokens'], PROBE_OUTPUT_TOKENS)
    return body


async def run_tool_probe(session, api_key, metadata, effort, apply_budget, *, data_collection=None):
    """Prove one synthetic tool roundtrip; no PBGui capability, data or conversation is used."""
    nonce = uuid4().hex
    tools = [{'type': 'function', 'function': {
        'name': PROBE_TOOL,
        'description': 'Synthetic PBGui connection probe. It has no effect.',
        'parameters': {'type': 'object', 'properties': {'nonce': {'type': 'string'}},
                       'required': ['nonce'], 'additionalProperties': False},
    }}]
    messages = [{'role': 'system', 'content': 'This is a PBGui connection probe. Follow the user instruction exactly.'},
                {'role': 'user', 'content': f'Call {PROBE_TOOL} exactly once with nonce {nonce}, '
                                            'then reply with the single word OK.'}]
    first = await request(session, api_key, _probe_body(metadata, messages, tools, effort, apply_budget), metadata,
                          data_collection=data_collection)
    message = first['choices'][0]['message']
    calls = validate_tool_calls({'tool_calls': message.get('tool_calls') or []}, {PROBE_TOOL}, 1024)
    if len(calls) != 1 or calls[0][2] != {'nonce': nonce}:
        raise _error('The model did not complete the synthetic tool call')
    messages = [*messages, assistant_turn(message),
                {'role': 'tool', 'tool_call_id': calls[0][0], 'content': json.dumps({'ok': True, 'nonce': nonce})}]
    follow_up = _probe_body(metadata, messages, tools, effort, apply_budget)
    if metadata.get('tool_choice'):
        follow_up['tool_choice'] = 'none'
    second = await request(session, api_key, follow_up, metadata, data_collection=data_collection)
    answer = second['choices'][0]['message']
    if answer.get('tool_calls') or not str(answer.get('content') or '').strip():
        raise _error('The model did not finish the synthetic tool roundtrip')
