"""Owner-scoped Claude subscription decisions with the Loop's read-only evidence tool."""
from __future__ import annotations

import asyncio
import json
import time

from pb8_loop_ai_policy import check_context
from pb8_loop_evidence import LoopAIPause, TOOL_DESCRIPTION, TOOL_SCHEMA, encode, sha

SERVICE = 'PB8Loop'


def _decision_schema(active):
    """Specify each Loop stage's typed result; native and domain validation both apply."""
    snapshot = active['session'].snapshot
    stage, evidence = snapshot['phase'], snapshot['data']['evidence']
    text = {'type': 'string'}
    properties = {'reason': {**text, 'minLength': 1}, 'knowledge': text,
                  'knowledge_hypothesis': text, 'jev_needed': {'type': 'boolean'}}
    required = ['reason']
    if stage == 'interpret':
        properties['rubric'] = {'type': 'array', 'minItems': 1, 'maxItems': 24, 'items': {
            'type': 'object', 'properties': {'goal': text,
                'metrics': {'type': 'array', 'items': text, 'maxItems': 10},
                'direction': {'type': 'string', 'enum': ['min', 'max']},
                'target': {'type': ['number', 'null']}, 'description': text,
                'scale': {'type': 'number'}, 'weight': {'type': 'number'}},
            'required': ['goal', 'metrics', 'direction', 'target'], 'additionalProperties': True}}
        required.append('rubric')
    elif stage == 'select':
        identifiers = [item['id'] for item in evidence['candidates']]
        properties['candidate_ids'] = {'type': 'array', 'items': {'type': 'string', 'enum': identifiers},
                                       'minItems': 1, 'maxItems': evidence['max_candidates'], 'uniqueItems': True}
        required.append('candidate_ids')
    elif stage in {'bootstrap', 'evaluate'}:
        config = {'type': 'object', 'properties': {key: {'type': 'object'}
                  for key in ('backtest', 'bot', 'live', 'optimize')},
                  'required': ['backtest', 'bot', 'live', 'optimize'], 'additionalProperties': True}
        properties['variants'] = {'type': 'array', 'items': {'type': 'object',
            'properties': {'config': config, 'overrides': {'type': 'object'}, 'reason': {**text, 'minLength': 1}},
            'required': ['config', 'overrides', 'reason'], 'additionalProperties': False}}
        required.append('variants')
        if stage == 'evaluate':
            properties.update(winner_operation={'type': ['string', 'null'],
                'enum': [*evidence.get('winner_options', {}), None]}, finish={'type': 'boolean'},
                contradicts={'type': 'array', 'items': text},
                evaluation_criteria={'type': 'array', 'minItems': 1, 'maxItems': 24, 'items': {
                    'type': 'object', 'properties': {'metric': text,
                        'direction': {'type': 'string', 'enum': ['min', 'max']},
                        'reducer': {'type': 'string', 'enum': ['mean', 'min', 'max', 'median']}, 'reason': text},
                    'required': ['metric', 'direction', 'reducer', 'reason'], 'additionalProperties': False}},
                candidate_assessments={'type': 'object', 'additionalProperties': text})
            required.extend(['winner_operation', 'finish'])
            if (evidence.get('evaluation_policy') or {}).get('mode') == 'adaptive':
                required.extend(['evaluation_criteria', 'candidate_assessments'])
    else:
        raise ValueError('Unsupported Claude Loop decision stage')
    return {'type': 'object', 'properties': properties, 'required': required, 'additionalProperties': False}


async def claude_dialog(ai, record, content, output_tokens, active):
    """Keep evidence coverage, cancellation and durable accounting outside the native SDK."""
    from pb8_loop_controller import owned_thread
    settings, session, ledger = record['settings'], active['session'], active['ledger']
    output_schema = _decision_schema(active)
    instructions = active['instructions'] + (
        '\nSubmit the final decision through the SDK StructuredOutput format tool. '
        'This only submits the final response; loop_evidence remains the sole evidence reader.')
    feedback = ''
    rejections = []
    for dialog in range(2):
        if dialog:
            session.start_dialog()
        message = encode({'content': content, **({'decision_correction': feedback} if feedback else {})})
        payload = {'instructions': instructions, 'message': message, 'tool': TOOL_SCHEMA,
                   'model': settings['model'], 'effort': settings.get('effort', ''), 'output_schema': output_schema}
        count = check_context(payload, active['profile'], output_tokens, codex=True,
                              required_pages=len(session.required_pages), initial=True)
        call_id = await owned_thread(ledger.reserve, count, output_tokens, sha(payload))
        usage = {}
        diagnostics = {'provider': 'claude-code', 'phase': 'model_request', 'started_at': time.time(),
                       'input_characters': len(message), 'outcome': 'interrupted'}

        async def evidence(name, arguments, native_call_id):
            """No chat tools, approvals, commands or model-supplied owner IDs are accepted."""
            if name != 'loop_evidence':
                return {'error': 'tool_not_allowed'}
            try:
                return await owned_thread(session.read, arguments, native_call_id)
            except (ValueError, KeyError, IndexError):
                return {'error': 'invalid_evidence_request'}

        async def event(value):
            """Compaction invalidates mandatory coverage; retain only numeric usage telemetry."""
            if value.get('event') == 'compaction':
                session.compacted()
            elif value.get('event') == 'usage':
                for key in ('input_tokens', 'output_tokens', 'context_window'):
                    number = value.get(key)
                    if type(number) is int and 0 <= number <= 10**12:
                        usage[key] = number

        try:
            remaining = await owned_thread(ledger.remaining)
            timeout = min(remaining, settings.get('model_call_timeout_seconds') or active['profile']['call_timeout'])
            text = await ai.service()._claude_reply(record['owner'], settings['model'],
                [{'role': 'user', 'content': message}], instructions,
                tools=[{'name': 'loop_evidence', 'description': TOOL_DESCRIPTION, 'input_schema': TOOL_SCHEMA}],
                handler=evidence, handler_with_id=True, on_event=event, effort=settings.get('effort', ''),
                timeout_seconds=timeout, output_tokens=output_tokens, max_reply_chars=1_048_576,
                output_schema=output_schema)
            diagnostics['outcome'] = 'completed'
        except asyncio.CancelledError:
            # The server cannot know whether a provider completed after cancellation.
            # Retain pending_ai and its reservation for the existing recovery flow.
            raise
        except BaseException as exc:
            diagnostics['outcome'] = 'error'
            await owned_thread(ledger.timeout, call_id)
            if str(exc) == 'Claude did not return the required structured decision.':
                diagnostics.update(decision_status='rejected', rejection={'kind': 'structured_output'})
                raise LoopAIPause('evidence_recovery', str(exc),
                                  stage=session.snapshot['phase'], kind='structured_output') from exc
            raise
        finally:
            diagnostics['ended_at'] = time.time()
            await owned_thread(ai.store.update, record['owner'], record['id'], lambda row: row.update(
                last_ai_request={'id': call_id, 'stage': session.snapshot['phase'], **diagnostics}))
        await owned_thread(ledger.settle, call_id, usage)
        window = usage.get('context_window')
        if type(window) is int and window > 0:
            await owned_thread(ai.profiles.observe_window, {**settings, 'profile_owner': record['owner']}, active['metadata'], window)
            active['profile'].update(context=window, context_confirmed=True, context_pending=False, context_source='runtime')
            check_context(payload, active['profile'], output_tokens, codex=True,
                          required_pages=len(session.required_pages))
        if session.missing():
            feedback = 'Mandatory evidence was compacted before completion. Re-evaluate the full supplied package.'
            rejection = {'kind': 'missing_evidence', 'missing_pages': session.missing()}
            await _retain_rejection(ai, record, call_id, session, text, rejection)
            rejections.append(rejection)
            continue
        try:
            if active.get('validate'):
                active['validate'](text)
        except (ValueError, KeyError, TypeError) as exc:
            feedback = 'Correct the JSON decision contract using the same evidence: ' + str(exc)[:500]
            if isinstance(exc, json.JSONDecodeError):
                rejection = {'kind': 'invalid_json', 'message': exc.msg[:200],
                             'line': exc.lineno, 'column': exc.colno, 'position': exc.pos}
                feedback += (' Return exactly one valid JSON object. Separate all members with commas; '
                             'use quoted keys and strings, with no comments or arithmetic expressions.')
            else:
                from ai_chat import AIChatService
                rejection = {'kind': 'decision_contract', 'error_type': type(exc).__name__,
                             'message': AIChatService._redact_page_evidence(str(exc))[:500]}
            await _retain_rejection(ai, record, call_id, session, text, rejection)
            rejections.append(rejection)
            continue
        return text, {'provider': 'claude-code', 'status': 'subscription', 'tokens': usage}
    raise LoopAIPause('evidence_recovery', 'Claude could not complete the required evidence and decision contract',
                      stage=session.snapshot['phase'], attempts=len(rejections), rejections=rejections)


async def _retain_rejection(ai, record, call_id, session, text, rejection):
    """Retain bounded failure metadata without model output, prompts or configurations."""
    from pb8_loop_controller import owned_thread
    details = {**rejection, 'call_id': call_id, 'round': record['round'],
               'stage': session.snapshot['phase'], 'response_characters': len(text),
               'response_sha256': sha(text), 'at': time.time()}

    def retain(row):
        """Keep only recent failures; preserve newer control state and request ownership."""
        row['claude_decision_rejections'] = (row.get('claude_decision_rejections', []) + [details])[-4:]
        if (row.get('last_ai_request') or {}).get('id') == call_id:
            row['last_ai_request'].update(decision_status='rejected', rejection=details)

    await owned_thread(ai.store.update, record['owner'], record['id'], retain)
