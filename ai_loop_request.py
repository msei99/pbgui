"""Bind reviewed chat Loop definitions to the user's original request."""
from pathlib import Path
import json
import uuid

from file_lock import advisory_file_lock
from pb8_loop_store import digest, identifier
from secure_files import atomic_write_private_text, ensure_private_directory, read_regular_file_nofollow

SERVICE = 'AILoopRequest'


def current_request(conversation_id, conversation):
    """Read the latest visible user message, never assistant-generated instructions."""
    messages = [message for message in conversation.get('messages', [])
                if message.get('role') == 'user' and not message.get('failed') and not message.get('hidden')]
    text = messages[-1].get('content') if messages else None
    if not isinstance(text, str) or not text.strip():
        raise ValueError('An owned chat with the current original Loop request is required')
    if len(text) > 8000:
        raise ValueError('The original Loop request exceeds 8000 characters; shorten it without losing constraints')
    from ai_loop_windows import conversation_window_request
    result = {'conversation_id': identifier(conversation_id), 'text': text,
              'digest': digest({'conversation_id': conversation_id, 'text': text})}
    plan, window_request = conversation_window_request(conversation)
    if plan is not None:
        result['window_plan'] = plan
        if window_request != text:
            result['window_request'] = window_request
    return result


def bind_request(request, instructions, reuse_existing=False):
    """Give each reviewed definition revision a fresh single-run identity."""
    return {**request, 'binding_id': uuid.uuid4().hex,
            'instruction_digest': instructions['digest'],
            'mode': 'reconciled_reuse' if reuse_existing else 'new'}


def preserve_request(request, instructions, definition, owner):
    """Bind an explicit unchanged reuse review without replacing saved objectives."""
    from ai_loop_windows import plan_from_text
    previous = definition['source'].get('loop_request') or {}
    goals = definition['settings'].get('goals') or {}
    window_request = previous.get('window_request', goals.get('text', ''))
    if not isinstance(window_request, str):
        raise ValueError('Saved Loop window request is invalid; reconcile the definition')
    # Keep the revision single-use, including legacy definitions without a binding.
    binding_id = previous.get('binding_id') or digest({
        'owner': owner, 'name': definition['name'], 'revision': definition['revision']})[:32]
    identifier(binding_id)
    return {**request, 'binding_id': binding_id, 'instruction_digest': instructions['digest'],
            'mode': 'preserved_reuse', 'goals_digest': digest(goals),
            'window_request': window_request, 'window_plan': plan_from_text(window_request)}


def validate_request(settings, request, instruction_digest=None, config=None):
    """Reject stale goals, foreign chats and unreconciled legacy definitions."""
    binding = (settings.get('source') or {}).get('loop_request')
    if not isinstance(binding, dict) or any(binding.get(k) != request.get(k) for k in ('conversation_id', 'digest', 'text')):
        raise ValueError('Create a new Loop definition from the current chat request before starting. '
                         'Old definitions cannot be reused silently; for an explicit unchanged restart, '
                         'prepare propose_ai_loop_run with reuse_existing=true and the exact revision.')
    identifier(binding.get('conversation_id'))
    identifier(binding.get('binding_id'))
    preserved = binding.get('mode') == 'preserved_reuse'
    if (binding.get('mode') not in {'new', 'reconciled_reuse', 'preserved_reuse'}
            or binding['digest'] != digest({'conversation_id': binding['conversation_id'], 'text': binding['text']})):
        raise ValueError('Invalid Loop request provenance; create a fresh reviewed definition')
    goals = settings.get('goals') or {}
    if preserved and binding.get('goals_digest') != digest(goals):
        raise ValueError('Saved Loop goals changed since the unchanged reuse review')
    if not preserved and (goals.get('text') != request['text'] or goals.get('presets') or goals.get('targets')):
        raise ValueError('The Loop goals differ from the original request. Reconcile the definition before starting')
    if instruction_digest is not None and binding.get('instruction_digest') != instruction_digest:
        raise ValueError('Default Loop instructions changed; create a fresh reviewed definition')
    if config is not None:
        from ai_loop_search_limits import exposure_ceiling, validate_exposure_ceiling
        validate_exposure_ceiling(config, exposure_ceiling(settings))
    from ai_loop_windows import plan_from_text, verify_config
    plan = plan_from_text(binding['window_request'] if preserved else request.get('window_request', request['text']))
    if ((preserved and binding.get('window_plan') != plan) or (not preserved and (
            binding.get('window_request') != request.get('window_request')
            or binding.get('window_plan') != plan or request.get('window_plan') != plan))):
        raise ValueError('The reviewed Loop window plan changed; create a fresh definition')
    if plan is not None and config is not None:
        verify_config(plan, config)
    if plan is not None:
        if 'max_twe' in plan and goals.get('max_twe') != plan['max_twe']:
            raise ValueError('Loop goals must preserve the reviewed maximum TWE ceiling')
        if 'max_validations' in plan and settings.get('max_validations') != plan['max_validations']:
            raise ValueError('Loop settings must preserve the reviewed CPU training validation budget')
    return binding


def claim_request(root, owner, binding_id, proposal_id):
    """Claim a definition once across concurrent approvals, retaining same-proposal replay."""
    root = Path(root)
    directory = ensure_private_directory(root / 'loop_request_claims')
    directory = ensure_private_directory(directory / identifier(owner))
    path = directory / (identifier(binding_id) + '.json')
    identifier(proposal_id)
    with advisory_file_lock(path):
        if path.exists() or path.is_symlink():
            saved = json.loads(read_regular_file_nofollow(path, root))
            if saved.get('proposal_id') != proposal_id:
                raise ValueError('This reviewed Loop definition has already been used. Create a fresh definition')
            return
        atomic_write_private_text(path, json.dumps({'proposal_id': proposal_id}, indent=4) + '\n')
