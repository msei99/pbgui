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
                if message.get('role') == 'user' and not message.get('failed')]
    text = messages[-1].get('content') if messages else None
    if not isinstance(text, str) or not text.strip():
        raise ValueError('An owned chat with the current original Loop request is required')
    if len(text) > 8000:
        raise ValueError('The original Loop request exceeds 8000 characters; shorten it without losing constraints')
    return {'conversation_id': identifier(conversation_id), 'text': text,
            'digest': digest({'conversation_id': conversation_id, 'text': text})}


def bind_request(request, instructions, reuse_existing=False):
    """Give each reviewed definition revision a fresh single-run identity."""
    return {**request, 'binding_id': uuid.uuid4().hex,
            'instruction_digest': instructions['digest'],
            'mode': 'reconciled_reuse' if reuse_existing else 'new'}


def validate_request(settings, request, instruction_digest=None):
    """Reject stale goals, foreign chats and unreconciled legacy definitions."""
    binding = (settings.get('source') or {}).get('loop_request')
    if not isinstance(binding, dict) or any(binding.get(k) != request.get(k) for k in ('conversation_id', 'digest', 'text')):
        raise ValueError('Create a new Loop definition from the current chat request before starting. '
                         'Old definitions cannot be reused silently; explicit reuse requires a fresh reconciliation proposal.')
    identifier(binding.get('conversation_id'))
    identifier(binding.get('binding_id'))
    if (binding.get('mode') not in {'new', 'reconciled_reuse'}
            or binding['digest'] != digest({'conversation_id': binding['conversation_id'], 'text': binding['text']})):
        raise ValueError('Invalid Loop request provenance; create a fresh reviewed definition')
    goals = settings.get('goals') or {}
    if goals.get('text') != request['text'] or goals.get('presets') or goals.get('targets'):
        raise ValueError('The Loop goals differ from the original request. Reconcile the definition before starting')
    if instruction_digest is not None and binding.get('instruction_digest') != instruction_digest:
        raise ValueError('Default Loop instructions changed; create a fresh reviewed definition')
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
