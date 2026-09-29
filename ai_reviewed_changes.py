"""User-reviewed config changes: typed proposals, one-use review tickets, no agent powers."""
from __future__ import annotations

import copy
import hashlib
import hmac
import math
import re
import secrets
import time
from contextlib import nullcontext
from uuid import uuid4

from file_lock import advisory_file_lock
from secure_files import read_regular_file_nofollow, secure_private_file

ACTION = 'reviewed_config_change'
PROPOSAL_TTL = 600
REVIEW_TTL = 120


class ReviewedConfigChanges:
    """Keep the explicit user execution bridge separate from ordinary AI capabilities."""

    def __init__(self, capabilities):
        """Own bounded, ephemeral review tickets; restarting invalidates all tickets."""
        self.caps = capabilities
        self.tickets = {}

    @staticmethod
    def _error(message):
        """Avoid a module import cycle while retaining the public safe error contract."""
        from ai_capabilities import AICapabilityError
        return AICapabilityError(message)

    def _target(self, kind, version, name):
        """Resolve only existing managed resources, never a path supplied by the model."""
        name = self.caps._name(name)
        version = self.caps._version({'version': version})
        if kind not in {'run', 'backtest', 'optimizer'} or (kind == 'optimizer' and version != 'v8'):
            raise self._error('Supported sources are PB7/PB8 Run/Backtest and PB8 Optimizer configs')
        if kind == 'optimizer':
            from api import optimize_v8
            path = optimize_v8._config_file(name)
            root = optimize_v8._configs_dir()
        else:
            path = self.caps._managed_config_location(kind, version, name)
            root = path.parent.parent
        if any(item.is_symlink() for item in (root.parent, root, path.parent, path)) or not path.is_file():
            raise self._error('Config source is unavailable or unsafe')
        path.resolve().relative_to(root.resolve())
        return path

    @staticmethod
    def _lock(kind, version):
        """Share the exact writer lock for every supported in-place save."""
        if version == 'v8' and kind in {'optimizer', 'backtest'}:
            if kind == 'optimizer':
                from api import optimize_v8 as module
            else:
                from api import backtest_v8 as module
            return module._config_lock()
        # These sources are never overwritten: approval creates a private draft.
        return nullcontext()

    def _snapshot(self, kind, version, name):
        """Read a canonical config and bind the whole bounded JSON bundle by content."""
        path = self._target(kind, version, name)
        paths = sorted(path.parent.glob('*.json'))
        if len(paths) > 200:
            raise self._error('Config bundle is too large for a reviewed change')
        def fingerprint():
            entries = []
            for source in paths:
                raw = read_regular_file_nofollow(source, path.parent)
                if len(raw) > 1024 * 1024:
                    raise self._error('Config bundle member is too large')
                entries.append((source.name, hashlib.sha256(raw).hexdigest()))
            return self.caps._digest(entries)
        before = fingerprint()
        if version == 'v8':
            from pb8_config import load_pb8_config
            config = load_pb8_config(path)
        else:
            from pb7_config import load_pb7_config
            config = load_pb7_config(path)
        if before != fingerprint() or paths != sorted(path.parent.glob('*.json')):
            raise self._error('Config changed while reading; request a new proposal')
        return config, before

    def _snapshot_locked(self, kind, version, name):
        """Use the same writer lock while checking the source at review time."""
        with self._lock(kind, version):
            return self._snapshot(kind, version, name)

    def _reject_credentials(self, value):
        """Never persist inherited credentials inside an AI proposal or draft."""
        if isinstance(value, dict):
            for key, item in value.items():
                if str(key) == 'override_config_path' and item:
                    raise self._error('File-based coin overrides require the regular config editor; no reviewed overwrite is offered')
                normalized = re.sub(r'[^a-z0-9]', '', str(key).lower())
                if any(part in normalized for part in ('apikey', 'password', 'privatekey', 'secret', 'token', 'credential', 'authorization', 'cookie', 'sshkey')):
                    raise self._error('Credential-bearing configs cannot enter reviewed AI changes')
                self._reject_credentials(item)
        elif isinstance(value, list):
            for item in value:
                self._reject_credentials(item)

    def _validate_value(self, path, value, current):
        """Coin lists cannot become filenames; bot parameters must remain finite scalars."""
        if path.startswith('/live/'):
            lists = list(value.values()) if isinstance(value, dict) and set(value) <= {'long', 'short'} else [value]
            if isinstance(value, dict) and not value:
                raise self._error('Coin-list object is empty')
            if any(not isinstance(items, list) or len(items) > 1000 or any(not isinstance(coin, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:/-]{0,79}', coin) or '..' in coin for coin in items) for items in lists):
                raise self._error('Coin lists must contain explicit public symbols, never file paths or commands')
        else:
            previous = current
            for part in path.strip('/').split('/'):
                if not isinstance(previous, dict) or part not in previous:
                    raise self._error('Bot parameter does not exist')
                previous = previous[part]
            if isinstance(previous, bool):
                valid = isinstance(value, bool)
            else:
                valid = isinstance(previous, (int, float)) and not isinstance(value, bool) and isinstance(value, (int, float)) and math.isfinite(value)
            if not valid:
                raise self._error('Bot changes must preserve numeric or boolean parameter types')

    def _prepare(self, owner, args):
        """Validate proposed trading fields without saving, queueing, deploying or restarting."""
        if set(args) != {'kind', 'version', 'name', 'operations'}:
            raise self._error('Provide only kind, version, name and operations')
        kind, version, name = args['kind'], self.caps._version(args), self.caps._name(args['name'])
        operations = args['operations']
        if not isinstance(operations, list) or not 1 <= len(operations) <= 64:
            raise self._error('Provide 1 to 64 explicit trading-field changes')
        with self._lock(kind, version):
            current, baseline = self._snapshot(kind, version, name)
            self._reject_credentials(current)
            candidate = copy.deepcopy(current)
            for operation in operations:
                path = operation.get('path', '') if isinstance(operation, dict) else ''
                if not isinstance(path, str) or not re.fullmatch(r'/(?:bot/(?:long|short)/[A-Za-z0-9_]+|live/(?:approved_coins|ignored_coins)(?:/(?:long|short))?)', path):
                    raise self._error('Only bot long/short parameters and approved/ignored coin lists can be proposed')
                if operation.get('op') != 'replace':
                    raise self._error('Reviewed changes must replace existing fields')
                self._validate_value(path, operation.get("value"), current)
                self.caps._apply_json_patch_operation(candidate, operation)
            mode = 'save' if version == 'v8' and kind in {'optimizer', 'backtest'} else 'draft'
            draft = None
            if mode == 'draft':
                candidate = self.caps._sanitize_config(candidate)
                self.caps._require_safe_draft(candidate)
                draft = self.caps._validated_draft_payload(owner, uuid4().hex, version, candidate, revision=1)
                if not draft['validation']['valid']:
                    raise self._error('Config validation failed: ' + '; '.join(draft['validation']['errors']))
                candidate = draft['config']
                current = self.caps._sanitize_config(current)
            elif kind == 'optimizer':
                candidate = self.caps._validate_pb8_config(name, candidate)
                self.caps._preserve_protected_config_fields(current, candidate)
            else:
                from pb8_config import prepare_pb8_config
                candidate = prepare_pb8_config(candidate, base_config_path=str(self._target(kind, version, name)))
                self.caps._preserve_protected_config_fields(current, candidate)
            self.caps._require_bounded_config(candidate)
            changes = self.caps._changed_entries(current, candidate)
            if not changes or len(changes) > 200:
                raise self._error('The proposal must have 1 to 200 reviewable field changes')
            # Normalization must not smuggle a change outside the reviewed trading fields.
            changed = self.caps._changed_paths(current, candidate)
            if mode == 'save' and any(not (p.startswith('bot.long.') or p.startswith('bot.short.') or p.startswith('live.approved_coins') or p.startswith('live.ignored_coins')) for p in changed):
                raise self._error('Runtime normalization changes other fields; normalize this config in its editor first')
            payload = {'kind': kind, 'version': version, 'name': name, 'mode': mode,
                       'baseline': baseline, 'config': candidate, 'draft': draft}
            preview = {'action': ACTION, 'name': name, 'version': version, 'kind': kind, 'mode': mode,
                       'baseline': baseline, 'changed_count': len(changes), 'changes': changes,
                       'changes_truncated': False, 'may_start_immediately': False,
                       'effect': 'Save config only' if mode == 'save' else 'Create private config draft; live config unchanged'}
            payload["review"] = copy.deepcopy(preview)
            return payload, preview

    async def propose(self, owner, conversation_id, args):
        """The only model-facing entry creates an inert proposal and keeps the chat restricted."""
        payload, preview = await self.caps._to_thread_uncancellable(self._prepare, owner, args)
        self.caps.restrict_to_analysis(owner, conversation_id)
        return await self.caps._create_custom_proposal(owner, conversation_id, ACTION, payload['name'], payload, preview,
                                                       expected_digest=payload['baseline'], create_only=False)

    def _verify(self, proposal):
        """Reject expired, rebound or modified persisted payloads before review or execution."""
        if proposal.action != ACTION or not isinstance(proposal.config, dict):
            raise self._error('Not a reviewed config change')
        if time.time() - proposal.created_at > PROPOSAL_TTL:
            raise self._error('Change proposal expired; request a new proposal')
        digest = self.caps._digest({'action': ACTION, 'name': proposal.name, 'payload': proposal.config})
        if not hmac.compare_digest(digest, proposal.payload_digest):
            raise self._error('Change proposal integrity check failed')
        payload = proposal.config
        if payload.get('review') != proposal.preview:
            raise self._error('Displayed review does not match the bound payload')
        if payload.get('name') != proposal.name or payload.get('baseline') != proposal.expected_digest:
            raise self._error('Change proposal target binding failed')
        expected_mode = 'save' if payload.get('version') == 'v8' and payload.get('kind') in {'optimizer', 'backtest'} else 'draft'
        if payload.get('mode') != expected_mode:
            raise self._error('Unsupported change execution mode')
        return payload

    async def review(self, owner, proposal_id, conversation_id, digest):
        """Issue a short-lived capability only to the authenticated, non-tool review endpoint."""
        proposal = await self.caps._owned_proposal(owner, proposal_id)
        async with proposal.lock:
            self._verify(proposal)
            if proposal.conversation_id != conversation_id or proposal.payload_digest != digest or proposal.status != 'awaiting_approval':
                raise self._error('Review does not match a pending proposal')
            payload = proposal.config
            _, baseline = await self.caps._to_thread_uncancellable(self._snapshot_locked, payload['kind'], payload['version'], proposal.name)
            if baseline != payload['baseline']:
                raise self._error('Config changed since proposal; request a new comparison')
            now = time.monotonic()
            self.tickets = {key: value for key, value in self.tickets.items() if value['expires'] > now}
            if len(self.tickets) >= 64 and proposal_id not in self.tickets:
                raise self._error('Review capacity reached')
            token = secrets.token_hex(32)
            self.tickets[proposal_id] = {'hash': hashlib.sha256(token.encode()).hexdigest(), 'owner': owner,
                'conversation_id': conversation_id, 'digest': digest, 'expires': now + REVIEW_TTL}
            return {'proposal': self.caps._proposal_projection(proposal), 'review_token': token, 'expires_in': REVIEW_TTL}

    def consume(self, proposal, owner, conversation_id, digest, token):
        """One use, exact owner/conversation/payload, no durable recovery authorization."""
        self._verify(proposal)
        ticket = self.tickets.get(proposal.id)
        if not isinstance(token, str) or not ticket or ticket['expires'] <= time.monotonic() or ticket['owner'] != owner or ticket['conversation_id'] != conversation_id or ticket['digest'] != digest or not hmac.compare_digest(ticket['hash'], hashlib.sha256(token.encode()).hexdigest()):
            raise self._error('Explicit review is required or has expired')
        del self.tickets[proposal.id]

    def execute(self, proposal):
        """Execute the reviewed local effect once, without any queue or live-save API."""
        if proposal.status != "executing":
            raise self._error("Execution requires a consumed explicit approval")
        payload = self._verify(proposal)
        kind, version, name = payload['kind'], payload['version'], payload['name']
        with self._lock(kind, version):
            _, baseline = self._snapshot(kind, version, name)
            if baseline != payload['baseline']:
                raise self._error('Config changed after review; nothing was applied')
            if payload['mode'] == 'draft':
                draft = payload['draft']
                if not isinstance(draft, dict) or draft.get('owner') != proposal.owner or draft.get('config') != payload['config']:
                    raise self._error('Draft binding failed')
                with advisory_file_lock(self.caps.lock_target):
                    from ai_capabilities import _MAX_DRAFTS_PER_OWNER, _MAX_DRAFTS_GLOBAL
                    if len(self.caps._owner_files(self.caps.draft_root, proposal.owner)) >= _MAX_DRAFTS_PER_OWNER or self.caps._count_private_files(self.caps.draft_root) >= _MAX_DRAFTS_GLOBAL:
                        raise self._error('Draft capacity reached')
                    target = self.caps._owner_path(self.caps.draft_root, proposal.owner, draft['id'])
                    if target.exists():
                        raise self._error('Reviewed draft already exists')
                    self.caps._write_private_json(target, draft)
                result = {'draft_id': draft['id'], 'resource': f"pbgui://draft/{version}/{draft['id']}"}
            else:
                from pb8_config import save_prepared_pb8_config
                target = self._target(kind, version, name)
                save_prepared_pb8_config(copy.deepcopy(payload['config']), target)
                secure_private_file(target)
                result = {}
        return {'proposal_id': proposal.id, 'status': 'executed', 'action': ACTION, 'name': name,
                'mode': payload['mode'], 'kind': kind, 'version': version, **result}
