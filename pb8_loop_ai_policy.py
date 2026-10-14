"""Automatic model runtime settings and durable per-call time/cost accounting."""
from __future__ import annotations

import json
import math
from pathlib import Path
import time
import uuid

from file_lock import advisory_file_lock
from secure_files import atomic_write_private_text, ensure_private_directory, read_regular_file_nofollow
from pb8_loop_evidence import LoopAIPause, MIN_CONTEXT, encode, sha, tokens

SERVICE = 'PB8LoopAIPolicy'
PROFILE_VERSION = 1
ADAPTER_PROTOCOL_VERSION = 'loop-evidence-v2.1'
TRANSPORT_CACHE_TTL_SECONDS = 24 * 60 * 60


def adapter_fingerprint():
    """Identify the wire/tool contract without tying support to source edits."""
    return ADAPTER_PROTOCOL_VERSION


def profile_identity(settings, metadata):
    """Key automatic telemetry and spend accounting by the actual selection."""
    return {key: settings.get(key, '') for key in ('provider', 'model', 'profile', 'effort', 'service_tier', 'profile_owner')} | {
        'protocol': 'codex' if settings['provider'] == 'chatgpt' else metadata.get('protocol'),
        'version': PROFILE_VERSION, 'adapter_protocol_version': adapter_fingerprint()}


def budget_identity(settings, metadata):
    """A spend approval is bound to the selected model, limits and quoted prices."""
    return sha([profile_identity(settings, metadata), metadata.get('cost'),
                settings.get('ai_decision_token_limit'), settings.get('ai_decision_usd_limit')])


class ModelProfiles:
    """Automatic runtime settings/cache; no model allowlist, acceptance or probe."""

    def __init__(self, root):
        self.root = Path(root)

    def load(self, settings, metadata, *, allow_stale=False):
        """Use catalog limits or normal-run telemetry without extra model requests."""
        identity = profile_identity(settings, metadata)
        context = metadata.get('context')
        source = 'provider'
        observed_at = None
        cached = {}
        path = self.root / 'model_runtime' / (sha(identity) + '.json')
        if path.exists():
            try:
                value = json.loads(read_regular_file_nofollow(path, self.root))
                if not isinstance(value, dict):
                    raise ValueError('Invalid runtime cache object')
                if value.get('identity') == identity:
                    cached = value
            except (ValueError, OSError) as exc:
                from logging_helpers import human_log as _log
                _log(SERVICE, 'Ignoring unavailable optional model runtime cache', level='WARNING',
                     meta={'error_type': type(exc).__name__})
        if type(context) is not int or context <= 0:
            context = None
        if context is None and settings['provider'] == 'chatgpt':
            if type(cached.get('observed_context')) is int:
                context = cached['observed_context']
                observed_at = cached.get('context_observed_at')
                source = 'runtime_cache'
        pending = context is None and settings['provider'] == 'chatgpt'
        if pending:
            # This is a conservative request-planning floor, not a claimed model
            # capability. The first ordinary decision must confirm native telemetry.
            context, source = MIN_CONTEXT, 'pending_runtime'
        if type(context) is not int or context < MIN_CONTEXT:
            raise LoopAIPause('context_budget', 'Model context must be known and at least 256k')
        now = time.time()
        def recent_transport(field):
            """Expire transport hints independently of normal context observations."""
            observed = cached.get(field + '_observed_at')
            return (type(observed) in (int, float) and math.isfinite(observed)
                    and 0 <= now - observed < TRANSPORT_CACHE_TTL_SECONDS)
        native = (not (recent_transport('tool_mode') and cached.get('tool_mode') == 'json')
                  and metadata.get('loop_tool_support') is not False
                  and metadata.get('tools') is not False)
        return {'identity': identity, 'context': context, 'context_confirmed': not pending,
                'context_source': source, 'context_observed_at': observed_at,
                'context_pending': pending, 'page_tokens': 5000, 'tool_output_tokens': 8192,
                'output_tokens': min(metadata.get('output_limit') or 32768, 32768),
                'streaming': settings['provider'] != 'chatgpt' and not (
                    recent_transport('streaming') and cached.get('streaming') is False),
                'connect_timeout': 30, 'first_timeout': 300, 'idle_timeout': 300,
                'call_timeout': 600, 'tool_mode': 'native' if native else 'json'}

    def observe_window(self, settings, metadata, context):
        """Cache telemetry from an actual decision, never from a separate test call."""
        if type(context) is not int or context < MIN_CONTEXT:
            raise LoopAIPause('context_budget', 'Runtime reported a context below 256k')
        self._observe(settings, metadata, observed_context=context, context_observed_at=time.time())

    def observe_transport(self, settings, metadata, *, streaming=None, tool_mode=None):
        """Remember transport negotiation from normal requests, not model probes."""
        if streaming is not None and type(streaming) is not bool:
            raise ValueError('Streaming observation must be boolean')
        if tool_mode is not None and tool_mode not in {'native', 'json'}:
            raise ValueError('Invalid reader transport')
        fields = {}
        observed_at = time.time()
        if streaming is not None:
            fields.update(streaming=streaming, streaming_observed_at=observed_at)
        if tool_mode is not None:
            fields.update(tool_mode=tool_mode, tool_mode_observed_at=observed_at)
        self._observe(settings, metadata, **fields)

    def _observe(self, settings, metadata, **fields):
        """Best-effort atomic cache updates never create a manual startup gate."""
        identity = profile_identity(settings, metadata)
        try:
            directory = ensure_private_directory(self.root / 'model_runtime')
            path = directory / (sha(identity) + '.json')
            with advisory_file_lock(path):
                value = {}
                if path.exists():
                    try:
                        value = json.loads(read_regular_file_nofollow(path, self.root))
                        if not isinstance(value, dict):
                            value = {}
                    except ValueError:
                        value = {}
                if value.get('identity') != identity:
                    value = {}
                value.update(identity=identity, **fields)
                atomic_write_private_text(path, json.dumps(value, indent=4, allow_nan=False) + '\n')
        except (ValueError, OSError) as exc:
            # Telemetry is already available in this decision. Optional caching
            # must not invalidate its result or become another startup gate.
            from logging_helpers import human_log as _log
            _log(SERVICE, 'Model context cache write deferred', level='WARNING',
                 meta={'error_type': type(exc).__name__})


def check_context(payload, profile, output_tokens, *, codex=False, required_pages=0, initial=False):
    """Check the actual cumulative wire payload; a call quota is not token capacity."""
    count = math.ceil(tokens(payload) * 1.25) + 512
    context = profile['context']
    if type(context) is not int or context < MIN_CONTEXT:
        raise LoopAIPause('context_budget', 'A known context budget of at least 256k is required')
    if codex:
        if len(encode(payload)) > 1_048_576:
            raise LoopAIPause('context_budget', 'Initial message exceeds the runtime character allowance')
        recovery_passes = profile.get('recovery_passes', 1)
        capacity = (64 - profile.get('optional_call_reserve', 12) - profile.get('recovery_call_reserve', 4)) // recovery_passes
        if required_pages > capacity:
            raise LoopAIPause('context_budget', 'Mandatory evidence exceeds the measured recovery call allowance', pages=required_pages)
        ceiling = int(context * .75 * .5)
    else:
        ceiling = int(context * .9)
    optional_reserve = math.ceil(profile.get('additional_context_tokens', 12000) * 1.25) if initial and not codex else 0
    if count + output_tokens + optional_reserve > ceiling:
        raise LoopAIPause('context_budget', 'Evidence and response reserve exceed the safe context budget',
                          input_reserved=count, output_reserved=output_tokens, capacity=ceiling)
    return count


def paid_cost(metadata, input_tokens, output_tokens):
    """Reserve uncached pricing; missing or nonfinite prices never mean free."""
    cost = metadata.get('cost') or {}
    if any(type(cost.get(key)) not in (int, float) or not math.isfinite(cost[key]) or cost[key] < 0
           for key in ('input', 'output')):
        raise LoopAIPause('ai_budget', 'Verified model prices are required before billing')
    # Conservatively cover more expensive cache writes when advertised.
    cache_rate = cost.get('cache_write', 0) or 0
    if type(cache_rate) not in (int, float) or not math.isfinite(cache_rate) or cache_rate < 0:
        raise LoopAIPause('ai_budget', 'Verified cache-write pricing is required before billing')
    input_rate = max(cost['input'], cache_rate)
    return (input_tokens * input_rate + output_tokens * cost['output']) / 1_000_000


class DecisionLedger:
    """One durable ledger survives retries, second dialogs and explicit resume."""

    def __init__(self, store, record, stage, snapshot_sha, metadata):
        self.store, self.record, self.metadata = store, record, metadata
        self.key = sha([record['id'], record['round'], stage, snapshot_sha])
        self.paid = record['settings']['provider'] == 'opencode-zen' and metadata.get('free') is not True
        def initialize(row):
            """Create only once without resetting previous reservations."""
            ledgers = row.setdefault('ai_decisions', {})
            current = ledgers.get(self.key)
            if current is None:
                settings = row['settings']
                explicit = settings.get('decision_timeout_seconds')
                siblings = [item for item in ledgers.values() if item['stage'] == stage and item['round'] == row['round']]
                deadline = min([row['deadline'] - 60] + [item['deadline'] for item in siblings])
                if explicit is not None:
                    deadline = min(deadline, time.time() + explicit)
                ledgers[self.key] = {'stage': stage, 'round': row['round'], 'snapshot_sha': snapshot_sha,
                                     'created_at': time.time(), 'deadline': deadline, 'calls': [],
                                     'tokens': 0, 'usd': 0.0, 'explicit_timeout': explicit is not None}
            elif current['snapshot_sha'] != snapshot_sha:
                raise LoopAIPause('evidence_recovery', 'Decision evidence changed during a reserved attempt')
        self.store.update(record['owner'], record['id'], initialize)

    def state(self):
        """Read authoritative cross-process state rather than an in-memory quota."""
        return self.store.read(self.record['owner'], self.record['id'])['ai_decisions'][self.key]

    def remaining(self):
        """Bound all dialogs and retries by the original deadline."""
        row = self.store.read(self.record['owner'], self.record['id'])
        state = row['ai_decisions'][self.key]
        remaining = min(state['deadline'], row['deadline'] - 60) - time.time()
        if remaining <= 0:
            reason = 'ai_timeout' if state['explicit_timeout'] else 'loop_deadline'
            raise LoopAIPause(reason, 'No decision time remains before the Loop deadline')
        return remaining

    def reserve(self, input_tokens, output_tokens, request_sha256=None):
        """Reserve every replayed full input before any HTTP request is sent."""
        call_id = uuid.uuid4().hex
        total = input_tokens + output_tokens
        usd = paid_cost(self.metadata, input_tokens, output_tokens) if self.paid else 0.0
        def reserve(row):
            """Serialize authorization, budget and usage checks with the owning record."""
            if row['status'] not in {'running', 'finishing'} or row['control_generation'] != self.record['control_generation']:
                raise LoopAIPause('evidence_recovery', 'Loop control changed before the model request')
            state = row['ai_decisions'][self.key]
            siblings = [item for item in row['ai_decisions'].values()
                        if item['stage'] == state['stage'] and item['round'] == state['round']]
            if sum(len(item['calls']) for item in siblings) >= 134:
                raise LoopAIPause('evidence_recovery', 'The decision-wide model-call allowance is exhausted')
            settings = row['settings']
            if self.paid:
                token_limit, usd_limit = settings.get('ai_decision_token_limit'), settings.get('ai_decision_usd_limit')
                if settings.get('ai_budget_confirmed') is not True or type(token_limit) is not int or token_limit <= 0 \
                        or type(usd_limit) not in (int, float) or not math.isfinite(usd_limit) or usd_limit <= 0:
                    raise LoopAIPause('ai_budget', 'Confirm positive token and USD decision limits before billing')
                reserved_tokens = sum(item['tokens'] for item in siblings)
                reserved_usd = sum(item['usd'] for item in siblings)
                if reserved_tokens + total > token_limit or reserved_usd + usd > usd_limit:
                    raise LoopAIPause('ai_budget', 'The next full-input reservation exceeds the decision budget',
                                      tokens_reserved=reserved_tokens, usd_reserved=reserved_usd)
            state['tokens'] += total
            state['usd'] += usd
            state['calls'].append({'id': call_id, 'input_reserved': input_tokens,
                                   'output_reserved': output_tokens, 'tokens': total, 'usd': usd,
                                   'sent_at': time.time(), 'usage_confirmed': False, 'request_sha256': request_sha256})
            row['ai_calls'] += 1
            row['ai_tokens_reserved'] += total
            row['ai_usd_reserved'] += usd
            row['pending_ai'] = {'id': call_id, 'stage': state['stage'], 'decision_id': self.key,
                                 'sha256': state['snapshot_sha'], 'tokens_reserved': total,
                                 'usd_reserved': usd, 'snapshot_id': row.get('ai_snapshot_id')}
            row['pending_ai']['request_sha256'] = request_sha256
        self.remaining()
        self.store.update(self.record['owner'], self.record['id'], reserve)
        return call_id

    def reserve_tool(self):
        """Do not replenish the logical decision's tool quota after retry or resume."""
        def charge(row):
            """Count under the same cross-process lock as billing and control changes."""
            if row['control_generation'] != self.record['control_generation'] or row['status'] not in {'running','finishing'}:
                raise LoopAIPause('evidence_recovery', 'Loop control changed before the evidence read')
            state = row['ai_decisions'][self.key]
            used = state.get('tool_calls', 0)
            if used >= 128:
                raise LoopAIPause('evidence_recovery', 'The decision-wide evidence-call allowance is exhausted')
            state['tool_calls'] = used + 1
        self.store.update(self.record['owner'], self.record['id'], charge)

    def settle(self, call_id, usage):
        """Replace a reservation exactly once; absent usage remains conservatively reserved."""
        usage = usage or {}
        input_used = usage.get('input_tokens', usage.get('prompt_tokens'))
        output_used = usage.get('output_tokens', usage.get('completion_tokens'))
        def settle(row):
            """Keep the audit record even if actual billing exceeds the estimate."""
            state = row['ai_decisions'][self.key]
            call = next(item for item in state['calls'] if item['id'] == call_id)
            if call.get('settled'):
                return
            call['settled'] = True
            if type(input_used) is int and type(output_used) is int and min(input_used, output_used) >= 0:
                actual_tokens = input_used + output_used
                actual_usd = paid_cost(self.metadata, input_used, output_used) if self.paid else 0.0
                state['tokens'] += actual_tokens - call['tokens']
                state['usd'] += actual_usd - call['usd']
                row['ai_tokens_reserved'] += actual_tokens - call['tokens']
                row['ai_usd_reserved'] += actual_usd - call['usd']
                call.update(tokens=actual_tokens, usd=actual_usd, usage_confirmed=True)
            if (row.get('pending_ai') or {}).get('id') == call_id:
                row['pending_ai'] = None
            row['last_usage'] = {'tokens': usage, 'status': 'reported' if call['usage_confirmed'] else 'reserved_unconfirmed',
                                 'usd': call['usd'] if self.paid else None}
        self.store.update(self.record['owner'], self.record['id'], settle)

    def timeout(self, call_id):
        """Keep uncertain billing while allowing a bounded retry of this call."""
        def uncertain(row):
            """Do not release possibly consumed token/USD reservations."""
            if (row.get('pending_ai') or {}).get('id') == call_id:
                row['pending_ai'] = None
        self.store.update(self.record['owner'], self.record['id'], uncertain)
