"""Explicit offline provider boundary for legacy policy tests using fake replies."""
import asyncio

from pb8_loop_ai import LoopAI as ProductionLoopAI
from pb8_loop_ai_v2 import _ACTIVE
from pb8_loop_ai_policy import DecisionLedger
from pb8_loop_evidence import EvidenceStore, EvidenceSession


class LoopAI(ProductionLoopAI):
    """Retain production orchestration while fake transports account per request."""

    async def _decide_once(self, record, stage, evidence):
        """Wrap only overrides: native adapters already own their actual-call ledger."""
        implementation = self._request
        if getattr(implementation, '__func__', None) is LoopAI._request:
            return await super()._decide_once(record, stage, evidence)
        original_override = self.__dict__.get('_request')
        async def fake_boundary(current, metadata, content, output):
            """Simulate the adapter's reserve/acknowledge boundary without network IO."""
            active = _ACTIVE.get()
            call = active['ledger'].reserve(1000, output)
            try:
                text, usage = await implementation(current, metadata, content, output)
            except asyncio.CancelledError:
                raise  # interrupted provider call intentionally retains pending_ai
            except BaseException:
                active['ledger'].timeout(call)
                raise
            active['ledger'].settle(call, usage.get('tokens'))
            return text, usage
        self._request = fake_boundary
        try:
            return await super()._decide_once(record, stage, evidence)
        finally:
            if original_override is None:
                del self._request
            else:
                self._request = original_override

    async def _request(self, record, metadata, content, output_tokens):
        """Direct adapter tests explicitly construct an isolated owning session."""
        if _ACTIVE.get() is not None:
            return await super()._request(record, metadata, content, output_tokens)
        profile = {'context': 4_000_000, 'page_tokens': 1500, 'call_timeout': 60, 'streaming': False}
        row = self.store.read(record['owner'], record['id'])
        row['settings'].update(record['settings'])
        self.store.update(row['owner'], row['id'], lambda current: current.update(settings=row['settings']))
        evidence_store = EvidenceStore(self.store)
        frozen = evidence_store.create(row, content, {'stage': 'adapter_test', **content}, [])
        ledger = DecisionLedger(self.store, row, 'adapter_test', frozen['sha256'], metadata)
        from pb8_loop_ai import instructions_for_run, EVALUATION_PROTOCOL
        active = {'profile': profile, 'session': EvidenceSession(evidence_store, frozen, 1500),
                  'ledger': ledger, 'instructions': instructions_for_run(record)['text'] + EVALUATION_PROTOCOL,
                  'validate': None}
        token = _ACTIVE.set(active)
        try:
            return await super()._request(row, metadata, content, output_tokens)
        finally:
            _ACTIVE.reset(token)
