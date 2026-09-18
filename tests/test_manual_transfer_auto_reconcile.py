"""Automatic delayed manual-transfer reconciliation never submits funds."""
import asyncio
from unittest.mock import Mock

from api import profit_sweep


def test_pending_manual_recovery_only_reconciles_submitted_operations(monkeypatch):
    """Prepared operations remain untouched; unknown ones use the owned account lock."""
    operations = [dict(user_name='alice', operation_id=str(i), state=state)
                  for i, state in enumerate(['prepared', 'unknown', 'submitting'])]
    store = Mock()
    store.list_unresolved_transfer_operations.return_value = operations
    monkeypatch.setattr(profit_sweep, '_store', lambda: store)
    calls = []

    async def run_thread(fn):
        """Execute only the fake store read."""
        return fn()

    async def run_account(user, fn, operation):
        """Capture the operation without reading credentials or writing funds."""
        calls.append((user, fn, operation['state']))

    monkeypatch.setattr(profit_sweep, '_run_owned_thread', run_thread)
    monkeypatch.setattr(profit_sweep, '_run_account_operation', run_account)
    asyncio.run(profit_sweep._reconcile_pending_manual_transfers())
    assert [(user, state) for user, _, state in calls] == [('alice', 'unknown'), ('alice', 'submitting')]
    assert all(fn is profit_sweep._reconcile_manual_operation_sync for _, fn, _ in calls)
