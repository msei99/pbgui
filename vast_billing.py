"""Cached provider-reported instance charges, separate from optimizer progress."""
import json
import time

from file_lock import advisory_file_lock
from logging_helpers import human_log as _log
from vast_credentials import VastCredentialStore
from vast_provider import VastClient, VastError, number, positive_id

SERVICE = 'Vast'


def instance_charges(store, lease_id):
    """Refresh a single rental's billing snapshot at most every five minutes."""
    with advisory_file_lock(store.directory(lease_id) / '.billing-lock'):
        row = store.read(lease_id)
        if not row.get('instance_id'):
            return None
        previous = row.get('billing') or {}
        now = time.time()
        if 0 <= now - previous.get('checked_at', 0) < 300:
            return previous
        intent = store.read(lease_id, 'intent.json')
        instance = positive_id(row['instance_id'])
        filters = {'day': {'gte': int(intent['accepted_at']) // 86400 * 86400,
                           'lte': int(now)}, 'type': {'in': ['instance']}}
        try:
            client = VastClient(VastCredentialStore(store.root).secrets()['api_key'])
            result = client.request('GET', '/charges/', {'select_filters': json.dumps(filters), 'limit': 500})
            if result.get('next_token') or not isinstance(result.get('results'), list):
                raise VastError('Billing response is incomplete')
            matches = [entry for entry in result['results'] if isinstance(entry, dict)
                       and entry.get('type') == 'instance' and entry.get('source') == f'instance-{instance}']
            total = 0.0
            breakdown = {}
            for entry in matches:
                amount = number(entry.get('amount'), minimum=None)
                if amount is None:
                    raise VastError('Billing amount is unavailable')
                total += amount
                for item in entry.get('items', []):
                    if isinstance(item, dict) and item.get('type') in {'gpu', 'disk', 'bwd', 'bwu'}:
                        value = number(item.get('amount'), minimum=None)
                        if value is not None:
                            breakdown[item['type']] = breakdown.get(item['type'], 0.0) + value
            billing = {'instance_id': instance, 'amount_usd': total if matches else None,
                       'breakdown': breakdown, 'checked_at': now, 'reported_at': now,
                       'error': None}
        except VastError as exc:
            _log(SERVICE, str(exc), level='WARNING')
            billing = dict(previous, instance_id=instance, checked_at=now, error=str(exc))
        store.update(lease_id, billing=billing)
        return billing
