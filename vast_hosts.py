"""Local GPU machine history and explicit rental preferences."""
from __future__ import annotations

from file_lock import advisory_file_lock
from secure_files import ensure_private_directory
from vast_jobs import job_id, write_json
from vast_provider import VastError, positive_id

SERVICE = 'VastRunner'


def host_preferences(state: dict) -> dict:
    """Validate saved marks without coercing malformed machine identifiers."""
    values = state.get('host_preferences', {})
    if not isinstance(values, dict):
        raise VastError('GPU host preferences cannot be read', 500)
    for key, value in values.items():
        if (not isinstance(key, str) or not key.isascii() or not key.isdigit()
                or str(int(key)) != key or int(key) <= 0 or not isinstance(value, dict)
                or set(value) - {'preferred', 'working'}
                or any(type(flag) is not bool for flag in value.values())):
            raise VastError('GPU host preferences cannot be read', 500)
    return values


def set_host_preference(queue, machine_id: int, *, preferred: bool | None = None,
                        working: bool | None = None) -> dict:
    """Merge one machine's explicit marks under the queue's cross-process lock."""
    machine_id = positive_id(machine_id)
    if any(value is not None and type(value) is not bool for value in (preferred, working)):
        raise VastError('Invalid GPU host preference', 422)
    ensure_private_directory(queue.root)
    with advisory_file_lock(queue.root / '.queue-lock'):
        state = queue.read()
        marks = host_preferences(state)
        row = dict(marks.get(str(machine_id), {}))
        for key, value in [('preferred', preferred), ('working', working)]:
            if value is not None:
                row[key] = value
        if any(row.values()):
            marks[str(machine_id)] = row
        else:
            marks.pop(str(machine_id), None)
        state['host_preferences'] = marks
        write_json(queue.root / 'queue.json', state)
        return marks


def host_history(store, state: dict) -> dict[int, dict]:
    """Join job evidence to physical machines; never infer identity from GPU type."""
    marks = host_preferences(state)
    profiles = {}

    def profile(machine):
        """Create one public host record with independent evidence and marks."""
        machine = positive_id(machine)
        return profiles.setdefault(machine, dict(machine_id=machine, used=False, working=False,
            working_detected=False, working_marked=False, preferred=False, rentals=0))

    for machine, flags in marks.items():
        row = profile(int(machine))
        row.update(preferred=flags.get('preferred', False), working_marked=flags.get('working', False))
        row['working'] = row['working_marked']
    rows = store.list()
    by_id = {job_id(row['id']): row for row in rows}
    machines = {}
    used_rentals = set()
    for row in rows:
        rental_id = job_id(row.get('lease_id') or row['id'])
        if rental_id not in machines:
            rental = by_id.get(rental_id)
            if rental is None:
                machines[rental_id] = None
                continue
            path = store.directory(rental_id) / 'intent.json'
            intent = store.read(rental_id, 'intent.json') if path.exists() else {}
            machines[rental_id] = (intent.get('offer') or {}).get('machine_id') or rental.get('host_machine_id')
        machine = machines[rental_id]
        if machine is None:
            continue
        record = profile(machine)
        rental = by_id[rental_id]
        # A worker reaching idle/completed alone does not prove optimization worked.
        evaluated = row.get('kind') != 'worker' and type(row.get('exact_completed')) in (int, float) and row['exact_completed'] > 0
        used = (type(rental.get('instance_id')) is int and rental['instance_id'] > 0) or bool(row.get('worker_ready')) or evaluated
        if used and rental_id not in used_rentals:
            record['rentals'] += 1
            used_rentals.add(rental_id)
        record['used'] |= used
        record['working_detected'] |= evaluated
        record['working'] = record['working_marked'] or record['working_detected']
    return profiles


def preferred_machine_ids(state: dict) -> list[int]:
    """Return explicit priorities, never promote a host merely because it was rented."""
    return sorted(int(machine) for machine, flags in host_preferences(state).items() if flags.get('preferred'))


def offer_priority(offer: dict, state: dict) -> tuple:
    """Rank preferred compatible offers first, then retain price ordering."""
    preferred = host_preferences(state).get(str(offer.get('machine_id')), {}).get('preferred', False)
    return (not preferred, offer.get('price_hour_usd') or 0, offer['id'])


def search_host_offers(client, state: dict, **limits) -> list[dict]:
    """Search preferred machines separately so a cheap first page cannot hide them."""
    rows = client.offers(**limits)
    preferred = sorted(set(preferred_machine_ids(state)) - set(limits.get('excluded_machine_ids', [])))
    if preferred and limits.get('offer_id') is None:
        preferred_rows = client.offers(**limits, included_machine_ids=preferred)
        rows = [*rows, *(row for row in preferred_rows if row.get('machine_id') in preferred)]
    unique = {row['id']: row for row in rows}
    return sorted(unique.values(), key=lambda row: offer_priority(row, state))
