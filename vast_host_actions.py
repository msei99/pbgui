"""Local, revision-bound host decisions and bounded atomic execution receipts."""
from contextlib import ExitStack, contextmanager
import copy
import math
import time

from file_lock import advisory_file_lock
from secure_files import ensure_private_directory
from vast_jobs import job_id, write_json
from vast_provider import VastError, positive_id
from vast_queue import blocked_machine_ids

SERVICE = 'VastRunner'
RECEIPT_TTL = 8 * 86400  # Longer than the seven-day proposal approval window.
MAX_RECEIPTS = 4096


def _receipt_timestamp(value):
    """Normalize a positive finite epoch without overflowing on damaged JSON integers."""
    if type(value) not in (int, float):
        return None
    try:
        timestamp = float(value)
    except OverflowError:
        return None
    return timestamp if math.isfinite(timestamp) and timestamp > 0 else None


def host_state(state):
    """Reject damaged policy data rather than interpreting it as permission."""
    revision = state.get('host_block_revision', 0)
    if type(revision) is not int or revision < 0:
        raise VastError('Host block revision cannot be read', 500)
    return {'blocked_machine_ids': blocked_machine_ids(state), 'host_block_revision': revision}


@contextmanager
def locked_target(queue, identifier):
    """Hold the queue, intent namespace and both actual state-writer locks.

    Queue.start takes queue then JobStore.root/.lock; JobStore.update takes
    .state-lock. Profile intent edits take the queue lock. The second read
    detects a lease reassignment before both state locks were acquired.
    """
    identifier = job_id(identifier)
    ensure_private_directory(queue.root)
    with advisory_file_lock(queue.root / '.queue-lock'), advisory_file_lock(queue.store.root / '.lock'):
        row = queue.store.read(identifier)
        lease = job_id(row.get('lease_id') or identifier)
        with ExitStack() as stack:
            for item in sorted({identifier, lease}):
                stack.enter_context(advisory_file_lock(queue.store.directory(item) / '.state-lock'))
            row = queue.store.read(identifier)
            if (row.get('lease_id') or identifier) != lease:
                raise VastError('Host target changed; read the job again', 409)
            rental = queue.store.read(lease)
            intent = queue.store.read(lease, 'intent.json')
            if row.get('id') != identifier or rental.get('id') != lease or intent.get('id') != lease:
                raise VastError('Host target identity conflicts; read the job again', 409)
            offer = intent.get('offer')
            if offer is not None and not isinstance(offer, dict):
                raise VastError('Host target metadata cannot be read', 500)
            offer_machine = (offer or {}).get('machine_id')
            saved_machine = rental.get('host_machine_id')
            machines = [positive_id(value) for value in (offer_machine, saved_machine, row.get('host_machine_id')) if value is not None]
            if not machines:
                raise VastError('Machine ID unknown locally; use the human host controls for this legacy rental', 409)
            if len(set(machines)) != 1:
                raise VastError('Host target metadata conflicts; read the job again', 409)
            yield {'job_id': identifier, 'lease_id': lease, 'machine_id': machines[0]}


def read_hosts(queue, identifier=None):
    """Read persisted exclusions without credentials, provider calls or writes."""
    if identifier is not None:
        with locked_target(queue, identifier) as target:
            result = host_state(queue.read())
            result['target'] = dict(target, blocked=target['machine_id'] in result['blocked_machine_ids'])
            return result
    return host_state(queue.read())


def prepare(queue, identifier, blocked):
    """Bind the explicit intent to the fresh local identity and global revision."""
    if type(blocked) is not bool:
        raise VastError('Specify an explicit blocked boolean', 422)
    state = read_hosts(queue, identifier)
    target = state['target']
    return {**target, 'before_blocked': target['blocked'], 'blocked': blocked,
            'expected_revision': state['host_block_revision']}


def receipt(queue, proposal):
    """Read an execution proof without granting permission for another mutation."""
    receipts = queue.read().get('host_block_receipts', {})
    if not isinstance(receipts, dict):
        raise VastError('Host execution receipts cannot be read', 500)
    found = receipts.get(proposal.id)
    if found is None:
        return None
    if not isinstance(found, dict) or found.get('payload_digest') != proposal.payload_digest:
        raise VastError('Host execution receipt conflicts with proposal', 409)
    result = found.get('result')
    if (_receipt_timestamp(found.get('at')) is None
            or not isinstance(result, dict)
            or result.get('status') != 'executed' or result.get('proposal_status') != 'executed'
            or result.get('execution_status') != 'succeeded' or result.get('action') != 'vast_host_block'
            or result.get('proposal_id') != proposal.id
            or type(result.get('machine_id')) is not int or result['machine_id'] <= 0
            or type(result.get('blocked')) is not bool
            or type(result.get('host_block_revision')) is not int or result['host_block_revision'] <= 0):
        raise VastError('Host execution receipt cannot be read', 500)
    try:
        job_id(result.get('job_id'))
        job_id(result.get('lease_id'))
    except VastError:
        raise VastError('Host execution receipt identity cannot be read', 500) from None
    payload = proposal.config
    if (not isinstance(payload, dict)
            or any(result[key] != payload.get(key) for key in ('job_id', 'lease_id', 'machine_id', 'blocked'))
            or type(payload.get('expected_revision')) is not int
            or result['host_block_revision'] != payload['expected_revision'] + 1):
        raise VastError('Host execution receipt conflicts with proposal', 409)
    # Never project arbitrary persisted fields into the browser/model result.
    return copy.deepcopy({key: result[key] for key in (
        'status', 'proposal_status', 'execution_status', 'action', 'proposal_id',
        'job_id', 'lease_id', 'machine_id', 'blocked', 'host_block_revision')})


def execute(queue, proposal):
    """Commit state, monotone revision and execution proof in one replacement."""
    ensure_private_directory(queue.root)
    with advisory_file_lock(queue.root / '.queue-lock'):
        previous = receipt(queue, proposal)
        if previous is not None:
            return previous
        payload = proposal.config
        # No Boolean shortcut before identity/revision checks, including ABA.
        with locked_target(queue, payload['job_id']) as target:
            state = queue.read()
            current = host_state(state)
            if any(payload[key] != target[key] for key in ('job_id', 'lease_id', 'machine_id')):
                raise VastError('Host target changed since review; request a new proposal', 409)
            if (current['host_block_revision'] != payload['expected_revision']
                    or (target['machine_id'] in current['blocked_machine_ids']) != payload['before_blocked']):
                raise VastError('Host block revision changed since review; request a new proposal', 409)
            if time.time() - proposal.created_at > 7 * 86400:
                raise VastError('Host proposal expired; request a new proposal', 409)
            machines = set(current['blocked_machine_ids'])
            (machines.add if payload['blocked'] else machines.discard)(target['machine_id'])
            revision = current['host_block_revision'] + 1
            result = {'status': 'executed', 'proposal_status': 'executed', 'action': 'vast_host_block',
                      'proposal_id': proposal.id, **target, 'execution_status': 'succeeded',
                      'blocked': payload['blocked'], 'host_block_revision': revision}
            now = time.time()
            receipts = state.get('host_block_receipts', {})
            if not isinstance(receipts, dict):
                raise VastError('Host execution receipts cannot be read', 500)
            retained = []
            for key, value in receipts.items():
                timestamp = _receipt_timestamp(value.get('at')) if isinstance(value, dict) else None
                if timestamp is not None and now - timestamp < RECEIPT_TTL:
                    retained.append((key, value, timestamp))
            retained.sort(key=lambda item: item[2], reverse=True)
            receipts = {key: value for key, value, _ in retained[:MAX_RECEIPTS - 1]}
            receipts[proposal.id] = {'at': now, 'payload_digest': proposal.payload_digest, 'result': result}
            state.update(blocked_machine_ids=sorted(machines), host_block_revision=revision,
                         host_block_receipts=receipts)
            write_json(queue.root / 'queue.json', state)
            return result
