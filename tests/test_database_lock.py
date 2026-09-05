"""Cross-process database maintenance leases isolated below Pytest's temporary root."""

import multiprocessing
import stat

import pytest

from database_lock import DatabaseBusyError, acquire_database_lock
from master_update_lock import acquire_master_update_lock


def _database_lock_owner(root, exclusive, ready, release, fail):
    """Own a lease in a spawned process and verify release after normal or error exit."""
    lease = acquire_database_lock(root, exclusive=exclusive)
    try:
        try:
            with lease:
                ready.set()
                assert release.wait(10), "Parent did not release database lock worker"
                if fail:
                    raise ValueError("isolated worker failure")
        except ValueError:
            if not fail:
                raise
    finally:
        lease.release()
    with acquire_database_lock(root, exclusive=True):
        pass


@pytest.mark.parametrize("owner_exclusive", [False, True])
@pytest.mark.parametrize("contender_exclusive", [False, True])
@pytest.mark.parametrize("fail", [False, True])
def test_database_lock_cross_process_compatibility_and_cleanup(
    tmp_path, owner_exclusive, contender_exclusive, fail,
):
    """Only SH/SH coexist across processes; every exit releases DB ownership, not its file."""
    context = multiprocessing.get_context("spawn")
    ready, release = context.Event(), context.Event()
    process = context.Process(
        target=_database_lock_owner,
        args=(tmp_path, owner_exclusive, ready, release, fail),
    )
    process.start()
    try:
        assert ready.wait(10), "Database lock worker did not acquire its lease"
        path = tmp_path / "data" / "locks" / "database-maintenance.lock"
        inode = path.stat().st_ino
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
        with acquire_master_update_lock(tmp_path):
            if owner_exclusive or contender_exclusive:
                with pytest.raises(DatabaseBusyError):
                    acquire_database_lock(tmp_path, exclusive=contender_exclusive)
            else:
                with acquire_database_lock(tmp_path) as lease:
                    assert lease.path == path
                    with pytest.raises(DatabaseBusyError):
                        acquire_database_lock(tmp_path, exclusive=True)
                lease.release()
    finally:
        release.set()
        process.join(10)
        if process.is_alive():
            process.terminate()
            process.join(10)
        exitcode = process.exitcode
        process.close()
    assert exitcode == 0
    assert path.stat().st_ino == inode
    with acquire_database_lock(tmp_path, exclusive=True) as lease:
        assert lease.path == path
    lease.release()
    with acquire_database_lock(tmp_path):
        pass
