"""Offline signer-scoped nonce coordination and persistence regressions."""

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import multiprocessing
from pathlib import Path
import stat

import pytest

import hyperliquid_nonce as nonces


SIGNER = "0x" + "a" * 40
NOW = 1_800_000_000_000


def _process_nonces(root: str) -> list[int]:
    """Allocate in an independent worker against only a supplied temporary root."""

    nonces.NONCE_ROOT = Path(root)
    nonces.time.time = lambda: NOW / 1000
    return [nonces.allocate_hyperliquid_nonce(SIGNER) for _ in range(20)]


def test_nonce_threads_processes_and_restart(tmp_path, monkeypatch):
    """Coordinate a shared signer across threads, processes, and a restarted caller."""

    root = tmp_path / "nonces"
    monkeypatch.setattr(nonces, "NONCE_ROOT", root)
    monkeypatch.setattr(nonces.time, "time", lambda: NOW / 1000)
    with ThreadPoolExecutor(max_workers=8) as executor:
        thread_values = list(executor.map(lambda _: nonces.allocate_hyperliquid_nonce(SIGNER), range(50)))
    with ProcessPoolExecutor(max_workers=3, mp_context=multiprocessing.get_context("spawn")) as executor:
        process_values = sum(executor.map(_process_nonces, [str(root)] * 3), [])
    values = thread_values + process_values
    assert sorted(values) == list(range(NOW, NOW + 110))
    assert nonces.allocate_hyperliquid_nonce(SIGNER.upper().replace("0X", "0x")) == NOW + 110
    assert stat.S_IMODE(root.stat().st_mode) == 0o700
    assert stat.S_IMODE((root / (SIGNER + ".nonce")).stat().st_mode) == 0o600


def test_nonce_clock_rollback_floor_and_distinct_signers(tmp_path, monkeypatch):
    """Retain durable maxima, respect predecessor floors, and separate signers."""

    monkeypatch.setattr(nonces, "NONCE_ROOT", tmp_path / "nonces")
    monkeypatch.setattr(nonces.time, "time", lambda: NOW / 1000)
    assert nonces.allocate_hyperliquid_nonce(SIGNER, minimum=NOW + 10) == NOW + 10
    monkeypatch.setattr(nonces.time, "time", lambda: (NOW - 1000) / 1000)
    assert nonces.allocate_hyperliquid_nonce(SIGNER) == NOW + 11
    assert nonces.allocate_hyperliquid_nonce("0x" + "b" * 40) == NOW - 1000
    with pytest.raises(ValueError, match="time window"):
        nonces.allocate_hyperliquid_nonce(SIGNER, minimum=NOW + 86_400_000)


@pytest.mark.parametrize("value", ["garbage", "-1", "", "1.2"])
def test_nonce_corrupt_counter_fails_closed(tmp_path, monkeypatch, value):
    """Never reset a corrupt persisted counter and accidentally reuse a nonce."""

    monkeypatch.setattr(nonces, "NONCE_ROOT", tmp_path)
    path = tmp_path / (SIGNER + ".nonce")
    path.write_text(value)
    with pytest.raises(ValueError, match="persisted"):
        nonces.allocate_hyperliquid_nonce(SIGNER)
    assert path.read_text() == value


@pytest.mark.parametrize("signer", ["../escape", "0x123", "0x" + "z" * 40])
def test_nonce_invalid_identity_has_no_io(tmp_path, monkeypatch, signer):
    """Reject malformed signer identities before creating state."""

    root = tmp_path / "nonces"
    monkeypatch.setattr(nonces, "NONCE_ROOT", root)
    with pytest.raises(ValueError, match="signer"):
        nonces.allocate_hyperliquid_nonce(signer)
    assert not root.exists()
