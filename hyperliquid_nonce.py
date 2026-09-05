"""Durable local nonce coordination for Hyperliquid signing wallets."""

from __future__ import annotations

import re
import time
from pathlib import Path

from file_lock import advisory_file_lock
from secure_files import atomic_write_private_text, ensure_private_directory, read_regular_file_nofollow


SERVICE = "HyperliquidNonce"
NONCE_ROOT = Path(__file__).resolve().parent / "data" / "state" / "hyperliquid_nonces"


def allocate_hyperliquid_nonce(signer: str, *, minimum: int = 0) -> int:
    """Allocate once per new action, shared by local processes using this signer."""

    if not isinstance(signer, str) or re.fullmatch(r"0x[0-9a-fA-F]{40}", signer) is None:
        raise ValueError("Invalid Hyperliquid signer address")
    if type(minimum) is not int or minimum < 0:
        raise ValueError("Invalid Hyperliquid nonce lower bound")
    root = ensure_private_directory(NONCE_ROOT)
    path = root / (signer.lower() + ".nonce")
    if path.is_symlink() or path.with_name(path.name + ".lock").is_symlink():
        raise ValueError("Hyperliquid nonce files must not be symlinks")
    with advisory_file_lock(path):
        previous = 0
        if path.exists():
            try:
                text = read_regular_file_nofollow(path, root).decode("ascii").strip()
                if not text.isdigit():
                    raise ValueError("invalid counter")
                previous = int(text)
            except (ValueError, UnicodeError) as exc:
                raise ValueError("Invalid persisted Hyperliquid nonce counter") from exc
        now = int(time.time() * 1000)
        nonce = max(now, previous + 1, minimum)
        if nonce >= now + 86_400_000:
            raise ValueError("Hyperliquid nonce counter is outside the provider time window")
        atomic_write_private_text(path, str(nonce) + "\n")
        return nonce
