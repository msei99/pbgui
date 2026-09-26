"""Offline coverage for VPS Manager Hyperliquid account-limit history."""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest
from fastapi import HTTPException

from api import vps_manager


WALLET = "0x33211c68b5e71794507163d6fdd3adabc4eb10c3"


def _history_in_tmp(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Redirect all history access to the test's temporary directory."""
    root = tmp_path / "hl_history"
    monkeypatch.setattr(vps_manager, "_HL_RATE_LIMIT_HISTORY_DIR", root)
    monkeypatch.setattr(vps_manager, "_HL_RATE_LIMIT_HISTORY_FILE", root / "samples.json")


def test_history_persists_deduplicates_and_prunes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Counter samples survive a read, replace duplicate times, and expire after a day."""
    _history_in_tmp(monkeypatch, tmp_path)
    now = int(time.time())
    vps_manager._append_hl_rate_limit_sample(WALLET, now - 86401, 1, 10)
    vps_manager._append_hl_rate_limit_sample(WALLET, now, 2, 10)
    vps_manager._append_hl_rate_limit_sample(WALLET, now, 3, 11)

    assert vps_manager._get_hl_rate_limit_history(WALLET) == [
        {"sampled_at": now, "used": 3, "cap": 11}
    ]
    stored = json.loads(vps_manager._HL_RATE_LIMIT_HISTORY_FILE.read_text())
    assert list(stored) == [WALLET]
    assert stored[WALLET] == [[now, 3, 11]]


def test_history_endpoint_exposes_only_named_account(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The authenticated handler resolves an account without returning its wallet."""
    _history_in_tmp(monkeypatch, tmp_path)
    now = int(time.time())
    vps_manager._append_hl_rate_limit_sample(WALLET, now, 7, 100)
    monkeypatch.setattr(vps_manager, "_configured_hl_wallets", lambda: {WALLET: ["hl_mani11"]})

    response = vps_manager.get_hl_user_rate_limit_history("hl_mani11", session=object())
    payload = json.loads(response.body)
    assert payload["account"] == "hl_mani11"
    assert payload["samples"] == [{"sampled_at": now, "used": 7, "cap": 100}]
    assert WALLET not in response.body.decode()

    with pytest.raises(HTTPException) as invalid:
        vps_manager.get_hl_user_rate_limit_history("..", session=object())
    assert invalid.value.status_code == 400
    with pytest.raises(HTTPException) as missing:
        vps_manager.get_hl_user_rate_limit_history("unknown", session=object())
    assert missing.value.status_code == 404
