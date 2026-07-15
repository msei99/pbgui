"""Tests for private AWS profile persistence."""

from __future__ import annotations

import os
from pathlib import Path

import market_data
import pytest


def test_aws_profile_writers_and_readers_repair_private_modes(tmp_path: Path, monkeypatch) -> None:
    """AWS profile directories and files remain owner-only across save and load."""
    aws_dir = tmp_path / ".aws"
    credentials = aws_dir / "credentials"
    config = aws_dir / "config"
    monkeypatch.setattr(market_data, "get_aws_credentials_path", lambda: credentials)
    monkeypatch.setattr(market_data, "get_aws_config_path", lambda: config)

    market_data.save_aws_profile_credentials(
        profile="pbgui",
        aws_access_key_id="access",
        aws_secret_access_key="secret",
    )
    market_data.save_aws_profile_region(profile="pbgui", region="eu-west-1")
    assert aws_dir.stat().st_mode & 0o777 == 0o700
    assert credentials.stat().st_mode & 0o777 == 0o600
    assert config.stat().st_mode & 0o777 == 0o600

    os.chmod(credentials, 0o644)
    os.chmod(config, 0o644)
    assert market_data.load_aws_profile_credentials("pbgui") == {
        "aws_access_key_id": "access",
        "aws_secret_access_key": "secret",
    }
    assert market_data.load_aws_profile_region("pbgui") == "eu-west-1"
    assert credentials.stat().st_mode & 0o777 == 0o600
    assert config.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize("profile", ["", "bad profile", "bad\n[other]", "bad]", "../default"])
def test_aws_profile_names_reject_ini_injection(tmp_path: Path, monkeypatch, profile: str) -> None:
    """Profile names cannot inject sections or escape into unrelated entries."""
    credentials = tmp_path / ".aws" / "credentials"
    monkeypatch.setattr(market_data, "get_aws_credentials_path", lambda: credentials)

    with pytest.raises(ValueError, match="invalid characters"):
        market_data.save_aws_profile_credentials(
            profile=profile,
            aws_access_key_id="access",
            aws_secret_access_key="secret",
        )
    assert not credentials.exists()
