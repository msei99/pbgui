"""Tests for resource-aware PBCoinData service lifecycle decisions."""

from pathlib import Path
from types import SimpleNamespace

import pbcoindata_lifecycle


def _write_instance(root: Path, name: str, *, dynamic_ignore: bool) -> Path:
    """Create a minimal V7 instance runtime fixture."""

    instance = root / "data" / "run_v7" / name
    instance.mkdir(parents=True)
    (instance / "config.json").write_text(
        '{"pbgui":{"dynamic_ignore":' + ("true" if dynamic_ignore else "false") + '}}',
        encoding="utf-8",
    )
    (instance / "config_run.json").write_text("{}", encoding="utf-8")
    return instance


def test_slave_without_running_dynamic_ignore_bot_does_not_expect_service(tmp_path: Path) -> None:
    """A slave releases PBCoinData when no running bot consumes dynamic-ignore data."""

    _write_instance(tmp_path, "plain", dynamic_ignore=False)

    assert pbcoindata_lifecycle.service_expected(
        tmp_path,
        role="slave",
        credential_active=True,
        process_iter=lambda: [],
    ) is False


def test_slave_with_running_dynamic_ignore_bot_expects_service(tmp_path: Path) -> None:
    """A slave keeps PBCoinData resident while a matching dynamic-ignore bot runs."""

    instance = _write_instance(tmp_path, "dynamic", dynamic_ignore=True)
    proc = SimpleNamespace(cmdline=lambda: [
        "python",
        "/opt/passivbot/src/main.py",
        str(instance / "config_run.json"),
    ])

    assert pbcoindata_lifecycle.service_expected(
        tmp_path,
        role="slave",
        credential_active=True,
        process_iter=lambda: [proc],
    ) is True


def test_master_preserves_credential_driven_service_behavior(tmp_path: Path) -> None:
    """Master nodes remain credential-driven because UI workflows may need Coin Data."""

    assert pbcoindata_lifecycle.service_expected(
        tmp_path, role="master", credential_active=True, process_iter=lambda: []
    ) is True
    assert pbcoindata_lifecycle.service_expected(
        tmp_path, role="master", credential_active=False, process_iter=lambda: []
    ) is False
    assert pbcoindata_lifecycle.should_unload(
        tmp_path, role="master", process_iter=lambda: []
    ) is False
