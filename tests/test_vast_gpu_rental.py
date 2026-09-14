"""Offline checks for create-once reconciliation and independent deadline setup."""

import importlib.util
import json
from pathlib import Path
from unittest.mock import Mock

import pytest

SPEC = importlib.util.spec_from_file_location("vast_rental", Path(__file__).resolve().parents[1] / "setup/vast_gpu_benchmark/rental.py")
rental = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(rental)


@pytest.fixture
def intent():
    """Use a synthetic contract with no provider calls."""
    return {"offer_id": 123, "label": "pbgui-gpu-test-" + "a" * 32,
        "created_at": 1000, "deadline": 6400, "destroy_authorized": True,
        "disk_gb": 40, "budget_usd": 1,
        "image": "ghcr.io/msei99/pbgui-pb8-gpu@sha256:" + "b" * 64}


def test_registry_credential_only_in_login_body(intent):
    """The worker gets its own provider key, not either account credential."""
    payload = rental.rental_payload(intent, "synthetic_pull_credential")
    assert payload.pop("image_login") == "-u msei99 -p synthetic_pull_credential ghcr.io"
    assert "synthetic_pull_credential" not in json.dumps(payload)
    assert payload["env"]["PBGUI_BENCHMARK_DEADLINE"] == "6400"
    compile(rental.WORKER_SOURCE, "worker_guard.py", "exec")


@pytest.mark.parametrize("authorized,budget,valid", [(True, 3, True), (False, 3, False), (True, 10, False)])
def test_extended_rental_uses_matching_authorization(intent, authorized, budget, valid):
    """Only the explicit twelve-hour grant enables the extended experiment cap."""
    intent.update(deadline=44200, extended_twelve_hour_lease_authorized=authorized, budget_usd=budget)
    if valid:
        assert rental.validate_rental_intent(intent) is intent
    else:
        with pytest.raises(ValueError):
            rental.validate_rental_intent(intent)


@pytest.mark.parametrize("field,value", [("offer_id", -1), ("deadline", 6401), ("budget_usd", 10), ("disk_gb", 80), ("image", "untrusted/image")])
def test_rental_limits(intent, field, value):
    """Unexpected persisted authorization fails before creating resources."""
    intent[field] = value
    with pytest.raises(ValueError):
        rental.validate_rental_intent(intent)


@pytest.mark.parametrize("restart", [False, True])
def test_ambiguous_create_recovers_without_second_rental(tmp_path, monkeypatch, intent, restart):
    """A lost creation response or process restart cannot create two instances."""
    (tmp_path / "credentials").mkdir()
    (tmp_path / "credentials/vast_api_key").write_text("synthetic")
    (tmp_path / "credentials/ghcr_pull_token").write_text("synthetic")
    path = tmp_path / "job.json"
    path.write_text(json.dumps(intent))
    if restart:
        path.with_suffix(".attempt.json").write_text("{}")
    monkeypatch.setattr(rental, "WORK_ROOT", tmp_path)
    monkeypatch.setattr(rental.sys, "argv", ["rental.py", "--intent", str(path)])
    monkeypatch.setattr(rental.time, "time", lambda: 1001)
    monkeypatch.setattr(rental.time, "monotonic", lambda: 1)
    monkeypatch.setattr(rental.time, "sleep", lambda seconds: path.with_suffix(".finish").touch())
    create = Mock(side_effect=RuntimeError("Vast transport failure"))
    monkeypatch.setattr(rental, "rental_request", create)
    client = Mock()
    client.instances.side_effect = [
        [{"id": 456, "label": intent["label"]}, {"id": 789, "label": "other"}],
        [{"id": 456, "label": intent["label"]}], [], [],
    ]
    monkeypatch.setattr(rental, "VastCleanupClient", lambda key: client)
    assert rental.vast_rental_main() == 0
    assert create.call_count == (0 if restart else 1)
    client.destroy.assert_called_once_with(456)
    assert path.with_suffix(".attempt.json").exists()
    assert json.loads(path.with_suffix(".status.json").read_text())["state"] == "deletion_verified"
