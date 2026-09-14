"""Offline safety tests for the independent Vast cleanup guard."""

import importlib.util
import json
from pathlib import Path
from unittest.mock import Mock

import pytest

SPEC = importlib.util.spec_from_file_location("vast_cleanup", Path(__file__).resolve().parents[1] / "setup/vast_gpu_benchmark/cleanup.py")
cleanup = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cleanup)


@pytest.fixture
def job():
    """Provide a synthetic rental never connected to a real provider."""
    return {"instance_id": 123, "label": "pbgui-gpu-test-" + "a" * 32, "created_at": 1000, "deadline": 6400, "destroy_authorized": True}


@pytest.mark.parametrize("authorization,seconds,valid", [(True, 14400, True), (True, 14401, False), (False, 14400, False), ("true", 14400, False)])
def test_extended_lease_requires_explicit_bounded_authorization(job, authorization, seconds, valid):
    """Longer debugging leases require a boolean grant and keep a hard limit."""
    job.update(deadline=job["created_at"] + seconds, extended_four_hour_lease_authorized=authorization)
    if valid:
        assert cleanup.validate_cleanup_job(job) is job
    else:
        with pytest.raises(ValueError):
            cleanup.validate_cleanup_job(job)


@pytest.mark.parametrize("authorization,seconds,valid", [(True, 43200, True), (True, 43201, False), (False, 43200, False), ("true", 43200, False)])
def test_twelve_hour_lease_keeps_an_explicit_hard_limit(job, authorization, seconds, valid):
    """The user's longer experiment grant cannot become an unbounded rental."""
    job.update(deadline=job["created_at"] + seconds, extended_twelve_hour_lease_authorized=authorization)
    if valid:
        assert cleanup.validate_cleanup_job(job) is job
    else:
        with pytest.raises(ValueError):
            cleanup.validate_cleanup_job(job)


def test_no_action_before_deadline(job):
    """The guard does not inspect or destroy an active authorized rental early."""
    client = Mock()
    assert cleanup.cleanup_step(job, client, 6399) == "waiting"
    client.instances.assert_not_called()
    client.destroy.assert_not_called()


def test_destroy_exact_owned_instance_only(job):
    """A matching label and ID permit only that one destruction request."""
    client = Mock()
    client.instances.return_value = [{"id": 123, "label": job["label"]}, {"id": 124, "label": "other"}]
    assert cleanup.cleanup_step(job, client, 6400) == "destroy_requested"
    client.destroy.assert_called_once_with(123)


def test_wrong_label_refuses_destruction(job):
    """Persisted IDs cannot authorize deleting an unrelated instance."""
    client = Mock()
    client.instances.return_value = [{"id": 123, "label": "another-job"}]
    with pytest.raises(ValueError, match="ownership"):
        cleanup.cleanup_step(job, client, 6400)
    client.destroy.assert_not_called()


def test_absent_instance_needs_no_destroy(job):
    """An absent instance is an observation, not a successful DELETE request."""
    client = Mock()
    client.instances.return_value = []
    assert cleanup.cleanup_step(job, client, 6400) == "absent"
    client.destroy.assert_not_called()


def test_transport_failure_not_mistaken_for_absence(job):
    """Network failures remain failures so the controller can retry."""
    client = Mock()
    client.instances.side_effect = RuntimeError("Vast transport failure")
    with pytest.raises(RuntimeError):
        cleanup.cleanup_step(job, client, 6400)
    client.destroy.assert_not_called()


@pytest.mark.parametrize("field,value", [("instance_id", "123"), ("instance_id", -1), ("instance_id", True), ("label", "../../other"), ("deadline", 6401), ("deadline", float("nan")), ("destroy_authorized", False)])
def test_invalid_job_rejected_before_network(job, field, value):
    """Untrusted persisted identifiers and unauthorized leases fail closed."""
    job[field] = value
    client = Mock()
    with pytest.raises(ValueError):
        cleanup.cleanup_step(job, client, 9000)
    client.instances.assert_not_called()


@pytest.mark.parametrize("response", [{}, {"instances": None}, {"instances": {}}, {"instances": [{"id": "123"}]}])
def test_unexpected_list_schema_is_not_empty(response):
    """Malformed provider responses must never confirm deletion."""
    client = cleanup.VastCleanupClient("synthetic-test-key")
    client._request = Mock(return_value=response)
    with pytest.raises(cleanup.PermanentCleanupError, match="schema"):
        client.instances()


def test_failed_delete_is_not_acknowledged():
    """A JSON failure payload cannot become successful cleanup."""
    client = cleanup.VastCleanupClient("synthetic-test-key")
    client._request = Mock(return_value={"success": False})
    with pytest.raises(cleanup.PermanentCleanupError, match="confirm"):
        client.destroy(123)


def test_guard_records_success_only_after_two_absent_reads(tmp_path, monkeypatch, job):
    """Exercise the complete guarded loop using only temporary files and mocks."""
    job_file = tmp_path / "job.json"
    job_file.write_text(json.dumps(job))
    (tmp_path / "credentials").mkdir()
    (tmp_path / "credentials/vast_api_key").write_text("synthetic-test-key")
    client = Mock()
    client.instances.side_effect = [[{"id": 123, "label": job["label"]}], [], RuntimeError("temporary outage"), [], []]
    monkeypatch.setattr(cleanup, "WORK_ROOT", tmp_path)
    monkeypatch.setattr(cleanup, "VastCleanupClient", lambda key: client)
    monkeypatch.setattr(cleanup.sys, "argv", ["cleanup.py", "--job", str(job_file), "--now"])
    monkeypatch.setattr(cleanup.time, "sleep", lambda seconds: None)
    assert cleanup.vast_cleanup_main() == 0
    assert client.instances.call_count == 5
    client.destroy.assert_called_once_with(123)
    record = json.loads(job_file.with_suffix(".status.json").read_text())
    assert record["state"] == "deletion_verified"
    assert record["error"] is None


def test_guard_stops_on_permanent_failure(tmp_path, monkeypatch, job):
    """A missing permission is recorded without an automatic retry loop."""
    job_file = tmp_path / "job.json"
    job_file.write_text(json.dumps(job))
    (tmp_path / "credentials").mkdir()
    (tmp_path / "credentials/vast_api_key").write_text("synthetic-test-key")
    client = Mock()
    client.instances.side_effect = cleanup.PermanentCleanupError("Vast HTTP 403")
    monkeypatch.setattr(cleanup, "WORK_ROOT", tmp_path)
    monkeypatch.setattr(cleanup, "VastCleanupClient", lambda key: client)
    monkeypatch.setattr(cleanup.sys, "argv", ["cleanup.py", "--job", str(job_file), "--now"])
    assert cleanup.vast_cleanup_main() == 2
    client.instances.assert_called_once()
    client.destroy.assert_not_called()
    assert json.loads(job_file.with_suffix(".status.json").read_text())["state"] == "refused"
