"""Offline Jobs Monitor routing contracts without workers or runtime job data."""

import importlib.util
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.auth import require_auth


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def jobs_page(monkeypatch):
    """Import the complete real router with fail-closed queue and worker doubles."""
    def reject_runtime(*_args, **_kwargs):
        """Never access real queue state or start a worker in page tests."""
        pytest.fail("Unexpected runtime access")

    monkeypatch.setitem(sys.modules, "task_queue", SimpleNamespace(**{
        name: reject_runtime for name in (
            "get_job_by_id", "list_jobs", "request_cancel_job", "delete_job",
            "retry_failed_job", "requeue_done_job",
        )
    }))
    monkeypatch.setitem(sys.modules, "task_worker", SimpleNamespace(start_pending_job=reject_runtime))
    spec = importlib.util.spec_from_file_location("jobs_page_under_test", ROOT / "api" / "jobs.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    app = FastAPI()
    app.include_router(module.router, prefix="/api/jobs")
    return SimpleNamespace(app=app, module=module)


@pytest.mark.parametrize("prefix", ["", "/proxy/pbgui", "/my gui"])
def test_authenticated_page_precedes_job_lookup_and_renders_urls(jobs_page, prefix):
    """The literal route renders no-store HTML, ignoring query and session secrets."""
    jobs_page.app.dependency_overrides[require_auth] = lambda: SimpleNamespace(token="private-session-fixture")
    with TestClient(jobs_page.app, root_path=prefix) as client:
        response = client.get("/api/jobs/main_page", params={
            "api_base": "https://evil.test", "root_path": "//evil.test",
            "token": "query-secret-fixture", "embed": "1", "exchange": "bitget",
        })
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert response.headers["cache-control"] == "no-store"
    assert "<title>Job Monitor - PBGui</title>" in response.text
    assert not any(marker in response.text for marker in (
        "%%API_BASE%%", "%%BASE_PREFIX%%", "%%WS_BASE%%", "private-session-fixture",
        "query-secret-fixture", "evil.test",
    ))
    expected = prefix.replace(" ", "%20")
    # script_json safely encodes percent signs in inline JavaScript.
    from api.page_templates import script_json

    assert f"const templateApiBase = {script_json(expected + '/api')};" in response.text
    assert f"const templatePrefix = {script_json(expected)};" in response.text


def test_page_rejects_unauthenticated_access_before_reading_template(jobs_page, monkeypatch):
    """The real auth dependency rejects a missing session without accessing disk."""
    def reject_read(*_args, **_kwargs):
        """Unauthenticated requests must never read the template."""
        pytest.fail("Template read before authentication")

    monkeypatch.setattr(jobs_page.module, "Path", reject_read)
    with TestClient(jobs_page.app) as client:
        response = client.get("/api/jobs/main_page")
    assert response.status_code == 401


def test_job_id_lookup_remains_json(jobs_page, monkeypatch):
    """Existing IDs and missing IDs still reach the original JSON handler."""
    calls = []

    def lookup(job_id):
        """Return only synthetic job data and record routing decisions."""
        calls.append(job_id)
        return {"id": job_id, "status": "done"} if job_id == "fixture-job" else None

    monkeypatch.setattr(jobs_page.module, "get_job_by_id", lookup)
    jobs_page.app.dependency_overrides[require_auth] = lambda: object()
    with TestClient(jobs_page.app) as client:
        assert client.get("/api/jobs/main_page").status_code == 200
        response = client.get("/api/jobs/fixture-job")
        assert response.status_code == 200
        assert response.json() == {"id": "fixture-job", "status": "done"}
        response = client.get("/api/jobs/missing-fixture")
        assert response.status_code == 404
        assert response.json() == {"detail": "Job missing-fixture not found"}
    assert calls == ["fixture-job", "missing-fixture"]


@pytest.mark.parametrize("failure", ["missing", "unreadable", "encoding", "read-error"])
def test_unavailable_template_is_logged_as_server_error(jobs_page, monkeypatch, tmp_path, failure):
    """Missing and unreadable isolated templates produce useful HTTP 500 errors."""
    fake_module = tmp_path / "api" / "jobs.py"
    template = tmp_path / "frontend" / "jobs_monitor.html"
    template.parent.mkdir()
    if failure == "encoding":
        template.write_bytes(b"\xff")
    elif failure == "unreadable":
        template.mkdir()  # An existing directory is not a usable template.
    elif failure == "read-error":
        template.write_text("<html>fixture</html>", encoding="utf-8")
        original_read = Path.read_text

        def fail_template_read(path, *args, **kwargs):
            """Simulate permission loss only for the isolated template."""
            if path == template:
                raise PermissionError("fixture permission error")
            return original_read(path, *args, **kwargs)

        monkeypatch.setattr(Path, "read_text", fail_template_read)
    monkeypatch.setattr(jobs_page.module, "__file__", str(fake_module))
    logs = []
    monkeypatch.setattr(jobs_page.module, "_log", lambda *args, **kwargs: logs.append((args, kwargs)))
    jobs_page.app.dependency_overrides[require_auth] = lambda: object()
    with TestClient(jobs_page.app) as client:
        response = client.get("/api/jobs/main_page")
    assert response.status_code == 500
    assert response.json() == {"detail": "Jobs Monitor template is unavailable"}
    assert len(logs) == 1
    assert logs[0][1]["level"] == "ERROR"
