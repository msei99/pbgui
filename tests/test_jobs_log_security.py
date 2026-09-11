"""Job-log route regressions using temporary files without worker startup."""

import ast
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest
from fastapi import APIRouter, Depends, FastAPI, HTTPException
from fastapi.testclient import TestClient


@pytest.fixture
def job_logs(tmp_path, monkeypatch):
    """Load the real route body and decorator without importing task workers."""
    monkeypatch.setitem(sys.modules, "pbgui_purefunc", SimpleNamespace(PBGDIR=tmp_path))
    source = Path(__file__).resolve().parents[1] / "api" / "jobs.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    body = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "get_job_log"]
    assert len(body) == 1
    namespace = {
        "router": APIRouter(), "HTTPException": HTTPException,
        "Depends": Depends, "SessionToken": object, "require_auth": lambda: object(),
    }
    exec(compile(ast.Module(body=body, type_ignores=[]), str(source), "exec"), namespace)
    root = tmp_path / "data" / "logs" / "jobs"
    root.mkdir(parents=True)
    app = FastAPI()
    app.include_router(namespace["router"], prefix="/api/jobs")
    with TestClient(app) as client:
        yield SimpleNamespace(root=root, read=namespace["get_job_log"], client=client)


@pytest.mark.parametrize("job_id", [
    "", ".", "..", "../../vps_monitor", "../jobs-other/private",
    "/tmp/private", r"..\private", r"C:\private", "bad\x00id",
    "bad\nid", "bad\rid", "bad\tid", "bad\x7fid",
])
def test_rejects_unsafe_job_ids(job_logs, job_id):
    """Unsafe identifiers fail at the boundary even when invoked directly."""
    with pytest.raises(HTTPException) as error:
        job_logs.read(job_id)
    assert error.value.status_code == 400
    assert error.value.detail == "Invalid job ID"


@pytest.mark.parametrize("target_exists", [False, True])
def test_rejects_symlink_escape(job_logs, target_exists):
    """Existing and dangling links cannot escape into a sibling directory."""
    sibling = job_logs.root.parent / "jobs-other"
    sibling.mkdir()
    target = sibling / "private.log"
    if target_exists:
        target.write_text("outside log", encoding="utf-8")
    (job_logs.root / "linked.log").symlink_to(target)
    response = job_logs.client.get("/api/jobs/linked/log")
    assert response.status_code == 400
    assert response.json() == {"detail": "Invalid job ID"}


@pytest.mark.parametrize("encoded_id", ["..%2F..%2Fvps_monitor", "..%5Cprivate", "bad%00id", "bad%0Aid"])
def test_encoded_traversal_requests(job_logs, encoded_id):
    """HTTP routing or ID validation rejects encoded traversal and controls."""
    (job_logs.root.parent.parent / "vps_monitor.log").write_text("outside log", encoding="utf-8")
    response = job_logs.client.get(f"/api/jobs/{encoded_id}/log")
    assert response.status_code in {400, 404}
    assert "outside log" not in response.text


@pytest.mark.parametrize("lines, expected", [(500, ["first", "second", "third"]), (2, ["second", "third"]), (0, ["first", "second", "third"])])
def test_valid_job_log_preserves_response(job_logs, lines, expected):
    """Normal generated IDs retain full-log and tail behavior."""
    job_id = "1789162421-abcdef0123"
    (job_logs.root / f"{job_id}.log").write_bytes(b"first\r\nsecond\nthird\n")
    response = job_logs.client.get(f"/api/jobs/{job_id}/log", params={"lines": lines})
    assert response.status_code == 200
    assert response.json() == {"log": expected, "exists": True}


def test_missing_job_log_preserves_response(job_logs):
    """Missing logs continue to return the existing empty success response."""
    response = job_logs.client.get("/api/jobs/missing/log")
    assert response.status_code == 200
    assert response.json() == {"log": [], "exists": False}
