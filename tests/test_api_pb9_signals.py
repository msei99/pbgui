"""Local Uvicorn stop regressions with only temporary roots and fixture helpers."""

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time

import httpx
import pytest

from tests.test_pb9_config import runtime


def _identity(pid):
    """Read only the identity/state of this test's recorded subprocess."""
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return fields[19], fields[0]
    except FileNotFoundError:
        return None


def test_sigterm_aborts_helper_group_before_uvicorn_request_drain(runtime, tmp_path):
    """An early signal cancels helpers even while the ASGI loop is temporarily blocked."""
    record_path = tmp_path / "helper-processes.json"
    with runtime.schema.open("a", encoding="utf-8") as stream:
        stream.write('import json, subprocess, sys, time\nfrom pathlib import Path\n'
                     'def get_template_config():\n'
                     '    child = subprocess.Popen([sys.executable, "-I", "-c", "import time; time.sleep(60)"])\n'
                     '    def identity(pid):\n'
                     '        return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19]\n'
                     f'    Path({str(record_path)!r}).write_text(json.dumps({{"helper": os.getpid(), '
                     '"helper_start": identity(os.getpid()), "child": child.pid, "child_start": identity(child.pid)}))\n'
                     '    time.sleep(60)\n')
    root = tmp_path / "api-root"
    root.mkdir()
    socket_path = tmp_path / "api.sock"
    blocked_path = tmp_path / "loop-blocked"
    configuration = tmp_path / "test-settings.json"
    configuration.write_text(json.dumps({"root": str(root), "socket": str(socket_path),
                                        "blocked": str(blocked_path), "pb9dir": str(runtime.root),
                                        "pb9venv": str(runtime.python)}), encoding="utf-8")
    script = tmp_path / "serve_fixture.py"
    script.write_text('''import asyncio, configparser, json, sys, time
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
settings = json.loads(Path(sys.argv[2]).read_text())
sys.path.insert(0, sys.argv[1])
import pbgui_purefunc
pbgui_purefunc.PBGDIR = settings["root"]
import logging_helpers
logging_helpers.LOG_ROOT = Path(settings["root"]) / "logs"
from api import auth, pb9
from fastapi import FastAPI
import uvicorn
parser = configparser.ConfigParser()
parser.read_dict({"main": {"pb9dir": settings["pb9dir"], "pb9venv": settings["pb9venv"]}})
pb9.load_ini_snapshot = lambda: parser
auth.validate_token = lambda token: SimpleNamespace(user_id="fixture") if token == "fixture-auth" else None
@asynccontextmanager
async def lifespan(app):
    pb9.startup(app, install_signal_hooks=True)
    try:
        yield
    finally:
        await pb9.shutdown(app)
app = FastAPI(lifespan=lifespan)
app.add_middleware(auth.BrowserOriginMiddleware)
app.include_router(pb9.router, prefix="/api/pb9")
@app.get("/block-loop")
async def block_loop():
    Path(settings["blocked"]).write_text("blocked")
    time.sleep(2.5)
    return {"done": True}
uvicorn.run(app, uds=settings["socket"], log_config=None, access_log=False, timeout_graceful_shutdown=10)
''', encoding="utf-8")
    environment = {**os.environ, "HOME": str(root), "PYTHONDONTWRITEBYTECODE": "1"}
    process = subprocess.Popen([sys.executable, "-I", "-B", str(script), str(Path(__file__).resolve().parents[1]),
                                str(configuration)], cwd=root, env=environment,
                               stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, start_new_session=True)
    responses, errors, workers = {}, [], []
    recorded = None

    def request(label, method, path, **kwargs):
        """Use only the temporary Unix socket, never a real host or service."""
        try:
            with httpx.Client(transport=httpx.HTTPTransport(uds=str(socket_path)),
                              base_url="http://fixture", timeout=8) as client:
                responses[label] = client.request(method, path, **kwargs)
        except Exception as exc:
            errors.append(exc)

    try:
        deadline = time.monotonic() + 8
        while True:
            assert process.poll() is None, "fixture server exited before accepting requests"
            try:
                with httpx.Client(transport=httpx.HTTPTransport(uds=str(socket_path)),
                                  base_url="http://fixture", timeout=0.2) as client:
                    if client.get("/api/pb9/runtime").status_code == 401:
                        break
            except httpx.HTTPError:
                pass
            assert time.monotonic() < deadline, "fixture server did not start"
            time.sleep(0.02)
        workers.append(threading.Thread(target=request, args=("template", "POST", "/api/pb9/template"),
                                       kwargs={"json": {}, "headers": {"Authorization": "Bearer fixture-auth"}}))
        workers[-1].start()
        deadline = time.monotonic() + 5
        while not record_path.exists():
            assert time.monotonic() < deadline, "fixture helper did not start"
            time.sleep(0.01)
        recorded = json.loads(record_path.read_text())
        workers.append(threading.Thread(target=request, args=("block", "GET", "/block-loop")))
        workers[-1].start()
        deadline = time.monotonic() + 3
        while not blocked_path.exists():
            assert time.monotonic() < deadline
            time.sleep(0.01)
        process.send_signal(signal.SIGTERM)
        # The loop remains asleep; transport cancellation must not wait for it.
        deadline = time.monotonic() + 1.5
        while any((identity := _identity(recorded[key])) is not None and identity[1] != "Z"
                  for key in ("helper", "child")):
            assert time.monotonic() < deadline, "SIGTERM failed to abort the owned process group promptly"
            time.sleep(0.01)
        process.wait(timeout=4)
        assert process.returncode in (0, -signal.SIGTERM)
        for worker in workers:
            worker.join(8)
            assert not worker.is_alive()
        assert not errors
        assert responses["template"].status_code == 503
        assert responses["template"].headers["x-pbgui-error-codes"] == "helper_cancelled"
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        # Contain only this test's recorded group if the implementation regresses.
        if recorded is None and record_path.exists():
            recorded = json.loads(record_path.read_text())
        if recorded is not None:
            identity = _identity(recorded["helper"])
            child_identity = _identity(recorded["child"])
            owned_pid = (recorded["helper"] if identity is not None and identity[0] == recorded["helper_start"] else
                         recorded["child"] if child_identity is not None and child_identity[0] == recorded["child_start"] else None)
            if owned_pid is not None:
                try:
                    if os.getpgid(owned_pid) == recorded["helper"]:
                        os.killpg(recorded["helper"], signal.SIGKILL)
                except ProcessLookupError:
                    pass
        for worker in workers:
            worker.join(8)
        process.stderr.close()
