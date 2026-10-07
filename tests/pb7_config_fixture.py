"""Run pinned, real PB7 configuration code without a local bot or global imports."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile


_SHA256 = "edbdb1027a2b5f87920556658c268f7d1198e4a30b4ad5e78e6e9e88eacce9ff"
_PROBE = '''
import contextlib, io, json, socket, sys
sys.path.insert(0, sys.argv[1])
def deny_network(*args, **kwargs):
    raise RuntimeError("PB7 configuration fixture must remain offline")
socket.socket.connect = deny_network
socket.socket.connect_ex = deny_network
socket.create_connection = deny_network
request = json.load(sys.stdin)
with contextlib.redirect_stdout(io.StringIO()):
    from config.schema import get_template_config
    from config.load import load_prepared_config
    from config_utils import strip_config_metadata
    from config import metrics, scoring
    operation = request["operation"]
    if operation == "template":
        result = get_template_config()
    elif operation == "load":
        result = load_prepared_config(*request["args"], **request["kwargs"])
    elif operation == "strip":
        result = strip_config_metadata(*request["args"], **request["kwargs"])
    elif operation == "metadata":
        names = set(metrics.CURRENCY_METRICS) | set(metrics.SHARED_METRICS)
        names.update(metrics.ANALYSIS_SHARED_KEYS)
        result = {"currency": list(metrics.CURRENCY_METRICS),
                  "shared": list(metrics.SHARED_METRICS),
                  "analysis": list(metrics.ANALYSIS_SHARED_KEYS),
                  "goals": {name: scoring.default_objective_goal(name) for name in names}}
    else:
        raise ValueError(operation)
json.dump(result, sys.stdout)
'''


class PB7ConfigRuntime:
    """Invoke real schema and normalization in an isolated temporary checkout."""

    def __init__(self, root: Path):
        """Verify and extract the allowlisted upstream source archive."""
        archive = Path(__file__).parent / "fixtures/pb7_config_runtime/source.tar.gz"
        assert hashlib.sha256(archive.read_bytes()).hexdigest() == _SHA256
        self.root = root
        with tarfile.open(archive) as source:
            source.extractall(root, filter="data")
        self.probe = root / "probe.py"
        self.probe.write_text(_PROBE, encoding="utf-8")
        self.cache = {}

    def call(self, operation, *args, **kwargs):
        """Keep network access disabled and source modules outside pytest imports."""
        if operation in self.cache:
            return json.loads(self.cache[operation])
        result = subprocess.run(
            [sys.executable, "-I", "-B", str(self.probe), str(self.root / "src")],
            input=json.dumps({"operation": operation, "args": args, "kwargs": kwargs}),
            text=True, capture_output=True, cwd=self.root, timeout=30,
            env={"HOME": str(self.root), "PATH": os.defpath,
                 "PYTHONDONTWRITEBYTECODE": "1"},
        )
        if result.returncode:
            raise RuntimeError(result.stderr)
        if operation in {"template", "metadata"}:
            self.cache[operation] = result.stdout
        return json.loads(result.stdout)

    def load(self, *args, **kwargs):
        """Delegate file loading to the pinned PB7 implementation."""
        return self.call("load", *args, **kwargs)

    def strip(self, *args, **kwargs):
        """Delegate metadata removal to the pinned PB7 implementation."""
        return self.call("strip", *args, **kwargs)
