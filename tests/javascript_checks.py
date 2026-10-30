"""Run existing Vanilla JS behavioral assertions in an isolated native browser.

The Python host supplies only versioned frontend/documentation source text. It
does not expose a filesystem bridge, user profile, credential store or network.
"""
from __future__ import annotations

import atexit
from functools import lru_cache
from pathlib import Path
import subprocess

from browser_driver import Browser

ROOT = Path(__file__).resolve().parents[1]
_browser = None
_page = None


class JavaScriptFailure(subprocess.CalledProcessError):
    """Keep bounded browser diagnostics visible when a checked fixture fails."""

    def __str__(self):
        """Avoid dumping the entire source program in a failure message."""
        return f"Browser JavaScript assertion failed (exit {self.returncode}): {self.stderr or self.stdout}"


@lru_cache(maxsize=1)
def _sources():
    """Supply only tracked non-secret browser assets and Guide fixture text."""
    result = {}
    for directory in (ROOT / "frontend", ROOT / "docs/help", ROOT / "docs/help_de"):
        for path in directory.rglob("*"):
            if path.is_file() and not path.is_symlink() and path.suffix in {".js", ".html", ".css", ".md"}:
                result[str(path)] = path.read_text()
    return result


def _close():
    """Release the process-owned fixture browser on interpreter shutdown."""
    global _browser, _page
    if _browser:
        _browser.close()
    _browser = _page = None


atexit.register(_close)


def _fixture_page():
    """Reuse one isolated blank fixture page without launching per assertion."""
    global _browser, _page
    if _browser is None:
        _browser = Browser()
        _page = _browser.new_page()
        _page.route("http://127.0.0.1/pbgui-test-fixture", lambda route: route.fulfill(
            content_type="text/html", body="<!doctype html><title>Isolated JS fixture</title>"))
        _page.goto("http://127.0.0.1/pbgui-test-fixture")
        _page.add_script_tag(path=Path(__file__).with_name("browser_assertions.js"))
        _page.evaluate("()=>{window.__pbguiFixtureSources={};}")
        batch = {}
        size = 0
        for name, source in _sources().items():
            batch[name] = source
            size += len(source)
            if size >= 512 * 1024:
                _page.evaluate("files=>{Object.assign(window.__pbguiFixtureSources,files);}", batch)
                batch, size = {}, 0
        if batch:
            _page.evaluate("files=>{Object.assign(window.__pbguiFixtureSources,files);}", batch)
    return _page


def run_javascript(args, *, cwd=None, input=None, text=False, capture_output=False,
                   timeout=30, check=False, encoding=None, **options):
    """Execute a script/expression in Chromium and retain explicit assertion results."""
    del capture_output
    if options:
        raise TypeError(f"Unsupported browser fixture options: {sorted(options)}")
    cwd = Path(cwd or ROOT).resolve()
    args = list(map(str, args))
    filename = ""
    check_only = bool(args and args[0] == "--check")
    if check_only:
        args.pop(0)
    if args and args[0] in {"-e", "--eval"}:
        source, remaining = args[1], args[2:]
        # Inline scripts retain the historical argument indexing explicitly.
        # Chromium receives only fixture arguments, never a command-line secret.
    elif args and args[0] != "-":
        path = (cwd / args[0]).resolve()
        if not (path.is_relative_to(ROOT / "tests") or path.is_relative_to(Path(tempfile_dir()))):
            raise ValueError("Browser fixture entry must belong to tests or a temporary directory")
        filename, source, remaining = str(path), path.read_text(), args[1:]
    else:
        source, remaining = input or "", args[1:] if args else []
    if isinstance(source, bytes):
        source = source.decode(encoding or "utf-8")
    page = _fixture_page()
    page.timeout = (timeout or 30) * 1000
    payload = {"source": source, "args": remaining, "input": input.decode() if isinstance(input, bytes) else input,
               "cwd": str(cwd), "filename": filename, "checkOnly": check_only}
    outcome = page.evaluate("arg=>window.__pbguiRunFixture({...arg,files:window.__pbguiFixtureSources})", payload)
    stdout, stderr = outcome["stdout"], outcome["stderr"]
    if not text and not encoding:
        stdout, stderr = stdout.encode(), stderr.encode()
    result = subprocess.CompletedProcess(["browser-fixture", filename or "<inline>"], outcome["returncode"], stdout, stderr)
    if check:
        if result.returncode:
            raise JavaScriptFailure(result.returncode, ["browser-fixture"], stdout, stderr)
    return result


def tempfile_dir():
    """Resolve the platform's temporary test directory without reading runtime files."""
    import tempfile
    return tempfile.gettempdir()
