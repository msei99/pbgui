"""Owner-scoped Claude subscription runtime and bounded SDK protocol bridge."""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
import platform
import re
import shutil
import signal
import sys
import time
import uuid
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from logging_helpers import human_log as _log
from secure_files import ensure_private_directory_tree

SERVICE = "AIChat"
RUNTIME = Path(__file__).resolve().parent / "runtime" / "claude_agent"
SDK_VERSION = "0.3.310"
CLAUDE_UNAVAILABLE = "Claude subscription is not connected"
MAX_LINE = 2 * 1024 * 1024
# uvloop rejects subprocess.Popen's umask option. Set it in an isolated child,
# before exec, without changing the threaded API server's process-wide umask.
_PRIVATE_EXEC = "import os,sys; os.umask(0o077); os.execvpe(sys.argv[1], sys.argv[1:], os.environ)"


class ClaudeCodeError(RuntimeError):
    """A non-secret, user-visible Claude runtime error."""


def claude_status() -> dict:
    """Inspect dependencies without opening an account profile."""
    return {"available": ClaudeRuntime.available(), "connected": False,
            "limits_available": False, "catalog": []}


class ClaudeRuntime:
    """Own private native profiles, login processes and ephemeral SDK requests."""

    def __init__(self, root: Path):
        """Keep process and cache ownership within the API service lifetime."""
        self.root = Path(root)
        self.processes = set()
        self.logins = {}
        self.cache = {}
        self.tasks = set()
        self.operations = {}
        self.login_lock = asyncio.Lock()
        self.spawn_lock = asyncio.Lock()
        self.closed = False

    @staticmethod
    def executable() -> Path:
        """Use the version-pinned native binary supplied with the local SDK."""
        system = {"Linux": "linux", "Darwin": "darwin"}.get(platform.system(), "unsupported")
        arch = {"x86_64": "x64", "aarch64": "arm64", "arm64": "arm64"}.get(platform.machine(), "unsupported")
        suffix = "-musl" if system == "linux" and platform.libc_ver()[0] == "musl" else ""
        return RUNTIME / "node_modules" / "@anthropic-ai" / f"claude-agent-sdk-{system}-{arch}{suffix}" / "claude"

    @classmethod
    def available(cls) -> bool:
        """Check pinned dependencies locally without installing or calling a model."""
        try:
            package = RUNTIME / "node_modules" / "@anthropic-ai" / "claude-agent-sdk" / "package.json"
            platform_package = cls.executable().parent / "package.json"
            schema_package = RUNTIME / "node_modules" / "zod" / "package.json"
            return bool(shutil.which("node") and package.is_file()
                        and json.loads(package.read_text())["version"] == SDK_VERSION
                        and platform_package.is_file() and json.loads(platform_package.read_text())["version"] == SDK_VERSION
                        and schema_package.is_file() and json.loads(schema_package.read_text())["version"] == "4.1.12"
                        and cls.executable().is_file() and os.access(cls.executable(), os.X_OK))
        except (OSError, ValueError, KeyError):
            return False

    def profile(self, owner: str) -> Path:
        """Validate persisted owner identifiers before resolving private paths."""
        if not re.fullmatch(r"[a-f0-9]{32}", owner):
            raise ClaudeCodeError("Invalid Claude account owner")
        path = self.root / owner
        for part in ("config", "workspace", "cache", "data"):
            ensure_private_directory_tree(self.root, path / part)
        return path

    def environment(self, owner: str) -> dict:
        """Build a fresh environment so API keys and routing never leak in."""
        profile = self.profile(owner)
        env = {k: v for k, v in os.environ.items()
               if k in {"PATH", "LANG", "LC_ALL", "TZ", "SSL_CERT_FILE", "SSL_CERT_DIR"}}
        env.update(HOME=str(profile), CLAUDE_CONFIG_DIR=str(profile / "config"),
                   XDG_CONFIG_HOME=str(profile / "config"), XDG_CACHE_HOME=str(profile / "cache"),
                   XDG_DATA_HOME=str(profile / "data"), CLAUDE_CODE_MAX_RETRIES="0",
                   BROWSER="false", CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC="1")
        return env

    async def _spawn(self, owner, *args, merge_stderr=False):
        """Register each process immediately with a private cwd and owned pipes."""
        if self.closed:
            raise ClaudeCodeError("Claude runtime is shutting down")
        if not self.available():
            raise ClaudeCodeError("Claude requires Node.js 22+ and the local SDK. See Guide.")
        async with self.spawn_lock:
            if self.closed or len(self.processes) >= 32:
                raise ClaudeCodeError("Claude runtime is busy. Try again shortly.")
            try:
                process = await asyncio.create_subprocess_exec(
                    sys.executable, "-I", "-S", "-c", _PRIVATE_EXEC, *map(str, args),
                    cwd=str(self.profile(owner) / "workspace"), env=self.environment(owner),
                    stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT if merge_stderr else asyncio.subprocess.PIPE,
                    start_new_session=True, limit=MAX_LINE)
            except (OSError, ValueError) as exc:
                _log(SERVICE, f"Claude process start failed: {type(exc).__name__}", level="WARNING")
                raise ClaudeCodeError("Claude runtime could not start. Check the local runtime installation.") from exc
            self.processes.add(process)
        return process

    async def _stop(self, process):
        """Close pipes and terminate the owned process group within bounds."""
        if process.stdin:
            process.stdin.close()
        for sig, grace in ((signal.SIGINT, 2), (signal.SIGTERM, 3), (signal.SIGKILL, 2)):
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, sig)
            try:
                await asyncio.wait_for(process.wait(), grace)
                break
            except asyncio.TimeoutError:
                continue
        self.processes.discard(process)
        # The group can still contain descendants after its leader has exited.
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)

    @staticmethod
    async def _drain(reader):
        """Discard diagnostics rather than exposing provider output or credentials."""
        if reader is None:
            return
        while await reader.read(8192):
            pass

    async def _native(self, owner, *args):
        """Run bounded native account commands retaining only structured output."""
        process = await self._spawn(owner, self.executable(), *args)
        drain = asyncio.create_task(self._drain(process.stderr))
        try:
            async with asyncio.timeout(12):
                chunks, size = [], 0
                while chunk := await process.stdout.read(8192):
                    chunks.append(chunk)
                    size += len(chunk)
                    if size > MAX_LINE:
                        raise ClaudeCodeError("Claude account command returned too much data")
                output = b"".join(chunks)
                await process.wait()
            if len(output) > MAX_LINE or process.returncode not in (0, 1):
                raise ClaudeCodeError("Claude account command failed")
            return process.returncode, output
        except TimeoutError as exc:
            raise ClaudeCodeError("Claude account command timed out") from exc
        finally:
            await self._stop(process)
            await asyncio.gather(drain, return_exceptions=True)

    async def status(self, owner):
        """Project allowlisted account metadata, never native authentication payloads."""
        result = {**claude_status(), "email": "", "plan": ""}
        if not result["available"]:
            result["error"] = "Claude runtime missing. See Guide."
            return result
        if not re.fullmatch(r"[a-f0-9]{32}", owner):
            raise ClaudeCodeError("Invalid Claude account owner")
        if not (self.root / owner / "config").exists():
            return result
        try:
            _, raw = await self._native(owner, "auth", "status", "--json")
            account = json.loads(raw)
            result["connected"] = bool(account.get("loggedIn") and account.get("authMethod") == "claude.ai")
            if result["connected"]:
                result.update(email=str(account.get("email") or "")[:255],
                              plan=str(account.get("subscriptionType") or "")[:80])
            elif account.get("loggedIn"):
                result["error"] = "Sign in with a Claude subscription; API billing is disabled."
            cached = self.cache.get(owner)
            if cached and result["connected"]:
                result["catalog"] = [item["id"] for item in cached[1]]
            login = self.logins.get(owner)
            if login:
                result["login"] = {k: login[k] for k in ("id", "url", "waiting_for_code", "error")}
                result["login"]["active"] = not login["task"].done()
            return result
        except (ValueError, OSError, ClaudeCodeError) as exc:
            _log(SERVICE, f"Claude account status failed: {type(exc).__name__}", level="WARNING")
            result["error"] = "Claude account status is unavailable"
            return result

    @staticmethod
    def _login_url(value):
        """Accept native Anthropic authorization URLs, never token URLs."""
        try:
            parsed = urlsplit(value)
            allowed = {("claude.ai", "/oauth/authorize"), ("claude.com", "/cai/oauth/authorize"),
                       ("console.anthropic.com", "/oauth/authorize"), ("platform.claude.com", "/oauth/authorize")}
            if (parsed.scheme == "https" and (parsed.hostname, parsed.path.rstrip("/")) in allowed
                    and parsed.port in (None, 443) and not parsed.fragment
                    and not parsed.username and not parsed.password
                    and not any("token" in key.lower() or "secret" in key.lower() for key in parse_qs(parsed.query))):
                return value
        except ValueError:
            pass
        return ""

    async def start_login(self, owner):
        """Run Anthropic's native subscription flow with its own credential storage."""
        async with self.login_lock:
            return await self._start_login(owner)

    async def _start_login(self, owner):
        """Serialize replacement flows so stale tabs cannot submit to a newer login."""
        await self._cancel_login(owner)
        for key, value in list(self.logins.items()):
            if value["task"].done():
                self.logins.pop(key, None)
        if len(self.logins) >= 16:
            raise ClaudeCodeError("Too many pending Claude sign-ins. Try again shortly.")
        self.cache.pop(owner, None)
        process = await self._spawn(owner, self.executable(), "auth", "login", "--claudeai", merge_stderr=True)
        record = {"id": uuid.uuid4().hex, "process": process, "url": "", "waiting_for_code": False, "error": ""}
        self.logins[owner] = record
        async def monitor():
            """Extract only the native login link, bounding pending login lifetime."""
            drain = asyncio.create_task(self._drain(process.stderr))
            buffer = ""
            try:
                async with asyncio.timeout(600):
                    while chunk := await process.stdout.read(4096):
                        buffer = (buffer + chunk.decode("utf-8", "replace"))[-16384:]
                        for candidate in re.findall(r"https://[^\s\x1b<>]+", buffer):
                            url = self._login_url(candidate.rstrip(".)"))
                            if url:
                                record["url"] = url
                        record["waiting_for_code"] = bool(record["url"])
                    await process.wait()
                    if process.returncode:
                        record["error"] = "Claude sign-in failed. Start sign-in again."
            except TimeoutError:
                record["error"] = "Claude sign-in expired. Start sign-in again."
            finally:
                await self._stop(process)
                await asyncio.gather(drain, return_exceptions=True)
                record["waiting_for_code"] = False
                record["url"] = ""
                self.cache.pop(owner, None)
        task = asyncio.create_task(monitor(), name=f"claude-login-{owner}")
        record["task"] = task
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return {"started": True, "login_id": record["id"]}

    async def login_code(self, owner, code, login_id):
        """Forward a one-time code in stdin without persisting or logging it."""
        async with self.login_lock:
            record = self.logins.get(owner)
            if (not record or record["id"] != login_id or not record["waiting_for_code"]
                    or record["process"].returncode is not None):
                raise ClaudeCodeError("No Claude sign-in is waiting for this code")
            if not isinstance(code, str) or not re.fullmatch(r"[A-Za-z0-9_.~#:/+=-]{1,2048}", code):
                raise ClaudeCodeError("Invalid Claude sign-in code")
            try:
                record["process"].stdin.write((code + "\n").encode())
                await record["process"].stdin.drain()
            except OSError as exc:
                raise ClaudeCodeError("Claude sign-in ended. Start sign-in again.") from exc

    async def cancel_login(self, owner):
        """Cancel a native flow without touching another user's profile."""
        async with self.login_lock:
            await self._cancel_login(owner)

    async def _cancel_login(self, owner):
        """Release a login while holding the lifecycle lock."""
        record = self.logins.pop(owner, None)
        if record:
            record["task"].cancel()
            await asyncio.gather(record["task"], return_exceptions=True)
            await self._stop(record["process"])

    async def logout(self, owner):
        """Disconnect through native logout after cancelling owned work."""
        async with self.login_lock:
            await self._logout(owner)

    async def _logout(self, owner):
        """Keep disconnect atomic with respect to a replacement login."""
        await self._cancel_login(owner)
        current = asyncio.current_task()
        tasks = [task for task, account in list(self.operations.items()) if account == owner and task is not current]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        code, _ = await self._native(owner, "auth", "logout")
        if code:
            raise ClaudeCodeError("Claude sign-out failed")
        self.cache.pop(owner, None)

    async def _request(self, owner, payload, handler=None, on_text=None, *, on_event=None, handler_with_id=False, timeout_seconds=None):
        """Exchange bounded JSON messages with an ephemeral SDK host."""
        timeout = 15 if payload['mode'] in {'models', 'context'} else 180 if timeout_seconds is None else timeout_seconds
        if type(timeout) not in (int, float) or not 0 < timeout <= 604800:
            raise ClaudeCodeError('Invalid Claude request timeout')
        process = await self._spawn(owner, shutil.which("node"), RUNTIME / "worker.mjs")
        task = asyncio.current_task()
        self.operations[task] = owner
        drain = asyncio.create_task(self._drain(process.stderr))
        tool_count, result = 0, None
        try:
            payload = {**payload, "executable": str(self.executable())}
            process.stdin.write((json.dumps(payload, allow_nan=False) + "\n").encode())
            await process.stdin.drain()
            async with asyncio.timeout(timeout):
                while line := await process.stdout.readline():
                    event = json.loads(line)
                    if not isinstance(event, dict):
                        raise ClaudeCodeError("Claude returned an invalid protocol message")
                    kind = event.get("type")
                    if kind == "tool":
                        tool_count += 1
                        if tool_count > 64 or not handler or not isinstance(event.get("id"), str):
                            raise ClaudeCodeError("Claude requested an unavailable PBGui capability")
                        output = await handler(event.get("name"), event.get("arguments"), event['id']) if handler_with_id else await handler(event.get("name"), event.get("arguments"))
                        process.stdin.write((json.dumps({"type": "tool_result", "id": event["id"], "result": output}, allow_nan=False) + "\n").encode())
                        await process.stdin.drain()
                    elif kind == "text":
                        if on_text:
                            await on_text(str(event.get("text") or "")[:100000])
                    elif kind == 'runtime' and on_event:
                        await on_event(event)
                    elif kind == "result":
                        result = event.get("result")
                    elif kind == "error":
                        raise ClaudeCodeError(str(event.get("message") or "Claude request failed")[:300])
                    else:
                        raise ClaudeCodeError("Claude returned an invalid protocol message")
                await process.wait()
            if process.returncode or result is None:
                raise ClaudeCodeError("Claude request ended without a completed result")
            return result
        except (TimeoutError, ValueError, OSError) as exc:
            _log(SERVICE, f"Claude request failed: {type(exc).__name__}", level="WARNING")
            raise ClaudeCodeError("Claude request timed out" if isinstance(exc, TimeoutError) else "Claude runtime protocol failed") from exc
        finally:
            await self._stop(process)
            await asyncio.gather(drain, return_exceptions=True)
            self.operations.pop(task, None)

    async def models(self, owner):
        """Use SDK metadata and persist concrete model identities instead of aliases."""
        if not (await self.status(owner))["connected"]:
            raise ClaudeCodeError(CLAUDE_UNAVAILABLE)
        cached = self.cache.get(owner)
        if cached and time.monotonic() - cached[0] < 60:
            return cached[1]
        result = await self._request(owner, {"mode": "models"})
        if not isinstance(result, list) or any(not isinstance(item, dict) for item in result):
            raise ClaudeCodeError("Claude returned an invalid model catalog")
        models = []
        for item in result:
            identity = item.get("resolvedModel") or item.get("value", "")
            if not isinstance(identity, str) or not re.fullmatch(r"claude-[A-Za-z0-9._-]{1,110}", identity):
                continue
            # Alias rows (including "default") can refer to the same model.
            # Derive the visible version from the SDK-confirmed wire identity.
            version = re.fullmatch(r"claude-([A-Za-z][A-Za-z0-9]*)-(\d+)(?:-(\d{1,2}))?(?:-\d{8})?", identity)
            name = (f"{version[1].title()} {version[2]}" + (f".{version[3]}" if version[3] else "")
                    if version else identity if item.get("value") == "default" else str(item.get("displayName") or identity)[:150])
            duplicate = next((model for model in models if model["id"] == identity), None)
            if duplicate is not None:
                if item.get("value") != "default":
                    duplicate["name"] = name
                continue
            models.append({"id": identity, "name": name,
                           "tools": True, "default": not models,
                           "reasoning_variants": [{"id": e, "label": e.title()} for e in item.get("supportedEffortLevels", []) if e in {"low", "medium", "high", "xhigh", "max"}]})
        if not models:
            raise ClaudeCodeError("Claude did not advertise any supported model identities")
        if len(self.cache) >= 100:
            self.cache.pop(next(iter(self.cache)))
        self.cache[owner] = (time.monotonic(), models)
        return models

    async def model_context(self, owner, model):
        """Read native context metadata for an advertised model without submitting a prompt."""
        selected = next((item for item in await self.models(owner) if item['id'] == model), None)
        if selected is None:
            raise ClaudeCodeError('Selected Claude model is unavailable')
        if type(selected.get('context')) is int and selected['context'] > 0:
            return {'context': selected['context']}
        metadata = await self._request(owner, {'mode': 'context', 'model': model})
        if (not isinstance(metadata, dict) or metadata.get('model') != model
                or type(metadata.get('context')) is not int or not 0 < metadata['context'] <= 100000000):
            raise ClaudeCodeError('Claude did not report a valid model context size')
        selected['context'] = metadata['context']
        return {'context': selected['context']}

    async def reply(self, owner, model, history, instructions, *, tools=None, handler=None, on_text=None, effort="",
                    on_event=None, handler_with_id=False, timeout_seconds=None, output_tokens=None,
                    max_reply_chars=100000, output_schema=None):
        """Pin an SDK turn to the selected owner and concrete model."""
        if model not in {item["id"] for item in await self.models(owner)}:
            raise ClaudeCodeError("Selected Claude model is unavailable")
        if output_schema is not None:
            try:
                if (not isinstance(output_schema, dict) or output_schema.get('type') != 'object'
                        or len(json.dumps(output_schema, allow_nan=False).encode()) > 131072):
                    raise ValueError('Invalid schema')
            except (ValueError, TypeError) as exc:
                raise ClaudeCodeError('Invalid Claude structured output schema') from exc
        result = await self._request(owner, {"mode": "chat", "model": model, "history": history,
            "instructions": instructions, "tools": tools or [], "effort": effort,
            'observe_runtime': bool(on_event), 'output_tokens': output_tokens,
            **({'output_schema': output_schema} if output_schema is not None else {})}, handler, on_text,
            on_event=on_event, handler_with_id=handler_with_id, timeout_seconds=timeout_seconds)
        if not isinstance(result, str) or not result or len(result) > max_reply_chars:
            raise ClaudeCodeError("Claude returned an invalid or empty reply")
        if output_schema is not None:
            try:
                value = json.loads(result, parse_constant=lambda _value: (_ for _ in ()).throw(
                    ValueError('Nonfinite structured response')))
                if not isinstance(value, dict):
                    raise ValueError('Expected a structured object')
                # Reject nonfinite values even if an unexpected worker bypassed serialization.
                json.dumps(value, allow_nan=False)
            except (ValueError, TypeError) as exc:
                raise ClaudeCodeError('Claude did not return the required structured decision.') from exc
        return result

    async def close(self):
        """Cancel login owners and close every API-owned process deterministically."""
        self.closed = True
        # Wait for any in-flight spawn to register its process before cleanup.
        async with self.spawn_lock:
            pass
        async with self.login_lock:
            pass
        tasks = list(self.tasks)
        tasks += [task for task in self.operations if task is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.gather(*(self._stop(p) for p in list(self.processes)), return_exceptions=True)
        self.logins.clear()
        self.cache.clear()
