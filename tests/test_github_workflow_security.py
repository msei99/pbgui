"""Security regression tests for repository GitHub Actions workflows."""

import ast
import base64
import io
import json
import re
import textwrap
import urllib.error
import urllib.request
from pathlib import Path
from unittest.mock import MagicMock

import pytest


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS_DIR = ROOT / ".github" / "workflows"
FULL_COMMIT_SHA = re.compile(r"[0-9a-fA-F]{40}")


def _workflow_source() -> str:
    """Return the Telegram commit-feed workflow source."""
    return (WORKFLOWS_DIR / "main.yml").read_text(encoding="utf-8")


def _embedded_client() -> str:
    """Extract the repository-controlled Python notification client."""
    source = _workflow_source()
    marker = "python3 - <<'PY'\n"
    assert marker in source
    return textwrap.dedent(source.split(marker, 1)[1].split("\n          PY", 1)[0])


def test_external_actions_are_pinned_to_full_commit_shas() -> None:
    """External actions must not execute code from mutable branches or tags."""
    for path in sorted(WORKFLOWS_DIR.glob("*.y*ml")):
        source = path.read_text(encoding="utf-8")
        for reference in re.findall(r"^\s*uses:\s*([^\s#]+)", source, flags=re.MULTILINE):
            if reference.startswith(("./", "docker://")):
                continue
            action, separator, revision = reference.rpartition("@")
            assert action and separator and FULL_COMMIT_SHA.fullmatch(revision), (
                f"{path.relative_to(ROOT)} uses unpinned action {reference}"
            )


def test_telegram_commit_feed_has_no_external_action() -> None:
    """The Telegram feed sends directly without exposing secrets to action code."""
    source = _workflow_source()

    assert "uses:" not in source
    permissions = source.split("permissions:", 1)[1].split("\njobs:", 1)[0]
    assert "contents: read" in permissions
    assert "write" not in permissions
    assert "toJSON(github)" not in source
    assert "EverythingSuckz/github-telegram-notify" not in source
    assert "branches:\n      - '**'" in source
    assert "workflow_dispatch:" in source


def test_telegram_commit_feed_python_is_valid() -> None:
    """The embedded notification client must remain syntactically valid Python."""
    ast.parse(_embedded_client())


def test_telegram_commit_feed_builds_safe_push_message(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The local client safely formats a push without making a network request."""
    event_path = tmp_path / "push.json"
    event_path.write_text(
        json.dumps({
            "ref": "refs/heads/dev",
            "compare": "https://github.com/msei99/pbgui/compare/old...new",
            "repository": {
                "full_name": "msei99/pbgui",
                "html_url": "https://github.com/msei99/pbgui",
            },
            "commits": [{
                "id": "1234567890abcdef",
                "url": "https://github.com/msei99/pbgui/commit/1234567890abcdef",
                "message": "Fix <unsafe> & notify",
                "author": {"name": "A <B>", "username": "example"},
            }],
        }),
        encoding="utf-8",
    )
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(event_path))
    monkeypatch.setenv("GITHUB_REPOSITORY", "msei99/pbgui")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "-100123")
    monkeypatch.setenv("TELEGRAM_TOPIC_ID", "42")

    requests = []
    response = MagicMock()
    response.__enter__.return_value = response
    response.read.return_value = b'{"ok": true}'

    def fake_urlopen(request: urllib.request.Request, timeout: int) -> MagicMock:
        """Capture the Telegram request and return a successful response."""
        requests.append((request, timeout))
        return response

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    exec(compile(_embedded_client(), "telegram-commit-feed", "exec"), {})

    assert len(requests) == 1
    request, timeout = requests[0]
    payload = json.loads(request.data)
    assert timeout == 15
    assert payload["chat_id"] == "-100123"
    assert payload["message_thread_id"] == 42
    assert "[<code>dev</code>]" in payload["text"]
    assert "Fix &lt;unsafe&gt; &amp; notify" in payload["text"]
    assert "A &lt;B&gt;" in payload["text"]
    assert payload["reply_markup"]["inline_keyboard"][0][0]["text"] == "Open Changes"


def test_telegram_release_feed_retries_rate_limited_notes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A transient Telegram rate limit must not drop release notes."""
    event_path = tmp_path / "release.json"
    event_path.write_text(
        json.dumps({
            "after": "abcdef1234567890",
            "ref": "refs/heads/main",
            "repository": {
                "full_name": "msei99/pbgui",
                "html_url": "https://github.com/msei99/pbgui",
            },
            "commits": [{
                "id": "abcdef1234567890",
                "url": "https://github.com/msei99/pbgui/commit/abcdef1234567890",
                "message": "Release v2.0.3",
                "author": {"name": "mani", "username": "msei99"},
            }],
        }),
        encoding="utf-8",
    )
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(event_path))
    monkeypatch.setenv("GITHUB_REPOSITORY", "msei99/pbgui")
    monkeypatch.setenv("GITHUB_TOKEN", "test-github-token")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-telegram-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "-100123")
    monkeypatch.delenv("TELEGRAM_TOPIC_ID", raising=False)

    requests = []
    sleeps = []
    success = MagicMock()
    success.__enter__.return_value = success
    success.read.return_value = b'{"ok": true}'
    github_response = MagicMock()
    github_response.__enter__.return_value = github_response
    github_response.read.return_value = json.dumps({
        "content": base64.b64encode(b"# v2.0.3\n\n## Fixed\n\n- Release notes delivered.\n").decode(),
    }).encode()

    def fake_urlopen(request: urllib.request.Request, timeout: int) -> MagicMock:
        """Rate-limit the first notes request, then accept its retry."""
        assert timeout == 15
        if request.full_url.startswith("https://api.github.com/"):
            return github_response
        requests.append(request)
        if len(requests) == 2:
            raise urllib.error.HTTPError(
                request.full_url,
                429,
                "Too Many Requests",
                {},
                io.BytesIO(b'{"parameters":{"retry_after":2}}'),
            )
        return success

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr("time.sleep", sleeps.append)
    exec(compile(_embedded_client(), "telegram-release-feed", "exec"), {})

    assert sleeps == [2.0]
    assert len(requests) == 3
    assert requests[1].data == requests[2].data
    notes_payload = json.loads(requests[2].data)
    assert "Release notes v2.0.3" in notes_payload["text"]
    assert "- Release notes delivered." in notes_payload["text"]


def test_telegram_release_feed_can_replay_notes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A manual workflow dispatch must send only the requested release notes."""
    event_path = tmp_path / "dispatch.json"
    event_path.write_text(
        json.dumps({
            "inputs": {"version": "v2.0.3"},
            "ref": "refs/heads/main",
            "repository": {
                "full_name": "msei99/pbgui",
                "html_url": "https://github.com/msei99/pbgui",
            },
        }),
        encoding="utf-8",
    )
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(event_path))
    monkeypatch.setenv("GITHUB_REPOSITORY", "msei99/pbgui")
    monkeypatch.setenv("GITHUB_SHA", "abcdef1234567890")
    monkeypatch.setenv("GITHUB_TOKEN", "test-github-token")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-telegram-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "-100123")
    monkeypatch.delenv("TELEGRAM_TOPIC_ID", raising=False)

    requests = []
    response = MagicMock()
    response.__enter__.return_value = response

    def fake_urlopen(request: urllib.request.Request, timeout: int) -> MagicMock:
        """Return release notes from GitHub and capture the Telegram replay."""
        assert timeout == 15
        if request.full_url.startswith("https://api.github.com/"):
            response.read.return_value = json.dumps({
                "content": base64.b64encode(b"# v2.0.3\n\n## Fixed\n\n- Replayed.\n").decode(),
            }).encode()
        else:
            requests.append(request)
            response.read.return_value = b'{"ok": true}'
        return response

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    exec(compile(_embedded_client(), "telegram-release-replay", "exec"), {})

    assert len(requests) == 1
    payload = json.loads(requests[0].data)
    assert payload["text"].startswith("Release notes v2.0.3\n")
    assert "- Replayed." in payload["text"]
    assert "new commit(s)" not in payload["text"]
