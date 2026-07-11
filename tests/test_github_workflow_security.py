"""Security regression tests for repository GitHub Actions workflows."""

import ast
import json
import re
import textwrap
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
    assert "permissions: {}" in source
    assert "toJSON(github)" not in source
    assert "EverythingSuckz/github-telegram-notify" not in source
    assert "branches:\n      - '**'" in source


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
