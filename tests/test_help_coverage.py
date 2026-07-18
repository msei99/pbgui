"""Contract tests for bilingual shared-help coverage."""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
NAV_PATH = ROOT / "frontend" / "pbgui_nav.js"
HELP_DIRS = (ROOT / "docs" / "help", ROOT / "docs" / "strategy_explorer")
HELP_DIRS_DE = (ROOT / "docs" / "help_de", ROOT / "docs" / "strategy_explorer_de")


def _js_object_entries(source: str, name: str) -> dict[str, str]:
    """Extract string entries from one simple JavaScript object literal."""
    match = re.search(rf"var\s+{re.escape(name)}\s*=\s*\{{(?P<body>.*?)\n\s*\}};", source, re.DOTALL)
    assert match, f"Could not find JavaScript object {name}"
    return dict(re.findall(r"'([^']+)'\s*:\s*'([^']+)'", match.group("body")))


def _topic_names(directories: tuple[Path, ...]) -> list[str]:
    """Return sorted Markdown topic stems across help directories."""
    return sorted(path.stem for directory in directories for path in directory.glob("*.md"))


def _configured_page_keys() -> set[str]:
    """Return page keys declared by standalone frontend templates."""
    pages: set[str] = set()
    config_pattern = re.compile(r"PBGUI_NAV_CONFIG\s*=\s*\{(?P<body>.*?)\};", re.DOTALL)
    current_pattern = re.compile(r"\bcurrent\s*:\s*['\"]([^'\"]+)['\"]")
    for path in (ROOT / "frontend").glob("*.html"):
        source = path.read_text(encoding="utf-8")
        for config in config_pattern.finditer(source):
            current = current_pattern.search(config.group("body"))
            if current:
                pages.add(current.group(1))
    return pages


def test_help_topics_have_exact_english_german_parity() -> None:
    """Every English general or Strategy Explorer topic has a German peer."""
    assert _topic_names(HELP_DIRS) == _topic_names(HELP_DIRS_DE)


def test_every_registered_page_maps_to_one_bilingual_help_topic() -> None:
    """Navigation routes and rendered page keys must have resolvable help topics."""
    source = NAV_PATH.read_text(encoding="utf-8")
    routes = _js_object_entries(source, "FASTAPI_PAGES")
    topics = _js_object_entries(source, "GUIDE_TOPICS")

    assert topics.keys() == routes.keys()
    assert _configured_page_keys() <= topics.keys()

    english = _topic_names(HELP_DIRS)
    german = _topic_names(HELP_DIRS_DE)
    for page, topic in topics.items():
        assert english.count(topic) == 1, f"{page} maps to missing or ambiguous EN topic {topic}"
        assert german.count(topic) == 1, f"{page} maps to missing or ambiguous DE topic {topic}"


def test_strategy_explorer_uses_shared_bilingual_help() -> None:
    """Strategy Explorer must not shadow its complete shared docs with inline help."""
    source = (ROOT / "frontend" / "v7_strategy_explorer.html").read_text(encoding="utf-8")
    topics = _js_object_entries(NAV_PATH.read_text(encoding="utf-8"), "GUIDE_TOPICS")

    assert topics["v7_strategy_explorer"] == "00_strategy_explorer_help"
    assert "window.PBGUI_HELP_OPENER" not in source
    assert 'id="help-ovl"' not in source
