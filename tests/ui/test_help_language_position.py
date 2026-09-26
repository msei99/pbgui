"""The guide language switch keeps the current topic and reading position."""

from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[2]


def test_translated_guide_restores_matching_paragraph() -> None:
    """Map the current heading and paragraph across different translation lengths."""
    source = (ROOT / "frontend/js/shared_help_overlay.js").read_text()
    helpers = source[source.index("  function guideBlocks("):source.index("  window.PBGuiHelpPosition =")]
    script = helpers + r'''
const assert = require('node:assert/strict');
function content(tops, heights, scrollTop) {
  const box = {top: 100};
  const nodes = tops.map((top, index) => ({
    tagName: index === 0 || index === 2 ? 'H2' : 'P',
    getBoundingClientRect: () => ({top: box.top + top - result.scrollTop, height: heights[index]})
  }));
  const result = {scrollTop, scrollHeight: 1200, clientHeight: 200,
    getBoundingClientRect: () => box, querySelectorAll: () => nodes};
  return result;
}
const english = content([0, 40, 180, 220, 310], [25, 100, 25, 60, 80], 250);
const position = captureGuidePosition(english);
assert.equal(position.heading, 1);
assert.equal(position.block, 1);
const german = content([0, 50, 240, 300, 400], [25, 160, 25, 80, 100], 0);
restoreGuidePosition(german, position);
assert.ok(german.scrollTop >= 300 && german.scrollTop < 400, german.scrollTop);
'''
    subprocess.run(["node", "-e", script], check=True, capture_output=True, text=True, timeout=10)


def test_guide_language_handlers_keep_selected_topic() -> None:
    """Full, shared and page-local guides use the selected file on language change."""
    pages = ["help.html", "v7_optimize.html", "v7_backtest.html", "v7_edit.html",
             "api_keys_editor.html", "dashboard_main.html", "services_monitor.html",
             "logging_monitor.html", "vps_monitor.html", "vps_manager.html",
             "market_data_main.html"]
    for name in pages:
        source = (ROOT / "frontend" / name).read_text()
        assert source.count("loadHelpIndex((helpTopics[helpSel] && helpTopics[helpSel].file)") == 2, name
    shared = (ROOT / "frontend/js/shared_help_overlay.js").read_text()
    assert "var keyword = selected && selected.file ? selected.file : state.currentKeyword;" in shared
