"""Execute real backtest modal renderers with hostile stored configuration values."""

import json
import re
import subprocess
from html.parser import HTMLParser
from pathlib import Path

import pytest


class ModalParser(HTMLParser):
    """Collect parsed tags and attributes to detect markup injection, not source patterns."""

    def __init__(self):
        """Initialize the tag stream."""
        super().__init__(convert_charrefs=True)
        self.tags = []

    def handle_starttag(self, tag, attrs):
        """Retain every parsed element and its attributes."""
        self.tags.append((tag, dict(attrs)))


@pytest.mark.parametrize("name", ["showInitialBacktestQueueDraftModal", "rebacktestSelected", "rebacktestSelectedArchive", "retestReplaceSelectedArchive", "rebacktestSelectedLegacy"])
@pytest.mark.parametrize("balance,expected", [
    ('"><img src=x onerror=alert(1)>', "1000"),
    ('\" autofocus onfocus=alert(1) x=\"', "1000"),
    ("Infinity", "1000"), ("NaN", "1000"), ("1e999", "1000"),
    (2500, "2500"), ("1234.5", "1234.5"),
])
def test_stored_balances_and_dates_cannot_create_modal_markup(name, balance, expected):
    """Real modal functions produce only expected elements for hostile values in attributes."""
    root = Path(__file__).resolve().parents[1]
    source = (root / "frontend" / "v7_backtest.html").read_text(encoding="utf-8")
    names = [name, "backtestDialogDateInputHtml", "esc", "escAttr", "normalizeArchiveMarketDataPath",
             "archiveConfigUsesPbguiMarketData", "archiveRetestDefaultDays"]
    functions = "\n".join(re.search(r"^function " + function + r"\(.*?^}", source, re.M | re.S).group() for function in names)
    cfg = {"backtest": {"starting_balance": balance, "exchanges": ["bybit", '"><svg onload=alert(1)>'],
                        "start_date": '"><img src=x onerror=alert(1)>', "end_date": '" autofocus onfocus=alert(1) x="'}}
    script = r"""
const cfg = CONFIG;
let body = null;
const errors = [];
const document = {createElement() { return {
  textContent: '',
  get innerHTML() { return this.textContent.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }
}; }};
function getSelectedResults() { return ['a', 'b']; }
function getSelectedArchiveResults() { return ['a', 'b']; }
function getSelectedLegacyResults() { return ['a', 'b']; }
function selectedArchiveIsOwn() { return true; }
function apiFetch() { return Promise.resolve(cfg); }
function archiveResultApiFetch() { return Promise.resolve(cfg); }
function pbguiMarketDataDefaultCheckedAttr() { return ''; }
function updateArchiveRetestFields() {}
function toast(message) { errors.push(message); }
function showModal(title, html) { body = html; }
FUNCTIONS
CALL
setImmediate(() => {
  if (errors.length || !body) throw new Error(JSON.stringify(errors));
  process.stdout.write(JSON.stringify(body));
});
""".replace("CONFIG", json.dumps(cfg)).replace("FUNCTIONS", functions).replace(
        "CALL", f"{name}([{{name:'test',config:cfg}}]);" if name == "showInitialBacktestQueueDraftModal" else f"{name}();"
    )
    result = subprocess.run(["node", "-e", script], cwd=root, text=True, capture_output=True, timeout=10, check=False)
    assert result.returncode == 0, result.stderr
    parser = ModalParser()
    parser.feed(json.loads(result.stdout))
    assert {tag for tag, _attrs in parser.tags} <= {"div", "input", "button", "select", "option", "label", "p", "hr"}
    for _tag, attrs in parser.tags:
        assert not {"autofocus", "onfocus", "onerror", "onload"}.intersection(attrs)
    balances = [attrs for tag, attrs in parser.tags if attrs.get("id") in {"rbt-balance", "arr-balance"}]
    assert len(balances) == 1
    assert balances[0]["value"] == expected
