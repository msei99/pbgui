"""Regression tests for VPS Manager frontend state handling."""

from __future__ import annotations

import subprocess
import textwrap
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
HTML_PATH = ROOT / "frontend" / "vps_manager.html"


def _extract_function(source: str, name: str) -> str:
    """Extract a named JavaScript function from the inline VPS manager script."""
    marker = f"function {name}("
    start = source.find(marker)
    assert start >= 0, f"Could not find JavaScript function {name!r}"

    brace_start = source.find("{", start)
    assert brace_start >= 0, f"Could not find opening brace for {name!r}"

    depth = 0
    quote: str | None = None
    escaped = False

    for index in range(brace_start, len(source)):
        char = source[index]
        if quote is not None:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue

        if char in ("'", '"', "`"):
            quote = char
            continue
        if char == "{":
            depth += 1
            continue
        if char == "}":
            depth -= 1
            if depth == 0:
                return source[start : index + 1]

    raise AssertionError(f"Could not extract complete JavaScript function {name!r}")


def _run_node_assertions(function_names: list[str], *, bootstrap: str, assertions: str) -> None:
    """Run focused Node assertions against extracted frontend helpers."""
    source = HTML_PATH.read_text(encoding="utf-8")
    extracted = "\n\n".join(_extract_function(source, name) for name in function_names)
    script = textwrap.dedent(
        f"""
        const assert = require('node:assert/strict');

        {bootstrap}

        {extracted}

        {assertions}
        """
    )
    result = subprocess.run(
        ["node", "-e", script],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (
        "Node-backed frontend regression failed\n"
        f"STDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )


class TestVpsManagerFrontendLogic:
    """Lock down VPS Manager form behavior against live metadata refreshes."""

    def test_dirty_optional_fields_survive_live_config_refresh(self) -> None:
        """Cleared optional fields must stay dirty while remote metadata is stale."""
        bootstrap = """
        const store = {
          view: 'vps-setup',
          hostname: 'manibot70',
          vps: {},
          detail: {
            kind: 'vps',
            hostname: 'manibot70',
            config: {
              bucket: 'pbguimani:',
              coinmarketcap_api_key: 'cmc-test-api-key',
              install_dir: '/home/mani/software',
              swap: '4G'
            },
            log_preview: {},
            branches: {}
          }
        };
        function refreshLocalInteractiveState() {}
        function renderSidebarActions() {}
        function renderStatusFlags() { return ''; }
        function setHtmlIfChanged() {}
        """
        assertions = """
        var ui = ensureVpsUi('manibot70', store.detail);
        assert.equal(canSaveForm(ui.form, ui.savedForm), false);

        setVpsField('manibot70', 'bucket', '');
        setVpsField('manibot70', 'coinmarketcap_api_key', '');
        assert.equal(ui.form.bucket, '');
        assert.equal(ui.form.coinmarketcap_api_key, '');
        assert.equal(ui.dirtyFields.bucket, true);
        assert.equal(ui.dirtyFields.coinmarketcap_api_key, true);
        assert.equal(canSaveForm(ui.form, ui.savedForm), true);

        store.detail = Object.assign({}, store.detail, {
          config: {
            bucket: 'pbguimani:',
            coinmarketcap_api_key: 'cmc-test-api-key',
            install_dir: '/opt/pbgui',
            swap: '8G'
          }
        });
        ensureVpsUi('manibot70', store.detail);

        assert.equal(ui.form.bucket, '');
        assert.equal(ui.form.coinmarketcap_api_key, '');
        assert.equal(ui.savedForm.bucket, 'pbguimani:');
        assert.equal(ui.savedForm.coinmarketcap_api_key, 'cmc-test-api-key');
        assert.equal(ui.form.install_dir, '/opt/pbgui');
        assert.equal(ui.form.swap, '8G');
        assert.equal(canSaveForm(ui.form, ui.savedForm), true);
        """
        _run_node_assertions(
            ["defaultVpsUi", "markVpsFieldDirtyState", "syncCurrentVpsFormFromDom", "ensureVpsUi", "setVpsField", "canSaveForm"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_dom_setup_field_survives_focus_triggered_refresh(self) -> None:
        """A focus-triggered refresh must not restore a cleared bucket from stale detail."""
        bootstrap = """
        var domFields = [];
        const store = {
          view: 'vps-setup',
          hostname: 'manibot70',
          vps: {},
          detail: {
            kind: 'vps',
            hostname: 'manibot70',
            config: {
              bucket: 'pbguimani:',
              coinmarketcap_api_key: 'cmc-test-api-key',
              install_dir: '/home/mani/software',
              swap: '4G'
            },
            log_preview: {},
            branches: {}
          }
        };
        global.document = {
          querySelectorAll: function(selector) {
            if (selector === '[data-vps-field]') return domFields;
            return [];
          }
        };
        function formField(name, value, host) {
          return {
            type: 'text',
            value: value,
            checked: false,
            getAttribute: function(attr) {
              if (attr === 'data-vps-field') return name;
              if (attr === 'data-vps-host') return host;
              return '';
            }
          };
        }
        """
        assertions = """
        var ui = ensureVpsUi('manibot70', store.detail);
        assert.equal(ui.form.bucket, 'pbguimani:');
        assert.equal(canSaveForm(ui.form, ui.savedForm), false);

        domFields = [
          formField('bucket', '', 'manibot70'),
          formField('coinmarketcap_api_key', 'cmc-test-api-key', 'manibot70')
        ];
        store.detail = Object.assign({}, store.detail, {
          config: {
            bucket: 'pbguimani:',
            coinmarketcap_api_key: 'cmc-test-api-key',
            install_dir: '/home/mani/software',
            swap: '4G'
          }
        });
        ensureVpsUi('manibot70', store.detail);

        assert.equal(ui.form.bucket, '');
        assert.equal(ui.dirtyFields.bucket, true);
        assert.equal(ui.savedForm.bucket, 'pbguimani:');
        assert.equal(canSaveForm(ui.form, ui.savedForm), true);
        """
        _run_node_assertions(
            ["defaultVpsUi", "markVpsFieldDirtyState", "syncCurrentVpsFormFromDom", "ensureVpsUi", "canSaveForm"],
            bootstrap=bootstrap,
            assertions=assertions,
        )
