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

    def test_read_vps_settings_prompts_when_password_missing(self) -> None:
        """Read VPS settings asks for a password before sending without one."""
        bootstrap = """
        var sent = [];
        var prompts = [];
        var renders = [];
        var domUserPw = '';
        const ui = { form: { user_pw: '' }, readSettingsLoading: false };
        const store = {
          detail: { kind: 'vps', hostname: 'manibot90' },
          suppressVpsDomSyncHost: ''
        };
        global.setTimeout = function(fn, delay) {
          fn();
          return 1;
        };
        global.document = {
          querySelectorAll: function(selector) {
            if (selector !== '[data-vps-field="user_pw"]') return [];
            return [{
              get value() { return domUserPw; },
              set value(next) { domUserPw = String(next || ''); },
              getAttribute: function(attr) {
                if (attr === 'data-vps-host') return 'manibot90';
                return '';
              }
            }];
          }
        };
        function ensureVpsUi(hostname, detail) {
          if (store.suppressVpsDomSyncHost !== hostname) {
            ui.form.user_pw = domUserPw;
          }
          return ui;
        }
        function getEffectiveVpsUserPw(hostname) {
          return ensureVpsUi(hostname, store.detail).form.user_pw;
        }
        function hasSessionSecret(hostname, field) {
          return false;
        }
        function openPasswordPrompt(hostname, onConfirm, options) {
          prompts.push({ hostname: hostname, options: options });
          onConfirm('fresh-password');
        }
        function buildSecretAwareForm(hostname, form) {
          return { user_pw: form.user_pw || '' };
        }
        function renderUi(options) {
          renders.push({ loading: ui.readSettingsLoading, options: options || {} });
        }
        function send(payload) {
          sent.push(payload);
        }
        """
        assertions = """
        refreshVpsSettings('manibot90');

        assert.equal(prompts.length, 1);
        assert.equal(prompts[0].hostname, 'manibot90');
        assert.equal(prompts[0].options.confirmLabel, 'Read Settings');
        assert.equal(sent.length, 1);
        assert.equal(sent[0].cmd, 'read_vps_settings');
        assert.equal(sent[0].hostname, 'manibot90');
        assert.equal(sent[0].form.user_pw, 'fresh-password');
        assert.equal(domUserPw, 'fresh-password');
        assert.equal(ui.readSettingsLoading, true);
        assert.equal(renders.length, 1);
        assert.equal(renders[0].loading, true);
        """
        _run_node_assertions(
            ["setPromptedVpsUserPassword", "refreshVpsSettings"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_save_vps_config_prompts_when_firewall_password_missing(self) -> None:
        """Saving firewall changes asks for a password before sending."""
        bootstrap = """
        var sent = [];
        var prompts = [];
        var passwordAvailable = false;
        const ui = {
          form: {
            user_pw: '',
            firewall: true,
            firewall_ssh_port: '22',
            firewall_ssh_ips: '198.51.100.1'
          },
          savedForm: {
            user_pw: '',
            firewall: false,
            firewall_ssh_port: 22,
            firewall_ssh_ips: ''
          }
        };
        const store = {
          detail: { kind: 'vps', hostname: 'manibot90' }
        };
        function ensureVpsUi(hostname, detail) {
          if (passwordAvailable) ui.form.user_pw = 'fresh-password';
          return ui;
        }
        function getEffectiveVpsUserPw(hostname) {
          return passwordAvailable ? 'fresh-password' : '';
        }
        function hasSessionSecret(hostname, field) {
          return false;
        }
        function openPasswordPrompt(hostname, onConfirm, options) {
          prompts.push({ hostname: hostname, options: options });
          passwordAvailable = true;
          onConfirm('fresh-password');
        }
        function buildSecretAwareForm(hostname, form) {
          return Object.assign({}, form);
        }
        function send(payload) {
          sent.push(payload);
        }
        """
        assertions = """
        saveVpsConfig('manibot90');

        assert.equal(prompts.length, 1);
        assert.equal(prompts[0].hostname, 'manibot90');
        assert.equal(prompts[0].options.confirmLabel, 'Apply Changes');
        assert.equal(sent.length, 1);
        assert.equal(sent[0].cmd, 'save_vps_config');
        assert.equal(sent[0].hostname, 'manibot90');
        assert.equal(sent[0].form.user_pw, 'fresh-password');
        assert.equal(sent[0].form.firewall, true);
        assert.equal(sent[0].form.firewall_ssh_ips, '198.51.100.1');
        """
        _run_node_assertions(
            ["vpsFirewallSettingsChanged", "saveVpsConfig"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_save_vps_config_prompts_when_swap_password_missing(self) -> None:
        """Saving swap changes asks for a password before sending."""
        bootstrap = """
        var sent = [];
        var prompts = [];
        var passwordAvailable = false;
        const ui = {
          form: { user_pw: '', swap: '4G', firewall: false, firewall_ssh_port: 22, firewall_ssh_ips: '' },
          savedForm: { user_pw: '', swap: '2G', firewall: false, firewall_ssh_port: 22, firewall_ssh_ips: '' }
        };
        const store = { detail: { kind: 'vps', hostname: 'manibot90' } };
        function ensureVpsUi(hostname, detail) {
          if (passwordAvailable) ui.form.user_pw = 'fresh-password';
          return ui;
        }
        function getEffectiveVpsUserPw(hostname) {
          return passwordAvailable ? 'fresh-password' : '';
        }
        function hasSessionSecret(hostname, field) { return false; }
        function openPasswordPrompt(hostname, onConfirm, options) {
          prompts.push({ hostname: hostname, options: options });
          passwordAvailable = true;
          onConfirm('fresh-password');
        }
        function buildSecretAwareForm(hostname, form) { return Object.assign({}, form); }
        function send(payload) { sent.push(payload); }
        """
        assertions = """
        saveVpsConfig('manibot90');

        assert.equal(prompts.length, 1);
        assert.equal(prompts[0].options.confirmLabel, 'Apply Changes');
        assert.equal(sent.length, 1);
        assert.equal(sent[0].cmd, 'save_vps_config');
        assert.equal(sent[0].form.user_pw, 'fresh-password');
        assert.equal(sent[0].form.swap, '4G');
        """
        _run_node_assertions(
            ["vpsFirewallSettingsChanged", "vpsSwapSettingsChanged", "saveVpsConfig"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_read_vps_settings_progress_updates_steps(self) -> None:
        """Read VPS settings progress events update and render ordered steps."""
        bootstrap = """
        var renders = [];
        const store = {
          view: 'vps-setup',
          hostname: 'manibot90',
          vps: {},
          detail: { kind: 'vps', hostname: 'manibot90' }
        };
        function syncCurrentVpsFormFromDom(hostname, ui) { return false; }
        function isVpsContextView() { return true; }
        function renderUi(options) { renders.push(options || {}); }
        function esc(value) { return String(value || ''); }
        function escAttr(value) { return String(value || ''); }
        """
        assertions = """
        updateReadSettingsProgress('manibot90', { step: 'ssh', label: 'Connecting to VPS', status: 'running' });
        updateReadSettingsProgress('manibot90', { step: 'firewall', label: 'Reading UFW firewall settings', status: 'running' });
        updateReadSettingsProgress('manibot90', { step: 'done', label: 'VPS settings refreshed', status: 'done' });

        const ui = store.vps.manibot90;
        assert.equal(ui.readSettingsLoading, false);
        assert.equal(ui.readSettingsProgress.current, 'done');
        assert.equal(ui.readSettingsProgress.steps.length, 3);
        assert.equal(ui.readSettingsProgress.steps[0].status, 'done');
        assert.equal(ui.readSettingsProgress.steps[1].status, 'done');
        assert.equal(ui.readSettingsProgress.steps[2].status, 'done');
        const html = renderReadSettingsProgress(ui);
        assert.match(html, /Read VPS settings completed/);
        assert.match(html, /read-settings-progress/);
        assert.match(html, /deploy-progress-bar/);
        assert.match(html, /read-settings-task/);
        assert.match(html, /100%/);
        assert.match(html, /Connecting to VPS/);
        assert.match(html, /Reading UFW firewall settings/);
        assert.match(html, /Password/);
        assert.doesNotMatch(html, /Open/);
        assert.equal(renders.length, 3);
        """
        _run_node_assertions(
            ["defaultVpsUi", "ensureVpsUi", "updateReadSettingsProgress", "renderReadSettingsProgress"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_read_vps_settings_updates_detail_before_render(self) -> None:
        """Freshly read VPS settings must survive the forced setup-view render."""
        bootstrap = """
        var renders = [];
        var toasts = [];
        var domFields = [];
        const store = {
          view: 'vps-setup',
          hostname: 'manibot90',
          suppressVpsDomSyncHost: '',
          vps: {
            manibot90: {
              initialized: true,
              readSettingsLoading: true,
              form: {
                user_pw: 'session-password',
                firewall: false,
                firewall_ssh_port: 22,
                firewall_ssh_ips: ''
              },
              savedForm: {
                user_pw: 'session-password',
                firewall: false,
                firewall_ssh_port: 22,
                firewall_ssh_ips: ''
              },
              dirtyFields: {}
            }
          },
          detail: {
            kind: 'vps',
            hostname: 'manibot90',
            config: {
              firewall: false,
              firewall_ssh_port: 22,
              firewall_ssh_ips: ''
            }
          }
        };
        global.document = {
          querySelectorAll: function(selector) {
            if (selector === '[data-vps-field]') return domFields;
            return [];
          }
        };
        function formField(name, value, host, type, checked) {
          return {
            type: type || 'text',
            value: value,
            checked: !!checked,
            getAttribute: function(attr) {
              if (attr === 'data-vps-field') return name;
              if (attr === 'data-vps-host') return host;
              return '';
            }
          };
        }
        function isVpsContextView() { return true; }
        function renderUi(options) {
          ensureVpsUi(store.hostname, store.detail);
          renders.push({
            options: options || {},
            firewall: store.vps.manibot90.form.firewall,
            firewall_ssh_port: store.vps.manibot90.form.firewall_ssh_port,
            firewall_ssh_ips: store.vps.manibot90.form.firewall_ssh_ips
          });
        }
        function toast(message, kind) { toasts.push({ message: message, kind: kind }); }
        """
        assertions = """
        domFields = [
          formField('firewall', '', 'manibot90', 'checkbox', false),
          formField('firewall_ssh_port', '22', 'manibot90'),
          formField('firewall_ssh_ips', '', 'manibot90')
        ];
        handleResult({
          cmd: 'read_vps_settings',
          data: {
            firewall: true,
            firewall_ssh_port: 22,
            firewall_ssh_ips: '198.51.100.1, 203.0.113.7'
          }
        });

        assert.equal(store.detail.config.firewall, true);
        assert.equal(store.detail.config.firewall_ssh_port, 22);
        assert.equal(store.detail.config.firewall_ssh_ips, '198.51.100.1, 203.0.113.7');
        assert.equal(store.vps.manibot90.form.firewall, true);
        assert.equal(store.vps.manibot90.form.firewall_ssh_ips, '198.51.100.1, 203.0.113.7');
        assert.equal(store.vps.manibot90.form.user_pw, 'session-password');
        assert.equal(store.vps.manibot90.readSettingsLoading, false);
        assert.equal(store.vps.manibot90.savedForm.firewall_ssh_ips, '198.51.100.1, 203.0.113.7');
        assert.deepEqual(store.vps.manibot90.dirtyFields, {});
        assert.equal(renders.length, 1);
        assert.equal(renders[renders.length - 1].firewall_ssh_ips, '198.51.100.1, 203.0.113.7');
        assert.equal(store.suppressVpsDomSyncHost, '');
        assert.equal(toasts[0].message, 'VPS settings refreshed.');
        """
        _run_node_assertions(
            ["defaultVpsUi", "markVpsFieldDirtyState", "syncCurrentVpsFormFromDom", "ensureVpsUi", "handleResult"],
            bootstrap=bootstrap,
            assertions=assertions,
        )

    def test_active_vps_setup_field_holds_live_view_render(self) -> None:
        """Focused setup inputs should not be replaced by automatic live renders."""
        bootstrap = """
        const store = { view: 'vps-setup', hostname: 'manibot90' };
        function activeField(name, host) {
          return {
            getAttribute: function(attr) {
              if (attr === 'data-vps-field') return name;
              if (attr === 'data-vps-host') return host;
              return '';
            }
          };
        }
        global.document = { activeElement: null };
        """
        assertions = """
        document.activeElement = activeField('firewall_ssh_ips', 'manibot90');
        assert.equal(isActiveVpsSetupFormField(), true);

        document.activeElement = activeField('firewall_ssh_ips', 'manibot91');
        assert.equal(isActiveVpsSetupFormField(), false);

        store.view = 'vps';
        document.activeElement = activeField('firewall_ssh_ips', 'manibot90');
        assert.equal(isActiveVpsSetupFormField(), false);
        """
        _run_node_assertions(
            ["isActiveVpsSetupFormField"],
            bootstrap=bootstrap,
            assertions=assertions,
        )
