"""Executable contracts for the shared PB7/PB8 Coin Overrides editor."""

from pathlib import Path
import subprocess
import textwrap


ROOT = Path(__file__).resolve().parents[2]


def _run_node(script: str) -> None:
    """Run one isolated Node contract and surface assertion output."""

    completed = subprocess.run(["node", "-e", script], cwd=ROOT, text=True, capture_output=True, check=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout


def test_async_v7_parameter_metadata_rerenders_open_editor_and_reports_failures() -> None:
    """Late PB7 metadata must populate an already open editor instead of leaving empty sections."""

    script = textwrap.dedent(
        """
        const assert = require('node:assert/strict');
        const fs = require('node:fs');
        function esc(value) {
          return String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
        }
        eval(fs.readFileSync('frontend/js/coin_overrides_editor.js', 'utf8'));
        let renders = 0;
        _covRender = function() { renders += 1; };
        _covState.overrides = {DOGE: {}};
        _covState.overrideConfigs = {};
        _covState.editCoin = 'DOGE';

        let resolveRequest;
        _covState.request = function() {
          return new Promise(function(resolve) { resolveRequest = resolve; });
        };
        const pending = _fetchAllowedParams();
        assert.match(_covEditHtml('DOGE'), /Loading override parameters/);
        resolveRequest({params: {
          bot: {
            long: {entry_initial_qty_pct: true},
            short: {entry_initial_qty_pct: true}
          },
          live: {leverage: true}
        }});

        pending.then(function(params) {
          assert.equal(renders, 1);
          assert.equal(params.bot.long.entry_initial_qty_pct, true);
          const html = _covEditHtml('DOGE');
          assert.match(html, /cov-ps-bot-long-input/);
          assert.match(html, /cov-ps-bot-short-input/);
          assert.match(html, /cov-ps-live-input/);
          assert.equal(_covAllowedParamCount(params), 3);
          assert.equal(_covAllowedParamCount({bot: {long: {'risk.n_positions': {type: 'number', default: 5}}}}), 1);

          _covState.request = function() { return Promise.reject(new Error('PB7 metadata failed')); };
          return _fetchAllowedParams();
        }).then(function(params) {
          assert.deepEqual(params, {});
          assert.equal(renders, 2);
          assert.match(_covEditHtml('DOGE'), /Override parameters unavailable: PB7 metadata failed/);
        }).catch(function(error) {
          console.error(error);
          process.exitCode = 1;
        });
        """
    )
    _run_node(script)


def test_pb8_context_suppresses_stale_metadata_and_preserves_override_file_content() -> None:
    """PB8 policy refreshes must be ordered and full override documents lossless."""

    script = textwrap.dedent(
        r"""
        const assert = require('node:assert/strict');
        const fs = require('node:fs');
        function esc(value) { return String(value); }
        function toast() {}
        eval(fs.readFileSync('frontend/js/coin_overrides_editor.js', 'utf8'));
        _covRender = function() {};
        _covNotifyStructuredSync = function() {};
        const requests = [];
        _covState.request = function(path) {
          return new Promise(function(resolve) { requests.push({path, resolve}); });
        };
        _covState.contextAware = true;
        _covState.context = {hslSignalMode: 'coin', strategyKind: 'trailing_martingale'};
        const oldRequest = _fetchAllowedParams();
        const newRequest = coinOvSetContext({hslSignalMode: 'pside', strategyKind: 'ema_anchor'});
        assert.match(requests[0].path, /hsl_signal_mode=coin/);
        assert.match(requests[1].path, /strategy_kind=ema_anchor/);
        requests[1].resolve({params: {bot: {long: {'strategy.ema_anchor.ema_dist_entry': {type: 'number', default: 0}}, short: {}}, live: {}}});
        requests[0].resolve({params: {bot: {long: {'hsl.enabled': {type: 'boolean', default: false}}, short: {}}, live: {}}});
        Promise.all([oldRequest, newRequest]).then(function() {
          assert.equal(_covState.allowedParams.bot.long['hsl.enabled'], undefined);
          assert.ok(_covState.allowedParams.bot.long['strategy.ema_anchor.ema_dist_entry']);
          assert.deepEqual(_covUnsupportedInlineParams({bot: {long: {hsl: {enabled: false}}}}, _covState.allowedParams), ['bot.long.hsl.enabled']);

          _covState.deferConfigFileWrites = true;
          _covState.overrides = {BTC: {override_config_path: 'BTC.json'}};
          _covState.overrideConfigs = {BTC: {live: {leverage: 3}, logging: {level: 2}, bot: {long: {risk: {entry_cooldown_minutes: 4}}}}};
          _covValidateCfgJsonField = function(side) {
            return {parsed: side === 'long' ? {risk: {entry_cooldown_minutes: 8}} : {}, error: null};
          };
          global.document = {getElementById: function(id) {
            return {value: id === 'cov-cfg-long' ? '{"risk":{"entry_cooldown_minutes":8}}' : '{}'};
          }};
          assert.equal(_covSaveConfigFile('BTC'), true);
          assert.equal(_covState.pendingConfigFileWrites.BTC.config.live.leverage, 3);
          assert.equal(_covState.pendingConfigFileWrites.BTC.config.logging.level, 2);
          assert.equal(_covState.pendingConfigFileWrites.BTC.config.bot.long.risk.entry_cooldown_minutes, 8);
        }).catch(function(error) {
          console.error(error);
          process.exitCode = 1;
        });
        """
    )
    _run_node(script)
