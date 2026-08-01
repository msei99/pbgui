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
