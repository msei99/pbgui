"""Executable shared tooltip resolution and safe text-rendering contracts."""

from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[2]


def test_original_help_resolves_editor_fields_without_cross_scope_guesses() -> None:
    """Native fields share original help while PB7 stays inactive and ambiguous leaves stay unresolved."""
    source = (ROOT / "frontend/js/pb8_parameter_help.js").read_text()
    script = "const assert = require('node:assert/strict'); const window = {}; const API_BASE = '/api/v7';\n" + source + r"""
      const help = window.PBGuiPB8ParameterHelp;
      const entries = {
        'optimize.enable_overrides.couple_unstuck_ema_spans': {text: 'Original coupled EMA search.'},
        'bot.*.unstuck.ema_span_0': {text: 'Original unstuck spans.'},
        'bot.*.strategy.trailing_martingale.entry.ema_span_0': {text: 'Original entry spans.'},
        'optimize.pymoo.shared.mutation_prob': {text: 'Per individual.'},
        'optimize.pymoo.shared.mutation_prob_per_variable': {text: 'Per variable.'},
        'backtest.start_date': {text: 'Original start date.'},
        'live.risk_input_max_attempts': {text: 'Original recovery limit.'},
        'monitor.enabled': {text: 'Original monitoring toggle.'},
        'live.enabled': {text: 'Unrelated setting.'},
        'optimize.gpu.population_size': {text: 'GPU population.'},
        'optimize.population_size': {text: 'CPU population.'}
      };
      assert.equal(help.resolveHelp(entries, {label: 'couple_unstuck_ema_spans', path: 'optimize.enable_overrides.couple_unstuck_ema_spans'}, 'optimize').text, 'Original coupled EMA search.');
      assert.equal(help.resolveHelp(entries, {label: 'long.unstuck.ema_span_0'}, 'optimize').text, 'Original unstuck spans.');
      assert.equal(help.resolveHelp(entries, {label: 'short.strategy.trailing_martingale.entry.ema_span_0'}, 'optimize').text, 'Original entry spans.');
      assert.equal(help.resolveHelp(entries, {label: 'ema_span_0'}, 'optimize'), null);
      assert.equal(help.resolveHelp(entries, {label: 'enabled', id: 'f-monitor-enabled'}, 'run').text, 'Original monitoring toggle.');
      assert.equal(help.resolveHelp(entries, {label: 'risk_input_max_attempts'}, 'run').text, 'Original recovery limit.');
      assert.equal(help.resolveHelp(entries, {label: 'population_size', id: 'opted-gpu-population-size'}, 'optimize').text, 'GPU population.');
      assert.equal(help.resolveHelp(entries, {label: 'mutation_prob_mode', id: 'opted-mutation_prob-mode'}, 'optimize').text, 'Per individual.');
      assert.equal(help.resolveHelp(entries, {label: 'mutation_prob_per_variable_mode', id: 'opted-mutation_prob_per_variable-mode'}, 'optimize').text, 'Per variable.');
      assert.equal(help.resolveHelp(entries, {label: 'start_date'}, 'backtest').text, 'Original start date.');
      assert.equal(help.resolveHelp(entries, {label: 'PBGui filter'}, 'run'), null);
      assert.equal(help.plainText('Original **wording** with `code` and [reference](local.md).'), 'Original wording with code and reference.');
      assert.equal(help.plainText('<img src=x onerror=alert(1)>'), '<img src=x onerror=alert(1)>');
    """
    result = subprocess.run(["node", "-e", script], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert "tooltip.textContent =" in source
    assert "innerHTML" not in source


def test_all_three_editors_load_the_local_shared_help_assets() -> None:
    """All productive PB8 editors use one cached asset version and original-text provider."""
    for page in ("v7_optimize.html", "v7_backtest.html", "v7_edit.html"):
        source = (ROOT / "frontend" / page).read_text()
        assert '<script defer src="/app/js/pb8_parameter_help.js?v=1"></script>' in source
        assert 'href="/app/css/pb8_parameter_help.css?v=1"' in source


def test_hover_uses_original_text_and_cleans_up_handlers_and_timers() -> None:
    """Exercise the actual hover listener, scroll handoff, stale-target guard, and page teardown."""
    source = (ROOT / "frontend/js/pb8_parameter_help.js").read_text()
    script = r"""
      const assert = require('node:assert/strict');
      const listeners = new Map();
      const windowListeners = new Map();
      const timers = new Map();
      let nextTimer = 0;
      const setTimeout = (fn, delay) => {const id = ++nextTimer; timers.set(id, {fn, delay}); return id;};
      const clearTimeout = id => timers.delete(id);
      const window = {innerWidth: 1200, innerHeight: 800,
        addEventListener: (type, fn) => windowListeners.set(type, fn)};
      const API_BASE = '/api/optimize-v8';
      const original = 'This opt-in setting restores coupled EMA search. <img src=x onerror=alert(1)>';
      let requested;
      const fetch = async (url, options) => {
        requested = {url, options};
        return {ok: true, json: async () => ({entries: {
          'optimize.enable_overrides.couple_unstuck_ema_spans': {text: original, source: 'optimizing.md'}
        }})};
      };
      const tooltipEvents = new Map();
      const tip = {style: {}, hidden: true, offsetWidth: 400, offsetHeight: 200,
        setAttribute() {}, contains(node) {return node === this;}, remove() {this.removed = true;},
        addEventListener: (type, fn) => tooltipEvents.set(type, fn)};
      const legacy = {style: {display: 'block'}};
      const document = {
        body: {appendChild() {}}, createElement: () => tip,
        getElementById: id => id === 'data-tip-tooltip' ? legacy : null,
        addEventListener: (type, fn) => listeners.set(type, fn),
        removeEventListener: (type, fn) => {if (listeners.get(type) === fn) listeners.delete(type);}
      };
      const control = {id: 'opted-enable-override-couple_unstuck_ema_spans',
        getAttribute: key => key === 'data-pb8-enable-override' ? 'couple_unstuck_ema_spans' : null};
      const group = {querySelector: () => control};
      const attrs = {'data-tip': 'PB8 optimizer override reported by the installed runtime.'};
      const target = {textContent: 'couple_unstuck_ema_spans', isConnected: true,
        closest: selector => selector.includes('.form-group') ? group : target,
        getAttribute: key => attrs[key], setAttribute: (key, value) => attrs[key] = value,
        removeAttribute: key => delete attrs[key], contains: node => node === target,
        getBoundingClientRect: () => ({left: 50, bottom: 70})};
      const event = {type: 'mouseover', target, clientX: 1000, clientY: 700,
        stopImmediatePropagation() {this.stopped = true;}};
    """ + source + r"""
      (async () => {
        for (let i = 0; i < 8; i++) await Promise.resolve();
        assert.equal(requested.url, '/api/v8/parameter-help');
        assert.equal(requested.options.credentials, 'same-origin');
        listeners.get('mouseover')(event);
        assert.equal(attrs['data-tip'], original);
        assert.equal(tip.textContent, original + '\n\nPassivbot · docs/optimizing.md'.replaceAll('\\n', '\n'));
        assert.equal(tip.hidden, false);
        assert.equal(event.stopped, true);
        assert.equal(legacy.style.display, 'none');
        assert.ok(parseFloat(tip.style.left) + tip.offsetWidth <= window.innerWidth);
        listeners.get('mouseout')({relatedTarget: tip});
        assert.equal([...timers.values()].filter(timer => timer.delay === 150).length, 0);
        listeners.get('mouseout')({relatedTarget: null});
        tooltipEvents.get('mouseenter')();
        assert.equal([...timers.values()].filter(timer => timer.delay === 150).length, 0);
        target.isConnected = false;
        listeners.get('mousemove')(event);
        assert.equal(tip.hidden, true);
        windowListeners.get('pagehide')({persisted: false});
        assert.equal(requested.options.signal.aborted, true);
        assert.equal(tip.removed, true);
        assert.equal(listeners.size, 0);
        assert.equal(timers.size, 0);
      })().catch(error => {console.error(error); process.exitCode = 1;});
    """
    result = subprocess.run(["node", "-e", script], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
