/* Read-only PB8 -> native PB9 migration review for the shared Backtest and Optimize pages. */
;(function () {
  'use strict';
  var generation = 0, controller;
  var TARGETS = {
    backtest: {api: '/api/backtest-v9', label: 'backtest', hash: 'configs'},
    optimize: {api: '/api/optimize-v9', label: 'Optimize', hash: 'configs'}
  };

  async function request(target, path, body, signal) {
    var response = await fetch((window.BASE_PREFIX || '') + target.api + '/migrate-v8/' + path, {
      method: 'POST', credentials: 'same-origin', signal: signal,
      headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)
    });
    var data = await response.json();
    if (!response.ok) throw new Error(typeof data.detail === 'string' ? data.detail : 'Migration request failed');
    return data;
  }

  async function convert(selection, button, kind) {
    var target = TARGETS[kind || 'backtest'];
    if (!target) return;
    var mine = ++generation;
    if (controller) controller.abort();
    controller = new AbortController();
    var signal = controller.signal;
    if (button) button.disabled = true;
    window.toast('Preparing official PB8 → PB9 migration…', 'info');
    try {
      var preview = await request(target, 'preview', selection, signal);
      if (mine !== generation) return;
      if (preview.status !== 'ready') throw new Error(preview.message || 'The official converter could not migrate this config');
      var accepted = await window.PBGuiDialogs.confirm({
        title: 'Review PB8 → PB9 migration', wide: true, codeDetail: true,
        message: 'The official converter proposes the changes below, including inferred policy and metric choices (best-effort mode). Review all changes and warnings. Results may differ from PB8. This uses the saved PB8 source; unsaved edits are excluded. Open an unsaved PB9 ' + target.label + ' draft?',
        detail: JSON.stringify(preview.report, null, 2), confirmText: 'Open PB9 draft', cancelText: 'Cancel'
      });
      if (!accepted || mine !== generation) return;
      var draft = await request(target, 'accept', {review_id: preview.review_id}, signal);
      if (mine !== generation) return;
      var url = new URL((window.BASE_PREFIX || '') + target.api + '/main_page', window.location.origin);
      url.searchParams.set('opt_draft_id', draft.draft_id);
      url.searchParams.set('draft_name', draft.suggested_name || preview.suggested_name);
      url.hash = target.hash;
      window.location.assign(url.href);
    } catch (error) {
      if (mine === generation && error.name !== 'AbortError') window.toast('PB9 migration: ' + error.message, 'err');
    } finally {
      if (button && button.isConnected) button.disabled = false;
    }
  }

  document.addEventListener('click', function (event) {
    var optimizeButton = event.target.closest('[data-migrate-pb9-optimize]');
    if (optimizeButton) {
      event.stopPropagation();
      var kind = optimizeButton.dataset.migratePb9Optimize;
      var value = kind === 'pareto' ? optimizeButton.dataset.path : optimizeButton.dataset.configName;
      if ((kind === 'config' || kind === 'pareto') && value) {
        convert({source_type: kind, source: value}, optimizeButton, 'optimize');
      }
      return;
    }
    var button = event.target.closest('[data-migrate-pb9]');
    if (!button) return;
    event.stopPropagation();
    var source = button.dataset.configName;
    if (button.dataset.migratePb9 === 'editor') source = window.editingConfig;
    var selection = {source_type: 'config', source: source};
    if (button.dataset.migratePb9 === 'result') {
      var rows = (window._activeResultsCtx || {}).data || [];
      var result = rows[Number(button.dataset.resultIndex)];
      if (!result || result.backtest_version !== 'v8') return;
      selection = {source_type: 'result', source: result.path};
    }
    if (!selection.source || selection.source === '__new__') return;
    convert(selection, button, 'backtest');
  }, true);
  window.addEventListener('pagehide', function () {
    generation++;
    if (controller) controller.abort();
  });
})();
