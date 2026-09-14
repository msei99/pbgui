/* Standalone validation drafts; does not enqueue or launch jobs itself. */
(function (root) {
  'use strict';
  root.PBGuiParetoValidation = {
    buildItems: function (detail, mode, name, groupId) {
      if (!['holdout_only', 'full_timerange', 'holdout_and_full_timerange', 'all_timeranges'].includes(mode)) throw new Error('Unknown validation mode.');
      if (!detail || !detail.full_config) throw new Error('Select a configuration first.');
      if (detail.override_error) throw new Error('Override configs unavailable: ' + detail.override_error);
      var config = detail.full_config, bt = config.backtest || {}, periods = [];
      var training = Array.isArray(bt.scenarios) ? bt.scenarios : [];
      var holdouts = Array.isArray(detail.validation_holdouts) ? detail.validation_holdouts : [];
      if (mode === 'all_timeranges') {
        if (!training.length) throw new Error('Training periods are unavailable.');
        periods.push.apply(periods, training);
      }
      if (mode !== 'full_timerange') {
        if (!holdouts.length) throw new Error('No saved Holdout plan is available. Choose Full timerange only.');
        periods.push.apply(periods, holdouts);
      }
      if (mode !== 'holdout_only') periods.push({label:'full_timerange', start_date:bt.start_date, end_date:bt.end_date});
      return periods.map(function (period, index) {
        if (!period.start_date || !period.end_date) throw new Error('Validation period dates are incomplete.');
        var label = String(period.label || ('period_' + (index + 1))).replace(/[^A-Za-z0-9_.-]/g, '_');
        var candidate = JSON.parse(JSON.stringify(config));
        candidate.backtest = Object.assign({}, candidate.backtest, {
          start_date:period.start_date, end_date:period.end_date, base_dir:'backtests/pbgui/' + name + '_' + label
        });
        ['suite_enabled','scenarios','reducer','aggregate'].forEach(function(key) { delete candidate.backtest[key]; });
        candidate.pbgui = candidate.pbgui || {};
        delete candidate.pbgui.scenario_template;
        candidate.pbgui.backtest_result_group = {schema_version:1, kind:'optimize_validate', id:groupId, label:name, item:label};
        var sections = {};
        ['backtest','bot','live','optimize','pbgui','coin_overrides'].forEach(function(key) {
          if (candidate[key] !== undefined) sections[key] = candidate[key];
        });
        return {name:name + '_' + label, config:sections, override_configs:detail.override_configs || {}, preserve_timerange:true, preserve_exchanges:true};
      });
    }
  };
}(window));
