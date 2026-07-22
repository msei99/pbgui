;(function () {
  'use strict';

  function create(version) {
    var isV8 = String(version || '').toLowerCase() === 'v8';

    function sideRisk(sideConfig) {
      sideConfig = sideConfig && typeof sideConfig === 'object' ? sideConfig : {};
      if (!isV8) return sideConfig;
      if (!sideConfig.risk || typeof sideConfig.risk !== 'object') sideConfig.risk = {};
      return sideConfig.risk;
    }

    return {
      version: isV8 ? 'v8' : 'v7',
      isV8: isV8,
      getSideValue: function (sideConfig, key, fallback) {
        var value = sideRisk(sideConfig)[key];
        return value === undefined || value === null ? fallback : value;
      },
      setSideValue: function (sideConfig, key, value) {
        sideRisk(sideConfig)[key] = value;
      },
      getHslValue: function (sideConfig, key, fallback) {
        sideConfig = sideConfig && typeof sideConfig === 'object' ? sideConfig : {};
        var source = isV8 && sideConfig.hsl && typeof sideConfig.hsl === 'object' ? sideConfig.hsl : sideConfig;
        var sourceKey = isV8 ? key : 'hsl_' + key;
        var value = source[sourceKey];
        return value === undefined || value === null ? fallback : value;
      },
      metadataApiBase: function (apiBase) {
        if (!isV8) return String(apiBase || '').replace('/backtest-v7', '/v7');
        var match = String(apiBase || '').match(/^(https?:\/\/[^/]+)/);
        return (match ? match[1] : window.location.origin) + '/api/v7';
      },
      docsApiBase: function (apiBase) {
        return String(apiBase || '').replace(/\/backtest-v[78]$/, '');
      },
      archiveApiBase: function (apiBase) {
        return String(apiBase || '').replace(/\/backtest-v[78]$/, '/backtest-v7');
      },
      websocketPath: isV8 ? '/api/backtest-v8/ws/bt7' : '/api/backtest-v7/ws/bt7',
      queueLogFile: function (filename) {
        return (isV8 ? 'backtests_v8/' : 'backtests/') + filename + '.log';
      },
      navItems: function () {
        var items = [
          { panel: 'configs', icon: '📋', label: 'Configs' },
          { panel: 'queue', icon: '⏳', label: 'Queue', badge: true },
          { panel: 'results', icon: '📊', label: 'Results' }
        ];
        items.push({ panel: 'archive', icon: '🗄️', label: 'Archive' });
        if (!isV8) {
          items.push({ panel: 'legacy', icon: '🧭', label: 'Legacy' });
        }
        return items;
      },
      initialPanels: isV8 ? ['configs', 'queue', 'results', 'archive'] : ['configs', 'queue', 'results', 'archive', 'legacy'],
      configureUi: function () {
        if (!isV8) return;
        var unsupported = [
          'addConfigToRunByName', 'goStrategyExplorer', 'addToRun', 'strategyExplorerFromResult',
          'optimizeFromResult', 'optimizePresetFromResult', 'addToRunFromArchive'
        ];
        document.querySelectorAll('#sidebar-editor button[onclick], #ctx-results button[onclick]').forEach(function (button) {
          var handler = String(button.getAttribute('onclick') || '');
          if (unsupported.some(function (name) { return handler.indexOf(name + '(') >= 0; })) button.remove();
        });
      }
    };
  }

  window.PBGuiBacktestEditorAdapter = { create: create };
}());
