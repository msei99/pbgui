;(function () {
  'use strict';

  function create(version) {
    var isV8 = String(version || '').toLowerCase() === 'v8';

    function installRunHandoff() {
      if (!isV8) return;
      var origin = String(window.API_BASE || '').replace(/\/api\/backtest-v8$/, '');
      var runBase = origin + '/api/v8';

      function navigate(config, overrideConfigs, suggestedName) {
        var candidate = JSON.parse(JSON.stringify(config || {}));
        candidate.pbgui = candidate.pbgui && typeof candidate.pbgui === 'object' ? candidate.pbgui : {};
        candidate.pbgui.runtime = 'pb8';
        candidate.pbgui.enabled_on = 'disabled';
        return fetch(runBase + '/draft', {
          method: 'POST',
          credentials: 'same-origin',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ config: candidate, override_configs: overrideConfigs || {} })
        }).then(function(response) {
          return response.json().then(function(payload) {
            if (!response.ok) throw new Error((payload && payload.detail) || ('HTTP ' + response.status));
            return payload;
          });
        }).then(function(payload) {
          var url = runBase + '/edit_page?new=1&draft_id=' + encodeURIComponent(payload.draft_id);
          if (suggestedName) url += '&name=' + encodeURIComponent(suggestedName);
          window.location.href = url;
        });
      }

      window.addConfigToRunByName = function(name) {
        if (!name || name === '__new__') {
          if (typeof window.toast === 'function') window.toast('Save the config first', 'err');
          return;
        }
        window.apiFetch('/configs/' + encodeURIComponent(name)).then(function(payload) {
          return navigate(payload.config || {}, payload.override_configs || {}, name);
        }).catch(function(error) {
          if (typeof window.toast === 'function') window.toast('Failed: ' + error.message, 'err');
        });
      };

      window.addToRun = function() {
        var selected = window.getSelectedResults();
        if (selected.length !== 1) {
          if (typeof window.toast === 'function') window.toast('Select exactly 1 result', 'err');
          return;
        }
        window.apiFetch('/results/config?path=' + encodeURIComponent(selected[0])).then(function(config) {
          var name = String(selected[0]).split('/').filter(Boolean).pop() || 'pb8-run';
          return navigate(config || {}, {}, name);
        }).catch(function(error) {
          if (typeof window.toast === 'function') window.toast('Failed: ' + error.message, 'err');
        });
      };

      window.addToRunFromArchive = function() {
        var selected = window.getSelectedArchiveResults();
        if (selected.length !== 1) {
          if (typeof window.toast === 'function') window.toast('Select exactly 1 result', 'err');
          return;
        }
        var item = window.archiveResultByPath(selected[0]) || {};
        if (String(item.backtest_version || 'v8').toLowerCase() !== 'v8') {
          if (typeof window.toast === 'function') window.toast('Open PB7 archive results from the PB7 Backtest page.', 'err');
          return;
        }
        window.archiveResultApiFetch(selected[0], '/results/config?path=' + encodeURIComponent(selected[0])).then(function(config) {
          var name = String(item.result_name || item.config_name || selected[0]).split('/').filter(Boolean).pop() || 'pb8-run';
          return navigate(config || {}, {}, name);
        }).catch(function(error) {
          if (typeof window.toast === 'function') window.toast('Failed: ' + error.message, 'err');
        });
      };

      window.optimizeFromResult = function() {
        var selected = window.getSelectedResults();
        if (selected.length !== 1) {
          if (typeof window.toast === 'function') window.toast('Select exactly 1 result', 'err');
          return;
        }
        var resultPath = selected[0];
        window.apiFetch('/results/config?path=' + encodeURIComponent(resultPath)).then(function(config) {
          return window.apiFetch('/optimize-draft', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ config: config })
          });
        }).then(function(draft) {
          var name = String(resultPath).split('/').filter(Boolean).pop() || 'pb8-optimize';
          window.location.href = origin + '/api/optimize-v8/main_page?opt_draft_id=' + encodeURIComponent(draft.draft_id || '')
            + '&draft_name=' + encodeURIComponent(name);
        }).catch(function(error) {
          if (typeof window.toast === 'function') window.toast('Failed: ' + error.message, 'err');
        });
      };
    }

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
        return (match ? match[1] : window.location.origin) + '/api/v8';
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
        installRunHandoff();
        var unsupported = [
          'optimizePresetFromResult'
        ];
        document.querySelectorAll('#sidebar-editor button[onclick], #ctx-results button[onclick]').forEach(function (button) {
          var handler = String(button.getAttribute('onclick') || '');
          if (unsupported.some(function (name) { return handler.indexOf(name + '(') >= 0; })) button.remove();
        });
        var runButton = document.getElementById('sb-btn-add-to-run');
        if (runButton) runButton.title = 'Open this config in the PB8 Run editor';
      }
    };
  }

  window.PBGuiBacktestEditorAdapter = { create: create };
}());
