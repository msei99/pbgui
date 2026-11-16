;(function () {
  'use strict';

  function createV9() {
    return {
      version:'v9', isV8:false, metadataDriven:true,
      metadataApiBase:function(base) { return String(base).replace(/\/backtest-v9$/, '/v9'); },
      docsApiBase:function(base) { return String(base).replace(/\/backtest-v9$/, ''); },
      archiveApiBase:function(base) { return String(base).replace(/\/backtest-v9$/, "/backtest-v7"); },
      websocketPath:'/api/backtest-v9/ws/bt9',
      queueLogFile:function(id) { return 'backtests_v9/' + id + '.log'; },
      navItems:function() { return [{panel:'configs',icon:'📋',label:'Configs'}, {panel:'queue',icon:'⏳',label:'Queue',badge:true}, {panel:'results',icon:'📊',label:'Results'}, {panel:'archive',icon:'🗄️',label:'Archive'}]; },
      initialPanels:['configs','queue','results','archive'],
      configureUi:function() {
        var versions=document.getElementById('results-version-filter');
        versions.replaceChildren();[['v9','PB9'],['v8','PB8'],['v7','PB7'],['both','All']].forEach(function(item){var option=document.createElement('option');option.value=item[0];option.textContent=item[1];versions.appendChild(option);});
        window.PBGuiBacktestV9.restoreSearch();
        document.querySelectorAll('button[onclick]').forEach(function(button) {
          var action=button.getAttribute('onclick') || '';
          if (/^(?:optimizeFromResult|optimizePresetFromResult|strategyExplorerFromResult|convertConfigToV8|goStrategyExplorer)/.test(action)) {
            button.hidden=true;button.disabled=true;button.style.display='none';
          }
        });
        ['archive-btn-retest-replace','arc-tab-schedules','arc-tab-optimize'].forEach(function(id){var node=document.getElementById(id);if(node){node.hidden=true;node.disabled=true;node.style.display='none';}});
        var readiness=document.getElementById('sb-btn-ohlcv-preflight');
        if (readiness) readiness.title='Check PB9 OHLCV readiness for the current config';
        ['goStrategyExplorer'].forEach(function(action) {
          window[action]=function() {window.toast('This tool is not yet available for native PB9.','err');};
        });
        var openCalculator=window.openBalanceCalculatorWithConfig, inlineCalculator=window._showBalanceCalcModal;
        window.openBalanceCalculatorWithConfig=async function(config,exchange) {
          return openCalculator(/^v9\./.test(String((config || {}).config_version)) ? await window.PBGuiBacktestV9.balanceConfig(config) : config,exchange);
        };
        window._showBalanceCalcModal=async function(config,exchange) {
          try {return inlineCalculator(/^v9\./.test(String((config || {}).config_version)) ? await window.PBGuiBacktestV9.balanceConfig(config) : config,exchange);} catch(error) {window.toast(error.message,'err');}
        };
        var runButton=document.getElementById('sb-btn-add-to-run');if(runButton)runButton.title='Open this config in PB9 Run editor';
        var convert=document.getElementById('sb-btn-convert-v8');if(convert)convert.remove();
        function openRun(config) {
          var run=String(window.API_BASE).replace(/\/backtest-v9$/, '/v9');
          var candidate=JSON.parse(JSON.stringify(config));candidate.pbgui=Object.assign({},candidate.pbgui || {},{runtime:'pb9',enabled_on:'disabled'});
          return fetch(run+'/draft',{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json'},body:JSON.stringify({config:candidate})}).then(function(response){if(!response.ok)throw new Error('PB9 Run draft was rejected');return response.json();}).then(function(data){window.location.href=run+'/edit_page?new=1&draft_id='+encodeURIComponent(data.draft_id);});
        }
        window.addToRunFromArchive=function() {
          var selected=window.getSelectedArchiveResults();
          if(selected.length!==1){window.toast('Select exactly 1 result','err');return;}
          if(window.archiveResultByPath(selected[0]).backtest_version!=='v9'){
            window.toast('Open this result from its own PB7/PB8 Backtest page to add it to Run.','err');return;
          }
          window.archiveResultApiFetch(selected[0],'/results/config?path='+encodeURIComponent(selected[0]))
            .then(openRun).catch(function(error){window.toast(error.message,'err');});
        };
        window.addConfigToRunByName=function(name) {
          if (window.currentPanel === 'configs' && window.editingConfig === name && document.getElementById('cfg-name')) {
            if (!window.PBGuiBacktestV9.validate() || !window.ensureRawJsonValidForSave()) return;
            openRun(window.PBGuiBacktestV9.collect()).catch(function(error){window.toast(error.message,'err');});return;
          }
          window.apiFetch('/configs/'+encodeURIComponent(name)).then(function(data){return openRun(data.config);}).catch(function(error){window.toast(error.message,'err');});
        };
        window.addToRun=function() {
          var selected=window.getSelectedResults();if(selected.length!==1){window.toast('Select exactly 1 result','err');return;}
          window.apiFetch('/results/config?path='+encodeURIComponent(selected[0])).then(function(config) {
            return openRun(config);
          }).catch(function(error){window.toast(error.message,'err');});
        };
        window.rebacktestSelected=function() {
          var selected=window.getSelectedResults();if(selected.length!==1){window.toast('Select exactly 1 result','err');return;}
          window.apiFetch('/results/config?path='+encodeURIComponent(selected[0])).then(function(config){window.selectPanel('configs');window._nativeConfigRevision=null;window.editingConfig='__new__';window.showConfigEditor('',config);}).catch(function(error){window.toast(error.message,'err');});
        };
      }
    };
  }

  function create(version) {
    if (String(version).toLowerCase() === 'v9') return createV9();
    var isV8 = String(version || '').toLowerCase() === 'v8';

    function installRunHandoff() {
      if (!isV8) return;
      var origin = String(window.API_BASE || '').replace(/\/api\/backtest-v8$/, '');
      var runBase = origin + '/api/v8';

      function openDraft(payload, suggestedName) {
        var url = runBase + '/edit_page?new=1&draft_id=' + encodeURIComponent(payload.draft_id);
        if (suggestedName) url += '&name=' + encodeURIComponent(suggestedName);
        window.location.href = url;
      }

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
        }).then(function(payload) { openDraft(payload, suggestedName); });
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
        window.apiFetch('/results/run-draft', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ path: selected[0] })
        }).then(function(payload) {
          openDraft(payload, payload.name || 'pb8-run');
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
      websocketPath: isV8 ? '/api/backtest-v8/ws/bt8' : '/api/backtest-v7/ws/bt7',
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
        items.push({ panel: 'legacy', icon: '🧭', label: 'Legacy' });
        return items;
      },
      initialPanels: ['configs', 'queue', 'results', 'archive', 'legacy'],
      configureUi: function () {
        if (!isV8) return;
        installRunHandoff();
        var unsupported = [
          'optimizePresetFromResult',
          'rebacktestSelectedLegacy',
          'addToRunFromLegacy',
          'deleteSelectedLegacyResults'
        ];
        document.querySelectorAll('#sidebar-editor button[onclick], #ctx-results button[onclick], #ctx-legacy button[onclick]').forEach(function (button) {
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
