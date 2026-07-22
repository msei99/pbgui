;(function () {
  'use strict';

  function injected(value, fallback) {
    var text = String(value == null ? '' : value).trim();
    return !text || /^%%[A-Z0-9_]+%%$/.test(text) ? fallback : text;
  }

  function object(value) {
    return value && typeof value === 'object' && !Array.isArray(value) ? value : {};
  }

  function clone(value) {
    return JSON.parse(JSON.stringify(value == null ? {} : value));
  }

  function pathParts(path) {
    return Array.isArray(path) ? path.slice() : String(path || '').split('.').filter(Boolean);
  }

  function getPath(root, path, fallback) {
    var target = root;
    var parts = pathParts(path);
    for (var i = 0; i < parts.length; i += 1) {
      if (!target || typeof target !== 'object' || !Object.prototype.hasOwnProperty.call(target, parts[i])) return fallback;
      target = target[parts[i]];
    }
    return target === undefined || target === null ? fallback : target;
  }

  function setPath(root, path, value) {
    var parts = pathParts(path);
    var target = root;
    parts.forEach(function (part, index) {
      if (index === parts.length - 1) {
        target[part] = value;
      } else {
        if (!target[part] || typeof target[part] !== 'object' || Array.isArray(target[part])) target[part] = {};
        target = target[part];
      }
    });
    return root;
  }

  function flattenBounds(root, prefix, output) {
    Object.keys(object(root)).forEach(function (key) {
      var value = root[key];
      var path = prefix ? prefix + '.' + key : key;
      if (Array.isArray(value) || value === null || typeof value !== 'object') output[path] = clone(value);
      else flattenBounds(value, path, output);
    });
    return output;
  }

  function create(version, options) {
    options = options || {};
    var normalized = injected(version, 'v7').toLowerCase() === 'v8' ? 'v8' : 'v7';
    var isV8 = normalized === 'v8';
    var backtestVersion = injected(options.backtestVersion, normalized).toLowerCase() === 'v8' ? 'v8' : 'v7';
    var apiBase = injected(options.apiBase, '/api/optimize-' + normalized).replace(/\/$/, '');
    var wsBase = injected(options.wsBase, '').replace(/\/$/, '');
    var versionNumber = normalized.slice(1);
    var runtimeOptions = {};
    var enableOverrideOptions = [];

    function getBotValue(sideConfig, key, fallback) {
      return getPath(sideConfig, isV8 ? ['risk', key] : [key], fallback);
    }

    function setBotValue(sideConfig, key, value) {
      return setPath(sideConfig, isV8 ? ['risk', key] : [key], value);
    }

    return {
      version: normalized,
      isV8: isV8,
      backtestVersion: backtestVersion,
      apiBase: apiBase,
      wsBase: wsBase,
      label: isV8 ? 'PB8' : 'PB7',
      navSubtitle: injected(options.navSubtitle, isV8 ? 'PBv8 OPTIMIZE' : 'PBv7 OPTIMIZE'),
      navCurrent: injected(options.navCurrent, isV8 ? 'v8_optimize' : 'v7_optimize'),
      supportsArchive: true,
      supportsResultPlots: true,
      supportsOhlcvPreflight: true,
      supportsParetoExplorer: true,
      supportsBacktestHandoff: true,
      websocketPath: '/api/optimize-' + normalized + '/ws/opt' + versionNumber,
      metadataPath: isV8 ? '/metadata' : '',
      getPath: getPath,
      setPath: setPath,
      getBotValue: getBotValue,
      setBotValue: setBotValue,
      getBotHslValue: function (sideConfig, key, fallback) {
        return getPath(sideConfig, isV8 ? ['hsl', key] : ['hsl_' + key], fallback);
      },
      setBotHslValue: function (sideConfig, key, value) {
        return setPath(sideConfig, isV8 ? ['hsl', key] : ['hsl_' + key], value);
      },
      hslRuntimeOverrideKey: function (side, key) {
        return 'bot.' + side + (isV8 ? '.hsl.' : '.hsl_') + key;
      },
      getBounds: function (optimize) {
        var bounds = object(object(optimize).bounds);
        return isV8 ? flattenBounds(bounds, '', {}) : clone(bounds);
      },
      setBounds: function (optimize, bounds) {
        if (!isV8) {
          optimize.bounds = clone(bounds);
          return optimize.bounds;
        }
        var nested = {};
        Object.keys(object(bounds)).forEach(function (key) {
          setPath(nested, key, clone(bounds[key]));
        });
        optimize.bounds = nested;
        return nested;
      },
      boundGroup: function (key) {
        var clean = String(key || '');
        if (isV8) {
          if (clean.indexOf('bot.') === 0) clean = clean.slice(4);
          if (clean.indexOf('long.') === 0) return 'long';
          if (clean.indexOf('short.') === 0) return 'short';
        }
        if (clean.indexOf('long_') === 0) return 'long';
        if (clean.indexOf('short_') === 0) return 'short';
        return 'other';
      },
      boundSuffix: function (key) {
        var clean = String(key || '');
        if (isV8) {
          if (clean.indexOf('bot.') === 0) clean = clean.slice(4);
          if (clean.indexOf('long.') === 0) clean = clean.slice(5);
          if (clean.indexOf('short.') === 0) clean = clean.slice(6);
          return clean;
        }
        if (clean.indexOf('long_') === 0) return clean.slice(5);
        if (clean.indexOf('short_') === 0) return clean.slice(6);
        return clean;
      },
      boundMetaKeys: function (key) {
        var clean = String(key || '').replace(/^bot\./, '');
        var suffix = this.boundSuffix(clean);
        var parts = suffix.split('.').filter(Boolean);
        var leaf = parts.slice(-1)[0];
        var flattened = suffix.replace(/\./g, '_');
        var strategyLocal = isV8 && parts[0] === 'strategy' && parts.length > 2
          ? parts.slice(2).join('_')
          : '';
        return [clean, suffix, flattened, strategyLocal, leaf].filter(function (item, index, values) {
          return item && values.indexOf(item) === index;
        });
      },
      canonicalFixedParam: function (value) {
        var clean = String(value || '').trim();
        if (!isV8 || !clean) return clean;
        return /^(?:long|short)(?:\.|$)/.test(clean) ? 'bot.' + clean : clean;
      },
      metadataApiBase: function () {
        return apiBase.replace(/\/optimize-v[78]$/, '/v7');
      },
      docsApiBase: function () {
        return apiBase.replace(/\/optimize-v[78]$/, '');
      },
      archiveApiBase: function () {
        return apiBase.replace(/\/optimize-v[78]$/, '/backtest-v7');
      },
      backtestApiBase: function () {
        return apiBase.replace(/\/optimize-v[78]$/, '/backtest-' + backtestVersion);
      },
      backtestMainPageUrl: function () {
        return this.backtestApiBase() + '/main_page';
      },
      queueLogFile: function (filename) {
        return (isV8 ? 'optimizes_v8/' : 'optimizes/') + filename + '.log';
      },
      resultsPath: '/results',
      resultConfigPath: function (path) { return '/results/config?path=' + encodeURIComponent(path); },
      resultDeletePath: '/results/delete',
      result3dPlotPath: '/results/3d-plot',
      resultParetoDashPath: '/results/pareto-dash',
      resultParetoDashSessionPath: function (sessionId) { return '/results/pareto-dash/' + encodeURIComponent(sessionId); },
      paretosPath: function (query) { return '/paretos' + (query ? '?' + query : ''); },
      paretoFilePath: function (path) { return '/paretos/file?path=' + encodeURIComponent(path); },
      paretoSeedBundlePath: '/paretos/seed-bundle',
      resumeQueueRequest: function (filename, resultPath) {
        return {
          path: '/queue/' + encodeURIComponent(filename) + '/resume-checkpoint',
          options: { method: 'POST', body: JSON.stringify({ source: resultPath }) }
        };
      },
      resultResumeRequest: function (name, resultPath) {
        return {
          path: '/results/resume',
          options: { method: 'POST', body: JSON.stringify({ name: name, path: resultPath }) }
        };
      },
      resultCapabilities: function (result) {
        result = object(result);
        if (isV8) {
          return {
            hasPareto: result.has_pareto === true,
            resumable: result.resumable === true,
            hasConfig: result.has_config === true,
            supports3d: result.supports_3d === true,
            supportsDash: result.supports_dash === true
          };
        }
        var hasPareto = Number(result.pareto_count || 0) > 0;
        return {
          hasPareto: hasPareto,
          resumable: false,
          hasConfig: true,
          supports3d: hasPareto,
          supportsDash: hasPareto
        };
      },
      versionRunSettingsHtml: function (config, escape) {
        if (!isV8) return '';
        config = object(config);
        var optimize = object(config.optimize);
        var runtime = object(object(config.pbgui).optimize_runtime);
        escape = escape || String;
        var fineTune = Array.isArray(runtime.fine_tune_params) ? runtime.fine_tune_params.join(', ') : (runtime.fine_tune_params || '');
        var polishMode = String(runtime.polish_bounds_mode || 'clamp');
        var polishModes = object(runtimeOptions.polish_bounds_mode).choices || ['clamp', 'override-tunable', 'override-all'];
        var enabledOverrides = Array.isArray(optimize.enable_overrides) ? optimize.enable_overrides : [];
        var additionalOverrides = enableOverrideOptions.slice();
        var polishPercent = runtime.polish_percentage == null ? '' : Number(runtime.polish_percentage) * 100;
        return ''
          + '<div class="form-row cols-8" data-optimize-version="v8">'
          + '<div class="form-group"><label><span data-tip="Optional deterministic optimizer RNG seed.">rng_seed</span></label><input type="number" id="opted-rng-seed" step="1" value="' + escape(optimize.seed == null ? '' : String(optimize.seed)) + '"></div>'
          + '<div class="form-group span-3"><label><span data-tip="Comma-separated dotted selectors to keep tunable; all other bounds are fixed.">fine_tune_params</span></label><input type="text" id="opted-fine-tune-params" value="' + escape(String(fineTune)) + '" placeholder="long.risk, short.strategy"></div>'
          + '<div class="form-group"><label><span data-tip="Percentage window used to polish bounds. PB8 receives this as a 0.0 to 1.0 fraction.">polish_percentage (%)</span></label><input type="number" id="opted-polish-pct" min="0" max="100" step="0.01" value="' + escape(polishPercent === '' || !Number.isFinite(polishPercent) ? '' : String(polishPercent)) + '"></div>'
          + '<div class="form-group span-2"><label><span data-tip="Controls whether polished bounds may extend beyond configured bounds.">polish_bounds_mode</span></label><select id="opted-polish-bounds-mode">'
          + polishModes.map(function (value) { return '<option value="' + value + '"' + (polishMode === value ? ' selected' : '') + '>' + value + '</option>'; }).join('')
          + '</select></div></div>'
          + (additionalOverrides.length ? '<div class="form-row cols-8" data-optimize-version="v8">'
            + additionalOverrides.map(function (value) {
              var id = 'opted-enable-override-' + value.replace(/[^a-z0-9_-]/gi, '-');
              return '<div class="form-group" style="justify-content:flex-end"><label>&nbsp;</label><div class="chk-row">'
                + '<input type="checkbox" id="' + id + '" data-pb8-enable-override="' + escape(value) + '"'
                + (enabledOverrides.indexOf(value) >= 0 ? ' checked' : '') + '><label for="' + id + '">'
                + '<span data-tip="PB8 optimizer override reported by the installed runtime.">' + escape(value) + '</span></label></div></div>';
            }).join('') + '</div>' : '');
      },
      collectVersionRunSettings: function (config, lookup, strict) {
        if (!isV8) return;
        var optimize = object(config.optimize);
        config.optimize = optimize;
        var pbgui = object(config.pbgui);
        config.pbgui = pbgui;
        var runtime = object(pbgui.optimize_runtime);
        pbgui.optimize_runtime = runtime;
        var seedText = String((lookup('opted-rng-seed') || {}).value || '').trim();
        if (seedText) {
          var seed = Number(seedText);
          if (strict && (!Number.isInteger(seed) || seed < 0)) throw new Error('RNG seed must be a non-negative integer.');
          optimize.seed = Number.isFinite(seed) ? Math.round(seed) : seedText;
        } else {
          optimize.seed = null;
        }
        var fineTune = String((lookup('opted-fine-tune-params') || {}).value || '').split(',').map(function (item) { return item.trim(); }).filter(Boolean);
        runtime.fine_tune_params = fineTune;
        var polishText = String((lookup('opted-polish-pct') || {}).value || '').trim();
        if (polishText) {
          var polish = Number(polishText);
          if (strict && (!Number.isFinite(polish) || polish < 0 || polish > 100)) throw new Error('Polish percentage must be between 0 and 100.');
          runtime.polish_percentage = Number.isFinite(polish) ? polish / 100 : polishText;
          runtime.polish_bounds_mode = String((lookup('opted-polish-bounds-mode') || {}).value || 'clamp');
        } else {
          runtime.polish_percentage = null;
          runtime.polish_bounds_mode = 'clamp';
        }
        runtime.mode = String(runtime.mode || 'fresh');
        var enabled = Array.isArray(optimize.enable_overrides) ? optimize.enable_overrides.slice() : [];
        if (typeof document !== 'undefined') {
          Array.prototype.forEach.call(document.querySelectorAll('[data-pb8-enable-override]'), function (node) {
            var value = String(node.getAttribute('data-pb8-enable-override') || '').trim();
            enabled = enabled.filter(function (item) { return item !== value; });
            if (value && node.checked) enabled.push(value);
          });
        }
        optimize.enable_overrides = enabled.filter(function (value, index, values) {
          return value && values.indexOf(value) === index;
        });
      },
      normalizeMetadata: function (payload) {
        var source = object(object(payload).metadata || payload);
        runtimeOptions = object(source.runtime_options);
        var template = object(source.template || source.config_template);
        var optimize = object(template.optimize || source.optimize_defaults);
        var scoring = object(source.scoring);
        var limits = object(source.limits);
        var bot = object(template.bot);
        var strategyBounds = {};
        Object.keys(object(source.active_bounds)).forEach(function (strategy) {
          strategyBounds[strategy] = flattenBounds(source.active_bounds[strategy], '', {});
        });
        var defaultOverrides = object(source.fixed_runtime_overrides || optimize.fixed_runtime_overrides);
        enableOverrideOptions = Array.isArray(source.optimizer_overrides) ? source.optimizer_overrides.slice() : [];
        var runtimeOverrides = [];
        function mergeRuntimeOverride(field) {
          if (!field || !field.key) return;
          var normalized = clone(field);
          var match = String(normalized.key).match(/^bot\.(long|short)\./);
          if (match && !normalized.side) normalized.side = match[1];
          var sideLabel = normalized.side === 'long' ? 'Long' : (normalized.side === 'short' ? 'Short' : '');
          if (/\.hsl\.restart_after_red_policy$/.test(normalized.key)) {
            normalized.label = normalized.label && normalized.label !== normalized.key
              ? normalized.label
              : sideLabel + ' HSL restart after RED';
            normalized.type = 'string';
            normalized.choices = ['always', 'threshold', 'never'];
            normalized.tip = normalized.tip || 'Restart policy after an HSL RED episode: always restarts after cooldown, threshold stops restarting after the no-restart drawdown threshold is breached, and never permanently halts after RED.';
          }
          var index = runtimeOverrides.findIndex(function (existing) { return existing.key === normalized.key; });
          if (index >= 0) runtimeOverrides[index] = normalized;
          else runtimeOverrides.push(normalized);
        }
        if (isV8) {
          ['long', 'short'].forEach(function (side) {
            var sideLabel = side === 'long' ? 'Long' : 'Short';
            var hsl = object(object(bot[side]).hsl);
            mergeRuntimeOverride({
              key: 'bot.' + side + '.hsl.enabled',
              label: sideLabel + ' HSL enabled',
              side: side,
              storage: 'bot_hsl',
              botKey: 'enabled',
              type: 'boolean',
              defaultValue: hsl.enabled === true,
              tip: 'Enable ' + side + '-side equity hard stop behavior during optimizer evaluations.'
            });
            mergeRuntimeOverride({
              key: 'bot.' + side + '.hsl.no_restart_drawdown_threshold',
              label: sideLabel + ' HSL no-restart drawdown threshold',
              side: side,
              storage: 'bot_hsl',
              botKey: 'no_restart_drawdown_threshold',
              type: 'number',
              defaultValue: hsl.no_restart_drawdown_threshold == null ? 1 : hsl.no_restart_drawdown_threshold,
              defaultValueText: String(hsl.no_restart_drawdown_threshold == null ? 1 : hsl.no_restart_drawdown_threshold),
              minimum: 0,
              maximum: 1,
              step: 0.01,
              tip: 'Persistent HSL drawdown at which threshold policy stops restarting. Keep 1.0 to avoid terminal truncation during optimizer evaluations.'
            });
          });
        }
        Object.keys(defaultOverrides).forEach(function (key) {
          var value = defaultOverrides[key];
          mergeRuntimeOverride({
            key: key,
            label: key,
            type: typeof value === 'boolean' ? 'boolean' : (typeof value === 'number' ? 'number' : 'string'),
            defaultValue: value,
            defaultValueText: value == null ? '' : String(value)
          });
        });
        if (Array.isArray(source.runtime_overrides)) source.runtime_overrides.forEach(mergeRuntimeOverride);
        var limitsMeta = limits.metrics ? {
          type_options: ['all'],
          metrics_by_group: { all: limits.metrics },
          metric_help_by_group: {},
          currency_metrics: [],
          shared_metrics: limits.metrics,
          all_valid_metrics: limits.metrics,
          penalize_if_options: limits.operators,
          stat_options: [''].concat(limits.statistics || []),
          goal_options: scoring.goals || ['min', 'max'],
          default_goal_map: scoring.default_goals || {}
        } : (source.limits_meta || null);
        return {
          optimizeDefaults: optimize,
          limitsMeta: limitsMeta,
          boundsMeta: source.bounds_meta || source.optimize_bounds_meta || null,
          runtimeOverrides: runtimeOverrides,
          runtimeOptions: source.runtime_options || null,
          strategies: Array.isArray(source.strategies) ? source.strategies.slice() : [],
          strategyBounds: strategyBounds,
          strategyDefaults: clone(source.strategy_defaults || {}),
          hslSignalModes: Array.isArray(source.hsl_signal_modes) ? source.hsl_signal_modes.slice() : (isV8 ? ['coin', 'pside', 'unified'] : []),
          enableOverrides: enableOverrideOptions.slice(),
          backendOptions: source.backend_options || source.backends || null,
          backendDefault: source.backend_default || optimize.backend || null,
          pymooAlgorithmOptions: source.pymoo_algorithm_options || object(source.pymoo).algorithms || null,
          pymooRefDirMethodOptions: source.pymoo_ref_dir_method_options || object(source.pymoo).ref_dir_methods || null
        };
      },
      configureUi: function () {
        document.title = 'PBGui - ' + (isV8 ? 'V8' : 'V7') + ' Optimize';
        var resume = document.getElementById('btn-resume-result');
        if (resume) resume.style.display = isV8 ? '' : 'none';
        var plotTitle = document.getElementById('plot-modal-title');
        if (plotTitle) plotTitle.textContent = (isV8 ? 'PB8' : 'PB7') + ' 3D plot';
        var plotFrame = document.getElementById('plot-frame');
        if (plotFrame) plotFrame.title = (isV8 ? 'PB8' : 'PB7') + ' 3D plot';
        var preflight = document.getElementById('opted-sidebar-ohlcv-preflight-btn');
        if (preflight) {
          preflight.title = 'Check ' + (isV8 ? 'PB8' : 'PB7') + ' OHLCV readiness for the current config';
          preflight.style.display = '';
        }
        var importVersion = document.getElementById('optimize-import-version');
        if (importVersion) importVersion.textContent = isV8 ? 'PB8' : 'PB7';
        var importJson = document.getElementById('import-config-json');
        if (importJson) importJson.placeholder = 'Paste full ' + (isV8 ? 'PB8' : 'PB7') + ' optimize config JSON here';
      }
    };
  }

  window.PBGuiOptimizeEditorAdapter = { create: create };
}());
