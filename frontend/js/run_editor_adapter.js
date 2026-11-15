;(function () {
  'use strict';

  function object(value) {
    return value && typeof value === 'object' && !Array.isArray(value) ? value : {};
  }

  function clone(value) {
    return value === undefined ? undefined : JSON.parse(JSON.stringify(value));
  }

  function coinList(value, allowAll) {
    if (allowAll && typeof value === 'string' && value.trim().toLowerCase() === 'all') return ['all'];
    return Array.isArray(value) ? value.map(String) : [];
  }

  function sameMembers(left, right) {
    return JSON.stringify(left.slice().sort()) === JSON.stringify(right.slice().sort());
  }

  function hideUnsupportedActions(adapter) {
    document.querySelectorAll('[data-run-capability]').forEach(function (element) {
      var capability = element.getAttribute ? element.getAttribute('data-run-capability') : '';
      if (capability && adapter[capability] === false) element.classList.add('run-version-hidden');
    });
  }

  var V9_SIDES = ['long', 'short'];
  var V9_SCOPES = ['global', 'long', 'short'];
  var V9_USER_PATH = ['live', 'account', 'user'];
  var V9_STRATEGY_PATH = ['bot', 'portfolio', 'strategy_kind'];
  var V9_RISK_PATHS = {
    total_wallet_exposure_limit: ['risk', 'total_wallet_exposure_limit_ratio'],
    n_positions: ['risk', 'max_positions']
  };
  var V9_BOT_INPUTS = { twe: 'total_wallet_exposure_limit', npos: 'n_positions' };
  var V9_COIN_INPUTS = {
    approved_coins: { long: 'ms-approved-long', short: 'ms-approved-short' },
    ignored_coins: { long: 'ms-ignored-long', short: 'ms-ignored-short' }
  };
  var V9_ACTIVATION_KEYS = ['bot.long.enabled', 'bot.short.enabled'];
  var V9_SECTIONS = [
    { id: 'live', host: 'settings', title: 'Live Settings', prefix: ['live'] },
    { id: 'logging', host: 'settings', title: 'Logging', prefix: ['logging'] },
    { id: 'monitor', host: 'settings', title: 'Monitoring', prefix: ['monitor'] },
    { id: 'activation', host: 'bot', title: 'Side activation', keys: V9_ACTIVATION_KEYS },
    { id: 'portfolio', host: 'bot', title: 'Portfolio', prefix: ['bot', 'portfolio'] },
    { id: 'global', host: 'bot', title: 'Global defaults', prefix: ['bot', 'global'] },
    { id: 'long', host: 'bot', title: 'Long overrides', prefix: ['bot', 'long'] },
    { id: 'short', host: 'bot', title: 'Short overrides', prefix: ['bot', 'short'] }
  ];
  var V9_UNSAFE_SEGMENTS = ['__proto__', 'constructor', 'prototype'];
  var V9_NO_COIN_OVERRIDE = /^(never|none|no|false|forbidden|disabled|unsupported)$/i;

  /* PB9 edits the native source document. bot.long/bot.short stay sparse:
   * a side value is written only once the user edits it or marks it explicit;
   * everything else is displayed from bot.global or the runtime prototype. */
  function createV9() {
    var metadata = { prototype: {}, strategies: [], strategyDefaults: {}, strategySpecs: {} };
    var fields = [];
    var fieldIndex = {};
    var renderer = null;
    var renderedKind = null;
    var activeKind = '';
    var strategyCache = { global: {}, long: {}, short: {} };
    var fixedInputs = {};
    var coinInputs = {};

    function paths() {
      return window.PBGuiEditorShared.configPath;
    }

    function inheritedValue(source, path) {
      var cp = paths();
      if (path[0] === 'bot' && V9_SIDES.indexOf(path[1]) >= 0) {
        var common = ['bot', 'global'].concat(path.slice(2));
        if (cp.has(source, common)) return cp.get(source, common);
        if (cp.has(metadata.prototype, common)) return cp.get(metadata.prototype, common);
      }
      if (cp.has(metadata.prototype, path)) return cp.get(metadata.prototype, path);
      var field = fieldIndex[path.join('.')];
      return field && field.hasDefault ? field.default : undefined;
    }

    function resolve(source, path) {
      var cp = paths();
      var inherited = inheritedValue(source, path);
      var explicit = cp.has(source, path);
      return { explicit: explicit, value: explicit ? cp.get(source, path) : inherited, inherited: inherited };
    }

    function resolveCoins(source, key, side) {
      // A null side list is the native "retain the shared source" marker.
      var path = ['bot', side, 'universe', key];
      var own = paths().get(source, path);
      var explicit = own !== undefined && own !== null;
      return { explicit: explicit, list: coinList(explicit ? own : inheritedValue(source, path), key === 'approved_coins') };
    }

    function addField(field) {
      var key = field.path.join('.');
      if (fieldIndex[key]) return;
      fieldIndex[key] = field;
      fields.push(field);
    }

    function normalizeField(raw) {
      if (!raw || !Array.isArray(raw.path) || !raw.path.length) return null;
      var safe = raw.path.every(function (segment) {
        return typeof segment === 'string' && segment && V9_UNSAFE_SEGMENTS.indexOf(segment) < 0;
      });
      if (!safe) return null;
      return {
        path: raw.path.slice(),
        types: Array.isArray(raw.types) ? raw.types.map(String) : [],
        nullable: raw.nullable === true,
        hasDefault: Object.prototype.hasOwnProperty.call(raw, 'default'),
        default: raw.default,
        domain: object(raw.domain),
        unit: raw.unit ? String(raw.unit) : '',
        description: raw.description ? String(raw.description) : '',
        order: Number(raw.order) || 0,
        coinOverride: raw.coin_override == null ? '' : String(raw.coin_override)
      };
    }

    function leafField(path, value, description) {
      var type = Array.isArray(value) ? 'array' : value == null ? '' : typeof value;
      return {
        path: path,
        types: type === 'number' ? ['float'] : type ? [type] : [],
        nullable: value === null,
        hasDefault: value !== undefined,
        default: value,
        domain: {},
        unit: '',
        description: description || '',
        order: 0,
        coinOverride: ''
      };
    }

    function flattenLeaves(value, prefix, result) {
      var keys = value && typeof value === 'object' && !Array.isArray(value) ? Object.keys(value) : [];
      if (!keys.length) {
        if (prefix.length) result.push({ path: prefix, value: value });
        return result;
      }
      keys.forEach(function (key) {
        if (V9_UNSAFE_SEGMENTS.indexOf(key) < 0) flattenLeaves(value[key], prefix.concat(key), result);
      });
      return result;
    }

    function strategyDescriptions(kind) {
      var descriptions = {};
      var parameters = object(metadata.strategySpecs[kind]).parameters;
      (Array.isArray(parameters) ? parameters : []).forEach(function (parameter) {
        var path = parameter && Array.isArray(parameter.config_path) ? parameter.config_path : [];
        if (!parameter || !parameter.description) return;
        for (var i = 0; i < path.length; i++) {
          var suffix = path.slice(i).join('.');
          if (!descriptions[suffix]) descriptions[suffix] = String(parameter.description);
        }
      });
      return descriptions;
    }

    function addStrategyFields(kind) {
      var registered = fields.some(function (field) {
        return field.path[0] === 'bot' && field.path[2] === 'strategy' && field.path[3] === kind;
      });
      if (registered) return;
      var descriptions = strategyDescriptions(kind);
      V9_SCOPES.forEach(function (scope) {
        var block = paths().get(metadata.prototype, ['bot', scope, 'strategy', kind]);
        if (!block || typeof block !== 'object') {
          block = object(object(metadata.strategyDefaults[scope === 'global' ? 'long' : scope])[kind]);
        }
        flattenLeaves(block, [], []).forEach(function (leaf) {
          var field = leafField(['bot', scope, 'strategy', kind].concat(leaf.path), leaf.value, descriptions[leaf.path.join('.')]);
          if (scope !== 'global') field.hasDefault = false;
          addField(field);
        });
      });
    }

    function loadMetadata(raw) {
      raw = object(raw);
      metadata = {
        prototype: object(raw.prototype),
        strategies: Array.isArray(raw.strategies) ? raw.strategies.map(String) : [],
        strategyDefaults: object(raw.strategy_defaults),
        strategySpecs: object(raw.strategy_specs)
      };
      fields = [];
      fieldIndex = {};
      (Array.isArray(raw.fields) ? raw.fields : []).forEach(function (item) {
        var field = normalizeField(item);
        if (field) addField(field);
      });
      metadata.strategies.forEach(addStrategyFields);
      // A side shares the shape of bot.global, so every common field can be pinned per side.
      fields.slice().forEach(function (field) {
        if (field.path[0] !== 'bot' || field.path[1] !== 'global') return;
        V9_SIDES.forEach(function (side) {
          var sideField = clone(field);
          sideField.path = ['bot', side].concat(field.path.slice(2));
          sideField.hasDefault = false;
          delete sideField.default;
          addField(sideField);
        });
      });
      V9_SIDES.forEach(function (side) {
        addField(leafField(['bot', side, 'enabled'], false, ''));
      });
      fields = fields.map(function (field, index) { return { field: field, index: index }; })
        .sort(function (left, right) { return left.field.order - right.field.order || left.index - right.index; })
        .map(function (item) { return item.field; });
      renderedKind = null;
    }

    function fixedKeys() {
      var keys = {};
      keys[V9_USER_PATH.join('.')] = true;
      keys[V9_STRATEGY_PATH.join('.')] = true;
      V9_SIDES.forEach(function (side) {
        Object.keys(V9_RISK_PATHS).forEach(function (name) {
          keys[['bot', side].concat(V9_RISK_PATHS[name]).join('.')] = true;
        });
        Object.keys(V9_COIN_INPUTS).forEach(function (name) {
          keys['bot.' + side + '.universe.' + name] = true;
        });
      });
      V9_ACTIVATION_KEYS.forEach(function (key) { keys[key] = true; });
      return keys;
    }

    function sectionItem(definition, field, kind) {
      var path = field.path;
      var key = path.join('.');
      if (definition.keys) return definition.keys.indexOf(key) >= 0 ? { group: '', label: path.slice(1).join('.') } : null;
      var prefix = definition.prefix;
      if (path.length <= prefix.length || prefix.some(function (segment, index) { return path[index] !== segment; })) return null;
      if (path[0] === 'bot' && path[2] === 'strategy' && path.length > 3) {
        return path[3] === kind ? { group: 'strategy · ' + kind, label: path.slice(4).join('.') || kind } : null;
      }
      var rest = path.slice(prefix.length);
      return rest.length > 1 ? { group: rest[0], label: rest.slice(1).join('.') } : { group: '', label: rest[0] };
    }

    function buildSections(kind) {
      var parents = {};
      fields.forEach(function (field) {
        for (var i = 1; i < field.path.length; i++) parents[field.path.slice(0, i).join('.')] = true;
      });
      var fixed = fixedKeys();
      return V9_SECTIONS.map(function (definition) {
        var groups = [];
        var byTitle = {};
        fields.forEach(function (field) {
          var key = field.path.join('.');
          if (parents[key] || (fixed[key] && !definition.keys)) return;
          var item = sectionItem(definition, field, kind);
          if (!item) return;
          if (!byTitle[item.group]) {
            byTitle[item.group] = { title: item.group, fields: [] };
            if (item.group) groups.push(byTitle[item.group]);
            else groups.unshift(byTitle[item.group]);
          }
          byTitle[item.group].fields.push({ field: field, label: item.label });
        });
        return { id: definition.id, host: definition.host, title: definition.title, expander: !definition.keys, groups: groups };
      }).filter(function (section) { return section.groups.length > 0; });
    }

    function render(kind) {
      if (renderer && renderedKind === kind) return;
      if (!renderer) {
        renderer = window.PBGuiEditorShared.createMetadataFieldRenderer({
          idPrefix: 'run-meta',
          resolve: function (source, field) { return resolve(source, field.path); },
          nullLabel: function (field) { return field.path[0] === 'live' && field.path[1] === 'modes' ? 'normal' : 'none'; }
        });
      }
      var open = renderer.clear();
      var sections = buildSections(kind);
      renderer.mount('run-meta-settings', sections.filter(function (section) { return section.host === 'settings'; }), open);
      renderer.mount('run-meta-bot', sections.filter(function (section) { return section.host === 'bot'; }), open);
      renderedKind = kind;
    }

    function markInput(id, explicit) {
      var input = document.getElementById(id);
      var group = input ? input.closest('.form-group') : null;
      if (group) group.dataset.metaState = explicit ? 'explicit' : 'inherited';
    }

    function trackFixedInput(page, id, path, source) {
      var resolved = resolve(source, path);
      page.setVal(id, resolved.value == null ? '' : String(resolved.value));
      fixedInputs[id] = { path: path, explicit: resolved.explicit, shown: page.getVal(id), numeric: id !== 'f-strategy-kind' };
      markInput(id, resolved.explicit);
    }

    function collectFixedInputs(page, target) {
      Object.keys(fixedInputs).forEach(function (id) {
        var state = fixedInputs[id];
        var current = page.getVal(id);
        if (!state.explicit && current !== state.shown) state.explicit = true;
        if (!state.explicit || current === '') return;
        var value = state.numeric ? Number(current) : current;
        if (state.numeric && !Number.isFinite(value)) return;
        paths().set(target, state.path, value);
        state.shown = current;
        markInput(id, true);
      });
    }

    function refreshInheritedInputs(page, source) {
      Object.keys(fixedInputs).forEach(function (id) {
        var state = fixedInputs[id];
        if (state.explicit || document.activeElement === document.getElementById(id)) return;
        var value = resolve(source, state.path).value;
        page.setVal(id, value == null ? '' : String(value));
        state.shown = page.getVal(id);
      });
    }

    function trackCoinInputs(source) {
      Object.keys(V9_COIN_INPUTS).forEach(function (key) {
        V9_SIDES.forEach(function (side) {
          var resolved = resolveCoins(source, key, side);
          coinInputs[V9_COIN_INPUTS[key][side]] = { key: key, side: side, explicit: resolved.explicit, shown: resolved.list };
        });
      });
    }

    function collectCoinInputs(page, target) {
      Object.keys(coinInputs).forEach(function (id) {
        var state = coinInputs[id];
        if (!page.hasMultiselect(id)) return;
        var current = page.getMultiselectValues(id);
        if (!state.explicit && !sameMembers(current, state.shown)) state.explicit = true;
        if (!state.explicit) return;
        var all = state.key === 'approved_coins' && current.indexOf('all') >= 0;
        paths().set(target, ['bot', state.side, 'universe', state.key], all ? 'all' : current.filter(function (coin) {
          return coin !== 'all';
        }));
        state.shown = current;
      });
    }

    function switchStrategy(target, kind) {
      // Only the selected strategy is authored; other blocks wait in memory for a switch back.
      var cp = paths();
      V9_SCOPES.forEach(function (scope) {
        var path = ['bot', scope, 'strategy'];
        var store = cp.get(target, path);
        if (!store || typeof store !== 'object' || Array.isArray(store)) return;
        Object.keys(store).forEach(function (name) {
          if (name === kind) return;
          strategyCache[scope][name] = clone(store[name]);
          delete store[name];
        });
        if (store[kind] === undefined && strategyCache[scope][kind] !== undefined) store[kind] = clone(strategyCache[scope][kind]);
        if (!Object.keys(store).length) cp.remove(target, path, 2);
      });
    }

    function coinOverrideType(field) {
      var types = field.types;
      if (types.indexOf('array') >= 0 || types.indexOf('object') >= 0) return 'json';
      if (types.length === 1 && types[0] === 'boolean') return 'boolean';
      var numeric = types.length > 0 && types.every(function (type) { return type === 'integer' || type === 'float'; });
      return numeric ? (field.nullable ? 'number_or_null' : 'number') : 'string';
    }

    function coinOverrideParams() {
      var allowed = { bot: { long: {}, short: {} }, live: {} };
      fields.forEach(function (field) {
        if (!field.coinOverride || V9_NO_COIN_OVERRIDE.test(field.coinOverride)) return;
        var target = field.path[0] === 'live' ? allowed.live
          : field.path[0] === 'bot' && V9_SIDES.indexOf(field.path[1]) >= 0 ? allowed.bot[field.path[1]] : null;
        if (!target) return;
        var entry = { type: coinOverrideType(field) };
        if (field.hasDefault) entry.default = field.default;
        target[field.path.slice(field.path[0] === 'live' ? 1 : 2).join('.')] = entry;
      });
      return allowed;
    }

    function sideNumber(source, side, name) {
      var value = Number(resolve(source, ['bot', side].concat(V9_RISK_PATHS[name])).value);
      return Number.isFinite(value) ? value : 0;
    }

    return {
      version: 'v9',
      isV8: false,
      isV9: true,
      label: 'PB9',
      navSubtitle: 'PBv9 EDIT',
      navCurrent: 'v9_run',
      helpTopic: '53_pbv9_run',
      runtimeNumber: '9',
      capabilityKey: 'pb9_capable',
      metadataDriven: true,
      overrideFiles: false,
      inlineOverrides: true,
      sideActivation: true,
      userPath: V9_USER_PATH,
      supportsBacktest: false,
      supportsStrategyExplorer: false,
      supportsDynamicIgnore: false,
      supportsBalanceCalculator: true,
      knownLiveParams: null,
      managedLiveKeys: [],
      managedLoggingKeys: [],
      managedMonitorKeys: [],
      getUser: function (config) {
        return String(paths().get(object(config), V9_USER_PATH) || '').trim();
      },
      setUser: function (config, user) {
        paths().set(config, V9_USER_PATH, user);
      },
      getStrategyKind: function (config) {
        return String(resolve(object(config), V9_STRATEGY_PATH).value || '').trim();
      },
      readCoinSelection: function (config, key, side) {
        return resolveCoins(object(config), key, side).list;
      },
      getBotValue: function (sideConfig, key, fallback) {
        var value = paths().get(object(sideConfig), V9_RISK_PATHS[key] || [key]);
        return value === undefined || value === null ? fallback : value;
      },
      setBotValue: function (sideConfig, key, value) {
        paths().set(sideConfig, V9_RISK_PATHS[key] || [key], value);
      },
      newInstanceName: function (config) {
        return this.getUser(config);
      },
      saveQuery: function (isNew) {
        return isNew ? '?create_only=true' : '';
      },
      saveBody: function (config, overrideConfigs, expectedVersion) {
        return { config: config, expected_version: Number(expectedVersion || 0) };
      },
      backtestDraftRequest: function () {
        return null;
      },
      configureUi: function () {
        document.querySelectorAll('[data-v7-only], [data-v8-only], [data-not-v9]').forEach(function (element) {
          element.classList.add('run-version-hidden');
        });
        document.querySelectorAll('[data-v9-only], [data-v9-also]').forEach(function (element) {
          element.classList.remove('run-version-hidden');
        });
        hideUnsupportedActions(this);
        var title = document.querySelector('.sb-title');
        if (title) title.textContent = 'Edit PB9 Instance';
        document.title = 'PBv9 Edit';
        var versionInput = document.getElementById('f-version');
        if (versionInput) versionInput.readOnly = true;
      },
      configureRuntimeMetadata: function (raw) {
        loadMetadata(raw);
        return [];
      },
      configureRuntimeConfig: function () {
        return [];
      },
      coinOverrideOptions: function (apiBase) {
        return {
          apiBase: apiBase,
          inlineOnly: true,
          preserveMarketIdentifiers: true,
          runtimeLabel: 'PB9',
          allowedParams: coinOverrideParams()
        };
      },
      populateForm: function (config, page) {
        var pbgui = object(config.pbgui);
        var user = this.getUser(config);
        page.ensureSelectOption('f-user', user);
        page.setVal('f-user', user);
        activeKind = this.getStrategyKind(config);
        page.ensureSelectOption('f-strategy-kind', activeKind);
        fixedInputs = {};
        trackFixedInput(page, 'f-strategy-kind', V9_STRATEGY_PATH, config);
        V9_SIDES.forEach(function (side) {
          Object.keys(V9_BOT_INPUTS).forEach(function (suffix) {
            trackFixedInput(page, 'f-' + side + '-' + suffix, ['bot', side].concat(V9_RISK_PATHS[V9_BOT_INPUTS[suffix]]), config);
          });
        });
        page.ensureHost(pbgui.enabled_on || 'disabled');
        page.setVal('f-version', pbgui.version != null ? pbgui.version : 0);
        page.setVal('f-note', pbgui.note || '');
        page.setVal('f-market-cap', pbgui.market_cap || 0);
        page.setVal('f-vol-mcap', pbgui.vol_mcap != null ? pbgui.vol_mcap : 10);
        page.setChk('f-only-cpt', pbgui.only_cpt);
        page.setChk('f-notices-ignore', pbgui.notices_ignore);
        page.setChk('f-dynamic-ignore', false);
        page.coinOvLoad(config);
        trackCoinInputs(config);
        render(activeKind);
        renderer.populate(config);
      },
      collectConfig: function (base, current, page) {
        var cp = paths();
        var result = clone(base && typeof base === 'object' && !Array.isArray(base) ? base : object(current));
        var basePbgui = object(result.pbgui);
        this.setUser(result, page.getVal('f-user'));
        collectFixedInputs(page, result);
        if (renderer) renderer.apply(result);
        var kind = this.getStrategyKind(result);
        if (kind && kind !== activeKind) switchStrategy(result, kind);
        refreshInheritedInputs(page, result);
        collectCoinInputs(page, result);
        var overrides = object(page.coinOvCollect().coin_overrides);
        if (Object.keys(overrides).length || cp.has(result, ['coin_overrides'])) result.coin_overrides = overrides;
        result.pbgui = Object.assign({}, basePbgui, {
          version: page.getInt('f-version'),
          enabled_on: page.getVal('f-enabled-on') || 'disabled',
          note: page.getVal('f-note'),
          market_cap: page.getInt('f-market-cap'),
          vol_mcap: page.getNum('f-vol-mcap'),
          tags: page.getMultiselectValues('ms-tags'),
          only_cpt: page.getChk('f-only-cpt'),
          notices_ignore: page.getChk('f-notices-ignore'),
          runtime: 'pb9'
        });
        delete result.pbgui.dynamic_ignore;
        return result;
      },
      firstInvalidField: function () {
        return renderer ? renderer.firstInvalid() : null;
      },
      // Coin status lookups may rewrite identifiers; an untouched list must stay inherited.
      pristineCoinSelections: function (getValues) {
        return Object.keys(coinInputs).filter(function (id) {
          return !coinInputs[id].explicit && sameMembers(getValues(id), coinInputs[id].shown);
        });
      },
      rebaseCoinSelections: function (ids, getValues) {
        ids.forEach(function (id) {
          if (coinInputs[id]) coinInputs[id].shown = getValues(id);
        });
      },
      // Read-only PB8-shaped inputs for the shared Balance Calculator; never saved.
      balanceCalcConfig: function (source) {
        source = object(source);
        var kind = this.getStrategyKind(source);
        var projection = {
          live: { user: this.getUser(source), strategy_kind: kind, approved_coins: {}, ignored_coins: {} },
          bot: {},
          pbgui: clone(object(source.pbgui))
        };
        delete projection.pbgui.dynamic_ignore;
        V9_SIDES.forEach(function (side) {
          var enabled = resolve(source, ['bot', side, 'enabled']).value === true;
          var approved = resolveCoins(source, 'approved_coins', side).list;
          var strategyPath = ['bot', side, 'strategy', kind];
          var quantity = [['entry', 'initial_qty_fraction'], ['entry', 'initial_qty_pct'], ['base_qty_fraction'], ['base_qty_pct']]
            .map(function (path) { return Number(resolve(source, strategyPath.concat(path)).value); })
            .filter(function (value) { return Number.isFinite(value); })[0] || 0;
          projection.live.approved_coins[side] = !enabled ? [] : approved.indexOf('all') >= 0 ? 'all' : approved;
          projection.live.ignored_coins[side] = resolveCoins(source, 'ignored_coins', side).list;
          projection.bot[side] = {
            risk: {
              n_positions: enabled ? sideNumber(source, side, 'n_positions') : 0,
              total_wallet_exposure_limit: enabled ? sideNumber(source, side, 'total_wallet_exposure_limit') : 0
            },
            strategy: {}
          };
          projection.bot[side].strategy[kind || 'strategy'] = { entry: { initial_qty_pct: quantity } };
        });
        return projection;
      }
    };
  }

  function create(version, options) {
    options = options || {};
    if (String(version || '').toLowerCase() === 'v9') return createV9();
    var isV8 = String(version || '').toLowerCase() === 'v8';
    var sharedLiveFields = {
      leverage: 'f-leverage', margin_mode_preference: 'f-margin-mode',
      minimum_coin_age_days: 'f-min-coin-age', pnls_max_lookback_days: 'f-pnls-lookback',
      warmup_ratio: 'f-warmup-ratio', max_realized_loss_pct: 'f-max-loss-pct',
      initial_entry_exec_max_market_dist_pct: 'f-price-dist', execution_delay_seconds: 'f-exec-delay',
      limit_order_create_max_market_dist_pct: 'f-price-dist',
      market_order_near_touch_threshold: 'f-market-order-threshold',
      filter_by_min_effective_cost: 'f-filter-min-cost', market_orders_allowed: 'f-market-orders',
      hedge_mode: 'f-hedge-mode', auto_gs: 'f-auto-gs', forced_mode_long: 'f-forced-long',
      forced_mode_short: 'f-forced-short', hsl_engine: 'f-hsl-engine',
      hsl_unavailable_grace_seconds: 'f-hsl-grace', hsl_signal_mode: 'f-hsl-signal-mode',
      hsl_position_during_cooldown_policy: 'f-hsl-cooldown-policy', time_in_force: 'f-time-in-force',
      max_n_cancellations_per_batch: 'f-max-cancel', max_n_creations_per_batch: 'f-max-create',
      max_n_restarts_per_day: 'f-max-restarts', recv_window_ms: 'f-recv-window',
      order_match_tolerance_pct: 'f-order-match-tol', fills_recent_overlap_minutes: 'f-fills-recent-overlap',
      fills_confirmation_overlap_minutes: 'f-fills-confirm-overlap', max_concurrent_api_requests: 'f-max-api-req',
      max_warmup_minutes: 'f-max-warmup-min', warmup_jitter_seconds: 'f-warmup-jitter',
      warmup_concurrency: 'f-warmup-conc', defer_broad_candle_warmup: 'f-defer-broad-candle-warmup',
      enable_archive_candle_fetch: 'f-archive-fetch', max_ohlcv_fetches_per_minute: 'f-max-ohlcv-fetches',
      candle_lock_timeout_seconds: 'f-candle-lock', market_snapshot_ticker_strategy: 'f-market-snapshot-strategy',
      forager_score_hysteresis_pct: 'f-forager-hysteresis',
      max_forager_candle_staleness_minutes: 'f-max-forager-stale',
      max_forager_candle_refresh_seconds: 'f-max-forager-refresh',
      max_disk_candles_per_symbol_per_tf: 'f-max-disk-candles',
      max_memory_candles_per_symbol: 'f-max-mem-candles', inactive_coin_candle_ttl_minutes: 'f-inactive-ttl',
      max_active_candle_tail_gap_minutes: 'f-max-active-tail-gap', balance_override: 'f-bal-override',
      balance_hysteresis_snap_pct: 'f-bal-hyst', custom_endpoints_path: 'f-custom-endpoints-path',
      enable_forager_ws_candles: 'f-enable-forager-ws', fee_conversion_max_age_ms: 'f-fee-conversion-age',
      exchange_symbol_unavailable_cooldown_hours: 'f-exchange-symbol-cooldown',
      fee_pct_fallback: 'f-fee-pct-fallback', fee_pct_sanity_abs_max: 'f-fee-pct-sanity',
      forager_ws_candle_rest_audit_minutes: 'f-forager-ws-audit', force_cold_startup: 'f-force-cold-startup',
      hsl_accept_incomplete_history: 'f-hsl-accept-incomplete',
      order_replacement_churn_gate_activation_count: 'f-churn-activation-count',
      order_replacement_churn_gate_market_dist_pct: 'f-churn-market-dist',
      order_replacement_churn_gate_stability_minutes: 'f-churn-stability-minutes',
      order_replacement_churn_gate_window_minutes: 'f-churn-window-minutes',
      startup_phase_budgets: 'f-startup-phase-budgets'
    };
    var sharedLoggingFields = {
      level: 'f-logging-level', backup_count: 'f-log-backup-count', dir: 'f-log-dir',
      live_event_debug_profiles: 'f-log-debug-profiles', max_bytes_mb: 'f-log-max-bytes',
      memory_snapshot_interval_minutes: 'f-mem-snapshot', persist_to_file: 'f-log-persist',
      rotation: 'f-log-rotation', volume_refresh_info_threshold_seconds: 'f-vol-refresh'
    };
    var sharedMonitorFields = {
      checkpoint_interval_minutes: 'f-monitor-checkpoint', compress_rotated_segments: 'f-monitor-compress',
      emit_completed_candles: 'f-monitor-emit-candles', enabled: 'f-monitor-enabled',
      event_rotation_mb: 'f-monitor-rotation-mb', event_rotation_minutes: 'f-monitor-rotation-minutes',
      include_raw_fill_payloads: 'f-monitor-raw-fills', max_total_bytes: 'f-monitor-max-bytes',
      price_tick_min_interval_ms: 'f-monitor-price-interval', retain_candles: 'f-monitor-retain-candles',
      retain_days: 'f-monitor-retain-days', retain_fills: 'f-monitor-retain-fills',
      retain_price_ticks: 'f-monitor-retain-ticks', root_dir: 'f-monitor-root-dir',
      snapshot_interval_seconds: 'f-monitor-snapshot-interval'
    };

    function configureFields(fields, params) {
      var managed = [];
      var targets = [];
      Object.keys(fields).forEach(function(key) {
        var input = document.getElementById(fields[key]);
        if (!input) return;
        var target = input.closest('.form-group') || input.closest('.chk-row');
        if (target && targets.indexOf(target) < 0) targets.push(target);
      });
      targets.forEach(function(target) { target.classList.add('run-version-hidden'); });
      Object.keys(fields).forEach(function(key) {
        var input = document.getElementById(fields[key]);
        if (!input) return;
        var available = Object.prototype.hasOwnProperty.call(params, key);
        var target = input.closest('.form-group') || input.closest('.chk-row');
        if (target && available) target.classList.remove('run-version-hidden');
        if (available) managed.push(key);
      });
      return managed;
    }

    function risk(sideConfig) {
      sideConfig = object(sideConfig);
      if (!isV8) return sideConfig;
      if (!sideConfig.risk || typeof sideConfig.risk !== 'object' || Array.isArray(sideConfig.risk)) sideConfig.risk = {};
      return sideConfig.risk;
    }

    return {
      version: isV8 ? 'v8' : 'v7',
      isV8: isV8,
      isV9: false,
      metadataDriven: false,
      overrideFiles: isV8,
      inlineOverrides: true,
      sideActivation: false,
      userPath: ['live', 'user'],
      supportsBacktest: true,
      label: isV8 ? 'PB8' : 'PB7',
      navSubtitle: isV8 ? 'PBv8 EDIT' : 'PBv7 EDIT',
      navCurrent: isV8 ? 'v8_run' : 'v7_run',
      backtestPath: isV8 ? '/api/backtest-v8/main_page' : '/api/backtest-v7/main_page',
      supportsStrategyExplorer: true,
      supportsDynamicIgnore: !isV8,
      supportsBalanceCalculator: true,
      capabilityKey: isV8 ? 'pb8_capable' : 'pb7_capable',
      knownLiveParams: isV8 ? ['user', 'approved_coins', 'ignored_coins'] : null,
      managedLiveKeys: [],
      managedLoggingKeys: [],
      managedMonitorKeys: [],
      managedLiveValue: function (key, values) {
        var sourceKey = key === 'limit_order_create_max_market_dist_pct'
          ? 'initial_entry_exec_max_market_dist_pct'
          : key;
        return object(values)[sourceKey];
      },
      readLiveValue: function (values, key) {
        values = object(values);
        if (isV8 && key === 'initial_entry_exec_max_market_dist_pct' && values.limit_order_create_max_market_dist_pct !== undefined) {
          return values.limit_order_create_max_market_dist_pct;
        }
        return values[key];
      },
      getBotValue: function (sideConfig, key, fallback) {
        var value = risk(sideConfig)[key];
        return value === undefined || value === null ? fallback : value;
      },
      setBotValue: function (sideConfig, key, value) {
        risk(sideConfig)[key] = value;
      },
      getUser: function (config) {
        return String(object(object(config).live).user || '').trim();
      },
      setUser: function (config, user) {
        if (!config.live || typeof config.live !== 'object' || Array.isArray(config.live)) config.live = {};
        config.live.user = user;
      },
      getStrategyKind: function (config) {
        return String(object(object(config).live).strategy_kind || '').trim();
      },
      readCoinSelection: function (config, key, side) {
        var value = object(object(config).live)[key];
        var allowAll = key === 'approved_coins';
        return coinList(value && typeof value === 'object' && !Array.isArray(value) ? value[side] : value, allowAll);
      },
      newInstanceName: function (config) {
        return String(object(config.live).user || '').trim();
      },
      saveQuery: function (isNew) {
        return isV8 && isNew ? '?create_only=true' : '';
      },
      saveBody: function (config, overrideConfigs, expectedVersion) {
        if (!isV8) return { config: config };
        return {
          config: config,
          override_configs: object(object(overrideConfigs).files),
          expected_version: Number(expectedVersion || 0)
        };
      },
      backtestDraftRequest: function (apiBase, config, overrideConfigs) {
        if (!isV8) {
          return {
            url: String(apiBase || '') + '/draft',
            body: { config: config },
            page: String(apiBase || '') + '/draft-target'
          };
        }
        var origin = String(apiBase || '').replace(/\/api\/v8$/, '');
        return {
          url: origin + '/api/backtest-v8/optimize-draft',
          body: { config: config, override_configs: overrideConfigs || {} },
          page: origin + '/api/backtest-v8/main_page'
        };
      },
      configureUi: function () {
        document.querySelectorAll(isV8 ? '[data-v7-only]' : '[data-v8-only]').forEach(function (element) {
          element.classList.add('run-version-hidden');
        });
        document.querySelectorAll(isV8 ? '[data-v8-only]' : '[data-v7-only]').forEach(function (element) {
          element.classList.remove('run-version-hidden');
        });
        hideUnsupportedActions(this);
        var title = document.querySelector('.sb-title');
        if (title) title.textContent = isV8 ? 'Edit PB8 Instance' : 'Edit Instance';
        document.title = isV8 ? 'PBv8 Edit' : 'PBv7 Edit';
        var versionInput = document.getElementById('f-version');
        if (versionInput && isV8) versionInput.readOnly = true;
      },
      configureRuntimeMetadata: function (metadata) {
        if (!isV8) return Object.keys(sharedLiveFields);
        var params = object(object(metadata).params);
        var liveParams = object(params.live);
        var managed = configureFields(sharedLiveFields, liveParams);
        var distanceInput = document.getElementById(sharedLiveFields.limit_order_create_max_market_dist_pct);
        if (distanceInput && Object.prototype.hasOwnProperty.call(liveParams, 'limit_order_create_max_market_dist_pct')) {
          var label = distanceInput.closest('.form-group');
          label = label ? label.querySelector('label span') : null;
          if (label) label.textContent = 'limit_order_create_max_market_dist_pct';
        }
        this.managedLoggingKeys = configureFields(sharedLoggingFields, object(params.logging));
        this.managedMonitorKeys = configureFields(sharedMonitorFields, object(params.monitor));
        var advanced = document.getElementById('exp-advanced');
        if (advanced) {
          advanced.classList.toggle(
            'run-version-hidden',
            managed.length === 0 && this.managedLoggingKeys.length === 0 && this.managedMonitorKeys.length === 0
          );
        }
        this.managedLiveKeys = managed;
        return managed.slice();
      },
      configureRuntimeConfig: function (config) {
        if (!isV8) return [];
        config = object(config);
        return this.configureRuntimeMetadata({
          params: {
            live: object(config.live),
            logging: object(config.logging),
            monitor: object(config.monitor)
          }
        });
      }
    };
  }

  window.PBGuiRunEditorAdapter = { create: create };
}());
