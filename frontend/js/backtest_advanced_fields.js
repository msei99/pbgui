(function(root) {
  'use strict';

  var MARKET_FIELDS = [
    'qty_step', 'price_step', 'min_qty', 'min_cost',
    'c_mult'
  ];

  function setOwn(target, key, value) {
    Object.defineProperty(target, key, {
      value: value,
      enumerable: true,
      configurable: true,
      writable: true
    });
  }

  function cloneObject(value) {
    var result = {};
    if (!value || typeof value !== 'object' || Array.isArray(value)) return result;
    Object.keys(value).forEach(function(key) { setOwn(result, key, value[key]); });
    return result;
  }

  function flattenMarketSettings(value) {
    if (value !== null && value !== undefined && (typeof value !== 'object' || Array.isArray(value))) {
      throw new TypeError('market_settings must be an object');
    }
    var source = value || {};
    var rows = [];
    if (source.overrides !== null && source.overrides !== undefined && (typeof source.overrides !== 'object' || Array.isArray(source.overrides))) {
      throw new TypeError('market_settings.overrides must be an object');
    }
    var globalOverrides = source.overrides || {};
    Object.keys(globalOverrides).sort().forEach(function(coin) {
      if (!globalOverrides[coin] || typeof globalOverrides[coin] !== 'object' || Array.isArray(globalOverrides[coin])) {
        throw new TypeError('market_settings.overrides.' + coin + ' must be an object');
      }
      rows.push({ scope: 'global', exchange: '', coin: coin, values: cloneObject(globalOverrides[coin]) });
    });
    if (source.overrides_by_exchange !== null && source.overrides_by_exchange !== undefined && (typeof source.overrides_by_exchange !== 'object' || Array.isArray(source.overrides_by_exchange))) {
      throw new TypeError('market_settings.overrides_by_exchange must be an object');
    }
    var byExchange = source.overrides_by_exchange || {};
    Object.keys(byExchange).sort().forEach(function(exchange) {
      if (!byExchange[exchange] || typeof byExchange[exchange] !== 'object' || Array.isArray(byExchange[exchange])) {
        throw new TypeError('market_settings.overrides_by_exchange.' + exchange + ' must be an object');
      }
      var coinOverrides = byExchange[exchange];
      Object.keys(coinOverrides).sort().forEach(function(coin) {
        if (!coinOverrides[coin] || typeof coinOverrides[coin] !== 'object' || Array.isArray(coinOverrides[coin])) {
          throw new TypeError('market_settings override ' + exchange + '.' + coin + ' must be an object');
        }
        rows.push({ scope: 'exchange', exchange: exchange, coin: coin, values: cloneObject(coinOverrides[coin]) });
      });
    });
    return rows;
  }

  function marketSettingsExtras(value) {
    if (value !== null && value !== undefined && (typeof value !== 'object' || Array.isArray(value))) {
      throw new TypeError('market_settings must be an object');
    }
    var source = cloneObject(value);
    delete source.overrides;
    delete source.overrides_by_exchange;
    return source;
  }

  function serializeMarketSettings(rows, extras) {
    var result = cloneObject(extras);
    result.overrides = {};
    result.overrides_by_exchange = {};
    (rows || []).forEach(function(row) {
      var coin = String(row.coin || '').trim().toUpperCase();
      if (!coin) return;
      var values = cloneObject(row.values);
      if (row.scope === 'exchange') {
        var exchange = String(row.exchange || '').trim().toLowerCase();
        if (!exchange) return;
        if (!Object.prototype.hasOwnProperty.call(result.overrides_by_exchange, exchange)) {
          setOwn(result.overrides_by_exchange, exchange, {});
        }
        setOwn(result.overrides_by_exchange[exchange], coin, values);
      } else {
        setOwn(result.overrides, coin, values);
      }
    });
    return result;
  }

  function visibleMetricsState(value) {
    if (value === null || value === undefined) return { mode: 'default', selected: [] };
    if (!Array.isArray(value)) throw new TypeError('visible_metrics must be null or a list');
    if (value.length === 0) return { mode: 'all', selected: [] };
    if (value.some(function(item) { return typeof item !== 'string' || !item.trim(); })) {
      throw new TypeError('visible_metrics entries must be non-empty strings');
    }
    return {
      mode: 'custom',
      selected: value.slice()
    };
  }

  function metricCategory(metric) {
    var name = String(metric || '').toLowerCase();
    if (name.indexOf('hard_stop_') === 0) return 'Hard Stop';
    if (/fills|position_|positions_|trade_|win_rate|volume_|entry_interval/.test(name)) return 'Trading Activity';
    if (/drawdown|recovery|underwater|shortfall|paper_loss|exposure|equity_balance/.test(name)) return 'Risk & Recovery';
    if (/ratio|sharpe|sortino|calmar|sterling|omega/.test(name)) return 'Performance Ratios';
    return 'Returns & Growth';
  }

  root.PBGuiBacktestAdvancedFields = {
    MARKET_FIELDS: MARKET_FIELDS.slice(),
    flattenMarketSettings: flattenMarketSettings,
    marketSettingsExtras: marketSettingsExtras,
    serializeMarketSettings: serializeMarketSettings,
    visibleMetricsState: visibleMetricsState,
    metricCategory: metricCategory
  };
})(window);
