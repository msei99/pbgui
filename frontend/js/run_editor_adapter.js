;(function () {
  'use strict';

  function object(value) {
    return value && typeof value === 'object' && !Array.isArray(value) ? value : {};
  }

  function create(version, options) {
    options = options || {};
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
      forced_mode_short: 'f-forced-short', hsl_signal_mode: 'f-hsl-signal-mode',
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
