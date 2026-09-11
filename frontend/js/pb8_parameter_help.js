/* Original local Passivbot documentation shared by all PB8 config editors. */
;(function () {
  'use strict';

  var FIELD_PATHS = {
    "f-archive-fetch": "live.enable_archive_candle_fetch",
    "f-auto-gs": "live.auto_gs",
    "f-bal-hyst": "live.balance_hysteresis_snap_pct",
    "f-bal-override": "live.balance_override",
    "f-candle-lock": "live.candle_lock_timeout_seconds",
    "f-churn-activation-count": "live.order_replacement_churn_gate_activation_count",
    "f-churn-market-dist": "live.order_replacement_churn_gate_market_dist_pct",
    "f-churn-stability-minutes": "live.order_replacement_churn_gate_stability_minutes",
    "f-churn-window-minutes": "live.order_replacement_churn_gate_window_minutes",
    "f-custom-endpoints-path": "live.custom_endpoints_path",
    "f-defer-broad-candle-warmup": "live.defer_broad_candle_warmup",
    "f-enable-forager-ws": "live.enable_forager_ws_candles",
    "f-exchange-symbol-cooldown": "live.exchange_symbol_unavailable_cooldown_hours",
    "f-exec-delay": "live.execution_delay_seconds",
    "f-fee-conversion-age": "live.fee_conversion_max_age_ms",
    "f-fee-pct-fallback": "live.fee_pct_fallback",
    "f-fee-pct-sanity": "live.fee_pct_sanity_abs_max",
    "f-fills-confirm-overlap": "live.fills_confirmation_overlap_minutes",
    "f-fills-recent-overlap": "live.fills_recent_overlap_minutes",
    "f-filter-min-cost": "live.filter_by_min_effective_cost",
    "f-forager-hysteresis": "live.forager_score_hysteresis_pct",
    "f-forager-ws-audit": "live.forager_ws_candle_rest_audit_minutes",
    "f-force-cold-startup": "live.force_cold_startup",
    "f-forced-long": "live.forced_mode_long",
    "f-forced-short": "live.forced_mode_short",
    "f-hedge-mode": "live.hedge_mode",
    "f-hsl-accept-incomplete": "live.hsl_accept_incomplete_history",
    "f-hsl-cooldown-policy": "live.hsl_position_during_cooldown_policy",
    "f-hsl-signal-mode": "live.hsl_signal_mode",
    "f-inactive-ttl": "live.inactive_coin_candle_ttl_minutes",
    "f-leverage": "live.leverage",
    "f-log-backup-count": "logging.backup_count",
    "f-log-debug-profiles": "logging.live_event_debug_profiles",
    "f-log-dir": "logging.dir",
    "f-log-max-bytes": "logging.max_bytes_mb",
    "f-log-persist": "logging.persist_to_file",
    "f-log-rotation": "logging.rotation",
    "f-logging-level": "logging.level",
    "f-long-npos": "bot.*.risk.n_positions",
    "f-long-twe": "bot.*.risk.total_wallet_exposure_limit",
    "f-margin-mode": "live.margin_mode_preference",
    "f-market-order-threshold": "live.market_order_near_touch_threshold",
    "f-market-orders": "live.market_orders_allowed",
    "f-market-snapshot-strategy": "live.market_snapshot_ticker_strategy",
    "f-max-active-tail-gap": "live.max_active_candle_tail_gap_minutes",
    "f-max-api-req": "live.max_concurrent_api_requests",
    "f-max-cancel": "live.max_n_cancellations_per_batch",
    "f-max-create": "live.max_n_creations_per_batch",
    "f-max-disk-candles": "live.max_disk_candles_per_symbol_per_tf",
    "f-max-forager-refresh": "live.max_forager_candle_refresh_seconds",
    "f-max-forager-stale": "live.max_forager_candle_staleness_minutes",
    "f-max-loss-pct": "live.max_realized_loss_pct",
    "f-max-mem-candles": "live.max_memory_candles_per_symbol",
    "f-max-ohlcv-fetches": "live.max_ohlcv_fetches_per_minute",
    "f-max-restarts": "live.max_n_restarts_per_day",
    "f-max-warmup-min": "live.max_warmup_minutes",
    "f-mem-snapshot": "logging.memory_snapshot_interval_minutes",
    "f-min-coin-age": "live.minimum_coin_age_days",
    "f-monitor-checkpoint": "monitor.checkpoint_interval_minutes",
    "f-monitor-compress": "monitor.compress_rotated_segments",
    "f-monitor-emit-candles": "monitor.emit_completed_candles",
    "f-monitor-enabled": "monitor.enabled",
    "f-monitor-max-bytes": "monitor.max_total_bytes",
    "f-monitor-price-interval": "monitor.price_tick_min_interval_ms",
    "f-monitor-raw-fills": "monitor.include_raw_fill_payloads",
    "f-monitor-retain-candles": "monitor.retain_candles",
    "f-monitor-retain-days": "monitor.retain_days",
    "f-monitor-retain-fills": "monitor.retain_fills",
    "f-monitor-retain-ticks": "monitor.retain_price_ticks",
    "f-monitor-root-dir": "monitor.root_dir",
    "f-monitor-rotation-mb": "monitor.event_rotation_mb",
    "f-monitor-rotation-minutes": "monitor.event_rotation_minutes",
    "f-monitor-snapshot-interval": "monitor.snapshot_interval_seconds",
    "f-order-match-tol": "live.order_match_tolerance_pct",
    "f-pnls-lookback": "live.pnls_max_lookback_days",
    "f-price-dist": "live.limit_order_create_max_market_dist_pct",
    "f-recv-window": "live.recv_window_ms",
    "f-short-npos": "bot.*.risk.n_positions",
    "f-short-twe": "bot.*.risk.total_wallet_exposure_limit",
    "f-startup-phase-budgets": "live.startup_phase_budgets",
    "f-strategy-kind": "live.strategy_kind",
    "f-time-in-force": "live.time_in_force",
    "f-user": "live.user",
    "f-vol-refresh": "logging.volume_refresh_info_threshold_seconds",
    "f-warmup-conc": "live.warmup_concurrency",
    "f-warmup-jitter": "live.warmup_jitter_seconds",
    "f-warmup-ratio": "live.warmup_ratio",
    "opted-btc-collateral-cap": "backtest.btc_collateral_cap",
    "opted-candle-interval": "backtest.candle_interval_minutes",
    "opted-compress-results": "optimize.compress_results_file",
    "opted-crossover-eta": "optimize.crossover_eta",
    "opted-deap-population-size": "optimize.population_size",
    "opted-end-date": "backtest.end_date",
    "opted-fine-tune-params": "runtime.fine_tune_params",
    "opted-hsl-signal-mode": "live.hsl_signal_mode",
    "opted-iters": "optimize.iters",
    "opted-log-level": "logging.level",
    "opted-max-pending-starting-evals": "optimize.max_pending_starting_evals_per_cpu",
    "opted-memory-snapshot": "logging.memory_snapshot_interval_minutes",
    "opted-min-coin-age": "live.minimum_coin_age_days",
    "opted-mutation-eta": "optimize.mutation_eta",
    "opted-mutation-indpb": "optimize.mutation_indpb",
    "opted-mutation_prob-mode": "optimize.pymoo.shared.mutation_prob",
    "opted-mutation_prob-value": "optimize.pymoo.shared.mutation_prob",
    "opted-mutation_prob_per_variable-mode": "optimize.pymoo.shared.mutation_prob_per_variable",
    "opted-mutation_prob_per_variable-value": "optimize.pymoo.shared.mutation_prob_per_variable",
    "opted-n-cpus": "optimize.n_cpus",
    "opted-offspring-multiplier": "optimize.offspring_multiplier",
    "opted-ohlcv-source-dir": "backtest.ohlcv_source_dir",
    "opted-opt-backend": "optimize.backend",
    "opted-pareto-max-size": "optimize.pareto_max_size",
    "opted-polish-bounds-mode": "runtime.polish_bounds_mode",
    "opted-polish-pct": "runtime.polish_percentage",
    "opted-pymoo-algorithm": "optimize.pymoo.algorithm",
    "opted-pymoo-crossover-eta": "optimize.pymoo.shared.crossover_eta",
    "opted-pymoo-crossover-prob-var": "optimize.pymoo.shared.crossover_prob_var",
    "opted-pymoo-eliminate-duplicates": "optimize.pymoo.shared.eliminate_duplicates",
    "opted-pymoo-mutation-eta": "optimize.pymoo.shared.mutation_eta",
    "opted-pymoo-mutation-prob-value": "optimize.pymoo.shared.mutation_prob_var",
    "opted-pymoo-ref-dir-method": "optimize.pymoo.algorithms.nsga3.ref_dirs.method",
    "opted-pymoo-ref-dir-npartitions": "optimize.pymoo.algorithms.nsga3.ref_dirs.n_partitions",
    "opted-rng-seed": "optimize.seed",
    "opted-start-date": "backtest.start_date",
    "opted-starting-balance": "backtest.starting_balance",
    "opted-strategy-kind": "live.strategy_kind",
    "opted-volume-refresh-threshold": "logging.volume_refresh_info_threshold_seconds",
    "opted-write-all-results": "optimize.write_all_results"
};

  function canonicalPath(value) {
    var key = String(value || '').trim().replace(/^optimize\.bounds\./, 'bot.');
    if (/^(long|short)\./.test(key)) key = 'bot.' + key;
    return key.replace(/^bot\.(long|short)\./, 'bot.*.');
  }

  function resolveHelp(entries, field, editor) {
    var label = String(field.label || '').trim().replace(/\s*\([^)]*\)$/, '');
    var explicit = field.path || FIELD_PATHS[field.id];
    if (explicit) return entries[canonicalPath(explicit)] || null;
    if (!/^[a-z][a-z0-9_.*]*$/.test(label)) return null;
    var exact = canonicalPath(label);
    if (entries[exact]) return entries[exact];
    if (/^(risk|strategy|forager|unstuck|hsl)\./.test(exact)) return entries['bot.*.' + exact] || null;
    var scope = editor === 'run' ? 'live' : editor === 'backtest' ? 'backtest' : 'optimize';
    if (/^opted-gpu-/.test(field.id || '')) {
      scope = /^opted-gpu-halving-/.test(field.id) ? 'optimize.gpu.successive_halving' : 'optimize.gpu';
    }
    if (entries[scope + '.' + label]) return entries[scope + '.' + label];
    var candidates = Object.keys(entries).filter(function(key) { return key.endsWith('.' + label); });
    // Never borrow another parameter's explanation merely because its leaf name matches.
    if (candidates.length === 1) return entries[candidates[0]];
    return null;
  }

  function plainText(markdown) {
    return String(markdown || '')
      .replace(/```[^\n]*\n?/g, '')
      .replace(/\[([^\]]+)\]\([^)]*\)/g, '$1')
      .replace(/\*\*([^*]+)\*\*/g, '$1')
      .replace(/`([^`]+)`/g, '$1')
      .replace(/([^\n])\n(?=[A-Za-z`(])/g, '$1 ')
      .trim();
  }

  function isPlaceholder(text) {
    return /reported by the installed runtime|^Adjust the optimize range for |^PB8 .*schema reported/.test(String(text || ''));
  }

  window.PBGuiPB8ParameterHelp = { resolveHelp: resolveHelp, plainText: plainText, canonicalPath: canonicalPath };
  function install() {
    var base = typeof API_BASE === 'string' ? API_BASE : '';
    var match = base.match(/^(.*)\/api\/(v8|optimize-v8|backtest-v8)$/);
    if (!match) return;
    var editor = match[2] === 'v8' ? 'run' : match[2] === 'backtest-v8' ? 'backtest' : 'optimize';
    var entries = {};
    var disposed = false;
    var controller = new AbortController();
    var hideTimer = null;
    var active = null;
    var tooltip = document.createElement('div');
    tooltip.id = 'pb8-parameter-tooltip';
    tooltip.setAttribute('role', 'tooltip');
    tooltip.hidden = true;
    document.body.appendChild(tooltip);

    function fieldFor(target) {
      var group = target.closest('.form-group, .runtime-override-field') || target.closest('label');
      var control = group && group.querySelector('input, select, textarea');
      var path = target.getAttribute('data-param-path');
      if (control) {
        var override = control.getAttribute('data-pb8-enable-override');
        path = path || (override ? 'optimize.enable_overrides.' + override : control.getAttribute('data-runtime-override'));
        var extraSection = control.getAttribute('data-extra-param-section');
        var extraKey = control.getAttribute('data-extra-param-key');
        if (extraSection && extraKey) path = path || extraSection + '.' + extraKey;
      }
      return {label: target.textContent, id: control ? control.id : '', path: path};
    }

    function hide() {
      clearTimeout(hideTimer);
      tooltip.hidden = true;
      active = null;
    }

    function position(event) {
      if (tooltip.hidden || tooltip.contains(event.target)) return;
      if (!active || !active.isConnected) { hide(); return; }
      var rect = active.getBoundingClientRect();
      var x = event.clientX == null ? rect.left : event.clientX;
      var y = event.clientY == null ? rect.bottom : event.clientY;
      var width = tooltip.offsetWidth;
      var height = tooltip.offsetHeight;
      tooltip.style.left = Math.max(8, Math.min(x + 14, window.innerWidth - width - 8)) + 'px';
      tooltip.style.top = Math.max(8, y + height + 18 <= window.innerHeight ? y + 14 : y - height - 10) + 'px';
    }

    function show(event) {
      if (disposed || !event.target.closest || tooltip.contains(event.target)) return;
      var target = event.target.closest('[data-tip], label, .bound-key-text, .runtime-override-label');
      if (!target && event.type === 'focusin') {
        var group = event.target.closest('.form-group');
        target = group && group.querySelector('[data-tip], label');
      }
      if (!target) return;
      var help = resolveHelp(entries, fieldFor(target), editor);
      if (!help) {
        if (isPlaceholder(target.getAttribute('data-tip'))) target.removeAttribute('data-tip');
        return;
      }
      clearTimeout(hideTimer);
      var text = plainText(help.text);
      target.setAttribute('data-tip', text);
      tooltip.textContent = text + '\n\nPassivbot · docs/' + help.source;
      active = target;
      tooltip.hidden = false;
      var legacy = document.getElementById('data-tip-tooltip');
      if (legacy) legacy.style.display = 'none';
      position(event);
      // The shared original-text tooltip owns this hover, including long, scrollable text.
      if (event.type === 'mouseover') event.stopImmediatePropagation();
    }

    function leave(event) {
      if (tooltip.hidden) return;
      var next = event.relatedTarget;
      if (next && (tooltip.contains(next) || (active && active.contains(next)))) return;
      clearTimeout(hideTimer);
      hideTimer = setTimeout(hide, 150);
    }

    function escape(event) { if (event.key === 'Escape') hide(); }
    function hold() { clearTimeout(hideTimer); }
    tooltip.addEventListener('mouseenter', hold);
    tooltip.addEventListener('mouseleave', leave);
    document.addEventListener('mouseover', show, true);
    document.addEventListener('focusin', show, true);
    document.addEventListener('mouseout', leave);
    document.addEventListener('focusout', leave);
    document.addEventListener('mousemove', position);
    document.addEventListener('keydown', escape);

    var timeout = setTimeout(function() { controller.abort(); }, 10000);
    fetch(match[1] + '/api/v8/parameter-help', {credentials: 'same-origin', signal: controller.signal})
      .then(function(response) {
        if (!response.ok) throw new Error('PB8 parameter documentation: HTTP ' + response.status);
        return response.json();
      })
      .then(function(payload) { if (!disposed) entries = payload.entries || {}; })
      .catch(function(error) { if (!disposed) console.error('Loading PB8 parameter documentation failed', error); })
      .finally(function() { clearTimeout(timeout); });

    window.addEventListener('pagehide', function() {
      disposed = true;
      controller.abort();
      clearTimeout(timeout);
      hide();
      tooltip.remove();
      document.removeEventListener('mouseover', show, true);
      document.removeEventListener('focusin', show, true);
      document.removeEventListener('mouseout', leave);
      document.removeEventListener('focusout', leave);
      document.removeEventListener('mousemove', position);
      document.removeEventListener('keydown', escape);
    }, {once: true});
    return true;
  }

  if (install()) {
    window.addEventListener('pageshow', function(event) {
      if (event.persisted) install();
    });
  }
})();
