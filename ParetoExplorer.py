#!/usr/bin/env python3
"""
ParetoExplorer - The Genius Pareto Explorer Streamlit App
6-Stage Progressive Disclosure Interface for Multi-Objective Optimization Analysis

Usage:
    streamlit run ParetoExplorer.py -- <results_path>
    
Example:
    streamlit run ParetoExplorer.py -- /path/to/optimize_results/2025-12-24T07_04_00_binance_bybit_2182days_DOGE_e020d49f
"""

import streamlit as st
import sys
import os
from pathlib import Path
import numpy as np
import pandas as pd
from typing import List, Dict, Optional, Any, Tuple

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

from ParetoDataLoader import ParetoDataLoader, ConfigMetrics
from ParetoVisualizations import ParetoVisualizations
import pbgui_purefunc as pbfunc
from Config import CURRENCY_METRICS, SHARED_METRICS


class ParetoExplorer:
    """Pareto Explorer - Main Application"""
    
    def __init__(self, results_path: str):
        """
        Initialize Explorer
        
        Args:
            results_path: Path to optimize_results directory
        """
        self.results_path = results_path
        self.loader = None
        self.viz = None
        
    def run(self):
        """Main entry point - runs the Streamlit app"""
        
        # Custom CSS
        st.markdown("""
        <style>
        .big-font {
            font-size:20px !important;
            font-weight: bold;
        }
        .metric-card {
            background-color: #f0f2f6;
            padding: 20px;
            border-radius: 10px;
            margin: 10px 0;
        }
        </style>
        """, unsafe_allow_html=True)
        
        # Initialize session state from pbgui.ini BEFORE loading data
        if 'load_strategy' not in st.session_state:
            # Try to load from ini, fallback to default
            saved_strategy = pbfunc.load_ini('pareto', 'load_strategy')
            if saved_strategy:
                st.session_state['load_strategy'] = saved_strategy.split(',')
            else:
                st.session_state['load_strategy'] = ['performance', 'robustness', 'sharpe']
        
        if 'max_configs' not in st.session_state:
            # Try to load from ini, fallback to default
            saved_max = pbfunc.load_ini('pareto', 'max_configs')
            if saved_max:
                st.session_state['max_configs'] = int(saved_max)
            else:
                st.session_state['max_configs'] = 2000
        
        # Initialize two-stage loading state
        if 'all_results_loaded' not in st.session_state:
            st.session_state['all_results_loaded'] = False
        
        # Initialize viz_type early to preserve across reruns (especially after loading all_results)
        if 'viz_type' not in st.session_state:
            st.session_state['viz_type'] = "2D Scatter"
        
        # Get load strategy and max_configs from session state (convert to tuple for caching)
        load_strategy = st.session_state.get('load_strategy', ['performance', 'robustness', 'sharpe'])
        max_configs = st.session_state.get('max_configs', 2000)
        load_strategy_tuple = tuple(sorted(load_strategy))  # Sorted tuple for stable cache key
        all_results_loaded = st.session_state.get('all_results_loaded', False)
        
        # Load data (with caching and two-stage approach)
        # Stage 1: Always load Pareto JSONs (fast)
        # Stage 2: Optionally load all_results.bin (slow)
        load_result = ParetoExplorer._load_data(self.results_path, load_strategy_tuple, max_configs, all_results_loaded)
        if load_result.get("error"):
            err = load_result.get("error")
            if err:
                st.error(f"❌ {err}")
            else:
                st.error("❌ Failed to load data")

            st.info(f"**Results Path:** `{self.results_path}`")

            # For legacy-format runs, avoid noisy follow-up errors.
            if err != "Old format not supported":
                # Check if path exists
                all_results_path = os.path.join(self.results_path, "all_results.bin")
                if not os.path.exists(self.results_path):
                    st.error(f"Directory not found: `{self.results_path}`")
                elif not os.path.exists(all_results_path):
                    st.error("File not found: `all_results.bin`")
                    st.caption("Make sure the optimization completed successfully and all_results.bin was created.")
                elif load_result.get("traceback"):
                    with st.expander("Details", expanded=False):
                        st.code(load_result["traceback"])
                else:
                    st.error("Unknown error loading data. Check the terminal for details.")

            st.stop()

        # Unpack cached data
        self.loader = load_result["loader"]
        self.viz = load_result["viz"]
        load_stats = load_result["load_stats"]
        
        # IMPORTANT: Store original configs in loader to prevent filtering corruption
        # Subsequent get_view_slice() calls will use this original list
        if not hasattr(self.loader, '_full_configs'):
            self.loader._full_configs = self.loader.configs.copy()
        
        # Store reference in explorer too
        self._original_configs = self.loader._full_configs
        
        # Render sidebar FIRST (includes display range slider)
        # This must happen before _get_view_configs() so the slider state is available
        self._render_sidebar(load_stats)
        
        # Get view slice AFTER sidebar is rendered (uses slider state)
        self.view_configs = self._get_view_configs()
        
        # Temporarily replace loader.configs with view for visualizations
        # This will be used by all visualization methods
        self.loader.configs = self.view_configs
        
        # IMPORTANT: Reinitialize visualizations AFTER filtering to ensure they use the correct data
        from ParetoVisualizations import ParetoVisualizations
        self.viz = ParetoVisualizations(self.loader)
        
        # Main content area
        stage = st.session_state.get('stage', 'Command Center')
        
        if stage == 'Command Center':
            self._show_command_center()
        elif stage == 'Pareto Playground':
            self._show_pareto_playground()
        elif stage == 'Deep Intelligence':
            self._show_deep_intelligence()
    
    def _get_view_configs(self) -> List:
        """
        Get currently visible configs based on view_range filter
        
        Returns:
            List of configs to display (filtered by view_range if in full mode)
        """
        all_results_loaded = st.session_state.get('all_results_loaded', False)
        
        # In fast mode, show all (Pareto only anyway)
        if not all_results_loaded:
            return self.loader.configs
        
        # In full mode, check if view_range_slider is set
        if 'view_range_slider' in st.session_state:
            start, end = st.session_state.view_range_slider
            view_configs = self.loader.get_view_slice(start, end)
            return view_configs
        else:
            # No filter set, show all
            return self.loader.configs
    
    @st.cache_resource
    def _load_data(results_path: str, load_strategy: tuple, max_configs: int, all_results_loaded: bool):
        """
        Two-stage data loading (uses @st.cache_resource)
        
        Stage 1 (Default): Load only Pareto configs from pareto/*.json (fast, ~0.5s)
        Stage 2 (Optional): Load all_results.bin with all configs (slow, ~10-30s)
        
        Args:
            results_path: Path to optimization results
            load_strategy: Tuple of strategy names (for cache key)
            max_configs: Max configs to load from all_results.bin
            all_results_loaded: If True, load all_results.bin; else only pareto JSONs
        
        Returns:
            Dict with keys: loader, viz, load_stats, error, traceback
        """
        try:
            loader = ParetoDataLoader(results_path)
            
            if not all_results_loaded:
                # STAGE 1: Fast mode - Load only Pareto configs from JSON
                with st.spinner("⚡ Loading Pareto configs from JSON..."):
                    success = loader.load_pareto_jsons_only()
                    
                    if not success:
                        if getattr(loader, "last_error", None):
                            return {"loader": None, "viz": None, "load_stats": None, "error": loader.last_error, "traceback": None}
                        # Fallback: Try all_results.bin if no pareto JSONs found
                        st.warning("⚠️ No pareto/*.json files found. Loading all_results.bin...")
                        strategy_list = list(load_strategy) if load_strategy else ['performance']
                        success = loader.load(load_strategy=strategy_list, max_configs=max_configs)
                        
                        if not success:
                            if getattr(loader, "last_error", None):
                                return {"loader": None, "viz": None, "load_stats": None, "error": loader.last_error, "traceback": None}
                            return {"loader": None, "viz": None, "load_stats": None, "error": "Failed to load data from both pareto JSONs and all_results.bin", "traceback": None}
            else:
                # STAGE 2: Full mode - Load all configs from all_results.bin
                # Show file size
                all_results_path = os.path.join(results_path, "all_results.bin")
                if os.path.exists(all_results_path):
                    file_size_mb = os.path.getsize(all_results_path) / (1024 * 1024)
                    
                    # Show progress in sidebar to avoid conflict with "Running..." text
                    with st.sidebar:
                        st.markdown("---")
                        status_text = st.empty()
                        progress_bar = st.progress(0)
                    
                    # Progress callback for loader
                    def update_progress(current, total, message):
                        progress = current / total if total > 0 else 0
                        status_text.markdown(f"**🔄 {message}**")
                        progress_bar.progress(progress)
                    
                    # Initial message
                    update_progress(0, 1, f"Preparing to load {file_size_mb:.1f} MB...")
                    
                    strategy_list = list(load_strategy) if load_strategy else ['performance']
                    success = loader.load(load_strategy=strategy_list, max_configs=max_configs, progress_callback=update_progress)
                    
                    # Clear progress indicators from sidebar
                    status_text.empty()
                    progress_bar.empty()
                else:
                    with st.spinner("🔄 Loading all configs from all_results.bin..."):
                        strategy_list = list(load_strategy) if load_strategy else ['performance']
                        success = loader.load(load_strategy=strategy_list, max_configs=max_configs)
                    
                if not success:
                    if getattr(loader, "last_error", None):
                        return {"loader": None, "viz": None, "load_stats": None, "error": loader.last_error, "traceback": None}
                    return {"loader": None, "viz": None, "load_stats": None, "error": "Failed to load all_results.bin", "traceback": None}
            
            viz = ParetoVisualizations(loader)
            load_stats = loader.load_stats
            
            return {"loader": loader, "viz": viz, "load_stats": load_stats, "error": None, "traceback": None}
            
        except Exception as e:
            import traceback
            return {
                "loader": None,
                "viz": None,
                "load_stats": None,
                "error": "Exception during data loading",
                "traceback": traceback.format_exc(),
            }
    
    def _render_sidebar(self, load_stats: Dict):
        """Render sidebar with navigation and info"""
        
        def _set_all_results_loaded(value: bool):
            st.session_state['all_results_loaded'] = value

        with st.sidebar:
            # === DATA SOURCE SECTION ===
            st.markdown("### 📊 Data Source")
            
            all_results_loaded = st.session_state.get('all_results_loaded', False)
            
            if not all_results_loaded:
                # FAST MODE: Showing Pareto only
                num_pareto = load_stats.get('pareto_configs', 0)
                st.success(f"✅ Loaded **{num_pareto} Pareto configs** from JSON")
                st.info("💡 **Fast mode:** Showing best configs only")
                
                # Button to load all results
                st.button(
                    "🔄 Load all_results.bin",
                    help="Load all optimization results to compare with non-Pareto configs. This may take 10-30 seconds.",
                    use_container_width=True,
                    on_click=_set_all_results_loaded,
                    args=(True,),
                )
            else:
                # FULL MODE: All results loaded
                num_total = load_stats.get('selected_configs', 0)
                num_pareto = load_stats.get('pareto_configs', 0)
                num_non_pareto = num_total - num_pareto
                
                st.success(f"✅ Loaded **{num_total:,} total configs**")
                st.caption(f"📋 {num_pareto} Pareto + {num_non_pareto:,} others")
                
                # Button to switch back to fast mode
                st.button(
                    "⚡ Switch to Fast Mode",
                    help="Show only Pareto configs for faster performance",
                    use_container_width=True,
                    on_click=_set_all_results_loaded,
                    args=(False,),
                )
            
            st.markdown("---")
            
            # Stats overview
            if self.loader:
                if all_results_loaded:
                    st.metric("Total Configs", f"{load_stats['selected_configs']:,}")
                st.metric("Pareto Front", load_stats['pareto_configs'])
                st.metric("Scenarios", len(load_stats['scenarios']))
                
                # Load info expander - NOW WITH DETAILED INFO
                with st.expander("📊 Load Details", expanded=False):
                    if all_results_loaded:
                        st.markdown(f"**✅ Loaded:** {load_stats['total_parsed']:,} configs from all_results.bin")
                        st.markdown(f"**⏳ Strategy:** {', '.join(load_stats['load_strategy'])}")
                        st.markdown(f"**✅ Selected:** {load_stats['selected_configs']:,} configs")
                    else:
                        st.markdown(f"**⚡ Fast Mode:** Loaded {load_stats['pareto_configs']} Pareto configs from JSON")
                        st.markdown(f"**📂 Source:** pareto/*.json")
                    
                    st.markdown(f"**📋 Pareto:** {load_stats['pareto_configs']} configs")
                    
                    st.markdown("---")
                    st.markdown("**Scoring Metrics:**")
                    for metric in load_stats['scoring_metrics']:
                        st.markdown(f"  • `{metric}`")
                    
                    st.markdown("---")
                    st.markdown("**Scenarios:**")
                    for scenario in load_stats['scenarios']:
                        st.markdown(f"  • {scenario}")
                
                st.markdown("---")
            
            # Display Range Filter (only in full mode)
            if all_results_loaded and self.loader and hasattr(self, '_original_configs') and len(self._original_configs) > 0:
                st.subheader("📊 Display Range")
                
                max_configs = len(self._original_configs)  # Use original count, not filtered!
                st.caption(f"**Loaded:** {max_configs:,} configs total")
                
                # Initialize or validate slider value
                if 'view_range_slider' not in st.session_state:
                    st.session_state.view_range_slider = (0, min(500, max_configs))
                else:
                    # Ensure current value doesn't exceed max_configs
                    current_start, current_end = st.session_state.view_range_slider
                    if current_end > max_configs:
                        st.session_state.view_range_slider = (
                            min(current_start, max_configs),
                            max_configs
                        )
                
                # Range slider - use current session state value
                current_value = st.session_state.view_range_slider
                
                view_range = st.slider(
                    "Show rank range",
                    min_value=0,
                    max_value=max_configs,
                    value=current_value,
                    step=10,
                    help="Filter which configs to display. Pareto front is computed only for visible range.",
                    key='view_range_slider'
                )
                
                # Show info
                num_showing = view_range[1] - view_range[0]
                st.info(f"📍 Showing **{num_showing:,}** configs (Rank {view_range[0]+1}-{view_range[1]})")
                
                st.markdown("---")
            
            # Navigation
            st.subheader("🗺️ Navigate")
            
            stages = [
                "🎯 Command Center",
                "🎨 Pareto Playground", 
                "🧠 Deep Intelligence",
            ]
            
            # Remove emoji for session state key
            stage_keys = [s.split(' ', 1)[1] for s in stages]
            
            # Get current stage from session state
            current_stage = st.session_state.get('stage', 'Command Center')
            
            # Find index of current stage
            try:
                current_index = stage_keys.index(current_stage)
            except ValueError:
                current_index = 0  # Default to Command Center
            
            selected = st.radio(
                "Choose Stage:",
                stages,
                index=current_index,
                key='stage_selector',
                label_visibility='collapsed'
            )
            
            # Store selected stage
            st.session_state['stage'] = selected.split(' ', 1)[1]
            
            st.markdown("---")
            
            # Quick actions
            st.subheader("⚡ Quick Actions")
            
            if st.button("🔄 Reload Data", width='stretch'):
                st.cache_resource.clear()
                st.rerun()
            
            if st.button("📊 Export DataFrame", width='stretch'):
                if self.loader:
                    df = self.loader.to_dataframe(pareto_only=False)
                    csv = df.to_csv(index=False)
                    st.download_button(
                        "💾 Download CSV",
                        csv,
                        "pareto_analysis.csv",
                        "text/csv",
                        width='stretch'
                    )
    
    def _show_config_details(self, config_index, key_prefix: str = "config_details"):
        """Show detailed config view with backtest option in a modal dialog"""
        
        config = self.loader.get_config_by_index(config_index)
        if not config:
            st.error(f"❌ Config #{config_index} not found")
            return
        
        st.subheader(f"📋 Configuration #{config_index}")
        
        # Metrics overview
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**📊 Performance**")
            
            # Get weighted toggle state
            use_weighted = st.session_state.get('use_weighted_metrics', True)
            
            # Define help texts for common metrics
            metric_helps = {
                'adg_w_usd': "Average Daily Gain (weighted) in USD - Daily profit rate accounting for wallet exposure",
                'adg_usd': "Average Daily Gain in USD - Daily profit rate",
                'sharpe_ratio_usd': "Risk-adjusted return metric - Higher is better. Measures excess return per unit of risk",
                'sharpe_ratio_w_usd': "Weighted Sharpe Ratio - Risk-adjusted return accounting for wallet exposure",
                'gain_usd': "Total profit multiplier - How many times the initial balance was gained",
                'calmar_ratio_usd': "Return vs maximum drawdown - Higher is better. Measures profit relative to worst loss",
                'calmar_ratio_w_usd': "Weighted Calmar Ratio - Return vs maximum drawdown accounting for wallet exposure",
                'sortino_ratio_usd': "Downside risk-adjusted return - Like Sharpe but only penalizes downside volatility",
                'sortino_ratio_w_usd': "Weighted Sortino Ratio - Downside risk-adjusted return accounting for wallet exposure",
                'omega_ratio_usd': "Probability-weighted ratio of gains vs losses - Higher is better",
                'omega_ratio_w_usd': "Weighted Omega Ratio - Probability-weighted ratio accounting for wallet exposure",
                'sterling_ratio_usd': "Return vs average drawdown - Consistency of returns relative to typical losses",
                'sterling_ratio_w_usd': "Weighted Sterling Ratio - Return vs average drawdown accounting for wallet exposure",
                'drawdown_worst_usd': "Maximum portfolio decline - Lower is better. Worst equity drop from peak",
            }
            
            # Convert scoring metrics to weighted/unweighted versions
            display_metrics = []
            for metric in self.loader.scoring_metrics[:3]:
                if use_weighted:
                    # Try to use weighted version if available
                    weighted_metric = metric.replace('_usd', '_w_usd') if '_w_' not in metric else metric
                    if weighted_metric in config.suite_metrics:
                        display_metrics.append(weighted_metric)
                    elif metric in config.suite_metrics:
                        display_metrics.append(metric)
                else:
                    # Use unweighted version
                    unweighted_metric = metric.replace('_w_usd', '_usd')
                    if unweighted_metric in config.suite_metrics:
                        display_metrics.append(unweighted_metric)
                    elif metric in config.suite_metrics:
                        display_metrics.append(metric)
            
            for metric in display_metrics:
                if metric in config.suite_metrics:
                    help_text = metric_helps.get(metric, f"{metric.replace('_', ' ').title()} - Performance metric")
                    st.metric(
                        metric.replace('_', ' ').title(), 
                        f"{config.suite_metrics[metric]:.6f}",
                        help=help_text
                    )
        
        with col2:
            st.markdown("**🎯 Trading Style**")
            style = self.loader.compute_trading_style(config)
            st.markdown(f"**{style}**")
            
            if 'positions_held_per_day' in config.suite_metrics:
                st.metric("Positions/Day", f"{config.suite_metrics['positions_held_per_day']:.2f}",
                         help="Average number of positions opened per day - Higher = more active trading")
            
            if 'position_held_hours_mean' in config.suite_metrics:
                st.metric("Avg Hold Hours", f"{config.suite_metrics['position_held_hours_mean']:.1f}",
                         help="Average time positions are held open - Lower = faster turnover, scalping style")
        
        with col3:
            st.markdown("**💪 Robustness**")
            robust = self.loader.compute_overall_robustness(config)
            stars = "⭐" * int(robust * 5)
            st.metric("Overall Score", f"{robust:.2f}",
                     help="Consistency score (0-1) - Higher = more stable across scenarios. Calculated as 1/(1+CV)")
            st.markdown(f"**{stars}**")
            
            # Show risk score if available
            risk_scores = self.loader.compute_risk_profile_score(config)
            if risk_scores:
                st.metric("Risk Score", f"{risk_scores['overall']:.1f}/10",
                         help="Combined risk assessment - Lower score = safer strategy. Based on drawdown, volatility, and recovery metrics")
        
        st.markdown("---")

        # Load full config once for both display and actions
        full_config_data = None
        try:
            full_config_data = self.loader.get_full_config(config.config_index)
        except Exception:
            full_config_data = None
        
        # Full config
        col_left, col_right = st.columns(2)
        
        with col_left:
            with st.expander("📋 Full Configuration", expanded=True):
                try:
                    if full_config_data:
                        st.json(full_config_data)
                    else:
                        st.warning("⚠️ Full config not available - showing bot params only")
                        st.json(config.bot_params)
                except Exception as e:
                    st.error(f"❌ Error loading config: {str(e)}")
                    st.json(config.bot_params)
        
        with col_right:
            with st.expander("📊 All Metrics", expanded=True):
                metrics_df = []
                for metric, value in sorted(config.suite_metrics.items()):
                    metrics_df.append({"Metric": metric, "Value": f"{value:.6f}"})
                st.dataframe(metrics_df, height=400, hide_index=True)
        
        # Backtest button
        st.markdown("---")
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            if st.button(
                "🚀 Run Backtest",
                width='stretch',
                type="primary",
                key=f"{key_prefix}_bt_modal_{config_index}",
            ):
                try:
                    import BacktestV7
                    from pbgui_func import get_navi_paths, pb7dir
                    from pathlib import Path
                    import json
                    import time

                    if not full_config_data:
                        st.error("❌ Full config data not available")
                        st.stop()
                    
                    config_dir = Path(f'{pb7dir()}/configs/pareto_selected')
                    config_dir.mkdir(parents=True, exist_ok=True)
                    
                    timestamp = int(time.time())
                    config_filename = f"pareto_config_{config.config_index}_{timestamp}.json"
                    config_path = config_dir / config_filename
                    
                    with open(config_path, 'w') as f:
                        json.dump(full_config_data, f, indent=2)
                    
                    # Cleanup backtest session state
                    for key in ["bt_v7_queue", "bt_v7_results", "bt_v7_edit_symbol", 
                               "config_v7_archives", "config_v7_config_archive"]:
                        if key in st.session_state:
                            del st.session_state[key]
                    
                    st.session_state.bt_v7 = BacktestV7.BacktestV7Item(str(config_path))
                    st.switch_page(get_navi_paths()["V7_BACKTEST"])
                    
                except Exception as e:
                    st.error(f"❌ Error preparing backtest: {str(e)}")
                    import traceback
                    st.code(traceback.format_exc())

        # Optimize preset generator (under configs)
        self._render_optimize_preset_generator(
            config=config,
            full_config_data=full_config_data,
            key_prefix=f"{key_prefix}_opt_{config_index}",
        )

    def _render_optimize_preset_generator(self, config, full_config_data, key_prefix: str):
        """Reusable UI block: create PBv7 Optimize preset from a selected config."""

        # Optimize preset generator (MVP)
        st.markdown("---")
        with st.expander("🧩 Create PBv7 Optimize Preset from this Config", expanded=False):
            st.caption(
                "Creates a new Optimize preset with bounds tightened around the selected config’s parameters. "
                "Useful for a follow-up run focused on ‘fine-tuning’ instead of broad exploration."
            )

            default_preset_name = f"pareto_refine_cfg_{config.config_index}"
            preset_name = st.text_input(
                "Preset name",
                value=default_preset_name,
                max_chars=64,
                key=f"{key_prefix}_preset_name",
            )

            default_window_pct = 10
            bounds_adjust = st.slider(
                "Bounds window adjustment",
                min_value=-50,
                max_value=50,
                value=0,
                step=1,
                help=(
                    "0 keeps the original run bounds unchanged. Negative = tighter window (less exploration). "
                    "Positive = looser window (more exploration). Bounds are always clamped to the original run bounds."
                ),
                key=f"{key_prefix}_bounds_adjust",
            )
            if bounds_adjust == 0:
                window_pct = 0.0
                st.caption("Bounds unchanged (set adjustment ≠ 0 to tighten/loosen).")
            else:
                window_pct = float(max(0, min(100, default_window_pct + bounds_adjust)))
                st.caption(
                    f"Effective bounds window: ±{window_pct:.0f}% around selected values (base {default_window_pct}%, adjustment {bounds_adjust:+d})."
                )

            # High-level intent selection (no manual JSON typing)
            base_optimize_block = full_config_data.get("optimize", {}) if isinstance(full_config_data, dict) else {}
            base_scoring = base_optimize_block.get("scoring")
            base_limits = base_optimize_block.get("limits")
            if not base_scoring:
                try:
                    base_scoring = (config.optimize_settings or {}).get("scoring")
                except Exception:
                    base_scoring = None
            if base_limits is None:
                try:
                    base_limits = (config.optimize_settings or {}).get("limits")
                except Exception:
                    base_limits = None
            if not isinstance(base_scoring, list) or not base_scoring:
                base_scoring = ["loss_profit_ratio", "mdg_w", "sharpe_ratio"]
            if base_limits is None:
                base_limits = []

            direction = st.selectbox(
                "Optimization goal (high-level)",
                [
                    "Balanced (keep run scoring)",
                    "More profit (risk can be higher)",
                    "Safer (lower drawdowns)",
                    "Smoother equity curve",
                    "Fewer/shorter holds (faster turnover)",
                    "Lower exposure (safer sizing)",
                ],
                index=0,
                help=(
                    "This sets reasonable defaults in the generated Optimize preset. "
                    "You can still fine-tune everything in the PBv7 Optimize GUI after opening it."
                ),
                key=f"{key_prefix}_direction",
            )

            risk_adjust = st.slider(
                "Risk adjustment",
                min_value=-50,
                max_value=50,
                value=0,
                step=5,
                help=(
                    "0 = neutral. Negative = more conservative, positive = more aggressive. "
                    "This can tweak exposure/unstuck/enforcer-related bounds. "
                    "It can also relax/tighten drawdown limits. "
                    "(Safety goals may add additional risk-limits.)"
                ),
                key=f"{key_prefix}_risk_adjust",
            )

            def _detect_metric_scheme(metrics: List[str]) -> str:
                # legacy: btc_* prefix; new: *_btc/_usd suffix; else base
                for m in metrics:
                    if isinstance(m, str) and m.startswith("btc_"):
                        return "btc_prefix"
                for m in metrics:
                    if isinstance(m, str) and (m.endswith("_btc") or m.endswith("_usd")):
                        return "suffix"
                return "base"

            def _m(name_base: str, scheme: str) -> str:
                # choose a consistent naming convention
                if scheme == "btc_prefix":
                    return f"btc_{name_base}"
                if scheme == "suffix":
                    # default to _usd for currency metrics if user didn't specify
                    if name_base in CURRENCY_METRICS:
                        return f"{name_base}_usd"
                    return name_base
                return name_base

            scheme = _detect_metric_scheme([str(x) for x in base_scoring])

            def _limit(metric: str, penalize_if: str, value: Any) -> Dict[str, Any]:
                return {"metric": metric, "penalize_if": penalize_if, "value": value}

            def _upsert_limit(limits: Any, entry: Dict[str, Any]) -> Any:
                """Insert or replace a limit entry by metric name (list format only)."""
                if not isinstance(limits, list):
                    return limits
                metric = entry.get("metric")
                if not metric:
                    return limits
                out = []
                replaced = False
                for e in limits:
                    if isinstance(e, dict) and e.get("metric") == metric:
                        out.append(entry)
                        replaced = True
                    else:
                        out.append(e)
                if not replaced:
                    out.append(entry)
                return out

            def _remove_limit(limits: Any, metric_name: str) -> Any:
                if not isinstance(limits, list):
                    return limits
                return [e for e in limits if not (isinstance(e, dict) and e.get("metric") == metric_name)]

            def _find_limit_entry(limits: Any, metric_name: str) -> Optional[Dict[str, Any]]:
                if not isinstance(limits, list):
                    return None
                for e in limits:
                    if isinstance(e, dict) and e.get("metric") == metric_name:
                        return e
                return None

            def _risk_limit_from_tolerance(
                metric_base: str,
                base_entry: Optional[Dict[str, Any]],
                margin_min: float,
                margin_max: float,
                use_abs_seed: bool = False,
                default_value: Optional[float] = None,
            ) -> Optional[Dict[str, Any]]:
                """Create a PassivBot list-format limit entry for a risk-related metric.

                The threshold is computed in the same units as the selected config's suite metric.
                Existing entry's `penalize_if`/`stat` are preserved when present.
                """
                metric_name = _m(metric_base, scheme)
                # PassivBot supports both symbolic and word operators.
                # Use symbolic operators by default and preserve any existing choice.
                penalize_if_raw = (base_entry or {}).get("penalize_if", ">")
                penalize_map = {
                    "greater_than": ">",
                    "less_than": "<",
                }
                penalize_if = penalize_map.get(str(penalize_if_raw), str(penalize_if_raw))
                # IMPORTANT: do not force `stat` here.
                # PassivBot defaults: '>' uses max, '<' uses min, range uses mean.
                stat = (base_entry or {}).get("stat", None)

                seed = config.suite_metrics.get(metric_name)
                seed_val = float(seed) if isinstance(seed, (int, float)) else None
                if seed_val is not None and use_abs_seed:
                    seed_val = abs(seed_val)

                # Symmetric risk adjustment around a neutral midpoint.
                # signed < 0 => more conservative (tighter risk limits for '>' operators)
                # signed > 0 => more aggressive (looser risk limits for '>' operators)
                signed = float(risk_adjust) / 50.0  # -1..+1
                mag = min(1.0, abs(signed))
                margin = margin_min + mag * (margin_max - margin_min)

                if seed_val is not None:
                    # Apply direction based on operator semantics.
                    # For '>' limits: higher threshold = looser, lower = tighter.
                    # For '<' limits: higher threshold = tighter, lower = looser.
                    op = str(penalize_if).strip()
                    is_less = op.startswith("<") or op == "less_than"
                    if signed > 0:
                        factor = (1.0 - margin) if is_less else (1.0 + margin)
                    else:
                        factor = (1.0 + margin) if is_less else (1.0 - margin)
                    factor = max(0.0, factor)
                    value = seed_val * factor
                else:
                    base_value = (base_entry or {}).get("value")
                    if isinstance(base_value, (int, float)):
                        value = float(base_value)
                    elif default_value is not None:
                        value = float(default_value)
                    else:
                        return None

                entry: Dict[str, Any] = {
                    "metric": metric_name,
                    "penalize_if": penalize_if,
                    "value": round(float(value), 6),
                }
                if stat is not None:
                    entry["stat"] = stat
                return entry

            def _risk_limits_pack_from_tolerance(limits: Any) -> Any:
                """Upsert a small set of risk-related limits (list format only)."""
                if not isinstance(limits, list):
                    return limits

                # Keep this small & impactful; limits don't add objectives but help shape the Pareto front.
                specs = [
                    # metric_base, margin_min, margin_max, use_abs_seed, fallback
                    ("drawdown_worst", 0.05, 0.50, False, 0.30),
                    ("expected_shortfall_1pct", 0.05, 0.60, False, 0.30),
                    ("equity_choppiness_w", 0.10, 1.00, False, None),
                    ("peak_recovery_hours_equity", 0.05, 0.80, False, None),
                    ("position_held_hours_max", 0.05, 0.80, False, None),
                    ("equity_balance_diff_neg_max", 0.05, 0.80, True, None),
                ]

                out = list(limits)
                for metric_base, m_min, m_max, use_abs, fallback in specs:
                    metric_name = _m(metric_base, scheme)
                    existing = _find_limit_entry(out, metric_name)
                    entry = _risk_limit_from_tolerance(
                        metric_base=metric_base,
                        base_entry=existing,
                        margin_min=m_min,
                        margin_max=m_max,
                        use_abs_seed=use_abs,
                        default_value=fallback,
                    )
                    if entry is not None:
                        out = _upsert_limit(out, entry)
                return out

            # Build scoring/limits defaults from intent
            scoring_out: List[str] = list(base_scoring)
            limits_out: Any = list(base_limits) if isinstance(base_limits, list) else base_limits

            # Objective candidates
            profit_set = [_m("mdg_w", scheme), _m("adg_w", scheme), _m("gain", scheme)]
            ratio_set = [_m("loss_profit_ratio", scheme), _m("sharpe_ratio", scheme), _m("sortino_ratio", scheme)]
            risk_set = [_m("drawdown_worst", scheme), _m("drawdown_worst_mean_1pct", scheme), _m("expected_shortfall_1pct", scheme)]
            smooth_set = [_m("equity_choppiness_w", scheme), _m("equity_jerkiness_w", scheme), _m("exponential_fit_error_w", scheme)]
            turnover_set = [_m("positions_held_per_day", scheme), _m("position_held_hours_mean", scheme), _m("position_held_hours_max", scheme)]

            def _unique_keep_order(items: List[str]) -> List[str]:
                seen = set()
                out = []
                for it in items:
                    if it not in seen:
                        seen.add(it)
                        out.append(it)
                return out

            def _cap_objectives(items: List[str], max_n: int = 4) -> List[str]:
                # Optimizer gets slow with too many objectives.
                capped = items[:max_n]
                return capped

            if direction == "Balanced (keep run scoring)":
                scoring_out = _cap_objectives(_unique_keep_order(list(base_scoring)), 4)
                limits_out = base_limits

            elif direction == "More profit (risk can be higher)":
                scoring_out = _cap_objectives(_unique_keep_order([
                    profit_set[0],
                    profit_set[1],
                    ratio_set[1],
                    ratio_set[0],
                ]), 4)
                limits_out = base_limits

            elif direction == "Safer (lower drawdowns)":
                scoring_out = _cap_objectives(_unique_keep_order([
                    risk_set[0],
                    risk_set[1],
                    ratio_set[1],
                    ratio_set[0],
                ]), 4)
                # Apply risk limit pack only when user adjusts risk
                limits_out = base_limits
                if risk_adjust != 0:
                    limits_out = _risk_limits_pack_from_tolerance(limits_out)

            elif direction == "Smoother equity curve":
                scoring_out = _cap_objectives(_unique_keep_order([
                    smooth_set[0],
                    smooth_set[1],
                    ratio_set[1],
                    profit_set[0],
                ]), 4)
                limits_out = base_limits

            elif direction == "Fewer/shorter holds (faster turnover)":
                scoring_out = _cap_objectives(_unique_keep_order([
                    turnover_set[1],
                    turnover_set[2],
                    profit_set[0],
                    ratio_set[1],
                ]), 4)
                limits_out = base_limits

            elif direction == "Lower exposure (safer sizing)":
                scoring_out = _cap_objectives(_unique_keep_order([
                    risk_set[0],
                    ratio_set[0],
                    profit_set[0],
                    ratio_set[1],
                ]), 4)
                limits_out = base_limits
                if risk_adjust != 0:
                    limits_out = _risk_limits_pack_from_tolerance(limits_out)

            # Risk adjustment should influence all goals (user can set 0 to disable).
            # Apply a generic drawdown guard adjustment for list-format limits.
            if isinstance(limits_out, list) and risk_adjust != 0:
                metric_name = _m("drawdown_worst", scheme)
                existing = _find_limit_entry(limits_out, metric_name)
                entry = _risk_limit_from_tolerance(
                    metric_base="drawdown_worst",
                    base_entry=existing,
                    margin_min=0.05,
                    margin_max=0.60,
                    use_abs_seed=False,
                    default_value=0.30,
                )
                if entry is not None:
                    limits_out = _upsert_limit(limits_out, entry)

            # Final safety cap
            scoring_out = _cap_objectives(_unique_keep_order([str(x) for x in scoring_out]), 4)

            # Small, user-visible preview (no typing)
            st.write("**Planned optimize defaults**")
            try:
                import json as _json
                st.code(_json.dumps({"scoring": scoring_out, "limits": limits_out}, indent=2), language="json")
            except Exception:
                st.code(str({"scoring": scoring_out, "limits": limits_out}), language="json")

            switch_to_optimize = st.toggle(
                "Open PBv7 Optimize after creating preset",
                value=True,
                key=f"{key_prefix}_switch_to_optimize",
            )

            def _as_bound_tuple(bound_value: Any) -> Tuple[Optional[float], Optional[float], Optional[float]]:
                """Return (low, high, step) from Passivbot-style bounds formats."""
                if isinstance(bound_value, (list, tuple)):
                    if len(bound_value) >= 2:
                        low = bound_value[0]
                        high = bound_value[1]
                        step = bound_value[2] if len(bound_value) >= 3 else None
                        return (
                            float(low) if low is not None else None,
                            float(high) if high is not None else None,
                            float(step) if step not in (None, 0) else None,
                        )
                    if len(bound_value) == 1:
                        v = bound_value[0]
                        return (float(v), float(v), None)
                if isinstance(bound_value, (int, float)):
                    v = float(bound_value)
                    return (v, v, None)
                return (None, None, None)

            def _tighten_bounds_around_value(
                low: Optional[float],
                high: Optional[float],
                value: float,
                pct: float,
                step: Optional[float],
            ) -> Any:
                if pct <= 0:
                    return [low, high, step] if step else [low, high]
                if low is None or high is None:
                    return [value, value]
                if high < low:
                    low, high = high, low

                delta = abs(value) * (pct / 100.0)
                if delta == 0:
                    # Use a tiny absolute delta so values near 0 can still produce a window
                    delta = (high - low) * (pct / 100.0)
                new_low = max(low, value - delta)
                new_high = min(high, value + delta)

                # Ensure non-degenerate range if optimization expects a range
                if new_high <= new_low:
                    if step:
                        new_low = max(low, new_low - step)
                        new_high = min(high, new_high + step)
                    else:
                        # fallback: expand by 1% of original range
                        expand = max((high - low) * 0.01, 1e-12)
                        new_low = max(low, new_low - expand)
                        new_high = min(high, new_high + expand)

                if step:
                    # If step looks like integer stepping, round bounds accordingly
                    if abs(step - 1.0) < 1e-12:
                        new_low = float(int(round(new_low)))
                        new_high = float(int(round(new_high)))
                        if new_high <= new_low:
                            new_high = min(high, new_low + 1.0)
                    return [new_low, new_high, step]
                return [new_low, new_high]

            def _tighten_bounds_around_value_asymmetric(
                low: Optional[float],
                high: Optional[float],
                value: float,
                pct_down: float,
                pct_up: float,
                step: Optional[float],
            ) -> Any:
                """Asymmetric window around a value.

                pct_down/pct_up are in percent and clamped to original bounds.
                """
                if (pct_down <= 0) and (pct_up <= 0):
                    return [low, high, step] if step else [low, high]
                if low is None or high is None:
                    return [value, value]
                if high < low:
                    low, high = high, low

                d_down = abs(value) * (pct_down / 100.0)
                d_up = abs(value) * (pct_up / 100.0)
                if d_down == 0:
                    d_down = (high - low) * (pct_down / 100.0)
                if d_up == 0:
                    d_up = (high - low) * (pct_up / 100.0)

                new_low = max(low, value - d_down)
                new_high = min(high, value + d_up)

                if new_high <= new_low:
                    if step:
                        new_low = max(low, new_low - step)
                        new_high = min(high, new_high + step)
                    else:
                        expand = max((high - low) * 0.01, 1e-12)
                        new_low = max(low, new_low - expand)
                        new_high = min(high, new_high + expand)

                if step:
                    if abs(step - 1.0) < 1e-12:
                        new_low = float(int(round(new_low)))
                        new_high = float(int(round(new_high)))
                        if new_high <= new_low:
                            new_high = min(high, new_low + 1.0)
                    return [new_low, new_high, step]
                return [new_low, new_high]

            def _normalize_bound_for_compare(v: Any) -> Any:
                """Normalize bound formats for stable comparisons in previews."""
                if isinstance(v, (list, tuple)):
                    out = []
                    for x in v:
                        if isinstance(x, float):
                            out.append(round(x, 12))
                        else:
                            out.append(x)
                    return tuple(out)
                if isinstance(v, float):
                    return round(v, 12)
                return v

            def _bound_pretty(v: Any) -> str:
                try:
                    import json as _json
                    return _json.dumps(v)
                except Exception:
                    return str(v)

            def _build_new_bounds(
                apply_risk_adjustments: bool = True,
                apply_window_adjustments: bool = True,
                apply_near_expansion: bool = True,
                expand_notes_out: Optional[Dict[str, str]] = None,
            ) -> Dict[str, Any]:
                """Build new optimize.bounds from current UI choices.

                Kept in one place so preview and save use identical logic.
                """
                base_bounds_local: Dict[str, Any] = dict(config.bounds or {})
                new_bounds_local: Dict[str, Any] = {}

                expand_enabled_local = bool(
                    apply_near_expansion
                    and show_near_bounds
                    and expand_near_bounds
                    and near_bounds_expand_pct > 0
                    and isinstance(near_map, dict)
                    and len(near_map) > 0
                )

                if expand_enabled_local:
                    # Expand ORIGINAL bounds on the side where params are near the edge.
                    for p_name, info in near_map.items():
                        if p_name not in base_bounds_local:
                            continue
                        edge = (info or {}).get("edge")
                        if edge not in ("lower", "upper"):
                            continue

                        low, high, step = _as_bound_tuple(base_bounds_local.get(p_name))
                        if low is None or high is None:
                            continue
                        if abs(low) < 1e-15 and abs(high) < 1e-15:
                            continue
                        if high < low:
                            low, high = high, low
                        rng = high - low
                        if rng < 1e-12:
                            continue

                        expand = rng * (near_bounds_expand_pct / 100.0)
                        new_low = low
                        new_high = high
                        if edge == "lower":
                            requested_low = low - expand
                            new_low = requested_low
                            if low >= 0:
                                if new_low < 0:
                                    if expand_notes_out is not None:
                                        expand_notes_out[p_name] = f"lower expansion clamped to 0 (requested {requested_low:g})"
                                new_low = max(0.0, new_low)
                        else:
                            requested_high = high + expand
                            new_high = requested_high

                        if step and abs(step - 1.0) < 1e-12:
                            rounded_low = float(int(round(new_low)))
                            rounded_high = float(int(round(new_high)))
                            if abs(rounded_low - new_low) > 1e-12 or abs(rounded_high - new_high) > 1e-12:
                                prev = expand_notes_out.get(p_name) if expand_notes_out is not None else None
                                note = "rounded to integer step"
                                if expand_notes_out is not None:
                                    expand_notes_out[p_name] = f"{prev}; {note}" if prev else note
                            new_low = rounded_low
                            new_high = rounded_high
                            if new_high <= new_low:
                                new_high = new_low + 1.0

                        base_bounds_local[p_name] = [new_low, new_high, step] if step else [new_low, new_high]

                # Risk adjustment can tweak exposure/unstuck/enforcer bounds across goals.
                # (Limits-pack remains safety-goal-only; handled outside this function.)
                risk_enabled_local = bool(apply_risk_adjustments and risk_adjust != 0)
                window_enabled_local = bool(apply_window_adjustments and window_pct > 0)

                # If user doesn't want bounds adjustments and isn't applying risk adjustments,
                # keep the original bounds exactly.
                if not window_enabled_local and not risk_enabled_local and not expand_enabled_local:
                    return dict(base_bounds_local)

                def _is_side_enabled(side: str) -> Optional[bool]:
                    """Return True/False if side can be determined, else None.

                    A side is considered disabled if either n_positions == 0 or
                    total_wallet_exposure_limit == 0 for that side.
                    """
                    if side not in ("long", "short"):
                        return None
                    bp = config.bot_params or {}
                    n_key = f"{side}_n_positions"
                    twel_key = f"{side}_total_wallet_exposure_limit"
                    has_any = False
                    disabled = False

                    n_val = bp.get(n_key)
                    if isinstance(n_val, (int, float)):
                        has_any = True
                        if float(n_val) <= 0:
                            disabled = True

                    twel_val = bp.get(twel_key)
                    if isinstance(twel_val, (int, float)):
                        has_any = True
                        if float(twel_val) <= 0:
                            disabled = True

                    if not has_any:
                        return None
                    return not disabled

                long_enabled = _is_side_enabled("long")
                short_enabled = _is_side_enabled("short")

                signed = float(risk_adjust) / 50.0  # -1..+1
                strength = min(1.0, abs(signed))
                # When bounds_adjust == 0 but risk is enabled, use a small dedicated window for risk params
                risk_window_pct = max(2.0, min(25.0, 5.0 + 20.0 * strength))

                def _risk_mults(invert: bool = False) -> Tuple[float, float]:
                    """Return (down_mult, up_mult) for asymmetric windows."""
                    if signed == 0:
                        return (1.0, 1.0)
                    aggressive = signed > 0
                    if invert:
                        aggressive = not aggressive
                    # For profit goal, allow a stronger bias to explore higher exposure.
                    if direction == "More profit (risk can be higher)" and not invert:
                        return (0.3, 2.0) if aggressive else (2.0, 0.3)
                    return (0.5, 1.5) if aggressive else (1.5, 0.5)

                for param_name, bound_value in base_bounds_local.items():
                    # If a side is disabled, do not modify its bounds and don't surface it in preview.
                    # Preserve original entries exactly.
                    pname = str(param_name)
                    if pname.startswith("long_") and long_enabled is False:
                        new_bounds_local[param_name] = bound_value
                        continue
                    if pname.startswith("short_") and short_enabled is False:
                        new_bounds_local[param_name] = bound_value
                        continue

                    # If bounds window is off, preserve all non-risk params exactly.
                    # Risk can still adjust risk-related params even when bounds_adjust == 0.
                    is_risk_param = (
                        "total_wallet_exposure_limit" in pname
                        or pname.endswith("n_positions")
                        or "_n_positions" in pname
                        or "risk_we_excess_allowance_pct" in pname
                        or "risk_wel_enforcer_threshold" in pname
                        or "risk_twel_enforcer_threshold" in pname
                        or "unstuck_loss_allowance_pct" in pname
                    )
                    if not window_enabled_local and not (risk_enabled_local and is_risk_param):
                        new_bounds_local[param_name] = bound_value
                        continue

                    low, high, step = _as_bound_tuple(bound_value)

                    # Preserve clearly disabled/invalid bounds instead of dropping them.
                    # Dropping would show as "removed" in preview and could change behavior.
                    if low is None or high is None:
                        new_bounds_local[param_name] = bound_value
                        continue
                    if abs(low) < 1e-15 and abs(high) < 1e-15:
                        new_bounds_local[param_name] = bound_value
                        continue

                    if param_name in config.bot_params and isinstance(config.bot_params[param_name], (int, float)):
                        v = float(config.bot_params[param_name])

                        if risk_enabled_local and is_risk_param:
                            name = str(param_name)
                            base = float(window_pct) if window_enabled_local else float(risk_window_pct)

                            if "total_wallet_exposure_limit" in name:
                                down_mult, up_mult = _risk_mults(invert=False)
                                new_bounds_local[param_name] = _tighten_bounds_around_value_asymmetric(
                                    low, high, v,
                                    pct_down=max(0.0, min(100.0, base * down_mult)),
                                    pct_up=max(0.0, min(100.0, base * up_mult)),
                                    step=step,
                                )
                                continue

                            if name.endswith("n_positions") or "_n_positions" in name:
                                # Invert: conservative explores higher n_positions; aggressive lower.
                                down_mult, up_mult = _risk_mults(invert=True)
                                new_bounds_local[param_name] = _tighten_bounds_around_value_asymmetric(
                                    low, high, v,
                                    pct_down=max(0.0, min(100.0, base * down_mult)),
                                    pct_up=max(0.0, min(100.0, base * up_mult)),
                                    step=step,
                                )
                                continue

                            if "risk_we_excess_allowance_pct" in name:
                                down_mult, up_mult = _risk_mults(invert=False)
                                new_bounds_local[param_name] = _tighten_bounds_around_value_asymmetric(
                                    low, high, v,
                                    pct_down=max(0.0, min(100.0, base * down_mult)),
                                    pct_up=max(0.0, min(100.0, base * up_mult)),
                                    step=step,
                                )
                                continue

                            if "risk_wel_enforcer_threshold" in name or "risk_twel_enforcer_threshold" in name:
                                down_mult, up_mult = _risk_mults(invert=False)
                                new_bounds_local[param_name] = _tighten_bounds_around_value_asymmetric(
                                    low, high, v,
                                    pct_down=max(0.0, min(100.0, base * down_mult)),
                                    pct_up=max(0.0, min(100.0, base * up_mult)),
                                    step=step,
                                )
                                continue

                            if "unstuck_loss_allowance_pct" in name:
                                down_mult, up_mult = _risk_mults(invert=False)
                                new_bounds_local[param_name] = _tighten_bounds_around_value_asymmetric(
                                    low, high, v,
                                    pct_down=max(0.0, min(100.0, base * down_mult)),
                                    pct_up=max(0.0, min(100.0, base * up_mult)),
                                    step=step,
                                )
                                continue

                        if window_enabled_local:
                            new_bounds_local[param_name] = _tighten_bounds_around_value(low, high, v, window_pct, step)
                        else:
                            new_bounds_local[param_name] = bound_value
                    else:
                        if step:
                            new_bounds_local[param_name] = [low, high, step]
                        else:
                            new_bounds_local[param_name] = [low, high]

                return new_bounds_local

            # Preview: which bounds will change?
            show_near_bounds = st.toggle(
                "Show parameters near bounds (Deep Intelligence)",
                value=False,
                help="Highlights parameters within a tolerance of the original optimization bounds, using the same logic as the Deep Intelligence stage.",
                key=f"{key_prefix}_show_near_bounds",
            )
            near_bounds_tol = 0.10
            near_map: Dict[str, Dict[str, Any]] = {}
            near_map_all: Dict[str, Dict[str, Any]] = {}
            near_rows: List[Dict[str, Any]] = []
            hidden_near_params: set[str] = set()
            hide_hard_limited_near = False
            if show_near_bounds:
                near_bounds_tol = st.slider(
                    "Near-bounds tolerance",
                    min_value=0.01,
                    max_value=0.25,
                    value=0.10,
                    step=0.01,
                    help="A parameter is considered 'near' a bound if it is within this fraction of the bound range.",
                    key=f"{key_prefix}_near_bounds_tol",
                )

                try:
                    bounds_info = self.loader.get_parameters_at_bounds(tolerance=float(near_bounds_tol), top_n=10)
                    for k, info in (bounds_info or {}).get("at_lower", {}).items():
                        near_map[str(k)] = {"edge": "lower", **(info or {})}
                    for k, info in (bounds_info or {}).get("at_upper", {}).items():
                        near_map[str(k)] = {"edge": "upper", **(info or {})}

                    # Keep an unfiltered snapshot for robust filtering in the preview.
                    near_map_all = dict(near_map)

                    for k, info in sorted(near_map.items(), key=lambda x: x[0]):
                        near_rows.append({
                            "param": str(k),
                            "edge": info.get("edge"),
                            "value": info.get("value"),
                            "bound": info.get("bound"),
                        })
                except Exception:
                    near_map = {}
                    near_rows = []

            expand_near_bounds = False
            near_bounds_expand_pct = 0.0
            if show_near_bounds:
                expand_near_bounds = st.toggle(
                    "Expand bounds for near-edge params",
                    value=False,
                    help="Expands the ORIGINAL optimize.bounds for parameters detected near the lower/upper edge.",
                    key=f"{key_prefix}_expand_near_bounds",
                )
                if expand_near_bounds:
                    near_bounds_expand_pct = float(
                        st.slider(
                            "Near-edge expansion (%)",
                            min_value=0,
                            max_value=100,
                            value=25,
                            step=5,
                            help="Expands the bound range by this % (of the original range) on the side where the parameter is near an edge.",
                            key=f"{key_prefix}_near_bounds_expand_pct",
                        )
                    )
                    if not near_map:
                        st.info("No near-edge parameters detected for the chosen tolerance.")

                hide_hard_limited_near = st.toggle(
                    "Hide near-bounds limited by 0-clamp",
                    value=False,
                    help="Hides parameters where LOWER near-edge expansion would be clamped to 0 (i.e. can't widen below 0).",
                    key=f"{key_prefix}_hide_hard_limited_near",
                )

                if hide_hard_limited_near and near_map:
                    _before_cnt = len(near_map)
                    base_bounds_for_hide = {str(k): v for k, v in (config.bounds or {}).items()}

                    # Derive "hard-limited" from the same near-expansion logic used for previews/saving.
                    # This avoids mismatches where clamped params still appear in the tables.
                    if expand_near_bounds and near_bounds_expand_pct > 0:
                        _hide_notes: Dict[str, str] = {}
                        try:
                            _build_new_bounds(
                                apply_risk_adjustments=False,
                                apply_window_adjustments=False,
                                apply_near_expansion=True,
                                expand_notes_out=_hide_notes,
                            )
                        except Exception:
                            _hide_notes = {}
                        hidden_near_params = {p for p, note in _hide_notes.items() if "clamped to 0" in str(note)}
                    else:
                        # If we don't have an expansion pct active, only treat exact 0 as hard-limited.
                        def _is_zero_lower(p_name: str, info: Dict[str, Any]) -> bool:
                            try:
                                if (info or {}).get("edge") != "lower":
                                    return False
                                low, _high, _step = _as_bound_tuple(base_bounds_for_hide.get(p_name))
                                if low is None:
                                    return False
                                return abs(float(low)) <= 1e-12
                            except Exception:
                                return False
                        hidden_near_params = {k for k, v in near_map.items() if _is_zero_lower(k, v)}

                    near_map = {k: v for k, v in near_map.items() if k not in hidden_near_params}
                    near_rows = [r for r in near_rows if str(r.get("param")) not in hidden_near_params]
                    _after_cnt = len(near_map)
                    if _after_cnt < _before_cnt:
                        st.caption(f"Hidden {_before_cnt - _after_cnt} 0-clamp-limited parameter(s) from near-bounds.")

            # Collect notes for near-edge expansion limitations (e.g. clamped to 0).
            expand_notes: Dict[str, str] = {}

            try:
                _base_bounds_preview = config.bounds or {}
                _result_bounds_preview = _build_new_bounds(
                    apply_risk_adjustments=True,
                    apply_window_adjustments=True,
                    apply_near_expansion=True,
                    expand_notes_out=expand_notes,
                )
                _expand_bounds_preview = _build_new_bounds(
                    apply_risk_adjustments=False,
                    apply_window_adjustments=False,
                    apply_near_expansion=True,
                    expand_notes_out=expand_notes,
                )
                _window_bounds_preview = _build_new_bounds(
                    apply_risk_adjustments=False,
                    apply_window_adjustments=True,
                    apply_near_expansion=False,
                )
                _risk_bounds_preview = _build_new_bounds(
                    apply_risk_adjustments=True,
                    apply_window_adjustments=False,
                    apply_near_expansion=False,
                )
                _base_keys = set(_base_bounds_preview.keys())
                _new_keys = set(_result_bounds_preview.keys())

                _rows = []
                for k in sorted(_base_keys | _new_keys):
                    # If user hid hard-limited near-bounds, also hide them from the bounds preview.
                    if show_near_bounds and hide_hard_limited_near and str(k) in hidden_near_params:
                        continue
                    before = _base_bounds_preview.get(k, None)
                    expand_v = _expand_bounds_preview.get(k, None)
                    window_v = _window_bounds_preview.get(k, None)
                    risk_v = _risk_bounds_preview.get(k, None)
                    result_v = _result_bounds_preview.get(k, None)
                    if k not in _base_keys:
                        change = "added"
                    elif k not in _new_keys:
                        change = "removed"
                    else:
                        change = "changed" if _normalize_bound_for_compare(before) != _normalize_bound_for_compare(result_v) else ""

                    # Also surface near-edge expansions that were limited (e.g. clamped to 0)
                    # even if the final bounds are unchanged.
                    if (
                        not change
                        and show_near_bounds
                        and expand_near_bounds
                        and near_bounds_expand_pct > 0
                        and str(k) in expand_notes
                    ):
                        change = "limited"
                    if change:
                        row: Dict[str, Any] = {
                            "param": str(k),
                            "change": change,
                            "before": _bound_pretty(before),
                            "expand": _bound_pretty(expand_v),
                            "window": _bound_pretty(window_v),
                            "risk": _bound_pretty(risk_v),
                            "result": _bound_pretty(result_v),
                        }
                        if show_near_bounds and expand_near_bounds and near_bounds_expand_pct > 0:
                            row["expand_note"] = expand_notes.get(str(k), "")
                        if show_near_bounds:
                            info = near_map.get(str(k))
                            if info:
                                row["near_edge"] = info.get("edge")
                                row["near_value"] = info.get("value")
                                row["near_bound"] = info.get("bound")
                            else:
                                row["near_edge"] = ""
                                row["near_value"] = ""
                                row["near_bound"] = ""
                        _rows.append({
                            **row
                        })

                st.write("**Bounds changes preview**")
                if bounds_adjust == 0 and risk_adjust == 0 and not (expand_near_bounds and near_bounds_expand_pct > 0):
                    st.caption("No bounds changes (bounds window = 0 and risk adjustment = 0).")
                else:
                    st.caption("'expand' = near-edge expansion-only, 'window' = window-only, 'risk' = risk-only, 'result' = combined output.")
                if show_near_bounds and expand_near_bounds and near_bounds_expand_pct > 0 and expand_notes:
                    st.info("Some near-edge expansions were limited (e.g. lower bound clamped to 0). See 'expand_note'.")
                if _rows:
                    st.caption(f"{len(_rows)} bound(s) listed (added/removed/changed/limited).")
                    st.dataframe(_rows, use_container_width=True, hide_index=True)
                else:
                    st.caption("No bounds changes detected.")

                if show_near_bounds:
                    st.write("**Parameters near bounds**")
                    if near_rows:
                        st.caption(f"{len(near_rows)} parameter(s) are near bounds (tolerance {near_bounds_tol:.0%}).")
                        st.dataframe(near_rows, use_container_width=True, hide_index=True)
                    else:
                        st.caption("No parameters near bounds detected (or data unavailable).")
            except Exception:
                # Preview is best-effort; do not block the generator UI.
                pass

            if st.button(
                "💾 Create Optimize Preset",
                type="primary",
                use_container_width=True,
                key=f"{key_prefix}_create_preset",
            ):
                try:
                    from pathlib import Path
                    import json
                    from pbgui_func import PBGDIR, get_navi_paths, replace_special_chars

                    if not full_config_data:
                        st.error("❌ Full config data not available; cannot build preset")
                        st.stop()

                    # Sanitize preset name
                    safe_name = replace_special_chars(preset_name.strip()) if preset_name else default_preset_name
                    safe_name = safe_name.replace("/", "_")
                    if not safe_name:
                        safe_name = default_preset_name

                    # Base: copy current full config (keeps backtest window, exchanges, etc.)
                    preset_config = dict(full_config_data)

                    # Tighten bounds around selected config's parameter values
                    new_bounds: Dict[str, Any] = _build_new_bounds(apply_risk_adjustments=True, apply_window_adjustments=True)

                    # Ensure optimize block exists
                    if "optimize" not in preset_config or not isinstance(preset_config.get("optimize"), dict):
                        preset_config["optimize"] = {}
                    preset_config["optimize"]["bounds"] = new_bounds

                    # Apply high-level intent defaults; user can refine in PBv7 Optimize GUI
                    preset_config["optimize"]["scoring"] = scoring_out
                    preset_config["optimize"]["limits"] = limits_out

                    # Save to normal optimize configs directory (not presets)
                    preset_dir = Path(f"{PBGDIR}/data/opt_v7")
                    preset_dir.mkdir(parents=True, exist_ok=True)
                    preset_file = preset_dir / f"{safe_name}.json"

                    with open(preset_file, "w", encoding="utf-8") as f:
                        json.dump(preset_config, f, indent=4)

                    st.success(f"✅ Optimize config created: {preset_file.name}")

                    if switch_to_optimize:
                        from OptimizeV7 import OptimizeV7Item

                        # Clear other optimize views so v7_optimize opens the editor
                        for k in [
                            "opt_v7_results",
                            "opt_v7_queue",
                            "opt_v7_pareto",
                            "opt_v7_pareto_name",
                            "opt_v7_pareto_directory",
                            "opt_v7_list",
                        ]:
                            if k in st.session_state:
                                del st.session_state[k]

                        st.session_state.opt_v7 = OptimizeV7Item(str(preset_file))
                        st.switch_page(get_navi_paths()["V7_OPTIMIZE"])

                except Exception as e:
                    st.error(f"❌ Failed to create preset: {e}")
                    import traceback
                    st.code(traceback.format_exc())
    
    def _show_command_center(self):
        """Stage 1: Command Center - Overview and Top Performers"""
        
        st.title("🎯 COMMAND CENTER")
        st.markdown("**High-level overview of your optimization run**")
        
        # Load Strategy Configuration Section
        # Session state already initialized in run()
        if 'load_strategy_expander_open' not in st.session_state:
            st.session_state['load_strategy_expander_open'] = False
        
        # Check if strategy was changed (triggers expander to stay open)
        if 'load_strategy_multiselect' in st.session_state:
            current_selection = st.session_state['load_strategy_multiselect']
            if current_selection != st.session_state['load_strategy']:
                st.session_state['load_strategy_expander_open'] = True
        
        with st.expander("⚙️ Load Strategy Settings", expanded=st.session_state['load_strategy_expander_open']):
            st.markdown("### Configuration Selection Criteria")
            st.markdown("""
            Configure which criteria are used to select the top N configs from all results.
            Choose multiple criteria to ensure diversity. The quota is split evenly across selections.
            """)
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.markdown("#### Max Configs to Load")
                new_max_configs = st.number_input(
                    "Total configs to load:",
                    min_value=100,
                    max_value=10000,
                    value=st.session_state['max_configs'],
                    step=100,
                    key='max_configs_input',
                    help="Higher values = more data but slower loading"
                )
                
                st.markdown("#### Select Criteria")
                new_strategy = st.multiselect(
                    "Choose one or more criteria:",
                    options=[
                        'performance',
                        'robustness',
                        'sharpe',
                        'drawdown',
                        'calmar',
                        'sortino',
                        'omega',
                        'volatility',
                        'recovery'
                    ],
                    default=st.session_state['load_strategy'],
                    key='load_strategy_multiselect',
                    help="Multiple selections ensure diverse high-quality configs are loaded"
                )
            
            with col2:
                st.markdown("#### Current Strategy")
                st.info(f"**Active:** {', '.join(st.session_state['load_strategy'])}")
                
                # Show quota distribution
                if new_strategy:
                    st.markdown("#### Quota Distribution")
                    configs_per_criterion = new_max_configs // len(new_strategy)
                    st.caption(f"**{len(new_strategy)} criteria** selected")
                    st.caption(f"→ **~{configs_per_criterion:,} configs** per criterion")
                    st.caption(f"→ Total: **{new_max_configs:,} configs** (deduplicated)")
                
                # Update button
                settings_changed = (new_strategy != st.session_state['load_strategy'] and new_strategy) or \
                                 (new_max_configs != st.session_state['max_configs'])
                
                if settings_changed:
                    if st.button("🔄 Apply & Reload", width='stretch', type="primary"):
                        # Save to session state
                        st.session_state['load_strategy'] = new_strategy
                        st.session_state['max_configs'] = new_max_configs
                        # Save to pbgui.ini
                        pbfunc.save_ini('pareto', 'load_strategy', ','.join(new_strategy))
                        pbfunc.save_ini('pareto', 'max_configs', str(new_max_configs))
                        st.session_state['load_strategy_expander_open'] = False
                        st.cache_resource.clear()
                        st.rerun()
                    st.warning("⚠️ Changes pending - click Apply to reload")
                elif not new_strategy:
                    st.error("⚠️ Select at least one criterion")
                else:
                    # Same strategy - show option to close expander
                    if st.button("✅ Close Settings", width='stretch'):
                        st.session_state['load_strategy_expander_open'] = False
                        st.rerun()
            
            # Detailed explanation
            st.markdown("---")
            st.markdown("#### 📚 Criterion Descriptions")
            
            cols = st.columns(3)
            with cols[0]:
                st.markdown("""
                **Performance Metrics:**
                - **performance**: CV + ADG (Passivbot official)
                - **sharpe**: Return/volatility ratio
                - **calmar**: Return/max drawdown
                """)
            
            with cols[1]:
                st.markdown("""
                **Risk Metrics:**
                - **drawdown**: Smallest equity drops
                - **volatility**: Stable, predictable returns
                - **recovery**: Fast bounce-back from DDs
                """)
            
            with cols[2]:
                st.markdown("""
                **Advanced Metrics:**
                - **robustness**: Low CV across scenarios
                - **sortino**: Return/downside deviation
                - **omega**: Probability-weighted gains/losses
                """)
        
        st.markdown("---")
        
        # Optimization Summary
        col1, col2, col3, col4 = st.columns(4)
        
        # Get original total (before view filter)
        all_results_loaded = st.session_state.get('all_results_loaded', False)
        if all_results_loaded and hasattr(self, '_original_configs'):
            total_loaded = len(self._original_configs)
            total_showing = len(self.view_configs)
        else:
            total_loaded = len(self.loader.configs)
            total_showing = len(self.view_configs)
        
        pareto_count = sum(1 for c in self.view_configs if c.is_pareto)
        convergence = min(100, (pareto_count / max(total_showing * 0.001, 1)) * 100)
        
        with col1:
            if total_loaded != total_showing:
                st.metric("📊 Showing", f"{total_showing:,}", 
                         help=f"Displaying {total_showing:,} of {total_loaded:,} loaded configs")
            else:
                st.metric("🔢 Total Configs", f"{total_loaded:,}")
        with col2:
            st.metric("⭐ Pareto Optimal", pareto_count,
                     help=f"Pareto front computed for visible {total_showing:,} configs")
        with col3:
            st.metric("📈 Convergence", f"{convergence:.0f}%")
        with col4:
            st.metric("🎯 Scenarios", len(self.loader.scenario_labels))
        
        st.markdown("---")
        
        # Top Champions
        st.subheader("🏆 TOP CHAMPIONS")
        
        # Selection criteria explanation
        col1, col2 = st.columns([3, 1])
        with col1:
            st.markdown("**Top 5 DIVERSE champions (near-duplicates filtered out, <2% difference = too similar)**")
        with col2:
            with st.popover("ℹ️ How are Champions selected?"):
                st.markdown("""
                **Selection Criteria:**
                1. **Performance**: Primary scoring metric ({})
                2. **Robustness**: Consistency across scenarios (1/CV)
                3. **Composite Score**: Performance × Robustness
                
                **Robustness Calculation:**
                - CV = Coefficient of Variation (std/mean)
                - Robustness = 1 / (1 + CV)
                - Range: 0.0 (unstable) to 1.0 (perfectly stable)
                - Lower CV = Higher robustness = More consistent performance
                
                **Why this matters:**
                - High performance + Low robustness = Risky (good in some scenarios, bad in others)
                - Medium performance + High robustness = Reliable (consistent across all scenarios)
                """.format(self.loader.scoring_metrics[0] if self.loader.scoring_metrics else 'adg_w_usd'))
        
        scoring_metrics = self.loader.scoring_metrics if self.loader.scoring_metrics else ['adg_w_usd']
        primary_metric = scoring_metrics[0] if scoring_metrics else 'adg_w_usd'
        
        # Get Pareto configs and sort by composite score (performance × robustness)
        pareto_configs = self.loader.get_pareto_configs()
        
        # Calculate composite scores
        configs_with_scores = []
        for config in pareto_configs:
            performance = config.suite_metrics.get(primary_metric, 0.0)
            robustness = self.loader.compute_overall_robustness(config)
            composite_score = performance * robustness  # Balanced score
            configs_with_scores.append((config, performance, robustness, composite_score))
        
        # Sort by composite score (descending), then robustness, then performance
        configs_with_scores.sort(key=lambda x: (-x[3], -x[2], -x[1]))
        
        # Filter out near-duplicates: configs that are too similar to already selected ones
        top_configs_data = []
        similarity_threshold = 0.02  # 2% difference threshold for composite score
        
        for candidate in configs_with_scores:
            if len(top_configs_data) >= 5:
                break
                
            candidate_config, candidate_perf, candidate_rob, candidate_score = candidate
            
            # Check if this config is too similar to any already selected config
            is_too_similar = False
            for selected in top_configs_data:
                selected_config, selected_perf, selected_rob, selected_score = selected
                
                # Calculate relative difference in composite score
                if selected_score > 0:
                    score_diff = abs(candidate_score - selected_score) / selected_score
                else:
                    score_diff = abs(candidate_score - selected_score)
                
                # Calculate relative difference in performance
                if selected_perf > 0:
                    perf_diff = abs(candidate_perf - selected_perf) / selected_perf
                else:
                    perf_diff = abs(candidate_perf - selected_perf)
                
                # Calculate difference in robustness
                rob_diff = abs(candidate_rob - selected_rob)
                
                # If all metrics are very similar, consider it a near-duplicate
                if score_diff < similarity_threshold and perf_diff < similarity_threshold and rob_diff < 0.02:
                    is_too_similar = True
                    break
            
            if not is_too_similar:
                top_configs_data.append(candidate)
        
        for i, (config, performance, robustness, composite_score) in enumerate(top_configs_data, 1):
            # Show composite score in expander title with more precision
            with st.expander(
                f"**#{i}: Config #{config.config_index}** - {self.loader.compute_trading_style(config)} | "
                f"🎯 Score: {composite_score:.9f} (Perf: {performance:.9f} × Rob: {robustness:.4f})", 
                expanded=(i==1)
            ):
                
                # Show the detailed breakdown with full precision
                st.markdown(
                    f"**Composite Score = Performance × Robustness**\n\n"
                    f"= {performance:.9f} × {robustness:.4f} = **{composite_score:.9f}**"
                )
                st.markdown("---")
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.markdown("**📊 Performance**")
                    
                    # Get weighted toggle state
                    use_weighted = st.session_state.get('use_weighted_metrics', True)
                    
                    # Adg metric (weighted or not)
                    adg_metric = 'adg_w_usd' if use_weighted else 'adg_usd'
                    if adg_metric in config.suite_metrics:
                        val = config.suite_metrics[adg_metric]
                        metric_label = "Adg W Usd" if use_weighted else "Adg Usd"
                        help_text = "Average Daily Gain (weighted) in USD - Daily profit rate accounting for wallet exposure" if use_weighted else "Average Daily Gain in USD - Daily profit rate"
                        st.metric(metric_label, f"{val:.6f}", help=help_text)
                    
                    # Sharpe Ratio metric (weighted or not)
                    sharpe_metric = 'sharpe_ratio_w_usd' if use_weighted else 'sharpe_ratio_usd'
                    if sharpe_metric in config.suite_metrics:
                        val = config.suite_metrics[sharpe_metric]
                        metric_label = "Sharpe Ratio W Usd" if use_weighted else "Sharpe Ratio Usd"
                        help_text = "Risk-adjusted return (weighted) - Accounts for wallet exposure" if use_weighted else "Risk-adjusted return metric - Higher is better. Measures excess return per unit of risk"
                        st.metric(metric_label, f"{val:.6f}", help=help_text)
                    
                    # Gain Usd (same for both)
                    if 'gain_usd' in config.suite_metrics:
                        val = config.suite_metrics['gain_usd']
                        st.metric("Gain Usd", f"{val:.6f}", help="Total profit multiplier - How many times the initial balance was gained")
                
                with col2:
                    st.markdown("**🛡️ Risk Profile**")
                    risk_scores = self.loader.compute_risk_profile_score(config)
                    st.metric("Overall Risk Score", f"{risk_scores['overall']:.1f}/10", 
                             help="Combined risk assessment - Lower score = safer strategy. Based on drawdown, volatility, and recovery metrics")
                    st.metric("Drawdown Score", f"{risk_scores['drawdown']:.1f}/10",
                             help="Maximum portfolio decline score - Lower = smaller drawdowns. Measures worst-case equity drop")
                
                with col3:
                    st.markdown("**💪 Robustness**")
                    overall_robust = self.loader.compute_overall_robustness(config)
                    stars = "⭐" * int(overall_robust * 5)
                    st.metric("Robustness", f"{overall_robust:.2f}",
                             help="Consistency score (0-1) - Higher = more stable across scenarios. Calculated as 1/(1+CV) where CV is coefficient of variation")
                    st.markdown(f"**{stars}**")
                
                # Scenario breakdown
                if config.scenario_metrics:
                    st.markdown("**🌍 Scenario Performance**")
                    scenario_cols = st.columns(len(self.loader.scenario_labels))
                    for j, scenario in enumerate(self.loader.scenario_labels):
                        with scenario_cols[j]:
                            if scenario in config.scenario_metrics:
                                perf = config.scenario_metrics[scenario].get(primary_metric, 0)
                                st.metric(scenario.title(), f"{perf:.6f}")
                
                st.markdown("---")
                
                # Action buttons
                col_action1, col_action2 = st.columns(2)
                
                with col_action1:
                    # Load full config for display and backtest
                    full_config_data = None
                    if st.button(f"📋 Show Full Config ##{i}_config", key=f"show_config_champ_{i}", width='stretch'):
                        try:
                            full_config_data = self.loader.get_full_config(config.config_index)
                            if full_config_data:
                                with st.expander(f"Full Configuration #{config.config_index}", expanded=True):
                                    st.json(full_config_data)
                            else:
                                st.warning("⚠️ Could not load full config - showing bot params only")
                                st.json(config.bot_params)
                        except Exception as e:
                            st.error(f"❌ Error loading config: {str(e)}")
                
                with col_action2:
                    if st.button(f"🚀 Run Backtest ##{i}_bt", key=f"run_bt_champ_{i}", width='stretch', type="primary"):
                        try:
                            import BacktestV7
                            from pbgui_func import get_navi_paths, pb7dir
                            from pathlib import Path
                            import json
                            import time
                            
                            # Load full config if not already loaded
                            if not full_config_data:
                                full_config_data = self.loader.get_full_config(config.config_index)
                            
                            if not full_config_data:
                                st.error("❌ Full config data not available")
                                st.stop()
                            
                            # Create config directory
                            config_dir = Path(f'{pb7dir()}/configs/pareto_selected')
                            config_dir.mkdir(parents=True, exist_ok=True)
                            
                            # Generate unique filename
                            timestamp = int(time.time())
                            config_filename = f"pareto_champ_{config.config_index}_{timestamp}.json"
                            config_path = config_dir / config_filename
                            
                            # Save the full config
                            with open(config_path, 'w') as f:
                                json.dump(full_config_data, f, indent=2)
                            
                            # Cleanup backtest session state
                            if "bt_v7_queue" in st.session_state:
                                del st.session_state.bt_v7_queue
                            if "bt_v7_results" in st.session_state:
                                del st.session_state.bt_v7_results
                            if "bt_v7_edit_symbol" in st.session_state:
                                del st.session_state.bt_v7_edit_symbol
                            if "config_v7_archives" in st.session_state:
                                del st.session_state.config_v7_archives
                            if "config_v7_config_archive" in st.session_state:
                                del st.session_state.config_v7_config_archive
                            
                            # Create BacktestV7Item and switch to backtest page
                            st.session_state.bt_v7 = BacktestV7.BacktestV7Item(str(config_path))
                            st.switch_page(get_navi_paths()["V7_BACKTEST"])
                            
                        except Exception as e:
                            st.error(f"❌ Error preparing backtest: {str(e)}")
                            import traceback
                            st.code(traceback.format_exc())
        
        st.markdown("---")
        
        # Smart Insights
        st.subheader("💡 INSIGHTS")
        
        insights = self._generate_insights()
        bounds_info = self.loader.get_parameters_at_bounds(tolerance=0.1)
        
        for insight_type, insight_text in insights:
            if insight_type == "warning":
                # Check if this is the bounds warning
                if "parameters are near bounds" in insight_text:
                    st.warning(f"⚠️ {insight_text}")
                    # Add expander with details
                    with st.expander("🔍 Show parameters near bounds"):
                        if bounds_info['at_lower']:
                            st.markdown("**At Lower Bound:**")
                            for param, info in bounds_info['at_lower'].items():
                                st.markdown(f"- `{param}`: {info['value']:.4f} (bound: {info['bound']:.4f})")
                        
                        if bounds_info['at_upper']:
                            st.markdown("**At Upper Bound:**")
                            for param, info in bounds_info['at_upper'].items():
                                st.markdown(f"- `{param}`: {info['value']:.4f} (bound: {info['bound']:.4f})")
                else:
                    st.warning(f"⚠️ {insight_text}")
            elif insight_type == "success":
                st.success(f"✅ {insight_text}")
            elif insight_type == "info":
                st.info(f"💡 {insight_text}")
        
        st.markdown("---")
        
        # Quick Visualization
        st.subheader("📈 PARETO FRONT PREVIEW")
        
        st.caption("💡 Click on any point to view config details or start a backtest")
        
        # Config counts for charts
        pareto_in_view = sum(1 for c in self.view_configs if c.is_pareto)
        
        # Metric weight toggle (session state)
        if 'use_weighted_metrics' not in st.session_state:
            st.session_state.use_weighted_metrics = True
        
        # Toggle in header
        col_toggle, col_spacer = st.columns([1, 5])
        with col_toggle:
            use_weighted = st.toggle("Use weighted (_w) metrics", value=st.session_state.use_weighted_metrics, 
                                    help="Weighted metrics account for wallet exposure and are generally more accurate",
                                    key='weighted_toggle')
            st.session_state.use_weighted_metrics = use_weighted
        
        # Adjust scoring metrics based on toggle
        if use_weighted:
            # Try to use weighted versions
            display_metrics = []
            for metric in scoring_metrics:
                if '_w_' not in metric and not metric.endswith('_w'):
                    # Try to find weighted version
                    weighted_metric = metric.replace('_usd', '_w_usd').replace('_btc', '_w_btc')
                    if weighted_metric in self.loader.configs[0].suite_metrics:
                        display_metrics.append(weighted_metric)
                    else:
                        display_metrics.append(metric)  # Fallback to unweighted
                else:
                    display_metrics.append(metric)
        else:
            # Use unweighted versions
            display_metrics = []
            for metric in scoring_metrics:
                if '_w_' in metric or metric.endswith('_w_usd') or metric.endswith('_w_btc'):
                    # Remove _w
                    unweighted_metric = metric.replace('_w_', '_').replace('_w_usd', '_usd').replace('_w_btc', '_btc')
                    display_metrics.append(unweighted_metric)
                else:
                    display_metrics.append(metric)
        
        col1, col2 = st.columns(2)
        
        with col1:
            col_title, col_help = st.columns([3, 1])
            with col_title:
                st.markdown(f"**Pareto Analysis** - Showing {len(self.view_configs):,} configs | {pareto_in_view} Pareto ⭐")
            with col_help:
                with st.popover("📖 Guide"):
                    st.markdown("""
                    **How to read this chart:**
                    
                    **Points:**
                    - **⭐ White stars**: Pareto-optimal configs (no other config is strictly better in all metrics)
                    - **Colored dots**: Non-Pareto configs (color shows drawdown - lighter = better)
                    
                    **Pareto Front:**
                    The upper-right boundary of points forms the Pareto front. These configs represent 
                    the best trade-offs between the two metrics - you can't improve one without 
                    worsening the other.
                    
                    **Strategy:**
                    - Look for stars (⭐) on the Pareto front in the upper-right
                    - Lighter colors = lower drawdown (safer)
                    - Click any point to see full config details and start a backtest
                    """)
            
            # 2D Scatter
            if len(display_metrics) >= 2:
                fig = self.viz.plot_pareto_scatter_2d(
                    x_metric=display_metrics[0],
                    y_metric=display_metrics[1],
                    color_metric='drawdown_worst_usd' if 'drawdown_worst_usd' in self.loader.configs[0].suite_metrics else None,
                    show_all=True
                )
                event = st.plotly_chart(fig, width='stretch', on_select="rerun", key='preview_2d')
                
                # Handle click event
                if event and hasattr(event, 'selection') and event.selection:
                    try:
                        points = event.selection.get('points', [])
                        if points:
                            point_data = points[0]
                            clicked_config_index = None
                            
                            # Try different ways to get config_index
                            if 'customdata' in point_data and point_data['customdata'] is not None:
                                customdata = point_data['customdata']
                                
                                # Handle different customdata formats
                                if isinstance(customdata, dict):
                                    clicked_config_index = int(customdata.get('0', customdata.get(0)))
                                elif isinstance(customdata, (list, tuple)) and len(customdata) > 0:
                                    clicked_config_index = int(customdata[0])
                                elif isinstance(customdata, (int, float)):
                                    clicked_config_index = int(customdata)
                            
                            # Fallback: use point_index
                            if clicked_config_index is None and 'point_index' in point_data:
                                idx = point_data['point_index']
                                configs = self.loader.configs[:1000]
                                if 0 <= idx < len(configs):
                                    clicked_config_index = configs[idx].config_index
                            
                            # Show config details and backtest button in expander
                            if clicked_config_index is not None:
                                config = self.loader.get_config_by_index(clicked_config_index)
                                if config:
                                    with st.expander(f"📋 Config #{clicked_config_index} Details", expanded=True):
                                        self._show_config_details(clicked_config_index, key_prefix="cc_2d")
                    except Exception as e:
                        st.warning(f"⚠️ Click handling error: {e}")
        
        with col2:
            col_title, col_help = st.columns([3, 1])
            with col_title:
                st.markdown(f"**Robustness vs Performance** - Showing {len(self.view_configs):,} configs | {pareto_in_view} Pareto ⭐")
            with col_help:
                with st.popover("📖 Guide"):
                    st.markdown("""
                    **How to read this chart:**
                    
                    **Quadrants:**
                    - **Top-Right (🏆 Best of Both)**: High performance + High robustness = IDEAL
                    - **Top-Left (🛡️ Stable but Slow)**: Low performance but very consistent
                    - **Bottom-Right (⚠️ High Risk)**: High performance but unstable = RISKY
                    - **Bottom-Left**: Low performance + Low robustness = Avoid
                    
                    **Lines:**
                    - **Vertical dashed**: Median performance (splits configs into above/below average)
                    - **Horizontal dashed**: Median robustness (splits configs into stable/unstable)
                    
                    **Strategy:**
                    Look for configs in the top-right quadrant (Best of Both) - they offer the best 
                    balance of high returns and consistent performance across market conditions.
                    """)
            
            # Robustness Quadrant
            primary_display_metric = display_metrics[0] if display_metrics else primary_metric
            fig = self.viz.plot_robustness_quadrant(
                performance_metric=primary_display_metric,
                show_all=True
            )
            event = st.plotly_chart(fig, width='stretch', on_select="rerun", key='preview_robustness')
            
            # Handle click event
            if event and hasattr(event, 'selection') and event.selection:
                try:
                    points = event.selection.get('points', [])
                    if points:
                        point_data = points[0]
                        clicked_config_index = None
                        
                        # Try different ways to get config_index
                        if 'customdata' in point_data and point_data['customdata'] is not None:
                            customdata = point_data['customdata']
                            
                            # Handle different customdata formats
                            if isinstance(customdata, dict):
                                clicked_config_index = int(customdata.get('0', customdata.get(0)))
                            elif isinstance(customdata, (list, tuple)) and len(customdata) > 0:
                                clicked_config_index = int(customdata[0])
                            elif isinstance(customdata, (int, float)):
                                clicked_config_index = int(customdata)
                        
                        # Fallback: use point_index
                        if clicked_config_index is None and 'point_index' in point_data:
                            idx = point_data['point_index']
                            pareto_configs = self.loader.get_pareto_configs()
                            if 0 <= idx < len(pareto_configs):
                                clicked_config_index = pareto_configs[idx].config_index
                        
                        # Show config details and backtest button in expander
                        if clicked_config_index is not None:
                            config = self.loader.get_config_by_index(clicked_config_index)
                            if config:
                                with st.expander(f"📋 Config #{clicked_config_index} Details", expanded=True):
                                    self._show_config_details(clicked_config_index, key_prefix="cc_robustness")
                except Exception as e:
                    st.warning(f"⚠️ Click handling error: {e}")
    
    @st.fragment
    def _show_config_details_fragment(self):
        """Fragment: Config details section - only reloads this part on selection change"""
        
        # Config selector
        st.subheader("🔍 SELECTED CONFIG DETAILS")
        
        # Get all configs (not just Pareto)
        all_configs = self.view_configs
        
        if not all_configs:
            st.warning("⚠️ No configs available")
            st.stop()
        
        all_indices = [c.config_index for c in all_configs]
        pareto_configs = [c for c in all_configs if c.is_pareto]
        pareto_count = len(pareto_configs)
        
        # Initialize session state for selected config if not exists
        if 'pareto_selected_config' not in st.session_state:
            st.session_state.pareto_selected_config = all_indices[0]
        
        # Ensure selected config is in the list
        if st.session_state.pareto_selected_config not in all_indices:
            st.session_state.pareto_selected_config = all_indices[0]
        
        # Build display labels (but keep widget values as stable ints)
        index_to_label = {}
        for c in all_configs:
            label = f"Config #{c.config_index}"
            if c.is_pareto:
                label += " ⭐"
            index_to_label[c.config_index] = label

        widget_key = 'fragment_config_selector'

        desired_idx = st.session_state.pareto_selected_config
        if desired_idx not in all_indices:
            desired_idx = all_indices[0]
            st.session_state.pareto_selected_config = desired_idx

        # Keep widget synced to the selected config (external changes)
        if widget_key not in st.session_state or st.session_state.get(widget_key) not in all_indices:
            st.session_state[widget_key] = desired_idx
        elif st.session_state.get(widget_key) != desired_idx:
            st.session_state[widget_key] = desired_idx

        def _on_fragment_config_change():
            idx = st.session_state.get(widget_key)
            if isinstance(idx, int) and idx in all_indices:
                st.session_state.pareto_selected_config = idx
                st.session_state['_pareto_last_change'] = 'fragment_selectbox'

        # Render selectbox (value=int config_index)
        st.selectbox(
            "🎯 Select Config:",
            options=all_indices,
            format_func=lambda idx: index_to_label.get(idx, f"Config #{idx}"),
            help=f"Choose from {len(all_configs)} configs ({pareto_count} Pareto-optimal ⭐)",
            key=widget_key,
            on_change=_on_fragment_config_change,
        )
        
        # Get the selected config from session state
        selected_config_index = st.session_state.pareto_selected_config
        
        # Find the config with this config_index
        config = next((c for c in all_configs if c.config_index == selected_config_index), None)
        
        if config is None:
            st.error(f"❌ Config #{selected_config_index} not found in loaded configs")
            st.stop()
        
        # Get currency and weighted settings from session state (set by Correlations tab)
        display_currency = st.session_state.get('pareto_currency', 'USD')
        display_weighted = st.session_state.get('pareto_use_weighted', True)
        currency_suffix = "_usd" if display_currency == "USD" else "_btc"
        
        # Build metric names based on settings
        def get_display_metric(base_name):
            """Get the actual metric name to display based on settings"""
            weighted_metric = f"{base_name}_w{currency_suffix}"
            base_metric = f"{base_name}{currency_suffix}"
            # Try weighted first if enabled and exists, otherwise base
            if display_weighted and weighted_metric in config.suite_metrics:
                return weighted_metric
            return base_metric
        
        # Metric help texts
        metric_helps = {
            'adg_w_usd': "Average Daily Gain (weighted) in USD - Daily profit rate accounting for wallet exposure",
            'sharpe_ratio_usd': "Risk-adjusted return metric - Higher is better. Measures excess return per unit of risk",
            'sharpe_ratio_w_usd': "Weighted Sharpe Ratio - Risk-adjusted return accounting for wallet exposure",
            'gain_usd': "Total profit multiplier - How many times the initial balance was gained",
            'calmar_ratio_usd': "Return vs maximum drawdown - Higher is better. Measures profit relative to worst loss",
            'sortino_ratio_usd': "Downside risk-adjusted return - Like Sharpe but only penalizes downside volatility",
            'omega_ratio_usd': "Probability-weighted ratio of gains vs losses - Higher is better",
            'sterling_ratio_usd': "Return vs average drawdown - Consistency of returns relative to typical losses",
            'drawdown_worst_usd': "Maximum portfolio decline - Lower is better. Worst equity drop from peak",
        }
        
        # Display metrics based on Top Performers selection or fallback to scoring metrics
        display_metrics = st.session_state.get('pareto_top_metrics', [])
        
        # Fallback: if no top metrics stored (e.g., first load or other tabs), use scoring metrics
        if not display_metrics:
            for base_metric in ['adg', 'sharpe_ratio', 'gain', 'calmar_ratio', 'sortino_ratio']:
                actual_metric = get_display_metric(base_metric)
                if actual_metric in config.suite_metrics:
                    display_metrics.append(actual_metric)
        
        max_display = 10 if st.session_state.get('pareto_top_metrics') else 5
        metrics_to_show = [m for m in display_metrics[:max_display] if m in config.suite_metrics]
        split_at = (len(metrics_to_show) + 1) // 2
        metrics_left = metrics_to_show[:split_at]
        metrics_right = metrics_to_show[split_at:]
        
        # Show config details: 2× Metrics + Trading Style + Robustness
        col_metrics_a, col_metrics_b, col_style, col_robust = st.columns(4)
        
        with col_metrics_a:
            st.markdown("**📊 Metrics**")
            for metric in metrics_left:
                help_text = metric_helps.get(metric, f"{metric.replace('_', ' ').title()} - Performance metric")
                st.metric(
                    metric.replace('_', ' ').title(),
                    f"{config.suite_metrics[metric]:.6f}",
                    help=help_text,
                )
        
        with col_metrics_b:
            st.markdown(" ")
            for metric in metrics_right:
                help_text = metric_helps.get(metric, f"{metric.replace('_', ' ').title()} - Performance metric")
                st.metric(
                    metric.replace('_', ' ').title(),
                    f"{config.suite_metrics[metric]:.6f}",
                    help=help_text,
                )
        
        with col_style:
            st.markdown("**🎯 Trading Style**")
            style = self.loader.compute_trading_style(config)
            st.markdown(f"**{style}**")
            st.metric("Positions/Day", f"{config.suite_metrics.get('positions_held_per_day', 0):.2f}",
                     help="Average number of positions opened per day - Higher = more active trading")
            st.metric("Avg Hold Hours", f"{config.suite_metrics.get('position_held_hours_mean', 0):.1f}",
                     help="Average time positions are held open - Lower = faster turnover, scalping style")
        
        with col_robust:
            st.markdown("**💪 Robustness**")
            robust = self.loader.compute_overall_robustness(config)
            st.metric("Overall Score", f"{robust:.3f}",
                     help="Consistency score (0-1) - Higher = more stable across scenarios. Calculated as 1/(1+CV)")
            
            # Use appropriate ADG metric for std dev display
            adg_metric = get_display_metric('adg')
            if config.metric_stats and adg_metric in config.metric_stats:
                stats = config.metric_stats[adg_metric]
                st.metric("Std Dev", f"{stats['std']:.6f}",
                         help="Standard deviation of daily gains - Lower = more predictable performance")
        
        st.markdown("---")
        
        # Load full config once for both display and backtest
        # Use loader's get_full_config method (loads Pareto JSON template + merges optimized params)
        full_config_data = None
        try:
            full_config_data = self.loader.get_full_config(config.config_index)
            if not full_config_data:
                st.warning("⚠️ Could not load full config - Pareto directory may be empty")
        except Exception as e:
            st.warning(f"⚠️ Could not load full config: {str(e)}")
            import traceback
            st.code(traceback.format_exc())
        
        # Detailed views in expanders
        col_left, col_right = st.columns(2)
        
        with col_left:
            with st.expander("📋 Full Configuration", expanded=False):
                if full_config_data:
                    st.json(full_config_data)
                else:
                    st.warning("Full config not available - showing bot params only")
                    st.json(config.bot_params)
        
        with col_right:
            with st.expander("📊 All Metrics & Statistics", expanded=False):
                # Show all suite metrics
                st.markdown("**Suite Metrics:**")
                metrics_df = []
                for metric, value in sorted(config.suite_metrics.items()):
                    metrics_df.append({"Metric": metric, "Value": f"{value:.6f}"})
                st.dataframe(metrics_df, width='stretch', hide_index=True)
                
                # Show metric statistics if available
                if config.metric_stats:
                    st.markdown("---")
                    st.markdown("**Metric Statistics (across scenarios):**")
                    for metric, stats in list(config.metric_stats.items())[:10]:
                        st.markdown(f"**{metric}:**")
                        st.text(f"  Mean: {stats.get('mean', 0):.6f}")
                        st.text(f"  Std:  {stats.get('std', 0):.6f}")
                        st.text(f"  Min:  {stats.get('min', 0):.6f}")
                        st.text(f"  Max:  {stats.get('max', 0):.6f}")
        
        # Backtest button
        st.markdown("---")
        col_bt1, col_bt2, col_bt3 = st.columns([1, 2, 1])
        with col_bt2:
            if st.button("🚀 Run Backtest with this Config", use_container_width=True, type="primary"):
                try:
                    import BacktestV7
                    from pbgui_func import get_navi_paths, pb7dir
                    from pathlib import Path
                    import json
                    import time
                    
                    # Use already loaded full_config_data
                    if not full_config_data:
                        st.error("❌ Full config data not available")
                        st.stop()
                    
                    # Create config directory if not exists
                    config_dir = Path(f'{pb7dir()}/configs/pareto_selected')
                    config_dir.mkdir(parents=True, exist_ok=True)
                    
                    # Generate unique filename
                    timestamp = int(time.time())
                    config_filename = f"pareto_config_{config.config_index}_{timestamp}.json"
                    config_path = config_dir / config_filename
                    
                    # Save the full config (has backtest, bot, optimize, live sections)
                    with open(config_path, 'w') as f:
                        json.dump(full_config_data, f, indent=2)
                    
                    # Cleanup backtest session state (like in OptimizeV7)
                    if "bt_v7_queue" in st.session_state:
                        del st.session_state.bt_v7_queue
                    if "bt_v7_results" in st.session_state:
                        del st.session_state.bt_v7_results
                    if "bt_v7_edit_symbol" in st.session_state:
                        del st.session_state.bt_v7_edit_symbol
                    if "config_v7_archives" in st.session_state:
                        del st.session_state.config_v7_archives
                    if "config_v7_config_archive" in st.session_state:
                        del st.session_state.config_v7_config_archive
                    
                    # Create BacktestV7Item and switch to backtest page
                    st.session_state.bt_v7 = BacktestV7.BacktestV7Item(str(config_path))
                    st.switch_page(get_navi_paths()["V7_BACKTEST"])
                    
                except Exception as e:
                    st.error(f"❌ Error preparing backtest: {str(e)}")
                    import traceback
                    st.code(traceback.format_exc())

        self._render_optimize_preset_generator(
            config=config,
            full_config_data=full_config_data,
            key_prefix=f"pp_opt_{config.config_index}",
        )
    
    def _show_pareto_playground(self):
        """Stage 2: Pareto Playground - Interactive Exploration"""
        
        st.title("🎨 PARETO PLAYGROUND")
        st.markdown("**Interactive multi-dimensional exploration**")
        
        st.markdown("---")
        
        # Preference sliders
        st.subheader("🎚️ YOUR PREFERENCES")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            perf_weight = st.slider(
                "Performance Priority", 
                0, 100, 80, 5, 
                key='perf_weight',
                help="How much you prioritize profit and returns. Higher values favor configs with better daily gains and overall performance metrics."
            )
        with col2:
            risk_weight = st.slider(
                "Risk Aversion", 
                0, 100, 60, 5, 
                key='risk_weight',
                help="How much you want to avoid losses. Higher values favor configs with lower maximum drawdowns and better risk management."
            )
        with col3:
            robust_weight = st.slider(
                "Robustness Importance", 
                0, 100, 70, 5, 
                key='robust_weight',
                help="How much you value consistency across different scenarios. Higher values favor configs with stable, predictable performance."
            )
        
        # Track previous slider values to detect changes
        current_weights = (perf_weight, risk_weight, robust_weight)
        if 'prev_slider_weights' not in st.session_state:
            st.session_state['prev_slider_weights'] = current_weights
        
        sliders_changed = st.session_state['prev_slider_weights'] != current_weights
        
        # Track all_results_loaded state to detect when it changes
        current_all_results_loaded = st.session_state.get('all_results_loaded', False)
        if 'prev_all_results_loaded' not in st.session_state:
            st.session_state['prev_all_results_loaded'] = current_all_results_loaded
        
        all_results_just_loaded = (not st.session_state['prev_all_results_loaded']) and current_all_results_loaded
        
        # Compute weighted score for Pareto configs
        pareto_configs = self.loader.get_pareto_configs()
        
        if not pareto_configs:
            st.warning("⚠️ No Pareto configs found - using all configs")
            pareto_configs = self.loader.configs[:100]  # Fallback: top 100
        
        # Now we have configs to work with
        scoring_metrics = self.loader.scoring_metrics if self.loader.scoring_metrics else ['adg_w_usd']
        primary_metric = scoring_metrics[0]
        
        best_match = None
        best_score = -np.inf
        
        for config in pareto_configs:
            perf = config.suite_metrics.get(primary_metric, 0)
            risk = 1.0 - config.suite_metrics.get('drawdown_worst_usd', 0.1)  # Invert (lower DD = better)
            robust = self.loader.compute_overall_robustness(config)
            
            # Normalize to [0, 1]
            perf_norm = perf / max(c.suite_metrics.get(primary_metric, 0.001) for c in pareto_configs)
            
            # Weighted score
            score = (perf_norm * perf_weight + risk * risk_weight + robust * robust_weight) / (perf_weight + risk_weight + robust_weight)
            
            if score > best_score:
                best_score = score
                best_match = config
        
        # Auto-select best match in session state
        # Update when: 1) first load, 2) sliders changed, 3) all_results just loaded
        if 'pareto_selected_config' not in st.session_state or sliders_changed or all_results_just_loaded:
            st.session_state['pareto_selected_config'] = best_match.config_index
            st.session_state['pareto_selectbox_key'] = best_match.config_index
            st.session_state['prev_slider_weights'] = current_weights
            st.session_state['prev_all_results_loaded'] = current_all_results_loaded
        
        st.success(f"🎯 **Best Match for Your Preferences:** Config #{best_match.config_index} (Score: {best_score:.3f})")
        
        st.markdown("---")
        
        # Visualization controls
        st.subheader("📊 MULTI-DIMENSIONAL EXPLORER")
        
        col1, col2 = st.columns([2, 1])
        
        with col2:
            st.markdown("**Chart Settings**")
            
            # Offer multiple visualization options
            viz_type_options = [
                "2D Scatter",
                "3D Scatter (WebGL)",
                "3D Projections (2D)",
                "Radar Chart"
            ]
            
            viz_type = st.radio(
                "Visualization:",
                viz_type_options,
                key='viz_type',
                help="💡 2D/Radar: No WebGL | 3D WebGL: Browser 3D | 3D Projections: Three 2D views (works everywhere)"
            )
            
            # Normalize viz_type for backward compatibility
            if "WebGL" in viz_type:
                viz_type = "3D Scatter"
            elif "Projections" in viz_type:
                viz_type = "3D Projections"
            
            # Defensive: ensure downstream logic uses the same value the widget is bound to
            viz_type_raw = st.session_state.get('viz_type', viz_type)
            if "WebGL" in viz_type_raw:
                viz_type = "3D Scatter"
            elif "Projections" in viz_type_raw:
                viz_type = "3D Projections"
            else:
                viz_type = viz_type_raw
            
            # Show "Show all configs" checkbox only for Scatter plots, not for Radar Chart
            if viz_type in ["2D Scatter", "3D Scatter", "3D Projections"]:
                all_results_loaded = st.session_state.get('all_results_loaded', False)
                
                if all_results_loaded:
                    show_all = st.checkbox("Show all configs", value=False, 
                                         key='show_all_playground',
                                         help="Show non-Pareto configs in grey for comparison")
                else:
                    show_all = False
                    st.caption("ℹ️ Load all_results.bin to compare with non-Pareto configs")
            else:
                show_all = False  # Radar Chart always shows only top configs
            
            available_metrics = list(self.loader.configs[0].suite_metrics.keys()) if self.loader.configs else []
            
            # Separate handling for 2D and 3D
            if viz_type == "2D Scatter":
                # 2D Scatter - Original preset logic
                # Preset configurations for common analysis patterns
                st.markdown("---")
                
                # Toggles for metric selection
                col_toggle1, col_toggle2 = st.columns(2)
                with col_toggle1:
                    use_weighted = st.toggle("Use Weighted (_w) Metrics", value=True, 
                                            help="Weighted metrics emphasize recent performance (recency-biased)")
                with col_toggle2:
                    use_btc = st.toggle("Use BTC instead of USD", value=False,
                                       help="Switch between USD and BTC denominated metrics")
                
                # Define presets based on metric weight and currency
                suffix = "_w" if use_weighted else ""
                currency = "btc" if use_btc else "usd"
                
                # Helper function to build metric name from base using centralized Config.py definitions
                def metric(base):
                    """Build full metric name with suffix and currency if needed"""
                    if base in CURRENCY_METRICS:
                        # Check if base already has the weighted suffix (_w)
                        # Must check for _w as suffix, not as part of the name (e.g., drawdown_worst)
                        has_w_suffix = base.endswith('_w') or '_w_per_exposure_' in base
                        
                        if use_weighted and not has_w_suffix:
                            # Try to add _w, but first check if weighted variant exists in Config.py
                            base_with_w = f"{base}_w"
                            if base_with_w in CURRENCY_METRICS:
                                return f"{base_with_w}_{currency}"
                            else:
                                # No weighted variant exists, use base as-is
                                return f"{base}_{currency}"
                        elif not use_weighted and has_w_suffix:
                            # Remove _w from base if weighted is disabled
                            if base.endswith('_w'):
                                base_without_w = base[:-2]  # Remove last 2 chars (_w)
                            else:
                                base_without_w = base.replace('_w_per_exposure_', '_per_exposure_')
                            return f"{base_without_w}_{currency}"
                        else:
                            # Already correct: either both have _w or both don't
                            return f"{base}_{currency}"
                    elif base in SHARED_METRICS:
                        # Shared metrics don't need currency suffix
                        return base
                    else:
                        # Unknown metric, assume currency needed
                        return f"{base}{suffix}_{currency}"
                
                # Reset preset selection when toggles change (to avoid stale selection)
                current_toggle_state = (use_weighted, use_btc)
                if 'prev_toggle_state' not in st.session_state:
                    st.session_state['prev_toggle_state'] = current_toggle_state
                
                # Store preset index before toggle changes
                prev_preset_index = None
                if 'preset_view' in st.session_state and 'prev_preset_options' in st.session_state:
                    prev_options = st.session_state['prev_preset_options']
                    if st.session_state['preset_view'] in prev_options:
                        prev_preset_index = prev_options.index(st.session_state['preset_view'])
                
                if st.session_state['prev_toggle_state'] != current_toggle_state:
                    # Toggles changed - update session state but keep preset index
                    if 'preset_view' in st.session_state:
                        del st.session_state['preset_view']
                    st.session_state['prev_toggle_state'] = current_toggle_state
                
                # For efficiency preset: use correct exposure metric based on weighted toggle
                exposure_metric = 'adg_w_per_exposure_long' if use_weighted else 'adg_per_exposure_long'
                
                preset_options = [
                    "**Profit vs Risk**",
                    "**Risk-Adjusted**",
                    "**Profit vs Quality**",
                    "**Efficiency**",
                    "**Multi-Risk**",
                    "**Profit vs Recovery**",
                    "**Performance Ratios**",
                    "**Exposure Analysis**",
                    "Custom..."
                ]
                
                # Help texts for each preset with metric details
                preset_help = {
                    preset_options[0]: f"📈 **Profit vs Risk**: {metric('adg')} vs {metric('drawdown_worst')}\n\nBalance daily gains against maximum drawdown. Ideal for finding high-return configs with acceptable risk levels.",
                    preset_options[1]: f"⚖️ **Risk-Adjusted**: {metric('sharpe_ratio')} vs {metric('sortino_ratio')}\n\nCompare Sharpe (total risk adjustment) vs Sortino (downside risk only). Shows which configs deliver best returns per unit of risk.",
                    preset_options[2]: f"🎯 **Profit vs Quality**: {metric('adg')} vs {metric('equity_choppiness')}\n\nDaily gains vs equity curve smoothness. Low choppiness = steadier growth with fewer ups and downs.",
                    preset_options[3]: f"💡 **Efficiency**: {metric('adg')} vs {metric(exposure_metric)}\n\nProfit per unit of capital exposure. Shows which configs generate most return with least capital at risk.",
                    preset_options[4]: f"🛡️ **Multi-Risk**: {metric('drawdown_worst')} vs {metric('expected_shortfall_1pct')}\n\nWorst drawdown vs extreme loss scenarios (1% VaR). Identifies configs that handle both typical and extreme losses well.",
                    preset_options[5]: f"⏱️ **Profit vs Recovery**: {metric('adg')} vs {metric('peak_recovery_hours_equity')}\n\nDaily gains vs time needed to recover from drawdowns. Fast recovery = capital back to work sooner.",
                    preset_options[6]: f"📊 **Performance Ratios**: {metric('calmar_ratio')} vs {metric('omega_ratio')}\n\nCalmar (return/drawdown) vs Omega (gains/losses). Advanced risk-adjusted metrics for sophisticated analysis.",
                    preset_options[7]: f"💰 **Exposure Analysis**: {metric('adg')} vs total_wallet_exposure_mean\n\nProfit vs average capital usage. Lower exposure = more capital available for other strategies.",
                    preset_options[8]: "Choose your own X and Y axis metrics for custom analysis"
                }
                
                # Store preset_options for next iteration
                st.session_state['prev_preset_options'] = preset_options
                
                # Initialize preset selection (default to Profit vs Risk on first load, or restore previous index)
                if 'preset_view' not in st.session_state:
                    if prev_preset_index is not None and prev_preset_index < len(preset_options):
                        # Restore previous preset index after toggle change
                        st.session_state['preset_view'] = preset_options[prev_preset_index]
                    else:
                        # First load - use default
                        st.session_state['preset_view'] = preset_options[0]  # Profit vs Risk
                
                # Show preset selection with help icon
                col_radio, col_help = st.columns([4, 1])
                with col_radio:
                    preset_choice = st.radio("Quick Views:", preset_options, key='preset_view', label_visibility="visible")
                with col_help:
                    st.write("")  # Spacing
                    st.write("")  # Spacing
                    with st.popover("📖 Guide"):
                        for preset, help_text in preset_help.items():
                            if preset == "Custom...":
                                st.markdown(f"**{preset}**\n\n{help_text}")
                            else:
                                st.markdown(f"{help_text}")
                            st.markdown("---")
                
                # Map preset to actual metrics using helper function
                preset_map = {
                    preset_options[0]: (metric('adg'), metric('drawdown_worst')),
                    preset_options[1]: (metric('sharpe_ratio'), metric('sortino_ratio')),
                    preset_options[2]: (metric('adg'), metric('equity_choppiness')),
                    preset_options[3]: (metric('adg'), metric(exposure_metric)),
                    preset_options[4]: (metric('drawdown_worst'), metric('expected_shortfall_1pct')),
                    preset_options[5]: (metric('adg'), metric('peak_recovery_hours_equity')),
                    preset_options[6]: (metric('calmar_ratio'), metric('omega_ratio')),
                    preset_options[7]: (metric('adg'), "total_wallet_exposure_mean")
                }
                
                st.markdown("---")
                
                # If preset selected, use those metrics; otherwise show dropdowns
                # Match preset by index instead of string to handle toggle state changes
                preset_index = preset_options.index(preset_choice) if preset_choice in preset_options else 0
                
                if preset_index < len(preset_options) - 1:  # Not "Custom..." (which is at last index)
                    # Use the preset mapping based on index
                    selected_preset_option = preset_options[preset_index]
                    
                    # Extract preset name without metric details (e.g., "Profit vs Risk")
                    preset_name = selected_preset_option.split('(')[0].strip()
                    
                    if selected_preset_option in preset_map:
                        x_metric, y_metric = preset_map[selected_preset_option]
                    else:
                        # Fallback to default using helper function
                        x_metric = metric('adg')
                        y_metric = metric('drawdown_worst')
                    
                    # Clear custom selectbox state to prevent stale values
                    if 'x_metric' in st.session_state:
                        del st.session_state['x_metric']
                    if 'y_metric' in st.session_state:
                        del st.session_state['y_metric']
                    
                    # Verify metrics exist and show warning if not
                    if x_metric not in available_metrics:
                        st.warning(f"⚠️ Metric `{x_metric}` not found in data. Using fallback.")
                        x_metric = available_metrics[0] if available_metrics else f'adg{suffix}_{currency}'
                    if y_metric not in available_metrics:
                        st.warning(f"⚠️ Metric `{y_metric}` not found in data. Using fallback.")
                        y_metric = available_metrics[min(1, len(available_metrics)-1)] if len(available_metrics) > 1 else x_metric
                    
                    st.caption(f"**X-Axis:** {x_metric}")
                    st.caption(f"**Y-Axis:** {y_metric}")
                else:
                    # Custom: show dropdowns with currency and weighted filtering
                    preset_name = "Custom"
                    st.markdown("**Custom Metric Selection**")
                    
                    # Filter options (order matches toggles above: Weighted left, BTC right)
                    col_filter1, col_filter2 = st.columns(2)
                    with col_filter1:
                        allow_mixed_weighted = st.checkbox("Allow Mixed Weighted", value=False,
                                                          help="Enable to compare weighted and non-weighted metrics",
                                                          key='allow_mixed_weighted')
                    with col_filter2:
                        allow_mixed_currency = st.checkbox("Allow Mixed USD/BTC", value=False,
                                                          help="Enable to compare metrics with different currencies",
                                                          key='allow_mixed_currency')
                    
                    # Filter metrics by currency and weighted unless mixed is allowed
                    filtered_metrics = available_metrics
                    
                    if not allow_mixed_currency:
                        # Only show metrics that match the selected currency
                        filtered_metrics = [m for m in filtered_metrics if m.endswith(f"_{currency}") or not (m.endswith('_usd') or m.endswith('_btc'))]
                    
                    if not allow_mixed_weighted:
                        # Only show metrics that match the weighted preference
                        def is_weighted_metric(m):
                            """Check if metric is weighted (_w variant)"""
                            return '_w_' in m or m.endswith('_w_usd') or m.endswith('_w_btc')
                        
                        if use_weighted:
                            # Show only weighted metrics
                            filtered_metrics = [m for m in filtered_metrics if is_weighted_metric(m)]
                        else:
                            # Show only non-weighted metrics
                            filtered_metrics = [m for m in filtered_metrics if not is_weighted_metric(m)]
                    
                    if not filtered_metrics:
                        st.warning(f"No matching metrics found. Enabling mixed mode...")
                        filtered_metrics = available_metrics
                    
                    # Dynamic defaults based on toggles
                    default_x = f'adg{suffix}_{currency}'
                    default_y = f'adg_per_exposure_long{suffix}_{currency}'
                    
                    # Find indices for defaults
                    x_index = filtered_metrics.index(default_x) if default_x in filtered_metrics else 0
                    y_index = filtered_metrics.index(default_y) if default_y in filtered_metrics else min(1, len(filtered_metrics)-1)
                    
                    x_metric = st.selectbox("X-Axis:", filtered_metrics, 
                                           index=x_index, 
                                           key='x_metric')
                    y_metric = st.selectbox("Y-Axis:", filtered_metrics, 
                                           index=y_index, 
                                           key='y_metric')
                
                color_metric = st.selectbox("Color by:", ["None"] + available_metrics, key='color_metric')
                color_metric = None if color_metric == "None" else color_metric
                
            elif viz_type in ["3D Scatter", "3D Projections"]:
                # 3D Scatter / Projections - Shared preset logic
                st.markdown("---")
                
                # Toggles for 3D metric selection
                col_toggle1, col_toggle2 = st.columns(2)
                with col_toggle1:
                    use_weighted = st.toggle("Use Weighted (_w) Metrics", value=True, 
                                            help="Weighted metrics emphasize recent performance (recency-biased)", 
                                            key='use_weighted_3d')
                with col_toggle2:
                    use_btc = st.toggle("Use BTC instead of USD", value=False,
                                       help="Switch between USD and BTC denominated metrics",
                                       key='use_btc_3d')
                
                # Define metric helper for 3D
                suffix = "_w" if use_weighted else ""
                currency = "btc" if use_btc else "usd"
                
                def metric(base):
                    """Build full metric name with suffix and currency if needed"""
                    if base in CURRENCY_METRICS:
                        has_w_suffix = base.endswith('_w') or '_w_per_exposure_' in base
                        
                        if use_weighted and not has_w_suffix:
                            base_with_w = f"{base}_w"
                            if base_with_w in CURRENCY_METRICS:
                                return f"{base_with_w}_{currency}"
                            else:
                                return f"{base}_{currency}"
                        elif not use_weighted and has_w_suffix:
                            if base.endswith('_w'):
                                base_without_w = base[:-2]
                            else:
                                base_without_w = base.replace('_w_per_exposure_', '_per_exposure_')
                            return f"{base_without_w}_{currency}"
                        else:
                            return f"{base}_{currency}"
                    elif base in SHARED_METRICS:
                        return base
                    else:
                        return f"{base}{suffix}_{currency}"
                
                exposure_metric = 'adg_w_per_exposure_long' if use_weighted else 'adg_per_exposure_long'
                
                # 3D Presets - Expanded logic similar to 2D presets
                preset_3d_options = [
                    "**Risk-Reward Triangle**",
                    "**Recovery Performance**",
                    "**Trading Efficiency**",
                    "**Risk Spectrum**",
                    "**Stability Analysis**",
                    "**Trading Activity**",
                    "**Stress Test**",
                    "Custom..."
                ]
                
                preset_3d_help = {
                    preset_3d_options[0]: f"🎯 **Risk-Reward Triangle**: {metric('adg')} vs {metric('drawdown_worst')} vs {metric('equity_jerkiness')}\n\nThe ultimate 3D view showing the raw ingredients: Profit × Max Risk × Volatility. Find configs with high returns, low drawdowns AND smooth equity curves. Uses equity jerkiness (rate of change volatility) instead of Sharpe Ratio to avoid mathematical dependencies.",
                    preset_3d_options[1]: f"⏱️ **Recovery Performance**: {metric('adg')} vs {metric('peak_recovery_hours_equity')} vs {metric('drawdown_worst')}\n\nProfit × Recovery Speed × Max Risk. Find configs that not only make money but recover quickly from losses. Critical for trading psychology: 'Deep & Fast' (deep drawdowns, quick recovery) vs 'Shallow & Slow'.",
                    preset_3d_options[2]: f"💡 **Trading Efficiency**: {metric('adg')} vs {metric(exposure_metric)} vs total_wallet_exposure_mean\n\nProfit × Capital Efficiency × Average Usage. Discover configs that generate maximum return with minimal capital at risk. Filter out strategies that lock up your entire wallet.",
                    preset_3d_options[3]: f"⚖️ **Risk Spectrum**: {metric('sharpe_ratio')} vs {metric('sortino_ratio')} vs {metric('calmar_ratio')}\n\nCompare three major risk-adjusted metrics: Sharpe (total risk), Sortino (downside only), Calmar (drawdown adjusted). Find configs that excel at ALL three, or discover which ones only look good under specific risk definitions.",
                    preset_3d_options[4]: f"📈 **Stability Analysis**: {metric('adg')} vs {metric('equity_choppiness')} vs {metric('loss_profit_ratio')}\n\nProfit × Smoothness × Win/Loss Balance. Find configs with steady, consistent growth patterns - the 'staircase to heaven' instead of a roller coaster. Shows if stability comes from high winrate (many small wins) or from smooth equity despite volatile trades.",
                    preset_3d_options[5]: f"🔄 **Trading Activity**: {metric('adg')} vs positions_held_per_day vs position_held_hours_mean\n\nProfit × Trade Frequency × Hold Duration. The 'strategy fingerprint' - instantly see clusters: Scalpers (many trades, short duration) vs Swing Traders (few trades, long hold). Perfect for diversification: pick one from each cluster.",
                    preset_3d_options[6]: f"🧪 **Stress Test**: {metric('drawdown_worst')} vs {metric('expected_shortfall_1pct')} vs {metric('loss_profit_ratio')}\n\nMax Drawdown × Expected Shortfall (VaR 1%) × Loss/Profit Ratio. Shows worst past event, statistical tail risk (1% worst case), and loss balance. Find configs that handle extreme events gracefully and maintain good win/loss structure.",
                    preset_3d_options[7]: "Choose your own X, Y and Z axis metrics for custom 3D analysis"
                }
                
                # Show preset selection with help icon (same layout as 2D)
                col_radio, col_help = st.columns([4, 1])
                with col_radio:
                    preset_3d_choice = st.radio("Quick Views:", preset_3d_options, key='preset_3d_view', label_visibility="visible", index=0)
                with col_help:
                    st.write("")  # Spacing
                    st.write("")  # Spacing
                    with st.popover("📖 Guide"):
                        for preset, help_text in preset_3d_help.items():
                            if preset == "Custom...":
                                st.markdown(f"**{preset}**\n\n{help_text}")
                            else:
                                st.markdown(f"{help_text}")
                            st.markdown("---")
                        st.markdown("💡 **Tip**: Use mouse to rotate the 3D view. Hover over points for details.")
                
                st.markdown("---")
                
                # Map 3D presets to metrics
                preset_3d_map = {
                    preset_3d_options[0]: (metric('adg'), metric('drawdown_worst'), metric('equity_jerkiness')),
                    preset_3d_options[1]: (metric('adg'), metric('peak_recovery_hours_equity'), metric('drawdown_worst')),
                    preset_3d_options[2]: (metric('adg'), metric(exposure_metric), "total_wallet_exposure_mean"),
                    preset_3d_options[3]: (metric('sharpe_ratio'), metric('sortino_ratio'), metric('calmar_ratio')),
                    preset_3d_options[4]: (metric('adg'), metric('equity_choppiness'), metric('loss_profit_ratio')),
                    preset_3d_options[5]: (metric('adg'), "positions_held_per_day", "position_held_hours_mean"),
                    preset_3d_options[6]: (metric('drawdown_worst'), metric('expected_shortfall_1pct'), metric('loss_profit_ratio'))
                }
                
                if preset_3d_choice != "Custom..." and preset_3d_choice in preset_3d_map:
                    x_metric, y_metric, z_metric = preset_3d_map[preset_3d_choice]
                    
                    # Clear custom selectbox state
                    if 'z_metric' in st.session_state:
                        del st.session_state['z_metric']
                    
                    # Verify metrics exist
                    if x_metric not in available_metrics:
                        st.warning(f"⚠️ X-Metric `{x_metric}` not found. Using fallback.")
                        x_metric = available_metrics[0] if available_metrics else metric('adg')
                    if y_metric not in available_metrics:
                        st.warning(f"⚠️ Y-Metric `{y_metric}` not found. Using fallback.")
                        y_metric = available_metrics[min(1, len(available_metrics)-1)] if len(available_metrics) > 1 else x_metric
                    if z_metric not in available_metrics:
                        st.warning(f"⚠️ Z-Metric `{z_metric}` not found. Using fallback.")
                        z_metric = available_metrics[min(2, len(available_metrics)-1)] if len(available_metrics) > 2 else y_metric
                    
                    st.caption(f"**X-Axis:** {x_metric}")
                    st.caption(f"**Y-Axis:** {y_metric}")
                    st.caption(f"**Z-Axis:** {z_metric}")
                else:
                    # Custom: show X, Y, Z dropdowns
                    st.markdown("**Custom 3D Metric Selection**")
                    
                    # Filter metrics by currency if needed
                    filtered_metrics = available_metrics
                    if not use_btc:
                        filtered_metrics = [m for m in filtered_metrics if not m.endswith('_btc')]
                    
                    # Find indices for defaults
                    default_x = metric('adg')
                    default_y = metric('drawdown_worst')
                    default_z = metric('sharpe_ratio')
                    
                    x_index = filtered_metrics.index(default_x) if default_x in filtered_metrics else 0
                    y_index = filtered_metrics.index(default_y) if default_y in filtered_metrics else min(1, len(filtered_metrics)-1)
                    z_index = filtered_metrics.index(default_z) if default_z in filtered_metrics else min(2, len(filtered_metrics)-1)
                    
                    x_metric = st.selectbox("X-Axis:", filtered_metrics, 
                                           index=x_index, 
                                           key='x_metric_3d')
                    y_metric = st.selectbox("Y-Axis:", filtered_metrics, 
                                           index=y_index, 
                                           key='y_metric_3d')
                    z_metric = st.selectbox("Z-Axis:", filtered_metrics, 
                                           index=z_index, 
                                           key='z_metric_3d')
                
                color_metric = st.selectbox("Color by:", ["None"] + available_metrics, key='color_metric_3d')
                color_metric = None if color_metric == "None" else color_metric
            
            elif viz_type == "Radar Chart":
                # Radar chart doesn't need axis selectors
                st.info("📊 Radar chart compares Best Match (⭐) against top 5 Pareto configs. Click markers to select.")
        
        # === CHART RENDERING (col1) ===
        with col1:
            if viz_type == "2D Scatter":
                # Variables are set in the 2D block above: x_metric, y_metric, preset_name
                fig = self.viz.plot_pareto_scatter_2d(
                    x_metric=x_metric,
                    y_metric=y_metric,
                    color_metric=color_metric,
                    show_all=show_all,
                    best_match_config=best_match,
                    title_prefix=preset_name
                )
                event = st.plotly_chart(fig, width='stretch', on_select="rerun", key='chart_2d')
                
                # Handle click event - extract config_index from clicked point
                if event and hasattr(event, 'selection') and event.selection:
                    try:
                        points = event.selection.get('points', [])
                        if points:
                            point_data = points[0]
                            clicked_config_index = None
                            
                            # Try different ways to get config_index
                            if 'customdata' in point_data and point_data['customdata'] is not None:
                                customdata = point_data['customdata']
                                
                                # Handle different customdata formats
                                if isinstance(customdata, dict):
                                    # customdata is a dict like {"0": 16287}
                                    clicked_config_index = int(customdata.get('0', customdata.get(0)))
                                elif isinstance(customdata, (list, tuple)) and len(customdata) > 0:
                                    clicked_config_index = int(customdata[0])
                                elif isinstance(customdata, (int, float)):
                                    clicked_config_index = int(customdata)
                            
                            # Fallback: use point_index
                            if clicked_config_index is None and 'point_index' in point_data:
                                idx = point_data['point_index']
                                configs = self.loader.get_pareto_configs() if not show_all else self.loader.configs[:1000]
                                if 0 <= idx < len(configs):
                                    clicked_config_index = configs[idx].config_index
                            
                            # Update session state (on_select="rerun" will trigger the rerun)
                            if clicked_config_index is not None:
                                current_selection = st.session_state.get('pareto_selected_config')
                                if current_selection != clicked_config_index:
                                    st.session_state.pareto_selected_config = clicked_config_index
                    except Exception as e:
                        st.warning(f"⚠️ Click handling error: {e}")
            
            elif viz_type == "3D Scatter":
                # Determine which configs to pass to the chart
                if show_all:
                    chart_configs = self.view_configs
                else:
                    chart_configs = [c for c in self.view_configs if c.is_pareto]
                
                fig = self.viz.plot_pareto_scatter_3d(
                    x_metric=x_metric,
                    y_metric=y_metric,
                    z_metric=z_metric,
                    color_metric=color_metric,
                    show_all=show_all,
                    best_match_config=best_match,
                    configs_list=chart_configs
                )
                
                # Render the chart with selection enabled
                event = st.plotly_chart(fig, key='chart_3d', on_select="rerun")
                
                # Inject JavaScript to forward plotly_click to plotly_selected
                # This is a workaround for scatter_3d not triggering plotly_selected events
                # See: https://github.com/streamlit/streamlit/issues/9001
                # Include preset in HTML comment to force re-execution on preset change
                preset_key = preset_3d_choice.replace("*", "").replace(" ", "_")
                js_code = f"""
                <!-- Preset: {preset_key} -->
                <script>
                    (function() {{
                        console.log('=== 3D CLICK HANDLER INIT [{preset_key}] ===');
                        
                        var parentDoc = window.parent.document;
                        var installedCharts = new WeakSet(); // Track which charts already have handlers
                        
                        function installClickHandlerOnElement(element, index) {{
                            // Skip if already installed
                            if (installedCharts.has(element)) {{
                                return false;
                            }}
                            
                            // Check if this is a 3D scatter (has 'scene' in layout)
                            if (element && element.layout && element.layout.scene && element.on) {{
                                console.log('✅ Installing handler on 3D chart', index);
                                
                                // Remove existing listener (just in case)
                                if (element.removeAllListeners) {{
                                    element.removeAllListeners('plotly_click');
                                }}
                                
                                // Install click handler
                                element.on('plotly_click', function(eventData) {{
                                    console.log('🎯 3D CLICK - Config:', eventData.points[0].customdata);
                                    
                                    if (eventData && eventData.points && eventData.points.length > 0) {{
                                        try {{
                                            element.emit('plotly_selected', {{ 
                                                points: eventData.points 
                                            }});
                                            console.log('✅ plotly_selected emitted');
                                        }} catch (err) {{
                                            console.error('❌ Error emitting plotly_selected:', err);
                                        }}
                                    }}
                                }});
                                
                                // Mark as installed
                                installedCharts.add(element);
                                console.log('✅ Handler installed on chart', index);
                                return true;
                            }}
                            return false;
                        }}
                        
                        function scanAndInstall() {{
                            var elements = parentDoc.querySelectorAll('.js-plotly-plot');
                            var newInstalls = 0;
                            
                            elements.forEach(function(element, index) {{
                                if (installClickHandlerOnElement(element, index)) {{
                                    newInstalls++;
                                }}
                            }});
                            
                            if (newInstalls > 0) {{
                                console.log('📊 Installed handlers on', newInstalls, 'new chart(s)');
                            }}
                        }}
                        
                        // Initial scan with retries (for initial page load)
                        var initialAttempts = 0;
                        var maxInitialAttempts = 10;
                        
                        function initialScan() {{
                            initialAttempts++;
                            var elements = parentDoc.querySelectorAll('.js-plotly-plot');
                            
                            if (elements.length === 0 && initialAttempts < maxInitialAttempts) {{
                                setTimeout(initialScan, 500);
                                return;
                            }}
                            
                            scanAndInstall();
                            
                            // After initial scan, set up continuous monitoring
                            setupContinuousMonitoring();
                        }}
                        
                        function setupContinuousMonitoring() {{
                            // Re-scan periodically to catch chart updates from rotation/zoom
                            setInterval(scanAndInstall, 1000);
                            
                            // Also use MutationObserver to catch DOM changes immediately
                            var observer = new MutationObserver(function(mutations) {{
                                var shouldScan = false;
                                mutations.forEach(function(mutation) {{
                                    if (mutation.addedNodes.length > 0 || mutation.attributeName === 'class') {{
                                        shouldScan = true;
                                    }}
                                }});
                                if (shouldScan) {{
                                    scanAndInstall();
                                }}
                            }});
                            
                            // Observe the entire document for changes
                            observer.observe(parentDoc.body, {{
                                childList: true,
                                subtree: true,
                                attributes: true,
                                attributeFilter: ['class']
                            }});
                            
                            console.log('🔄 Continuous monitoring active');
                        }}
                        
                        // Start
                        initialScan();
                    }})();
                </script>
                """
                st.components.v1.html(js_code, height=0)
                
                # Handle click event - extract config_index from clicked point
                if event and hasattr(event, 'selection') and event.selection:
                    try:
                        points = event.selection.get('points', [])
                        if points:
                            point_data = points[0]
                            clicked_config_index = None
                            
                            # PRIORITY: Use customdata which contains the actual config_index
                            # This is reliable even when multiple traces exist (show_all=True creates 3 traces)
                            if 'customdata' in point_data and point_data['customdata'] is not None:
                                customdata = point_data['customdata']
                                if isinstance(customdata, dict):
                                    val = customdata.get('0', customdata.get(0))
                                    if val is not None:
                                        clicked_config_index = int(val)
                                elif isinstance(customdata, (list, tuple)) and len(customdata) > 0:
                                    clicked_config_index = int(customdata[0])
                                elif isinstance(customdata, (int, float)):
                                    clicked_config_index = int(customdata)
                            
                            # FALLBACK: If customdata failed, try point_number (only reliable for single trace)
                            if clicked_config_index is None:
                                point_number = point_data.get('point_number')
                                if point_number is not None:
                                    # Use the SAME configs list as passed to the chart
                                    if show_all:
                                        configs = self.view_configs
                                    else:
                                        configs = [c for c in self.view_configs if c.is_pareto]
                                    
                                    # point_number is the index in the configs list
                                    if 0 <= point_number < len(configs):
                                        clicked_config_index = configs[point_number].config_index
                            
                            # Update session state if we found a config
                            if clicked_config_index is not None:
                                current_selection = st.session_state.get('pareto_selected_config')
                                if current_selection != clicked_config_index:
                                    st.session_state.pareto_selected_config = clicked_config_index
                    except Exception as e:
                        st.warning(f"⚠️ Click handling error: {e}")
            
            elif viz_type == "3D Projections":
                # Three 2D projections (XY, XZ, YZ) - works without WebGL
                st.info("📊 **3D Projections** - Three 2D views showing all dimensions (no WebGL required)")
                
                # Determine which configs to pass to the charts
                if show_all:
                    chart_configs = self.view_configs
                else:
                    chart_configs = [c for c in self.view_configs if c.is_pareto]
                
                # Create three columns for the projections
                col_xy, col_xz, col_yz = st.columns(3)
                
                with col_xy:
                    st.markdown(f"**XY Plane**<br><small>{x_metric} vs {y_metric}</small>", unsafe_allow_html=True)
                    fig_xy = self.viz.plot_pareto_scatter_2d(
                        x_metric=x_metric,
                        y_metric=y_metric,
                        color_metric=z_metric,  # Color by Z for 3D effect
                        show_all=show_all,
                        best_match_config=best_match,
                        title_prefix=""
                    )
                    fig_xy.update_layout(height=400, showlegend=False)
                    event_xy = st.plotly_chart(fig_xy, key='chart_xy', on_select="rerun", use_container_width=True)
                
                with col_xz:
                    st.markdown(f"**XZ Plane**<br><small>{x_metric} vs {z_metric}</small>", unsafe_allow_html=True)
                    fig_xz = self.viz.plot_pareto_scatter_2d(
                        x_metric=x_metric,
                        y_metric=z_metric,
                        color_metric=y_metric,  # Color by Y for 3D effect
                        show_all=show_all,
                        best_match_config=best_match,
                        title_prefix=""
                    )
                    fig_xz.update_layout(height=400, showlegend=False)
                    event_xz = st.plotly_chart(fig_xz, key='chart_xz', on_select="rerun", use_container_width=True)
                
                with col_yz:
                    st.markdown(f"**YZ Plane**<br><small>{y_metric} vs {z_metric}</small>", unsafe_allow_html=True)
                    fig_yz = self.viz.plot_pareto_scatter_2d(
                        x_metric=y_metric,
                        y_metric=z_metric,
                        color_metric=x_metric,  # Color by X for 3D effect
                        show_all=show_all,
                        best_match_config=best_match,
                        title_prefix=""
                    )
                    fig_yz.update_layout(height=400, showlegend=False)
                    event_yz = st.plotly_chart(fig_yz, key='chart_yz', on_select="rerun", use_container_width=True)
                
                # Handle click events from any of the three charts
                for event in [event_xy, event_xz, event_yz]:
                    if event and hasattr(event, 'selection') and event.selection:
                        try:
                            points = event.selection.get('points', [])
                            if points:
                                point_data = points[0]
                                clicked_config_index = None
                                
                                # PRIORITY: Use customdata which contains the actual config_index
                                if 'customdata' in point_data and point_data['customdata'] is not None:
                                    customdata = point_data['customdata']
                                    if isinstance(customdata, dict):
                                        val = customdata.get('0', customdata.get(0))
                                        if val is not None:
                                            clicked_config_index = int(val)
                                    elif isinstance(customdata, (list, tuple)) and len(customdata) > 0:
                                        clicked_config_index = int(customdata[0])
                                    elif isinstance(customdata, (int, float)):
                                        clicked_config_index = int(customdata)
                                
                                # FALLBACK: If customdata failed, try point_number
                                if clicked_config_index is None:
                                    point_number = point_data.get('point_number')
                                    if point_number is not None:
                                        # Use the SAME configs list as passed to the chart
                                        if show_all:
                                            configs = self.view_configs
                                        else:
                                            configs = [c for c in self.view_configs if c.is_pareto]
                                        
                                        # point_number is the index in the configs list
                                        if 0 <= point_number < len(configs):
                                            clicked_config_index = configs[point_number].config_index
                                
                                # Update session state if we found a config
                                if clicked_config_index is not None:
                                    current_selection = st.session_state.get('pareto_selected_config')
                                    if current_selection != clicked_config_index:
                                        st.session_state.pareto_selected_config = clicked_config_index
                                        break  # Stop after first successful click
                        except Exception as e:
                            pass  # Ignore errors from other charts
            
            else:  # Radar Chart
                # Get comparison configs for consistent display
                pareto_configs = self.loader.get_pareto_configs()
                primary_metric = self.loader.scoring_metrics[0] if self.loader.scoring_metrics else 'adg_w_usd'
                comparison_configs = sorted(
                    [c for c in pareto_configs if c.config_index != best_match.config_index],
                    key=lambda c: c.suite_metrics.get(primary_metric, 0),
                    reverse=True
                )[:5]
                
                fig = self.viz.plot_radar_chart(
                    best_match_config=best_match,
                    comparison_configs=comparison_configs,
                    metrics=None,  # Auto-select balanced metrics
                    top_n_comparison=5
                )
                event = st.plotly_chart(fig, width='stretch', on_select="rerun", key='chart_radar')
                
                # Handle click event on markers
                if event and hasattr(event, 'selection') and event.selection:
                    try:
                        points = event.selection.get('points', [])
                        if points:
                            point_data = points[0]
                            clicked_config_index = None
                            
                            # Extract config_index from customdata
                            if 'customdata' in point_data and point_data['customdata'] is not None:
                                customdata = point_data['customdata']
                                
                                # Handle different customdata formats
                                if isinstance(customdata, dict):
                                    clicked_config_index = int(customdata.get('0', customdata.get(0)))
                                elif isinstance(customdata, (list, tuple)) and len(customdata) > 0:
                                    clicked_config_index = int(customdata[0])
                                elif isinstance(customdata, (int, float)):
                                    clicked_config_index = int(customdata)
                            
                            # Update session state
                            if clicked_config_index is not None:
                                current_selection = st.session_state.get('pareto_selected_config')
                                if current_selection != clicked_config_index:
                                    st.session_state.pareto_selected_config = clicked_config_index
                    except Exception as e:
                        st.warning(f"⚠️ Click handling error: {e}")
        
        # Call the config details fragment (will only reload this section)
        self._show_config_details_fragment()
    
    def _show_deep_intelligence(self):
        """Stage 3: Deep Intelligence - Parameter & Market Analysis"""
        
        st.title("🧠 DEEP INTELLIGENCE")
        st.markdown("**Advanced parameter and scenario analysis**")


        # Persist active tab across reruns/mode switches.
        # Streamlit tabs do not expose a readable active tab state, so we use a stateful
        # control (segmented_control) and mirror it into the URL query params.
        tab_keys = ["parameters", "scenarios", "evolution", "correlations"]
        tab_labels = {
            "parameters": "📊 Parameters",
            "scenarios": "🎬 Scenarios",
            "evolution": "📈 Evolution",
            "correlations": "🔗 Correlations",
        }

        def _format_tab(tab_key: str) -> str:
            return tab_labels.get(tab_key, tab_key)

        qp_tab = st.query_params.get("deep_intel_tab", None)
        if isinstance(qp_tab, list):
            qp_tab = qp_tab[0] if qp_tab else None
        if qp_tab not in tab_keys:
            qp_tab = None

        default_tab = qp_tab or st.session_state.get("deep_intel_active_tab", "parameters")
        if default_tab not in tab_keys:
            default_tab = "parameters"

        active_tab = st.segmented_control(
            "",
            tab_keys,
            default=default_tab,
            format_func=_format_tab,
            key="deep_intel_active_tab",
            label_visibility="collapsed",
            width="stretch",
        )

        qp_current = st.query_params.get("deep_intel_tab", None)
        if isinstance(qp_current, list):
            qp_current = qp_current[0] if qp_current else None
        if active_tab in tab_keys and qp_current != active_tab:
            st.query_params["deep_intel_tab"] = active_tab

        if active_tab == "parameters":
            _hg_title, _hg_guide = st.columns([10, 2], vertical_alignment="top")
            with _hg_title:
                st.subheader("Parameter Influence Analysis")
            with _hg_guide:
                with st.popover("📖 Guide"):
                    st.markdown(
                        """
                        **🔥 Parameter Influence Heatmap**
                        
                        **📊 What it shows:**
                        Correlations between bot parameters (rows) and performance metrics (columns).
                        
                        **🎨 How to read colors:**
                        - 🔵 **Blue (+1.0)**: Parameter ↑ → Metric ↑ (strong positive)
                        - ⚪ **White (0.0)**: No clear linear relationship
                        - 🔴 **Red (-1.0)**: Parameter ↑ → Metric ↓ (strong negative)
                        
                        **⚠️ Important:**
                        Correlation ≠ causation! Use this as a hint for focused experiments.
                        
                        **🎯 Action items:**
                        1. Spot rows/columns with strong correlations (|corr| > 0.5)
                        2. Test these parameters with targeted bound adjustments
                        3. Run follow-up optimizations to confirm relationships
                        """
                    )

            top_n = st.slider(
                "Top N Parameters",
                10,
                40,
                20,
                key='top_n_params',
                help=(
                    "How many of the most variable parameters to show in the correlation heatmap. "
                    "More parameters = more coverage, but more scrolling and visual density."
                ),
            )

            fig = self.viz.plot_parameter_influence_heatmap(top_n=top_n)
            st.plotly_chart(fig, width='stretch')
            
            # Parameters at bounds
            st.markdown("---")
            _b_title, _b_guide = st.columns([10, 2])
            with _b_title:
                st.subheader("⚠️ Parameters Near Bounds")
            with _b_guide:
                with st.popover("📖 Guide"):
                    st.markdown(
                        """
                        **⚠️ Parameters Near Bounds**
                        
                        **📊 What it shows:**
                        Parameters within 10% of their lower/upper optimization bounds.
                        
                        **🎨 Color coding:**
                        - 🔴 **Red (< 5%)**: Critical — optimizer hitting wall
                        - 🟠 **Orange (5-10%)**: Warning — getting close to limit
                        
                        **🔍 What it means:**
                        Small percentages = optimizer wants to go further but can't.
                        → Your search space might be too restrictive!
                        
                        **🎯 Action items:**
                        1. **Expand bounds** where results make sense (e.g., if edge value performs well)
                        2. **Keep tight** if edge values show poor performance (bound is correct)
                        3. **Increase resolution** near boundaries for finer tuning
                        4. **Re-run optimization** after adjustments to explore new space
                        """
                    )
            
            fig = self.viz.plot_parameter_bounds_distance(top_n=15)
            st.plotly_chart(fig, width='stretch')
            
            bounds_info = self.loader.get_parameters_at_bounds(tolerance=0.1)
            
            if bounds_info['at_lower'] or bounds_info['at_upper']:
                st.warning(f"**{len(bounds_info['at_lower']) + len(bounds_info['at_upper'])} parameters are near bounds!** Consider extending search space.")

        elif active_tab == "scenarios":
            # Title and guide
            _mr_title, _mr_guide = st.columns([10, 2])
            with _mr_title:
                st.subheader("Scenario Analysis")
            with _mr_guide:
                with st.popover("📖 Guide"):
                    st.markdown("""
                    ### 📊 Scenario Comparison
                    
                    **📈 What it shows:**
                    Performance distribution of Pareto configs across different backtest scenarios you defined (e.g., different time periods, symbols, or market conditions).
                    
                    **🎨 How to read the boxplots:**
                    - **Box**: Contains 50% of data (25th to 75th percentile)
                    - **Line inside box**: Median (50th percentile)
                    - **Whiskers**: Extend to min/max within 1.5×IQR
                    - **Dots**: Outliers beyond whiskers
                    
                    **🔍 What to look for:**
                    - **Compare medians**: Which scenarios perform better overall?
                    - **Compare box widths**: In which scenarios is performance more variable?
                    - **Spot outliers**: Are there configs with exceptional (good/bad) performance in specific scenarios?
                    - **Look for patterns**: Do all scenarios show similar distributions, or are there major differences?
                    
                    **🎯 Action items:**
                    1. **Check consistency**: Configs performing well across all scenarios are more robust
                    2. **Identify risk**: Scenarios with low medians or large variance may indicate higher risk
                    3. **Compare statistics**: Use Mean/Std/Min/Max below to quantify differences between scenarios
                    4. **Select accordingly**: Choose configs based on your scenario priorities (e.g., prioritize scenarios most relevant to your use case)
                    """)
            
            if self.loader.scenario_labels:
                st.markdown("**Scenario Performance Comparison**")
                
                # Get all available metrics from suite_metrics (not just scoring_metrics)
                # This gives access to all computed metrics, not just the ones used for optimization
                pareto_configs = self.loader.get_pareto_configs()
                if pareto_configs and pareto_configs[0].suite_metrics:
                    available_metrics = list(pareto_configs[0].suite_metrics.keys())
                else:
                    available_metrics = self.loader.scoring_metrics if self.loader.scoring_metrics else ['adg_w_usd', 'sharpe_ratio_usd']
                
                selected_metric = st.selectbox(
                    "Select Metric:", 
                    available_metrics, 
                    key='scenario_metric',
                    help="Choose which performance metric to compare across scenarios. All computed metrics are available, not just optimization objectives."
                )
                
                fig = self.viz.plot_scenario_comparison_boxplots(selected_metric)
                st.plotly_chart(fig, width='stretch')
                
                # Scenario statistics
                st.markdown("---")
                st.subheader("📊 Scenario Statistics")
                
                pareto_configs = self.loader.get_pareto_configs()
                
                for scenario in self.loader.scenario_labels:
                    values = [c.scenario_metrics.get(scenario, {}).get(selected_metric, np.nan) 
                             for c in pareto_configs if scenario in c.scenario_metrics]
                    values = [v for v in values if not np.isnan(v)]
                    
                    if values:
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric(
                                f"{scenario.title()} - Mean", 
                                f"{np.mean(values):.6f}",
                                help="Average performance across all Pareto configs in this scenario. Higher mean indicates better overall performance."
                            )
                        with col2:
                            st.metric(
                                f"{scenario.title()} - Std", 
                                f"{np.std(values):.6f}",
                                help="Standard deviation shows performance variability. Lower Std means more consistent results, higher Std means more spread between configs."
                            )
                        with col3:
                            st.metric(
                                f"{scenario.title()} - Min", 
                                f"{np.min(values):.6f}",
                                help="Worst-case performance among Pareto configs in this scenario. Important for risk assessment - how bad can it get?"
                            )
                        with col4:
                            st.metric(
                                f"{scenario.title()} - Max", 
                                f"{np.max(values):.6f}",
                                help="Best-case performance among Pareto configs in this scenario. Shows the potential upside if conditions align perfectly."
                            )
            else:
                st.info("No scenario data available")

        elif active_tab == "evolution":
            # Title and guide
            _ev_title, _ev_guide = st.columns([10, 2])
            with _ev_title:
                st.subheader("Optimization Evolution Timeline")
            with _ev_guide:
                with st.popover("📖 Guide"):
                    st.markdown("""
                    ### 📈 Optimization Evolution Timeline
                    
                    **📊 What it shows:**
                    How the optimizer explored the parameter space over time - tracking performance progression from first iteration to final result.
                    
                    **🎨 How to read the chart:**
                    - **Light blue dots**: Every single config tested (raw exploration)
                    - **Blue line**: Smoothed trend showing average performance evolution
                    - **Red stars ⭐**: Pareto-optimal configs discovered at that iteration
                    
                    **🔍 What to look for:**
                    - **Early stars**: Optimizer found good configs quickly (efficient search)
                    - **Late stars**: Breakthrough discoveries in later iterations (thorough exploration)
                    - **Upward trend**: Performance generally improving over time (good convergence)
                    - **Flat sections**: Optimizer exploring similar parameter regions
                    - **Clusters of stars**: Multiple Pareto configs found in succession (rich area discovered)
                    
                    **🎯 Key insights:**
                    1. **Search efficiency**: Stars in first 25% of iterations = fast convergence
                    2. **Exploration quality**: Stars spread throughout timeline = thorough search, not premature convergence
                    3. **Performance ceiling**: Last stars show if optimizer reached limits or could continue
                    4. **Smoothing window**: Adjust slider to see short-term fluctuations (low value) vs long-term trends (high value)
                    
                    **💡 Action items:**
                    - **Mostly early stars?** Your bounds may be too narrow - good configs found immediately
                    - **Only late stars?** Consider running longer optimizations - best results come with patience
                    - **No clear trend?** High variance indicates complex parameter interactions
                    - **Steep upward trend?** Optimizer learning efficiently - consider similar setups for future runs
                    """)
            
            # Metric selection with toggles like 2D
            col_toggle1, col_toggle2 = st.columns(2)
            with col_toggle1:
                use_weighted = st.toggle("Use Weighted (_w) Metrics", value=True, 
                                        help="Weighted metrics emphasize recent performance (recency-biased)", 
                                        key='use_weighted_evolution')
            with col_toggle2:
                use_btc = st.toggle("Use BTC instead of USD", value=False,
                                   help="Switch between USD and BTC denominated metrics",
                                   key='use_btc_evolution')
            
            # Get all available metrics and filter them
            pareto_configs = self.loader.get_pareto_configs()
            if pareto_configs and pareto_configs[0].suite_metrics:
                available_metrics = list(pareto_configs[0].suite_metrics.keys())
            else:
                available_metrics = self.loader.scoring_metrics if self.loader.scoring_metrics else ['adg_w_usd']
            
            # Filter metrics based on toggles
            filtered_metrics = []
            for metric in available_metrics:
                # Check currency
                if use_btc and not metric.endswith('_btc'):
                    continue
                if not use_btc and not metric.endswith('_usd') and ('_usd' in available_metrics or '_btc' in available_metrics):
                    # Skip if it's a currency metric but wrong currency
                    if metric.replace('_usd', '_btc') in available_metrics or metric.replace('_btc', '_usd') in available_metrics:
                        continue
                
                # Check weighted
                if use_weighted and '_w_' not in metric and not metric.endswith('_w_usd') and not metric.endswith('_w_btc'):
                    # Check if weighted version exists
                    weighted_version = metric.replace('_usd', '_w_usd').replace('_btc', '_w_btc')
                    if weighted_version in available_metrics:
                        continue
                if not use_weighted and ('_w_' in metric or metric.endswith('_w_usd') or metric.endswith('_w_btc')):
                    continue
                
                filtered_metrics.append(metric)
            
            # Fallback if no metrics match
            if not filtered_metrics:
                filtered_metrics = available_metrics
            
            # Find best default metric
            currency = 'btc' if use_btc else 'usd'
            suffix = '_w' if use_weighted else ''
            preferred_defaults = [f'adg{suffix}_{currency}', f'sharpe_ratio{suffix}_{currency}', f'adg_{currency}', f'sharpe_ratio_{currency}']
            default_index = 0
            for pref in preferred_defaults:
                if pref in filtered_metrics:
                    default_index = filtered_metrics.index(pref)
                    break
            
            selected_metric = st.selectbox(
                "Select Metric:", 
                filtered_metrics,
                index=default_index,
                key='evolution_metric',
                help="Choose which metric to track over optimization iterations. Shows how this specific metric evolved as the optimizer explored the parameter space."
            )
            
            # Calculate adaptive window range based on data size
            total_configs = len(self.loader.configs)
            max_window = min(500, total_configs // 2)  # Max 500 or half the data
            default_window = min(100, total_configs // 10)  # Default 100 or 10% of data
            
            window_percent = st.slider(
                "Smoothing Window (%)", 
                1, 25, 5, 1, 
                key='evolution_window_pct',
                help="Rolling average window as percentage of total iterations. Lower values (1-5%) show short-term fluctuations. Higher values (10-25%) reveal long-term trends."
            )
            
            window = max(10, int(total_configs * window_percent / 100))  # At least 10 iterations
            
            fig = self.viz.plot_evolution_timeline(selected_metric, window=window)
            evolution_selection = st.plotly_chart(fig, key=f'evolution_chart_{window}', on_select='rerun')
            
            st.info("💡 **Insight:** Click red stars (or any point) to view detailed config information!")
            
            # Handle clicks on Pareto stars - Use session state like 2D/3D charts
            if evolution_selection and evolution_selection.selection.points:
                try:
                    point_data = evolution_selection.selection.points[0]
                    clicked_config_index = None
                    
                    # Try customdata first (Pareto stars)
                    if 'customdata' in point_data and point_data['customdata']:
                        customdata = point_data['customdata']
                        if isinstance(customdata, dict):
                            val = customdata.get('0', customdata.get(0))
                            if val is not None:
                                clicked_config_index = int(val)
                        elif isinstance(customdata, (list, tuple)) and len(customdata) > 0:
                            clicked_config_index = int(customdata[0])
                        elif isinstance(customdata, (int, float)):
                            clicked_config_index = int(customdata)
                    
                    # Fallback: Use x-value as iteration/config_index
                    if clicked_config_index is None and 'x' in point_data:
                        clicked_config_index = int(point_data['x'])
                    
                    # Update session state like 2D/3D charts do
                    if clicked_config_index is not None:
                        current_selection = st.session_state.get('pareto_selected_config')
                        if current_selection != clicked_config_index:
                            st.session_state.pareto_selected_config = clicked_config_index
                except Exception as e:
                    pass  # Silently ignore errors
            
            # Show config details using the fragment (same as 2D/3D)
            self._show_config_details_fragment()

        elif active_tab == "correlations":
            # Title and guide (match Evolution)
            _corr_title, _corr_guide = st.columns([10, 2])
            with _corr_title:
                st.subheader("Multi-Metric Correlation")
            with _corr_guide:
                with st.popover("📖 Guide"):
                    st.markdown("""
                    ### 🔗 Multi‑Metric Correlation (Radar)

                    **📊 What it does:**
                    Compares multiple configs side‑by‑side across a *risk profile* using a radar chart. It’s designed to answer: *Which config is best overall for my goal, and what trade‑offs does it make?*

                    **🕸️ How to read the radar:**
                    - Each colored shape = one config.
                    - Each axis = one risk/profile dimension (normalized scores).
                    - Bigger / more “outward” shapes generally indicate stronger scores on those dimensions.
                    - A balanced, round shape = consistent profile; sharp spikes = specialized strengths.

                    **🎛️ Controls (top row):**
                    - **🎯 Selection Strategy**
                      - **🏆 Top Performers**: picks configs that win on different metrics (great for “best of each category”).
                      - **🧬 Diverse Styles**: picks configs across trading styles (great for diversification).
                      - **⚖️ Risk Spectrum**: picks configs from higher‑risk to lower‑risk (great to visualize the risk ladder).
                    - **🔢 Configs**: how many configs are compared in the radar.
                    - **⚖️ Use Weighted (_w) Metrics**: when available, uses weighted metric variants for ranking/display.
                    - **₿ Use BTC instead of USD**: switches the currency‑denominated metrics used for ranking/display.

                    **🖱️ How to select a config:**
                    - **Click a shape/point in the radar** to select that config.
                    - **Select Configuration** chooses from the configs currently shown in the radar.
                    - **🎯 Select Config** (below) chooses from *all* visible configs.
                    - All three selection methods stay synchronized.

                    **💡 Workflow tips:**
                    1. Start with **🏆 Top Performers** and 6–10 configs to see the “winners”.
                    2. Switch to **🧬 Diverse Styles** to find complementary behavior.
                    3. Use **⚖️ Risk Spectrum** to decide how much risk you want.
                    """)
            
            pareto_configs = self.loader.get_pareto_configs()
            if not pareto_configs:
                st.info("No Pareto configs available.")
            else:
                # Define widget keys at the start
                correlation_key = 'correlation_config_selector'
                fragment_key = 'fragment_config_selector'
                
                # User controls in one row with spacing
                col1, col2, col_spacer, col3 = st.columns([3, 2, 1, 3])
                
                with col1:
                    selection_strategy = st.radio(
                        "Selection Strategy:",
                        ["Top Performers", "Diverse Styles", "Risk Spectrum"],
                        horizontal=False,
                        key='risk_profile_strategy',
                        help=(
                            "**Top Performers:** Best configs across different metrics\n\n"
                            "**Diverse Styles:** One config per trading style\n\n"
                            "**Risk Spectrum:** Configs from lowest to highest risk"
                        )
                    )
                
                with col2:
                    num_configs = st.slider(
                        "Configs:",
                        3, 10, 5, 1,
                        key='risk_profile_num',
                        help="Number of configs to compare"
                    )
                
                # col_spacer is empty for spacing
                
                with col3:
                    # Toggle controls like Evolution tab
                    use_weighted = st.toggle(
                        "Use Weighted (_w) Metrics", 
                        value=True,
                        help="Weighted metrics emphasize recent performance",
                        key='risk_profile_weighted_toggle'
                    )
                    
                    use_btc = st.toggle(
                        "Use BTC instead of USD",
                        value=False,
                        help="Switch between USD and BTC metrics",
                        key='risk_profile_btc_toggle'
                    )
                    
                    currency = "BTC" if use_btc else "USD"
                
                # Store settings in session state for config details display
                st.session_state.pareto_currency = currency
                st.session_state.pareto_use_weighted = use_weighted
                st.session_state.pareto_top_metrics = []  # Will be filled by Top Performers selection
                
                # Build metric suffix based on selections
                currency_suffix = "_usd" if currency == "USD" else "_btc"
                weight_suffix = "_w" if use_weighted else ""
                
                selected_configs = []
                labels = []
                
                if selection_strategy == "Top Performers":
                    # Diverse top performers across key metrics
                    # Note: Allow duplicate configs - same config can be best in multiple metrics
                    
                    # Use helper to find best metric variant that exists
                    def find_metric(base_name: str, prefer_weighted: bool = True, need_currency: bool = True):
                        """Find best available variant of a metric
                        
                        Logic: All metrics exist without _w, but not all have _w variant.
                        - If prefer_weighted: try _w first, fallback to base
                        - If not prefer_weighted: use base directly
                        """
                        if not pareto_configs:
                            return base_name + (currency_suffix if need_currency else "")
                        
                        # Check first config's available metrics
                        available_metrics = set(pareto_configs[0].suite_metrics.keys())
                        
                        if need_currency:
                            # Currency metrics: base_usd or base_w_usd
                            base_metric = f"{base_name}{currency_suffix}"
                            weighted_metric = f"{base_name}_w{currency_suffix}"
                            
                            if prefer_weighted and weighted_metric in available_metrics:
                                return weighted_metric
                            return base_metric
                        else:
                            # Shared metrics: base or base_w
                            base_metric = base_name
                            weighted_metric = f"{base_name}_w"
                            
                            if prefer_weighted and weighted_metric in available_metrics:
                                return weighted_metric
                            return base_metric
                    
                    candidates = []
                    used_metric_names = []  # Track actual metric names used
                    
                    # Define metrics to check (name, maximize/minimize, need_currency, label_format)
                    metric_defs = [
                        ('adg', True, True, 'Top ADG', '.6f'),
                        ('sharpe_ratio', True, True, 'Top Sharpe', '.3f'),
                        ('drawdown_worst', False, True, 'Best DD', '.4f'),  # minimize
                        ('calmar_ratio', True, True, 'Top Calmar', '.3f'),
                        ('sortino_ratio', True, True, 'Top Sortino', '.3f'),
                        ('omega_ratio', True, True, 'Top Omega', '.3f'),
                        ('gain', True, True, 'Top Gain', '.2f'),
                        ('loss_profit_ratio', False, False, 'Best L/P', '.3f'),  # minimize, shared metric
                        ('position_held_hours_mean', False, False, 'Fast Trade', '.1f'),  # minimize
                        ('volume_pct_per_day_avg', True, False, 'Top Volume', '.2f'),  # shared metric
                    ]
                    
                    for base_name, maximize, need_currency, label_prefix, fmt in metric_defs:
                        metric_name = find_metric(base_name, use_weighted, need_currency)
                        used_metric_names.append(metric_name)  # Track for display
                        
                        # Find best config for this metric
                        if maximize:
                            best = max(pareto_configs, key=lambda c: c.suite_metrics.get(metric_name, float('-inf')), default=None)
                        else:
                            best = min(pareto_configs, key=lambda c: c.suite_metrics.get(metric_name, float('inf')), default=None)
                        
                        if best:
                            val = best.suite_metrics.get(metric_name, 0)
                            label = f"{label_prefix} ({val:{fmt}})"
                            if base_name == 'position_held_hours_mean':
                                label = f"{label_prefix} ({val:.1f}h)"
                            elif base_name == 'volume_pct_per_day_avg':
                                label = f"{label_prefix} ({val:.2f}%)"
                            candidates.append((best, label))
                    
                    # Store used metrics in session state for config details display
                    st.session_state.pareto_top_metrics = used_metric_names
                    
                    # Take top N candidates
                    for config, label in candidates[:num_configs]:
                        selected_configs.append(config.config_index)
                        labels.append(f"#{config.config_index} {label}")
                
                elif selection_strategy == "Diverse Styles":
                    # One or more configs per trading style
                    styles = {}
                    for config in pareto_configs:
                        style = self.loader.compute_trading_style(config)
                        if style not in styles:
                            styles[style] = []
                        styles[style].append(config)
                    
                    # Sort each style by ADG and pick best
                    configs_per_style = max(1, num_configs // len(styles)) if styles else 1
                    
                    for style, style_configs in sorted(styles.items()):
                        sorted_style = sorted(style_configs, 
                                            key=lambda c: c.suite_metrics.get('adg_w_usd', float('-inf')), 
                                            reverse=True)
                        for i, config in enumerate(sorted_style[:configs_per_style]):
                            if len(selected_configs) >= num_configs:
                                break
                            selected_configs.append(config.config_index)
                            adg = config.suite_metrics.get('adg_w_usd', 0)
                            labels.append(f"#{config.config_index} {style} (ADG: {adg:.6f})")
                        if len(selected_configs) >= num_configs:
                            break
                
                elif selection_strategy == "Risk Spectrum":
                    # Sort by overall risk score, pick evenly distributed
                    configs_with_risk = []
                    for config in pareto_configs:
                        risk_profile = self.loader.compute_risk_profile_score(config)
                        # Average risk score (0-10, higher = safer)
                        avg_risk = sum(risk_profile.values()) / len(risk_profile)
                        configs_with_risk.append((config, avg_risk))
                    
                    # Sort by risk (lowest to highest)
                    configs_with_risk.sort(key=lambda x: x[1])
                    
                    # Pick evenly distributed across spectrum
                    if len(configs_with_risk) <= num_configs:
                        selected_items = configs_with_risk
                    else:
                        step = (len(configs_with_risk) - 1) / (num_configs - 1)
                        indices = [int(i * step) for i in range(num_configs)]
                        selected_items = [configs_with_risk[i] for i in indices]
                    
                    for config, risk_score in selected_items:
                        selected_configs.append(config.config_index)
                        risk_label = "High Risk" if risk_score < 4 else "Medium Risk" if risk_score < 7 else "Low Risk"
                        labels.append(f"#{config.config_index} {risk_label} ({risk_score:.1f}/10)")
                
                if selected_configs:
                    # Center the radar chart with better sizing
                    col_left, col_chart, col_right = st.columns([0.5, 4, 0.5])
                    
                    with col_chart:
                        fig = self.viz.plot_risk_profile_radar(selected_configs, labels)
                        
                        # Make radar chart clickable to select configs
                        radar_selection = st.plotly_chart(fig, use_container_width=True, on_select="rerun", key='risk_profile_radar')
                        
                        # Handle clicks on radar chart (both points and legend)
                        if radar_selection and hasattr(radar_selection, 'selection'):
                            clicked_config_index = None
                            
                            # Check for point clicks
                            if radar_selection.selection.points:
                                try:
                                    point_data = radar_selection.selection.points[0]
                                    
                                    # Priority 1: customdata
                                    if 'customdata' in point_data and point_data['customdata'] is not None:
                                        customdata = point_data['customdata']
                                        if isinstance(customdata, (list, tuple)) and len(customdata) > 0:
                                            clicked_config_index = int(customdata[0])
                                        elif isinstance(customdata, (int, float)):
                                            clicked_config_index = int(customdata)
                                    
                                    # Priority 2: point_index + curve_number mapping
                                    if clicked_config_index is None:
                                        curve_num = point_data.get('curve_number')
                                        if curve_num is not None and 0 <= curve_num < len(selected_configs):
                                            clicked_config_index = selected_configs[curve_num]
                                    
                                    # Priority 3: trace_index
                                    if clicked_config_index is None:
                                        trace_idx = point_data.get('trace_index')
                                        if trace_idx is not None and 0 <= trace_idx < len(selected_configs):
                                            clicked_config_index = selected_configs[trace_idx]
                                    
                                    # Priority 4: Just use first selected config if nothing else works
                                    if clicked_config_index is None and selected_configs:
                                        # Try to extract curve number from any numeric field
                                        for key in ['curveNumber', 'curve_number', 'traceIndex', 'trace_index']:
                                            if key in point_data:
                                                idx = point_data[key]
                                                if isinstance(idx, (int, float)) and 0 <= int(idx) < len(selected_configs):
                                                    clicked_config_index = selected_configs[int(idx)]
                                                    break
                                
                                except Exception as e:
                                    pass
                            
                            # Update session state if we got a valid config index
                            if clicked_config_index is not None:
                                # IMPORTANT: plotly selection persists across reruns.
                                # Without a guard, an old click keeps re-applying and overwrites dropdown changes.
                                last_clicked = st.session_state.get('_risk_profile_last_clicked_config')
                                if last_clicked != clicked_config_index:
                                    st.session_state['_risk_profile_last_clicked_config'] = clicked_config_index

                                    current_selection = st.session_state.get('pareto_selected_config')
                                    if current_selection != clicked_config_index:
                                        st.session_state.pareto_selected_config = clicked_config_index
                                        # Sync both widget keys (store int config_index)
                                        st.session_state[fragment_key] = clicked_config_index
                                        if clicked_config_index in selected_configs:
                                            st.session_state[correlation_key] = clicked_config_index
                                        st.session_state['_pareto_last_change'] = 'radar_chart'
                    
                    # Config selector dropdown
                    st.markdown("---")
                    col_select, col_info = st.columns([1, 3], vertical_alignment="bottom")
                    
                    with col_select:
                        # Build labels but keep widget values as stable ints
                        idx_to_corr_label = {idx: label for idx, label in zip(selected_configs, labels)}
                        
                        # Get current selection or default to first
                        current_selected = st.session_state.get('pareto_selected_config')
                        if current_selected not in selected_configs:
                            current_selected = selected_configs[0]
                            st.session_state.pareto_selected_config = current_selected

                        # Keep correlation widget valid and synced (when selected config is in the top list)
                        if correlation_key not in st.session_state or st.session_state.get(correlation_key) not in selected_configs:
                            st.session_state[correlation_key] = current_selected
                        elif current_selected in selected_configs and st.session_state.get(correlation_key) != current_selected:
                            st.session_state[correlation_key] = current_selected

                        def _on_correlation_config_change():
                            idx = st.session_state.get(correlation_key)
                            if isinstance(idx, int) and idx in selected_configs:
                                st.session_state.pareto_selected_config = idx
                                st.session_state[fragment_key] = idx
                                st.session_state['_pareto_last_change'] = 'correlation_selectbox'
                        
                        # Dropdown selector with key (value=int config_index)
                        st.selectbox(
                            "Select Configuration:",
                            options=selected_configs,
                            format_func=lambda idx: idx_to_corr_label.get(idx, f"Config #{idx}"),
                            key=correlation_key,
                            on_change=_on_correlation_config_change,
                            help="Choose a configuration to view detailed metrics and backtest results"
                        )
                    
                    with col_info:
                        st.info("💡 **Tip:** Click on the chart or use dropdown to select a config and view full details below.")
                    
                    # Show config details using the same fragment as 2D/3D/Evolution tabs
                    self._show_config_details_fragment()
                else:
                    st.info("No configs available for comparison.")
    
    def _generate_insights(self) -> List[tuple]:
        """Generate smart insights about the optimization"""
        
        insights = []
        
        # Check parameters at bounds
        bounds_info = self.loader.get_parameters_at_bounds(tolerance=0.1)
        at_bounds_count = len(bounds_info['at_lower']) + len(bounds_info['at_upper'])
        
        if at_bounds_count > 0:
            insights.append(("warning", f"{at_bounds_count} parameters are near bounds - consider extending search space!"))
        else:
            insights.append(("success", "All parameters are well within bounds - good search space coverage!"))
        
        # Check robustness
        pareto_configs = self.loader.get_pareto_configs()
        if pareto_configs:
            robustness_scores = [self.loader.compute_overall_robustness(c) for c in pareto_configs]
            avg_robust = np.mean(robustness_scores)
            
            # Check if we have scenarios (suite mode) or single backtest (non-suite)
            has_scenarios = self.loader.scenario_labels and len(self.loader.scenario_labels) > 1
            
            if avg_robust > 0.85:
                if has_scenarios:
                    insights.append(("success", f"Excellent robustness across scenarios (avg: {avg_robust:.2f})!"))
                else:
                    insights.append(("success", f"Excellent consistency in metrics (robustness: {avg_robust:.2f})!"))
            elif avg_robust < 0.70:
                if has_scenarios:
                    insights.append(("warning", f"Configs show high variability across scenarios (avg robustness: {avg_robust:.2f})"))
                else:
                    insights.append(("warning", f"Configs show high variability in metrics (robustness: {avg_robust:.2f})"))
        
        # Check scenario performance
        if self.loader.scenario_labels and len(self.loader.scenario_labels) > 1:
            primary_metric = self.loader.scoring_metrics[0] if self.loader.scoring_metrics else 'adg_w_usd'
            
            scenario_avgs = {}
            for scenario in self.loader.scenario_labels:
                values = [c.scenario_metrics.get(scenario, {}).get(primary_metric, 0) 
                         for c in pareto_configs if scenario in c.scenario_metrics]
                if values:
                    scenario_avgs[scenario] = np.mean(values)
            
            if len(scenario_avgs) > 1:
                best_scenario = max(scenario_avgs, key=scenario_avgs.get)
                worst_scenario = min(scenario_avgs, key=scenario_avgs.get)
                
                diff_pct = ((scenario_avgs[best_scenario] - scenario_avgs[worst_scenario]) / scenario_avgs[worst_scenario] * 100)
                
                insights.append(("info", f"{best_scenario.title()} outperforms {worst_scenario.title()} by {diff_pct:.0f}%"))
        
        # Trading style diversity
        styles = {}
        for config in pareto_configs:
            style = self.loader.compute_trading_style(config)
            styles[style] = styles.get(style, 0) + 1
        
        if len(styles) > 1:
            insights.append(("info", f"Good diversity: {len(styles)} different trading styles in Pareto set"))
        
        return insights


def main():
    """Main entry point"""
    
    # Get results path from command line or ask user
    if len(sys.argv) > 1:
        results_path = sys.argv[1]
    else:
        st.error("❌ Please provide results path as argument")
        st.code("streamlit run ParetoExplorer.py -- /path/to/optimize_results/...")
        st.stop()
    
    # Validate path
    if not os.path.exists(results_path):
        st.error(f"❌ Path not found: {results_path}")
        st.stop()
    
    all_results_path = os.path.join(results_path, "all_results.bin")
    if not os.path.exists(all_results_path):
        st.error(f"❌ all_results.bin not found in: {results_path}")
        st.stop()
    
    # Run explorer
    explorer = ParetoExplorer(results_path)
    explorer.run()


if __name__ == "__main__":
    main()
