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
from typing import List, Dict, Optional

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

from ParetoDataLoader import ParetoDataLoader, ConfigMetrics
from ParetoVisualizations import ParetoVisualizations
import pbgui_purefunc as pbfunc
from Config import CURRENCY_METRICS, SHARED_METRICS


class ParetoExplorer:
    """The Genius Pareto Explorer - Main Application"""
    
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
        
        # Get load strategy and max_configs from session state (convert to tuple for caching)
        load_strategy = st.session_state.get('load_strategy', ['performance', 'robustness', 'sharpe'])
        max_configs = st.session_state.get('max_configs', 2000)
        load_strategy_tuple = tuple(sorted(load_strategy))  # Sorted tuple for stable cache key
        all_results_loaded = st.session_state.get('all_results_loaded', False)
        
        # Load data (with caching and two-stage approach)
        # Stage 1: Always load Pareto JSONs (fast)
        # Stage 2: Optionally load all_results.bin (slow)
        data = ParetoExplorer._load_data(self.results_path, load_strategy_tuple, max_configs, all_results_loaded)
        if data is None:
            st.error("❌ Failed to load data")
            st.info(f"**Results Path:** `{self.results_path}`")
            
            # Check if path exists
            all_results_path = os.path.join(self.results_path, "all_results.bin")
            if not os.path.exists(self.results_path):
                st.error(f"Directory not found: `{self.results_path}`")
            elif not os.path.exists(all_results_path):
                st.error(f"File not found: `all_results.bin`")
                st.caption("Make sure the optimization completed successfully and all_results.bin was created.")
            else:
                st.error("Unknown error loading data. Check the terminal for details.")
            
            st.stop()
        
        # Unpack cached data
        self.loader, self.viz, load_stats = data
        
        # Sidebar navigation
        self._render_sidebar(load_stats)
        
        # Main content area
        stage = st.session_state.get('stage', 'Command Center')
        
        if stage == 'Command Center':
            self._show_command_center()
        elif stage == 'Pareto Playground':
            self._show_pareto_playground()
        elif stage == 'Deep Intelligence':
            self._show_deep_intelligence()
        elif stage == 'Adversarial Lab':
            self._show_adversarial_lab()
        elif stage == 'Portfolio Architect':
            self._show_portfolio_architect()
        elif stage == 'What-If Sandbox':
            self._show_whatif_sandbox()
    
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
            Tuple of (loader, viz, load_stats) or None on error
        """
        try:
            loader = ParetoDataLoader(results_path)
            
            if not all_results_loaded:
                # STAGE 1: Fast mode - Load only Pareto configs from JSON
                with st.spinner("⚡ Loading Pareto configs from JSON..."):
                    success = loader.load_pareto_jsons_only()
                    
                    if not success:
                        # Fallback: Try all_results.bin if no pareto JSONs found
                        st.warning("⚠️ No pareto/*.json files found. Loading all_results.bin...")
                        strategy_list = list(load_strategy) if load_strategy else ['performance']
                        success = loader.load(load_strategy=strategy_list, max_configs=max_configs)
                        
                        if not success:
                            st.error("❌ Failed to load data from both pareto JSONs and all_results.bin")
                            return None
            else:
                # STAGE 2: Full mode - Load all configs from all_results.bin
                with st.spinner("🔄 Loading all configs from all_results.bin... this may take 10-30 seconds"):
                    strategy_list = list(load_strategy) if load_strategy else ['performance']
                    success = loader.load(load_strategy=strategy_list, max_configs=max_configs)
                    
                    if not success:
                        st.error("❌ Failed to load all_results.bin")
                        return None
            
            viz = ParetoVisualizations(loader)
            load_stats = loader.load_stats
            
            return (loader, viz, load_stats)
            
        except Exception as e:
            st.error(f"❌ Exception during data loading:")
            st.exception(e)
            import traceback
            st.code(traceback.format_exc())
            return None
    
    def _render_sidebar(self, load_stats: Dict):
        """Render sidebar with navigation and info"""
        
        with st.sidebar:
            st.title("🎯 Genius Pareto Explorer")
            
            # === DATA SOURCE SECTION ===
            st.markdown("### 📊 Data Source")
            
            all_results_loaded = st.session_state.get('all_results_loaded', False)
            
            if not all_results_loaded:
                # FAST MODE: Showing Pareto only
                num_pareto = load_stats.get('pareto_configs', 0)
                st.success(f"✅ Loaded **{num_pareto} Pareto configs** from JSON")
                st.info("💡 **Fast mode:** Showing best configs only")
                
                # Button to load all results
                if st.button("🔄 Load More from all_results.bin", 
                           help="Load all optimization results to compare with non-Pareto configs. This may take 10-30 seconds.",
                           use_container_width=True):
                    st.session_state['all_results_loaded'] = True
                    st.rerun()
            else:
                # FULL MODE: All results loaded
                num_total = load_stats.get('selected_configs', 0)
                num_pareto = load_stats.get('pareto_configs', 0)
                num_non_pareto = num_total - num_pareto
                
                st.success(f"✅ Loaded **{num_total:,} total configs**")
                st.caption(f"📋 {num_pareto} Pareto + {num_non_pareto:,} others")
                
                # Button to switch back to fast mode
                if st.button("⚡ Switch to Fast Mode", 
                           help="Show only Pareto configs for faster performance",
                           use_container_width=True):
                    st.session_state['all_results_loaded'] = False
                    st.rerun()
            
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
            
            # Navigation
            st.subheader("🗺️ Navigate")
            
            stages = [
                "🎯 Command Center",
                "🎨 Pareto Playground", 
                "🧠 Deep Intelligence",
                "🎲 Adversarial Lab",
                "💼 Portfolio Architect",
                "🎮 What-If Sandbox"
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
    
    def _show_config_details(self, config_index):
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
            for metric in self.loader.scoring_metrics[:3]:
                if metric in config.suite_metrics:
                    st.metric(metric.replace('_', ' ').title(), f"{config.suite_metrics[metric]:.6f}")
        
        with col2:
            st.markdown("**🎯 Trading Style**")
            style = self.loader.compute_trading_style(config)
            st.markdown(f"**{style}**")
        
        with col3:
            st.markdown("**💪 Robustness**")
            robust = self.loader.compute_overall_robustness(config)
            stars = "⭐" * int(robust * 5)
            st.metric("Score", f"{robust:.2f}")
            st.markdown(f"**{stars}**")
        
        st.markdown("---")
        
        # Full config
        col_left, col_right = st.columns(2)
        
        with col_left:
            with st.expander("📋 Full Configuration", expanded=True):
                try:
                    full_config_data = self.loader.get_full_config(config.config_index)
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
            if st.button("🚀 Run Backtest", width='stretch', type="primary", key=f"bt_modal_{config_index}"):
                try:
                    import BacktestV7
                    from pbgui_func import get_navi_paths, pb7dir
                    from pathlib import Path
                    import json
                    import time
                    
                    full_config_data = self.loader.get_full_config(config.config_index)
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
        
        total_configs = len(self.loader.configs)
        pareto_count = sum(1 for c in self.loader.configs if c.is_pareto)
        convergence = min(100, (pareto_count / max(total_configs * 0.001, 1)) * 100)
        
        with col1:
            st.metric("🔢 Total Configs", f"{total_configs:,}")
        with col2:
            st.metric("⭐ Pareto Optimal", pareto_count)
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
                    
                    # Adg W Usd
                    if 'adg_w_usd' in config.suite_metrics:
                        val = config.suite_metrics['adg_w_usd']
                        st.metric("Adg W Usd", f"{val:.6f}", help="Average Daily Gain (weighted) in USD - Daily profit rate accounting for wallet exposure")
                    
                    # Sharpe Ratio Usd
                    if 'sharpe_ratio_usd' in config.suite_metrics:
                        val = config.suite_metrics['sharpe_ratio_usd']
                        st.metric("Sharpe Ratio Usd", f"{val:.6f}", help="Risk-adjusted return metric - Higher is better. Measures excess return per unit of risk")
                    
                    # Gain Usd
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
        st.subheader("💡 GENIUS INSIGHTS")
        
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
        
        col1, col2 = st.columns(2)
        
        with col1:
            # 2D Scatter
            if len(scoring_metrics) >= 2:
                fig = self.viz.plot_pareto_scatter_2d(
                    x_metric=scoring_metrics[0],
                    y_metric=scoring_metrics[1],
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
                                        self._show_config_details(clicked_config_index)
                                        
                                        # Backtest button
                                        if st.button(f"🚀 Start Backtest for Config #{clicked_config_index}", 
                                                   key=f'bt_preview_2d_{clicked_config_index}'):
                                            self._start_backtest(config)
                    except Exception as e:
                        st.warning(f"⚠️ Click handling error: {e}")
        
        with col2:
            # Robustness Quadrant
            fig = self.viz.plot_robustness_quadrant(
                performance_metric=primary_metric,
                show_all=False
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
                                    self._show_config_details(clicked_config_index)
                                    
                                    # Backtest button
                                    if st.button(f"🚀 Start Backtest for Config #{clicked_config_index}", 
                                               key=f'bt_preview_rob_{clicked_config_index}'):
                                        self._start_backtest(config)
                except Exception as e:
                    st.warning(f"⚠️ Click handling error: {e}")
    
    def _show_pareto_playground(self):
        """Stage 2: Pareto Playground - Interactive Exploration"""
        
        st.title("🎨 PARETO PLAYGROUND")
        st.markdown("**Interactive multi-dimensional exploration**")
        
        st.markdown("---")
        
        # Preference sliders
        st.subheader("🎚️ YOUR PREFERENCES")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            perf_weight = st.slider("Performance Priority", 0, 100, 80, 5, key='perf_weight')
        with col2:
            risk_weight = st.slider("Risk Aversion", 0, 100, 60, 5, key='risk_weight')
        with col3:
            robust_weight = st.slider("Robustness Importance", 0, 100, 70, 5, key='robust_weight')
        
        # Track previous slider values to detect changes
        current_weights = (perf_weight, risk_weight, robust_weight)
        if 'prev_slider_weights' not in st.session_state:
            st.session_state['prev_slider_weights'] = current_weights
        
        sliders_changed = st.session_state['prev_slider_weights'] != current_weights
        
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
        # Update when: 1) first load, 2) sliders changed
        if 'pareto_selected_config' not in st.session_state or sliders_changed:
            st.session_state['pareto_selected_config'] = best_match.config_index
            st.session_state['pareto_selectbox_key'] = best_match.config_index
            st.session_state['prev_slider_weights'] = current_weights
        
        st.success(f"🎯 **Best Match for Your Preferences:** Config #{best_match.config_index} (Score: {best_score:.3f})")
        
        st.markdown("---")
        
        # Visualization controls
        st.subheader("📊 MULTI-DIMENSIONAL EXPLORER")
        
        col1, col2 = st.columns([2, 1])
        
        with col2:
            st.markdown("**Chart Settings**")
            
            viz_type = st.radio("Visualization:", ["2D Scatter", "3D Scatter", "Radar Chart"], key='viz_type')
            
            # Show "Show all configs" checkbox only for Scatter plots, not for Radar Chart
            if viz_type in ["2D Scatter", "3D Scatter"]:
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
                    "Custom...",
                    "**Profit vs Risk**",
                    "**Risk-Adjusted**",
                    "**Profit vs Quality**",
                    "**Efficiency**",
                    "**Multi-Risk**",
                    "**Profit vs Recovery**",
                    "**Performance Ratios**",
                    "**Exposure Analysis**"
                ]
                
                # Help texts for each preset with metric details
                preset_help = {
                    preset_options[0]: "Choose your own X and Y axis metrics for custom analysis",
                    preset_options[1]: f"📈 **Profit vs Risk**: {metric('adg')} vs {metric('drawdown_worst')}\n\nBalance daily gains against maximum drawdown. Ideal for finding high-return configs with acceptable risk levels.",
                    preset_options[2]: f"⚖️ **Risk-Adjusted**: {metric('sharpe_ratio')} vs {metric('sortino_ratio')}\n\nCompare Sharpe (total risk adjustment) vs Sortino (downside risk only). Shows which configs deliver best returns per unit of risk.",
                    preset_options[3]: f"🎯 **Profit vs Quality**: {metric('adg')} vs {metric('equity_choppiness')}\n\nDaily gains vs equity curve smoothness. Low choppiness = steadier growth with fewer ups and downs.",
                    preset_options[4]: f"💡 **Efficiency**: {metric('adg')} vs {metric(exposure_metric)}\n\nProfit per unit of capital exposure. Shows which configs generate most return with least capital at risk.",
                    preset_options[5]: f"🛡️ **Multi-Risk**: {metric('drawdown_worst')} vs {metric('expected_shortfall_1pct')}\n\nWorst drawdown vs extreme loss scenarios (1% VaR). Identifies configs that handle both typical and extreme losses well.",
                    preset_options[6]: f"⏱️ **Profit vs Recovery**: {metric('adg')} vs {metric('peak_recovery_hours_equity')}\n\nDaily gains vs time needed to recover from drawdowns. Fast recovery = capital back to work sooner.",
                    preset_options[7]: f"📊 **Performance Ratios**: {metric('calmar_ratio')} vs {metric('omega_ratio')}\n\nCalmar (return/drawdown) vs Omega (gains/losses). Advanced risk-adjusted metrics for sophisticated analysis.",
                    preset_options[8]: f"💰 **Exposure Analysis**: {metric('adg')} vs total_wallet_exposure_mean\n\nProfit vs average capital usage. Lower exposure = more capital available for other strategies."
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
                        st.session_state['preset_view'] = preset_options[1]  # Profit vs Risk
                
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
                    preset_options[1]: (metric('adg'), metric('drawdown_worst')),
                    preset_options[2]: (metric('sharpe_ratio'), metric('sortino_ratio')),
                    preset_options[3]: (metric('adg'), metric('equity_choppiness')),
                    preset_options[4]: (metric('adg'), metric(exposure_metric)),
                    preset_options[5]: (metric('drawdown_worst'), metric('expected_shortfall_1pct')),
                    preset_options[6]: (metric('adg'), metric('peak_recovery_hours_equity')),
                    preset_options[7]: (metric('calmar_ratio'), metric('omega_ratio')),
                    preset_options[8]: (metric('adg'), "total_wallet_exposure_mean")
                }
                
                st.markdown("---")
                
                # If preset selected, use those metrics; otherwise show dropdowns
                # Match preset by index instead of string to handle toggle state changes
                preset_index = preset_options.index(preset_choice) if preset_choice in preset_options else 0
                
                if preset_index > 0:  # Not "Custom..." (which is at index 0)
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
                
            elif viz_type == "3D Scatter":
                # 3D Scatter - Separate preset logic
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
                    "Custom...",
                    "**Risk-Reward Triangle**",
                    "**Recovery Performance**",
                    "**Trading Efficiency**",
                    "**Risk Spectrum**",
                    "**Stability Analysis**",
                    "**Trading Activity**",
                    "**Stress Test**"
                ]
                
                preset_3d_help = {
                    preset_3d_options[0]: "Choose your own X, Y and Z axis metrics for custom 3D analysis",
                    preset_3d_options[1]: f"🎯 **Risk-Reward Triangle**: {metric('adg')} vs {metric('drawdown_worst')} vs {metric('equity_jerkiness')}\n\nThe ultimate 3D view showing the raw ingredients: Profit × Max Risk × Volatility. Find configs with high returns, low drawdowns AND smooth equity curves. Uses equity jerkiness (rate of change volatility) instead of Sharpe Ratio to avoid mathematical dependencies.",
                    preset_3d_options[2]: f"⏱️ **Recovery Performance**: {metric('adg')} vs {metric('peak_recovery_hours_equity')} vs {metric('drawdown_worst')}\n\nProfit × Recovery Speed × Max Risk. Find configs that not only make money but recover quickly from losses. Critical for trading psychology: 'Deep & Fast' (deep drawdowns, quick recovery) vs 'Shallow & Slow'.",
                    preset_3d_options[3]: f"💡 **Trading Efficiency**: {metric('adg')} vs {metric(exposure_metric)} vs total_wallet_exposure_mean\n\nProfit × Capital Efficiency × Average Usage. Discover configs that generate maximum return with minimal capital at risk. Filter out strategies that lock up your entire wallet.",
                    preset_3d_options[4]: f"⚖️ **Risk Spectrum**: {metric('sharpe_ratio')} vs {metric('sortino_ratio')} vs {metric('calmar_ratio')}\n\nCompare three major risk-adjusted metrics: Sharpe (total risk), Sortino (downside only), Calmar (drawdown adjusted). Find configs that excel at ALL three, or discover which ones only look good under specific risk definitions.",
                    preset_3d_options[5]: f"📈 **Stability Analysis**: {metric('adg')} vs {metric('equity_choppiness')} vs {metric('loss_profit_ratio')}\n\nProfit × Smoothness × Win/Loss Balance. Find configs with steady, consistent growth patterns - the 'staircase to heaven' instead of a roller coaster. Shows if stability comes from high winrate (many small wins) or from smooth equity despite volatile trades.",
                    preset_3d_options[6]: f"🔄 **Trading Activity**: {metric('adg')} vs positions_held_per_day vs position_held_hours_mean\n\nProfit × Trade Frequency × Hold Duration. The 'strategy fingerprint' - instantly see clusters: Scalpers (many trades, short duration) vs Swing Traders (few trades, long hold). Perfect for diversification: pick one from each cluster.",
                    preset_3d_options[7]: f"🧪 **Stress Test**: {metric('drawdown_worst')} vs {metric('expected_shortfall_1pct')} vs {metric('loss_profit_ratio')}\n\nMax Drawdown × Expected Shortfall (VaR 1%) × Loss/Profit Ratio. Shows worst past event, statistical tail risk (1% worst case), and loss balance. Find configs that handle extreme events gracefully and maintain good win/loss structure."
                }
                
                # Show preset selection with help icon (same layout as 2D)
                col_radio, col_help = st.columns([4, 1])
                with col_radio:
                    preset_3d_choice = st.radio("Quick Views:", preset_3d_options, key='preset_3d_view', label_visibility="visible")
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
                    preset_3d_options[1]: (metric('adg'), metric('drawdown_worst'), metric('equity_jerkiness')),
                    preset_3d_options[2]: (metric('adg'), metric('peak_recovery_hours_equity'), metric('drawdown_worst')),
                    preset_3d_options[3]: (metric('adg'), metric(exposure_metric), "total_wallet_exposure_mean"),
                    preset_3d_options[4]: (metric('sharpe_ratio'), metric('sortino_ratio'), metric('calmar_ratio')),
                    preset_3d_options[5]: (metric('adg'), metric('equity_choppiness'), metric('loss_profit_ratio')),
                    preset_3d_options[6]: (metric('adg'), "positions_held_per_day", "position_held_hours_mean"),
                    preset_3d_options[7]: (metric('drawdown_worst'), metric('expected_shortfall_1pct'), metric('loss_profit_ratio'))
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
        
        with col1:
            if viz_type == "2D Scatter":
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
                                    # Also update the selectbox key so it shows the new selection
                                    st.session_state.pareto_selectbox_key = clicked_config_index
                    except Exception as e:
                        st.warning(f"⚠️ Click handling error: {e}")
            
            elif viz_type == "3D Scatter":
                fig = self.viz.plot_pareto_scatter_3d(
                    x_metric=x_metric,
                    y_metric=y_metric,
                    z_metric=z_metric,
                    color_metric=color_metric,
                    show_all=show_all,
                    best_match_config=best_match
                )
                event = st.plotly_chart(fig, width='stretch', on_select="rerun", key='chart_3d')
                
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
                                configs = self.loader.get_pareto_configs() if not show_all else self.loader.configs[:1000]
                                if 0 <= idx < len(configs):
                                    clicked_config_index = configs[idx].config_index
                            
                            # Update session state (on_select="rerun" will trigger the rerun)
                            if clicked_config_index is not None:
                                current_selection = st.session_state.get('pareto_selected_config')
                                if current_selection != clicked_config_index:
                                    st.session_state.pareto_selected_config = clicked_config_index
                                    # Also update the selectbox key so it shows the new selection
                                    st.session_state.pareto_selectbox_key = clicked_config_index
                    except Exception as e:
                        st.warning(f"⚠️ Click handling error: {e}")
            
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
                                    st.session_state.pareto_selectbox_key = clicked_config_index
                    except Exception as e:
                        st.warning(f"⚠️ Click handling error: {e}")
        
        st.markdown("---")
        
        # Config selector
        st.subheader("🔍 SELECTED CONFIG DETAILS")
        
        pareto_configs = self.loader.get_pareto_configs()
        
        if not pareto_configs:
            st.warning("⚠️ No Pareto configs available")
            st.stop()
        
        pareto_indices = [c.config_index for c in pareto_configs]
        
        # Initialize session state for selected config if not exists
        if 'pareto_selected_config' not in st.session_state:
            st.session_state.pareto_selected_config = pareto_indices[0]
        
        # Ensure selected config is in the list (important after click events)
        if st.session_state.pareto_selected_config not in pareto_indices:
            st.warning(f"⚠️ Selected config {st.session_state.pareto_selected_config} is not in Pareto front")
            st.session_state.pareto_selected_config = pareto_indices[0]
        
        # Calculate the index for the selectbox based on current selection
        try:
            default_index = pareto_indices.index(st.session_state.pareto_selected_config)
        except ValueError:
            default_index = 0
            st.session_state.pareto_selected_config = pareto_indices[0]
        
        # Selectbox with key - Streamlit manages the state automatically
        st.selectbox(
            "🎯 Select Config Index:", 
            pareto_indices, 
            index=default_index,
            key='pareto_selectbox_key',
            help=f"Choose from {len(pareto_indices)} Pareto-optimal configs"
        )
        
        # Read the value from session_state (set by the widget)
        selected_config_index = st.session_state.pareto_selectbox_key
        
        # Update our tracking variable
        st.session_state.pareto_selected_config = selected_config_index
        
        # Find the config with this config_index
        config = next((c for c in self.loader.configs if c.config_index == selected_config_index), None)
        
        if config is None:
            st.error(f"❌ Config {selected_idx} not found in loaded configs")
            st.info(f"Available config_indices: {[c.config_index for c in self.loader.configs[:10]]}...")
            st.stop()
        
        # Show config details
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**📊 Metrics**")
            
            # Show first 5 scoring metrics with helpful tooltips
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
            
            for metric in self.loader.scoring_metrics[:5]:
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
            st.metric("Positions/Day", f"{config.suite_metrics.get('positions_held_per_day', 0):.2f}",
                     help="Average number of positions opened per day - Higher = more active trading")
            st.metric("Avg Hold Hours", f"{config.suite_metrics.get('position_held_hours_mean', 0):.1f}",
                     help="Average time positions are held open - Lower = faster turnover, scalping style")
        
        with col3:
            st.markdown("**💪 Robustness**")
            robust = self.loader.compute_overall_robustness(config)
            st.metric("Overall Score", f"{robust:.3f}",
                     help="Consistency score (0-1) - Higher = more stable across scenarios. Calculated as 1/(1+CV)")
            
            if config.metric_stats and 'adg_w_usd' in config.metric_stats:
                stats = config.metric_stats['adg_w_usd']
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
            if st.button("🚀 Run Backtest with this Config", width='stretch', type="primary"):
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
    
    def _show_deep_intelligence(self):
        """Stage 3: Deep Intelligence - Parameter & Market Analysis"""
        
        st.title("🧠 DEEP INTELLIGENCE")
        st.markdown("**Advanced parameter and market regime analysis**")
        
        st.markdown("---")
        
        tab1, tab2, tab3, tab4 = st.tabs(["📊 Parameters", "🌍 Markets", "📈 Evolution", "🔗 Correlations"])
        
        with tab1:
            st.subheader("Parameter Influence Analysis")
            
            col1, col2 = st.columns([3, 1])
            
            with col2:
                top_n = st.slider("Top N Parameters", 10, 40, 20, key='top_n_params')
            
            with col1:
                fig = self.viz.plot_parameter_influence_heatmap(top_n=top_n)
                st.plotly_chart(fig, width='stretch')
            
            # Parameters at bounds
            st.markdown("---")
            st.subheader("⚠️ Parameters Near Bounds")
            
            fig = self.viz.plot_parameter_bounds_distance(top_n=15)
            st.plotly_chart(fig, width='stretch')
            
            bounds_info = self.loader.get_parameters_at_bounds(tolerance=0.1)
            
            if bounds_info['at_lower'] or bounds_info['at_upper']:
                st.warning(f"**{len(bounds_info['at_lower']) + len(bounds_info['at_upper'])} parameters are near bounds!** Consider extending search space.")
        
        with tab2:
            st.subheader("Market Regime Analysis")
            
            if self.loader.scenario_labels:
                st.markdown("**Scenario Performance Comparison**")
                
                metrics = self.loader.scoring_metrics if self.loader.scoring_metrics else ['adg_w_usd', 'sharpe_ratio_usd']
                
                selected_metric = st.selectbox("Select Metric:", metrics, key='scenario_metric')
                
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
                            st.metric(f"{scenario.title()} - Mean", f"{np.mean(values):.6f}")
                        with col2:
                            st.metric(f"{scenario.title()} - Std", f"{np.std(values):.6f}")
                        with col3:
                            st.metric(f"{scenario.title()} - Min", f"{np.min(values):.6f}")
                        with col4:
                            st.metric(f"{scenario.title()} - Max", f"{np.max(values):.6f}")
            else:
                st.info("No scenario data available")
        
        with tab3:
            st.subheader("Optimization Evolution Timeline")
            
            metrics = self.loader.scoring_metrics if self.loader.scoring_metrics else ['adg_w_usd']
            selected_metric = st.selectbox("Select Metric:", metrics, key='evolution_metric')
            
            window = st.slider("Smoothing Window", 10, 500, 100, 10, key='evolution_window')
            
            fig = self.viz.plot_evolution_timeline(selected_metric, window=window)
            st.plotly_chart(fig, width='stretch')
            
            st.info("💡 **Insight:** Red stars show when Pareto configs were discovered during optimization.")
        
        with tab4:
            st.subheader("Multi-Metric Correlation")
            
            # Trading style distribution
            col1, col2 = st.columns(2)
            
            with col1:
                fig = self.viz.plot_trading_style_distribution()
                st.plotly_chart(fig, width='stretch')
            
            with col2:
                # Risk profile comparison of top 3
                top_3 = self.loader.get_top_configs(
                    metric_name=self.loader.scoring_metrics[0] if self.loader.scoring_metrics else 'adg_w_usd',
                    n=3,
                    pareto_only=True
                )
                
                if top_3:
                    indices = [c.config_index for c in top_3]
                    labels = [f"Config #{idx}" for idx in indices]
                    
                    fig = self.viz.plot_risk_profile_radar(indices, labels)
                    st.plotly_chart(fig, width='stretch')
    
    def _show_adversarial_lab(self):
        """Stage 4: Adversarial Lab - Stress Testing"""
        
        st.title("🎲 ADVERSARIAL LAB")
        st.markdown("**Stress test configs under extreme scenarios**")
        
        st.markdown("---")
        
        st.info("🚧 **Coming Soon:** Stress testing with Monte Carlo simulations, adversarial scenarios, and fragility analysis.")
        
        # Config selector
        pareto_indices = [c.config_index for c in self.loader.get_pareto_configs()]
        selected_idx = st.selectbox("Select Config to Test:", pareto_indices, key='stress_test_config')
        
        if selected_idx is not None:
            config = self.loader.configs[selected_idx]
            
            st.subheader(f"Config #{config.config_index} - {self.loader.compute_trading_style(config)}")
            
            # Current metrics
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Current Drawdown", f"{config.suite_metrics.get('drawdown_worst_usd', 0):.4f}")
            with col2:
                st.metric("Current Sharpe", f"{config.suite_metrics.get('sharpe_ratio_usd', 0):.4f}")
            with col3:
                st.metric("Current ADG", f"{config.suite_metrics.get('adg_w_usd', 0):.6f}")
            
            st.markdown("---")
            
            # Stress scenarios
            st.subheader("🔥 STRESS SCENARIOS")
            
            scenarios = [
                ("2x Drawdown", "What if worst drawdown doubles?"),
                ("50% Volume Drop", "What if trading volume drops 50%?"),
                ("2x Volatility", "What if market volatility doubles?"),
                ("50% Fee Increase", "What if exchange fees increase 50%?"),
                ("3x Slippage", "What if slippage triples?")
            ]
            
            for scenario_name, scenario_desc in scenarios:
                with st.expander(scenario_name):
                    st.markdown(f"**{scenario_desc}**")
                    st.info("Simulation results would appear here...")
    
    def _show_portfolio_architect(self):
        """Stage 5: Portfolio Architect - Multi-Config Strategy"""
        
        st.title("💼 PORTFOLIO ARCHITECT")
        st.markdown("**Combine multiple configs for better risk-adjusted returns**")
        
        st.markdown("---")
        
        st.info("🚧 **Coming Soon:** Portfolio construction with correlation analysis, allocation optimization, and combined equity simulation.")
        
        st.markdown("""
        **Portfolio Benefits:**
        - 🛡️ **Reduced Drawdown** through diversification
        - 📈 **Smoother Equity Curve** 
        - 🎯 **Better Risk-Adjusted Returns**
        - 💪 **More Robust** across market regimes
        """)
        
        # Config selector for portfolio
        st.subheader("🎨 Build Your Portfolio")
        
        pareto_indices = [c.config_index for c in self.loader.get_pareto_configs()]
        
        selected_configs = st.multiselect(
            "Select 2-3 Configs:",
            pareto_indices,
            default=pareto_indices[:3] if len(pareto_indices) >= 3 else pareto_indices,
            max_selections=3,
            key='portfolio_configs'
        )
        
        if selected_configs:
            st.success(f"Selected {len(selected_configs)} configs for portfolio")
            
            # Show selected configs
            cols = st.columns(len(selected_configs))
            
            for i, idx in enumerate(selected_configs):
                config = self.loader.configs[idx]
                with cols[i]:
                    st.markdown(f"**Config #{idx}**")
                    st.markdown(f"{self.loader.compute_trading_style(config)}")
                    
                    primary_metric = self.loader.scoring_metrics[0] if self.loader.scoring_metrics else 'adg_w_usd'
                    st.metric("Performance", f"{config.suite_metrics.get(primary_metric, 0):.6f}")
                    st.metric("Robustness", f"{self.loader.compute_overall_robustness(config):.3f}")
    
    def _show_whatif_sandbox(self):
        """Stage 6: What-If Sandbox - Interactive Parameter Tuning"""
        
        st.title("🎮 WHAT-IF SANDBOX")
        st.markdown("**Experiment with parameter modifications**")
        
        st.markdown("---")
        
        st.info("🚧 **Coming Soon:** Interactive parameter sliders with ML-based performance prediction and quick validation.")
        
        # Config selector
        pareto_indices = [c.config_index for c in self.loader.get_pareto_configs()]
        selected_idx = st.selectbox("Select Base Config:", pareto_indices, key='whatif_config')
        
        if selected_idx is not None:
            config = self.loader.configs[selected_idx]
            
            st.subheader(f"Base Config #{config.config_index}")
            
            # Original metrics
            col1, col2, col3 = st.columns(3)
            
            primary_metric = self.loader.scoring_metrics[0] if self.loader.scoring_metrics else 'adg_w_usd'
            
            with col1:
                st.metric("Original Performance", f"{config.suite_metrics.get(primary_metric, 0):.6f}")
            with col2:
                st.metric("Original Drawdown", f"{config.suite_metrics.get('drawdown_worst_usd', 0):.4f}")
            with col3:
                st.metric("Original Sharpe", f"{config.suite_metrics.get('sharpe_ratio_usd', 0):.4f}")
            
            st.markdown("---")
            
            # Parameter playground
            st.subheader("🎛️ PARAMETER PLAYGROUND")
            
            st.markdown("**Top 5 Most Influential Parameters:**")
            
            # Example parameters (would be data-driven)
            param_examples = [
                'long_ema_span_0',
                'long_entry_initial_qty_pct',
                'long_close_grid_markup_end',
                'long_unstuck_threshold',
                'long_entry_trailing_threshold_pct'
            ]
            
            for param in param_examples:
                if param in config.bot_params:
                    current_val = config.bot_params[param]
                    
                    if param in self.loader.optimize_bounds:
                        lower, upper = self.loader.optimize_bounds[param]
                        
                        new_val = st.slider(
                            param.replace('long_', '').replace('_', ' ').title(),
                            float(lower),
                            float(upper),
                            float(current_val),
                            key=f'slider_{param}'
                        )
                        
                        if abs(new_val - current_val) > 0.001:
                            delta_pct = ((new_val - current_val) / current_val * 100) if current_val != 0 else 0
                            st.caption(f"Changed by {delta_pct:+.1f}%")
    
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
