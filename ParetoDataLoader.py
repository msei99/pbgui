"""
ParetoDataLoader - Loads and analyzes all_results.bin from Passivbot optimization runs
Provides comprehensive metrics, robustness scores, and parameter analysis
"""

import os
import msgpack
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
import json
import glob
from pathlib import Path, PurePath


@dataclass
class ConfigMetrics:
    """Holds all metrics and analysis for a single config"""
    config_index: int
    config_hash: str
    
    # Objectives
    objectives: Dict[str, float]
    constraint_violation: float
    
    # Suite metrics (aggregated)
    suite_metrics: Dict[str, float]
    
    # Scenario-specific metrics
    scenario_metrics: Dict[str, Dict[str, float]]  # scenario -> metric -> value
    
    # Robustness scores
    robustness_scores: Dict[str, float]  # metric -> robustness score (1/CV)
    
    # Bot parameters
    bot_params: Dict[str, Any]
    
    # Optimize bounds (parameter search space)
    bounds: Dict[str, Tuple[float, float]] = None
    
    # Optimize settings (scoring, limits, etc)
    optimize_settings: Dict[str, Any] = None
    
    # Backtest scenarios details
    scenario_details: List[Dict[str, Any]] = None
    
    # Stats for each metric (mean, min, max, std across scenarios)
    metric_stats: Dict[str, Dict[str, float]] = None
    
    # Is this config on the Pareto front?
    is_pareto: bool = False


class ParetoDataLoader:
    """Loads and analyzes all_results.bin from optimize runs"""
    
    def __init__(self, results_path: str):
        """
        Initialize loader with results directory
        
        Args:
            results_path: Path to optimize_results directory (contains all_results.bin)
        """
        self.results_path = results_path
        self.all_results_path = os.path.join(results_path, "all_results.bin")
        self.pareto_dir = os.path.join(results_path, "pareto")
        
        self.configs: List[ConfigMetrics] = []
        self.pareto_hashes: set = set()
        self.scoring_metrics: List[str] = []
        self.scenario_labels: List[str] = []
        
        # Cache for pareto JSON configs (index -> full config)
        self.pareto_configs_cache: Dict[int, Dict] = {}
        
        # Global optimization settings (same for all configs)
        self.optimize_bounds: Dict[str, Tuple[float, float]] = {}
        self.optimize_limits: List[Dict] = []
        self.backtest_scenarios: List[Dict] = []
        
    def load(self, load_strategy: List[str] = None, max_configs: int = 2000) -> bool:
        """
        Load all_results.bin and parse all configs
        
        Args:
            load_strategy: List of criteria to use for selecting top configs
                         Options: 'performance', 'robustness', 'sharpe', 'drawdown', 
                                 'calmar', 'sortino', 'omega', 'volatility', 'recovery'
                         Default: ['performance'] (Passivbot official)
            max_configs: Maximum number of configs to keep (default: 2000)
        
        Returns:
            True if successful, False otherwise
        """
        if load_strategy is None:
            load_strategy = ['performance']  # Default: Passivbot official
        
        if not os.path.exists(self.all_results_path):
            return False
        
        # Load pareto hashes to mark pareto configs
        self._load_pareto_hashes()
        
        # Load all configs from binary file
        try:
            configs_data = self._load_binary_file()
        except Exception as e:
            import traceback
            traceback.print_exc()
            return False
        
        # Parse each config
        parsed_count = 0
        
        # Parse ALL configs first (we need to find the best ones)
        for idx, config_data in enumerate(configs_data):
            try:
                metrics = self._parse_config(idx, config_data)
                self.configs.append(metrics)
                parsed_count += 1
            except Exception as e:
                if idx < 3:  # Show details for first 3 errors
                    import traceback
                    traceback.print_exc()
                continue
        
        if parsed_count == 0:
            return False
        
        # Select top N configs based on load_strategy
        total_parsed = len(self.configs)
        self.configs = self._select_top_configs_by_strategy(self.configs, load_strategy, max_count=max_configs)
        
        # Compute Pareto front based on objectives (optimized)
        self._compute_pareto_front_fast()
        
        # Store stats for GUI display
        self.load_stats = {
            'total_parsed': total_parsed,
            'selected_configs': len(self.configs),
            'pareto_configs': sum(1 for c in self.configs if c.is_pareto),
            'scenarios': self.scenario_labels,
            'scoring_metrics': self.scoring_metrics,
            'load_strategy': load_strategy,
            'max_configs': max_configs
        }
        
        return True
    
    def _select_top_configs_by_strategy(self, all_configs: List[ConfigMetrics], 
                                        strategy: List[str], 
                                        max_count: int = 2000) -> List[ConfigMetrics]:
        """
        Select top configs based on multiple criteria
        
        Args:
            all_configs: List of all parsed configs
            strategy: List of selection criteria
            max_count: Maximum number of configs to keep
        
        Returns:
            Selected configs (deduplicated)
        """
        if len(all_configs) <= max_count:
            return all_configs
        
        primary_metric = self.scoring_metrics[0] if self.scoring_metrics else 'adg_w_usd'
        
        # Calculate how many configs per criterion
        configs_per_criterion = max_count // len(strategy) if strategy else max_count
        
        selected_configs = []  # Use list (ConfigMetrics is not hashable)
        selected_indices = set()  # Track indices to avoid duplicates
        
        for criterion in strategy:
            if criterion == 'performance':
                # Sort by constraint_violation, then primary metric (Passivbot official)
                sorted_configs = sorted(
                    all_configs,
                    key=lambda c: (
                        c.constraint_violation,
                        -c.suite_metrics.get(primary_metric, float('-inf'))
                    )
                )
                for config in sorted_configs[:configs_per_criterion]:
                    if config.config_index not in selected_indices:
                        selected_configs.append(config)
                        selected_indices.add(config.config_index)
            
            elif criterion == 'robustness':
                # Sort by robustness score (low CV across scenarios)
                configs_with_robustness = [
                    (c, self.compute_overall_robustness(c)) for c in all_configs
                ]
                configs_with_robustness.sort(key=lambda x: (-x[1], x[0].constraint_violation))
                for config, _ in configs_with_robustness[:configs_per_criterion]:
                    if config.config_index not in selected_indices:
                        selected_configs.append(config)
                        selected_indices.add(config.config_index)
            
            elif criterion == 'sharpe':
                # Best Sharpe ratio
                sorted_configs = sorted(
                    all_configs,
                    key=lambda c: (
                        c.constraint_violation,
                        -c.suite_metrics.get('sharpe_ratio_usd', float('-inf'))
                    )
                )
                for config in sorted_configs[:configs_per_criterion]:
                    if config.config_index not in selected_indices:
                        selected_configs.append(config)
                        selected_indices.add(config.config_index)
            
            elif criterion == 'drawdown':
                # Lowest drawdown (higher is better, drawdown is negative)
                sorted_configs = sorted(
                    all_configs,
                    key=lambda c: (
                        c.constraint_violation,
                        -c.suite_metrics.get('drawdown_worst_usd', float('inf'))
                    )
                )
                for config in sorted_configs[:configs_per_criterion]:
                    if config.config_index not in selected_indices:
                        selected_configs.append(config)
                        selected_indices.add(config.config_index)
            
            elif criterion == 'calmar':
                # Best Calmar ratio
                sorted_configs = sorted(
                    all_configs,
                    key=lambda c: (
                        c.constraint_violation,
                        -c.suite_metrics.get('calmar_ratio_usd', float('-inf'))
                    )
                )
                for config in sorted_configs[:configs_per_criterion]:
                    if config.config_index not in selected_indices:
                        selected_configs.append(config)
                        selected_indices.add(config.config_index)
            
            elif criterion == 'sortino':
                # Best Sortino ratio
                sorted_configs = sorted(
                    all_configs,
                    key=lambda c: (
                        c.constraint_violation,
                        -c.suite_metrics.get('sortino_ratio_usd', float('-inf'))
                    )
                )
                for config in sorted_configs[:configs_per_criterion]:
                    if config.config_index not in selected_indices:
                        selected_configs.append(config)
                        selected_indices.add(config.config_index)
            
            elif criterion == 'omega':
                # Best Omega ratio
                sorted_configs = sorted(
                    all_configs,
                    key=lambda c: (
                        c.constraint_violation,
                        -c.suite_metrics.get('omega_ratio_usd', float('-inf'))
                    )
                )
                for config in sorted_configs[:configs_per_criterion]:
                    if config.config_index not in selected_indices:
                        selected_configs.append(config)
                        selected_indices.add(config.config_index)
            
            elif criterion == 'volatility':
                # Lowest volatility (stable returns)
                sorted_configs = sorted(
                    all_configs,
                    key=lambda c: (
                        c.constraint_violation,
                        c.suite_metrics.get('equity_volatility_usd', float('inf'))
                    )
                )
                for config in sorted_configs[:configs_per_criterion]:
                    if config.config_index not in selected_indices:
                        selected_configs.append(config)
                        selected_indices.add(config.config_index)
            
            elif criterion == 'recovery':
                # Fastest recovery from drawdowns
                sorted_configs = sorted(
                    all_configs,
                    key=lambda c: (
                        c.constraint_violation,
                        c.suite_metrics.get('drawdown_recovery_hours_mean', float('inf'))
                    )
                )
                for config in sorted_configs[:configs_per_criterion]:
                    if config.config_index not in selected_indices:
                        selected_configs.append(config)
                        selected_indices.add(config.config_index)
        
        # If we still need more configs to reach max_count, fill with best by performance
        if len(selected_configs) < max_count:
            remaining = max_count - len(selected_configs)
            sorted_configs = sorted(
                all_configs,
                key=lambda c: (
                    c.constraint_violation,
                    -c.suite_metrics.get(primary_metric, float('-inf'))
                )
            )
            for config in sorted_configs:
                if len(selected_configs) >= max_count:
                    break
                if config.config_index not in selected_indices:
                    selected_configs.append(config)
                    selected_indices.add(config.config_index)
        
        return selected_configs
    
    def load_pareto_jsons_only(self) -> bool:
        """
        Fast load: Load only Pareto configs from pareto/*.json files
        This is much faster than loading all_results.bin for large optimizations.
        
        Returns:
            True if successful, False otherwise
        """
        if not os.path.exists(self.pareto_dir):
            return False
        
        # Find all JSON files in pareto directory
        json_pattern = os.path.join(self.pareto_dir, "*.json")
        json_files = glob.glob(json_pattern)
        
        if not json_files:
            return False
        
        # Parse each JSON file
        parsed_count = 0
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)
                    
                    # Create ConfigMetrics from JSON
                    metrics = self._parse_json_config(config_data, parsed_count)
                    if metrics:
                        metrics.is_pareto = True  # All configs in pareto/ are Pareto-optimal
                        self.configs.append(metrics)
                        parsed_count += 1
            except Exception as e:
                if parsed_count < 3:  # Show first 3 errors
                    import traceback
                    print(f"Error parsing {json_file}:")
                    traceback.print_exc()
                continue
        
        if parsed_count == 0:
            return False
        
        # Extract scenario labels and scoring metrics from first config
        # NOTE: Use optimize.scoring, not all suite_metrics, for consistency with all_results.bin mode
        if self.configs:
            first_config = self.configs[0]
            if first_config.scenario_metrics:
                self.scenario_labels = list(first_config.scenario_metrics.keys())
            # scoring_metrics are already set by _parse_json_config from optimize.scoring
            # Fallback: if not set, use first few metrics from suite_metrics
            if not self.scoring_metrics and first_config.suite_metrics:
                # Use first 3 metrics as default
                self.scoring_metrics = list(first_config.suite_metrics.keys())[:3]
        
        # Store stats
        self.load_stats = {
            'total_parsed': parsed_count,
            'selected_configs': parsed_count,
            'pareto_configs': parsed_count,  # All are Pareto
            'scenarios': self.scenario_labels,
            'scoring_metrics': self.scoring_metrics,
            'load_strategy': ['pareto_only'],
            'max_configs': parsed_count
        }
        
        return True
    
    def _compute_pareto_front_fast(self):
        """
        Fast Pareto-optimal computation using sorting
        For single-objective (constraint_violation), just take configs with CV=0 or minimum CV
        """
        # Sort by constraint_violation (ascending - lower is better)
        sorted_configs = sorted(self.configs, key=lambda c: c.constraint_violation)
        
        # If we have multiple objectives in the future, use proper Pareto logic
        # For now: mark configs with minimum constraint_violation as Pareto
        if sorted_configs:
            min_cv = sorted_configs[0].constraint_violation
            
            # Mark all configs with CV within 1% of minimum as Pareto
            threshold = min_cv * 1.01
            for config in self.configs:
                if config.constraint_violation <= threshold:
                    config.is_pareto = True
    
    def _load_binary_file(self) -> List[Dict]:
        """Load msgpack binary file (handles multiple packed objects)"""
        configs = []
        
        with open(self.all_results_path, 'rb') as f:
            unpacker = msgpack.Unpacker(f, raw=False, strict_map_key=False)
            for obj in unpacker:
                configs.append(obj)
        
        return configs
    
    def _load_pareto_hashes(self):
        """Load config hashes from pareto/*.json files"""
        if not os.path.exists(self.pareto_dir):
            return
        
        for filename in os.listdir(self.pareto_dir):
            if filename.endswith('.json'):
                config_hash = filename.replace('.json', '')
                self.pareto_hashes.add(config_hash)
    
    def _compute_config_hash(self, config_data: Dict) -> str:
        """
        Compute config hash (simplified - uses results_filename if available)
        In real implementation, would hash the bot parameters
        """
        # Try to extract hash from results_filename or results_dir
        results_dir = config_data.get('results_dir', '')
        if results_dir:
            return os.path.basename(results_dir)
        return f"config_{hash(str(config_data.get('bot', {})))}"
    
    def get_full_config(self, config_index: int) -> Optional[Dict]:
        """
        Get full config for a specific index by merging Pareto JSON + all_results.bin
        
        Strategy:
        1. Load any Pareto JSON (they all have same base config structure)
        2. Load optimized bot params from all_results.bin at config_index
        3. Merge bot params into pareto config
        
        Args:
            config_index: Index of config in all_results.bin
            
        Returns:
            Complete config dict with all sections (backtest, bot, optimize, live, etc.)
            or None if not found
        """
        # Check cache first
        if config_index in self.pareto_configs_cache:
            return self.pareto_configs_cache[config_index]
        
        try:
            import json
            
            # 1. Load ANY pareto JSON as template (they share same base config)
            pareto_files = [f for f in os.listdir(self.pareto_dir) if f.endswith('.json')]
            if not pareto_files:
                return None
            
            template_file = os.path.join(self.pareto_dir, pareto_files[0])
            with open(template_file, 'r') as f:
                full_config = json.load(f)
            
            # 2. Load optimized bot params from all_results.bin at config_index
            with open(self.all_results_path, 'rb') as f:
                unpacker = msgpack.Unpacker(f, raw=False, max_buffer_size=2**31-1)
                for idx, config_data in enumerate(unpacker):
                    if idx == config_index:
                        # 3. Merge optimized bot params (keep missing params from pareto template)
                        if 'bot' in config_data:
                            # Merge each side (long/short) individually
                            for side in ['long', 'short']:
                                if side in config_data['bot']:
                                    # Update only the parameters that exist in all_results.bin
                                    if side in full_config['bot']:
                                        full_config['bot'][side].update(config_data['bot'][side])
                                    else:
                                        full_config['bot'][side] = config_data['bot'][side]
                                # If side missing in all_results.bin, keep pareto template values
                        
                        # Cache and return
                        self.pareto_configs_cache[config_index] = full_config
                        return full_config
            
            return None
            
        except Exception as e:
            import traceback
            print(f"Error loading full config for index {config_index}: {e}")
            traceback.print_exc()
            return None
    
    def _parse_config(self, idx: int, config_data: Dict) -> ConfigMetrics:
        """Parse a single config from all_results.bin"""
        
        # Extract objectives
        metrics_block = config_data.get('metrics', {})
        objectives = metrics_block.get('objectives', {})
        constraint_violation = metrics_block.get('constraint_violation', 0.0)
        
        # Extract suite metrics - support both suite and non-suite formats
        # Suite format: {"suite_metrics": {"metrics": {...}}}
        # Non-suite format: {"metrics": {"stats": {...}, "objectives": {...}}}
        if 'suite_metrics' in config_data:
            suite_metrics_block = config_data.get('suite_metrics', {})
            metrics_dict = suite_metrics_block.get('metrics', {})
            # Store scenario labels (first time only)
            if not self.scenario_labels:
                self.scenario_labels = suite_metrics_block.get('scenario_labels', [])
        else:
            # Non-suite format: metrics.stats contains the actual metrics
            metrics_block = config_data.get('metrics', {})
            metrics_dict = metrics_block.get('stats', {})
        
        # Extract scoring metrics from optimize config (first time only)
        if not self.scoring_metrics and 'optimize' in config_data:
            optimize_config = config_data['optimize']
            self.scoring_metrics = optimize_config.get('scoring', [])
            
            # Extract global optimize settings (first time only)
            if not self.optimize_bounds:
                self.optimize_bounds = optimize_config.get('bounds', {})
                self.optimize_limits = optimize_config.get('limits', [])
        
        # Extract backtest scenarios (first time only)
        if not self.backtest_scenarios and 'backtest' in config_data:
            backtest_config = config_data['backtest']
            suite_config = backtest_config.get('suite', {})
            if suite_config.get('enabled'):
                self.backtest_scenarios = suite_config.get('scenarios', [])
        
        # Parse aggregated suite metrics
        suite_metrics = {}
        scenario_metrics = {}
        robustness_scores = {}
        metric_stats = {}
        
        for metric_name, metric_data in metrics_dict.items():
            if isinstance(metric_data, dict):
                # Try to get aggregated value, fallback to mean if not present
                aggregated = metric_data.get('aggregated', metric_data.get('mean', 0.0))
                suite_metrics[metric_name] = aggregated
                
                # Stats - either from nested 'stats' dict or direct dict values
                if 'stats' in metric_data:
                    stats = metric_data.get('stats', {})
                    std = stats.get('std', 0.0)
                    mean = stats.get('mean', 0.0)
                    min_val = stats.get('min', 0.0)
                    max_val = stats.get('max', 0.0)
                else:
                    # Direct format: {"mean": X, "std": Y, ...}
                    std = metric_data.get('std', 0.0)
                    mean = metric_data.get('mean', 0.0)
                    min_val = metric_data.get('min', 0.0)
                    max_val = metric_data.get('max', 0.0)
                
                # Store full stats
                metric_stats[metric_name] = {
                    'mean': mean,
                    'std': std,
                    'min': min_val,
                    'max': max_val
                }
                
                # Robustness score = 1 / CV (Coefficient of Variation)
                # Lower CV = more robust (consistent across scenarios)
                if abs(mean) > 1e-10:
                    cv = abs(std / mean)
                    robustness_scores[metric_name] = 1.0 / (1.0 + cv)  # Normalize to [0, 1]
                else:
                    robustness_scores[metric_name] = 1.0
                
                # Scenario-specific values
                scenarios = metric_data.get('scenarios', {})
                for scenario_label, scenario_value in scenarios.items():
                    if scenario_label not in scenario_metrics:
                        scenario_metrics[scenario_label] = {}
                    scenario_metrics[scenario_label][metric_name] = scenario_value
        
        # Extract bot parameters
        bot_params = {}
        bot_config = config_data.get('bot', {})
        if 'long' in bot_config:
            for param, value in bot_config['long'].items():
                bot_params[f'long_{param}'] = value
        if 'short' in bot_config:
            for param, value in bot_config['short'].items():
                bot_params[f'short_{param}'] = value
        
        # Extract optimize settings for this config
        optimize_settings = {}
        if 'optimize' in config_data:
            opt = config_data['optimize']
            optimize_settings = {
                'scoring': opt.get('scoring', []),
                'limits': opt.get('limits', []),
                'iters': opt.get('iters', 0),
                'population_size': opt.get('population_size', 0),
                'pareto_max_size': opt.get('pareto_max_size', 0),
            }
        
        # Extract scenario details
        scenario_details = None
        if 'backtest' in config_data:
            suite_config = config_data['backtest'].get('suite', {})
            if suite_config.get('enabled'):
                scenario_details = suite_config.get('scenarios', [])
        
        # Compute config hash
        config_hash = self._compute_config_hash(config_data)
        is_pareto = config_hash in self.pareto_hashes
        
        return ConfigMetrics(
            config_index=idx,
            config_hash=config_hash,
            objectives=objectives,
            constraint_violation=constraint_violation,
            suite_metrics=suite_metrics,
            scenario_metrics=scenario_metrics,
            robustness_scores=robustness_scores,
            bot_params=bot_params,
            bounds=self.optimize_bounds,
            optimize_settings=optimize_settings,
            scenario_details=scenario_details,
            metric_stats=metric_stats,
            is_pareto=is_pareto
        )
    
    def _parse_json_config(self, config_data: Dict, idx: int) -> Optional[ConfigMetrics]:
        """
        Parse a single config from pareto/*.json file
        Similar to _parse_config but adapted for JSON format
        """
        try:
            # Extract objectives
            metrics_block = config_data.get('metrics', {})
            objectives = metrics_block.get('objectives', {})
            constraint_violation = metrics_block.get('constraint_violation', 0.0)
            
            # Extract suite metrics - support both suite and non-suite formats
            if 'suite_metrics' in config_data:
                suite_metrics_block = config_data.get('suite_metrics', {})
                metrics_dict = suite_metrics_block.get('metrics', {})
                # Store scenario labels (first time only)
                if not self.scenario_labels:
                    self.scenario_labels = suite_metrics_block.get('scenario_labels', [])
            else:
                # Non-suite format
                metrics_dict = metrics_block.get('stats', {})
            
            # Extract scoring metrics (first time only)
            if not self.scoring_metrics and 'optimize' in config_data:
                optimize_config = config_data['optimize']
                self.scoring_metrics = optimize_config.get('scoring', [])
                
                # Extract global optimize settings
                if not self.optimize_bounds:
                    self.optimize_bounds = optimize_config.get('bounds', {})
                    self.optimize_limits = optimize_config.get('limits', [])
            
            # Extract backtest scenarios (first time only)
            if not self.backtest_scenarios and 'backtest' in config_data:
                backtest_config = config_data['backtest']
                suite_config = backtest_config.get('suite', {})
                if suite_config.get('enabled'):
                    self.backtest_scenarios = suite_config.get('scenarios', [])
            
            # Parse metrics (same logic as _parse_config)
            suite_metrics = {}
            scenario_metrics = {}
            robustness_scores = {}
            metric_stats = {}
            
            for metric_name, metric_data in metrics_dict.items():
                if isinstance(metric_data, dict):
                    aggregated = metric_data.get('aggregated', metric_data.get('mean', 0.0))
                    suite_metrics[metric_name] = aggregated
                    
                    # Stats
                    if 'stats' in metric_data:
                        stats = metric_data.get('stats', {})
                        std = stats.get('std', 0.0)
                        mean = stats.get('mean', 0.0)
                        min_val = stats.get('min', 0.0)
                        max_val = stats.get('max', 0.0)
                    else:
                        # Fallback: use direct dict values
                        std = metric_data.get('std', 0.0)
                        mean = metric_data.get('mean', aggregated)
                        min_val = metric_data.get('min', aggregated)
                        max_val = metric_data.get('max', aggregated)
                    
                    metric_stats[metric_name] = {
                        'mean': mean,
                        'std': std,
                        'min': min_val,
                        'max': max_val
                    }
                    
                    # Robustness score: 1/(1+CV) normalized to [0,1]
                    # Higher score = more consistent across scenarios
                    if std > 0 and abs(mean) > 1e-10:
                        cv = abs(std / mean)
                        robustness_scores[metric_name] = 1.0 / (1.0 + cv)
                    else:
                        robustness_scores[metric_name] = 1.0  # Perfect consistency
                        robustness_scores[metric_name] = 1.0  # Perfect consistency
                    
                    # Scenario-specific metrics
                    scenarios = metric_data.get('scenarios', {})
                    for scenario_name, scenario_value in scenarios.items():
                        if scenario_name not in scenario_metrics:
                            scenario_metrics[scenario_name] = {}
                        scenario_metrics[scenario_name][metric_name] = scenario_value
                else:
                    # Simple value
                    suite_metrics[metric_name] = metric_data
            
            # Extract bot parameters
            bot_params = {}
            bot_config = config_data.get('bot', {})
            if 'long' in bot_config:
                for param, value in bot_config['long'].items():
                    bot_params[f'long_{param}'] = value
            if 'short' in bot_config:
                for param, value in bot_config['short'].items():
                    bot_params[f'short_{param}'] = value
            
            # Extract optimize settings
            optimize_settings = {}
            if 'optimize' in config_data:
                opt = config_data['optimize']
                optimize_settings = {
                    'scoring': opt.get('scoring', []),
                    'limits': opt.get('limits', []),
                    'iters': opt.get('iters', 0),
                    'population_size': opt.get('population_size', 0),
                    'pareto_max_size': opt.get('pareto_max_size', 0),
                }
            
            # Extract scenario details
            scenario_details = None
            if 'backtest' in config_data:
                suite_config = config_data['backtest'].get('suite', {})
                if suite_config.get('enabled'):
                    scenario_details = suite_config.get('scenarios', [])
            
            # Compute config hash
            config_hash = self._compute_config_hash(config_data)
            
            return ConfigMetrics(
                config_index=idx,
                config_hash=config_hash,
                objectives=objectives,
                constraint_violation=constraint_violation,
                suite_metrics=suite_metrics,
                scenario_metrics=scenario_metrics,
                robustness_scores=robustness_scores,
                bot_params=bot_params,
                bounds=self.optimize_bounds,
                optimize_settings=optimize_settings,
                scenario_details=scenario_details,
                metric_stats=metric_stats,
                is_pareto=True  # All JSON configs are Pareto
            )
        
        except Exception as e:
            import traceback
            print(f"Error parsing JSON config at index {idx}:")
            traceback.print_exc()
            return None
    
    def get_pareto_configs(self) -> List[ConfigMetrics]:
        """Get only Pareto-optimal configs"""
        return [c for c in self.configs if c.is_pareto]
    
    def get_all_configs(self) -> List[ConfigMetrics]:
        """Get all configs"""
        return self.configs
    
    def to_dataframe(self, pareto_only: bool = False) -> pd.DataFrame:
        """
        Convert configs to pandas DataFrame for easy analysis
        
        Args:
            pareto_only: If True, only return Pareto configs
        
        Returns:
            DataFrame with all metrics and parameters
        """
        configs = self.get_pareto_configs() if pareto_only else self.get_all_configs()
        
        if not configs:
            return pd.DataFrame()
        
        rows = []
        for config in configs:
            row = {
                'config_index': config.config_index,
                'config_hash': config.config_hash,
                'is_pareto': config.is_pareto,
                'constraint_violation': config.constraint_violation,
            }
            
            # Add objectives
            for key, value in config.objectives.items():
                row[f'obj_{key}'] = value
            
            # Add suite metrics
            row.update(config.suite_metrics)
            
            # Add robustness scores
            for metric, score in config.robustness_scores.items():
                row[f'robust_{metric}'] = score
            
            # Add bot parameters
            row.update(config.bot_params)
            
            # Add scenario-specific metrics (optional)
            for scenario, metrics in config.scenario_metrics.items():
                for metric, value in metrics.items():
                    row[f'scenario_{scenario}_{metric}'] = value
            
            rows.append(row)
        
        return pd.DataFrame(rows)
    
    def get_metric_summary(self, metric_name: str) -> Dict[str, float]:
        """
        Get summary statistics for a specific metric across all configs
        
        Args:
            metric_name: Name of the metric
        
        Returns:
            Dict with min, max, mean, std, median
        """
        values = [c.suite_metrics.get(metric_name, 0.0) for c in self.configs]
        values = [v for v in values if v is not None]
        
        if not values:
            return {}
        
        return {
            'min': float(np.min(values)),
            'max': float(np.max(values)),
            'mean': float(np.mean(values)),
            'std': float(np.std(values)),
            'median': float(np.median(values)),
            'count': len(values)
        }
    
    def get_top_configs(self, metric_name: str, n: int = 10, 
                       ascending: bool = False, pareto_only: bool = False) -> List[ConfigMetrics]:
        """
        Get top N configs sorted by a specific metric
        
        Args:
            metric_name: Metric to sort by
            n: Number of configs to return
            ascending: If True, sort ascending (lower is better)
            pareto_only: If True, only consider Pareto configs
        
        Returns:
            List of top N configs
        """
        configs = self.get_pareto_configs() if pareto_only else self.get_all_configs()
        
        # Sort by metric
        sorted_configs = sorted(
            configs,
            key=lambda c: c.suite_metrics.get(metric_name, float('-inf') if ascending else float('inf')),
            reverse=not ascending
        )
        
        return sorted_configs[:n]
    
    def compute_overall_robustness(self, config: ConfigMetrics) -> float:
        """
        Compute overall robustness score for a config
        Average of all individual robustness scores
        
        Args:
            config: ConfigMetrics instance
        
        Returns:
            Overall robustness score [0, 1] (higher is more robust)
        """
        if not config.robustness_scores:
            return 0.0
        
        scores = list(config.robustness_scores.values())
        return float(np.mean(scores))
    
    def find_similar_configs(self, reference_config: ConfigMetrics, 
                            n: int = 5, 
                            param_subset: Optional[List[str]] = None) -> List[Tuple[ConfigMetrics, float]]:
        """
        Find configs similar to a reference config based on parameter distance
        
        Args:
            reference_config: Reference config to compare against
            n: Number of similar configs to return
            param_subset: Optional list of parameters to consider (default: all)
        
        Returns:
            List of (config, distance) tuples, sorted by distance
        """
        if param_subset is None:
            param_subset = list(reference_config.bot_params.keys())
        
        distances = []
        for config in self.configs:
            if config.config_index == reference_config.config_index:
                continue
            
            # Compute euclidean distance in parameter space
            dist = 0.0
            for param in param_subset:
                ref_val = reference_config.bot_params.get(param, 0.0)
                cfg_val = config.bot_params.get(param, 0.0)
                dist += (ref_val - cfg_val) ** 2
            
            dist = np.sqrt(dist)
            distances.append((config, dist))
        
        # Sort by distance and return top N
        distances.sort(key=lambda x: x[1])
        return distances[:n]
    
    def get_parameters_at_bounds(self, tolerance: float = 0.05) -> Dict[str, List[str]]:
        """
        Find parameters that are at or near bounds limits
        
        Args:
            tolerance: How close to bounds (0.05 = within 5% of limit)
        
        Returns:
            Dict with 'at_lower', 'at_upper', 'within_range' lists
        """
        at_lower = []
        at_upper = []
        within_range = []
        
        for param_name, (lower, upper) in self.optimize_bounds.items():
            param_range = upper - lower
            threshold_lower = lower + (param_range * tolerance)
            threshold_upper = upper - (param_range * tolerance)
            
            # Check Pareto configs
            pareto_configs = self.get_pareto_configs()
            values = [c.bot_params.get(param_name, 0) for c in pareto_configs if param_name in c.bot_params]
            
            if not values:
                continue
            
            min_val = min(values)
            max_val = max(values)
            
            if min_val <= threshold_lower:
                at_lower.append(param_name)
            elif max_val >= threshold_upper:
                at_upper.append(param_name)
            else:
                within_range.append(param_name)
        
        return {
            'at_lower': at_lower,
            'at_upper': at_upper,
            'within_range': within_range
        }
    
    def get_special_metrics(self) -> Dict[str, List[str]]:
        """
        Categorize metrics by type for easier visualization
        
        Returns:
            Dict with categorized metric lists
        """
        all_metrics = list(self.configs[0].suite_metrics.keys()) if self.configs else []
        
        categories = {
            'performance': [],
            'risk': [],
            'quality': [],
            'activity': [],
            'risk_adjusted': []
        }
        
        for metric in all_metrics:
            metric_lower = metric.lower()
            
            # Performance metrics
            if any(x in metric_lower for x in ['adg', 'gain', 'profit']):
                categories['performance'].append(metric)
            
            # Risk metrics
            elif any(x in metric_lower for x in ['drawdown', 'loss', 'shortfall', 'equity_balance_diff_neg']):
                categories['risk'].append(metric)
            
            # Quality metrics (equity curve quality)
            elif any(x in metric_lower for x in ['choppiness', 'jerkiness', 'exponential_fit']):
                categories['quality'].append(metric)
            
            # Activity metrics
            elif any(x in metric_lower for x in ['position', 'volume', 'held', 'recovery_hours']):
                categories['activity'].append(metric)
            
            # Risk-adjusted metrics
            elif any(x in metric_lower for x in ['sharpe', 'sortino', 'calmar', 'omega', 'sterling']):
                categories['risk_adjusted'].append(metric)
        
        return categories
    
    def compute_trading_style(self, config: ConfigMetrics) -> str:
        """
        Classify config into trading style based on activity metrics
        
        Args:
            config: ConfigMetrics instance
        
        Returns:
            Style label: "Sniper", "Scalper", "Balanced", "Idle"
        """
        positions_per_day = config.suite_metrics.get('positions_held_per_day', 0)
        holding_hours_mean = config.suite_metrics.get('position_held_hours_mean', 0)
        volume_pct = config.suite_metrics.get('volume_pct_per_day_avg', 0)
        
        # Classification logic
        if positions_per_day < 0.5 and holding_hours_mean > 20:
            return "🎯 Sniper (Low frequency, long holds)"
        elif positions_per_day > 2 and holding_hours_mean < 5:
            return "⚡ Scalper (High frequency, quick trades)"
        elif volume_pct < 0.2:
            return "💤 Idle (Very low activity)"
        else:
            return "⚖️ Balanced (Moderate trading)"
    
    def compute_risk_profile_score(self, config: ConfigMetrics) -> Dict[str, float]:
        """
        Compute multi-dimensional risk profile
        
        Args:
            config: ConfigMetrics instance
        
        Returns:
            Dict with risk scores [0-10] for different dimensions
        """
        # Extract key risk metrics (normalize to 0-10 scale)
        dd = config.suite_metrics.get('drawdown_worst_usd', 0)
        choppiness = config.suite_metrics.get('equity_choppiness_usd', 0)
        jerkiness = config.suite_metrics.get('equity_jerkiness_usd', 0)
        shortfall = config.suite_metrics.get('expected_shortfall_1pct_usd', 0)
        loss_profit = config.suite_metrics.get('loss_profit_ratio', 0)
        
        # Normalize (lower risk = higher score)
        dd_score = max(0, 10 - (dd / 0.01))  # 0.1 DD = score 0
        chop_score = max(0, 10 - choppiness)
        jerk_score = max(0, 10 - (jerkiness / 0.001))
        shortfall_score = max(0, 10 - (shortfall / 0.01))
        lpr_score = max(0, 10 - (loss_profit / 0.05))
        
        return {
            'drawdown': min(10, dd_score),
            'choppiness': min(10, chop_score),
            'jerkiness': min(10, jerk_score),
            'tail_risk': min(10, shortfall_score),
            'loss_magnitude': min(10, lpr_score),
            'overall': np.mean([dd_score, chop_score, jerk_score, shortfall_score, lpr_score])
        }
