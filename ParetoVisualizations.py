"""
ParetoVisualizations - All Plotly visualization functions for the Pareto Explorer
Provides interactive charts for multi-dimensional config analysis
"""

import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple
from ParetoDataLoader import ConfigMetrics, ParetoDataLoader


class ParetoVisualizations:
    """Creates all visualizations for Pareto analysis"""
    
    def __init__(self, loader: ParetoDataLoader):
        """
        Initialize with loaded data
        
        Args:
            loader: ParetoDataLoader instance with loaded configs
        """
        self.loader = loader
        self.color_palette = px.colors.qualitative.Set3
        
    def plot_pareto_scatter_2d(self, 
                               x_metric: str, 
                               y_metric: str, 
                               color_metric: Optional[str] = None,
                               size_metric: Optional[str] = None,
                               show_all: bool = False,
                               best_match_config = None,
                               title_prefix: str = "Pareto Analysis") -> go.Figure:
        """
        2D scatter plot with Pareto front highlighted
        
        Args:
            x_metric: Metric for x-axis
            y_metric: Metric for y-axis
            color_metric: Optional metric for color coding
            size_metric: Optional metric for marker size
            show_all: If True, show all configs; if False, only Pareto
            best_match_config: Optional ConfigMetrics to highlight as best match
            title_prefix: Prefix for plot title (e.g., "Profit vs Risk")
        
        Returns:
            Plotly figure
        """
        if show_all:
            # Show all loaded configs (respects display range filter)
            df = pd.DataFrame([{
                'config_index': c.config_index,
                'is_pareto': c.is_pareto,
                **c.suite_metrics
            } for c in self.loader.configs])
        else:
            df = self.loader.to_dataframe(pareto_only=True)
        
        if df.empty:
            return go.Figure().add_annotation(text="No data available", showarrow=False)
        
        # Check if required metrics exist in DataFrame
        import streamlit as st
        missing_metrics = []
        for metric in [x_metric, y_metric, color_metric, size_metric]:
            if metric and metric not in df.columns:
                missing_metrics.append(metric)
        
        if missing_metrics:
            st.error(f"❌ Metrics not found: {', '.join(missing_metrics)}")
            st.info(f"Available metrics: {', '.join([c for c in df.columns if c not in ['config_index', 'is_pareto']])}")
            return go.Figure().add_annotation(
                text=f"Metrics not available: {', '.join(missing_metrics)}", 
                showarrow=False
            )
        
        # Prepare data
        hover_data = ['config_index', x_metric, y_metric]
        if color_metric:
            hover_data.append(color_metric)
        if size_metric:
            hover_data.append(size_metric)
        
        # Create figure
        fig = px.scatter(
            df,
            x=x_metric,
            y=y_metric,
            color=color_metric if color_metric else 'is_pareto',
            size=size_metric if size_metric else None,
            hover_data=hover_data,
            title=f"{title_prefix}: {x_metric} vs {y_metric}",
            labels={
                x_metric: x_metric.replace('_', ' ').title(),
                y_metric: y_metric.replace('_', ' ').title()
            },
            color_continuous_scale='Viridis' if color_metric else None,
            opacity=0.7,
            custom_data=['config_index']  # Add config_index to all points
        )
        
        # Highlight Pareto configs (smaller stars)
        if show_all:
            pareto_df = df[df['is_pareto'] == True]
            fig.add_trace(go.Scatter(
                x=pareto_df[x_metric],
                y=pareto_df[y_metric],
                mode='markers',
                marker=dict(size=7, color='red', symbol='star', line=dict(width=1, color='white')),
                name='Pareto Front',
                hovertemplate='<b>Pareto Config #%{customdata[0]}</b><br>%{x:.6f}, %{y:.6f}<extra></extra>',
                customdata=pareto_df[['config_index']].values
            ))
        
        # Highlight Best Match config
        if best_match_config is not None:
            best_match_df = df[df['config_index'] == best_match_config.config_index]
            if not best_match_df.empty:
                fig.add_trace(go.Scatter(
                    x=best_match_df[x_metric],
                    y=best_match_df[y_metric],
                    mode='markers',
                    marker=dict(size=20, color='lime', symbol='star', line=dict(width=3, color='darkgreen')),
                    name='🎯 Best Match',
                    hovertemplate='<b>🎯 Best Match Config #%{customdata[0]}</b><br>%{x:.6f}, %{y:.6f}<extra></extra>',
                    customdata=best_match_df[['config_index']].values
                ))
        
        # Update hover template for all points to show config_index
        fig.update_traces(
            hovertemplate='<b>Config #%{customdata[0]}</b><br>' + 
                         f'{x_metric}: %{{x:.6f}}<br>{y_metric}: %{{y:.6f}}<extra></extra>',
            selector=dict(mode='markers')
        )
        
        # Add annotation showing point counts
        pareto_count = len(df[df['is_pareto'] == True])
        total_count = len(df)
        
        fig.update_layout(
            height=600,
            hovermode='closest',
            template='plotly_white',
            dragmode='zoom',  # Default to zoom instead of select
            modebar_remove=['select2d', 'lasso2d']  # Remove box/lasso select tools
        )
        
        return fig
    
    def plot_pareto_scatter_3d(self,
                               x_metric: str,
                               y_metric: str,
                               z_metric: str,
                               color_metric: Optional[str] = None,
                               show_all: bool = False,
                               best_match_config = None,
                               configs_list = None) -> go.Figure:
        """
        Interactive 3D scatter plot
        
        Args:
            x_metric: Metric for x-axis
            y_metric: Metric for y-axis
            z_metric: Metric for z-axis
            color_metric: Optional metric for color coding
            show_all: If True, show all configs; if False, only Pareto
            best_match_config: Optional ConfigMetrics to highlight as best match
            configs_list: Optional list of configs to use (for consistent ordering)
        
        Returns:
            Plotly figure
        """
        # Use provided configs_list for consistent ordering, or fall back to loader
        if configs_list is not None:
            configs_to_use = configs_list
        elif show_all:
            configs_to_use = self.loader.configs
        else:
            configs_to_use = self.loader.get_pareto_configs()
        
        df = pd.DataFrame([{
            'config_index': c.config_index,
            'is_pareto': c.is_pareto,
            **c.suite_metrics
        } for c in configs_to_use])
        
        if df.empty:
            return go.Figure().add_annotation(text="No data available", showarrow=False)
        
        # Separate Pareto and non-Pareto
        pareto_df = df[df['is_pareto'] == True]
        non_pareto_df = df[df['is_pareto'] == False]
        
        fig = go.Figure()
        
        # Add non-Pareto configs (if show_all)
        if show_all and not non_pareto_df.empty:
            color_values = non_pareto_df[color_metric] if color_metric else None
            fig.add_trace(go.Scatter3d(
                x=non_pareto_df[x_metric],
                y=non_pareto_df[y_metric],
                z=non_pareto_df[z_metric],
                mode='markers',
                marker=dict(
                    size=6,
                    color=color_values,
                    colorscale='Viridis',
                    opacity=0.4,
                    colorbar=dict(title=color_metric) if color_metric else None
                ),
                name='All Configs',
                hovertemplate=f'<b>Config %{{customdata[0]}}</b><br>{x_metric}: %{{x:.6f}}<br>{y_metric}: %{{y:.6f}}<br>{z_metric}: %{{z:.6f}}<extra></extra>',
                customdata=non_pareto_df[['config_index']].values
            ))
        
        # Add Pareto configs
        if not pareto_df.empty:
            color_values = pareto_df[color_metric] if color_metric else None
            fig.add_trace(go.Scatter3d(
                x=pareto_df[x_metric],
                y=pareto_df[y_metric],
                z=pareto_df[z_metric],
                mode='markers',
                marker=dict(
                    size=10,
                    color=color_values if color_metric else 'red',
                    colorscale='Viridis' if color_metric else None,
                    symbol='circle',  # Changed from 'diamond' to 'circle' for better 3D visualization
                    line=dict(width=2, color='white'),
                    opacity=0.9
                ),
                name='Pareto Front',
                hovertemplate=f'<b>Pareto Config %{{customdata[0]}}</b><br>{x_metric}: %{{x:.6f}}<br>{y_metric}: %{{y:.6f}}<br>{z_metric}: %{{z:.6f}}<extra></extra>',
                customdata=pareto_df[['config_index']].values
            ))
        
        # Highlight Best Match config
        if best_match_config is not None:
            best_match_df = df[df['config_index'] == best_match_config.config_index]
            if not best_match_df.empty:
                fig.add_trace(go.Scatter3d(
                    x=best_match_df[x_metric],
                    y=best_match_df[y_metric],
                    z=best_match_df[z_metric],
                    mode='markers',
                    marker=dict(
                        size=18,
                        color='lime',
                        symbol='circle',  # Changed from 'diamond' for consistency
                        line=dict(width=4, color='darkgreen'),
                        opacity=1.0
                    ),
                    name='🎯 Best Match',
                    hovertemplate=f'<b>🎯 Best Match Config %{{customdata[0]}}</b><br>{x_metric}: %{{x:.6f}}<br>{y_metric}: %{{y:.6f}}<br>{z_metric}: %{{z:.6f}}<extra></extra>',
                    customdata=best_match_df[['config_index']].values
                ))
        
        fig.update_layout(
            title=f"3D Pareto Space: {x_metric} vs {y_metric} vs {z_metric}",
            scene=dict(
                xaxis_title=x_metric.replace('_', ' ').title(),
                yaxis_title=y_metric.replace('_', ' ').title(),
                zaxis_title=z_metric.replace('_', ' ').title()
            ),
            height=1080,
            template='plotly_white',
            clickmode='event+select',  # Enable click events
            dragmode='orbit'  # Set default drag mode for better 3D interaction
        )
        
        return fig
    
    def plot_parameter_influence_heatmap(self,
                                         params: Optional[List[str]] = None,
                                         metrics: Optional[List[str]] = None,
                                         top_n: int = 20) -> go.Figure:
        """
        Correlation heatmap: parameters vs metrics
        
        Args:
            params: List of parameters to analyze (default: top 20 most variable)
            metrics: List of metrics to analyze (default: scoring metrics)
            top_n: Number of top parameters to show if not specified
        
        Returns:
            Plotly figure
        """
        df = self.loader.to_dataframe(pareto_only=False)
        
        if df.empty:
            return go.Figure().add_annotation(text="No data available", showarrow=False)
        
        # Get parameter columns
        param_cols = [col for col in df.columns if col.startswith('long_') or col.startswith('short_')]
        
        # Select most variable parameters if not specified
        if params is None:
            param_variance = df[param_cols].var().sort_values(ascending=False)
            params = param_variance.head(top_n).index.tolist()
        
        # Use scoring metrics if not specified
        if metrics is None:
            metrics = self.loader.scoring_metrics
            if not metrics:
                metrics = ['adg_w_usd', 'sharpe_ratio_usd', 'gain_usd', 'drawdown_worst_usd']
        
        # Filter available metrics
        metrics = [m for m in metrics if m in df.columns]
        params = [p for p in params if p in df.columns]
        
        if not params or not metrics:
            return go.Figure().add_annotation(text="Insufficient data for correlation", showarrow=False)
        
        # Compute correlation matrix
        corr_matrix = df[params + metrics].corr().loc[params, metrics]
        
        # Create heatmap
        fig = go.Figure(data=go.Heatmap(
            z=corr_matrix.values,
            x=[m.replace('_', ' ').title() for m in metrics],
            y=[p.replace('_', ' ').replace('long ', '').replace('short ', '') for p in params],
            colorscale='RdBu',
            zmid=0,
            text=np.round(corr_matrix.values, 2),
            texttemplate='%{text}',
            textfont={"size": 10},
            colorbar=dict(title="Correlation")
        ))
        
        fig.update_layout(
            title="Parameter Influence Heatmap (Correlation Analysis)",
            xaxis_title="Metrics",
            yaxis_title="Parameters",
            height=max(400, len(params) * 25),
            template='plotly_white'
        )
        
        return fig
    
    def plot_scenario_comparison_boxplots(self, metric: str) -> go.Figure:
        """
        Box plots comparing metric across scenarios
        
        Args:
            metric: Metric to compare
        
        Returns:
            Plotly figure
        """
        configs = self.loader.get_pareto_configs()
        
        if not configs or not self.loader.scenario_labels:
            return go.Figure().add_annotation(text="No scenario data available", showarrow=False)
        
        fig = go.Figure()
        
        for scenario in self.loader.scenario_labels:
            values = [c.scenario_metrics.get(scenario, {}).get(metric, np.nan) 
                     for c in configs if scenario in c.scenario_metrics]
            values = [v for v in values if not np.isnan(v)]
            
            if values:
                fig.add_trace(go.Box(
                    y=values,
                    name=scenario.title(),
                    boxmean='sd'
                ))
        
        fig.update_layout(
            title=f"Scenario Comparison: {metric.replace('_', ' ').title()}",
            yaxis_title=metric.replace('_', ' ').title(),
            xaxis_title="Scenario",
            height=500,
            template='plotly_white',
            showlegend=False
        )
        
        return fig
    
    def plot_risk_profile_radar(self, 
                                config_indices: List[int],
                                config_labels: Optional[List[str]] = None) -> go.Figure:
        """
        Radar chart comparing risk profiles of multiple configs
        
        Args:
            config_indices: List of config indices to compare
            config_labels: Optional labels for configs
        
        Returns:
            Plotly figure
        """
        if not config_indices:
            return go.Figure().add_annotation(text="No configs selected", showarrow=False)
        
        configs = [self.loader.configs[i] for i in config_indices if i < len(self.loader.configs)]
        
        if not configs:
            return go.Figure().add_annotation(text="Invalid config indices", showarrow=False)
        
        # Risk dimensions
        risk_dimensions = ['Drawdown', 'Choppiness', 'Jerkiness', 'Tail Risk', 'Loss Magnitude']
        
        fig = go.Figure()
        
        for i, config in enumerate(configs):
            risk_scores = self.loader.compute_risk_profile_score(config)
            values = [
                risk_scores['drawdown'],
                risk_scores['choppiness'],
                risk_scores['jerkiness'],
                risk_scores['tail_risk'],
                risk_scores['loss_magnitude']
            ]
            values.append(values[0])  # Close the polygon
            
            label = config_labels[i] if config_labels and i < len(config_labels) else f"Config #{config.config_index}"
            
            fig.add_trace(go.Scatterpolar(
                r=values,
                theta=risk_dimensions + [risk_dimensions[0]],
                fill='toself',
                name=label,
                opacity=0.6
            ))
        
        fig.update_layout(
            polar=dict(
                radialaxis=dict(
                    visible=True,
                    range=[0, 10]
                )
            ),
            title="Risk Profile Comparison (Higher = Lower Risk)",
            height=600,
            template='plotly_white'
        )
        
        return fig
    
    def plot_robustness_quadrant(self, 
                                 performance_metric: str = 'adg_w_usd',
                                 show_all: bool = False) -> go.Figure:
        """
        2D plot: Performance vs Robustness
        
        Args:
            performance_metric: Metric to use for performance axis
            show_all: Show all configs or only Pareto
        
        Returns:
            Plotly figure
        """
        # Use loader.configs directly (respects view filtering)
        configs = self.loader.configs if show_all else self.loader.get_pareto_configs()
        
        if not configs:
            return go.Figure().add_annotation(text="No data available", showarrow=False)
        
        performance = []
        robustness = []
        labels = []
        colors = []
        config_indices = []
        
        for config in configs:
            perf = config.suite_metrics.get(performance_metric, 0)
            robust = self.loader.compute_overall_robustness(config)
            
            performance.append(perf)
            robustness.append(robust)
            labels.append(f"Config #{config.config_index}")
            colors.append('red' if config.is_pareto else 'blue')
            config_indices.append(config.config_index)
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=performance,
            y=robustness,
            mode='markers',
            marker=dict(
                size=10,
                color=colors,
                line=dict(width=1, color='white')
            ),
            text=labels,
            customdata=config_indices,
            hovertemplate='<b>%{text}</b><br>Performance: %{x:.6f}<br>Robustness: %{y:.3f}<extra></extra>'
        ))
        
        # Add quadrant lines
        mean_perf = np.mean(performance)
        mean_robust = np.mean(robustness)
        
        fig.add_hline(y=mean_robust, line_dash="dash", line_color="gray", opacity=0.5)
        fig.add_vline(x=mean_perf, line_dash="dash", line_color="gray", opacity=0.5)
        
        # Add quadrant labels
        max_perf = max(performance)
        max_robust = max(robustness)
        
        fig.add_annotation(x=max_perf * 0.9, y=max_robust * 0.9, 
                          text="🏆 Best of Both", showarrow=False, font=dict(size=14, color="green"))
        fig.add_annotation(x=mean_perf * 0.3, y=max_robust * 0.9,
                          text="🛡️ Stable but Slow", showarrow=False, font=dict(size=12, color="blue"))
        fig.add_annotation(x=max_perf * 0.9, y=mean_robust * 0.3,
                          text="🎲 High Risk", showarrow=False, font=dict(size=12, color="orange"))
        
        # Point count removed - displayed outside chart in Streamlit
        
        fig.update_layout(
            title=f"Robustness vs Performance: {performance_metric.replace('_', ' ').title()}",
            xaxis_title=f"{performance_metric.replace('_', ' ').title()} (Performance)",
            yaxis_title="Robustness Score (0-1)",
            height=600,
            template='plotly_white'
        )
        
        return fig
    
    def plot_evolution_timeline(self, metric: str, window: int = 100) -> go.Figure:
        """
        Line chart showing metric evolution over iterations
        
        Args:
            metric: Metric to track
            window: Rolling window size for smoothing
        
        Returns:
            Plotly figure
        """
        df = self.loader.to_dataframe(pareto_only=False)
        
        if df.empty or metric not in df.columns:
            return go.Figure().add_annotation(text="No data available", showarrow=False)
        
        df = df.sort_values('config_index')
        
        fig = go.Figure()
        
        # Raw values
        fig.add_trace(go.Scatter(
            x=df['config_index'],
            y=df[metric],
            mode='markers',
            marker=dict(size=3, color='lightblue', opacity=0.5),
            name='All Configs',
            hovertemplate='Iter: %{x}<br>Value: %{y:.6f}<extra></extra>'
        ))
        
        # Rolling average
        if len(df) > window:
            rolling_mean = df[metric].rolling(window=window, center=True).mean()
            fig.add_trace(go.Scatter(
                x=df['config_index'],
                y=rolling_mean,
                mode='lines',
                line=dict(color='blue', width=2),
                name=f'Rolling Avg ({window})',
                hovertemplate='Iter: %{x}<br>Avg: %{y:.6f}<extra></extra>'
            ))
        
        # Highlight Pareto configs
        pareto_df = df[df['is_pareto'] == True]
        if not pareto_df.empty:
            fig.add_trace(go.Scatter(
                x=pareto_df['config_index'],
                y=pareto_df[metric],
                mode='markers',
                marker=dict(size=10, color='red', symbol='star'),
                name='Pareto Configs',
                hovertemplate='<b>Pareto at Iter %{x}</b><br>Value: %{y:.6f}<extra></extra>'
            ))
        
        fig.update_layout(
            title=f"Evolution Timeline: {metric.replace('_', ' ').title()}",
            xaxis_title="Iteration",
            yaxis_title=metric.replace('_', ' ').title(),
            height=500,
            template='plotly_white',
            hovermode='x unified'
        )
        
        return fig
    
    def plot_radar_chart(self,
                        best_match_config,
                        comparison_configs: Optional[List] = None,
                        metrics: Optional[List[str]] = None,
                        top_n_comparison: int = 5) -> go.Figure:
        """
        Radar/Spider chart comparing Best Match against other top configs
        Uses cartesian scatter plot to enable click functionality
        
        Args:
            best_match_config: ConfigMetrics to highlight as best match
            comparison_configs: Optional list of configs to compare against
            metrics: List of metrics to display (auto-selected if None)
            top_n_comparison: Number of top configs to compare (default: 5)
        
        Returns:
            Plotly figure with clickable markers
        """
        if best_match_config is None:
            return go.Figure().add_annotation(text="No Best Match config available", showarrow=False)
        
        # Auto-select metrics if not specified (balanced view)
        if metrics is None:
            available_metrics = list(best_match_config.suite_metrics.keys())
            metrics = []
            
            # Performance
            for m in ['adg_w_usd', 'gain_usd']:
                if m in available_metrics:
                    metrics.append(m)
                    break
            
            # Risk-adjusted
            for m in ['sharpe_ratio_usd', 'sortino_ratio_usd', 'calmar_ratio_usd']:
                if m in available_metrics:
                    metrics.append(m)
                    break
            
            # Risk
            for m in ['drawdown_worst_usd', 'equity_choppiness_usd']:
                if m in available_metrics:
                    metrics.append(m)
                    break
            
            # Robustness
            metrics.append('robustness')  # Special synthetic metric
            
            # Volatility
            for m in ['equity_volatility_usd']:
                if m in available_metrics:
                    metrics.append(m)
                    break
        
        # Get comparison configs if not provided
        if comparison_configs is None:
            pareto_configs = self.loader.get_pareto_configs()
            # Get top N by primary metric
            primary_metric = self.loader.scoring_metrics[0] if self.loader.scoring_metrics else 'adg_w_usd'
            comparison_configs = sorted(
                [c for c in pareto_configs if c.config_index != best_match_config.config_index],
                key=lambda c: c.suite_metrics.get(primary_metric, 0),
                reverse=True
            )[:top_n_comparison]
        
        # Normalize metrics to 0-1 scale
        all_configs = [best_match_config] + comparison_configs
        
        # Collect all values for normalization
        metric_ranges = {}
        for metric in metrics:
            values = []
            for config in all_configs:
                if metric == 'robustness':
                    val = self.loader.compute_overall_robustness(config)
                else:
                    val = config.suite_metrics.get(metric, 0)
                values.append(val)
            
            if values:
                metric_ranges[metric] = {
                    'min': min(values),
                    'max': max(values),
                    'range': max(values) - min(values) if max(values) != min(values) else 1
                }
        
        # Convert to cartesian coordinates for clickable scatter plot
        import numpy as np
        n_metrics = len(metrics)
        angles = np.linspace(0, 2 * np.pi, n_metrics, endpoint=False).tolist()
        
        # Create readable labels
        theta_labels = []
        for metric in metrics:
            label = metric.replace('_usd', '').replace('_', ' ').title()
            if metric == 'robustness':
                label = 'Robustness'
            theta_labels.append(label)
        
        # Create figure
        fig = go.Figure()
        
        # Helper function to convert polar to cartesian
        def polar_to_cartesian(r, theta):
            x = r * np.cos(theta)
            y = r * np.sin(theta)
            return x, y
        
        # Add Best Match trace
        r_values = []
        for metric in metrics:
            if metric == 'robustness':
                val = self.loader.compute_overall_robustness(best_match_config)
            else:
                val = best_match_config.suite_metrics.get(metric, 0)
            
            # Normalize
            if metric in metric_ranges and metric_ranges[metric]['range'] > 0:
                normalized = (val - metric_ranges[metric]['min']) / metric_ranges[metric]['range']
            else:
                normalized = 0.5
            
            # Invert for "bad" metrics
            if 'drawdown' in metric.lower() or 'choppiness' in metric.lower() or 'volatility' in metric.lower():
                normalized = 1.0 - normalized
            
            r_values.append(normalized)
        
        # Convert to cartesian and close the loop
        r_values_closed = r_values + [r_values[0]]
        angles_closed = angles + [angles[0]]
        
        x_vals = []
        y_vals = []
        for r, theta in zip(r_values_closed, angles_closed):
            x, y = polar_to_cartesian(r, theta)
            x_vals.append(x)
            y_vals.append(y)
        
        # Add filled area (polygon)
        fig.add_trace(go.Scatter(
            x=x_vals,
            y=y_vals,
            mode='lines',
            name='⭐ Best Match (area)',
            line=dict(color='rgba(0,255,100,0)', width=0),
            fill='toself',
            fillcolor='rgba(0,255,100,0.2)',
            showlegend=False,
            hoverinfo='skip'
        ))
        
        # Add line
        fig.add_trace(go.Scatter(
            x=x_vals,
            y=y_vals,
            mode='lines',
            name='⭐ Best Match',
            line=dict(color='lime', width=3),
            showlegend=True,
            hoverinfo='skip'
        ))
        
        # Add CLICKABLE markers
        customdata_best = [[best_match_config.config_index, theta_labels[i % len(theta_labels)], r_values[i % len(r_values)]] 
                          for i in range(len(r_values))]
        
        fig.add_trace(go.Scatter(
            x=[polar_to_cartesian(r, theta)[0] for r, theta in zip(r_values, angles)],
            y=[polar_to_cartesian(r, theta)[1] for r, theta in zip(r_values, angles)],
            mode='markers',
            name='⭐ Best Match (points)',
            marker=dict(size=15, color='lime', symbol='star', line=dict(color='darkgreen', width=2)),
            customdata=customdata_best,
            hovertemplate='<b>⭐ Best Match (Config %{customdata[0]})</b><br>%{customdata[1]}: %{customdata[2]:.1%}<extra></extra>',
            showlegend=False
        ))
        
        # Add comparison configs
        colors_line = ['rgb(100,150,255)', 'rgb(255,150,100)', 'rgb(150,100,255)', 
                      'rgb(255,200,100)', 'rgb(100,255,200)']
        colors_fill = ['rgba(100,150,255,0.15)', 'rgba(255,150,100,0.15)', 'rgba(150,100,255,0.15)', 
                      'rgba(255,200,100,0.15)', 'rgba(100,255,200,0.15)']
        symbols = ['circle', 'square', 'diamond', 'cross', 'x']
        
        for idx, config in enumerate(comparison_configs):
            r_values = []
            for metric in metrics:
                if metric == 'robustness':
                    val = self.loader.compute_overall_robustness(config)
                else:
                    val = config.suite_metrics.get(metric, 0)
                
                # Normalize
                if metric in metric_ranges and metric_ranges[metric]['range'] > 0:
                    normalized = (val - metric_ranges[metric]['min']) / metric_ranges[metric]['range']
                else:
                    normalized = 0.5
                
                # Invert for "bad" metrics
                if 'drawdown' in metric.lower() or 'choppiness' in metric.lower() or 'volatility' in metric.lower():
                    normalized = 1.0 - normalized
                
                r_values.append(normalized)
            
            # Convert to cartesian and close the loop
            r_values_closed = r_values + [r_values[0]]
            angles_closed = angles + [angles[0]]
            
            x_vals = []
            y_vals = []
            for r, theta in zip(r_values_closed, angles_closed):
                x, y = polar_to_cartesian(r, theta)
                x_vals.append(x)
                y_vals.append(y)
            
            color_line = colors_line[idx % len(colors_line)]
            color_fill = colors_fill[idx % len(colors_fill)]
            symbol = symbols[idx % len(symbols)]
            
            # Add filled area
            fig.add_trace(go.Scatter(
                x=x_vals,
                y=y_vals,
                mode='lines',
                name=f'Config {config.config_index} (area)',
                line=dict(color='rgba(0,0,0,0)', width=0),
                fill='toself',
                fillcolor=color_fill,
                showlegend=False,
                hoverinfo='skip'
            ))
            
            # Add line
            fig.add_trace(go.Scatter(
                x=x_vals,
                y=y_vals,
                mode='lines',
                name=f'Config {config.config_index}',
                line=dict(color=color_line, width=2),
                showlegend=True,
                hoverinfo='skip'
            ))
            
            # Add CLICKABLE markers
            customdata_config = [[config.config_index, theta_labels[i], r_values[i]] for i in range(len(r_values))]
            
            fig.add_trace(go.Scatter(
                x=[polar_to_cartesian(r, theta)[0] for r, theta in zip(r_values, angles)],
                y=[polar_to_cartesian(r, theta)[1] for r, theta in zip(r_values, angles)],
                mode='markers',
                name=f'Config {config.config_index} (points)',
                marker=dict(size=12, color=color_line, symbol=symbol, line=dict(color='white', width=1)),
                customdata=customdata_config,
                hovertemplate=f'<b>Config {{config.config_index}}</b><br>%{{customdata[1]}}: %{{customdata[2]:.1%}}<extra></extra>'.replace('{config.config_index}', str(config.config_index)),
                showlegend=False
            ))
        
        # Add axis lines and labels
        max_r = 1.2
        for i, (angle, label) in enumerate(zip(angles, theta_labels)):
            x_line = [0, max_r * np.cos(angle)]
            y_line = [0, max_r * np.sin(angle)]
            
            # Add axis line
            fig.add_trace(go.Scatter(
                x=x_line,
                y=y_line,
                mode='lines',
                line=dict(color='lightgray', width=1),
                showlegend=False,
                hoverinfo='skip'
            ))
            
            # Add label
            label_x = 1.3 * np.cos(angle)
            label_y = 1.3 * np.sin(angle)
            
            fig.add_annotation(
                x=label_x,
                y=label_y,
                text=f'<b>{label}</b>',
                showarrow=False,
                font=dict(size=14, color='white'),
                xanchor='center',
                yanchor='middle',
                bgcolor='rgba(0,0,0,0.7)',
                borderpad=4
            )
        
        # Add concentric circles for scale
        for r_circle in [0.25, 0.5, 0.75, 1.0]:
            circle_angles = np.linspace(0, 2 * np.pi, 100)
            x_circle = [r_circle * np.cos(a) for a in circle_angles]
            y_circle = [r_circle * np.sin(a) for a in circle_angles]
            
            fig.add_trace(go.Scatter(
                x=x_circle,
                y=y_circle,
                mode='lines',
                line=dict(color='lightgray', width=1, dash='dot'),
                showlegend=False,
                hoverinfo='skip'
            ))
            
            # Add scale label
            fig.add_annotation(
                x=0,
                y=r_circle,
                text=f'<b>{int(r_circle * 100)}%</b>',
                showarrow=False,
                font=dict(size=10, color='white'),
                xanchor='center',
                yanchor='bottom',
                bgcolor='rgba(0,0,0,0.5)',
                borderpad=2
            )
        
        # Update layout
        fig.update_layout(
            title="Best Match vs Top Configs (Click markers to select)",
            xaxis=dict(
                showgrid=False,
                zeroline=False,
                showticklabels=False,
                scaleanchor='y',
                scaleratio=1
            ),
            yaxis=dict(
                showgrid=False,
                zeroline=False,
                showticklabels=False
            ),
            showlegend=True,
            height=700,
            hovermode='closest'
        )
        
        return fig
    
    def plot_parameter_bounds_distance(self, top_n: int = 15) -> go.Figure:
        """
        Bar chart showing parameters at or near bounds
        
        Args:
            top_n: Number of parameters to show
        
        Returns:
            Plotly figure
        """
        bounds_info = self.loader.get_parameters_at_bounds(tolerance=0.1)
        
        at_bounds = bounds_info['at_lower'] + bounds_info['at_upper']
        
        if not at_bounds:
            return go.Figure().add_annotation(text="No parameters at bounds", showarrow=False)
        
        # Compute distance to bounds for these parameters
        distances = []
        param_names = []
        colors = []
        
        for param in at_bounds[:top_n]:
            if param not in self.loader.optimize_bounds:
                continue
            
            lower, upper = self.loader.optimize_bounds[param]
            param_range = upper - lower
            
            # Get values from Pareto configs
            pareto_configs = self.loader.get_pareto_configs()
            values = [c.bot_params.get(param, 0) for c in pareto_configs if param in c.bot_params]
            
            if not values:
                continue
            
            min_val = min(values)
            max_val = max(values)
            
            # Distance to bounds (normalized)
            dist_lower = (min_val - lower) / param_range
            dist_upper = (upper - max_val) / param_range
            
            # Show the closer bound
            if dist_lower < dist_upper:
                distances.append(dist_lower * 100)
                colors.append('red' if dist_lower < 0.05 else 'orange')
                label = f"{param.replace('long_', '').replace('short_', '')} (Lower)"
            else:
                distances.append(dist_upper * 100)
                colors.append('red' if dist_upper < 0.05 else 'orange')
                label = f"{param.replace('long_', '').replace('short_', '')} (Upper)"
            
            param_names.append(label)
        
        fig = go.Figure(data=[
            go.Bar(
                x=distances,
                y=param_names,
                orientation='h',
                marker=dict(color=colors),
                text=[f"{d:.1f}%" for d in distances],
                textposition='auto',
                hovertemplate='<b>%{y}</b><br>Distance: %{x:.1f}%<extra></extra>'
            )
        ])
        
        fig.update_layout(
            title="Parameters Near Bounds (< 10% from limit)",
            xaxis_title="Distance from Bound (%)",
            yaxis_title="Parameter",
            height=max(400, len(param_names) * 30),
            template='plotly_white'
        )
        
        return fig
    
    def plot_trading_style_distribution(self) -> go.Figure:
        """
        Pie chart showing distribution of trading styles
        
        Returns:
            Plotly figure
        """
        configs = self.loader.get_pareto_configs()
        
        if not configs:
            return go.Figure().add_annotation(text="No configs available", showarrow=False)
        
        styles = {}
        for config in configs:
            style = self.loader.compute_trading_style(config)
            styles[style] = styles.get(style, 0) + 1
        
        fig = go.Figure(data=[go.Pie(
            labels=list(styles.keys()),
            values=list(styles.values()),
            hole=0.3,
            textinfo='label+percent',
            marker=dict(colors=self.color_palette)
        )])
        
        fig.update_layout(
            title="Trading Style Distribution (Pareto Configs)",
            height=500,
            template='plotly_white'
        )
        
        return fig
