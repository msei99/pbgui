import streamlit as st  # Streamlit library for creating web apps
from pbgui_func import set_page_config, is_session_state_not_initialized, error_popup, is_pb7_installed, is_authenticted, get_navi_paths
import numpy as np  # NumPy for numerical operations
import pandas as pd  # Pandas for data manipulation and analysis
import plotly.graph_objs as go  # Plotly for interactive data visualization
from dataclasses import dataclass, field, asdict
import json
from typing import List
from Config import ConfigV7
import json
from dataclasses import dataclass, field, asdict
from typing import List
from GridVisualizerV7 import (
    calc_entries_long,
    calc_closes_long,
    calc_entries_short,
    calc_closes_short,
    ExchangeParams,
    StateParams,
    BotParams,
    EmaBands,
    OrderBook,
    Position,
    TrailingPriceBundle,
    Order,
    OrderType,
    Side,
    GridTrailingMode
)

from dataclasses import dataclass, asdict
import json

def get_GridTrailing_mode(trailing_grid_ratio: float) -> GridTrailingMode:
    if trailing_grid_ratio == 0.0:
        return GridTrailingMode.GridOnly
    elif trailing_grid_ratio == -1.0:
        return  GridTrailingMode.TrailingOnly
    elif trailing_grid_ratio == 1.0:
        return  GridTrailingMode.TrailingOnly
    elif trailing_grid_ratio < 0.0:
        return  GridTrailingMode.GridFirst
    elif trailing_grid_ratio > 0.0:
        return  GridTrailingMode.TrailingFirst
    return GridTrailingMode.Unknown 

@dataclass
class GVData:
    exchange_params: ExchangeParams = ExchangeParams(min_qty=0.001, min_cost=1.0, qty_step=0.001, price_step=0.01, c_mult=1.0)
    state_params: StateParams = StateParams(
        balance=1000.0,
        order_book=OrderBook(bid=100.0, ask=100),
        ema_bands=EmaBands(lower=100.0, upper=100.0)
    )
        
    normal_bot_params_long: BotParams = BotParams(
        wallet_exposure_limit=1.5,
        n_positions=1,
        entry_initial_qty_pct=0.03,
        entry_initial_ema_dist=0.03,
        entry_grid_spacing_pct=0.04,
        entry_grid_spacing_weight=1.2,
        entry_grid_double_down_factor=1.2,
        entry_trailing_threshold_pct=0.05,
        entry_trailing_retracement_pct=0.03,
        entry_trailing_grid_ratio=-0.7, 

        close_grid_min_markup=0.03,
        close_grid_markup_range=0.02,
        close_grid_qty_pct=0.3,
        close_trailing_threshold_pct=0.05,
        close_trailing_retracement_pct=0.03,
        close_trailing_qty_pct=0.3,
        close_trailing_grid_ratio=0.0,
    )
    gridonly_bot_params_long = normal_bot_params_long.clone()
    
    normal_bot_params_short: BotParams = BotParams(
        wallet_exposure_limit=1.5,
        n_positions=1,
        entry_initial_qty_pct=0.03,
        entry_initial_ema_dist=0.03,
        entry_grid_spacing_pct=0.04,
        entry_grid_spacing_weight=1.2,
        entry_grid_double_down_factor=1.2,
        entry_trailing_threshold_pct=0.05,
        entry_trailing_retracement_pct=0.03,
        entry_trailing_grid_ratio=-0.8, 

        close_grid_min_markup=0.03,
        close_grid_markup_range=0.02,
        close_grid_qty_pct=0.3,
        close_trailing_threshold_pct=0.05,
        close_trailing_retracement_pct=0.03,
        close_trailing_qty_pct=0.3,
        close_trailing_grid_ratio=0.0,
    )
    gridonly_bot_params_short = normal_bot_params_short.clone()
    
    position_long_enty: Position = Position(size=0.00, price=100.0)
    position_long_close: Position = Position(size=10.00, price=100.0)
    position_short_entry: Position = Position(size=0.00, price=100.0)
    position_short_close: Position = Position(size=-10.00, price=100.0)
    
    trailing_price_bundle: TrailingPriceBundle = TrailingPriceBundle(
        max_since_open=100.0,
        min_since_open=100.0,
        max_since_min=100.0,
        min_since_max=100.0
    )
    
    long_entry_mode = GridTrailingMode.Unknown
    long_close_mode = GridTrailingMode.Unknown
    short_entry_mode = GridTrailingMode.Unknown
    short_close_mode = GridTrailingMode.Unknown

    # Everything else
    is_external_config: bool = False
    title: str = ""
    
    # Results
    normal_entries_long = []
    normal_closes_long = []
    normal_entries_short = []
    normal_closes_short = []
    gridonly_entries_long = []
    gridonly_closes_long = []
    gridonly_entries_short = []
    gridonly_closes_short = []
    
    long_entry_grid = 0
    long_close_grid = 0
    short_entry_grid = 0
    short_close_grid = 0
    
    def to_json(self) -> str:
        # Only serialize bot_params_long and bot_params_short
        data_dict = {
            "bot": {
                "long": asdict(self.normal_bot_params_long),
                "short": asdict(self.normal_bot_params_short)
            }
        }
        return json.dumps(data_dict, indent=4)

    def prepare_data(self):
        # Apply TWE to position sizes
        self.position_long_enty = Position(size=0.00, price=100.0)
        self.position_long_close = Position(size=10.00 * self.normal_bot_params_long.wallet_exposure_limit, price=100.0)
        self.position_short_entry = Position(size=0.00, price=100.0)
        self.position_short_close = Position(size=-10.00 * self.normal_bot_params_short.wallet_exposure_limit, price=100.0)
        
        # Prepare gridonly bot params
        self.gridonly_bot_params_long = self.normal_bot_params_long.clone()
        self.gridonly_bot_params_short = self.normal_bot_params_short.clone()
        self.gridonly_bot_params_long.entry_trailing_grid_ratio = 0.0
        self.gridonly_bot_params_long.close_trailing_grid_ratio = 0.0
        self.gridonly_bot_params_short.entry_trailing_grid_ratio = 0.0
        self.gridonly_bot_params_short.close_trailing_grid_ratio = 0.0
        
        # Set modes
        self.long_entry_mode = get_GridTrailing_mode(self.normal_bot_params_long.entry_trailing_grid_ratio)
        self.long_close_mode = get_GridTrailing_mode(self.normal_bot_params_long.close_trailing_grid_ratio)
        self.short_entry_mode = get_GridTrailing_mode(self.normal_bot_params_short.entry_trailing_grid_ratio)
        self.short_close_mode = get_GridTrailing_mode(self.normal_bot_params_short.close_trailing_grid_ratio)
    
    def isActive(self, side: OrderType) -> bool:
        if side == Side.Long:
            return self.normal_bot_params_long.wallet_exposure_limit > 0.0 and self.normal_bot_params_long.n_positions > 0
        else:
            return self.normal_bot_params_short.wallet_exposure_limit > 0.0 and self.normal_bot_params_short.n_positions > 0
        
    @classmethod
    def from_json(cls, json_str: str) -> 'GVData':
        # Expect the "bot" structure with "long" and "short" keys
        data = json.loads(json_str)
        bot_data = data.get("bot", {})
        
        long_data = bot_data.get("long", {})
        short_data = bot_data.get("short", {})

        return cls(
            normal_bot_params_long=BotParams(**long_data),
            normal_bot_params_short=BotParams(**short_data)
        )

def prepare_config() -> GVData:
    # If there's no ConfigV7 in the session, load (probably passed from another page)
    if "v7_grid_visualizer_config" in st.session_state:
        # Build GVData from v7 config
        config_v7: ConfigV7 = st.session_state.v7_grid_visualizer_config
        
        data = GVData()
        # Build Title identifying the config
        data.title = f"Loaded Configuration: {config_v7.pbgui.note} (v{config_v7.pbgui.version})"
        data.is_external_config = True
        
        data.normal_bot_params_long = BotParams(
            wallet_exposure_limit=          config_v7.bot.long.total_wallet_exposure_limit,
            n_positions=                    config_v7.bot.long.n_positions,
            entry_initial_qty_pct=          config_v7.bot.long.entry_initial_qty_pct,
            entry_initial_ema_dist=         config_v7.bot.long.entry_initial_ema_dist,
            entry_grid_spacing_pct=         config_v7.bot.long.entry_grid_spacing_pct,
            entry_grid_spacing_weight=      config_v7.bot.long.entry_grid_spacing_weight,
            entry_grid_double_down_factor=  config_v7.bot.long.entry_grid_double_down_factor,
            entry_trailing_threshold_pct=   config_v7.bot.long.entry_trailing_threshold_pct,
            entry_trailing_retracement_pct= config_v7.bot.long.entry_trailing_retracement_pct,
            entry_trailing_grid_ratio=      config_v7.bot.long.entry_trailing_grid_ratio,
            
            close_grid_min_markup=          config_v7.bot.long.close_grid_markup_end,
            close_grid_markup_range=        config_v7.bot.long.close_grid_markup_start - config_v7.bot.long.close_grid_markup_end,
            close_grid_qty_pct=             config_v7.bot.long.close_grid_qty_pct,
            close_trailing_threshold_pct=   config_v7.bot.long.close_trailing_threshold_pct,
            close_trailing_retracement_pct= config_v7.bot.long.close_trailing_retracement_pct,
            close_trailing_qty_pct=         config_v7.bot.long.close_trailing_qty_pct,
            close_trailing_grid_ratio=      config_v7.bot.long.close_trailing_grid_ratio,
        )
        
        data.normal_bot_params_short = BotParams(
            wallet_exposure_limit=          config_v7.bot.short.total_wallet_exposure_limit,
            n_positions=                    config_v7.bot.long.n_positions,
            entry_initial_qty_pct=          config_v7.bot.short.entry_initial_qty_pct,
            entry_initial_ema_dist=         config_v7.bot.short.entry_initial_ema_dist,
            entry_grid_spacing_pct=         config_v7.bot.short.entry_grid_spacing_pct,
            entry_grid_spacing_weight=      config_v7.bot.short.entry_grid_spacing_weight,
            entry_grid_double_down_factor=  config_v7.bot.short.entry_grid_double_down_factor,
            entry_trailing_threshold_pct=   config_v7.bot.short.entry_trailing_threshold_pct,
            entry_trailing_retracement_pct= config_v7.bot.short.entry_trailing_retracement_pct,
            entry_trailing_grid_ratio=      config_v7.bot.short.entry_trailing_grid_ratio,
            
            close_grid_min_markup=          config_v7.bot.short.close_grid_markup_end,
            close_grid_markup_range=        config_v7.bot.short.close_grid_markup_start - config_v7.bot.short.close_grid_markup_end,
            close_grid_qty_pct=             config_v7.bot.short.close_grid_qty_pct,
            close_trailing_threshold_pct=   config_v7.bot.short.close_trailing_threshold_pct,
            close_trailing_retracement_pct= config_v7.bot.short.close_trailing_retracement_pct,
            close_trailing_qty_pct=         config_v7.bot.short.close_trailing_qty_pct,
            close_trailing_grid_ratio=      config_v7.bot.short.close_trailing_grid_ratio,
        )
        
        data.prepare_data()
        st.session_state.v7_grid_visualizer_data = data
        del st.session_state.v7_grid_visualizer_config
        return data
    
    # If there's a data object in the session, use it (e.g. from editor)
    if "v7_grid_visualizer_data" in st.session_state:
        data = st.session_state.v7_grid_visualizer_data
        data.prepare_data()
        return data
    
    data = GVData()
    data.prepare_data()
    return data



def create_plotly_graph(side: OrderType, data: GVData):
    
    if not data.isActive(side):
        return None
    
    normal_entry_orders = []
    normal_entry_prices = []
    normal_enty_grid_min = 0
    normal_enty_grid_max = 0
    
    normal_close_orders = []
    normap_close_prices = []
    normal_close_grid_min = 0
    normal_close_grid_max = 0
    
    fullgrid_entry_orders = []
    fullgrid_entry_prices = []
    fullgrid_entry_grid_min = 0
    fullgrid_entry_grid_max = 0
    fullgrid_close_orders = []
    fullgrid_close_prices = []
    fullgrid_close_grid_min = 0
    fullgrid_close_grid_max = 0
    
    trailing_entry_orders = []
    trailing_entry_prices = []
    trailing_entry_grid_min = 0
    trailing_entry_grid_max = 0
    trailing_close_orders = []
    trailing_close_prices = []
    trailing_close_grid_min = 0
    trailing_close_grid_max = 0
    
    bot_params = None
    position_price = None
    state_params = None
    entry_mode = GridTrailingMode.Unknown
    close_mode = GridTrailingMode.Unknown
    
    # Determine which bot params to use depending on side
    if side == Side.Long:
        bot_params = data.normal_bot_params_long
        position_price = data.position_long_enty.price
        normal_entry_orders = data.normal_entries_long
        normal_close_orders = data.normal_closes_long
        fullgrid_entry_orders = data.gridonly_entries_long
        fullgrid_close_orders = data.gridonly_closes_long
        entry_mode = data.long_entry_mode
        close_mode = data.long_close_mode
        state_params = data.state_params
        title_side = "LONG"
    else:
        bot_params = data.normal_bot_params_short
        position_price = data.position_short_entry.price
        normal_entry_orders = data.normal_entries_short
        normal_close_orders = data.normal_closes_short
        fullgrid_entry_orders = data.gridonly_entries_short
        fullgrid_close_orders = data.gridonly_closes_short
        entry_mode = data.short_entry_mode
        close_mode = data.short_close_mode
        state_params = data.state_params
        title_side = "SHORT"

    # Derive a "start_price" for plotting. 
    start_price = 100

    # Extract entry and close prices
    normal_entry_prices = [o.price for o in normal_entry_orders]
    if len(normal_entry_prices) > 0:
        normal_enty_grid_min = min(normal_entry_prices)
        normal_enty_grid_max = max(normal_entry_prices)
        
        if side == Side.Long:
            data.long_entry_grid = normal_enty_grid_max - normal_enty_grid_min
        else:
            data.short_entry_grid = normal_enty_grid_max - normal_enty_grid_min
    else:
        normal_enty_grid_min = 100
        normal_enty_grid_max = 100
    
    
    normal_close_prices = [o.price for o in normal_close_orders]
    if len(normal_close_prices) > 0:
        normal_close_grid_min = min(normal_close_prices)
        normal_close_grid_max = max(normal_close_prices)
        
        if side == Side.Long:
            data.long_close_grid = normal_enty_grid_max - normal_enty_grid_min
        else:
            data.short_close_grid = normal_close_grid_max - normal_close_grid_min
    else:
        normal_close_grid_min = 100
        normal_close_grid_max = 100
    
    fullgrid_entry_prices = [o.price for o in fullgrid_entry_orders]
    if len(fullgrid_entry_prices) > 0:
        fullgrid_entry_grid_min = min(fullgrid_entry_prices)
        fullgrid_entry_grid_max = max(fullgrid_entry_prices)
  
    fullgrid_close_prices = [o.price for o in fullgrid_close_orders]
    if len(fullgrid_close_prices) > 0:
        fullgrid_close_grid_min = min(fullgrid_close_prices)
        fullgrid_close_grid_max = max(fullgrid_close_prices)
    
    
    # Handle Trailing Grids
    warnings = []
    
    ############
    #  LONG   #
    ############
    if side == Side.Long:
        ############
        #  ENTRY   #
        ############
        # st.warning(f"normal_entry_orders: {normal_entry_orders}")
        # st.warning(f"fullgrid_entry_orders: {fullgrid_entry_orders}")
        # st.error(f"normal_close_orders: {normal_close_orders}")
        # st.error(f"fullgrid_close_orders: {fullgrid_close_orders}")
        if entry_mode == GridTrailingMode.GridFirst:
            trailing_entry_grid_min = normal_enty_grid_min
            trailing_entry_grid_max = trailing_entry_grid_min - 10
            warnings.append("Enty: The trailing area can't be calculated; 10% is just a placeholder.")
        if entry_mode == GridTrailingMode.TrailingFirst:
            trailing_entry_grid_min = fullgrid_entry_grid_max
            trailing_entry_grid_max = trailing_entry_grid_min - 10
            warnings.append("Enty: The trailing area can't be calculated; 10% is just a placeholder.")
            warnings.append("Enty: There will be Grid-Entries after the trailing area.")
        if entry_mode == GridTrailingMode.TrailingOnly:
            trailing_entry_grid_min = fullgrid_entry_grid_max
            trailing_entry_grid_max = trailing_entry_grid_min - 10
            warnings.append("Enty: The trailing area can't be calculated; 10% is just a placeholder.")
        ############
        #  CLOSE   #
        ############
        if close_mode == GridTrailingMode.GridFirst:
            trailing_close_grid_min = fullgrid_close_grid_min
            trailing_close_grid_max = trailing_close_grid_min + 10
            warnings.append("Close: Since WE=100% at close start, grid comes first, then trailing.")
            warnings.append("Close: The trailing area can't be calculated; 10% is just a placeholder.")
            warnings.append("Close: Grid closes follow the trailing area (not displayed).")
        if close_mode == GridTrailingMode.TrailingFirst:
            trailing_close_grid_min = normal_close_grid_max
            trailing_close_grid_max = trailing_close_grid_min + 10
            warnings.append("Close: Since WE=100% at close start, grid comes first, then trailing.")
            warnings.append("Close: The trailing area can't be calculated; 10% is just a placeholder.")
        if close_mode == GridTrailingMode.TrailingOnly:
            trailing_close_grid_min = fullgrid_close_grid_min
            trailing_close_grid_max = trailing_close_grid_min + 10
            warnings.append("Close: The trailing area can't be calculated; 10% is just a placeholder.")
    ############
    #  SHORT   #
    ############
    elif side == Side.Short:
        #st.warning(f"normal_entry_orders: {normal_entry_orders}")
        #st.warning(f"fullgrid_entry_orders: {fullgrid_entry_orders}")
        #st.error(f"normal_close_orders: {normal_close_orders}")
        #st.error(f"fullgrid_close_orders: {fullgrid_close_orders}")
        ############
        #  ENTRY   #
        ############
        if entry_mode == GridTrailingMode.GridFirst:
            trailing_entry_grid_min = normal_enty_grid_max
            trailing_entry_grid_max = trailing_entry_grid_min + 10
            warnings.append("Entry: The trailing area can't be calculated; 10% is just a placeholder.")
        if entry_mode == GridTrailingMode.TrailingFirst:
            trailing_entry_grid_min = fullgrid_entry_grid_min
            trailing_entry_grid_max = trailing_entry_grid_min + 10
            warnings.append("Entry: The trailing area can't be calculated; 10% is just a placeholder.")
        if entry_mode == GridTrailingMode.TrailingOnly:
            trailing_entry_grid_min = fullgrid_entry_grid_min
            trailing_entry_grid_max = trailing_entry_grid_min + 10
            warnings.append("Entry: The trailing area can't be calculated; 10% is just a placeholder.")
        ############
        #  CLOSE   #
        ############
        if close_mode == GridTrailingMode.GridFirst:
            trailing_close_grid_min = fullgrid_close_grid_min
            trailing_close_grid_max = trailing_close_grid_min -10
            warnings.append("Close: Since WE=100% at close start, grid comes first, then trailing.")
            warnings.append("Close: The trailing area can't be calculated; 10% is just a placeholder.")
            warnings.append("Close: Grid closes follow the trailing area (not displayed).")
        if close_mode == GridTrailingMode.TrailingFirst:
            trailing_close_grid_min = normal_close_grid_min
            trailing_close_grid_max = trailing_close_grid_min - 10
            warnings.append("Close: Since WE=100% at close start, grid comes first, then trailing.")
            warnings.append("Close: The trailing area can't be calculated; 10% is just a placeholder.")
        if close_mode == GridTrailingMode.TrailingOnly:
            trailing_close_grid_min = fullgrid_close_grid_min
            trailing_close_grid_max = trailing_close_grid_min - 10
            warnings.append("Close: The trailing area can't be calculated; 10% is just a placeholder.")
            
    # Create Plotly Figure
    fig = go.Figure()

    # Add a bold purple continuous horizontal line at start_price
    fig.add_trace(go.Scatter(
        x=[0, 1],
        y=[start_price, start_price],
        mode='lines',
        name='EMA Band',
        line=dict(
            color='purple',
            dash='solid',
            width=4
        )
    ))

    # Add entry grid range as a shaded area
    fig.add_shape(
        type='rect',
        xref='paper',
        x0=0,
        x1=1,
        yref='y',
        y0=normal_enty_grid_min,
        y1=normal_enty_grid_max,
        fillcolor='red',
        opacity=0.2,
        layer='below',
        line_width=0
    )
    # Add a dummy trace to represent Entry Grid in the legend
    fig.add_trace(go.Scatter(
        x=[None],
        y=[None],
        mode='lines',
        line=dict(color='rgba(255,0,0,0.2)', width=10),
        name='Entry Grid (Area)',
        showlegend=True
    ))
    
    # Add one Entry Grid line and show it in the legend
    if len(normal_entry_prices) > 0:
        fig.add_trace(go.Scatter(
            x=[0, 1],
            y=[normal_entry_prices[0], normal_entry_prices[0]],
            mode='lines',
            name='Entry Grid (Lines)',
            line=dict(
                color='rgba(255, 0, 0, 0.6)',
                dash='dash',
                width=1
            ),
            showlegend=True,
            legendgroup='entry'
        ))

        # Add the rest of the entry lines without showing them individually in the legend
        for entry_price in normal_entry_prices[1:]:
            fig.add_trace(go.Scatter(
                x=[0, 1],
                y=[entry_price, entry_price],
                mode='lines',
                line=dict(
                    color='rgba(255, 0, 0, 0.6)',
                    dash='dash',
                    width=1
                ),
                showlegend=False,
                legendgroup='entry'
            ))
    

    if trailing_entry_grid_min != 0 and trailing_entry_grid_max != 0:
        # Add entry grid range as a shaded area
        fig.add_shape(
            type='rect',
            xref='paper',
            x0=0,
            x1=1,
            yref='y',
            y0=trailing_entry_grid_min,
            y1=trailing_entry_grid_max,
            fillcolor='yellow',
            opacity=0.2,
            layer='below',
            line_width=0
        )
        # Add a dummy trace to represent Entry Grid in the legend
        fig.add_trace(go.Scatter(
            x=[None],
            y=[None],
            mode='lines',
            line=dict(color='rgba(255,255,0,0.2)', width=10),
            name='Entry Trailing (Area)',
            showlegend=True
        ))
        
    
    # Add closing grid range as a shaded area
    fig.add_shape(
        type='rect',
        xref='paper',
        x0=0,
        x1=1,
        yref='y',
        y0=normal_close_grid_min,
        y1=normal_close_grid_max,
        fillcolor='lightgreen',
        opacity=0.2,
        layer='below',
        line_width=0
    )
    # Add a dummy trace to represent Close Grid in the legend
    fig.add_trace(go.Scatter(
        x=[None],
        y=[None],
        mode='lines',
        line=dict(color='rgba(0,255,0,0.2)', width=10),
        name='Close Grid (Area)',
        showlegend=True
    ))

    # Add one Close Grid line and show it in the legend
    if len(normal_close_prices) > 0:
        fig.add_trace(go.Scatter(
            x=[0, 1],
            y=[normal_close_prices[0], normal_close_prices[0]],
            mode='lines',
            name='Close Grid (Lines)',
            line=dict(
                color='rgba(0, 255, 0, 0.6)',
                dash='dot',
                width=1
            ),
            showlegend=True,
            legendgroup='close'
        ))

        # Add the rest of the close lines without showing them individually in the legend
        for close_price in normal_close_prices[1:]:
            fig.add_trace(go.Scatter(
                x=[0, 1],
                y=[close_price, close_price],
                mode='lines',
                line=dict(
                    color='rgba(0, 255, 0, 0.6)',
                    dash='dot',
                    width=1
                ),
                showlegend=False,
                legendgroup='close'
            ))
        
    if trailing_close_grid_min != 0 and trailing_close_grid_max != 0:
        # Add close grid range as a shaded area
        fig.add_shape(
            type='rect',
            xref='paper',
            x0=0,
            x1=1,
            yref='y',
            y0=trailing_close_grid_min,
            y1=trailing_close_grid_max,
            fillcolor='blue',
            opacity=0.2,
            layer='below',
            line_width=0
        )
        # Add a close trace to represent Entry Grid in the legend
        fig.add_trace(go.Scatter(
            x=[None],
            y=[None],
            mode='lines',
            line=dict(color='rgba(0,0,255,0.2)', width=10),
            name='Close Trailing (Area)',
            showlegend=True
        ))

    # Determine Y-axis range
    all_prices_for_min = normal_entry_prices + normal_close_prices
    y_min = min(all_prices_for_min) - 15
    y_max = max(all_prices_for_min) + 15

    # Adjust Layout for Dark Mode
    fig.update_layout(
        template='plotly_dark',
        title=f'📈 {title_side} Entry and Close Grids Visualization',
        xaxis=dict(
            showticklabels=False,
            title=None  # This removes the x-axis title
        ),
        yaxis=dict(
            range=[y_min, y_max],
        ),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            bordercolor="rgba(0,0,0,0)"
        ),
        margin=dict(l=40, r=40, t=40, b=40),
        height=500,
        width=1400
    )

    # Render the figure using Streamlit
    st.plotly_chart(fig, use_container_width=True)

    # Display warnings
    if warnings:
        st.info("ℹ️ **Please note:**")
        for warning in warnings:
            st.write(f"⚠️ {warning}")
    
    st.markdown("---")
    return fig


def create_statistics(side: OrderType, data: GVData):
    
    if not data.isActive(side):
        return None
    
    entries = []
    closes = []
    wallet_exposure_limit = 0.0
    # Determine if we're dealing with LONG or SHORT side
    if side == OrderType.Default or side == Side.Long:
        title_side = "LONG"
        entries = data.normal_entries_long
        closes = data.normal_closes_long
        wallet_exposure_limit = data.normal_bot_params_long.wallet_exposure_limit
    else:
        title_side = "SHORT"
        entries = data.normal_entries_short
        closes = data.normal_closes_short
        wallet_exposure_limit = data.normal_bot_params_short.wallet_exposure_limit

    # Calculate statistics for entries
    total_entry_qty = sum(o.qty for o in entries)
    total_close_qty = sum(o.qty for o in closes)

    # Weighted average price for entries
    if total_entry_qty > 0:
        avg_entry_price = sum(o.qty * o.price for o in entries) / total_entry_qty
    else:
        avg_entry_price = None

    # Weighted average price for closes
    if total_close_qty > 0:
        avg_close_price = sum(o.qty * o.price for o in closes) / total_close_qty
    else:
        avg_close_price = None

    # Count of orders
    entry_count = len(entries)
    close_count = len(closes)

    # Display the main statistics as a table
    st.write(f"**{title_side} Statistics**")

    stats_data = {
        "Metric": [
            "Entry: Mode",
            "Entry: Orders",
            "Entry: Average Price",
            "Entry: Grid Size",
            "Close: Mode",
            "Close: Orders",
            "Close: Average Price",
            "Close: Grid Size",
        ],
        "Value": [
            str(data.long_entry_mode.name if title_side == "LONG" else data.short_entry_mode.name),
            str(entry_count),
            str(avg_entry_price) if avg_entry_price is not None else "N/A", 
            f"{int(data.long_entry_grid)}%" if title_side == "LONG" else f"{int(data.short_entry_grid)}%",
            str(data.long_close_mode.name if title_side == "LONG" else data.short_close_mode.name),
            str(close_count),
            str(avg_close_price) if avg_close_price is not None else "N/A",
            f"{int(data.long_close_grid)}%" if title_side == "LONG" else f"{int(data.short_close_grid)}%",
        ]
    }

    stats_df = pd.DataFrame(stats_data)
    st.table(stats_df)
    
    # Calulate Total Wallet Exposure
    entry_wallet_expore_sum = 0
    entry_twe_budegt = data.state_params.balance * wallet_exposure_limit
    entry_twe_pct = []
    
    for entry in entries:
        entry_wallet_expore_sum += entry.qty * entry.price
        entry_pct = int(entry_wallet_expore_sum / entry_twe_budegt * 100)
        if entry_pct < 0:
            entry_pct = 0
        if entry_pct > 100:
            entry_pct = 100
        entry_twe_pct.append(entry_pct)    
    
    # Detailed tables of entries and closes
    # For entries
    if entries:
        entry_details = {
            "Qty": [o.qty for o in entries],
            "Price": [o.price for o in entries],
            "Max-TWE% After": entry_twe_pct,
            "Order Type": [o.order_type.name for o in entries]
        }
        entry_df = pd.DataFrame(entry_details)
        st.write(f"**{title_side} Entry Orders**")
        st.table(entry_df)
    else:
        st.write(f"**{title_side} Entry Orders:** None")

    # Calulate Total Wallet Exposure
    close_wallet_expore_sum = data.state_params.balance * wallet_exposure_limit
    close_twe_budegt = data.state_params.balance * wallet_exposure_limit
    close_twe_pct = []
    
    for close in closes:
        close_wallet_expore_sum -= close.qty * close.price
        close_pct = int(close_wallet_expore_sum / close_twe_budegt * 100)
        if close_pct < 0:
            close_pct = 0
        if close_pct > 100:
            close_pct = 100
            
        close_twe_pct.append(close_pct)  
        
    # For closes
    if closes:
        close_details = {
            "Qty": [o.qty for o in closes],
            "Price": [o.price for o in closes],
            "Max-TWE% After": close_twe_pct,
            "Order Type": [o.order_type.name for o in closes]
        }
        close_df = pd.DataFrame(close_details)
        st.write(f"**{title_side} Close Orders**")
        st.table(close_df)
    else:
        st.write(f"**{title_side} Close Orders:** None")

def adjust_order_quantities(orders: List[Order]) -> List[Order]:
    for order in orders:
        order.qty = abs(order.qty)
    return orders
    
def show_visualizer():
    # Load the config
    data = prepare_config()
    
    # Title
    if not data.title == "":
        st.subheader(data.title)
    
    # Create columns for organizing parameters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        json_str = st.text_area("hidden",data.to_json(), height=1000, label_visibility = "collapsed")
        if st.button("Apply"):
            data = GVData.from_json(json_str)
            st.session_state.v7_grid_visualizer_data = data
            st.rerun()

    
    # NORMAL LONG ENTRIES
    normal_entries_long = calc_entries_long(data.exchange_params, data.state_params, data.normal_bot_params_long, data.position_long_enty, data.trailing_price_bundle)
    data.normal_entries_long = adjust_order_quantities(normal_entries_long) 
    # GRIDONLY LONG ENTRIES
    gridonly_entries_long = calc_entries_long(data.exchange_params, data.state_params, data.gridonly_bot_params_long, data.position_long_enty, data.trailing_price_bundle)
    data.gridonly_entries_long = adjust_order_quantities(gridonly_entries_long) 
    
    # NORMAL LONG CLOSES
    normal_closes_long = calc_closes_long(data.exchange_params, data.state_params, data.normal_bot_params_long, data.position_long_close, data.trailing_price_bundle)
    data.normal_closes_long = adjust_order_quantities(normal_closes_long) 
    # GRIDONLY LONG CLOSES
    gridonly_closes_long = calc_closes_long(data.exchange_params, data.state_params, data.gridonly_bot_params_long, data.position_long_close, data.trailing_price_bundle)
    data.gridonly_closes_long = adjust_order_quantities(gridonly_closes_long) 
    
    # NORMAL SHORT ENTRIES
    normal_entries_short = calc_entries_short(data.exchange_params, data.state_params, data.normal_bot_params_short, data.position_short_entry, data.trailing_price_bundle)
    data.normal_entries_short = adjust_order_quantities(normal_entries_short)
    # GRIDONLY SHORT ENTRIES
    gridonly_entries_short = calc_entries_short(data.exchange_params, data.state_params, data.gridonly_bot_params_short, data.position_short_entry, data.trailing_price_bundle)
    data.gridonly_entries_short = adjust_order_quantities(gridonly_entries_short)
    # NORMAL SHORT CLOSES
    normal_closes_short = calc_closes_short(data.exchange_params, data.state_params, data.normal_bot_params_short, data.position_short_close, data.trailing_price_bundle)
    data.normal_closes_short = adjust_order_quantities(normal_closes_short)
    # GRIDONLY SHORT CLOSES
    gridonly_closes_short = calc_closes_short(data.exchange_params, data.state_params, data.gridonly_bot_params_short, data.position_short_close, data.trailing_price_bundle)
    data.gridonly_closes_short = adjust_order_quantities(gridonly_closes_short)

    st.session_state.v7_grid_visualizer_data = data
    
    with col2:
        if data.isActive(Side.Long):
            create_plotly_graph(Side.Long, data)
            create_statistics(Side.Long, data)
        else:
            st.write("LONG is inactive")
    
    with col3:
        if data.isActive(Side.Short):
            create_plotly_graph(Side.Short, data)
            create_statistics(Side.Short, data)
        else:
            st.write("SHORT is inactive")


def build_sidebar():
    # Navigation
    with st.sidebar:
        if st.button("Reset"):
            if "v7_grid_visualizer_data" in st.session_state:
                del st.session_state.v7_grid_visualizer_data
            if "v7_grid_visualizer_config" in st.session_state:
                del st.session_state.v7_grid_visualizer_config
            st.rerun()

# Redirect to Login if not authenticated or session state not initialized
if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

# Page Setup
set_page_config("PBv7 Grid Visualizer")
st.header("PBv7 Grid Visualizer", divider="red")
st.info("Visualization of trailing parameters is currently very limited. Range of GridOnly-Mode is shown as trailing range. Threshold and Retracement have no effect.")

build_sidebar()
show_visualizer()
