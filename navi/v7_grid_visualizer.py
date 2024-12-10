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

@dataclass
class GVData:
    # Configuration parameters (to be exported/imported to JSON)
    start_balance: int = 1000
    start_price: int = 100
    long_total_wallet_exposure_limit: float = 1.0
    long_entry_grid_spacing_pct: float = 0.06
    long_entry_grid_spacing_weight: float = 8.0
    long_entry_grid_double_down_factor: float = 1.0
    long_entry_initial_qty_pct: float = 0.05
    long_close_grid_min_markup: float = 0.03
    long_close_grid_markup_range: float = 0.03
    long_close_grid_qty_pct: float = 0.2

    # Computed data (not to be exported/imported)
    long_entry_prices: List[float] = field(default_factory=list)
    long_entry_quantities: List[float] = field(default_factory=list)
    long_entry_costs: List[float] = field(default_factory=list)
    long_entry_exposures: List[float] = field(default_factory=list)
    long_close_grid_tp_prices: List[float] = field(default_factory=list)
    long_close_grid_tp_quantities: List[float] = field(default_factory=list)

    # TODO: SHORT (not to be exported/imported)

    # Everything else (not to be exported/imported)
    is_external_config: bool = False
    title: str = ""

    def to_json(self) -> str:
        # Include only the configuration parameters
        allowed_fields = [
            "start_balance",
            "start_price",
            "long_total_wallet_exposure_limit",
            "long_entry_grid_spacing_pct",
            "long_entry_grid_spacing_weight",
            "long_entry_grid_double_down_factor",
            "long_entry_initial_qty_pct",
            "long_close_grid_min_markup",
            "long_close_grid_markup_range",
            "long_close_grid_qty_pct",
        ]
        
        data_dict = asdict(self)
        filtered_data_dict = {k: data_dict[k] for k in allowed_fields}
        return json.dumps(filtered_data_dict, indent=4)

    @classmethod
    def from_json(cls, json_str: str) -> 'GVData':
        # Load JSON and filter only the allowed fields for initialization
        allowed_fields = {
            "start_balance": int,
            "start_price": int,
            "long_total_wallet_exposure_limit": float,
            "long_entry_grid_spacing_pct": float,
            "long_entry_grid_spacing_weight": float,
            "long_entry_grid_double_down_factor": float,
            "long_entry_initial_qty_pct": float,
            "long_close_grid_min_markup": float,
            "long_close_grid_markup_range": float,
            "long_close_grid_qty_pct": float,
        }

        data = json.loads(json_str)
        filtered_data = {}

        for field_name, field_type in allowed_fields.items():
            if field_name in data:
                filtered_data[field_name] = field_type(data[field_name])

        return cls(**filtered_data)

    

# Calculation of Statistics
def calculate_statistics(data: GVData):
    entry_prices = data.long_entry_prices
    close_grid_tp_prices = data.long_close_grid_tp_prices
    entry_quantities = data.long_entry_quantities
    close_grid_tp_quantities = data.long_close_grid_tp_quantities

    stats = {}
    stats['Number of Entry Levels'] = len(entry_prices)  # Total number of entry levels
    stats['Number of Close TP Levels'] = len(close_grid_tp_prices)  # Total number of close TP levels
    stats['Entry Prices Range'] = f"{min(entry_prices):.2f} - {max(entry_prices):.2f}"
    stats['Entry Grid Size'] = f"{100 - entry_prices[-1]:.2f}%"
    stats['Close TP Prices Range'] = f"{min(close_grid_tp_prices):.2f} - {max(close_grid_tp_prices):.2f}"
    stats['Close Grid Size'] = f"{100 - close_grid_tp_prices[-1]:.2f}%"
    return stats

# Function to Load Config
def get_config() -> GVData:
    # If there's no ConfigV7 in the session, load (probably passed from another page)
    if "v7_grid_visualizer_config" in st.session_state:
        # Build GVData from v7 config
        config_v7: ConfigV7 = st.session_state.v7_grid_visualizer_config
        del st.session_state.v7_grid_visualizer_config
        
        # Build Title identifying the config
        title = f"Loaded Configuration: {config_v7.pbgui.note} (v{config_v7.pbgui.version})"
        
        return GVData(
            start_balance=1000,
            start_price=100,
            long_total_wallet_exposure_limit=config_v7.bot.long.total_wallet_exposure_limit,
            long_entry_grid_spacing_pct=config_v7.bot.long.entry_grid_spacing_pct,
            long_entry_grid_spacing_weight=config_v7.bot.long.entry_grid_spacing_weight,
            long_entry_grid_double_down_factor=config_v7.bot.long.entry_grid_double_down_factor,
            long_entry_initial_qty_pct=config_v7.bot.long.entry_initial_qty_pct,
            long_close_grid_min_markup=config_v7.bot.long.close_grid_min_markup,
            long_close_grid_markup_range=config_v7.bot.long.close_grid_markup_range,
            long_close_grid_qty_pct=config_v7.bot.long.close_grid_qty_pct,
            title=title,
            is_external_config=True
        )
    
    # If there's a data object in the session, use it (e.g. from editor)
    if "v7_grid_visualizer_data" in st.session_state:
        return st.session_state.v7_grid_visualizer_data
    
    return GVData()

# Function to Calculate Entry Grid Levels
def calculate_long_entry_grid(data: GVData) -> GVData:
    entry_prices = []
    entry_quantities = []
    entry_exposures = []
    entry_costs = []
    
    price = data.start_price
    size = data.long_entry_initial_qty_pct
    costs = price * data.long_total_wallet_exposure_limit * data.long_entry_initial_qty_pct
    entry_prices.append(price)
    entry_quantities.append(size)
    entry_costs.append(costs)
    wallet_exposure = sum(entry_costs) / data.start_balance
    entry_exposures.append(wallet_exposure)
    wallet_exposure = (sum(entry_costs) + costs) / data.start_balance

    stop = False
    while not stop:
        ratio = wallet_exposure / data.long_total_wallet_exposure_limit
        modifier = (1 + (ratio * data.long_entry_grid_spacing_weight))

        old_price = price
        price = price * (1 - (data.long_entry_grid_spacing_pct * modifier))
        size = sum(entry_quantities) * data.long_entry_grid_double_down_factor
        costs = price * size
        wallet_exposure = (sum(entry_costs) + costs) / data.start_balance

        if wallet_exposure <= data.long_total_wallet_exposure_limit and price > 0:
            entry_prices.append(price)
            entry_quantities.append(size)
            entry_costs.append(costs)
            entry_exposures.append(wallet_exposure)
        else:
            # Special treatment for the last entry level
            if (wallet_exposure > data.long_total_wallet_exposure_limit) or (old_price > 0 and price < 0):
                entry_prices.append(price)
                we_reamining = data.long_total_wallet_exposure_limit - entry_exposures[-1]
                funds_remaining = we_reamining * data.start_balance
                size = funds_remaining / price
                costs = price * size
                entry_quantities.append(size)
                entry_costs.append(costs)
                wallet_exposure = sum(entry_costs) / data.start_balance
                entry_exposures.append(wallet_exposure)
            stop = True
        if price <= 1:
            stop = True

    data.long_entry_prices = entry_prices
    data.long_entry_quantities = entry_quantities
    data.long_entry_costs = entry_costs
    data.long_entry_exposures = entry_exposures
    return data

# Function to Calculate Close Grid Levels
def calculate_long_close_grid(data: GVData) -> GVData:
    close_grid_tp_prices = []
    close_grid_tp_quantities = []

    num_tp_levels = int(1 / data.long_close_grid_qty_pct)
    if (num_tp_levels * data.long_close_grid_qty_pct < 1):
        num_tp_levels += 1

    num_tp_levels = max(num_tp_levels, 1)

    tp_prices = np.linspace(
        data.start_price * (1 + data.long_close_grid_min_markup),
        data.start_price * (1 + data.long_close_grid_min_markup + data.long_close_grid_markup_range),
        num_tp_levels
    )

    for tp_price in tp_prices:
        close_grid_tp_prices.append(tp_price)
        # Special treatment for the last TP level
        if tp_price == tp_prices[-1]:
            close_grid_tp_quantities.append(1.0 - sum(close_grid_tp_quantities))
        else:
            close_grid_tp_quantities.append(data.long_close_grid_qty_pct)

    data.long_close_grid_tp_prices = close_grid_tp_prices
    data.long_close_grid_tp_quantities = close_grid_tp_quantities
    return data

def build_statistics(data: GVData):
        stats = calculate_statistics(data)

        # Create three columns for stats and tables
        c1, c2, c3 = st.columns(3)

        # Display Statistics
        with c1:
            st.subheader("📊 Statistics")
            stats_df = pd.DataFrame.from_dict(stats, orient='index', columns=['Value'])
            stats_df.index.name = 'Statistic'
            stats_df['Value'] = stats_df['Value'].astype(str)
            st.table(stats_df)

        # Display Entry Grid as Table
        with c2:
            entry_pos_sizes = np.cumsum(data.long_entry_quantities)
            
            st.subheader("🔴 Entry Grid Levels")
            entry_grid_df = pd.DataFrame({
                'Entry Price': [f"{qty:.2f}" for qty in data.long_entry_prices],
                'Quantity': [f"{qty:.2f}" for qty in data.long_entry_quantities],
                'Pos_Size': [f"{qty:.2f}" for qty in entry_pos_sizes],
                'Cost': [f"{qty:.2f}" for qty in data.long_entry_costs],
                'WE': [f"{qty*100:.2f}%" for qty in data.long_entry_exposures],
            })
            st.table(entry_grid_df)

        # Display Close Grid as Table
        with c3:
            st.subheader("🟢 Close Grid Levels")
            close_grid_df = pd.DataFrame({
                'Close TP Price': [f"{qty:.2f}" for qty in data.long_close_grid_tp_prices],
                'Quantity': [f"{qty*100:.2f}%" for qty in data.long_close_grid_tp_quantities]
            })
            st.table(close_grid_df)

def create_plotly_graph(data: GVData):
    # Create Plotly Graph with Dark Mode
    fig = go.Figure()

    # Add a bold purple continuous horizontal line at start_price
    fig.add_trace(go.Scatter(
        x=[0, 1],
        y=[data.start_price, data.start_price],
        mode='lines',
        name='Start Price',
        line=dict(
            color='purple',
            dash='solid',
            width=4
        )
    ))

    # Add entry grid range as a shaded area
    enty_grid_start = data.long_entry_prices[0]
    enty_grid_stop = data.long_entry_prices[-1]
    fig.add_shape(
        type='rect',
        xref='paper',
        x0=0,
        x1=1,
        yref='y',
        y0=enty_grid_start,
        y1=enty_grid_stop,
        fillcolor='red',
        opacity=0.2,
        layer='below',
        line_width=0,
        name='Enty Grid Range',
        showlegend=True
    )

    # Add closing grid range as a shaded area
    close_grid_start = data.start_price * (1 + data.long_close_grid_min_markup)
    close_grid_stop = data.start_price * (1 + data.long_close_grid_min_markup + data.long_close_grid_markup_range)
    fig.add_shape(
        type='rect',
        xref='paper',
        x0=0,
        x1=1,
        yref='y',
        y0=close_grid_start,
        y1=close_grid_stop,
        fillcolor='lightgreen',
        opacity=0.2,
        layer='below',
        line_width=0,
        name='Close Grid Range',
        showlegend=True
    )

    # Add Entry Grid lines in Red Dashes
    for entry_price in data.long_entry_prices:
        fig.add_trace(go.Scatter(
            x=[0, 1],
            y=[entry_price, entry_price],
            mode='lines',
            name='Entry Grid',
            line=dict(
                color='rgba(255, 0, 0, 0.6)',
                dash='dash',
                width=1
            ),
            showlegend=False
        ))

    # Add Close Grid lines in Green Dots
    for close_price in data.long_close_grid_tp_prices:
        fig.add_trace(go.Scatter(
            x=[0, 1],
            y=[close_price, close_price],
            mode='lines',
            name='Close Grid',
            line=dict(
                color='rgba(0, 255, 0, 0.6)',
                dash='dot',
                width=1
            ),
            showlegend=False
        ))

    # Adjust Layout for Dark Mode
    all_prices_for_min = data.long_entry_prices + data.long_close_grid_tp_prices + [50]
    all_prices_for_max = data.long_entry_prices + data.long_close_grid_tp_prices + [120]
    y_min = min(all_prices_for_min) - 10
    y_max = max(all_prices_for_max) + 10

    fig.update_layout(
        template='plotly_dark',
        title='📈 LONG Entry and Close Grids Visualization',
        xaxis_title='No_Relevant_Data_Here',
        yaxis_title='Price',
        yaxis=dict(
            range=[y_min, y_max],
        ),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            bordercolor="rgba(0,0,0,0)"
        ),
        margin=dict(l=40, r=40, t=40, b=40),
        height=700,
        width=1400
    )

    return fig

def display_long_entry_grid_parameters(data: GVData) -> GVData:
    # Basic Settings
    with st.expander("Basics", expanded=False):
        start_balance = st.slider(
            "start_balance",
            min_value=500,
            max_value=10000,
            value=int(data.start_balance),
            step=100
        )

        wallet_exposure_limit = st.slider(
            "wallet_exposure_limit (%)",
            min_value=10,
            max_value=500,
            value=int(data.long_total_wallet_exposure_limit * 100),
            step=5
        ) / 100.0

        start_price = st.slider(
            "start_price",
            min_value=10,
            max_value=500,
            value=int(data.start_price),
            step=5
        )

    # Expander for grouping entry grid parameters
    with st.expander("🔴 LONG Entry Grid Parameters", expanded=True):
        entry_grid_spacing_pct = st.slider(
            "entry_grid_spacing_pct (%)",
            min_value=1.0,
            max_value=25.0,
            value=float(data.long_entry_grid_spacing_pct * 100),
            step=0.1
        ) / 100.0

        entry_grid_spacing_weight = st.slider(
            "entry_grid_spacing_weight",
            min_value=0.0,
            max_value=20.0,
            value=float(data.long_entry_grid_spacing_weight),
            step=0.1
        )

        entry_grid_double_down_factor = st.slider(
            "entry_grid_double_down_factor",
            min_value=0.1,
            max_value=5.0,
            value=float(data.long_entry_grid_double_down_factor),
            step=0.1
        )

        entry_initial_qty_pct = st.slider(
            "entry_initial_qty_pct (%)",
            min_value=0.1,
            max_value=10.0,
            value=float(data.long_entry_initial_qty_pct * 100),
            step=0.1
        ) / 100.0

    # Expander for grouping close grid parameters
    with st.expander("🟢 LONG Close Grid Parameters", expanded=True):
        close_grid_markup_range = st.slider(
            "close_grid_markup_range (%)",
            min_value=0.1,
            max_value=10.0,
            value=float(data.long_close_grid_markup_range * 100),
            step=0.1
        ) / 100.0

        close_grid_min_markup = st.slider(
            "close_grid_min_markup (%)",
            min_value=0.1,
            max_value=5.0,
            value=float(data.long_close_grid_min_markup * 100),
            step=0.1
        ) / 100.0

        close_grid_qty_pct = st.slider(
            "close_grid_qty_pct (%)",
            min_value=1.0,
            max_value=100.0,
            value=float(data.long_close_grid_qty_pct * 100),
            step=1.0
        ) / 100.0

    # Update the config object with the new values from the sliders
    data = GVData(
        start_balance=start_balance,
        start_price=start_price,
        long_total_wallet_exposure_limit=wallet_exposure_limit,
        long_entry_grid_spacing_pct=entry_grid_spacing_pct,
        long_entry_grid_spacing_weight=entry_grid_spacing_weight,
        long_entry_grid_double_down_factor=entry_grid_double_down_factor,
        long_entry_initial_qty_pct=entry_initial_qty_pct,
        long_close_grid_min_markup=close_grid_min_markup,
        long_close_grid_markup_range=close_grid_markup_range,
        long_close_grid_qty_pct=close_grid_qty_pct
    )

    return data
    
def show_visualizer():
    # Load the config
    data = get_config()
    
    # Titele
    if not data.title == "":
        st.subheader(data.title)
    
    # Create columns for organizing parameters
    col1, col2 = st.columns(2)
    
    with col1:
        # Display the sliders for the parameters   
        options = ["Sliders", "JSON"]
        selection = st.segmented_control("Edit Mode:", options, selection_mode="single", default="Sliders", key="v7_grid_visualizer_edit_mode")

        #if "Sliders" in selection:
        if selection == "Sliders":
            data = display_long_entry_grid_parameters(data)
        else:
            json_str = st.text_area("JSON Editor", data.to_json(), height=300)
            if st.button("Apply"):
                data = GVData.from_json(json_str)
                st.session_state.v7_grid_visualizer_data = data
                st.rerun()

    # Save the data object in the session
    st.session_state.v7_grid_visualizer_data = data
    
    # Calculate Entry Grid
    data = calculate_long_entry_grid(data)

    # Calculate Close Grid
    data = calculate_long_close_grid(data)

    # Create and display the graph
    fig = create_plotly_graph(data)
    with col2:
        st.plotly_chart(fig, use_container_width=True)

    # Call the function to build statistics
    build_statistics(data)

def build_sidebar():
    # Navigation
    with st.sidebar:
        # TODO: Load V7 Config (RUN)
        # if st.button("Load V7 Config"):
        #     st.session_state.v7_grid_visualizer_config = st.session_state.v7_instances.instances[0].config
        #     st.rerun()
        if st.button("Reset"):
            if "v7_grid_visualizer_data" in st.session_state:
                del st.session_state.v7_grid_visualizer_data
                st.session_state.v7_grid_visualizer_edit_mode = "Sliders"
            st.rerun()

# Redirect to Login if not authenticated or session state not initialized
if not is_authenticted() or is_session_state_not_initialized():
    st.switch_page(get_navi_paths()["SYSTEM_LOGIN"])
    st.stop()

# Page Setup
set_page_config("PBv7 Grid Visualizer")
st.header("PBv7 Grid Visualizer", divider="red")

build_sidebar()
show_visualizer()
