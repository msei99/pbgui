import streamlit as st  # Streamlit library for creating web apps
from pbgui_func import set_page_config, is_session_state_initialized, error_popup, is_pb7_installed
import numpy as np  # NumPy for numerical operations
import pandas as pd  # Pandas for data manipulation and analysis
import plotly.graph_objs as go  # Plotly for interactive data visualization

# Calculation of Statistics
def calculate_statistics(entry_prices, close_grid_tp_prices, entry_quantities, close_grid_tp_quantities):
    stats = {}
    stats['Number of Entry Levels'] = len(entry_prices)  # Total number of entry levels
    stats['Number of Close TP Levels'] = len(close_grid_tp_prices)  # Total number of close TP levels
    stats['Entry Prices Range'] = f"{min(entry_prices):.2f} - {max(entry_prices):.2f}"  # Range of entry prices
    # entry gid site %
    stats['Entry Grid Size'] = f"{100 - entry_prices[-1]:.2f}%"  # Range of entry prices
    stats['Close TP Prices Range'] = f"{min(close_grid_tp_prices):.2f} - {max(close_grid_tp_prices):.2f}"  # Range of close TP prices
    stats['Close Grid Size'] = f"{100 - close_grid_tp_prices[-1]:.2f}%"
    return stats  # Return the statistics dictionary

# Function to Calculate Entry Grid Levels
def calculate_entry_grid(start_price, start_balance, wallet_exposure_limit,
                         entry_grid_spacing_pct, entry_grid_spacing_weight, 
                         entry_grid_double_down_factor, entry_initial_qty_pct):

    # Initialize lists to hold entry prices and quantities
    entry_prices = []  # List to hold entry prices
    entry_quantities = []  # List to hold quantities at each entry price
    entry_exposures = []  # List to hold wallet exposures at each entry price
    entry_costs = []  # List to hold costs at each entry price
    iteration = 0  # Iteration counter   
    
    # Inital Entry
    price = start_price  # Set price to current price (statring point)
    size = entry_initial_qty_pct  # Starting quantity percentage
    costs = price * wallet_exposure_limit * entry_initial_qty_pct  # Calculate the entry cost
    entry_prices.append(price)  # Append the calculated price to the entry prices list
    entry_quantities.append(size)  # Append the quantity percentage to the quantities list
    entry_costs.append(costs)  # Append the initial entry cost to the costs list
    wallet_exposure = sum(entry_costs) / (start_balance)  # Calculate the wallet exposure
    entry_exposures.append(wallet_exposure)  # Append the wallet exposure to the exposures list
    wallet_exposure = (sum(entry_costs) + costs) / (start_balance) # Calculate the wallet exposure
    iteration += 1
    
    # Loop to calculate remaining entry grid levels    
    stop = False
    while not stop: 
        # Calculate the next entry price based on the parameters
        ratio = wallet_exposure / wallet_exposure_limit  # Calculate the exposure ratio
        modifier = (1 + (ratio * entry_grid_spacing_weight))  # Adjust the spacing modifier
        
        old_price = price
        price = price * (1 - (entry_grid_spacing_pct * modifier))  # Calculate the next entry price
        size = sum(entry_quantities) * entry_grid_double_down_factor  # Increase the quantity for the next level
        costs = price * size  # Calculate the costs for the next level
        wallet_exposure = (sum(entry_costs) + costs) / (start_balance)  # Calculate the wallet exposure
        iteration += 1  # Increment the iteration counter
        
        # Append the calculated price and quantity to the respective lists
        if wallet_exposure <= wallet_exposure_limit and price > 0:
            entry_prices.append(price)  # Append the calculated price to the entry prices list
            entry_quantities.append(size)  # Append the quantity percentage to the quantities list
            entry_costs.append(costs)  # Append the initial entry cost to the costs list
            entry_exposures.append(wallet_exposure)  # Append the wallet exposure to the exposures list
        else:
            # Special treatment for the last entry level
            if (wallet_exposure > wallet_exposure_limit) or (old_price > 0 and price < 0):
                entry_prices.append(price)
                
                # Calculate remaining funds, size and costs
                we_reamining = wallet_exposure_limit - entry_exposures[-1]
                funds_remaining = we_reamining * start_balance
                size = funds_remaining / price
                costs = price * size
                entry_quantities.append(size)  # Append the quantity percentage to the quantities list
                entry_costs.append(costs)  # Append the initial entry cost to the costs list
                wallet_exposure = sum(entry_costs) / (start_balance)
                entry_exposures.append(wallet_exposure)  # Append the wallet exposure to the exposures list
            stop = True
        if price <= 1:
            stop = True    
    return entry_prices, entry_quantities, entry_costs, entry_exposures  # Return the lists of entry prices and quantities

# Function to Calculate Close Grid Levels
def calculate_close_grid(start_price, close_grid_min_markup, close_grid_markup_range, close_grid_qty_pct):
    close_grid_tp_prices = []  # List to hold close take-profit prices
    close_grid_tp_quantities = []  # List to hold quantities at each close price

    num_tp_levels = int(1 / close_grid_qty_pct)  # Calculate the number of TP levels
    if(num_tp_levels*close_grid_qty_pct < 1):
        num_tp_levels += 1
        
    # Ensure at least one TP level
    num_tp_levels = max(num_tp_levels, 1)

    # Generate TP prices using numpy's linspace for even distribution
    tp_prices = np.linspace(
        start_price * (1 + close_grid_min_markup),  # Start price for TP
        start_price * (1 + close_grid_min_markup + close_grid_markup_range),  # End price for TP
        num_tp_levels  # Number of TP levels
    )

    for tp_price in tp_prices:
        close_grid_tp_prices.append(tp_price)
        
        # Special treatment for the last TP level
        if tp_price == tp_prices[-1]:
            close_grid_qty_pct = 1.0 - sum(close_grid_tp_quantities)
        else:
            close_grid_qty_pct = close_grid_qty_pct
        close_grid_tp_quantities.append(close_grid_qty_pct)
    return close_grid_tp_prices, close_grid_tp_quantities


def show_visualizer():
    st.warning("""
    ⚠️ This tool is ignoring trailing entry/closes and ema distances! \n
    It's pupose is to visualize the grid levels and help you to understand the PBv7 parameters better.\n""")

    st.header("📊 Settings")
    # Create four columns for organizing parameters
    col1, col2 = st.columns(2)

    # Entry Grid Parameters
    with col1:
        # Basic Settings
        with st.expander("Basics", expanded=False):
            start_balance = st.slider(
                "start_balance",
                min_value=500,
                max_value=10000,
                value=1000,
                step=100
            )

            wallet_exposure_limit = st.slider(
                "wallet_exposure_limit (%)",
                min_value=10,
                max_value=500,
                value=100,
                step=5
            ) / 100 
            
            start_price = st.slider(
                "start_price",
                min_value=10,
                max_value=500,
                value=100,
                step=5
            )
        # Expander for grouping entry grid parameters
        with st.expander("🔴 Entry Grid Parameters", expanded=True):
            # Slider for entry grid spacing percentage
            entry_grid_spacing_pct = st.slider(
                "entry_grid_spacing_pct (%)",
                min_value=1.0,
                max_value=25.0,
                value=20.0,
                step=0.1
            ) / 100  # Convert percentage to decimal

            # Slider for entry grid spacing weight
            entry_grid_spacing_weight = st.slider(
                "entry_grid_spacing_weight",
                min_value=0.0,
                max_value=20.0,
                value=1.0,
                step=0.1
            )

            # Slider for entry grid double down factor
            entry_grid_double_down_factor = st.slider(
                "entry_grid_double_down_factor",
                min_value=0.1,
                max_value=5.0,
                value=1.0,
                step=0.1
            )

            # Slider for initial quantity percentage
            entry_initial_qty_pct = st.slider(
                "entry_initial_qty_pct (%)",
                min_value=0.1,
                max_value=10.0,
                value=5.0,
                step=0.1
            ) / 100  # Convert percentage to decimal

    # Close Grid Parameters
        # Expander for grouping close grid parameters
        with st.expander("🟢 Close Grid Parameters", expanded=True):
            # Slider for close grid markup range
            close_grid_markup_range = st.slider(
                "close_grid_markup_range (%)",
                min_value=0.1,
                max_value=10.0,
                value=5.0,
                step=0.1
            ) / 100  # Convert percentage to decimal

            # Slider for close grid minimum markup
            close_grid_min_markup = st.slider(
                "close_grid_min_markup (%)",
                min_value=0.1,
                max_value=5.0,
                value=1.0,
                step=0.1
            ) / 100  # Convert percentage to decimal

            # Slider for close grid quantity percentage
            close_grid_qty_pct = st.slider(
                "close_grid_qty_pct (%)",
                min_value=1.0,
                max_value=100.0,
                value=20.0,
                step=1.0
            ) / 100  # Convert percentage to decimal


    # Calculate Entry Grid
    entry_prices, entry_quantities, entry_costs, entry_exposures = calculate_entry_grid(
        start_price=start_price, start_balance=start_balance, wallet_exposure_limit=wallet_exposure_limit,
        entry_grid_spacing_pct=entry_grid_spacing_pct,
        entry_grid_spacing_weight=entry_grid_spacing_weight,
        entry_grid_double_down_factor=entry_grid_double_down_factor,
        entry_initial_qty_pct=entry_initial_qty_pct
    )

    # Calculate Close Grid
    close_grid_tp_prices, close_grid_tp_quantities = calculate_close_grid(
        start_price=start_price,
        close_grid_min_markup=close_grid_min_markup,
        close_grid_markup_range=close_grid_markup_range,
        close_grid_qty_pct=close_grid_qty_pct
    )

    # Create Plotly Graph with Dark Mode
    fig = go.Figure()  # Initialize a new figure

    # Add a bold purple continuous horizontal line at start_price
    fig.add_trace(go.Scatter(
        x=[0,1],  # Start and end dates
        y=[start_price, start_price],  # Horizontal line at start_price
        mode='lines',
        name='Start Price',
        line=dict(
            color='purple',  # Purple color
            dash='solid',    # Continuous line
            width=4          # Bold line width
        )
    ))

    # Add enty grid range as a shaded area
    enty_grid_start = entry_prices[0] # Set the start of the entry grid range
    enty_grid_stop = entry_prices[-1] # Set the end of the entry grid range
    fig.add_shape(
        type='rect',
        xref='paper',  # Reference the full plot width
        x0=0,          # Start at the left of the plot
        x1=1,          # End at the right of the plot
        yref='y',      # Reference the y-axis for y-values
        y0=enty_grid_start,        # Start of the area on the y-axis
        y1=enty_grid_stop,        # End of the area on the y-axis
        fillcolor='red',
        opacity=0.2,   # Adjust transparency (0=transparent, 1=opaque)
        layer='below', # Draw the shape below data traces
        line_width=0,   # No border line
        name='Enty Grid Range',  # Name for the legend
        showlegend=True
    )

    # Add closing grid range as a shaded area
    close_grid_start = start_price * (1 + close_grid_min_markup)  # Calculate the start of the close grid range
    close_grid_stop = start_price * (1 + close_grid_min_markup + close_grid_markup_range)  # Calculate the end of the close grid range
    fig.add_shape(
        type='rect',
        xref='paper',  # Reference the full plot width
        x0=0,          # Start at the left of the plot
        x1=1,          # End at the right of the plot
        yref='y',      # Reference the y-axis for y-values
        y0=close_grid_start,        # Start of the area on the y-axis
        y1=close_grid_stop,        # End of the area on the y-axis
        fillcolor='lightgreen',
        opacity=0.2,   # Adjust transparency (0=transparent, 1=opaque)
        layer='below', # Draw the shape below data traces
        line_width=0,   # No border line
        name='Close Grid Range',  # Name for the legend
        showlegend=True
    )
        
    # Add Entry Grid lines in Red Dashes
    for entry_price in entry_prices:
        fig.add_trace(go.Scatter(
            x=[0,1],  # Horizontal line across the plot
            y=[entry_price, entry_price],  # Y-axis position for the entry grid line
            mode='lines',
            name='Entry Grid',  # Name for the legend
            line=dict(
                color='rgba(255, 0, 0, 0.6)',  # Red color with transparency
                dash='dash',  # Dashed line style
                width=1  # Line width
            ),
            showlegend=False
        ))

    # Add Close Grid lines in Green Dots
    for close_price in close_grid_tp_prices:
        fig.add_trace(go.Scatter(
            x=[0,1],  # Horizontal line across the plot
            y=[close_price, close_price],  # Y-axis position for the close grid line
            mode='lines',
            name='Close Grid',  # Name for the legend
            line=dict(
                color='rgba(0, 255, 0, 0.6)',  # Green color with transparency
                dash='dot',  # Dotted line style
                width=1  # Line width
            ),
            showlegend=False  # Do not show individual lines in the legend
        ))

    # Adjust Layout for Dark Mode
    all_prices_for_min = entry_prices + close_grid_tp_prices + [50]
    all_prices_for_max = entry_prices + close_grid_tp_prices + [120]
    y_min = min(all_prices_for_min) - 10
    y_max = max(all_prices_for_max) + 10

    fig.update_layout(
        template='plotly_dark',  # Use the dark theme
        title='📈 Entry and Close Grids Visualization',  # Title of the plot
        xaxis_title='No_Relevant_Data_Here',  # X-axis label
        yaxis_title='Price',  # Y-axis label
        yaxis=dict(
            range=[y_min, y_max],  # Set Y-axis range
        ),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",  # Transparent legend background
            bordercolor="rgba(0,0,0,0)"  # No border for the legend
        ),
        margin=dict(l=40, r=40, t=40, b=40),  # Margins around the plot
        height=700,  # Height of the plot in pixels
        width=1400  # Width of the plot in pixels
    )

    # Display the Graph
    with col2:
        st.plotly_chart(fig, use_container_width=True)  # Render the plotly chart in the Streamlit app

    stats = calculate_statistics(entry_prices, close_grid_tp_prices, entry_quantities, close_grid_tp_quantities)  # Compute statistics

    # Create four columns for organizing parameters
    c1, c2, c3 = st.columns(3)

    # Display Statistics
    with c1:
        st.subheader("📊 Statistics")  # Subheader for the statistics section
        stats_df = pd.DataFrame.from_dict(stats, orient='index', columns=['Value'])  # Create a DataFrame from the stats
        stats_df.index.name = 'Statistic'  # Name the index column
        st.table(stats_df)  # Display the statistics table

    # Display Entry Grid as Table
    with c2:
        # Prepare list of cumulated entry quantities
        entry_pos_sizes = np.cumsum(entry_quantities)
        
        st.subheader("🔴 Entry Grid Levels")  # Subheader for entry grid levels
        entry_grid_df = pd.DataFrame({
            'Entry Price': [f"{qty:.2f}" for qty in entry_prices],
            'Quantity': [f"{qty:.2f}" for qty in entry_quantities],
            # position size
            'Pos_Size': [f"{qty:.2f}" for qty in entry_pos_sizes],
            'Cost': [f"{qty:.2f}" for qty in entry_costs],
            'WE': [f"{qty*100:.2f}%" for qty in entry_exposures],
        })
        st.table(entry_grid_df)  # Display the entry grid DataFrame as a table

    # Display Close Grid as Table
    with c3:
        st.subheader("🟢 Close Grid Levels")  # Subheader for close grid levels
        close_grid_df = pd.DataFrame({
            'Close TP Price': [f"{qty:.2f}" for qty in close_grid_tp_prices],
            'Quantity': [f"{qty*100:.2f}%" for qty in close_grid_tp_quantities]  # Format quantities as percentages
        })
        st.table(close_grid_df)  # Display the close grid DataFrame as a table


set_page_config()

st.header("Passivbot V7 Grid Visualizer", divider="red")  # Display the app title with an icon

# Init session states
if is_session_state_initialized():
    st.switch_page("pbgui.py")

show_visualizer()  # Call the show_visualizer function to display the grid visualizer