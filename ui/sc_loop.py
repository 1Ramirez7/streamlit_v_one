"""
Loop Parameters UI Components

Provides Streamlit widgets for configuring loop parameters in multi-run simulations.
"""

import streamlit as st
import numpy as np


def build_loop_values(use_list, value_list, range_min, range_max, range_mode, range_param):
    """
    Build list of values based on configuration.
    """
    if use_list:
        return value_list
    
    if range_mode == 'all':
        # Every value in range
        return list(range(range_min, range_max + 1))
    
    elif range_mode == 'interval':
        # Every X interval
        values = list(range(range_min, range_max + 1, range_param))
        # Ensure max is included if not already
        if values[-1] != range_max:
            values.append(range_max)
        return values
    
    elif range_mode == 'count':
        # X evenly spaced values
        if range_param <= 1:
            return [range_max]
        return [int(round(v)) for v in np.linspace(range_min, range_max, range_param)]
    
    return list(range(range_min, range_max + 1))


def parse_list_input(text):
    """Parse comma-separated string into list of integers."""
    try:
        return [int(x.strip()) for x in text.split(',') if x.strip()]
    except ValueError:
        return []


def render_loop_params():
    """
    Render loop parameters UI for multi-run simulations.
    This is displayed in the Setup tab of the main area.
    
    Returns:
        dict: Dictionary containing parts_values, depot_values, and total_runs
    """
    st.subheader("🔄 Loop Parameters")
    
    # --- N TOTAL PARTS ---
    st.markdown("#### Total Parts")
    use_parts_list = st.checkbox("Use specific list", value=True, key="parts_list")
    
    if use_parts_list:
        parts_input = st.text_input("Values (comma-separated)", "851, 861, 871, 881, 891, 901, 911, 921, 931", key="parts_vals")
        parts_values = parse_list_input(parts_input)
    else:
        col1, col2 = st.columns(2)
        parts_min = col1.number_input("Min", value=851, key="parts_min")
        parts_max = col2.number_input("Max", value=931, key="parts_max")
        
        parts_range_mode = st.radio(
            "Range mode",
            ["All values", "Every X interval", "X evenly spaced"],
            key="parts_mode",
            horizontal=True
        )
        
        if parts_range_mode == "Every X interval":
            parts_interval = st.number_input("Interval", value=2, min_value=1, key="parts_interval")
            parts_values = build_loop_values(False, [], parts_min, parts_max, 'interval', parts_interval)
        elif parts_range_mode == "X evenly spaced":
            parts_count = st.number_input("Number of values", value=5, min_value=2, key="parts_count")
            parts_values = build_loop_values(False, [], parts_min, parts_max, 'count', parts_count)
        else:
            parts_values = build_loop_values(False, [], parts_min, parts_max, 'all', 1)
    
    st.caption(f"Parts values: {parts_values}")

    st.markdown("---")

    # --- DEPOT CAPACITY ---
    st.markdown("#### Depot Capacity")
    use_depot_list = st.checkbox("Use specific list", value=True, key="depot_list")
    
    if use_depot_list:
        depot_input = st.text_input("Values (comma-separated)", "29, 31, 33, 35, 37, 39, 41, 43, 45", key="depot_vals")
        depot_values = parse_list_input(depot_input)
    else:
        col1, col2 = st.columns(2)
        depot_min = col1.number_input("Min", value=29, key="depot_min")
        depot_max = col2.number_input("Max", value=45, key="depot_max")
        
        depot_range_mode = st.radio(
            "Range mode",
            ["All values", "Every X interval", "X evenly spaced"],
            key="depot_mode",
            horizontal=True
        )
        
        if depot_range_mode == "Every X interval":
            depot_interval = st.number_input("Interval", value=5, min_value=1, key="depot_interval")
            depot_values = build_loop_values(False, [], depot_min, depot_max, 'interval', depot_interval)
        elif depot_range_mode == "X evenly spaced":
            depot_count = st.number_input("Number of values", value=5, min_value=2, key="depot_count")
            depot_values = build_loop_values(False, [], depot_min, depot_max, 'count', depot_count)
        else:
            depot_values = build_loop_values(False, [], depot_min, depot_max, 'all', 1)
    
    st.caption(f"Depot values: {depot_values}")
    
    # Total runs
    total_runs = len(depot_values) * len(parts_values)
    st.markdown("---")
    st.metric("Total Simulations", total_runs)
    
    if total_runs > 500:
        st.warning(f"⚠️ {total_runs} runs may take a long time!")
    
    return {
        'parts_values': parts_values,
        'depot_values': depot_values,
        'total_runs': total_runs,
    }

