"""
Scenarios UI Components

Provides Streamlit sidebar widgets for scenarios simulation parameter input.
Combines multi_run and multi_fast functionality with a Fast Mode toggle.
"""

import streamlit as st
import numpy as np
from utils import init_fleet_random, init_depot_random, weibull_mean


def render_scenarios_sidebar():
    """
    Render sidebar with scenarios simulation parameters.
    
    Includes Fast Mode toggle at top. When Fast Mode is ON:
    - render_plots is set to False
    - use_percentage_plots option is hidden (not needed)
    
    Returns:
        dict: All sidebar parameter values including 'fast_mode' flag
    """
    
    # ================================================================
    # FAST MODE TOGGLE (at top of sidebar)
    # ================================================================
    st.sidebar.header("⚡ Run Mode")
    fast_mode = st.sidebar.checkbox(
        "Fast Mode",
        value=False,
        help="Enable for faster runs without time series plot generation. Use for large parameter sweeps.",
        key="scenario_fast_mode"
    )
    
    if fast_mode:
        st.sidebar.info("⚡ Fast Mode: Plot rendering disabled for speed.")
    
    st.sidebar.markdown("---")
    
    # ================================================================
    # SIDEBAR: FIXED PARAMETERS
    # ================================================================
    st.sidebar.header("Fixed Parameters")

    n_total_aircraft = st.sidebar.number_input(
        "Total Aircraft", min_value=1, value=826, step=1,
        key="scenario_n_total_aircraft"
    )

    st.sidebar.markdown("**Simulation Timeline**")
    analysis_periods = st.sidebar.number_input(
        "Analysis Periods (days)", min_value=1, value=7300, step=1,
        key="scenario_analysis_periods"
    )

    condemn_cycle = st.sidebar.number_input(
        "Condemn at Cycle", min_value=2, value=1000, step=1,
        key="scenario_condemn_cycle"
    )
    condemn_depot_fraction = st.sidebar.number_input(
        "Condemned Depot Time Fraction", min_value=0.0, max_value=1.0, value=0.10, step=0.01,
        key="scenario_condemn_depot_fraction"
    )

    part_order_lag = st.sidebar.number_input(
        "Part Order Lag (days)", min_value=0, value=365, step=1,
        key="scenario_part_order_lag"
    )
    
    random_seed = st.sidebar.number_input(
        "Random Seed", min_value=1, value=132, step=1,
        key="scenario_random_seed"
    )

    mission_capable_rate = st.sidebar.number_input(
        "Mission Capable Rate", min_value=0.0, max_value=1.0, value=0.92, step=0.01,
        key="scenario_mission_capable_rate"
    )
    
    # Distribution settings
    distribution_selections = ["Normal", "Weibull"]
    st.sidebar.markdown("---")
    st.sidebar.subheader("Distribution Selection")
    sone_dist = st.sidebar.selectbox(
        "Fleet Distribution", options=distribution_selections, index=0,
        key="scenario_sone_dist"
    )
    sthree_dist = st.sidebar.selectbox(
        "Depot Distribution", options=distribution_selections, index=1,
        key="scenario_sthree_dist"
    )

    st.sidebar.markdown("---")
    st.sidebar.subheader("Fleet: Fleet (Part on Aircraft)")
    if sone_dist == distribution_selections[0]:  # Normal
        sone_mean = st.sidebar.number_input(
            "Fleet Mean Duration", value=700.0, min_value=1.0,
            key="scenario_sone_mean"
        )
        sone_sd = st.sidebar.number_input(
            "Fleet Std Dev", value=140.0, min_value=0.01,
            key="scenario_sone_sd"
        )
        fleet_mean_for_buffer = sone_mean
    elif sone_dist == distribution_selections[1]:  # Weibull
        sone_mean = st.sidebar.number_input(
            "Fleet Shape", value=46.099, min_value=1.0,
            key="scenario_sone_mean"
        )
        sone_sd = st.sidebar.number_input(
            "Fleet Scale", value=36.946, min_value=0.01,
            key="scenario_sone_sd"
        )
        fleet_mean_for_buffer = weibull_mean(sone_mean, sone_sd)

    st.sidebar.subheader("Depot")
    if sthree_dist == distribution_selections[0]:  # Normal
        sthree_mean = st.sidebar.number_input(
            "Depot Mean Duration", value=20.0, min_value=1.0,
            key="scenario_sthree_mean"
        )
        sthree_sd = st.sidebar.number_input(
            "Depot Std Dev", value=2.0, min_value=0.01,
            key="scenario_sthree_sd"
        )
    elif sthree_dist == distribution_selections[1]:  # Weibull
        sthree_mean = st.sidebar.number_input(
            "Depot Shape", value=6.11, min_value=1.0,
            key="scenario_sthree_mean"
        )
        sthree_sd = st.sidebar.number_input(
            "Depot Scale", value=22.61, min_value=0.01,
            key="scenario_sthree_sd"
        )
    
    # ----- INITIAL CONDITIONS -----
    st.sidebar.header("Initial Conditions")

    st.sidebar.markdown("**Buffer Time**")
    double_periods = st.sidebar.checkbox(
        "Add Buffer Time",
        value=True,
        help="If checked, The Fleet duration is multiplied by the buffer multiplier and split between the beginning and end of Simulation.",
        key="scenario_double_periods"
    )

    buffer_multiplier = st.sidebar.number_input(
        "Buffer Multiplier",
        min_value=1,
        value=2,
        step=1,
        help="Multiplier for warmup and closing periods (e.g., 2 means warmup = fleet_mean * 2)",
        disabled=not double_periods,
        key="scenario_buffer_multiplier"
    )

    warmup_periods = fleet_mean_for_buffer * buffer_multiplier
    closing_periods = fleet_mean_for_buffer * buffer_multiplier

    if double_periods:
        sim_time = warmup_periods + analysis_periods + closing_periods
    else:
        sim_time = analysis_periods
        warmup_periods = 0
        closing_periods = 0

    st.sidebar.info(f"**Total Simulation Time: {sim_time} days**")

    # Plot display option - only show when NOT in fast mode
    use_percentage_plots = True  # Default
    if not fast_mode:
        st.sidebar.markdown("**Plot Display**")
        use_percentage_plots = st.sidebar.checkbox(
            "Show Plots as Percentage",
            value=True,
            help="If checked, WIP plots display values as percentages. If unchecked, plots show raw counts.",
            key="scenario_use_percentage_plots"
        )

    # Get randomization parameters
    fleet_rand_params = init_fleet_random()
    depot_rand_params = init_depot_random()

    # Return all sidebar values
    return {
        'fast_mode': fast_mode,
        'n_total_aircraft': n_total_aircraft,
        'analysis_periods': analysis_periods,
        'condemn_cycle': condemn_cycle,
        'condemn_depot_fraction': condemn_depot_fraction,
        'part_order_lag': part_order_lag,
        'random_seed': random_seed,
        'mission_capable_rate': mission_capable_rate,
        'sone_dist': sone_dist,
        'sthree_dist': sthree_dist,
        'sone_mean': sone_mean,
        'sone_sd': sone_sd,
        'sthree_mean': sthree_mean,
        'sthree_sd': sthree_sd,
        'double_periods': double_periods,
        'warmup_periods': warmup_periods,
        'closing_periods': closing_periods,
        'sim_time': sim_time,
        'use_percentage_plots': use_percentage_plots,
        'fleet_rand_params': fleet_rand_params,
        'depot_rand_params': depot_rand_params,
    }
