"""
UI Components for DES Simulation

Provides Streamlit sidebar widgets for simulation parameter input.
All parameters match main_r-code.R exactly.
"""

import streamlit as st
from utils import render_allocation_inputs, init_fleet_random, init_depot_random, solve_weibull_params
import numpy as np

def render_sidebar():
    """Render sidebar with simulation parameters matching main_r-code.R exactly."""
    
    st.sidebar.header("Simulation Parameters")
    
    # Toggle for plot rendering
    render_plots = st.sidebar.checkbox(
        "Render Plots",
        value=True,
        help="Uncheck to skip plot rendering (faster for testing)"
    )

    # Basic parameters
    n_total_parts = st.sidebar.number_input(
        "Total Parts",
        min_value=1,
        value=891,  # 1100
        step=1,
        help="Total number of parts in the system"
    )
    
    n_total_aircraft = st.sidebar.number_input(
        "Total Aircraft",
        min_value=1,
        value=826,  # 900
        step=1,
        help="Total number of aircraft in the fleet"
    )
    
    # Simulation time broken into 3 components
    st.sidebar.markdown("**Simulation Timeline**")
    # warmup_periods will be set after sone_mean is defined
    
    analysis_periods = st.sidebar.number_input(
        "Simulation Time (days)",
        min_value=1,
        value=7800,
        step=1,
        help="Number of days for analysis period"
    )

    # NEW: Part condemn parameters
    st.sidebar.subheader("Part Condemn Parameters")
    
    """
    The model is programmed to condemn parts after a specified number of cycles.
    Randomizing initial cycles requires `condemn_cycle` to be greater than 1.
     - np.random.randint(1, condemn_cycle)
     - With condemn_cycle = 1, this is randint(1, 1) which raises ValueError: low >= high

     
    `SimulationEngine.initialize_depot` is not designed to handle part condemnation.

    The cycle randomization logic avoids generating values that would 
    immediately cause a part to be condemned.

    Special handling will be required if `condemn_cycle` is ever set to 1.

    - If it is set to 1
        - edit depot random function to not generate condemn value
        - a part can't be condemn and in condition A per mdoel logic
        - this function follows same logic as all other random cycle gen
    - edit engine.initialize_depot to handle condemnations
    - continue list
    """

    depot_capacity = st.sidebar.number_input(
        "Depot Capacity",
        min_value=1,
        value=37,
        step=1,
        help="Maximum number of parts that can be in the depot at once"
    )

    condemn_cycle = st.sidebar.number_input(
        "Condemn at Cycle",
        min_value=2,
        value=1000,
        step=1,
        help="Cycle number at which parts are condemned and replaced"
    )
    
    condemn_depot_fraction = st.sidebar.number_input(
        "Condemned Depot Time Fraction",
        min_value=0.0,
        max_value=1.0,
        value=0.10,
        step=0.01,
        format="%.2f",
        help="Fraction of normal depot time for condemned parts (e.g., 0.10 = 10% of normal time). The norm.dist will still be done and result will be multiply by 10%."
    )

    part_order_lag = st.sidebar.number_input(
        "Part Order Lag (days)",
        min_value=0,
        value=365, # 100
        step=1,
        help="Time delay between ordering a new part and it arriving in Condition A"
    )

    random_seed = st.sidebar.number_input(
        "Random Seed",
        min_value=1,
        value=132,
        step=1,
        help="Random seed for reproducibility"
    )

    mission_capable_rate = st.sidebar.number_input(
        "Mission Capable Rate",
        min_value=0.0,
        max_value=1.0,
        value=0.92,
        step=0.01,
        format="%.2f",
        help="The percentage of total aircraft that start in Fleet with a part (0.0 to 1.0)"
    )


# --- NEW: Initial Part Allocation Inputs ---
    parts_air_dif = n_total_parts - min(n_total_parts, int(np.ceil(mission_capable_rate * n_total_aircraft)))
    
    parts_in_depot, parts_in_cond_f, parts_in_cond_a = render_allocation_inputs(
        n_total_parts=n_total_parts,
        n_total_aircraft=n_total_aircraft,
        mission_capable_rate=mission_capable_rate,
        depot_capacity=depot_capacity,
        parts_air_dif=parts_air_dif
    )

    distribution_selections = ["Normal", "Weibull"]
    st.sidebar.markdown("---")
    st.sidebar.subheader("Distribution Selection")
    sone_dist = st.sidebar.selectbox("Fleet Distribution", options=distribution_selections, index=0)
    sthree_dist = st.sidebar.selectbox("Depot Distribution", options=distribution_selections, index=1)
    
    # --- Weibull Parameter Calculator ---
    st.sidebar.markdown("---")
    st.sidebar.subheader("Weibull Parameter Calculator")
    st.sidebar.caption("Convert mean and standard deviation to Weibull shape and scale parameters")
    
    wei_mean = st.sidebar.number_input(
        "Mean",
        value=360.0,
        min_value=1.0,
        step=1.0,
        key="wei_mean"
    )
    
    wei_std = st.sidebar.number_input(
        "Standard Deviation",
        value=10.0,
        min_value=0.01,
        step=0.1,
        key="wei_std"
    )
    
    # Calculate Weibull parameters
    wei_shape, wei_scale = solve_weibull_params(wei_mean, wei_std)
    
    if wei_shape is not None and wei_scale is not None:
        col1, col2 = st.sidebar.columns(2)
        with col1:
            st.metric("Shape (k)", f"{wei_shape:.3f}")
        with col2:
            st.metric("Scale (λ)", f"{wei_scale:.3f}")
    else:
        st.sidebar.warning("Could not calculate Weibull parameters")
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("Fleet: Fleet (Part on Aircraft)")
    if sone_dist == distribution_selections[0]: #Normal
        sone_mean = st.sidebar.number_input("Fleet Mean Duration", value=700.0, min_value=1.0)
        sone_sd = st.sidebar.number_input("Fleet Std Dev", value=140.0, min_value=0.01)
    elif sone_dist == distribution_selections[1]: #Weibull
        sone_mean = st.sidebar.number_input("Fleet Shape", value=46.099, min_value=1.0)
        sone_sd = st.sidebar.number_input("Fleet Scale", value=36.946, min_value=0.01)

    st.sidebar.subheader("Depot")
    if sthree_dist == distribution_selections[0]: #Normal
        sthree_mean = st.sidebar.number_input("Depot Mean Duration", value=20.0, min_value=1.0)
        sthree_sd = st.sidebar.number_input("Depot Std Dev", value=2.0, min_value=0.01)
    elif sthree_dist == distribution_selections[1]: #Weibull
        sthree_mean = st.sidebar.number_input("Depot Shape", value=6.11, min_value=1.0)
        sthree_sd = st.sidebar.number_input("Depot Scale", value=22.61, min_value=0.01)

    # ----- INITIAL CONDITIONS
    st.sidebar.header("Initial Conditions")

    st.sidebar.markdown("**Buffer Time**")
    double_periods = st.sidebar.checkbox(
        "Add Buffer Time",
        value=True,
        help="If checked, The Fleet duration is multiplied by the buffer multiplier and split between the beginning and end of Simulation."
    )

    buffer_multiplier = st.sidebar.number_input(
        "Buffer Multiplier",
        min_value=1,
        value=1,
        step=1,
        help="Multiplier for warmup and closing periods (e.g., 2 means warmup = sone_mean * 2)",
        disabled=not double_periods
    )

    # Set warmup_periods and closing_periods based on user-controlled multiplier
    warmup_periods = sone_mean * buffer_multiplier
    closing_periods = sone_mean * buffer_multiplier

    if double_periods:
        sim_time = warmup_periods + analysis_periods + closing_periods
    else:
        sim_time = analysis_periods
        warmup_periods = 0
        closing_periods = 0

    # Display total sim_time
    st.sidebar.info(f"**Total Simulation Time: {sim_time} days**")
    
    st.sidebar.markdown("**Plot Display**")
    use_percentage_plots = st.sidebar.checkbox(
        "Show Plots as Percentage",
        value=True,
        help="If checked, WIP plots display values as percentages. If unchecked, plots show raw counts."
    )

    # Get randomization parameters
    fleet_rand_params = init_fleet_random()
    depot_rand_params = init_depot_random()

    # Combine all parameters
    return {
        'render_plots': render_plots,
        'n_total_parts': n_total_parts,
        'n_total_aircraft': n_total_aircraft,
        'warmup_periods': warmup_periods,
        'analysis_periods': analysis_periods,
        'closing_periods': closing_periods,
        'sim_time': sim_time,
        'use_buffer': double_periods,
        'use_percentage_plots': use_percentage_plots,
        'depot_capacity': depot_capacity,
        'condemn_cycle': condemn_cycle,
        'condemn_depot_fraction': condemn_depot_fraction,
        'part_order_lag': part_order_lag,
        'random_seed': random_seed,
        'mission_capable_rate': mission_capable_rate,
        'parts_in_depot': int(parts_in_depot),
        'parts_in_cond_f': int(parts_in_cond_f),
        'parts_in_cond_a': int(parts_in_cond_a),
        'sone_dist': sone_dist,
        'sthree_dist': sthree_dist,
        'sone_mean': sone_mean,
        'sone_sd': sone_sd,
        'sthree_mean': sthree_mean,
        'sthree_sd': sthree_sd,
        # Fleet randomization
        'use_fleet_rand': fleet_rand_params['use_fleet_rand'],
        'fleet_rand_min': fleet_rand_params['fleet_rand_min'],
        'fleet_rand_max': fleet_rand_params['fleet_rand_max'],
        # Depot randomization
        'use_depot_rand': depot_rand_params['use_depot_rand'],
        'depot_rand_min': depot_rand_params['depot_rand_min'],
        'depot_rand_max': depot_rand_params['depot_rand_max']
    }