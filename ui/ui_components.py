"""

"""

import streamlit as st
from utils import render_allocation_inputs, init_fleet_random, init_depot_random
import numpy as np


def render_sidebar():
    """Render sidebar with simulation parameters matching main_r-code.R exactly."""
    
    st.sidebar.header("Simulation Parameters")

    # Basic parameters
    n_total_parts = st.sidebar.number_input(
        "Total Parts",
        min_value=1,
        value=1100,  # 35
        step=1,
        help="Total number of parts in the system"
    )
    
    n_total_aircraft = st.sidebar.number_input(
        "Total Aircraft",
        min_value=1,
        value=900,  # 30
        step=1,
        help="Total number of aircraft in the fleet"
    )
    
    # Simulation time broken into 3 components
    st.sidebar.markdown("**Simulation Timeline**")
    warmup_periods = st.sidebar.number_input(
        "Warmup Periods (days)",
        min_value=0,
        value=100,
        step=1,
        help="Number of days for warmup calculations. The initial calculations distort the actual representation of a DES due to fleet start time is the same for the first set of parts and aircraft in fleet is all the same. Also all the parts starting in Condition A also start at the same time."
    )
    
    analysis_periods = st.sidebar.number_input(
        "Simulation Time (days)",
        min_value=1,
        value=4000,
        step=1,
        help="Number of days for analysis period"
    )
    
    closing_periods = st.sidebar.number_input(
        "Closing Periods (days)",
        min_value=0,
        value=100,
        step=1,
        help="Number of days for closing period to not have incomplete cycles at end of simulation time"
    )

    # Calculate total sim_time and display
    sim_time = warmup_periods + analysis_periods + closing_periods
    st.sidebar.info(f"**Total Simulation Time: {sim_time} days**")

    depot_capacity = st.sidebar.number_input(
        "Depot Capacity",
        min_value=1,
        value=30,
        step=1,
        help="Maximum number of parts that can be in the depot at once"
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
        - edit df_manager._create_condition_a_df to not generate condemn value
        - a part can't be condemn and in condition A per mdoel logic
        - this function follows same logic as all other random cycle gen
    - edit engine.initialize_depot to handle condemnations
    - continue list
    """

    condemn_cycle = st.sidebar.number_input(
        "Condemn at Cycle",
        min_value=2,
        value=20,
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
        value=0.60,
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
    sone_dist = st.sidebar.selectbox("Fleet Distribution", options=distribution_selections)
    sthree_dist = st.sidebar.selectbox("Depot Distribution", options=distribution_selections)
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("Fleet: Fleet (Part on Aircraft)")
    if sone_dist == distribution_selections[0]: #Normal
        sone_mean = st.sidebar.number_input("Fleet Mean Duration", value=365.0, min_value=1.0)
        sone_sd = st.sidebar.number_input("Fleet Std Dev", value=10.0, min_value=0.01, key="sone_sd")
    elif sone_dist == distribution_selections[1]: #Weibull
        sone_mean = st.sidebar.number_input("Fleet Shape", value=365.0, min_value=1.0)
        sone_sd = st.sidebar.number_input("Fleet Scale", value=10.0, min_value=0.01, key="sone_sd")
    
    st.sidebar.subheader("Depot")
    if sthree_dist == distribution_selections[0]: #Normal
        sthree_mean = st.sidebar.number_input("Depot Mean Duration", value=365.0, min_value=1.0)
        sthree_sd = st.sidebar.number_input("Depot Std Dev", value=10.0, min_value=0.01, key="sthree_sd")
    elif sthree_dist == distribution_selections[1]: #Weibull
        sthree_mean = st.sidebar.number_input("Depot Shape", value=365.0, min_value=1.0)
        sthree_sd = st.sidebar.number_input("Depot Scale", value=10.0, min_value=0.01, key="sthree_sd")
    
    # Get randomization parameters
    fleet_rand_params = init_fleet_random()
    depot_rand_params = init_depot_random()
    
    # Combine all parameters
    return {
        'n_total_parts': n_total_parts,
        'n_total_aircraft': n_total_aircraft,
        'sim_time': sim_time,
        'warmup_periods': warmup_periods,
        'analysis_periods': analysis_periods,
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
        'sone_mean': sone_mean,
        'sone_sd': sone_sd,
        'sthree_dist': sthree_dist,
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