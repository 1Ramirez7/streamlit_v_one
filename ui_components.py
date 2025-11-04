"""

"""

import streamlit as st

def render_sidebar():
    """Render sidebar with simulation parameters matching main_r-code.R exactly."""
    
    st.sidebar.header("Simulation Parameters")

    # Basic parameters
    n_total_parts = st.sidebar.number_input(
        "Total Parts",
        min_value=1,
        value=35,  # Test value (30 in production)
        step=1,
        help="Total number of parts in the system"
    )
    
    n_total_aircraft = st.sidebar.number_input(
        "Total Aircraft",
        min_value=1,
        value=30,
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
        value=300,
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
        value=5,
        step=1,
        help="Maximum number of parts that can be in the depot at once"
    )

    # NEW: Part condemn parameters
    st.sidebar.subheader("Part Condemn Parameters")
    
    condemn_cycle = st.sidebar.number_input(
        "Condemn at Cycle",
        min_value=1,
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
        value=100,
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

    
    st.sidebar.markdown("---")
    st.sidebar.subheader("Fleet: Fleet (Part on Aircraft)")
    sone_mean = st.sidebar.number_input("Mean Duration", value=30.0, min_value=0.0)
    sone_sd = st.sidebar.number_input("Std Dev", value=10.0, min_value=0.0, key="sone_sd")
    
    st.sidebar.subheader("Depot")
    sthree_mean = st.sidebar.number_input("Mean Duration", value=1.0, min_value=0.0, key="sthree_mean")
    sthree_sd = st.sidebar.number_input("Std Dev", value=0.2, min_value=0.0, key="sthree_sd")
    
    return {
        'n_total_parts': n_total_parts,
        'n_total_aircraft': n_total_aircraft,
        'sim_time': sim_time,
        'warmup_periods': warmup_periods,  # NEW - to plot only analysis periods
        'analysis_periods': analysis_periods,  # NEW - to plot only analysis periods
        'depot_capacity': depot_capacity,
        'condemn_cycle': condemn_cycle,  # new params for depot logic
        'condemn_depot_fraction': condemn_depot_fraction, # new params for depot logic
        'part_order_lag': part_order_lag, # new params for depot logic
        'random_seed': random_seed,
        'sone_mean': sone_mean,
        'sone_sd': sone_sd,
        'sthree_mean': sthree_mean,
        'sthree_sd': sthree_sd
    }
