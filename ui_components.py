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
    
    st.sidebar.subheader("Condition F: Condition F")
    stwo_mean = st.sidebar.number_input("Mean Duration", value=3.0, min_value=0.0, key="stwo_mean")
    stwo_sd = st.sidebar.number_input("Std Dev", value=0.5, min_value=0.0, key="stwo_sd")
    
    st.sidebar.subheader("Depot")
    sthree_mean = st.sidebar.number_input("Mean Duration", value=1.0, min_value=0.0, key="sthree_mean")
    sthree_sd = st.sidebar.number_input("Std Dev", value=0.2, min_value=0.0, key="sthree_sd")
    
    st.sidebar.subheader("Condition A: Install")
    sfour_mean = st.sidebar.number_input("Mean Duration", value=1.0, min_value=0.0, key="sfour_mean")
    sfour_sd = st.sidebar.number_input("Std Dev", value=0.5, min_value=0.0, key="sfour_sd")
    
    return {
        'n_total_parts': n_total_parts,
        'n_total_aircraft': n_total_aircraft,
        'sim_time': sim_time,
        'warmup_periods': warmup_periods,  # NEW - to plot only analysis periods
        'analysis_periods': analysis_periods,  # NEW - to plot only analysis periods
        'depot_capacity': depot_capacity,
        'random_seed': random_seed,
        'sone_mean': sone_mean,
        'sone_sd': sone_sd,
        'stwo_mean': stwo_mean,
        'stwo_sd': stwo_sd,
        'sthree_mean': sthree_mean,
        'sthree_sd': sthree_sd,
        'sfour_mean': sfour_mean,
        'sfour_sd': sfour_sd
    }
