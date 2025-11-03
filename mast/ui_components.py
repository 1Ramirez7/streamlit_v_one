"""
R code reference (main_r-code.R lines 1-11):

set.seed(123)
n_total_parts <- 30
n_total_aircraft <- 30
sim_time <- 500

sone_mean <- 30;  sone_sd <- 10
stwo_mean <- 3;   stwo_sd <- 0.5
sthree_mean <- 1; sthree_sd <- 0.2
sfour_mean <- 1;  sfour_sd <- 0.5
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
    
    sim_time = st.sidebar.number_input(
        "Simulation Time (days)",
        min_value=1,
        value=500,
        step=1,
        help="Total simulation duration in days"
    )
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("Stage 1: Fleet (Part on Aircraft)")
    sone_mean = st.sidebar.number_input("Mean Duration", value=30.0, min_value=0.0)
    sone_sd = st.sidebar.number_input("Std Dev", value=10.0, min_value=0.0, key="sone_sd")
    
    st.sidebar.subheader("Stage 2: Condition F")
    stwo_mean = st.sidebar.number_input("Mean Duration", value=3.0, min_value=0.0, key="stwo_mean")
    stwo_sd = st.sidebar.number_input("Std Dev", value=0.5, min_value=0.0, key="stwo_sd")
    
    st.sidebar.subheader("Stage 3: Depot")
    sthree_mean = st.sidebar.number_input("Mean Duration", value=1.0, min_value=0.0, key="sthree_mean")
    sthree_sd = st.sidebar.number_input("Std Dev", value=0.2, min_value=0.0, key="sthree_sd")
    
    st.sidebar.subheader("Stage 4: Install")
    sfour_mean = st.sidebar.number_input("Mean Duration", value=1.0, min_value=0.0, key="sfour_mean")
    sfour_sd = st.sidebar.number_input("Std Dev", value=0.5, min_value=0.0, key="sfour_sd")
    
    return {
        'n_total_parts': n_total_parts,
        'n_total_aircraft': n_total_aircraft,
        'sim_time': sim_time,
        'sone_mean': sone_mean,
        'sone_sd': sone_sd,
        'stwo_mean': stwo_mean,
        'stwo_sd': stwo_sd,
        'sthree_mean': sthree_mean,
        'sthree_sd': sthree_sd,
        'sfour_mean': sfour_mean,
        'sfour_sd': sfour_sd
    }
