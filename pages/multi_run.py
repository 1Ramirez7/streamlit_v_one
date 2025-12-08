"""
multi_run.py
-----------------
Multi Run page - two-parameter DES simulation (depot_capacity x n_total_parts).

Varies depot_capacity and n_total_parts to find optimal configurations.
"""
import streamlit as st
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from io import BytesIO
from datetime import datetime
import zipfile

import warnings
warnings.simplefilter("ignore", category=FutureWarning)

from simulation_engine import SimulationEngine
from utils import calculate_initial_allocation, init_fleet_random, init_depot_random
from parameters import Parameters


def get_multi_wip_figs_as_bytes(post_sim, depot_cap, n_parts):
    """
    Get the WIP figures needed for multi-model results as PNG bytes.
    
    Converts figures to bytes immediately and closes them to prevent
    memory accumulation across multiple simulation runs.
    
    Args:
        post_sim: PostSim object with pre-rendered figures
        depot_cap: Depot capacity for title
        n_parts: Number of parts for title
        
    Returns:
        dict: Selected figures as PNG bytes for multi-model time series tab
    """
    wip_figs = post_sim.wip_figs
    result = {}
    
    # Keys to extract (add more here to include additional plots)
    keys_to_extract = [
        'micap',
        'depot',
        # === ADD MORE PLOTS HERE ===
        # 'fleet',
        # 'condition_f',
        # 'condition_a',
        # 'wip_overview',
    ]
    
    for key in keys_to_extract:
        fig = wip_figs.get(key)
        if fig is not None:
            # Update title to show which simulation
            if fig.axes:
                display_name = key.replace('_', ' ').title()
                fig.axes[0].set_title(f"{display_name} Over Time (Depot={depot_cap}, Parts={n_parts})")
            # Convert to bytes
            result[key] = fig_to_bytes(fig)
            # Close figure immediately to free memory
            plt.close(fig)
        else:
            result[key] = None
    
    return result


def run_single_simulation(params, depot_cap, n_parts):
    """
    Run a single simulation and return results.
    
    Args:
        params: Parameters object with all simulation settings
        depot_cap: Depot capacity value (for plot titles)
        n_parts: Number of parts value (for plot titles)
        
    Returns:
        dict: Results including averages for all metrics and pre-rendered figure bytes
    """
    allocation = calculate_initial_allocation(params)

    sim_engine = SimulationEngine(
        params=params,
        allocation=allocation
    )
    validation_results = sim_engine.run()

    # Get pre-rendered figures from PostSim as bytes (closes figs immediately)
    post_sim = validation_results['post_sim']
    multi_figs_bytes = get_multi_wip_figs_as_bytes(post_sim, depot_cap, n_parts)

    # Close remaining PostSim figures we don't need for multi-model
    for key, fig in post_sim.wip_figs.items():
        if fig is not None:
            plt.close(fig)
    for key, fig in post_sim.dist_figs.items():
        if fig is not None:
            plt.close(fig)

    # Use multi_run_averages from PostSim (pre-computed)
    averages = post_sim.multi_run_averages
    return {
        'avg_micap': averages['avg_micap'],
        'avg_fleet': averages['avg_fleet'],
        'avg_cd_f': averages['avg_cd_f'],
        'avg_depot': averages['avg_depot'],
        'avg_cd_a': averages['avg_cd_a'],
        'count': averages['count'],
        # Pre-rendered figures as PNG bytes (memory efficient)
        'wip_figs_bytes': multi_figs_bytes,
    }


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


def generate_analysis_text(df, best_results, best_by_parts, params_dict, depot_values, parts_values):
    """Generate the analysis text file content similar to _forloop3.py output."""
    lines = []
    
    # Header
    lines.append("Optimal n_total_parts by depot_capacity Analysis")
    lines.append("=" * 70)
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    
    # Parameter ranges
    lines.append("Parameter Values Tested:")
    lines.append(f"depot_capacity: {depot_values}")
    lines.append(f"n_total_parts: {parts_values}")
    lines.append("")
    
    # Fixed parameters
    lines.append("Fixed Parameters:")
    lines.append(f"n_total_aircraft = {params_dict['n_total_aircraft']}")
    lines.append(f"part_order_lag = {params_dict['part_order_lag']}")
    lines.append(f"mission_capable_rate = {params_dict['mission_capable_rate']}")
    lines.append(f"sim_time = {params_dict['sim_time']}")
    lines.append(f"sone_dist = {params_dict['sone_dist']}")
    lines.append(f"sone_mean = {params_dict['sone_mean']}")
    lines.append(f"sone_sd = {params_dict['sone_sd']}")
    lines.append(f"sthree_dist = {params_dict['sthree_dist']}")
    lines.append(f"sthree_mean = {params_dict['sthree_mean']}")
    lines.append(f"sthree_sd = {params_dict['sthree_sd']}")
    lines.append(f"condemn_cycle = {params_dict['condemn_cycle']}")
    lines.append(f"condemn_depot_fraction = {params_dict['condemn_depot_fraction']}")
    lines.append(f"random_seed = {params_dict['random_seed']}")
    lines.append("")
    
    # Summary: Best for each depot_capacity
    lines.append("=" * 70)
    lines.append("SUMMARY: Best (Lowest) Average MICAP for Each depot_capacity")
    lines.append("=" * 70)
    lines.append("")
    
    for depot_cap in sorted(best_results.keys()):
        row = best_results[depot_cap]
        lines.append(f"depot_capacity = {depot_cap}")
        lines.append(f"  ★ Best: n_total_parts = {row['n_total_parts']}")
        lines.append(f"    Avg MICAP = {row['avg_micap']:.2f}")
        lines.append(f"    Avg Fleet = {row['avg_fleet']:.2f}")
        lines.append(f"    Avg Cd_F = {row['avg_cd_f']:.2f}")
        lines.append(f"    Avg Depot = {row['avg_depot']:.2f}")
        lines.append(f"    Avg Cd_A = {row['avg_cd_a']:.2f}")
        lines.append(f"    (count: {row['count']})")
        lines.append("")
    
    # Detailed results by depot
    lines.append("")
    lines.append("=" * 70)
    lines.append("DETAILED RESULTS: All n_total_parts values for each depot_capacity")
    lines.append("=" * 70)
    lines.append("")
    
    for depot_cap in sorted(df['depot_capacity'].unique()):
        lines.append(f"depot_capacity = {depot_cap}")
        lines.append("-" * 50)
        
        depot_df = df[df['depot_capacity'] == depot_cap].sort_values('n_total_parts')
        best_n_parts = best_results[depot_cap]['n_total_parts'] if depot_cap in best_results else None
        
        for _, row in depot_df.iterrows():
            marker = " ★ BEST" if row['n_total_parts'] == best_n_parts else ""
            lines.append(f"  n_total_parts = {int(row['n_total_parts'])}: "
                        f"MICAP={row['avg_micap']:.2f}, Fleet={row['avg_fleet']:.2f}, "
                        f"Cd_F={row['avg_cd_f']:.2f}, Depot={row['avg_depot']:.2f}, "
                        f"Cd_A={row['avg_cd_a']:.2f} (count: {int(row['count'])}){marker}")
        lines.append("")
    
    # Summary: Best for each n_total_parts
    lines.append("")
    lines.append("=" * 70)
    lines.append("SUMMARY: Best (Lowest) Average MICAP for Each n_total_parts")
    lines.append("=" * 70)
    lines.append("")
    
    for n_parts in sorted(best_by_parts.keys()):
        row = best_by_parts[n_parts]
        lines.append(f"n_total_parts = {n_parts}")
        lines.append(f"  ★ Best: depot_capacity = {row['depot_capacity']}")
        lines.append(f"    Avg MICAP = {row['avg_micap']:.2f}")
        lines.append(f"    Avg Fleet = {row['avg_fleet']:.2f}")
        lines.append(f"    Avg Cd_F = {row['avg_cd_f']:.2f}")
        lines.append(f"    Avg Depot = {row['avg_depot']:.2f}")
        lines.append(f"    Avg Cd_A = {row['avg_cd_a']:.2f}")
        lines.append("")
    
    # Detailed results by parts
    lines.append("")
    lines.append("=" * 70)
    lines.append("DETAILED RESULTS: All depot_capacity values for each n_total_parts")
    lines.append("=" * 70)
    lines.append("")
    
    for n_parts in sorted(df['n_total_parts'].unique()):
        lines.append(f"n_total_parts = {int(n_parts)}")
        lines.append("-" * 50)
        
        parts_df = df[df['n_total_parts'] == n_parts].sort_values('depot_capacity')
        best_depot = best_by_parts[n_parts]['depot_capacity'] if n_parts in best_by_parts else None
        
        for _, row in parts_df.iterrows():
            marker = " ★ BEST" if row['depot_capacity'] == best_depot else ""
            lines.append(f"  depot_capacity = {int(row['depot_capacity'])}: "
                        f"MICAP={row['avg_micap']:.2f}, Fleet={row['avg_fleet']:.2f}, "
                        f"Cd_F={row['avg_cd_f']:.2f}, Depot={row['avg_depot']:.2f}, "
                        f"Cd_A={row['avg_cd_a']:.2f} (count: {int(row['count'])}){marker}")
        lines.append("")
    
    # Overall best
    best_overall = df.loc[df['avg_micap'].idxmin()]
    lines.append("")
    lines.append("=" * 70)
    lines.append("OVERALL BEST RESULT")
    lines.append("=" * 70)
    lines.append(f"★★★ ABSOLUTE BEST across all {len(df)} simulations:")
    lines.append(f"    depot_capacity = {int(best_overall['depot_capacity'])}")
    lines.append(f"    n_total_parts = {int(best_overall['n_total_parts'])}")
    lines.append(f"    Avg MICAP = {best_overall['avg_micap']:.2f}")
    lines.append(f"    Avg Fleet = {best_overall['avg_fleet']:.2f}")
    lines.append(f"    Avg Cd_F = {best_overall['avg_cd_f']:.2f}")
    lines.append(f"    Avg Depot = {best_overall['avg_depot']:.2f}")
    lines.append(f"    Avg Cd_A = {best_overall['avg_cd_a']:.2f}")
    
    return "\n".join(lines)


def fig_to_bytes(fig):
    """Convert matplotlib figure to bytes for download."""
    buf = BytesIO()
    fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
    buf.seek(0)
    return buf.getvalue()


def main() -> None:
    st.title("🔄 Multi Run - Parameter Sweep")
    st.markdown("Vary **depot_capacity** and **n_total_parts**")
    
    # ================================================================
    # SIDEBAR: FIXED PARAMETERS
    # ================================================================
    st.sidebar.header("Fixed Parameters")
    
    # --- TOTAL PARTS NORMALLY HERE  USING -- 891 --- FOR DEFAULT IF NEEDED

    n_total_aircraft = st.sidebar.number_input( # TOTAL AIRCRAFT
        "Total Aircraft", min_value=1, value=826, step=1
    )

    #  -------- SIM-TIME PARAMS -------------
    st.sidebar.markdown("**Simulation Timeline**")
    analysis_periods = st.sidebar.number_input(
        "Analysis Periods (days)", min_value=1, value=7300, step=1
    )
    
    # --- DEPOT CAPACITY NORMALLY HERE--

    condemn_cycle = st.sidebar.number_input( # ----- CONDEMN CYCLE----
        "Condemn at Cycle", min_value=2, value=1000, step=1
    )
    condemn_depot_fraction = st.sidebar.number_input( # CONDEMN DEPOT FRACTION
        "Condemned Depot Time Fraction", min_value=0.0, max_value=1.0, value=0.10, step=0.01
    )

    part_order_lag = st.sidebar.number_input( # PART ORDER --LAG--
        "Part Order Lag (days)", min_value=0, value=365, step=1
    )
    
    random_seed = st.sidebar.number_input( # RANDOM SEED
        "Random Seed", min_value=1, value=132, step=1
    )

    mission_capable_rate = st.sidebar.number_input( # ---MISSION CAPABLE RATE---
        "Mission Capable Rate", min_value=0.0, max_value=1.0, value=0.92, step=0.01
    )
    
    # Distribution settings
    distribution_selections = ["Normal", "Weibull"]
    st.sidebar.markdown("---")
    st.sidebar.subheader("Distribution Selection")
    sone_dist = st.sidebar.selectbox("Fleet Distribution", options=distribution_selections, index=0)
    sthree_dist = st.sidebar.selectbox("Depot Distribution", options=distribution_selections, index=1)

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
    
    # ----- INITIAL CONDITIONS -----
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

    # ================================================================
    # SIDEBAR: LOOP PARAMETERS
    # ================================================================
    st.sidebar.markdown("---")
    st.sidebar.header("🔄 Loop Parameters")
    
    # --- N TOTAL PARTS ---
    st.sidebar.subheader("N Total Parts")
    use_parts_list = st.sidebar.checkbox("Use specific list", value=True, key="parts_list")
    
    if use_parts_list:
        parts_input = st.sidebar.text_input("Values (comma-separated)", "851, 861, 871, 881, 891, 901, 911, 921, 931", key="parts_vals")
        parts_values = parse_list_input(parts_input)
    else:
        col1, col2 = st.sidebar.columns(2)
        parts_min = col1.number_input("Min", value=851, key="parts_min")
        parts_max = col2.number_input("Max", value=931, key="parts_max")
        
        parts_range_mode = st.sidebar.radio(
            "Range mode",
            ["All values", "Every X interval", "X evenly spaced"],
            key="parts_mode",
            horizontal=True
        )
        
        if parts_range_mode == "Every X interval":
            parts_interval = st.sidebar.number_input("Interval", value=2, min_value=1, key="parts_interval")
            parts_values = build_loop_values(False, [], parts_min, parts_max, 'interval', parts_interval)
        elif parts_range_mode == "X evenly spaced":
            parts_count = st.sidebar.number_input("Number of values", value=5, min_value=2, key="parts_count")
            parts_values = build_loop_values(False, [], parts_min, parts_max, 'count', parts_count)
        else:
            parts_values = build_loop_values(False, [], parts_min, parts_max, 'all', 1)
    
    st.sidebar.caption(f"Parts values: {parts_values}")

    # --- DEPOT CAPACITY ---
    st.sidebar.subheader("Depot Capacity")
    use_depot_list = st.sidebar.checkbox("Use specific list", value=True, key="depot_list")
    
    if use_depot_list:
        depot_input = st.sidebar.text_input("Values (comma-separated)", "29, 31, 33, 35, 37, 39, 41, 43, 45", key="depot_vals")
        depot_values = parse_list_input(depot_input)
    else:
        col1, col2 = st.sidebar.columns(2)
        depot_min = col1.number_input("Min", value=29, key="depot_min")
        depot_max = col2.number_input("Max", value=45, key="depot_max")
        
        depot_range_mode = st.sidebar.radio(
            "Range mode",
            ["All values", "Every X interval", "X evenly spaced"],
            key="depot_mode",
            horizontal=True
        )
        
        if depot_range_mode == "Every X interval":
            depot_interval = st.sidebar.number_input("Interval", value=5, min_value=1, key="depot_interval")
            depot_values = build_loop_values(False, [], depot_min, depot_max, 'interval', depot_interval)
        elif depot_range_mode == "X evenly spaced":
            depot_count = st.sidebar.number_input("Number of values", value=5, min_value=2, key="depot_count")
            depot_values = build_loop_values(False, [], depot_min, depot_max, 'count', depot_count)
        else:
            depot_values = build_loop_values(False, [], depot_min, depot_max, 'all', 1)
    
    st.sidebar.caption(f"Depot values: {depot_values}")
    
    # Total runs
    total_runs = len(depot_values) * len(parts_values)
    st.sidebar.markdown("---")
    st.sidebar.metric("Total Simulations", total_runs)
    
    if total_runs > 500:
        st.sidebar.warning(f"⚠️ {total_runs} runs may take a long time!")
    
    # ================================================================
    # RUN BUTTON
    # ================================================================
    run_button = st.sidebar.button("▶️ Run All Simulations", type="primary")
    
    # ================================================================
    # MAIN AREA
    # ================================================================
    
    if 'loop2_results' not in st.session_state:
        st.session_state.loop2_results = None
    if 'loop2_params' not in st.session_state:
        st.session_state.loop2_params = None
    if 'loop2_depot_values' not in st.session_state:
        st.session_state.loop2_depot_values = None
    if 'loop2_parts_values' not in st.session_state:
        st.session_state.loop2_parts_values = None
    
    if run_button:
        if not depot_values or not parts_values:
            st.error("Please enter valid values for all loop parameters.")
            return
        
        # Clear any lingering figures from previous runs
        plt.close('all')
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        all_results = []
        run_count = 0
        
        # Build base params dict (for text file generation)
        base_params = {
            'n_total_aircraft': n_total_aircraft,
            'warmup_periods': warmup_periods,
            'analysis_periods': analysis_periods,
            'closing_periods': closing_periods,
            'sim_time': sim_time,
            'use_buffer': double_periods,
            'use_percentage_plots': use_percentage_plots,
            'part_order_lag': part_order_lag,
            'mission_capable_rate': mission_capable_rate,
            'condemn_cycle': condemn_cycle,
            'condemn_depot_fraction': condemn_depot_fraction,
            'sone_dist': sone_dist,
            'sone_mean': sone_mean,
            'sone_sd': sone_sd,
            'sthree_dist': sthree_dist,
            'sthree_mean': sthree_mean,
            'sthree_sd': sthree_sd,
            'use_fleet_rand': fleet_rand_params['use_fleet_rand'],
            'fleet_rand_min': fleet_rand_params['fleet_rand_min'],
            'fleet_rand_max': fleet_rand_params['fleet_rand_max'],
            'use_depot_rand': depot_rand_params['use_depot_rand'],
            'depot_rand_min': depot_rand_params['depot_rand_min'],
            'depot_rand_max': depot_rand_params['depot_rand_max'],
            'render_plots': True,
            'random_seed': random_seed,
        }
        
        # Run all combinations
        for depot_cap in depot_values:
            for n_parts in parts_values:
                run_count += 1
                progress = run_count / total_runs
                progress_bar.progress(progress)
                status_text.text(f"Running {run_count}/{total_runs}: depot={depot_cap}, parts={n_parts}")
                
                np.random.seed(random_seed)
                
                params = Parameters()
                params.set_all(base_params)
                params.set('n_total_parts', n_parts)
                params.set('depot_capacity', depot_cap)
                
                # Calculate allocation
                n_aircraft_with_parts = min(n_parts, int(np.ceil(mission_capable_rate * n_total_aircraft)))
                parts_air_dif = n_parts - n_aircraft_with_parts
                parts_in_depot = min(parts_air_dif, depot_cap)
                remaining_parts = parts_air_dif - parts_in_depot
                
                params.set('parts_in_depot', parts_in_depot)
                params.set('parts_in_cond_f', remaining_parts)
                params.set('parts_in_cond_a', 0)
                
                try:
                    result = run_single_simulation(params, depot_cap, n_parts)
                    result['depot_capacity'] = depot_cap
                    result['n_total_parts'] = n_parts
                    all_results.append(result)
                except Exception as e:
                    st.warning(f"Run {run_count} failed: {e}")
                    import traceback
                    st.code(traceback.format_exc())
        
        progress_bar.empty()
        status_text.empty()
        
        st.session_state.loop2_results = pd.DataFrame(all_results)
        st.session_state.loop2_params = base_params
        st.session_state.loop2_depot_values = depot_values
        st.session_state.loop2_parts_values = parts_values
        st.success(f"✅ Completed {len(all_results)} simulations!")
    
    # ================================================================
    # DISPLAY RESULTS
    # ================================================================
    if st.session_state.loop2_results is not None:
        df = st.session_state.loop2_results
        params_dict = st.session_state.loop2_params
        depot_vals = st.session_state.loop2_depot_values
        parts_vals = st.session_state.loop2_parts_values
        
        st.markdown("---")
        st.header("Results")
        
        # Best overall
        best_row = df.loc[df['avg_micap'].idxmin()]
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Best MICAP", f"{best_row['avg_micap']:.2f}")
        with col2:
            st.metric("Best Depot", f"{int(best_row['depot_capacity'])}")
        with col3:
            st.metric("Best Parts", f"{int(best_row['n_total_parts'])}")
        
        # Compute best by depot and best by parts for summaries
        depots = sorted(df['depot_capacity'].unique())
        parts_unique = sorted(df['n_total_parts'].unique())
        
        # Best by depot
        best_results = {}
        summary_rows = []
        for depot in depots:
            depot_df = df[df['depot_capacity'] == depot]
            best = depot_df.loc[depot_df['avg_micap'].idxmin()]
            best_results[depot] = best.to_dict()
            summary_rows.append({
                'Depot Cap': int(depot),
                'Best Parts': int(best['n_total_parts']),
                'Avg MICAP': round(best['avg_micap'], 2),
                'Avg Fleet': round(best['avg_fleet'], 2),
                'Avg Cd_F': round(best['avg_cd_f'], 2),
                'Avg Depot': round(best['avg_depot'], 2),
                'Avg Cd_A': round(best['avg_cd_a'], 2),
            })
        
        summary_df = pd.DataFrame(summary_rows)
        
        # Best by parts
        best_by_parts = {}
        summary_parts_rows = []
        for n_parts in parts_unique:
            parts_df = df[df['n_total_parts'] == n_parts]
            best = parts_df.loc[parts_df['avg_micap'].idxmin()]
            best_by_parts[n_parts] = best.to_dict()
            summary_parts_rows.append({
                'N Parts': int(n_parts),
                'Best Depot': int(best['depot_capacity']),
                'Avg MICAP': round(best['avg_micap'], 2),
                'Avg Fleet': round(best['avg_fleet'], 2),
                'Avg Cd_F': round(best['avg_cd_f'], 2),
                'Avg Depot': round(best['avg_depot'], 2),
                'Avg Cd_A': round(best['avg_cd_a'], 2),
            })
        
        summary_parts_df = pd.DataFrame(summary_parts_rows)
        
        # Tabs
        # tab1, tab2, tab3, tab4 = st.tabs(["Charts", "All Metrics", "Full Data", "Download"])  # === ORIGINAL LINE (MICAP TIME SERIES) ===
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["Charts", "All Metrics", "Full Data", "MICAP Time Series", "Download"])  # === MICAP TIME SERIES PLOTS ADDITION ===

        # 
        # -------PLOT HELPER FOR MICAP VS: DEPOT-PART COMPARION (with lines)---
        #
        metrics = [
            ('avg_micap', 'Avg MICAP'),
            ('avg_fleet', 'Avg Fleet'),
            ('avg_cd_f', 'Avg Cd_F'),
            ('avg_depot', 'Avg Depot (WIP)'),
            ('avg_cd_a', 'Avg Cd_A')
        ]
        
        colors_by_depot = plt.cm.viridis(np.linspace(0, 1, len(depots)))
        colors_by_parts = plt.cm.plasma(np.linspace(0, 1, len(parts_unique)))
        
        def create_metric_plot(metric, title, by_depot=True, figsize=(8, 4)):
            """Create a metric plot grouped by depot or parts."""
            fig, ax = plt.subplots(figsize=figsize)
            if by_depot:
                for i, depot in enumerate(depots):
                    depot_df = df[df['depot_capacity'] == depot]
                    means = depot_df.groupby('n_total_parts')[metric].mean()
                    ax.plot(means.index, means.values, 'o-', 
                            label=f'{depot}', color=colors_by_depot[i], markersize=4, linewidth=1.5)
                ax.set_xlabel('N Total Parts', fontsize=10)
                ax.legend(title='Depot Cap', bbox_to_anchor=(1.02, 1), loc='upper left', fontsize=8)
            else:
                for i, n_parts in enumerate(parts_unique):
                    parts_df = df[df['n_total_parts'] == n_parts]
                    means = parts_df.groupby('depot_capacity')[metric].mean()
                    ax.plot(means.index, means.values, 'o-', 
                            label=f'{int(n_parts)}', color=colors_by_parts[i], markersize=4, linewidth=1.5)
                ax.set_xlabel('Depot Capacity', fontsize=10)
                ax.legend(title='N Parts', bbox_to_anchor=(1.02, 1), loc='upper left', fontsize=8)
            ax.set_ylabel(title, fontsize=10)
            ax.set_title(title, fontsize=11)
            ax.grid(True, alpha=0.3)
            plt.tight_layout()
            return fig
        
        with tab1:
            # Line plot - MICAP vs Parts, lines = Depot
            st.subheader("MICAP vs Number of Total Parts (by Depot Capacity)")
            fig_micap = create_metric_plot(*metrics[0], by_depot=True)
            st.pyplot(fig_micap)

            # MICAP (single centered)
            st.subheader("MICAP vs Depot Capacity (by Total Parts)")
            fig_micap2 = create_metric_plot(*metrics[0], by_depot=False)
            st.pyplot(fig_micap2)
        
        with tab2:
            # All Metrics
            st.subheader("All Metrics by Configuration")
            
            # Section 1: By N Total Parts (lines = Depot Capacity)
            st.markdown("#### By N Total Parts (lines = Depot Capacity)")
            
            # Row 1: MICAP (single centered)
            st.pyplot(fig_micap) # made in tab1
            
            # Row 2: Fleet and Cd_F side by side
            col1, col2 = st.columns(2)
            with col1:
                fig_fleet = create_metric_plot(*metrics[1], by_depot=True)
                st.pyplot(fig_fleet)
                plt.close(fig_fleet)
            with col2:
                fig_cdf = create_metric_plot(*metrics[2], by_depot=True)
                st.pyplot(fig_cdf)
                plt.close(fig_cdf)
            
            # Row 3: Depot WIP and Cd_A side by side
            col3, col4 = st.columns(2)
            with col3:
                fig_depot = create_metric_plot(*metrics[3], by_depot=True)
                st.pyplot(fig_depot)
                plt.close(fig_depot)
            with col4:
                fig_cda = create_metric_plot(*metrics[4], by_depot=True)
                st.pyplot(fig_cda)
                plt.close(fig_cda)
            
            # Section 2: By Depot Capacity (lines = N Total Parts)
            st.markdown("---")
            st.markdown("#### By Depot Capacity (lines = N Total Parts)")
            
            # Row 1: MICAP (single centered)
            st.pyplot(fig_micap2) # made in tab1
            plt.close(fig_micap2)
            
            # Row 2: Fleet and Cd_F side by side
            col5, col6 = st.columns(2)
            with col5:
                fig_fleet2 = create_metric_plot(*metrics[1], by_depot=False)
                st.pyplot(fig_fleet2)
                plt.close(fig_fleet2)
            with col6:
                fig_cdf2 = create_metric_plot(*metrics[2], by_depot=False)
                st.pyplot(fig_cdf2)
                plt.close(fig_cdf2)
            
            # Row 3: Depot WIP and Cd_A side by side
            col7, col8 = st.columns(2)
            with col7:
                fig_depot2 = create_metric_plot(*metrics[3], by_depot=False)
                st.pyplot(fig_depot2)
                plt.close(fig_depot2)
            with col8:
                fig_cda2 = create_metric_plot(*metrics[4], by_depot=False)
                st.pyplot(fig_cda2)
                plt.close(fig_cda2)
        
        with tab3:
            st.subheader("Full Results")
            
            display_cols = ['depot_capacity', 'n_total_parts', 
                          'avg_micap', 'avg_fleet', 'avg_cd_f', 'avg_depot', 'avg_cd_a', 'count']
            display_df = df[display_cols].copy()
            display_df.columns = ['Depot', 'Parts', 'MICAP', 'Fleet', 'Cd_F', 'Depot(WIP)', 'Cd_A', 'Count']
            
            for col in ['MICAP', 'Fleet', 'Cd_F', 'Depot(WIP)', 'Cd_A']:
                display_df[col] = display_df[col].round(2)
            
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            
            st.subheader("Best Depot for Each N_Parts")
            st.dataframe(summary_parts_df, use_container_width=True, hide_index=True)

            st.subheader("Best Number of Parts for Each Depot Capacity")
            st.dataframe(summary_df, use_container_width=True, hide_index=True)
        
        # === TIME SERIES PLOTS (Pre-rendered as PNG bytes) ===
        # ============================================================
        # Figures are converted to PNG bytes immediately after creation
        # to prevent memory accumulation. Displayed using st.image().
        # To add more plots: update get_multi_wip_figs_as_bytes() at top of file.
        # ============================================================
        
        # Figure display names (keys must match get_multi_wip_figs_as_bytes keys)
        FIG_DISPLAY_NAMES = {
            'micap': 'MICAP',
            'depot': 'Depot WIP',
            # === ADD MORE HERE (must match keys in get_multi_wip_figs_as_bytes) ===
            # 'fleet': 'Fleet WIP',
            # 'condition_f': 'Condition F WIP',
            # 'condition_a': 'Condition A WIP',
        }
        
        with tab4:
            st.subheader("Time Series Plots (Per Simulation)")
            st.markdown("Each plot shows metrics over simulation time for a specific depot/parts combination.")
            
            # Store all figure bytes for download: {plot_type: {sim_key: bytes}}
            all_ts_bytes = {}
            
            # Display pre-rendered figures (as bytes) for each plot type
            for fig_key, display_name in FIG_DISPLAY_NAMES.items():
                st.markdown(f"### {display_name} Over Time")
                
                plot_bytes = {}  # Store bytes for this plot type
                
                for idx, row in df.iterrows():
                    depot_cap = int(row['depot_capacity'])
                    n_parts = int(row['n_total_parts'])
                    wip_figs_bytes = row.get('wip_figs_bytes', {})
                    fig_bytes = wip_figs_bytes.get(fig_key) if wip_figs_bytes else None
                    
                    if fig_bytes is not None:
                        sim_key = f"depot_{depot_cap}_parts_{n_parts}"
                        plot_bytes[sim_key] = fig_bytes
                
                # Display plots in expandable sections
                if plot_bytes:
                    st.info(f"Generated {len(plot_bytes)} {display_name} plot(s)")
                    for sim_key, img_bytes in plot_bytes.items():
                        with st.expander(f"📈 {display_name}: {sim_key.replace('_', ' ').title()}", expanded=False):
                            st.image(img_bytes, use_container_width=True)
                else:
                    st.warning(f"No {display_name} data available.")
                
                # Store for download
                all_ts_bytes[fig_key] = plot_bytes
            
            # Store all bytes in session state for download tab
            st.session_state.loop2_ts_bytes = all_ts_bytes
        
        with tab5:  # === CHANGED FROM tab4 (MICAP TIME SERIES) ===
            st.subheader("Download Results")
            
            # Create zip file with all results
            zip_buffer = BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
                # Excel file with multiple sheets
                excel_buffer = BytesIO()
                with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                    df.to_excel(writer, sheet_name='Full Results', index=False)
                    summary_df.to_excel(writer, sheet_name='Best by Depot', index=False)
                    summary_parts_df.to_excel(writer, sheet_name='Best by Parts', index=False)
                zf.writestr('loop2_results.xlsx', excel_buffer.getvalue())
                
                # Text analysis file
                analysis_text = generate_analysis_text(
                    df, best_results, best_by_parts, params_dict, depot_vals, parts_vals
                )
                zf.writestr('loop2_analysis.txt', analysis_text)
                
                # tab1 Plot images to download
                zf.writestr('micap_vs_parts_ln_depot.png', fig_to_bytes(fig_micap))
                zf.writestr('micap_vsdepot_ln_parts.png', fig_to_bytes(fig_micap2))
                # tab2 plots Metrics vs parts comparison
                zf.writestr('fleet_vs_parts_ln_depot.png', fig_to_bytes(fig_fleet))
                zf.writestr('cdf_vs_parts_ln_depot.png', fig_to_bytes(fig_cdf))
                zf.writestr('depot_vs_parts_ln_depot.png', fig_to_bytes(fig_depot))
                zf.writestr('cda_vs_parts_ln_depot.png', fig_to_bytes(fig_cda))
                # tab2 plots Metrics vs parts comparison
                zf.writestr('fleet_vs_depot_ln_parts.png', fig_to_bytes(fig_fleet2))
                zf.writestr('cdf_vs_depot_ln_parts.png', fig_to_bytes(fig_cdf2))
                zf.writestr('depot_vs_depot_ln_parts.png', fig_to_bytes(fig_depot2))
                zf.writestr('cda_vs_depot_ln_parts.png', fig_to_bytes(fig_cda2))
                
                # === TIME SERIES PLOTS - Download all (already bytes) ===
                all_ts_bytes_download = st.session_state.get('loop2_ts_bytes', {})
                for plot_type, plot_bytes in all_ts_bytes_download.items():
                    for sim_key, img_bytes in plot_bytes.items():
                        zf.writestr(f'timeseries_{plot_type}_{sim_key}.png', img_bytes)
            
            zip_buffer.seek(0)
            
            st.download_button(
                label="Download All Results (ZIP)",
                data=zip_buffer,
                file_name="loop2_results.zip",
                mime="application/zip"
            )
            
            # Close tab1/tab2 comparison plots 
            plt.close(fig_micap)
            plt.close(fig_micap2)
            plt.close(fig_fleet)
            plt.close(fig_cdf)
            plt.close(fig_depot)
            plt.close(fig_cda)
            plt.close(fig_fleet2)
            plt.close(fig_cdf2)
            plt.close(fig_depot2)
            plt.close(fig_cda2)
            
            # Time series plots are already bytes - no figures to close
    
    else:
        st.info("Configure parameters in the sidebar and click **Run All Simulations** to start.")


if __name__ == "__main__":
    main()
