import streamlit as st
import numpy as np
import pandas as pd
import io

import warnings # to silent future warnings, comment to test
warnings.simplefilter("ignore", category=FutureWarning)


from data_manager import DataFrameManager
from simulation_engine import SimulationEngine
from ui.ui_components import render_sidebar
from ui.downloads import render_download_section
from ui.stats import render_stats_tab
from ds.data_science import DataSets
from utils import calculate_initial_allocation
from ui.dist_plots import render_duration_plots
from loop_runner import calculate_depot_values, calculate_loop_values, run_depot_capacity_loop, run_single_simulation, run_parts_loop, run_nested_loop

def main() -> None:
    # Set page config - MUST be first Streamlit command
    st.set_page_config(
        page_title=" SIMULATION",
        layout="wide",  # Use full screen width
        initial_sidebar_state="expanded"
    )
    
    st.title("Discrete Event Simulation")
    st.markdown("Configure simulation parameters in the sidebar and click **Run Simulation**.")

    # --- Initialize session state for storing results ---
    if 'simulation_run' not in st.session_state:
        st.session_state.simulation_run = False
    if 'datasets' not in st.session_state:
        st.session_state.datasets = None
    if 'validation_results' not in st.session_state:
        st.session_state.validation_results = None
    if 'allocation' not in st.session_state:
        st.session_state.allocation = None
    if 'df_manager' not in st.session_state:
        st.session_state.df_manager = None
    # Loop mode session state
    if 'loop_results' not in st.session_state:
        st.session_state.loop_results = None
    if 'loop_run' not in st.session_state:
        st.session_state.loop_run = False

    # --- Get inputs from UI ---
    params = render_sidebar()
    
    # --- Run Simulation after clicking "Run Simulation" button ---
    run_button = st.sidebar.button("Run Simulation", type="primary")
    
    if run_button:
        # Create placeholders for progress updates
        progress_placeholder = st.empty()
        event_details = st.empty()
        
        with st.spinner("Running simulation..."):
            # Set random seed for reproducibility
            np.random.seed(params['random_seed'])
            
            # calculate initial conditions
            allocation = calculate_initial_allocation(
                n_total_parts=params['n_total_parts'],
                n_total_aircraft=params['n_total_aircraft'],
                mission_capable_rate=params['mission_capable_rate'],
                depot_capacity=params['depot_capacity'],
                condemn_cycle=params['condemn_cycle'],
                parts_in_depot=params['parts_in_depot'],  # NEW
                parts_in_cond_f=params['parts_in_cond_f'],  # NEW
                parts_in_cond_a=params['parts_in_cond_a']   # NEW
            )
            
            # Create DataFrameManager
            df_manager = DataFrameManager( # params are passed to DataFrameManager purely for calc df sizes. 
                n_total_parts=params['n_total_parts'], # maybe we should use only one mean value for calc df size
                n_total_aircraft=params['n_total_aircraft'], # params are you to be changing so its ideal 
                sim_time=params['sim_time'],
                sone_mean=params['sone_mean'],
                sthree_mean=params['sthree_mean'], 
                allocation=allocation
            )
            
            # Store all user parameters for logging/export
            df_manager.store_user_params(params)
            
            # Create DataSets class
            datasets = DataSets()
            
            # Create SimulationEngine
            engine = SimulationEngine(
                df_manager=df_manager,
                datasets=datasets,
                sone_dist=params['sone_dist'],
                sone_mean=params['sone_mean'],
                sone_sd=params['sone_sd'],
                sthree_dist=params['sthree_dist'],
                sthree_mean=params['sthree_mean'],
                sthree_sd=params['sthree_sd'],
                sim_time=params['sim_time'],
                depot_capacity=params['depot_capacity'], # new params for depot logic
                condemn_cycle=params['condemn_cycle'],  # new params for depot logic
                condemn_depot_fraction=params['condemn_depot_fraction'],  # new params for depot logic
                part_order_lag=params['part_order_lag'],
                # NEW: Fleet randomization parameters
                use_fleet_rand=params['use_fleet_rand'],
                fleet_rand_min=params['fleet_rand_min'],
                fleet_rand_max=params['fleet_rand_max'],
                # NEW: Depot randomization parameters
                use_depot_rand=params['use_depot_rand'],
                depot_rand_min=params['depot_rand_min'],
                depot_rand_max=params['depot_rand_max']
            )
            
            # Define progress callback for live updates 777
            def update_progress(event_type, event_count, total_count):
                progress_placeholder.write(f"**Processing Events:** {total_count:,} total")
                event_details.caption(f"Latest: {event_type} (#{event_count})")
            
            # Run simulation with progress tracking
            validation_results = engine.run(progress_callback=update_progress) # progress_callback=update_progress 777
            
            # Clear progress displays
            progress_placeholder.empty()
            event_details.empty()
        
        # Store results in session state so they persist across reruns
        st.session_state.simulation_run = True
        st.session_state.datasets = datasets
        st.session_state.validation_results = validation_results
        st.session_state.allocation = allocation
        st.session_state.df_manager = df_manager
        
        st.success("Simulation complete!")
    
    # --- Always render tabs (even before simulation runs) ---
    tab0, tab1, tab2, tab3 = st.tabs(["⚙️ Setup", "Cycle", "Simulation Results", "WIP Plots"])
    
    ############################
    # TAB 0 - Setup/Inputs (always visible)
    ############################
    with tab0:
        st.subheader("🔄 Loop Configuration")
        st.write("Run multiple simulations with varying parameter values.")
        
        # ===== PARTS LOOP SECTION =====
        st.markdown("---")
        st.markdown("### 🔧 Total Parts Loop")
        
        enable_parts_loop = st.checkbox("Enable Total Parts Loop", value=False,
                                        help="Run simulation multiple times with different n_total_parts values")
        
        parts_values = [params['n_total_parts']]  # Default to single value from sidebar
        
        if enable_parts_loop:
            # Loop mode selection for parts (exclude pct_parts since we're changing parts)
            parts_loop_mode = st.selectbox(
                "Parts Loop Mode",
                options=['range', 'step', 'auto', 'custom', 'pct_aircraft'],
                format_func=lambda x: {
                    'range': '📊 Range (every integer)',
                    'step': '📏 Step (every X value)',
                    'auto': '🔢 Auto (N evenly spaced values)',
                    'custom': '✏️ Custom List',
                    'pct_aircraft': '✈️ % of Total Aircraft'
                }.get(x, x),
                help="Select how to generate n_total_parts values",
                key="parts_loop_mode"
            )
            
            col1, col2 = st.columns(2)
            
            # Mode-specific inputs for parts loop
            if parts_loop_mode in ['range', 'step', 'auto']:
                with col1:
                    parts_range_min = st.number_input("Parts Range Min", min_value=1, value=20, step=1, key="parts_range_min")
                with col2:
                    parts_range_max = st.number_input("Parts Range Max", min_value=1, value=30, step=1, key="parts_range_max")
                
                if parts_loop_mode == 'step':
                    parts_step_value = st.number_input("Parts Step Value", min_value=1, value=2, step=1, key="parts_step")
                else:
                    parts_step_value = 1
                
                if parts_loop_mode == 'auto':
                    parts_num_loops = st.number_input("Number of Parts Values", min_value=2, value=5, step=1, key="parts_num_loops")
                else:
                    parts_num_loops = 5
                
                if parts_loop_mode in ['step', 'auto']:
                    parts_include_min = st.checkbox("Include minimum parts value", value=True, key="parts_include_min")
                else:
                    parts_include_min = True
                
                parts_custom_list = None
                parts_pct_min, parts_pct_max, parts_pct_step = 0.5, 1.5, 0.1
                
            elif parts_loop_mode == 'custom':
                st.write("Enter comma-separated n_total_parts values:")
                parts_custom_input = st.text_input("Custom Parts Values", value="20, 24, 28, 32, 36",
                                                   help="e.g., 20, 25, 30, 35, 40", key="parts_custom")
                try:
                    parts_custom_list = [int(x.strip()) for x in parts_custom_input.split(',') if x.strip()]
                except ValueError:
                    st.error("Invalid input. Please enter comma-separated integers.")
                    parts_custom_list = []
                
                parts_range_min, parts_range_max, parts_step_value, parts_num_loops = 20, 30, 1, 5
                parts_include_min = True
                parts_pct_min, parts_pct_max, parts_pct_step = 0.5, 1.5, 0.1
                
            else:  # pct_aircraft
                with col1:
                    parts_pct_min = st.number_input("Min % (of aircraft)", min_value=0.1, max_value=5.0, value=0.8, step=0.1,
                                                    format="%.1f", key="parts_pct_min")
                with col2:
                    parts_pct_max = st.number_input("Max % (of aircraft)", min_value=0.1, max_value=5.0, value=1.5, step=0.1,
                                                    format="%.1f", key="parts_pct_max")
                
                parts_pct_step = st.number_input("Step % (of aircraft)", min_value=0.05, max_value=1.0, value=0.1, step=0.05,
                                                 format="%.2f", key="parts_pct_step")
                
                parts_include_min = st.checkbox("Include minimum percentage", value=True, key="parts_pct_include_min")
                
                st.info(f"Base value: {params['n_total_aircraft']} aircraft")
                
                parts_range_min, parts_range_max, parts_step_value, parts_num_loops = 20, 30, 1, 5
                parts_custom_list = None
            
            # Calculate parts values
            parts_values = calculate_loop_values(
                loop_mode=parts_loop_mode,
                range_min=parts_range_min,
                range_max=parts_range_max,
                step_value=parts_step_value,
                num_loops=parts_num_loops,
                custom_list=parts_custom_list,
                n_total_aircraft=params['n_total_aircraft'],
                n_total_parts=params['n_total_parts'],
                pct_min=parts_pct_min,
                pct_max=parts_pct_max,
                pct_step=parts_pct_step,
                include_min=parts_include_min
            )
            
            st.write(f"**Parts values ({len(parts_values)}):** `{parts_values}`")
        
        # ===== DEPOT LOOP SECTION =====
        st.markdown("---")
        st.markdown("### 🏭 Depot Capacity Loop")
        
        enable_depot_loop = st.checkbox("Enable Depot Capacity Loop", value=False, 
                                        help="Run simulation multiple times with different depot capacity values")
        
        depot_values = [params['depot_capacity']]  # Default to single value from sidebar
        
        if enable_depot_loop:
            # Loop mode selection
            depot_loop_mode = st.selectbox(
                "Depot Loop Mode",
                options=['range', 'step', 'auto', 'custom', 'pct_aircraft', 'pct_parts'],
                format_func=lambda x: {
                    'range': '📊 Range (every integer)',
                    'step': '📏 Step (every X value)',
                    'auto': '🔢 Auto (N evenly spaced values)',
                    'custom': '✏️ Custom List',
                    'pct_aircraft': '✈️ % of Total Aircraft',
                    'pct_parts': '🔧 % of Total Parts'
                }.get(x, x),
                help="Select how to generate depot capacity values",
                key="depot_loop_mode"
            )
            
            col1, col2 = st.columns(2)
            
            # Mode-specific inputs
            if depot_loop_mode in ['range', 'step', 'auto']:
                with col1:
                    depot_range_min = st.number_input("Depot Range Min", min_value=1, value=5, step=1, key="depot_range_min")
                with col2:
                    depot_range_max = st.number_input("Depot Range Max", min_value=1, value=15, step=1, key="depot_range_max")
                
                if depot_loop_mode == 'step':
                    depot_step_value = st.number_input("Depot Step Value", min_value=1, value=2, step=1, key="depot_step")
                else:
                    depot_step_value = 1
                
                if depot_loop_mode == 'auto':
                    depot_num_loops = st.number_input("Number of Depot Values", min_value=2, value=5, step=1, key="depot_num_loops")
                else:
                    depot_num_loops = 5
                
                if depot_loop_mode in ['step', 'auto']:
                    depot_include_min = st.checkbox("Include minimum depot value", value=True, key="depot_include_min")
                else:
                    depot_include_min = True
                
                depot_custom_list = None
                depot_pct_min, depot_pct_max, depot_pct_step = 0.5, 1.5, 0.1
                
            elif depot_loop_mode == 'custom':
                st.write("Enter comma-separated depot capacity values:")
                depot_custom_input = st.text_input("Custom Depot Values", value="5, 10, 15, 20, 25",
                                                   help="e.g., 40, 47, 56, 57, 80, 100", key="depot_custom")
                try:
                    depot_custom_list = [int(x.strip()) for x in depot_custom_input.split(',') if x.strip()]
                except ValueError:
                    st.error("Invalid input. Please enter comma-separated integers.")
                    depot_custom_list = []
                
                depot_range_min, depot_range_max, depot_step_value, depot_num_loops = 5, 15, 1, 5
                depot_include_min = True
                depot_pct_min, depot_pct_max, depot_pct_step = 0.5, 1.5, 0.1
                
            else:  # pct_aircraft or pct_parts
                with col1:
                    depot_pct_min = st.number_input("Depot Min %", min_value=0.1, max_value=5.0, value=0.5, step=0.1,
                                                    format="%.1f", key="depot_pct_min")
                with col2:
                    depot_pct_max = st.number_input("Depot Max %", min_value=0.1, max_value=5.0, value=1.5, step=0.1,
                                                    format="%.1f", key="depot_pct_max")
                
                depot_pct_step = st.number_input("Depot Step %", min_value=0.05, max_value=1.0, value=0.1, step=0.05,
                                                 format="%.2f", key="depot_pct_step")
                
                depot_include_min = st.checkbox("Include minimum depot percentage", value=True, key="depot_pct_include_min")
                
                base_value = params['n_total_aircraft'] if depot_loop_mode == 'pct_aircraft' else params['n_total_parts']
                st.info(f"Base value: {base_value} ({'aircraft' if depot_loop_mode == 'pct_aircraft' else 'parts'})")
                
                depot_range_min, depot_range_max, depot_step_value, depot_num_loops = 5, 15, 1, 5
                depot_custom_list = None
            
            # Calculate depot values
            depot_values = calculate_loop_values(
                loop_mode=depot_loop_mode,
                range_min=depot_range_min,
                range_max=depot_range_max,
                step_value=depot_step_value,
                num_loops=depot_num_loops,
                custom_list=depot_custom_list,
                n_total_aircraft=params['n_total_aircraft'],
                n_total_parts=params['n_total_parts'],
                pct_min=depot_pct_min,
                pct_max=depot_pct_max,
                pct_step=depot_pct_step,
                include_min=depot_include_min
            )
            
            st.write(f"**Depot values ({len(depot_values)}):** `{depot_values}`")
        
        # ===== SUMMARY AND RUN BUTTON =====
        st.markdown("---")
        
        # Determine which loops are enabled
        any_loop_enabled = enable_parts_loop or enable_depot_loop
        
        if any_loop_enabled:
            total_simulations = len(parts_values) * len(depot_values)
            
            st.subheader("📊 Loop Summary")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Parts Values", len(parts_values))
            with col2:
                st.metric("Depot Values", len(depot_values))
            with col3:
                st.metric("Total Simulations", total_simulations)
            
            if enable_parts_loop and enable_depot_loop:
                st.info(f"**Nested Loop:** {len(parts_values)} parts × {len(depot_values)} depot = {total_simulations} simulations")
            
            # Run Loop button
            if st.button("🚀 Run Parameter Loop", type="primary", disabled=total_simulations == 0):
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                with st.spinner(f"Running {total_simulations} simulations..."):
                    if enable_parts_loop and enable_depot_loop:
                        # Nested loop
                        def update_progress_nested(current, total, parts_val, depot_val):
                            progress_bar.progress(current / total)
                            status_text.write(f"Running {current}/{total}: parts={parts_val}, depot={depot_val}")
                        
                        loop_results = run_nested_loop(
                            params=params,
                            parts_values=parts_values,
                            depot_values=depot_values,
                            random_seed=params['random_seed'],
                            progress_callback=update_progress_nested
                        )
                    elif enable_parts_loop:
                        # Parts loop only
                        def update_progress_parts(current, total, parts_val):
                            progress_bar.progress(current / total)
                            status_text.write(f"Running {current}/{total}: n_total_parts={parts_val}")
                        
                        loop_results = run_parts_loop(
                            params=params,
                            parts_values=parts_values,
                            random_seed=params['random_seed'],
                            progress_callback=update_progress_parts
                        )
                    else:
                        # Depot loop only
                        def update_progress_depot(current, total, depot_val):
                            progress_bar.progress(current / total)
                            status_text.write(f"Running {current}/{total}: depot_capacity={depot_val}")
                        
                        loop_results = run_depot_capacity_loop(
                            params=params,
                            depot_values=depot_values,
                            random_seed=params['random_seed'],
                            progress_callback=update_progress_depot
                        )
                
                progress_bar.empty()
                status_text.empty()
                
                # Store results
                st.session_state.loop_results = loop_results
                st.session_state.loop_run = True
                st.session_state.simulation_run = False  # Clear single run
                
                st.success(f"✅ Completed {total_simulations} simulations!")
                st.rerun()
        
        else:
            st.info("Enable at least one loop to run multiple simulations with different parameter values.")
            st.markdown("---")
            st.write("**Available Loop Modes:**")
            st.write("- **Range**: Run for every integer in range (e.g., 5-15 → 5,6,7...15)")
            st.write("- **Step**: Run every X values (e.g., step=5, range 100-200 → 100,105,110...)")
            st.write("- **Auto**: Automatically calculate N evenly spaced values")
            st.write("- **Custom**: Specify exact values to test")
            st.write("- **% Aircraft**: Use percentages of total aircraft count")
            st.write("- **% Parts**: Use percentages of total parts count (depot only)")
    
    # --- Display loop results if loop has been run ---
    if st.session_state.loop_run and st.session_state.loop_results is not None:
        loop_results = st.session_state.loop_results
        summary_df = loop_results['summary_df']
        
        # Determine if this is a nested loop (both parts and depot varied)
        has_parts_variation = summary_df['n_total_parts'].nunique() > 1 if 'n_total_parts' in summary_df.columns else False
        has_depot_variation = summary_df['depot_capacity'].nunique() > 1 if 'depot_capacity' in summary_df.columns else False
        is_nested = has_parts_variation and has_depot_variation
        
        with tab1:
            st.subheader("🔄 Parameter Loop Results")
            
            # Summary metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Simulations Run", len(summary_df))
            with col2:
                if has_parts_variation:
                    st.metric("Parts Range", f"{summary_df['n_total_parts'].min()} - {summary_df['n_total_parts'].max()}")
                else:
                    st.metric("Parts (fixed)", summary_df['n_total_parts'].iloc[0] if 'n_total_parts' in summary_df.columns else "N/A")
            with col3:
                if has_depot_variation:
                    st.metric("Depot Range", f"{summary_df['depot_capacity'].min()} - {summary_df['depot_capacity'].max()}")
                else:
                    st.metric("Depot (fixed)", summary_df['depot_capacity'].iloc[0] if 'depot_capacity' in summary_df.columns else "N/A")
            with col4:
                best_idx = summary_df['avg_micap_with_zeros'].idxmin()
                best_row = summary_df.loc[best_idx]
                best_parts = best_row.get('n_total_parts', 'N/A')
                best_depot = best_row.get('depot_capacity', 'N/A')
                best_micap = best_row['avg_micap_with_zeros']
                st.metric("Best Config", f"P:{best_parts} D:{best_depot}")
                st.caption(f"MICAP: {best_micap:.2f}")
            
            st.markdown("---")
            
            # MICAP Results Table
            st.subheader("📊 MICAP Statistics by Configuration")
            
            # Build display columns based on what varied
            display_cols = []
            col_names = []
            
            if 'n_total_parts' in summary_df.columns:
                display_cols.append('n_total_parts')
                col_names.append('Total Parts')
            if 'depot_capacity' in summary_df.columns:
                display_cols.append('depot_capacity')
                col_names.append('Depot Capacity')
            
            display_cols.extend(['avg_micap_with_zeros', 'count_all', 'avg_micap_no_zeros', 
                                'count_nonzero', 'max_micap', 'total_events'])
            col_names.extend(['Avg MICAP (incl. zeros)', 'Count (all)', 'Avg MICAP (excl. zeros)', 
                             'Count (non-zero)', 'Max MICAP', 'Total Events'])
            
            display_df = summary_df[display_cols].copy()
            display_df.columns = col_names
            
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            
            st.markdown("---")
            
            # Download section for loop results
            st.subheader("📥 Download Loop Results")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                # Summary CSV
                csv_summary = summary_df.to_csv(index=False)
                st.download_button(
                    label="📊 Download Summary (CSV)",
                    data=csv_summary,
                    file_name="loop_summary.csv",
                    mime="text/csv"
                )
            
            with col2:
                # Parameters CSV
                params_df = loop_results['all_params_df']
                csv_params = params_df.to_csv(index=False)
                st.download_button(
                    label="⚙️ Download Parameters (CSV)",
                    data=csv_params,
                    file_name="loop_params.csv",
                    mime="text/csv"
                )
            
            with col3:
                # Combined Excel with multiple sheets
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    summary_df.to_excel(writer, sheet_name='Summary', index=False)
                    params_df.to_excel(writer, sheet_name='Parameters', index=False)
                buffer.seek(0)
                
                st.download_button(
                    label="📁 Download All (Excel)",
                    data=buffer,
                    file_name="loop_results.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        
        with tab2:
            st.subheader("📊 Loop Simulation Data")
            st.info("Select a specific run to view detailed data.")
            
            # Create dropdown options with both parts and depot info
            run_options = []
            for r in loop_results['all_results']:
                parts_val = r.get('n_total_parts', 'N/A')
                depot_val = r.get('depot_capacity', 'N/A')
                run_options.append(f"Parts: {parts_val}, Depot: {depot_val}")
            
            selected_run_idx = st.selectbox("Select Run", options=range(len(run_options)), 
                                            format_func=lambda i: run_options[i])
            
            if selected_run_idx is not None and selected_run_idx < len(loop_results['all_results']):
                selected_result = loop_results['all_results'][selected_run_idx]
                datasets = selected_result['datasets']
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Parts", selected_result.get('n_total_parts', 'N/A'))
                with col2:
                    st.metric("Depot Capacity", selected_result.get('depot_capacity', 'N/A'))
                with col3:
                    st.metric("all_parts_df Rows", f"{len(datasets.all_parts_df):,}")
                with col4:
                    st.metric("all_ac_df Rows", f"{len(datasets.all_ac_df):,}")
                
                with st.expander("all_parts_df (Part Event Log) - First 10 Rows"):
                    st.dataframe(datasets.all_parts_df.head(10))
                
                with st.expander("all_ac_df (Aircraft Event Log) - First 10 Rows"):
                    st.dataframe(datasets.all_ac_df.head(10))
        
        with tab3:
            st.subheader("📈 Loop WIP Plots")
            
            if params['render_plots']:
                # Create dropdown options with both parts and depot info
                run_options_plot = []
                for r in loop_results['all_results']:
                    parts_val = r.get('n_total_parts', 'N/A')
                    depot_val = r.get('depot_capacity', 'N/A')
                    run_options_plot.append(f"Parts: {parts_val}, Depot: {depot_val}")
                
                selected_plot_idx = st.selectbox("Select Run for Plots", options=range(len(run_options_plot)), 
                                                 format_func=lambda i: run_options_plot[i], key="plot_selector")
                
                if selected_plot_idx is not None and selected_plot_idx < len(loop_results['all_results']):
                    selected_result = loop_results['all_results'][selected_plot_idx]
                    datasets = selected_result['datasets']
                    
                    if datasets.wip_df is not None and len(datasets.wip_df) > 0:
                        st.pyplot(datasets.plot_aircraft_wip_combined())
                        st.pyplot(datasets.plot_micap_over_time())
                        st.pyplot(datasets.plot_fleet_over_time())
                    else:
                        st.warning("No WIP data available for this run.")
                    
                    st.pyplot(datasets.plot_condition_a_wip_over_time())
            else:
                st.info("Plot rendering is disabled. Check 'Render Plots' in sidebar to enable.")
    
    # --- Display results if single simulation has been run ---
    elif st.session_state.simulation_run:
        # Retrieve from session state
        datasets = st.session_state.datasets
        validation_results = st.session_state.validation_results
        allocation = st.session_state.allocation
        df_manager = st.session_state.df_manager
        
        # Display event summary 777
        if 'event_counts' in validation_results:
            event_counts = validation_results['event_counts']

            ############################
            # TAB 1 
            ############################
            with tab1:
                st.subheader("📊 Event Processing Summary")
                st.write("Event count statistics")

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Events", f"{event_counts['total']:,}")
                    st.metric("Depot Completions", f"{event_counts.get('depot_complete', 0):,}")
                with col2:
                    st.metric("Fleet Completions", f"{event_counts.get('fleet_complete', 0):,}")
                    st.metric("Part Fleet Ends", f"{event_counts.get('part_fleet_end', 0):,}")
                with col3:
                    st.metric("New Parts Arrived", f"{event_counts.get('new_part_arrives', 0):,}")
                    st.metric("Parts Condemned", f"{event_counts.get('part_condemn', 0):,}")
                
                render_stats_tab(datasets)

            ############################
            # TAB 2
            ############################
            with tab2:
                st.subheader("📊 Simulation Statistics")
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.write("Part and Aircraft data exported from managers (need to update this section to include more proper information)")
                with col2:
                    st.metric("all_parts_df Rows", 
                            f"{len(datasets.all_parts_df):,}")
                with col3:
                    st.metric("all_ac_df Rows", 
                            f"{len(datasets.all_ac_df):,}")
                
                # --- Display Sample Data ---
                st.subheader("🔍 Sample Data")
                
                with st.expander("all_parts_df (Part Event Log) - First 10 Rows"):
                    st.dataframe(datasets.all_parts_df.head(10))
                
                with st.expander("all_ac_df (Aircraft Event Log) - First 10 Rows"):
                    st.dataframe(datasets.all_ac_df.head(10))

                ###########################
                # Render all duration plots 
                # from ui/dist_plots.py
                #############################
                if params['render_plots']:
                    render_duration_plots(datasets.all_parts_df, allocation)
                

            ############################
            # TAB 3
            ############################
            with tab3:           
                # Plot WIP over time using new DataSets methods
                if params['render_plots']:
                    if datasets.wip_df is not None and len(datasets.wip_df) > 0:
                        st.subheader("📈 Aircraft WIP Over Time (Combined)")
                        st.pyplot(datasets.plot_aircraft_wip_combined())
                        
                        st.subheader("📈 MICAP Over Time")
                        st.pyplot(datasets.plot_micap_over_time())
                        
                        st.subheader("📈 Fleet Over Time")
                        st.pyplot(datasets.plot_fleet_over_time())
                    else:
                        st.warning("No Aircraft WIP data available to plot.")
                    
                    # Condition A plot (separate from Aircraft WIP)
                    st.subheader("📈 Condition A Over Time")
                    st.pyplot(datasets.plot_condition_a_wip_over_time())
                else:
                    st.info("Plot rendering is disabled. Check 'Render Plots' in sidebar to enable.")

                # --- Download Results ---
                render_download_section(datasets, df_manager)
            ############################
            # END OF TAB 3 
            ############################
    
    # Show message when no simulation has run yet
    if not st.session_state.simulation_run and not st.session_state.loop_run:
        with tab1:
            st.info("Run the simulation to see cycle statistics, or enable the loop in Setup tab.")
        with tab2:
            st.info("Run the simulation to see results.")
        with tab3:
            st.info("Run the simulation to see WIP plots.")

if __name__ == "__main__":
    main()
