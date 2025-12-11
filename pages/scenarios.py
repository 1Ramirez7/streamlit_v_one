"""
scenarios.py
-----------------
Scenarios page - Runs multiple simulations with different parameters

- Provides summarize comparison results
- Fast Mode OFF: Full simulation with time series plot generation
- Fast Mode ON: Speed optimized without time series plots

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

from parameters import Parameters
from streamlit_app.ui.sc_sidebar import render_scenarios_sidebar
from streamlit_app.ui.sc_loop import render_loop_params
from streamlit_app.ui.sc_results import (
    compute_summaries,
    display_best_metrics,
)
from streamlit_app.ui.sc_tabs import (
    render_charts_tab,
    render_all_metrics_tab,
    render_full_data_tab,
    close_all_figures,
)
from streamlit_app.sc_utils import (
    run_single_simulation,
    run_single_simulation_fast,
    generate_analysis_text,
    fig_to_bytes
)


def main() -> None:
    st.title("📊 Scenarios: Discrete Event Simulations")
    st.markdown("Vary Depot Capacity and Number of Total Parts")
    
    # ==========================================================================
    # SIDEBAR: Get all parameters from sc_sidebar.render_scenarios_sidebar
    # ==========================================================================
    sidebar_params = render_scenarios_sidebar()
    
    # Extract values from sidebar_params
    fast_mode = sidebar_params['fast_mode']
    n_total_aircraft = sidebar_params['n_total_aircraft']
    analysis_periods = sidebar_params['analysis_periods']
    condemn_cycle = sidebar_params['condemn_cycle']
    condemn_depot_fraction = sidebar_params['condemn_depot_fraction']
    part_order_lag = sidebar_params['part_order_lag']
    random_seed = sidebar_params['random_seed']
    mission_capable_rate = sidebar_params['mission_capable_rate']
    sone_dist = sidebar_params['sone_dist']
    sthree_dist = sidebar_params['sthree_dist']
    sone_mean = sidebar_params['sone_mean']
    sone_sd = sidebar_params['sone_sd']
    sthree_mean = sidebar_params['sthree_mean']
    sthree_sd = sidebar_params['sthree_sd']
    double_periods = sidebar_params['double_periods']
    warmup_periods = sidebar_params['warmup_periods']
    closing_periods = sidebar_params['closing_periods']
    sim_time = sidebar_params['sim_time']
    use_percentage_plots = sidebar_params['use_percentage_plots']
    fleet_rand_params = sidebar_params['fleet_rand_params']
    depot_rand_params = sidebar_params['depot_rand_params']
    
    # ==========================================================================
    # MAIN AREA - TABS
    # ==========================================================================
    tab0, tab1, tab2, tab3, tab4, tab5 = st.tabs(["⚙️ Setup", "Charts", "All Metrics", "Full Data", "MICAP Time Series", "Download"])
    
    # ==========================================================================
    # TAB 0 - Setup (Loop Parameters)
    # ==========================================================================
    with tab0:
        st.subheader("🔧 Setup Options")
        st.write("Configure loop parameters for the scenario simulation.")
        
        if fast_mode:
            st.info("⚡ **Fast Mode**: Plot rendering is disabled for maximum speed.")
        
        # Render loop parameters in the Setup tab
        loop_params = render_loop_params()
        parts_values = loop_params['parts_values']
        depot_values = loop_params['depot_values']
        total_runs = loop_params['total_runs']
        
        # Run button in the main area (Setup tab)
        run_button_main = st.button("▶️ Run All Simulations", type="primary", key="run_scenario_main")
    

    # ==========================================================================
    # RUN Simulation
    # ==========================================================================
    run_button_sidebar = st.sidebar.button("▶️ Run All Simulations", type="primary", key="run_scenario_sidebar")
    
    # Either button triggers the simulation
    run_button = run_button_main or run_button_sidebar
    
    # ==========================================================================
    # SESSION STATE INIT
    # st.session_state is needed so when session state changes (field updates)
    # but run simulation was not trigger than previous run simulation results will persist
    # ==========================================================================
    if 'scenario_results' not in st.session_state: 
        st.session_state.scenario_results = None
    if 'scenario_params' not in st.session_state:
        st.session_state.scenario_params = None
    if 'scenario_depot_values' not in st.session_state:
        st.session_state.scenario_depot_values = None
    if 'scenario_parts_values' not in st.session_state:
        st.session_state.scenario_parts_values = None
    
    if run_button:
        if not depot_values or not parts_values:
            st.error("Please enter valid values for all loop parameters.")
            return
        
        # Required: Need to close them or previous data will counpond over runs
        plt.close('all')
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Cumulative event counts display
        event_counts_container = st.container()
        with event_counts_container:
            st.markdown("### 📊 Cumulative Event Counts")
            event_cols = st.columns(2)
            with event_cols[0]:
                cumulative_events_display = st.empty()
            with event_cols[1]:
                last_run_events_display = st.empty()
        
        # Terminal-like output display
        terminal_container = st.container()
        with terminal_container:
            st.markdown("### Live Results")
            terminal_max_lines = 20 # Fixed at 20 lines
            terminal_display = st.empty()
        
        all_results = []
        run_count = 0
        cumulative_total_events = 0
        terminal_output_lines = [] # Store terminal output lines
        
        # Build base params dict
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
            'render_plots': not fast_mode,  # False when fast_mode is ON
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
                    # Use appropriate simulation function based on fast_mode
                    if fast_mode:
                        result = run_single_simulation_fast(params, depot_cap, n_parts)
                    else:
                        result = run_single_simulation(params, depot_cap, n_parts)
                    
                    result['depot_capacity'] = depot_cap
                    result['n_total_parts'] = n_parts
                    all_results.append(result)
                    
                    # Update cumulative event counts
                    last_run_events = result.get('total_events', 0)
                    cumulative_total_events += last_run_events
                    
                    # Update live display
                    cumulative_events_display.metric(
                        "Cumulative Total Events", 
                        f"{cumulative_total_events:,}"
                    )
                    last_run_events_display.metric(
                        f"Last Run Events (depot={depot_cap}, parts={n_parts})", 
                        f"{last_run_events:,}"
                    )
                    
                    # Add result line to terminal output
                    avg_micap = result.get('avg_micap', 0)
                    avg_fleet = result.get('avg_fleet', 0)
                    terminal_line = f"[{run_count:3d}/{total_runs}] depot={depot_cap:3d}, parts={n_parts:3d} | Avg MICAP: {avg_micap:6.2f}, Avg Fleet: {avg_fleet:6.2f}, Events: {last_run_events:,}"
                    terminal_output_lines.append(terminal_line)
                    
                    # Keep only the last N lines based on user selection, reversed (newest first)
                    display_lines = terminal_output_lines[-terminal_max_lines:][::-1]
                    terminal_text = "\n".join(display_lines)
                    terminal_display.code(terminal_text, language="text")
                    
                except Exception as e:
                    st.warning(f"Run {run_count} failed: {e}")
                    import traceback
                    st.code(traceback.format_exc())
                    
                    # Add error line to terminal output
                    terminal_line = f"[{run_count:3d}/{total_runs}] depot={depot_cap:3d}, parts={n_parts:3d} | ERROR: {str(e)}"
                    terminal_output_lines.append(terminal_line)
                    display_lines = terminal_output_lines[-terminal_max_lines:][::-1]
                    terminal_text = "\n".join(display_lines)
                    terminal_display.code(terminal_text, language="text")
        
        progress_bar.empty()
        status_text.empty()
        
        # Final cumulative event count display
        cumulative_events_display.metric(
            "✅ Total Events Processed", 
            f"{cumulative_total_events:,}"
        )
        last_run_events_display.empty()
        
        st.session_state.scenario_results = pd.DataFrame(all_results)
        st.session_state.scenario_params = base_params
        st.session_state.scenario_depot_values = depot_values
        st.session_state.scenario_parts_values = parts_values
        
        mode_label = "Fast Mode" if fast_mode else "Full Mode"
        st.success(f"✅ Completed {len(all_results)} simulations ({mode_label})! Total events processed: {cumulative_total_events:,}")
    
    # ==========================================================================
    # DISPLAY RESULTS IN TABS
    # ==========================================================================
    has_results = st.session_state.scenario_results is not None
    
    if has_results:
        df = st.session_state.scenario_results
        params_dict = st.session_state.scenario_params
        depot_vals = st.session_state.scenario_depot_values
        parts_vals = st.session_state.scenario_parts_values
        
        # Compute summaries using shared function
        summaries = compute_summaries(df)
        depots = summaries['depots']
        parts_unique = summaries['parts_unique']
        best_results = summaries['best_results']
        best_by_parts = summaries['best_by_parts']
        summary_df = summaries['summary_df']
        summary_parts_df = summaries['summary_parts_df']
        best_row = summaries['best_row']
        
        # Display best metrics at top
        display_best_metrics(best_row)
        
        # Tab 1: Charts
        with tab1:
            fig_micap, fig_micap2 = render_charts_tab(df, depots, parts_unique)
        
        # Tab 2: All Metrics
        with tab2:
            fig_fleet, fig_cdf, fig_depot, fig_cda, fig_fleet2, fig_cdf2, fig_depot2, fig_cda2 = \
                render_all_metrics_tab(df, depots, parts_unique, fig_micap, fig_micap2)
        
        # Tab 3: Full Data
        with tab3:
            render_full_data_tab(df, summary_df, summary_parts_df)
        
        # Tab 4: MICAP Time Series
        with tab4:
            st.subheader("Time Series Plots (Per Simulation)")
            st.markdown("Each plot shows metrics over simulation time for a specific depot/parts combination.")
            
            # Figure display names
            FIG_DISPLAY_NAMES = {
                'micap': 'MICAP',
                'depot': 'Depot WIP',
            }
            
            # Store all figure bytes for download
            all_ts_bytes = {}
            
            for fig_key, display_name in FIG_DISPLAY_NAMES.items():
                st.markdown(f"### {display_name} Over Time")
                
                plot_bytes = {}
                
                for idx, row in df.iterrows():
                    depot_cap = int(row['depot_capacity'])
                    n_parts = int(row['n_total_parts'])
                    wip_figs_bytes = row.get('wip_figs_bytes', {})
                    fig_bytes = wip_figs_bytes.get(fig_key) if wip_figs_bytes else None
                    
                    if fig_bytes is not None:
                        sim_key = f"depot_{depot_cap}_parts_{n_parts}"
                        plot_bytes[sim_key] = fig_bytes
                
                if plot_bytes:
                    st.info(f"Generated {len(plot_bytes)} {display_name} plot(s)")
                    for sim_key, img_bytes in plot_bytes.items():
                        with st.expander(f"📈 {display_name}: {sim_key.replace('_', ' ').title()}", expanded=False):
                            st.image(img_bytes, use_container_width=True)
                else:
                    st.warning(f"No {display_name} data available.")
                
                all_ts_bytes[fig_key] = plot_bytes
            
            st.session_state.scenario_ts_bytes = all_ts_bytes
        
        # Tab 5: Download
        with tab5:
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
                zf.writestr('scenario_results.xlsx', excel_buffer.getvalue())
                
                # Text analysis file
                analysis_text = generate_analysis_text(
                    df, best_results, best_by_parts, params_dict, depot_vals, parts_vals
                )
                zf.writestr('scenario_analysis.txt', analysis_text)
                
                # Comparison plot images
                zf.writestr('micap_vs_parts_ln_depot.png', fig_to_bytes(fig_micap))
                zf.writestr('micap_vs_depot_ln_parts.png', fig_to_bytes(fig_micap2))
                zf.writestr('fleet_vs_parts_ln_depot.png', fig_to_bytes(fig_fleet))
                zf.writestr('cdf_vs_parts_ln_depot.png', fig_to_bytes(fig_cdf))
                zf.writestr('depot_vs_parts_ln_depot.png', fig_to_bytes(fig_depot))
                zf.writestr('cda_vs_parts_ln_depot.png', fig_to_bytes(fig_cda))
                zf.writestr('fleet_vs_depot_ln_parts.png', fig_to_bytes(fig_fleet2))
                zf.writestr('cdf_vs_depot_ln_parts.png', fig_to_bytes(fig_cdf2))
                zf.writestr('depot_vs_depot_ln_parts.png', fig_to_bytes(fig_depot2))
                zf.writestr('cda_vs_depot_ln_parts.png', fig_to_bytes(fig_cda2))
                
                # Time series plots
                all_ts_bytes_download = st.session_state.get('scenario_ts_bytes', {})
                for plot_type, plot_bytes in all_ts_bytes_download.items():
                    for sim_key, img_bytes in plot_bytes.items():
                        zf.writestr(f'timeseries_{plot_type}_{sim_key}.png', img_bytes)
            
            zip_buffer.seek(0)
            
            st.download_button(
                label="Download All Results (ZIP)",
                data=zip_buffer,
                file_name="scenario_results.zip",
                mime="application/zip"
            )
            
            # Close all figures
            close_all_figures(
                fig_micap, fig_micap2, fig_fleet, fig_cdf, fig_depot, fig_cda,
                fig_fleet2, fig_cdf2, fig_depot2, fig_cda2
            )
    
    # If no results yet, show message in results tabs
    if not has_results:
        with tab1:
            st.info("Configure parameters in the **⚙️ Setup** tab and click **Run All Simulations** to start.")
        with tab2:
            st.info("Configure parameters in the **⚙️ Setup** tab and click **Run All Simulations** to start.")
        with tab3:
            st.info("Configure parameters in the **⚙️ Setup** tab and click **Run All Simulations** to start.")
        with tab4:
            st.info("Configure parameters in the **⚙️ Setup** tab and click **Run All Simulations** to start.")
        with tab5:
            st.info("Configure parameters in the **⚙️ Setup** tab and click **Run All Simulations** to start.")


if __name__ == "__main__":
    main()
