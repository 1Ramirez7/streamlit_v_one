"""
main.py
-----------------
Streamlit entrypoint for the Hill AFB DES simulation interface.

This module handles UI flow and delegates logic to supporting modules:
    - ui_components: user interface and parameter input
    - data_manager: DataFrame structure management
    - simulation_engine: DES core logic
    - plotting: visualizations

Usage
-----
Run 'run_streamlit_app.py' in root of directory
"""
import streamlit as st
import numpy as np
import pandas as pd
from io import BytesIO

import warnings # to silent future warnings, comment to test
warnings.simplefilter("ignore", category=FutureWarning)


from data_manager import DataFrameManager
from simulation_engine import SimulationEngine
from ui.ui_components import render_sidebar
#from streamlit_app.ui.cycle import render_cycle
from ui.wip_plots import render_wip_plots, plot_wip_over_time # 777
from utils import calculate_initial_allocation
from ui.dist_plots import (
    plot_fleet_duration_full,
    plot_fleet_duration_no_init,
    plot_fleet_duration_init_only,
    plot_condition_f_duration,
    plot_depot_duration_full,
    plot_depot_duration_no_init,
    plot_depot_duration_init_only,
    plot_cond_a_duration,
    render_duration_plots
)

def main() -> None:
    st.title("Hill AFB Discrete Event Simulation")
    st.markdown("Configure simulation parameters in the sidebar and click **Run Simulation**.")

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
            
            # Create SimulationEngine
            engine = SimulationEngine(
                df_manager=df_manager,
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
        
        st.success("Simulation complete!")
        
        # Display event summary 777
        if 'event_counts' in validation_results:
            st.subheader("📊 Event Processing Summary")
            event_counts = validation_results['event_counts']
            
            tab1, tab2, tab3 = st.tabs(["Cycle", "Simulation Results", "WIP Plots"])

            ############################
            # TAB 1 
            ############################
            with tab1:
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

            ############################
            # TAB 2
            ############################
            with tab2:
                st.subheader("📊 Simulation Statistics")
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("sim_df Rows Used", 
                            f"{validation_results['sim_df_rows_used']:,}")
                    st.metric("sim_df Usage", 
                            f"{validation_results['sim_df_usage_pct']:.1f}%")
                with col2:
                    st.metric("des_df Rows Used", 
                            f"{validation_results['des_df_rows_used']:,}")
                    st.metric("des_df Usage", 
                            f"{validation_results['des_df_usage_pct']:.1f}%")
                
                # Display warnings if any
                if validation_results['warnings']:
                    st.warning("⚠️ Validation Warnings:")
                    for warning in validation_results['warnings']:
                        st.write(f"- {warning}")
                
                # --- Display Sample Data ---
                st.subheader("🔍 Sample Data")
                
                with st.expander("sim_df (Part Event Log) - First 10 Rows"):
                    st.dataframe(df_manager.sim_df.head(10))
                
                with st.expander("des_df (Aircraft Event Log) - First 10 Rows"):
                    st.dataframe(df_manager.des_df.head(10))

                ###########################
                # Render all duration plots 
                # from ui/dist_plots.py
                #############################
                render_duration_plots(df_manager.sim_df, allocation)
                
                # --- Download Results as CSV ---
                st.markdown("---")
                st.subheader("💾 Download Results")
                
                # Combined des_df and sim_df into an Excel file with separate tabs
                from io import BytesIO
                
                output = BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df_manager.sim_df.to_excel(writer, sheet_name='sim_df', index=False)
                    df_manager.des_df.to_excel(writer, sheet_name='des_df', index=False)
                
                excel_data = output.getvalue()
                
                st.download_button(
                    label="📥 Download Simulation Results (Excel)",
                    data=excel_data,
                    file_name="simulation_results.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            ############################
            # TAB 3
            ############################
            with tab3:           
                # Plot WIP over time 777
                if 'wip_history' in validation_results and len(validation_results['wip_history']) > 0:
                    st.subheader("📈 Work-in-Progress Over Time")
                    fig6 = plot_wip_over_time(validation_results['wip_history'])
                    st.pyplot(fig6)     
                # Add WIP plots section
                if 'wip_history' in validation_results and len(validation_results['wip_history']) > 0:
                    st.subheader("📈 Individual Work-in-Progress Over Time")
                    render_wip_plots(validation_results['wip_history'])
            ############################
            # END OF TAB 3 
            ############################
    
    else:
        st.info("Adjust parameters in the sidebar and click **Run Simulation** to begin.")

if __name__ == "__main__":
    main()
