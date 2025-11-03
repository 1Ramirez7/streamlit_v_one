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

from data_manager import DataFrameManager
from simulation_engine import SimulationEngine
from ui_components import render_sidebar
from plotting import (
    plot_fleet_duration,
    plot_condition_f_duration,
    plot_depot_duration,
    plot_install_duration,
    plot_micap_over_time
)

def main() -> None:
    st.title("Hill AFB Discrete Event Simulation")
    st.markdown("Configure simulation parameters in the sidebar and click **Run Simulation**.")

    # --- Get inputs from UI ---
    params = render_sidebar()
    
    # --- Run Simulation after clicking "Run Simulation" button ---
    run_button = st.sidebar.button("Run Simulation", type="primary")
    
    if run_button:
        with st.spinner("Running simulation..."):
            # Set random seed for reproducibility
            np.random.seed(123)
            
            # Create DataFrameManager
            df_manager = DataFrameManager(
                n_total_parts=params['n_total_parts'],
                n_total_aircraft=params['n_total_aircraft'],
                sim_time=params['sim_time'],
                sone_mean=params['sone_mean'],
                stwo_mean=params['stwo_mean'],
                sthree_mean=params['sthree_mean'],
                sfour_mean=params['sfour_mean']
            )
            
            # Create SimulationEngine
            engine = SimulationEngine(
                df_manager=df_manager,
                sone_mean=params['sone_mean'],
                sone_sd=params['sone_sd'],
                stwo_mean=params['stwo_mean'],
                stwo_sd=params['stwo_sd'],
                sthree_mean=params['sthree_mean'],
                sthree_sd=params['sthree_sd'],
                sfour_mean=params['sfour_mean'],
                sfour_sd=params['sfour_sd'],
                sim_time=params['sim_time']
            )
            
            # Run simulation
            validation_results = engine.run()
            
            st.success("Simulation complete!")
            
            # --- Display Validation Results ---
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
            #with col3:
            #    st.metric("MICAP Events", 
            #             f"{len(df_manager.micap_df)}")
            #    st.metric("Parts in Available", 
            #             f"{len(df_manager.condition_a_df)}")
            
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
            
            #with st.expander("micap_df (MICAP Events)"):
             #   if len(df_manager.micap_df) > 0:
             #       st.dataframe(df_manager.micap_df)
             #   else:
             #       st.info("No MICAP events occurred during simulation.")
            
            #with st.expander("condition_a_df (Parts Waiting)"):
             #   if len(df_manager.condition_a_df) > 0:
              #      st.dataframe(df_manager.condition_a_df)
             #   else:
             #       st.info("No parts currently in available inventory.")
            
            # --- Plot Results ---
            st.subheader("📈 Stage Duration Distributions")
            
            col1, col2 = st.columns(2)
            with col1:
                fig1 = plot_fleet_duration(df_manager.sim_df)
                st.pyplot(fig1)
            with col2:
                fig2 = plot_condition_f_duration(df_manager.sim_df)
                st.pyplot(fig2)
            
            col3, col4 = st.columns(2)
            with col3:
                fig3 = plot_depot_duration(df_manager.sim_df)
                st.pyplot(fig3)
            with col4:
                fig4 = plot_install_duration(df_manager.sim_df)
                st.pyplot(fig4)
            
            # Plot MICAP events
            st.subheader("🚨 MICAP Events")
            fig5 = plot_micap_over_time(df_manager.des_df) # controls what df plotting.py uses
            st.pyplot(fig5)
            
            # --- Download Results as CSV ---
            st.markdown("---")
            st.subheader("💾 Download Results")
            
            # Combined des_df and sim_df into an Excel file with separate tabs
            # Excluded micap_df from download (MICAP data already in des_df)
            # TODO: Inspect full codebase impact before removing micap_df entirely
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
    
    else:
        st.info("Adjust parameters in the sidebar and click **Run Simulation** to begin.")

if __name__ == "__main__":
    main()
