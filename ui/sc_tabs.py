"""
Stores the code for scenario TABs
"""
import streamlit as st
import matplotlib.pyplot as plt

from ui.sc_results import get_metrics_list, create_metric_plot


def render_charts_tab(df, depots, parts_unique):
    """
    Render Tab 1: Charts (MICAP comparison plots).
    
    Args:
        df: DataFrame with simulation results
        depots: List of depot capacity values
        parts_unique: List of n_total_parts values
        
    Returns:
        tuple: (fig_micap, fig_micap2) for use in other tabs/downloads
    """
    metrics = get_metrics_list()
    
    st.subheader("MICAP vs Number of Total Parts (by Depot Capacity)")
    fig_micap = create_metric_plot(df, depots, parts_unique, *metrics[0], by_depot=True)
    st.pyplot(fig_micap)

    st.subheader("MICAP vs Depot Capacity (by Total Parts)")
    fig_micap2 = create_metric_plot(df, depots, parts_unique, *metrics[0], by_depot=False)
    st.pyplot(fig_micap2)
    
    return fig_micap, fig_micap2


def render_all_metrics_tab(df, depots, parts_unique, fig_micap, fig_micap2):
    """
    Render Tab 2: All Metrics comparison plots.
    
    Args:
        df: DataFrame with simulation results
        depots: List of depot capacity values
        parts_unique: List of n_total_parts values
        fig_micap: MICAP by depot figure (from charts tab)
        fig_micap2: MICAP by parts figure (from charts tab)
        
    Returns:
        tuple: All metric figures for download
               (fig_fleet, fig_cdf, fig_depot, fig_cda, 
                fig_fleet2, fig_cdf2, fig_depot2, fig_cda2)
    """
    metrics = get_metrics_list()
    
    st.subheader("All Metrics by Configuration")
    
    # Section 1: By N Total Parts (lines = Depot Capacity)
    st.markdown("#### By N Total Parts (lines = Depot Capacity)")
    st.pyplot(fig_micap)
    
    col1, col2 = st.columns(2)
    with col1:
        fig_fleet = create_metric_plot(df, depots, parts_unique, *metrics[1], by_depot=True)
        st.pyplot(fig_fleet)
    with col2:
        fig_cdf = create_metric_plot(df, depots, parts_unique, *metrics[2], by_depot=True)
        st.pyplot(fig_cdf)
    
    col3, col4 = st.columns(2)
    with col3:
        fig_depot = create_metric_plot(df, depots, parts_unique, *metrics[3], by_depot=True)
        st.pyplot(fig_depot)
    with col4:
        fig_cda = create_metric_plot(df, depots, parts_unique, *metrics[4], by_depot=True)
        st.pyplot(fig_cda)
    
    st.markdown("---")
    st.markdown("#### By Depot Capacity (lines = N Total Parts)")
    st.pyplot(fig_micap2)
    
    col5, col6 = st.columns(2)
    with col5:
        fig_fleet2 = create_metric_plot(df, depots, parts_unique, *metrics[1], by_depot=False)
        st.pyplot(fig_fleet2)
    with col6:
        fig_cdf2 = create_metric_plot(df, depots, parts_unique, *metrics[2], by_depot=False)
        st.pyplot(fig_cdf2)
    
    col7, col8 = st.columns(2)
    with col7:
        fig_depot2 = create_metric_plot(df, depots, parts_unique, *metrics[3], by_depot=False)
        st.pyplot(fig_depot2)
    with col8:
        fig_cda2 = create_metric_plot(df, depots, parts_unique, *metrics[4], by_depot=False)
        st.pyplot(fig_cda2)
    
    return fig_fleet, fig_cdf, fig_depot, fig_cda, fig_fleet2, fig_cdf2, fig_depot2, fig_cda2


def render_full_data_tab(df, summary_df, summary_parts_df):
    """
    Render Tab 3: Full Results data tables.
    
    Args:
        df: DataFrame with simulation results
        summary_df: Best by depot summary DataFrame
        summary_parts_df: Best by parts summary DataFrame
    """
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


def close_all_figures(*figs):
    """Close all provided matplotlib figures."""
    for fig in figs:
        if fig is not None:
            plt.close(fig)
