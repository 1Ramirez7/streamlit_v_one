import streamlit as st
import matplotlib.pyplot as plt


def plot_wip_over_time(wip_df): # wip_df = self.engine.wip_history
    """
    Plot work-in-progress (WIP) levels over time for parts and aircraft.
    
    Parameters
    ----------
    wip_df : pandas.DataFrame
        DataFrame with columns: time, parts_fleet, parts_condition_f, 
        parts_depot, parts_condition_a, aircraft_fleet, aircraft_micap
    """
    if len(wip_df) == 0:
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
        ax1.text(0.5, 0.5, 'No WIP data available', ha='center', va='center')
        ax2.text(0.5, 0.5, 'No WIP data available', ha='center', va='center')
        ax1.set_title('Parts WIP Over Time')
        ax2.set_title('Aircraft WIP Over Time')
        return fig
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
    
    # Plot 1: Parts in each stage
    ax1.plot(wip_df['time'], wip_df['parts_fleet'], label='Fleet', linewidth=2)
    ax1.plot(wip_df['time'], wip_df['parts_condition_f'], label='Condition F', linewidth=2)
    ax1.plot(wip_df['time'], wip_df['parts_depot'], label='Depot', linewidth=2)
    ax1.plot(wip_df['time'], wip_df['parts_condition_a'], label='Condition A', linewidth=2)
    
    ax1.set_xlabel('Simulation Time')
    ax1.set_ylabel('Number of Parts')
    ax1.set_title('Parts Work-in-Progress by Stage')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Aircraft status
    ax2.plot(wip_df['time'], wip_df['aircraft_fleet'], label='Fleet (Active)', linewidth=2, color='green')
    ax2.plot(wip_df['time'], wip_df['aircraft_micap'], label='MICAP', linewidth=2, color='red')
    
    ax2.set_xlabel('Simulation Time')
    ax2.set_ylabel('Number of Aircraft')
    ax2.set_title('Aircraft Status Over Time')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    return fig

def render_wip_plots(wip_history_df):
    """
    Render individual WIP plots over time for each of the 4 main stages plus MICAP.
    
    Parameters:
    wip_history_df : pandas.DataFrame
        DataFrame from validation_results['wip_history'] with columns:
        time, parts_fleet, parts_condition_f, parts_depot, parts_condition_a, aircraft_micap
    """
    if len(wip_history_df) == 0:
        st.warning("No WIP data available to plot.")
        return
    
    # Create individual plots for each stage
    micap_fig = plot_micap_over_time(wip_history_df)
    fleet_fig = plot_fleet_wip_over_time(wip_history_df)
    condition_f_fig = plot_condition_f_wip_over_time(wip_history_df)
    depot_fig = plot_depot_wip_over_time(wip_history_df)
    condition_a_fig = plot_condition_a_wip_over_time(wip_history_df)
    
    # Display plots
    st.pyplot(micap_fig)
    st.pyplot(fleet_fig)
    st.pyplot(condition_f_fig)
    st.pyplot(depot_fig)
    st.pyplot(condition_a_fig)


def plot_micap_over_time(wip_history_df):
    """Plot MICAP aircraft count over time."""
    fig, ax = plt.subplots(figsize=(10, 5))
    
    ax.step(wip_history_df['time'], wip_history_df['aircraft_micap'], where='post', linewidth=2, color='red')
    ax.set_xlabel('Simulation Time')
    ax.set_ylabel('Number of Aircraft in MICAP')
    ax.set_title('MICAP Status Over Time')
    ax.grid(True, alpha=0.3)
    ax.set_ylim(bottom=0)
    
    plt.tight_layout()
    return fig

def plot_fleet_wip_over_time(wip_history_df):
    """Plot Fleet WIP over time."""
    fig, ax = plt.subplots(figsize=(10, 5))
    
    ax.plot(wip_history_df['time'], wip_history_df['parts_fleet'], linewidth=2, color='steelblue')
    ax.set_xlabel('Simulation Time')
    ax.set_ylabel('Number of Parts')
    ax.set_title('Fleet WIP Over Time')
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    return fig


def plot_condition_f_wip_over_time(wip_history_df):
    """Plot Condition F WIP over time."""
    fig, ax = plt.subplots(figsize=(10, 5))
    
    ax.plot(wip_history_df['time'], wip_history_df['parts_condition_f'], linewidth=2, color='coral')
    ax.set_xlabel('Simulation Time')
    ax.set_ylabel('Number of Parts')
    ax.set_title('Condition F WIP Over Time')
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    return fig


def plot_depot_wip_over_time(wip_history_df):
    """Plot Depot WIP over time."""
    fig, ax = plt.subplots(figsize=(10, 5))
    
    ax.plot(wip_history_df['time'], wip_history_df['parts_depot'], linewidth=2, color='mediumseagreen')
    ax.set_xlabel('Simulation Time')
    ax.set_ylabel('Number of Parts')
    ax.set_title('Depot WIP Over Time')
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    return fig


def plot_condition_a_wip_over_time(wip_history_df):
    """Plot Condition A WIP over time."""
    fig, ax = plt.subplots(figsize=(10, 5))
    
    ax.plot(wip_history_df['time'], wip_history_df['parts_condition_a'], linewidth=2, color='mediumpurple')
    ax.set_xlabel('Simulation Time')
    ax.set_ylabel('Number of Parts')
    ax.set_title('Condition A WIP Over Time')
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    return fig



