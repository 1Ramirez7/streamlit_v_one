import streamlit as st
import matplotlib.pyplot as plt


def add_stats_box(ax, data, column_name):
    """Add average and standard deviation statistics to plot."""
    avg = data[column_name].mean()
    stats_text = f'avg = {avg:.2f}'
    if avg != 0:
        std = data[column_name].std()
        stats_text += f'\nstd = {std:.2f}'
    ax.text(0.98, 0.97, stats_text, transform=ax.transAxes, 
            verticalalignment='top', horizontalalignment='right',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))


def render_wip_plots(post_sim):
    """
    Render individual WIP plots using pre-computed figures from PostSim.
    
    Parameters
    ----------
    post_sim : PostSim
        PostSim object containing pre-computed wip_figs
    """
    if not post_sim.has_wip_data():
        st.warning("No WIP data available to plot.")
        return
    
    if not post_sim.wip_figs:
        st.info("Plot rendering is disabled. Check 'Render Plots' in sidebar to enable.")
        return
    
    # Display pre-computed plots
    if post_sim.wip_figs.get('micap'):
        st.pyplot(post_sim.wip_figs['micap'])
    if post_sim.wip_figs.get('fleet'):
        st.pyplot(post_sim.wip_figs['fleet'])
    if post_sim.wip_figs.get('condition_f'):
        st.pyplot(post_sim.wip_figs['condition_f'])
    if post_sim.wip_figs.get('depot'):
        st.pyplot(post_sim.wip_figs['depot'])
    if post_sim.wip_figs.get('condition_a'):
        st.pyplot(post_sim.wip_figs['condition_a'])


def plot_micap_over_time(wip_ac_raw, n_total_aircraft, use_percentage=True):
    """Plot MICAP aircraft count over time."""
    fig, ax = plt.subplots(figsize=(10, 5))
    
    if use_percentage:
        y_data = wip_ac_raw['micap'] / n_total_aircraft * 100
        ax.step(wip_ac_raw['sim_time'], y_data, where='post', linewidth=2, color='red')
        ax.set_ylabel('Percentage of Aircraft (%)')
        # Stats box showing percentage
        avg_val = y_data.mean()
        std_val = y_data.std()
        stats_text = f'avg = {avg_val:.2f}%\nstd = {std_val:.2f}%'
    else:
        y_data = wip_ac_raw['micap']
        ax.step(wip_ac_raw['sim_time'], y_data, where='post', linewidth=2, color='red')
        ax.set_ylabel('Number of MICAP Aircraft')
        # Stats box showing counts
        avg_val = y_data.mean()
        std_val = y_data.std()
        stats_text = f'avg = {avg_val:.2f}\nstd = {std_val:.2f}'
    
    ax.set_xlabel('Simulation Time (Days)')
    ax.set_title('MICAP Status Over Time')
    ax.grid(True, alpha=0.3)
    ax.text(0.98, 0.97, stats_text, transform=ax.transAxes, 
            verticalalignment='top', horizontalalignment='right',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    plt.tight_layout()
    return fig

def plot_fleet_wip_over_time(wip_ac_raw, n_total_aircraft, use_percentage=True):
    """Plot Fleet WIP over time."""
    fig, ax = plt.subplots(figsize=(10, 5))
    
    if use_percentage:
        y_data = wip_ac_raw['fleet'] / n_total_aircraft * 100
        ax.plot(wip_ac_raw['sim_time'], y_data, linewidth=2, color='steelblue')
        ax.set_ylabel('Percentage of Aircraft (%)')
        # Stats box showing percentage
        avg_val = y_data.mean()
        std_val = y_data.std()
        stats_text = f'avg = {avg_val:.2f}%\nstd = {std_val:.2f}%'
    else:
        y_data = wip_ac_raw['fleet']
        ax.plot(wip_ac_raw['sim_time'], y_data, linewidth=2, color='steelblue')
        ax.set_ylabel('Number of Aircraft')
        # Stats box showing counts
        avg_val = y_data.mean()
        std_val = y_data.std()
        stats_text = f'avg = {avg_val:.2f}\nstd = {std_val:.2f}'
    
    ax.set_xlabel('Simulation Time (Days)')
    ax.set_title('Fleet WIP Over Time')
    ax.grid(True, alpha=0.3)
    ax.text(0.98, 0.97, stats_text, transform=ax.transAxes, 
            verticalalignment='top', horizontalalignment='right',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    plt.tight_layout()
    return fig


def plot_condition_f_wip_over_time(wip_raw, n_total_parts, use_percentage=True):
    """Plot Condition F WIP over time."""
    fig, ax = plt.subplots(figsize=(10, 5))
    
    if use_percentage:
        y_data = wip_raw['condition_f'] / n_total_parts * 100
        ax.plot(wip_raw['sim_time'], y_data, linewidth=2, color='coral')
        ax.set_ylabel('Percentage of Parts (%)')
        # Stats box showing percentage
        avg_val = y_data.mean()
        std_val = y_data.std()
        stats_text = f'avg = {avg_val:.2f}%\nstd = {std_val:.2f}%'
    else:
        y_data = wip_raw['condition_f']
        ax.plot(wip_raw['sim_time'], y_data, linewidth=2, color='coral')
        ax.set_ylabel('Number of Parts')
        # Stats box showing counts
        avg_val = y_data.mean()
        std_val = y_data.std()
        stats_text = f'avg = {avg_val:.2f}\nstd = {std_val:.2f}'
    
    ax.set_xlabel('Simulation Time (Days)')
    ax.set_title('Condition F WIP Over Time')
    ax.grid(True, alpha=0.3)
    ax.text(0.98, 0.97, stats_text, transform=ax.transAxes, 
            verticalalignment='top', horizontalalignment='right',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    plt.tight_layout()
    return fig

def plot_depot_wip_over_time(wip_raw, depot_capacity, use_percentage=True):
    """Plot Depot WIP over time."""
    fig, ax = plt.subplots(figsize=(10, 5))
    
    if use_percentage:
        y_data = wip_raw['depot'] / depot_capacity * 100
        ax.plot(wip_raw['sim_time'], y_data, linewidth=2, color='mediumseagreen')
        ax.set_ylabel('Percentage of Depot Capacity (%)')
        # Stats box showing percentage
        avg_val = y_data.mean()
        std_val = y_data.std()
        stats_text = f'avg = {avg_val:.2f}%\nstd = {std_val:.2f}%'
    else:
        y_data = wip_raw['depot']
        ax.plot(wip_raw['sim_time'], y_data, linewidth=2, color='mediumseagreen')
        ax.set_ylabel('Number of Parts')
        # Stats box showing counts
        avg_val = y_data.mean()
        std_val = y_data.std()
        stats_text = f'avg = {avg_val:.2f}\nstd = {std_val:.2f}'
    
    ax.set_xlabel('Simulation Time (Days)')
    ax.set_title('Depot WIP Over Time')
    ax.grid(True, alpha=0.3)
    ax.text(0.98, 0.97, stats_text, transform=ax.transAxes, 
            verticalalignment='top', horizontalalignment='right',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    plt.tight_layout()
    return fig

def plot_condition_a_wip_over_time(wip_raw, n_total_parts, use_percentage=True):
    """Plot Condition A WIP over time."""
    fig, ax = plt.subplots(figsize=(10, 5))
    
    if use_percentage:
        y_data = wip_raw['condition_a'] / n_total_parts * 100
        ax.plot(wip_raw['sim_time'], y_data, linewidth=2, color='mediumpurple')
        ax.set_ylabel('Percentage of Parts (%)')
        # Stats box showing percentage
        avg_val = y_data.mean()
        std_val = y_data.std()
        stats_text = f'avg = {avg_val:.2f}%\nstd = {std_val:.2f}%'
    else:
        y_data = wip_raw['condition_a']
        ax.plot(wip_raw['sim_time'], y_data, linewidth=2, color='mediumpurple')
        ax.set_ylabel('Number of Parts')
        # Stats box showing counts
        avg_val = y_data.mean()
        std_val = y_data.std()
        stats_text = f'avg = {avg_val:.2f}\nstd = {std_val:.2f}'
    
    ax.set_xlabel('Simulation Time (Days)')
    ax.set_title('Condition A WIP Over Time')
    ax.grid(True, alpha=0.3)
    ax.text(0.98, 0.97, stats_text, transform=ax.transAxes, 
            verticalalignment='top', horizontalalignment='right',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    plt.tight_layout()
    return fig

