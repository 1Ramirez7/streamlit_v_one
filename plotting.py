import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

def plot_fleet_duration(sim_df):
    """Simple histogram of Fleet (Fleet) durations."""
    durations = sim_df['fleet_duration'].dropna()
    
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(durations, bins=30, color='steelblue', edgecolor='black', alpha=0.7)
    ax.set_xlabel('Duration (days)')
    ax.set_ylabel('Frequency')
    ax.set_title(f'Fleet: Fleet Duration Distribution\nMean: {durations.mean():.2f} days')
    ax.grid(axis='y', alpha=0.3)
    
    return fig

def plot_fleet_duration_filtered(sim_df, n_aircraft_with_parts):
    """Simple histogram of Fleet durations excluding initial conditions."""
    # Filter out initial condition sim_ids (1 to n_aircraft_with_parts)
    filtered_df = sim_df[sim_df['sim_id'] > n_aircraft_with_parts]
    durations = filtered_df['fleet_duration'].dropna()
    
    if len(durations) == 0:
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.text(0.5, 0.5, 'No fleet duration data after filtering initial conditions', 
                ha='center', va='center', fontsize=14)
        ax.set_title('Fleet Duration Distribution (Filtered)')
        ax.set_xlabel('Duration (days)')
        ax.set_ylabel('Frequency')
        return fig
    
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(durations, bins=30, color='steelblue', edgecolor='black', alpha=0.7)
    ax.set_xlabel('Duration (days)')
    ax.set_ylabel('Frequency')
    ax.set_title(f'Fleet: Fleet Duration Distribution (Filtered)\nMean: {durations.mean():.2f} days')
    ax.grid(axis='y', alpha=0.3)
    
    return fig

def plot_condition_f_duration(sim_df):
    """Simple histogram of Condition F (Condition F) durations."""
    durations = sim_df['condition_f_duration'].dropna()
    
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(durations, bins=30, color='coral', edgecolor='black', alpha=0.7)
    ax.set_xlabel('Duration (days)')
    ax.set_ylabel('Frequency')
    ax.set_title(f'Condition F: Condition F Duration Distribution\nMean: {durations.mean():.2f} days')
    ax.grid(axis='y', alpha=0.3)
    
    return fig

def plot_depot_duration(sim_df):
    """Simple histogram of Depot durations."""
    durations = sim_df['depot_duration'].dropna()
    
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(durations, bins=30, color='mediumseagreen', edgecolor='black', alpha=0.7)
    ax.set_xlabel('Duration (days)')
    ax.set_ylabel('Frequency')
    ax.set_title(f'Depot Duration Distribution\nMean: {durations.mean():.2f} days')
    ax.grid(axis='y', alpha=0.3)
    
    return fig

def plot_depot_duration_filtered(sim_df, depot_part_ids):
    """Simple histogram of Depot durations excluding initial conditions."""
    filtered_df = sim_df[(~sim_df['sim_id'].isin(depot_part_ids)) & (sim_df['condemn'] != 'yes')]
    durations = filtered_df['depot_duration'].dropna()
    
    if len(durations) == 0:
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.text(0.5, 0.5, 'No depot duration data after filtering initial conditions', 
                ha='center', va='center', fontsize=14)
        ax.set_title('Depot Duration Distribution (Filtered)')
        ax.set_xlabel('Duration (days)')
        ax.set_ylabel('Frequency')
        return fig
    
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(durations, bins=30, color='mediumseagreen', edgecolor='black', alpha=0.7)
    ax.set_xlabel('Duration (days)')
    ax.set_ylabel('Frequency')
    ax.set_title(f'Depot: Depot Duration Distribution (Filtered)\nMean: {durations.mean():.2f} days')
    ax.grid(axis='y', alpha=0.3)
    
    return fig

def plot_install_duration(sim_df):
    """Simple histogram of Condition A Install durations."""
    durations = sim_df['install_duration'].dropna()
    
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(durations, bins=30, color='mediumpurple', edgecolor='black', alpha=0.7)
    ax.set_xlabel('Duration (days)')
    ax.set_ylabel('Frequency')
    ax.set_title(f'Condition A: Install Duration Distribution\nMean: {durations.mean():.2f} days')
    ax.grid(axis='y', alpha=0.3)
    
    return fig

def plot_micap_over_time(des_df):
    """
    Need to update this to use MicapState classes
    """
    
    # Python implementation:
    # Filter des_df for completed MICAP events
    micap_complete = des_df[
        des_df['micap_start'].notna() & des_df['micap_end'].notna()
    ][['micap_start', 'micap_end']].copy()
    
    if len(micap_complete) == 0:
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.text(0.5, 0.5, 'No completed MICAP events occurred', 
                ha='center', va='center', fontsize=14)
        ax.set_title('MICAP Status Over Time')
        ax.set_xlabel('Simulation Time')
        ax.set_ylabel('Number of Aircraft in MICAP')
        return fig
    
    # Pivot longer: create event_type and time columns
    micap_events = pd.melt(
        micap_complete,
        value_vars=['micap_start', 'micap_end'],
        var_name='event_type',
        value_name='time'
    )
    
    #micap_events = micap_events[micap_events['time'] > 200]   # <-- keep only > 200

    # Add change column (+1 for start, -1 for end)
    micap_events['change'] = micap_events['event_type'].apply(
        lambda x: 1 if x == 'micap_start' else -1
    )
    
    # Sort by time and calculate running count
    micap_events = micap_events.sort_values('time').reset_index(drop=True)
    micap_events['n_micap'] = micap_events['change'].cumsum()
    
    # Plot as step function
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.step(micap_events['time'], micap_events['n_micap'], where='post', linewidth=2)
    ax.set_xlabel('Simulation Time')
    ax.set_ylabel('Number of Aircraft in MICAP')
    ax.set_title('MICAP Status Over Time')
    ax.grid(True, alpha=0.3)
    ax.set_ylim(bottom=0)
    
    return fig


def plot_wip_over_time(wip_df): # 777
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