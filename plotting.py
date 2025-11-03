"""
Simple plotting functions for Hill AFB DES Simulation.

Creates basic visualizations of stage durations.
"""

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
    R code reference (user-provided R code):
    
    micap_events <- des_df |>
      dplyr::filter(!is.na(micap_start) & !is.na(micap_end)) |>
      dplyr::select(micap_start, micap_end) |>
      tidyr::pivot_longer(cols = everything(), 
                          names_to = "event_type", 
                          values_to = "time") |>
      dplyr::mutate(
        change = dplyr::if_else(event_type == "micap_start", 1, -1)
      ) |>
      dplyr::arrange(time) |>
      dplyr::mutate(
        n_micap = cumsum(change)
      )
    
    ggplot2::ggplot(micap_events, ggplot2::aes(x = time, y = n_micap)) +
      ggplot2::geom_step() +
      ggplot2::labs(...) +
      ggplot2::theme_minimal()
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
