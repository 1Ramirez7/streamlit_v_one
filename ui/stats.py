"""
stats.py
--------
Calculates and renders simulation statistics for the UI.
"""
import streamlit as st
import pandas as pd
import numpy as np


def calculate_duration_stats(series, name):
    """
    Calculate min, mean, max for a duration series.
    
    Parameters
    ----------
    series : pd.Series
        Duration values (with NaN allowed)
    name : str
        Name of the duration for display
    
    Returns
    -------
    dict
        Keys: name, count, mean, min, max
    """
    clean = series.dropna()
    if len(clean) == 0:
        return {'name': name, 'count': 0, 'mean': np.nan, 'min': np.nan, 'max': np.nan}
    
    return {
        'name': name,
        'count': len(clean),
        'mean': clean.mean(),
        'min': clean.min(),
        'max': clean.max()
    }


def calculate_simulation_stats(datasets):
    """
    Calculate all simulation statistics from datasets.
    
    Parameters
    ----------
    datasets : DataSets
        Contains all_parts_df, all_ac_df, wip_df
    
    Returns
    -------
    dict
        All calculated statistics
    """
    stats = {}
    
    # --- MICAP Stats (from wip_df) ---
    wip_df = datasets.wip_df
    if wip_df is not None and len(wip_df) > 0:
        micap_all = wip_df['aircraft_micap']
        micap_nonzero = wip_df[wip_df['aircraft_micap'] > 0]['aircraft_micap']
        
        stats['micap'] = {
            'avg_with_zeros': micap_all.mean(),
            'count_all': len(micap_all),
            'avg_no_zeros': micap_nonzero.mean() if len(micap_nonzero) > 0 else np.nan,
            'count_nonzero': len(micap_nonzero),
            'max_micap': micap_all.max(),
            'min_micap': micap_all.min()
        }
    else:
        stats['micap'] = None
    
    # --- Duration Stats (from all_parts_df) ---
    parts_df = datasets.all_parts_df
    if parts_df is not None and len(parts_df) > 0:
        stats['fleet_duration'] = calculate_duration_stats(
            parts_df['fleet_duration'], 'Fleet Duration')
        stats['depot_duration'] = calculate_duration_stats(
            parts_df['depot_duration'], 'Depot Duration')
        stats['condition_a_duration'] = calculate_duration_stats(
            parts_df['condition_a_duration'], 'Condition A Duration')
        stats['condition_f_duration'] = calculate_duration_stats(
            parts_df['condition_f_duration'], 'Condition F Duration')
    
    # --- MICAP Duration (from all_ac_df) ---
    ac_df = datasets.all_ac_df
    if ac_df is not None and len(ac_df) > 0:
        stats['micap_duration'] = calculate_duration_stats(
            ac_df['micap_duration'], 'MICAP Duration')
    
    return stats


def render_stats_tab(datasets):
    """
    Render statistics in Tab 1 of main.py.
    
    Parameters
    ----------
    datasets : DataSets
        Contains all simulation data
    """
    stats = calculate_simulation_stats(datasets)
    
    # --- MICAP Statistics ---
    st.subheader("📊 MICAP Statistics")
    
    if stats.get('micap'):
        micap = stats['micap']
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Avg MICAP (incl. zeros)", f"{micap['avg_with_zeros']:.2f}")
            st.caption(f"n = {micap['count_all']:,}")
        with col2:
            st.metric("Avg MICAP (excl. zeros)", 
                     f"{micap['avg_no_zeros']:.2f}" if not np.isnan(micap['avg_no_zeros']) else "N/A")
            st.caption(f"n = {micap['count_nonzero']:,}")
        with col3:
            st.metric("Max MICAP", f"{micap['max_micap']:.0f}")
            st.metric("Min MICAP", f"{micap['min_micap']:.0f}")
    else:
        st.warning("No MICAP data available")
    
    st.markdown("---")
    
    # --- Duration Statistics ---
    st.subheader("📊 Duration Statistics")
    
    # Create a table for all durations
    duration_keys = ['fleet_duration', 'condition_f_duration', 'depot_duration', 
                     'condition_a_duration', 'micap_duration']
    
    for key in duration_keys:
        if key in stats and stats[key]['count'] > 0:
            d = stats[key]
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.markdown(f"**{d['name']}**")
                st.caption(f"n = {d['count']:,}")
            with col2:
                st.metric("Mean", f"{d['mean']:.2f}")
            with col3:
                st.metric("Min", f"{d['min']:.2f}")
            with col4:
                st.metric("Max", f"{d['max']:.2f}")
            
            st.markdown("---")
