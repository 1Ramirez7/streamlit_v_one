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
    df = datasets.wip_ac_raw
    if df is not None and len(df) > 0:
        micap_all = df['micap']
        micap_nonzero = df[df['micap'] > 0]['micap']
        
        stats['micap'] = {
            'avg_with_zeros': micap_all.mean(), # average w/ no-micap days included
            'count_all': len(micap_all),
            # average micap including days where at least one MICAP existed
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


 # POSTSIM CLASS - NEW: render_stats_tab now takes post_sim
def render_stats_tab(post_sim):
    """
    Render statistics in Tab 1 of main.py.
    
    Parameters
    ----------
    post_sim : PostSim
        PostSim object containing pre-computed stats
    """
    stats = post_sim.stats
    
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



def calculate_multi_run_averages(datasets):
    """
    Compute averages for multi-model results (for a single run).
    """
    wip_ac_raw = datasets.wip_ac_raw
    wip_raw = datasets.wip_raw
    if wip_ac_raw is None or len(wip_ac_raw) == 0 or wip_raw is None or len(wip_raw) == 0:
        return {
            'avg_micap': np.nan,
            'avg_fleet': np.nan,
            'avg_cd_f': np.nan,
            'avg_depot': np.nan,
            'avg_cd_a': np.nan,
            'count': 0
        }
    return {
        'avg_micap': wip_ac_raw['micap'].mean(),
        'avg_fleet': wip_raw['fleet'].mean(),
        'avg_cd_f': wip_raw['condition_f'].mean(),
        'avg_depot': wip_raw['depot'].mean(),
        'avg_cd_a': wip_raw['condition_a'].mean(),
        'count': len(wip_ac_raw)
    }
def render_multi_run_averages(post_sim):
    """
    Render the multi-model averages (used in multi_run) for a single run.
    """
    avgs = post_sim.multi_run_averages
    st.subheader("Multi-Model Averages (Single Run)")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Avg MICAP", f"{avgs['avg_micap']:.2f}")
        st.metric("Avg Fleet", f"{avgs['avg_fleet']:.2f}")
    with col2:
        st.metric("Avg Cd_F", f"{avgs['avg_cd_f']:.2f}")
        st.metric("Avg Depot", f"{avgs['avg_depot']:.2f}")
    with col3:
        st.metric("Avg Cd_A", f"{avgs['avg_cd_a']:.2f}")
        st.caption(f"n = {avgs['count']:,}")