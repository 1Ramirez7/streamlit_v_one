import pandas as pd
import numpy as np


def compute_unified_wip(all_parts, sim_time, interval):
    """
    Compute WIP counts over time with forward fill from all_parts dictionary.
    
    Args:
        all_parts (dict): Dictionary {sim_id: record} from get_all_parts_data()
        sim_time (int/float): End time of simulation
        interval (int): Time interval for sampling
    """
    time_index = np.arange(0, sim_time + interval, interval)
    
    if not all_parts:
        return pd.DataFrame({
            'sim_time': time_index,
            'fleet': np.zeros(len(time_index), dtype=int),
            'condition_f': np.zeros(len(time_index), dtype=int),
            'depot': np.zeros(len(time_index), dtype=int),
            'condition_a': np.zeros(len(time_index), dtype=int)
        })
    
    # Extract arrays from all_parts
    all_parts_list = list(all_parts.values())
    
    # Build raw WIP counts for each field
    raw_counts = _compute_raw_counts(all_parts_list)
    
    # Interpolate to regular intervals with forward fill
    unified_df = pd.DataFrame({
        'sim_time': time_index,
        'fleet': _interpolate_counts(raw_counts['fleet'], time_index),
        'condition_f': _interpolate_counts(raw_counts['condition_f'], time_index),
        'depot': _interpolate_counts(raw_counts['depot'], time_index),
        'condition_a': _interpolate_counts(raw_counts['condition_a'], time_index)
    })
    
    return unified_df


def _compute_raw_counts(all_parts_list):
    """
    Compute raw WIP counts for each field from all_parts list.
    
    Args:
        all_parts_list (list): List of part record dictionaries
    
    Returns:
        dict: {field_name: DataFrame with 'index' and 'count' columns}
    """
    # Extract start/end arrays for each field
    fleet_starts = np.array([r['fleet_start'] for r in all_parts_list], dtype=np.float64)
    fleet_ends = np.array([r['fleet_end'] for r in all_parts_list], dtype=np.float64)
    cdf_starts = np.array([r['condition_f_start'] for r in all_parts_list], dtype=np.float64)
    cdf_ends = np.array([r['condition_f_end'] for r in all_parts_list], dtype=np.float64)
    depot_starts = np.array([r['depot_start'] for r in all_parts_list], dtype=np.float64)
    depot_ends = np.array([r['depot_end'] for r in all_parts_list], dtype=np.float64)
    cda_starts = np.array([r['condition_a_start'] for r in all_parts_list], dtype=np.float64)
    cda_ends = np.array([r['condition_a_end'] for r in all_parts_list], dtype=np.float64)
    
    return {
        'fleet': _compute_single_count(fleet_starts, fleet_ends),
        'condition_f': _compute_single_count(cdf_starts, cdf_ends),
        'depot': _compute_single_count(depot_starts, depot_ends),
        'condition_a': _compute_single_count(cda_starts, cda_ends)
    }


def _compute_single_count(starts, ends):
    """
    Compute cumulative WIP count over time for a single start/end pair.
    
    Args:
        starts (np.array): Array of start times (may contain NaN)
        ends (np.array): Array of end times (may contain NaN)
    
    Returns:
        pd.DataFrame: DataFrame with 'index' (time) and 'count' columns
    """
    start_mask = ~np.isnan(starts)
    end_mask = ~np.isnan(ends)
    
    valid_starts = starts[start_mask]
    valid_ends = ends[end_mask]
    
    if len(valid_starts) == 0 and len(valid_ends) == 0:
        return pd.DataFrame(columns=['index', 'count'])
    
    indices = np.concatenate([valid_starts, valid_ends])
    sums = np.concatenate([
        np.ones(len(valid_starts), dtype=np.int8),
        -np.ones(len(valid_ends), dtype=np.int8)
    ])
    
    sort_order = np.argsort(indices)
    indices = indices[sort_order]
    sums = sums[sort_order]
    counts = np.cumsum(sums)
    
    return pd.DataFrame({'index': indices, 'count': counts})


def _interpolate_counts(event_df, time_index):
    """
    Interpolate WIP counts to regular time intervals with forward fill.
    
    Args:
        event_df (pd.DataFrame): DataFrame with 'index' and 'count' columns
        time_index (np.array): Regular time intervals to interpolate to
    
    Returns:
        np.array: Count values at each time_index point
    """
    if event_df.empty or len(event_df) == 0:
        return np.zeros(len(time_index), dtype=int)
    
    event_times = event_df['index'].values
    event_counts = event_df['count'].values
    
    # Find most recent WIP at or before each time point
    indices = np.searchsorted(event_times, time_index, side='right') - 1
    
    result = np.zeros(len(time_index), dtype=int)
    valid_mask = indices >= 0
    result[valid_mask] = event_counts[indices[valid_mask]]
    
    return result

def compute_raw_wip(all_parts):
    """
    Compute raw WIP counts (no interpolation) from all_parts dictionary.
    
    Returns the actual WIP times and counts - one row per WIP.
    Useful for seeing exact when counts change vs forward-filled intervals.
    
    Args:
        all_parts (dict): Dictionary {sim_id: record} from get_all_parts_data()
    
    Returns:
        pd.DataFrame: Raw WIP counts with columns:
            - sim_time: Actual WIP times (not regular intervals)
            - fleet, condition_f, depot, condition_a: Count at each WIP
    """
    if not all_parts:
        return pd.DataFrame(columns=['sim_time', 'fleet', 'condition_f', 'depot', 'condition_a'])
    
    all_parts_list = list(all_parts.values())
    raw_counts = _compute_raw_counts(all_parts_list)
    
    # Collect all unique WIP times from all fields
    all_times = set()
    for field in ['fleet', 'condition_f', 'depot', 'condition_a']:
        if not raw_counts[field].empty:
            all_times.update(raw_counts[field]['index'].values)
    
    if not all_times:
        return pd.DataFrame(columns=['sim_time', 'fleet', 'condition_f', 'depot', 'condition_a'])
    
    # Sort times
    all_times = np.array(sorted(all_times))
    
    # For each field, get count at each time
    result = pd.DataFrame({'sim_time': all_times})
    
    for field in ['fleet', 'condition_f', 'depot', 'condition_a']:
        result[field] = _interpolate_counts(raw_counts[field], all_times)
    
    return result

# ===========================================================
# AIRCRAFT WIP HELPERS
# ===========================================================

def compute_unified_wip_ac(all_ac, sim_time, interval):
    """
    Compute unified WIP counts over time with forward fill from all_ac dictionary.
    
    Args:
        all_ac (dict): Dictionary {des_id: record} from get_all_ac_data()
        sim_time (int/float): End time of simulation
        interval (int): Time interval for sampling
    """
    time_index = np.arange(0, sim_time + interval, interval)
    
    if not all_ac:
        return pd.DataFrame({
            'sim_time': time_index,
            'fleet': np.zeros(len(time_index), dtype=int),
            'micap': np.zeros(len(time_index), dtype=int)
        })
    
    all_ac_list = list(all_ac.values())
    raw_counts = _compute_raw_counts_ac(all_ac_list)
    
    unified_df = pd.DataFrame({
        'sim_time': time_index,
        'fleet': _interpolate_counts(raw_counts['fleet'], time_index),
        'micap': _interpolate_counts(raw_counts['micap'], time_index)
    })
    
    return unified_df


def _compute_raw_counts_ac(all_ac_list):
    """
    Compute raw WIP counts for AC fields from all_ac list.
    
    Args:
        all_ac_list (list): List of aircraft record dictionaries
    
    Returns:
        dict: {field_name: DataFrame with 'index' and 'count' columns}
    """
    fleet_starts = np.array([r['fleet_start'] for r in all_ac_list], dtype=np.float64)
    fleet_ends = np.array([r['fleet_end'] for r in all_ac_list], dtype=np.float64)
    micap_starts = np.array([r['micap_start'] for r in all_ac_list], dtype=np.float64)
    micap_ends = np.array([r['micap_end'] for r in all_ac_list], dtype=np.float64)
    
    return {
        'fleet': _compute_single_count(fleet_starts, fleet_ends),
        'micap': _compute_single_count(micap_starts, micap_ends)
    }


def compute_raw_wip_ac(all_ac):
    """
    Compute raw WIP counts (no interpolation) from all_ac dictionary.
    
    Returns the actual WIP times and counts - one row per WIP.
    
    Args:
        all_ac (dict): Dictionary {des_id: record} from get_all_ac_data()
    
    Returns:
        pd.DataFrame: Raw WIP counts with columns:
            - sim_time: Actual WIP times (not regular intervals)
            - fleet, micap: Count at each WIP
    """
    if not all_ac:
        return pd.DataFrame(columns=['sim_time', 'fleet', 'micap'])
    
    all_ac_list = list(all_ac.values())
    raw_counts = _compute_raw_counts_ac(all_ac_list)
    
    # Collect all unique WIP times from all fields
    all_times = set()
    for field in ['fleet', 'micap']:
        if not raw_counts[field].empty:
            all_times.update(raw_counts[field]['index'].values)
    
    if not all_times:
        return pd.DataFrame(columns=['sim_time', 'fleet', 'micap'])
    
    all_times = np.array(sorted(all_times))
    
    result = pd.DataFrame({'sim_time': all_times})
    
    for field in ['fleet', 'micap']:
        result[field] = _interpolate_counts(raw_counts[field], all_times)
    
    return result