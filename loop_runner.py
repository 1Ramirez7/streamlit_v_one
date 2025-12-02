"""
loop_runner.py
--------------
Handles running multiple simulations in a loop with varying depot capacity and/or n_total_parts values.
Collects statistics and parameters from each run for export.
"""

import pandas as pd
import numpy as np
from data_manager import DataFrameManager
from simulation_engine import SimulationEngine
from utils import calculate_initial_allocation
from ds.data_science import DataSets

# Import calculate_simulation_stats - handle both import contexts
try:
    from streamlit_app.ui.stats import calculate_simulation_stats
except ModuleNotFoundError:
    from ui.stats import calculate_simulation_stats


def calculate_loop_values(
    loop_mode: str,
    range_min: int,
    range_max: int,
    step_value: int = 1,
    num_loops: int = 5,
    custom_list: list = None,
    n_total_aircraft: int = None,
    n_total_parts: int = None,
    pct_min: float = 0.5,
    pct_max: float = 1.5,
    pct_step: float = 0.1,
    include_min: bool = True
) -> list:
    """
    Calculate the list of values to loop through (generic for depot or parts).
    
    Parameters
    ----------
    loop_mode : str
        One of: 'range', 'step', 'auto', 'custom', 'pct_aircraft', 'pct_parts'
    range_min : int
        Minimum value for range-based modes
    range_max : int
        Maximum value for range-based modes
    step_value : int
        Step size for 'step' mode
    num_loops : int
        Number of loops for 'auto' mode
    custom_list : list
        List of custom values for 'custom' mode
    n_total_aircraft : int
        Total aircraft (for pct_aircraft mode)
    n_total_parts : int
        Total parts (for pct_parts mode)
    pct_min : float
        Minimum percentage (e.g., 0.5 = 50%)
    pct_max : float
        Maximum percentage (e.g., 1.5 = 150%)
    pct_step : float
        Percentage step (e.g., 0.1 = 10%)
    include_min : bool
        Whether to include the minimum value (for step/auto/pct modes)
    
    Returns
    -------
    list
        List of values (integers)
    """
    
    if loop_mode == 'range':
        # Every integer from min to max (always includes both endpoints)
        return list(range(range_min, range_max + 1))
    
    elif loop_mode == 'step':
        # Every step_value from min to max
        if include_min:
            values = list(range(range_min, range_max + 1, step_value))
        else:
            values = list(range(range_min + step_value, range_max + 1, step_value))
        # Ensure max is included if it's not already
        if range_max not in values and include_min:
            values.append(range_max)
        return values
    
    elif loop_mode == 'auto':
        # Auto-calculate evenly spaced values
        if num_loops <= 1:
            return [range_min] if include_min else [range_max]
        
        # Calculate step to get num_loops values
        span = range_max - range_min
        step = span / (num_loops - 1) if include_min else span / num_loops
        
        if include_min:
            values = [int(round(range_min + i * step)) for i in range(num_loops)]
        else:
            values = [int(round(range_min + (i + 1) * step)) for i in range(num_loops)]
        
        # Remove duplicates and sort
        return sorted(list(set(values)))
    
    elif loop_mode == 'custom':
        # User-provided list
        if custom_list is None:
            return []
        return sorted([int(v) for v in custom_list])
    
    elif loop_mode == 'pct_aircraft':
        # Percentage of n_total_aircraft
        if n_total_aircraft is None:
            return []
        
        pct_values = []
        pct = pct_min if include_min else pct_min + pct_step
        while pct <= pct_max + 0.001:  # Small epsilon for float comparison
            val = int(round(n_total_aircraft * pct))
            if val > 0:  # Ensure positive values
                pct_values.append(val)
            pct += pct_step
        
        # Remove duplicates and sort
        return sorted(list(set(pct_values)))
    
    elif loop_mode == 'pct_parts':
        # Percentage of n_total_parts
        if n_total_parts is None:
            return []
        
        pct_values = []
        pct = pct_min if include_min else pct_min + pct_step
        while pct <= pct_max + 0.001:
            val = int(round(n_total_parts * pct))
            if val > 0:
                pct_values.append(val)
            pct += pct_step
        
        return sorted(list(set(pct_values)))
    
    return []


# Alias for backwards compatibility
def calculate_depot_values(*args, **kwargs):
    """Alias for calculate_loop_values (backwards compatibility)."""
    return calculate_loop_values(*args, **kwargs)


def run_single_simulation(params: dict, depot_capacity_override: int = None, 
                          n_total_parts_override: int = None):
    """
    Run a single simulation with given parameters.
    Optionally override depot_capacity and/or n_total_parts.
    
    Parameters
    ----------
    params : dict
        All simulation parameters from render_sidebar()
    depot_capacity_override : int, optional
        If provided, overrides params['depot_capacity']
    n_total_parts_override : int, optional
        If provided, overrides params['n_total_parts']
    
    Returns
    -------
    tuple
        (datasets, df_manager, validation_results, stats, allocation)
    """
    # Override values if specified
    depot_capacity = depot_capacity_override if depot_capacity_override is not None else params['depot_capacity']
    n_total_parts = n_total_parts_override if n_total_parts_override is not None else params['n_total_parts']
    
    # Calculate initial allocation
    allocation = calculate_initial_allocation(
        n_total_parts=n_total_parts,
        n_total_aircraft=params['n_total_aircraft'],
        mission_capable_rate=params['mission_capable_rate'],
        depot_capacity=depot_capacity,
        condemn_cycle=params['condemn_cycle'],
        parts_in_depot=params['parts_in_depot'],
        parts_in_cond_f=params['parts_in_cond_f'],
        parts_in_cond_a=params['parts_in_cond_a']
    )
    
    # Create DataFrameManager
    df_manager = DataFrameManager(
        n_total_parts=n_total_parts,
        n_total_aircraft=params['n_total_aircraft'],
        sim_time=params['sim_time'],
        sone_mean=params['sone_mean'],
        sthree_mean=params['sthree_mean'],
        allocation=allocation
    )
    
    # Store parameters (with overridden values)
    params_copy = params.copy()
    params_copy['depot_capacity'] = depot_capacity
    params_copy['n_total_parts'] = n_total_parts
    df_manager.store_user_params(params_copy)
    
    # Create DataSets
    datasets = DataSets()
    
    # Create SimulationEngine
    engine = SimulationEngine(
        df_manager=df_manager,
        datasets=datasets,
        sone_dist=params['sone_dist'],
        sone_mean=params['sone_mean'],
        sone_sd=params['sone_sd'],
        sthree_dist=params['sthree_dist'],
        sthree_mean=params['sthree_mean'],
        sthree_sd=params['sthree_sd'],
        sim_time=params['sim_time'],
        depot_capacity=depot_capacity,
        condemn_cycle=params['condemn_cycle'],
        condemn_depot_fraction=params['condemn_depot_fraction'],
        part_order_lag=params['part_order_lag'],
        use_fleet_rand=params['use_fleet_rand'],
        fleet_rand_min=params['fleet_rand_min'],
        fleet_rand_max=params['fleet_rand_max'],
        use_depot_rand=params['use_depot_rand'],
        depot_rand_min=params['depot_rand_min'],
        depot_rand_max=params['depot_rand_max']
    )
    
    # Run simulation
    validation_results = engine.run()
    
    # Calculate statistics
    stats = calculate_simulation_stats(datasets)
    
    return datasets, df_manager, validation_results, stats, allocation


def run_depot_capacity_loop(params: dict, depot_values: list, random_seed: int, 
                            progress_callback=None):
    """
    Run simulations for multiple depot capacity values (single loop).
    
    Parameters
    ----------
    params : dict
        Base simulation parameters
    depot_values : list
        List of depot capacity values to loop through
    random_seed : int
        Random seed (reset for each iteration for reproducibility)
    progress_callback : callable, optional
        Function(current_idx, total, depot_value) for progress updates
    
    Returns
    -------
    dict
        Keys:
        - 'summary_df': DataFrame with MICAP averages per depot capacity
        - 'all_stats': List of stats dicts per iteration
        - 'all_params_df': Combined params DataFrame with depot_capacity column
        - 'all_results': List of (datasets, df_manager, validation_results) per iteration
    """
    results = {
        'summary_df': None,
        'all_stats': [],
        'all_params_df': None,
        'all_results': []
    }
    
    summary_rows = []
    all_params_rows = []
    
    total = len(depot_values)
    
    for idx, depot_cap in enumerate(depot_values):
        # Reset random seed for reproducibility
        np.random.seed(random_seed)
        
        # Progress callback
        if progress_callback:
            progress_callback(idx + 1, total, depot_cap)
        
        try:
            # Run simulation
            datasets, df_manager, validation_results, stats, allocation = run_single_simulation(
                params, depot_capacity_override=depot_cap
            )
            
            # Store results
            results['all_results'].append({
                'depot_capacity': depot_cap,
                'n_total_parts': params['n_total_parts'],
                'datasets': datasets,
                'df_manager': df_manager,
                'validation_results': validation_results,
                'stats': stats,
                'allocation': allocation
            })
            results['all_stats'].append(stats)
            
            # Build summary row
            micap = stats.get('micap', {})
            summary_rows.append({
                'depot_capacity': depot_cap,
                'n_total_parts': params['n_total_parts'],
                'avg_micap_with_zeros': micap.get('avg_with_zeros', np.nan),
                'count_all': micap.get('count_all', 0),
                'avg_micap_no_zeros': micap.get('avg_no_zeros', np.nan),
                'count_nonzero': micap.get('count_nonzero', 0),
                'max_micap': micap.get('max_micap', np.nan),
                'min_micap': micap.get('min_micap', np.nan),
                'fleet_duration_mean': stats.get('fleet_duration', {}).get('mean', np.nan),
                'depot_duration_mean': stats.get('depot_duration', {}).get('mean', np.nan),
                'total_events': validation_results.get('event_counts', {}).get('total', 0)
            })
            
            # Build params row
            params_row = {'depot_capacity': depot_cap, 'n_total_parts': params['n_total_parts']}
            for ui_label, code_name, value in df_manager.user_params:
                params_row[code_name] = value
            all_params_rows.append(params_row)
            
        except Exception as e:
            # Log error but continue
            summary_rows.append({
                'depot_capacity': depot_cap,
                'n_total_parts': params['n_total_parts'],
                'avg_micap_with_zeros': np.nan,
                'count_all': 0,
                'avg_micap_no_zeros': np.nan,
                'count_nonzero': 0,
                'max_micap': np.nan,
                'min_micap': np.nan,
                'fleet_duration_mean': np.nan,
                'depot_duration_mean': np.nan,
                'total_events': 0,
                'error': str(e)
            })
    
    # Create summary DataFrame
    results['summary_df'] = pd.DataFrame(summary_rows)
    results['all_params_df'] = pd.DataFrame(all_params_rows)
    
    return results


def run_nested_loop(params: dict, parts_values: list, depot_values: list, 
                    random_seed: int, progress_callback=None):
    """
    Run simulations for all combinations of n_total_parts and depot_capacity values.
    Outer loop: parts, Inner loop: depot.
    
    Parameters
    ----------
    params : dict
        Base simulation parameters
    parts_values : list
        List of n_total_parts values to loop through (outer loop)
    depot_values : list
        List of depot capacity values to loop through (inner loop)
    random_seed : int
        Random seed (reset for each iteration for reproducibility)
    progress_callback : callable, optional
        Function(current_idx, total, parts_val, depot_val) for progress updates
    
    Returns
    -------
    dict
        Keys:
        - 'summary_df': DataFrame with MICAP averages per combination
        - 'all_stats': List of stats dicts per iteration
        - 'all_params_df': Combined params DataFrame
        - 'all_results': List of result dicts per iteration
    """
    results = {
        'summary_df': None,
        'all_stats': [],
        'all_params_df': None,
        'all_results': []
    }
    
    summary_rows = []
    all_params_rows = []
    
    total = len(parts_values) * len(depot_values)
    current = 0
    
    for parts_val in parts_values:
        for depot_cap in depot_values:
            current += 1
            
            # Reset random seed for reproducibility
            np.random.seed(random_seed)
            
            # Progress callback
            if progress_callback:
                progress_callback(current, total, parts_val, depot_cap)
            
            try:
                # Run simulation with both overrides
                datasets, df_manager, validation_results, stats, allocation = run_single_simulation(
                    params, 
                    depot_capacity_override=depot_cap,
                    n_total_parts_override=parts_val
                )
                
                # Store results
                results['all_results'].append({
                    'n_total_parts': parts_val,
                    'depot_capacity': depot_cap,
                    'datasets': datasets,
                    'df_manager': df_manager,
                    'validation_results': validation_results,
                    'stats': stats,
                    'allocation': allocation
                })
                results['all_stats'].append(stats)
                
                # Build summary row
                micap = stats.get('micap', {})
                summary_rows.append({
                    'n_total_parts': parts_val,
                    'depot_capacity': depot_cap,
                    'avg_micap_with_zeros': micap.get('avg_with_zeros', np.nan),
                    'count_all': micap.get('count_all', 0),
                    'avg_micap_no_zeros': micap.get('avg_no_zeros', np.nan),
                    'count_nonzero': micap.get('count_nonzero', 0),
                    'max_micap': micap.get('max_micap', np.nan),
                    'min_micap': micap.get('min_micap', np.nan),
                    'fleet_duration_mean': stats.get('fleet_duration', {}).get('mean', np.nan),
                    'depot_duration_mean': stats.get('depot_duration', {}).get('mean', np.nan),
                    'total_events': validation_results.get('event_counts', {}).get('total', 0)
                })
                
                # Build params row
                params_row = {'n_total_parts': parts_val, 'depot_capacity': depot_cap}
                for ui_label, code_name, value in df_manager.user_params:
                    params_row[code_name] = value
                all_params_rows.append(params_row)
                
            except Exception as e:
                # Log error but continue
                summary_rows.append({
                    'n_total_parts': parts_val,
                    'depot_capacity': depot_cap,
                    'avg_micap_with_zeros': np.nan,
                    'count_all': 0,
                    'avg_micap_no_zeros': np.nan,
                    'count_nonzero': 0,
                    'max_micap': np.nan,
                    'min_micap': np.nan,
                    'fleet_duration_mean': np.nan,
                    'depot_duration_mean': np.nan,
                    'total_events': 0,
                    'error': str(e)
                })
    
    # Create summary DataFrame
    results['summary_df'] = pd.DataFrame(summary_rows)
    results['all_params_df'] = pd.DataFrame(all_params_rows)
    
    return results


def run_parts_loop(params: dict, parts_values: list, random_seed: int, 
                   progress_callback=None):
    """
    Run simulations for multiple n_total_parts values (single loop, no depot loop).
    
    Parameters
    ----------
    params : dict
        Base simulation parameters
    parts_values : list
        List of n_total_parts values to loop through
    random_seed : int
        Random seed (reset for each iteration for reproducibility)
    progress_callback : callable, optional
        Function(current_idx, total, parts_value) for progress updates
    
    Returns
    -------
    dict
        Same structure as run_depot_capacity_loop
    """
    results = {
        'summary_df': None,
        'all_stats': [],
        'all_params_df': None,
        'all_results': []
    }
    
    summary_rows = []
    all_params_rows = []
    
    total = len(parts_values)
    
    for idx, parts_val in enumerate(parts_values):
        # Reset random seed for reproducibility
        np.random.seed(random_seed)
        
        # Progress callback
        if progress_callback:
            progress_callback(idx + 1, total, parts_val)
        
        try:
            # Run simulation
            datasets, df_manager, validation_results, stats, allocation = run_single_simulation(
                params, n_total_parts_override=parts_val
            )
            
            # Store results
            results['all_results'].append({
                'n_total_parts': parts_val,
                'depot_capacity': params['depot_capacity'],
                'datasets': datasets,
                'df_manager': df_manager,
                'validation_results': validation_results,
                'stats': stats,
                'allocation': allocation
            })
            results['all_stats'].append(stats)
            
            # Build summary row
            micap = stats.get('micap', {})
            summary_rows.append({
                'n_total_parts': parts_val,
                'depot_capacity': params['depot_capacity'],
                'avg_micap_with_zeros': micap.get('avg_with_zeros', np.nan),
                'count_all': micap.get('count_all', 0),
                'avg_micap_no_zeros': micap.get('avg_no_zeros', np.nan),
                'count_nonzero': micap.get('count_nonzero', 0),
                'max_micap': micap.get('max_micap', np.nan),
                'min_micap': micap.get('min_micap', np.nan),
                'fleet_duration_mean': stats.get('fleet_duration', {}).get('mean', np.nan),
                'depot_duration_mean': stats.get('depot_duration', {}).get('mean', np.nan),
                'total_events': validation_results.get('event_counts', {}).get('total', 0)
            })
            
            # Build params row
            params_row = {'n_total_parts': parts_val, 'depot_capacity': params['depot_capacity']}
            for ui_label, code_name, value in df_manager.user_params:
                params_row[code_name] = value
            all_params_rows.append(params_row)
            
        except Exception as e:
            # Log error but continue
            summary_rows.append({
                'n_total_parts': parts_val,
                'depot_capacity': params['depot_capacity'],
                'avg_micap_with_zeros': np.nan,
                'count_all': 0,
                'avg_micap_no_zeros': np.nan,
                'count_nonzero': 0,
                'max_micap': np.nan,
                'min_micap': np.nan,
                'fleet_duration_mean': np.nan,
                'depot_duration_mean': np.nan,
                'total_events': 0,
                'error': str(e)
            })
    
    # Create summary DataFrame
    results['summary_df'] = pd.DataFrame(summary_rows)
    results['all_params_df'] = pd.DataFrame(all_params_rows)
    
    return results
