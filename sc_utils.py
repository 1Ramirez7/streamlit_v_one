"""
sc_utils.py
-----------------
Utility functions for multi-run simulations.

Contains helper functions for running simulations, generating analysis text,
and handling figure conversions.
"""
import matplotlib.pyplot as plt
from io import BytesIO
from datetime import datetime

from simulation_engine import SimulationEngine
from utils import calculate_initial_allocation


def fig_to_bytes(fig):
    """Convert matplotlib figure to bytes for download."""
    buf = BytesIO()
    fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
    buf.seek(0)
    return buf.getvalue()


def get_multi_wip_figs_as_bytes(post_sim, depot_cap, n_parts):
    """
    Get the WIP figures needed for multi-model results as PNG bytes.
    
    Converts figures to bytes immediately and closes them to prevent
    memory accumulation across multiple simulation runs.
    
    Args:
        post_sim: PostSim object with pre-rendered figures
        depot_cap: Depot capacity for title
        n_parts: Number of parts for title
        
    Returns:
        dict: Selected figures as PNG bytes for multi-model time series tab
    """
    wip_figs = post_sim.wip_figs
    result = {}
    
    # Keys to extract (add more here to include additional plots)
    keys_to_extract = [
        'micap',
        'depot',
        # === ADD MORE PLOTS HERE ===
        # 'fleet',
        # 'condition_f',
        # 'condition_a',
        # 'wip_overview',
    ]
    
    for key in keys_to_extract:
        fig = wip_figs.get(key)
        if fig is not None:
            # Update title to show which simulation
            if fig.axes:
                display_name = key.replace('_', ' ').title()
                fig.axes[0].set_title(f"{display_name} Over Time (Depot={depot_cap}, Parts={n_parts})")
            # Convert to bytes
            result[key] = fig_to_bytes(fig)
            # Close figure immediately to free memory
            plt.close(fig)
        else:
            result[key] = None
    
    return result


def run_single_simulation(params, depot_cap, n_parts):
    """
    Run a single simulation and return results.
    
    Args:
        params: Parameters object with all simulation settings
        depot_cap: Depot capacity value (for plot titles)
        n_parts: Number of parts value (for plot titles)
        
    Returns:
        dict: Results including averages for all metrics and pre-rendered figure bytes
    """
    allocation = calculate_initial_allocation(params)

    sim_engine = SimulationEngine(
        params=params,
        allocation=allocation
    )
    validation_results = sim_engine.run()

    # Get pre-rendered figures from PostSim as bytes (closes figs immediately)
    post_sim = validation_results['post_sim']
    multi_figs_bytes = get_multi_wip_figs_as_bytes(post_sim, depot_cap, n_parts)

    # Close remaining PostSim figures we don't need for multi-model
    for key, fig in post_sim.wip_figs.items():
        if fig is not None:
            plt.close(fig)
    for key, fig in post_sim.dist_figs.items():
        if fig is not None:
            plt.close(fig)

    # Use multi_run_averages from PostSim (pre-computed)
    averages = post_sim.multi_run_averages
    
    # Get event_counts from validation_results
    event_counts = validation_results.get('event_counts', {})
    total_events = event_counts.get('total', 0)
    
    return {
        'avg_micap': averages['avg_micap'],
        'avg_fleet': averages['avg_fleet'],
        'avg_cd_f': averages['avg_cd_f'],
        'avg_depot': averages['avg_depot'],
        'avg_cd_a': averages['avg_cd_a'],
        'count': averages['count'],
        # Pre-rendered figures as PNG bytes (memory efficient)
        'wip_figs_bytes': multi_figs_bytes,
        # Event counts for cumulative tracking
        'total_events': total_events,
    }


def run_single_simulation_fast(params, depot_cap, n_parts):
    """
    Run a single simulation WITHOUT figure generation for maximum speed.
    
    This is a faster version of run_single_simulation that skips all
    figure generation and only returns numerical averages.
    
    Args:
        params: Parameters object with all simulation settings
        depot_cap: Depot capacity value
        n_parts: Number of parts value
        
    Returns:
        dict: Results including averages for all metrics (no figures)
    """
    allocation = calculate_initial_allocation(params)

    sim_engine = SimulationEngine(
        params=params,
        allocation=allocation
    )
    validation_results = sim_engine.run()

    # Get PostSim for averages only - no figure processing
    post_sim = validation_results['post_sim']

    # Use multi_run_averages from PostSim (pre-computed)
    averages = post_sim.multi_run_averages
    
    # Get event_counts from validation_results
    event_counts = validation_results.get('event_counts', {})
    total_events = event_counts.get('total', 0)
    
    return {
        'avg_micap': averages['avg_micap'],
        'avg_fleet': averages['avg_fleet'],
        'avg_cd_f': averages['avg_cd_f'],
        'avg_depot': averages['avg_depot'],
        'avg_cd_a': averages['avg_cd_a'],
        'count': averages['count'],
        # No figures - this is the fast version
        # Event counts for cumulative tracking
        'total_events': total_events,
    }


def generate_analysis_text(df, best_results, best_by_parts, params_dict, depot_values, parts_values):
    """Generate the analysis text file content similar to _forloop3.py output."""
    lines = []
    
    # Header
    lines.append("Optimal n_total_parts by depot_capacity Analysis")
    lines.append("=" * 70)
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    
    # Parameter ranges
    lines.append("Parameter Values Tested:")
    lines.append(f"depot_capacity: {depot_values}")
    lines.append(f"n_total_parts: {parts_values}")
    lines.append("")
    
    # Fixed parameters
    lines.append("Fixed Parameters:")
    lines.append(f"n_total_aircraft = {params_dict['n_total_aircraft']}")
    lines.append(f"part_order_lag = {params_dict['part_order_lag']}")
    lines.append(f"mission_capable_rate = {params_dict['mission_capable_rate']}")
    lines.append(f"sim_time = {params_dict['sim_time']}")
    lines.append(f"sone_dist = {params_dict['sone_dist']}")
    lines.append(f"sone_mean = {params_dict['sone_mean']}")
    lines.append(f"sone_sd = {params_dict['sone_sd']}")
    lines.append(f"sthree_dist = {params_dict['sthree_dist']}")
    lines.append(f"sthree_mean = {params_dict['sthree_mean']}")
    lines.append(f"sthree_sd = {params_dict['sthree_sd']}")
    lines.append(f"condemn_cycle = {params_dict['condemn_cycle']}")
    lines.append(f"condemn_depot_fraction = {params_dict['condemn_depot_fraction']}")
    lines.append(f"random_seed = {params_dict['random_seed']}")
    lines.append("")
    
    # Summary: Best for each depot_capacity
    lines.append("=" * 70)
    lines.append("SUMMARY: Best (Lowest) Average MICAP for Each depot_capacity")
    lines.append("=" * 70)
    lines.append("")
    
    for depot_cap in sorted(best_results.keys()):
        row = best_results[depot_cap]
        lines.append(f"depot_capacity = {depot_cap}")
        lines.append(f"  ★ Best: n_total_parts = {row['n_total_parts']}")
        lines.append(f"    Avg MICAP = {row['avg_micap']:.2f}")
        lines.append(f"    Avg Fleet = {row['avg_fleet']:.2f}")
        lines.append(f"    Avg Cd_F = {row['avg_cd_f']:.2f}")
        lines.append(f"    Avg Depot = {row['avg_depot']:.2f}")
        lines.append(f"    Avg Cd_A = {row['avg_cd_a']:.2f}")
        lines.append(f"    (count: {row['count']})")
        lines.append("")
    
    # Detailed results by depot
    lines.append("")
    lines.append("=" * 70)
    lines.append("DETAILED RESULTS: All n_total_parts values for each depot_capacity")
    lines.append("=" * 70)
    lines.append("")
    
    for depot_cap in sorted(df['depot_capacity'].unique()):
        lines.append(f"depot_capacity = {depot_cap}")
        lines.append("-" * 50)
        
        depot_df = df[df['depot_capacity'] == depot_cap].sort_values('n_total_parts')
        best_n_parts = best_results[depot_cap]['n_total_parts'] if depot_cap in best_results else None
        
        for _, row in depot_df.iterrows():
            marker = " ★ BEST" if row['n_total_parts'] == best_n_parts else ""
            lines.append(f"  n_total_parts = {int(row['n_total_parts'])}: "
                        f"MICAP={row['avg_micap']:.2f}, Fleet={row['avg_fleet']:.2f}, "
                        f"Cd_F={row['avg_cd_f']:.2f}, Depot={row['avg_depot']:.2f}, "
                        f"Cd_A={row['avg_cd_a']:.2f} (count: {int(row['count'])}){marker}")
        lines.append("")
    
    # Summary: Best for each n_total_parts
    lines.append("")
    lines.append("=" * 70)
    lines.append("SUMMARY: Best (Lowest) Average MICAP for Each n_total_parts")
    lines.append("=" * 70)
    lines.append("")
    
    for n_parts in sorted(best_by_parts.keys()):
        row = best_by_parts[n_parts]
        lines.append(f"n_total_parts = {n_parts}")
        lines.append(f"  ★ Best: depot_capacity = {row['depot_capacity']}")
        lines.append(f"    Avg MICAP = {row['avg_micap']:.2f}")
        lines.append(f"    Avg Fleet = {row['avg_fleet']:.2f}")
        lines.append(f"    Avg Cd_F = {row['avg_cd_f']:.2f}")
        lines.append(f"    Avg Depot = {row['avg_depot']:.2f}")
        lines.append(f"    Avg Cd_A = {row['avg_cd_a']:.2f}")
        lines.append("")
    
    # Detailed results by parts
    lines.append("")
    lines.append("=" * 70)
    lines.append("DETAILED RESULTS: All depot_capacity values for each n_total_parts")
    lines.append("=" * 70)
    lines.append("")
    
    for n_parts in sorted(df['n_total_parts'].unique()):
        lines.append(f"n_total_parts = {int(n_parts)}")
        lines.append("-" * 50)
        
        parts_df = df[df['n_total_parts'] == n_parts].sort_values('depot_capacity')
        best_depot = best_by_parts[n_parts]['depot_capacity'] if n_parts in best_by_parts else None
        
        for _, row in parts_df.iterrows():
            marker = " ★ BEST" if row['depot_capacity'] == best_depot else ""
            lines.append(f"  depot_capacity = {int(row['depot_capacity'])}: "
                        f"MICAP={row['avg_micap']:.2f}, Fleet={row['avg_fleet']:.2f}, "
                        f"Cd_F={row['avg_cd_f']:.2f}, Depot={row['avg_depot']:.2f}, "
                        f"Cd_A={row['avg_cd_a']:.2f} (count: {int(row['count'])}){marker}")
        lines.append("")
    
    # Overall best
    best_overall = df.loc[df['avg_micap'].idxmin()]
    lines.append("")
    lines.append("=" * 70)
    lines.append("OVERALL BEST RESULT")
    lines.append("=" * 70)
    lines.append(f"★★★ ABSOLUTE BEST across all {len(df)} simulations:")
    lines.append(f"    depot_capacity = {int(best_overall['depot_capacity'])}")
    lines.append(f"    n_total_parts = {int(best_overall['n_total_parts'])}")
    lines.append(f"    Avg MICAP = {best_overall['avg_micap']:.2f}")
    lines.append(f"    Avg Fleet = {best_overall['avg_fleet']:.2f}")
    lines.append(f"    Avg Cd_F = {best_overall['avg_cd_f']:.2f}")
    lines.append(f"    Avg Depot = {best_overall['avg_depot']:.2f}")
    lines.append(f"    Avg Cd_A = {best_overall['avg_cd_a']:.2f}")
    
    return "\n".join(lines)
