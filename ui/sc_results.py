"""
Multi Results Display Components

Handles summary computation, metric plots.
"""
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


def get_metrics_list():
    """Return the standard metrics list for comparison plots."""
    return [
        ('avg_micap', 'Avg MICAP'),
        ('avg_fleet', 'Avg Fleet'),
        ('avg_cd_f', 'Avg Cd_F'),
        ('avg_depot', 'Avg Depot (WIP)'),
        ('avg_cd_a', 'Avg Cd_A')
    ]


def compute_summaries(df):
    """
    Compute best configurations from results DataFrame.
    
    Args:
        df: DataFrame with simulation results
        
    Returns:
        dict with: depots, parts_unique, best_results, best_by_parts,
                   summary_df, summary_parts_df, best_row
    """
    best_row = df.loc[df['avg_micap'].idxmin()]
    depots = sorted(df['depot_capacity'].unique())
    parts_unique = sorted(df['n_total_parts'].unique())
    
    # Best by depot
    best_results = {}
    summary_rows = []
    for depot in depots:
        depot_df = df[df['depot_capacity'] == depot]
        best = depot_df.loc[depot_df['avg_micap'].idxmin()]
        best_results[depot] = best.to_dict()
        summary_rows.append({
            'Depot Cap': int(depot),
            'Best Parts': int(best['n_total_parts']),
            'Avg MICAP': round(best['avg_micap'], 2),
            'Avg Fleet': round(best['avg_fleet'], 2),
            'Avg Cd_F': round(best['avg_cd_f'], 2),
            'Avg Depot': round(best['avg_depot'], 2),
            'Avg Cd_A': round(best['avg_cd_a'], 2),
        })
    
    summary_df = pd.DataFrame(summary_rows)
    
    # Best by parts
    best_by_parts = {}
    summary_parts_rows = []
    for n_parts in parts_unique:
        parts_df = df[df['n_total_parts'] == n_parts]
        best = parts_df.loc[parts_df['avg_micap'].idxmin()]
        best_by_parts[n_parts] = best.to_dict()
        summary_parts_rows.append({
            'N Parts': int(n_parts),
            'Best Depot': int(best['depot_capacity']),
            'Avg MICAP': round(best['avg_micap'], 2),
            'Avg Fleet': round(best['avg_fleet'], 2),
            'Avg Cd_F': round(best['avg_cd_f'], 2),
            'Avg Depot': round(best['avg_depot'], 2),
            'Avg Cd_A': round(best['avg_cd_a'], 2),
        })
    
    summary_parts_df = pd.DataFrame(summary_parts_rows)
    
    return {
        'depots': depots,
        'parts_unique': parts_unique,
        'best_results': best_results,
        'best_by_parts': best_by_parts,
        'summary_df': summary_df,
        'summary_parts_df': summary_parts_df,
        'best_row': best_row,
    }


def display_best_metrics(best_row):
    """Display the best overall metrics at top of results."""
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Best MICAP", f"{best_row['avg_micap']:.2f}")
    with col2:
        st.metric("Best Depot", f"{int(best_row['depot_capacity'])}")
    with col3:
        st.metric("Best Parts", f"{int(best_row['n_total_parts'])}")


def create_metric_plot(df, depots, parts_unique, metric, title, by_depot=True, figsize=(8, 4)):
    """
    Create a metric plot grouped by depot or parts.
    
    Args:
        df: DataFrame with simulation results
        depots: List of depot capacity values
        parts_unique: List of n_total_parts values
        metric: Column name to plot (e.g., 'avg_micap')
        title: Plot title
        by_depot: If True, group by depot (x=parts). If False, group by parts (x=depot)
        figsize: Figure size tuple
        
    Returns:
        matplotlib Figure
    """
    colors_by_depot = plt.cm.viridis(np.linspace(0, 1, len(depots)))
    colors_by_parts = plt.cm.plasma(np.linspace(0, 1, len(parts_unique)))
    
    fig, ax = plt.subplots(figsize=figsize)
    if by_depot:
        for i, depot in enumerate(depots):
            depot_df = df[df['depot_capacity'] == depot]
            means = depot_df.groupby('n_total_parts')[metric].mean()
            ax.plot(means.index, means.values, 'o-', 
                    label=f'{depot}', color=colors_by_depot[i], markersize=4, linewidth=1.5)
        ax.set_xlabel('N Total Parts', fontsize=10)
        ax.legend(title='Depot Cap', bbox_to_anchor=(1.02, 1), loc='upper left', fontsize=8)
    else:
        for i, n_parts in enumerate(parts_unique):
            parts_df = df[df['n_total_parts'] == n_parts]
            means = parts_df.groupby('depot_capacity')[metric].mean()
            ax.plot(means.index, means.values, 'o-', 
                    label=f'{int(n_parts)}', color=colors_by_parts[i], markersize=4, linewidth=1.5)
        ax.set_xlabel('Depot Capacity', fontsize=10)
        ax.legend(title='N Parts', bbox_to_anchor=(1.02, 1), loc='upper left', fontsize=8)
    ax.set_ylabel(title, fontsize=10)
    ax.set_title(title, fontsize=11)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    return fig



def close_all_figures(*figs):
    """Close all provided matplotlib figures."""
    for fig in figs:
        if fig is not None:
            plt.close(fig)
