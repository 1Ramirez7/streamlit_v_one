import matplotlib.pyplot as plt
import streamlit as st


# === POSTSIM CLASS - NEW: render_duration_plots now takes post_sim ===
def render_duration_plots(post_sim):
    """
    Render all duration comparison plots using pre-computed figures from PostSim.
    This code was previously in main.py but move here since it made main.py harder to read sections
    """
    if not post_sim.dist_figs:
        st.info("Plot rendering is disabled. Check 'Render Plots' in sidebar to enable.")
        return
    
    ############################
    # FLEET DURATION PLOTS
    ############################
    st.subheader("📈 Fleet Duration Comparison")
    
    col1, col2 = st.columns(2)
    with col1:
        st.write("**Fleet Duration (No Initial Conditions)**")
        if post_sim.dist_figs.get('fleet_no_init'):
            st.pyplot(post_sim.dist_figs['fleet_no_init'])
    
    with col2:
        st.write("**Fleet Duration (Initial Conditions Only)**")
        if post_sim.dist_figs.get('fleet_init_only'):
            st.pyplot(post_sim.dist_figs['fleet_init_only'])
    
    ############################
    # DEPOT DURATION PLOTS
    ############################
    st.subheader("📈 Depot Duration Comparison")
    
    col3, col4 = st.columns(2)
    with col3:
        st.write("**Depot Duration (No Initial Conditions)**")
        if post_sim.dist_figs.get('depot_no_init'):
            st.pyplot(post_sim.dist_figs['depot_no_init'])
    
    with col4:
        st.write("**Depot Duration (Initial Conditions Only)**")
        if post_sim.dist_figs.get('depot_init_only'):
            st.pyplot(post_sim.dist_figs['depot_init_only'])
        
    ############################
    # Rest of distribution plots
    ############################
    st.subheader("📈 Stage Duration Distributions")
    
    col1, col2 = st.columns(2)
    with col1:
        if post_sim.dist_figs.get('fleet_full'):
            st.pyplot(post_sim.dist_figs['fleet_full'])
    with col2:
        if post_sim.dist_figs.get('condition_f'):
            st.pyplot(post_sim.dist_figs['condition_f'])
    
    col3, col4 = st.columns(2)
    with col3:
        if post_sim.dist_figs.get('depot_full'):
            st.pyplot(post_sim.dist_figs['depot_full'])
    with col4:
        if post_sim.dist_figs.get('condition_a'):
            st.pyplot(post_sim.dist_figs['condition_a'])



def plot_fleet_duration_full(all_parts_df):
    """Simple histogram of Fleet (Fleet) durations."""
    durations = all_parts_df['fleet_duration'].dropna()
    
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(durations, bins=30, color='steelblue', edgecolor='black', alpha=0.7)
    ax.set_xlabel('Duration (days)')
    ax.set_ylabel('Frequency')
    ax.set_title(f'All Observations: Fleet Duration Distribution\nMean: {durations.mean():.2f} days')
    ax.grid(axis='y', alpha=0.3)
    
    return fig

def plot_fleet_duration_no_init(all_parts_df, n_aircraft_with_parts):
    """Simple histogram of Fleet durations excluding initial conditions."""
    # Filter out initial condition sim_ids (1 to n_aircraft_with_parts)
    filtered_df = all_parts_df[all_parts_df['sim_id'] > n_aircraft_with_parts]
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
    ax.set_title(f'Fleet Duration Distribution (Filtered)\nMean: {durations.mean():.2f} days')
    ax.grid(axis='y', alpha=0.3)
    
    return fig

def plot_fleet_duration_init_only(all_parts_df, n_aircraft_with_parts):
    """Simple histogram of Fleet durations for initial conditions only."""
    # Include only initial condition sim_ids (1 to n_aircraft_with_parts)
    initial_df = all_parts_df[all_parts_df['sim_id'] <= n_aircraft_with_parts]
    durations = initial_df['fleet_duration'].dropna()
    
    if len(durations) == 0:
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.text(0.5, 0.5, 'No fleet duration data for initial conditions', 
                ha='center', va='center', fontsize=14)
        ax.set_title('Fleet Duration Distribution (Initial Conditions Only)')
        ax.set_xlabel('Duration (days)')
        ax.set_ylabel('Frequency')
        return fig
    
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(durations, bins=30, color='steelblue', edgecolor='black', alpha=0.7)
    ax.set_xlabel('Duration (days)')
    ax.set_ylabel('Frequency')
    ax.set_title(f'Fleet Duration Distribution (Initial Conditions Only)\nMean: {durations.mean():.2f} days')
    ax.grid(axis='y', alpha=0.3)
    
    return fig

def plot_condition_f_duration(all_parts_df):
    """Simple histogram of Condition F (Condition F) durations."""
    filtered_df = all_parts_df[
        (all_parts_df['condition_f_start'].notna()) & 
        (all_parts_df['condition_f_end'].notna()) & 
        (all_parts_df['condition_f_start'] != all_parts_df['condition_f_end'])
    ]
    durations = filtered_df['condition_f_duration'].dropna()
    
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(durations, bins=30, color='coral', edgecolor='black', alpha=0.7)
    ax.set_xlabel('Duration (days)')
    ax.set_ylabel('Frequency')
    ax.set_title(f'Condition F Duration Distribution\nMean: {durations.mean():.2f} days')
    ax.grid(axis='y', alpha=0.3)
    
    return fig

def plot_depot_duration_full(all_parts_df): # full data 
    """Simple histogram of Depot durations."""
    durations = all_parts_df['depot_duration'].dropna()
    
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(durations, bins=30, color='mediumseagreen', edgecolor='black', alpha=0.7)
    ax.set_xlabel('Duration (days)')
    ax.set_ylabel('Frequency')
    ax.set_title(f'All Observations: Depot Duration Distribution\nMean: {durations.mean():.2f} days')
    ax.grid(axis='y', alpha=0.3)
    
    return fig

def plot_depot_duration_no_init(all_parts_df, depot_part_ids):
    """Simple histogram of Depot durations excluding initial conditions."""
    filtered_df = all_parts_df[(~all_parts_df['sim_id'].isin(depot_part_ids)) & (all_parts_df['condemn'] != 'yes')]
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
    ax.set_title(f'Depot Duration Distribution (Filtered)\nMean: {durations.mean():.2f} days')
    ax.grid(axis='y', alpha=0.3)
    
    return fig

def plot_depot_duration_init_only(all_parts_df, depot_part_ids):
    """Simple histogram of Depot durations for initial conditions only, excluding condemned parts."""
    initial_df = all_parts_df[all_parts_df['sim_id'].isin(depot_part_ids) & (all_parts_df['condemn'] != 'yes')]
    durations = initial_df['depot_duration'].dropna()
    
    if len(durations) == 0:
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.text(0.5, 0.5, 'No depot duration data for initial conditions', 
                ha='center', va='center', fontsize=14)
        ax.set_title('Depot Duration Distribution (Initial Conditions Only)')
        ax.set_xlabel('Duration (days)')
        ax.set_ylabel('Frequency')
        return fig
    
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(durations, bins=30, color='mediumseagreen', edgecolor='black', alpha=0.7)
    ax.set_xlabel('Duration (days)')
    ax.set_ylabel('Frequency')
    ax.set_title(f'Depot Duration Distribution (Initial Conditions Only)\nMean: {durations.mean():.2f} days')
    ax.grid(axis='y', alpha=0.3)
    
    return fig



def plot_cond_a_duration(all_parts_df):
    """Simple histogram of Condition A durations."""
    # Filter out rows where cond_a_dura.. was zero. since cond is register for all, its just zero when use immediatly
    filtered_df = all_parts_df[
        (all_parts_df['condition_a_start'].notna()) & 
        (all_parts_df['condition_a_end'].notna()) & 
        (all_parts_df['condition_a_start'] != all_parts_df['condition_a_end'])
    ]
    durations = filtered_df['condition_a_duration'].dropna()
    
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(durations, bins=30, color='mediumpurple', edgecolor='black', alpha=0.7)
    ax.set_xlabel('Duration (days)')
    ax.set_ylabel('Frequency')
    ax.set_title(f'Condition A Duration Distribution\nMean: {durations.mean():.2f} days')
    ax.grid(axis='y', alpha=0.3)
    
    return fig
