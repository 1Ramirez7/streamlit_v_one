"""
PostSim class - holds all post-simulation computed results.

Created to ensure all stats, figures, and results
are computed inside engine.run()

Ensures post-simulation does not use updated params post-sim
"""
from ui.stats import calculate_simulation_stats
from ui.wip_plots import (
    plot_micap_over_time,
    plot_fleet_wip_over_time,
    plot_condition_f_wip_over_time,
    plot_depot_wip_over_time,
    plot_condition_a_wip_over_time
)
from ui.dist_plots import (
    plot_fleet_duration_full,
    plot_fleet_duration_no_init,
    plot_fleet_duration_init_only,
    plot_condition_f_duration,
    plot_depot_duration_full,
    plot_depot_duration_no_init,
    plot_depot_duration_init_only,
    plot_cond_a_duration
)


class PostSim:
    """
    Holds all post-simulation computed results.
    
    Instantiated at end of SimulationEngine.run() with:
    - datasets: DataSets object with simulation data
    - event_counts: dict of event counts from simulation
    - params: Parameters object used for this run
    - allocation: dict with initial allocation info
    
    Computes and stores:
    - stats: dict of all calculated statistics (from stats.py)
    - wip_figs: dict of WIP plot figures (if render_plots=True)
    - dist_figs: dict of distribution plot figures (if render_plots=True)
    
    UI code (main.py) accesses these pre-computed values for display.
    """
    
    def __init__(self, datasets, event_counts, params, allocation):
        """
        Initialize PostSim with simulation results.
        
        Parameters
        ----------
        datasets : DataSets
            Contains all_parts_df, all_ac_df, wip_raw, wip_ac_raw
        event_counts : dict
            Event counts from simulation (total, depot_complete, etc.)
        params : Parameters
            Parameters object used for this simulation run
        allocation : dict
            Initial allocation dict (n_aircraft_with_parts, depot_part_ids, etc.)
        """
        # Store references
        self.datasets = datasets
        self.event_counts = event_counts
        self.params = params
        self.allocation = allocation
        
        # Extract commonly used params
        self.n_total_aircraft = params['n_total_aircraft']
        self.n_total_parts = params['n_total_parts']
        self.depot_capacity = params['depot_capacity']
        self.render_plots = params['render_plots']
        self.use_percentage_plots = params.get('use_percentage_plots', True)
        
        # === Compute statistics (always computed) ===
        self.stats = calculate_simulation_stats(datasets)

        # === Compute multi-run averages (for multi-model and solo UI) ===
        from ui.stats import calculate_multi_run_averages
        self.multi_run_averages = calculate_multi_run_averages(datasets)
        
        # === Compute figures (only if render_plots=True) ===
        self.wip_figs = {}
        self.dist_figs = {}
        
        if self.render_plots:
            self._generate_wip_figures()
            self._generate_dist_figures()
    
    def _generate_wip_figures(self):
        """
        Generate all WIP plot figures.
        """
        wip_raw = self.datasets.wip_raw
        wip_ac_raw = self.datasets.wip_ac_raw
        
        # Check if data exists
        if wip_raw is None or len(wip_raw) == 0:
            return
        
        # Generate figures
        self.wip_figs['micap'] = plot_micap_over_time(wip_ac_raw, self.n_total_aircraft, self.use_percentage_plots)
        self.wip_figs['fleet'] = plot_fleet_wip_over_time(wip_ac_raw, self.n_total_aircraft, self.use_percentage_plots)
        self.wip_figs['condition_f'] = plot_condition_f_wip_over_time(wip_raw, self.n_total_parts, self.use_percentage_plots)
        self.wip_figs['depot'] = plot_depot_wip_over_time(wip_raw, self.depot_capacity, self.use_percentage_plots)
        self.wip_figs['condition_a'] = plot_condition_a_wip_over_time(wip_raw, self.n_total_parts, self.use_percentage_plots)
    
    def _generate_dist_figures(self):
        """
        Generate all distribution plot figures.
        
        Stores figures in self.dist_figs dict with keys:
        - 'fleet_full': All fleet durations 
        - 'condition_a': Condition A durations
        """
        all_parts_df = self.datasets.all_parts_df
        
        # Check if data exists
        if all_parts_df is None or len(all_parts_df) == 0:
            return
        
        # Extract allocation values needed for filtering
        n_aircraft_with_parts = self.allocation['n_aircraft_with_parts']
        depot_part_ids = self.allocation['depot_part_ids']
        
        # Generate figures
        self.dist_figs['fleet_full'] = plot_fleet_duration_full(all_parts_df)
        self.dist_figs['fleet_no_init'] = plot_fleet_duration_no_init(all_parts_df, n_aircraft_with_parts)
        self.dist_figs['fleet_init_only'] = plot_fleet_duration_init_only(all_parts_df, n_aircraft_with_parts)
        self.dist_figs['condition_f'] = plot_condition_f_duration(all_parts_df)
        self.dist_figs['depot_full'] = plot_depot_duration_full(all_parts_df)
        self.dist_figs['depot_no_init'] = plot_depot_duration_no_init(all_parts_df, depot_part_ids)
        self.dist_figs['depot_init_only'] = plot_depot_duration_init_only(all_parts_df, depot_part_ids)
        self.dist_figs['condition_a'] = plot_cond_a_duration(all_parts_df)
    
    def has_wip_data(self):
        """Check if WIP data is available."""
        return self.datasets.wip_raw is not None and len(self.datasets.wip_raw) > 0
    
    def get_wip_fig(self, key):
        """
        Get a WIP figure by key.
        
        Parameters
        ----------
        key : str
            One of: 'micap', 'fleet', 'condition_f',
                    'depot', 'condition_a'
        """
        return self.wip_figs.get(key)
    
    def get_dist_fig(self, key):
        """
        Get a distribution figure by key.
        
        Parameters
        ----------
        key : str
            One of: 'fleet_full', 'fleet_no_init', 'fleet_init_only', 
                    'condition_f', 'depot_full', 'depot_no_init', 
                    'depot_init_only', 'condition_a'
        """
        return self.dist_figs.get(key)
