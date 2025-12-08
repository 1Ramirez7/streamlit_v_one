"""
Parameters class for centralized simulation parameter management.

This module provides a single source of truth for all simulation parameters,
enabling easy scenario comparison and future extensions like time-varying params.
"""

import pandas as pd
from typing import Optional

# Parameter metadata: (ui_label, code_name)
# Keep Order: matches UI input order
# maybe add render_plots, and closing_periods
PARAM_METADATA = [
    ("Render Plots", "render_plots"),
    # Basic Parameters
    ("Total Parts", "n_total_parts"),
    ("Total Aircraft", "n_total_aircraft"),
    ("Warmup Periods (days)", "warmup_periods"),
    ("Simulation Time (days)", "analysis_periods"),
    ("Closing Periods (days)", "closing_periods"),
    ("Total Simulation Time", "sim_time"),
    ("Depot Capacity", "depot_capacity"),
    # Condemn Parameters
    ("Condemn at Cycle", "condemn_cycle"),
    ("Condemned Depot Time Fraction", "condemn_depot_fraction"),
    ("Part Order Lag (days)", "part_order_lag"),
    # Seed & Mission
    ("Random Seed", "random_seed"),
    ("Mission Capable Rate", "mission_capable_rate"),
    # Initial Allocation
    ("Parts Starting in Depot", "parts_in_depot"),
    ("Parts Starting in Condition F", "parts_in_cond_f"),
    ("Parts Starting in Condition A", "parts_in_cond_a"),
    # Distribution Selection
    ("Fleet Distribution", "sone_dist"),
    ("Depot Distribution", "sthree_dist"),
    # Fleet Distribution Parameters
    ("Fleet Mean/Shape", "sone_mean"),
    ("Fleet Std Dev/Scale", "sone_sd"),
    # Depot Distribution Parameters
    ("Depot Mean/Shape", "sthree_mean"),
    ("Depot Std Dev/Scale", "sthree_sd"),
    # Fleet Randomization
    ("Randomize Fleet Durations", "use_fleet_rand"),
    ("Fleet Rand Min", "fleet_rand_min"),
    ("Fleet Rand Max", "fleet_rand_max"),
    # Depot Randomization
    ("Randomize Depot Durations", "use_depot_rand"),
    ("Depot Rand Min", "depot_rand_min"),
    ("Depot Rand Max", "depot_rand_max"),
]

class Parameters:
    """
    Centralized container for all simulation parameters.
    
    Usage:
        params = Parameters()
        params.set_all(sidebar_params)  # Load from UI
        
        # Access params:
        params['depot_capacity']      # Dictionary-style
        params.get('depot_capacity')  # Method with optional default
    """
    
    def __init__(self):
        """Initialize empty parameters dictionary."""
        self._params = {}
    
    def set(self, key: str, value) -> None:
        """
        Set a single parameter.
        
        Args:
            key: Parameter name
            value: Parameter value
        """
        self._params[key] = value
    
    def set_all(self, params_dict: dict) -> None:
        """
        Set all parameters from a dictionary at once.
        
        Args:
            params_dict: Dictionary of parameter names and values
        """
        self._params.update(params_dict)
    
    def get(self, key: str, default=None):
        """
        Get a parameter value with optional default.
        
        Args:
            key: Parameter name
            default: Value to return if key not found (default: None)
            
        Returns:
            Parameter value or default if not found
        """
        return self._params.get(key, default)
    
    def __getitem__(self, key: str):
        """
        Allow dictionary-style access: params['key']
        
        Sample CLASS usage: 
            def __init__(self, df_manager, datasets, params):
                self.params = params

            def handle_aircraft_needs_part(self, des_id):
                if self.params['sone_mean'] > 100:  # <-- Uses __getitem__
    
        """
        return self._params[key]
    
    
    def keys(self):
        """
        Return all parameter names only.
        
        Returns:
            dict_keys of parameter names

            ['depot_capacity', 'sim_time', 'sone_mean']  <-- CODE NAMES
        """
        return self._params.keys()
    
    def to_dict(self) -> dict:
        """
        Export parameters as a dictionary.
        
        Useful for storing in session_state or exporting to CSV.
        
        Returns:
            Copy of parameters dictionary
        """
        return self._params.copy()
    
    def get_params_df(self) -> Optional[pd.DataFrame]:
        """
        Generate params DataFrame on-demand for export.
        
        Only creates DataFrame when called (at download time),
        saving memory when user doesn't download.
        
        Returns
        -------
        pd.DataFrame or None
            Columns: ui_label, code_name, value
            Returns None if no simulation has run.
        """
        params = self.get_params()
        if params is None:
            return None
        
        rows = []
        for ui_label, code_name in PARAM_METADATA:
            if code_name in params:
                value = params[code_name]
                rows.append((ui_label, code_name, value))
        
        return pd.DataFrame(rows, columns=['ui_label', 'code_name', 'value'])
    
    def __repr__(self) -> str:
        """
        utils method
        String representation showing number of parameters.
        Don't call is just to print human redable params
        """
        return f"Parameters({len(self._params)} params)"


