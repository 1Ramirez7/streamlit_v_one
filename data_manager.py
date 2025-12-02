import pandas as pd
import numpy as np
from math import ceil

# Parameter metadata: (ui_label, code_name)
# Order matches UI input order for easy tracking
PARAM_METADATA = [
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


class DataFrameManager:
    """
    Manages DataFrames for the simulation.
    
    Stores simulation parameters and allocation data.
    DataFrame creation moved to respective state management classes.
    
    Attributes
    ----------
    user_params : list
        Ordered list of tuples: (ui_label, code_name, value)
        Preserves UI input order for logging/export.
    """
    
    def __init__(self, n_total_parts, n_total_aircraft, sim_time,
                 sone_mean, sthree_mean, allocation=None):
        """
        Maybe use type hints for better documentation, example 
        self, 
        n_total_parts: int, 
        n_total_aircraft: int,  ....
        """
        # Store parameters
        self.n_total_parts = n_total_parts
        self.n_total_aircraft = n_total_aircraft
        self.sim_time = sim_time
        self.sone_mean = sone_mean
        self.sthree_mean = sthree_mean
        self.allocation = allocation
        
        # User parameters log - populated by store_user_params()
        # List of tuples: (ui_label, code_name, value)
        self.user_params = []
    
    def store_user_params(self, params_dict):
        """
        Store user parameters in order matching UI input sequence.
        Returns DataFrame for easy export to Excel/CSV.
        
        Parameters
        ----------
        params_dict : dict
            Dictionary from render_sidebar() containing all user inputs.
            Keys are code_name values (e.g., 'n_total_parts', 'sone_dist')
        
        Returns
        -------
        pd.DataFrame
            Columns: ui_label, code_name, value
        
        Example
        -------
        >>> params_df = df_manager.store_user_params(params)
        >>> params_df.to_excel("params.xlsx")
        """
        self.user_params = []
        
        for ui_label, code_name in PARAM_METADATA:
            if code_name in params_dict:
                value = params_dict[code_name]
                self.user_params.append((ui_label, code_name, value))
        
        # Return as DataFrame for easy export
        self.params_df = pd.DataFrame(
            self.user_params,
            columns=['ui_label', 'code_name', 'value']
        )
        return self.params_df
