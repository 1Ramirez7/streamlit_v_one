"""
DataFrame Manager for Hill AFB DES Simulation

Handles ONLY DataFrame structure and pre-allocation.
NO simulation logic, formulas, or event processing.

All structure matches main_r-code.R exactly.
"""

import pandas as pd
import numpy as np
from math import ceil

class DataFrameManager:
    """
    Manages all DataFrames for the simulation with pre-allocation.
    
    Creates 5 DataFrames:
    - sim_df: Part event log (pre-allocated)
    - des_df: Aircraft event log (pre-allocated)
    - aircraft_df: Initial aircraft-part assignments
    - condition_a_df: Parts waiting in inventory
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
        
        # Calculate pre-allocation sizes
        self.max_sim_events, self.max_des_events = self._calculate_max_events()
        
        # Create all DataFrames
        self.aircraft_df = self._create_aircraft_df()
        self.condition_a_df = self._create_condition_a_df()
        self.new_part_df = self._create_new_part_df() # adding to handle new part logic
        self.sim_df = self._create_sim_df()
        self.des_df = self._create_des_df()
        
        # Initialize row counters (R lines 145-146)
        self.current_sim_row = 0  # Python 0-based (will use as index)
        self.current_des_row = 0  # Python 0-based (will use as index)
        
        self.condemn_new_log = []  # List to track condemned parts and replacements
    
    def _calculate_max_events(self):
        """
        Calculates possible number of events to pre-allocate rows to the SIM and DES dataframes
        Not needed once we stop using pandas dfs
        """
        # Calculate minimum cycle time
        min_cycle_time = (self.sone_mean + self.sthree_mean)
        
        # Maximum possible cycles per part
        max_cycles_per_part = ceil(self.sim_time / min_cycle_time)
        
        # Pre-allocation with safety factors
        max_sim_events = self.n_total_parts * max_cycles_per_part * 3
        max_des_events = self.n_total_aircraft * max_cycles_per_part * 2
        
        return max_sim_events, max_des_events
    
    def _create_aircraft_df(self):
        """
        Creates aircraft dataframe with initial conditions.
        calculated in utils.calculate_initial_allocation
        """
        x = self.allocation['n_aircraft_with_parts']

        return pd.DataFrame({
            'des_id': range(x),
            'ac_id': range(x),
            'aircraft_name': ['strike'] * x,
            'micap': ['IC_IZFS'] * x,
            'part_id': range(x),
            'sim_id': range(x)
        })
    
    def _create_condition_a_df(self):
        """
        Creates condition A dataframe for parts waiting in available inventory.
        """
        # Always return empty DataFrame with proper schema
        return pd.DataFrame({
            'sim_id': pd.Series(dtype='Int64'),
            'part_id': pd.Series(dtype='Int64'),
            'desone_id': pd.Series(dtype='Int64'),
            'acone_id': pd.Series(dtype='Int64'),
            'micap': pd.Series(dtype='object'),
            'fleet_duration': pd.Series(dtype='float64'),
            'condition_f_duration': pd.Series(dtype='float64'),
            'depot_duration': pd.Series(dtype='float64'),
            'condition_a_duration': pd.Series(dtype='float64'),
            'install_duration': pd.Series(dtype='float64'),
            'fleet_start': pd.Series(dtype='float64'),
            'fleet_end': pd.Series(dtype='float64'),
            'condition_f_start': pd.Series(dtype='float64'),
            'condition_f_end': pd.Series(dtype='float64'),
            'depot_start': pd.Series(dtype='float64'),
            'depot_end': pd.Series(dtype='float64'),
            'destwo_id': pd.Series(dtype='Int64'),
            'actwo_id': pd.Series(dtype='Int64'),
            'condition_a_start': pd.Series(dtype='float64'),
            'condition_a_end': pd.Series(dtype='float64'),
            'install_start': pd.Series(dtype='float64'),
            'install_end': pd.Series(dtype='float64'),
            'cycle': pd.Series(dtype='Int64'),
            'condemn': pd.Series(dtype='object')
        })
        
    # adding code for new_part_df
    def _create_new_part_df(self):
        """
        Create DataFrame for tracking the next available part_id to be created.
        
        Purpose: Track next part_id when dynamic part creation is needed
        
        Initial state: Single row with part_id = n_total_parts (30 if n_total_parts=30)
        When a new part is created:
        1. Use the part_id from this row (e.g., 30)
        2. Create the new part with that ID
        3. Add a new row with part_id = previous + 1 (e.g., 31)
        
        Returns:
            pd.DataFrame: Single-row DataFrame with only part_id populated
        """
        # Create single row with just part_id = n_total_parts
        return pd.DataFrame({
            'sim_id': [np.nan],
            'part_id': [self.n_total_parts],  # Start at next available ID
            # this will result in part_id gaps ifs initial conditions fail to allocate all of n_total_parts
            'desone_id': [np.nan],
            'acone_id': [np.nan],
            'micap': [np.nan],
            'fleet_duration': [np.nan],
            'condition_f_duration': [np.nan],
            'depot_duration': [np.nan],
            'condition_a_duration': [np.nan],
            'install_duration': [np.nan],
            'fleet_start': [np.nan],
            'fleet_end': [np.nan],
            'condition_f_start': [np.nan],
            'condition_f_end': [np.nan],
            'depot_start': [np.nan],
            'depot_end': [np.nan],
            'destwo_id': [np.nan],
            'actwo_id': [np.nan],
            'condition_a_start': [np.nan],
            'condition_a_end': [np.nan],
            'install_start': [np.nan],
            'install_end': [np.nan],
            'cycle': [np.nan],
            'condemn': [np.nan]
        })

    def _create_sim_df(self):
        """
        MAIN event tracker for parts 
        Every event timing and related information is log in this sim_df

        Not 100% log because event handlers edit the dataframe 
        """
        # Pre-allocated DataFrame for part events
        return pd.DataFrame({
            'sim_id': [None] * self.max_sim_events,
            'part_id': [None] * self.max_sim_events,
            'desone_id': [None] * self.max_sim_events,
            'acone_id': [None] * self.max_sim_events,
            'micap': [None] * self.max_sim_events,
            'fleet_duration': [np.nan] * self.max_sim_events,
            'condition_f_duration': [np.nan] * self.max_sim_events,
            'depot_duration': [np.nan] * self.max_sim_events,
            'condition_a_duration': [np.nan] * self.max_sim_events,
            'install_duration': [np.nan] * self.max_sim_events,
            'fleet_start': [np.nan] * self.max_sim_events,
            'fleet_end': [np.nan] * self.max_sim_events,
            'condition_f_start': [np.nan] * self.max_sim_events,
            'condition_f_end': [np.nan] * self.max_sim_events,
            'depot_start': [np.nan] * self.max_sim_events,
            'depot_end': [np.nan] * self.max_sim_events,
            'destwo_id': [None] * self.max_sim_events,
            'actwo_id': [None] * self.max_sim_events,
            'condition_a_start': [np.nan] * self.max_sim_events,
            'condition_a_end': [np.nan] * self.max_sim_events,
            'install_start': [np.nan] * self.max_sim_events,
            'install_end': [np.nan] * self.max_sim_events,
            'cycle': [None] * self.max_sim_events,
            'condemn': [None] * self.max_sim_events
        })
    
    def _create_des_df(self):
        """
        MAIN event tracker for AIRCRAFT 
        Every event timing and related information is log here.

        Not 100% log because event handlers edit the dataframe 
        """
        # Pre-allocated DataFrame for aircraft events
        return pd.DataFrame({
            'des_id': [None] * self.max_des_events,
            'ac_id': [None] * self.max_des_events,
            'micap': [None] * self.max_des_events,
            'simone_id': [None] * self.max_des_events,
            'partone_id': [None] * self.max_des_events,
            'fleet_duration': [np.nan] * self.max_des_events,
            'fleet_start': [np.nan] * self.max_des_events,
            'fleet_end': [np.nan] * self.max_des_events,
            'micap_duration': [np.nan] * self.max_des_events,
            'micap_start': [np.nan] * self.max_des_events,
            'micap_end': [np.nan] * self.max_des_events,
            'simtwo_id': [None] * self.max_des_events,
            'parttwo_id': [None] * self.max_des_events,
            'install_duration': [np.nan] * self.max_des_events,
            'install_start': [np.nan] * self.max_des_events,
            'install_end': [np.nan] * self.max_des_events
        })
    
    def trim_dataframes(self):
        """
        Trims SIM and DES dataframes at end of simulation

        _calculate_max_events pre-allocates x-number of rows 
        and this function rmeoves unsused rows before SIM and DES are
        use for filtering. 
        """
        # Remove unused pre-allocated rows
        self.sim_df = self.sim_df.iloc[:self.current_sim_row].copy()
        self.des_df = self.des_df.iloc[:self.current_des_row].copy()
        
        # Convert integer columns to Int64 (pandas nullable integer)
        int_cols_sim = ['sim_id', 'part_id', 'desone_id', 'acone_id', 
                        'destwo_id', 'actwo_id', 'cycle']
        int_cols_des = ['des_id', 'ac_id', 'simone_id', 'partone_id', 
                        'simtwo_id', 'parttwo_id']
        
        for col in int_cols_sim:
            if col in self.sim_df.columns:
                self.sim_df[col] = pd.to_numeric(self.sim_df[col], errors='coerce').astype('Int64')
        
        for col in int_cols_des:
            if col in self.des_df.columns:
                self.des_df[col] = pd.to_numeric(self.des_df[col], errors='coerce').astype('Int64')
    
    def validate_structure(self):
        """
        Tracks how effective _calculate_max_events is!
        """
        validation_results = {
            'sim_df_rows_used': self.current_sim_row,
            'sim_df_rows_allocated': self.max_sim_events,
            'des_df_rows_used': self.current_des_row,
            'des_df_rows_allocated': self.max_des_events,
            'sim_df_usage_pct': (self.current_sim_row / self.max_sim_events * 100) if self.max_sim_events > 0 else 0,
            'des_df_usage_pct': (self.current_des_row / self.max_des_events * 100) if self.max_des_events > 0 else 0,
            'sim_df_na_sim_ids': self.sim_df['sim_id'].isna().sum(),
            'des_df_na_des_ids': self.des_df['des_id'].isna().sum(),
            'warnings': []
        }
        
        # Check for issues
        if validation_results['sim_df_na_sim_ids'] > 0:
            validation_results['warnings'].append(
                f"Found {validation_results['sim_df_na_sim_ids']} NA sim_ids in final sim_df"
            )
        
        if validation_results['des_df_na_des_ids'] > 0:
            validation_results['warnings'].append(
                f"Found {validation_results['des_df_na_des_ids']} NA des_ids in final des_df"
            )
        
        if validation_results['sim_df_usage_pct'] > 90:
            validation_results['warnings'].append(
                f"sim_df usage at {validation_results['sim_df_usage_pct']:.1f}% - consider increasing safety factor"
            )
        
        if validation_results['des_df_usage_pct'] > 90:
            validation_results['warnings'].append(
                f"des_df usage at {validation_results['des_df_usage_pct']:.1f}% - consider increasing safety factor"
            )
        
        return validation_results
    
    def create_daily_metrics(self):
        """
        Create time-series dataframe tracking stage counts and flow events per day.
        Called after simulation completes and dataframes are trimmed.
        """
        sim = self.sim_df
        des = self.des_df
        cond_a = self.condition_a_df # remove MICAP due to class. still need to figure out to use it here
        
        metrics = []
        for day in range(1, self.sim_time + 1):
            # WIP counts (parts currently in stage at time=day)
            wip_fleet = ((sim['fleet_start'] <= day) & (day < sim['fleet_end'])).sum()
            wip_condition_f = ((sim['condition_f_start'] <= day) & (day < sim['condition_f_end'])).sum()
            wip_depot = ((sim['depot_start'] <= day) & (day < sim['depot_end'])).sum()
            wip_condition_a = ((cond_a['condition_a_start'] <= day) & (day < cond_a['condition_a_end'])).sum()
            wip_install = ((sim['install_start'] <= day) & (day < sim['install_end'])).sum()
            
            # Flow events (stage transitions occurring on this day)
            parts_breaking = (sim['fleet_end'].round() == day).sum()
            parts_leaving_condition_f = (sim['condition_f_end'].round() == day).sum()
            parts_entering_depot = (sim['depot_start'].round() == day).sum()
            parts_completing_depot = (sim['depot_end'].round() == day).sum()
            parts_entering_condition_a = (cond_a['condition_a_start'].round() == day).sum()
            parts_leaving_condition_a = (cond_a['condition_a_end'].round() == day).sum()
            
            # Aircraft metrics
            aircraft_in_fleet = ((des['fleet_start'] <= day) & (day < des['fleet_end'])).sum()
            
            metrics.append({
                'day': day,
                # WIP
                'wip_fleet': wip_fleet,
                'wip_condition_f': wip_condition_f,
                'wip_depot': wip_depot,
                'wip_condition_a': wip_condition_a,
                'wip_install': wip_install,
                # Flow events
                'parts_breaking': parts_breaking,
                'parts_leaving_condition_f': parts_leaving_condition_f,
                'parts_entering_depot': parts_entering_depot,
                'parts_completing_depot': parts_completing_depot,
                'parts_entering_condition_a': parts_entering_condition_a,
                'parts_leaving_condition_a': parts_leaving_condition_a,
                # Aircraft
                'aircraft_in_fleet': aircraft_in_fleet
            })
        
        return pd.DataFrame(metrics)