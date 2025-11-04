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
    - micap_df: Aircraft waiting for parts
    """
    
    def __init__(self, n_total_parts, n_total_aircraft, sim_time,
                 sone_mean, sthree_mean):
        """
        Initialize DataFrameManager with simulation parameters.
        
        R code reference (main_r-code.R lines 1-11, 15-21, 67-156)
        """
        # Store parameters
        self.n_total_parts = n_total_parts
        self.n_total_aircraft = n_total_aircraft
        self.sim_time = sim_time
        self.sone_mean = sone_mean
        self.sthree_mean = sthree_mean
        
        # Calculate pre-allocation sizes
        self.max_sim_events, self.max_des_events = self._calculate_max_events()
        
        # Create all DataFrames
        self.aircraft_df = self._create_aircraft_df()
        self.condition_a_df = self._create_condition_a_df()
        self.new_part_df = self._create_new_part_df() # adding to handle new part logic
        self.sim_df = self._create_sim_df()
        self.des_df = self._create_des_df()
        self.micap_df = self._create_micap_df()
        
        # Initialize row counters (R lines 145-146)
        self.current_sim_row = 0  # Python 0-based (will use as index)
        self.current_des_row = 0  # Python 0-based (will use as index)
    
    def _calculate_max_events(self):
        """
        R code reference (main_r-code.R lines 15-21):
        
        min_cycle_time <- sone_mean + stwo_mean + sthree_mean + sfour_mean
        max_cycles_per_part <- ceiling(sim_time / min_cycle_time)
        max_sim_events <- n_total_parts * max_cycles_per_part * 3
        max_des_events <- n_total_aircraft * max_cycles_per_part * 2
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
        R code reference (main_r-code.R lines 67-73):
        
        aircraft_df <- tibble(
          des_id = seq_len(n_total_aircraft),
          ac_id = seq_len(n_total_aircraft),
          aircraft_name = "strike",
          micap = "no",
          part_id = seq_len(n_total_aircraft),
          sim_id = seq_len(n_total_aircraft)
        )
        NOTe: python code starts index at 0 vs r that starts at 1. 
        So we will have part_id and ac_id 0.
        """
        # First n_total_aircraft parts are paired 1-to-1 with aircraft
        return pd.DataFrame({
            'des_id': range(self.n_total_aircraft),
            'ac_id': range(self.n_total_aircraft),
            'aircraft_name': ['strike'] * self.n_total_aircraft,
            'micap': ['no'] * self.n_total_aircraft,
            'part_id': range(self.n_total_aircraft),
            'sim_id': range(self.n_total_aircraft)
        })
    
    def _create_condition_a_df(self):
        """
        R code reference (main_r-code.R lines 76-139):
        
        leftover_parts <- if (n_total_parts > n_total_aircraft) {
          seq(n_total_aircraft + 1, n_total_parts)
        } else {
          integer(0)
        }
        
        condition_a_df <- if (length(leftover_parts) > 0) {
          tibble(...with leftover part rows...)
        } else {
          tibble(...empty with same schema...)
        }
        """
        # Calculate leftover parts (not assigned to aircraft initially)
        if self.n_total_parts > self.n_total_aircraft:
            leftover_parts = list(range(self.n_total_aircraft, 
                                       self.n_total_parts))
        else:
            leftover_parts = []
        
        # Create schema matching R code exactly
        if len(leftover_parts) > 0:
            # Parts start in available inventory at time 1
            return pd.DataFrame({
                'sim_id': [None] * len(leftover_parts),
                'part_id': leftover_parts,
                'desone_id': [None] * len(leftover_parts),
                'acone_id': [None] * len(leftover_parts),
                'micap': [None] * len(leftover_parts),
                'fleet_duration': [np.nan] * len(leftover_parts),
                'condition_f_duration': [np.nan] * len(leftover_parts),
                'depot_duration': [np.nan] * len(leftover_parts),
                'condition_a_duration': [np.nan] * len(leftover_parts),
                'install_duration': [np.nan] * len(leftover_parts),
                'fleet_start': [np.nan] * len(leftover_parts),
                'fleet_end': [np.nan] * len(leftover_parts),
                'condition_f_start': [np.nan] * len(leftover_parts),
                'condition_f_end': [np.nan] * len(leftover_parts),
                'depot_start': [np.nan] * len(leftover_parts),
                'depot_end': [np.nan] * len(leftover_parts),
                'destwo_id': [None] * len(leftover_parts),
                'actwo_id': [None] * len(leftover_parts),
                'condition_a_start': [1.0] * len(leftover_parts),
                'condition_a_end': [np.nan] * len(leftover_parts),
                'install_start': [np.nan] * len(leftover_parts),
                'install_end': [np.nan] * len(leftover_parts),
                'cycle': [0] * len(leftover_parts),
                'condemn': ['no'] * len(leftover_parts)
            })
        else:
            # Empty dataframe with same schema
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
        
        R code reference: This is NEW logic not in main_r-code.R
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
        R code reference (main_r-code.R lines 97-122):
        
        sim_df <- tibble::tibble(
          sim_id = rep(NA_integer_, max_sim_events),
          part_id = rep(NA_integer_, max_sim_events),
          desone_id = rep(NA_integer_, max_sim_events),
          ...all columns with rep(NA_real_/NA_integer_/NA_character_, max_sim_events)...
        )
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
        R code reference (main_r-code.R lines 125-141):
        
        des_df <- tibble::tibble(
          des_id = rep(NA_integer_, max_des_events),
          ac_id = rep(NA_integer_, max_des_events),
          micap = rep(NA_character_, max_des_events),
          ...all columns with rep(NA_real_/NA_integer_/NA_character_, max_des_events)...
        )
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
    
    def _create_micap_df(self):
        """
        R code reference (main_r-code.R lines 146-156):
        
        micap_df <- tibble::tibble(
          des_id = integer(),
          ac_id = integer(),
          micap = character(),
          ...empty columns...
        )
        """
        # Empty DataFrame for tracking MICAP events
        return pd.DataFrame({
            'des_id': pd.Series(dtype='Int64'),
            'ac_id': pd.Series(dtype='Int64'),
            'micap': pd.Series(dtype='object'),
            'fleet_duration': pd.Series(dtype='float64'),
            'fleet_start': pd.Series(dtype='float64'),
            'fleet_end': pd.Series(dtype='float64'),
            'micap_duration': pd.Series(dtype='float64'),
            'micap_start': pd.Series(dtype='float64'),
            'micap_end': pd.Series(dtype='float64')
        })
    
    def trim_dataframes(self):
        """
        R code reference (main_r-code.R lines 755-757):
        
        sim_df <- sim_df[1:(current_sim_row - 1), ]
        des_df <- des_df[1:(current_des_row - 1), ]
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
        R code reference (main_r-code.R lines 759-774):
        
        Validation checks after trimming.
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
        cond_a = self.condition_a_df
        micap = self.micap_df
        
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
            aircraft_in_micap = ((micap['micap_start'] <= day) & (day < micap['micap_end'])).sum()
            aircraft_entering_micap = (micap['micap_start'].round() == day).sum()
            aircraft_leaving_micap = (micap['micap_end'].round() == day).sum()
            
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
                'aircraft_in_fleet': aircraft_in_fleet,
                'aircraft_in_micap': aircraft_in_micap,
                'aircraft_entering_micap': aircraft_entering_micap,
                'aircraft_leaving_micap': aircraft_leaving_micap
            })
        
        return pd.DataFrame(metrics)