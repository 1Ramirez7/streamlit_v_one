"""
Simulation Engine for Hill AFB DES Simulation

Handles simulation logic, formulas, and event processing.
Works with DataFrameManager to access and update DataFrames.

All logic matches main_r-code.R exactly.
"""

import numpy as np
import pandas as pd

class SimulationEngine:
    """
    Manages simulation logic and event processing.
    
    Works with DataFrameManager for DataFrame access and updates.
    Contains formulas for stage durations and helper functions for event management.
    """
    
    def __init__(self, df_manager, sone_mean, sone_sd, stwo_mean, stwo_sd,
                 sthree_mean, sthree_sd, sfour_mean, sfour_sd, sim_time):
        """
        Initialize SimulationEngine with DataFrameManager and stage parameters.
        
        Parameters from main_r-code.R lines 1-11.
        
        Args:
            df_manager: DataFrameManager instance with all DataFrames
            sone_mean, sone_sd: Stage 1 (Fleet) normal distribution parameters
            stwo_mean, stwo_sd: Stage 2 (Condition F) normal distribution parameters
            sthree_mean, sthree_sd: Stage 3 (Depot) normal distribution parameters
            sfour_mean, sfour_sd: Stage 4 (Install) normal distribution parameters
            sim_time: Total simulation time
        """
        self.df = df_manager
        self.sone_mean = sone_mean
        self.sone_sd = sone_sd
        self.stwo_mean = stwo_mean
        self.stwo_sd = stwo_sd
        self.sthree_mean = sthree_mean
        self.sthree_sd = sthree_sd
        self.sfour_mean = sfour_mean
        self.sfour_sd = sfour_sd
        self.sim_time = sim_time
    
    # ==========================================================================
    # STAGE DURATION FORMULAS
    # ==========================================================================
    
    def calculate_fleet_duration(self):
        """
        R code reference (main_r-code.R lines 1-11, example line 301):
        
        d1 <- max(0, rnorm(1, mean = sone_mean, sd = sone_sd))
        """
        return max(0, np.random.normal(self.sone_mean, self.sone_sd))
    
    def calculate_condition_f_duration(self):
        """
        R code reference (main_r-code.R lines 1-11, example line 343):
        
        d2 <- max(0, rnorm(1, mean = stwo_mean, sd = stwo_sd))
        """
        return max(0, np.random.normal(self.stwo_mean, self.stwo_sd))
    
    def calculate_depot_duration(self):
        """
        R code reference (main_r-code.R lines 1-11, example line 350):
        
        d3 <- max(0, rnorm(1, mean = sthree_mean, sd = sthree_sd))
        """
        return max(0, np.random.normal(self.sthree_mean, self.sthree_sd))
    
    def calculate_install_duration(self):
        """
        R code reference (main_r-code.R lines 1-11, example line 468):
        
        d4_install <- max(0, rnorm(1, mean = sfour_mean, sd = sfour_sd))
        """
        return max(0, np.random.normal(self.sfour_mean, self.sfour_sd))
    
    # ==========================================================================
    # HELPER FUNCTIONS: ID GENERATION
    # ==========================================================================
    
    def get_next_sim_id(self):
        """
        R code reference (main_r-code.R lines 226-228):
        
        get_next_sim_id <- function() {
          return(current_sim_row)
        }
        
        Note: In Python, we use 0-based indexing. The counter value IS the next row index.
        In R, current_sim_row starts at 1, in Python it starts at 0.
        """
        return self.df.current_sim_row
    
    def get_next_des_id(self):
        """
        R code reference (main_r-code.R lines 230-232):
        
        get_next_des_id <- function() {
          return(current_des_row)
        }
        
        Note: In Python, we use 0-based indexing. The counter value IS the next row index.
        """
        return self.df.current_des_row
    
    # ==========================================================================
    # HELPER FUNCTIONS: ADD EVENTS TO PRE-ALLOCATED DATAFRAMES
    # ==========================================================================
    
    def add_sim_event(self, sim_id, part_id, desone_id, acone_id, micap,
                      fleet_duration, condition_f_duration, depot_duration,
                      condition_a_duration, install_duration,
                      fleet_start, fleet_end,
                      condition_f_start, condition_f_end,
                      depot_start, depot_end,
                      destwo_id, actwo_id,
                      condition_a_start, condition_a_end,
                      install_start, install_end,
                      cycle, condemn):
        """
        R code reference (main_r-code.R lines 163-200):
        
        add_sim_event <- function(df, row_idx, 
                                  sim_id, part_id, desone_id, acone_id, micap,
                                  fleet_duration, condition_f_duration, depot_duration,
                                  condition_a_duration, install_duration,
                                  fleet_start, fleet_end,
                                  condition_f_start, condition_f_end,
                                  depot_start, depot_end,
                                  destwo_id, actwo_id,
                                  condition_a_start, condition_a_end,
                                  install_start, install_end,
                                  cycle, condemn) {
          
          df$sim_id[row_idx] <- sim_id
          df$part_id[row_idx] <- part_id
          ...
          df$condemn[row_idx] <- condemn
          
          return(df)
        }
        
        Python note: We modify the DataFrame in place and increment the counter.
        R uses row_idx parameter, Python uses self.df.current_sim_row directly.
        """
        row_idx = self.df.current_sim_row
        
        # Write all values to the current row using .at[] for scalar assignment
        self.df.sim_df.at[row_idx, 'sim_id'] = sim_id
        self.df.sim_df.at[row_idx, 'part_id'] = part_id
        self.df.sim_df.at[row_idx, 'desone_id'] = desone_id
        self.df.sim_df.at[row_idx, 'acone_id'] = acone_id
        self.df.sim_df.at[row_idx, 'micap'] = micap
        self.df.sim_df.at[row_idx, 'fleet_duration'] = fleet_duration
        self.df.sim_df.at[row_idx, 'condition_f_duration'] = condition_f_duration
        self.df.sim_df.at[row_idx, 'depot_duration'] = depot_duration
        self.df.sim_df.at[row_idx, 'condition_a_duration'] = condition_a_duration
        self.df.sim_df.at[row_idx, 'install_duration'] = install_duration
        self.df.sim_df.at[row_idx, 'fleet_start'] = fleet_start
        self.df.sim_df.at[row_idx, 'fleet_end'] = fleet_end
        self.df.sim_df.at[row_idx, 'condition_f_start'] = condition_f_start
        self.df.sim_df.at[row_idx, 'condition_f_end'] = condition_f_end
        self.df.sim_df.at[row_idx, 'depot_start'] = depot_start
        self.df.sim_df.at[row_idx, 'depot_end'] = depot_end
        self.df.sim_df.at[row_idx, 'destwo_id'] = destwo_id
        self.df.sim_df.at[row_idx, 'actwo_id'] = actwo_id
        self.df.sim_df.at[row_idx, 'condition_a_start'] = condition_a_start
        self.df.sim_df.at[row_idx, 'condition_a_end'] = condition_a_end
        self.df.sim_df.at[row_idx, 'install_start'] = install_start
        self.df.sim_df.at[row_idx, 'install_end'] = install_end
        self.df.sim_df.at[row_idx, 'cycle'] = cycle
        self.df.sim_df.at[row_idx, 'condemn'] = condemn
        
        # Increment the row counter
        self.df.current_sim_row += 1
    
    def add_des_event(self, des_id, ac_id, micap,
                      simone_id, partone_id,
                      fleet_duration, fleet_start, fleet_end,
                      micap_duration, micap_start, micap_end,
                      simtwo_id, parttwo_id,
                      install_duration,
                      install_start,
                      install_end):
        """
        R code reference (main_r-code.R lines 203-223):
        
        add_des_event <- function(df, row_idx,
                                  des_id, ac_id, micap,
                                  simone_id, partone_id,
                                  fleet_duration, fleet_start, fleet_end,
                                  micap_duration, micap_start, micap_end,
                                  simtwo_id, parttwo_id,
                                  install_duration, 
                                  install_start, 
                                  install_end) {
          
          df$des_id[row_idx] <- des_id
          df$ac_id[row_idx] <- ac_id
          ...
          df$install_end[row_idx] <- install_end
          
          return(df)
        }
        
        Python note: We modify the DataFrame in place and increment the counter.
        """
        row_idx = self.df.current_des_row
        
        # Write all values to the current row using .at[] for scalar assignment
        self.df.des_df.at[row_idx, 'des_id'] = des_id
        self.df.des_df.at[row_idx, 'ac_id'] = ac_id
        self.df.des_df.at[row_idx, 'micap'] = micap
        self.df.des_df.at[row_idx, 'simone_id'] = simone_id
        self.df.des_df.at[row_idx, 'partone_id'] = partone_id
        self.df.des_df.at[row_idx, 'fleet_duration'] = fleet_duration
        self.df.des_df.at[row_idx, 'fleet_start'] = fleet_start
        self.df.des_df.at[row_idx, 'fleet_end'] = fleet_end
        self.df.des_df.at[row_idx, 'micap_duration'] = micap_duration
        self.df.des_df.at[row_idx, 'micap_start'] = micap_start
        self.df.des_df.at[row_idx, 'micap_end'] = micap_end
        self.df.des_df.at[row_idx, 'simtwo_id'] = simtwo_id
        self.df.des_df.at[row_idx, 'parttwo_id'] = parttwo_id
        self.df.des_df.at[row_idx, 'install_duration'] = install_duration
        self.df.des_df.at[row_idx, 'install_start'] = install_start
        self.df.des_df.at[row_idx, 'install_end'] = install_end
        
        # Increment the row counter
        self.df.current_des_row += 1
    
    # ==========================================================================
    # HELPER FUNCTION: PROCESS NEW CYCLE STAGES (After Installation Completes)
    # ==========================================================================
    
    def process_new_cycle_stages(self, part_id, ac_id, s4_install_end, 
                                  new_sim_id, new_des_id):
        """
        R code reference (main_r-code.R lines 235-295):
        
        process_new_cycle_stages <- function(sim_df, des_df, part_id, ac_id, 
                                             s4_install_end, new_sim_id, new_des_id,
                                             sone_mean, sone_sd, stwo_mean, stwo_sd, 
                                             sthree_mean, sthree_sd, sim_time) {
          
          sim_row_idx <- new_sim_id
          des_row_idx <- new_des_id
          
          # --- Stage One Calculation (for both part and aircraft) ---
          d1 <- max(0, rnorm(1, mean = sone_mean, sd = sone_sd))
          s1_start <- s4_install_end
          s1_end <- s1_start + d1
          
          # Check sim_time boundary for stage 1
          if (s1_end > sim_time) {
            # Update sim_df - only stage 1 partial info
            sim_df$fleet_duration[sim_row_idx] <- d1
            sim_df$fleet_end[sim_row_idx] <- NA_real_
            
            # Update des_df - only stage 1 partial info
            des_df$fleet_duration[des_row_idx] <- d1
            des_df$fleet_end[des_row_idx] <- NA_real_
            
            return(list(sim_df = sim_df, des_df = des_df))
          }
          
          # Stage 1 fits within sim_time, update both dataframes
          sim_df$fleet_duration[sim_row_idx] <- d1
          sim_df$fleet_end[sim_row_idx] <- s1_end
          
          des_df$fleet_duration[des_row_idx] <- d1
          des_df$fleet_end[des_row_idx] <- s1_end
          
          # --- Stage Two Calculation (part only) ---
          d2 <- max(0, rnorm(1, mean = stwo_mean, sd = stwo_sd))
          s2_start <- s1_end
          s2_end <- s2_start + d2
          
          # Check sim_time boundary for stage 2
          if (s2_end > sim_time) {
            sim_df$condition_f_duration[sim_row_idx] <- d2
            sim_df$condition_f_start[sim_row_idx] <- s2_start
            sim_df$condition_f_end[sim_row_idx] <- NA_real_
            
            return(list(sim_df = sim_df, des_df = des_df))
          }
          
          # Update sim_df with stage 2 info
          sim_df$condition_f_duration[sim_row_idx] <- d2
          sim_df$condition_f_start[sim_row_idx] <- s2_start
          sim_df$condition_f_end[sim_row_idx] <- s2_end
          
          # --- Stage Three Calculation (part only) ---
          d3 <- max(0, rnorm(1, mean = sthree_mean, sd = sthree_sd))
          s3_start <- s2_end
          s3_end <- s3_start + d3
          
          # Check sim_time boundary for stage 3
          if (s3_end > sim_time) {
            sim_df$depot_duration[sim_row_idx] <- d3
            sim_df$depot_start[sim_row_idx] <- s3_start
            sim_df$depot_end[sim_row_idx] <- NA_real_
            
            return(list(sim_df = sim_df, des_df = des_df))
          }
          
          # Update sim_df with stage 3 info
          sim_df$depot_duration[sim_row_idx] <- d3
          sim_df$depot_start[sim_row_idx] <- s3_start
          sim_df$depot_end[sim_row_idx] <- s3_end
          
          return(list(sim_df = sim_df, des_df = des_df))
        }
        
        Python note: We modify DataFrames directly through self.df instead of returning them.
        Row indices (new_sim_id, new_des_id) are already pointing to correct pre-allocated rows.
        """
        # Get row indices (in Python these ARE the row indices, not IDs)
        sim_row_idx = new_sim_id
        des_row_idx = new_des_id
        
        # --- Stage One Calculation (for both part and aircraft) ---
        d1 = self.calculate_fleet_duration()
        s1_start = s4_install_end
        s1_end = s1_start + d1
        
        # Check sim_time boundary for stage 1
        if s1_end > self.sim_time:
            # Update sim_df - only stage 1 partial info
            self.df.sim_df.at[sim_row_idx, 'fleet_duration'] = d1
            self.df.sim_df.at[sim_row_idx, 'fleet_end'] = np.nan
            
            # Update des_df - only stage 1 partial info
            self.df.des_df.at[des_row_idx, 'fleet_duration'] = d1
            self.df.des_df.at[des_row_idx, 'fleet_end'] = np.nan
            
            return  # Early exit - simulation time exceeded
        
        # Stage 1 fits within sim_time, update both dataframes
        self.df.sim_df.at[sim_row_idx, 'fleet_duration'] = d1
        self.df.sim_df.at[sim_row_idx, 'fleet_end'] = s1_end
        
        self.df.des_df.at[des_row_idx, 'fleet_duration'] = d1
        self.df.des_df.at[des_row_idx, 'fleet_end'] = s1_end
        
        # --- Stage Two Calculation (part only) ---
        d2 = self.calculate_condition_f_duration()
        s2_start = s1_end
        s2_end = s2_start + d2
        
        # Check sim_time boundary for stage 2
        if s2_end > self.sim_time:
            self.df.sim_df.at[sim_row_idx, 'condition_f_duration'] = d2
            self.df.sim_df.at[sim_row_idx, 'condition_f_start'] = s2_start
            self.df.sim_df.at[sim_row_idx, 'condition_f_end'] = np.nan
            
            return  # Early exit - simulation time exceeded
        
        # Update sim_df with stage 2 info
        self.df.sim_df.at[sim_row_idx, 'condition_f_duration'] = d2
        self.df.sim_df.at[sim_row_idx, 'condition_f_start'] = s2_start
        self.df.sim_df.at[sim_row_idx, 'condition_f_end'] = s2_end
        
        # --- Stage Three Calculation (part only) ---
        d3 = self.calculate_depot_duration()
        s3_start = s2_end
        s3_end = s3_start + d3
        
        # Check sim_time boundary for stage 3
        if s3_end > self.sim_time:
            self.df.sim_df.at[sim_row_idx, 'depot_duration'] = d3
            self.df.sim_df.at[sim_row_idx, 'depot_start'] = s3_start
            self.df.sim_df.at[sim_row_idx, 'depot_end'] = np.nan
            
            return  # Early exit - simulation time exceeded
        
        # Update sim_df with stage 3 info
        self.df.sim_df.at[sim_row_idx, 'depot_duration'] = d3
        self.df.sim_df.at[sim_row_idx, 'depot_start'] = s3_start
        self.df.sim_df.at[sim_row_idx, 'depot_end'] = s3_end
    
    # ==========================================================================
    # STUB METHODS FOR PARTS 3-4 (Implementation in later parts)
    # ==========================================================================
    
    def initialize_first_cycle(self):
        """
        R code reference (main_r-code.R lines 297-343):
        
        for (row_idx in seq_len(nrow(aircraft_df))) {
          ac_row <- aircraft_df[row_idx, ]
          
          d1 <- max(0, rnorm(1, mean = sone_mean, sd = sone_sd))
          
          s1_start <- 1
          s1_end   <- s1_start + d1
          
          # Add to sim_df using helper function
          sim_df <- add_sim_event(
            sim_df, current_sim_row,
            sim_id = ac_row$sim_id,
            part_id = ac_row$part_id,
            desone_id = ac_row$des_id,
            acone_id = ac_row$ac_id,
            micap = ac_row$micap,
            fleet_duration = d1,
            condition_f_duration = NA_real_,
            depot_duration = NA_real_,
            condition_a_duration = NA_real_,
            install_duration = NA_real_,
            fleet_start = s1_start,
            fleet_end = s1_end,
            condition_f_start = NA_real_,
            condition_f_end = NA_real_,
            depot_start = NA_real_,
            depot_end = NA_real_,
            destwo_id = NA_integer_,
            actwo_id = NA_integer_,
            condition_a_start = NA_real_,
            condition_a_end = NA_real_,
            install_start = NA_real_,
            install_end = NA_real_,
            cycle = 1L,
            condemn = "no"
          )
          current_sim_row <- current_sim_row + 1
          
          # Add to des_df using helper function
          des_df <- add_des_event(
            des_df, current_des_row,
            des_id = ac_row$des_id,
            ac_id = ac_row$ac_id,
            micap = ac_row$micap,
            simone_id = ac_row$sim_id,
            partone_id = ac_row$part_id,
            fleet_duration = d1,
            fleet_start = s1_start,
            fleet_end = s1_end,
            micap_duration = NA_real_,
            micap_start = NA_real_,
            micap_end = NA_real_,
            simtwo_id = NA_integer_,
            parttwo_id = NA_integer_,
            install_duration = NA_real_,
            install_start = NA_real_,
            install_end = NA_real_
          )
          current_des_row <- current_des_row + 1
        }
        
        Initializes stage 1 for all aircraft-part pairs.
        Loop through aircraft_df and create initial part-aircraft pairings.
        """
        # Loop through each aircraft in aircraft_df
        for row_idx in range(len(self.df.aircraft_df)):
            # Extract aircraft row data
            ac_row = self.df.aircraft_df.iloc[row_idx]
            
            # Calculate stage 1 duration
            d1 = self.calculate_fleet_duration()
            
            # Timing calculations
            s1_start = 1.0  # Simulation starts at day 1
            s1_end = s1_start + d1
            
            # Add to sim_df using helper function
            # Note: ac_row['sim_id'] and ac_row['des_id'] are the IDs from aircraft_df
            self.add_sim_event(
                sim_id=ac_row['sim_id'],
                part_id=ac_row['part_id'],
                desone_id=ac_row['des_id'],  # Foreign key to des_df
                acone_id=ac_row['ac_id'],
                micap=ac_row['micap'],
                fleet_duration=d1,
                condition_f_duration=np.nan,
                depot_duration=np.nan,
                condition_a_duration=np.nan,
                install_duration=np.nan,
                fleet_start=s1_start,
                fleet_end=s1_end,
                condition_f_start=np.nan,
                condition_f_end=np.nan,
                depot_start=np.nan,
                depot_end=np.nan,
                destwo_id=None,
                actwo_id=None,
                condition_a_start=np.nan,
                condition_a_end=np.nan,
                install_start=np.nan,
                install_end=np.nan,
                cycle=1,
                condemn="no"
            )
            # Note: add_sim_event increments current_sim_row automatically
            
            # Add to des_df using helper function
            self.add_des_event(
                des_id=ac_row['des_id'],
                ac_id=ac_row['ac_id'],
                micap=ac_row['micap'],
                simone_id=ac_row['sim_id'],  # Foreign key to sim_df
                partone_id=ac_row['part_id'],
                fleet_duration=d1,
                fleet_start=s1_start,
                fleet_end=s1_end,
                micap_duration=np.nan,
                micap_start=np.nan,
                micap_end=np.nan,
                simtwo_id=None,
                parttwo_id=None,
                install_duration=np.nan,
                install_start=np.nan,
                install_end=np.nan
            )
            # Note: add_des_event increments current_des_row automatically
    
    def initialize_stages_two_three(self):
        """
        R code reference (main_r-code.R lines 347-374):
        
        for (i in aircraft_df$part_id) {
          
          # Get Stage One info - find row where this part_id is in cycle 1
          part_record_idx <- which(sim_df$part_id[1:(current_sim_row-1)] == i & 
                                     sim_df$cycle[1:(current_sim_row-1)] == 1)
          
          # --- Stage Two ---
          d2 <- max(0, rnorm(1, mean = stwo_mean, sd = stwo_sd))
          s2_start <- sim_df$fleet_end[part_record_idx]
          s2_end   <- s2_start + d2
          
          # --- Stage Three ---
          d3 <- max(0, rnorm(1, mean = sthree_mean, sd = sthree_sd))
          s3_start <- s2_end
          s3_end   <- s3_start + d3
          
          # Update sim_df using direct assignment
          sim_df$condition_f_duration[part_record_idx] <- d2
          sim_df$condition_f_start[part_record_idx] <- s2_start
          sim_df$condition_f_end[part_record_idx] <- s2_end
          sim_df$depot_duration[part_record_idx] <- d3
          sim_df$depot_start[part_record_idx] <- s3_start
          sim_df$depot_end[part_record_idx] <- s3_end
        }
        
        Calculates stages 2-3 for all cycle 1 parts.
        Updates existing sim_df rows (no new rows created).
        """
        # Loop through all part_ids in aircraft_df
        for part_id in self.df.aircraft_df['part_id']:
            
            # Find the sim_df row where this part_id is in cycle 1
            # Only search filled rows (up to current_sim_row)
            filled_sim_df = self.df.sim_df.iloc[:self.df.current_sim_row]
            
            # Find row index where part_id matches AND cycle == 1
            mask = (filled_sim_df['part_id'] == part_id) & (filled_sim_df['cycle'] == 1)
            part_record_idx = filled_sim_df[mask].index[0]
            
            # --- Stage Two ---
            d2 = self.calculate_condition_f_duration()
            s2_start = self.df.sim_df.at[part_record_idx, 'fleet_end']
            s2_end = s2_start + d2
            
            # --- Stage Three ---
            d3 = self.calculate_depot_duration()
            s3_start = s2_end
            s3_end = s3_start + d3
            
            # Update sim_df using direct assignment (not add_sim_event)
            self.df.sim_df.at[part_record_idx, 'condition_f_duration'] = d2
            self.df.sim_df.at[part_record_idx, 'condition_f_start'] = s2_start
            self.df.sim_df.at[part_record_idx, 'condition_f_end'] = s2_end
            self.df.sim_df.at[part_record_idx, 'depot_duration'] = d3
            self.df.sim_df.at[part_record_idx, 'depot_start'] = s3_start
            self.df.sim_df.at[part_record_idx, 'depot_end'] = s3_end
    
    def build_event_index(self, period):
        """
        R code reference (main_r-code.R lines 386-406):
        
        # Get parts finishing stage 3 this period
        matching_parts <- sim_df |> 
          dplyr::filter(round(depot_end) == p)
        
        # Get aircraft finishing stage 1 this period
        matching_aircraft <- des_df |> 
          dplyr::filter(round(fleet_end) == p)
        
        # Build index - just the times and IDs
        parts_index <- if (nrow(matching_parts) > 0) {
          matching_parts |>
            dplyr::select(sim_id, part_id, time = depot_end)
        } else {
          tibble::tibble(sim_id = integer(), part_id = integer(), time = numeric())
        }
        
        aircraft_index <- if (nrow(matching_aircraft) > 0) {
          matching_aircraft |>
            dplyr::select(des_id, ac_id, time = fleet_end)
        } else {
          tibble::tibble(des_id = integer(), ac_id = integer(), time = numeric())
        }
        
        # Combine and sort by time
        index <- dplyr::bind_rows(parts_index, aircraft_index) |>
          dplyr::arrange(time)
        
        Builds chronological event index for this period.
        Returns DataFrame with events sorted by time.
        """
        # Get filled rows only
        filled_sim_df = self.df.sim_df.iloc[:self.df.current_sim_row]
        filled_des_df = self.df.des_df.iloc[:self.df.current_des_row]
        
        # Find parts finishing stage 3 this period
        matching_parts = filled_sim_df[
            filled_sim_df['depot_end'].round() == period
        ].copy()
        
        # Find aircraft finishing stage 1 this period
        matching_aircraft = filled_des_df[
            filled_des_df['fleet_end'].round() == period
        ].copy()
        
        # Build parts index
        if len(matching_parts) > 0:
            parts_index = matching_parts[['sim_id', 'part_id']].copy()
            parts_index['time'] = matching_parts['depot_end']
        else:
            parts_index = pd.DataFrame({
                'sim_id': pd.Series(dtype='Int64'),
                'part_id': pd.Series(dtype='Int64'),
                'time': pd.Series(dtype='float64')
            })
        
        # Build aircraft index
        if len(matching_aircraft) > 0:
            aircraft_index = matching_aircraft[['des_id', 'ac_id']].copy()
            aircraft_index['time'] = matching_aircraft['fleet_end']
        else:
            aircraft_index = pd.DataFrame({
                'des_id': pd.Series(dtype='Int64'),
                'ac_id': pd.Series(dtype='Int64'),
                'time': pd.Series(dtype='float64')
            })
        
        # Combine and sort by time
        index = pd.concat([parts_index, aircraft_index], ignore_index=True)
        index = index.sort_values('time').reset_index(drop=True)
        
        return index
    
    def handle_part_completes_stage_three(self, sim_id):
        """
        R code reference (main_r-code.R lines 411-534):
        
        EVENT TYPE A: Part Completes Stage 3
        
        part_row <- sim_df |> 
          dplyr::filter(sim_id == sim_i) |>
          dplyr::slice(1)
        
        s3_end <- part_row$depot_end
        
        # Check if any aircraft currently in MICAP
        micap_aircraft <- micap_df |> 
          dplyr::filter(is.na(micap_end))
        
        n_micap <- nrow(micap_aircraft)
        
        # CASE A1: No MICAP aircraft → Part goes to available inventory
        if (n_micap == 0) {
          # Add to condition_a_df
          condition_a_df <- condition_a_df |>
            dplyr::add_row(...)
        
        # CASE A2: MICAP aircraft exists → Install part directly
        } else { 
          first_micap <- micap_aircraft |> 
            dplyr::arrange(micap_start) |>
            dplyr::slice(1)
          
          d4_install <- max(0, rnorm(1, mean = sfour_mean, sd = sfour_sd))
          s4_install_start <- s3_end
          s4_install_end <- s4_install_start + d4_install
          
          micap_duration_ <- s3_end - first_micap$micap_start
          micap_end_ <- s3_end
          
          # Update existing sim_df row
          # Add new sim_df row for cycle restart
          # Update des_df for MICAP resolution
          # Add new des_df row for cycle restart
          # Call process_new_cycle_stages
          # Remove resolved MICAP from micap_df
        }
        
        Event Type A: Part completes stage 3 and becomes available.
        """
        # Get part details
        filled_sim_df = self.df.sim_df.iloc[:self.df.current_sim_row]
        part_row = filled_sim_df[filled_sim_df['sim_id'] == sim_id].iloc[0]
        
        s3_end = part_row['depot_end']
        
        # Check if any aircraft in MICAP
        micap_aircraft = self.df.micap_df[self.df.micap_df['micap_end'].isna()].copy()
        n_micap = len(micap_aircraft)
        
        # CASE A1: No MICAP aircraft → Part goes to Condition A
        if n_micap == 0:
            # Create new row for condition_a_df
            new_row = pd.DataFrame([{
                'sim_id': part_row['sim_id'],
                'part_id': part_row['part_id'],
                'desone_id': part_row['desone_id'],
                'acone_id': part_row['acone_id'],
                'micap': part_row['micap'],
                'fleet_duration': part_row['fleet_duration'],
                'condition_f_duration': part_row['condition_f_duration'],
                'depot_duration': part_row['depot_duration'],
                'condition_a_duration': np.nan,
                'install_duration': np.nan,
                'fleet_start': part_row['fleet_start'],
                'fleet_end': part_row['fleet_end'],
                'condition_f_start': part_row['condition_f_start'],
                'condition_f_end': part_row['condition_f_end'],
                'depot_start': part_row['depot_start'],
                'depot_end': part_row['depot_end'],
                'destwo_id': None,
                'actwo_id': None,
                'condition_a_start': s3_end,
                'condition_a_end': np.nan,
                'install_start': np.nan,
                'install_end': np.nan,
                'cycle': 1,
                'condemn': 'no'
            }])
            self.df.condition_a_df = pd.concat(
                [self.df.condition_a_df, new_row], 
                ignore_index=True
            )
        
        # CASE A2: MICAP aircraft exists → Install part directly
        else:
            # Get first MICAP aircraft (earliest micap_start)
            first_micap = micap_aircraft.sort_values('micap_start').iloc[0]
            
            # Calculate install duration
            d4_install = self.calculate_install_duration()
            s4_install_start = s3_end
            s4_install_end = s4_install_start + d4_install
            
            micap_duration_ = s3_end - first_micap['micap_start']
            micap_end_ = s3_end
            
            # Update existing sim_df row for this part (install info)
            part_row_idx = filled_sim_df[filled_sim_df['sim_id'] == sim_id].index[0]
            self.df.sim_df.at[part_row_idx, 'condition_a_duration'] = np.nan
            self.df.sim_df.at[part_row_idx, 'install_duration'] = d4_install
            self.df.sim_df.at[part_row_idx, 'destwo_id'] = first_micap['des_id']
            self.df.sim_df.at[part_row_idx, 'actwo_id'] = first_micap['ac_id']
            self.df.sim_df.at[part_row_idx, 'condition_a_start'] = np.nan
            self.df.sim_df.at[part_row_idx, 'condition_a_end'] = np.nan
            self.df.sim_df.at[part_row_idx, 'install_start'] = s4_install_start
            self.df.sim_df.at[part_row_idx, 'install_end'] = s4_install_end
            
            # Generate IDs for new cycle
            new_sim_id = self.get_next_sim_id()
            new_des_id = self.get_next_des_id()
            
            # Add new row to sim_df for cycle restart
            self.add_sim_event(
                sim_id=new_sim_id,
                part_id=part_row['part_id'],
                desone_id=new_des_id,
                acone_id=first_micap['ac_id'],
                micap="no",
                fleet_duration=np.nan,
                condition_f_duration=np.nan,
                depot_duration=np.nan,
                condition_a_duration=np.nan,
                install_duration=np.nan,
                fleet_start=s4_install_end,
                fleet_end=np.nan,
                condition_f_start=np.nan,
                condition_f_end=np.nan,
                depot_start=np.nan,
                depot_end=np.nan,
                destwo_id=None,
                actwo_id=None,
                condition_a_start=np.nan,
                condition_a_end=np.nan,
                install_start=np.nan,
                install_end=np.nan,
                cycle=part_row['cycle'] + 1,
                condemn="no"
            )
            
            # Update des_df for MICAP resolution
            filled_des_df = self.df.des_df.iloc[:self.df.current_des_row]
            micap_des_row_idx = filled_des_df[filled_des_df['des_id'] == first_micap['des_id']].index[0]
            self.df.des_df.at[micap_des_row_idx, 'micap_duration'] = micap_duration_
            self.df.des_df.at[micap_des_row_idx, 'micap_start'] = first_micap['micap_start']
            self.df.des_df.at[micap_des_row_idx, 'micap_end'] = micap_end_
            self.df.des_df.at[micap_des_row_idx, 'simtwo_id'] = part_row['sim_id']
            self.df.des_df.at[micap_des_row_idx, 'parttwo_id'] = part_row['part_id']
            self.df.des_df.at[micap_des_row_idx, 'install_duration'] = d4_install
            self.df.des_df.at[micap_des_row_idx, 'install_start'] = s4_install_start
            self.df.des_df.at[micap_des_row_idx, 'install_end'] = s4_install_end
            
            # Add new row to des_df for cycle restart
            self.add_des_event(
                des_id=new_des_id,
                ac_id=first_micap['ac_id'],
                micap="no",
                simone_id=new_sim_id,
                partone_id=part_row['part_id'],
                fleet_duration=np.nan,
                fleet_start=s4_install_end,
                fleet_end=np.nan,
                micap_duration=np.nan,
                micap_start=np.nan,
                micap_end=np.nan,
                simtwo_id=None,
                parttwo_id=None,
                install_duration=np.nan,
                install_start=np.nan,
                install_end=np.nan
            )
            
            # Process stages 1-3 for the new cycle
            self.process_new_cycle_stages(
                part_id=part_row['part_id'],
                ac_id=first_micap['ac_id'],
                s4_install_end=s4_install_end,
                new_sim_id=new_sim_id,
                new_des_id=new_des_id
            )
            
            # Remove resolved MICAP from micap_df
            self.df.micap_df = self.df.micap_df[
                self.df.micap_df['des_id'] != first_micap['des_id']
            ].reset_index(drop=True)
    
    def handle_aircraft_needs_part(self, des_id):
        """
        R code reference (main_r-code.R lines 539-752):
        
        EVENT TYPE B: Aircraft Completes Stage 1
        
        ac_row <- des_df |>
          dplyr::filter(des_id == des_i) |>
          dplyr::slice(1)
        
        s1_end <- ac_row$fleet_end
        old_part_id <- ac_row$partone_id
        
        # CHECK: Are there available parts?
        available_parts <- condition_a_df |>
          dplyr::filter(!is.na(part_id))
        
        n_available <- nrow(available_parts)
        
        # CASE B1: Part Available
        if (n_available > 0) {
          first_available <- available_parts |> 
            dplyr::arrange(condition_a_start, part_id) |>
            dplyr::slice(1)
          
          d4_install <- max(0, rnorm(1, mean = sfour_mean, sd = sfour_sd))
          s4_install_start <- s1_end
          s4_install_end <- s4_install_start + d4_install
          
          condition_a_end <- s4_install_start
          condition_a_duration <- condition_a_end - first_available$condition_a_start
          
          has_sim_id <- !is.na(first_available$sim_id)
          
          if (has_sim_id) {
            # Part went through stages 1-3, MUTATE existing row
            # Add new row to sim_df for cycle restart
            # Update des_df
            # Add new row to des_df for cycle restart
            # Call process_new_cycle_stages
          } else {
            # Part started in available inventory, ADD NEW row
            # Add ANOTHER row for cycle restart
            # Update des_df
            # Add new row to des_df for cycle restart
            # Call process_new_cycle_stages
          }
          
          # Remove allocated part from available inventory
          condition_a_df <- condition_a_df |>
            dplyr::filter(part_id != first_available$part_id)
        
        # CASE B2: No Parts Available → Aircraft Goes MICAP
        } else {
          micap_start_time <- s1_end
          
          # Log to micap_df
          micap_df <- micap_df |>
            dplyr::add_row(...)
        }
        
        Event Type B: Aircraft completes stage 1 and needs replacement part.
        """
        # Get aircraft details
        filled_des_df = self.df.des_df.iloc[:self.df.current_des_row]
        ac_row = filled_des_df[filled_des_df['des_id'] == des_id].iloc[0]
        
        s1_end = ac_row['fleet_end']
        old_part_id = ac_row['partone_id']
        
        # Check if available parts exist
        available_parts = self.df.condition_a_df[
            self.df.condition_a_df['part_id'].notna()
        ].copy()
        
        n_available = len(available_parts)
        
        # CASE B1: Part Available
        if n_available > 0:
            # Get first available part (earliest condition_a_start, then lowest part_id)
            first_available = available_parts.sort_values(
                ['condition_a_start', 'part_id']
            ).iloc[0]
            
            # Calculate install duration
            d4_install = self.calculate_install_duration()
            s4_install_start = s1_end
            s4_install_end = s4_install_start + d4_install
            
            condition_a_end = s4_install_start
            condition_a_duration = (
                condition_a_end - first_available['condition_a_start']
            )
            
            # Check if this part has a previous sim_id (went through stages 1-3)
            has_sim_id = pd.notna(first_available['sim_id'])
            
            if has_sim_id:
                # Part went through stages 1-3, MUTATE existing row in sim_df
                filled_sim_df = self.df.sim_df.iloc[:self.df.current_sim_row]
                part_row_idx = filled_sim_df[
                    filled_sim_df['sim_id'] == first_available['sim_id']
                ].index[0]
                
                self.df.sim_df.at[part_row_idx, 'condition_a_duration'] = condition_a_duration
                self.df.sim_df.at[part_row_idx, 'install_duration'] = d4_install
                self.df.sim_df.at[part_row_idx, 'destwo_id'] = des_id
                self.df.sim_df.at[part_row_idx, 'actwo_id'] = ac_row['ac_id']
                self.df.sim_df.at[part_row_idx, 'condition_a_start'] = first_available['condition_a_start']
                self.df.sim_df.at[part_row_idx, 'condition_a_end'] = condition_a_end
                self.df.sim_df.at[part_row_idx, 'install_start'] = s4_install_start
                self.df.sim_df.at[part_row_idx, 'install_end'] = s4_install_end
                
                # Generate IDs for cycle restart
                new_sim_id = self.get_next_sim_id()
                new_des_id = self.get_next_des_id()
                
                # Add new row to sim_df for cycle restart
                self.add_sim_event(
                    sim_id=new_sim_id,
                    part_id=first_available['part_id'],
                    desone_id=new_des_id,
                    acone_id=ac_row['ac_id'],
                    micap="no",
                    fleet_duration=np.nan,
                    condition_f_duration=np.nan,
                    depot_duration=np.nan,
                    condition_a_duration=np.nan,
                    install_duration=np.nan,
                    fleet_start=s4_install_end,
                    fleet_end=np.nan,
                    condition_f_start=np.nan,
                    condition_f_end=np.nan,
                    depot_start=np.nan,
                    depot_end=np.nan,
                    destwo_id=None,
                    actwo_id=None,
                    condition_a_start=np.nan,
                    condition_a_end=np.nan,
                    install_start=np.nan,
                    install_end=np.nan,
                    cycle=first_available['cycle'] + 1,
                    condemn="no"
                )
                
                # Update des_df
                ac_row_idx = filled_des_df[filled_des_df['des_id'] == des_id].index[0]
                self.df.des_df.at[ac_row_idx, 'simtwo_id'] = first_available['sim_id']
                self.df.des_df.at[ac_row_idx, 'parttwo_id'] = first_available['part_id']
                self.df.des_df.at[ac_row_idx, 'install_duration'] = d4_install
                self.df.des_df.at[ac_row_idx, 'install_start'] = s4_install_start
                self.df.des_df.at[ac_row_idx, 'install_end'] = s4_install_end
                
                # Add new row to des_df for cycle restart
                self.add_des_event(
                    des_id=new_des_id,
                    ac_id=ac_row['ac_id'],
                    micap="no",
                    simone_id=new_sim_id,
                    partone_id=first_available['part_id'],
                    fleet_duration=np.nan,
                    fleet_start=s4_install_end,
                    fleet_end=np.nan,
                    micap_duration=np.nan,
                    micap_start=np.nan,
                    micap_end=np.nan,
                    simtwo_id=None,
                    parttwo_id=None,
                    install_duration=np.nan,
                    install_start=np.nan,
                    install_end=np.nan
                )
                
                # Process stages 1-3 for the new cycle
                self.process_new_cycle_stages(
                    part_id=first_available['part_id'],
                    ac_id=ac_row['ac_id'],
                    s4_install_end=s4_install_end,
                    new_sim_id=new_sim_id,
                    new_des_id=new_des_id
                )
                
            else:
                # Part started in available inventory, ADD NEW row to sim_df
                new_sim_id = self.get_next_sim_id()
                
                self.add_sim_event(
                    sim_id=new_sim_id,
                    part_id=first_available['part_id'],
                    desone_id=None,
                    acone_id=None,
                    micap="no",
                    fleet_duration=np.nan,
                    condition_f_duration=np.nan,
                    depot_duration=np.nan,
                    condition_a_duration=condition_a_duration,
                    install_duration=d4_install,
                    fleet_start=np.nan,
                    fleet_end=np.nan,
                    condition_f_start=np.nan,
                    condition_f_end=np.nan,
                    depot_start=np.nan,
                    depot_end=np.nan,
                    destwo_id=des_id,
                    actwo_id=ac_row['ac_id'],
                    condition_a_start=first_available['condition_a_start'],
                    condition_a_end=condition_a_end,
                    install_start=s4_install_start,
                    install_end=s4_install_end,
                    cycle=0,
                    condemn="no"
                )
                
                # Generate IDs for cycle restart
                new_sim_id_restart = self.get_next_sim_id()
                new_des_id_restart = self.get_next_des_id()
                
                # Add ANOTHER row to sim_df for cycle restart
                self.add_sim_event(
                    sim_id=new_sim_id_restart,
                    part_id=first_available['part_id'],
                    desone_id=new_des_id_restart,
                    acone_id=ac_row['ac_id'],
                    micap="no",
                    fleet_duration=np.nan,
                    condition_f_duration=np.nan,
                    depot_duration=np.nan,
                    condition_a_duration=np.nan,
                    install_duration=np.nan,
                    fleet_start=s4_install_end,
                    fleet_end=np.nan,
                    condition_f_start=np.nan,
                    condition_f_end=np.nan,
                    depot_start=np.nan,
                    depot_end=np.nan,
                    destwo_id=None,
                    actwo_id=None,
                    condition_a_start=np.nan,
                    condition_a_end=np.nan,
                    install_start=np.nan,
                    install_end=np.nan,
                    cycle=1,
                    condemn="no"
                )
                
                # Update des_df
                ac_row_idx = filled_des_df[filled_des_df['des_id'] == des_id].index[0]
                self.df.des_df.at[ac_row_idx, 'simtwo_id'] = new_sim_id
                self.df.des_df.at[ac_row_idx, 'parttwo_id'] = first_available['part_id']
                self.df.des_df.at[ac_row_idx, 'install_duration'] = d4_install
                self.df.des_df.at[ac_row_idx, 'install_start'] = s4_install_start
                self.df.des_df.at[ac_row_idx, 'install_end'] = s4_install_end
                
                # Add new row to des_df for cycle restart
                self.add_des_event(
                    des_id=new_des_id_restart,
                    ac_id=ac_row['ac_id'],
                    micap="no",
                    simone_id=new_sim_id_restart,
                    partone_id=first_available['part_id'],
                    fleet_duration=np.nan,
                    fleet_start=s4_install_end,
                    fleet_end=np.nan,
                    micap_duration=np.nan,
                    micap_start=np.nan,
                    micap_end=np.nan,
                    simtwo_id=None,
                    parttwo_id=None,
                    install_duration=np.nan,
                    install_start=np.nan,
                    install_end=np.nan
                )
                
                # Process stages 1-3 for the new cycle
                self.process_new_cycle_stages(
                    part_id=first_available['part_id'],
                    ac_id=ac_row['ac_id'],
                    s4_install_end=s4_install_end,
                    new_sim_id=new_sim_id_restart,
                    new_des_id=new_des_id_restart
                )
            
            # Remove allocated part from available inventory
            self.df.condition_a_df = self.df.condition_a_df[
                self.df.condition_a_df['part_id'] != first_available['part_id']
            ].reset_index(drop=True)
        
        # CASE B2: No Parts Available → Aircraft Goes MICAP
        else:
            micap_start_time = s1_end
            
            # Log to micap_df
            new_micap = pd.DataFrame([{
                'des_id': des_id,
                'ac_id': ac_row['ac_id'],
                'micap': 'yes',
                'fleet_duration': ac_row['fleet_duration'],
                'fleet_start': ac_row['fleet_start'],
                'fleet_end': ac_row['fleet_end'],
                'micap_duration': np.nan,
                'micap_start': micap_start_time,
                'micap_end': np.nan
            }])
            self.df.micap_df = pd.concat(
                [self.df.micap_df, new_micap],
                ignore_index=True
            )
    
    def run(self):
        """
        R code reference (main_r-code.R lines 298-299, 347-348, 379-753):
        
        # Initialize first cycle (lines 298-343)
        for (row_idx in seq_len(nrow(aircraft_df))) {
          ...
        }
        
        # Initialize stages 2-3 (lines 347-374)
        for (i in aircraft_df$part_id) {
          ...
        }
        
        # Main simulation loop (lines 379-753)
        for (p in params_df$period) {
          
          # Build event index
          index <- ...
          
          # Process each event in chronological order
          for (event_idx in seq_len(nrow(index))) {
            
            if (!is.na(index$sim_id[event_idx])) {
              # EVENT TYPE A: Part Completes Stage 3
              ...
            } else if (!is.na(index$des_id[event_idx])) {
              # EVENT TYPE B: Aircraft Completes Stage 1
              ...
            }
          }
        }
        
        # Post-processing (lines 755-774)
        sim_df <- sim_df[1:(current_sim_row - 1), ]
        des_df <- des_df[1:(current_des_row - 1), ]
        
        Main simulation loop.
        """
        # Step 1: Initialize first cycle
        self.initialize_first_cycle()
        
        # Step 2: Initialize stages 2-3
        self.initialize_stages_two_three()
        
        # Step 3: Main simulation loop
        for period in range(1, self.sim_time + 1):
            
            # Build chronological event index for this period
            index = self.build_event_index(period)
            
            # Process each event in chronological order
            for event_idx in range(len(index)):
                event = index.iloc[event_idx]
                
                # Check if this is a part event or aircraft event
                if pd.notna(event.get('sim_id')):
                    # EVENT TYPE A: Part Completes Stage 3
                    self.handle_part_completes_stage_three(event['sim_id'])
                    
                elif pd.notna(event.get('des_id')):
                    # EVENT TYPE B: Aircraft Completes Stage 1
                    self.handle_aircraft_needs_part(event['des_id'])
        
        # Step 4: Post-processing
        self.df.trim_dataframes()
        validation_results = self.df.validate_structure()
        
        return validation_results
