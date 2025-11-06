"""
Simulation Engine for Hill AFB DES Simulation

Handles simulation logic, formulas, and event processing.
Works with DataFrameManager to access and update DataFrames.

All logic matches main_r-code.R exactly.
"""

import numpy as np
import pandas as pd
import heapq

class SimulationEngine:
    """
    Manages simulation logic and event processing.
    
    Works with DataFrameManager for DataFrame access and updates.
    Contains formulas for stage durations and helper functions for event management.
    """
    
    def __init__(self, df_manager, sone_mean, sone_sd,
                 sthree_mean, sthree_sd, sim_time, depot_capacity,condemn_cycle, condemn_depot_fraction,  part_order_lag):
        """
        Initialize SimulationEngine with DataFrameManager and stage parameters.
        
        Parameters from main_r-code.R lines 1-11.
        
        Args:
            df_manager: DataFrameManager instance with all DataFrames
            sone_mean, sone_sd: Fleet normal distribution parameters
            stwo_mean, stwo_sd: Condition F normal distribution parameters
            sthree_mean, sthree_sd: Depot normal distribution parameters
            sfour_mean, sfour_sd: Condition A (Install) normal distribution parameters
            sim_time: Total simulation time
        'depot_capacity': depot_capacity,
        'condemn_cycle': condemn_cycle,  # new params for depot logic
        'condemn_depot_fraction': condemn_depot_fraction, # new params for depot logic
        'part_order_lag': part_order_lag, # new params for depot logic
        """
        self.df = df_manager
        self.sone_mean = sone_mean
        self.sone_sd = sone_sd
        self.sthree_mean = sthree_mean
        self.sthree_sd = sthree_sd
        self.sim_time = sim_time
        self.active_depot: list = []
        self.depot_capacity: int = depot_capacity # adding depot capacity code
        self.condemn_cycle: int = condemn_cycle  # NEW: Store condemn cycle
        self.condemn_depot_fraction: float = condemn_depot_fraction # NEW: Store depot time fraction
        self.part_order_lag: int = part_order_lag  # NEW: Store lag parameter
        
        # NEW: Event-driven structures
        self.event_heap = []  # Priority queue: (time, counter, event_type, entity_id)
        self.event_counter = 0  # FIFO tie-breaker for same-time events
        self.current_time = 0  # Simulation clock
    
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
        Not in use, delete or leave as spacer
        """
        return 0
    
    def calculate_depot_duration(self):
        """
        R code reference (main_r-code.R lines 1-11, example line 350):
        
        d3 <- max(0, rnorm(1, mean = sthree_mean, sd = sthree_sd))
        """
        return max(0, np.random.normal(self.sthree_mean, self.sthree_sd))
    
    def calculate_install_duration(self):
        """
        There is no install duration to account for so leaving old install
        duration formula set to zero. Laving it as a sapcer for possibly other uses. 
        """
        return 0 
    
    # ==========================================================================
    # EVENT SCHEDULING METHODS
    # ==========================================================================
    
    def schedule_event(self, event_time, event_type, entity_id):
        """
        Schedule a future event in the priority queue.
        
        Parameters
        ----------
        event_time : float
            Simulation time when event occurs (e.g., depot_end, fleet_end)
        event_type : str
            One of: 'depot_complete', 'fleet_complete', 'new_part_arrives'
        entity_id : int
            - For 'depot_complete': sim_id from sim_df
            - For 'fleet_complete': des_id from des_df  
            - For 'new_part_arrives': part_id from new_part_df
        
        Notes
        -----
        Uses heapq.heappush() to maintain sorted order by event_time.
        The event_counter ensures FIFO for simultaneous events.
        """
        heapq.heappush(
            self.event_heap,
            (event_time, self.event_counter, event_type, entity_id)
        )
        self.event_counter += 1
    
    # ==========================================================================
    # HELPER FUNCTIONS: ID GENERATION
    # ==========================================================================
    
    def get_next_sim_id(self):
        """
        Return the next available row index for adding an aircraft event to des_df.

        Retrieves the current value of `df_manager.current_des_row`, which tracks how many
        rows in the aircraft event log have been used. The returned index identifies where
        the next event (e.g., Fleet completion, MICAP, or installation) will be written.

        Returns
        -------
        int
            Next available row index in `des_df`.

        Notes
        -----
        Ensures sequential event recording without overwriting data. Used by
        `add_des_event()` and related event-processing methods to maintain index integrity.
        """
        return self.df.current_sim_row
    
    def get_next_des_id(self):
        """
        Return the next available row index for adding an aircraft event to des_df.

        Retrieves the current value of `df_manager.current_des_row`, which tracks how many
        rows in the aircraft event log have been used. The returned index identifies where
        the next event (e.g., Fleet completion, MICAP, or installation) will be written.

        Returns
        -------
        int: Next available row index in `des_df`.

        Notes
        -----
        Ensures sequential event recording without overwriting data. Used by
        `add_des_event()` and related event-processing methods to maintain index integrity.
        """
        return self.df.current_des_row
    
    # ==========================================================================
    # HELPER FUNCTIONS: ADD EVENTS TO DATAFRAMES
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
        Record a new part event in sim_df at the current simulation row index.

        Writes all stage details for a single part's lifecycle event — including fleet,
        condition F, depot, condition A, and installation phases — directly into the
        preallocated part event DataFrame (`sim_df`). Each call appends one complete
        event record for a specific part-cycle combination.

        Notes
        -----
        This function updates one row in `sim_df` using the current value of
        `df_manager.current_sim_row`, then increments the counter to maintain
        sequential event recording. It is used internally by event-processing
        functions such as `initialize_first_cycle()` and `handle_part_completes_depot()`.
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
        Record a new aircraft event in des_df at the current simulation row index.

        Writes all lifecycle details for a single aircraft event — including fleet,
        MICAP, and installation stages — directly into the preallocated aircraft
        event DataFrame (`des_df`). Each call appends one complete event record for
        a specific aircraft-cycle combination.

        Notes
        -----
        Updates one row in `des_df` using the current value of
        `df_manager.current_des_row`, then increments the counter to ensure
        sequential, non-overlapping event recording. Used internally by
        event-handling functions such as `initialize_first_cycle()` and
        `handle_aircraft_needs_part()` to maintain aircraft event integrity.
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
        Advance a part–aircraft pair into the next maintenance cycle.

        Calculates and updates Fleet, Condition F, and Depot stage timings for the
        new cycle, applying depot capacity and condemn logic as needed. Updates both
        `sim_df` and `des_df` records in place.

        Notes
        -----
        Handles cycle continuation after installation events. Detailed logic and
        stage timing formulas need to be documented separately.
        """
        # Get row indices (in Python these ARE the row indices, not IDs)
        sim_row_idx = new_sim_id
        des_row_idx = new_des_id
        
        # --- Stage One Calculation (for both part and aircraft) ---
        d1 = self.calculate_fleet_duration()
        s1_start = s4_install_end
        s1_end = s1_start + d1
        
        # Fleet fits within sim_time, update both dataframes
        self.df.sim_df.at[sim_row_idx, 'fleet_duration'] = d1
        self.df.sim_df.at[sim_row_idx, 'fleet_end'] = s1_end
        
        self.df.des_df.at[des_row_idx, 'fleet_duration'] = d1
        self.df.des_df.at[des_row_idx, 'fleet_end'] = s1_end
        
        # NEW: Schedule fleet completion event for the aircraft (des_df)
        self.schedule_event(s1_end, 'fleet_complete', new_des_id)
        
        # --- Condition F and Depot handling) ---
        if len(self.active_depot) < self.depot_capacity:
            s3_start = s1_end
        else:
            # Wait until the earliest depot slot frees up
            earliest_free = heapq.heappop(self.active_depot)
            s3_start = max(s1_end, earliest_free)
            s2_end = s3_start
        # Condition F calculations
        s2_start = s1_end
        s2_end = s3_start
        d2 = s2_end - s2_start # duration for condition f. Will be zero if depot has capacity
        # depot calculations
        # Get the cycle value for this row
        cycle = self.df.sim_df.at[sim_row_idx, 'cycle']
        
        # Check if cycle = 20 for condemn logic
        if cycle == self.condemn_cycle:
            # Mark part as condemned
            self.df.sim_df.at[sim_row_idx, 'condemn'] = 'yes'
            # Condemned parts take 10% of normal depot time
            d3 = self.calculate_depot_duration() * self.condemn_depot_fraction
            
            # --- Order new part logic ---
            # Calculate depot_end for the condemned part
            depot_end_condemned = s3_start + d3
            
            # Find the row in new_part_df where condition_a_start is empty (np.nan)
            # This will be the row with only part_id populated
            empty_row_mask = self.df.new_part_df['condition_a_start'].isna()
            empty_row_idx = self.df.new_part_df[empty_row_mask].index[0]
            
            # Get the part_id from this row (we'll use it for the new row)
            new_part_id = self.df.new_part_df.at[empty_row_idx, 'part_id']
            
            # Edit this row: set condition_a_start and cycle
            self.df.new_part_df.at[empty_row_idx, 'condition_a_start'] = depot_end_condemned + self.part_order_lag
            self.df.new_part_df.at[empty_row_idx, 'cycle'] = 0
            
            # Add new row with only part_id = previous part_id + 1
            new_row = pd.DataFrame({
                'sim_id': [np.nan],
                'part_id': [new_part_id + 1],  # Increment from the edited row's part_id
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
            self.df.new_part_df = pd.concat([self.df.new_part_df, new_row], 
                                            ignore_index=True)
            
            # NEW: Schedule the new part arrival event
            new_part_arrival_time = depot_end_condemned + self.part_order_lag
            self.schedule_event(new_part_arrival_time, 'new_part_arrives', new_part_id)
            
        else:
            # Normal depot duration
            d3 = self.calculate_depot_duration()
        
        s3_end = s3_start + d3
        heapq.heappush(self.active_depot, s3_end)

        # Update sim_df with Condition F info
        self.df.sim_df.at[sim_row_idx, 'condition_f_duration'] = d2
        self.df.sim_df.at[sim_row_idx, 'condition_f_start'] = s2_start
        self.df.sim_df.at[sim_row_idx, 'condition_f_end'] = s2_end
        
        self.df.sim_df.at[sim_row_idx, 'depot_duration'] = d3
        self.df.sim_df.at[sim_row_idx, 'depot_start'] = s3_start
        self.df.sim_df.at[sim_row_idx, 'depot_end'] = s3_end
        
        # NEW: Schedule depot completion event for the part (sim_df) - ONLY if not condemned
        if cycle != self.condemn_cycle:  # Only schedule if part not condemned
            self.schedule_event(s3_end, 'depot_complete', new_sim_id)
    
    # ==========================================================================
    # STUB METHODS FOR PARTS 3-4 (Implementation in later parts)
    # ==========================================================================
    
    def initialize_first_cycle(self):
        """
        Initialize the first Fleet cycle for all aircraft–part pairs.

        Generates the initial Fleet stage for every aircraft in `aircraft_df`,
        assigning start and end times using Fleet durations. Populates both
        `sim_df` (part events) and `des_df` (aircraft events) to establish baseline
        records before the main simulation loop begins.

        Notes
        -----
        Serves as the simulation warm-up phase, ensuring every aircraft–part pair
        starts with one complete Fleet event. Subsequent stages (Condition F, Depot)
        depend on these initial values.
        """
        # Loop through each aircraft in aircraft_df
        for row_idx in range(len(self.df.aircraft_df)):
            # Extract aircraft row data
            ac_row = self.df.aircraft_df.iloc[row_idx]
            
            # Calculate Fleet duration
            d1 = self.calculate_fleet_duration()
            
            # Timing calculations
            s1_start = self.calculate_fleet_duration()  # So not all aircraft start at sim day 1
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
    
    def initialize_condition_f_depot(self):
        """
        Initialize Condition F and Depot stages for all first-cycle parts.

        Uses Fleet completion times from the initial cycle to compute Condition F
        wait durations and Depot processing times for each part. Applies depot
        capacity constraints to stagger entry times and updates `sim_df` records
        accordingly.

        Notes
        -----
        Completes the warm-up phase by establishing initial Condition F and Depot
        timing data before the main simulation loop begins.
        """
        # Loop through all part_ids in aircraft_df
        for part_id in self.df.aircraft_df['part_id']:
            
            # Find the sim_df row where this part_id is in cycle 1
            # Only search filled rows (up to current_sim_row)
            filled_sim_df = self.df.sim_df.iloc[:self.df.current_sim_row]
            
            # Find row index where part_id matches AND cycle == 1
            mask = (filled_sim_df['part_id'] == part_id) & (filled_sim_df['cycle'] == 1)
            part_record_idx = filled_sim_df[mask].index[0]
            
            # Get fleet_end time
            s1_end = self.df.sim_df.at[part_record_idx, 'fleet_end']
            d3 = self.calculate_depot_duration() # Depot calculatuion 
            
            # --- Condition F - Reactive to depot capacity ---
            if len(self.active_depot) < self.depot_capacity:
                # Depot has capacity - part enters immediately
                s3_start = s1_end
            else:
                # Depot full - wait for earliest slot
                earliest_free = heapq.heappop(self.active_depot)
                s3_start = max(s1_end, earliest_free)
            
            # Condition F waiting time
            s2_start = s1_end
            s2_end = s3_start
            d2 = s2_end - s2_start  # Zero if depot has capacity
            
            # Depot timing
            s3_end = s3_start + d3
            heapq.heappush(self.active_depot, s3_end)
            
            # Update sim_df using direct assignment (not add_sim_event)
            self.df.sim_df.at[part_record_idx, 'condition_f_duration'] = d2
            self.df.sim_df.at[part_record_idx, 'condition_f_start'] = s2_start
            self.df.sim_df.at[part_record_idx, 'condition_f_end'] = s2_end
            self.df.sim_df.at[part_record_idx, 'depot_duration'] = d3
            self.df.sim_df.at[part_record_idx, 'depot_start'] = s3_start
            self.df.sim_df.at[part_record_idx, 'depot_end'] = s3_end
    
    def _schedule_initial_events(self):
        """
        Schedule all initial events after initialization phase completes.
        
        Called by run() after initialize_first_cycle() and 
        initialize_condition_f_depot() have set up the initial state.
        
        Schedules three event types:
        1. Depot completions (parts finishing initial depot stage)
        2. Fleet completions (aircraft finishing initial fleet stage)
        3. New part arrivals (from new_part_df with condition_a_start set)
        """
        # Get filled rows only
        filled_sim = self.df.sim_df.iloc[:self.df.current_sim_row]
        filled_des = self.df.des_df.iloc[:self.df.current_des_row]
        
        # 1. Schedule depot completions from initialization
        depot_parts = filled_sim[
            (filled_sim['depot_end'].notna()) & 
            (filled_sim['condemn'] == 'no')
        ]
        for _, row in depot_parts.iterrows():
            self.schedule_event(row['depot_end'], 'depot_complete', row['sim_id'])
        
        # 2. Schedule fleet completions from initialization
        fleet_aircraft = filled_des[filled_des['fleet_end'].notna()]
        for _, row in fleet_aircraft.iterrows():
            self.schedule_event(row['fleet_end'], 'fleet_complete', row['des_id'])
        
        # 3. Schedule new part arrivals (if any exist in new_part_df)
        new_parts = self.df.new_part_df[self.df.new_part_df['condition_a_start'].notna()]
        for _, row in new_parts.iterrows():
            self.schedule_event(row['condition_a_start'], 'new_part_arrives', row['part_id'])
    
    def handle_part_completes_depot(self, sim_id):
        """
        Handle the event where a part completes Depot repair.

        For the given part (identified by `sim_id`), this function determines whether
        any aircraft are currently in MICAP status and routes the part accordingly:

        - **No MICAP aircraft:** The part moves into Condition A (available inventory)
        and waits until triggered by an aircraft completing its Fleet stage.
        - **MICAP aircraft present:** The part is immediately installed on the earliest
        MICAP aircraft, resolving its MICAP status. Both `sim_df` and `des_df` are
        updated to close the current cycle, new rows are created for the next cycle,
        and `process_new_cycle_stages()` advances both entities to their next event.

        Notes
        -----
        This function represents the "Part Completes Depot" event type in the
        simulation event loop. It manages the transition from Depot completion to
        either inventory availability or direct MICAP resolution.
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
                'cycle': part_row['cycle'],
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
        Handle the event where an aircraft completes its Fleet stage and requires a replacement part.

        For the given aircraft (`des_id`), this function determines whether any parts are 
        available in Condition A inventory and proceeds along one of two main paths:

        - **Part available:** The earliest available part (based on `condition_a_start`) 
        is selected and installed immediately. Both `sim_df` and `des_df` are updated 
        to record installation details, new rows are created for the next 
        cycle, and `process_new_cycle_stages()` advances the aircraft–part pair to 
        their next stage trigger.
        
        If the part came from inventory (never cycled), two sim_df rows are created:
        one to record installation, and one to initialize its first full cycle.

        - **No parts available:** The aircraft enters MICAP status. A new record is added 
        to `micap_df` to log the start of the MICAP period until a future Depot 
        completion or new part arrival resolves it.

        Notes
        -----
        Represents the "Aircraft Completes Fleet" event type in the simulation loop. 
        This function links part availability, installation, and MICAP handling within 
        the event-driven flow.
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

    def handle_new_part_arrives(self, part_id):
        """
        Handle the event where a newly ordered part arrives after its lead-time delay.

        Triggered when a part in `new_part_df` reaches its arrival day, this function
        determines whether any aircraft are currently in MICAP status and proceeds
        along one of two paths:

        - **No MICAP aircraft:** The arriving part is added to `condition_a_df`
        (available inventory) and waits until triggered by an "Aircraft Completes
        Fleet" event.
        - **MICAP aircraft present:** The part is immediately installed on the
        earliest MICAP aircraft, resolving its MICAP status. Both `sim_df` and
        `des_df` are updated to record installation details, new rows are created
        for the next maintenance cycle, and `process_new_cycle_stages()` advances
        the aircraft–part pair to their next event trigger.

        Notes
        -----
        Represents the "New Part Arrives" event type in the simulation loop.
        Handles the integration of newly ordered parts into the model and their
        immediate or deferred assignment based on MICAP status. New parts are
        created when condemned parts trigger an order via `process_new_cycle_stages()`.
        """
        # Get the part's arrival info from new_part_df
        part_row = self.df.new_part_df[self.df.new_part_df['part_id'] == part_id].iloc[0]
        condition_a_start = part_row['condition_a_start']
        
        # Check if any aircraft currently in MICAP
        micap_aircraft = self.df.micap_df[self.df.micap_df['micap_end'].isna()]
        n_micap = len(micap_aircraft)
        
        # --- PATH 1: No MICAP → Part goes to condition_a_df ---
        if n_micap == 0:
            # Add to condition_a_df with only part_id, condition_a_start, cycle
            new_row = pd.DataFrame({
                'sim_id': [np.nan],
                'part_id': [part_id],
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
                'condition_a_start': [condition_a_start],
                'condition_a_end': [np.nan],
                'install_start': [np.nan],
                'install_end': [np.nan],
                'cycle': [0],
                'condemn': ['no']
            })
            self.df.condition_a_df = pd.concat([self.df.condition_a_df, new_row], 
                                              ignore_index=True)
            
            # Remove part from new_part_df
            self.df.new_part_df = self.df.new_part_df[
                self.df.new_part_df['part_id'] != part_id
            ].reset_index(drop=True)
            
        # --- PATH 2: MICAP exists → Install directly ---
        else:
            # Get first MICAP aircraft
            first_micap = micap_aircraft.sort_values('micap_start').iloc[0]
            
            # Calculate install timing
            d4_install = self.calculate_install_duration()
            s4_install_start = condition_a_start
            s4_install_end = s4_install_start + d4_install
            
            # Calculate condition_a_duration (time part waited)
            condition_a_end = s4_install_start
            condition_a_duration = condition_a_end - condition_a_start
            
            # --- Add NEW row to sim_df for cycle 0 (install event) ---
            new_sim_id = self.get_next_sim_id()
            
            self.add_sim_event(
                sim_id=new_sim_id,
                part_id=part_id,
                desone_id=np.nan,
                acone_id=np.nan,
                micap='no',
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
                destwo_id=first_micap['des_id'],
                actwo_id=first_micap['ac_id'],
                condition_a_start=condition_a_start,
                condition_a_end=condition_a_end,
                install_start=s4_install_start,
                install_end=s4_install_end,
                cycle=0,
                condemn='no'
            )
            
            # Generate IDs for cycle restart
            new_sim_id_restart = self.get_next_sim_id()
            new_des_id_restart = self.get_next_des_id()
            
            # --- Add ANOTHER row to sim_df for cycle 1 (restart) ---
            self.add_sim_event(
                sim_id=new_sim_id_restart,
                part_id=part_id,
                desone_id=new_des_id_restart,
                acone_id=first_micap['ac_id'],
                micap='no',
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
                destwo_id=np.nan,
                actwo_id=np.nan,
                condition_a_start=np.nan,
                condition_a_end=np.nan,
                install_start=np.nan,
                install_end=np.nan,
                cycle=1,
                condemn='no'
            )
            
            # --- Update des_df (existing MICAP aircraft row) ---
            micap_duration = condition_a_start - first_micap['micap_start']
            micap_end = condition_a_start
            
            row_idx = self.df.des_df[self.df.des_df['des_id'] == first_micap['des_id']].index[0]
            self.df.des_df.at[row_idx, 'micap_duration'] = micap_duration
            self.df.des_df.at[row_idx, 'micap_start'] = first_micap['micap_start']
            self.df.des_df.at[row_idx, 'micap_end'] = micap_end
            self.df.des_df.at[row_idx, 'simtwo_id'] = new_sim_id  # Use cycle 0 sim_id
            self.df.des_df.at[row_idx, 'parttwo_id'] = part_id
            self.df.des_df.at[row_idx, 'install_duration'] = d4_install
            self.df.des_df.at[row_idx, 'install_start'] = s4_install_start
            self.df.des_df.at[row_idx, 'install_end'] = s4_install_end
            
            # --- Add new row to des_df for cycle 1 (restart) ---
            self.add_des_event(
                des_id=new_des_id_restart,
                ac_id=first_micap['ac_id'],
                micap='no',
                simone_id=new_sim_id_restart,
                partone_id=part_id,
                fleet_duration=np.nan,
                fleet_start=s4_install_end,
                fleet_end=np.nan,
                micap_duration=np.nan,
                micap_start=np.nan,
                micap_end=np.nan,
                simtwo_id=np.nan,
                parttwo_id=np.nan,
                install_duration=np.nan,
                install_start=np.nan,
                install_end=np.nan
            )
            
            # --- Process stages 1-3 for the new cycle ---
            self.process_new_cycle_stages(
                part_id=part_id,
                ac_id=first_micap['ac_id'],
                s4_install_end=s4_install_end,
                new_sim_id=new_sim_id_restart,
                new_des_id=new_des_id_restart
            )
            
            # --- Remove part from new_part_df ---
            self.df.new_part_df = self.df.new_part_df[
                self.df.new_part_df['part_id'] != part_id
            ].reset_index(drop=True)
            
            # --- Remove resolved MICAP from micap_df ---
            self.df.micap_df = self.df.micap_df[
                self.df.micap_df['des_id'] != first_micap['des_id']
            ].reset_index(drop=True)
    
    def run(self):
        """
        Execute event-driven discrete-event simulation.
        
        Architecture Change:
        - OLD: Period-based scanning (O(n × sim_time))
        - NEW: Event-driven heap processing (O(n_events × log(n_events)))
        
        Flow:
        1. Initialize first cycle (parts and aircraft start in Fleet)
        2. Initialize Condition F and Depot stages
        3. Schedule all initial events into heap
                - Part Completes Depot → `handle_part_completes_depot()`
                - Aircraft Completes Fleet → `handle_aircraft_needs_part()`
                - New Part Arrives → `handle_new_part_arrives()`
        4. Process events chronologically from heap
        5. Each event handler schedules future events
        6. Continue until heap empty or time limit reached
        7. Return trimmed and validated results
        
        Returns
        -------
        dict
            Validation results and daily metrics
        """
        # Phase 1: Initialization (unchanged)
        self.initialize_first_cycle()
        self.initialize_condition_f_depot()
        
        # Phase 2: Schedule all initial events
        self._schedule_initial_events()
        
        # Phase 3: Event-driven main loop
        while self.event_heap:
            # Get next event chronologically
            event_time, _, event_type, entity_id = heapq.heappop(self.event_heap)
            
            # Stop if event exceeds simulation time limit
            if event_time > self.sim_time:
                break
            
            # Advance simulation clock
            self.current_time = event_time
            
            # Process event (handlers will schedule future events)
            if event_type == 'depot_complete':
                # EVENT TYPE: Part Completes Depot
                self.handle_part_completes_depot(entity_id)
                # EVENT TYPE: Aircraft Completes Fleet
            elif event_type == 'fleet_complete':
                self.handle_aircraft_needs_part(entity_id)
                # EVENT TYPE: New Part Arrives
            elif event_type == 'new_part_arrives':
                self.handle_new_part_arrives(entity_id)
        
        # Phase 4: Post-processing
        self.df.trim_dataframes()
        validation_results = self.df.validate_structure()
        daily_metrics = self.df.create_daily_metrics()
        validation_results['daily_metrics'] = daily_metrics
        
        return validation_results
