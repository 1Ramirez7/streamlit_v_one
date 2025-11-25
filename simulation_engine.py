import numpy as np
from scipy.special import gamma
import pandas as pd
import heapq

try:
    # Try relative imports first (when used as module)
    from .initialization import Initialization
    from .micap_state import MicapState
except ImportError:
    # Fall back to absolute imports (when run directly)
    from initialization import Initialization
    from micap_state import MicapState

class SimulationEngine:
    """
    Manages simulation logic and event processing.
    
    Works with DataFrameManager for DataFrame access and updates.
    Contains formulas for stage durations and helper functions for event management.
    """
    
    def __init__(self, df_manager, sone_dist, sone_mean, sone_sd, sthree_dist, sthree_mean, sthree_sd, 
                 sim_time, depot_capacity,condemn_cycle, condemn_depot_fraction,  part_order_lag,
                 use_fleet_rand, fleet_rand_min, fleet_rand_max, use_depot_rand, depot_rand_min, 
                 depot_rand_max):
        """
        Initialize SimulationEngine with DataFrameManager and stage parameters.
        
        
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
        self.sone_dist = sone_dist
        self.sone_mean = sone_mean
        self.sone_sd = sone_sd
        self.sthree_dist = sthree_dist
        self.sthree_mean = sthree_mean
        self.sthree_sd = sthree_sd
        self.sim_time = sim_time
        self.active_depot: list = []
        self.depot_capacity: int = depot_capacity # adding depot capacity code
        self.condemn_cycle: int = condemn_cycle  # NEW: Store condemn cycle
        self.condemn_depot_fraction: float = condemn_depot_fraction # NEW: Store depot time fraction
        self.part_order_lag: int = part_order_lag  # NEW: Store lag parameter

        self.use_fleet_rand = use_fleet_rand
        self.fleet_rand_min = fleet_rand_min
        self.fleet_rand_max = fleet_rand_max
        self.use_depot_rand = use_depot_rand
        self.depot_rand_min = depot_rand_min
        self.depot_rand_max = depot_rand_max
        
        # NEW: Event-driven structures
        self.event_heap = []  # Priority queue: (time, counter, event_type, entity_id)
        self.event_counter = 0  # FIFO tie-breaker for same-time events
        self.current_time = 0  # Simulation clock
        self.micap_state = MicapState()  # Manage MICAP aircraft
        
        # Initialize MICAP state with initial conditions micap787
        if hasattr(df_manager, 'allocation'):
            self.micap_state._create_micap_df(df_manager.allocation)

        # WIP tracking 777
        self.wip_history = []  # List of (time, wip_snapshot) tuples
        self.wip_snapshot_interval = 1.0  # Record WIP every 1 day
        self.next_wip_time = 0.0

        # Event tracking for progress display 777
        self.event_counts = {
            'depot_complete': 0,
            'fleet_complete': 0,
            'new_part_arrives': 0,
            'CF_DE': 0,
            'part_fleet_end': 0,
            'part_condemn': 0,
            'total': 0
        }
        self.progress_callback = None  # Callback for UI updates 777 end
    
    # ==========================================================================
    # STAGE DURATION FORMULAS
    # ==========================================================================
        
    def calculate_fleet_duration(self):
        """
        Calculates distribution for length of stage based on chosen distribution:
        Normal or Weibull
        """
        if self.sone_dist == "Normal":
            return max(0, np.random.normal(self.sone_mean, self.sone_sd))
        elif self.sone_dist == "Weibull":
            return max(0, np.random.weibull(self.sone_mean) * self.sone_sd)

    def calculate_condition_f_duration(self):
        """
        Not in use, delete or leave as spacer
        """
        return 0
    
    def calculate_depot_duration(self):
        """
        Calculates distribution for length of stage based on chosen distribution:
        Normal or Weibull
        """
        if self.sthree_dist == "Normal":
            return max(0, np.random.normal(self.sthree_mean, self.sthree_sd))
        elif self.sone_dist == "Weibull":
            return max(0, np.random.weibull(self.sthree_mean) * self.sthree_sd)
    
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
    # HELPER FUNCTIONS: ID GENERATION & ADD events to DFs
    # ==========================================================================
    
    def get_next_sim_id(self):
        """
        current_sim_row is acting as a pointer to the next row to write in sim_df

        Retrieves the current value of `df_manager.current_sim_row`, which tracks how many
        rows in the part event log have been used.
         
        Uses
            1. use to slice dataframe up to current_sim_row:
            `filled_sim_df = self.df.sim_df.iloc[:self.df.current_sim_row]`

            2. Use current_sim_row as the row index to write into:
                1. Set what row index to write into
                `row_idx = self.df.current_sim_row`
                2. Edit row index
                `self.df.sim_df.at[row_idx, 'sim_id'] = sim_id`
                3. Increment counter so the next event goes into the next row
                `self.df.current_sim_row += 1`

        Returns
        -------
        int: Next available row index in `sim_df`.
        """
        return self.df.current_sim_row
    
    def get_next_des_id(self):
        """
        Returns the next available row index in `des_df`.

        `current_des_row` indicates both:
        • The write-position for inserting the next DES event.

        Uses
        ----
        1. Retrieve all filled des rows by slicing at current_des_row:
        `filled_des = self.df.des_df.iloc[:self.df.current_des_row]`

        2. Create new des_id values during MICAP resolution and cycle restarts:
        `new_des_id = self.get_next_des_id()`

        3. Provide the row index consumed by `add_des_event()`:
        `row_idx = self.df.current_des_row`

        Notes
        -----
        Ensures sequential event recording without overwriting data. Used by
        `add_des_event()` and related event-processing methods to maintain index integrity.
        """
        return self.df.current_des_row
    
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
        Appends a new simulation-event row to `sim_df` at `current_sim_row`.

        The function writes all provided scalar values into the target row using
        `.at[]`, then increments `current_sim_row` so the next call writes to the
        following row.

        Role
        ----
        • Generates the foundation rows used when cycles restart.
        (fleet → condition F → depot → condition A → install).

        Uses
        ----
        1. Called during initialization (e.g., first-cycle,
        injected depot/cond-F parts).
        2. Called during event-handling routines to log:
            • new cycles,
            • the initial cycle for new part arrivals.
        3. Consumes the index returned by `get_next_sim_id()`.

        Notes
        -----
        • After writing the row, `current_sim_row` is incremented by 1.
        • The sim_id parameter is not the row index; the row index is always
        `current_sim_row`.
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
        Appends a new aircraft-event row to `des_df` at `current_des_row`.

        The function writes all provided aircraft-stage values into the target
        row using `.at[]`, then increments `current_des_row` so subsequent DES
        events append sequentially.

        Role
        ----
        • Creates new DES rows during cycle restarts.

        Uses
        ----
        1. Called during initialization to create the aircraft's initial DES row.
        2. Used extensively in MICAP and install logic to:
            • generate the next-cycle DES record.
        3. Consumes the index returned by `get_next_des_id()`.

        Notes
        -----
        • After writing the row, `current_des_row` is incremented by 1.
        • The des_id parameter is not the row index; the row index is always
        `current_des_row`.
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

    def event_acp_fs_fe(self, s4_install_end, new_sim_id, new_des_id):
        """
        EVENT: Aircraft-Part Fleet Start to Fleet End
        
        Handles fleet stage timing for both aircraft (des_df) and part (sim_df).
        Schedules:
        - fleet_complete (aircraft event)
        - part_fleet_end (part event to trigger CF→DE flow)
        
        Parameters
        ----------
        s4_install_end : float
            Installation end time = fleet start time
        new_sim_id : int
            Row index in sim_df
        new_des_id : int
            Row index in des_df
        """
        sim_row_idx = new_sim_id
        des_row_idx = new_des_id
        
        # Calculate fleet duration and timing
        d1 = self.calculate_fleet_duration()
        s1_start = s4_install_end
        s1_end = s1_start + d1
        
        # Update sim_df (part) and des_df (ac)
        self.df.sim_df.at[sim_row_idx, 'fleet_duration'] = d1
        self.df.sim_df.at[sim_row_idx, 'fleet_end'] = s1_end

        self.df.des_df.at[des_row_idx, 'fleet_duration'] = d1
        self.df.des_df.at[des_row_idx, 'fleet_end'] = s1_end
        
        # Schedule fleet_complete event
        self.schedule_event(s1_end, 'fleet_complete', new_des_id)
        
        # Schedule part_fleet_end event 
        self.schedule_event(s1_end, 'part_fleet_end', new_sim_id)


    def event_p_cfs_de(self, sim_id):
        """
        EVENT: Part Condition F Start to Depot End
        
        Handles part flow from fleet_end → condition F → depot.
        Triggered by 'part_fleet_end' event.
        
        For condemned parts:
        - Marks condemn='yes'
        - Uses reduced depot duration
        - Schedules 'part_condemn' event at depot_end
        
        For normal parts:
        - Uses full depot duration  
        - Schedules 'depot_complete' event at depot_end
        
        Parameters
        ----------
        sim_id : int
            Row index in sim_df to process
        """
        # load proper sim_df row
        sim_row_idx = sim_id
        s1_end = self.df.sim_df.at[sim_row_idx, 'fleet_end']

        # EVENT TYPES
        eventtypec="CONDEMN" # part is condemn
        eventtypede="CFs_DE"
        
        # pre-Calculate depot_start given DEPOT CONSTRAINT is satisfy
        if len(self.active_depot) < self.depot_capacity:
            s3_start = s1_end
        else:
            # Get the earliest depot slot frees up
            earliest_free = heapq.heappop(self.active_depot)
            s3_start = max(s1_end, earliest_free)
        
        # Condition F calculations
        s2_start = s1_end
        s2_end = s3_start
        d2 = s2_end - s2_start  # Wait time for depot
        
        # Update Condition F in sim_df
        self.df.sim_df.at[sim_row_idx, 'condition_f_start'] = s2_start
        self.df.sim_df.at[sim_row_idx, 'condition_f_end'] = s2_end
        self.df.sim_df.at[sim_row_idx, 'condition_f_duration'] = d2
        
        # --- Cycle Condemn Logic ---
        cycle = self.df.sim_df.at[sim_row_idx, 'cycle']
        
        # CONDEMN PART: Cycle equals CONDEMN CYCLE
        if cycle == self.condemn_cycle:
            self.df.sim_df.at[sim_row_idx, 'condemn'] = 'yes'
            # Condemned parts takes user determined rate of normal depot time
            d3 = self.calculate_depot_duration() * self.condemn_depot_fraction
            s3_end = s3_start + d3
            heapq.heappush(self.active_depot, s3_end)
            
            # Update depot info
            self.df.sim_df.at[sim_row_idx, 'micap'] = eventtypec
            self.df.sim_df.at[sim_row_idx, 'depot_start'] = s3_start
            self.df.sim_df.at[sim_row_idx, 'depot_end'] = s3_end
            self.df.sim_df.at[sim_row_idx, 'depot_duration'] = d3
            
            # Schedule condemn event at depot_end
            self.schedule_event(s3_end, 'part_condemn', sim_id)
            
        else:
            # NORMAL PART
            d3 = self.calculate_depot_duration()
            s3_end = s3_start + d3
            heapq.heappush(self.active_depot, s3_end)
            
            self.df.sim_df.at[sim_row_idx, 'micap'] = eventtypede
            self.df.sim_df.at[sim_row_idx, 'depot_start'] = s3_start
            self.df.sim_df.at[sim_row_idx, 'depot_end'] = s3_end
            self.df.sim_df.at[sim_row_idx, 'depot_duration'] = d3

            # Schedule normal depot completion
            self.schedule_event(s3_end, 'depot_complete', sim_id)


    def event_p_condemn(self, sim_id):
        """
        EVENT: Part Condemn
        
        Handles condemned part processing:
        1. Orders replacement part
        2. Logs condemnation details
        3. Schedules new part arrival
        
        Triggered when condemned part reaches depot_end.
        
        Parameters
        ----------
        sim_id : int
            Row index in sim_df of condemned part
        """
        # note that previous functions use sim_id = new_sim_id
        # so keep an eye on it when changing new_sim_id
        sim_row_idx = sim_id
        
        part_id = self.df.sim_df.at[sim_row_idx, 'part_id']
        depot_end_condemned = self.df.sim_df.at[sim_row_idx, 'depot_end']
        
        # Find the row in new_part_df where condition_a_start is empty (np.nan)
        # This will be the row with only part_id populated
        empty_row_mask = self.df.new_part_df['condition_a_start'].isna()
        empty_row_idx = self.df.new_part_df[empty_row_mask].index[0]
        
        # Get precalculated part_id to use now and recalculate
        new_part_id = self.df.new_part_df.at[empty_row_idx, 'part_id']
        
        # calculate new part arrival time
        new_part_arrival_time = depot_end_condemned + self.part_order_lag # temp for log needed?
        self.df.new_part_df.at[empty_row_idx, 'condition_a_start'] = new_part_arrival_time
        self.df.new_part_df.at[empty_row_idx, 'cycle'] = 0 # hardcode cycle start value? 
        
        # Log condemnation. Use for debugging. Decide if still using!
        self.df.condemn_new_log.append({
            'part_id': part_id,
            'depot_end': depot_end_condemned,
            'new_part_id': new_part_id,
            'condition_a_start': new_part_arrival_time
        })
        
        # Add new row with only part_id = previous part_id + 1. For next new part
        new_row = pd.DataFrame({
            'sim_id': [np.nan],
            'part_id': [new_part_id + 1],
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
        
        # Schedule new part arrival
        self.schedule_event(new_part_arrival_time, 'new_part_arrives', new_part_id)


    def _schedule_initial_events(self):
        """
        Schedule all initial events after initialization phase completes.
        
        Called by run() after initialization class has ran.
        
        Schedules three event types:
        1. Depot completions (parts finishing initial depot stage)
        2. Fleet completions (aircraft finishing initial fleet stage)
        3. New part arrivals (from new_part_df with condition_a_start set)
        4. Condition F starts (parts injected into Condition F queue)
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
        
        # 4. Schedule Condition F PART-EVENTS (CF_DE parts)
        cond_f_parts = filled_sim[
            ((filled_sim['micap'] == 'IC_IjCF') & (filled_sim['condition_f_start'] == 0)) |
            (filled_sim['micap'] == 'IC_FE_CF')]  # IMPORTANT: DONT add IC_FE_CF that DONT
        for _, row in cond_f_parts.iterrows():
            self.schedule_event(row['condition_f_start'], 'CF_DE', row['sim_id'])
    
    def handle_part_completes_depot(self, sim_id):
        """
        Handle the event where a part completes Depot repair.

        For the given part (identified by `sim_id`), this function determines whether
        any aircraft are currently in MICAP status and routes the part accordingly:

        - **No MICAP aircraft:** The part moves into Condition A
        and waits until triggered by an aircraft completing its Fleet stage.
        - **MICAP aircraft present:** The part is immediately installed on the earliest
        MICAP aircraft, resolving its MICAP status. Both `sim_df` and `des_df` are
        updated to close the current cycle, new rows are created for the next cycle,
        and `event_acp_fs_fe()` advances both entities to their next event.

        Notes
        -----
        - eventtypeca="DE_CA" # sim_df
        - eventtypemi="DE_MI" # sim & des DFs - part resolve micap & cycle ends
        - eventtypedemicr="DE_MI_CR" # sim & des DFs. DE resolve MICAP and CR
        - eventtypedesmi="DE_SMI" # AC started micap & resolve
        - eventtypedesmi="DE_SMI_CR" # AC started micap & CR
        """
        # Get part details
        filled_sim_df = self.df.sim_df.iloc[:self.df.current_sim_row]
        part_row = filled_sim_df[filled_sim_df['sim_id'] == sim_id].iloc[0]
        
        s3_end = part_row['depot_end']

        eventtypeca="DE_CA" # sim_df
        eventtypemi="DE_MI" # sim & des DFs - part resolve micap & cycle ends
        eventtypedemicr="DE_MI_CR" # sim & des DFs. DE resolve MICAP and CR
        eventtypedesmi="DE_SMI" # AC started micap & resolve
        eventtypedesmi="DE_SMI_CR" # AC started micap & CR
        
        # Check if any aircraft in MICAP 787 new
        micap_pa_rm = self.micap_state.pop_and_rm_first(s3_end, event_type=eventtypemi)
        
        # CASE A1: No MICAP aircraft → Part goes to Condition A
        if micap_pa_rm is None:
            # Create new row for condition_a_df
            new_row = pd.DataFrame([{
                'sim_id': part_row['sim_id'],
                'part_id': part_row['part_id'],
                'desone_id': part_row['desone_id'],
                'acone_id': part_row['acone_id'],
                'micap': eventtypeca,
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
            # Use micap info fetch in micap_pa_rm.
            first_micap = micap_pa_rm # do i need this if replace first_micap with micap_pa_rm
            
            # Calculate install duration
            d4_install = self.calculate_install_duration()
            s4_install_start = s3_end
            s4_install_end = s4_install_start + d4_install
            micap_duration_ = s3_end - first_micap['micap_start']
            micap_end_ = s3_end
            
            # Check if aircraft has des_id BEFORE updating sim_df
            has_des_id = pd.notna(first_micap['des_id'])
            
            # Determine which des_id to use for sim_df destwo_id
            if has_des_id:
                des_id_for_sim = first_micap['des_id']
            else:
                # Aircraft has no des_id, generate one now for MICAP resolution
                des_id_for_sim = self.get_next_des_id()

            # Get part details
            filled_sim_df = self.df.sim_df.iloc[:self.df.current_sim_row]
            part_row = filled_sim_df[filled_sim_df['sim_id'] == sim_id].iloc[0]
        
            # Update existing sim_df row for this part (install info)
            part_row_idx = filled_sim_df[filled_sim_df['sim_id'] == sim_id].index[0]
            self.df.sim_df.at[part_row_idx, 'micap'] = eventtypemi
            self.df.sim_df.at[part_row_idx, 'condition_a_duration'] = np.nan
            self.df.sim_df.at[part_row_idx, 'install_duration'] = d4_install
            self.df.sim_df.at[part_row_idx, 'destwo_id'] = des_id_for_sim
            self.df.sim_df.at[part_row_idx, 'actwo_id'] = first_micap['ac_id']
            self.df.sim_df.at[part_row_idx, 'condition_a_start'] = np.nan
            self.df.sim_df.at[part_row_idx, 'condition_a_end'] = np.nan
            self.df.sim_df.at[part_row_idx, 'install_start'] = s4_install_start
            self.df.sim_df.at[part_row_idx, 'install_end'] = s4_install_end
            
            # Generate IDs for new cycle
            new_sim_id = self.get_next_sim_id()

            if has_des_id:
                new_des_id_restart = self.get_next_des_id()
            else:
                new_des_id_restart = self.get_next_des_id() + 1
            
            # Add new row to sim_df for cycle restart
            self.add_sim_event(
                sim_id=new_sim_id,
                part_id=part_row['part_id'],
                desone_id=new_des_id_restart,
                acone_id=first_micap['ac_id'],
                micap=eventtypedemicr,
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
            
            # Handle des_df based on whether aircraft had des_id
            if has_des_id:
                # Aircraft has des_df row, MUTATE existing row
                filled_des_df = self.df.des_df.iloc[:self.df.current_des_row]
                micap_des_row_idx = filled_des_df[filled_des_df['des_id'] == first_micap['des_id']].index[0]
                self.df.des_df.at[micap_des_row_idx, 'micap'] = eventtypemi
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
                    des_id=new_des_id_restart,
                    ac_id=first_micap['ac_id'],
                    micap=eventtypedemicr,
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
            else:
                # Aircraft started in MICAP, ADD NEW row for MICAP resolution
                # (des_id_for_sim was already generated and used in sim_df)
                self.add_des_event(
                    des_id=des_id_for_sim,
                    ac_id=first_micap['ac_id'],
                    micap=eventtypedesmi, # resolve by DE part
                    simone_id=None,
                    partone_id=None,
                    fleet_duration=np.nan,
                    fleet_start=np.nan,
                    fleet_end=np.nan,
                    micap_duration=micap_duration_,
                    micap_start=first_micap['micap_start'],
                    micap_end=micap_end_,
                    simtwo_id=part_row['sim_id'],
                    parttwo_id=part_row['part_id'],
                    install_duration=d4_install,
                    install_start=s4_install_start,
                    install_end=s4_install_end
                )
                
                # Add ANOTHER row to des_df for cycle restart
                self.add_des_event(
                    des_id=new_des_id_restart,
                    ac_id=first_micap['ac_id'],
                    micap=eventtypedesmi,
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
            self.event_acp_fs_fe(
                s4_install_end=s4_install_end,
                new_sim_id=new_sim_id,
                new_des_id=new_des_id_restart
            )
            
    
    def handle_aircraft_needs_part(self, des_id):
        """
        Handle Event where aircraft completes Fleet stage and requires a replacement part.

        For the given aircraft (`des_id`), this function determines whether any parts are 
        available in Condition A inventory and proceeds along one of two main paths:

        - **Part available:** The earliest available part (based on `condition_a_start`) 
        is selected and installed immediately. Both `sim_df` and `des_df` are updated 
        to record installation details, new rows are created for the next 
        cycle, and `event_acp_fs_fe()` advances the aircraft-part pair to 
        their next stage trigger.
        
        If the part came started inventory (IC or new part), two sim_df rows are created:
        one to record installation, and one to initialize its first full cycle.

        - **No parts available:** The aircraft enters MICAP status. AC added to `micap_df` 

        Notes
        -----
        A. Besides AFE_MICAP. all 4 eventtypes go in both sim&des DFs
        - eventtypeca="AFE_CA" # AC takes part from CA.
        - eventtypecacr="AFE_CA_CR" # AC-PART cycle restart
        - eventtypesca="AFE_SCA" # part started CA
        - eventtypescacr="AFE_SCA_CR" # part started CA, cycle restart
        - eventtype="AFE_MICAP" # AC goes MICAP
        """
        # Get aircraft details
        filled_des_df = self.df.des_df.iloc[:self.df.current_des_row]
        ac_row = filled_des_df[filled_des_df['des_id'] == des_id].iloc[0]
        
        s1_end = ac_row['fleet_end']

        # EVENT TYPEs 
        eventtypeca="AFE_CA" # AC takes part from CA.
        eventtypecacr="AFE_CA_CR" # AC-PART cycle restart
        eventtypesca="AFE_SCA" # part started CA
        eventtypescacr="AFE_SCA_CR" # part started CA, cycle restart
        eventtype="AFE_MICAP" # AC goes MICAP
        
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
                
                self.df.sim_df.at[part_row_idx, 'micap'] = eventtypeca
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
                    micap=eventtypecacr,
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
                self.df.des_df.at[ac_row_idx, 'micap'] = eventtypeca
                self.df.des_df.at[ac_row_idx, 'simtwo_id'] = first_available['sim_id']
                self.df.des_df.at[ac_row_idx, 'parttwo_id'] = first_available['part_id']
                self.df.des_df.at[ac_row_idx, 'install_duration'] = d4_install
                self.df.des_df.at[ac_row_idx, 'install_start'] = s4_install_start
                self.df.des_df.at[ac_row_idx, 'install_end'] = s4_install_end
                
                # Add new row to des_df for cycle restart
                self.add_des_event(
                    des_id=new_des_id,
                    ac_id=ac_row['ac_id'],
                    micap=eventtypecacr,
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
                self.event_acp_fs_fe(
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
                    micap=eventtypesca, # AFE_SCA
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
                    cycle=first_available['cycle'],
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
                    micap=eventtypescacr,
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
                self.df.des_df.at[ac_row_idx, 'micap'] = eventtypesca
                self.df.des_df.at[ac_row_idx, 'simtwo_id'] = new_sim_id
                self.df.des_df.at[ac_row_idx, 'parttwo_id'] = first_available['part_id']
                self.df.des_df.at[ac_row_idx, 'install_duration'] = d4_install
                self.df.des_df.at[ac_row_idx, 'install_start'] = s4_install_start
                self.df.des_df.at[ac_row_idx, 'install_end'] = s4_install_end
                
                # Add new row to des_df for cycle restart
                self.add_des_event(
                    des_id=new_des_id_restart,
                    ac_id=ac_row['ac_id'],
                    micap=eventtypescacr,
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
                self.event_acp_fs_fe(
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
            
            # Add aircraft to MICAP state micap787
            self.micap_state.add_aircraft(
                des_id=des_id,
                ac_id=ac_row['ac_id'],
                micap_type=eventtype,
                fleet_duration=ac_row['fleet_duration'],
                fleet_start=ac_row['fleet_start'],
                fleet_end=ac_row['fleet_end'],
                micap_start=micap_start_time
            )

    def handle_new_part_arrives(self, part_id):
        """
        Handle the event where a newly ordered part arrives after its order-lag

        This function determines whether any aircraft are currently in MICAP 
        status and proceeds along one of two paths:

        1. **No MICAP aircraft:** New PART is added to `condition_a_df` - end of path.
        2. **MICAP aircraft:** The part is immediately installed on the
        earliest MICAP aircraft, resolving MICAP. Both `sim_df` and
        `des_df` are updated to record installation details, new rows are created
        for the next maintenance cycle, and `event_acp_fs_fe()` advances
        the aircraft-part pair to their next event trigger.

        Notes
        -----
        eventtypenca="PNEW_CA"
        eventtypenma="PNEW_MICAP" # sim_df has both of this
        # Need expand func to allow  different event types in sim_df
        eventtypenmacr="PNEW_MI_CR" # same as above. 
        eventtypensmi="PNEW_SMICAP" # resolve MICAP for AC started MICAP
        eventtypensmicr="PNEW_SMI_CR" # CR for AC started MICAP
        """
        # Get the part's arrival info from new_part_df
        part_row = self.df.new_part_df[self.df.new_part_df['part_id'] == part_id].iloc[0]
        condition_a_start = part_row['condition_a_start']
        cycle = part_row['cycle']

        # EVENT TYPES
        eventtypenca="PNEW_CA"
        eventtypenma="PNEW_MICAP" # sim_df has both of this
        # Need expand func to allow  different event types in sim_df
        eventtypenmacr="PNEW_MI_CR" # same as above. 
        eventtypensmi="PNEW_SMICAP" # resolve MICAP for AC started MICAP
        eventtypensmicr="PNEW_SMI_CR" # CR for AC started MICAP

        # Check if any aircraft currently in MICAP micap787 new
        micap_npa_rm = self.micap_state.pop_and_rm_first(condition_a_start, event_type=eventtypenma)
        
        # --- PATH 1: No MICAP → Part goes to condition_a_df ---
        if micap_npa_rm is None:
            # Add to condition_a_df with only part_id, condition_a_start, cycle
            new_row = pd.DataFrame({
                'sim_id': [np.nan],
                'part_id': [part_id],
                'desone_id': [np.nan],
                'acone_id': [np.nan],
                'micap': [eventtypenca],
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
                'cycle': [cycle], # no need to override here as cycle is set in new_part_df
                'condemn': ['no'] # new to streamline condemn so it is not hardcoded here
            })
            self.df.condition_a_df = pd.concat([self.df.condition_a_df, new_row], 
                                              ignore_index=True)
            
            # Remove part from new_part_df
            self.df.new_part_df = self.df.new_part_df[
                self.df.new_part_df['part_id'] != part_id
            ].reset_index(drop=True)
            
        # --- PATH 2: MICAP exists → Install directly ---
        else:
            # Use micap info fetch in micap_npa_rm.
            first_micap = micap_npa_rm # do i need this if replace first_micap with micap_npa_rm
            
            # Calculate install timing
            d4_install = self.calculate_install_duration()
            s4_install_start = condition_a_start
            s4_install_end = s4_install_start + d4_install
            
            # Calculate condition_a_duration (time part waited)
            condition_a_end = s4_install_start
            condition_a_duration = condition_a_end - condition_a_start
            
            # Check if aircraft has des_id BEFORE adding sim_df row
            has_des_id = pd.notna(first_micap['des_id'])
            
            # Determine which des_id to use for sim_df destwo_id
            if has_des_id:
                des_id_for_sim = first_micap['des_id']
            else:
                # Aircraft has no des_id, generate one now for MICAP resolution
                des_id_for_sim = self.get_next_des_id()
            
            # --- Add NEW row to sim_df for cycle 0 (install event) ---
            new_sim_id = self.get_next_sim_id()
            
            self.add_sim_event(
                sim_id=new_sim_id,
                part_id=part_id,
                desone_id=np.nan,
                acone_id=np.nan,
                micap=eventtypenma,
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
                destwo_id=des_id_for_sim,
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
            
            # FIXED: Adjust des_id based on whether we already consumed one
            if has_des_id:
                new_des_id_restart = self.get_next_des_id()
            else:
                new_des_id_restart = self.get_next_des_id() + 1  # We used current_des_row for MICAP resolution
            
            # --- Add ANOTHER row to sim_df for cycle 1 (restart) ---
            self.add_sim_event(
                sim_id=new_sim_id_restart,
                part_id=part_id,
                desone_id=new_des_id_restart,
                acone_id=first_micap['ac_id'],
                micap=eventtypenmacr, # PNEW_SMI_CR
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
            
            # Handle des_df based on whether aircraft had des_id
            if has_des_id:
                # Aircraft has des_df row, MUTATE existing row
                micap_duration = condition_a_start - first_micap['micap_start']
                micap_end = condition_a_start
                
                row_idx = self.df.des_df[self.df.des_df['des_id'] == first_micap['des_id']].index[0]
                self.df.des_df.at[row_idx, 'micap'] = eventtypenma
                self.df.des_df.at[row_idx, 'micap_duration'] = micap_duration
                self.df.des_df.at[row_idx, 'micap_start'] = first_micap['micap_start']
                self.df.des_df.at[row_idx, 'micap_end'] = micap_end
                self.df.des_df.at[row_idx, 'simtwo_id'] = new_sim_id  # Use cycle 0 sim_id
                self.df.des_df.at[row_idx, 'parttwo_id'] = part_id
                self.df.des_df.at[row_idx, 'install_duration'] = d4_install
                self.df.des_df.at[row_idx, 'install_start'] = s4_install_start
                self.df.des_df.at[row_idx, 'install_end'] = s4_install_end
                
                # Add new row to des_df for cycle restart
                self.add_des_event(
                    des_id=new_des_id_restart,
                    ac_id=first_micap['ac_id'],
                    micap=eventtypenmacr,
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
            else:
                # Aircraft started in MICAP, ADD NEW row for MICAP resolution
                # (des_id_for_sim was already generated and used in sim_df)
                micap_duration = condition_a_start - first_micap['micap_start']
                micap_end = condition_a_start
                
                self.add_des_event(
                    des_id=des_id_for_sim,
                    ac_id=first_micap['ac_id'],
                    micap=eventtypensmi,
                    simone_id=None,
                    partone_id=None,
                    fleet_duration=np.nan,
                    fleet_start=np.nan,
                    fleet_end=np.nan,
                    micap_duration=micap_duration,
                    micap_start=first_micap['micap_start'],
                    micap_end=micap_end,
                    simtwo_id=new_sim_id,  # Use cycle 0 sim_id
                    parttwo_id=part_id,
                    install_duration=d4_install,
                    install_start=s4_install_start,
                    install_end=s4_install_end
                )
                
                # Add ANOTHER row to des_df for cycle restart
                self.add_des_event(
                    des_id=new_des_id_restart,
                    ac_id=first_micap['ac_id'],
                    micap=eventtypensmicr,
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
            self.event_acp_fs_fe(
                s4_install_end=s4_install_end,
                new_sim_id=new_sim_id_restart,
                new_des_id=new_des_id_restart
            )
            
            # --- Remove part from new_part_df ---
            self.df.new_part_df = self.df.new_part_df[
                self.df.new_part_df['part_id'] != part_id
            ].reset_index(drop=True)
            


    #def event_cf_de(self, sim_id):
    def event_cf_de(self, sim_id):
        """
        EVENT-TYPE: CF_DE
        ----------
        * Move parts from CF to DE
        * function: `event_cf_de`
        * reference in
            * engine.run
            * engine._schedule_initial_events
        
        Parameters
        ----------
        sim_id : int
            To fetch sim_id row in sim_df for editing.
        """
        # Get the part's row from sim_df
        filled_sim_df = self.df.sim_df.iloc[:self.df.current_sim_row]
        part_row_idx = filled_sim_df[filled_sim_df['sim_id'] == sim_id].index[0]
        part_row = filled_sim_df.loc[part_row_idx]
        
        # Verify correct event type. (add code so it logs the event types, and obviously when error)
        if part_row['micap'] == 'IC_IjCF':
            assert part_row['condition_f_start'] == 0, \
                f"IC_IjCF event must have condition_f_start=0, got {part_row['condition_f_start']}"
        elif part_row['micap'] == 'IC_FE_CF':
            pass
        else:
            raise AssertionError(f"Expected IC_IjCF or IC_FE_CF event, got {part_row['micap']}")
        
        cf_start = part_row['condition_f_start']
        
        # --- Depot queue logic ---
        d_dur = self.calculate_depot_duration()
        if len(self.active_depot) < self.depot_capacity:
            d_start = cf_start
        else:
            earliest = heapq.heappop(self.active_depot)
            d_start = max(cf_start, earliest)
        
        cf_end = d_start
        d2 = cf_end - cf_start  # Condition F duration (wait time)
        d_end = d_start + d_dur
        heapq.heappush(self.active_depot, d_end)
        eventtype="CF_DE"

        # Write results back to sim_df
        self.df.sim_df.at[part_row_idx, 'micap'] = eventtype
        self.df.sim_df.at[part_row_idx, 'condition_f_end'] = cf_end
        self.df.sim_df.at[part_row_idx, 'condition_f_duration'] = d2
        self.df.sim_df.at[part_row_idx, 'depot_start'] = d_start
        self.df.sim_df.at[part_row_idx, 'depot_end'] = d_end
        self.df.sim_df.at[part_row_idx, 'depot_duration'] = d_dur
        
        # Schedule depot completion event (standard flow from here)
        self.schedule_event(d_end, 'depot_complete', sim_id)

    def _record_wip_snapshot(self): # 777
        """Record current work-in-progress counts at current_time."""
        # Get filled dataframes
        filled_sim = self.df.sim_df.iloc[:self.df.current_sim_row]
        filled_des = self.df.des_df.iloc[:self.df.current_des_row]
        
        # Count parts in each stage (parts currently active in stage)
        parts_in_fleet = len(filled_sim[
            (filled_sim['fleet_start'] <= self.current_time) & 
            (filled_sim['fleet_end'] > self.current_time)])
        
        parts_in_condition_f = len(filled_sim[
            (filled_sim['condition_f_start'] <= self.current_time) & 
            (filled_sim['condition_f_end'] > self.current_time)])
        
        parts_in_depot = len(filled_sim[
            (filled_sim['depot_start'] <= self.current_time) & 
            (filled_sim['depot_end'] > self.current_time)])
        
        parts_in_condition_a = len(self.df.condition_a_df)  # Available parts waiting
        
        # Count aircraft in each stage
        aircraft_in_fleet = len(filled_des[
            (filled_des['fleet_start'] <= self.current_time) & 
            (filled_des['fleet_end'] > self.current_time)])
        
        aircraft_in_micap = self.micap_state.count_active()
        
        # Record snapshot
        snapshot = {
            'time': self.current_time,
            'parts_fleet': parts_in_fleet,
            'parts_condition_f': parts_in_condition_f,
            'parts_depot': parts_in_depot,
            'parts_condition_a': parts_in_condition_a,
            'aircraft_fleet': aircraft_in_fleet,
            'aircraft_micap': aircraft_in_micap
        }
        
        self.wip_history.append(snapshot)



    def run(self, progress_callback=None): # , progress_callback=None 777 lone
        """
        Execute event-driven discrete-event simulation.

        Flow:
        1. Phase 1: Initialization (moved to Initialization class)
            1. Initialize first cycle (parts and aircraft start in Fleet)
            2. Initialize Condition F, Depot & COndition A Stages
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
        self.progress_callback = progress_callback
        
        # Phase 1: Initialization
        initializer = Initialization(self)
        initializer.run_initialization()
        
        # Phase 2: Schedule all initial events
        self._schedule_initial_events()

        # Record initial WIP 777
        self._record_wip_snapshot()
        
        # Phase 3: Event-driven main loop
        while self.event_heap:
            # Get next event chronologically
            event_time, _, event_type, entity_id = heapq.heappop(self.event_heap)
            
            # Stop if event exceeds simulation time limit
            if event_time > self.sim_time:
                break
            
            # Advance simulation clock
            self.current_time = event_time

            # Record WIP snapshots at regular intervals 777
            if self.current_time >= self.next_wip_time:
                self._record_wip_snapshot()
                self.next_wip_time += self.wip_snapshot_interval
            
            # Track event processing 777
            self.event_counts[event_type] = self.event_counts.get(event_type, 0) + 1
            self.event_counts['total'] += 1
            
            # Update progress UI. callback provided 777
            if self.progress_callback and self.event_counts['total'] % 100 == 0:
                self.progress_callback(event_type, self.event_counts[event_type], 
                                    self.event_counts['total'])
            
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
                # EVENT TYPE: CF_DE
            elif event_type == 'CF_DE':
                self.event_cf_de(entity_id)
                # EVENT TYPE:
            elif event_type == 'part_fleet_end':
                self.event_p_cfs_de(entity_id)
                # EVENT TYPE:
            elif event_type == 'part_condemn':
                self.event_p_condemn(entity_id)

        # Final WIP snapshot 777
        self._record_wip_snapshot()

        # Phase 4: Post-processing 777
        self.df.trim_dataframes()
        validation_results = self.df.validate_structure()
        
        # Add event counts to results 777
        validation_results['event_counts'] = self.event_counts.copy()
        # Add WIP history to results 777
        validation_results['wip_history'] = pd.DataFrame(self.wip_history)
        
        return validation_results
