import numpy as np
from scipy.special import gamma
import pandas as pd
import heapq

try:
    # Try relative imports first (when used as module)
    from .initialization import Initialization
    from .micap_state import MicapState
    from .part_manager import PartManager
    from .ac_manager import AircraftManager
    from .cond_a import ConditionAState
    from .new_part import NewPart
except ImportError:
    # Fall back to absolute imports (when run directly)
    from initialization import Initialization
    from micap_state import MicapState
    from part_manager import PartManager
    from ac_manager import AircraftManager
    from cond_a import ConditionAState
    from new_part import NewPart

class SimulationEngine:
    """
    Manages simulation logic and event processing.
    
    Works with:
    - DataFrameManager: DataFrame schemas and post-simulation data access
    - PartManager: Active part tracking with O(1) dictionary lookups
    - MicapState: MICAP queue management
    
    Contains formulas for stage durations and helper functions for event management.
    """
    
    def __init__(self, df_manager, datasets, sone_dist, sone_mean, sone_sd, sthree_dist, sthree_mean, sthree_sd, 
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
        self.micap_state = MicapState()  # Manage MICAP aircraft
        self.part_manager = PartManager() # Manage parts
        self.ac_manager = AircraftManager() # Manage Aircrafts
        self.cond_a_state = ConditionAState()  # Manage Condition A parts
        self.new_part_state = NewPart(n_total_parts=df_manager.n_total_parts)  # Manage new parts on order
        self.datasets = datasets # 456
        # Initialize MICAP state with initial conditions micap787
        if hasattr(df_manager, 'allocation'):
            self.micap_state._create_micap_df(df_manager.allocation)

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
            One of: 'depot_complete', 'fleet_complete', 'new_part_arrives', 'CF_DE', 'part_fleet_end', 'part_condemn'
        entity_id : int
            - For part events ('depot_complete', 'part_fleet_end', 'part_condemn', 'CF_DE'): sim_id from PartManager
            - For aircraft events ('fleet_complete'): des_id from AircraftManager  
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
    
    # remove: def get_next_sim_id(self): PartManager replacing
    # remove: def get_next_des_id(self): AircraftManager replacing
    
    # remove: def add_sim_event PartManager replacing
    # remove: def add_des_event AircraftManager replacing
    
    # ==========================================================================
    # HELPER FUNCTION: PROCESS NEW CYCLE STAGES (After Installation Completes)
    # ==========================================================================

    def event_acp_fs_fe(self, s4_install_end, new_sim_id, new_des_id):
        """
        EVENT: Aircraft-Part Fleet Start to Fleet End
        
        Handles fleet stage timing for both aircraft (ac_manager) and part (part_manager).
        Schedules:
        - fleet_complete (aircraft event)
        - part_fleet_end (part event to trigger CF→DE flow)
        
        Parameters
        ----------
        s4_install_end : float
            Installation end time = fleet start time
        new_sim_id : int
            Primary key for Part ID in PartManager active tracking
        new_des_id : int
            Primary key for Aircraft ID in AircraftManager active tracking
        """
        sim_id = new_sim_id
        des_id = new_des_id
        
        # Calculate fleet duration and timing
        d1 = self.calculate_fleet_duration()
        s1_start = s4_install_end
        s1_end = s1_start + d1
        
        # Update part_manager (part)
        self.part_manager.update_fields(sim_id, {
            'fleet_end': s1_end,
            'fleet_duration': d1
        })

        # Update ac_manager (aircraft)
        self.ac_manager.update_fields(des_id, {
            'fleet_duration': d1,
            'fleet_end': s1_end
        })
        
        # Schedule fleet_complete event
        self.schedule_event(s1_end, 'fleet_complete', des_id)
        
        # Schedule part_fleet_end event 
        self.schedule_event(s1_end, 'part_fleet_end', sim_id)


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
            Primery key for Part ID in PartManager active tracking
        """
        # load PART row
        active_part = self.part_manager.get_part(sim_id)
        s1_end = active_part['fleet_end']
        
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
        self.part_manager.update_fields(sim_id, {
        'condition_f_start': s2_start,
        'condition_f_end': s2_end,
        'condition_f_duration': d2
        })
        
        # --- Cycle Condemn Logic ---
        cycle = active_part['cycle']
        
        # CONDEMN PART: Cycle equals CONDEMN CYCLE
        if cycle == self.condemn_cycle:
            condemn="yes"
            # Condemned parts takes user determined rate of normal depot time
            d3 = self.calculate_depot_duration() * self.condemn_depot_fraction
            s3_end = s3_start + d3
            heapq.heappush(self.active_depot, s3_end)
            
            # Update depot info
            self.part_manager.update_fields(sim_id, {
            'condemn': condemn,
            'micap': eventtypec,
            'depot_start': s3_start,
            'depot_end': s3_end,
            'depot_duration': d3,
            })
            
            # Schedule condemn event at depot_end
            self.schedule_event(s3_end, 'part_condemn', sim_id)
            
        else:
            # NORMAL PART
            d3 = self.calculate_depot_duration()
            s3_end = s3_start + d3
            heapq.heappush(self.active_depot, s3_end)
            
            self.part_manager.update_fields(sim_id, {
            'micap': eventtypede,
            'depot_start': s3_start,
            'depot_end': s3_end,
            'depot_duration': d3,
            })

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
        # get PART row information
        active_part = self.part_manager.get_part(sim_id)
        part_id = active_part['part_id']
        depot_end_condemned = active_part['depot_end']
        
        # Get next available part_id from new_part_state
        new_part_id = self.new_part_state.get_next_part_id()
        
        # Calculate new part arrival time
        new_part_arrival_time = depot_end_condemned + self.part_order_lag
        
        # Add new part to new_part_state (cycle always 0 for new parts)
        self.new_part_state.add_new_part(
            part_id=new_part_id,
            condition_a_start=new_part_arrival_time
        )
        
        # Log condemnation event
        self.new_part_state.log_condemnation(
            old_part_id=part_id,
            depot_end=depot_end_condemned,
            new_part_id=new_part_id,
            condition_a_start=new_part_arrival_time
        )
        
        # Schedule new part arrival
        self.schedule_event(new_part_arrival_time, 'new_part_arrives', new_part_id)


    def _schedule_initial_events(self):
        """
        Schedule all initial events after initialization phase completes.
        
        Called by run() after initialization class has ran.
        
        Schedules four event types:
        1. Depot completions (parts finishing initial depot stage)
        2. Fleet completions (aircraft finishing initial fleet stage)
        3. New part arrivals (from new_part_df with condition_a_start set)
        4. Condition F starts (parts injected into Condition F queue)
        """
        # Get active parts from PartManager
        active_parts = self.part_manager.get_all_active_parts()
        
        # 1. Schedule depot completions from initialization
        for sim_id, part in active_parts.items():
            if pd.notna(part.get('depot_end')) and part.get('condemn') == 'no':
                self.schedule_event(part['depot_end'], 'depot_complete', sim_id)
        
        # 2. Schedule fleet completions from initialization (using ac_manager)
        # Under assumption no aircraft were previously processed from fleet_end to MICAP or install
        # That should not happen in initial conditions
        active_aircraft = self.ac_manager.get_all_active_ac()
        for des_id, ac in active_aircraft.items():
            if pd.notna(ac.get('fleet_end')):
                self.schedule_event(ac['fleet_end'], 'fleet_complete', des_id)
        
        # 3. Schedule new part arrivals (if any exist in new_part_state)
        active_new_parts = self.new_part_state.get_all_active()
        for part_id, part in active_new_parts.items():
            self.schedule_event(part['condition_a_start'], 'new_part_arrives', part_id)
        
        # 4. Schedule Condition F PART-EVENTS (CF_DE parts)
        for sim_id, part in active_parts.items():
            is_ic_ijcf = (part.get('micap') == 'IC_IjCF') and (part.get('condition_f_start') == 0)
            is_ic_fe_cf = (part.get('micap') == 'IC_FE_CF')  # IMPORTANT: DONT add IC_FE_CF that DONT
            
            if is_ic_ijcf or is_ic_fe_cf:
                self.schedule_event(part['condition_f_start'], 'CF_DE', sim_id)
    
    def handle_part_completes_depot(self, sim_id):
        """
        Handle the event where a part completes Depot repair.

        For the given part (identified by `sim_id`), this function determines whether
        any aircraft are currently in MICAP status and routes the part accordingly:

        - **No MICAP aircraft:** The part moves into Condition A
          and waits until triggered by an aircraft completing its Fleet stage.
        - **MICAP aircraft present:** The part is immediately installed on the earliest
          MICAP aircraft, resolving its MICAP status. Both part_manager and ac_manager
          are updated to close the current cycle, new records are created for the next cycle,
          and `event_acp_fs_fe()` advances both entities to their next event.

        Notes
        -----
        - eventtypeca="DE_CA" # part goes to Condition A
        - eventtypemi="DE_MI" # part resolves MICAP & cycle ends
        - eventtypedemicr="DE_MI_CR" # part resolves MICAP and cycle restart
        - eventtypedesmi="DE_SMI" # AC started in MICAP & resolved
        - eventtypedesmicr="DE_SMI_CR" # AC started in MICAP & cycle restart
        """
        # Get part details
        part_row = self.part_manager.get_part(sim_id)
        
        s3_end = part_row['depot_end']

        eventtypeca="DE_CA"
        eventtypemi="DE_MI"
        eventtypedemicr="DE_MI_CR"
        eventtypedesmi="DE_SMI"
        eventtypedesmicr="DE_SMI_CR"
        
        # Check if any aircraft in MICAP
        micap_pa_rm = self.micap_state.pop_and_rm_first(s3_end, event_type=eventtypemi)
        
        # CASE A1: No MICAP aircraft → Part goes to Condition A
        if micap_pa_rm is None:
            # Update PartManager with condition_a_start and micap type
            self.part_manager.update_fields(sim_id, {
                'micap': eventtypeca, 'condition_a_start': s3_end})
            
            # Add to Condition A inventory using cond_a_state
            self.cond_a_state.add_part(
                sim_id=part_row['sim_id'],
                part_id=part_row['part_id'],
                condition_a_start=s3_end
            )
        
        # CASE A2: MICAP aircraft exists → Install part directly
        else:
            first_micap = micap_pa_rm
            
            # Calculate install duration
            d4_install = self.calculate_install_duration()
            s4_install_start = s3_end
            s4_install_end = s4_install_start + d4_install
            micap_duration = s3_end - first_micap['micap_start']
            micap_end = s3_end
            
            # Check if aircraft has des_id BEFORE updating part
            has_des_id = pd.notna(first_micap['des_id'])
            
            # Determine which des_id to use for part's destwo_id
            if has_des_id:
                des_id_for_sim = first_micap['des_id']
            else:
                # Aircraft has no des_id, generate one now for MICAP resolution
                des_id_for_sim = self.ac_manager.get_next_des_id()

            # Update existing active part with install information
            self.part_manager.update_fields(sim_id, {
                'micap': eventtypemi,
                'install_duration': d4_install,
                'install_start': s4_install_start,
                'install_end': s4_install_end,
                'destwo_id': des_id_for_sim,
                'actwo_id': first_micap['ac_id']
            })
            
            # Complete the cycle for this part (logs it and removes from active)
            self.part_manager.complete_part_cycle(sim_id)
            
            # Generate IDs for new cycle
            new_sim_id = self.part_manager.get_next_sim_id()
            new_des_id_restart = self.ac_manager.get_next_des_id()
            
            # Add new part record for cycle restart
            self.part_manager.add_part(
                sim_id=new_sim_id,
                part_id=part_row['part_id'],
                cycle=part_row['cycle'] + 1,
                micap=eventtypedemicr,
                fleet_start=s4_install_end,
                desone_id=new_des_id_restart,
                acone_id=first_micap['ac_id'],
                condemn="no"
            )
            
            # Handle aircraft based on whether it had des_id
            if has_des_id:
                # Aircraft has active record, UPDATE existing then complete cycle
                self.ac_manager.update_fields(first_micap['des_id'], {
                    'micap': eventtypemi,
                    'micap_duration': micap_duration,
                    'micap_start': first_micap['micap_start'],
                    'micap_end': micap_end,
                    'install_duration': d4_install,
                    'install_start': s4_install_start,
                    'install_end': s4_install_end,
                    'simtwo_id': part_row['sim_id'],
                    'parttwo_id': part_row['part_id']
                })
                # Complete the aircraft cycle (logs it and removes from active)
                self.ac_manager.complete_ac_cycle(first_micap['des_id'])
                
                # Add new aircraft record for cycle restart
                self.ac_manager.add_ac(
                    des_id=new_des_id_restart,
                    ac_id=first_micap['ac_id'],
                    micap=eventtypedemicr,
                    fleet_start=s4_install_end,
                    simone_id=new_sim_id,
                    partone_id=part_row['part_id']
                )
            else:
                # Aircraft started in MICAP, ADD NEW record for MICAP resolution
                self.ac_manager.add_ac(
                    des_id=des_id_for_sim,
                    ac_id=first_micap['ac_id'],
                    micap=eventtypedesmi,
                    micap_duration=micap_duration,
                    micap_start=first_micap['micap_start'],
                    micap_end=micap_end,
                    install_duration=d4_install,
                    install_start=s4_install_start,
                    install_end=s4_install_end,
                    simtwo_id=part_row['sim_id'],
                    parttwo_id=part_row['part_id']
                )
                # Complete the MICAP resolution cycle
                self.ac_manager.complete_ac_cycle(des_id_for_sim)
                
                # Add new aircraft record for cycle restart
                self.ac_manager.add_ac(
                    des_id=new_des_id_restart,
                    ac_id=first_micap['ac_id'],
                    micap=eventtypedesmicr,
                    fleet_start=s4_install_end,
                    simone_id=new_sim_id,
                    partone_id=part_row['part_id']
                )
            
            # Process fleet stage for the new cycle
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
          is selected and installed immediately. Both part_manager and ac_manager are updated 
          to record installation details, new records are created for the next 
          cycle, and `event_acp_fs_fe()` advances the aircraft-part pair to 
          their next stage trigger.

        - **No parts available:** The aircraft enters MICAP status. AC added to `micap_state`

        Notes
        -----
        - eventtypeca="AFE_CA" # AC takes part from CA
        - eventtypecacr="AFE_CA_CR" # AC-PART cycle restart
        - eventtype="AFE_MICAP" # AC goes MICAP
        """
        # Get aircraft details from ac_manager (O(1) lookup)
        ac_record = self.ac_manager.get_ac(des_id)
        
        s1_end = ac_record['fleet_end']

        # EVENT TYPEs 
        eventtypeca = "AFE_CA"
        eventtypecacr = "AFE_CA_CR"
        eventtype = "AFE_MICAP"
        
        # Check if part available in Condition A
        first_available = self.cond_a_state.pop_first_available(s1_end)
        
        # CASE B1: Part Available
        if first_available is not None:
            
            # Calculate install duration
            d4_install = self.calculate_install_duration()
            s4_install_start = s1_end
            s4_install_end = s4_install_start + d4_install
            
            condition_a_end = s4_install_start
            condition_a_duration = (
                condition_a_end - first_available['condition_a_start']
            )
            
            # Get sim_id from cond_a_state record
            sim_id = first_available['sim_id']
            
            # Get cycle from part_manager (cond_a_state only stores minimal fields)
            part_record = self.part_manager.get_part(sim_id)
            cycle = part_record['cycle']
            
            # Update part with install information
            self.part_manager.update_fields(sim_id, {
                'micap': eventtypeca,
                'condition_a_duration': condition_a_duration,
                'condition_a_end': condition_a_end,
                'install_duration': d4_install,
                'install_start': s4_install_start,
                'install_end': s4_install_end,
                'destwo_id': des_id,
                'actwo_id': ac_record['ac_id']
            })
            
            # Complete the part cycle
            self.part_manager.complete_part_cycle(sim_id)
            
            # Generate IDs for cycle restart
            new_sim_id = self.part_manager.get_next_sim_id()
            new_des_id = self.ac_manager.get_next_des_id()
            
            # Add new part record for cycle restart
            self.part_manager.add_part(
                sim_id=new_sim_id,
                part_id=first_available['part_id'],
                cycle=cycle + 1,
                micap=eventtypecacr,
                fleet_start=s4_install_end,
                desone_id=new_des_id,
                acone_id=ac_record['ac_id'],
                condemn="no"
            )
            
            # Update aircraft with install information, then complete cycle
            self.ac_manager.update_fields(des_id, {
                'micap': eventtypeca,
                'install_duration': d4_install,
                'install_start': s4_install_start,
                'install_end': s4_install_end,
                'simtwo_id': first_available['sim_id'],
                'parttwo_id': first_available['part_id']
            })
            self.ac_manager.complete_ac_cycle(des_id)
            
            # Add new aircraft record for cycle restart
            self.ac_manager.add_ac(
                des_id=new_des_id,
                ac_id=ac_record['ac_id'],
                micap=eventtypecacr,
                fleet_start=s4_install_end,
                simone_id=new_sim_id,
                partone_id=first_available['part_id']
            )
            
            # Process fleet stage for the new cycle
            self.event_acp_fs_fe(
                s4_install_end=s4_install_end,
                new_sim_id=new_sim_id,
                new_des_id=new_des_id
            )
            # Part already removed from cond_a_state by pop_first_available()
        
        # CASE B2: No Parts Available → Aircraft Goes MICAP
        else:
            micap_start_time = s1_end
            
            # Add aircraft to MICAP state
            self.micap_state.add_aircraft(
                des_id=des_id,
                ac_id=ac_record['ac_id'],
                micap_type=eventtype,
                fleet_duration=ac_record['fleet_duration'],
                fleet_start=ac_record['fleet_start'],
                fleet_end=ac_record['fleet_end'],
                micap_start=micap_start_time
            )

    def handle_new_part_arrives(self, part_id):
        """
        Handle the event where a newly ordered part arrives after its order-lag

        This function determines whether any aircraft are currently in MICAP 
        status and proceeds along one of two paths:

        1. **No MICAP aircraft:** New PART is added to `condition_a_df` - end of path.
        2. **MICAP aircraft:** The part is immediately installed on the
           earliest MICAP aircraft, resolving MICAP. Both part_manager and ac_manager
           are updated to record installation details, new records are created
           for the next maintenance cycle, and `event_acp_fs_fe()` advances
           the aircraft-part pair to their next event trigger.

        Notes
        -----
        - eventtypenca="PNEW_CA" # New part goes to Condition A
        - eventtypenma="PNEW_MICAP" # New part resolves MICAP
        - eventtypenmacr="PNEW_MI_CR" # New part resolves MICAP, cycle restart
        - eventtypensmi="PNEW_SMICAP" # Resolve MICAP for AC that started in MICAP
        - eventtypensmicr="PNEW_SMI_CR" # Cycle restart for AC that started in MICAP
        """
        # Get the part's arrival info from new_part_state
        part_record = self.new_part_state.get_part(part_id)
        condition_a_start = part_record['condition_a_start']
        cycle = part_record['cycle']

        # Remove part from new_part_state
        self.new_part_state.remove_part(part_id)

        # EVENT TYPES
        eventtypenca="PNEW_CA"
        eventtypenma="PNEW_MICAP"
        eventtypenmacr="PNEW_MI_CR"
        eventtypensmi="PNEW_SMICAP"
        eventtypensmicr="PNEW_SMI_CR"

        # Check if any aircraft currently in MICAP
        micap_npa_rm = self.micap_state.pop_and_rm_first(condition_a_start, event_type=eventtypenma)
        
        # --- PATH 1: No MICAP → Part goes to cond_a_state ---
        if micap_npa_rm is None:

            # Add NEW PART event to part_manager first
            result = self.part_manager.add_initial_part(
                part_id=part_id,
                cycle=cycle,
                micap=eventtypenca,
                condition_a_start=condition_a_start,
            )
            sim_id = result['sim_id']

            # Add to Condition A inventory using cond_a_state
            self.cond_a_state.add_part(
                sim_id=sim_id,
                part_id=part_id,
                condition_a_start=condition_a_start
            )
            
            
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
            
            # Calculate MICAP duration
            micap_duration = condition_a_start - first_micap['micap_start']
            micap_end = condition_a_start
            
            # Check if aircraft has des_id BEFORE adding part record
            has_des_id = pd.notna(first_micap['des_id'])
            
            # Determine which des_id to use for part's destwo_id
            if has_des_id:
                des_id_for_sim = first_micap['des_id']
            else:
                # Aircraft has no des_id, generate one now for MICAP resolution
                des_id_for_sim = self.ac_manager.get_next_des_id()
            
            # --- Add NEW row to part_manager for cycle 0 (install event) ---
            result = self.part_manager.add_initial_part(
                part_id=part_id,
                cycle=cycle, # set in new_part_df
                micap=eventtypenma,
                condition_a_duration=condition_a_duration,
                condition_a_start=condition_a_start,
                condition_a_end=condition_a_end,
                install_duration=d4_install,
                install_start=s4_install_start,
                install_end=s4_install_end,
                destwo_id=des_id_for_sim,
                actwo_id=first_micap['ac_id']
            )
            sim_id = result['sim_id']

            # Complete the install cycle for this part
            self.part_manager.complete_part_cycle(sim_id)
            
            # Generate des_id for cycle restart
            new_des_id_restart = self.ac_manager.get_next_des_id()

            # --- Add ANOTHER row to part_manager for cycle 1 (restart) ---
            result = self.part_manager.add_initial_part(
                part_id=part_id,
                cycle=cycle + 1, # need to add cycle here
                micap=eventtypenmacr,
                fleet_start=s4_install_end,
                desone_id=new_des_id_restart,
                acone_id=first_micap['ac_id']
            )
            new_sim_id_restart = result['sim_id']# ID for cycle restart
            
            # Handle aircraft based on whether it had des_id
            if has_des_id:
                # Aircraft has active record, UPDATE existing then complete cycle
                self.ac_manager.update_fields(first_micap['des_id'], {
                    'micap': eventtypenma,
                    'micap_duration': micap_duration,
                    'micap_start': first_micap['micap_start'],
                    'micap_end': micap_end,
                    'install_duration': d4_install,
                    'install_start': s4_install_start,
                    'install_end': s4_install_end,
                    'simtwo_id': sim_id,
                    'parttwo_id': part_id
                })
                # Complete the aircraft cycle
                self.ac_manager.complete_ac_cycle(first_micap['des_id'])
                
                # Add new aircraft record for cycle restart
                self.ac_manager.add_ac(
                    des_id=new_des_id_restart,
                    ac_id=first_micap['ac_id'],
                    micap=eventtypenmacr,
                    fleet_start=s4_install_end,
                    simone_id=new_sim_id_restart,
                    partone_id=part_id
                )
            else:
                # Aircraft started in MICAP, ADD NEW record for MICAP resolution
                self.ac_manager.add_ac(
                    des_id=des_id_for_sim,
                    ac_id=first_micap['ac_id'],
                    micap=eventtypensmi,
                    micap_duration=micap_duration,
                    micap_start=first_micap['micap_start'],
                    micap_end=micap_end,
                    install_duration=d4_install,
                    install_start=s4_install_start,
                    install_end=s4_install_end,
                    simtwo_id=sim_id,
                    parttwo_id=part_id
                )
                # Complete the MICAP resolution cycle
                self.ac_manager.complete_ac_cycle(des_id_for_sim)
                
                # Add new aircraft record for cycle restart
                self.ac_manager.add_ac(
                    des_id=new_des_id_restart,
                    ac_id=first_micap['ac_id'],
                    micap=eventtypensmicr,
                    fleet_start=s4_install_end,
                    simone_id=new_sim_id_restart,
                    partone_id=part_id
                )
            
            # Process fleet stage for the new cycle
            self.event_acp_fs_fe(
                s4_install_end=s4_install_end,
                new_sim_id=new_sim_id_restart,
                new_des_id=new_des_id_restart
            )


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
        part_row = self.part_manager.get_part(sim_id)
        
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
        self.part_manager.update_fields(sim_id, {
            'micap': eventtype,
            'condition_f_duration': d2,
            'depot_duration': d_dur,
            'condition_f_end': cf_end,
            'depot_start': d_start,
            'depot_end': d_end,
        })
        
        # Schedule depot completion event (standard flow from here)
        self.schedule_event(d_end, 'depot_complete', sim_id)
    

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
        
        # Phase 3: Event-driven main loop
        while self.event_heap:
            # Get next event chronologically
            event_time, _, event_type, entity_id = heapq.heappop(self.event_heap)
            
            # Stop if event exceeds simulation time limit
            if event_time > self.sim_time:
                break
            
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

        # Convert PartManager and AircraftManager data to DataFrames for analysis
        self.datasets.build_part_ac_df(
            self.part_manager.get_all_parts_data_df,
            self.ac_manager.get_all_ac_data_df,
            self.cond_a_state.get_log_dataframe)
        
        # Build WIP from micap_state log (fast O(N) approach)
        micap_log_df = self.micap_state.get_log_dataframe()
        self.datasets.build_wip_from_micap_log(
            micap_log_df, 
            n_total_aircraft=self.df.n_total_aircraft)
        
        # Build results dictionary with event counts
        # Note: wip_df is now accessible via self.datasets.wip_df
        validation_results = {
            'event_counts': self.event_counts.copy(),
        }
        
        return validation_results
