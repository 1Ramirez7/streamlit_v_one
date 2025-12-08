"""
Simulation Engine for DES Simulation
Handles simulation logic, formulas, and event processing.
"""

import numpy as np
from scipy.special import gamma
import pandas as pd
import heapq

try:
    # Try relative imports first (when used as module)
    from .initialization import Initialization
    from .ph_micap import MicapState
    from .entity_part import PartManager
    from .entity_ac import AircraftManager
    from .ph_cda import ConditionAState
    from .ph_new_part import NewPart
    from .ds.data_science import DataSets
    from .post_sim import PostSim  # === POSTSIM CLASS - NEW ===
except ImportError:
    # Fall back to absolute imports (when run directly)
    from initialization import Initialization
    from ph_micap import MicapState
    from entity_part import PartManager
    from entity_ac import AircraftManager
    from ph_cda import ConditionAState
    from ph_new_part import NewPart
    from ds.data_science import DataSets
    from post_sim import PostSim

def append_event(current_event, new_event):
    return f"{current_event}, {new_event}"

class SimulationEngine:
    """
    Manages simulation logic and event processing.
    
    Works with:
    - PartManager: Active part tracking with O(1) dictionary lookups
    - MicapState: MICAP queue management
    
    Contains formulas for stage durations and helper functions for event management.
    """
    
    def __init__(self, params, allocation):
        """
        Initialize SimulationEngine with centralized Parameters.
        
        Args:
            datasets: DataSets instance for storing simulation outputs
            params: Parameters object with all simulation parameters
            allocation: dict with initial part/aircraft allocation
        """
        self.params = params
        self.allocation = allocation
        self.active_depot: list = []
        
        # NEW: Event-driven structures
        self.event_heap = []  # Priority queue: (time, counter, event_type, entity_id)
        self.event_counter = 0  # FIFO tie-breaker for same-time events
        self.micap_state = MicapState()  # Manage MICAP aircraft
        self.part_manager = PartManager() # Manage parts
        self.ac_manager = AircraftManager() # Manage Aircrafts
        self.cond_a_state = ConditionAState()  # Manage Condition A parts
        self.new_part_state = NewPart(n_total_parts=params['n_total_parts'])  # Manage new parts on order
        self.datasets = DataSets(warmup_periods=params['warmup_periods'], closing_periods=params['closing_periods'], sim_time=params['sim_time'], use_buffer=params.get('use_buffer', False))

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
        if self.params['sone_dist'] == "Normal":
            return max(0, np.random.normal(self.params['sone_mean'], self.params['sone_sd']))
        elif self.params['sone_dist'] == "Weibull":
            return max(0, np.random.weibull(self.params['sone_mean']) * self.params['sone_sd'])
    
    def calculate_depot_duration(self):
        """
        Calculates distribution for length of stage based on chosen distribution:
        Normal or Weibull
        """
        if self.params['sthree_dist'] == "Normal":
            return max(0, np.random.normal(self.params['sthree_mean'], self.params['sthree_sd']))
        elif self.params['sthree_dist'] == "Weibull":
            return max(0, np.random.weibull(self.params['sthree_mean']) * self.params['sthree_sd'])
    
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
        
        # EVENT TYPES logic
        eventtype_cfs_cfe = "CFS_CFE"
        eventtype_ds_de_condemn="DS_DE_CONDEMN" # part is condemn
        eventtype_ds_de="DS_DE"

        current_event = active_part['event_path']

        new_event = eventtype_cfs_cfe # event 1
        add_event_cfs_cfe = append_event(current_event, new_event)

        new_event = eventtype_ds_de_condemn  # event 2
        add_event_dsdecondemn = append_event(add_event_cfs_cfe, new_event)

        new_event = eventtype_ds_de # event 3
        add_event_ds_de = append_event(add_event_cfs_cfe, new_event)

        
        # pre-Calculate depot_start given DEPOT CONSTRAINT is satisfy
        if len(self.active_depot) < self.params['depot_capacity']:
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
            'event_path': add_event_cfs_cfe,
            'condition_f_start': s2_start,
            'condition_f_end': s2_end,
            'condition_f_duration': d2
        })
        
        # --- Cycle Condemn Logic ---
        cycle = active_part['cycle']
        
        # CONDEMN PART: Cycle equals CONDEMN CYCLE
        if cycle == self.params['condemn_cycle']:
            condemn="yes"
            # Condemned parts takes user determined rate of normal depot time
            d3 = self.calculate_depot_duration() * self.params['condemn_depot_fraction']
            s3_end = s3_start + d3
            heapq.heappush(self.active_depot, s3_end)
            
            # Update depot info
            self.part_manager.update_fields(sim_id, {
            'condemn': condemn,
            'event_path': add_event_dsdecondemn,
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
            'event_path': add_event_ds_de,
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
        new_part_arrival_time = depot_end_condemned + self.params['part_order_lag']
        
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
            is_ic_ijcf = (part.get('event_path') == 'IC_IjCF') and (part.get('condition_f_start') == 0)
            is_ic_fe_cf = (part.get('event_path') == 'IC_IZ_FS_FE, IC_FE_CF')  # IMPORTANT: DONT add IC_IZ_FS_FE, IC_FE_CF that DONT 
            
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
        - eventtypemi="DE_DMR_IE" # part resolves MICAP & cycle ends
        - eventtypedemicr="DMR_CR_FS_FE" # part resolves MICAP and cycle restart
        """
        # Get part details
        part_row = self.part_manager.get_part(sim_id)
        
        s3_end = part_row['depot_end']

        eventtypeca="DE_CA"
        eventtypemi="DE_DMR_IE"
        eventtype_mac="ME_DMR_IE"
        eventtypedemicr="DMR_CR_FS_FE"
        
        # Check if any aircraft in MICAP
        micap_pa_rm = self.micap_state.pop_and_rm_first(s3_end)
        
        # CASE A1: No MICAP aircraft → Part goes to Condition A
        if micap_pa_rm is None:
            # Update PartManager with condition_a_start and micap type
            current_event = part_row['event_path'] 
            new_event = eventtypeca
            add_event = append_event(current_event, new_event)
            
            self.part_manager.update_fields(sim_id, {
                'event_path': add_event, 'condition_a_start': s3_end})
            
            # Add to Condition A inventory using cond_a_state
            self.cond_a_state.add_part(
                sim_id=part_row['sim_id'],
                part_id=part_row['part_id'],
                event_path=add_event,
                condition_a_start=s3_end
            )
        
        # CASE A2: MICAP aircraft exists → Install part directly
        else:
            first_micap = micap_pa_rm
            
            current_event = part_row['event_path'] 
            new_event = eventtypemi
            add_event_p = append_event(current_event, new_event)
            # Calculate install duration
            d4_install = 0
            s4_install_start = s3_end
            s4_install_end = s4_install_start + d4_install
            micap_duration = s3_end - first_micap['micap_start']
            micap_end = s3_end
            
            # Update existing active part with install information
            self.part_manager.update_fields(sim_id, {
                'event_path': add_event_p,
                'install_duration': d4_install,
                'install_start': s4_install_start,
                'install_end': s4_install_end,
                'destwo_id': first_micap['des_id'],
                'actwo_id': first_micap['ac_id']
            })
            
            # Complete the cycle for this part (logs it and removes from active)
            self.part_manager.complete_part_cycle(sim_id)
            
            # Generate IDs for new cycle
            new_sim_id = self.part_manager.get_next_sim_id()
            new_des_id = self.ac_manager.get_next_des_id()
            
            # Add new part record for cycle restart
            self.part_manager.add_part(
                sim_id=new_sim_id,
                part_id=part_row['part_id'],
                cycle=part_row['cycle'] + 1,
                event_path=eventtypedemicr,
                fleet_start=s4_install_end,
                desone_id=new_des_id,
                acone_id=first_micap['ac_id'],
                condemn="no"
            )

            # UPDATE existing aircraft record then complete cycle
            current_event = first_micap['event_path']
            new_event = eventtype_mac
            add_event = append_event(current_event, new_event)

            self.ac_manager.update_fields(first_micap['des_id'], {
                'event_path': add_event,
                'micap_duration': micap_duration,
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
                des_id=new_des_id,
                ac_id=first_micap['ac_id'],
                event_path=eventtypedemicr,
                fleet_start=s4_install_end,
                simone_id=new_sim_id,
                partone_id=part_row['part_id']
            )

            # Process fleet stage for the new cycle
            self.event_acp_fs_fe(
                s4_install_end=s4_install_end,
                new_sim_id=new_sim_id,
                new_des_id=new_des_id
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
        - eventtypeca="CAE_IE" # AC takes part from CA
        - eventtypecacr="CAE_IE_CR" # AC-PART cycle restart
        - eventtype="FE_MS" # AC goes MICAP
        """
        # Get aircraft details from ac_manager (O(1) lookup)
        ac_record = self.ac_manager.get_ac(des_id)
        
        s1_end = ac_record['fleet_end']

        # EVENT TYPEs 
        eventtypeca = "CAE_IE"
        eventtype_ac = "FE_IE"
        eventtypecacr = "CAP_CR_FS_FE"
        eventtype = "FE_MS" 
        
        # Check if part available in Condition A
        first_available = self.cond_a_state.pop_first_available(s1_end)
        
        # CASE B1: Part Available
        if first_available is not None:
            
            # Calculate install duration
            d4_install = 0
            s4_install_start = s1_end
            s4_install_end = s4_install_start + d4_install
            
            condition_a_end = s4_install_start
            condition_a_duration = (
                condition_a_end - first_available['condition_a_start'])
            
            # Get sim_id from cond_a_state record
            sim_id = first_available['sim_id']
            
            # Get cycle from part_manager (cond_a_state only stores minimal fields)
            part_record = self.part_manager.get_part(sim_id)
            cycle = part_record['cycle']

            current_event = part_record['event_path'] # part CAE_IE
            new_event = eventtypeca 
            add_event = append_event(current_event, new_event)
            
            # Update part with install information
            self.part_manager.update_fields(sim_id, {
                'event_path': add_event,
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
                event_path=eventtypecacr,
                fleet_start=s4_install_end,
                desone_id=new_des_id,
                acone_id=ac_record['ac_id'],
                condemn="no"
            )
            
            current_event = ac_record['event_path'] # AIRCRAFT FE_IE
            new_event = eventtype_ac
            add_event = append_event(current_event, new_event)

            # Update aircraft with install information, then complete cycle
            self.ac_manager.update_fields(des_id, {
                'event_path': add_event,
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
                event_path=eventtypecacr,
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

            current_event = ac_record['event_path']
            new_event = eventtype
            add_event = append_event(current_event, new_event)

            self.ac_manager.update_fields(des_id, {
                'event_path': add_event,
                'micap_start': micap_start_time
            })
            
            # Add aircraft to MICAP state
            self.micap_state.add_aircraft(
                des_id=des_id,
                ac_id=ac_record['ac_id'],
                event_path=add_event,
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
        - eventtypenca="NP_CA" # New part goes to Condition A
        - eventtypenma="NP_NMR_IE" # New part resolves MICAP
        - eventtype="ME_NMR_IE" # Micap resolved by new part
        - eventtypenmacr="NMR_CR_FS_FE" # New part resolves MICAP, cycle restart

        """
        # Get the part's arrival info from new_part_state
        part_record = self.new_part_state.get_part(part_id)
        condition_a_start = part_record['condition_a_start']
        cycle = part_record['cycle']

        # Remove part from new_part_state
        self.new_part_state.remove_part(part_id)

        # EVENT TYPES
        eventtypenca="NP_CA"
        eventtypenma="NP_NMR_IE"
        eventtype="ME_NMR_IE"
        eventtypenmacr="NMR_CR_FS_FE"

        # Check if any aircraft currently in MICAP
        micap_npa_rm = self.micap_state.pop_and_rm_first(condition_a_start)
        
        # --- PATH 1: No MICAP → Part goes to cond_a_state ---
        if micap_npa_rm is None:

            # Add NEW PART event to part_manager first
            result = self.part_manager.add_initial_part(
                part_id=part_id,
                cycle=cycle,
                event_path=eventtypenca,
                condition_a_start=condition_a_start,
            )
            sim_id = result['sim_id']

            # Add to Condition A inventory using cond_a_state
            self.cond_a_state.add_part(
                sim_id=sim_id,
                part_id=part_id,
                event_path=eventtypenca,
                condition_a_start=condition_a_start
            )
            
            
        # --- PATH 2: MICAP exists → Install directly ---
        else:
            # Use micap info fetch in micap_npa_rm.
            first_micap = micap_npa_rm # do i need this if replace first_micap with micap_npa_rm
            
            # Calculate install timing
            d4_install = 0
            s4_install_start = condition_a_start
            s4_install_end = s4_install_start + d4_install
            
            # Calculate condition_a_duration (time part waited)
            condition_a_end = s4_install_start
            condition_a_duration = condition_a_end - condition_a_start
            
            # Calculate MICAP duration
            micap_duration = condition_a_start - first_micap['micap_start']
            micap_end = condition_a_start
            
            # --- Add NEW row to part_manager for cycle 0 (install event) ---
            result = self.part_manager.add_initial_part(
                part_id=part_id,
                cycle=cycle, # set in new_part_df
                event_path=eventtypenma,
                condition_a_duration=condition_a_duration,
                condition_a_start=condition_a_start,
                condition_a_end=condition_a_end,
                install_duration=d4_install,
                install_start=s4_install_start,
                install_end=s4_install_end,
                destwo_id=first_micap['des_id'],
                actwo_id=first_micap['ac_id']
            )
            sim_id = result['sim_id']

            # Complete the install cycle for this part
            self.part_manager.complete_part_cycle(sim_id)
            
            # Generate des_id for cycle restart
            new_des_id = self.ac_manager.get_next_des_id()

            # --- Add ANOTHER row to part_manager for cycle 1 (restart) ---
            result = self.part_manager.add_initial_part(
                part_id=part_id,
                cycle=cycle + 1, # need to add cycle here
                event_path=eventtypenmacr,
                fleet_start=s4_install_end,
                desone_id=new_des_id,
                acone_id=first_micap['ac_id']
            )
            new_sim_id = result['sim_id']# SIM ID for cycle restart
            
            current_event = first_micap['event_path']
            new_event = eventtype
            add_event = append_event(current_event, new_event)

            # update aircraft and end cycle 
            self.ac_manager.update_fields(first_micap['des_id'], {
                'event_path': add_event,
                'micap_duration': micap_duration,
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
                des_id=new_des_id,
                ac_id=first_micap['ac_id'],
                event_path=eventtypenmacr,
                fleet_start=s4_install_end,
                simone_id=new_sim_id,
                partone_id=part_id
            )
            
            # Process fleet stage for the new cycle
            self.event_acp_fs_fe(
                s4_install_end=s4_install_end,
                new_sim_id=new_sim_id,
                new_des_id=new_des_id
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
        if part_row['event_path'] == 'IC_IjCF':
            assert part_row['condition_f_start'] == 0, \
                f"IC_IjCF event must have condition_f_start=0, got {part_row['condition_f_start']}"
        elif part_row['event_path'] == 'IC_IZ_FS_FE, IC_FE_CF':
            pass
        else:
            raise AssertionError(f"Expected IC_IjCF or IC_IZ_FS_FE, IC_FE_CF event, got {part_row['event_path']}")
        
        cf_start = part_row['condition_f_start']
        
        # --- Depot queue logic ---
        d_dur = self.calculate_depot_duration()
        if len(self.active_depot) < self.params['depot_capacity']:
            d_start = cf_start
        else:
            earliest = heapq.heappop(self.active_depot)
            d_start = max(cf_start, earliest)
        
        cf_end = d_start
        d2 = cf_end - cf_start  # Condition F duration (wait time)
        d_end = d_start + d_dur
        heapq.heappush(self.active_depot, d_end)
        eventtype="CF_DE"

        # update event info 
        current_event = part_row['event_path'] # part conditoon_f to depot_end
        new_event = eventtype 
        add_event = append_event(current_event, new_event)
        # Write results back to sim_df
        self.part_manager.update_fields(sim_id, {
            'event_path': add_event,
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
            if event_time > self.params['sim_time']:
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
            get_all_parts_data_df=self.part_manager.get_all_parts_data_df,
            get_ac_df_func=self.ac_manager.get_all_ac_data_df,
            get_wip_end=self.part_manager.get_wip_end,
            get_wip_raw=self.part_manager.get_wip_raw,
            get_wip_ac_end=self.ac_manager.get_wip_ac_end,
            get_wip_ac_raw=self.ac_manager.get_wip_ac_raw,
            sim_time=self.params['sim_time'],
        )
        self.datasets.filter_by_remove_days()
        
        # === POSTSIM CLASS - NEW ===
        # Create PostSim to compute all stats and figures inside engine.run()
        # This ensures multi-scenario runs always get correct results tied to this run's params
        post_sim = PostSim(
            datasets=self.datasets,
            event_counts=self.event_counts.copy(),
            params=self.params,
            allocation=self.allocation
        )
        
        # Build results dictionary with event counts, datasets, and post_sim
        # datasets is created fresh each run() call - prevents cache data in multi-scenario runs
        validation_results = {
            'event_counts': self.event_counts.copy(),
            'datasets': self.datasets,
            'post_sim': post_sim
        }
        
        # i'll re add clear code later
        
        return validation_results
