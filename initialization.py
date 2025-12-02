"""
Initialization Module for Initial Conditions

Contains initialization logic for the simulation initial conditions phase.
"""
import numpy as np
import pandas as pd
import heapq

class Initialization:
    """
    Handles initialization of the simulation state.
    
    Contains methods to set up initial Fleet cycles, Condition F, 
    Depot injections, and Depot queue processing.
    """
    def __init__(self, sim_engine):
        """
        Connect the initialization workflow to the active SimulationEngine.

        Args:
            sim_engine: The SimulationEngine instance that owns all event
                handlers, timing functions, and the shared DataFrameManager.

        This stores:
            self.engine: Used to call add_sim_event, add_des_event, duration
                formulas, ID generators, and to access depot queue state.
            self.df: Direct reference to sim_engine.df so initialization
                functions can modify sim_df, des_df and condition_a_df during warmup.
        """
        self.engine = sim_engine
        self.df = sim_engine.df

    def run_initialization(self):
        """
        Execute the full warmup sequence that builds all initial simulation state.

        Steps performed:
            1. event_ic_izfs:
                Calculate Depot Duration all aircraft-part pairs, record time.

            2. event_ic_ijcf:
                Insert parts that begin in Condition F.

            3. event_ic_ijca:
                Insert parts that begin in Condition A and add them to inventory.

            4. eventm_ic_izca_cr:
                Immediately resolve MICAP aircraft using available Condition A parts.
                Installs parts, closes MICAP rows, and starts next cycles where needed.

            5. event_ic_ijd:
                Preload parts directly into Depot with known cycles.
        """
        # Phase 1: Initialization
        self.event_ic_izfs()

        self.event_ic_ijd() # MUST: done before init depot

        self.event_ic_ijcf() # MUST: dont after inject depot parts
       
        self.event_ic_ijca() # must be done after inject_cond_f

        self.eventm_ic_izca_cr()

        self.eventm_ic_fe_cf() # 


    # ------------------------------------------- 1 --------------------------------------------------
    def event_ic_izfs(self):
        """
        Initialize the first Fleet cycle for all aircraft–part pairs.

        Generates the initial Fleet stage for every aircraft-part pair using
        f_start_ac_part_ids from allocation. Populates both PartManager (parts)
        and AircraftManager (aircraft) to establish baseline records before
        the main simulation loop begins.

        Notes
        -----
        - Uses f_start_ac_part_ids where ac_id = part_id (same value from list)
        - Generates sim_id and des_id using respective managers
        - No longer depends on aircraft_df
        """
        # Get list of aircraft-part IDs from allocation
        f_start_ac_part_ids = self.df.allocation['f_start_ac_part_ids']
        
        for entity_id in f_start_ac_part_ids:
            # entity_id is both ac_id and part_id for fleet start pairs
            ac_id = entity_id
            part_id = entity_id
            
            # Generate IDs from managers FIRST
            sim_id = self.engine.part_manager.get_next_sim_id()
            des_id = self.engine.ac_manager.get_next_des_id()
            
            # Calculate Fleet duration & optionally randomize duration per user settings
            d1_base = self.engine.calculate_fleet_duration()
            if self.engine.use_fleet_rand:
                random_multiplier = np.random.uniform(self.engine.fleet_rand_min, self.engine.fleet_rand_max)
            else:
                random_multiplier = 1.0
            d1 = d1_base * random_multiplier
            eventtype = "IC_IZFS"
            
            # Timing calculations
            s1_start = self.engine.calculate_fleet_duration()  # So not all aircraft start at sim day 1
            s1_start = 0  # So not all aircraft start at sim day 1
            s1_end = s1_start + d1

            # Randomize cycle for steady-state initialization
            initial_cycle = np.random.randint(1, self.engine.condemn_cycle)
            
            # Add to PartManager using add_part
            self.engine.part_manager.add_part(
                sim_id=sim_id,
                part_id=part_id,
                cycle=initial_cycle,
                micap=eventtype,
                fleet_start=s1_start,
                fleet_end=s1_end,
                fleet_duration=d1,
                desone_id=des_id,  # Foreign key to ac_manager
                acone_id=ac_id,
                condemn="no"
            )
            
            # Add to AircraftManager using add_ac
            self.engine.ac_manager.add_ac(
                des_id=des_id,
                ac_id=ac_id,
                micap=eventtype,
                fleet_duration=d1,
                fleet_start=s1_start,
                fleet_end=s1_end,
                simone_id=sim_id,  # Foreign key to part_manager
                partone_id=part_id
            )
    

    # ------------------------------------------- 2 --------------------------------------------------
    def event_ic_ijd(self):
        """
        Manually preload parts into Depot before the warm-up.
        Initialize depot with parts from utils.calculate_initial_allocation().
        Uses depot_part_ids for both sim_id and part_id,
        and assigns cycles from depot_cycles.
        """
        depot_part_ids = self.df.allocation['depot_part_ids']
        depot_cycles = self.df.allocation['depot_cycles']

        for part_id, cycle in zip(depot_part_ids, depot_cycles):
            s3_start = 0.0
            d3_base = self.engine.calculate_depot_duration()
            if self.engine.use_depot_rand:
                random_multiplier = np.random.uniform(self.engine.depot_rand_min, self.engine.depot_rand_max)
            else:
                random_multiplier = 1.0
            d3 = d3_base * random_multiplier
            s3_end = s3_start + d3
            eventtype = "IC_IjD"

            # spacer
            self.engine.part_manager.add_initial_part(
                part_id=part_id,
                cycle=cycle, # randomizing cycle
                micap=eventtype,
                depot_start=s3_start,
                depot_end=s3_end,
                depot_duration=d3
            )
            heapq.heappush(self.engine.active_depot, s3_end)

            # parts here progress in event calendar


    # ------------------------------------------- 3 --------------------------------------------------
    def event_ic_ijcf(self):
        """
        Initialize parts starting in Condition F queue.
        These parts are waiting for depot capacity to open up.

        uses function: utils.calculate_initial_allocation().
        Uses cond_f_part_ids for both sim_id and part_id,
        and assigns cycles from cond_f_cycles.
        """
        cond_f_part_ids = self.df.allocation['cond_f_part_ids']
        #cond_f_part_ids = list(range(23, 11)) # testing mismatch code
        cond_f_cycles = self.df.allocation['cond_f_cycles']
        assert len(cond_f_part_ids) == len(cond_f_cycles), "Mismatch in Condition F part_ids and cycles"

        for part_id, cycle in zip(cond_f_part_ids, cond_f_cycles):
            s2_start = 0
            eventtype = "IC_IjCF"

            # Add Condition F event
            self.engine.part_manager.add_initial_part(
                part_id=part_id,
                cycle=cycle,  # randomizing cycle
                micap=eventtype,
                condition_f_start=s2_start
            )


    # ------------------------------------------- 4 --------------------------------------------------
    def event_ic_ijca(self):
        """
        Initialize parts starting in Condition A.

        uses function: utils.calculate_initial_allocation().
        Uses cond_a_part_ids for both sim_id and part_id,
        and assigns cycles from cond_a_cycles.
        
        Adds parts to part_manager and cond_a_state.
        """
        cond_a_part_ids = self.df.allocation['cond_a_part_ids']
        #cond_f_part_ids = list(range(23, 11)) # testing mismatch code
        cond_a_cycles = self.df.allocation['cond_a_cycles']
        assert len(cond_a_part_ids) == len(cond_a_cycles), "Mismatch in Condition A part_ids and cycles"

        for part_id, cycle in zip(cond_a_part_ids, cond_a_cycles):
            ca_start = 0
            eventtype = "IC_IjCA"

            # Add Condition A event to part_manager
            result = self.engine.part_manager.add_initial_part(
                part_id=part_id,
                cycle=cycle,  # randomizing cycle
                micap=eventtype,
                condition_a_start=ca_start
            )
            
            sim_id = result['sim_id']
            
            # Add to Condition A inventory using cond_a_state
            self.engine.cond_a_state.add_part(
                sim_id=sim_id,
                part_id=part_id,
                condition_a_start=ca_start
            )


    # ------------------------------------------- 5 --------------------------------------------------
    def eventm_ic_izca_cr(self):
        """
        Process parts from cond_a_state during initial conditions phase.
        This function should be called AFTER event_ic_izfs and 
        BEFORE _schedule_initial_events in the run() method.
        
        While parts exist in cond_a_state:
        1. Check if MICAP exists
        2. If no MICAP: stop (remaining parts stay in cond_a_state)
        3. If MICAP exists: Install part immediately and advance to next cycle

        - MICAP aircraft that get resolved will be advanced to fleet_end
        """
        eventtype="IC_MICAP"
        
        # Keep processing while both MICAP aircraft and Condition A parts exist
        while self.engine.cond_a_state.count_active() > 0:
            # Check if MICAP exists FIRST
            if self.engine.micap_state.count_active() == 0:
                break  # No MICAP aircraft, stop processing
            
            # Pop first available part from cond_a_state
            first_part = self.engine.cond_a_state.pop_first_available(current_time=0)
            
            if first_part is None:
                break
            
            sim_id = first_part['sim_id']
            part_id = first_part['part_id']
            condition_a_start = first_part['condition_a_start']
            
            # Get cycle from part_manager
            part_record = self.engine.part_manager.get_part(sim_id)
            cycle = part_record['cycle']
            
            # Pop MICAP aircraft (we already confirmed one exists)
            micap_pa_rm = self.engine.micap_state.pop_and_rm_first(condition_a_start, event_type=eventtype)
            
            # --- PATH 2: MICAP exists ---
            first_micap = micap_pa_rm
            
            # Calculate install timing
            d4_install = self.engine.calculate_install_duration()
            s4_install_start = condition_a_start
            s4_install_end = s4_install_start + d4_install
            
            # Calculate condition_a_duration (should be 0 or condition_a_start set by init cond)
            condition_a_end = s4_install_start
            condition_a_duration = condition_a_end - condition_a_start
            
            # Generate des_id for MICAP resolution using ac_manager
            des_id_for_sim = self.engine.ac_manager.get_next_des_id()
            
            # Update the existing active part with install information
            self.engine.part_manager.update_fields(sim_id, {
                'condition_a_end': condition_a_end,
                'condition_a_duration': condition_a_duration,
                'install_start': s4_install_start,
                'install_end': s4_install_end,
                'install_duration': d4_install,
                'destwo_id': des_id_for_sim,
                'actwo_id': first_micap['ac_id']
            })
            
            # Complete the cycle for this part (logs it and removes from active)
            self.engine.part_manager.complete_pca_cycle(sim_id, part_id)
            
            # Handle aircraft: Aircraft started in MICAP, ADD NEW row
            micap_duration = condition_a_start - first_micap['micap_start']
            micap_end = condition_a_start
            # Add aircraft event for MICAP resolution using ac_manager
            self.engine.ac_manager.add_ac(
                des_id=des_id_for_sim,
                ac_id=first_micap['ac_id'],
                micap=first_micap['micap'],
                micap_duration=micap_duration,
                micap_start=first_micap['micap_start'],
                micap_end=micap_end,
                install_duration=d4_install,
                install_start=s4_install_start,
                install_end=s4_install_end,
                simtwo_id=sim_id,
                parttwo_id=part_id
            )
            # Complete the cycle for this Aircraft (logs it and removes from active)
            self.engine.ac_manager.complete_ac_cycle(des_id_for_sim)

            # Generate IDs for cycle restart (cycle + 1)
            new_sim_id_restart = self.engine.part_manager.get_next_sim_id()
            new_des_id_restart = self.engine.ac_manager.get_next_des_id()
            
            # Fleet Calculation
            d1 = self.engine.calculate_fleet_duration()
            s1_start = s4_install_end
            s1_end = s1_start + d1

            # --- Add row to PartManager for cycle + 1 (restart) ---
            self.engine.part_manager.add_part(
                sim_id=new_sim_id_restart,
                part_id=part_id,
                cycle=cycle + 1,
                micap='IC_CRCA',
                fleet_start=s4_install_end,
                fleet_end=s1_end,
                fleet_duration=d1,
                desone_id=new_des_id_restart,
                acone_id=first_micap['ac_id'],
                condemn='no'
            )
            
            # Add aircraft event for cycle restart using ac_manager
            eventtype_restart = "IC_CRCA_A"
            self.engine.ac_manager.add_ac(
                des_id=new_des_id_restart,
                ac_id=first_micap['ac_id'],
                micap=eventtype_restart,
                fleet_duration=d1,
                fleet_start=s4_install_end,
                fleet_end=s1_end,
                simone_id=new_sim_id_restart,
                partone_id=part_id,
            )


    # ------------------------------------------- 6 --------------------------------------------------
    def eventm_ic_fe_cf(self):
        """
        This can be a sole function made to move parts from FE to CF, not limited to ICs.
        EVENT TYPE: IC_FE_CF

        Set Condition F start-times for all parts that arrive at fleet_end during INITIALIZATION
        - Must be ran last or at after all parts that can reach fleet_end, have done so.

        INITIALIZE parts FLEET_END to CF_START
        - from fleet start: IC_IZFS 
        - from CA_start > resolve micap > fleet end: IC_CRCA

        Since adding the part_manager class, need a better way to keep track of fe-cf parts
        if we do decide to use this as a sole function, since this is using the temp micap event tracking
        """
        # Get all active parts from PartManager
        active_parts = self.engine.part_manager.get_all_active_parts()
        
        # push IC_IZFS & IC_CRCA from fleet_end to CF_Start
        valid_parts = []
        for sim_id, part in active_parts.items():
            if part['micap'] in ['IC_IZFS', 'IC_CRCA']:
                valid_parts.append(part)
        
        # Sort by fleet_end. Maintain chronological order
        valid_parts.sort(key=lambda x: x['fleet_end'] if pd.notna(x['fleet_end']) else float('inf'))
        
        eventtype = "IC_FE_CF"
        
        for part in valid_parts:
            sim_id = part['sim_id']
            s1_end = part['fleet_end']
            
            # Update fields using PartManager
            self.engine.part_manager.update_fields(sim_id, {
                'micap': eventtype,
                'condition_f_start': s1_end
            })

# spacer

