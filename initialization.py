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
                functions can modify sim_df, des_df, condition_a_df,
                and aircraft_df during warmup.
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
            
            # Calculate Fleet duration & optionally randomize duration per user settings
            d1_base = self.engine.calculate_fleet_duration()
            if self.engine.use_fleet_rand:
                random_multiplier = np.random.uniform(self.engine.fleet_rand_min, self.engine.fleet_rand_max)
            else:
                random_multiplier = 1.0
            d1 = d1_base * random_multiplier
            eventtypepart = "IC_IZFS"
            eventtypeac = "IC_IZFS"
            
            # Timing calculations
            s1_start = self.engine.calculate_fleet_duration()  # So not all aircraft start at sim day 1
            s1_start = 0  # So not all aircraft start at sim day 1
            s1_end = s1_start + d1

            # Randomize cycle for steady-state initialization
            initial_cycle = np.random.randint(1, self.engine.condemn_cycle)
            
            # Add to sim_df using helper function
            # Note: ac_row['sim_id'] and ac_row['des_id'] are the IDs from aircraft_df
            self.engine.add_sim_event(
                sim_id=ac_row['sim_id'],
                part_id=ac_row['part_id'],
                desone_id=ac_row['des_id'],  # Foreign key to des_df
                acone_id=ac_row['ac_id'],
                micap=eventtypepart,
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
                cycle=initial_cycle, # randomizing cycle
                condemn="no"
            )
            # Note: add_sim_event increments current_sim_row automatically
            
            # Add to des_df using helper function
            self.engine.add_des_event(
                des_id=ac_row['des_id'],
                ac_id=ac_row['ac_id'],
                micap=eventtypeac,
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
            self.engine.add_sim_event(
                sim_id=part_id,
                part_id=part_id,
                desone_id=None,
                acone_id=None,
                micap=eventtype,
                fleet_duration=np.nan,
                condition_f_duration=np.nan,
                depot_duration=d3,
                condition_a_duration=np.nan,
                install_duration=np.nan,
                fleet_start=np.nan,
                fleet_end=np.nan,
                condition_f_start=np.nan,
                condition_f_end=np.nan,
                depot_start=s3_start,
                depot_end=s3_end,
                destwo_id=None,
                actwo_id=None,
                condition_a_start=np.nan,
                condition_a_end=np.nan,
                install_start=np.nan,
                install_end=np.nan,
                cycle=cycle, # randomizing cycle
                condemn="no"
            )
            heapq.heappush(self.engine.active_depot, s3_end)


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
            self.engine.add_sim_event(
                sim_id=part_id,
                part_id=part_id,
                desone_id=None,
                acone_id=None,
                micap=eventtype,
                fleet_duration=np.nan,
                condition_f_duration=np.nan,
                depot_duration=np.nan,
                condition_a_duration=np.nan,
                install_duration=np.nan,
                fleet_start=np.nan,
                fleet_end=np.nan,
                condition_f_start=s2_start,
                condition_f_end=np.nan,
                depot_start=np.nan,
                depot_end=np.nan,
                destwo_id=None,
                actwo_id=None,
                condition_a_start=np.nan,
                condition_a_end=np.nan,
                install_start=np.nan,
                install_end=np.nan,
                cycle=cycle,  # randomizing cycle
                condemn="no"
            )


    # ------------------------------------------- 4 --------------------------------------------------
    def event_ic_ijca(self):
        """
        Initialize parts starting in Condition A.

        uses function: utils.calculate_initial_allocation().
        Uses cond_a_part_ids for both sim_id and part_id,
        and assigns cycles from cond_a_cycles.
        
        Adds parts to both sim_df and condition_a_df.
        """
        cond_a_part_ids = self.df.allocation['cond_a_part_ids']
        #cond_f_part_ids = list(range(23, 11)) # testing mismatch code
        cond_a_cycles = self.df.allocation['cond_a_cycles']
        assert len(cond_a_part_ids) == len(cond_a_cycles), "Mismatch in Condition A part_ids and cycles"

        for part_id, cycle in zip(cond_a_part_ids, cond_a_cycles):
            ca_start = 0
            eventtype = "IC_IjCA"

            # Add Condition A event to sim_df
            self.engine.add_sim_event(
                sim_id=part_id,
                part_id=part_id,
                desone_id=None,
                acone_id=None,
                micap=eventtype,
                fleet_duration=np.nan,
                condition_f_duration=np.nan,
                depot_duration=np.nan,
                condition_a_duration=np.nan,
                install_duration=np.nan,
                fleet_start=np.nan,
                fleet_end=np.nan,
                condition_f_start=np.nan,
                condition_f_end=np.nan,
                depot_start=np.nan,
                depot_end=np.nan,
                destwo_id=None,
                actwo_id=None,
                condition_a_start=ca_start,
                condition_a_end=np.nan,
                install_start=np.nan,
                install_end=np.nan,
                cycle=cycle,  # randomizing cycle
                condemn="no"
            )
            
            # Also add to condition_a_df for inventory tracking
            new_cond_a_row = pd.DataFrame([{
                'sim_id': part_id,
                'part_id': part_id,
                'desone_id': None,
                'acone_id': None,
                'micap': eventtype,
                'fleet_duration': np.nan,
                'condition_f_duration': np.nan,
                'depot_duration': np.nan,
                'condition_a_duration': np.nan,
                'install_duration': np.nan,
                'fleet_start': np.nan,
                'fleet_end': np.nan,
                'condition_f_start': np.nan,
                'condition_f_end': np.nan,
                'depot_start': np.nan,
                'depot_end': np.nan,
                'destwo_id': None,
                'actwo_id': None,
                'condition_a_start': ca_start,
                'condition_a_end': np.nan,
                'install_start': np.nan,
                'install_end': np.nan,
                'cycle': cycle,
                'condemn': "no"
            }])
            
            self.df.condition_a_df = pd.concat(
                [self.df.condition_a_df, new_cond_a_row], 
                ignore_index=True
            )


    # ------------------------------------------- 5 --------------------------------------------------
    def eventm_ic_izca_cr(self):
        """
        Process parts from condition_a_df during initial conditions phase.
        This function should be called AFTER event_ic_izfs and 
        BEFORE _schedule_initial_events in the run() method.
        
        For each part in condition_a_df:
        1. Check if MICAP exists
        2. If no MICAP: do nothing (part stays in condition_a_df, will be handled by normal events)
        3. If MICAP exists: Install part immediately and advance to next cycle

        - MICAP aircraft that get resolved will be advanced to fleet_end
        """
        # Get list of part_ids BEFORE starting the loop
        part_ids = self.df.condition_a_df['part_id'].tolist()

        eventtype="IC_MICAP"
        
        # Now iterate over part_ids (not the dataframe)
        for part_id in part_ids:
            # Get the part's info from condition_a_df
            part_row = self.df.condition_a_df[self.df.condition_a_df['part_id'] == part_id].iloc[0]
            condition_a_start = part_row['condition_a_start']
            cycle = part_row['cycle']
            
            # applying micap class 787 new
            micap_pa_rm = self.engine.micap_state.pop_and_rm_first(condition_a_start, event_type=eventtype)

            if micap_pa_rm is None: # PATH 1: No MICAP - End processing for this and all remaining parts
                break
            else: # --- PATH 2: MICAP exists ---
                # can just call self.micap_state.remove_aircraft? micap77
                first_micap = micap_pa_rm
                
                # Calculate install timing
                d4_install = self.engine.calculate_install_duration()
                s4_install_start = condition_a_start
                s4_install_end = s4_install_start + d4_install
                
                # Calculate condition_a_duration (should be 0 or condition_a_start set by init cond)
                condition_a_end = s4_install_start
                condition_a_duration = condition_a_end - condition_a_start
                
                # For initial conditions, all MICAP aircraft don't have des_id
                des_id_for_sim = self.engine.get_next_des_id()
                
                # --- Find and EDIT existing row in sim_df for this part_id ---
                filled_sim_df = self.df.sim_df.iloc[:self.df.current_sim_row]
                part_row_idx = filled_sim_df[filled_sim_df['part_id'] == part_id].index[0]
                
                # Update the existing sim_df row with install information
                self.df.sim_df.at[part_row_idx, 'condition_a_duration'] = condition_a_duration
                self.df.sim_df.at[part_row_idx, 'install_duration'] = d4_install
                self.df.sim_df.at[part_row_idx, 'destwo_id'] = des_id_for_sim
                self.df.sim_df.at[part_row_idx, 'actwo_id'] = first_micap['ac_id']
                self.df.sim_df.at[part_row_idx, 'condition_a_end'] = condition_a_end
                self.df.sim_df.at[part_row_idx, 'install_start'] = s4_install_start
                self.df.sim_df.at[part_row_idx, 'install_end'] = s4_install_end
                
                # Generate IDs for cycle restart (cycle + 1)
                new_sim_id_restart = self.engine.get_next_sim_id()
                new_des_id_restart = self.engine.get_next_des_id() + 1  # Account for des_id used for MICAP
                # Fleet Calculation
                d1 = self.engine.calculate_fleet_duration()
                s1_start = s4_install_end
                s1_end = s1_start + d1

                # --- Add row to sim_df for cycle + 1 (restart) ---
                self.engine.add_sim_event(
                    sim_id=new_sim_id_restart,
                    part_id=part_id,
                    desone_id=new_des_id_restart,
                    acone_id=first_micap['ac_id'],
                    micap='IC_CRCA',
                    fleet_duration=d1,
                    condition_f_duration=np.nan,
                    depot_duration=np.nan,
                    condition_a_duration=np.nan,
                    install_duration=np.nan,
                    fleet_start=s4_install_end,
                    fleet_end=s1_end,
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
                    cycle=cycle + 1,  # Increment cycle
                    condemn='no'
                )
                
                # Handle des_df: Aircraft started in MICAP, ADD NEW row
                micap_duration = condition_a_start - first_micap['micap_start']
                micap_end = condition_a_start
                eventtype="IC_CRCA_A"
                
                self.engine.add_des_event(
                    des_id=des_id_for_sim,
                    ac_id=first_micap['ac_id'],
                    micap=first_micap['micap'],
                    simone_id=None,
                    partone_id=None,
                    fleet_duration=np.nan,
                    fleet_start=np.nan,
                    fleet_end=np.nan,
                    micap_duration=micap_duration,
                    micap_start=first_micap['micap_start'],
                    micap_end=micap_end,
                    simtwo_id=part_row_idx,  # Use the existing sim_df row index as sim_id
                    parttwo_id=part_id,
                    install_duration=d4_install,
                    install_start=s4_install_start,
                    install_end=s4_install_end
                )
                
                # Add ANOTHER row to des_df for cycle restart
                self.engine.add_des_event(
                    des_id=new_des_id_restart,
                    ac_id=first_micap['ac_id'],
                    micap=eventtype,
                    simone_id=new_sim_id_restart,
                    partone_id=part_id,
                    fleet_duration=d1,
                    fleet_start=s4_install_end,
                    fleet_end=s1_end,
                    micap_duration=np.nan,
                    micap_start=np.nan,
                    micap_end=np.nan,
                    simtwo_id=np.nan,
                    parttwo_id=np.nan,
                    install_duration=np.nan,
                    install_start=np.nan,
                    install_end=np.nan
                )
                
                # Remove part from condition_a_df
                self.df.condition_a_df = self.df.condition_a_df[
                    self.df.condition_a_df['part_id'] != part_id
                ].reset_index(drop=True)


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
        """
        # push IC_IZFS & IC_CRCA from fleet_end to CF_Start
        filled_sim_df = self.df.sim_df.iloc[:self.df.current_sim_row]
        valid_rows = filled_sim_df[(filled_sim_df['micap'] == 'IC_IZFS') | 
            (filled_sim_df['micap'] == 'IC_CRCA')]
        valid_rows = valid_rows.sort_values('fleet_end')
        eventtype="IC_FE_CF"
        
        if not valid_rows.empty: # ONLY run if valid_rows has data
            for idx in valid_rows.index:# Iterate over the INDEX
                s1_end = self.df.sim_df.at[idx, 'fleet_end']
                self.df.sim_df.at[idx, 'micap'] = eventtype # update event
                self.df.sim_df.at[idx, 'condition_f_start'] = s1_end



# spacer

