"""
Aircraft Manager: Replace des_df Row Tracking

This module implements the AircraftManager system to replace row-based aircraft tracking
with dictionary-based O(1) lookups, separating active aircraft state from historical logging.

Classes:
    AircraftManager: Manages aircraft lifecycle, logging, and export with O(1) lookups
"""
import numpy as np
import pandas as pd


class AircraftManager:
    """
    Manages aircraft lifecycle, logging, and export with dictionary-based O(1) lookups.
    
    Replaces DataFrame row searching with direct dictionary access for active aircraft,
    enabling fast lookups during simulation and proper logging for analysis.
    
    Parallel to PartManager class but tracks aircraft (des_df) instead of parts (sim_df).
    """
    
    def __init__(self):
        """Initialize manager with active dictionary, ID counter, and completion log."""
        self.active = {}  # {des_id: record} - dictionary storage for O(1) lookups
        self.next_des_id = 0  # ID counter (replacing current_des_row)
        self.ac_log = []  # Completed cycles
        self.micap_count = 0  # Current count of aircraft in MICAP
        self.micap_log = []   # Event log for MICAP entries/exits
    
    # ===========================================================
    # CORE OPERATIONS: ID GENERATION
    # ===========================================================

    def get_next_des_id(self):
        """
        Generate next des_id (replacing get_next_des_id()).
        
        Returns:
            int: Next available des_id
        """
        current_id = self.next_des_id
        self.next_des_id += 1
        return current_id
    
    # ===========================================================
    # CORE OPERATIONS: ADD AIRCRAFT to ACTIVE DICTIONARY
    # ===========================================================
    
    def add_ac(self, des_id, ac_id, **fields):
        """
        Add aircraft event to active tracking with all des_df fields.
        
        Args:
            des_id (int): DES event ID (primary key for aircraft events)
            ac_id (int): Aircraft identifier
            **fields: Additional fields matching des_df schema

        NOTE: DES_ID NEEDS TO BE GENERATED ALREADY
            
        Returns:
            dict: {'success': bool, 'error': str or None}
        """
        # Check for duplicate des_id
        if des_id in self.active:
            return {'success': False, 'error': f'Duplicate des_id {des_id}'}
        
        # Build complete record with all des_df fields
        record = {
            'des_id': des_id,
            'ac_id': ac_id,
            'event_path': fields.get('event_path', ''),
            'fleet_duration': fields.get('fleet_duration', np.nan),
            'fleet_start': fields.get('fleet_start', np.nan),
            'fleet_end': fields.get('fleet_end', np.nan),
            'micap_duration': fields.get('micap_duration', np.nan),
            'micap_start': fields.get('micap_start', np.nan),
            'micap_end': fields.get('micap_end', np.nan),
            'install_duration': fields.get('install_duration', np.nan),
            'install_start': fields.get('install_start', np.nan),
            'install_end': fields.get('install_end', np.nan),
            'simone_id': fields.get('simone_id', np.nan),
            'partone_id': fields.get('partone_id', np.nan),
            'simtwo_id': fields.get('simtwo_id', np.nan),
            'parttwo_id': fields.get('parttwo_id', np.nan)
        }
        
        # Add to active dictionary
        self.active[des_id] = record
        self.track_micap_wip(des_id, record['micap_start'], record['micap_end'])
        return {'success': True, 'error': None}

    def add_initial_ac(self, ac_id, **fields):
        """
        Add aircraft during initialization phase with auto-generated des_id.
        
        This method is specifically for initial conditions (event_ic_iz_fs_fe, etc.)
        where des_id needs to be auto-generated and incremented for each aircraft.
        Unlike add_ac(), this doesn't require to pass des_id.
        # i think its not needed now
        Args:
            ac_id (int): Aircraft identifier
            **fields: Additional fields matching des_df schema
            
        Returns:
            dict: {'des_id': int, 'success': bool, 'error': str or None}
        """
        # Auto-generate des_id for this aircraft
        des_id = self.next_des_id
        self.next_des_id += 1
        
        # Build complete record with all des_df fields
        record = {
            'des_id': des_id,
            'ac_id': ac_id,
            'event_path': fields.get('event_path', ''),
            'fleet_duration': fields.get('fleet_duration', np.nan),
            'fleet_start': fields.get('fleet_start', np.nan),
            'fleet_end': fields.get('fleet_end', np.nan),
            'micap_duration': fields.get('micap_duration', np.nan),
            'micap_start': fields.get('micap_start', np.nan),
            'micap_end': fields.get('micap_end', np.nan),
            'install_duration': fields.get('install_duration', np.nan),
            'install_start': fields.get('install_start', np.nan),
            'install_end': fields.get('install_end', np.nan),
            'simone_id': fields.get('simone_id', np.nan),
            'partone_id': fields.get('partone_id', np.nan),
            'simtwo_id': fields.get('simtwo_id', np.nan),
            'parttwo_id': fields.get('parttwo_id', np.nan)
        }
        
        # Add to active dictionary
        self.active[des_id] = record
        self.track_micap_wip(des_id, record['micap_start'], record['micap_end'])
        return {'des_id': des_id, 'success': True, 'error': None}
    
    # ===========================================================
    # CORE OPERATIONS: GET AIRCRAFT RECORD INFORMATION
    # ===========================================================
    
    def get_ac(self, des_id):
        """
        Get active aircraft by des_id - O(1) lookup.
        
        Args:
            des_id (int): DES event ID of the aircraft to retrieve
        
        Returns:
            dict or None: Aircraft record if found, None if not found
        """
        return self.active.get(des_id)
    
    def get_all_active_ac(self):
        """
        Get all currently active aircraft.
        
        Returns:
            dict: Dictionary of all active aircraft {des_id: record}
        """
        return self.active.copy()
    
    # ===========================================================
    # CORE OPERATIONS: MODIFY/UPDATE AIRCRAFT FIELDS
    # ===========================================================
    
    def update_fields(self, des_id, updates):
        """
        Update multiple fields on active aircraft in a single call.
        
        Args:
            des_id (int): DES event ID of the aircraft to update
            updates (dict): Dictionary of {field_name: value} to update
        
        Returns:
            bool: True if aircraft found and updated, False if aircraft not found
        """
        record = self.active.get(des_id)
        if record:
            record.update(updates)

        if 'micap_start' in updates or 'micap_end' in updates: # need to add argument if start and end only ?
            self.track_micap_wip(des_id, record['micap_start'], record['micap_end'])

            return True
        return False
    
    # ===========================================================
    # CORE OPERATIONS: CYCLE COMPLETE AIRCRAFT-REMOVE from ACTIVE
    # ===========================================================
    
    def complete_ac_cycle(self, des_id):
        """
        Log completed cycle and remove from active tracking.
        
        This method should be called when:
        - install_end is recorded (cycle completion)
        - Aircraft cycle ends and new cycle begins
        
        Args:
            des_id (int): DES event ID of the aircraft completing its cycle
        
        temp notes:
            * OLD way - manual DataFrame writes
            new_des_id = self.get_next_des_id()  # Get next row index
            self.add_des_event(...)  # Write new cycle to DataFrame

            * NEW way When about to create new_des_id for cycle restart,
            * that's when the PREVIOUS cycle is complete!
            self.ac_manager.complete_ac_cycle(des_id)  # Log & remove old
            new_des_id = self.ac_manager.get_next_des_id()  # Get ID for new cycle
            
        Returns:
            dict or None: Completed aircraft record if found, None if not found
        """
        record = self.active.pop(des_id, None)
        if record:
            self.ac_log.append(record.copy())
        return record
    
    # ===========================================================
    # EXPORT/ANALYSIS: DATA EXPORT
    # ===========================================================
    
    def exp_active_ac(self):
        """
        Export ACTIVE AIRCRAFT as pandas DataFrame for analysis.
        
        Returns:
            pd.DataFrame: DataFrame of active aircraft records
        """
        if not self.active:
            return pd.DataFrame(columns=[  # returns empty df with proper column structure
                'des_id', 'ac_id', 'event_path', 
                'fleet_duration', 'fleet_start', 'fleet_end',
                'micap_duration', 'micap_start', 'micap_end',
                'install_duration', 'install_start', 'install_end',
                'simone_id', 'partone_id', 'simtwo_id', 'parttwo_id'
            ])
        return pd.DataFrame(list(self.active.values()))
    
    def exp_log_cycles(self):
        """
        Export log (complted cycle, not active ac) cycles as pandas DataFrame for analysis. 
        
        Returns:
            pd.DataFrame: DataFrame of completed aircraft records
        """
        if not self.ac_log:
            return pd.DataFrame(columns=[
                'des_id', 'ac_id', 'event_path', 
                'fleet_duration', 'fleet_start', 'fleet_end',
                'micap_duration', 'micap_start', 'micap_end',
                'install_duration', 'install_start', 'install_end',
                'simone_id', 'partone_id', 'simtwo_id', 'parttwo_id'
            ])
        return pd.DataFrame(self.ac_log)
    
    def get_all_ac_data(self):
        """
        Combine active aircraft and completed cycles into single dictionary.
        
        Use at end of simulation to get complete aircraft history for analysis.
        Merges self.active (aircraft still in progress) with self.ac_log 
        (completed cycles).
        
        Returns:
            dict: Combined dictionary {des_id: record} containing all aircraft
        """
        all_ac_dict = {}

        for record in self.ac_log:
            des_id = record['des_id']
            all_ac_dict[des_id] = record

        # Add all active aircraft (from active dict)
        for des_id, record in self.active.items():
            if des_id in all_ac_dict:  # Check for duplicate des_id
                raise ValueError(
                    f"Duplicate des_id {des_id} found in both completed cycles and active aircraft."
                )
            all_ac_dict[des_id] = record
        
        return all_ac_dict
    
    def get_all_ac_data_df(self):
        """
        Export all aircraft (active + completed) as pandas DataFrame.
        * all_ac_df will be the name of the dataframe. 
        * will be used via data_manager.py. From an engineering perspective, ac_manager is like the chef that prepares the food
        * while data_manager is the waiter. We wouldn't have the customer go straight to chef for the food. 


        Sample Usage:
            engine.run: self.df.all_ac_dict_df = self.ac_manager.get_all_ac_data_df()
            Can then be used via data_manager class
            main.py: df_manager.all_ac_dict_df
        """
        all_ac_dict = self.get_all_ac_data()
        
        if not all_ac_dict:
            # Return empty DataFrame with proper schema
            return pd.DataFrame(columns=[
                'des_id', 'ac_id', 'event_path', 
                'fleet_duration', 'fleet_start', 'fleet_end',
                'micap_duration', 'micap_start', 'micap_end',
                'install_duration', 'install_start', 'install_end',
                'simone_id', 'partone_id', 'simtwo_id', 'parttwo_id'
            ])
        
        # Convert dictionary values to list for consistency with other export methods
        return pd.DataFrame(list(all_ac_dict.values()))
    

    # ===========================================================
    # WIP TRACKING & Dwonloading
    # ===========================================================
    def track_micap_wip(self, des_id, micap_start, micap_end):
        """
        Track MICAP wip

        Usage in add_ac, add_initial_ac: 
            self.track_micap_wip(des_id, record['micap_start'], record['micap_end'])
        
        Usage in update_fields:
            if 'micap_start' in updates or 'micap_end' in updates:
                self.track_micap_wip(des_id, record['micap_start'], record['micap_end'])
        """
        if pd.notna(micap_start) and pd.isna(micap_end):
            self.micap_count += 1
            self.micap_log.append({
                'event_time': micap_start,
                'event': 'ENTER_MICAP',
                'des_id': des_id,
                'micap_count': self.micap_count
            })
        
        elif pd.notna(micap_start) and pd.notna(micap_end):
            self.micap_count -= 1
            self.micap_log.append({
                'event_time': micap_end,
                'event': 'EXIT_MICAP',
                'des_id': des_id,
                'micap_count': self.micap_count
            })

    def get_micap_log(self):
        """
        Get Condition A event history as DataFrame
        """
        if not self.micap_log:
            return pd.DataFrame(columns=['event_time', 'micap_count'])
        
        return pd.DataFrame(self.micap_log)[['event_time', 'count']]

    # FUTURE POSSIBLE OPTIONS
    # ===========================================================
    # UTILITY: VALIDATION & MAINTENANCE 
    # ===========================================================

    def get_wip_ac_end(self, sim_time, interval=5):
        """
        Get WIP counts over time with forward fill for aircraft.
        """
        from ds.helpers import compute_unified_wip_ac
        
        all_ac = self.get_all_ac_data()
        return compute_unified_wip_ac(all_ac, sim_time, interval)
    

    def get_wip_ac_raw(self):
        """
        Get raw event counts (no interpolation/forward fill) for aircraft.
        """
        from ds.helpers import compute_raw_wip_ac
        
        all_ac = self.get_all_ac_data()
        return compute_raw_wip_ac(all_ac)