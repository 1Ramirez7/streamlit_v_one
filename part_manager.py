"""
Part Manager: Replace sim_df Row Tracking

This module implements the PartManager system to replace row-based part tracking
with dictionary-based O(1) lookups, separating active part state from historical logging.

Classes:
    PartManager: Manages part lifecycle, logging, and export with O(1) lookups
"""
import numpy as np
import pandas as pd


class PartManager:
    """
    Manages part lifecycle, logging, and export with dictionary-based O(1) lookups.
    
    Replaces DataFrame row searching with direct dictionary access for active parts,
    enabling fast lookups during simulation and proper logging for analysis.
    """
    
    def __init__(self):
        """Initialize manager with active dictionary, ID counter, and completion log."""
        self.active = {}  # {sim_id: record} - dictionary storage for O(1) lookups
        self.next_sim_id = 0  # ID counter (replacing current_sim_row)
        self.part_log = []  # Completed cycles
    
    # ===========================================================
    # CORE OPERATIONS: ID GENERATION
    # ===========================================================

    def get_next_sim_id(self):
        """
        Generate next sim_id (replacing get_next_sim_id()).
        
        Returns:
            int: Next available sim_id
        """
        current_id = self.next_sim_id
        self.next_sim_id += 1
        return current_id
    
    # ===========================================================
    # CORE OPERATIONS: ADD/CREATE PARTS
    # ===========================================================
    
    def add_part(self, sim_id, part_id, cycle, **fields):
        """
        Add part to active tracking with all sim_df fields.
        
        Args:
            sim_id (int): Simulation ID for the part
            part_id (int): Part identifier
            cycle (int): Current cycle number for the part
            **fields: Additional fields matching sim_df schema

        NOTE: SIM_ID NEEDS TO BE GENERATED ALREADY
            
        Returns:
            dict: {'success': bool, 'error': str or None}
        """
        # Check for duplicate sim_id
        if sim_id in self.active:
            return {'success': False, 'error': f'Duplicate sim_id {sim_id}'}
        
        # Build complete record with all sim_df fields
        record = {
            'sim_id': sim_id,
            'part_id': part_id,
            'cycle': cycle,
            'micap': fields.get('micap', ''),
            'fleet_start': fields.get('fleet_start', np.nan), # added np.nan to allow to just include filled variables when calling add_part that way not all variables need to be coded in call 
            'fleet_end': fields.get('fleet_end', np.nan),
            'fleet_duration': fields.get('fleet_duration', np.nan),
            'condition_f_start': fields.get('condition_f_start', np.nan),
            'condition_f_end': fields.get('condition_f_end', np.nan),
            'condition_f_duration': fields.get('condition_f_duration', np.nan),
            'depot_start': fields.get('depot_start', np.nan),
            'depot_end': fields.get('depot_end', np.nan),
            'depot_duration': fields.get('depot_duration', np.nan),
            'condition_a_start': fields.get('condition_a_start', np.nan),
            'condition_a_end': fields.get('condition_a_end', np.nan),
            'condition_a_duration': fields.get('condition_a_duration', np.nan),
            'install_start': fields.get('install_start', np.nan),
            'install_end': fields.get('install_end', np.nan),
            'install_duration': fields.get('install_duration', np.nan),
            'desone_id': fields.get('desone_id', np.nan),
            'acone_id': fields.get('acone_id', np.nan),
            'destwo_id': fields.get('destwo_id', np.nan),
            'actwo_id': fields.get('actwo_id', np.nan),
            'condemn': fields.get('condemn', 'no')
        }
        
        # Add to active dictionary
        self.active[sim_id] = record
        return {'success': True, 'error': None}

    def add_initial_part(self, part_id, cycle, **fields):
        """
        Add part during initialization phase with auto-generated sim_id.
        Look into this but i think it is not needed? 
        
        This method is specifically for initial conditions (event_ic_izfs, etc.)
        where sim_id needs to be auto-generated and incremented for each part.
        Unlike add_part(), this doesn't require to pass sim_id.
        
        Args:
            part_id (int): Part identifier
            cycle (int): Current cycle number for the part
            **fields: Additional fields matching sim_df schema
            
        Returns:
            dict: {'sim_id': int, 'success': bool, 'error': str or None}
        """
        # Auto-generate sim_id for this part
        sim_id = self.next_sim_id
        self.next_sim_id += 1
        
        # Build complete record with all sim_df fields
        record = {
            'sim_id': sim_id,
            'part_id': part_id,
            'cycle': cycle,
            'micap': fields.get('micap', ''),
            'fleet_start': fields.get('fleet_start', np.nan),
            'fleet_end': fields.get('fleet_end', np.nan),
            'fleet_duration': fields.get('fleet_duration', np.nan),
            'condition_f_start': fields.get('condition_f_start', np.nan),
            'condition_f_end': fields.get('condition_f_end', np.nan),
            'condition_f_duration': fields.get('condition_f_duration', np.nan),
            'depot_start': fields.get('depot_start', np.nan),
            'depot_end': fields.get('depot_end', np.nan),
            'depot_duration': fields.get('depot_duration', np.nan),
            'condition_a_start': fields.get('condition_a_start', np.nan),
            'condition_a_end': fields.get('condition_a_end', np.nan),
            'condition_a_duration': fields.get('condition_a_duration', np.nan),
            'install_start': fields.get('install_start', np.nan),
            'install_end': fields.get('install_end', np.nan),
            'install_duration': fields.get('install_duration', np.nan),
            'desone_id': fields.get('desone_id', np.nan),
            'acone_id': fields.get('acone_id', np.nan),
            'destwo_id': fields.get('destwo_id', np.nan),
            'actwo_id': fields.get('actwo_id', np.nan),
            'condemn': fields.get('condemn', 'no')
        }
        
        # Add to active dictionary
        self.active[sim_id] = record
        return {'sim_id': sim_id, 'success': True, 'error': None}
    
    # ===========================================================
    # CORE OPERATIONS: READ/ACCESS PARTS
    # ===========================================================
    
    def get_part(self, sim_id):
        """
        Get active part by sim_id - O(1) lookup.
        
        Args:
            sim_id (int): Simulation ID of the part to retrieve

        temp comment: this method replaces old way of searching sim_id row in sim_df
            * OLD way - (searches entire DataFrame)
            filled_sim_df = self.df.sim_df.iloc[:self.df.current_sim_row]
            part_row = filled_sim_df[filled_sim_df['sim_id'] == sim_id].iloc[0]

            * NEW way - fast (O(1) dictionary lookup)
            active_part = self.part_manager.get_part(sim_id)
            
        Returns:
            dict or None: Part record if found, None if not found
        """
        return self.active.get(sim_id)
    
    def get_all_active_parts(self):
        """
        Get all currently active parts.
        
        Returns:
            dict: Dictionary of all active parts {sim_id: record}
        """
        return self.active.copy()
    
    # ===========================================================
    # CORE OPERATIONS: MODIFY/UPDATE PARTS
    # ===========================================================
    
    def update_fields(self, sim_id, updates):
        """
        Update multiple fields on active part in a single call.
        
        Args:
            sim_id (int): Simulation ID of the part to update
            updates (dict): Dictionary of {field_name: value} to update
        
        Temp comment: 
            * OLD way - using .at[] for DataFrame scalar assignment
            self.df.sim_df.at[part_row_idx, 'condition_a_duration'] = condition_a_duration
            self.df.sim_df.at[part_row_idx, 'install_duration'] = d4_install
            
            * NEW way - direct dictionary update
            self.engine.part_manager.update_fields(sim_id, {
            'condition_a_duration': condition_a_duration,
            'install_duration': d4_install})

        Returns:
            bool: True if part found and updated, False if part not found
        """
        record = self.active.get(sim_id)
        if record:
            record.update(updates)
            return True
        return False
    
    # ===========================================================
    # CORE OPERATIONS: LIFECYCLE/COMPLETE PARTS
    # ===========================================================
    
    def complete_part_cycle(self, sim_id):
        """
        Log completed cycle and remove from active tracking.
        
        This method should be called when:
        - install_end is recorded (cycle completion)
        - condemn='yes' is set (condemnation)
        
        Args:
            sim_id (int): Simulation ID of the part completing its cycle
        
        temp notes:
            * OLD way - manual DataFrame writes
            new_sim_id = self.get_next_sim_id()  # Get next row index
            self.add_sim_event(...)  # Write new cycle to DataFrame

            * NEW way When about to create new_sim_id for cycle restart,
            * that's when the PREVIOUS cycle is complete!
            self.part_manager.complete_part_cycle(sim_id)  # Log & remove old
            new_sim_id = self.part_manager.get_next_sim_id()  # Get ID for new cycle
            
        Returns:
            dict or None: Completed part record if found, None if not found
        """
        record = self.active.pop(sim_id, None)
        if record:
            self.part_log.append(record.copy())
        return record
    
    def complete_pca_cycle(self, sim_id, part_id):
        """
        Log completed cycle for a part from condition_a_df and remove from active tracking.
        
        Validates that the part_id in the active record matches the provided part_id
        (from condition_a_df) before completing the cycle.
        
        Args:
            sim_id (int): Simulation ID of the part completing its cycle
            part_id (int): Part ID from condition_a_df to validate against
            
        Returns:
            dict or None: Completed part record if found and valid, None if not found
        
        Notes: 
            method complete_part_cycle could have been use, this function adds an extra check
            to make sure transition in condition_a_df does not corrupt sim_id & part_id pair
            
        Raises:
            ValueError: If part_id in active record does not match provided part_id
        """
        record = self.active.get(sim_id)
        if not record:
            return None
            
        if record['part_id'] != part_id:
            raise ValueError(
                f"Part ID mismatch for sim_id {sim_id}: "
                f"Active record has {record['part_id']}, "
                f"condition_a_df has {part_id}"
            )
            
        return self.complete_part_cycle(sim_id)
    
    # ===========================================================
    # EXPORT/ANALYSIS: DATA EXPORT
    # ===========================================================
    
    def export_active_parts(self):
        """
        Export ACTIVE PARTS as pandas DataFrame for analysis.
        
        Returns:
            pd.DataFrame: DataFrame of active part records
        """
        if not self.active:
            return pd.DataFrame(columns=[ # returns empty df with proper column structure
                'sim_id', 'part_id', 'cycle', 'micap', 'fleet_start', 'fleet_end', 
                'fleet_duration', 'condition_f_start', 'condition_f_end', 
                'condition_f_duration', 'depot_start', 'depot_end', 'depot_duration', 
                'condition_a_start', 'condition_a_end', 'condition_a_duration', 
                'install_start', 'install_end', 'install_duration', 'desone_id', 
                'acone_id', 'destwo_id', 'actwo_id', 'condemn'
            ])
        return pd.DataFrame(list(self.active.values()))
    
    def export_completed_cycles(self):
        """
        Export completed cycles as pandas DataFrame for analysis.
        
        Returns:
            pd.DataFrame: DataFrame of completed part records
        """
        if not self.part_log:
            return pd.DataFrame(columns=[
                'sim_id', 'part_id', 'cycle', 'micap', 'fleet_start', 'fleet_end', 
                'fleet_duration', 'condition_f_start', 'condition_f_end', 
                'condition_f_duration', 'depot_start', 'depot_end', 'depot_duration', 
                'condition_a_start', 'condition_a_end', 'condition_a_duration', 
                'install_start', 'install_end', 'install_duration', 'desone_id', 
                'acone_id', 'destwo_id', 'actwo_id', 'condemn'
            ])
        return pd.DataFrame(self.part_log)
    
    def get_all_parts_data(self):
        """
        Combine active parts and completed cycles into single dictionary.
        
        Use at end of simulation to get complete part history for analysis.
        Merges self.active (parts still in progress) with self.part_log 
        (completed cycles).
        
        Returns:
            dict: Combined dictionary {sim_id: record} containing all parts
        """
        all_parts = {}

        for record in self.part_log:
            sim_id = record['sim_id']
            all_parts[sim_id] = record

        # Add all active parts (from active dict)
        for sim_id, record in self.active.items():
            if sim_id in all_parts: # Check for duplicate sim_id
                raise ValueError(
                    f"Duplicate sim_id {sim_id} found in both completed cycles and active parts."
                )
            all_parts[sim_id] = record
        
        return all_parts
    
    def get_all_parts_data_df(self):
        """
        Export all parts (active + completed) as pandas DataFrame.
        * all_parts_df will be the name of the dataframe. 
        * will be use via data_manager.py. From an engineering perspective, part_manager is like the chef that prepares the food
        * while data_manager is the waiter. We wouldn't have the custumor go straight to chef for the food. 

        all_parts_df
        Combines active parts and completed cycles into single DataFrame
        for analysis and export. Replacement for df_manager.sim_df.

        Sample Usage:
            engine.run: 
                self.datasets.build_part_ac_df(
                    self.part_manager.get_all_parts_data_df,
                    self.ac_manager.get_all_ac_data_df) # includes ac manager class
            Can then be used via datasets class
            main.py: datasets.all_parts_df
        
        Returns:
            pd.DataFrame: All parts with complete sim_df schema
        """
        all_parts = self.get_all_parts_data()
        
        if not all_parts:
            # Return empty DataFrame with proper schema
            return pd.DataFrame(columns=[
                'sim_id', 'part_id', 'cycle', 'micap', 'fleet_start', 'fleet_end', 
                'fleet_duration', 'condition_f_start', 'condition_f_end', 
                'condition_f_duration', 'depot_start', 'depot_end', 'depot_duration', 
                'condition_a_start', 'condition_a_end', 'condition_a_duration', 
                'install_start', 'install_end', 'install_duration', 'desone_id', 
                'acone_id', 'destwo_id', 'actwo_id', 'condemn'
            ])
        
        # Convert dictionary values to list for consistency with other export methods
        return pd.DataFrame(list(all_parts.values()))
    
    # FUTURE POSSIBLE OPTIONs
    # ===========================================================
    # UTILITY: VALIDATION & MAINTENANCE 
    # ===========================================================