"""
NewPart class to manage new parts ordered after condemnation.

Tracks parts awaiting arrival (after part_order_lag).
Parts enter when condemned part triggers replacement order.
Parts exit when they arrive and move to Condition A or resolve MICAP.
"""

import pandas as pd


class NewPart:
    """
    Manages new parts on order with O(1) dictionary lookups.
    
    Key: part_id (incrementing from n_total_parts)
    Minimal storage: part_id, condition_a_start (arrival time), cycle (always 0)
    
    Also tracks condemnation log for debugging/analysis.
    """
    
    def __init__(self, n_total_parts):
        """
        Initialize NewPart state management.
        
        Parameters
        ----------
        n_total_parts : int
            Starting value for part_id counter (from user input)
        """
        self.next_part_id = n_total_parts  # Incrementing counter starts at n_total_parts
        self.active = {}                   # {part_id: record} for O(1) lookups
        self.condemn_log = []              # Track condemnation events (moved from data_manager)
    
    def get_next_part_id(self):
        """
        Get and increment the next available part_id.
        
        Returns
        -------
        int
            Next available part_id
        """
        part_id = self.next_part_id
        self.next_part_id += 1
        return part_id
    
    def add_new_part(self, part_id, condition_a_start):
        """
        Add new part to active tracking (on order, awaiting arrival).
        
        Parameters
        ----------
        part_id : int
            Part identifier (from get_next_part_id)
        condition_a_start : float
            Scheduled arrival time (depot_end + part_order_lag)
        
        Returns
        -------
        dict
            {'success': bool, 'error': str or None}
        """
        if part_id in self.active:
            return {'success': False, 'error': f'Duplicate part_id {part_id} in NewPart'}
        
        record = {
            'part_id': part_id,
            'condition_a_start': condition_a_start,
            'cycle': 0  # Always 0 for new parts
        }
        
        self.active[part_id] = record
        return {'success': True, 'error': None}
    
    def get_part(self, part_id):
        """
        Get part record by part_id.
        
        Parameters
        ----------
        part_id : int
            Part identifier
        
        Returns
        -------
        dict or None
            Part record or None if not found
        """
        return self.active.get(part_id)
    
    def remove_part(self, part_id):
        """
        Remove part from active tracking (called when part arrives).
        
        Parameters
        ----------
        part_id : int
            Part identifier to remove
        
        Returns
        -------
        dict or None
            Removed record or None if not found
        """
        return self.active.pop(part_id, None)
    
    def get_all_active(self):
        """
        Get all parts currently on order.
        
        Returns
        -------
        dict
            {part_id: record} dictionary
        """
        return self.active
    
    def count_active(self):
        """
        Count parts currently on order.
        
        Returns
        -------
        int
            Number of parts awaiting arrival
        """
        return len(self.active)
    
    def log_condemnation(self, old_part_id, depot_end, new_part_id, condition_a_start):
        """
        Log condemnation event for debugging/analysis.
        
        Parameters
        ----------
        old_part_id : int
            Condemned part's ID
        depot_end : float
            Time condemned part finished depot
        new_part_id : int
            Replacement part's ID
        condition_a_start : float
            Scheduled arrival time for replacement
        """
        self.condemn_log.append({
            'part_id': old_part_id,
            'depot_end': depot_end,
            'new_part_id': new_part_id,
            'condition_a_start': condition_a_start
        })
    
    def get_condemn_log_dataframe(self):
        """
        Get condemnation log as DataFrame.
        
        Returns
        -------
        pd.DataFrame
            Condemnation history with columns:
            - part_id: Condemned part ID
            - depot_end: When condemned part finished depot
            - new_part_id: Replacement part ID
            - condition_a_start: Replacement arrival time
        """
        if not self.condemn_log:
            return pd.DataFrame(columns=[
                'part_id', 'depot_end', 'new_part_id', 'condition_a_start'
            ])
        
        return pd.DataFrame(self.condemn_log)
