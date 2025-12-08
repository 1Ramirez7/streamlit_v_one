"""
ConditionAState class to manage parts waiting in Condition A inventory.

Simple class tracking parts available for installation on aircraft.
Parts enter after depot completion or new part arrival.
Parts exit when installed on aircraft (from fleet_complete or MICAP resolution).
"""

import pandas as pd
from collections import deque


class ConditionAState:
    """
    Manages parts in Condition A (available inventory) with FIFO ordering.
    
    Uses deque + dict for O(1) operations while maintaining insertion order.
    Logs enter/exit events for WIP tracking.
    
    Minimal storage: only sim_id, part_id, condition_a_start.
    Full part details available via part_manager.get_part(sim_id).
    """
    
    def __init__(self):
        """Initialize Condition A state management."""
        self.queue = deque()          # Maintains insertion order (FIFO)
        self.lookup = {}              # {sim_id: record} for O(1) access
        self.condition_a_log = []     # Enter/exit events for WIP tracking
    
    def add_part(self, sim_id, part_id, event_path, condition_a_start):
        """
        Add part to Condition A inventory.
        
        Parameters
        ----------
        sim_id : int
            Part's simulation ID (primary key for part_manager lookup)
        part_id : int
            Part identifier
        condition_a_start : float
            Time when part entered Condition A
        
        Returns
        -------
        dict
            {'success': bool, 'error': str or None}
        """
        if sim_id in self.lookup:
            return {'success': False, 'error': f'Duplicate sim_id {sim_id} in Condition A'}
        
        record = {
            'event_time': condition_a_start,
            'event': 'ENTER_COND_A',
            'sim_id': sim_id,
            'part_id': part_id,
            'event_path': event_path,
            'condition_a_start': condition_a_start,
            'condition_a_end': None,
            'count': self.count_active()
        }
        
        self.queue.append(record)
        self.lookup[sim_id] = record
        
        # Log entry event
        self.condition_a_log.append({
            'event_time': condition_a_start,
            'event': 'ENTER_COND_A',
            'sim_id': sim_id,
            'part_id': part_id,
            'event_path': event_path,
            'condition_a_start': condition_a_start,
            'condition_a_end': None,
            'count': self.count_active()
        })
        
        return {'success': True, 'error': None}
    
    def pop_first_available(self, current_time):
        """
        Remove and return earliest available part (sorted by condition_a_start, then part_id).
        
        Parameters
        ----------
        current_time : float
            Current simulation time (used as condition_a_end)
        
        Returns
        -------
        dict or None
            Part record with condition_a_end added, or None if empty
        """
        if not self.queue:
            return None
        
        # Sort queue to find earliest part (by condition_a_start, then part_id)
        sorted_queue = sorted(self.queue, key=lambda x: (x['condition_a_start'], x['part_id']))
        first_record = sorted_queue[0]
        
        sim_id = first_record['sim_id']
        
        # Remove from lookup
        self.lookup.pop(sim_id)
        
        # Remove from deque (rebuild without this record)
        self.queue = deque(r for r in self.queue if r['sim_id'] != sim_id)
        
        # Add condition_a_end to record
        first_record['condition_a_end'] = current_time
        
        # Log exit event
        self.condition_a_log.append({
            'event_time': current_time,
            'event': 'EXIT_COND_A',
            'sim_id': sim_id,
            'part_id': first_record['part_id'],
            'event_path': first_record['event_path'],
            'condition_a_start': first_record['condition_a_start'],
            'condition_a_end': current_time,
            'count': self.count_active()
        })
        
        return first_record
    
    def count_active(self):
        """
        Count number of parts currently in Condition A.

        Number of available parts
        """
        return len(self.queue)
    
    def is_empty(self):
        """Check if no parts are available."""
        return len(self.queue) == 0
    
    def get_log_dataframe(self):
        """
        Get Condition A event history as DataFrame (event_time and count only).
        
        Returns (available fields (not required))
        -------
        pd.DataFrame
            Event log with columns:
            - event_time: Time when event occurred
            - event: 'ENTER_COND_A' or 'EXIT_COND_A'
            - sim_id, part_id
            - condition_a_start, condition_a_end
            - count: Number of parts in Condition A at event time
        """
        if not self.condition_a_log:
            return pd.DataFrame(columns=['event_time', 'count'])
        
        return pd.DataFrame(self.condition_a_log)[['event_time', 'count']]
