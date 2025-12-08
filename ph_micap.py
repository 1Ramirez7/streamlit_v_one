"""
MicapState class to manage aircraft MICAP (Mission Capability) status.

Replaces direct pandas DataFrame operations on micap_df with a more structured
approach while maintaining the same column names and sorting behavior.
"""

import pandas as pd
import numpy as np
import random
from collections import deque


class MicapQueue:
    """
    Low-level MICAP queue using deque + dict for fast operations.
    
    Maintains chronological order (FIFO) while providing O(1) lookup and removal.
    """
    
    def __init__(self):
        self.queue = deque()  # Chronological order
        self.lookup = {}      # {ac_id: record} for O(1) operations
        self.active_ids = set()  # For duplicate detection
    
    def add(self, record):
        """
        Add aircraft record to MICAP queue.
        
        Parameters
        ----------
        record : dict
            Aircraft MICAP record
        
        Returns
        -------
        dict
            {'success': bool, 'error': str or None}
        """
        ac_id = record['ac_id']
        
        if ac_id in self.active_ids:
            return {'success': False, 'error': f'Duplicate ac_id {ac_id} in MICAP queue'}
        
        self.queue.append(record)
        self.lookup[ac_id] = record
        self.active_ids.add(ac_id)
        
        return {'success': True, 'error': None}
    
    def pop_first(self):
        """
        Remove and return first aircraft (earliest micap_start).
        
        Remove from 3 data structures:
            - deque, lookup dict, active_ids
        Returns
        -------
        dict or None
            First aircraft record or None if empty
        """
        if not self.queue:
            return None
        
        record = self.queue.popleft()
        ac_id = record['ac_id']
        self.lookup.pop(ac_id)
        self.active_ids.remove(ac_id)
        
        return record
    
    def count(self):
        """Return number of active aircraft."""
        return len(self.queue)
    
    def is_empty(self):
        """Check if queue is empty."""
        return len(self.queue) == 0


class MicapState:
    """
    Manages aircraft MICAP (Mission Capability) status with business logic.
    
    Uses MicapQueue internally for efficient operations while maintaining
    compatibility with existing simulation engine interface.
    """
    
    def __init__(self):
        """
        Initialize MICAP state management.
        """
        self.active_queue = MicapQueue()
        self.micap_log = []  # Resolved MICAP history
        self.errors = []     # Critical errors list
        self._counter = 0    # Track total MICAP events for debugging
    
    def add_aircraft(self, des_id, ac_id, event_path, fleet_duration, 
                    fleet_start, fleet_end, micap_start):
        """
        Add aircraft to MICAP state.
        
        Parameters
        ----------
        des_id : int or None
            DES event ID (may be None if aircraft started in MICAP)
        ac_id : int
            Aircraft ID
        micap_type : str
            Event type (e.g., 'FE_MS')
        fleet_duration : float
            Duration of fleet stage
        fleet_start : float
            Fleet start time
        fleet_end : float
            Fleet end time (when MICAP began)
        micap_start : float
            Time when MICAP status began
        
        Notes
        -----
        The log entry event is always 'ENTER_MICAP'
        """
        record = {
            'des_id': des_id,
            'ac_id': ac_id,
            'event_path': event_path,
            'fleet_duration': fleet_duration,
            'fleet_start': fleet_start,
            'fleet_end': fleet_end,
            'micap_duration': np.nan,
            'micap_start': micap_start,
            'micap_end': np.nan
        }
        
        result = self.active_queue.add(record)
        
        if not result['success']:
            self.errors.append({
                'type': 'DUPLICATE_AC_ID',
                'message': result['error'],
                'ac_id': ac_id
            })
        else:
            # Log entry event when aircraft enters MICAP
            log_entry = record.copy()
            log_entry['event'] = 'ENTER_MICAP'
            log_entry['micap_count'] = self.count_active()  # Count after adding
            log_entry['event_time'] = micap_start
            self.micap_log.append(log_entry)
        
        self._counter += 1
    
    def pop_and_rm_first(self, current_time):
        """
        Remove first MICAP aircraft (earliest micap_start), log it, return it.
        
        Returns
        -------
        dict or None
            Removed aircraft record with micap_end/duration set, or None if empty
        
        Notes
        -----
        The log entry event is always 'EXIT_MICAP'
        """
        record = self.active_queue.pop_first()
        if record is None:
            return None
        
        # Set removal details
        record['micap_end'] = current_time
        record['micap_duration'] = current_time - record['micap_start']
        
        # Log the exit event
        log_entry = record.copy()
        log_entry['event'] = 'EXIT_MICAP'
        log_entry['micap_count'] = self.count_active()  # Count after removal
        log_entry['event_time'] = current_time
        self.micap_log.append(log_entry)
        
        return pd.Series(record)
    
    def count_active(self):
        """
        Count number of aircraft currently in MICAP.
        
        Returns
        -------
        int
            Number of active MICAP aircraft
        """
        return self.active_queue.count()
    
    def get_log_dataframe(self):
        """
        Get complete MICAP event history as DataFrame (entries and exits).
        
        Returns
        -------
        pd.DataFrame
            Complete MICAP event log with columns:
            - event_time: Time when event occurred
            - event: 'ENTER_MICAP' or 'EXIT_MICAP'
            - ac_id, micap_start, micap_end
            - micap_count: Number of aircraft in MICAP at this event time
        """
        if not self.micap_log:
            return pd.DataFrame(columns=[
                'event_time', 'event', 'micap_count', 'des_id', 'ac_id', 
                'event_path', 'fleet_duration', 'fleet_start', 'fleet_end',
                'micap_start', 'micap_end'
            ])
        # add code so when sim ends (events stop processing so need to define when it ends)
        # to log_entry for avtive micap at sim end and event name will be end_active_micap 
        # tracks all MICAP, I'm sure log_entry = record.copy() tracks entry but no event name yet. 
        return pd.DataFrame(self.micap_log)
    
    def get_micap_wip_df(self):
        """
        Get MICAP event summary with minimal fields for WIP tracking.
        
        Returns
        -------
        pd.DataFrame
            MICAP event log with columns:
            - event_time: Time when event occurred
            - event: 'ENTER_MICAP' or 'EXIT_MICAP'
            - micap_count: Number of aircraft in MICAP at this event time
        """
        if not self.micap_log:
            return pd.DataFrame(columns=['event_time', 'event', 'micap_count'])
        
        df = pd.DataFrame(self.micap_log)
        return df[['event_time', 'event', 'micap_count']]