import pandas as pd
import numpy as np
import random
from collections import deque


class MicapQueue:
    def __init__(self):
        self.queue = deque()  # Chronological order
        self.lookup = {}      # {ac_id: record} for O(1) operations
        self.active_ids = set()  # For duplicate detection
    
    def add(self, record):
        ac_id = record['ac_id']
        
        if ac_id in self.active_ids:
            return {'success': False, 'error': f'Duplicate ac_id {ac_id} in MICAP queue'}
        
        self.queue.append(record)
        self.lookup[ac_id] = record
        self.active_ids.add(ac_id)
        
        return {'success': True, 'error': None}
    
    def pop_first(self):
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
    def __init__(self, n_total_aircraft=None):
        self.active_queue = MicapQueue()
        self.micap_log = []  # Resolved MICAP history
        self.errors = []     # Critical errors list
        self.n_total_aircraft = n_total_aircraft
        self._counter = 0    # Track total MICAP events for debugging
    
    def add_aircraft(self, des_id, ac_id, micap_type, fleet_duration, 
                    fleet_start, fleet_end, micap_start):
        # Check aircraft count limit
        if (self.n_total_aircraft is not None and 
            self.count_active() >= self.n_total_aircraft):
            error = f"MICAP count ({self.count_active()}) would exceed total aircraft ({self.n_total_aircraft})"
            self.errors.append({
                'type': 'MICAP_COUNT_EXCEEDED',
                'message': error,
                'ac_id': ac_id,
                'current_count': self.count_active()
            }) # Error handling, can remove if issue handle else where. 
        
        record = {
            'des_id': des_id,
            'ac_id': ac_id,
            'micap': micap_type,
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
    
    def pop_and_rm_first(self, current_time, event_type):
        record = self.active_queue.pop_first()
        if record is None:
            return None
        
        # Set removal details
        record['micap_end'] = current_time
        record['micap_duration'] = current_time - record['micap_start']
        
        # Log the exit event
        log_entry = record.copy()
        log_entry['event'] = 'EXIT_MICAP'
        log_entry['event_type'] = event_type
        log_entry['micap_count'] = self.count_active()  # Count after removal
        log_entry['event_time'] = current_time
        self.micap_log.append(log_entry)
        
        return pd.Series(record)
    
    def count_active(self):
        return self.active_queue.count()
    
    def _create_micap_df(self, allocation):
        micap_ac_ids = allocation.get('micap_ac_ids', [])
        
        # Add each aircraft to MICAP queue
        for ac_id in micap_ac_ids:
            self.add_aircraft(
                des_id=None,
                ac_id=ac_id,
                micap_type='IC_MICAP',
                fleet_duration=np.nan,
                fleet_start=np.nan,
                fleet_end=np.nan,
                micap_start=0
            )
    
    def get_log_dataframe(self):
        if not self.micap_log:
            return pd.DataFrame(columns=[
                'event_time', 'event', 'ac_id', 'micap', 'micap_start', 'micap_end',
                'micap_count', 'event_type', 'des_id', 'fleet_duration', 'fleet_start', 'fleet_end'
            ])
        # add code so when sim ends (events stop processing so need to define when it ends)
        # to log_entry for avtive micap at sim end and event name will be end_active_micap 
        # tracks all MICAP, I'm sure log_entry = record.copy() tracks entry but no event name yet. 
        return pd.DataFrame(self.micap_log)