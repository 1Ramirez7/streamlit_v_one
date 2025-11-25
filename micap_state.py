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
    
    def remove_by_id(self, ac_id):
        if ac_id not in self.lookup:
            return None
        
        record = self.lookup.pop(ac_id)
        self.active_ids.remove(ac_id)
        
        # Remove from deque (O(n) but only over active MICAP count)
        new_queue = deque(r for r in self.queue if r['ac_id'] != ac_id)
        self.queue = new_queue
        
        return record
    
    def pop_first(self):
        if not self.queue:
            return None
        
        record = self.queue.popleft()
        ac_id = record['ac_id']
        self.lookup.pop(ac_id)
        self.active_ids.remove(ac_id)
        
        return record
    
    def peek_first(self):
        return self.queue[0] if self.queue else None
    
    def get_by_criteria(self, count, strategy, current_time=None):
        available = min(count, len(self.queue))
        if available == 0:
            return []
        
        queue_list = list(self.queue)
        
        if strategy == 'first':
            return queue_list[:available]
        
        elif strategy == 'random':
            return random.sample(queue_list, available)
        
        elif strategy == 'longest_micap':
            if current_time is None:
                raise ValueError("current_time required for 'longest_micap' strategy")
            
            # Calculate time in MICAP for each aircraft
            with_duration = [
                (current_time - r['micap_start'], r) 
                for r in queue_list
            ]
            # Sort by longest duration first
            with_duration.sort(key=lambda x: x[0], reverse=True)
            return [r for _, r in with_duration[:available]]
        
        else:
            raise ValueError(f"Unknown strategy: {strategy}")
    
    def count(self):
        """Return number of active aircraft."""
        return len(self.queue)
    
    def is_empty(self):
        """Check if queue is empty."""
        return len(self.queue) == 0
    
    def get_all(self):
        """Return all aircraft as list (copy)."""
        return list(self.queue)


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
        
        self._counter += 1
    
    def pop_and_rm_first(self, current_time, event_type):
        record = self.active_queue.pop_first()
        if record is None:
            return None
        
        # Set removal details
        record['micap_end'] = current_time
        record['micap_duration'] = current_time - record['micap_start']
        
        # Log the resolution
        log_entry = record.copy()
        log_entry['event_type'] = event_type
        self.micap_log.append(log_entry)
        
        return pd.Series(record)

    def remove_aircraft(self, ac_id, micap_end=None, event_type='PART_AVAILABLE'):
        record = self.active_queue.remove_by_id(ac_id)
        
        if record is None:
            return None # this is an untrack error, as none should not happen
        
        # Calculate micap_duration and set micap_end
        if micap_end is not None:
            record['micap_end'] = micap_end
            record['micap_duration'] = micap_end - record['micap_start']
        
        # Log the resolution
        log_entry = record.copy()
        log_entry['event_type'] = event_type
        self.micap_log.append(log_entry)
        
        return record
    
    def remove_multiple(self, count, strategy, current_time, event_type='BATCH_REMOVAL'):
        # Get aircraft by criteria
        candidates = self.active_queue.get_by_criteria(count, strategy, current_time)
        
        removed_records = []
        for record in candidates:
            removed = self.remove_aircraft(
                record['ac_id'], 
                micap_end=current_time,
                event_type=event_type
            )
            if removed:
                removed_records.append(removed)
        
        return removed_records
    
    def get_first_aircraft(self):
        record = self.active_queue.peek_first()
        if record is None:
            return None
        
        # Convert to pandas Series for compatibility with existing code
        return pd.Series(record)
    
    def get_active_aircraft(self):
        records = self.active_queue.get_all()
        if not records:
            return pd.DataFrame(columns=[
                'des_id', 'ac_id', 'micap', 'fleet_duration', 'fleet_start', 
                'fleet_end', 'micap_duration', 'micap_start', 'micap_end'
            ])
        
        df = pd.DataFrame(records)
        # Sort by micap_start, then ac_id for tie-breaking (same as original)
        df = df.sort_values(['micap_start', 'ac_id']).reset_index(drop=True)
        return df
    
    def count_active(self):
        return self.active_queue.count()
    
    def is_empty(self):
        return self.active_queue.is_empty()
    
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
    
    
    def get_dataframe(self):
        return self.get_active_aircraft()
    
    def get_log_dataframe(self):
        if not self.micap_log:
            return pd.DataFrame(columns=[
                'des_id', 'ac_id', 'micap', 'fleet_duration', 'fleet_start', 
                'fleet_end', 'micap_duration', 'micap_start', 'micap_end', 'event_type'
            ])
        # add code so when sim ends (events stop processing so need to define when it ends)
        # to log_entry for avtive micap at sim end and event name will be end_active_micap 
        # tracks all MICAP, I'm sure log_entry = record.copy() tracks entry but no event name yet. 
        return pd.DataFrame(self.micap_log)
    
    def get_errors(self):
        return self.errors.copy()
    
    def clear_errors(self):
        self.errors.clear()
    
    def has_critical_errors(self):
        return len(self.errors) > 0