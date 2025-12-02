import pandas as pd


def calculate_wip_overtime(all_ac_df, all_parts_df):
    """
    temp code: WIP code for some reason is extremely SLOW
    Need to just trash all WIP code maybe
    """
    
    # Collect all unique event times from aircraft and parts state changes
    event_times = set()
    
    # Aircraft fleet events
    event_times.update(all_ac_df['fleet_start'].dropna())
    event_times.update(all_ac_df['fleet_end'].dropna())
    
    # Aircraft MICAP events
    event_times.update(all_ac_df['micap_start'].dropna())
    event_times.update(all_ac_df['micap_end'].dropna())
    
    # Part fleet events
    event_times.update(all_parts_df['fleet_start'].dropna())
    event_times.update(all_parts_df['fleet_end'].dropna())
    
    # Part Condition F events
    event_times.update(all_parts_df['condition_f_start'].dropna())
    event_times.update(all_parts_df['condition_f_end'].dropna())
    
    # Part Depot events
    event_times.update(all_parts_df['depot_start'].dropna())
    event_times.update(all_parts_df['depot_end'].dropna())
    
    # Part Condition A events
    event_times.update(all_parts_df['condition_a_start'].dropna())
    event_times.update(all_parts_df['condition_a_end'].dropna())
    
    # Sort all unique event times chronologically
    event_times = sorted(event_times)
    
    # Calculate WIP counts at each event time
    wip_snapshots = []
    
    for t in event_times:
        snapshot = {'time': t}
        
        # Count aircraft in fleet (operational)
        # Aircraft is in fleet if fleet_start <= t and (fleet_end > t OR fleet_end is NaN)
        # NaN fleet_end means aircraft is still in fleet at end of simulation
        snapshot['aircraft_fleet'] = len(all_ac_df[
            (all_ac_df['fleet_start'].notna()) &
            (all_ac_df['fleet_start'] <= t) & 
            ((all_ac_df['fleet_end'] > t) | (all_ac_df['fleet_end'].isna()))
        ])
        
        # Count aircraft in MICAP
        # Aircraft is in MICAP if micap_start <= t and (micap_end > t OR micap_end is NaN)
        snapshot['aircraft_micap'] = len(all_ac_df[
            (all_ac_df['micap_start'].notna()) &
            (all_ac_df['micap_start'] <= t) &
            ((all_ac_df['micap_end'] > t) | (all_ac_df['micap_end'].isna()))
        ])
        
        # Count parts in fleet
        # Part is in fleet if fleet_start <= t and (fleet_end > t OR fleet_end is NaN)
        # NaN fleet_end means part is still in fleet at end of simulation
        snapshot['parts_fleet'] = len(all_parts_df[
            (all_parts_df['fleet_start'].notna()) &
            (all_parts_df['fleet_start'] <= t) & 
            ((all_parts_df['fleet_end'] > t) | (all_parts_df['fleet_end'].isna()))
        ])
        
        # Count parts in Condition F
        # Part is in Condition F if condition_f_start <= t and condition_f_end > t
        snapshot['parts_condition_f'] = len(all_parts_df[
            (all_parts_df['condition_f_start'].notna()) &
            (all_parts_df['condition_f_start'] <= t) &
            ((all_parts_df['condition_f_end'] > t) | (all_parts_df['condition_f_end'].isna()))
        ])
        
        # Count parts in Depot
        # Part is in Depot if depot_start <= t and depot_end > t
        snapshot['parts_depot'] = len(all_parts_df[
            (all_parts_df['depot_start'].notna()) &
            (all_parts_df['depot_start'] <= t) &
            ((all_parts_df['depot_end'] > t) | (all_parts_df['depot_end'].isna()))
        ])
        
        # Count parts in Condition A
        # Part is in Condition A if condition_a_start <= t and condition_a_end > t
        snapshot['parts_condition_a'] = len(all_parts_df[
            (all_parts_df['condition_a_start'].notna()) &
            (all_parts_df['condition_a_start'] <= t) &
            ((all_parts_df['condition_a_end'] > t) | (all_parts_df['condition_a_end'].isna()))
        ])
        
        wip_snapshots.append(snapshot)
    
    return pd.DataFrame(wip_snapshots)
