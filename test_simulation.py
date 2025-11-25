"""
The test file skips ui_components.py entirely and is basically a stripped-down version of 
main.py where you manually set all parameters for testing. No UI, just direct function calls.

Returns sim_df and des_df for custom testing.
- add so micap cant be > than n_total_aircraft
- add code so if sim_df has install values, then it must have a destwo_id and actwo_id
- how to add check to make sure micap_df does not have duplicate ac_id
- how to add check to make sure condtion_a & f df do not have duplicate part_id
- add code so when part-row is added it must have a value for cycle
- add code so check if part_id has a duplicate cycle number. 
    - check if part_id has rows after condemn = yes
- add check if acone_id or actwo_id present then the desone_id or destwo_id must be present. vice versa as well. 
"""

import pandas as pd
import numpy as np
from data_manager import DataFrameManager
from simulation_engine import SimulationEngine
from initialization import Initialization
from utils import calculate_initial_allocation

import warnings # to silent future warnings, comment to test
warnings.simplefilter("ignore", category=FutureWarning)

# Set random seed for reproducibility (matching ui_components.py default)
np.random.seed(132)

# Simulation parameters (matching ui_components.py defaults)
n_total_parts = 12 # 35
n_total_aircraft = 10 # 30

# NEW: Mission capable rate
mission_capable_rate = 0.60 # 0.60 

# Timeline parameters
warmup_periods = 0
analysis_periods = 6200
closing_periods = 0
sim_time = warmup_periods + analysis_periods + closing_periods  # Total: 500 days

# Stage duration parameters
sone_dist = "Weibull"  # Distribution type for Fleet stage
sone_mean = 9.17  # Shape parameter for Weibull
sone_sd = 384.13  # Scale parameter for Weibull
sthree_dist = "Normal"  # Distribution type for Depot stage  
sthree_mean = 40.0 # 1.0
sthree_sd = 1.2 # 0.2

# Depot capacity
depot_capacity = 6 # 20
condemn_cycle = 10 # 20
condemn_depot_fraction = 0.10 # 0.10
part_order_lag = 55 # 25

# NEW: Hardcoded initial part allocation (manually set for testing)
parts_in_depot = 2 # 5
parts_in_cond_f = 2 # 12
parts_in_cond_a = 2 # 0

# Randomization initial fleet and depot durations
# functions: init_fleet_random & init_depot_random
use_fleet_rand = False
fleet_rand_min = 1.0
fleet_rand_max = 1.0
use_depot_rand = False
depot_rand_min = 1.0
depot_rand_max = 1.0

print("Running simulation...")
print(f"Parameters: {n_total_parts} parts, {n_total_aircraft} aircraft, {sim_time} days")

# Calculate initial allocation & include exogenous params
allocation = calculate_initial_allocation(
    n_total_parts=n_total_parts,
    n_total_aircraft=n_total_aircraft,
    mission_capable_rate=mission_capable_rate,
    depot_capacity=depot_capacity,
    condemn_cycle=condemn_cycle,
    parts_in_depot=parts_in_depot,
    parts_in_cond_f=parts_in_cond_f,
    parts_in_cond_a=parts_in_cond_a
)

print(f"\nInitial Allocation:")
print(f"  Aircraft with parts: {allocation['n_aircraft_with_parts']}")
print(f"  Aircraft without parts (MICAP): {allocation['n_aircraft_w_out_parts']}")
print(f"  Parts in depot: {allocation['parts_in_depot']}")
print(f"  Parts in Condition F: {allocation['parts_in_cond_f']}")
print(f"  Parts in Condition A: {allocation['parts_in_cond_a']}")

# error check
total_allocated = (parts_in_depot + parts_in_cond_f + parts_in_cond_a + 
                   allocation['n_aircraft_with_parts'])
if total_allocated != n_total_parts:
    print(f"\n❌ ERROR: Part allocation mismatch!")
    print(f"  n_total_parts: {n_total_parts}")
    print(f"  parts_in_depot: {parts_in_depot}")
    print(f"  parts_in_cond_f: {parts_in_cond_f}")
    print(f"  parts_in_cond_a: {parts_in_cond_a}")
    print(f"  n_aircraft_with_parts: {allocation['n_aircraft_with_parts']}")
    print(f"  Total allocated: {total_allocated}")
    print(f"  Difference: {total_allocated - n_total_parts}")
    import sys
    sys.exit(1)

# Create DataFrameManager
df_manager = DataFrameManager(
    n_total_parts=n_total_parts, 
    n_total_aircraft=n_total_aircraft, 
    sim_time=sim_time,
    sone_mean=sone_mean,
    sthree_mean=sthree_mean,
    allocation=allocation
)

# Create SimulationEngine
sim_engine = SimulationEngine(
    df_manager=df_manager,
    sone_dist=sone_dist,
    sone_mean=sone_mean,
    sone_sd=sone_sd,
    sthree_dist=sthree_dist,
    sthree_mean=sthree_mean,
    sthree_sd=sthree_sd,
    sim_time=sim_time,
    depot_capacity=depot_capacity,
    condemn_cycle=condemn_cycle,
    condemn_depot_fraction=condemn_depot_fraction,
    part_order_lag=part_order_lag,
    use_fleet_rand=use_fleet_rand,
    fleet_rand_min=fleet_rand_min,
    fleet_rand_max=fleet_rand_max,
    use_depot_rand=use_depot_rand,
    depot_rand_min=depot_rand_min,
    depot_rand_max=depot_rand_max
)

# Run simulation
validation_results = sim_engine.run()

# Run ONLY initialization steps. comment out steps to not run as needed
#initializer = Initialization(sim_engine) # always uncomment if just running init...
#initializer.run_initialization() # uncomment this to run all functions in init
#initializer.event_ic_izfs()  # Initialize first Fleet cycles
#initializer.event_ic_ijd()  # Inject parts into Depot
#initializer.event_ic_ijcf() # Inject parts into Condition F
#initializer.event_ic_ijca() # Inject parts into Condition A
#initializer.eventm_ic_izca_cr() # Resolve MICAP with Condition A parts
#initializer.eventm_ic_fe_cf() # Move Fleet End to Condition F
#sim_engine._schedule_initial_events()

# Get final DataFrames as needed
sim_df = df_manager.sim_df
des_df = df_manager.des_df
# temp condemn new part log
condemn_new_log = pd.DataFrame(df_manager.condemn_new_log)

# Sort des_df for validation functions
des_df = des_df.sort_values(['ac_id','des_id']).reset_index(drop=True)

# Sort sim_df for validation functions
sim_df = sim_df.sort_values(['part_id','sim_id']).reset_index(drop=True)

print(f"Simulation complete!")
print(f"sim_df: {len(sim_df)} rows")
print(f"des_df: {len(des_df)} rows")


# ============================================================================
# VALIDATION FUNCTIONS
# ============================================================================


def function_one(des_df):
    """
    Purpose:
        Validate continuity of fleet_start relative to previous install_end
        for each ac_id in des_df. Also confirm each aircraft has the expected
        number of continuity checks (n_cycles - 1).
        Assumption: des_df is pre-sorted by ['ac_id', 'des_id'] 

    Logic:
        1. Identify all unique ac_id values.
        2. For each aircraft:
            - Compare consecutive rows belonging to the same ac_id.
            - Compute delta = fleet_start - previous install_end.
        3. Count expected vs actual continuity checks.
        4. Flag any ac_id that does not have (n_cycles - 1) checks.

    Returns:
        dict with:
            'deltas': list of delta values aligned to des_df index
            'flagged_aircraft': list of ac_id values missing checks
            'summary': DataFrame of counts per aircraft
    """
    result = [np.nan] * len(des_df)
    flagged_aircraft = []
    summary_rows = []

    unique_aircraft = des_df['ac_id'].unique()

    for ac in unique_aircraft:
        ac_rows = des_df[des_df['ac_id'] == ac]
        n_cycles = len(ac_rows)
        expected_checks = n_cycles - 1
        actual_checks = 0

        for i in range(1, len(ac_rows)):
            curr_idx = ac_rows.index[i]
            prev_idx = ac_rows.index[i - 1]

            prev_install_end = des_df.loc[prev_idx, 'install_end']
            curr_fleet_start = des_df.loc[curr_idx, 'fleet_start']

            if pd.notna(prev_install_end):
                result[curr_idx] = curr_fleet_start - prev_install_end
                actual_checks += 1
            else:
                result[curr_idx] = np.nan

        summary_rows.append({
            'ac_id': ac,
            'n_cycles': n_cycles,
            'expected_checks': expected_checks,
            'actual_checks': actual_checks
        })

        if actual_checks != expected_checks:
            flagged_aircraft.append(ac)

    summary_df = pd.DataFrame(summary_rows)

    return {
        'deltas': result,
        'flagged_aircraft': flagged_aircraft,
        'summary': summary_df
    }

def function_two(des_df):
    """
    Validate continuity between aircraft replacement events.

    Purpose:
        Ensures timing consistency from the point an aircraft requires a new part 
        (install_start) to when it receives one — either immediately or after MICAP.

    Logic:
        • If install_start = NaN -> no event to check -> return NaN  
        • If MICAP is empty -> aircraft received part immediately  
            fleet_end - install_start ≈ 0  
            (handled by handle_aircraft_needs_part when part available)  
        • If MICAP exists -> aircraft waited for part  
            micap_end - install_start ≈ 0  
            (handled by handle_part_completes_depot when part becomes available)

    Validation goal:
        Confirm that either:
            fleet_end = install_start   (no MICAP)
        or  micap_end = install_start   (MICAP resolved)
        ensuring seamless event chaining between Fleet -> MICAP/Install.
    
    Excel equivalent:
    =IF(O2="","", IF(K2="", H2-O2, K2-O2))
    """
    result = []
    
    for i in range(len(des_df)):
        install_start = des_df.loc[i, 'install_start']
        micap_end = des_df.loc[i, 'micap_end']
        fleet_end = des_df.loc[i, 'fleet_end']
        
        if pd.isna(install_start):
            result.append(np.nan)
        elif pd.isna(micap_end):
            result.append(fleet_end - install_start)
        else:
            result.append(micap_end - install_start)
    
    return result


def function_three(des_df):
    """
    Validate Fleet stage timing consistency.

    Purpose:
        Ensures that Fleet duration aligns with recorded start and end times:
            (fleet_end - fleet_start) = fleet_duration

    Logic:
        - If fleet_end = NaN -> skip (no completed Fleet event)
        - Else -> delta = (fleet_end - fleet_start) - fleet_duration
          delta = 0 confirms correct timing

    Validation goal:
        Confirms event_ic_izfs() and process_new_cycle_stages()
        correctly calculate Fleet durations.
        If delta != 0:
            - Fleet duration logic was altered incorrectly, or
            - Model miswrote DataFrame via faulty sim_id or des_id linkage, or
            - Python 0-based indexing caused misalignment of event rows.
        If fleet_start = Nan 
            - connection between install_end can be the issue
            - or the aircraft ac_id did not properly log
                - Got Nan when adding ac start at MICAP
                - but if ac start at MICAP then it shouldn't have fleet values. 

    Output:
        Dict with:
            'deltas': list of delta values per record (rounded to 4 decimals)
            'failed_rows': DataFrame with first 3 failing rows (specific columns only)
        Expected: all deltas = 0.
    """
    result = []
    failed_indices = []
    
    for i in range(len(des_df)):
        fleet_end = des_df.loc[i, 'fleet_end']
        fleet_start = des_df.loc[i, 'fleet_start']
        fleet_duration = des_df.loc[i, 'fleet_duration']
        
        if pd.isna(fleet_end):
            result.append(np.nan)
        elif pd.isna(fleet_start) or pd.isna(fleet_duration):
            result.append(-999)  # Error: fleet_end exists but start/duration missing
            failed_indices.append(i)
        else:
            val = (fleet_end - fleet_start) - fleet_duration
            result.append(round(val, 4))
            if round(val, 4) != 0:
                failed_indices.append(i)
    
    # Get first 3 failed rows with specific columns
    cols = ['des_id', 'ac_id', 'simone_id', 'fleet_duration', 'fleet_start', 
            'fleet_end', 'micap_start', 'micap_end', 'parttwo_id', 
            'install_start', 'install_end']
    
    if failed_indices:
        failed_rows = des_df.loc[failed_indices[:3], cols].copy()
    else:
        failed_rows = pd.DataFrame(columns=cols)
    
    return {
        'deltas': result,
        'failed_rows': failed_rows
    }

def function_four(des_df):
    """
    Validate MICAP timing integrity and path-specific write logic.

    Purpose:
        Ensures MICAP start/end times and duration were written correctly 

    MICAP is reactionary
    Aircraft enter or leave MICAP only through these functions:

        1. handle_aircraft_needs_part()
           -> Writes micap_start when no replacement part is available.
              (Only entry path to MICAP)

        2. handle_part_completes_depot()
           -> Writes micap_end and calculates micap_duration
              when a depot-repaired part resolves a MICAP aircraft.

        3. handle_new_part_arrives()
           -> Writes micap_end and calculates micap_duration
              when a newly ordered part arrives and resolves a MICAP.

    Validation logic:
        - If micap_end = NaN -> skip (no completed MICAP)
        - Else -> delta = (micap_end - micap_start) - micap_duration
          delta = 0 confirms correct timing written by one of the three handlers.

    Diagnostic meaning:
        If delta != 0:
            - Edits may have altered any of the MICAP-related handlers above, or
            - Improper sim_id or des_id (primary key) linkage exists, or
            - Python 0-based indexing caused event misalignment.

    Output:
        List of delta values (expected all = 0).
        Non-zero results indicate handler or key linkage issues in MICAP timing.
    """
    result = []
    
    for i in range(len(des_df)):
        micap_end = des_df.loc[i, 'micap_end']
        micap_start = des_df.loc[i, 'micap_start']
        micap_duration = des_df.loc[i, 'micap_duration']
        
        if pd.isna(micap_end):
            result.append(np.nan)
        elif pd.isna(micap_start) or pd.isna(micap_duration):
            result.append(-999)  # Error: micap_end exists but start/duration missing
        else:
            result.append((micap_end - micap_start) - micap_duration)
    
    return result


def function_five(des_df):
    """
    Validate Install stage timing integrity.

    Purpose:
        Ensures install_start, install_end, and install_duration are written correctly.
        Install duration should always be zero:
            (install_end - install_start) = install_duration = 0

    Logic:
        - If install_end = NaN -> skip
        - Else -> delta = (install_end - install_start) - install_duration
          delta = 0 confirms correct timing.

    Source functions:
        Values are written together in:
            handle_aircraft_needs_part(),
            handle_part_completes_depot(),
            handle_new_part_arrives().

    Diagnostic meaning:
        If delta != 0 -> timing or linkage error in one of the above functions,
        or misalignment from sim_id/des_id handling or Python indexing.

    Output:
        List of delta values (expected all = 0).

    Excel equivalent:
        =IF(P2="","", (P2-O2)-N2)
    """
    result = []
    
    for i in range(len(des_df)):
        install_end = des_df.loc[i, 'install_end']
        install_start = des_df.loc[i, 'install_start']
        install_duration = des_df.loc[i, 'install_duration']
        
        if pd.isna(install_end):
            result.append(np.nan)
        elif pd.isna(install_start) or pd.isna(install_duration):
            result.append(-999)  # Error: install_end exists but start/duration missing
        else:
            result.append((install_end - install_start) - install_duration)
    
    return result


def function_six(des_df):
    """
    Validate aircraft cycle distribution by ac_id.

    Purpose:
        Counts how many rows (cycles) each aircraft completed in the simulation.
        Returns basic statistics and identifies the top and bottom 5 aircraft 
        based on cycle frequency.

    Output:
        Dictionary with:
            'mean': average cycle count across aircraft,
            'std': standard deviation,
            'top_5': list of (ac_id, count) tuples for highest 5,
            'bottom_5': list of (ac_id, count) tuples for lowest 5.
    """
    # Count cycles per aircraft
    ac_counts = des_df['ac_id'].value_counts().sort_index()

    # Compute statistics
    mean_cycles = ac_counts.mean()
    std_cycles = ac_counts.std()

    # Get top and bottom 5 aircraft by count
    top_5 = list(ac_counts.nlargest(5).items())
    bottom_5 = list(ac_counts.nsmallest(5).items())

    # Return results
    return {
        'mean': round(mean_cycles, 2),
        'std': round(std_cycles, 2),
        'top_5': top_5,
        'bottom_5': bottom_5
    }

def function_seven(des_df):
    """
    Validate unique ID integrity for des_id, simone_id, and simtwo_id.

    Purpose:
        Checks for duplicate values within each of the three key columns 
        (des_id, simone_id, simtwo_id). 
        Each should contain unique identifiers across all rows.
    There should be no duplicate values. 
    """
    duplicate_report = {}

    for col in ['des_id', 'simone_id', 'simtwo_id']:
        # Count occurrences
        counts = des_df[col].value_counts(dropna=True)
        # Extract values that appear more than once
        duplicates = counts[counts > 1].index.tolist()
        duplicate_report[col] = duplicates

    return duplicate_report

def function_eight(sim_df):
    """
    Validate unique ID integrity for sim_id, desone_id, and destwo_id.

    Purpose:
        Checks for duplicate values in sim_df key columns 
        - Count occurrences and flag any values appearing more than once.
        - Check for gaps in sim_id sequence.

    Diagnostic meaning:
        There should be no duplicate values.
    """
    duplicate_report = {}
    for col in ['sim_id', 'desone_id', 'destwo_id']:
        counts = sim_df[col].value_counts(dropna=True)
        duplicates = counts[counts > 1].index.tolist()
        duplicate_report[col] = duplicates
    
    # Check for gaps in sim_id sequence
    unique_sim_ids = sorted(sim_df['sim_id'].dropna().unique())
    gaps = []
    
    for i in range(len(unique_sim_ids) - 1):
        if unique_sim_ids[i+1] - unique_sim_ids[i] > 1:
            gaps.append((int(unique_sim_ids[i]), int(unique_sim_ids[i+1])))
    
    return {
        'duplicates': duplicate_report,
        'gaps': gaps
    }

def function_nine(sim_df, condemn_cycle=5):
    """
    Validate part cycle counts and flag overused parts.
    Function that handles condemn parts
        build_event_index
    
    Purpose:
        Ensures no part exceeds condemn cycle limit + 1
        Need plus + 1 because new parts always have a zero cycle 
        obersevation to record what aircraft it was installed in.

    Returns:
        dict:
            {
                'status': 'PASS' | 'FAIL' | 'EXCEEDED_LIMIT',
                'flagged_count': int,
                'flagged_parts': list (only if <=5 flagged),
                'mean': float,
                'std': float,
                'top_5': list of tuples (part_id, count),
                'bottom_5': list of tuples (part_id, count)
            }
    """
    # --- Compute usage counts ---
    part_counts = sim_df['part_id'].value_counts().sort_index()

    mean_parts = round(part_counts.mean(), 2)
    std_parts = round(part_counts.std(), 2)
    top_5 = list(part_counts.nlargest(5).items())
    bottom_5 = list(part_counts.nsmallest(5).items())

    # --- Check for cycle limit violations ---
    max_allowed = condemn_cycle + 1
    flagged = part_counts[part_counts > max_allowed]
    flagged_count = len(flagged)

    # --- Check for sequential gaps ---
    unique_part_ids = sorted(sim_df['part_id'].dropna().unique())
    gaps = [(int(unique_part_ids[i]), int(unique_part_ids[i + 1]))
            for i in range(len(unique_part_ids) - 1)
            if unique_part_ids[i + 1] - unique_part_ids[i] > 1]

    # --- Determine overall statuses independently ---
    status = "PASS"
    if flagged_count > 0:
        status = "FAIL" if flagged_count <= 5 else "EXCEEDED_LIMIT"
    gap_status = "GAPS_DETECTED" if gaps else "PASS"

    result = {
        'mean': mean_parts,
        'std': std_parts,
        'top_5': top_5,
        'bottom_5': bottom_5,
        'flagged_count': flagged_count,
        'flagged_parts': list(flagged.index[:5]) if flagged_count > 0 else [],
        'gaps': gaps,
        'status': status,
        'gap_status': gap_status
    }

    return result


    # --- Determine overall status ---
    if len(gaps) > 0:
        result['status'] = 'GAPS_DETECTED'
    elif flagged_count == 0:
        result['status'] = 'PASS'
    elif flagged_count <= 5:
        result['status'] = 'FAIL'
        result['flagged_parts'] = list(flagged.index)
    else:
        result['status'] = 'EXCEEDED_LIMIT'

    return result

def function_ten(sim_df):
    """
    Validate Fleet stage timing consistency.

    Purpose:
        Ensures that Fleet duration aligns with recorded start and end times:
            (fleet_end - fleet_start) = fleet_duration

    Logic:
        - If fleet_end = NaN -> skip (no completed Fleet event)
        - Else -> delta = (fleet_end - fleet_start) - fleet_duration
          delta = 0 confirms correct timing

    Validation goal:
        Confirms event_ic_izfs() and process_new_cycle_stages()
        correctly calculate Fleet durations.
        If delta != 0:
            - Fleet duration logic was altered incorrectly, or
            - Model miswrote DataFrame via faulty sim_id or des_id linkage, or
            - Python 0-based indexing caused misalignment of event rows.

    Output:
        List of delta values per record (rounded to 4 decimals).
        Expected: all = 0.
    """
    result = []
    
    for i in range(len(sim_df)):
        fleet_end = sim_df.loc[i, 'fleet_end']
        fleet_start = sim_df.loc[i, 'fleet_start']
        fleet_duration = sim_df.loc[i, 'fleet_duration']
        
        if pd.isna(fleet_end):
            result.append(np.nan)
        elif pd.isna(fleet_start) or pd.isna(fleet_duration):
            result.append(-999)  # Error: fleet_end exists but start/duration missing
        else:
            val = (fleet_end - fleet_start) - fleet_duration
            result.append(round(val, 4))
    
    return result

def function_eleven(sim_df):
    """
    Validate Install stage timing integrity.

    Purpose:
        Ensures install_start, install_end, and install_duration are written correctly.
        Install duration should always be zero:
            (install_end - install_start) = install_duration = 0

    Logic:
        - If install_end = NaN -> skip
        - Else -> delta = (install_end - install_start) - install_duration
          delta = 0 confirms correct timing.

    Source functions:
        Values are written together in:
            handle_aircraft_needs_part(),
            handle_part_completes_depot(),
            handle_new_part_arrives().

    Diagnostic meaning:
        If delta != 0 -> timing or linkage error in one of the above functions,
        or misalignment from sim_id/des_id handling or Python indexing.

    Output:
        List of delta values (expected all = 0).

    Excel equivalent:
        =IF(P2="","", (P2-O2)-N2)
    """
    result = []
    
    for i in range(len(sim_df)):
        install_end = sim_df.loc[i, 'install_end']
        install_start = sim_df.loc[i, 'install_start']
        install_duration = sim_df.loc[i, 'install_duration']
        
        if pd.isna(install_end):
            result.append(np.nan)
        elif pd.isna(install_start) or pd.isna(install_duration):
            result.append(-999)  # Error: install_end exists but start/duration missing
        else:
            result.append((install_end - install_start) - install_duration)
    
    return result


def function_twelve(sim_df):
    """
    Purpose:
        Validate continuity of fleet_start relative to previous install_end
        for each part_id in sim_df. Also confirm each part_id has the expected
        number of continuity checks (n_cycles - 1).

    Assumptions:
        - sim_df is pre-sorted by ['part_id', 'sim_id'] before being passed in.

    Logic:
        1. Count how many continuity checks are made per part.
        2. Flag any part_id that does not have (n_cycles - 1) continuity checks.

    Returns:
        dict with:
            'deltas': list of delta values (aligned to sim_df index)
            'flagged_parts': list of part_id values missing checks
            'summary': DataFrame of counts per part
    """

    result = [np.nan] * len(sim_df)
    flagged_parts = []
    summary_rows = []

    unique_parts = sim_df['part_id'].unique()

    # Track continuity counts for each part_id
    for part in unique_parts:
        part_rows = sim_df[sim_df['part_id'] == part]
        n_cycles = len(part_rows)
        expected_checks = n_cycles - 1
        actual_checks = 0

        # Iterate through consecutive rows (already sorted)
        for i in range(1, len(part_rows)):
            curr_idx = part_rows.index[i]
            prev_idx = part_rows.index[i - 1]

            prev_install_end = sim_df.loc[prev_idx, 'install_end']
            curr_fleet_start = sim_df.loc[curr_idx, 'fleet_start']

            if pd.notna(prev_install_end):
                result[curr_idx] = curr_fleet_start - prev_install_end
                actual_checks += 1
            else:
                result[curr_idx] = np.nan

        # Track validation summary for this part
        summary_rows.append({
            'part_id': part,
            'n_cycles': n_cycles,
            'expected_checks': expected_checks,
            'actual_checks': actual_checks
        })

        if actual_checks != expected_checks:
            flagged_parts.append(part)

    summary_df = pd.DataFrame(summary_rows)

    return {
        'deltas': result,
        'flagged_parts': flagged_parts,
        'summary': summary_df
    }

def function_thirteen(sim_df):
    """
    Validate chronological order of stage start and end times in sim_df.
    This function might overlap with other checks so look to see if needed. 

    Purpose:
        Ensures all stage end times are greater than or equal to their
        corresponding start times. Detects logic or write-order errors
        that would result in negative durations.

    Checks:
        - fleet_end < fleet_start
        - condition_f_end < condition_f_start
        - depot_end < depot_start
        - condition_a_end < condition_a_start
        - install_end < install_start
    """
    failed_checks = []

    # Each check: compare start/end for NaN-safe rows only
    if (sim_df['fleet_end'] < sim_df['fleet_start']).any():
        failed_checks.append("fleet_end < fleet_start")
    if (sim_df['condition_f_end'] < sim_df['condition_f_start']).any():
        failed_checks.append("condition_f_end < condition_f_start")
    if (sim_df['depot_end'] < sim_df['depot_start']).any():
        failed_checks.append("depot_end < depot_start")
    if (sim_df['condition_a_end'] < sim_df['condition_a_start']).any():
        failed_checks.append("condition_a_end < condition_a_start")
    if (sim_df['install_end'] < sim_df['install_start']).any():
        failed_checks.append("install_end < install_start")

    if failed_checks:
        return {"status": "FAIL", "failed_checks": failed_checks}
    else:
        return {"status": "PASS", "failed_checks": []} # end F13

def function_thirteen_one(sim_df):
    """
    For every part_id (integer):
        At least ONE of these must be a REAL float (not NaN):
            fleet_end, condition_f_end, depot_end, condition_a_end
    FAIL:
        Count failures + return first 5 failing rows.
        Returned table prints vertically.
    WHY IT WAS MADE
        when adding initial condition f parts, those parts where never process in the model
        so they had NA for almost every column. No other check pick up the 
        incorrect results. 

    - first culript to cause error: engine.initialize_condition_f (see push notes in cca6fed)
        - cause: addition of engine.event_ic_ijcf led to bug
    """
    required_cols = [
        'sim_id','part_id','desone_id','acone_id',
        'fleet_start','fleet_end','condition_f_start','condition_f_end',
        'depot_start','depot_end','destwo_id','actwo_id',
        'condition_a_start','condition_a_end',
        'install_start','install_end','cycle','condemn']

    df = sim_df[pd.to_numeric(sim_df['part_id'], errors='coerce').notna()].copy()
    test_cols = ['fleet_end','condition_f_end','depot_end','condition_a_end']
    def has_real_float(row):
        return any(isinstance(row[c], float) and pd.notna(row[c]) for c in test_cols)
    fail_mask = ~df.apply(has_real_float, axis=1)
    failing_rows = df[fail_mask].copy()
    failing_rows = failing_rows.round(2)# Round float values for readability
    if failing_rows.empty:
        return {"status": "PASS", "fail_count": 0,
            "fail_rows": pd.DataFrame(columns=required_cols)}
    out = failing_rows[required_cols].head(5).T# Return in vertical layout (transpose)
    return {"status": "FAIL", "fail_count": len(failing_rows), "fail_rows": out}



def function_fourteen():
    """
    Check if sim_df row count matches expected events based on
    Formula: expected = sim_time / (sone_mean + sthree_mean) * (n_total_aircraft + 1)
    PASS if within ±3%, WARN if ±5%, FAIL if beyond.
    
    Sometimes edits don't properly continue events in the simulation, but
    the checks in place can return proper results. So this formula make sures 
    sim events or number of rows in the sim_df are close to what is expected
    base on the params. 

    IMPORTANT: This only works when no MICAP aircraft so depot capacity and lag days 
    need to allow no MICAP
    """
    expected = (sim_time / (sone_mean + sthree_mean)) * (n_total_aircraft + 1)
    actual = len(sim_df)
    pct = (actual / expected) * 100
    dev = abs(pct - 100)
    if dev <= 3: status = "PASS"
    elif dev <= 5: status = "FAIL" # both fail as # of events should be close to calculated
    else: status = "FAIL"
    return {"expected_count": round(expected, 2), "actual_count": actual, 
            "percent_of_expected": round(pct, 2), "status": status} # end F14


# ============================================================================
# Helper Functions
# ============================================================================




# ============================================================================
# RUN VALIDATION FUNCTIONS AND CHECK RESULTS
# ============================================================================

print("\n" + "="*80)
print("VALIDATION CHECKS")
print("="*80)

# Function 1
print("\nFunction 1: Fleet Start Continuity (des_df)")
func1_result = function_one(des_df)
func1_series = pd.Series(func1_result['deltas'])
func1_errors = func1_series[func1_series.notna() & (func1_series != 0)]
if len(func1_errors) == 0:
    print("✓ PASS: All values are 0 or empty")
else:
    print(f"❌ FAIL: Found {len(func1_errors)} non-zero values")
    print(f"  Non-zero values: {func1_errors.tolist()[:10]}")
if func1_result['flagged_aircraft']:
    print(f"  ⚠ Flagged ac_id(s) missing continuity: {func1_result['flagged_aircraft']}") # end F1


# Function 2
print("\nFunction 2: Install Start Logic")
func2_result = function_two(des_df)
func2_series = pd.Series(func2_result)
func2_errors = func2_series[func2_series.notna() & (func2_series != 0)]
if len(func2_errors) == 0:
    print("✓ PASS: All values are 0 or empty")
else:
    print(f"❌ FAIL: Found {len(func2_errors)} non-zero values")
    print(f"  Non-zero values: {func2_errors.tolist()[:10]}")

# Function 3
print("\nFunction 3: Fleet Duration Validation")
func3_result = function_three(des_df)
func3_series = pd.Series(func3_result['deltas'])
func3_errors = func3_series[func3_series.notna() & (func3_series != 0)]
if len(func3_errors) == 0:
    print("✓ PASS: All values are 0 or empty")
else:
    print(f"❌ FAIL: Found {len(func3_errors)} non-zero values")
    print(f"  Non-zero values: {func3_errors.tolist()[:10]}")
    if not func3_result['failed_rows'].empty:
        print("\n  First 3 Failed Rows:")
        print(func3_result['failed_rows'].to_string(index=False))

# Function 4
print("\nFunction 4: MICAP Duration Validation")
func4_result = function_four(des_df)
func4_series = pd.Series(func4_result)
func4_errors = func4_series[func4_series.notna() & (func4_series != 0)]
if len(func4_errors) == 0:
    print("✓ PASS: All values are 0 or empty")
else:
    print(f"❌ FAIL: Found {len(func4_errors)} non-zero values")
    print(f"  Non-zero values: {func4_errors.tolist()[:10]}")

# Function 5
print("\nFunction 5: Install Duration Validation")
func5_result = function_five(des_df)
func5_series = pd.Series(func5_result)
func5_errors = func5_series[func5_series.notna() & (func5_series != 0)]
if len(func5_errors) == 0:
    print("✓ PASS: All values are 0 or empty")
else:
    print(f"❌ FAIL: Found {len(func5_errors)} non-zero values")
    print(f"  Non-zero values: {func5_errors.tolist()[:10]}")

# Function 6
print("\nFunction 6: Aircraft Cycle Count Validation")
func6_result = function_six(des_df)

# Extract values from returned dictionary
mean_cycles = func6_result['mean']
std_cycles = func6_result['std']
top_5 = func6_result['top_5']
bottom_5 = func6_result['bottom_5']

# function 6: Display summary
print(f"\nAverage Cycles per Aircraft: {mean_cycles:.2f}")
print(f"Standard Deviation: {std_cycles:.2f}")

print("\nTop 5 Aircraft (Highest Cycle Counts):")
for ac_id, count in top_5:
    print(f"  ac_id {ac_id}: {count} cycles")

print("\nBottom 5 Aircraft (Lowest Cycle Counts):")
for ac_id, count in bottom_5:
    print(f"  ac_id {ac_id}: {count} cycles") # end of F6

# Function 7
print("\nFunction 7: Unique ID Validation (des_id, simone_id, simtwo_id)")
func7_result = function_seven(des_df)

for col, dup_list in func7_result.items():
    if len(dup_list) == 0:
        print(f"✓ {col}: No duplicates found")
    else:
        print(f"❌ {col}: Found {len(dup_list)} duplicate values")
        print(f"  Duplicates: {dup_list[:10]}")  # prints first 10 if many. end of F7

print("SIM_DF TEST FOLLOW")

# Function 8
print("\nFunction 8: Unique ID Validation (sim_id, desone_id, destwo_id)")
func8_result = function_eight(sim_df)

# Check duplicates
for col, dup_list in func8_result['duplicates'].items():
    if len(dup_list) == 0:
        print(f"✓ {col}: No duplicates found")
    else:
        print(f"❌ {col}: Found {len(dup_list)} duplicate values")
        print(f"  Duplicates: {dup_list[:10]}")

# Check for gaps in sim_id
if func8_result['gaps']:
    print(f"\n❌ Gap Check FAILED: {len(func8_result['gaps'])} gap(s) detected in sim_id")
    for before, after in func8_result['gaps']:
        print(f"  Gap between sim_id {before} and {after}")
else:
    print("\n✅ Gap Check PASSED")  # end of F8

# Function 9
print("\nFunction 9: Part Usage Distribution")
func9_result = function_nine(sim_df, condemn_cycle=condemn_cycle)

print(f"Average Cycles per Part: {func9_result['mean']:.2f}")
print(f"Standard Deviation: {func9_result['std']:.2f}")

print("\nTop 5 Parts (Most Cycles):")
for part_id, count in func9_result['top_5']:
    print(f"  part_id {part_id}: {count} cycles")

print("\nBottom 5 Parts (Fewest Cycles):")
for part_id, count in func9_result['bottom_5']:
    print(f"  part_id {part_id}: {count} cycles")

# --- Check for gaps ---
if func9_result['gaps']:
    print(f"\n❌ Gap Check FAILED: {len(func9_result['gaps'])} gap(s) detected")
    for before, after in func9_result['gaps']:
        print(f"  Gap between part_id {before} and {after}")
else:
    print("\n✅ Gap Check PASSED")

# --- Check cycle limits ---
status = func9_result["status"]
flagged_count = func9_result["flagged_count"]

if status == "PASS":
    print("✅ Cycle Limit PASSED")
else:
    print(f"❌ Cycle Limit FAILED: {flagged_count} parts exceeded limit")
    print(f"Flagged Parts: {func9_result['flagged_parts']}") # end F9

# Function 10
print("\nFunction 10: Fleet Duration Validation (sim_df)")
func10_result = function_ten(sim_df)
func10_series = pd.Series(func10_result)
func10_errors = func10_series[func10_series.notna() & (func10_series != 0)]

if len(func10_errors) == 0:
    print("✓ PASS: All values are 0 or empty")
else:
    print(f"❌ FAIL: Found {len(func10_errors)} non-zero values")
    print(f"  Non-zero values: {func10_errors.tolist()[:10]}") # end F10

# Function 11
print("\nFunction 11: Install Duration Validation (sim_df)")
func11_result = function_eleven(sim_df)
func11_series = pd.Series(func11_result)
func11_errors = func11_series[func11_series.notna() & (func11_series != 0)]

if len(func11_errors) == 0:
    print("✓ PASS: All values are 0 or empty")
else:
    print(f"❌ FAIL: Found {len(func11_errors)} non-zero values")
    print(f"  Non-zero values: {func11_errors.tolist()[:10]}")  # end of F11

# Function 12
print("\nFunction 12: Fleet Start Continuity (sim_df)")
func12_result = function_twelve(sim_df)
# Extract the deltas list for validation
func12_series = pd.Series(func12_result['deltas'])
func12_errors = func12_series[func12_series.notna() & (func12_series != 0)]
if len(func12_errors) == 0:
    print("✓ PASS: All values are 0 or empty")
else:
    print(f"❌ FAIL: Found {len(func12_errors)} non-zero values")
    print(f"  Non-zero values: {func12_errors.tolist()[:10]}")
if func12_result['flagged_parts']:
    print(f"  ⚠ Flagged part_id(s) missing continuity: {func12_result['flagged_parts']}") # end F12

# Function 13
print("\nFunction 13: Chronological Consistency Check")
func13_result = function_thirteen(sim_df)

if func13_result["status"] == "PASS":
    print("✅ Function 13 PASSED: All stage end times are >= start times.")
else:
    print("❌ Function 13 FAILED: One or more stages have inconsistent times.")
    print(f"Failed Checks: {func13_result['failed_checks']}") # end F13

# Function 13.1
print("\nFunction 13.1: Part End-Stage Completion Check (sim_df)")
func13_1 = function_thirteen_one(sim_df)

if func13_1["status"] == "PASS":
    print("✓ PASS: At least one valid end-stage float found for every part_id")
else:
    print(f"❌ FAIL: {func13_1['fail_count']} part_id rows failed checks")
    print("\nFirst 5 failing rows (vertical format):\n")
    print(func13_1["fail_rows"].to_string()) # end F13.1

# Function 14
print("\nFunction 14: Simulation Volume Validation (sim_df count vs expected)")
func14_result = function_fourteen()

print(f"Expected Event Count: {func14_result['expected_count']:.0f}")
print(f"Actual Event Count:   {func14_result['actual_count']}")
print(f"Percent of Expected:  {func14_result['percent_of_expected']:.2f}%")

if func14_result["status"] == "PASS":
    print("✅ Within ±3% of expected count.")
elif func14_result["status"] == "WARN":
    print("⚠️ Deviation within ±5% of expected count.")
else:
    print("❌ Deviation exceeds ±5% of expected count.")

print("\n" + "="*80)
print("VALIDATION COMPLETE")
print("="*80)

# Add results as new columns to des_df
des_df['func1_result'] = func1_result['deltas']
des_df['func2_result'] = func2_result
des_df['func3_result'] = func3_result['deltas']
des_df['func4_result'] = func4_result
des_df['func5_result'] = func5_result

# Add function_ten results to sim_df
sim_df['func10_result'] = func10_result
sim_df['func11_result'] = func11_result
sim_df['func12_result'] = func12_result


# ============================================================================
# VIEW DATAFRAME IN VS CODE
# ============================================================================
# To view des_df in VS Code:
# 1. Set a breakpoint on the line below
# 2. Run in debug mode (F5)
# 3. In the debug console, type: des_df
# 4. Click the table icon to open the Data Viewer

print("\nTo view des_df in VS Code Data Viewer:")
print("1. Set breakpoint here and run in debug mode")
print("2. Type 'des_df' in debug console")
print("3. Click the table icon next to the variable")

# ============================================================================
# EXPORT TO EXCEL (UNCOMMENT TO USE)
# ============================================================================
# Export both DataFrames to Excel with separate sheets
output_file = "simulation_results_with_validation.xlsx"
with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
    sim_df.to_excel(writer, sheet_name='sim_df', index=False)
    des_df.to_excel(writer, sheet_name='des_df', index=False)
    condemn_new_log.to_excel(writer, sheet_name='p_log', index=False)
print(f"\n✓ Results exported to {output_file}")


