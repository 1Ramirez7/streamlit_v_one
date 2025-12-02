import streamlit as st
import numpy as np


def calculate_initial_allocation( # need to add to main.py 
    n_total_parts: int,
    n_total_aircraft: int,
    mission_capable_rate: float,
    depot_capacity: int,
    condemn_cycle: int,
    parts_in_depot: int,  # NEW: from UI
    parts_in_cond_f: int,  # NEW: from UI
    parts_in_cond_a: int  # NEW: from UI
) -> dict:
    """
    Calculate Initial Conditions.
    
    Parameters
    ----------
    n_total_parts : int - Total parts in the system
    n_total_aircraft : int - Total aircraft in the fleet
    mission_capable_rate : float
        Percentage (0.0 to 1.0) of aircraft that start with a part installed
    depot_capacity : int - Maximum parts that can be in depot
    condemn_cycle : int
        Maximum cycle number for randomizing initial cycles
    parts_in_depot : int - Number of parts starting in depot (from UI)
    parts_in_cond_f : int - Number of parts starting in Condition F (from UI)
    parts_in_cond_a : int - Number of parts starting in Condition A (from UI)
        
    Returns
    -------
    dict with keys:
        'parts_in_depot': Number of parts starting in depot
        'parts_in_cond_a': Number of parts starting in Condition A
        'parts_in_cond_f': Number of parts starting in Condition F
        'n_aircraft_with_parts': Number of aircraft with parts installed
        'n_aircraft_w_out_parts': Number of aircraft starting in MICAP
        'cond_a_part_ids': List of part_ids for Condition A
        'cond_a_cycles': List of randomized cycle numbers for Condition A parts (NEW)
        'cond_f_part_ids': List of part_ids for Condition F
        'depot_part_ids': List of part_ids for depot
        'micap_ac_ids': List of aircraft IDs starting in MICAP
    """
    
    # Validate mission_capable_rate
    if not (0.0 <= mission_capable_rate <= 1.0):
        raise ValueError(
            f"mission_capable_rate must be between 0.0 and 1.0, got {mission_capable_rate}"
        )
    
    # Step 1: Calculate aircraft with parts (mission capable). rounds up
    n_aircraft_with_parts = min(n_total_parts, int(np.ceil(mission_capable_rate * n_total_aircraft)))

    
    # Aircraft without parts (will start in MICAP)
    n_aircraft_w_out_parts = n_total_aircraft - n_aircraft_with_parts
    
    # Step 2: Calculate part_id lists
    # Part allocation order: aircraft -> depot -> cond_f -> cond_a
    
    # NEED to Get RID of using this to also allocate initial sim_id and des_id
    # until then the order these events are added need to happen in this order. IC_FS is 1st
    f_start_ac_part_ids = list(range(0, n_aircraft_with_parts))
    depot_part_ids = list(range(n_aircraft_with_parts, n_aircraft_with_parts + parts_in_depot)) # 2nd
    cond_f_part_ids = list(range(n_aircraft_with_parts + parts_in_depot, n_aircraft_with_parts
                                  + parts_in_depot + parts_in_cond_f)) # 3rd
    cond_a_part_ids = list(range(n_aircraft_with_parts + parts_in_depot + parts_in_cond_f, 
                                 n_aircraft_with_parts + parts_in_depot + parts_in_cond_f + parts_in_cond_a)) # 4th
    micap_ac_ids = list(range(n_aircraft_with_parts, n_aircraft_with_parts + n_aircraft_w_out_parts))

    # Generate randomized cycles for Condition A parts
    # added in DataFrameManager._create_condition_a_df
    # (1, condemn_cycle + 1) = 1 ≤ cycle ≤ condemn_cycle = randomly chosen between 1 and 20
    # remove the 1 so randomized does not start part in condemn. 
    # it will be better to have +1 so it starts parts in condemn
    # just need to handle the initial functions
    # to handle condemn parts
    # I guess code can be added to condmen parts with cycle >=20
    # but also parts shouldn't have cycles higher then condemn
    # so its better to find a part with a cycle above 20 then to have the 
    # model blindly handle them

    # I temporarily set cond_a_cycle to do one less because its restarting cycle in initial
    # so if it starts at cycle 19, it will be at cycle 20 by end of init cond
    # and init cond does not have code to handle condemn parts yet
    cond_a_cycles = [np.random.randint(1, condemn_cycle - 1) for _ in range(parts_in_cond_a)]

    # generate randomized cycles for parts starting in DEPOT
    # added in SimulationEngine.inject_initial_depot_parts
    # (1, condemn_cycle + 1) = 1 ≤ cycle ≤ condemn_cycle = randomly chosen between 1 and 20
    depot_cycles = [np.random.randint(1, condemn_cycle) for _ in range(parts_in_depot)]

    # generate randomized cycles for parts starting in CONDITION F
    # added in SimulationEngine.inject_init_cond_f
    cond_f_cycles = [np.random.randint(1, condemn_cycle) for _ in range(parts_in_cond_f)]

    return {
        'parts_in_depot': parts_in_depot,
        'parts_in_cond_a': parts_in_cond_a,
        'parts_in_cond_f': parts_in_cond_f,
        'n_aircraft_with_parts': n_aircraft_with_parts,
        'n_aircraft_w_out_parts': n_aircraft_w_out_parts,
        'f_start_ac_part_ids': f_start_ac_part_ids,
        'cond_a_part_ids': cond_a_part_ids,
        'cond_f_part_ids': cond_f_part_ids,
        'depot_part_ids': depot_part_ids,
        'micap_ac_ids': micap_ac_ids,
        'cond_a_cycles': cond_a_cycles,
        'depot_cycles': depot_cycles,
        'cond_f_cycles': cond_f_cycles # NEW: Return randomized cycles for Condition F
    }

def render_allocation_inputs(n_total_parts, n_total_aircraft, mission_capable_rate, 
                              depot_capacity, parts_air_dif):
    """
    Render Initial Part Allocation UI inputs in sidebar.
    
    Parameters
    ----------
    n_total_parts : int - Total parts in system
    n_total_aircraft : int - Total aircraft in fleet
    mission_capable_rate : float - Mission capable rate (0.0 to 1.0)
    depot_capacity : int - Maximum depot capacity
    parts_air_dif : int - Parts remaining after fleet allocation
    
    Returns
    -------
    tuple of (parts_in_depot, parts_in_cond_f, parts_in_cond_a)
    """
    import streamlit as st
    import numpy as np
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("Initial Part Allocation")
    
    # Calculate aircraft with parts for display
    n_aircraft_with_parts = min(n_total_parts, int(np.ceil(mission_capable_rate * n_total_aircraft)))
    
    # Display info
    st.sidebar.info(
        f"**Parts in Fleet:** {n_aircraft_with_parts}\n\n"
        f"**Parts to Allocate:** {parts_air_dif}"
    )

    parts_in_depot = st.sidebar.number_input(
        "Parts Starting in Depot",
        min_value=0,
        max_value=min(parts_air_dif, depot_capacity),
        value=min(parts_air_dif, depot_capacity),
        step=1,
        help=f"Parts starting in depot. Cannot exceed depot_capacity ({depot_capacity}) or remaining parts ({parts_air_dif})"
    )

    remaining_parts = parts_air_dif - parts_in_depot

    parts_in_cond_f = st.sidebar.number_input(
        "Parts Starting in Condition F",
        min_value=0,
        max_value=remaining_parts,
        value=remaining_parts,
        step=1,
        help=f"Parts starting in Condition F queue. Max: {remaining_parts} (remaining after depot allocation)"
    )

    parts_in_cond_a = st.sidebar.number_input(
        "Parts Starting in Condition A",
        min_value=0,
        max_value=remaining_parts - parts_in_cond_f,
        value=remaining_parts - parts_in_cond_f,
        step=1,
        help=f"Parts starting in Condition A (available inventory). Max: {remaining_parts - parts_in_cond_f}"
    )

    # Validation display
    total_allocated = n_aircraft_with_parts + parts_in_depot + parts_in_cond_f + parts_in_cond_a
    unallocated = n_total_parts - total_allocated
    
    if unallocated > 0:
        st.sidebar.warning(f"⚠️ {unallocated} parts unallocated")
    elif unallocated == 0:
        st.sidebar.success(f"✅ All {n_total_parts} parts allocated")
    else:
        st.sidebar.error(f"❌ Over-allocated by {abs(unallocated)} parts")

    with st.sidebar.expander("View Allocation Breakdown"):
        st.write(f"Fleet: {n_aircraft_with_parts}")
        st.write(f"Depot: {parts_in_depot}")
        st.write(f"Condition F: {parts_in_cond_f}")
        st.write(f"Condition A: {parts_in_cond_a}")
        st.write(f"Total: {total_allocated} / {n_total_parts}")
    
    return int(parts_in_depot), int(parts_in_cond_f), int(parts_in_cond_a)



def init_fleet_random():
    """
    Render UI controls for fleet duration random multiplier feature.
    
    Returns dict with: use_fleet_rand, fleet_rand_min, fleet_rand_max
    """
    st.sidebar.markdown("---")
    st.sidebar.subheader("Fleet Duration Randomization")
    
    use_fleet_rand = st.sidebar.checkbox(
        "Randomize Fleet Durations",
        value=True,
        help="Apply random multiplier to fleet durations for initial conditions to stagger fleet times"
    )
    
    if use_fleet_rand:
        st.sidebar.markdown("**Random Multiplier Range**")
        
        col1, col2 = st.sidebar.columns(2)
        
        with col1:
            fleet_rand_min = st.sidebar.number_input(
                "Min",
                min_value=0.01,
                max_value=1.0,
                value=0.01,
                step=0.01,
                format="%.2f",
                help="Minimum random multiplier value",
                key="fleet_rand_min"
            )
        
        with col2:
            fleet_rand_max = st.sidebar.number_input(
                "Max",
                min_value=0.01,
                max_value=1.0,
                value=1.0,
                step=0.01,
                format="%.2f",
                help="Maximum random multiplier value",
                key="fleet_rand_max"
            )
        
        # Validation
        if fleet_rand_min >= fleet_rand_max:
            st.sidebar.error("⚠️ Min must be less than Max")
            fleet_rand_min = 0.01
            fleet_rand_max = 1.0
        
        st.sidebar.info(f"Multiplier range: {fleet_rand_min:.2f} - {fleet_rand_max:.2f}")
        
    else:
        fleet_rand_min = 1.0
        fleet_rand_max = 1.0
    
    return {
        'use_fleet_rand': use_fleet_rand,
        'fleet_rand_min': fleet_rand_min,
        'fleet_rand_max': fleet_rand_max
    }




def init_depot_random():
    """
    Render UI controls for depot duration random multiplier feature.
    
    Returns dict with: use_depot_rand, depot_rand_min, depot_rand_max
    """
    st.sidebar.markdown("---")
    st.sidebar.subheader("Depot Duration Randomization")
    
    use_depot_rand = st.sidebar.checkbox(
        "Randomize Depot Durations",
        value=True,
        help="Apply random multiplier to depot durations in inject_initial_depot_parts to stagger depot completion times"
    )
    
    if use_depot_rand:
        st.sidebar.markdown("**Random Multiplier Range**")
        
        col1, col2 = st.sidebar.columns(2)
        
        with col1:
            depot_rand_min = st.sidebar.number_input(
                "Min",
                min_value=0.01,
                max_value=1.0,
                value=0.01,
                step=0.01,
                format="%.2f",
                help="Minimum random multiplier value for depot durations",
                key="depot_rand_min"
            )
        
        with col2:
            depot_rand_max = st.sidebar.number_input(
                "Max",
                min_value=0.01,
                max_value=1.0,
                value=1.0,
                step=0.01,
                format="%.2f",
                help="Maximum random multiplier value for depot durations",
                key="depot_rand_max"
            )
        
        # Validation
        if depot_rand_min >= depot_rand_max:
            st.sidebar.error("⚠️ Min must be less than Max")
            depot_rand_min = 0.01
            depot_rand_max = 1.0
        
        st.sidebar.info(f"Depot multiplier range: {depot_rand_min:.2f} - {depot_rand_max:.2f}")
        
    else:
        depot_rand_min = 1.0
        depot_rand_max = 1.0
    
    return {
        'use_depot_rand': use_depot_rand,
        'depot_rand_min': depot_rand_min,
        'depot_rand_max': depot_rand_max
    }