"""
Session State Manager for DES Simulation

Manages Streamlit session state for simulation results.
Keeps track of:
- Whether a simulation has run
- The parameters used for that run
- The results (datasets, validation, allocation)

This fixes the bug where changing sidebar params after a run
would not update until a full restart.
"""
import streamlit as st
from typing import Dict, Any, Optional


class SessionStateManager:
    """
    Minimal session state manager for single simulation runs.
    
    Usage:
        session_mgr = SessionStateManager()  # Auto-initializes
        
        # After running simulation:
        session_mgr.store_run(params, datasets, validation_results, allocation)
        
        # To check/retrieve:
        if session_mgr.has_run():
            data = session_mgr.get_run()
            params = data['params']
    """
    
    def __init__(self):
        """Initialize session state if not already done."""
        self._initialize_if_needed()
    
    def _initialize_if_needed(self):
        """Create session state keys if they don't exist."""
        if 'run_data' not in st.session_state:
            st.session_state.run_data = {
                'has_run': False,
                'params': None,
                'datasets': None,
                'validation_results': None,
                'allocation': None,
                'post_sim': None  # === POSTSIM CLASS - NEW ===
            }
    
    def has_run(self) -> bool:
        """Check if a simulation has been run."""
        return st.session_state.run_data['has_run']
    
    def store_run(self, params: Dict, datasets, validation_results: Dict, 
                  allocation: Dict) -> None:
        """
        Store results from a simulation run.
        
        Args:
            params: Dictionary of parameters used (from render_sidebar)
            datasets: DataSets object with simulation results
            validation_results: Dictionary from engine.run()
            allocation: Dictionary from calculate_initial_allocation
        """
        st.session_state.run_data = {
            'has_run': True,
            'params': params.to_dict(), # this fix the first bug: not properly storing params so confirm it is best to use now witl params class
            'datasets': datasets,
            'validation_results': validation_results,
            'allocation': allocation,
            'post_sim': validation_results.get('post_sim')  # === POSTSIM CLASS - NEW ===
        }
    
    def get_run(self) -> Dict[str, Any]:
        """
        Get all run data as a dictionary.
        
        Returns:
            Dictionary with keys: has_run, params, datasets, 
            validation_results, allocation
        """
        return st.session_state.run_data
    
    def get_params(self) -> Optional[Dict]:
        """Get just the params from the stored run."""
        return st.session_state.run_data['params']
    
    def get_datasets(self):
        """Get just the datasets from the stored run."""
        return st.session_state.run_data['datasets']
    
    def get_validation_results(self) -> Optional[Dict]:
        """Get just the validation_results from the stored run."""
        return st.session_state.run_data['validation_results']
    
    def get_allocation(self) -> Optional[Dict]:
        """Get just the allocation from the stored run."""
        return st.session_state.run_data['allocation']
    
    # === POSTSIM CLASS - NEW ===
    def get_post_sim(self):
        """Get just the post_sim from the stored run."""
        return st.session_state.run_data.get('post_sim')
    
    def clear_run(self) -> None:
        """Clear all stored run data (reset to initial state)."""
        st.session_state.run_data = {
            'has_run': False,
            'params': None,
            'datasets': None,
            'validation_results': None,
            'allocation': None,
            'post_sim': None  # === POSTSIM CLASS - NEW ===
        }
