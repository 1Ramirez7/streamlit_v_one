import pandas as pd
import matplotlib.pyplot as plt


class DataSets:
    """
    Container for post-simulation datasets ready for data science/analysis.
    Created after simulation finishes.

    TLDR Implementation Steps:
    1. Created DataSets class in ds/data_science.py with build_part_ac_df method.
    2. simulation_engine.py: Added datasets parameter to __init__, set self.datasets = datasets, called build_part_ac_df in run().
    3. main.py: Added import 'from ds.data_science import DataSets'.
    4. main.py: Created 'datasets = DataSets()' after df_manager creation.
    5. main.py: Added 'datasets=datasets' to SimulationEngine call.
    6. main.py: Updated Excel export to use 'datasets.all_parts_df' and 'datasets.aircraft_df'.
    """
    def __init__(self):
        self.all_parts_df = None
        self.all_ac_df = None
        self.wip_df = None  # WIP metrics calculated post-simulation
        self.n_total_aircraft = None  # Store for fleet calculation
        # Add more datasets as needed


    def build_part_ac_df(self, get_parts_df_func, get_ac_df_func, get_log_dataframe):
        """
        Populate datasets using exports from PARTS and AIRCRAFT classes.
        Also calculates WIP (Work In Progress) metrics from aircraft data.

        Sample UPDATE usage: (note update imports as needed)
            engine.run
                self.datasets.build_part_ac_df(
                            self.part_manager.get_all_parts_data_df, 
                            self.ac_manager.get_all_ac_data_df)
            main.py
                datasets.all_parts_df
                datasets.all_ac_df
                datasets.wip_df
            test_simulation.py: 
                all_parts_df = datasets.all_parts_df
                wip_df = datasets.wip_df
        """
        self.all_parts_df = get_parts_df_func()
        self.all_ac_df = get_ac_df_func()
        self.cond_a_df = get_log_dataframe()
        #self.wip_df = calculate_wip_overtime(self.all_ac_df, self.all_parts_df) # temporarily removing

    # temporarily putting WIP plot code here for testing, but its still slow

    def build_wip_from_micap_log(self, micap_log_df, n_total_aircraft):
        """
        Build WIP DataFrame from micap_state.get_log_dataframe().
        
        Uses pre-logged micap_count from simulation events.
        O(N) where N = number of MICAP events (fast!).
        
        Parameters
        ----------
        micap_log_df : pd.DataFrame
            From micap_state.get_log_dataframe() with columns:
            - event_time: Time of event
            - micap_count: MICAP count at that time
            - event: 'ENTER_MICAP' or 'EXIT_MICAP'
        n_total_aircraft : int
            Total aircraft in simulation (for fleet calculation)
        
        Returns
        -------
        pd.DataFrame
            Columns: ['time', 'aircraft_micap', 'aircraft_fleet']
        """
        self.n_total_aircraft = n_total_aircraft
        
        if micap_log_df.empty:
            self.wip_df = pd.DataFrame(columns=['time', 'aircraft_micap', 'aircraft_fleet'])
            return self.wip_df
        
        # Sort by event_time
        sorted_log = micap_log_df.sort_values('event_time').copy()
        
        # Build WIP data from log (already has micap_count!)
        self.wip_df = pd.DataFrame({
            'time': sorted_log['event_time'].values,
            'aircraft_micap': sorted_log['micap_count'].values,
            'aircraft_fleet': n_total_aircraft - sorted_log['micap_count'].values
        })
        
        return self.wip_df

    def plot_aircraft_wip_combined(self):
        """
        Plot both Fleet and MICAP on same figure for comparison.
        
        Returns
        -------
        matplotlib.figure.Figure
            Figure with both Fleet and MICAP plots
        """
        if self.wip_df is None or len(self.wip_df) == 0:
            fig, ax = plt.subplots(figsize=(12, 6))
            ax.text(0.5, 0.5, 'No WIP data available', ha='center', va='center')
            ax.set_title('Aircraft Status Over Time')
            return fig
        
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.step(self.wip_df['time'], self.wip_df['aircraft_fleet'], 
                where='post', linewidth=2, color='green', label='Fleet')
        ax.step(self.wip_df['time'], self.wip_df['aircraft_micap'], 
                where='post', linewidth=2, color='red', label='MICAP')
        ax.set_xlabel('Simulation Time')
        ax.set_ylabel('Number of Aircraft')
        ax.set_title('Aircraft Status Over Time (Fleet + MICAP)')
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.set_ylim(bottom=0)
        plt.tight_layout()
        return fig
    
    def plot_micap_over_time(self):
        """
        Plot MICAP aircraft count over time using step plot.
        
        Returns
        -------
        matplotlib.figure.Figure
            Figure with MICAP plot
        """
        if self.wip_df is None or len(self.wip_df) == 0:
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.text(0.5, 0.5, 'No MICAP data available', ha='center', va='center')
            ax.set_title('MICAP Status Over Time')
            return fig
        
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.step(self.wip_df['time'], self.wip_df['aircraft_micap'], 
                where='post', linewidth=2, color='red')
        ax.set_xlabel('Simulation Time')
        ax.set_ylabel('Number of Aircraft in MICAP')
        ax.set_title('MICAP Status Over Time')
        ax.grid(True, alpha=0.3)
        ax.set_ylim(bottom=0)
        plt.tight_layout()
        return fig


    def plot_fleet_over_time(self):
        """
        Plot Fleet aircraft count over time using step plot.
        
        Returns
        -------
        matplotlib.figure.Figure
            Figure with Fleet plot
        """
        if self.wip_df is None or len(self.wip_df) == 0:
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.text(0.5, 0.5, 'No Fleet data available', ha='center', va='center')
            ax.set_title('Fleet Status Over Time')
            return fig
        
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.step(self.wip_df['time'], self.wip_df['aircraft_fleet'], 
                where='post', linewidth=2, color='green')
        ax.set_xlabel('Simulation Time')
        ax.set_ylabel('Number of Aircraft in Fleet')
        ax.set_title('Fleet Status Over Time')
        ax.grid(True, alpha=0.3)
        ax.set_ylim(bottom=0)
        plt.tight_layout()
        return fig


    def plot_condition_a_wip_over_time(self):
        """
        Plot Condition A WIP (parts in inventory) over time using step plot.
        
        Returns
        -------
        matplotlib.figure.Figure
            Figure with Condition A WIP plot
        """
        if self.cond_a_df is None or len(self.cond_a_df) == 0:
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.text(0.5, 0.5, 'No Condition A data available', ha='center', va='center')
            ax.set_title('Condition A WIP Over Time')
            return fig
        
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.step(self.cond_a_df['event_time'], self.cond_a_df['count'], 
                where='post', linewidth=2, color='mediumpurple')
        ax.set_xlabel('Simulation Time')
        ax.set_ylabel('Parts in Condition A')
        ax.set_title('Condition A WIP Over Time')
        ax.grid(True, alpha=0.3)
        ax.set_ylim(bottom=0)
        plt.tight_layout()
        return fig