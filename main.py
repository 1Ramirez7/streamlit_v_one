"""
main.py
-------
Landing page for DES Simulation - select Solo or Multi run mode.
"""
import streamlit as st

st.set_page_config(
    page_title="DES Simulation",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("✈️ DES Simulation Runner")
st.markdown("### Aircraft Parts Lifecycle Simulation")
st.markdown("---")

st.markdown("""
Select a simulation mode to get started:

- **Solo Run**: Run a single simulation with full visualization and detailed results
- **Multi Run**: Run multiple simulations varying depot capacity and total parts
""")

col1, col2 = st.columns(2)

with col1:
    st.markdown("### 🔬 Solo Run")
    st.markdown("""
    - Single simulation execution
    - Full parameter control
    - Detailed statistics and plots
    - WIP over time visualizations
    - Duration distribution plots
    """)
    st.page_link("pages/solo_run.py", label="Go to Solo Run →", icon="🔬")

with col2:
    st.markdown("### 🔄 Multi Run")
    st.markdown("""
    - Vary depot_capacity and n_total_parts
    - Compare multiple scenarios
    - Find optimal configurations
    - Comparative charts and analysis
    - Export results to Excel/ZIP
    """)
    st.page_link("pages/multi_run.py", label="Go to Multi Run →", icon="🔄")

st.markdown("---")
st.caption("Use the sidebar to navigate between pages.")
