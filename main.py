"""
main.py
-------
Landing page for DES Simulation - select Solo or Scenarios run mode.
"""
import streamlit as st

st.set_page_config(
    page_title="DES",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("Discrete Event Simulation Model")
st.markdown("### Aircraft Parts Lifecycle Simulation")
st.markdown("---")

st.markdown("""
Select a simulation mode to get started:

- **Solo Run**: Run a single simulation with full visualization and detailed results
- **Scenarios**: Run multiple simulations varying depot capacity and total parts
""")

col1, col2 = st.columns(2)

with col1:
    st.markdown("### Solo Run")
    st.markdown("""
    - Single simulation execution
    - Detailed statistics and plots
    - WIP over time visualizations
    - Duration distribution plots
    """)
    st.page_link("pages/solo_run.py", label="Go to Solo Run →", icon="🔬")

with col2:
    st.markdown("### Scenarios")
    st.markdown("""
    - Vary Depot Capacity and Total Parts
    - Compare multiple scenarios
    - Find optimal configurations
    - Fast Mode toggle for speed
    """)
    st.page_link("pages/scenarios.py", label="Go to Scenarios →", icon="📊")

st.markdown("---")

st.warning("""
**⚠️ Resource Limits:** Streamlit Community Cloud has resource limits (≈2.7GB memory, 2 cores max). 
Large simulations may be throttled or stopped. For production use, deploy locally or use a dedicated cloud service.  
[Streamlit documentation →](https://docs.streamlit.io/deploy/streamlit-community-cloud/manage-your-app)
""")

st.caption("Use the sidebar to navigate between pages.")
