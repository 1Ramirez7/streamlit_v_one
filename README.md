# DES - Streamlit Application

This directory contains the Streamlit web application for the Discrete Event Simulation.

## Overview

The Streamlit app provides an interactive interface for:
- Configuring simulation parameters
- Running discrete event simulations
- Visualizing simulation results
- Exporting results to Excel

## Application Structure

### Core Files

| File | Purpose |
|------|---------|
| `main.py` | Landing page - navigation to Solo Run or Scenarios |
| `run_streamlit_app.py` | Application launcher |
| `simulation_engine.py` | Core simulation logic |
| `initialization.py` | Initial conditions setup |
| `parameters.py` | Centralized parameter management |
| `post_sim.py` | Post-simulation statistics and figures |
| `session_manager.py` | Session state handling |
| `utils.py` | Utility functions |

### Pages

| File | Purpose |
|------|---------|
| `pages/solo_run.py` | Single simulation with full visualization |
| `pages/scenarios.py` | Multi-scenario simulations |

### UI Components

| File | Purpose |
|------|---------|
| `ui/ui_components.py` | Solo Run sidebar widgets |
| `ui/dist_plots.py` | Duration distribution plots |
| `ui/wip_plots.py` | Work-in-progress plots |
| `ui/stats.py` | Statistics display |
| `ui/downloads.py` | Export functionality |

## Running the Application

From the project root:

```bash
python run_streamlit_app.py
```

Or directly:

```bash
cd streamlit_app
streamlit run main.py
```

## Variable Names Reference

See [VARIABLE_REFERENCE.md](VARIABLE_REFERENCE.md) for complete variable names documentation.

git mv streamlit_app/ui/ui_scenarios.py streamlit_app/ui/new_name.py

git mv streamlit_app/utils_multi.py streamlit_app/sc_utils.py