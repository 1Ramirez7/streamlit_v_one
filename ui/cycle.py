import streamlit as st
import plotly.graph_objects as go
import math
import pandas as pd
from simulation_engine import SimulationEngine

def render_cycle(sim_engine: SimulationEngine):
    sim_time = sim_engine.df.sim_time
    st.title("Cycle Visualization")
    fig = make_animation(sim_engine, sim_time)


def get_metrics(sim_engine: SimulationEngine, sim_time):
    counts = get_stage_counts(sim_engine.df.sim_df, sim_time)
    total = sim_engine.df.n_total_parts
    depot_capacity = sim_engine.depot_capacity
    required_parts = sim_engine.df.n_total_aircraft

    return {
        "Fleet": {"Parts": f"{counts['Fleet']} / {required_parts}"},
        "Condition F": {"Parts": f"{counts['Condition F']} / {total}"},
        "Depot": {"Parts": f"{counts['Depot']} / {depot_capacity}"},
        "Condition A": {"Parts": f"{counts['Condition A']} / {total}"},
    }


def get_stage_counts(sim_df, sim_time):
    counts = {}
    counts["Fleet"] = ((sim_df["fleet_start"] <= sim_time) & (sim_df["fleet_end"] > sim_time)).sum()
    counts["Condition F"] = ((sim_df["condition_f_start"] <= sim_time) & (sim_df["condition_f_end"] > sim_time)).sum()
    counts["Depot"] = ((sim_df["depot_start"] <= sim_time) & (sim_df["depot_end"] > sim_time)).sum()
    counts["Condition A"] = ((sim_df["condition_a_start"] <= sim_time) & (sim_df["condition_a_end"] > sim_time)).sum()
    return counts


def make_frame(metrics):
    """Return a list of Scatter traces for this time step."""
    radius = 2
    angles = {
        "Fleet": math.radians(90),
        "Condition F": math.radians(0),
        "Depot": math.radians(270),
        "Condition A": math.radians(180)
    }
    positions = {n: (radius * math.cos(a), radius * math.sin(a)) for n, a in angles.items()}
    traces = []
    for name, (x, y) in positions.items():
        label = "<br>".join([f"{k}: {v}" for k, v in metrics[name].items()])
        traces.append(go.Scatter(
            x=[x], y=[y],
            mode="markers+text",
            text=[f"<b>{name}</b><br>{label}"],
            textposition="middle center",
            marker=dict(size=140, color="#1f77b4", opacity=0.9, line=dict(width=3, color='white'))
        ))
    return traces


def make_animation(sim_engine: SimulationEngine, max_sim_time):
    fig = go.Figure()
    frames = []
    fig.update_layout(
    annotations=[{
        "text": f"<b style='color:white;'>Sim Day: 0</b>",
        "x": 0.02, "y": 0.95,  # top-left corner
        "xref": "paper", "yref": "paper",
        "showarrow": False,
        "font": {"color": "white", "size": 18}
    }]
    )
    for t in range(0, int(max_sim_time) + 1, 1):
        metrics = get_metrics(sim_engine, t)
        frame_data = make_frame(metrics)
        frames.append(go.Frame(
            data=frame_data,
            name=str(t),
            layout=go.Layout(
                annotations=[{
                    "text": f"<b style='color:white;'>Sim Day: {t}</b>",
                    "x": 0.02, "y": 0.95,
                    "xref": "paper", "yref": "paper",
                    "showarrow": False,
                    "font": {"color": "white", "size": 18}
                }]
            )
        ))
    fig.frames = frames
    fig.add_traces(frames[0].data)
    fig.update_layout(
        updatemenus=[{
            "type": "buttons",
            "buttons": [
                {"label": "▶ Play", "method": "animate",
                 "args": [None, {"frame": {"duration": 800, "redraw": True}, "fromcurrent": True}]},
                {"label": "⏸ Pause", "method": "animate",
                 "args": [[None], {"mode": "immediate"}]},
            ],
            "direction": "left",
            "pad": {"r": 10, "t": 80},
            "showactive": False,
            "x": 0.1, "xanchor": "right",
            "y": 0, "yanchor": "top"
        }],
        plot_bgcolor="#0e1117",
        paper_bgcolor="#0e1117",
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        margin=dict(l=0, r=0, t=0, b=0),
        height=650,
        width=650,
        title=dict(
            text="<b style='color:white;'>Part Lifecycle Cycle Animation</b>",
            x=0.5,
            font=dict(color="white", size=22)
        )
    )
    st.plotly_chart(fig, use_container_width=True)