import streamlit as st
from streamlit_app.simulation_engine import SimulationEngine
from plotnine import ggplot, aes, geom_line, labs, theme_minimal

def render_wip_plots(sim_engine: SimulationEngine):
    plot_params = {
        "Fleet": ("Fleet Wip Graph", "cycle", )
    }

def viz_wip(sim_engine, dataframe, title, x_col, y_col):
    clean_df = (
        sim_engine.df.sim_df
        .groupby('cycle', as_index=False)
        .agg(part_count=('part_id', 'count'))
    )
    clean_df2 = (
        sim_engine.df.sim_df
    )
    plot = (
        ggplot(clean_df, aes(x=x_col, y=y_col)) +
        geom_line() +
        labs(title=title, x=x_col, y=y_col) +
        theme_minimal()
    )



import streamlit as st
import plotly.express as px

def render_wip_plots(sim_engine: SimulationEngine):
    fleet_wip_df = fleet_wip(sim_engine)
    condition_f_wip_df = condition_f_wip(sim_engine)
    depot_wip_df = depot_wip(sim_engine)
    condition_a_wip_df = condition_a_wip(sim_engine)
    install_wip_df = install_wip(sim_engine)

    st.plotly_chart(fleet_wip_df, use_container_width=True)
    st.plotly_chart(condition_f_wip_df, use_container_width=True)
    st.plotly_chart(depot_wip_df, use_container_width=True)
    st.plotly_chart(condition_a_wip_df, use_container_width=True)
    st.plotly_chart(install_wip_df, use_container_width=True)


def fleet_wip(sim_engine: SimulationEngine):
    df = sim_engine.df.sim_df.groupby('cycle', as_index=False)['fleet_duration'].mean()

    fig = px.line(
        df, x='cycle', y='fleet_duration',
        title='Fleet WIP Over Cycles',
        labels={'cycle': 'Cycle', 'fleet_duration': 'Average Fleet Duration'}
    )

    fig.update_layout(
        plot_bgcolor='#0e1117',
        paper_bgcolor='#0e1117',
        font=dict(color='white'),
        title=dict(font=dict(size=16, color='white', family='Arial', weight='bold')),
    )
    return fig


def condition_f_wip(sim_engine: SimulationEngine):
    df = sim_engine.df.sim_df.groupby('cycle', as_index=False)['condition_f_duration'].mean()

    fig = px.line(
        df, x='cycle', y='condition_f_duration',
        title='Condition F WIP Over Cycles',
        labels={'cycle': 'Cycle', 'condition_f_duration': 'Average Condition F Duration'}
    )

    fig.update_layout(
        plot_bgcolor='#0e1117',
        paper_bgcolor='#0e1117',
        font=dict(color='white'),
        title=dict(font=dict(size=16, color='white', family='Arial', weight='bold')),
    )
    return fig


def depot_wip(sim_engine: SimulationEngine):
    df = sim_engine.df.sim_df.groupby('cycle', as_index=False)['depot_duration'].mean()

    fig = px.line(
        df, x='cycle', y='depot_duration',
        title='Depot WIP Over Cycles',
        labels={'cycle': 'Cycle', 'depot_duration': 'Average Depot Duration'}
    )

    fig.update_layout(
        plot_bgcolor='#0e1117',
        paper_bgcolor='#0e1117',
        font=dict(color='white'),
        title=dict(font=dict(size=16, color='white', family='Arial', weight='bold')),
    )
    return fig

def condition_a_wip(sim_engine: SimulationEngine):
    df = sim_engine.df.sim_df.groupby('cycle', as_index=False)['condition_a_duration'].mean()

    fig = px.line(
        df, x='cycle', y='condition_a_duration',
        title='Condition A WIP Over Cycles',
        labels={'cycle': 'Cycle', 'condition_a_duration': 'Average Condition A Duration'}
    )

    fig.update_layout(
        plot_bgcolor='#0e1117',
        paper_bgcolor='#0e1117',
        font=dict(color='white'),
        title=dict(font=dict(size=16, color='white', family='Arial', weight='bold')),
    )
    return fig


def install_wip(sim_engine: SimulationEngine):
    df = sim_engine.df.sim_df.groupby('cycle', as_index=False)['install_duration'].mean()

    fig = px.line(
        df, x='cycle', y='install_duration',
        title='Install WIP Over Cycles',
        labels={'cycle': 'Cycle', 'install_duration': 'Average Install Duration'}
    )

    fig.update_layout(
        plot_bgcolor='#0e1117',
        paper_bgcolor='#0e1117',
        font=dict(color='white'),
        title=dict(font=dict(size=16, color='white', family='Arial', weight='bold')),
    )
    return fig