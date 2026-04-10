import streamlit as st
import pandas as pd
import subprocess
import time
import os
import plotly.graph_objects as go
from stock_estimates_db import StockEstimatesDB

st.set_page_config(page_title="Stock Estimates Dashboard", layout="wide")

db = StockEstimatesDB('data/stock_estimates.db')

base_path = os.path.dirname(os.path.abspath(__file__))


def load_data():
    data = db.select_all()
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date']).dt.date
    return df


def dashboard_page():
    st.title("📈 Analyst Stock Estimates Dashboard")

    df = load_data()

    if df.empty:
        st.warning("No data found in the database.")
    else:
        st.sidebar.header("Global Filters")

        all_symbols = sorted(df['symbol'].unique())
        selected_symbols = st.sidebar.multiselect(
            "Select Tickers", options=all_symbols, default=[])

        min_date = df['date'].min()
        max_date = df['date'].max()

        date_range = st.sidebar.date_input(
            "Select Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )

        if len(date_range) == 2:
            start_date, end_date = date_range
            mask = (
                (df['symbol'].isin(selected_symbols)) &
                (df['date'] >= start_date) &
                (df['date'] <= end_date)
            )
            filtered_df = df.loc[mask]
        else:
            st.info("Please select an end date to update the view.")
            filtered_df = df[df['symbol'].isin(selected_symbols)]

        if not filtered_df.empty:
            m1, m2 = st.columns(2)
            m1.metric("Records Found", len(filtered_df))
            m2.metric(
                "Last Updated", f"{df['date'].max()}")

            tab1, tab2 = st.tabs(["📋 Data Table", "📈 Price Trends"])

            with tab1:
                st.dataframe(filtered_df, width="content")

            with tab2:
                st.subheader("Mean Price Target Over Time")
                line_fig = go.Figure()
                for symbol in filtered_df['symbol'].unique():
                    sym_df = filtered_df[filtered_df['symbol'] == symbol].sort_values('date')
                    line_fig.add_trace(go.Scatter(
                        x=sym_df['date'], y=sym_df['meanPriceTarget'],
                        mode='lines+markers', name=f"{symbol} Mean Target",
                        legendgroup=symbol
                    ))
                    line_fig.add_trace(go.Scatter(
                        x=sym_df['date'], y=sym_df['price'],
                        mode='lines+markers', name=f"{symbol} Price",
                        legendgroup=symbol, line=dict(dash='dot')
                    ))
                line_fig.update_layout(title="Trend of Analyst Price Targets vs Current Price")
                st.plotly_chart(line_fig)

                st.subheader("Recommendation Rate Over Time")
                rec_fig = go.Figure()
                for symbol in filtered_df['symbol'].unique():
                    sym_df = filtered_df[filtered_df['symbol'] == symbol].sort_values('date')
                    rec_fig.add_trace(go.Scatter(
                        x=sym_df['date'], y=sym_df['recommendationRate'],
                        mode='lines+markers', name=symbol
                    ))
                rec_fig.update_layout(
                    title="Analyst Recommendation Rate",
                    yaxis=dict(range=[0, 5], title="Recommendation Rate")
                )
                st.plotly_chart(rec_fig)
        else:
            st.error("No data matches the selected filters.")


def script_runner_page():
    st.title("Script Runner")

    scripts = {
        "processor": {"name": "Dump estimates", "description": "This Python script is an ETL pipeline that retrieves analyst estimates and price targets for B3 stocks via the MSN Finance API. It maps tickers from a CSV file, fetches financial metrics like recommendations and volatility, and transforms the raw JSON into structured records. Finally, it saves the data to a database while managing API rate limits through built-in delays and logging.", "path": os.path.join(base_path, "dump_stock_estimates.py"), "log": "data/dump_stock_estimates.log"},
        "fetcher": {"name": "Map symbols", "description": "This script automates the mapping of B3 stock tickers to their corresponding MSN Finance internal IDs using the Bing Autosuggest API. It iterates through a list of symbols, cleans the API response by removing unnecessary metadata, and incrementally saves the results to a CSV file.",  "path": os.path.join(base_path, "map_symbols.py"), "log": 'data/map_symbols.log'}
    }

    if 'processes' not in st.session_state:
        st.session_state.processes = {key: None for key in scripts.keys()}

    for key, info in scripts.items():
            st.write("---")
            
            # --- HEADER & STATUS ---
            process = st.session_state.processes[key]
            is_running = process is not None and process.poll() is None

            st.subheader(info["name"])
            
            btn_col1, btn_col2, _ = st.columns([2, 2, 6])
            
            with btn_col1:
                if st.button(f"Execute", key=f"run_{key}", disabled=is_running, use_container_width=True):
                    try:
                        proc = subprocess.Popen(["python", info["path"]])
                        st.session_state.processes[key] = proc
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")

            with btn_col2:
                if st.button(f"Abort", key=f"abort_{key}", disabled=not is_running, type="secondary", use_container_width=True):
                    if process:
                        process.terminate()
                        st.session_state.processes[key] = None
                        st.toast(f"Termination signal sent to {info['name']}", icon="🛑")
                        st.rerun()

            st.caption(info["description"])

            with st.expander("Show Console Logs", expanded=is_running):
                if os.path.exists(info['log']):
                    with open(info['log'], "r") as f:
                        lines = f.readlines()
                        st.code("".join(lines[-10:]))
                    logListen = st.checkbox("Listen to new logs (outside of execution trigger section)", key=key)
                    if logListen:
                        time.sleep(5)
                        st.rerun()
                else:
                    st.info("Log file not found.")

    if any(p and p.poll() is None for p in st.session_state.processes.values()):
        time.sleep(2)
        st.rerun()


pg = st.navigation([
    st.Page(dashboard_page, title="Dashboard", icon=":material/tile_medium:"),
    st.Page(script_runner_page, title="Script Runner",
            icon=":material/play_arrow:"),
])

pg.run()

db.close()
