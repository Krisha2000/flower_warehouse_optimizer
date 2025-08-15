# ./scripts/app.py

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import time

# --- Configuration ---
DB_URI = "postgresql+psycopg2://airflow:airflow@postgres/airflow"
ENGINE = create_engine(DB_URI)

# --- Dashboard UI ---
st.set_page_config(layout="wide")
st.title("🌸 Flower Warehouse Real-Time Dashboard")

# --- Data Loading Function ---
def load_data():
    """Queries the database to get the latest data."""
    try:
        inventory_df = pd.read_sql("SELECT * FROM inventory ORDER BY arrival_date", ENGINE)
        tasks_df = pd.read_sql("SELECT * FROM tasks ORDER BY timestamp DESC LIMIT 100", ENGINE)
        return inventory_df, tasks_df
    except Exception as e:
        st.error(f"Error connecting to the database: {e}")
        return pd.DataFrame(), pd.DataFrame()

# --- Refresh Control ---
refresh_interval = 10  # seconds
st.caption(f"⏳ Dashboard refreshes every {refresh_interval} seconds")

# --- Main Dashboard Display ---
placeholder = st.empty()

inventory, tasks = load_data()

with placeholder.container():
    # KPIs
    total_items = int(inventory['quantity'].sum()) if not inventory.empty else 0
    unique_flowers = int(inventory['flower_type'].nunique()) if not inventory.empty else 0
    pending_tasks = int(tasks['task_type'].count()) if not tasks.empty else 0

    st.markdown("### 📈 Key Metrics")
    kpi1, kpi2, kpi3 = st.columns(3)
    kpi1.metric(label="Total Items in Inventory", value=f"{total_items:,}")
    kpi2.metric(label="Unique Flower Types", value=unique_flowers)
    kpi3.metric(label="Recent Pending Tasks", value=pending_tasks)

    st.markdown(f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Data Tables
    st.markdown("### 📦 Current Inventory")
    st.dataframe(inventory, use_container_width=True)

    st.markdown("### 📋 Recent Tasks")
    st.dataframe(tasks, use_container_width=True)

# Auto-refresh after interval
time.sleep(refresh_interval)

# Use correct rerun method depending on Streamlit version
if hasattr(st, "experimental_rerun"):
    st.experimental_rerun()
elif hasattr(st, "rerun"):
    st.rerun()
