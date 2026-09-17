"""
Ride Analytics Dashboard — reads Gold-layer CSVs exported from the Databricks pipeline.
Run: streamlit run streamlit_app.py
Expects hourly_demand.csv, zone_summary.csv, tip_by_hour.csv in the same folder
(pulled from /dbfs/tmp/gold_export in the Databricks notebook).
"""

import streamlit as st
import pandas as pd
import os

st.set_page_config(page_title="Ride Analytics Lakehouse", layout="wide")

DATA_DIR = os.path.dirname(os.path.abspath(__file__))


def load_csv(name):
    path = os.path.join(DATA_DIR, name)
    if not os.path.exists(path):
        st.error(f"Missing {name} — export it from the 03_aggregate_gold notebook first.")
        st.stop()
    return pd.read_csv(path)


st.title("🚕 Ride Analytics Lakehouse")
st.caption("NYC TLC data → Bronze/Silver/Gold on Databricks → this dashboard")

hourly_demand = load_csv("hourly_demand.csv")
zone_summary = load_csv("zone_summary.csv")
tip_by_hour = load_csv("tip_by_hour.csv")

col1, col2, col3 = st.columns(3)
col1.metric("Total trips (sample)", f"{int(hourly_demand['trip_count'].sum()):,}")
col2.metric("Avg fare", f"${hourly_demand['avg_fare'].mean():.2f}")
col3.metric("Avg tip %", f"{tip_by_hour['avg_tip_pct'].mean():.1f}%")

st.subheader("Demand by hour of day")
st.line_chart(hourly_demand.set_index("pickup_hour")["trip_count"])

col_a, col_b = st.columns(2)

with col_a:
    st.subheader("Top 25 pickup zones")
    st.bar_chart(zone_summary.set_index("pickup_zone_id")["trip_count"])

with col_b:
    st.subheader("Avg tip % by hour")
    st.line_chart(tip_by_hour.set_index("pickup_hour")["avg_tip_pct"])

with st.expander("Raw gold tables"):
    st.write("Hourly demand", hourly_demand)
    st.write("Zone summary", zone_summary)
    st.write("Tip by hour", tip_by_hour)
