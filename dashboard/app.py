import sys
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from src.config import DATABASE_URL


st.title("Electricity Demand Analytics")

if not DATABASE_URL:
    st.error("DATABASE_URL is missing. Check your .env file.")
    st.stop()


@st.cache_data(ttl=600)
def load_data(database_url: str) -> pd.DataFrame:
    engine = create_engine(database_url)

    query = """
    SELECT
        timestamp_utc,
        region,
        demand_mwh,
        temperature_2m,
        hour,
        day_of_week,
        month
    FROM demand_weather_features
    ORDER BY timestamp_utc;
    """

    df = pd.read_sql_query(query, engine)
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)

    return df

df = load_data(DATABASE_URL)

month_names = {
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December",
}


start_date = df["timestamp_utc"].min().strftime("%Y-%m-%d")
end_date = df["timestamp_utc"].max().strftime("%Y-%m-%d")

avg_demand = df["demand_mwh"].mean()
peak_demand = df["demand_mwh"].max()

avg_temp_c = df["temperature_2m"].mean()

col1, col2, col3 = st.columns(3)
col4, col5, col6 = st.columns(3)

col1.metric(
    label="Start Date",
    value=start_date,
    border=True,
)

col2.metric(
    label="End Date",
    value=end_date,
    border=True,
)

col3.metric(
    label="Total Rows",
    value=f"{len(df):,}",
    border=True,
)

col4.metric(
    label="Average Demand",
    value=f"{avg_demand:,.0f} MWh",
    border=True,
)

col5.metric(
    label="Peak Demand",
    value=f"{peak_demand:,.0f} MWh",
    border=True,
)

col6.metric(
    label="Average Temperature",
    value=f"{avg_temp_c:.2f}°C",
    border=True,
)

st.divider()

# Temperature to Demand Relationship
scatter_df = df[df["demand_mwh"] > 0]

st.subheader("Temperature vs Electricity Demand")
st.scatter_chart(
    data=scatter_df,
    x="temperature_2m", 
    y="demand_mwh",
    size=15,
    x_label="Temperature (°C)", 
    y_label="Demand (MWh)", 
)

st.caption(
    "This chart shows how hourly electricity demand changes across observed temperatures. "
    "Higher temperatures appear associated with higher demand, likely reflecting cooling demand."
)

st.divider()

# Average demand daily
daily_demand = (
    df.set_index("timestamp_utc")
    .resample("D")["demand_mwh"]
    .mean()
    .reset_index()
    .rename(columns={"demand_mwh": "avg_daily_demand_mwh"})
)

# Average Daily Demand
st.subheader("Average Daily Electricity Demand")
st.line_chart(
    data=daily_demand,
    x="timestamp_utc",
    y="avg_daily_demand_mwh",
    x_label="Date",
    y_label="Average Daily Demand (MWh)",
)

st.caption(
    "Daily averages smooth the hourly data and make seasonal demand patterns easier to see. "
    "The highest demand periods appear to occur during the summer months, which may reflect increased cooling demand."
)

st.divider()

# Top 20 Highest Demand Hours  
top_demand_df = df.sort_values("demand_mwh", ascending=False).head(20).copy()

top_demand_df["day_of_month"] = top_demand_df["timestamp_utc"].dt.day

top_demand_df["month_name"] = top_demand_df["month"].map(month_names)

display_top_demand_df = top_demand_df[
    [
        "timestamp_utc",
        "region",
        "demand_mwh",
        "temperature_2m",
        "hour",
        "day_of_month",
        "month_name",
    ]
].rename(
    columns={
        "timestamp_utc": "Timestamp (UTC)",
        "region": "Region",
        "demand_mwh": "Demand (MWh)",
        "temperature_2m": "Temperature (°C)",
        "hour": "Hour",
        "day_of_month": "Day",
        "month_name": "Month",
    }
).reset_index()

st.subheader("Top 20 Highest-Demand Hours")
st.dataframe(display_top_demand_df, use_container_width=True)

avg_top_temp = top_demand_df["temperature_2m"].mean()
overall_avg_temp = df["temperature_2m"].mean()

temp_col1, temp_col2 = st.columns(2)

temp_col1.metric(
    "Avg Temp During Top 20 Demand Hours",
    f"{avg_top_temp:.1f} °C",
    border=True,
)

temp_col2.metric(
    "Overall Avg Temp",
    f"{overall_avg_temp:.1f} °C",
    border=True,
)

st.divider()

# Top 10 Highest Daily Demands
top_days = (
    daily_demand
    .sort_values("avg_daily_demand_mwh", ascending=False)
    .head(10)
)

display_top_days = top_days.rename(
    columns={
        "timestamp_utc": "Timestamp (UTC)",
        "avg_daily_demand_mwh": "Average Daily Demand (MWh)",
    }
).reset_index()

st.subheader("Top 10 Highest Average Demand Days")
st.dataframe(display_top_days, use_container_width=True)

st.divider()

# Average Demand at Different Temperatures
df["temperature_bucket"] = pd.cut(
    df["temperature_2m"],
    bins=[float("-inf"), 0, 5, 10, 15, 20, 25, 30, float("inf")],
    labels= [
        "<0°C",
        "0-5°C",
        "5-10°C",
        "10-15°C",
        "15-20°C",
        "20-25°C",
        "25-30°C",
        "30°C+",
    ],
)

temp_bucket_demand = (
    df.groupby("temperature_bucket", as_index=False)["demand_mwh"]
    .mean()
    .rename(columns={"demand_mwh": "avg_demand_mwh"})
)   

st.subheader("Average Demand by Temperature Range")
st.bar_chart(
    data=temp_bucket_demand,
    x="temperature_bucket",
    y="avg_demand_mwh",
    x_label="Temperature Range (°C)",
    y_label="Average Demand (MWh)",
)

st.caption(
    "Binning temperatures into ranges makes the temperature-demand relationship easier to compare than individual hourly points."
)

st.divider()

# Outlier Demand Values
outlier_df = df[df["demand_mwh"] <= 0]

st.subheader("Potential Demand Outliers")

if outlier_df.empty:
    st.write("No zero or negative demand values found.")
else:
    st.dataframe(outlier_df, use_container_width=True)