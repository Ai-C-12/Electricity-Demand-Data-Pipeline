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

day_names = {
    0: "Monday",
    1: "Tuesday",
    2: "Wednesday",
    3: "Thursday",
    4: "Friday",
    5: "Saturday",
    6: "Sunday",
}

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


# Average demand hourly, daily, and monthly
hourly_demand = (
    df.groupby("hour", as_index=False)["demand_mwh"]
    .mean()
    .rename(columns={"demand_mwh": "avg_demand_mwh"})
)

daily_demand = (
    df.set_index("timestamp_utc")
    .resample("D")["demand_mwh"]
    .mean()
    .reset_index()
    .rename(columns={"demand_mwh": "avg_daily_demand_mwh"})
)

monthly_demand = (
    df.groupby("month", as_index=False)["demand_mwh"]
    .mean()
    .rename(columns={"demand_mwh": "avg_demand_mwh"})
)


st.subheader("Average Daily Electricity Demand")
st.line_chart(
    data=daily_demand,
    x="timestamp_utc",
    y="avg_daily_demand_mwh",
    x_label="Date",
    y_label="Average Daily Demand (MWh)",
)


top_demand_df = df.sort_values("demand_mwh", ascending=False).head(20).copy()

top_demand_df["date"] = top_demand_df["timestamp_utc"].dt.date
top_demand_df["day_of_month"] = top_demand_df["timestamp_utc"].dt.day

top_demand_df["month_name"] = top_demand_df["month"].map(month_names)

st.subheader("Top 20 Highest-Demand Hours")
st.dataframe(
    top_demand_df[
        [
            "timestamp_utc",
            "region",
            "demand_mwh",
            "temperature_2m",
            "hour",
            "day_of_month",
            "month",
            "month_name",
        ]
    ],
    use_container_width=True,
)