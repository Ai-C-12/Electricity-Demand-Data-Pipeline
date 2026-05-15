import pandas as pd

def merge_df(
    demand_df: pd.DataFrame,
    weather_df: pd.DataFrame,
) -> pd.DataFrame:
    required_demand_cols = {"timestamp_utc", "region", "demand_mwh"}
    required_weather_cols = {"timestamp_utc", "temperature_2m"}

    for col in required_demand_cols:
        if col not in demand_df.columns:
            raise ValueError(f"Missing required column in demand data: {col}")

    for col in required_weather_cols:
        if col not in weather_df.columns:
            raise ValueError(f"Missing required column in weather data: {col}")
        
    merged_df = pd.merge(demand_df, weather_df, on="timestamp_utc", how="inner")

    if merged_df.empty:
        raise ValueError("Merged dataset is empty. Check timestamp overlap between demand and weather data.")

    merged_df["hour"] = merged_df["timestamp_utc"].dt.hour
    merged_df["day_of_week"] = merged_df["timestamp_utc"].dt.dayofweek
    merged_df["month"] = merged_df["timestamp_utc"].dt.month

    merged_df = merged_df[
        [
            "timestamp_utc",
            "region",
            "demand_mwh",
            "temperature_2m",
            "hour",
            "day_of_week",
            "month",
        ]
    ]

    return merged_df