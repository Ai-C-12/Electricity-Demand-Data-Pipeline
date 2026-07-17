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
        
    merged_df = pd.merge(demand_df, weather_df, on="timestamp_utc", how="inner", validate="one_to_one")

    if merged_df.empty:
        raise ValueError("Merged dataset is empty. Check timestamp overlap between demand and weather data.")

    local_timestamp = merged_df["timestamp_utc"].dt.tz_convert(
        "America/New_York"
    )

    merged_df["hour"] = local_timestamp.dt.hour
    merged_df["day_of_week"] = local_timestamp.dt.dayofweek
    merged_df["month"] = local_timestamp.dt.month

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


def merge_forecasting_inputs(
    demand_df: pd.DataFrame,
    forecast_weather_df: pd.DataFrame,
) -> pd.DataFrame:
    required_demand_cols = {"timestamp_utc", "region", "demand_mwh"}
    required_weather_cols = {"timestamp_utc", "temperature_forecast_24h"}

    for col in required_demand_cols:
        if col not in demand_df.columns:
            raise ValueError(f"Missing required column in demand data: {col}")

    for col in required_weather_cols:
        if col not in forecast_weather_df.columns:
            raise ValueError(f"Missing required column in forecast weather data: {col}")
        
    merged_df = pd.merge(demand_df, forecast_weather_df, on="timestamp_utc", how="inner", validate="one_to_one")

    if merged_df.empty:
        raise ValueError("Merged dataset is empty. Check timestamp overlap between demand and forecast weather data.")
    
    local_timestamp = merged_df["timestamp_utc"].dt.tz_convert(
        "America/New_York"
    )

    merged_df["hour"] = local_timestamp.dt.hour
    merged_df["day_of_week"] = local_timestamp.dt.dayofweek
    merged_df["month"] = local_timestamp.dt.month

    merged_df = merged_df[
        [
            "timestamp_utc",
            "region",
            "demand_mwh",
            "temperature_forecast_24h",
            "hour",
            "day_of_week",
            "month",
        ]
    ]

    return merged_df