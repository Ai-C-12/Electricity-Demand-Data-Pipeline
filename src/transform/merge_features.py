import pandas as pd

def merge_df(
    demand_df: pd.DataFrame,
    weather_df: pd.DataFrame,
) -> pd.DataFrame:
    required_demand_cols = {"timestamp_utc", "respondent", "demand_mwh"}
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

    return merged_df