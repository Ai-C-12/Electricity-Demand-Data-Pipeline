import pandas as pd

def merge_df(
    demand_df: pd.DataFrame,
    weather_df: pd.DataFrame,
) -> pd.DataFrame:
    merged_df = pd.merge(demand_df, weather_df, on="timestamp_utc", how="inner")

    return merged_df