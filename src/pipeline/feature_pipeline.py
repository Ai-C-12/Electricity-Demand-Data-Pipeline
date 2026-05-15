import pandas as pd

from src.transform.merge_features import merge_df
from src.pipeline.eia_pipeline import run_eia_pipeline
from src.pipeline.weather_pipeline import run_weather_pipeline
from src.storage.write_raw import make_run_id, save_partitioned_csv
from src.storage.paths import PROCESSED_DIR

def run_feature_pipeline() -> pd.DataFrame:
    demand_df = run_eia_pipeline()
    weather_df = run_weather_pipeline()

    merged_df = merge_df(demand_df, weather_df)

    merged_df["hour"] = merged_df["timestamp_utc"].dt.hour
    merged_df["day_of_week"] = merged_df["timestamp_utc"].dt.dayofweek
    merged_df["month"] = merged_df["timestamp_utc"].dt.month

    run_id = make_run_id()

    merged_df = merged_df[
        [
            "timestamp_utc",
            "region",
            "type",
            "demand_mwh",
            "latitude",
            "longitude",
            "temperature_2m",
            "hour",
            "day_of_week",
            "month",
        ]
    ]

    save_partitioned_csv(
        base_dir =  PROCESSED_DIR,
        source = "demand_weather_features",
        df = merged_df,
        run_id = run_id,
    )

    print(f"Feature pipeline completed: {len(merged_df)} rows & {len(merged_df.columns)} columns. Run ID: {run_id}")
    
    return merged_df

if __name__ == "__main__":
    merged_df = run_feature_pipeline()
    print(merged_df.head())