import pandas as pd

from src.transform.merge_features import merge_df
from src.pipeline.eia_pipeline import run_eia_pipeline
from src.pipeline.weather_pipeline import run_weather_pipeline
from src.storage.write_raw import make_run_id, save_partitioned_csv
from src.storage.paths import PROCESSED_DIR
from src.validation.checks import (
    check_required_columns,
    check_no_missing_values,
    check_timestamp_format,
    check_demand_values,
    check_duplicate_timestamps_region,
)

def run_feature_pipeline() -> pd.DataFrame:
    demand_df = run_eia_pipeline()
    weather_df = run_weather_pipeline()

    merged_df = merge_df(demand_df, weather_df)

    dataset_name = "Merged feature dataset"

    check_required_columns(
        merged_df,
        ["timestamp_utc", "region", "demand_mwh", "temperature_2m", "hour", "day_of_week", "month"],
        "Merged feature dataset",
    )

    check_no_missing_values(merged_df, dataset_name)
    check_timestamp_format(merged_df, dataset_name)
    check_demand_values(merged_df, dataset_name)
    check_duplicate_timestamps_region(merged_df, dataset_name)

    run_id = make_run_id()

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