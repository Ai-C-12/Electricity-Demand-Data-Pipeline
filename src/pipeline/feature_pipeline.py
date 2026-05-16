import pandas as pd

from src.transform.merge_features import merge_df
from src.pipeline.eia_pipeline import run_eia_pipeline
from src.pipeline.weather_pipeline import run_weather_pipeline
from src.storage.write_raw import make_run_id, save_partitioned_csv, save_partitioned_parquet
from src.storage.paths import PROCESSED_DIR
from src.validation.checks import (
    check_not_empty,
    check_required_columns,
    check_no_missing_values,
    check_timestamp_format,
    check_demand_values,
    check_temperature_values,
    check_duplicate_timestamps_region,
)
from src.utils.logger import get_logger
from src.config import FEATURE_SOURCE

logger = get_logger("src.pipeline.feature_pipeline")

def run_feature_pipeline() -> pd.DataFrame:
    logger.info("Starting feature pipeline")

    demand_df = run_eia_pipeline()
    weather_df = run_weather_pipeline()
    logger.info(f"Fetched demand and weather data for feature engineering: {len(demand_df)} demand rows, {len(weather_df)} weather rows")

    dataset_name = "Merged feature dataset"

    merged_df = merge_df(demand_df, weather_df)
    logger.info(f"Merged demand and weather data: {len(merged_df)} rows, {len(merged_df.columns)} columns")

    check_required_columns(
        merged_df,
        ["timestamp_utc", "region", "demand_mwh", "temperature_2m", "hour", "day_of_week", "month"],
        dataset_name,
    )

    check_not_empty(merged_df, dataset_name)
    check_no_missing_values(merged_df, dataset_name)
    check_timestamp_format(merged_df, dataset_name)
    check_demand_values(merged_df, dataset_name)
    check_temperature_values(merged_df, dataset_name)
    check_duplicate_timestamps_region(merged_df, dataset_name)
    logger.info(f"Validated merged feature dataset")

    run_id = make_run_id()

    save_partitioned_csv(
        base_dir =  PROCESSED_DIR,
        source = FEATURE_SOURCE,
        df = merged_df,
        run_id = run_id,
    )
    logger.info(f"Saved merged feature dataset into CSV. Run ID: {run_id}")

    save_partitioned_parquet(
        base_dir = PROCESSED_DIR,
        source= FEATURE_SOURCE,
        df = merged_df,
        run_id = run_id,
    )
    logger.info(f"Saved merged feature dataset into Parquet. Run ID: {run_id}")

    logger.info(f"Feature pipeline completed: {len(merged_df)} rows & {len(merged_df.columns)} columns. Run ID: {run_id}")

    return merged_df

if __name__ == "__main__":
    run_feature_pipeline()