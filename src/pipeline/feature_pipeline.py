import pandas as pd
from time import perf_counter

from src.transform.merge_features import merge_df
from src.pipeline.eia_pipeline import run_eia_pipeline
from src.pipeline.weather_pipeline import run_weather_pipeline
from src.storage.write_raw import make_run_id, save_partitioned_csv, save_partitioned_parquet
from src.storage.paths import PROCESSED_DIR, LOGS_DIR
from src.validation.checks import (
    check_not_empty,
    check_required_columns,
    check_no_missing_values,
    check_timestamp_format,
    check_demand_values,
    check_temperature_values,
    check_duplicate_timestamps_region,
    check_merge_retention, 
    check_hourly_timestamp_coverage,
)
from src.utils.logger import get_logger
from src.utils.run_summary import write_run_summary
from src.config import FEATURE_SOURCE


logger = get_logger("src.pipeline.feature_pipeline")

def build_feature_dataset(
    demand_df: pd.DataFrame,
    weather_df: pd.DataFrame,
) -> pd.DataFrame:
    logger.info("Starting feature dataset build")
    start_time = perf_counter()

    dataset_name = "Merged feature dataset"

    run_id = make_run_id()

    # Merge demand and weather data
    merged_df = merge_df(demand_df, weather_df)
    logger.info(
        f"Merged demand and weather data: {len(merged_df)} rows, {len(merged_df.columns)} columns"
    )

    # Validate merged dataset
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

    # Check merge retention
    check_merge_retention(merged_df, demand_df, weather_df, dataset_name)

    # Check hourly timestamp coverage
    check_hourly_timestamp_coverage(merged_df, dataset_name)
    logger.info("Validated merged feature dataset")

    # Save to csv
    csv_output_paths = save_partitioned_csv(
        base_dir = PROCESSED_DIR,
        source = FEATURE_SOURCE,
        df = merged_df,
        run_id = run_id,
    )
    logger.info(f"Saved {len(csv_output_paths)} CSV feature partitions. Run ID: {run_id}")

    # Save to parquet
    parquet_output_paths = save_partitioned_parquet(
        base_dir = PROCESSED_DIR,
        source= FEATURE_SOURCE,
        df = merged_df,
        run_id = run_id,
    )
    logger.info(f"Saved {len(parquet_output_paths)} Parquet feature partitions. Run ID: {run_id}")

    pipeline_duration_seconds = round(perf_counter() - start_time, 2)

    expected_rows = min(len(demand_df), len(weather_df))
    merge_retention_rate = len(merged_df) / expected_rows

    output_base_path = f"data/processed/{FEATURE_SOURCE}"

    # Write run summary
    extra_metadata = {
        "pipeline_stage": "feature_engineering",
        "demand_row_count": len(demand_df),
        "weather_row_count": len(weather_df),
        "merged_row_count": len(merged_df),
        "expected_merge_rows": expected_rows,
        "merge_retention_rate": merge_retention_rate,
        "merge_retention_status": "passed",
        "timestamp_coverage_status": "passed",
        "pipeline_duration_seconds": pipeline_duration_seconds,
        "outputs": {
            "csv": {
                "file_count": len(csv_output_paths),
                "base_path": output_base_path,
                "partitioning": ["year", "month", "day"],
            },
            "parquet": {
                "file_count": len(parquet_output_paths),
                "base_path": output_base_path,
                "partitioning": ["year", "month", "day"],
            },
        },
    }

    write_run_summary(
        base_dir = LOGS_DIR,
        source = FEATURE_SOURCE,
        run_id = run_id,
        df = merged_df,
        output_formats = ["csv", "parquet"],
        validation_status = "passed",
        extra_metadata = extra_metadata,
    )
    logger.info(f"Wrote feature run summary. Run ID: {run_id}")

    logger.info(
        f"Feature dataset build completed in {pipeline_duration_seconds}s. "
        f"Rows: {len(merged_df)}. Run ID: {run_id}"
    )

    return merged_df



def run_feature_pipeline() -> pd.DataFrame:
    logger.info("Starting feature pipeline")

    demand_df = run_eia_pipeline()
    weather_df = run_weather_pipeline()
    logger.info(f"Fetched demand and weather data for feature engineering: {len(demand_df)} demand rows, {len(weather_df)} weather rows")

    merged_df = build_feature_dataset(demand_df, weather_df)

    logger.info(
        f"Feature pipeline completed: {len(merged_df)} rows & {len(merged_df.columns)} columns"
    )

    return merged_df

if __name__ == "__main__":
    run_feature_pipeline()