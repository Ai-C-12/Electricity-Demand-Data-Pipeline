import pandas as pd

from src.ingest.weather_client import fetch_weather_data
from src.transform.weather_transform import transform_weather_data
from src.storage.write_raw import make_run_id, save_raw_per_run, save_partitioned_csv
from src.storage.paths import RAW_DIR, PROCESSED_DIR
from src.validation.checks import (
    check_not_empty,
    check_required_columns,
    check_no_missing_values,
    check_timestamp_format,
    check_temperature_values,
)
from src.utils.logger import get_logger

logger = get_logger("src.pipeline.weather_pipeline")

def run_weather_pipeline() -> pd.DataFrame:
    logger.info("Starting weather pipeline")

    run_id = make_run_id()

    df, payload, request_meta = fetch_weather_data(
        latitude=40.7128,
        longitude=-74.0060,
        start_date="2026-01-01",
        end_date="2026-03-31",
        hourly_variable="temperature_2m",
    )
    logger.info(f"Fetched raw weather data: {len(df)} rows")

    check_not_empty(df, "Raw weather data")
    check_required_columns(df, ["date", "temperature_2m", "longitude", "latitude"], "Raw weather data")
    logger.info("Validated raw weather data")

    clean_df = transform_weather_data(df)
    logger.info(f"Transformed weather data: {clean_df.shape}")

    check_not_empty(clean_df, "Processed weather data")
    check_required_columns(clean_df, ["timestamp_utc", "latitude", "longitude", "temperature_2m"], "Processed weather data")
    check_no_missing_values(clean_df, "Processed weather data")
    check_timestamp_format(clean_df, "Processed weather data")
    check_temperature_values(clean_df, "Processed weather data")
    logger.info("Validated processed weather data")

    save_raw_per_run(
        base_dir = RAW_DIR,
        source = "weather_data",
        run_id = run_id,
        payload = payload,
        request_meta = request_meta,
    )
    logger.info(f"Saved raw weather payload and request metadata. Run ID: {run_id}")

    save_partitioned_csv(
        base_dir =  PROCESSED_DIR,
        source = "weather_data",
        df = clean_df,
        run_id = run_id,
    )
    logger.info(f"Saved processed weather data. Run ID: {run_id}")

    logger.info(f"Weather pipeline completed. Run ID: {run_id}")
    logger.info(f"Processed weather shape: {clean_df.shape}")

    return clean_df

if __name__ == "__main__":
    run_weather_pipeline()