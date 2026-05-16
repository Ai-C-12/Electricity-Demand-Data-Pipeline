import pandas as pd

from src.ingest.weather_client import fetch_weather_data
from src.transform.weather_transform import transform_weather_data
from src.storage.write_raw import make_run_id, save_raw_per_run, save_partitioned_csv
from src.storage.paths import RAW_DIR, PROCESSED_DIR, LOGS_DIR
from src.validation.checks import (
    check_not_empty,
    check_required_columns,
    check_no_missing_values,
    check_timestamp_format,
    check_temperature_values,
)
from src.utils.logger import get_logger
from src.utils.run_summary import write_run_summary
from src.config import (
    WEATHER_LATITUDE,
    WEATHER_LONGITUDE,
    WEATHER_SOURCE,
    WEATHER_START_DATE,
    WEATHER_END_DATE,
    WEATHER_HOURLY_VARIABLE,
)

logger = get_logger("src.pipeline.weather_pipeline")

def run_weather_pipeline() -> pd.DataFrame:
    logger.info("Starting weather pipeline")

    run_id = make_run_id()

    df, payload, request_meta = fetch_weather_data(
        latitude=WEATHER_LATITUDE,
        longitude=WEATHER_LONGITUDE,
        start_date=WEATHER_START_DATE,
        end_date=WEATHER_END_DATE,
        hourly_variable=WEATHER_HOURLY_VARIABLE,
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
        source = WEATHER_SOURCE,
        run_id = run_id,
        payload = payload,
        request_meta = request_meta,
    )
    logger.info(f"Saved raw weather payload and request metadata. Run ID: {run_id}")

    save_partitioned_csv(
        base_dir =  PROCESSED_DIR,
        source = WEATHER_SOURCE,
        df = clean_df,
        run_id = run_id,
    )
    logger.info(f"Saved processed weather data. Run ID: {run_id}")

    write_run_summary(
        base_dir = LOGS_DIR,
        source = WEATHER_SOURCE,
        run_id = run_id,
        df = clean_df,
        output_formats = ["csv"],
        validation_status = "passed",
    )
    logger.info(f"Wrote run summary. Run ID: {run_id}")

    logger.info(f"Weather pipeline completed. Run ID: {run_id}")
    logger.info(f"Processed weather shape: {clean_df.shape}")

    return clean_df

if __name__ == "__main__":
    run_weather_pipeline()