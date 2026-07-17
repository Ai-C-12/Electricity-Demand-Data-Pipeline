import pandas as pd

from src.ingest.weather_client import fetch_historical_weather
from src.transform.weather_transform import transform_weather_data
from src.storage.write_raw import make_run_id, save_raw_per_run, save_partitioned_csv
from src.storage.azure_blob_writer import upload_files_to_azure
from src.storage.paths import RAW_DIR, PROCESSED_DIR, LOGS_DIR
from src.validation.checks import (
    check_not_empty,
    check_required_columns,
    check_no_missing_values,
    check_timestamp_format,
    check_temperature_values,
    check_duplicate_timestamps,
)
from src.utils.logger import get_logger
from src.utils.run_summary import write_run_summary
from src.config import (
    HISTORICAL_WEATHER_API_URL,
    WEATHER_LATITUDE,
    WEATHER_LONGITUDE,
    WEATHER_SOURCE,
    WEATHER_START_DATE,
    WEATHER_END_DATE,
    WEATHER_HOURLY_VARIABLE,
    ENABLE_AZURE_UPLOAD,
    AZURE_STORAGE_CONNECTION_STRING,
    AZURE_STORAGE_CONTAINER,
)

logger = get_logger("src.pipeline.weather_pipeline")

def run_weather_pipeline() -> pd.DataFrame:
    logger.info("Starting weather pipeline")

    run_id = make_run_id()

    df, payload, request_meta = fetch_historical_weather(
        url = HISTORICAL_WEATHER_API_URL,
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

    raw_output_paths = save_raw_per_run(
        base_dir = RAW_DIR,
        source = WEATHER_SOURCE,
        run_id = run_id,
        payload = payload,
        request_meta = request_meta,
    )
    logger.info(f"Saved raw weather payload and request metadata. Run ID: {run_id}")

    csv_output_paths = save_partitioned_csv(
        base_dir =  PROCESSED_DIR,
        source = WEATHER_SOURCE,
        df = clean_df,
        run_id = run_id,
    )
    logger.info(f"Saved processed weather data. Run ID: {run_id}")


    artifact_paths = raw_output_paths + csv_output_paths

    azure_uploaded = False
    azure_uploaded_file_count = 0

    if ENABLE_AZURE_UPLOAD:
        if not AZURE_STORAGE_CONNECTION_STRING:
            raise ValueError(
                "ENABLE_AZURE_UPLOAD is true, but AZURE_STORAGE_CONNECTION_STRING is not set."
            )
        
        uploaded_blob_names = upload_files_to_azure(
            local_paths = artifact_paths,
            connection_string = AZURE_STORAGE_CONNECTION_STRING,
            container_name = AZURE_STORAGE_CONTAINER,
        )

        azure_uploaded = True
        azure_uploaded_file_count = len(uploaded_blob_names)

        logger.info(
            f"Uploaded {azure_uploaded_file_count} weather artifacts to Azure Blob Storage. "
            f"Run ID: {run_id}"
        )
    else:
        logger.info("Skipping Azure upload for weather artifacts because ENABLE_AZURE_UPLOAD is false")

    extra_metadata = {
        "azure_uploaded": azure_uploaded,
        "azure_container": AZURE_STORAGE_CONTAINER if azure_uploaded else None,
        "azure_uploaded_file_count": azure_uploaded_file_count,
    }


    summary_path = write_run_summary(
        base_dir = LOGS_DIR,
        source = WEATHER_SOURCE,
        run_id = run_id,
        df = clean_df,
        output_formats = ["csv"],
        validation_status = "passed",
        extra_metadata = extra_metadata
    )
    logger.info(f"Wrote run summary. Run ID: {run_id}")

    if ENABLE_AZURE_UPLOAD:
        upload_files_to_azure(
            local_paths = [summary_path],
            connection_string = AZURE_STORAGE_CONNECTION_STRING,
            container_name = AZURE_STORAGE_CONTAINER,
        )
        logger.info(f"Uploaded weather run summary to Azure Blob Storage. Run ID: {run_id}")


    logger.info(f"Weather pipeline completed. Run ID: {run_id}")
    logger.info(f"Processed weather shape: {clean_df.shape}")

    return clean_df

if __name__ == "__main__":
    run_weather_pipeline()