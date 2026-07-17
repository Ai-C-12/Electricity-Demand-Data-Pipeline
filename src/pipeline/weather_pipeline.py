import pandas as pd

from src.ingest.weather_client import (
    fetch_historical_weather,
    fetch_archived_forecast_weather,
)
from src.transform.weather_transform import (
    transform_historical_weather,
    transform_archived_forecast_weather,
)
from src.storage.write_raw import make_run_id, save_raw_per_run, save_partitioned_csv
from src.storage.azure_blob_writer import upload_files_to_azure
from src.storage.paths import RAW_DIR, PROCESSED_DIR, LOGS_DIR
from src.validation.weather_validation import validate_historical_weather, validate_archived_forecast_weather
from src.utils.logger import get_logger
from src.utils.run_summary import write_run_summary
from src.config import (
    WEATHER_LATITUDE,
    WEATHER_LONGITUDE,
    WEATHER_START_DATE,
    WEATHER_END_DATE,
    ENABLE_AZURE_UPLOAD,
    AZURE_STORAGE_CONNECTION_STRING,
    AZURE_STORAGE_CONTAINER,
)

logger = get_logger("src.pipeline.weather_pipeline")

def _run_weather_pipeline(
    fetch_function,
    transform_function,
    validate_function,
    dataset_name: str,
    source_name: str,
) -> pd.DataFrame:
    logger.info("Starting weather pipeline")

    run_id = make_run_id()

    df, payload, request_meta = fetch_function(
        latitude=WEATHER_LATITUDE,
        longitude=WEATHER_LONGITUDE,
        start_date=WEATHER_START_DATE,
        end_date=WEATHER_END_DATE,
    )
    logger.info(f"Fetched raw weather data: {len(df)} rows")

    clean_df = transform_function(df)
    logger.info(f"Transformed weather data: {clean_df.shape}")

    validate_function(df, clean_df)
    logger.info(f"Validated {dataset_name}")

    raw_output_paths = save_raw_per_run(
        base_dir = RAW_DIR,
        source = source_name,
        run_id = run_id,
        payload = payload,
        request_meta = request_meta,
    )
    logger.info(f"Saved raw weather payload and request metadata. Run ID: {run_id}")

    csv_output_paths = save_partitioned_csv(
        base_dir =  PROCESSED_DIR,
        source = source_name,
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
        source = source_name,
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


def run_historical_weather_pipeline() -> pd.DataFrame:
    return _run_weather_pipeline(
        fetch_function=fetch_historical_weather,
        transform_function=transform_historical_weather,
        validate_function=validate_historical_weather,
        dataset_name="Historical Weather Data",
        source_name="weather_historical",
    )


def run_archived_forecast_weather_pipeline() -> pd.DataFrame:
    return _run_weather_pipeline(
        fetch_function=fetch_archived_forecast_weather,
        transform_function=transform_archived_forecast_weather,
        validate_function=validate_archived_forecast_weather,
        dataset_name="Archived Forecast Weather",
        source_name="weather_forecast_24h",
    )


if __name__ == "__main__":
    run_historical_weather_pipeline()
    run_archived_forecast_weather_pipeline()