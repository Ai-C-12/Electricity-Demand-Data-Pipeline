import pandas as pd

from src.ingest.eia_client import fetch_eia_data
from src.transform.eia_transform import transform_eia_data
from src.storage.write_raw import make_run_id, save_raw_per_run, save_partitioned_csv
from src.storage.azure_blob_writer import upload_files_to_azure
from src.storage.paths import RAW_DIR, PROCESSED_DIR, LOGS_DIR
from src.validation.checks import (
    check_not_empty,
    check_required_columns,
    check_no_missing_values,
    check_timestamp_format,
    check_demand_values,
)
from src.utils.logger import get_logger
from src.utils.run_summary import write_run_summary
from src.config import (
    DEFAULT_RESPONDENT,
    DEFAULT_EIA_TYPE,
    EIA_SOURCE,
    EIA_START,
    EIA_END,
    ENABLE_AZURE_UPLOAD,
    AZURE_STORAGE_CONNECTION_STRING,
    AZURE_STORAGE_CONTAINER,
)

logger = get_logger("src.pipeline.eia_pipeline")

def run_eia_pipeline() -> pd.DataFrame:
    logger.info("Starting EIA pipeline")

    run_id = make_run_id()

    df, payload, request_meta = fetch_eia_data(
        respondent=DEFAULT_RESPONDENT,
        data_type=DEFAULT_EIA_TYPE,
        start=EIA_START,
        end=EIA_END,
        length=5000,  # Max page size for EIA API
    )
    logger.info(f"Fetched raw EIA data: {len(df)} rows")

    check_not_empty(df, "Raw EIA data")
    check_required_columns(df, ["period", "respondent", "type", "value"], "Raw EIA data",)
    logger.info("Validated raw EIA data")

    clean_df = transform_eia_data(df)
    logger.info(f"Transformed EIA data: {clean_df.shape}")

    check_not_empty(clean_df, "Processed EIA data")
    check_required_columns(clean_df, ["timestamp_utc", "region", "type", "demand_mwh"], "Processed EIA data")
    check_no_missing_values(clean_df, "Processed EIA data")
    check_timestamp_format(clean_df, "Processed EIA data")
    check_demand_values(clean_df, "Processed EIA data")
    logger.info("Validated processed EIA data")

    raw_output_paths = save_raw_per_run(
        base_dir = RAW_DIR,
        source = EIA_SOURCE,
        run_id = run_id,
        payload = payload,
        request_meta = request_meta,
    )
    logger.info(f"Saved raw EIA payload and request metadata. Run ID: {run_id}")

    csv_output_paths = save_partitioned_csv(
        base_dir =  PROCESSED_DIR,
        source = EIA_SOURCE,
        df = clean_df,
        run_id = run_id,
    )
    logger.info(f"Saved processed EIA data. Run ID: {run_id}")


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
            f"Uploaded {azure_uploaded_file_count} EIA artifacts to Azure Blob Storage. "
            f"Run ID: {run_id}"
        )
    else:
        logger.info("Skipping Azure upload for EIA artifacts because ENABLE_AZURE_UPLOAD is false")


    extra_metadata = {
        "azure_uploaded": azure_uploaded,
        "azure_container": AZURE_STORAGE_CONTAINER if azure_uploaded else None,
        "azure_uploaded_file_count": azure_uploaded_file_count,
    }

    summary_path = write_run_summary(
        base_dir = LOGS_DIR,
        source = EIA_SOURCE,
        run_id = run_id,
        df = clean_df,
        output_formats = ["csv"],
        validation_status = "passed",
        extra_metadata = extra_metadata,
    )
    logger.info(f"Wrote EIA run summary. Run ID: {run_id}")

    if ENABLE_AZURE_UPLOAD:
        upload_files_to_azure(
            local_paths=[summary_path],
            connection_string=AZURE_STORAGE_CONNECTION_STRING,
            container_name=AZURE_STORAGE_CONTAINER,
        )
        logger.info(f"Uploaded EIA run summary to Azure Blob Storage. Run ID: {run_id}")

    logger.info(f"EIA pipeline completed. Run ID: {run_id}")
    logger.info(f"Processed EIA shape: {clean_df.shape}")

    return clean_df

if __name__ == "__main__":
    run_eia_pipeline()