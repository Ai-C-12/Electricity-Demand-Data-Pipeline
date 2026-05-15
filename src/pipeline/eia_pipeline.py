import pandas as pd

from src.ingest.eia_client import fetch_eia_data
from src.transform.eia_transform import transform_eia_data
from src.storage.write_raw import make_run_id, save_raw_per_run, save_partitioned_csv 
from src.storage.paths import RAW_DIR, PROCESSED_DIR
from src.validation.checks import (
    check_not_empty,
    check_required_columns,
    check_no_missing_values,
    check_timestamp_format,
    check_demand_values,
)
from src.utils.logger import get_logger
from src.config import (
    DEFAULT_RESPONDENT,
    DEFAULT_EIA_TYPE,
    EIA_SOURCE,
    EIA_START,
    EIA_END
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

    save_raw_per_run(
        base_dir = RAW_DIR,
        source = EIA_SOURCE,
        run_id = run_id,
        payload = payload,
        request_meta = request_meta,
    )
    logger.info(f"Saved raw EIA payload and request metadata. Run ID: {run_id}")

    save_partitioned_csv(
        base_dir =  PROCESSED_DIR,
        source = EIA_SOURCE,
        df = clean_df,
        run_id = run_id,
    )
    logger.info(f"Saved processed EIA data. Run ID: {run_id}")

    logger.info(f"EIA pipeline completed. Run ID: {run_id}")
    logger.info(f"Processed EIA shape: {clean_df.shape}")

    return clean_df

if __name__ == "__main__":
    run_eia_pipeline()