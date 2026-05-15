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

def run_eia_pipeline() -> pd.DataFrame:
    run_id = make_run_id()

    df, payload, request_meta = fetch_eia_data(
        respondent="NYIS",
        data_type="D",
        start="2026-01-01T00",
        end="2026-03-31T23",
        length=5000,
    )

    check_not_empty(df, "Raw EIA data")
    check_required_columns(df, ["period", "respondent", "type", "value"], "Raw EIA data",)

    clean_df = transform_eia_data(df)

    check_not_empty(clean_df, "Processed EIA data")
    check_required_columns(clean_df, ["timestamp_utc", "region", "type", "demand_mwh"], "Processed EIA data")
    check_no_missing_values(clean_df, "Processed EIA data")
    check_timestamp_format(clean_df, "Processed EIA data")
    check_demand_values(clean_df, "Processed EIA data")

    save_raw_per_run(
        base_dir = RAW_DIR,
        source = "eia_region_data",
        run_id = run_id,
        payload = payload,
        request_meta = request_meta,
    )

    save_partitioned_csv(
        base_dir =  PROCESSED_DIR,
        source = "eia_region_data",
        df = clean_df,
        run_id = run_id,
    )

    print(f"EIA pipeline completed. Run ID: {run_id}")
    print(clean_df.head())
    print(clean_df.shape)

    return clean_df

if __name__ == "__main__":
    run_eia_pipeline()