import pandas as pd

from src.ingest.eia_client import fetch_eia_data
from src.transform.eia_transform import transform_eia_data
from src.storage.write_raw import make_run_id, save_raw_per_run, save_partitioned_csv 
from src.storage.paths import RAW_DIR, PROCESSED_DIR

def run_eia_pipeline() -> pd.DataFrame:
    run_id = make_run_id()

    df, payload, request_meta = fetch_eia_data(
        respondent="NYIS",
        data_type="D",
        start="2026-02-01T00",
        end="2026-03-02T23",
        length=5000,
    )

    save_raw_per_run(
        base_dir = RAW_DIR,
        source = "eia_region_data",
        run_id = run_id,
        payload = payload,
        request_meta = request_meta,
    )

    clean_df = transform_eia_data(df)

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