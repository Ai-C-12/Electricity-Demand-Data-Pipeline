from src.ingest.eia_client import fetch_eia_data
from src.transform.eia_transform import transform_eia_data
from src.storage.write_raw import make_run_id, save_partitioned_csv

def run_eia_pipeline() -> None:
    run_id = make_run_id()

    df = fetch_eia_data(
        respondent="NYIS",
        data_type="D",
        start="2026-02-20T00",
        end="2026-02-21T23",
        length=5000,
    )

    clean_df = transform_eia_data(df)

    save_partitioned_csv(
        base_dir =  "data/processed",
        source = "eia_region_data",
        df = clean_df,
        run_id = run_id,
    )

    print(f"EIA pipeline completed. Run ID: {run_id}")
    print(clean_df.head())
    print(clean_df.shape)

if __name__ == "__main__":
    run_eia_pipeline()