import pandas as pd

from src.ingest.weather_client import fetch_weather_data
from src.transform.weather_transform import transform_weather_data
from src.storage.write_raw import make_run_id, save_raw_per_run, save_partitioned_csv
from src.storage.paths import RAW_DIR, PROCESSED_DIR

def run_weather_pipeline() -> pd.DataFrame:
    run_id = make_run_id()

    df, payload, request_meta = fetch_weather_data(
        latitude=40.7128,
        longitude=-74.0060,
        start_date="2026-02-20",
        end_date="2026-02-21",
        hourly_variable="temperature_2m",
    )

    save_raw_per_run(
        base_dir = RAW_DIR,
        source = "weather_data",
        run_id = run_id,
        payload = payload,
        request_meta = request_meta,
    )

    clean_df = transform_weather_data(df)

    save_partitioned_csv(
        base_dir =  PROCESSED_DIR,
        source = "weather_data",
        df = clean_df,
        run_id = run_id,
    )

    print(f"Weather pipeline completed. Run ID: {run_id}")
    print(clean_df.head())
    print(clean_df.shape)

    return clean_df

if __name__ == "__main__":
    run_weather_pipeline()