from src.ingest.weather_client import fetch_weather_data
from src.transform.weather_transform import transform_weather_data
from src.storage.write_raw import make_run_id, save_partitioned_csv

def run_weather_pipeline() -> None:
    run_id = make_run_id()

    df = fetch_weather_data(
        latitude=40.7128,
        longitude=-74.0060,
        start_date="2026-02-20",
        end_date="2026-02-21",
        hourly_variable="temperature_2m",
    )

    clean_df = transform_weather_data(df)

    save_partitioned_csv(
        base_dir =  "data/processed",
        source = "weather_data",
        df = clean_df,
        run_id = run_id,
    )

    print(f"Weather pipeline completed. Run ID: {run_id}")
    print(clean_df.head())
    print(clean_df.shape)

if __name__ == "__main__":
    run_weather_pipeline()