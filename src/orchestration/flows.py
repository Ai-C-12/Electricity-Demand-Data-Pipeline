import pandas as pd
from prefect import flow, task

from src.pipeline.eia_pipeline import run_eia_pipeline
from src.pipeline.weather_pipeline import run_weather_pipeline
from src.pipeline.feature_pipeline import build_feature_dataset


@task(
    name="Run EIA pipeline",
    retries=1,
    retry_delay_seconds=60,
)
def run_eia_pipeline_task() -> pd.DataFrame:
    return run_eia_pipeline()


@task(
    name="Run weather pipeline",
    retries=1,
    retry_delay_seconds=60,
)
def run_weather_pipeline_task() -> pd.DataFrame:
    return run_weather_pipeline()


@task(
    name="Build feature dataset",
    retries=1,
    retry_delay_seconds=60,
)
def build_feature_dataset_task(
    demand_df: pd.DataFrame, 
    weather_df: pd.DataFrame,
) -> pd.DataFrame:
    return build_feature_dataset(demand_df, weather_df)


@flow(name="Electricity demand feature pipeline")
def electricity_demand_feature_flow() -> pd.DataFrame:
    demand_df = run_eia_pipeline_task()
    weather_df = run_weather_pipeline_task()

    merged_df = build_feature_dataset_task(demand_df, weather_df)

    return merged_df


if __name__ == "__main__":
    electricity_demand_feature_flow()