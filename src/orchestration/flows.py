import pandas as pd
from prefect import flow, task

from src.pipeline.feature_pipeline import run_feature_pipeline


@task(
    name="Run feature pipeline",
    retries=1,
    retry_delay_seconds=60,
)
def run_feature_pipeline_task() -> pd.DataFrame:
    return run_feature_pipeline()


@flow(name="Electricity demand feature pipeline")
def electricity_demand_feature_flow() -> pd.DataFrame:
    return run_feature_pipeline_task()


if __name__ == "__main__":
    electricity_demand_feature_flow()