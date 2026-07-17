import pandas as pd

from src.pipeline.eia_pipeline import run_eia_pipeline
from src.pipeline.weather_pipeline import (
    run_archived_forecast_weather_pipeline,
)
from src.transform.merge_features import merge_forecasting_inputs
from src.transform.forecast_features import add_demand_lag_features
from src.experiment.split_data import split_forecasting_data
from src.experiment.experiment_config import (
    load_experiment_config,
    validate_experiment_config,
)
from src.experiment.evaluate_baselines import (
    evaluate_weekly_naive,
    evaluate_historical_mean,
)


def prepare_experiment_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    config = load_experiment_config()
    validate_experiment_config(config)

    raw_start_date = config["data"]["raw_start_date"]

    test_end_exclusive = pd.Timestamp(
        config["data"]["test_end_exclusive"]
    )

    api_end_date = (
        test_end_exclusive - pd.Timedelta(days=1)
    ).strftime("%Y-%m-%d")


    demand_df = run_eia_pipeline(
        start_date=raw_start_date,
        end_date=api_end_date,
    )

    forecast_weather_df = (
        run_archived_forecast_weather_pipeline(
            start_date=raw_start_date,
            end_date=api_end_date,
        )
    )


    merged_df = merge_forecasting_inputs(
        demand_df,
        forecast_weather_df,
    )

    featured_df = add_demand_lag_features(
        merged_df
    )

    train_df, validation_df, test_df = (
        split_forecasting_data(
            featured_df,
            config,
        )
    )

    historical_mean_metrics = evaluate_historical_mean(
        train_df,
        validation_df,
    )

    print(
        "Historical mean validation metrics:",
        historical_mean_metrics,
    )

    return train_df, validation_df, test_df


if __name__ == "__main__":
    prepare_experiment_data()