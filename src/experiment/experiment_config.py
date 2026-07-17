from pathlib import Path

import yaml
import pandas as pd


DEFAULT_CONFIG_PATH = Path("configs/forecast_experiment.yaml")


def load_experiment_config(
    config_path: str | Path = DEFAULT_CONFIG_PATH,
) -> dict:
    with open(config_path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    raw_start_date = pd.Timestamp(
        config["data"]["raw_start_date"]
    )

    return config


def validate_experiment_config(config: dict) -> None:
    raw_start_date = pd.Timestamp(
        config["data"]["raw_start_date"]
    )
    study_start_date = pd.Timestamp(
        config["data"]["study_start_date"]
    )
    train_end_exclusive = pd.Timestamp(
        config["data"]["train_end_exclusive"]
    )
    validation_end_exclusive = pd.Timestamp(
        config["data"]["validation_end_exclusive"]
    )
    test_end_exclusive = pd.Timestamp(
        config["data"]["test_end_exclusive"]
    )

    # Check chronological order
    dates_are_ordered = (
        raw_start_date
        < study_start_date
        < train_end_exclusive
        < validation_end_exclusive
        < test_end_exclusive
    )

    if not dates_are_ordered:
        raise ValueError(
            "Experiment dates must be in chronological order."
        )

    # Check warm-up period
    warmup_hours = (
        study_start_date - raw_start_date
    ).total_seconds() / 3600

    maximum_lag_hours = config["features"]["maximum_lag_hours"]

    if warmup_hours < maximum_lag_hours:
        raise ValueError(
            "The raw data period does not provide enough "
            "warm-up data for the maximum lag."
        )


if __name__ == "__main__":
    config = load_experiment_config()
    validate_experiment_config(config)

    print("Experiment configuration is valid.")