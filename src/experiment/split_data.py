import pandas as pd

import pandas as pd


def split_forecasting_data(
    df: pd.DataFrame,
    config: dict,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    study_start = pd.Timestamp(
        config["data"]["study_start_date"],
        tz="UTC",
    )

    train_end = pd.Timestamp(
        config["data"]["train_end_exclusive"],
        tz="UTC",
    )

    validation_end = pd.Timestamp(
        config["data"]["validation_end_exclusive"],
        tz="UTC",
    )

    test_end = pd.Timestamp(
        config["data"]["test_end_exclusive"],
        tz="UTC",
    )

    train_df = df[
        (df["timestamp_utc"] >= study_start)
        & (df["timestamp_utc"] < train_end)
    ].copy()

    validation_df = df[
        (df["timestamp_utc"] >= train_end)
        & (df["timestamp_utc"] < validation_end)
    ].copy()

    test_df = df[
        (df["timestamp_utc"] >= validation_end)
        & (df["timestamp_utc"] < test_end)
    ].copy()

    if train_df.empty:
        raise ValueError("Training split is empty.")

    if validation_df.empty:
        raise ValueError("Validation split is empty.")

    if test_df.empty:
        raise ValueError("Test split is empty.")

    if (
        train_df["timestamp_utc"].max()
        >= validation_df["timestamp_utc"].min()
    ):
        raise ValueError(
            "Training and validation splits overlap."
        )

    if (
        validation_df["timestamp_utc"].max()
        >= test_df["timestamp_utc"].min()
    ):
        raise ValueError(
            "Validation and test splits overlap."
        )

    return (
        train_df.reset_index(drop=True),
        validation_df.reset_index(drop=True),
        test_df.reset_index(drop=True),
    )