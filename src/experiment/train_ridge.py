import pandas as pd


def train_and_evaluate_ridge(
    train_df: pd.DataFrame,
    validation_df: pd.DataFrame,
    feature_columns: list[str],
) -> dict:
    feature_columns = ["demand_lag_168"]

    X_train = train_df[feature_columns]
    y_train = train_df["demand_mwh"]

    X_validation = validation_df[feature_columns]
    y_validation = validation_df["demand_mwh"]