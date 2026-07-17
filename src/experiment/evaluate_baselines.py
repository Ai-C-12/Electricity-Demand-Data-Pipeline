import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, root_mean_squared_error


def evaluate_weekly_naive(
    validation_df: pd.DataFrame,
) -> dict:
    y_true = validation_df["demand_mwh"]
    y_pred = validation_df["demand_lag_168"]

    mae = mean_absolute_error(y_true, y_pred)
    rmse = root_mean_squared_error(y_true, y_pred)

    mean_signed_error = np.mean(y_pred - y_true)

    normalized_mae_percent = (
        mae / y_true.mean()
    ) * 100

    return {
        "mae_mwh": float(mae),
        "rmse_mwh": float(rmse),
        "mean_signed_error_mwh": float(mean_signed_error),
        "normalized_mae_percent": float(normalized_mae_percent),
    }


def evaluate_historical_mean(
    train_df: pd.DataFrame,
    validation_df: pd.DataFrame,
) -> dict:
    historical_mean = (
        train_df
        .groupby(["day_of_week", "hour"])["demand_mwh"]
        .mean()
        .reset_index()
        .rename(
            columns={"demand_mwh": "historical_mean_prediction"}
        )
    )

    prediction_df = validation_df.merge(
        historical_mean,
        on=["day_of_week", "hour"],
        how="left",
        validate="many_to_one",
    )

    if prediction_df["historical_mean_prediction"].isna().any():
        raise ValueError(
            "Historical mean baseline produced missing predictions."
        )
    
    y_true = prediction_df["demand_mwh"]
    y_pred = prediction_df["historical_mean_prediction"]

    mae = mean_absolute_error(y_true, y_pred)
    rmse = root_mean_squared_error(y_true, y_pred)
    mean_signed_error = np.mean(y_pred - y_true)

    normalized_mae_percent = (mae / y_true.mean()) * 100

    return {
        "mae_mwh": float(mae),
        "rmse_mwh": float(rmse),
        "mean_signed_error_mwh": float(mean_signed_error),
        "normalized_mae_percent": float(normalized_mae_percent),
    }