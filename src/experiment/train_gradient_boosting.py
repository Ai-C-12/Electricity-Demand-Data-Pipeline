import pandas as pd
import numpy as np

from sklearn.ensemble import GradientBoostingRegressor
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_absolute_error, root_mean_squared_error

from src.experiment.prepare_data import prepare_experiment_data
from src.transform.forecast_features import add_calendar_encoding


def train_and_evaluate_gradient_boosting(
    train_df: pd.DataFrame,
    validation_df: pd.DataFrame,
    feature_columns: list[str],
) -> dict:
    X_train = train_df[feature_columns]
    y_train = train_df["demand_mwh"]

    X_validation = validation_df[feature_columns]
    y_validation = validation_df["demand_mwh"]

    gradient_boost_pipeline = Pipeline(
        [
            ("gradient boosting regression", GradientBoostingRegressor(random_state=42))
        ]
    )

    gradient_boost_pipeline.fit(X_train, y_train)

    y_prediction = gradient_boost_pipeline.predict(X_validation)

    mae = mean_absolute_error(y_validation, y_prediction)
    rmse = root_mean_squared_error(y_validation, y_prediction)
    mean_error = np.mean(y_prediction - y_validation)

    mae_percent = (mae / y_validation.mean()) * 100

    return {
        "mae_mwh": float(mae),
        "rmse_mwh": float(rmse),
        "mean_signed_error_mwh": float(mean_error),
        "normalized_mae_percent": float(mae_percent)
    }


if __name__ == "__main__":
    # Reference Prediction
    train_df, validation_df, test_df = prepare_experiment_data()


    # Gradient Boosting A
    feature_column_a = ["demand_lag_168"]

    gradient_boost_a_result = train_and_evaluate_gradient_boosting(
        train_df,
        validation_df,
        feature_column_a,
    )

    print(f"Gradient Boost A mean validation metrics: {gradient_boost_a_result}")


    # Gradient Boosting B
    # Raw Calendar
    raw_feature_column_b = [
        "demand_lag_168",
        "day_of_week",
        "hour",
        "month",
    ]

    raw_gradient_boost_b_result = train_and_evaluate_gradient_boosting(
        train_df,
        validation_df,
        raw_feature_column_b,
    )

    print(f"Gradient Boost B mean validation metrics (Raw calendar): {raw_gradient_boost_b_result}")

    # Encoded Calendar
    encoded_train_df, encoded_validation_df = add_calendar_encoding(train_df, validation_df)

    encoded_feature_column_b = [
        "demand_lag_168",
        "day_of_week_0",
        "day_of_week_1",
        "day_of_week_2",
        "day_of_week_3",
        "day_of_week_4",
        "day_of_week_5",
        "day_of_week_6",
        "hour_sin",
        "hour_cos",
        "month_sin",
        "month_cos",
    ]

    encoded_gradient_boost_b_result = train_and_evaluate_gradient_boosting(
        encoded_train_df,
        encoded_validation_df,
        encoded_feature_column_b,
    )

    print(f"Gradient Boost B mean validation metrics (Encoded calendar): {encoded_gradient_boost_b_result}")


    # Gradient Boosting C
    feature_column_c = [
        "demand_lag_168",
        "day_of_week",
        "hour",
        "month",
        "temperature_forecast_24h",
    ]

    gradient_boost_c_result = train_and_evaluate_gradient_boosting(
        train_df,
        validation_df,
        feature_column_c,
    )

    print(f"Gradient Boost C mean validation metrics: {gradient_boost_c_result}")