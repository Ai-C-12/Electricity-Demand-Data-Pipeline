import pandas as pd
from src.utils.logger import get_logger

from src.validation.checks import (
    check_duplicate_timestamps,
    check_hourly_timestamp_coverage,
    check_no_missing_values,
    check_not_empty,
    check_required_columns,
    check_temperature_values,
    check_timestamp_format,
)


logger = get_logger("src.validation.weather_validation")

def validate_historical_weather(
    raw_df: pd.DataFrame,
    clean_df: pd.DataFrame,
) -> None:
    raw_name = "Raw historical weather"
    clean_name = "Processed historical weather"

    check_not_empty(raw_df, raw_name)

    check_required_columns(
        raw_df,
        [
            "date",
            "temperature_2m",
            "latitude",
            "longitude",
        ],
        raw_name,
    )
    logger.info(f"Validated {raw_name}.")

    check_not_empty(clean_df, clean_name)

    check_required_columns(
        clean_df,
        [
            "timestamp_utc",
            "temperature_2m",
            "latitude",
            "longitude",
        ],
        clean_name,
    )

    check_no_missing_values(clean_df, clean_name)
    check_timestamp_format(clean_df, clean_name)

    check_temperature_values(
        clean_df,
        clean_name,
        temperature_col="temperature_2m",
    )

    check_duplicate_timestamps(clean_df, clean_name)
    check_hourly_timestamp_coverage(clean_df, clean_name)

    logger.info(f"Validated {clean_name}.")


def validate_archived_forecast_weather(
    raw_df: pd.DataFrame,
    clean_df: pd.DataFrame,
) -> None:
    raw_name = "Raw archived forecast weather"
    clean_name = "Processed archived forecast weather"

    check_not_empty(raw_df, raw_name)

    check_required_columns(
        raw_df,
        [
            "date",
            "temperature_2m_previous_day1",
            "latitude",
            "longitude",
        ],
        raw_name,
    )
    logger.info(f"Validated {raw_name}.")

    check_not_empty(clean_df, clean_name)

    check_required_columns(
        clean_df,
        [
            "timestamp_utc",
            "temperature_forecast_24h",
            "latitude",
            "longitude",
        ],
        clean_name,
    )

    check_no_missing_values(clean_df, clean_name)
    check_timestamp_format(clean_df, clean_name)

    check_temperature_values(
        clean_df,
        clean_name,
        temperature_col="temperature_forecast_24h",
    )

    check_duplicate_timestamps(clean_df, clean_name)
    check_hourly_timestamp_coverage(clean_df, clean_name)

    logger.info(f"Validated {clean_name}.")