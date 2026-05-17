import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from src.storage.postgres_writer import create_postgres_tables, write_to_postgres


load_dotenv()

database_url = os.getenv("DATABASE_URL")

if not database_url:
    raise ValueError("DATABASE_URL is not set. Add it to your .env file.")


mock_df = pd.DataFrame({
    "timestamp_utc": pd.to_datetime([
        "2025-01-01T00:00:00Z",
        "2025-01-01T01:00:00Z",
    ]),
    "region": ["NYIS", "NYIS"],
    "demand_mwh": [18000.0, 18500.0],
    "temperature_2m": [2.5, 3.0],
    "hour": [0, 1],
    "day_of_week": [2, 2],
    "month": [1, 1],
})


def main() -> None:
    create_postgres_tables(database_url)

    write_to_postgres(
        df=mock_df,
        database_url=database_url,
        run_id="test_run_001",
    )

    mock_df_changed = mock_df.copy()
    mock_df_changed.loc[0, "demand_mwh"] = 99999.0

    write_to_postgres(
        df=mock_df_changed,
        database_url=database_url,
        run_id="test_run_002",
    )

    engine = create_engine(database_url)

    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT timestamp_utc, region, demand_mwh, run_id
            FROM public.demand_weather_features
            ORDER BY timestamp_utc;
        """)).fetchall()

    for row in rows:
        print(row)


if __name__ == "__main__":
    main()