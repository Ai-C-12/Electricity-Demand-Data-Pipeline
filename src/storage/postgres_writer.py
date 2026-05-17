from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.dialects.postgresql import insert

from src.utils.logger import get_logger


logger = get_logger("src.storage.postgres_writer")

def create_postgres_tables(
    database_url: str,
    schema_path: str = "sql/demand_weather_features.sql",
) -> None:
    logger.info(f"Creating PostgreSQL tables from schema file: {schema_path}")

    engine = create_engine(database_url)

    schema_sql = Path(schema_path).read_text(encoding="utf-8")

    with engine.begin() as conn:
        conn.execute(text(schema_sql))


def write_to_postgres(
    df: pd.DataFrame,
    database_url: str,
    run_id: str,
) -> None:
    logger.info(f"Starting PostgreSQL upsert for demand_weather_features. Run ID: {run_id}")
    
    engine = create_engine(database_url)

    df_to_write = df.copy()
    df_to_write["run_id"] = run_id

    records = df_to_write.to_dict(orient="records")

    if not records:
        logger.warning(f"No records to write to PostgreSQL. Run ID: {run_id}")
        return

    metadata = MetaData()

    table = Table(
        "demand_weather_features",
        metadata,
        autoload_with=engine,
    )

    insert_stmt = insert(table).values(records)

    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=["region", "timestamp_utc"],
        set_={
            "demand_mwh": insert_stmt.excluded.demand_mwh,
            "temperature_2m": insert_stmt.excluded.temperature_2m,
            "hour": insert_stmt.excluded.hour,
            "day_of_week": insert_stmt.excluded.day_of_week,
            "month": insert_stmt.excluded.month,
            "run_id": insert_stmt.excluded.run_id,
            "loaded_at_utc": text("NOW()"),
        },
    )

    with engine.begin() as conn:
        conn.execute(upsert_stmt)

    logger.info(
        f"PostgreSQL upsert completed for demand_weather_features: "
        f"{len(records)} records. Run ID: {run_id}"
    )