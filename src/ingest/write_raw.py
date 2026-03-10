from pathlib import Path
import json
import pandas as pd
from datetime import datetime, timezone


def make_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def save_raw_per_run(
    base_dir: str,
    source: str,
    run_id: str,
    payload: dict,
    request_meta: dict
) -> None:
    run_dir = Path(base_dir) / source / "_runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    # IMPORTANT: ensure request_meta does NOT include api_key
    with open(run_dir / "raw.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    with open(run_dir / "request.json", "w", encoding="utf-8") as f:
        json.dump(request_meta, f, ensure_ascii=False, indent=2)


def save_partitioned_csv(
    base_dir: str,
    source: str,
    df: pd.DataFrame,
    run_id: str
) -> None:
    if "timestamp_utc" not in df.columns:
        raise ValueError("DataFrame must contain a 'timestamp_utc' column.")

    if not pd.api.types.is_datetime64_any_dtype(df["timestamp_utc"]):
        raise TypeError("'timestamp_utc' must be a datetime column.")

    df = df.copy()

    df["year"] = df["timestamp_utc"].dt.year
    df["month"] = df["timestamp_utc"].dt.strftime("%m")
    df["day"] = df["timestamp_utc"].dt.strftime("%d")

    for (y, m, d), group_df in df.groupby(["year", "month", "day"], sort=True):
        out_dir = Path(base_dir) / source / f"year={y}" / f"month={m}" / f"day={d}"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / f"part-{run_id}.csv"
        group_df.drop(columns=["year", "month", "day"]).to_csv(out_path, index=False)
