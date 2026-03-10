from pathlib import Path
import json
import pandas as pd
from datetime import datetime, timezone

def make_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def save_raw_per_run(base_dir: str, source: str, run_id: str, payload: dict, request_meta: dict):
    run_dir = Path(base_dir) / source / "_runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    # IMPORTANT: ensure request_meta does NOT include api_key
    with open(run_dir / "raw.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)

    with open(run_dir / "request.json", "w", encoding="utf-8") as f:
        json.dump(request_meta, f, ensure_ascii=False, indent=2)

def save_partitioned_csv(base_dir: str, source: str, df: pd.DataFrame):
    df = df.copy()

    # expects df["timestamp_utc"] to be timezone-aware datetime
    df["year"]  = df["timestamp_utc"].dt.year
    df["month"] = df["timestamp_utc"].dt.strftime("%m")
    df["day"]   = df["timestamp_utc"].dt.strftime("%d")

    for (y, m, d), g in df.groupby(["year", "month", "day"], sort=True):
        out_dir = Path(base_dir) / source / f"year={y}" / f"month={m}" / f"day={d}"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_path = out_dir / "part-000.csv"
        g.drop(columns=["year", "month", "day"]).to_csv(out_path, index=False)