import pandas as pd
from datetime import datetime, timezone
import json
from pathlib import Path

def write_run_summary(
    base_dir: str,
    source: str,
    run_id: str,
    df: pd.DataFrame,
    output_formats: list[str],
    validation_status: str = "passed",
) -> None:
    if "timestamp_utc" not in df.columns:
        raise ValueError("DataFrame must contain a 'timestamp_utc' column.")

    summary_dir = Path(base_dir) / "run_summaries" / source
    summary_dir.mkdir(parents=True, exist_ok=True)

    summary_path = summary_dir / f"{run_id}.json"

    summary = {
        "source": source,
        "run_id": run_id,
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": list(df.columns),
        "timestamp_min": df["timestamp_utc"].min().isoformat(),
        "timestamp_max": df["timestamp_utc"].max().isoformat(),
        "output_formats": output_formats,
        "validation_status": validation_status,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)