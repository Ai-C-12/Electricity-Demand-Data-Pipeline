import json
import pandas as pd

from src.utils.run_summary import write_run_summary

def test_write_run_summary(tmp_path):
    df = pd.DataFrame({
        "timestamp_utc": pd.to_datetime([
            "2025-01-01T00:00:00Z",
            "2025-01-01T01:00:00Z",
        ]),
        "region": ["NYIS", "NYIS"],
        "demand_mwh": [18000, 18500],
        "temperature_2m": [2.5, 3.0],
    })

    run_id = "test_run_001"
    source = "demand_weather_features"

    write_run_summary(
        base_dir = tmp_path,
        source = source,
        run_id = run_id,
        df = df,
        output_formats = ["csv", "parquet"],
        validation_status = "passed",
    )

    summary_path = tmp_path / "run_summaries" / source / f"{run_id}.json"

    assert summary_path.exists()

    with open(summary_path, "r", encoding="utf-8") as f:
        summary = json.load(f)

    assert summary["source"] == source
    assert summary["run_id"] == run_id
    assert summary["row_count"] == 2
    assert summary["column_count"] == 4
    assert summary["output_formats"] == ["csv", "parquet"]
    assert summary["validation_status"] == "passed"
    assert summary["timestamp_min"] == "2025-01-01T00:00:00+00:00"
    assert summary["timestamp_max"] == "2025-01-01T01:00:00+00:00"