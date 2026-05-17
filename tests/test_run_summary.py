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

    extra_metadata = {
        "pipeline_stage": "feature_engineering",
        "demand_row_count": 2,
        "weather_row_count": 2,
        "merged_row_count": 2,
        "expected_merge_rows": 2,
        "merge_retention_rate": 1.0,
        "merge_retention_status": "passed",
        "timestamp_coverage_status": "passed",
    }

    run_id = "test_run_001"
    source = "demand_weather_features"

    write_run_summary(
        base_dir = tmp_path,
        source = source,
        run_id = run_id,
        df = df,
        output_formats = ["csv", "parquet"],
        validation_status = "passed",
        extra_metadata = extra_metadata,
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

    assert summary["pipeline_stage"] == "feature_engineering"
    assert summary["demand_row_count"] == 2
    assert summary["weather_row_count"] == 2
    assert summary["merged_row_count"] == 2
    assert summary["expected_merge_rows"] == 2
    assert summary["merge_retention_rate"] == 1.0
    assert summary["merge_retention_status"] == "passed"
    assert summary["timestamp_coverage_status"] == "passed"