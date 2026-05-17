import pandas as pd
from pathlib import Path

from src.storage.write_raw import save_partitioned_csv, save_partitioned_parquet


def make_mock_feature_df() -> pd.DataFrame:
    return pd.DataFrame({
        "timestamp_utc": pd.to_datetime([
            "2025-01-01T00:00:00Z",
            "2025-01-01T01:00:00Z",
            "2025-01-02T00:00:00Z",
        ]),
        "region": ["NYIS", "NYIS", "NYIS"],
        "demand_mwh": [18000, 18500, 19000],
        "temperature_2m": [2.5, 3.0, 1.8],
        "hour": [0, 1, 0],
        "day_of_week": [2, 2, 3],
        "month": [1, 1, 1],
    })


def test_save_partitioned_csv(tmp_path):
    df = make_mock_feature_df()
    source = "demand_weather_features"
    run_id = "test_run_001"

    written_paths = save_partitioned_csv(
        base_dir = tmp_path,
        source = source,
        df = df,
        run_id = run_id,
    )

    assert len(written_paths) == 2

    for path in written_paths:
        assert Path(path).exists()

    day_1_path = tmp_path / source / "year=2025" / "month=01" / "day=01" / f"part-{run_id}.csv"
    day_2_path = tmp_path / source / "year=2025" / "month=01" / "day=02" / f"part-{run_id}.csv"
    assert day_1_path.exists()
    assert day_2_path.exists()

    csv_files = list((tmp_path / source).rglob("*.csv"))
    assert len(csv_files) == 2

    for csv_file in csv_files:
        partition_df = pd.read_csv(csv_file)
        assert not partition_df.empty
        assert set(partition_df.columns) == set(df.columns)


def test_save_partitioned_parquet(tmp_path):
    df = make_mock_feature_df()
    source = "demand_weather_features"
    run_id = "test_run_002"

    written_paths = save_partitioned_parquet(
        base_dir = tmp_path,
        source = source,
        df = df,
        run_id = run_id,
    )

    assert len(written_paths) == 2

    for path in written_paths:
        assert Path(path).exists()

    day_1_path = tmp_path / source / "year=2025" / "month=01" / "day=01" / f"part-{run_id}.parquet"
    day_2_path = tmp_path / source / "year=2025" / "month=01" / "day=02" / f"part-{run_id}.parquet"
    assert day_1_path.exists()
    assert day_2_path.exists()

    parquet_files = list((tmp_path / source).rglob("*.parquet"))
    assert len(parquet_files) == 2

    for parquet_file in parquet_files:
        partition_df = pd.read_parquet(parquet_file)
        assert not partition_df.empty
        assert set(partition_df.columns) == set(df.columns)