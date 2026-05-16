import json

from src.storage.write_raw import save_raw_per_run


def test_save_raw_per_run(tmp_path):
    payload = {
    "data": [
        {"period": "2025-01-01T00", "value": 18000},
        {"period": "2025-01-01T01", "value": 18500},
        ]
    }

    request_meta = {
        "source": "eia",
        "respondent": "NYIS",
        "type": "D",
        "start": "2025-01-01T00",
        "end": "2025-01-01T01",
    }

    source = "eia_region_data"
    run_id = "test_run_001"

    save_raw_per_run(
        base_dir = tmp_path,
        source = source,
        run_id = run_id,
        payload = payload,
        request_meta = request_meta,
    )

    raw_path = tmp_path / source / "_runs" / run_id / "raw.json"
    request_path = tmp_path / source / "_runs" / run_id / "request.json"

    assert raw_path.exists()
    assert request_path.exists()

    with open(raw_path, "r", encoding="utf-8") as f:
        saved_payload = json.load(f)

    with open(request_path, "r", encoding="utf-8") as f:
        saved_request_meta = json.load(f)

    assert saved_payload == payload
    assert saved_request_meta == request_meta

    assert "api_key" not in saved_request_meta