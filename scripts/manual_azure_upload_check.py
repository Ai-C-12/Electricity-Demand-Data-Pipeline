from pathlib import Path

from src.config import AZURE_STORAGE_CONNECTION_STRING, AZURE_STORAGE_CONTAINER
from src.storage.azure_blob_writer import upload_file_to_azure


def main() -> None:
    if not AZURE_STORAGE_CONNECTION_STRING:
        raise ValueError("Missing AZURE_STORAGE_CONNECTION_STRING in .env")
    
    local_path = Path("mock_artifacts/demand_weather_features_mock.csv")

    blob_name = (
        "processed/demand_weather_features/"
        "year=2025/month=01/day=01/"
        "demand_weather_features_mock.csv"
    )

    upload_file_to_azure(
        local_path = local_path,
        blob_name = blob_name,
        connection_string = AZURE_STORAGE_CONNECTION_STRING,
        container_name = AZURE_STORAGE_CONTAINER,
        overwrite = True
    )

    print("Azure upload test completed.")
    print(f"Uploaded local file: {local_path}")
    print(f"Azure blob path: {blob_name}")

if __name__ == "__main__":
    main()