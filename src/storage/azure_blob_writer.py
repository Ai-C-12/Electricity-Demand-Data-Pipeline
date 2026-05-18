from pathlib import Path

from azure.storage.blob import BlobServiceClient

from src.utils.logger import get_logger


logger = get_logger("src.storage.azure_blob_writer")

def upload_file_to_azure(
    local_path: str | Path,
    blob_name: str,
    connection_string: str,
    container_name: str,
    overwrite: bool = True,
) -> None:
    local_path = Path(local_path)

    if not local_path.exists():
        raise FileNotFoundError(f"Local file does not exist: {local_path}")
    
    if not local_path.is_file():
        raise ValueError(f"Path is not a file: {local_path}")
    
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    container_client = blob_service_client.get_container_client(container_name)

    try:
        container_client.create_container()
        logger.info(f"Created Azure container: {container_name}")
    except Exception:
        logger.info(f"Azure container already exists or could not be created: {container_name}")

    with local_path.open("rb") as file_data:
        container_client.upload_blob(
            name = blob_name,
            data = file_data,
            overwrite = overwrite
        )

    logger.info(f"Uploaded file to Azure Blob Storage: {blob_name}")


def local_artifact_path_to_blob_name(local_path: str | Path) -> str:
    """
    Convert a local project artifact path into a clean Azure blob path.

    Examples:
    data/processed/demand_weather_features/... -> processed/demand_weather_features/...
    data/raw/eia_region_data/...              -> raw/eia_region_data/...
    logs/run_summaries/...                    -> run_summaries/...
    """

    local_path = Path(local_path)
    parts = local_path.as_posix().split("/")

    if "data" in parts:
        data_index = parts.index("data")
        return "/".join(parts[data_index + 1:])

    if "logs" in parts and "run_summaries" in parts:
        summary_index = parts.index("run_summaries")
        return "/".join(parts[summary_index:])

    raise ValueError(f"Cannot convert local path to Azure blob name: {local_path}") 


def upload_files_to_azure(
    local_paths: list[str | Path],
    connection_string: str,
    container_name: str,
) -> list[str]: 
    """
    Upload multiple local artifact files to Azure Blob Storage.

    Returns the Azure blob names that were uploaded.
    """

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    try:
        container_client.create_container()
        logger.info(f"Created Azure container: {container_name}")
    except Exception:
        logger.info(f"Azure container already exists or could not be created: {container_name}")

    uploaded_blob_names = []

    logger.info(f"Uploading {len(local_paths)} files to Azure Blob Storage...")
    for local_path in local_paths:
        local_path = Path(local_path)

        if not local_path.exists():
            raise FileNotFoundError(f"Local file does not exist: {local_path}")

        if not local_path.is_file():
            raise ValueError(f"Path is not a file: {local_path}")

        blob_name = local_artifact_path_to_blob_name(local_path)

        with local_path.open("rb") as file_data:
            container_client.upload_blob(
                name=blob_name,
                data=file_data,
                overwrite=True,
            )

        uploaded_blob_names.append(blob_name)
    
    logger.info(f"{len(uploaded_blob_names)} files uploaded.")
    
    return uploaded_blob_names