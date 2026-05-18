from pathlib import Path

from azure.storage.blob import BlobServiceClient

from src.utils.logger import get_logger


logger = get_logger("src.storage.azure_blob_writer")

def upload_file_to_azure(
    local_path: str,
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
        logger.info(f"Created Azure Conatiner: {container_name}")
    except Exception:
        logger.info(f"Azure container already exists or could not be created: {container_name}")

    with local_path.open("rb") as file_data:
        container_client.upload_blob(
            name = blob_name,
            data = file_data,
            overwrite = overwrite
        )

    logger.info(f"Uploaded file to Azure Blob Storage: {blob_name}")