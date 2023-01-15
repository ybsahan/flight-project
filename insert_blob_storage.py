import json
import os

from azure.storage.blob import BlobServiceClient


def insert_data_into_bs(data: object, blob_name: str, container_name: str, year: int, month: int, day: int, hour: int) -> object:

    # Azure Blob Storage için BlobServisClient nesnesi oluşturun
    service = BlobServiceClient.from_connection_string(conn_str=os.getenv("BLOB_CONNECTION"))

    # Depolama kapsayıcısı oluşturun veya mevcut bir kapsayıcı seçin
    container = service.get_container_client(container_name)

    # Dosya yüklemek için BlobClient nesnesi oluşturun
    blob = container.get_blob_client(blob_name + f"-{year}-{month}-{day}-{hour}.json")

    blob.upload_blob(json.dumps(data), overwrite=True)
