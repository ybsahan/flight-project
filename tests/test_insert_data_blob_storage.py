import unittest
from unittest import mock
from unittest.mock import patch
import os
import json
from insert_blob_storage import insert_data_into_bs


class TestInsertDataIntoBS(unittest.TestCase):

    @patch('azure.storage.blob.BlobServiceClient.from_connection_string')
    def test_insert_data_into_bs(self, mock_service):
        # Create test data
        data = {"flight_number": "123", "status": "L"}
        blob_name = "flight_data"
        container_name = "test_container"
        year = 2023
        month = 1
        day = 15
        hour = 12

        # Set up mock values for the BlobServiceClient and BlobClient
        container_client = unittest.mock.MagicMock()
        blob_client = unittest.mock.MagicMock()
        mock_service.return_value.get_container_client.return_value = container_client
        container_client.get_blob_client.return_value = blob_client

        # Call the function
        insert_data_into_bs(data, blob_name, container_name, year, month, day, hour)

        # Assert that the correct methods were called with the correct arguments
        mock_service.assert_called_with(conn_str=os.getenv("BLOB_CONNECTION"))
        mock_service.return_value.get_container_client.assert_called_with(container_name)
        container_client.get_blob_client.assert_called_with(blob_name + f"-{year}-{month}-{day}-{hour}.json")
        blob_client.upload_blob.assert_called_with(
            json.dumps(data), overwrite=True
        )
