import os
import json

from azure.storage.blob import BlobServiceClient, BlobType
from azure.core.exceptions import ResourceNotFoundError, ResourceModifiedError
from src.utils.config import CONFIG

class AzureStorageClient:
    def __init__(self, logger, migration_id):
        self.container_name = CONFIG['AZURE_STORAGE_CONTAINER_NAME']
        self.connection_string = CONFIG['AZURE_CONNECTION_STRING']
        self.client = BlobServiceClient.from_connection_string(self.connection_string)
        self.container_client = self.client.get_container_client(self.container_name)
        self.logger = logger
        self.migration_id = migration_id

    def ensure_append_blob_exists(self, blob_client):
        try:
            blob_client.get_blob_properties()
        except ResourceNotFoundError:
            self.logger.info(f'Blob "{blob_client.blob_name}" not found, creating new append blob.',
                extra={
                    'migration_id': self.migration_id
                })
            blob_client.create_append_blob()

    def store(self, path, data):
        try:
            blob_client = self.container_client.get_blob_client(path)

            if isinstance(data, (dict, list)):
                blob_data = json.dumps(data) + '\n'
            else:
                blob_data = str(data) + '\n'

            blob_client.upload_blob(blob_data.encode('utf-8'), overwrite=True)

            self.logger.info(f'Successfully appended data to: {path}', extra={
                    'migration_id': self.migration_id
                })

        except ResourceModifiedError as e:
            self.logger.warning(f'Conflict when appending to blob (may be from concurrent writes): {e}', extra={
                    'migration_id': self.migration_id
                })
        except Exception as e:
            self.logger.error(f'Error appending to blob {path}: {e}', extra={
                    'migration_id': self.migration_id
                })

    def retrieve(self, blob_path):
        try:
            self.logger.info(f'Fetching azure blob storage item from path: {blob_path}', extra={
                'migration_id': self.migration_id
            })
            blob_client = self.container_client.get_blob_client(blob=blob_path)
            download_stream = blob_client.download_blob()
            return download_stream.readall()
        except ResourceNotFoundError:
            self.logger.warning(f'Blob not found: {blob_path} in container {self.container_name}',
                extra={
                    'migration_id': self.migration_id
                })
            return None
        except Exception as e:
            self.logger.error(f'An error occurred while retrieving blob "{blob_path}": {e}', extra={
                    'migration_id': self.migration_id
                })
            return None


class StorageClient:
    def __init__(self, logger, migration_id):
        self.storage = AzureStorageClient(logger, migration_id=migration_id)

    def get_data(self, full_path):
        return self.storage.retrieve(full_path)

    def store_data(self, path, data):
        self.storage.store(path, data)
