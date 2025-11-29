import pytest
from unittest.mock import Mock, patch
from azure.core.exceptions import ResourceNotFoundError, ResourceModifiedError

import src.azure.storage as azure_storage


@pytest.fixture
def logger():
    return Mock()

@pytest.fixture
def config_patch(monkeypatch):
    monkeypatch.setattr(azure_storage, 'CONFIG', {
        'AZURE_STORAGE_CONTAINER_NAME': 'test-container',
        'AZURE_CONNECTION_STRING': 'fake-connection-string'
    })

@pytest.fixture
def azure_patch():
    with patch.object(azure_storage, 'BlobServiceClient') as mock_blob_service_client:
        yield mock_blob_service_client


def make_client_with_mocks(logger, azure_patch):
    # Setup the whole fake Azure client chain
    blob_client = Mock()
    container_client = Mock(get_blob_client=Mock(return_value=blob_client))
    fake_service_client = Mock(get_container_client=Mock(return_value=container_client))
    azure_patch.from_connection_string.return_value = fake_service_client

    client = azure_storage.AzureStorageClient(logger, 'abc123')
    return client, blob_client


def test_ensure_append_blob_exists_creates_if_not_found(config_patch, logger, azure_patch):
    client, blob = make_client_with_mocks(logger, azure_patch)
    blob.get_blob_properties.side_effect = ResourceNotFoundError('not found')

    client.ensure_append_blob_exists(blob)

    blob.create_append_blob.assert_called_once()
    logger.info.assert_called_with(f'Blob "{blob.blob_name}" not found, creating new append blob.', extra={
        'migration_id': 'abc123'
    })

def test_ensure_append_blob_exists_does_nothing_if_exists(config_patch, logger, azure_patch):
    client, blob = make_client_with_mocks(logger, azure_patch)
    blob.get_blob_properties.return_value = {}

    client.ensure_append_blob_exists(blob)

    blob.create_append_blob.assert_not_called()

def test_store_with_dict_data(config_patch, logger, azure_patch):
    client, blob = make_client_with_mocks(logger, azure_patch)
    client.store('some/path', {'foo': 'bar'})

    blob.upload_blob.assert_called_once()
    logger.info.assert_called_with('Successfully appended data to: some/path', extra={
        'migration_id': 'abc123'
    })

def test_store_with_string_data(config_patch, logger, azure_patch):
    client, blob = make_client_with_mocks(logger, azure_patch)
    client.store('other/path', 'hello world')

    blob.upload_blob.assert_called_once()
    logger.info.assert_called_with('Successfully appended data to: other/path', extra={
        'migration_id': 'abc123'
    })

def test_store_handles_resource_modified_error(config_patch, logger, azure_patch):
    client, blob = make_client_with_mocks(logger, azure_patch)
    blob.upload_blob.side_effect = ResourceModifiedError('conflict')

    client.store('conflict/path', {'foo': 'bar'})

    logger.warning.assert_called()
    assert 'Conflict' in logger.warning.call_args[0][0]

def test_store_handles_generic_exception(config_patch, logger, azure_patch):
    client, blob = make_client_with_mocks(logger, azure_patch)
    blob.upload_blob.side_effect = Exception('unexpected error')

    client.store('bad/path', {'foo': 'bar'})

    logger.error.assert_called()
    assert 'Error' in logger.error.call_args[0][0]

def test_retrieve_success(config_patch, logger, azure_patch):
    client, blob = make_client_with_mocks(logger, azure_patch)
    blob.download_blob.return_value.readall.return_value = b'some data'

    result = client.retrieve('some/path')

    assert result == b'some data'
    logger.info.assert_called_with('Fetching azure blob storage item from path: some/path', extra={
        'migration_id': 'abc123'
    })

def test_retrieve_handles_not_found(config_patch, logger, azure_patch):
    client, blob = make_client_with_mocks(logger, azure_patch)
    blob.download_blob.side_effect = ResourceNotFoundError('not found')

    result = client.retrieve('missing/path')

    assert result is None
    logger.warning.assert_called_with(f'Blob not found: missing/path in container test-container', extra={'migration_id': 'abc123'})

def test_retrieve_handles_generic_exception(config_patch, logger, azure_patch):
    client, blob = make_client_with_mocks(logger, azure_patch)
    blob.download_blob.side_effect = Exception('unexpected failure')

    result = client.retrieve('fail/path')

    assert result is None
    logger.error.assert_called()
    assert 'An error occurred' in logger.error.call_args[0][0]

def test_storage_client_delegates(config_patch, logger):
    with patch.object(azure_storage, 'AzureStorageClient') as mock_azure:
        instance = mock_azure.return_value

        client = azure_storage.StorageClient(logger, migration_id='abc123')
        client.store_data('path', 'data')
        client.get_data('other/path')

        instance.store.assert_called_once_with('path', 'data')
        instance.retrieve.assert_called_once_with('other/path')
