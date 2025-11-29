import pytest
from unittest.mock import ANY, patch, MagicMock

from fastapi.testclient import TestClient
from src.main import app

client = TestClient(app)

@pytest.fixture
def mock_logger():
    mock = MagicMock()
    mock.info = MagicMock()
    mock.error = MagicMock()
    return mock

@pytest.fixture
def config_value():
    return {
        'name': 'test_plan',
        'executedBy': 'tester',
        'sourceModuleName': 'source_mod',
        'targetModuleName': 'target_mod',
    }

@patch('src.main.read_migration_config')
@patch('src.main.Transformer')
@patch('src.main.redis_client')
def test_trigger_transform_success(
    mock_redis_client,
    mock_transformer,
    mock_read_config,
    config_value,
    mock_logger
):
    mock_read_config.return_value = config_value

    mock_transformer_instance = MagicMock()
    mock_transformer_instance.run.return_value = True
    mock_transformer.return_value = mock_transformer_instance

    # Ensure redis operations in background task don't hit a real Redis
    mock_redis_client.delete = MagicMock(return_value=1)

    response = client.post('/transform', json={'migration_id': 'abc123'})
    assert response.status_code == 200
    assert 'Successfully started' in response.json()['message']

    mock_read_config.assert_called_once_with(ANY, 'abc123')
    mock_transformer.assert_called_once()
    mock_redis_client.delete.assert_called_once()

@patch('src.main.read_migration_config')
def test_trigger_transform_bad_request_missing_migration_id(mock_read_config):
    response = client.post('/transform', json={'migration_id': ' '})
    assert response.status_code == 400
    assert 'Invalid / missing migration_id' == response.json()['detail']
    mock_read_config.assert_not_called()

@patch('src.main.read_migration_config')
def test_trigger_transform_error_fetching_config(mock_read_config):
    mock_read_config.return_value = None
    response = client.post('/transform', json={'migration_id': 'abc123'})
    assert response.status_code == 400
    assert 'Error fetching migration configs' == response.json()['detail']
