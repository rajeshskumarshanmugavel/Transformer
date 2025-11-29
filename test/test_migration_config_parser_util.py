import json
import pytest
from unittest.mock import patch, mock_open, MagicMock

from src.utils import migration_config_parser as parser_module


@pytest.fixture
def example_json():
    return json.dumps({
        'field1': 'value1',
        'field2': 'value2'
    })


def test_parser_in_local_mode_adds_createdAt(example_json):
    migration_details = {
        'transformation_json': example_json
    }

    result = parser_module.parser(migration_details)

    assert 'field1' in result
    assert 'createdAt' in result
    assert result['field1'] == 'value1'
    assert isinstance(result['createdAt'], str)


def test_parser_in_non_local_mode_retains_created_at(example_json):
    migration_details = {
        'transformation_json': example_json,
        'created_at': '2023-01-01T00:00:00Z'
    }

    original_exec_env = parser_module.CONFIG['EXEC_ENVIRONMENT']
    parser_module.CONFIG['EXEC_ENVIRONMENT'] = 'production'

    try:
        result = parser_module.parser(migration_details)
    finally:
        parser_module.CONFIG['EXEC_ENVIRONMENT'] = original_exec_env

    assert result['createdAt'] == '2023-01-01T00:00:00Z'
    assert result['field1'] == 'value1'


def test_read_migration_config_local_reads_file(example_json):
    assert parser_module.CONFIG['EXEC_ENVIRONMENT'] == 'local'

    with patch('builtins.open', mock_open(read_data=example_json)) as mock_file:
        result = parser_module.read_migration_config(logger=MagicMock(), migration_id='123')

    assert 'field1' in result
    mock_file.assert_called_once()
    assert result['createdAt'] is not None


def test_read_migration_config_non_local_fetches_remote(example_json):
    original_exec_env = parser_module.CONFIG['EXEC_ENVIRONMENT']
    parser_module.CONFIG['EXEC_ENVIRONMENT'] = 'production'

    mock_logger = MagicMock()
    fake_migration_details = {
        'transformation_json': example_json,
        'created_at': '2024-02-02T00:00:00Z'
    }

    with patch('src.utils.migration_config_parser.get_migration_details', return_value=fake_migration_details) as mock_get:
        result = parser_module.read_migration_config(mock_logger, migration_id='abc')

    parser_module.CONFIG['EXEC_ENVIRONMENT'] = original_exec_env

    assert result['field1'] == 'value1'
    assert result['createdAt'] == '2024-02-02T00:00:00Z'
    mock_get.assert_called_once_with(mock_logger, 'abc')


def test_read_migration_config_handles_exceptions():
    assert parser_module.CONFIG['EXEC_ENVIRONMENT'] == 'local'
    mock_logger = MagicMock()

    with patch('builtins.open', side_effect=Exception('File error')):
        result = parser_module.read_migration_config(mock_logger, migration_id='123')

    assert result is None
    mock_logger.error.assert_called()


def test_read_migration_config_handles_unicode_decode_error():
    """Test that UnicodeDecodeError is handled with fallback encoding"""
    assert parser_module.CONFIG['EXEC_ENVIRONMENT'] == 'local'
    mock_logger = MagicMock()
    example_json = json.dumps({'field1': 'value1'})
    
    # First call raises UnicodeDecodeError, second call (fallback) succeeds
    mock_file = mock_open(read_data=example_json)
    
    with patch('builtins.open', mock_file) as mocked_open:
        # Configure the mock to raise UnicodeDecodeError on first call
        mocked_open.side_effect = [
            UnicodeDecodeError('utf-8', b'', 0, 1, 'invalid start byte'),
            mock_open(read_data=example_json).return_value
        ]
        
        result = parser_module.read_migration_config(mock_logger, migration_id='123')
    
    # Should log the unicode error
    assert mock_logger.error.call_count >= 1
    assert any('Unicode decode error' in str(call) for call in mock_logger.error.call_args_list)


def test_read_migration_config_unicode_error_fallback_also_fails():
    """Test when both UTF-8 and fallback encodings fail"""
    assert parser_module.CONFIG['EXEC_ENVIRONMENT'] == 'local'
    mock_logger = MagicMock()
    
    with patch('builtins.open') as mocked_open:
        # Both attempts raise errors
        mocked_open.side_effect = [
            UnicodeDecodeError('utf-8', b'', 0, 1, 'invalid start byte'),
            Exception('Fallback also failed')
        ]
        
        result = parser_module.read_migration_config(mock_logger, migration_id='123')
    
    assert result is None
    # Should have logged both the unicode error and fallback error
    assert mock_logger.error.call_count >= 2
