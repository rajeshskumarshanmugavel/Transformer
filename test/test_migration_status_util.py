import json
import pytest
from unittest.mock import patch, MagicMock, mock_open

import src.utils.migration_status as migration_status_util


def test_get_date_components_returns_expected_parts():
    date_str = '2024-07-01T12:00:00'
    year, month, day = migration_status_util.get_date_components(date_str)
    assert year == 2024
    assert month == '07'
    assert day == '01'


def test_get_migration_storage_path_formats_correctly():
    migration_id = 'abc123'
    migration_configs = {
        'createdAt': '2024-07-01T12:00:00'
    }
    path = migration_status_util.get_migration_storage_path(migration_id, migration_configs)
    assert path == '2024/07/01/abc123'


@patch('src.utils.migration_status.get_current_ts', return_value=99999999)
@patch('src.utils.migration_status.StorageClient')
def test_create_migration_status_in_cloud(mock_storage_cls, mock_ts):
    mock_logger = MagicMock()
    mock_storage = MagicMock()
    mock_storage_cls.return_value = mock_storage

    result = migration_status_util.create_migration_status_in_cloud(mock_logger, 'abc123', 'some/path')

    assert result['status'] == 'IN_PROGRESS'
    assert result['started_at'] == 99999999
    mock_storage.store_data.assert_called_once()
    stored_json = json.loads(mock_storage.store_data.call_args.args[1])
    assert stored_json['status'] == 'IN_PROGRESS'


@patch('builtins.open', new_callable=mock_open, read_data='some logs')
@patch('src.utils.migration_status.get_current_ts', return_value=99999999)
@patch('src.utils.migration_status.StorageClient')
def test_update_migration_status_to_cloud(mock_storage_cls, mock_ts, mock_file):
    mock_logger = MagicMock()
    mock_storage = MagicMock()
    mock_storage_cls.return_value = mock_storage

    migration_id = 'abc123'
    migration_status = {'status': 'IN_PROGRESS'}
    migration_configs = {'createdAt': '2024-07-01T12:00:00'}

    migration_status_util.update_migration_status_to_cloud(mock_logger, migration_id, migration_status, migration_configs)

    assert migration_status['updated_at'] == 99999999
    assert mock_file.call_args_list[0][0][0] == 'logs/migration_status_abc123.json'
    mock_storage.store_data.call_args_list[0][0] == (
        '2024/07/01/abc123/migration_status.json',
        json.dumps(migration_status)
    )


@patch('src.utils.migration_status.StorageClient')
def test_get_migration_status_from_cloud_existing(mock_storage_cls):
    mock_logger = MagicMock()
    migration_id = 'abc123'
    migration_configs = {'createdAt': '2024-07-01T12:00:00'}

    existing_status = json.dumps({'status': 'IN_PROGRESS'})

    mock_storage = MagicMock()
    mock_storage.get_data.return_value = existing_status
    mock_storage_cls.return_value = mock_storage

    result = migration_status_util.get_migration_status_from_cloud(mock_logger, migration_id, migration_configs)

    assert result['status'] == 'IN_PROGRESS'
    mock_storage.get_data.assert_called_once()


@patch('src.utils.migration_status.create_migration_status_in_cloud', return_value={'status': 'IN_PROGRESS'})
@patch('src.utils.migration_status.StorageClient')
def test_get_migration_status_from_cloud_creates_if_missing(mock_storage_cls, mock_create):
    mock_logger = MagicMock()
    migration_id = 'abc123'
    migration_configs = {'createdAt': '2024-07-01T12:00:00'}

    mock_storage = MagicMock()
    mock_storage.get_data.return_value = None
    mock_storage_cls.return_value = mock_storage

    result = migration_status_util.get_migration_status_from_cloud(mock_logger, migration_id, migration_configs)

    assert result['status'] == 'IN_PROGRESS'
    mock_create.assert_called_once()



def test_traverse_local_migration_status_retrieves_value():
    migration_status = {
        'entity': {
            'root': {
                'id1': {
                    'status': 'IN_PROGRESS'
                }
            }
        }
    }

    components = ['root', 'id1']
    result = migration_status_util.traverse_local_migration_status(components, migration_status)
    assert result['status'] == 'IN_PROGRESS'


def test_find_named_group_in_dependents_finds_correct_group():
    dependents = [{'foo': {'bar': 1}}, {'baz': {'qux': 2}}]
    result = migration_status_util.find_named_group_in_dependents(dependents, 'baz')
    assert result == {'qux': 2}


def test_extract_template_components_parses_correctly():
    templated = '{{root.id1}}'
    result = migration_status_util.extract_template_components(templated)
    assert result == ['root', 'id1']

    bad = 'notemplate'
    assert migration_status_util.extract_template_components(bad) is None


def test_local_migration_status_operation_invalid_op():
    with pytest.raises(ValueError):
        migration_status_util.local_migration_status_operation('invalid', '{{root.id1}}', {}, {})
