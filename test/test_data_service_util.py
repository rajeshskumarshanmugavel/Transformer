import pytest
from unittest.mock import Mock, patch
from requests.models import Response

import src.utils.data_service as data_service


@pytest.fixture
def logger():
    return Mock()

def make_response(status_code=200, json_data=None):
    mock_resp = Mock(spec=Response)
    mock_resp.status_code = status_code
    mock_resp.json.return_value = json_data
    return mock_resp


def test_get_migration_details_success(logger):
    expected_data = {'id': '123', 'name': 'test'}
    mock_response = make_response(200, [expected_data])

    with patch('src.utils.data_service.requests.get', return_value=mock_response) as mock_get:
        result = data_service.get_migration_details(logger, '123')

    assert result == expected_data
    mock_get.assert_called_once()

    called_url = mock_get.call_args.args[0]

    assert mock_get.call_args.kwargs['params'] == {'id': '123'}
    assert mock_get.call_args.kwargs['auth'] == ('xxx', 'xxx')
    assert 'https://xxx.azurewebsites.net/public/transformations' == called_url

    logger.info.assert_called_once_with('Successfully retrieved migration details for id - 123')


def test_get_migration_details_no_results(logger):
    mock_response = make_response(200, [])

    with patch('src.utils.data_service.requests.get', return_value=mock_response):
        with pytest.raises(ValueError, match='Migration details not found'):
            data_service.get_migration_details(logger, '999')

    logger.error.assert_called()
    assert 'Error retrieving migration details for id - 999' == logger.error.call_args[0][0]


def test_get_migration_details_http_error(logger):
    error_response = make_response(400)
    error_response.raise_for_status.side_effect = Exception("Bad request")

    with patch('src.utils.data_service.requests.get', return_value=error_response):
        with pytest.raises(Exception, match='Bad request'):
            data_service.get_migration_details(logger, '123')

    logger.error.assert_called()
    assert 'Error retrieving migration details for id - 123' == logger.error.call_args[0][0]


def test_get_migration_details_requests_exception(logger):
    with patch('src.utils.data_service.requests.get', side_effect=Exception("network error")):
        with pytest.raises(Exception, match='network error'):
            data_service.get_migration_details(logger, '123')

    logger.error.assert_called()
    assert 'Error retrieving migration details' in logger.error.call_args[0][0]


def test_update_migration_details_success(logger):
    mock_response = make_response(200)

    with patch('src.utils.data_service.requests.put', return_value=mock_response) as mock_put:
        data_service.update_migration_details(logger, '123', {'status': 'COMPLETED'})


    mock_put.assert_called_once()
 
    assert mock_put.call_args.kwargs['json'] == {'data': {'status': 'COMPLETED'}}
    assert mock_put.call_args.kwargs['auth'] == ('xxx', 'xxx')
    called_url = mock_put.call_args.args[0]
    assert 'https://xxx.azurewebsites.net/public/transformations/123' == called_url

    logger.info.assert_called_once_with('Successfully updated migration details for id - 123')


def test_update_migration_details_http_error(logger):
    error_response = make_response(400)
    error_response.raise_for_status.side_effect = Exception("Bad request")

    with patch('src.utils.data_service.requests.put', return_value=error_response):
        res = data_service.update_migration_details(logger, '123', {'field': 'value'})

    assert res is None
    logger.error.assert_called()
    assert 'Error updating migration details for id - 123' == logger.error.call_args[0][0]


def test_update_migration_details_requests_exception(logger):
    with patch('src.utils.data_service.requests.put', side_effect=Exception("network error")):
       res = data_service.update_migration_details(logger, '123', {'field': 'value'})

    assert res is None
    logger.error.assert_called()
    assert 'Error updating migration details for id - 123' == logger.error.call_args[0][0]
