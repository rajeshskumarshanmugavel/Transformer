from http import HTTPStatus
import requests

from src.utils.config import CONFIG

def get_migration_details(logger, migration_id):
    api_domain = CONFIG['MIGRATIONS_API_DOMAIN']
    username = CONFIG['MIGRATIONS_API_USERNAME']
    password = CONFIG['MIGRATIONS_API_PASSWORD']

    migration_details_url =  f'https://{api_domain}/public/transformations'

    params = {
        'id': migration_id
    }

    try:
        response = requests.get(migration_details_url, params=params, auth=(username, password))

        is_success = response != None and response.status_code >= HTTPStatus.OK and response.status_code < HTTPStatus.BAD_REQUEST
        
        if not is_success:
            response.raise_for_status()

        logger.info(f'Successfully retrieved migration details for id - {migration_id}')

        migration_details = response.json()

        if type(migration_details) == list and len(migration_details) > 0:
            return migration_details[0]

        raise ValueError('Migration details not found')

    except Exception as ex:
        logger.error(f'Error retrieving migration details for id - {migration_id}', {'context': {
            'error_msg': ex
        }})

        raise


def update_migration_details(logger, migration_id, migration_details_to_update):
    api_domain = CONFIG['MIGRATIONS_API_DOMAIN']
    username = CONFIG['MIGRATIONS_API_USERNAME']
    password = CONFIG['MIGRATIONS_API_PASSWORD']

    migration_details_url =  f'https://{api_domain}/public/transformations/{migration_id}'

    try:
        body = {
            'data': {
                **migration_details_to_update
            }
        }

        response = requests.put(migration_details_url, json=body, auth=(username, password))

        is_success = response != None and response.status_code >= HTTPStatus.OK and response.status_code < HTTPStatus.BAD_REQUEST

        if not is_success:
            response.raise_for_status()

        logger.info(f'Successfully updated migration details for id - {migration_id}')

    except Exception as ex:
        logger.error(f'Error updating migration details for id - {migration_id}', {'context': {
            'error_msg': ex
        }})

        return None
