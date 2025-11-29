import json
from datetime import datetime, timezone
from src.utils.data_service import get_migration_details
from src.utils.config import CONFIG

def parser(migration_details):
    result = json.loads(migration_details['transformation_json'])
    exec_env = CONFIG['EXEC_ENVIRONMENT']

    result['createdAt'] = datetime.now(timezone.utc).isoformat() if exec_env == 'local' else migration_details.get('created_at', None)
    return result

def read_migration_config(logger, migration_id):
    try:
        exec_env = CONFIG['EXEC_ENVIRONMENT']

        if exec_env == 'local':
            CONFIG_FILE_NAME = CONFIG['TRANSFORMATION_JSON_FILE_PATH']
            # Add encoding parameter to handle special characters
            with open(f'test/data/migration_configs/{CONFIG_FILE_NAME}', 'r', encoding='utf-8') as f:
                return parser({ 'transformation_json': f.read() })

        migration_details = get_migration_details(logger, migration_id)

        return parser(migration_details)
    except UnicodeDecodeError as ex:
        logger.error(f'Unicode decode error while reading configuration file: {ex}')
        # Try with a different encoding as fallback
        try:
            with open(f'test/data/migration_configs/{CONFIG_FILE_NAME}', 'r', encoding='latin-1') as f:
                return parser({ 'transformation_json': f.read() })
        except Exception as fallback_ex:
            logger.error(f'Fallback encoding also failed: {fallback_ex}')
            return None
    except Exception as ex:
        logger.error('Exception occured while getting the configuration')
        return None
