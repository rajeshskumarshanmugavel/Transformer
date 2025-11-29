import os

from dotenv import load_dotenv

load_dotenv()

CONFIG = {
    'EXEC_ENVIRONMENT': os.environ.get('EXEC_ENVIRONMENT', 'local'),
    'AZURE_CONNECTION_STRING': os.environ.get('AZURE_CONNECTION_STRING', ''),
    'AZURE_STORAGE_CONTAINER_NAME': os.environ.get('AZURE_STORAGE_CONTAINER_NAME', ''),
    'TRANSFORMATION_JSON_FILE_PATH': os.environ.get('TRANSFORMATION_JSON_FILE_PATH', ''),
    'MIGRATIONS_API_DOMAIN': os.environ.get('MIGRATIONS_API_DOMAIN', ''),
    'MIGRATIONS_API_USERNAME': os.environ.get('MIGRATIONS_API_USERNAME', ''),
    'MIGRATIONS_API_PASSWORD': os.environ.get('MIGRATIONS_API_PASSWORD', ''),
    'MAX_PARALLEL_WORKERS': int(os.environ.get('MAX_PARALLEL_WORKERS', 10)),
    'PARALLEL_PROCESS_ROOT_ENABLED': os.environ.get('PARALLEL_PROCESS_ROOT_ENABLED', 'true').lower() == 'true',
    'PARALLEL_PROCESS_CHILDREN_ENABLED': os.environ.get('PARALLEL_PROCESS_CHILDREN_ENABLED', 'true').lower() == 'true',
    'NO_OF_RECORDS_FOR_STATUS_UPDATE': int(os.environ.get('NO_OF_RECORDS_FOR_STATUS_UPDATE', 25)),
    'REDIS_HOST': os.environ.get('REDIS_HOST', 'localhost'),
    'REDIS_PORT': int(os.environ.get('REDIS_PORT', 6379)),
    'REDIS_PASSWORD': os.environ.get('REDIS_PASSWORD', ''),
}
