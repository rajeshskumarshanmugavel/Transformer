from datetime import datetime
import json
import time
import re
from src.azure.storage import StorageClient
from src.utils.config import CONFIG

def get_date_components(date_str):
    date_obj = datetime.fromisoformat(date_str)

    current_year = date_obj.year
    current_month = f'{date_obj.month:02d}'
    current_date = f'{date_obj.day:02d}'

    return (current_year, current_month, current_date)

def get_migration_storage_path(migration_id, migration_configs):
    (year, month, date) = get_date_components(migration_configs['createdAt'])

    return f'{year}/{month}/{date}/{migration_id}'

def get_current_ts():
    return int(time.time())

def create_migration_status_in_cloud(logger, migration_id, path):
    storage = StorageClient(logger=logger, migration_id=migration_id)

    current_time = get_current_ts()

    initial_status = {
        'started_at': current_time,
        'updated_at': current_time,
        'entity': {},
        'status': 'IN_PROGRESS'
    }
    storage.store_data(path, json.dumps(initial_status))
    return initial_status

# Updating migration status involves two tasks
    # 1. Updating the status json of records
    # 2. Updating the log file to azure
def update_migration_status_to_cloud(logger, migration_id, migration_status, migration_configs):
    migration_status['updated_at'] = get_current_ts()
    migration_storage_path = get_migration_storage_path(migration_id, migration_configs)

    migration_status_path = f'{migration_storage_path}/migration_status.json'

    storage = StorageClient(logger=logger, migration_id=migration_id)

    storage.store_data(migration_status_path, json.dumps(migration_status))

    if CONFIG['EXEC_ENVIRONMENT'] == 'local':
        with open(f'logs/migration_status_{migration_id}.json', 'w') as f:
            json.dump(migration_status, f, indent=2)


def get_migration_status_from_cloud(logger, migration_id, migration_configs):
    migration_storage_path = get_migration_storage_path(migration_id, migration_configs)

    storage = StorageClient(logger=logger, migration_id=migration_id)

    migration_status_path = f'{migration_storage_path}/migration_status.json'

    raw_status = storage.get_data(migration_status_path)

    migration_status = json.loads(raw_status) if raw_status != None else create_migration_status_in_cloud(logger, migration_id, migration_status_path)

    return migration_status

def update_counts(migration_status, components, status, from_status=None):
    def adjust(scope, from_status, to_status):
        if from_status == 'failed' and to_status == 'passed':
            scope['failed'] -= 1
            scope['passed'] += 1
        elif from_status != 'failed':
            scope[to_status] += 1

    ext_to_int_status = {
        'SUCCESS': 'passed',
        'FAILED': 'failed'
    }
    to_internal_status = ext_to_int_status.get(status)
    from_internal_status = ext_to_int_status.get(from_status) if from_status else None

    if 'count_status' not in migration_status:
        migration_status['count_status'] = {
            "passed_records": 0,
            "failed_records": 0
        }

    scope_global = {
        'passed': migration_status['count_status']['passed_records'],
        'failed': migration_status['count_status']['failed_records']
    }
    adjust(scope_global, from_internal_status, to_internal_status)
    migration_status['count_status']['passed_records'] = scope_global['passed']
    migration_status['count_status']['failed_records'] = scope_global['failed']

    if len(components) == 2:
        entity = components[-2]
        if entity not in migration_status['count_status']:
            migration_status['count_status'][entity] = {
                "passed": 0,
                "failed": 0
            }

        adjust(
            migration_status['count_status'][entity],
            from_internal_status,
            to_internal_status
        )

    return migration_status

def update_local_migration_status(components, migration_status, current_context_status, from_status=None):
    migration_status['updated_at'] = get_current_ts()
    if 'entity' not in migration_status:
        migration_status['entity'] = {}

    entity = migration_status['entity']
    current = entity

    first_key = components[0]
    if first_key not in current:
        current[first_key] = {}
    current = current[first_key]

    if len(components) == 2:
        id_key = components[1]
        current[id_key] = current_context_status
        return update_counts(migration_status, components, current_context_status['status'], from_status)

    id_key = components[1]
    current = current[id_key]

    for idx in range(2, len(components) - 2, 2):
        entity_type = components[idx]
        entity_id = components[idx + 1]
        # Ensure current has a dependents list
        if 'dependents' not in current or current['dependents'] is None:
            current['dependents'] = []

        next_level = find_named_group_in_dependents(current['dependents'], entity_type)
        if next_level is None:
            # Create the group for this entity_type since it does not exist yet
            new_group = {entity_type: {}}
            current['dependents'].append(new_group)
            next_level = new_group[entity_type]

        # If the specific entity id doesn't exist yet, create a placeholder so that deeper nesting works
        if entity_id not in next_level:
            next_level[entity_id] = {
                'status': 'IN_PROGRESS',
                'dependents': []
            }

        current = next_level[entity_id]

    final_entity_type = components[-2]
    final_entity_id = components[-1]

    final_group = find_named_group_in_dependents(current['dependents'], final_entity_type)
    if final_group is None:
        new_group = {final_entity_type: {}}
        current['dependents'].append(new_group)
        final_group = new_group[final_entity_type]

    final_group[final_entity_id] = current_context_status

    return update_counts(migration_status, components, current_context_status['status'], from_status)

def traverse_local_migration_status(components, migration_status):
    current = migration_status.get('entity', {})
    
    current = current.get(components[0])
    if current is None:
        return {}

    current = current.get(components[1])
    if current is None:
        return {}

    for idx in range(2, len(components)):
        key = components[idx]
        if idx % 2 == 0:
            current = find_named_group_in_dependents(current['dependents'], key)
            if current is None:
                return {}
        else:
            # ID
            if key not in current:
                return {}
            current = current[key]

    return current

def find_named_group_in_dependents(dependents, key):
    target_dependent = next((group for group in dependents if key in group), None)
    return target_dependent[key] if target_dependent else None

def extract_template_components(templatized_str):
        template_regex = r'^\{\{\s*(.+?)\s*\}\}$'
        pattern = re.compile(template_regex)

        match = pattern.match(templatized_str)
        if not match:
            return None

        path = match.group(1)
        components = path.split('.')
        return components

def local_migration_status_operation(op, templatized_entity, migration_status, current_context_status, from_status=None):
    components = extract_template_components(templatized_entity)

    if op == 'update':
        return update_local_migration_status(components, migration_status, current_context_status, from_status)
    elif op == 'get':
        return traverse_local_migration_status(components, migration_status)
    else:
        raise ValueError(f'Unsupported operation: {op}')
