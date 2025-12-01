import json
import re
import copy
import threading
import logging
import time

from http import HTTPStatus
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback

import sys
sys.setrecursionlimit(3000)

# Targets
from src.connector.freshdeskv2 import create_freshdesk_ticket, create_freshdesk_conversation
from src.connector.freshservicev2 import create_freshservice_ticket, create_freshservice_conversation , create_freshservice_changes , create_freshservice_ticket_tasks, create_freshservice_ticket_tasks, create_freshservice_change_notes, create_freshservice_change_tasks, create_freshservice_service_requests, create_freshservice_child_service_requests
from src.connector.target_jsm import create_jsm_incident, create_jsm_note, create_jsm_comment, create_jsm_request
from src.connector.target_csv import create_csv_ticket, create_csv_conversation    
from src.connector.intercom import create_intercom_conversation, create_intercom_message, create_intercom_ticket

# Sources
from src.connector.zendesk import get_zendesk_tickets_v1, get_zendesk_conversation_v1
from src.connector.servicenowv2 import get_servicenow_incidents_v1, get_servicenow_service_requests_v1, get_servicenow_changes_v1, get_servicenow_notes_v1, get_servicenow_incident_tasks_v1, get_servicenow_change_tasks_v1, get_servicenow_incident_tasks_unfiltered_v1, get_servicenow_all_notes_v1, get_servicenow_ctask_from_ritm_v1, get_servicenow_ritm_from_req_v1
from src.connector.source_csv import get_csv_tickets_v1, get_csv_changes_v1, get_csv_conversations_v1
from src.connector.source_freshdeskv2 import get_freshdesk_tickets, get_freshdesk_conversations
from src.connector.source_freshdesk_jsonv2 import get_freshdesk_json_tickets_v2, get_freshdesk_json_conversations_v2
from src.connector.source_freshservice import get_freshservice_tickets , get_freshservice_tasks , get_freshservice_changes , get_freshservice_contact_details , get_freshservice_conversations
from src.connector.source_csv import get_csv_tickets_v1, get_csv_changes_v1, get_csv_conversations_v1
from src.connector.source_csvjson import get_csvjson_tickets, get_csvjson_conversations
from src.connector.solarwinds import get_solarwinds_incidents_v1, get_solarwinds_tasks_v1, get_solarwinds_notes_v1, get_solarwinds_changes_v1, get_solarwinds_change_by_id_v1

# Misc
from src.utils.helpers import canonical_str, type_cast
from src.utils.migration_status import extract_template_components, get_migration_status_from_cloud, local_migration_status_operation, update_migration_status_to_cloud
from src.utils.config import CONFIG
from src.utils.data_service import update_migration_details

record_statuses = {
    'IN_PROGRESS': 'IN_PROGRESS',
    'SUCCESS': 'SUCCESS',
    'FAILED': 'FAILED'
}

# Test method. Will be replaced by actual handler from package
def handler(module, method_name, **args): # pragma: no cover
    # if method_name == 'get_csv_tickets_v1':
    #     return get_csv_tickets_v1(**args)
    # if method_name == 'get_csv_conversations_v1':
    #     return get_csv_conversations_v1(**args)
    
    if method_name == 'get_csv_tickets_v1':
        if any(
            (qp.get("key") == "jsonfilePath" and str(qp.get("value", "")).strip())
            for qp in args.get("queryParams", [])
        ):
            return get_csvjson_tickets(**args)
        else:
            return get_csv_tickets_v1(**args)
    if method_name == 'get_csv_conversations_v1':
        if any(
            (qp.get("key") == "jsonfilePath" and str(qp.get("value", "")).strip())
            for qp in args.get("queryParams", [])
        ):
            return get_csvjson_conversations(**args)
        else:
            return get_csv_conversations_v1(**args)
      
    if method_name == 'get_csv_changes_v1':
        return get_csv_changes_v1(**args)
    if method_name == 'get_csv_changes_v1':
        return get_csv_changes_v1(**args)

    if method_name == 'get_zendesk_tickets_v1':
        return get_zendesk_tickets_v1(**args)
    if method_name == 'get_zendesk_conversation_v1':
        return get_zendesk_conversation_v1(**args)
    
    if method_name == 'get_freshdesk_tickets_v1':
        return get_freshdesk_tickets(**args)
    if method_name == 'get_freshdesk_conversations_v1':
        return get_freshdesk_conversations(**args)
    
    if method_name == 'get_freshservice_tickets':
        return get_freshservice_tickets(**args)
    if method_name == 'get_freshservice_conversations':
        return get_freshservice_conversations(**args)  
      
    if method_name == 'get_freshdesk_json_tickets_v2':
        return get_freshdesk_json_tickets_v2(**args)
    if method_name == 'get_freshdesk_json_conversations_v2':
        return get_freshdesk_json_conversations_v2(**args)
    
    if method_name == 'get_servicenow_incidents_v1':
        return get_servicenow_incidents_v1(**args)
    if method_name == 'get_servicenow_service_requests_v1':
        return get_servicenow_service_requests_v1(**args)
    if method_name == 'get_servicenow_changes_v1':
        return get_servicenow_changes_v1(**args)
    if method_name == 'get_servicenow_notes_v1':
        return get_servicenow_notes_v1(**args)
    if method_name == 'get_servicenow_incident_tasks_v1':
        return get_servicenow_incident_tasks_v1(**args)
    if method_name == 'get_servicenow_incident_tasks_unfiltered_v1':
        return get_servicenow_incident_tasks_unfiltered_v1(**args)
    if method_name == 'get_servicenow_change_tasks_v1':
        return get_servicenow_change_tasks_v1(**args)
    if method_name == 'get_servicenow_ctask_from_ritm_v1':
        return get_servicenow_ctask_from_ritm_v1(**args)
    if method_name == 'get_servicenow_all_notes_v1':
        return get_servicenow_all_notes_v1(**args)
    if method_name == 'get_servicenow_ritm_from_req_v1':
        return get_servicenow_ritm_from_req_v1(**args)
    
    # SolarWinds handler mappings
    if method_name == 'get_solarwinds_incidents_v1':
        return get_solarwinds_incidents_v1(**args)
    if method_name == 'get_solarwinds_tasks_v1':
        return get_solarwinds_tasks_v1(**args)
    if method_name == 'get_solarwinds_notes_v1':
        return get_solarwinds_notes_v1(**args)
    if method_name == 'get_solarwinds_changes_v1':
        return get_solarwinds_changes_v1(**args)
    if method_name == 'get_solarwinds_change_by_id_v1':
        return get_solarwinds_change_by_id_v1(**args)

    if method_name == 'create_freshdesk_ticket':
        return create_freshdesk_ticket(**args)
    if method_name == 'create_freshdesk_conversation':
        return create_freshdesk_conversation(**args)
    

    if method_name == 'create_jsm_request':
        return create_jsm_request(**args)
    if method_name == 'create_jsm_comment':
        return create_jsm_comment(**args)
    if method_name == 'create_intercom_conversation':
       return create_intercom_conversation(**args)
    if method_name == 'create_intercom_message':
       return create_intercom_message(**args)
    if method_name == 'create_intercom_ticket':
       return create_intercom_ticket(**args)
    if method_name == 'create_csv_ticket':
        return create_csv_ticket(**args)
    if method_name == 'create_csv_conversation':
        return create_csv_conversation(**args)
    if method_name == 'get_freshservice_tickets':
        return get_freshservice_tickets(**args)
    if method_name == 'get_freshservice_conversations':
        return get_freshservice_conversations(**args)
    if method_name == 'get_freshservice_tasks':
        return get_freshservice_tasks(**args)
    if method_name == 'get_freshservice_changes':
        return get_freshservice_changes(**args) 
    if method_name == 'create_freshservice_ticket':
        return create_freshservice_ticket(**args)
    if method_name == 'create_freshservice_conversation':
        return create_freshservice_conversation(**args)
    if method_name == 'create_freshservice_ticket_tasks':
        return create_freshservice_ticket_tasks(**args)
    if method_name == 'create_freshservice_changes':
        return create_freshservice_changes(**args)
    if method_name == 'create_freshservice_change_tasks':
        return create_freshservice_change_tasks(**args)
    if method_name == 'create_freshservice_change_notes':
        return create_freshservice_change_notes(**args)
    if method_name == 'create_freshservice_service_request':
        return create_freshservice_service_requests(**args)
    if method_name == 'create_freshservice_child_service_requests':
        return create_freshservice_child_service_requests(**args)

class Transformer:
    def __init__(self, logger, migration_id, source_name, target_name, configs, redis_client):
        self.logger = logger
        self.migration_id = migration_id
        self.redis_client = redis_client
        self.source_name = source_name
        self.target_name = target_name
        self.configs = configs
        self.total_no_records = 0
        self.current_count_of_processed_records = 0
        self.migration_status = None
        self._migration_status_lock = threading.Lock()
        self.__stop_updater = False

    def __detemplatize(self, data, context):
        components = extract_template_components(str(data['value']))
        
        if components is None:
            return data

        resolved = context
        original_template = str(data['value'])
        
        for component in components:
            if isinstance(resolved, dict) and component in resolved:
                resolved = resolved[component]
            else:
                # If template can't be resolved, log it and return the original template
                # This helps identify when templates aren't being resolved properly
                if original_template.startswith('{{') and original_template.endswith('}}'):
                    self.logger.warning(
                        f"Template '{original_template}' could not be resolved. Context available: {list(context.keys()) if isinstance(context, dict) else type(context)}",
                        extra={'migration_id': self.migration_id}
                    )
                resolved = str(data['value'])
                break

        data['value'] = resolved
        return data


    def __get_executor_props(self, executor, context):
        method_name = executor.get('methodName', '')
        query_params = [self.__detemplatize(qp, context) for qp in executor['queryParams']] if 'queryParams' in executor else []
        headers = executor.get('headers', [])
        auth_type = executor.get('authType', '')
        method_params = executor.get('methodParams', [])

        return method_name, query_params, headers, auth_type, method_params
    
    def __build_request_body(self, source_data, field_mappings):
        request_body = {}
        default_values = {}
        
        # Preserve attachments and planning_fields if present (not in fieldMappings but needed for upload)
        if 'attachments' in source_data:
            request_body['attachments'] = source_data['attachments']
            logging.info(f"[TRANSFORM] Preserved {len(source_data['attachments'])} attachments from source data")
        
        if 'planning_fields' in source_data:
            request_body['planning_fields'] = source_data['planning_fields']
            logging.info(f"[TRANSFORM] Preserved planning_fields from source data")

        def get_source_value(source_data, source_key, mapping):
            if mapping.get('isCustomField'):
                custom_field_key = mapping.get('customFieldKey')
                if custom_field_key and custom_field_key in source_data:
                    value = source_data.get(custom_field_key, {}).get(source_key)
                    if value is not None:
                        return value
                if source_key and source_key in source_data and source_data.get(source_key) is not None and source_data.get(source_key) != '' and source_data.get(source_key) != []:
                    return source_data.get(source_key)
                return None
            
            # Handle nested field access using dot notation 
            if source_key and '.' in source_key:
                keys = source_key.split('.')
                value = source_data
                for key in keys:
                    if isinstance(value, dict) and key in value:
                        value = value[key]
                    else:
                        return None
                return value
            
            return source_data.get(source_key)

        def map_value_with_override(source_value, mapping):
            source_str = canonical_str(source_value)
            for override in mapping.get('override', []):
                if canonical_str(override['sourcevalue']) == source_str:
                    return override['targetvalue']
                
            if mapping.get('default') is not None and mapping.get('default') != '' and mapping.get('default') != 0:
                return mapping.get('default')        
            return source_value

        def apply_default_if_empty(value, mapping):
            """Apply default value if source value is None, empty string, or whitespace only"""
            if (value is None or (isinstance(value, str) and value.strip() == '')) and mapping.get('sourcefield'): #and mapping.get('required') == True:
                return mapping.get('default') or mapping.get('value')
            return value

        for mapping in field_mappings:
            if mapping.get('skip'):
                continue

            target_key = mapping['targetfield']
            source_key = mapping.get('sourcefield')
            mapping_type = mapping.get('mappingType')
            value = None
            
            # Capture default value if present
            if mapping.get('default') is not None:
                default_values[target_key] = mapping.get('default')

            if mapping.get('isCustomField'):
                # Custom Field Logic
                if mapping.get('combinedFields'):
                    combined_values = []
                    for field_name in mapping['combinedFields']:
                        field_value = None
                        if mapping.get('customFieldKey') and mapping['customFieldKey'] in source_data:
                            field_value = source_data.get(mapping['customFieldKey'], {}).get(field_name)
                        if field_value is None:
                            field_value = source_data.get(field_name)
                        if field_value is not None and str(field_value).strip() != "":
                            combined_values.append(f"{field_name}: {field_value}")

                    value = ", ".join(combined_values)  # Join labeled field:value pairs
                else:
                    # Standard custom field value logic
                    if mapping_type == 'static':
                        value = mapping.get('default')
                    elif mapping_type == 'valueMapped' and mapping.get('override'):
                        source_value = get_source_value(source_data, source_key, mapping)
                        value = map_value_with_override(source_value, mapping)
                    else:
                        value = get_source_value(source_data, source_key, mapping)

                if value is None or (isinstance(value, str) and value.strip() == ''):
                    value = apply_default_if_empty(value, mapping)
                
                if (mapping.get("includeValuesFrom") and (mapping.get("includeValuesFrom") == 'tags' or mapping.get("includeValuesFrom") == 'tag')):
                    tagsData = mapping.get('default') or mapping.get('value')
                    if tagsData and isinstance(tagsData, list):
                        value = tagsData + value if isinstance(value, list) else tagsData
                    
                value = type_cast(value, mapping.get('type'))
                custom_key = mapping['customFieldKey']
                request_body.setdefault(custom_key, {})[target_key] = value

            else:
                #Normal Logic
                if mapping.get('combinedFields'):
                    combined_values = []
                    for field_name in mapping['combinedFields']:
                        field_value = None
                        if mapping.get('customFieldKey') and mapping['customFieldKey'] in source_data:
                            field_value = source_data.get(mapping['customFieldKey'], {}).get(field_name)
                        if field_value is None:
                            field_value = source_data.get(field_name)
                        if field_value is not None and str(field_value).strip() != "":
                            combined_values.append(f"{field_name}: {field_value}")

                    value = ", ".join(combined_values)  # Join labeled field:value pairs

                else:
                    if mapping_type == 'static':
                        value = mapping.get('default')
                    elif mapping_type == 'valueMapped' and mapping.get('override'):
                        source_value = get_source_value(source_data, source_key, mapping)
                        value = map_value_with_override(source_value, mapping)
                    else:
                        value = get_source_value(source_data, source_key, mapping)   

                if value is None or (isinstance(value, str) and value.strip() == ''):
                    value = apply_default_if_empty(value, mapping)

                if (mapping.get("includeValuesFrom") and (mapping.get("includeValuesFrom") == 'tags' or mapping.get("includeValuesFrom") == 'tag')):
                    tagsData = mapping.get('default') or mapping.get('value')
                    if tagsData and isinstance(tagsData, list):
                        value = tagsData + value if isinstance(value, list) else tagsData

                value = type_cast(value, mapping.get('type'))
                request_body[target_key] = value
        return request_body, default_values


    def __pick_method_args(self, module, method_name, method_params, available_args, source_config=None):
        base_args = {
            'module': module,
            'method_name': method_name,
            'sourceHeaders': self.configs['common']['source']['headers'],
            'targetHeaders': self.configs['common']['target']['headers'],
        }
        if source_config:
            base_identifier = source_config.get('uniqueIdentifier', {})
    
            # Check if any queryParam has a template value (starts and ends with {{ }})
            query_params = source_config.get('sourceExecutor').get('queryParams', [])
            template_param = next(
                (qp.get('value') for qp in query_params 
                if isinstance(qp.get('value'), str) and 
                qp.get('value').strip().startswith('{{') and 
                qp.get('value').strip().endswith('}}')
                ),
                None
            )
    
            if template_param:
                # Extract the last value before }} from template
                # Remove {{ and }} and split by dots
                template_value = template_param.strip()[2:-2].strip()
                template_parts = template_value.split('.')
                last_template_value = template_parts[-1] if template_parts else ''
            
                # Combine source name with last template value
                source_name = source_config.get('name', '')
                base_args['sourceConfigUniqueIdentifier'] = f"{{{{{source_name}.{last_template_value}}}}}"
            else:
                # Use the base identifier as-is
                base_args['sourceConfigUniqueIdentifier'] = base_identifier

        dynamic_args = {k: available_args[k] for k in method_params if k in available_args}

        final_args = {**base_args, **dynamic_args}
        
        # Always include defaultValues if present, even if not in method_params
        if 'defaultValues' in available_args:
            final_args['defaultValues'] = available_args['defaultValues']

        return final_args


    def __request_handler(self, **args):
        self.logger.info(f'Executing method of module `{args["module"]}` with name `{args["method_name"]}`',
                    extra={'migration_id': self.migration_id})
        return handler(**args)
    
    def __find_source_path(self, source, sourceKey):
        def dfs(node, path):
            if node.get('name') == sourceKey:
                return path
            for idx, child in enumerate(node.get('dependentSources', [])):
                result = dfs(child, path + [idx])
                if result is not None:
                    return result

        return dfs(source, [])

    
    def __get_target_by_path(self, target, path):
        node = target
        for idx in path:
            dependents = node.get('dependentTargets', [])
            node = dependents[idx]
        return node


    def __get_target_config(self, sourceKey):
        source = self.configs['source']
        target = self.configs['target']
        path = self.__find_source_path(source, sourceKey)
        # Assuming the path is always valid and leads to a target
        return self.__get_target_by_path(target, path)

    def __construct_status_update_template_key(self, root_source, current_source_context, current_entity):
        # Step 1: find the path to the entity
        path = self.__find_source_path(root_source, current_entity)
        if path is None:
            raise ValueError(f"Entity {current_entity} not found in source tree")

        # Step 2: traverse along the path and build the template
        result_template = "{{"
        node = root_source
        for depth, idx in enumerate(path + [None]):  # +[None] to finalize at entity
            # always add the source name
            if result_template != "{{":
                result_template += "."
            result_template += node["name"]

            # after the name, add the resolved uniqueIdentifier
            current_source_id = (
                self.__detemplatize(
                    {"value": node["uniqueIdentifier"]}, current_source_context
                )
            )["value"]
            result_template += f".{current_source_id}"

            # descend further if not at the end
            if idx is not None:
                node = node["dependentSources"][idx]

        result_template += "}}"
        return result_template

    def __log_request_result(self, entity, source_id, target_id, result, method_name, headers, target_request_body={}):
        is_success = result['status_code'] >= HTTPStatus.OK and result['status_code'] < HTTPStatus.BAD_REQUEST
        msg = f'Successfully processed request with method name : {method_name}'if is_success else f'Error processing request with method name : {method_name}'
        log_method = self.logger.info if is_success else self.logger.error
        extra_log_args = {
            'migration_id': self.migration_id,
            'entity': entity,
            'source_id': str(source_id) if isinstance(source_id, int) or (isinstance(source_id, str) and not source_id.startswith('{{')) else '',
            'target_id': str(target_id) if isinstance(target_id, int) or (isinstance(target_id, str) and not target_id.startswith('{{')) else ''
        }

        if not is_success:
            extra_log_args['error_source'] = 'API'
            extra_log_args['context'] = {
                'headers': json.dumps(headers),
                'request_body': json.dumps(target_request_body),
                'response': json.dumps(result)
            }

        log_method(msg, extra={**extra_log_args})

    def __get_current_migration_status(self):
        with self._migration_status_lock:
            return self.migration_status

    def __update_current_migration_status(self, new_status):
        with self._migration_status_lock:
            self.migration_status = new_status

    def __modify_current_migration_status(self, modify_fn):
        with self._migration_status_lock:
            self.migration_status = modify_fn(self.migration_status)

    def __set_migration_status_field(self, key, value):
        def updater(status):
            status[key] = value
            return status
        self.__modify_current_migration_status(updater)


    def __buildTargetContext(self, migrated_record, uniqueIdentifier):
        template_regex = r'^\{\{\s*(.+?)\s*\}\}$'
        pattern = re.compile(template_regex)

        match = pattern.match(uniqueIdentifier)

        path = match.group(1)
        components = path.split('.')

        components = components[1:]

        result = current = {}

        for component in components[:-1]:
            current[component] = {}
            current = current[component]

        final_key = components[-1]
        current[final_key] = str(migrated_record['target_id'])  # Ensure value is a string

        return result

    def __process_target_chain(self, is_root_level, source_config, target_config, source_data, record_specific_context_params):
        field_mappings = target_config['fieldMappings']
        record_process_start_time = int(datetime.now().timestamp())
        entity = source_config['name']

        executor_configs_copy = copy.deepcopy(target_config['targetExecutor'])
        method_name, query_params, headers, auth_type, method_params = self.__get_executor_props(
            executor_configs_copy, record_specific_context_params['current_target_context']
        )

        target_request_body, default_values = self.__build_request_body(source_data, field_mappings)

        available_args = {
            'queryParams': query_params,
            'headers': headers,
            'authType': auth_type,
            'body': target_request_body,
            'defaultValues': default_values,
            'numberOfProcessedRecords': self.current_count_of_processed_records
        }

        args_to_pick = self.__pick_method_args(self.target_name, method_name, method_params, available_args)

        source_id = (self.__detemplatize({'value': source_config['uniqueIdentifier']}, record_specific_context_params['current_source_context']))['value']
        target_id = None

        result = self.__request_handler(**args_to_pick)

        record_process_end_time = int(datetime.now().timestamp())

        is_success = result['status_code'] >= HTTPStatus.OK and result['status_code'] < HTTPStatus.BAD_REQUEST
        
        final_result = result.get('body') if (result and is_success and isinstance(result, dict)) else None

        if final_result:
            record_specific_context_params['current_target_context'][target_config['name']] = final_result
            target_id = (self.__detemplatize({'value': target_config['uniqueIdentifier']}, record_specific_context_params['current_target_context']))['value']

        status = record_statuses['SUCCESS'] if final_result else record_statuses['FAILED']

        self.__log_request_result(
            entity, source_id, target_id,
            result, method_name, headers,
            target_request_body
        )

        record_specific_context_params['current_context_status'] = {
            'source_id': str(source_id) if source_id is not None else source_id,
            'target_id': str(target_id) if target_id is not None else target_id,
            'start_time': record_process_start_time,
            'end_time': record_process_end_time,
            'status': status,
            'dependents': [],
        }

        return final_result, record_specific_context_params


    def __process_dependent_sources(self, source_config, record_specific_context_params):
        for dep in source_config.get('dependentSources', []):
            self.__process_source_chain(dep, self.__safe_deepcopy_context_params(record_specific_context_params), is_root_level=False)

        # Finalize closed Intercom tickets after all dependent sources processed
        if self.target_name == 'intercom' and source_config.get('name') == 'tickets':
            try:
                ticket_context = record_specific_context_params.get('current_target_context', {}).get('tickets', {})
                ticket_id = ticket_context.get('ticket_id') or ticket_context.get('id')

                source_context = record_specific_context_params.get('current_source_context', {})
                source_data = source_context.get('tickets', {})
                ticket_state = source_data.get('ticket_state') or source_data.get('status')

                # Handle both integer (Freshdesk: 4, 5) and string (Zendesk: "closed", "solved") values
                should_close = False
                if ticket_id and ticket_state:
                    if isinstance(ticket_state, int) and ticket_state in [4, 5]:
                        should_close = True
                        self.logger.info(f'Ticket {ticket_id} should close (integer status: {ticket_state})', extra={'migration_id': self.migration_id})
                    elif isinstance(ticket_state, str) and ticket_state.lower() in ['closed', 'solved', 'resolved', 'complete', 'done', 'finished']:
                        should_close = True
                        self.logger.info(f'Ticket {ticket_id} should close (string status: {ticket_state})', extra={'migration_id': self.migration_id})
                    elif isinstance(ticket_state, str) and ticket_state.isdigit() and int(ticket_state) in [4, 5]:
                        should_close = True
                        self.logger.info(f'Ticket {ticket_id} should close (numeric string status: {ticket_state})', extra={'migration_id': self.migration_id})

                if should_close:
                    from src.connector.intercom import finalize_closed_ticket_universal
                    headers = self.configs['common']['target']['headers']

                    # Try to get admin_id from ticket context or source data
                    admin_id = (ticket_context.get('admin_assignee_id') or
                               ticket_context.get('admin_id') or
                               source_data.get('admin_assignee_id') or
                               source_data.get('admin_id'))

                    finalize_body = {"conversation_id": str(ticket_id)}
                    if admin_id:
                        finalize_body["admin_id"] = str(admin_id)

                    result = finalize_closed_ticket_universal(finalize_body, headers)

                    if result.get('status_code') == 200:
                        self.logger.info(
                            f'Finalized closed ticket {ticket_id}',
                            extra={'migration_id': self.migration_id}
                        )
                    else:
                        self.logger.warning(
                            f'Failed to finalize ticket {ticket_id}: {result.get("body")}',
                            extra={'migration_id': self.migration_id}
                        )
            except Exception as e:
                self.logger.error(
                    f'Error finalizing closed ticket: {e}',
                    extra={'migration_id': self.migration_id}
                )

    def __process_single_source_record(self, is_root_level, source_config, target_config, source_data, record_specific_context_params=None):
        thread_id = threading.current_thread().ident
        thread_name = threading.current_thread().name
        self.logger.info(
            f'[THREAD] Starting task in thread with id : {thread_id} and name : {thread_name}'
        )

        record_specific_context_params['current_source_context'] = {
            **record_specific_context_params.get('current_source_context', {}),
            source_config['name']: source_data
        }

        record_specific_context_params['current_context_template_key'] = self.__construct_status_update_template_key(
            self.configs['source'],
            record_specific_context_params['current_source_context'],
            source_config['name']
        )

        migrated_record = local_migration_status_operation(
            'get',
            record_specific_context_params['current_context_template_key'],
            self.__get_current_migration_status(),
            None
        )
        # TEMPORARILY COMMENTED FOR TESTING - Re-process changes with attachments
        # if migrated_record.get('status', '') == record_statuses['SUCCESS']:
        #     # Dom - Extract ticket info for logging
        #     source_id = source_data.get('id', 'unknown')
        #     source_subject = source_data.get('subject', 'unknown')
        #     
        #     self.logger.info(
        #         f'[SKIP] Resource belonging to `{source_config["name"]}` already processed successfully - ID: {source_id}, Subject: {source_subject}',
        #         extra={'migration_id': self.migration_id}
        #     )               
        #     
        #     record_specific_context_params['current_target_context'][target_config['name']] = self.__buildTargetContext(
        #         migrated_record, target_config['uniqueIdentifier']
        #     )
        #     return record_specific_context_params['current_target_context'][target_config['name']], record_specific_context_params

        result, record_specific_context_params = self.__process_target_chain(
            is_root_level,
            source_config,
            target_config,
            source_data,
            record_specific_context_params
        )

        self.__update_current_migration_status(
            local_migration_status_operation(
                'update',
                record_specific_context_params['current_context_template_key'],
                self.__get_current_migration_status(),
                self.__safe_deepcopy_context_params(record_specific_context_params['current_context_status']),
                from_status=migrated_record['status'] if migrated_record else None,
            )
        )

        return result, record_specific_context_params

    # Dom - 8/2 - Added a copy method to avoid deep copy issues in multi-threaded environment
    def __get_current_migration_status_copy(self):
        with self._migration_status_lock:
            return copy.deepcopy(self.migration_status)
    
    def __safe_deepcopy_context_params(self, context_params):
        """Safely create a deep copy of context parameters"""
        try:
            return copy.deepcopy(context_params)
        except RuntimeError as e:
            if "dictionary changed size during iteration" in str(e):
                # If we hit the threading issue, create a new copy with a lock
                with self._migration_status_lock:
                    return copy.deepcopy(context_params)
            raise
    
    def __post_result_retrieve_handler(self, result, source_config, record_specific_context_params, is_root_level):
        if is_root_level:
            self.current_count_of_processed_records += 1
            # Dom - 8/2 - Moved status update logic to a separate timer thread
            # if self.current_count_of_processed_records % CONFIG['NO_OF_RECORDS_FOR_STATUS_UPDATE'] == 0:
            #     #update_migration_status_to_cloud(self.logger, self.migration_id, copy.deepcopy(self.__get_current_migration_status()), self.configs)
            #     #Dom - 8/2 - revised deepcopy and commented earlier line
            #     update_migration_status_to_cloud(self.logger, self.migration_id, self.__get_current_migration_status_copy(), self.configs)

        termination_status_key = f'mg_{self.migration_id}_should_terminate'

        is_terminated = self.redis_client.get(termination_status_key) == '1'

        self.logger.info(
            f'[TERMINATION] Checking termination status for migration id : {self.migration_id}. Termination status key : {termination_status_key}, Terminated: {is_terminated}',
            extra={'migration_id': self.migration_id}
        )

        if is_terminated:
            return {'terminated': True}

        if result:
            self.__process_dependent_sources(source_config, record_specific_context_params)
        return

    def __process_source_list_parallel(self, source_config, target_config, source_data_list, record_specific_context_params, is_root_level):
        with ThreadPoolExecutor(max_workers=CONFIG['MAX_PARALLEL_WORKERS']) as executor:
            futures = [
                executor.submit(
                    self.__process_single_source_record,
                    is_root_level,
                    source_config,
                    target_config,
                    source_data,
                    self.__safe_deepcopy_context_params(record_specific_context_params)
                )
                for source_data in source_data_list
            ]
            for future in as_completed(futures):
                result, record_specific_context_params = future.result()
                res = self.__post_result_retrieve_handler(result, source_config, record_specific_context_params, is_root_level)
                if res and res.get('terminated'):
                    return {'terminated': True}


    def __process_source_list_sequential(self, source_config, target_config, source_data_list, record_specific_context_params, is_root_level):
        for source_data in source_data_list:
            result, record_specific_context_params = self.__process_single_source_record(is_root_level, source_config, target_config, source_data, record_specific_context_params)
            res = self.__post_result_retrieve_handler(result, source_config, record_specific_context_params, is_root_level)
            if res and res.get('terminated'):
                return {'terminated': True}

    def __process_source_chain(self, source_config, records_specific_context_params = None, is_root_level=False):
        if records_specific_context_params is None:
            records_specific_context_params = {
                'current_source_context': {},
                'current_target_context': {},
                'current_context_status': None,
                'current_context_template_key': None
            }

        source_executor_config = copy.deepcopy(source_config['sourceExecutor'])


        # Dom - 7/11 - Added this to inject sliding window metadata into query params
        if is_root_level and 'last_ticket_updated_at' in records_specific_context_params:
            last_updated_at = records_specific_context_params['last_ticket_updated_at']
            self.logger.info(f'[SLIDING] Injecting last_ticket_updated_at: {last_updated_at}')
            
            # Add to queryParams if not already present
            query_params = source_executor_config.get('queryParams', [])
            
            # Remove existing updated_since if present
            query_params = [qp for qp in query_params if qp.get('key') != 'updated_since']
            
            # Add the new value
            query_params.append({
                'key': 'updated_since',
                'value': last_updated_at
            })
            
            source_executor_config['queryParams'] = query_params


        method_name, query_params, headers, auth_type, method_params = self.__get_executor_props(source_executor_config, records_specific_context_params['current_source_context'])

        # Get target config to access field mappings (needed for source connectors like SolarWinds)
        target_config = self.configs['target'] if is_root_level else self.__get_target_config(source_config['name'])
        field_mappings = target_config.get('fieldMappings', []) if target_config else []

        available_args = {
            'queryParams': query_params,
            'headers': headers,
            'authType': auth_type,
            'numberOfProcessedRecords': self.current_count_of_processed_records,
            'fieldMappings': field_mappings
        }

        args_to_pick = self.__pick_method_args(self.source_name, method_name, method_params, available_args, source_config)

        result = self.__request_handler(**args_to_pick)

        if result is None or result.get('body') is None or result['body'][source_config['name']] is None:
            return records_specific_context_params

        if is_root_level:
            if result and result.get('body') is not None:
                records_specific_context_params['current_source_context'] = {**result['body']}
            else:
                records_specific_context_params['current_source_context'] = {}

        # # Dom 7/11 - After getting the result
        # if is_root_level and 'meta' in result.get('body', None):
        #     meta = result['body']['meta']
        #     if 'last_ticket_updated_at' in meta:
        #         # Store for next call
        #         records_specific_context_params['last_ticket_updated_at'] = meta['last_ticket_updated_at']
        
        if is_root_level and result.get('body') and isinstance(result['body'], dict):
            meta = result['body'].get('meta')
            if isinstance(meta, dict):
                last_updated = meta.get('last_ticket_updated_at')
                if last_updated:
                    # Store for next call
                    records_specific_context_params['last_ticket_updated_at'] = last_updated

        self.__log_request_result(
            source_config['name'], None, None,
            result, method_name, headers
        )

        source_data_list = result['body'][source_config['name']]

        if is_root_level:
            remaining_records = self.total_no_records - self.current_count_of_processed_records
            # If there are no records to process mark it as completed
            if remaining_records <= 0:
                records_specific_context_params['completed'] = True
                return records_specific_context_params
            source_data_list = source_data_list[:remaining_records]
            # also if there are no records left from API calls to process, mark it as completed
            # Dom/Shravan - 9/17 - Condition to accommodate custom filtering logic from FD  and FS connectors
            has_more_records =  'meta' in result['body'] and 'has_more' in result['body']['meta'] and result['body']['meta']['has_more'] == True
            if len(source_data_list) == 0 and not has_more_records:
                records_specific_context_params['completed'] = True
                return records_specific_context_params
            root_source_processor_method = self.__process_source_list_parallel if CONFIG['PARALLEL_PROCESS_ROOT_ENABLED'] else self.__process_source_list_sequential
            res = root_source_processor_method(source_config, target_config, source_data_list, records_specific_context_params, is_root_level)
            if res and res.get('terminated'):
                records_specific_context_params['terminated'] = True
                return records_specific_context_params
        else:
            child_source_processor_method = self.__process_source_list_parallel if CONFIG['PARALLEL_PROCESS_CHILDREN_ENABLED'] else self.__process_source_list_sequential
            res = child_source_processor_method(source_config, target_config, source_data_list, records_specific_context_params, is_root_level)
            if res and res.get('terminated'):
                records_specific_context_params['terminated'] = True
                return records_specific_context_params
        return records_specific_context_params
    
    def __mark_status(self, status):
        self.__set_migration_status_field('status', status)
        # Stop the long running updater
        self.__stop_updater = True
        update_migration_status_to_cloud(self.logger, self.migration_id, self.__get_current_migration_status_copy(), self.configs)
        update_migration_details(self.logger, self.migration_id, {'status': status})

    def __process_batch(self, start_time, context_params=None):
        context_params = self.__process_source_chain(
            self.configs['source'], context_params, is_root_level=True
        )

        if context_params.get('terminated', False):
            self.logger.info(
                f'[TERMINATION] Migration with id : {self.migration_id} has been terminated by the user.',
                extra={'migration_id': self.migration_id}
            )
            self.__mark_status('TERMINATED')
            return True

        current_ctx = context_params['current_source_context']
        meta = current_ctx.get('meta', {})
        source_reported = meta.get('number_of_processed_records')

        if source_reported is not None:
            self.logger.info(
                f"Overriding processed count from {self.current_count_of_processed_records} "
                f"to {source_reported} based on source report"
            )
            self.current_count_of_processed_records = source_reported

        effective_count = self.current_count_of_processed_records

        # Mark as completed if all records are processed
        if effective_count >= self.total_no_records:
            context_params['completed'] = True

        # If completed, log and update status
        if context_params.get('completed', False):
            end_time = int(datetime.now().timestamp())
            self.logger.info(
                f'Completed migration with id: {self.migration_id}. '
                f'Total records processed: {self.current_count_of_processed_records} '
                f'in {end_time - start_time} seconds. Marking status as `COMPLETED`',
                extra={'migration_id': self.migration_id}
            )
            self.__mark_status('COMPLETED')
            # for additional_source in self.configs.get("additionalSources", []):
            #     self.logger.info(f"[INFO] Processing additional source: {additional_source['name']}")
            #     self.__process_source_chain(additional_source, is_root_level=True)
            return True
        

        # Recurse to process next batch
        return self.__process_batch(start_time, context_params)

    # Dom/Shravan - 9/17 - New method for timed update of the migration status log
    def __status_updater(self):
        while not self.__stop_updater:
            try:
                update_migration_status_to_cloud(
                    self.logger,
                    self.migration_id,
                    self.__get_current_migration_status_copy(),
                    self.configs
                )
            except Exception as e:
                self.logger.warning(
                    f'Failed to update migration status: {e}',
                    extra={'migration_id': self.migration_id}
                )
            TIME_FOR_UPDATE = 1 * 60
            time.sleep(TIME_FOR_UPDATE)
 
    def run(self):
        start_time = int(datetime.now().timestamp())
        try:
            self.migration_status = get_migration_status_from_cloud(self.logger, self.migration_id, self.configs)
            # Dom/Shravan - 9/17 - Start a background thread to update status periodically
            t = threading.Thread(target=self.__status_updater, daemon=True)
            t.start()
            self.total_no_records = self.configs['totalRecords']
            return self.__process_batch(start_time)
        except Exception as ex:
            self.logger.error(
                f'Error during migration with id : {self.migration_id}. Marking status as `TERMINATED`',
                exc_info=True,
                extra={'migration_id': self.migration_id, 'context': {'error_msg': str(ex)}}
            )
            self.__mark_status('TERMINATED')

            self.__stop_updater = True
            return False
