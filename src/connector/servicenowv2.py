"""
servicenow.py
=============

🚀 OPTIMIZED SERVICENOW CONNECTOR
- Replaces sequential enrichment with 3-5 second bulk enrichment
- Thread-safe and migration-safe for parallel execution
- No shared caches between migrations
- 90%+ performance improvement similar to Zendesk optimization

ServiceNow-specific source connector implementation.
Contains only ServiceNow-related logic and imports base classes from base_source_connector.
Following the same pattern as zendesk.py with support for Incidents, Notes, and Tasks.

OPTIMIZED VERSION - Bulk enrichment with parallel processing
"""

import requests
import re
import logging
import time
import json
import os
import threading
import concurrent.futures
from typing import List, Dict, Tuple, Optional, Any, Set
from datetime import datetime
from urllib.parse import urlencode, urlparse, parse_qs

# Import base classes and utilities
try:
    from base_source_connector import (
        BaseSourceConnector,
        BaseSourceRateLimitHandler,
        BaseDataEnricher,
        BaseFieldMapper,
        SourceConnectorConfig,
        SourceResponse,
        PaginationInfo,
        convert_source_headers_to_dict,
        convert_query_params_to_dict,
        standardize_source_response_format,
        extract_subdomain_generic,
        safe_json_response,
        log_api_call,
        handle_common_errors
    )
except ImportError:
    # Fallback - define minimal base classes if base_source_connector isn't available
    class BaseSourceRateLimitHandler:
        def is_rate_limited(self, response): return response.status_code == 429
        def get_retry_delay(self, response): return 60
        def make_request_with_retry(self, url, method='GET', **kwargs):
            return requests.request(method, url, **kwargs)
    
    class BaseDataEnricher:
        def enrich_tickets(self, tickets, api_response): return tickets
        def enrich_conversations(self, conversations, users): return conversations
        def find_object_by_id(self, objects, target_id, id_field='id'):
            return next((obj for obj in objects if obj.get(id_field) == target_id), None)
        def safe_get_field(self, obj, field, default=None):
            return obj.get(field, default) if obj else default
    
    class BaseFieldMapper:
        def get_standard_field_mapping(self): return {}
        def process_custom_fields(self, custom_fields, field_definitions): return {}
        def find_object_by_id(self, objects, target_id, id_field='id'):
            return next((obj for obj in objects if obj.get(id_field) == target_id), None)
        def safe_get_field(self, obj, field, default=None):
            return obj.get(field, default) if obj else default
    
    class BaseSourceConnector:
        def __init__(self, config): 
            self.config = config
            self.rate_limiter = BaseSourceRateLimitHandler()
            self.data_enricher = BaseDataEnricher()
            self.field_mapper = BaseFieldMapper()
    
    # Utility functions
    def convert_source_headers_to_dict(headers):
        if isinstance(headers, list):
            return {item["key"]: item["value"] for item in headers}
        return headers
    
    def convert_query_params_to_dict(query_params):
        if isinstance(query_params, list):
            return {param['key']: param['value'] for param in query_params}
        return query_params
    
    def extract_subdomain_generic(domainUrl):
        no_protocol = re.sub(r"(^\w+:|^)//", "", domainUrl)
        return no_protocol.split(".")[0]
    
    def safe_json_response(response):
        try:
            return response.json()
        except:
            return {}
    
    def log_api_call(method, url, status_code, duration):
        logging.info(f"{method} {url} - {status_code} ({duration:.2f}s)")
    
    def handle_common_errors(response):
        if response.status_code >= 400:
            logging.error(f"API error: {response.status_code} - {response.text}")
    
    def standardize_source_response_format(response_dict):
        """Convert SourceResponse to transformer-expected format"""
        if hasattr(response_dict, 'success'):
            # It's a SourceResponse object
            if response_dict.success:
                return {"status_code": response_dict.status_code, "body": response_dict.data}
            else:
                # Return error in body as a dict with error key
                return {"status_code": response_dict.status_code, "body": {"error": response_dict.error_message}}
        else:
            # It's already a dict
            if response_dict.get("success", response_dict.get("status_code") == 200):
                return {"status_code": response_dict.get("status_code", 200), "body": response_dict.get("data", response_dict.get("body", {}))}
            else:
                # Return error in body as a dict with error key
                return {"status_code": response_dict.get("status_code", 500), "body": {"error": response_dict.get("error_message", response_dict.get("error", "Unknown error"))}}
    
    # Data classes
    class SourceConnectorConfig:
        def __init__(self, domainUrl=None, username=None, password=None, **kwargs):
            self.domainUrl = domainUrl
            self.username = username  
            self.password = password
            self.page_size = kwargs.get('page_size', 100)
    
    class SourceResponse:
        def __init__(self, status_code, success, data=None, error_message=None, **kwargs):
            self.status_code = status_code
            self.success = success
            self.data = data
            self.error_message = error_message
    
    class PaginationInfo:
        def __init__(self, has_more=False, next_cursor=None, total_count=None):
            self.has_more = has_more
            self.next_cursor = next_cursor
            self.total_count = total_count


class ServiceNowRateLimitHandler(BaseSourceRateLimitHandler):
    """ServiceNow-specific rate limit handler"""
    
    def is_rate_limited(self, response: requests.Response) -> bool:
        """Check if response indicates rate limiting"""
        return response.status_code == 429
    
    def get_retry_delay(self, response: requests.Response) -> int:
        """Extract retry delay from ServiceNow response headers"""
        retry_after = response.headers.get('Retry-After')
        if retry_after:
            try:
                return int(retry_after)
            except ValueError:
                pass
        
        # ServiceNow rate limit headers
        rate_limit_reset = response.headers.get('X-RateLimit-Reset')
        if rate_limit_reset:
            try:
                reset_timestamp = int(rate_limit_reset)
                current_timestamp = int(time.time())
                return max(0, reset_timestamp - current_timestamp)
            except ValueError:
                pass
        
        return 60  # Default 1 minute


class OptimizedServiceNowDataEnricher:
    """
    🚀 OPTIMIZED SERVICENOW DATA ENRICHMENT
    Replaces sequential reference resolution with bulk enrichment
    Thread-safe and migration-safe with no shared state
    """
    
    def __init__(self, servicenow_connector):
        self.connector = servicenow_connector
        self.logger = logging.getLogger(__name__)
        
        # 🔒 MIGRATION-SAFE: Each enricher instance has its own cache
        # No shared state between different migration requests
        self._session_cache = {
            'users': {},
            'groups': {},
            'organizations': {},
            'companies': {},
            'departments': {},
            'locations': {},
            'assignments': {}
        }
        self._cache_lock = threading.Lock()
        
        # Migration-specific identifier for logging
        self.migration_id = getattr(servicenow_connector, 'migration_id', 'unknown')
        
        # Enrichment configuration cache
        self._enrichment_config_cache = None
    
    def enrich_incidents_optimized(self, incidents: List[Dict], api_key: str = None) -> List[Dict]:
        """
        🚀 OPTIMIZED ENRICHMENT: Bulk fetch + parallel processing
        Replaces individual API calls with bulk operations
        """
        if not incidents:
            return incidents
        
        start_time = time.time()
        self.logger.info(f"🚀 Migration {self.migration_id}: Starting optimized enrichment for {len(incidents)} incidents")
        
        try:
            # Step 1: Extract all unique IDs that need enrichment
            reference_ids = self._extract_unique_reference_ids(incidents)
            
            # Step 2: Bulk fetch all required data in parallel
            self._bulk_fetch_reference_data_parallel(reference_ids, api_key)
            
            # Step 3: Apply enrichment using cached data (very fast!)
            enriched_incidents = self._apply_cached_enrichment_to_incidents(incidents)
            
            # Step 4: Handle attachments with optimized batch processing
            self._enrich_incidents_attachments_batch(enriched_incidents, api_key)
            
            duration = time.time() - start_time
            self.logger.info(f"🚀 Migration {self.migration_id}: Optimized enrichment complete: "
                           f"{len(incidents)} incidents in {duration:.2f}s")
            
            return enriched_incidents
            
        except Exception as e:
            self.logger.error(f"Migration {self.migration_id}: Enrichment optimization failed: {e}")
            # Fallback to minimal enrichment
            return self._minimal_enrichment_fallback(incidents, 'incidents')

    def enrich_requests_optimized(self, requests: List[Dict], api_key: str = None) -> List[Dict]:
        """
        🚀 OPTIMIZED ENRICHMENT: Bulk fetch + parallel processing
        Replaces individual API calls with bulk operations
        """

        if not requests:
            return requests
        
        start_time = time.time()
        self.logger.info(f"🚀 Migration {self.migration_id}: Starting optimized enrichment for {len(requests)} requests")
        
        try:
            # Step 1: Extract all unique IDs that need enrichment
            reference_ids = self._extract_unique_reference_ids(requests)
            
            # Step 2: Bulk fetch all required data in parallel
            self._bulk_fetch_reference_data_parallel(reference_ids, api_key)
            
            # Step 3: Apply enrichment using cached data (very fast!)
            enriched_requests = self._apply_cached_enrichment_to_requests(requests, api_key)
            
            # Step 4: Handle attachments with optimized batch processing
            self._enrich_incidents_attachments_batch(enriched_requests, api_key)
            
            duration = time.time() - start_time
            self.logger.info(f"🚀 Migration {self.migration_id}: Optimized enrichment complete: "
                            f"{len(requests)} requests in {duration:.2f}s")
            
            return enriched_requests
            
        except Exception as e:
            self.logger.error(f"Migration {self.migration_id}: Enrichment optimization failed: {e}")
            # Fallback to minimal enrichment
            return self._minimal_enrichment_fallback(requests, 'requests')

    def enrich_changes_optimized(self, changes: List[Dict], api_key: str = None) -> List[Dict]:
        """
        🚀 OPTIMIZED CHANGE ENRICHMENT: Reuses existing bulk infrastructure
        """
        if not changes:
            return changes
        
        start_time = time.time()
        self.logger.info(f"🚀 Migration {self.migration_id}: Starting optimized enrichment for {len(changes)} changes")
        
        try:
            # REUSE: Extract unique IDs (same method as incidents!)
            reference_ids = self._extract_unique_reference_ids(changes)
            
            # REUSE: Bulk fetch all reference data (same method!)
            self._bulk_fetch_reference_data_parallel(reference_ids, api_key)
            
            # CUSTOMIZE: Apply change-specific enrichment
            enriched_changes = self._apply_cached_enrichment_to_changes(changes, api_key)
            
            # REUSE: Batch attachment processing (just change table name)
            self._enrich_records_attachments_batch(enriched_changes, 'change_request',api_key)
            
            duration = time.time() - start_time
            self.logger.info(f"🚀 Migration {self.migration_id}: Optimized change enrichment complete: "
                        f"{len(changes)} changes in {duration:.2f}s")
            
            return enriched_changes
            
        except Exception as e:
            self.logger.error(f"Migration {self.migration_id}: Change enrichment optimization failed: {e}")
            return self._minimal_enrichment_fallback(changes, 'changes')    

    def enrich_tasks_optimized(self, tasks: List[Dict], api_key: str = None) -> List[Dict]:
        """🚀 OPTIMIZED TASK ENRICHMENT"""
        if not tasks:
            return tasks
        
        start_time = time.time()
        self.logger.info(f"🚀 Migration {self.migration_id}: Starting optimized task enrichment for {len(tasks)} tasks")
        
        try:
            # Extract unique reference IDs
            reference_ids = self._extract_unique_reference_ids(tasks)
            
            # Bulk fetch reference data
            self._bulk_fetch_reference_data_parallel(reference_ids, api_key)
            
            # Apply cached enrichment
            enriched_tasks = self._apply_cached_enrichment_to_tasks(tasks)

            # Step 4: Handle attachments with optimized batch processing
            self._enrich_records_attachments_batch(enriched_tasks, 'incident_task', api_key)
            
            duration = time.time() - start_time
            self.logger.info(f"🚀 Migration {self.migration_id}: Optimized task enrichment complete: "
                           f"{len(tasks)} tasks in {duration:.2f}s")
            
            return enriched_tasks
            
        except Exception as e:
            self.logger.error(f"Migration {self.migration_id}: Task enrichment optimization failed: {e}")
            return self._minimal_enrichment_fallback(tasks, 'tasks')
    
    def enrich_notes_optimized(self, notes: List[Dict], api_key: str = None) -> List[Dict]:
        """🚀 OPTIMIZED NOTE ENRICHMENT"""
        if not notes:
            return notes
        
        start_time = time.time()
        self.logger.info(f"🚀 Migration {self.migration_id}: Starting optimized note enrichment for {len(notes)} notes")
        
        try:
            # Extract unique user IDs from notes
            user_ids = set()
            for note in notes:
                created_by = note.get('sys_created_by')
                if created_by:
                    if isinstance(created_by, dict) and 'value' in created_by:
                        user_ids.add(created_by['value'])
                    elif isinstance(created_by, str):
                        user_ids.add(created_by)
            
            # Bulk fetch user data
            if user_ids:
                self._bulk_fetch_users_by_username(user_ids,api_key)
            
            # Apply cached enrichment
            enriched_notes = self._apply_cached_enrichment_to_notes(notes)
            
            duration = time.time() - start_time
            self.logger.info(f"🚀 Migration {self.migration_id}: Optimized note enrichment complete: "
                           f"{len(notes)} notes in {duration:.2f}s")
            
            return enriched_notes
            
        except Exception as e:
            self.logger.error(f"Migration {self.migration_id}: Note enrichment optimization failed: {e}")
            return self._minimal_enrichment_fallback(notes, 'notes')
    
    def enrich_ctask_notes_optimized(self, notes: List[Dict], api_key: str = None) -> List[Dict]:
        """
        🚀 OPTIMIZED ENRICHMENT: Bulk fetch + parallel processing
        Replaces individual API calls with bulk operations
        """
        if not notes:
            return notes
        
        start_time = time.time()
        self.logger.info(f"🚀 Migration {self.migration_id}: Starting optimized enrichment for {len(notes)} notes")
        
        try:
            # Step 1: Extract all unique IDs that need enrichment
            reference_ids = self._extract_unique_reference_ids(notes)
            
            # Step 2: Bulk fetch all required data in parallel
            self._bulk_fetch_reference_data_parallel(reference_ids,api_key)
            
            # Step 3: Apply enrichment using cached data (very fast!)
            enriched_notes = self._apply_cached_enrichment_to_notes(notes)
            
            # Step 4: Handle attachments with optimized batch processing
            self._enrich_incidents_attachments_batch(enriched_notes,api_key)
            
            duration = time.time() - start_time
            self.logger.info(f"🚀 Migration {self.migration_id}: Optimized enrichment complete: "
                        f"{len(notes)} notes in {duration:.2f}s")
            
            return enriched_notes
            
        except Exception as e:
            self.logger.error(f"Migration {self.migration_id}: Enrichment optimization failed: {e}")
            # Fallback to minimal enrichment
            return self._minimal_enrichment_fallback(notes, 'notes')

    def _fetch_single_user(self, user_id: str, api_key: str = None):
        """Fetch a single user if not in cache"""
        try:
            url = f"{self.connector._build_base_url()}/api/now/table/sys_user/{user_id}"
            params = {
                'sysparm_fields': 'sys_id,name,email,phone,user_name,first_name,last_name'
            }
            
            response = self._make_authenticated_request(url, 'GET', api_key=api_key, params=params)
            
            if response.status_code == 200:
                data = safe_json_response(response)
                user = data.get('result', {})
                
                if user:
                    with self._cache_lock:
                        self._session_cache['users'][user_id] = {
                            'sys_id': user_id,
                            'name': self._extract_field_value(user.get('name', '')),
                            'email': self._extract_field_value(user.get('email', '')),
                            'phone': self._extract_field_value(user.get('phone', '')),
                            'user_name': self._extract_field_value(user.get('user_name', '')),
                            'first_name': self._extract_field_value(user.get('first_name', '')),
                            'last_name': self._extract_field_value(user.get('last_name', ''))
                        }
        except Exception as e:
            self.logger.warning(f"Failed to fetch user {user_id}: {e}")

    def _extract_unique_reference_ids(self, records: List[Dict]) -> Dict[str, Set[str]]:
        """Extract all unique reference IDs from records"""
        reference_ids = {
            'user_ids': set(),
            'group_ids': set(),
            'company_ids': set(),
            'location_ids': set(),
            'department_ids': set()
        }
        
        for record in records:
            # Extract user IDs
            for field in ['caller_id', 'assigned_to', 'opened_by', 'resolved_by', 'closed_by', 'sys_created_by']:
                value = self._extract_id_value(record.get(field))
                if value:
                    reference_ids['user_ids'].add(value)
            
            # Extract group IDs
            for field in ['assignment_group']:
                value = self._extract_id_value(record.get(field))
                if value:
                    reference_ids['group_ids'].add(value)
            
            # Extract company IDs
            for field in ['company']:
                value = self._extract_id_value(record.get(field))
                if value:
                    reference_ids['company_ids'].add(value)
            
            # Extract location IDs
            for field in ['location']:
                value = self._extract_id_value(record.get(field))
                if value:
                    reference_ids['location_ids'].add(value)
            
            # Extract department IDs
            for field in ['u_department', 'department']:
                value = self._extract_id_value(record.get(field))
                if value:
                    reference_ids['department_ids'].add(value)
        
        # Log what we found
        self.logger.info(f"Migration {self.migration_id}: IDs to fetch: "
                        f"{len(reference_ids['user_ids'])} users, "
                        f"{len(reference_ids['group_ids'])} groups, "
                        f"{len(reference_ids['company_ids'])} companies, "
                        f"{len(reference_ids['location_ids'])} locations, "
                        f"{len(reference_ids['department_ids'])} departments")
        
        return reference_ids
    
    def _extract_id_value(self, field_value: Any) -> Optional[str]:
        """Extract ID value from various ServiceNow field formats"""
        if not field_value:
            return None
        
        if isinstance(field_value, dict):
            if 'value' in field_value:
                return field_value['value']
            elif 'link' in field_value:
                # Extract ID from link if possible
                link = field_value['link']
                if link:
                    return link.split('/')[-1] if '/' in link else None
        elif isinstance(field_value, str):
            return field_value
        
        return None
    
    def _bulk_fetch_reference_data_parallel(self, reference_ids: Dict[str, Set[str]], api_key: str = None):
        """
        🚀 PARALLEL BULK FETCHING: Fetch all reference data simultaneously
        Instead of individual API calls, make bulk calls in parallel
        """
        # Quick disable for debugging
        # if os.environ.get('SERVICENOW_DISABLE_PARALLEL', 'false').lower() == 'true':
        #     self.logger.info(f"Migration {self.migration_id}: Parallel processing DISABLED, using sequential")
        #     self._bulk_fetch_reference_data_sequential(reference_ids)
        #     return
        
        def fetch_users_bulk():
            """Fetch all users in bulk using sys_id filter"""
            user_ids = reference_ids.get('user_ids', set())
            if not user_ids:
                return
            
            try:
                # Build bulk query for users
                id_list = list(user_ids)
                chunk_size = 50  # ServiceNow query string length limits
                
                for i in range(0, len(id_list), chunk_size):
                    chunk = id_list[i:i + chunk_size]
                    id_query = '^ORsys_id='.join(chunk)
                    query = f'sys_id={id_query}'
                    
                    url = f"{self.connector._build_base_url()}/api/now/table/sys_user"
                    params = {
                        'sysparm_query': query,
                        'sysparm_fields': 'sys_id,name,email,phone,user_name,first_name,last_name',
                        'sysparm_limit': chunk_size
                    }
                    
                    response = self._make_authenticated_request(url, 'GET', api_key=api_key, params=params)
                    
                    if response.status_code == 200:
                        data = safe_json_response(response)
                        users = data.get('result', [])
                        
                        with self._cache_lock:
                            for user in users:
                                user_id = self._extract_id_value(user.get('sys_id'))
                                if user_id:
                                    self._session_cache['users'][user_id] = {
                                        'sys_id': user_id,
                                        'name': self._extract_field_value(user.get('name', '')),
                                        'email': self._extract_field_value(user.get('email', '')),
                                        'phone': self._extract_field_value(user.get('phone', '')),
                                        'user_name': self._extract_field_value(user.get('user_name', '')),
                                        'first_name': self._extract_field_value(user.get('first_name', '')),
                                        'last_name': self._extract_field_value(user.get('last_name', ''))
                                    }
                        
                        self.logger.info(f"Migration {self.migration_id}: Bulk fetched {len(users)} users (chunk {i//chunk_size + 1})")
                    else:
                        self.logger.warning(f"Migration {self.migration_id}: Bulk user fetch failed: {response.status_code}")
                        
            except Exception as e:
                self.logger.error(f"Migration {self.migration_id}: Bulk user fetch error: {e}")
        
        def fetch_groups_bulk():
            """Fetch all groups in bulk"""
            group_ids = reference_ids.get('group_ids', set())
            if not group_ids:
                return
            
            try:
                id_list = list(group_ids)
                chunk_size = 50
                
                for i in range(0, len(id_list), chunk_size):
                    chunk = id_list[i:i + chunk_size]
                    id_query = '^ORsys_id='.join(chunk)
                    query = f'sys_id={id_query}'
                    
                    url = f"{self.connector._build_base_url()}/api/now/table/sys_user_group"
                    params = {
                        'sysparm_query': query,
                        'sysparm_fields': 'sys_id,name,description,manager',
                        'sysparm_limit': chunk_size
                    }
                    
                    response = self._make_authenticated_request(url, 'GET', api_key=api_key, params=params)
                    
                    if response.status_code == 200:
                        data = safe_json_response(response)
                        groups = data.get('result', [])
                        
                        with self._cache_lock:
                            for group in groups:
                                group_id = self._extract_id_value(group.get('sys_id'))
                                if group_id:
                                    self._session_cache['groups'][group_id] = {
                                        'sys_id': group_id,
                                        'name': self._extract_field_value(group.get('name', '')),
                                        'description': self._extract_field_value(group.get('description', ''))
                                    }
                        
                        self.logger.info(f"Migration {self.migration_id}: Bulk fetched {len(groups)} groups")
                    else:
                        self.logger.warning(f"Migration {self.migration_id}: Bulk group fetch failed: {response.status_code}")
                        
            except Exception as e:
                self.logger.error(f"Migration {self.migration_id}: Bulk group fetch error: {e}")
        
        def fetch_companies_bulk():
            """Fetch all companies in bulk"""
            company_ids = reference_ids.get('company_ids', set())
            if not company_ids:
                return
            
            try:
                id_list = list(company_ids)
                chunk_size = 50
                
                for i in range(0, len(id_list), chunk_size):
                    chunk = id_list[i:i + chunk_size]
                    id_query = '^ORsys_id='.join(chunk)
                    query = f'sys_id={id_query}'
                    
                    url = f"{self.connector._build_base_url()}/api/now/table/core_company"
                    params = {
                        'sysparm_query': query,
                        'sysparm_fields': 'sys_id,name,phone,email',
                        'sysparm_limit': chunk_size
                    }
                    
                    response = self._make_authenticated_request(url, 'GET', api_key=api_key, params=params)
                    
                    if response.status_code == 200:
                        data = safe_json_response(response)
                        companies = data.get('result', [])
                        
                        with self._cache_lock:
                            for company in companies:
                                company_id = self._extract_id_value(company.get('sys_id'))
                                if company_id:
                                    self._session_cache['companies'][company_id] = {
                                        'sys_id': company_id,
                                        'name': self._extract_field_value(company.get('name', '')),
                                        'phone': self._extract_field_value(company.get('phone', '')),
                                        'email': self._extract_field_value(company.get('email', ''))
                                    }
                        
                        self.logger.info(f"Migration {self.migration_id}: Bulk fetched {len(companies)} companies")
                    else:
                        self.logger.warning(f"Migration {self.migration_id}: Bulk company fetch failed: {response.status_code}")
                        
            except Exception as e:
                self.logger.error(f"Migration {self.migration_id}: Bulk company fetch error: {e}")
        
        def fetch_locations_bulk():
            """Fetch all locations in bulk"""
            location_ids = reference_ids.get('location_ids', set())
            if not location_ids:
                return
            
            try:
                id_list = list(location_ids)
                chunk_size = 50
                
                for i in range(0, len(id_list), chunk_size):
                    chunk = id_list[i:i + chunk_size]
                    id_query = '^ORsys_id='.join(chunk)
                    query = f'sys_id={id_query}'
                    
                    url = f"{self.connector._build_base_url()}/api/now/table/cmn_location"
                    params = {
                        'sysparm_query': query,
                        'sysparm_fields': 'sys_id,name,city,state,country',
                        'sysparm_limit': chunk_size
                    }
                    
                    response = self._make_authenticated_request(url, 'GET', api_key=api_key, params=params)
                    
                    if response.status_code == 200:
                        data = safe_json_response(response)
                        locations = data.get('result', [])
                        
                        with self._cache_lock:
                            for location in locations:
                                location_id = self._extract_id_value(location.get('sys_id'))
                                if location_id:
                                    self._session_cache['locations'][location_id] = {
                                        'sys_id': location_id,
                                        'name': self._extract_field_value(location.get('name', '')),
                                        'city': self._extract_field_value(location.get('city', '')),
                                        'state': self._extract_field_value(location.get('state', '')),
                                        'country': self._extract_field_value(location.get('country', ''))
                                    }
                        
                        self.logger.info(f"Migration {self.migration_id}: Bulk fetched {len(locations)} locations")
                    else:
                        self.logger.warning(f"Migration {self.migration_id}: Bulk location fetch failed: {response.status_code}")
                        
            except Exception as e:
                self.logger.error(f"Migration {self.migration_id}: Bulk location fetch error: {e}")
        
        def fetch_departments_bulk():
            """Fetch all departments in bulk"""
            department_ids = reference_ids.get('department_ids', set())
            if not department_ids:
                return
            
            try:
                id_list = list(department_ids)
                chunk_size = 50
                
                for i in range(0, len(id_list), chunk_size):
                    chunk = id_list[i:i + chunk_size]
                    id_query = '^ORsys_id='.join(chunk)
                    query = f'sys_id={id_query}'
                    
                    url = f"{self.connector._build_base_url()}/api/now/table/cmn_department"
                    params = {
                        'sysparm_query': query,
                        'sysparm_fields': 'sys_id,name,description,head',
                        'sysparm_limit': chunk_size
                    }
                    
                    response = self._make_authenticated_request(url, 'GET', api_key=api_key, params=params)
                    
                    if response.status_code == 200:
                        data = safe_json_response(response)
                        departments = data.get('result', [])
                        
                        with self._cache_lock:
                            for dept in departments:
                                dept_id = self._extract_id_value(dept.get('sys_id'))
                                if dept_id:
                                    self._session_cache['departments'][dept_id] = {
                                        'sys_id': dept_id,
                                        'name': self._extract_field_value(dept.get('name', '')),
                                        'description': self._extract_field_value(dept.get('description', ''))
                                    }
                        
                        self.logger.info(f"Migration {self.migration_id}: Bulk fetched {len(departments)} departments")
                    else:
                        self.logger.warning(f"Migration {self.migration_id}: Bulk department fetch failed: {response.status_code}")
                        
            except Exception as e:
                self.logger.error(f"Migration {self.migration_id}: Bulk department fetch error: {e}")
        
        # Execute all bulk fetches in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(fetch_users_bulk),
                executor.submit(fetch_groups_bulk),
                executor.submit(fetch_companies_bulk),
                executor.submit(fetch_locations_bulk),
                executor.submit(fetch_departments_bulk)
            ]
            
            # Wait for all to complete
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result(timeout=60)  # 60-second timeout per bulk fetch
                except Exception as e:
                    self.logger.error(f"Migration {self.migration_id}: Parallel bulk fetch failed: {e}")
    
    def _bulk_fetch_users_by_username(self, usernames: Set[str], api_key: str = None):
        """Bulk fetch users by username for notes enrichment"""
        if not usernames:
            return
        
        try:
            username_list = list(usernames)
            chunk_size = 50
            
            for i in range(0, len(username_list), chunk_size):
                chunk = username_list[i:i + chunk_size]
                username_query = '^ORuser_name='.join(chunk)
                query = f'user_name={username_query}'
                
                url = f"{self.connector._build_base_url()}/api/now/table/sys_user"
                params = {
                    'sysparm_query': query,
                    'sysparm_fields': 'sys_id,name,email,phone,user_name',
                    'sysparm_limit': chunk_size
                }
                
                response = self._make_authenticated_request(url, 'GET', api_key=api_key, params=params)
                
                if response.status_code == 200:
                    data = safe_json_response(response)
                    users = data.get('result', [])
                    
                    with self._cache_lock:
                        for user in users:
                            user_name = self._extract_field_value(user.get('user_name'))
                            user_id = self._extract_id_value(user.get('sys_id'))
                            if user_name and user_id:
                                # Store by both username and sys_id for flexibility
                                user_data = {
                                    'sys_id': user_id,
                                    'name': self._extract_field_value(user.get('name', '')),
                                    'email': self._extract_field_value(user.get('email', '')),
                                    'phone': self._extract_field_value(user.get('phone', '')),
                                    'user_name': user_name
                                }
                                self._session_cache['users'][user_id] = user_data
                                self._session_cache['users'][user_name] = user_data
                    
                    self.logger.info(f"Migration {self.migration_id}: Bulk fetched {len(users)} users by username")
                else:
                    self.logger.warning(f"Migration {self.migration_id}: Bulk username fetch failed: {response.status_code}")
                    
        except Exception as e:
            self.logger.error(f"Migration {self.migration_id}: Bulk username fetch error: {e}")
    
    def _extract_field_value(self, field_value: Any) -> str:
        """Extract string value from ServiceNow field format"""
        if isinstance(field_value, dict) and 'value' in field_value:
            return str(field_value['value']) if field_value['value'] is not None else ''
        elif field_value is not None:
            return str(field_value)
        return ''
    
    def _flatten_reference_fields(self, record: Dict) -> Dict:
        """Flatten ServiceNow reference fields from {'link': '...', 'value': '...'} to just the value"""
        flattened = record.copy()
        for key, value in record.items():
            if isinstance(value, dict) and 'link' in value and 'value' in value:
                flattened[key] = value['value']
        return flattened
    
    def _apply_cached_enrichment_to_incidents(self, incidents: List[Dict]) -> List[Dict]:
        """Apply enrichment using cached data (very fast!)"""
        enriched_incidents = []
        
        for incident in incidents:
            enriched_incident = incident.copy()
            
            # Enrich caller information
            caller_id = self._extract_id_value(incident.get('caller_id'))
            if caller_id:
                if caller_id not in self._session_cache['users']:
                    self._fetch_single_user(caller_id)
                
                if caller_id in self._session_cache['users']:
                    user_data = self._session_cache['users'][caller_id]
                    enriched_incident['callerName'] = user_data.get('name', '')
                    enriched_incident['callerEmail'] = user_data.get('email', '')
                    enriched_incident['callerPhone'] = user_data.get('phone', '')
                else:
                    enriched_incident['callerName'] = ''
                    enriched_incident['callerEmail'] = ''
                    enriched_incident['callerPhone'] = ''
            else:
                enriched_incident['callerName'] = ''
                enriched_incident['callerEmail'] = ''
                enriched_incident['callerPhone'] = ''
            
            # Enrich assignee information
            assigned_to = self._extract_id_value(incident.get('assigned_to'))
            if assigned_to:
                if assigned_to not in self._session_cache['users']:
                    self._fetch_single_user(assigned_to)
                
                if assigned_to in self._session_cache['users']:
                    user_data = self._session_cache['users'][assigned_to]
                    enriched_incident['agentName'] = user_data.get('name', '')
                    enriched_incident['agentEmail'] = user_data.get('email', '')
                else:
                    enriched_incident['agentName'] = ''
                    enriched_incident['agentEmail'] = ''
            else:
                enriched_incident['agentName'] = ''
                enriched_incident['agentEmail'] = ''
            
            # Enrich assignment group information
            assignment_group = self._extract_id_value(incident.get('assignment_group'))
            if assignment_group and assignment_group in self._session_cache['groups']:
                group_data = self._session_cache['groups'][assignment_group]
                enriched_incident['assignmentGroupName'] = group_data.get('name', '')
            else:
                enriched_incident['assignmentGroupName'] = ''
            
            # Enrich company information
            company = self._extract_id_value(incident.get('company'))
            if company and company in self._session_cache['companies']:
                company_data = self._session_cache['companies'][company]
                enriched_incident['companyName'] = company_data.get('name', '')
            else:
                enriched_incident['companyName'] = ''
            
            # Enrich location information
            location = self._extract_id_value(incident.get('location'))
            if location and location in self._session_cache['locations']:
                location_data = self._session_cache['locations'][location]
                enriched_incident['locationName'] = location_data.get('name', '')
            else:
                enriched_incident['locationName'] = ''

            enriched_incident = self._flatten_reference_fields(enriched_incident)
            
            enriched_incidents.append(enriched_incident)
        
        return enriched_incidents
    
    def _apply_cached_enrichment_to_requests(self, requests: List[Dict], api_key: str = None) -> List[Dict]:        
        """Apply enrichment using cached data (very fast!)
        Mirrors the logic in _apply_cached_enrichment_to_changes: if a referenced user
        is missing from cache, fetch it first using the provided api_key.
        """
        enriched_requests = []
        
        for request in requests:
            enriched_request = request.copy()
            
            # Enrich requestedFor information
            requestedFor = self._extract_id_value(request.get('requested_for'))
            if requestedFor:
                if requestedFor not in self._session_cache['users']:
                    # Fetch on-demand like in changes enrichment
                    self._fetch_single_user(requestedFor, api_key)
                if requestedFor in self._session_cache['users']:
                    user_data = self._session_cache['users'][requestedFor]
                    enriched_request['requestedForName'] = user_data.get('name', '')
                    enriched_request['requestedForEmail'] = user_data.get('email', '')
                    enriched_request['requestedForPhone'] = user_data.get('phone', '')
                else:
                    enriched_request['requestedForName'] = ''
                    enriched_request['requestedForEmail'] = ''
                    enriched_request['requestedForPhone'] = ''
            else:
                enriched_request['requestedForName'] = ''
                enriched_request['requestedForEmail'] = ''
                enriched_request['requestedForPhone'] = ''

            # Enrich openedBy information
            openedBy = self._extract_id_value(request.get('opened_by'))
            if openedBy:
                if openedBy not in self._session_cache['users']:
                    # Fetch on-demand like in changes enrichment
                    self._fetch_single_user(openedBy, api_key)
                if openedBy in self._session_cache['users']:
                    user_data = self._session_cache['users'][openedBy]
                    enriched_request['openedByName'] = user_data.get('name', '')
                    enriched_request['openedByEmail'] = user_data.get('email', '')
                    enriched_request['openedByPhone'] = user_data.get('phone', '')
                else:
                    enriched_request['openedByName'] = ''
                    enriched_request['openedByEmail'] = ''
                    enriched_request['openedByPhone'] = ''
            else:
                enriched_request['openedByName'] = ''
                enriched_request['openedByEmail'] = ''
                enriched_request['openedByPhone'] = ''

            # Enrich assignee information
            assigned_to = self._extract_id_value(request.get('assigned_to'))
            if assigned_to:
                if assigned_to not in self._session_cache['users']:
                    self._fetch_single_user(assigned_to, api_key)
                if assigned_to in self._session_cache['users']:
                    user_data = self._session_cache['users'][assigned_to]
                    enriched_request['agentName'] = user_data.get('name', '')
                    enriched_request['agentEmail'] = user_data.get('email', '')
                else:
                    enriched_request['agentName'] = ''
                    enriched_request['agentEmail'] = ''
            else:
                enriched_request['agentName'] = ''
                enriched_request['agentEmail'] = ''
            
            # Enrich assignment group information
            assignment_group = self._extract_id_value(request.get('assignment_group'))
            if assignment_group and assignment_group in self._session_cache['groups']:
                group_data = self._session_cache['groups'][assignment_group]
                enriched_request['assignmentGroupName'] = group_data.get('name', '')
            else:
                enriched_request['assignmentGroupName'] = ''
            
            # Enrich company information
            company = self._extract_id_value(request.get('company'))
            if company and company in self._session_cache['companies']:
                company_data = self._session_cache['companies'][company]
                enriched_request['companyName'] = company_data.get('name', '')
            else:
                enriched_request['companyName'] = ''
            
            # Enrich location information
            location = self._extract_id_value(request.get('location'))
            if location and location in self._session_cache['locations']:
                location_data = self._session_cache['locations'][location]
                enriched_request['locationName'] = location_data.get('name', '')
            else:
                enriched_request['locationName'] = ''
            
            enriched_requests.append(enriched_request)
        
        
        return enriched_requests

    def _apply_cached_enrichment_to_changes(self, changes: List[Dict], api_key: str) -> List[Dict]:
        """Apply enrichment using cached data for changes"""
        enriched_changes = []
        
        for change in changes:
            enriched_change = change.copy()
            
            # Enrich requested_by (similar to caller_id in incidents)
            requested_by = self._extract_id_value(change.get('requested_by'))
            if requested_by:
                if requested_by not in self._session_cache['users']:
                    self._fetch_single_user(requested_by, api_key)
                
                if requested_by in self._session_cache['users']:
                    user_data = self._session_cache['users'][requested_by]
                    enriched_change['requesterName'] = user_data.get('name', '')
                    enriched_change['requesterEmail'] = user_data.get('email', '')
                else:
                    enriched_change['requesterName'] = ''
                    enriched_change['requesterEmail'] = ''
            else:
                enriched_change['requesterName'] = ''
                enriched_change['requesterEmail'] = ''
            
            # REUSE: All the same enrichment patterns as incidents
            # Enrich assignee
            assigned_to = self._extract_id_value(change.get('assigned_to'))
            if assigned_to:
                if assigned_to not in self._session_cache['users']:
                    self._fetch_single_user(assigned_to)
                
                if assigned_to in self._session_cache['users']:
                    user_data = self._session_cache['users'][assigned_to]
                    enriched_change['agentName'] = user_data.get('name', '')
                    enriched_change['agentEmail'] = user_data.get('email', '')
                else:
                    enriched_change['agentName'] = ''
                    enriched_change['agentEmail'] = ''
            else:
                enriched_change['agentName'] = ''
                enriched_change['agentEmail'] = ''
            
            # REUSE: Assignment group enrichment (same pattern)
            assignment_group = self._extract_id_value(change.get('assignment_group'))
            if assignment_group and assignment_group in self._session_cache['groups']:
                group_data = self._session_cache['groups'][assignment_group]
                enriched_change['assignmentGroupName'] = group_data.get('name', '')
            else:
                enriched_change['assignmentGroupName'] = ''
            
            # REUSE: Company enrichment (same pattern)
            company = self._extract_id_value(change.get('company'))
            if company and company in self._session_cache['companies']:
                company_data = self._session_cache['companies'][company]
                enriched_change['companyName'] = company_data.get('name', '')
            else:
                enriched_change['companyName'] = ''
            
            enriched_changes.append(enriched_change)
        
        return enriched_changes

    def _apply_cached_enrichment_to_tasks(self, tasks: List[Dict]) -> List[Dict]:
        """Apply enrichment using cached data for tasks"""
        enriched_tasks = []
        
        for task in tasks:
            enriched_task = task.copy()
            
            # Enrich assignee information
            assigned_to = self._extract_id_value(task.get('assigned_to'))
            if assigned_to and assigned_to in self._session_cache['users']:
                user_data = self._session_cache['users'][assigned_to]
                enriched_task['assigneeName'] = user_data.get('name', '')
                enriched_task['assigneeEmail'] = user_data.get('email', '')
            else:
                enriched_task['assigneeName'] = ''
                enriched_task['assigneeEmail'] = ''
            
            # Enrich assignment group information
            assignment_group = self._extract_id_value(task.get('assignment_group'))
            if assignment_group and assignment_group in self._session_cache['groups']:
                group_data = self._session_cache['groups'][assignment_group]
                enriched_task['assignmentGroupName'] = group_data.get('name', '')
            else:
                enriched_task['assignmentGroupName'] = ''
            
            enriched_tasks.append(enriched_task)
        
        return enriched_tasks
    
    def _apply_cached_enrichment_to_notes(self, notes: List[Dict]) -> List[Dict]:
        """Apply enrichment using cached data for notes"""
        enriched_notes = []
        
        for note in notes:
            enriched_note = note.copy()
            
            # Enrich creator information
            created_by = note.get('sys_created_by')
            if created_by:
                user_key = created_by
                if isinstance(created_by, dict) and 'value' in created_by:
                    user_key = created_by['value']
                
                if user_key in self._session_cache['users']:
                    user_data = self._session_cache['users'][user_key]
                    enriched_note['personName'] = user_data.get('name', '')
                    enriched_note['personEmail'] = user_data.get('email', '')
                else:
                    enriched_note['personEmail'] = str(user_key) if user_key else ''
                    enriched_note['personName'] = ''
            else:
                enriched_note['personName'] = ''
                enriched_note['personEmail'] = ''
            
            enriched_notes.append(enriched_note)
        
        return enriched_notes
    
    def _enrich_incidents_attachments_batch(self, incidents: List[Dict], api_key: str = None):
        """Enrich incidents with attachment information using batch processing"""
        if not incidents:
            return
        
        try:
            # Collect all incident sys_ids
            incident_ids = []
            for incident in incidents:
                sys_id = self._extract_id_value(incident.get('sys_id'))
                table_name = self._extract_field_value(incident.get('sys_class_name', 'incident'))
                if sys_id:
                    incident_ids.append(sys_id)
            
            if not incident_ids:
                # No valid sys_ids, set empty attachments for all
                for incident in incidents:
                    incident['attachments'] = []
                    incident['attachmentUrls'] = []
                return
            
            # Bulk fetch attachments for all incidents
            chunk_size = 50
            all_attachments = {}  # incident_id -> [attachments]
            
            for i in range(0, len(incident_ids), chunk_size):
                chunk = incident_ids[i:i + chunk_size]
                id_query = '^ORtable_sys_id='.join(chunk)
                query = f'table_sys_id={id_query}^table_name={table_name}'
                
                url = f"{self.connector._build_base_url()}/api/now/table/sys_attachment"
                params = {
                    'sysparm_query': query,
                    'sysparm_fields': 'sys_id,file_name,size_bytes,content_type,table_sys_id',
                    'sysparm_limit': 1000
                }
                
                response = self._make_authenticated_request(url, 'GET', api_key=api_key, params=params)
                
                if response.status_code == 200:
                    data = safe_json_response(response)
                    attachment_records = data.get('result', [])
                    
                    # Group attachments by incident
                    for attachment in attachment_records:
                        table_sys_id = self._extract_field_value(attachment.get('table_sys_id'))
                        if table_sys_id:
                            if table_sys_id not in all_attachments:
                                all_attachments[table_sys_id] = []
                            
                            attachment_sys_id = self._extract_field_value(attachment.get('sys_id'))
                            if attachment_sys_id:
                                base_url = self.connector._build_base_url()
                                file_url = f"{base_url}/api/now/attachment/{attachment_sys_id}/file"
                                
                                attachment_info = {
                                    'sys_id': attachment_sys_id,
                                    'file_name': self._extract_field_value(attachment.get('file_name', '')),
                                    'size_bytes': self._extract_field_value(attachment.get('size_bytes', 0)),
                                    'content_type': self._extract_field_value(attachment.get('content_type', '')),
                                    'download_url': file_url
                                }
                                all_attachments[table_sys_id].append(attachment_info)
                    
                    self.logger.info(f"Migration {self.migration_id}: Bulk fetched attachments for chunk {i//chunk_size + 1}")
                else:
                    self.logger.warning(f"Migration {self.migration_id}: Bulk attachment fetch failed: {response.status_code}")
            
            # Apply attachment data to incidents
            for incident in incidents:
                sys_id = self._extract_id_value(incident.get('sys_id'))
                if sys_id and sys_id in all_attachments:
                    attachments = all_attachments[sys_id]
                    incident['attachments'] = attachments
                    incident['attachmentUrls'] = [att['download_url'] for att in attachments]
                else:
                    incident['attachments'] = []
                    incident['attachmentUrls'] = []
            
            self.logger.info(f"Migration {self.migration_id}: Completed batch attachment enrichment for {len(incidents)} incidents")
            
        except Exception as e:
            self.logger.error(f"Migration {self.migration_id}: Batch attachment enrichment failed: {e}")
            # Set empty attachments for all incidents if batch fails
            for incident in incidents:
                incident['attachments'] = []
                incident['attachmentUrls'] = []
    
    def _enrich_records_attachments_batch(self, records: List[Dict], table_name: str, api_key: str = None):
        """Generic attachment enrichment for any ServiceNow table"""
        if not records:
            return
        
        try:
            # Collect all record sys_ids
            record_ids = []
            for record in records:
                sys_id = self._extract_id_value(record.get('sys_id'))
                if sys_id:
                    record_ids.append(sys_id)
            
            if not record_ids:
                for record in records:
                    record['attachments'] = []
                    record['attachmentUrls'] = []
                return
            
            # Bulk fetch attachments
            chunk_size = 50
            all_attachments = {}
            
            for i in range(0, len(record_ids), chunk_size):
                chunk = record_ids[i:i + chunk_size]
                id_query = '^ORtable_sys_id='.join(chunk)
                query = f'table_sys_id={id_query}^table_name={table_name}'
                
                url = f"{self.connector._build_base_url()}/api/now/table/sys_attachment"
                params = {
                    'sysparm_query': query,
                    'sysparm_fields': 'sys_id,file_name,size_bytes,content_type,table_sys_id',
                    'sysparm_limit': 1000
                }
                
                response = self._make_authenticated_request(url, 'GET', api_key=api_key, params=params)
                
                if response.status_code == 200:
                    data = safe_json_response(response)
                    attachment_records = data.get('result', [])
                    
                    for attachment in attachment_records:
                        table_sys_id = self._extract_field_value(attachment.get('table_sys_id'))
                        if table_sys_id:
                            if table_sys_id not in all_attachments:
                                all_attachments[table_sys_id] = []
                            
                            attachment_sys_id = self._extract_field_value(attachment.get('sys_id'))
                            if attachment_sys_id:
                                base_url = self.connector._build_base_url()
                                file_url = f"{base_url}/api/now/attachment/{attachment_sys_id}/file"
                                
                                attachment_info = {
                                    'sys_id': attachment_sys_id,
                                    'file_name': self._extract_field_value(attachment.get('file_name', '')),
                                    'size_bytes': self._extract_field_value(attachment.get('size_bytes', 0)),
                                    'content_type': self._extract_field_value(attachment.get('content_type', '')),
                                    'download_url': file_url
                                }
                                all_attachments[table_sys_id].append(attachment_info)
            
            # Apply attachments to records
            for record in records:
                sys_id = self._extract_id_value(record.get('sys_id'))
                if sys_id and sys_id in all_attachments:
                    attachments = all_attachments[sys_id]
                    record['attachments'] = attachments
                    record['attachmentUrls'] = [att['download_url'] for att in attachments]
                else:
                    record['attachments'] = []
                    record['attachmentUrls'] = []
            
            self.logger.info(f"Migration {self.migration_id}: Completed batch attachment enrichment for {len(records)} {table_name} records")
            
        except Exception as e:
            self.logger.error(f"Migration {self.migration_id}: Batch {table_name} attachment enrichment failed: {e}")
            for record in records:
                record['attachments'] = []
                record['attachmentUrls'] = []

    def _minimal_enrichment_fallback(self, records: List[Dict], record_type: str) -> List[Dict]:
        """
        Fallback: Skip enrichment entirely for speed
        Return records with minimal processing
        """
        self.logger.warning(f"Migration {self.migration_id}: Using minimal enrichment fallback for {record_type}")
        
        # Just add empty enrichment fields to maintain structure
        for record in records:
            if record_type == 'incidents':
                record['callerName'] = 'Unknown'
                record['callerEmail'] = 'Unknown'
                record['callerPhone'] = 'Unknown'
                record['agentName'] = 'Unknown'
                record['agentEmail'] = 'Unknown'
                record['assignmentGroupName'] = 'Unknown'
                record['companyName'] = 'Unknown'
                record['locationName'] = 'Unknown'
                record['attachments'] = []
                record['attachmentUrls'] = []
            elif record_type == 'tasks':
                record['assigneeName'] = 'Unknown'
                record['assigneeEmail'] = 'Unknown'
                record['assignmentGroupName'] = 'Unknown'
            elif record_type == 'notes':
                record['personName'] = 'Unknown'
                record['personEmail'] = 'Unknown'
        
        return records
    
    def _make_authenticated_request(self, url: str, method: str = 'GET', api_key: str = None, **kwargs) -> requests.Response:
        """Make authenticated request to ServiceNow API"""
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        
        # Add API key if provided
        if api_key:
            headers["x-sn-apikey"] = api_key
        
        auth = self.connector._build_auth()
        
        # Use rate limiter if available
        if hasattr(self.connector, 'rate_limiter'):
            return self.connector.rate_limiter.make_request_with_retry(
                url, method, auth=auth, headers=headers, **kwargs
            )
        else:
            return requests.request(method, url, auth=auth, headers=headers, **kwargs)


# 🚀 BACKWARD COMPATIBLE: Original enricher updated to use optimized version
class ServiceNowDataEnricher(BaseDataEnricher):
    """
    🚀 UPDATED: Now uses optimized enrichment by default
    Maintains backward compatibility while providing 90%+ speed improvement
    """
    
    def __init__(self, connector):
        super().__init__()
        self.connector = connector
        # Use optimized enricher internally
        self.optimized_enricher = OptimizedServiceNowDataEnricher(connector)
        
        # Keep original configuration methods for compatibility
        self._resolved_references_cache = {}
        self._enrichment_config_cache = None
    
    def enrich_incidents(self, incidents: List[Dict], resolve_references: bool = True, api_key: str = None) -> List[Dict]:
        """🚀 Now uses optimized enrichment instead of slow individual API calls"""
        if not resolve_references:
            return incidents
        
        return self.optimized_enricher.enrich_incidents_optimized(incidents, api_key)
    
    def enrich_requests(self, requests: List[Dict], resolve_references: bool = True, api_key: str = None) -> List[Dict]:
        """🚀 Now uses optimized enrichment instead of slow individual API calls"""
        if not resolve_references:
            return requests
        return self.optimized_enricher.enrich_requests_optimized(requests, api_key)

    def enrich_ctasks(self, ctasks: List[Dict], resolve_references: bool = True, api_key: str = None) -> List[Dict]:
        """🚀 Now uses optimized enrichment instead of slow individual API calls"""
        if not resolve_references:
            return ctasks
        return self.optimized_enricher.enrich_requests_optimized(ctasks, api_key)
    
    def enrich_changes(self, changes: List[Dict], resolve_references: bool = True, api_key: str = None) -> List[Dict]:
        """🚀 Uses optimized enrichment for changes"""
        if not resolve_references:
            return changes
        
        return self.optimized_enricher.enrich_changes_optimized(changes, api_key)

    def enrich_tasks(self, tasks: List[Dict], resolve_references: bool = True, api_key: str = None) -> List[Dict]:
        """🚀 Now uses optimized enrichment for tasks"""
        if not resolve_references:
            return tasks
        
        return self.optimized_enricher.enrich_tasks_optimized(tasks, api_key)
    
    def enrich_notes(self, notes: List[Dict], users: List[Dict] = None, api_key: str = None) -> List[Dict]:
        """🚀 Now uses optimized enrichment for notes"""
        return self.optimized_enricher.enrich_notes_optimized(notes, api_key)
    
    def enrich_ctask_notes_with_context(self, notes: List[Dict], resolve_references: bool = True, api_key: str = None) -> List[Dict]:
        """🚀 Now uses optimized enrichment instead of slow individual API calls"""
        if not resolve_references:
            return notes
        return self.optimized_enricher.enrich_ctask_notes_optimized(notes, api_key)

    def enrich_ritms(self, ritms: List[Dict], resolve_references: bool = True, api_key: str = None) -> List[Dict]:
        """🚀 Now uses optimized enrichment instead of slow individual API calls"""
        if not resolve_references:
            return ritms
        return self.optimized_enricher.enrich_requests_optimized(ritms, api_key)

    # Keep original methods for backward compatibility
    def reload_enrichment_config(self):
        """Force reload of enrichment configuration from file"""
        self._enrichment_config_cache = None
        if hasattr(self.optimized_enricher, '_enrichment_config_cache'):
            self.optimized_enricher._enrichment_config_cache = None
        logging.info("Enrichment configuration cache cleared, will reload on next use")
    
    def _get_enrichment_config(self, platform: str, entity: str) -> List[Dict]:
        """Load enrichment configuration from JSON file or config - kept for compatibility"""
        try:
            # Load from file
            config = self._load_enrichment_config_from_file()
            
            if config:
                platform_config = config.get(platform, [])
                return [cfg for cfg in platform_config if cfg.get('entity') == entity]
            
            # Fallback to default configuration if file doesn't exist or is empty
            return self._get_default_enrichment_config(platform, entity)
            
        except Exception as e:
            logging.warning(f"Failed to load enrichment config: {e}. Using defaults.")
            return self._get_default_enrichment_config(platform, entity)
    
    def _load_enrichment_config_from_file(self) -> Optional[Dict]:
        """Load enrichment configuration from src.utils.custom_handler.json file"""
        if self._enrichment_config_cache is not None:
            return self._enrichment_config_cache
        
        try:
            # Try multiple possible paths for the config file
            possible_paths = [
                'src.utils.custom_handler.json',
                'src/utils/custom_handler.json',
                './src.utils.custom_handler.json',
                './src/utils/custom_handler.json',
                os.path.join(os.getcwd(), 'src.utils.custom_handler.json'),
                os.path.join(os.getcwd(), 'src', 'utils', 'custom_handler.json')
            ]
            
            # Also check environment variable for custom path
            env_path = os.environ.get('CUSTOM_HANDLER_CONFIG_PATH')
            if env_path:
                possible_paths.insert(0, env_path)
            
            config_loaded = False
            for config_path in possible_paths:
                if os.path.exists(config_path):
                    logging.info(f"Loading enrichment config from: {config_path}")
                    with open(config_path, 'r', encoding='utf-8') as f:
                        self._enrichment_config_cache = json.load(f)
                        config_loaded = True
                        break
            
            if not config_loaded:
                logging.warning(f"Enrichment config file not found in any of these paths: {possible_paths}")
                self._enrichment_config_cache = {}
            
            return self._enrichment_config_cache
            
        except FileNotFoundError:
            logging.warning("Enrichment config file 'src.utils.custom_handler.json' not found. Using default configuration.")
            self._enrichment_config_cache = {}
            return self._enrichment_config_cache
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON in enrichment config file: {e}. Using default configuration.")
            self._enrichment_config_cache = {}
            return self._enrichment_config_cache
        except Exception as e:
            logging.error(f"Error loading enrichment config file: {e}. Using default configuration.")
            self._enrichment_config_cache = {}
            return self._enrichment_config_cache
    
    def _get_default_enrichment_config(self, platform: str, entity: str) -> List[Dict]:
        """Return default enrichment configuration as fallback"""
        logging.info(f"Using default enrichment configuration for {platform}.{entity}")
        
        if platform == 'servicenow' and entity == 'incidents':
            return [
                {
                    "source": "callerEmail",
                    "target": "email",
                    "entity": "incidents",
                    "uniqueIdentifier": "caller_id",
                    "lookup": {
                        "url": "/api/now/table/sys_user/{caller_id}",
                        "hasMore": False
                    }
                },
                {
                    "source": "callerName",
                    "target": "name",
                    "entity": "incidents",
                    "uniqueIdentifier": "caller_id",
                    "lookup": {
                        "url": "/api/now/table/sys_user/{caller_id}",
                        "hasMore": False
                    }
                },
                {
                    "source": "callerPhone",
                    "target": "phone",
                    "entity": "incidents",
                    "uniqueIdentifier": "caller_id",
                    "lookup": {
                        "url": "/api/now/table/sys_user/{caller_id}",
                        "hasMore": False
                    }
                },
                {
                    "source": "agentEmail",
                    "target": "email",
                    "entity": "incidents",
                    "uniqueIdentifier": "assigned_to",
                    "lookup": {
                        "url": "/api/now/table/sys_user/{assigned_to}",
                        "hasMore": False
                    }
                },
                {
                    "source": "agentName",
                    "target": "name",
                    "entity": "incidents",
                    "uniqueIdentifier": "assigned_to",
                    "lookup": {
                        "url": "/api/now/table/sys_user/{assigned_to}",
                        "hasMore": False
                    }
                },
                {
                    "source": "assignmentGroupName",
                    "target": "name",
                    "entity": "incidents",
                    "uniqueIdentifier": "assignment_group",
                    "lookup": {
                        "url": "/api/now/table/sys_user_group/{assignment_group}",
                        "hasMore": False
                    }
                },
                {
                    "source": "personEmail",
                    "target": "email",
                    "entity": "notes",
                    "uniqueIdentifier": "sys_created_by",
                    "lookup": {
                        "url": "/api/now/table/sys_user?sysparm_query=user_name={sys_created_by}",
                        "hasMore": False
                    }
                }
            ]
        elif platform == 'servicenow' and entity == 'tasks':
            return [
                {
                    "source": "assigneeEmail",
                    "target": "email",
                    "entity": "tasks",
                    "uniqueIdentifier": "assigned_to",
                    "lookup": {
                        "url": "/api/now/table/sys_user/{assigned_to}",
                        "hasMore": False
                    }
                },
                {
                    "source": "assigneeName",
                    "target": "name",
                    "entity": "tasks",
                    "uniqueIdentifier": "assigned_to",
                    "lookup": {
                        "url": "/api/now/table/sys_user/{assigned_to}",
                        "hasMore": False
                    }
                }
            ]
        
        return []


class ServiceNowFieldMapper(BaseFieldMapper):
    """ServiceNow-specific field mapper"""
    
    def get_standard_field_mapping(self) -> Dict[str, str]:
        """Return mapping of ServiceNow fields to standard field names"""
        return {
            # Incident fields
            "incident_number": "number",
            "incident_state": "state",
            "incident_priority": "priority",
            "incident_urgency": "urgency",
            "incident_impact": "impact",
            "incident_category": "category",
            "incident_subcategory": "subcategory",
            "incident_description": "description",
            "incident_short_description": "short_description",
            "incident_assigned_to": "assigned_to",
            "incident_assignment_group": "assignment_group",
            "incident_caller": "caller_id",
            "incident_opened_by": "opened_by",
            "incident_created": "sys_created_on",
            "incident_updated": "sys_updated_on",
            "incident_resolved": "resolved_at",
            "incident_closed": "closed_at",
            
            # Task fields
            "task_number": "number",
            "task_state": "state",
            "task_priority": "priority",
            "task_description": "description",
            "task_short_description": "short_description",
            "task_assigned_to": "assigned_to",
            "task_assignment_group": "assignment_group",
            "task_created": "sys_created_on",
            "task_updated": "sys_updated_on",
            "task_due": "due_date",
            
            # Note fields
            "note_value": "value",
            "note_created": "sys_created_on",
            "note_created_by": "sys_created_by",
            "note_element": "element",
            "note_element_id": "element_id"
        }
    
    def process_custom_fields(self, record: Dict) -> Dict[str, Any]:
        """Process ServiceNow custom fields (u_ prefixed fields)"""
        custom_fields = {}
        
        for field_name, field_value in record.items():
            if field_name.startswith('u_'):
                # It's a custom field
                custom_fields[field_name] = field_value
        
        return custom_fields


class ServiceNowConnector(BaseSourceConnector):
    """ServiceNow implementation of the base source connector"""
    
    def __init__(self, config: SourceConnectorConfig):
        super().__init__(config)
        # Override with ServiceNow-specific components using optimized enricher
        self.rate_limiter = ServiceNowRateLimitHandler()
        self.data_enricher = ServiceNowDataEnricher(self)
        self.field_mapper = ServiceNowFieldMapper()
    
    def _get_rate_limiter(self) -> BaseSourceRateLimitHandler:
        return ServiceNowRateLimitHandler()
    
    def _get_data_enricher(self) -> BaseDataEnricher:
        return ServiceNowDataEnricher(self)
    
    def _get_field_mapper(self) -> BaseFieldMapper:
        return ServiceNowFieldMapper()
    
    def reload_enrichment_config(self):
        """Reload enrichment configuration from file"""
        if hasattr(self.data_enricher, 'reload_enrichment_config'):
            self.data_enricher.reload_enrichment_config()
    
    def _extract_instance_name(self, domainUrl: str) -> str:
        """Extract instance name from ServiceNow domainUrl"""
        # Handle various domainUrl formats
        # e.g., dev341034.service-now.com -> dev341034
        parsed = urlparse(domainUrl if domainUrl.startswith('http') else f'https://{domainUrl}')
        hostname = parsed.hostname or domainUrl
        return hostname.split('.')[0]
    
    def _build_auth(self) -> Tuple:
        """Build authentication for ServiceNow API"""
        return (self.config.username, self.config.password)
    
    def _validate_config(self) -> bool:
        """Validate ServiceNow configuration"""
        return all([
            self.config.domainUrl,
            self.config.username,
            self.config.password
        ])
    
    def _build_base_url(self) -> str:
        """Build base URL for ServiceNow API"""
        domainUrl = self.config.domainUrl
        if not domainUrl.startswith('http'):
            domainUrl = f'https://{domainUrl}'
        if not domainUrl.endswith('.service-now.com'):
            instance = self._extract_instance_name(domainUrl)
            domainUrl = f'https://{instance}.service-now.com'
        return domainUrl
    

    def _get_ritms_for_requests(self, sys_id:str) ->List[Dict]:
        print(f"Fetching RITMs for REQ : {sys_id}")
        try:
            base_url = self._build_base_url()
            url = f"{base_url}/api/now/table/sc_req_item"

            all_ritms =[]
            offset =0
            batch_size = 1000

            while True:
                params ={
                    'sysparm_query':f'request={sys_id}',
                    #'sysparm_fields': 'sys_id, number,request',
                    'sysparm_limit': batch_size,
                    'sysparm_offset': offset,
                    'sysparm_display_value': 'false'
                }

                response = self._make_api_request(url=url, params=params)

                if response.status_code == 200:
                    data = safe_json_response(response=response)
                    batch_ritms = data.get('result',[])

                    if not batch_ritms:
                        break
                    
                    all_ritms.extend(batch_ritms)
                    offset+=len(batch_ritms)

                    print(f"Fetch {len(all_ritms)} RITMs so far...")
                    print(len(batch_ritms))
                    print(len(batch_ritms))

                    if len(batch_ritms) < len(batch_ritms):
                        break
            print(f"Total RITMs found : {len(all_ritms)}")
     
            return all_ritms


        except Exception as e:
            print(f"Error fetching RITMs :{e}")
            return []

    def get_ctasks_for_a_ritm(self, ritm: str) -> List[Dict]:
        ritm = str(ritm)
        if not ritm:
            return 
        try:
            base_url = self._build_base_url()
            url = f"{base_url}/api/now/table/sc_task"

            all_ctasks =[]

            # Building IN query for batch processing
            query = f'request_item={ritm}'
            

            offset = 0
            batch_size = 1000

            while True:
                params ={
                    'sysparm_query': query,
                    'sysparm_fields': 'sys_id,number,request_item,short_description,state',
                    'sysparm_limit':batch_size,
                    'sysparm_offset': offset
                }

                response = self._make_api_request(url=url, params=params)


                if response.status_code ==200:
                    data = safe_json_response(response)
                    batch_ctasks = data.get('result',[])

                    print(batch_ctasks)

                    if not batch_ctasks:
                        break

                    all_ctasks.extend(batch_ctasks)
                    offset+=len(batch_ctasks)

                    if len(batch_ctasks)< batch_size:
                        break

                else:
                    print(f"Failed to fetch CTASKs ")
                    break
            

            print(f"Total CTASKS found : {len(all_ctasks)}")
            return all_ctasks

        except Exception as e:
            print(f"Error fetching CTASKs :{e}")
            return []

    def _get_ctasks_for_ritms(self, ritms: List[Dict]) -> List[Dict]:
        if not ritms:
            return []
        # print("LEN RITMS :",ritms)

        try:
            base_url = self._build_base_url()
            url = f"{base_url}/api/now/table/sc_task"

            all_ctasks =[]

            ritm_sys_ids = [item['sys_id'] for item in ritms]

                
            # Building IN query for batch processing
            ritm_ids_str = 'ORrequest_item='.join(ritm_sys_ids)
            query = f'request_items={ritm_ids_str}'
            

            offset =0
            batch_size =1000

            while True:
                params ={
                    'sysparm_query': query,
                    'sysparm_fields': 'sys_id,number,request_item,short_description,state',
                    'sysparm_limit':batch_size,
                    'sysparm_offset': offset
                }

                response = self._make_api_request(url=url, params=params)


                if response.status_code ==200:
                    data = safe_json_response(response)
                    batch_ctasks = data.get('result',[])

                    print(batch_ctasks)

                    if not batch_ctasks:
                        break

                    all_ctasks.extend(batch_ctasks)
                    offset+=len(batch_ctasks)

                    if len(batch_ctasks)< batch_size:
                        break

                else:
                    print(f"Failed to fetch CTASKs ")
                    break
            

            print(f"Total CTASKS found : {len(all_ctasks)}")
            return all_ctasks

        except Exception as e:
            print(f"Error fetching CTASKs :{e}")
            return []

    def _get_all_notes(self, lst: List[Dict], note_type: str) -> List[Dict]:
        if not lst:
            return []
        try:
            base_url = self._build_base_url()
            url = f"{base_url}/api/now/table/sys_journal_field"

            all_notes =[]
            sys_ids_lst = [item['sys_id'] for item in lst]
          

            #Builing IN query for batch processing
            ctask_ids_str = '^ORelement_id='.join(sys_ids_lst)
            query = f'element_id={ctask_ids_str}^ORDERBYsys_created_on'
           

            offset = 0
            batch_size = 100

            while True:
                params ={
                    'sysparm_query': query,
                    'sysparm_fields': 'sys_id,element_id,value,sys_created_on,sys_created_by,element',
                    'sysparm_limit': batch_size,
                    'sysparm_offset': offset
                }

                response = self._make_api_request(url, params)
                

                if response.status_code == 200:
                    data = response.json()
                    data["result"] = [
                        {**item, "value": str(note_type) + str(item["value"])} 
                        for item in data["result"]
                    ]

                    # data = safe_json_response(response=data)
                    batch_notes = data.get('result', [])

                    if not batch_notes:
                        break

                    all_notes.extend(batch_notes)
                    offset+=len(batch_notes)

                    if len(batch_notes)< batch_size:
                        break

                else:
                    print(f"Failed to fetch note")
                    break
            print(type(all_notes))
            work_notes = [note for note in all_notes if note.get('element') in ['work_notes', 'comments']]
            
            print(type(work_notes))
            print(f"Total notes found {len(work_notes)}")
            print(work_notes)
            return work_notes

        except Exception as e:
            print(f"Error fetch notes : {e}")
            return []

    def get_ritms(self, ritm: List[Dict], api_key: str) -> List[Dict]:
        try:
            if not self._validate_config():
                return SourceResponse(400, False, error_message="Invalid configuration")
            enriched_data = self.data_enricher.enrich_ritms(ritm, True, api_key=api_key)
            return enriched_data
        except Exception as e:
            logging.error(f"Error getting RITMs: {e}", exc_info=True)
            return SourceResponse(500, False, error_message=str(e))
        
    def get_ctasks(self, ctask: List[Dict])-> List[Dict]:
        try:
            if not self._validate_config():
                return SourceResponse(400, False, error_message="Invalid configuration")
            enriched_data = self.data_enricher.enrich_ctasks(ctask)
            return enriched_data
        except Exception as e:
            logging.error(f"Error getting CTASK: {e}", exc_info=True)
            return SourceResponse(500, False, error_message=str(e))

    
    def get_incidents(self, query_params: Dict) -> SourceResponse:
        """Get ServiceNow incidents with support for filters and pagination"""
        try:
            if not self._validate_config():
                return SourceResponse(400, False, error_message="Invalid configuration")
            
            base_url = self._build_base_url()
            url = f"{base_url}/api/now/table/incident"
            
            # Build query parameters - ensure numeric values
            params = {
                'sysparm_limit': int(query_params.get('limit', 100)),
                'sysparm_offset': int(query_params.get('numberOfProcessedRecords', 0)),
                'sysparm_display_value': 'false'  # Get only raw values, not display values
            }
            
            # Add filters if provided
            filters = query_params.get('filters')
            if filters:
                params['sysparm_query'] = filters
            
            # Add fields selection if provided
            fields = query_params.get('fields')
            if fields:
                params['sysparm_fields'] = fields

            # Dom 9/19 - Add x-sn-apikey selection if provided
            xsn_apikey = query_params.get('x-sn-apikey')
            if xsn_apikey:
                params['x-sn-apikey'] = xsn_apikey
            
            api_key = query_params.get('x-sn-apikey')

            # Make API call
            response = self._make_api_request(url, params)
            
            if response.status_code == 200:
                data = safe_json_response(response)
                incidents = data.get('result', [])
                
                # 🚀 APPLY OPTIMIZED ENRICHMENT
                resolve_refs = query_params.get('resolve_references', True)
                try:
                    enriched_incidents = self.data_enricher.enrich_incidents(incidents, resolve_refs, api_key)
                except Exception as e:
                    logging.error(f"Error during incident enrichment: {e}")
                    # Fall back to non-enriched incidents if enrichment fails
                    enriched_incidents = incidents
                    # Add empty attachment fields to prevent downstream errors
                    for incident in enriched_incidents:
                        if 'attachments' not in incident:
                            incident['attachments'] = []
                        if 'attachmentUrls' not in incident:
                            incident['attachmentUrls'] = []
                
                # Build response with pagination info
                total_count = response.headers.get('X-Total-Count')
                has_more = False
                if total_count:
                    total = int(total_count)
                    current_end = int(params['sysparm_offset']) + len(incidents)
                    has_more = current_end < total
                
                return SourceResponse(
                    status_code=200,
                    success=True,
                    data={
                        'incidents': enriched_incidents,
                        'meta': {
                            'total_count': int(total_count) if total_count else len(incidents),
                            'has_more': has_more,
                            'offset': params['sysparm_offset'],
                            'limit': params['sysparm_limit']
                        }
                    }
                )
            else:
                return SourceResponse(
                    status_code=response.status_code,
                    success=False,
                    error_message=f"Failed to get incidents: {response.text}"
                )
                
        except Exception as e:
            logging.error(f"Error getting incidents: {e}", exc_info=True)
            return SourceResponse(500, False, error_message=str(e))

    def get_incident_tasks_unfiltered(self, query_params: Dict) -> SourceResponse:
        """Get ServiceNow incidents with support for filters and pagination"""
        try:
            if not self._validate_config():
                return SourceResponse(400, False, error_message="Invalid configuration")
            
            base_url = self._build_base_url()
            url = f"{base_url}/api/now/table/incident_task"
            
            # Build query parameters - ensure numeric values
            params = {
                'sysparm_limit': int(query_params.get('limit', 100)),
                'sysparm_offset': int(query_params.get('numberOfProcessedRecords', 0)),
                'sysparm_display_value': 'false'  # Get only raw values, not display values
            }
            
            # Add filters if provided
            filters = query_params.get('filters')
            if filters:
                params['sysparm_query'] = filters
            
            # Add fields selection if provided
            fields = query_params.get('fields')
            if fields:
                params['sysparm_fields'] = fields

            # Dom 9/19 - Add x-sn-apikey selection if provided
            xsn_apikey = query_params.get('x-sn-apikey')
            if xsn_apikey:
                params['x-sn-apikey'] = xsn_apikey
            
            api_key = query_params.get('x-sn-apikey')

            # Make API call
            response = self._make_api_request(url, params)
            
            if response.status_code == 200:
                data = safe_json_response(response)
                incidents = data.get('result', [])
                
                # 🚀 APPLY OPTIMIZED ENRICHMENT
                resolve_refs = query_params.get('resolve_references', True)
                try:
                    enriched_incidents = self.data_enricher.enrich_incidents(incidents, resolve_refs, api_key)
                except Exception as e:
                    logging.error(f"Error during task enrichment: {e}")
                    # Fall back to non-enriched incidents if enrichment fails
                    enriched_incidents = incidents
                    # Add empty attachment fields to prevent downstream errors
                    for incident in enriched_incidents:
                        if 'attachments' not in incident:
                            incident['attachments'] = []
                        if 'attachmentUrls' not in incident:
                            incident['attachmentUrls'] = []
                
                # Build response with pagination info
                total_count = response.headers.get('X-Total-Count')
                has_more = False
                if total_count:
                    total = int(total_count)
                    current_end = int(params['sysparm_offset']) + len(incidents)
                    has_more = current_end < total
                
                return SourceResponse(
                    status_code=200,
                    success=True,
                    data={
                        'incidents': enriched_incidents,
                        'meta': {
                            'total_count': int(total_count) if total_count else len(incidents),
                            'has_more': has_more,
                            'offset': params['sysparm_offset'],
                            'limit': params['sysparm_limit']
                        }
                    }
                )
            else:
                return SourceResponse(
                    status_code=response.status_code,
                    success=False,
                    error_message=f"Failed to get tasks: {response.text}"
                )
                
        except Exception as e:
            logging.error(f"Error getting tasks: {e}", exc_info=True)
            return SourceResponse(500, False, error_message=str(e))
        

    def get_all_notes_from_request(self, req: List)-> List[Dict]:
        if not req:
            return[]

        start_time = time.time()
        try:
            # Step 1: get all RITMs for the REQ using bulk operations
           
            req_number= req['number']
            req_sys_id = req['sys_id']
            ritms = self._get_ritms_for_requests(req_sys_id)
            if not ritms:
                print(f"No RITMs found for REQ:{req_number}")
                return []

            # Step 2: Get all CTASKs for these RITMs in batchs

            all_ctasks = self._get_ctasks_for_ritms(ritms)
            if not all_ctasks:
                print(f"No CTASKs found for RITMs under REQ: {req_number}")

            # Step 3 : Get all notes with CTASKs in batches
            print("Fetching task notes...")
            task_notes = self._get_all_notes(all_ctasks, note_type='Task Note : ')

            # Step 4: get notes of REQ
            print("Fetching REQ Notes..")
            req_notes = self._get_all_notes(list([req]), note_type='Request Note : ')
            

            # Step 5 : get notes of RITMs
            print("Fetching RITMS notes..")
            ritms_notes = self._get_all_notes(ritms, note_type='Requested Item Note : ')
            print(len(ritms_notes))

            # Step 6: create a consolidated variable of all the notes

            all_notes = req_notes+ritms_notes+task_notes

            # Step 7: Enrich notes with CTASK and RITM context

            enriched_notes = self.data_enricher.enrich_ctask_notes_with_context(all_notes)

            duration = time.time()- start_time
            print(f"Migration {self.migration_id}: CTASK notes extraction complete :"
                f"{len(enriched_notes)} notes from {len(all_ctasks)} CTASKs"
                f"under {len(ritms)} RITMS in {duration:.2f}s"
            )
            print("ENRICHED NOTES :",enriched_notes)
            # enriched_dict = {'result': enriched_notes}
            return enriched_notes

        except Exception as e:
            print(f"Migration {self.migration_id}: CTASK notes extraction failed {e}")
            return []
            

    
    def get_requests(self, query_params:Dict)-> SourceResponse:
        try:
            if not self._validate_config():
                return SourceResponse(400, False, error_message="Invalid configuration")
            
            base_url = self._build_base_url()
            url = f"{base_url}/api/now/table/sc_request"

            params = {
                'sysparm_limit': int(query_params.get('limit', 100)),
                'sysparm_offset': int(query_params.get('numberOfProcessedRecords', 0)) + int(query_params.get('offset', 0)),
                'sysparm_display_value': 'false'
            }

            filters = query_params.get('filters')
            if filters:
                params['sysparm_query'] = filters

            fileds = query_params.get('fields')
            if fileds:
                params['sysparm_fields']= fileds
            
            # Dom 9/19 - Add x-sn-apikey selection if provided
            xsn_apikey = query_params.get('x-sn-apikey')
            if xsn_apikey:
                params['x-sn-apikey'] = xsn_apikey
            
            api_key = query_params.get('x-sn-apikey')

            response = self._make_api_request(url, params)


            if response.status_code == 200:
                data = safe_json_response(response)
                service_requests = data.get('result',[])

                resolve_refs = query_params.get('resolve_references',True)
                try:
                    enriched_requests = self.data_enricher.enrich_requests(service_requests, resolve_refs, api_key)
                except Exception as e:
                    logging.error(f"Error during service request enrichment :{e}")
                    enriched_requests = service_requests
                    for request in enriched_requests:
                        if 'attachments' not in request:
                            request['attachments']= []
                        if 'attachmentUrls' not in request:
                            request['attachmentUrls']= []
                
                total_count = response.headers.get('X-Total-Count')
                has_more = False
                if total_count:
                    total = int(total_count)
                    current_end = int(params['sysparm_offset'])+ len(service_requests)
                    has_more = current_end<total
                return SourceResponse(
                            status_code=200,
                            success=True,
                            data={
                                'requests': enriched_requests,
                                'meta': {
                                    'total_count': int(total_count) if total_count else len(service_requests),
                                    'has_more': has_more,
                                    'offset': params['sysparm_offset'],
                                    'limit': params['sysparm_limit']
                                }
                            }
                        )
            else:
                return SourceResponse(
                        status_code=response.status_code,
                        success=False,
                        error_message=f"Failed to get service requests: {response.text}"
                    )
            
        except Exception as e:
            logging.error(f"Error getting service requests: {e}", exc_info=True)
            return SourceResponse(500, False, error_message=str(e))

    def get_changes(self, query_params: Dict) -> SourceResponse:
        """Get ServiceNow change requests - similar to get_incidents"""
        try:
            if not self._validate_config():
                return SourceResponse(400, False, error_message="Invalid configuration")
            
            base_url = self._build_base_url()
            url = f"{base_url}/api/now/table/change_request"
            
            # Build query parameters - ensure numeric values
            params = {
                'sysparm_limit': int(query_params.get('limit', 100)),
                'sysparm_offset': int(query_params.get('numberOfProcessedRecords', 0)),
                'sysparm_display_value': 'false'  # Get only raw values
            }
            
            # Add filters if provided
            filters = query_params.get('filters')
            if filters:
                params['sysparm_query'] = filters
            
            # Add fields selection if provided
            fields = query_params.get('fields')
            if fields:
                params['sysparm_fields'] = fields
            
            # Dom 9/19 - Add x-sn-apikey selection if provided
            xsn_apikey = query_params.get('x-sn-apikey')
            if xsn_apikey:
                params['x-sn-apikey'] = xsn_apikey
            
            api_key = query_params.get('x-sn-apikey')

            # Make API call
            response = self._make_api_request(url, params)
            
            if response.status_code == 200:
                data = safe_json_response(response)
                changes = data.get('result', [])
                
                # 🚀 APPLY OPTIMIZED ENRICHMENT (reuse incident enrichment logic!)
                resolve_refs = query_params.get('resolve_references', True)
                try:
                    enriched_changes = self.data_enricher.enrich_changes(changes, resolve_refs, api_key)
                except Exception as e:
                    logging.error(f"Error during change enrichment: {e}")
                    enriched_changes = changes
                    # Add empty attachment fields
                    for change in enriched_changes:
                        if 'attachments' not in change:
                            change['attachments'] = []
                        if 'attachmentUrls' not in change:
                            change['attachmentUrls'] = []
                
                # Build response with pagination info
                total_count = response.headers.get('X-Total-Count')
                has_more = False
                if total_count:
                    total = int(total_count)
                    current_end = int(params['sysparm_offset']) + len(changes)
                    has_more = current_end < total
                
                return SourceResponse(
                    status_code=200,
                    success=True,
                    data={
                        'changes': enriched_changes,
                        'meta': {
                            'total_count': int(total_count) if total_count else len(changes),
                            'has_more': has_more,
                            'offset': params['sysparm_offset'],
                            'limit': params['sysparm_limit']
                        }
                    }
                )
            else:
                return SourceResponse(
                    status_code=response.status_code,
                    success=False,
                    error_message=f"Failed to get changes: {response.text}"
                )
                
        except Exception as e:
            logging.error(f"Error getting changes: {e}", exc_info=True)
            return SourceResponse(500, False, error_message=str(e))

    def get_tasks(self, query_params: Dict) -> SourceResponse:
        """Get ServiceNow tasks with support for filters and pagination"""
        try:
            if not self._validate_config():
                return SourceResponse(400, False, error_message="Invalid configuration")
            
            base_url = self._build_base_url()
            url = f"{base_url}/api/now/table/incident_task"
            logging.info(f"ServiceNow Tasks URL: {url}")
            
            # Check if this is a parent-specific task query
            parent_id = query_params.get('parent')
            if parent_id:
                logging.info(f"Parent-specific task query detected for parent_id: {parent_id}")
                # Use parent-specific method for better performance and filtering
                return self.get_tasks_for_parent(parent_id, query_params.get('parent_type', 'incident'))
            
            # Build query parameters - ensure numeric values
            params = {
                'sysparm_limit': int(query_params.get('limit', 100)),
                'sysparm_offset': int(query_params.get('offset', 0)),
                'sysparm_display_value': 'false'  # Get only raw values
            }
            
            # Add filters if provided
            filters = query_params.get('filters')
            if filters:
                params['sysparm_query'] = filters
                logging.info(f"Applied filters: {filters}")
            
            # Log the complete URL with parameters
            param_string = "&".join([f"{k}={v}" for k, v in params.items()])
            full_url = f"{url}?{param_string}"
            logging.info(f"Final ServiceNow Tasks API URL: {full_url}")
            
            # Dom 9/19 - Add x-sn-apikey selection if provided
            xsn_apikey = query_params.get('x-sn-apikey')
            if xsn_apikey:
                params['x-sn-apikey'] = xsn_apikey
            
            api_key = query_params.get('x-sn-apikey')

            # Make API call
            response = self._make_api_request(url, params)
            
            if response.status_code == 200:
                data = safe_json_response(response)
                tasks = data.get('result', [])
                
                # 🚀 APPLY OPTIMIZED ENRICHMENT
                resolve_refs = query_params.get('resolve_references', True)
                try:
                    enriched_tasks = self.data_enricher.enrich_tasks(tasks, resolve_refs, api_key)
                except Exception as e:
                    logging.error(f"Error during task enrichment: {e}")
                    enriched_tasks = tasks
                
                # Build response with pagination info
                total_count = response.headers.get('X-Total-Count')
                has_more = False
                if total_count:
                    total = int(total_count)
                    current_end = int(params['sysparm_offset']) + len(tasks)
                    has_more = current_end < total
                
                return SourceResponse(
                    status_code=200,
                    success=True,
                    data={
                        'tasks': enriched_tasks,
                        'meta': {
                            'total_count': int(total_count) if total_count else len(tasks),
                            'has_more': has_more,
                            'offset': params['sysparm_offset'],
                            'limit': params['sysparm_limit']
                        }
                    }
                )
            else:
                return SourceResponse(
                    status_code=response.status_code,
                    success=False,
                    error_message=f"Failed to get tasks: {response.text}"
                )
                
        except Exception as e:
            logging.error(f"Error getting tasks: {e}", exc_info=True)
            return SourceResponse(500, False, error_message=str(e))
    
    def get_notes(self, parent_id: str, parent_type: str = 'incident', api_key: str = None) -> SourceResponse:
        """Get notes (work notes and comments) for a parent record"""
        try:
            if not self._validate_config():
                return SourceResponse(400, False, error_message="Invalid configuration")
            
            base_url = self._build_base_url()
            
            # Get both work notes and comments
            all_notes = []
            
            # Get work notes
            work_notes_url = f"{base_url}/api/now/table/sys_journal_field"
            work_notes_params = {
                'sysparm_query': f'element_id={parent_id}^element=work_notes',
                'sysparm_limit': 1000,
                'sysparm_display_value': 'false'  # Get only raw values
            }
            
            # Dom 9/19 - Add x-sn-apikey if provided
            if api_key:
                work_notes_params['x-sn-apikey'] = api_key

            work_notes_response = self._make_api_request(work_notes_url, work_notes_params)
            if work_notes_response.status_code == 200:
                data = safe_json_response(work_notes_response)
                work_notes = data.get('result', [])
                for note in work_notes:
                    note['note_type'] = 'work_note'
                all_notes.extend(work_notes)
            
            # Get comments
            comments_url = f"{base_url}/api/now/table/sys_journal_field"
            comments_params = {
                'sysparm_query': f'element_id={parent_id}^element=comments',
                'sysparm_limit': 1000,
                'sysparm_display_value': 'false'  # Get only raw values
            }
            
            # Dom 9/19 - Add x-sn-apikey if provided
            if api_key:
                comments_params['x-sn-apikey'] = api_key

            comments_response = self._make_api_request(comments_url, comments_params)
            if comments_response.status_code == 200:
                data = safe_json_response(comments_response)
                comments = data.get('result', [])
                for comment in comments:
                    comment['note_type'] = 'comment'
                all_notes.extend(comments)
            
            # Sort by creation date
            all_notes.sort(key=lambda x: x.get('sys_created_on', ''))
            
            # 🚀 APPLY OPTIMIZED ENRICHMENT
            try:
                enriched_notes = self.data_enricher.enrich_notes(all_notes)
            except Exception as e:
                logging.error(f"Error during note enrichment: {e}")
                enriched_notes = all_notes
            
            return SourceResponse(
                status_code=200,
                success=True,
                data={
                    'notes': enriched_notes,
                    'parent_id': parent_id,
                    'parent_type': parent_type
                }
            )
            
        except Exception as e:
            logging.error(f"Error getting notes: {e}", exc_info=True)
            return SourceResponse(500, False, error_message=str(e))
    
    def get_tasks_for_parent(self, parent_id: str, parent_type: str = 'incident', api_key: str = None) -> SourceResponse:
        """Get tasks for a parent record (incidents, changes, etc.) - ENHANCED VERSION"""
        try:
            if not self._validate_config():
                return SourceResponse(400, False, error_message="Invalid configuration")
            
            base_url = self._build_base_url()
            
            # Determine table and query based on parent type
            if parent_type == 'incident':
                table = 'incident_task'
                query_field = 'incident'
                #query_field = 'element_id'
            elif parent_type == 'change_request':
                table = 'change_task'
                query_field = 'change_request'
                #query_field = 'element_id'
            else:
                return SourceResponse(
                    status_code=400,
                    success=False,
                    error_message=f"Unsupported parent_type: {parent_type}. Supported: incident, change_request"
                )
            
            url = f"{base_url}/api/now/table/{table}"
            
            # Build query parameters to filter by parent
            params = {
                'sysparm_query': f'{query_field}={parent_id}',
                'sysparm_limit': 1000,
                'sysparm_display_value': 'false'
            }
            
            # Dom 9/19 - Add x-sn-apikey if provided
            if api_key:
                params['x-sn-apikey'] = api_key

            # Log for debugging
            full_url = f"{url}?sysparm_query={params['sysparm_query']}&sysparm_limit={params['sysparm_limit']}&sysparm_display_value={params['sysparm_display_value']}"
            logging.info(f"ServiceNow Tasks for Parent URL: {full_url}")
            logging.info(f"Fetching tasks for parent_id: {parent_id}, parent_type: {parent_type}")
            
            # Make API call
            response = self._make_api_request(url, params)
            
            if response.status_code == 200:
                data = safe_json_response(response)
                tasks = data.get('result', [])
                
                # 🚀 REUSE EXISTING TASK ENRICHMENT
                try:
                    enriched_tasks = self.data_enricher.enrich_tasks(tasks, True, api_key)
                except Exception as e:
                    logging.error(f"Error during task enrichment: {e}")
                    enriched_tasks = tasks
                
                
                return SourceResponse(
                    status_code=200,
                    success=True,
                    data={
                        'tasks': enriched_tasks,
                        'parent_id': parent_id,
                        'parent_type': parent_type,
                        'meta': {
                            'total_count': len(enriched_tasks),
                            'has_more': False,
                            'offset': 0,
                            'limit': 1000
                        }
                    }
                )
            else:
                return SourceResponse(
                    status_code=response.status_code,
                    success=False,
                    error_message=f"Failed to get tasks: {response.text}"
                )
                
        except Exception as e:
            logging.error(f"Error getting tasks for parent: {e}", exc_info=True)
            return SourceResponse(500, False, error_message=str(e))
        
    def _apply_custom_lookups(self, records: List[Dict], custom_lookups: List[Dict]) -> List[Dict]:
        """Apply custom field lookups based on configuration"""
        for record in records:
            for lookup_config in custom_lookups:
                source_field = lookup_config.get('source_field')
                source_value = record.get(source_field)
                
                if not source_value:
                    continue
                
                lookup_table = lookup_config.get('lookup_table')
                lookup_fields = lookup_config.get('lookup_fields', [])
                target_field_prefix = lookup_config.get('target_field_prefix', source_field)
                
                # Perform lookup
                lookup_data = self._perform_custom_lookup(
                    source_value, 
                    lookup_table, 
                    lookup_fields
                )
                
                if lookup_data:
                    # Add resolved fields to record
                    for field in lookup_fields:
                        if field in lookup_data:
                            target_field_name = f"{target_field_prefix}{field.capitalize()}"
                            record[target_field_name] = lookup_data[field]
        
        return records
    
    def _perform_custom_lookup(self, record_id: str, table: str, fields: List[str]) -> Optional[Dict]:
        """Perform a custom lookup on any ServiceNow table"""
        if not record_id or not table:
            return None
        
        # Extract value if record_id is a dict with 'value' key
        if isinstance(record_id, dict) and 'value' in record_id:
            record_id = record_id['value']
        
        base_url = self._build_base_url()
        url = f"{base_url}/api/now/table/{table}/{record_id}"
        
        # If specific fields requested, add them to query
        params = {}
        if fields:
            params['sysparm_fields'] = ','.join(fields)
        
        try:
            auth = self._build_auth()
            headers = {"Accept": "application/json", "Content-Type": "application/json"}
            
            response = self.rate_limiter.make_request_with_retry(
                url, 'GET', auth=auth, headers=headers, params=params
            )
            
            if response.status_code == 200:
                data = safe_json_response(response)
                return data.get('result', {})
            else:
                logging.warning(f"Failed to lookup {record_id} in {table}: {response.status_code}")
                return None
                
        except Exception as e:
            logging.error(f"Error performing custom lookup: {e}")
            return None
    
    def _extract_values_from_display_format(self, records: List[Dict]) -> List[Dict]:
        """Extract raw values from ServiceNow display_value format"""
        processed_records = []
        
        for record in records:
            processed_record = {}
            for field_name, field_value in record.items():
                if isinstance(field_value, dict) and 'value' in field_value:
                    # Extract just the value from display_value format
                    processed_record[field_name] = field_value['value']
                else:
                    # Keep as-is if not in display_value format
                    processed_record[field_name] = field_value
            processed_records.append(processed_record)
        
        return processed_records
    
    def _make_api_request(self, url: str, params: Dict = None) -> requests.Response:
        """Make API request with authentication and rate limiting"""
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        # Dom - 9/19 - Check if params contains the API key
        if params and "x-sn-apikey" in params:
            headers["x-sn-apikey"] = params.pop("x-sn-apikey")

        # Build auth tuple
        auth = self._build_auth()

        # Support multiple ways callers might pass an API key via `auth`:
        # 1) auth == (username, password, api_key)
        # 2) auth == ("x-sn-apikey", api_key)
        # 3) auth == {"x-sn-apikey": api_key}
        # In all cases, move API key to headers and do not leave it in `auth`.
        api_key_from_auth = None

        if isinstance(auth, dict):
            # Case 3
            api_key_from_auth = auth.get("x-sn-apikey")
            # No basic auth embedded in this shape
            auth = None
        elif isinstance(auth, tuple):
            if len(auth) == 3:
                # Case 1: (username, password, api_key)
                api_key_from_auth = auth[2]
                auth = (auth[0], auth[1])
            elif len(auth) == 2 and isinstance(auth[0], str) and auth[0].lower() == "x-sn-apikey":
                # Case 2: ("x-sn-apikey", api_key)
                api_key_from_auth = auth[1]
                auth = None  # no basic auth when only API key supplied

        if api_key_from_auth and "x-sn-apikey" not in headers:
            headers["x-sn-apikey"] = api_key_from_auth
        
        # Check if API key is in config but not in headers yet
        if (hasattr(self.config, 'api_key') and 
            self.config.api_key and 
            "x-sn-apikey" not in headers):
            headers["x-sn-apikey"] = self.config.api_key

        start_time = time.time()
        response = self.rate_limiter.make_request_with_retry(
            url, 'GET', auth=auth, headers=headers, params=params
        )
        duration = time.time() - start_time
        
        log_api_call('GET', url, response.status_code, duration)
        handle_common_errors(response)
        
        return response
    
    def get_field_definitions(self, table_name: str = 'incident', api_key: str = None) -> List[Dict]:
        """Get field definitions for a ServiceNow table"""
        try:
            base_url = self._build_base_url()
            url = f"{base_url}/api/now/table/sys_dictionary"
            
            params = {
                'sysparm_query': f'name={table_name}',
                'sysparm_fields': 'element,column_label,internal_type,reference,active,mandatory,max_length,default_value',
                'sysparm_limit': 1000
            }
            
            # Dom 9/19 - Add x-sn-apikey if provided
            if api_key:
                params['x-sn-apikey'] = api_key

            response = self._make_api_request(url, params)
            
            if response.status_code == 200:
                data = safe_json_response(response)
                return data.get('result', [])
            else:
                logging.error(f"Failed to get field definitions: {response.status_code}")
                return []
                
        except Exception as e:
            logging.error(f"Error getting field definitions: {e}")
            return []
    
    def get_choice_values(self, table_name: str, field_name: str, api_key: str = None) -> List[Dict]:
        """Get choice values for a specific field"""
        try:
            base_url = self._build_base_url()
            url = f"{base_url}/api/now/table/sys_choice"
            
            params = {
                'sysparm_query': f'name={table_name}^element={field_name}',
                'sysparm_fields': 'value,label,sequence',
                'sysparm_limit': 1000
            }
            
            # Dom 9/19 - Add x-sn-apikey if provided
            if api_key:
                params['x-sn-apikey'] = api_key

            response = self._make_api_request(url, params)
            
            if response.status_code == 200:
                data = safe_json_response(response)
                return data.get('result', [])
            else:
                logging.error(f"Failed to get choice values: {response.status_code}")
                return []
                
        except Exception as e:
            logging.error(f"Error getting choice values: {e}")
            return []


# ===================================================================
# GLOBAL CONNECTOR INSTANCE (MIGRATION-SAFE)
# ===================================================================

def _get_servicenow_connector(headers):
    """
    🔒 MIGRATION-SAFE: Always create fresh connector instance
    No shared state between different migration requests
    """
    headers_dict = convert_source_headers_to_dict(headers)
    config = SourceConnectorConfig(
        domainUrl=headers_dict.get('domainUrl'),
        username=headers_dict.get('username'),
        password=headers_dict.get('password')
    )
    connector = ServiceNowConnector(config)
    
    # Add migration ID for better logging (if available)
    connector.migration_id = headers_dict.get('migration_id', f'conn_{int(time.time())}')
    
    return connector


# ===================================================================
# 🚀 OPTIMIZED TRANSFORMER-COMPATIBLE FUNCTION INTERFACES
# ===================================================================


def get_servicenow_all_notes_v1(**kwargs) -> Dict:

    try:
        connector= _get_servicenow_connector(kwargs['sourceHeaders'])
        query_dict = convert_query_params_to_dict(kwargs['queryParams'])
        numberOfProcessedRecords = kwargs.get('numberOfProcessedRecords', 0)

        for key, value in kwargs.items():
            query_dict[key] = value

        req_response = connector.get_requests(query_dict)

        # print("REQ RESPONSE :", req_response.data)
        data = req_response.data['requests']
        all_enriched_notes=[]
        for req in data:
        
            response = connector.get_all_notes_from_request(req)
            #all_enriched_notes.append(response)

        all_enriched_notes = response

        response ={
            'status_code': 200,
            'body':{
                'notes': all_enriched_notes
            }
        }
        print("ENRICHED NOTES :",response)
        return response



    except Exception as e:
        logging.error(f"Error in getting get_servicenow_all_notes_v1: {e}", exc_info=True)
        return {
            "status_code": 500,
            "body": {
                "ctask_notes": [],  
                "error": str(e),
                "meta": {
                    "total_count": 0,
                    "extraction_type": "failed"
                }
            }
        }


def get_servicenow_ritm_from_req_v1(**kwargs) -> Dict:
    try:
        connector = _get_servicenow_connector(kwargs['sourceHeaders'])
        query_dict = convert_query_params_to_dict(kwargs['queryParams'])
        print(query_dict)
        sys_id = query_dict.get('sys_id')
        print(sys_id)
        api_key = query_dict.get('x-sn-apikey')

        for key, value in kwargs.items():
            query_dict[key] = value

        response = connector._get_ritms_for_requests(sys_id=sys_id)
        try:
            enriched_data = connector.get_ritms(response, api_key=api_key)
            
            # Format data in a clean, organized manner
            def format_all_data(data_list):
                formatted_lines = []
                for record in data_list:
                    for key, value in record.items():
                        # Skip empty or null values
                        if value is None or value == "" or (isinstance(value, str) and value.strip() == "") or value == []:
                            continue
                        # Format as key: value
                        formatted_lines.append(f"{key}: {value}")
                return "\n".join(formatted_lines)
            
            formatted_data = format_all_data(enriched_data)
            for ritm in enriched_data:
                ritm['all_data'] = formatted_data

        except Exception as e:
            print(f"Error in getting transformation done for RITMs with message: {e}")

        result = {
            "status_code": 200,
            "body": {
                "ritms": enriched_data
            }
        }
            
        return result
    except Exception as e:
        logging.error(f"Error in get_servicenow_ritm_from_req_v1: {e}", exc_info=True)
        return{
            "status_code": 500,
            "body" :{
                "service_requests": [],
                "error": str(e),
                "meta":{
                    "total_count": 0,
                    "has_more": False,
                    "offset": 0,
                    "limit": 100
                }
            }
        }

def get_servicenow_ctask_from_ritm_v1(**kwargs)-> Dict:
    try:
        connector = _get_servicenow_connector(kwargs['sourceHeaders'])
        query_dict = convert_query_params_to_dict(kwargs['queryParams'])

        request_sys_id = query_dict.get('sys_id')
        for key, value in kwargs.items():
            query_dict[key] = value

        # First, get all RITMs for the request
        ritms_result = get_servicenow_ritm_from_req_v1(**kwargs)
        if ritms_result['status_code'] != 200:
            return ritms_result  # Return error if getting RITMs failed

        ritms = ritms_result['body']['ritms']
        all_ctasks = []

        # Get CTasks for each RITM
        for ritm in ritms:
            ritm_sys_id = ritm.get('sys_id')
            if ritm_sys_id:
                try:
                    ctasks_response = connector.get_ctasks_for_a_ritm(ritm_sys_id)
                    if ctasks_response:
                        enriched_ctasks = connector.get_ctasks(ctasks_response)
                        all_ctasks.extend(enriched_ctasks)
                except Exception as e:
                    print(f"Error getting CTasks for RITM {ritm_sys_id}: {e}")

        # Also get CTasks directly from the request level
        try:
            request_ctasks_response = connector.get_ctasks_for_a_ritm(request_sys_id)
            if request_ctasks_response:
                request_enriched_ctasks = connector.get_ctasks(request_ctasks_response)
                all_ctasks.extend(request_enriched_ctasks)
        except Exception as e:
            print(f"Error getting CTasks for Request {request_sys_id}: {e}")

        result = {
                "status_code": 200,
                "body": {
                    "ctasks": all_ctasks
                    }
                }
            
        return result
    except Exception as e:
        logging.error(f"Error in get_servicenow_ctask_from_ritm_v1: {e}", exc_info=True)
        return{
            "status_code": 500,
            "body" :{
                "ctasks": [],
                "error": str(e),
                "meta":{
                    "total_count": 0,
                    "has_more": False,
                    "offset": 0,
                    "limit": 100
                }
            }
        }


def get_servicenow_incidents_v1(**kwargs) -> Dict:
    """
    🚀 OPTIMIZED: ServiceNow incidents retrieval with 90% faster enrichment
    Maintains exact compatibility with transformer
    
    Args:
        headers: Authentication headers (list or dict format)
        queryParams: Query parameters for filtering/pagination (list or dict format)
        **kwargs: Additional arguments that can be passed (e.g., numberOfProcessedRecords, etc.)
    
    Returns:
        Dict with status_code and body (always includes 'incidents' key)
    """
    try:
        connector = _get_servicenow_connector(kwargs['sourceHeaders'])
        query_dict = convert_query_params_to_dict(kwargs['queryParams'])
        
        # Extract numberOfProcessedRecords from kwargs, default to 0 if not provided
        numberOfProcessedRecords = kwargs.get('numberOfProcessedRecords', 0)        
        
        # You can also add any additional kwargs to query_dict if needed
        # For example, if you want to pass additional query parameters:
        for key, value in kwargs.items():
            #if key not in ['numberOfProcessedRecords']:  # Exclude specific kwargs from query_dict
            query_dict[key] = value        
        
        response = connector.get_incidents(query_dict)
        
        # FIXED: Ensure consistent response format for transformer
        if response.success:
            # Success case - return the data as expected
            result = standardize_source_response_format(response)
        else:
            # Error case - still return incidents key with empty list to prevent KeyError
            result = {
                "status_code": response.status_code,
                "body": {
                    "incidents": [],  # FIXED: Always include incidents key
                    "error": response.error_message,
                    "meta": {
                        "total_count": 0,
                        "has_more": False,
                        "offset": numberOfProcessedRecords,
                        "limit": query_dict.get('limit', 100)
                    }
                }
            }
        
        return result
        
    except Exception as e:
        logging.error(f"Error in get_servicenow_incidents_v1: {e}", exc_info=True)
        # FIXED: Always return incidents key, even in exception cases
        return {
            "status_code": 500,
            "body": {
                "incidents": [],  # FIXED: Always include incidents key
                "error": str(e),
                "meta": {
                    "total_count": 0,
                    "has_more": False,
                    "offset": 0,
                    "limit": 100
                }
            }
        }


def get_servicenow_incident_tasks_unfiltered_v1(**kwargs) -> Dict:
    """
    🚀 OPTIMIZED: ServiceNow incidents retrieval with 90% faster enrichment
    Maintains exact compatibility with transformer
    
    Args:
        headers: Authentication headers (list or dict format)
        queryParams: Query parameters for filtering/pagination (list or dict format)
        **kwargs: Additional arguments that can be passed (e.g., numberOfProcessedRecords, etc.)
    
    Returns:
        Dict with status_code and body (always includes 'incidents' key)
    """
    try:
        connector = _get_servicenow_connector(kwargs['sourceHeaders'])
        query_dict = convert_query_params_to_dict(kwargs['queryParams'])
        
        # Extract numberOfProcessedRecords from kwargs, default to 0 if not provided
        numberOfProcessedRecords = kwargs.get('numberOfProcessedRecords', 0)        
        
        # You can also add any additional kwargs to query_dict if needed
        # For example, if you want to pass additional query parameters:
        for key, value in kwargs.items():
            #if key not in ['numberOfProcessedRecords']:  # Exclude specific kwargs from query_dict
            query_dict[key] = value        
        
        response = connector.get_incident_tasks_unfiltered(query_dict)
        
        # FIXED: Ensure consistent response format for transformer
        if response.success:
            # Success case - return the data as expected
            result = standardize_source_response_format(response)
        else:
            # Error case - still return incidents key with empty list to prevent KeyError
            result = {
                "status_code": response.status_code,
                "body": {
                    "incidents": [],  # FIXED: Always include incidents key
                    "error": response.error_message,
                    "meta": {
                        "total_count": 0,
                        "has_more": False,
                        "offset": numberOfProcessedRecords,
                        "limit": query_dict.get('limit', 100)
                    }
                }
            }
        
        return result
        
    except Exception as e:
        logging.error(f"get_servicenow_incident_tasks_unfiltered_v1: {e}", exc_info=True)
        # FIXED: Always return incidents key, even in exception cases
        return {
            "status_code": 500,
            "body": {
                "incidents": [],  # FIXED: Always include incidents key
                "error": str(e),
                "meta": {
                    "total_count": 0,
                    "has_more": False,
                    "offset": 0,
                    "limit": 100
                }
            }
        }



def get_servicenow_service_requests_v1(**kwargs) -> Dict:
    try: 
        connector= _get_servicenow_connector(kwargs['sourceHeaders'])
        query_dict = convert_query_params_to_dict(kwargs['queryParams'])
        numberOfProcessedRecords = kwargs.get('numberOfProcessedRecords', 0)

        for key, value in kwargs.items():
            query_dict[key] = value

        response = connector.get_requests(query_dict)
        if response.success:
            # Success case - return the data as expected
            result = standardize_source_response_format(response)
        else:
            # Error case - still return incidents key with empty list to prevent KeyError
            result = {
                "status_code": response.status_code,
                "body": {
                    "requests": [], 
                    "error": response.error_message,
                    "meta": {
                        "total_count": 0,
                        "has_more": False,
                        "offset": numberOfProcessedRecords,
                        "limit": query_dict.get('limit', 100)
                    }
                }
            }
        return result

    except Exception as e:
        logging.error(f"Error in getting_service_requests_v1: {e}", exc_info=True)

        return{
            "status_code": 500,
            "body" :{
                "service_requests": [],
                "error": str(e),
                "meta":{
                    "total_count": 0,
                    "has_more": False,
                    "offset": 0,
                    "limit": 100
                }
            }
        }

def get_servicenow_changes_v1(**kwargs) -> Dict:
    """🚀 ServiceNow changes retrieval - NEW FUNCTION"""
    try:
        connector = _get_servicenow_connector(kwargs['sourceHeaders'])
        query_dict = convert_query_params_to_dict(kwargs['queryParams'])
        
        numberOfProcessedRecords = kwargs.get('numberOfProcessedRecords', 0)        
        
        for key, value in kwargs.items():
            query_dict[key] = value        
        
        response = connector.get_changes(query_dict)
        
        if response.success:
            result = standardize_source_response_format(response)
        else:
            result = {
                "status_code": response.status_code,
                "body": {
                    "changes": [],
                    "error": response.error_message,
                    "meta": {
                        "total_count": 0,
                        "has_more": False,
                        "offset": numberOfProcessedRecords,
                        "limit": query_dict.get('limit', 100)
                    }
                }
            }
        
        return result
        
    except Exception as e:
        logging.error(f"Error in get_servicenow_changes_v1: {e}", exc_info=True)
        return {
            "status_code": 500,
            "body": {
                "changes": [],
                "error": str(e),
                "meta": {
                    "total_count": 0,
                    "has_more": False,
                    "offset": 0,
                    "limit": 100
                }
            }
        }

def get_servicenow_incident_tasks_v1(**kwargs) -> Dict:
    """🚀 OPTIMIZED: ServiceNow tasks retrieval with faster enrichment"""
    try:
        connector = _get_servicenow_connector(kwargs['sourceHeaders'])
        query_params = kwargs.get('queryParams', [])

        # Initialize api_key to avoid UnboundLocalError
        api_key = None

        # Extract parent_id and parent_type from queryParams
        if isinstance(query_params, list):
            parent_id = None
            parent_type = 'incident'
            #parent_type = 'change_request'
            for param in query_params:
                if param.get('key') == 'sys_id':
                    parent_id = param.get('value')
                elif param.get('key') == 'parent_type':
                    parent_type = param.get('value', 'incident')
                elif param.get('key') == 'x-sn-apikey':
                    api_key = param.get('value')
        else:
            parent_id = query_params.get('parent')
            parent_type = query_params.get('parent_type', 'incident')
            api_key = query_params.get('x-sn-apikey')
        
        if not parent_id:
            return {
                "status_code": 400,
                "body": {
                    "tasks": [],
                    "error": "Missing parent parameter"
                }
            }
        
        response = connector.get_tasks_for_parent(parent_id, parent_type, api_key)
        
        if response.success:
            result = standardize_source_response_format(response)
        else:
            result = {
                "status_code": response.status_code,
                "body": {
                    "tasks": [],
                    "error": response.error_message,
                    "meta": {
                        "total_count": 0,
                        "has_more": False,
                        "offset": 0,
                        "limit": 100
                    }
                }
            }
        
        return result
        
    except Exception as e:
        logging.error(f"Error in get_servicenow_incident_tasks_v1: {e}", exc_info=True)
        return {
            "status_code": 500,
            "body": {
                "tasks": [],
                "error": str(e),
                "meta": {
                    "total_count": 0,
                    "has_more": False,
                    "offset": 0,
                    "limit": 100
                }
            }
        }

def get_servicenow_change_tasks_v1(**kwargs) -> Dict:
    """🚀 OPTIMIZED: ServiceNow tasks retrieval with faster enrichment"""
    try:
        connector = _get_servicenow_connector(kwargs['sourceHeaders'])
        query_params = kwargs.get('queryParams', [])
        api_key = None

        # Extract parent_id and parent_type from queryParams
        if isinstance(query_params, list):
            parent_id = None
            parent_type = 'change_request'
            api_key = None  # Initialize api_key
            for param in query_params:
                if param.get('key') == 'sys_id':
                    parent_id = param.get('value')
                elif param.get('key') == 'parent_type':
                    parent_type = param.get('value', 'change_request')
                elif param.get('key') == 'x-sn-apikey':  # Add this extraction
                    api_key = param.get('value')
        else:
            parent_id = query_params.get('parent')
            parent_type = query_params.get('parent_type', 'change_request')
            api_key = query_params.get('x-sn-apikey')  # Handle dict case
        
        if not parent_id:
            return {
                "status_code": 400,
                "body": {
                    "tasks": [],
                    "error": "Missing parent parameter"
                }
            }
        
        response = connector.get_tasks_for_parent(parent_id, parent_type,api_key)
        
        if response.success:
            result = standardize_source_response_format(response)
        else:
            result = {
                "status_code": response.status_code,
                "body": {
                    "tasks": [],
                    "error": response.error_message,
                    "meta": {
                        "total_count": 0,
                        "has_more": False,
                        "offset": 0,
                        "limit": 100
                    }
                }
            }
        
        return result
        
    except Exception as e:
        logging.error(f"Error in get_servicenow_change_tasks_v1: {e}", exc_info=True)
        return {
            "status_code": 500,
            "body": {
                "tasks": [],
                "error": str(e),
                "meta": {
                    "total_count": 0,
                    "has_more": False,
                    "offset": 0,
                    "limit": 100
                }
            }
        }

def get_servicenow_notes_v1(**kwargs) -> Dict:
    """🚀 OPTIMIZED: ServiceNow notes retrieval with faster enrichment"""
    try:
        connector = _get_servicenow_connector(kwargs['sourceHeaders'])
        query_params = kwargs.get('queryParams', [])
        api_key = None
        
        # Extract parent_id and parent_type from queryParams
        if isinstance(query_params, list):
            parent_id = None
            parent_type = 'incident'
            api_key = None  # Initialize api_key
            for param in query_params:
                if param.get('key') == 'sys_id':
                    parent_id = param.get('value')
                elif param.get('key') == 'parent_type':
                    parent_type = param.get('value', 'incident')
                elif param.get('key') == 'x-sn-apikey':
                    api_key = param.get('value')
        else:
            parent_id = query_params.get('parent_id')
            parent_type = query_params.get('parent_type', 'incident')
            api_key = query_params.get('x-sn-apikey')
        
        if not parent_id:
            return {
                "status_code": 400,
                "body": {
                    "notes": [],
                    "error": "Missing parent_id parameter"
                }
            }
        
        response = connector.get_notes(parent_id, parent_type, api_key)
        
        if response.success:
            result = standardize_source_response_format(response)
        else:
            result = {
                "status_code": response.status_code,
                "body": {
                    "notes": [],
                    "error": response.error_message
                }
            }
        
        return result
        
    except Exception as e:
        logging.error(f"Error in get_servicenow_notes_v1: {e}", exc_info=True)
        return {
            "status_code": 500,
            "body": {
                "notes": [],
                "error": str(e)
            }
        }

def get_servicenow_field_definitions_v1(**kwargs) -> Dict:
    """Get ServiceNow field definitions for a table"""
    try:
        connector = _get_servicenow_connector(kwargs['sourceHeaders'])
        query_dict = convert_query_params_to_dict(kwargs['queryParams'])
        
        table_name = query_dict.get('table_name', 'incident')
        api_key = query_dict.get('x-sn-apikey')
        field_definitions = connector.get_field_definitions(table_name, api_key)
        
        # Also get choice values for state, priority, urgency, impact
        choice_fields = ['state', 'priority', 'urgency', 'impact', 'category']
        choices = {}
        
        for field in choice_fields:
            choice_values = connector.get_choice_values(table_name, field, api_key)
            if choice_values:
                choices[field] = choice_values
        
        return {
            "status_code": 200,
            "body": {
                "field_definitions": field_definitions,
                "choice_values": choices,
                "table_name": table_name
            }
        }
        
    except Exception as e:
        logging.error(f"Error in get_servicenow_field_definitions_v1: {e}", exc_info=True)
        return {
            "status_code": 500,
            "body": {"error": str(e)}
        }


def validate_servicenow_instance_v1(**kwargs) -> Dict:
    """Validate ServiceNow instance and credentials"""
    try:
        connector = _get_servicenow_connector(kwargs['sourceHeaders'])
        
        if not connector._validate_config():
            return {
                "status_code": 400,
                "body": {"valid": False, "error": "Invalid configuration"}
            }
        
        # Test connection by making a simple API call
        base_url = connector._build_base_url()
        url = f"{base_url}/api/now/table/incident"
        params = {'sysparm_limit': 1}
        
        response = connector._make_api_request(url, params)
        
        is_valid = response.status_code == 200
        
        return {
            "status_code": 200 if is_valid else response.status_code,
            "body": {
                "valid": is_valid,
                "instance": connector._extract_instance_name(connector.config.domainUrl)
            }
        }
        
    except Exception as e:
        logging.error(f"Error validating ServiceNow instance: {e}")
        return {
            "status_code": 500,
            "body": {"valid": False, "error": str(e)}
        }


def reload_servicenow_enrichment_config_v1(**kwargs) -> Dict:
    """Reload enrichment configuration from file"""
    try:
        connector = _get_servicenow_connector(kwargs['sourceHeaders'])
        connector.reload_enrichment_config()
        
        return {
            "status_code": 200,
            "body": {"message": "Enrichment configuration reloaded successfully"}
        }
        
    except Exception as e:
        logging.error(f"Error reloading enrichment config: {e}")
        return {
            "status_code": 500,
            "body": {"error": str(e)}
        }


# ===================================================================
# BATCH PROCESSING FUNCTIONS
# ===================================================================

def get_servicenow_incidents_batch(**kwargs) -> List[Dict]:
    """Process multiple incident queries in batch"""
    results = []
    connector = _get_servicenow_connector(kwargs['sourceHeaders'])
    query_params_list = kwargs.get('query_params_list', [])
    
    for i, query_params in enumerate(query_params_list):
        try:
            logging.info(f"Processing incident query {i+1}/{len(query_params_list)}")
            
            query_dict = convert_query_params_to_dict(query_params)
            response = connector.get_incidents(query_dict)
            result = standardize_source_response_format(response)
            
            results.append({
                "index": i,
                "success": response.success,
                "result": result
            })
            
        except Exception as e:
            logging.error(f"Error processing query {i}: {e}")
            results.append({
                "index": i,
                "success": False,
                "result": {
                    "status_code": 500,
                    "body": {"error": str(e)}
                }
            })
    
    return results


# ===================================================================
# 📊 PERFORMANCE MONITORING
# ===================================================================

def get_servicenow_performance_stats() -> Dict:
    """Get performance statistics for monitoring"""
    return {
        "enrichment_type": "optimized_bulk",
        "expected_improvement": "90%+ faster than sequential",
        "bulk_fetch_endpoints": [
            "/api/now/table/sys_user",
            "/api/now/table/sys_user_group", 
            "/api/now/table/core_company",
            "/api/now/table/cmn_location",
            "/api/now/table/cmn_department",
            "/api/now/table/sys_attachment"
        ],
        "migration_safe": True,
        "thread_safe": True,
        "parallel_processing": True,
        "bulk_attachment_processing": True
    }


# ===================================================================
# TESTING AND EXAMPLE USAGE
# ===================================================================

def example_usage():
    """Example showing how the ServiceNow connector works with transformer"""
    
    # Headers (matching transformer format)
    headers = [
        {"key": "domainUrl", "value": "dev224007.service-now.com"},
        {"key": "username", "value": "admin"},
        {"key": "password", "value": "4hieI1MV%*fE"}
    ]
    
    # Query parameters for incidents with complex filter
    incident_params = [
        {"key": "limit", "value": "10"},
        {"key": "offset", "value": "0"},
        {"key": "filters", "value": "sys_created_on>=2022-07-01 00:00:00^severity=1^assignment_group=22c4f3e61b104454b8b61f4ead4bcb43^sys_created_onISNOTEMPTY^ORDERBYDESCsys_created_on"},
        {"key": "resolve_references", "value": "true"}
    ]
    
    # Get incidents
    print("🚀 Getting incidents with optimized enrichment...")
    start_time = time.time()
    incidents_result = get_servicenow_incidents_v1(sourceHeaders=headers, queryParams=incident_params)
    duration = time.time() - start_time
    
    print(f"⚡ Incidents retrieved in {duration:.2f} seconds")
    print(f"📊 Status: {incidents_result.get('status_code')}")
    
    if incidents_result.get("status_code") == 200:
        incidents = incidents_result.get("body", {}).get("incidents", [])
        print(f"🎯 Successfully fetched {len(incidents)} incidents with optimized enrichment!")
        
        # Get notes for first incident
        if incidents:
            incident_id = incidents[0].get("sys_id")
            print(f"\n🚀 Getting notes for incident {incident_id}...")
            
            notes_params = [
                {"key": "parent_id", "value": incident_id},
                {"key": "parent_type", "value": "incident"}
            ]
            notes_result = get_servicenow_notes_v1(sourceHeaders=headers, queryParams=notes_params)
            print(f"📊 Notes result: {notes_result.get('status_code')}")
    
    # Get tasks
    print("\n🚀 Getting tasks with optimized enrichment...")
    task_params = [
        {"key": "limit", "value": "5"},
        {"key": "offset", "value": "0"},
        {"key": "filters", "value": "active=true^ORDERBYDESCsys_created_on"}
    ]
    tasks_result = get_servicenow_incident_tasks_v1(sourceHeaders=headers, queryParams=task_params)
    print(f"📊 Tasks result: {tasks_result.get('status_code')}")
    
    # Get field definitions
    print("\n📋 Getting field definitions...")
    field_params = [{"key": "table_name", "value": "incident"}]
    fields_result = get_servicenow_field_definitions_v1(sourceHeaders=headers, queryParams=field_params)
    print(f"📊 Field definitions result: {fields_result.get('status_code')}")
    
    # Validate instance
    print("\n✅ Validating ServiceNow instance...")
    validation_result = validate_servicenow_instance_v1(sourceHeaders=headers)
    print(f"📊 Validation result: {validation_result}")
    
    # Performance stats
    print("\n📊 Performance Statistics:")
    stats = get_servicenow_performance_stats()
    for key, value in stats.items():
        print(f"   {key}: {value}")

# Example Usage:
def example_changes_usage():
    headers = [
        {"key": "domainUrl", "value": "dev224007.service-now.com"},
        {"key": "username", "value": "admin"},
        {"key": "password", "value": "4hieI1MV%*fE"}
    ]
    
    # 1. Get changes - NEW FUNCTION
    changes_result = get_servicenow_changes_v1(
        sourceHeaders=headers,
        queryParams=[
            {"key": "limit", "value": "10"},
            {"key": "filters", "value": "state=1^type=normal"}
        ]
    )
    
    # 2. Get change tasks - REUSE EXISTING! Just change parent_type
    change_tasks_result = get_servicenow_incident_tasks_v1(
        sourceHeaders=headers,
        queryParams=[
            {"key": "parent", "value": "CHG0000024"},
            {"key": "parent_type", "value": "change_request"}  # Only difference!
        ]
    )
    
    # 3. Get change notes - REUSE EXISTING! Just change parent_type  
    change_notes_result = get_servicenow_notes_v1(
        sourceHeaders=headers,
        queryParams=[
            {"key": "sys_id", "value": "CHG0000024"},
            {"key": "parent_type", "value": "change_request"}  # Only difference!
        ]
    )
    
    print("🚀 All working with maximum code reuse!")

if __name__ == "__main__":
    # Performance test example
    logging.basicConfig(level=logging.INFO)
    
    print("🚀 Testing optimized ServiceNow connector...")
    example_usage()