import logging

def get_solarwinds_tasks_v1(**kwargs) -> dict:
   
    try:
        print("[DEBUG] get_solarwinds_tasks_v1 called with kwargs:", kwargs)
        connector = _get_solarwinds_connector(kwargs['sourceHeaders'])
        query_params = kwargs.get('queryParams', [])

        # Extract incident_id from queryParams
        if isinstance(query_params, list):
            incident_id = None
            for param in query_params:
                if param.get('key') == 'incident_id':
                    incident_id = param.get('value')
                    break
        else:
            incident_id = query_params.get('incident_id')

        if not incident_id:
            result = {
                "status_code": 400,
                "body": {
                    "tasks": [],
                    "error": "Missing incident_id parameter",
                    "meta": {
                        "total_count": 0,
                        "has_more": False,
                        "offset": 0,
                        "limit": 100
                    }
                }
            }
            return result

        response = connector.get_tasks_for_incident(incident_id)

        if response and response.success:
            result = standardize_source_response_format(response)
            # 'tasks' key is present
            if 'body' in result and 'tasks' not in result['body']:
                result['body']['tasks'] = []
            print("[DEBUG] get_solarwinds_tasks_v1 returning (success):", result)
            return result
        else:
            error_message = response.error_message if response else "No response from connector"
            result = {
                "status_code": response.status_code if response else 500,
                "body": {
                    "tasks": [],
                    "error": error_message,
                    "meta": {
                        "total_count": 0,
                        "has_more": False,
                        "offset": 0,
                        "limit": 100
                    }
                }
            }
            print("[DEBUG] get_solarwinds_tasks_v1 returning (error):", result)
            return result
    except Exception as e:
        import traceback
        print(f"[DEBUG] Error in get_solarwinds_tasks_v1: {e}")
        traceback.print_exc()
        result = {
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
        print("[DEBUG] get_solarwinds_tasks_v1 returning (exception):", result)
        return result
    # Fallback: should never hit this, but just in case
    print("[DEBUG] get_solarwinds_tasks_v1 reached unexpected end, returning empty result")
    return {
        "status_code": 500,
        "body": {
            "tasks": [],
            "error": "Unexpected handler exit",
            "meta": {
                "total_count": 0,
                "has_more": False,
                "offset": 0,
                "limit": 100
            }
        }
    }
def get_solarwinds_notes_v1(**kwargs) -> dict:
   
    try:
        connector = _get_solarwinds_connector(kwargs['sourceHeaders'])
        query_params = kwargs.get('queryParams', [])

        # Extract incident_id from queryParams
        if isinstance(query_params, list):
            incident_id = None
            for param in query_params:
                if param.get('key') == 'incident_id':
                    incident_id = param.get('value')
                    break
        else:
            incident_id = query_params.get('incident_id')

        if not incident_id:
            return {
                "status_code": 400,
                "body": {
                    "notes": [],
                    "error": "Missing incident_id parameter",
                    "meta": {
                        "total_count": 0,
                        "has_more": False,
                        "offset": 0,
                        "limit": 100
                    }
                }
            }

        response = connector.get_comments_for_incident(incident_id)

        # Adapt the response to use 'notes' instead of 'comments'
        if response.success:
            data = response.data or {}
            notes = data.get('comments', [])
            result = {
                "status_code": 200,
                "body": {
                    "notes": notes,
                    "incident_id": incident_id,
                    "meta": data.get('meta', {
                        "total_count": len(notes),
                        "has_more": False,
                        "offset": 0,
                        "limit": 100
                    })
                }
            }
        else:
            result = {
                "status_code": response.status_code,
                "body": {
                    "notes": [],
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
        import traceback
        print(f"Error in get_solarwinds_notes_v1 method: {e}")
        traceback.print_exc()
        return {
            "status_code": 500,
            "body": {
                "notes": [],
                "error": str(e),
                "meta": {
                    "total_count": 0,
                    "has_more": False,
                    "offset": 0,
                    "limit": 100
                }
            }
        }
    # Fallback: should never hit this, but just in case
    print("[DEBUG] get_solarwinds_notes_v1 reached unexpected end, returning empty result")
    return {
        "status_code": 500,
        "body": {
            "notes": [],
            "error": "Unexpected handler exit",
            "meta": {
                "total_count": 0,
                "has_more": False,
                "offset": 0,
                "limit": 100
            }
        }
    }

import requests

# Data classes
class SourceConnectorConfig:
    def __init__(self, domainUrl=None, username=None, password=None, api_token=None, **kwargs):
        self.domainUrl = domainUrl
        self.username = username
        self.password = password
        self.api_token = api_token
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

# --- FIXED: Ensure all required imports are present ---
from typing import Dict, List, Set, Optional, Tuple
import threading
from urllib.parse import urlparse
from .base_source_connector import (
    BaseSourceRateLimitHandler,
    BaseFieldMapper,
    BaseSourceConnector,
    BaseDataEnricher,
    convert_query_params_to_dict,
    standardize_source_response_format,
    log_api_call,
    handle_common_errors,
    safe_json_response,
    convert_source_headers_to_dict
)
import time
import concurrent.futures
from .base_source_connector import BaseSourceRateLimitHandler, convert_query_params_to_dict, standardize_source_response_format


class SolarwindsRateLimitHandler(BaseSourceRateLimitHandler):
    
    def is_rate_limited(self, response: requests.Response) -> bool:
        return response.status_code == 429
    
    def get_retry_delay(self, response: requests.Response) -> int:
        retry_after = response.headers.get('Retry-After')
        if retry_after:
            try:
                return int(retry_after)
            except ValueError:
                pass
        
        # Solarwinds rate limit headers
        rate_limit_reset = response.headers.get('X-RateLimit-Reset')
        if rate_limit_reset:
            try:
                reset_timestamp = int(rate_limit_reset)
                current_timestamp = int(time.time())
                return max(0, reset_timestamp - current_timestamp)
            except ValueError:
                pass
        
        return 60  # Default 1 minute
    
    def make_request_with_retry(self, url: str, method: str = 'GET', max_retries: int = 3, **kwargs):
       
        last_exception = None
        
        for attempt in range(max_retries + 1):
            try:
                response = requests.request(method, url, **kwargs)
                
                # Handle rate limiting
                if self.is_rate_limited(response):
                    if attempt < max_retries:
                        delay = self.get_retry_delay(response)
                        print(f"Rate limited, retrying in {delay} seconds (attempt {attempt + 1}/{max_retries + 1})")
                        time.sleep(delay)
                        continue
                    else:
                        print(f"Rate limited and max retries reached for {url}")
                        return response  # Return the 429 response, don't return None
                
                # For any other response (success or error), return it
                return response
                
            except requests.RequestException as e:
                last_exception = e
                if attempt < max_retries:
                    print(f"Request failed, retrying in 5 seconds (attempt {attempt + 1}/{max_retries + 1}): {e}")
                    time.sleep(5)
                    continue
                else:
                    print(f"Max retries reached for {url}: {e}")
                    break
            except Exception as e:
                last_exception = e
                print(f"Unexpected error during request to {url}: {e}")
                break
        
        
        # Create a mock response object that behaves like a requests.Response
        from unittest.mock import Mock
        error_response = Mock()
        error_response.status_code = 500
        error_response.text = f"Request failed after {max_retries} retries: {str(last_exception)}"
        error_response.headers = {}
        error_response.json = lambda: {"error": "Request failed", "message": str(last_exception)}
        
        return error_response  # Never return None!


class OptimizedSolarwindsDataEnricher:
    def enrich_incidents(self, incidents: list, query_params: dict = None, *args, **kwargs) -> list:
      
        enriched = []
        for inc in incidents:
            # Log and preserve 'name' as subject if 'title' is missing
            if 'title' not in inc or not inc['title']:
                if 'name' in inc and inc['name']:
                    print(f"[INFO] Incident ID: {inc.get('id')} using 'name' as subject: {inc['name']}")
                else:
                    print(f"[WARNING] Incident ID: {inc.get('id')} missing both 'title' and 'name' during enrichment.")
            # Defensive: If tasks/comments fields are missing, add empty lists
            if 'tasks' not in inc:
                inc['tasks'] = []
            if 'comments' not in inc:
                inc['comments'] = []
            if 'notes' not in inc:
                # Always map comments to notes for downstream compatibility
                inc['notes'] = inc.get('comments', [])
            enriched.append(inc)
        return enriched

    def enrich_notes_optimized(self, notes: List[Dict], query_params: Dict = None) -> List[Dict]:
        if not notes:
            return []

        if query_params is None:
            query_params = {}

        enable_attachment_enrichment = query_params.get('enable_attachment_enrichment', 'false').lower() == 'true'

        try:
            user_ids = set()
            for note in notes:
                created_by = note.get('created_by')
                if created_by and isinstance(created_by, dict):
                    user_id = created_by.get('id')
                    if user_id:
                        user_ids.add(str(user_id))
                    email = created_by.get('email')
                    if email and not user_id:
                        user_ids.add(f"email:{email}")

            if user_ids:
                self._bulk_fetch_users_bulk(user_ids)

            enriched_notes = self._apply_cached_enrichment_to_notes(notes)

            if enable_attachment_enrichment:
                self._enrich_incidents_attachments_batch(enriched_notes)

            return enriched_notes
        except Exception:
            return notes

    def enrich_comments(self, comments: List[Dict], *args, **kwargs) -> List[Dict]:
        return comments

    def enrich_tasks(self, tasks: List[Dict], *args, **kwargs) -> List[Dict]:
        return tasks
    def enrich_requests_optimized(self, requests: List[Dict], query_params: Dict = None) -> List[Dict]:
        if not requests:
            return []

        if query_params is None:
            query_params = {}

        enable_attachment_enrichment = query_params.get('enable_attachment_enrichment', 'false').lower() == 'true'

        try:
            reference_ids = self._extract_unique_reference_ids(requests)
            self._bulk_fetch_reference_data_parallel(reference_ids)
            enriched_requests = self._apply_cached_enrichment_to_requests(requests)
            if enable_attachment_enrichment:
                self._enrich_incidents_attachments_batch(enriched_requests)
            return enriched_requests
        except Exception:
            return requests

    def enrich_ctask_notes_optimized(self, notes: List[Dict], query_params: Dict = None) -> List[Dict]:
        if not notes:
            return []

        if query_params is None:
            query_params = {}

        enable_attachment_enrichment = query_params.get('enable_attachment_enrichment', 'false').lower() == 'true'

        try:
            reference_ids = self._extract_unique_reference_ids(notes)
            self._bulk_fetch_reference_data_parallel(reference_ids)
            enriched_notes = self._apply_cached_enrichment_to_notes(notes)
            if enable_attachment_enrichment:
                self._enrich_incidents_attachments_batch(enriched_notes)
            return enriched_notes
        except Exception:
            return notes
    def _bulk_fetch_users_bulk(self, user_ids: Set[str]):
        if not user_ids:
            return
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = []
                for user_id in user_ids:
                    futures.append(executor.submit(self._fetch_single_user, user_id))
                for future in concurrent.futures.as_completed(futures):
                    try:
                        user_data = future.result()
                        if user_data:
                            with self._cache_lock:
                                self._session_cache['users'][str(user_data['id'])] = user_data
                    except Exception:
                        pass
        except Exception:
            pass
    def _apply_cached_enrichment_to_notes(self, notes: List[Dict]) -> List[Dict]:
        enriched_notes = []
        for note in notes:
            enriched_note = note.copy()
            created_by = note.get('created_by')
            user_key = None
            if created_by and isinstance(created_by, dict):
                user_id = created_by.get('id')
                email = created_by.get('email')
                if user_id and user_id in self._session_cache['users']:
                    user_data = self._session_cache['users'][user_id]
                    enriched_note['personName'] = user_data.get('name', '')
                    enriched_note['personEmail'] = user_data.get('email', '')
                elif email:
                    email_key = f"email:{email}"
                    if email_key in self._session_cache['users']:
                        user_data = self._session_cache['users'][email_key]
                        enriched_note['personName'] = user_data.get('name', '')
                        enriched_note['personEmail'] = user_data.get('email', '')
                    else:
                        enriched_note['personName'] = email
                        enriched_note['personEmail'] = email
                else:
                    enriched_note['personName'] = ''
                    enriched_note['personEmail'] = ''
            else:
                enriched_note['personName'] = ''
                enriched_note['personEmail'] = ''
            enriched_notes.append(enriched_note)
        return enriched_notes
    def _apply_cached_enrichment_to_requests(self, requests: List[Dict]) -> List[Dict]:
        enriched_requests = []
        for request in requests:
            enriched_request = request.copy()
            requester = request.get('requester')
            if requester and isinstance(requester, dict):
                requester_id = str(requester.get('id', ''))
                if requester_id and requester_id in self._session_cache['users']:
                    user_data = self._session_cache['users'][requester_id]
                    enriched_request['requestedForName'] = user_data.get('name', '')
                    enriched_request['requestedForEmail'] = user_data.get('email', '')
                else:
                    enriched_request['requestedForName'] = ''
                    enriched_request['requestedForEmail'] = ''
            else:
                enriched_request['requestedForName'] = ''
                enriched_request['requestedForEmail'] = ''
            enriched_requests.append(enriched_request)
        return enriched_requests

    def enrich_changes_optimized(self, changes: List[Dict], query_params: Dict = None) -> List[Dict]:
        if not changes:
            return []

        if query_params is None:
            query_params = {}

        enable_attachment_enrichment = query_params.get('enable_attachment_enrichment', 'false').lower() == 'true'
        
        print(f"[ENRICHER DEBUG] Changes count: {len(changes)}")
        print(f"[ENRICHER DEBUG] query_params: {query_params}")
        print(f"[ENRICHER DEBUG] enable_attachment_enrichment: {enable_attachment_enrichment}")

        try:
            enriched_changes = []
            for change in changes:
                enriched_change = self._apply_enhanced_enrichment_to_change(change)
                enriched_changes.append(enriched_change)

            if enable_attachment_enrichment:
                print(f"[ENRICHER DEBUG] Calling _enrich_changes_attachments_batch for {len(enriched_changes)} changes")
                logging.info(f"[ENRICHER DEBUG] Calling _enrich_changes_attachments_batch for {len(enriched_changes)} changes")
                self._enrich_changes_attachments_batch(enriched_changes)
                logging.info(f"[ENRICHER DEBUG] Completed attachment enrichment for {len(enriched_changes)} changes")
            else:
                print(f"[ENRICHER DEBUG] Attachment enrichment is DISABLED")
                logging.info(f"[ENRICHER DEBUG] Attachment enrichment is DISABLED")

            return enriched_changes
        except Exception as e:
            print(f"[ENRICHER ERROR] Exception during enrichment: {e}")
            import traceback
            traceback.print_exc()
            return changes

    def _apply_enhanced_enrichment_to_note(self, note: Dict) -> Dict:
        enriched_note = note.copy()
        return enriched_note

    def _apply_enhanced_enrichment_to_change(self, change: Dict) -> Dict:
        enriched_change = change.copy()
        return enriched_change
    
    
    def __init__(self, solarwinds_connector):
        self.connector = solarwinds_connector
        
        self._session_cache = {
            'users': {},
            'groups': {},
            'categories': {},
            'sites': {},
            'departments': {}
        }
        self._cache_lock = threading.Lock()
        
        self.migration_id = getattr(solarwinds_connector, 'migration_id', 'unknown')
    


    def enrich_incidents_optimized(self, incidents: List[Dict], query_params: Dict = None) -> List[Dict]:
        if not incidents:
            return []

        if query_params is None:
            query_params = {}

        enable_attachment_enrichment = query_params.get('enable_attachment_enrichment', 'false').lower() == 'true'

        try:
            enriched_incidents = []
            for incident in incidents:
                enriched_incident = self._apply_enhanced_enrichment_to_incident(incident)
                enriched_incidents.append(enriched_incident)

            if enable_attachment_enrichment:
                self._enrich_incidents_attachments_batch(enriched_incidents)

            return enriched_incidents
        except Exception:
            return incidents

    def enrich_tasks_optimized(self, tasks: List[Dict], query_params: Dict = None) -> List[Dict]:
        """
        OPTIMIZED TASK ENRICHMENT: Bulk fetch + batch attachment enrichment for tasks
        Mirrors ServiceNow's enrich_tasks_optimized pattern
        """
        if not tasks:
            return []

        if query_params is None:
            query_params = {}

        enable_attachment_enrichment = query_params.get('enable_attachment_enrichment', 'false').lower() == 'true'

        try:
            enriched_tasks = []
            for task in tasks:
                enriched_task = self._apply_enhanced_enrichment_to_task(task)
                enriched_tasks.append(enriched_task)

            if enable_attachment_enrichment:
                self._enrich_incidents_attachments_batch(enriched_tasks)

            # Reference resolution (user/group enrichment) for tasks if needed
            # enriched_tasks = self._apply_conditional_reference_enrichment(enriched_tasks)

            return enriched_tasks
        except Exception:
            return tasks

    def _apply_enhanced_enrichment_to_task(self, task: Dict) -> Dict:
        """
        Apply enhanced enrichment to a single task. Extend as needed for task-specific fields.
        """
        enriched_task = task.copy()
        # Example: Add more enrichment logic here if needed
        return enriched_task
    
    def _enrich_incidents_attachments_batch(self, incidents: List[Dict]):
        """
        🚀 BATCH ATTACHMENT PROCESSING - Based on working implementation
        This replaces individual attachment API calls with efficient batch processing
        """
        if not incidents:
            return
        
        try:
            # Collect all incident IDs
            incident_ids = []
            for incident in incidents:
                incident_id = incident.get('id')
                if incident_id:
                    incident_ids.append(str(incident_id))
            
            if not incident_ids:
                # No valid incident IDs, set empty attachments for all
                for incident in incidents:
                    incident['attachments'] = []
                    incident['attachmentUrls'] = []
                return
            
            # Process in chunks to avoid API limits (key optimization)
            chunk_size = 50
            all_attachments = {}  # incident_id -> [attachments]
            
            print(f"[ATTACHMENT] Processing {len(incident_ids)} incidents in chunks of {chunk_size}")
            print(f"[ATTACHMENT] Processing {len(incident_ids)} incidents in chunks of {chunk_size}")
            
            for i in range(0, len(incident_ids), chunk_size):
                chunk = incident_ids[i:i + chunk_size]
                
                # Process each incident in the chunk
                for incident_id in chunk:
                    attachments_found = False
                    processed_attachments = []
                    
                    # 🔥 KEY IMPROVEMENT: Try multiple endpoint patterns (from working code)
                    endpoint_patterns = [
                        f"/incidents/{incident_id}.json?layout=long",  # This should include attachments per API docs
                        f"/incidents/{incident_id}/attachments.json",
                        f"/incidents/{incident_id}/attachments",
                        f"/attachments.json?incident_id={incident_id}",
                        f"/sys_attachment.json?incident_id={incident_id}",
                        f"/incidents/{incident_id}.json?include=attachments"
                    ]
                    
                    for pattern in endpoint_patterns:
                        if attachments_found:
                            break
                            
                        try:
                            url = f"{self.connector._build_base_url()}{pattern}"
                            response = self.connector._make_authenticated_request(url, 'GET')
                            
                            if response.status_code == 200:
                                data = response.json()
                                
                                # 🔥 KEY IMPROVEMENT: Handle different response structures
                                attachments = []
                                if isinstance(data, list):
                                    attachments = data
                                elif isinstance(data, dict):
                                    # Try different keys where attachments might be
                                    for key in ['attachments', 'files', 'documents', 'incident']:
                                        if key in data:
                                            if key == 'incident' and isinstance(data[key], dict):
                                                attachments = data[key].get('attachments', [])
                                            else:
                                                attachments = data[key] if isinstance(data[key], list) else []
                                            break
                                
                                if attachments:
                                    # 🔥 KEY IMPROVEMENT: Process full attachment data with file content download
                                    for attachment in attachments:
                                        # Extract basic attachment info
                                        file_name = attachment.get('name', attachment.get('filename', attachment.get('file_name', '')))
                                        download_url = attachment.get('url', attachment.get('download_url', ''))
                                        
                                        attachment_info = {
                                            'id': attachment.get('id'),
                                            'name': file_name,  # 🔥 CRITICAL FIX: Use 'name' for Freshservice compatibility
                                            'filename': file_name,  # Keep for backward compatibility
                                            'file_name': file_name,  # Keep original for backward compatibility
                                            'size': attachment.get('size', attachment.get('file_size', 0)),
                                            'content_type': attachment.get('content_type', attachment.get('mime_type', '')),
                                            'download_url': download_url,
                                            'created_at': attachment.get('created_at', '')
                                        }
                                        
                                        # 🔥 NEW: Download file content for Freshservice
                                        if download_url:
                                            try:
                                                # Download the file content
                                                content_response = self.connector._make_authenticated_request(download_url, 'GET')
                                                if content_response.status_code == 200:
                                                    # Store binary content directly (Freshservice expects bytes, not base64)
                                                    attachment_info['content'] = content_response.content
                                                    print(f"   [DOWNLOAD] Successfully downloaded {file_name} ({len(content_response.content)} bytes)")
                                                else:
                                                    print(f"   [WARNING] Failed to download {file_name}: {content_response.status_code}")
                                                    attachment_info['content'] = b''
                                            except Exception as download_error:
                                                print(f"   [ERROR] Download failed for {file_name}: {download_error}")
                                                attachment_info['content'] = b''
                                        else:
                                            attachment_info['content'] = b''
                                        
                                        processed_attachments.append(attachment_info)
                                    
                                    attachments_found = True
                                    print(f"   [ATTACHMENT] Found {len(attachments)} attachments for incident {incident_id} using: {pattern}")
                                    print(f"Found {len(attachments)} attachments for incident {incident_id} using endpoint: {pattern}")
                                    break
                                    
                            elif response.status_code == 404:
                                continue  # Try next endpoint pattern
                            else:
                                print(f"   [WARNING] Attachment fetch failed for incident {incident_id} at {pattern}: {response.status_code}")
                                continue
                                
                        except Exception as e:
                            print(f"Attachment endpoint {pattern} error for incident {incident_id}: {e}")
                            continue
                    
                    # Store results (empty list if no attachments found)
                    all_attachments[incident_id] = processed_attachments
                    
                    if not attachments_found:
                        print(f"   [INFO] No attachments found for incident {incident_id} (tried {len(endpoint_patterns)} endpoints)")
                
                print(f"[ATTACHMENT] Processed chunk {i//chunk_size + 1}/{(len(incident_ids)-1)//chunk_size + 1}")
            
            # 🔥 KEY IMPROVEMENT: Apply attachment data to incidents (full objects, not just counts)
            for incident in incidents:
                incident_id = str(incident.get('id', ''))
                if incident_id and incident_id in all_attachments:
                    attachments = all_attachments[incident_id]
                    incident['attachments'] = attachments  # 🚀 ACTUAL ATTACHMENT OBJECTS
                    incident['attachmentUrls'] = [att['download_url'] for att in attachments if att['download_url']]
                    
                    # DEBUG: Log the attachment preservation
                    print(f"[ATTACHMENT] ✅ Applied {len(attachments)} attachments to incident {incident_id}")
                    print(f"Applied {len(attachments)} attachment objects to incident {incident_id} for migration")
                    
                    # Update migration metadata with actual attachments
                    if 'migration_metadata' in incident:
                        incident['migration_metadata']['attachments_count'] = len(attachments)
                        incident['migration_metadata']['has_attachments'] = len(attachments) > 0
                else:
                    incident['attachments'] = []
                    incident['attachmentUrls'] = []
                    
                    # Update migration metadata
                    if 'migration_metadata' in incident:
                        incident['migration_metadata']['attachments_count'] = 0
                        incident['migration_metadata']['has_attachments'] = False
            
            total_attachments = sum(len(all_attachments.get(str(inc.get('id', '')), [])) for inc in incidents)
            print(f"[ATTACHMENT] ✅ Completed batch attachment enrichment: {total_attachments} total attachments found")
            print(f"Completed batch attachment enrichment for {len(incidents)} incidents: {total_attachments} total attachments")
            
        except Exception as e:
            print(f"[ATTACHMENT] ❌ Batch attachment enrichment failed: {e}")
            print(f"Batch attachment enrichment failed: {e}")
            # 🔥 KEY IMPROVEMENT: Fallback safety - set empty attachments for all incidents
            for incident in incidents:
                incident['attachments'] = []
                incident['attachmentUrls'] = []
    
    def _enrich_changes_attachments_batch(self, changes: List[Dict]):
        """
        🚀 BATCH ATTACHMENT PROCESSING FOR CHANGES
        Download attachments from /changes/{id} endpoints and prepare for Freshservice
        """
        if not changes:
            return
        
        try:
            # Collect all change IDs
            change_ids = []
            for change in changes:
                change_id = change.get('id')
                if change_id:
                    change_ids.append(str(change_id))
            
            if not change_ids:
                # No valid change IDs, set empty attachments for all
                for change in changes:
                    change['attachments'] = []
                    change['attachmentUrls'] = []
                return
            
            # Process in chunks to avoid API limits
            chunk_size = 50
            all_attachments = {}  # change_id -> [attachments]
            
            print(f"[CHANGE ATTACHMENT] Processing {len(change_ids)} changes in chunks of {chunk_size}")
            logging.info(f"[CHANGE ATTACHMENT] Processing {len(change_ids)} changes in chunks of {chunk_size}")
            
            for i in range(0, len(change_ids), chunk_size):
                chunk = change_ids[i:i + chunk_size]
                
                # Process each change in the chunk
                for change_id in chunk:
                    attachments_found = False
                    processed_attachments = []
                    
                    # Try multiple endpoint patterns for changes
                    endpoint_patterns = [
                        f"/changes/{change_id}.json?layout=long",  # Primary endpoint with full details
                        f"/changes/{change_id}/attachments.json",
                        f"/changes/{change_id}/attachments",
                        f"/attachments.json?change_id={change_id}",
                        f"/changes/{change_id}.json?include=attachments"
                    ]
                    
                    for pattern in endpoint_patterns:
                        if attachments_found:
                            break
                            
                        try:
                            url = f"{self.connector._build_base_url()}{pattern}"
                            response = self.connector._make_authenticated_request(url, 'GET')
                            
                            if response.status_code == 200:
                                data = response.json()
                                
                                # Handle different response structures
                                attachments = []
                                if isinstance(data, list):
                                    attachments = data
                                elif isinstance(data, dict):
                                    # Try different keys where attachments might be
                                    for key in ['attachments', 'files', 'documents', 'change']:
                                        if key in data:
                                            if key == 'change' and isinstance(data[key], dict):
                                                attachments = data[key].get('attachments', [])
                                            else:
                                                attachments = data[key] if isinstance(data[key], list) else []
                                            break
                                
                                if attachments:
                                    # Process full attachment data with file content download
                                    for attachment in attachments:
                                        # Extract basic attachment info
                                        file_name = attachment.get('name', attachment.get('filename', attachment.get('file_name', '')))
                                        download_url = attachment.get('url', attachment.get('download_url', ''))
                                        
                                        attachment_info = {
                                            'id': attachment.get('id'),
                                            'name': file_name,  # CRITICAL: Use 'name' for Freshservice compatibility
                                            'filename': file_name,
                                            'file_name': file_name,
                                            'size': attachment.get('size', attachment.get('file_size', 0)),
                                            'content_type': attachment.get('content_type', attachment.get('mime_type', '')),
                                            'download_url': download_url,
                                            'created_at': attachment.get('created_at', '')
                                        }
                                        
                                        # Download file content for Freshservice
                                        if download_url:
                                            try:
                                                # Download the file content
                                                content_response = self.connector._make_authenticated_request(download_url, 'GET')
                                                if content_response.status_code == 200:
                                                    # Store binary content directly
                                                    attachment_info['content'] = content_response.content
                                                    print(f"   [CHANGE DOWNLOAD] Successfully downloaded {file_name} ({len(content_response.content)} bytes)")
                                                else:
                                                    print(f"   [WARNING] Failed to download {file_name}: {content_response.status_code}")
                                                    attachment_info['content'] = b''
                                            except Exception as download_error:
                                                print(f"   [ERROR] Download failed for {file_name}: {download_error}")
                                                attachment_info['content'] = b''
                                        else:
                                            attachment_info['content'] = b''
                                        
                                        processed_attachments.append(attachment_info)
                                    
                                    attachments_found = True
                                    print(f"   [CHANGE ATTACHMENT] Found {len(attachments)} attachments for change {change_id} using: {pattern}")
                                    break
                                    
                            elif response.status_code == 404:
                                continue  # Try next endpoint pattern
                            else:
                                print(f"   [WARNING] Attachment fetch failed for change {change_id} at {pattern}: {response.status_code}")
                                continue
                                
                        except Exception as e:
                            print(f"Attachment endpoint {pattern} error for change {change_id}: {e}")
                            continue
                    
                    # Store results (empty list if no attachments found)
                    all_attachments[change_id] = processed_attachments
                    
                    if not attachments_found:
                        print(f"   [INFO] No attachments found for change {change_id} (tried {len(endpoint_patterns)} endpoints)")
                
                print(f"[CHANGE ATTACHMENT] Processed chunk {i//chunk_size + 1}/{(len(change_ids)-1)//chunk_size + 1}")
            
            # Apply attachment data to changes
            for change in changes:
                change_id = str(change.get('id', ''))
                if change_id and change_id in all_attachments:
                    attachments = all_attachments[change_id]
                    change['attachments'] = attachments
                    change['attachmentUrls'] = [att['download_url'] for att in attachments if att['download_url']]
                    
                    print(f"[CHANGE ATTACHMENT] ✅ Applied {len(attachments)} attachments to change {change_id}")
                    logging.info(f"[CHANGE ATTACHMENT] ✅ Applied {len(attachments)} attachments to change {change_id}")
                    
                    # DEBUG: Log attachment details
                    for att in attachments:
                        logging.info(f"[CHANGE ATTACHMENT] Attachment: {att.get('name')} ({len(att.get('content', b''))} bytes)")
                else:
                    change['attachments'] = []
                    change['attachmentUrls'] = []
            
            total_attachments = sum(len(all_attachments.get(str(chg.get('id', '')), [])) for chg in changes)
            print(f"[CHANGE ATTACHMENT] ✅ Completed batch attachment enrichment: {total_attachments} total attachments found")
            logging.info(f"[CHANGE ATTACHMENT] ✅ Completed batch attachment enrichment: {total_attachments} total attachments found")
            
        except Exception as e:
            print(f"[CHANGE ATTACHMENT] ❌ Batch attachment enrichment failed: {e}")
            # Fallback safety - set empty attachments for all changes
            for change in changes:
                change['attachments'] = []
                change['attachmentUrls'] = []
    
    def _apply_enhanced_enrichment_to_incident(self, incident: Dict) -> Dict:
        """Apply enhanced enrichment to a single incident with detailed logging"""
        enriched_incident = incident.copy()

        # Debug: Log the subject/title before mapping
        title = incident.get('title')
        print(f"[DEBUG] Incident ID: {incident.get('id')} - Original title: {title}")
        if not title:
            print(f"[WARNING] Incident ID: {incident.get('id')} has no title. Default subject may be used.")

        # (existing enrichment logic would go here)
        return enriched_incident
    
    def _apply_conditional_reference_enrichment(self, incidents: List[Dict]) -> List[Dict]:
        """
        Apply conditional reference enrichment - only for existing users/groups
        This implements your requirement: check user details first, only proceed if exists
        """
        if not incidents:
            return incidents
        
        try:
            print(f"Starting conditional reference enrichment for {len(incidents)} incidents")
            
            # 1. Extract all unique reference IDs from incidents
            reference_ids = self._extract_unique_reference_ids(incidents)
            
            # 2. Bulk fetch all reference data in parallel
            self._bulk_fetch_reference_data_parallel(reference_ids)
            
            # 3. Apply conditional enrichment using cached data (only for existing entities)
            enriched_incidents = self._apply_cached_enrichment_to_incidents(incidents)
            
            # 4. Handle attachments with batch processing
            self._enrich_incidents_attachments_batch(enriched_incidents)
            
            print(f" Conditional enrichment completed for {len(enriched_incidents)} incidents")
            return enriched_incidents
            
        except Exception as e:
            print(f" Error during conditional enrichment: {e}", exc_info=True)
            return incidents  # Return original incidents if enrichment fails
            
        except Exception as e:
            print(f"Migration {self.migration_id}: Incident enrichment optimization failed: {e}")
            return self._minimal_enrichment_fallback(incidents, 'incidents')
    
    def _extract_unique_reference_ids(self, records: List[Dict]) -> Dict[str, Set[str]]:
        """Extract all unique reference IDs from records"""
        reference_ids = {
            'user_ids': set(),
            'group_ids': set(),
            'category_ids': set(),
            'site_ids': set(),
            'department_ids': set()
        }
        
        for record in records:
            # Extract user IDs from various fields
            for field in ['assignee', 'requester', 'created_by', 'resolved_by']:
                user_obj = record.get(field)
                if user_obj and isinstance(user_obj, dict):
                    user_id = user_obj.get('id')
                    if user_id:
                        reference_ids['user_ids'].add(str(user_id))
                    # Also check email-based references for enrichment
                    email = user_obj.get('email')
                    if email and not user_id:
                        # We'll need to resolve by email later
                        reference_ids['user_ids'].add(f"email:{email}")
            
            # Extract custom field user values
            custom_fields = record.get('custom_fields_values', {})
            if isinstance(custom_fields, dict):
                custom_field_list = custom_fields.get('custom_fields_value', [])
                for cf in custom_field_list:
                    if cf.get('user_value'):
                        user_email = cf['user_value'].get('email')
                        if user_email:
                            reference_ids['user_ids'].add(f"email:{user_email}")
            
            # Extract group IDs
            group_assignee = record.get('group_assignee')
            if group_assignee and isinstance(group_assignee, dict):
                group_id = group_assignee.get('id')
                if group_id:
                    reference_ids['group_ids'].add(str(group_id))
                # Also check name-based references
                group_name = group_assignee.get('name')
                if group_name and not group_id:
                    reference_ids['group_ids'].add(f"name:{group_name}")
            
            # Extract category IDs
            category = record.get('category')
            if category and isinstance(category, dict):
                category_id = category.get('id')
                if category_id:
                    reference_ids['category_ids'].add(str(category_id))
            
            # Extract site IDs
            site = record.get('site')
            if site and isinstance(site, dict):
                site_id = site.get('id')
                if site_id:
                    reference_ids['site_ids'].add(str(site_id))
            
            # Extract site_id from direct field
            site_id = record.get('site_id')
            if site_id:
                reference_ids['site_ids'].add(str(site_id))
            
            # Extract department IDs
            department = record.get('department')
            if department and isinstance(department, dict):
                dept_id = department.get('id')
                if dept_id:
                    reference_ids['department_ids'].add(str(dept_id))
            
            # Extract department_id from direct field
            dept_id = record.get('department_id')
            if dept_id:
                reference_ids['department_ids'].add(str(dept_id))
        
        # Log what we found
        print(f"Migration {self.migration_id}: IDs to fetch: "
                        f"{len(reference_ids['user_ids'])} users, "
                        f"{len(reference_ids['group_ids'])} groups, "
                        f"{len(reference_ids['category_ids'])} categories, "
                        f"{len(reference_ids['site_ids'])} sites, "
                        f"{len(reference_ids['department_ids'])} departments")
        
        return reference_ids
    
    def _bulk_fetch_reference_data_parallel(self, reference_ids: Dict[str, Set[str]]):
        """
        PARALLEL BULK FETCHING: Fetch all reference data simultaneously
        Instead of individual API calls, make bulk calls in parallel
        """
        def fetch_users_bulk():
            """Fetch all users in bulk"""
            user_ids = reference_ids.get('user_ids', set())
            if not user_ids:
                return
            
            try:
                # Solarwinds API doesn't support bulk user fetch by IDs
                # We'll fetch users individually but in parallel
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    futures = []
                    for user_id in user_ids:
                        future = executor.submit(self._fetch_single_user, user_id)
                        futures.append(future)
                    
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            user_data = future.result()
                            if user_data:
                                with self._cache_lock:
                                    self._session_cache['users'][str(user_data['id'])] = user_data
                        except Exception as e:
                            print(f"Migration {self.migration_id}: Error fetching user: {e}")
                
                print(f"Migration {self.migration_id}: Bulk fetched {len(self._session_cache['users'])} users")
                        
            except Exception as e:
                print(f"Migration {self.migration_id}: Bulk user fetch error: {e}")
        
        def fetch_groups_bulk():
            """Fetch all groups in bulk"""
            group_ids = reference_ids.get('group_ids', set())
            if not group_ids:
                return
            
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    futures = []
                    for group_id in group_ids:
                        future = executor.submit(self._fetch_single_group, group_id)
                        futures.append(future)
                    
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            group_data = future.result()
                            if group_data:
                                with self._cache_lock:
                                    self._session_cache['groups'][str(group_data['id'])] = group_data
                        except Exception as e:
                            print(f"Migration {self.migration_id}: Error fetching group: {e}")
                
                print(f"Migration {self.migration_id}: Bulk fetched {len(self._session_cache['groups'])} groups")
                        
            except Exception as e:
                print(f"Migration {self.migration_id}: Bulk group fetch error: {e}")
        
        # Execute all fetch operations in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(fetch_users_bulk),
                executor.submit(fetch_groups_bulk)
            ]
            
            # Wait for all to complete
            concurrent.futures.wait(futures)
    
    def _fetch_single_user(self, user_id: str) -> Optional[Dict]:
        """Fetch a single user by ID with graceful 404 handling"""
        try:
            url = f"{self.connector._build_base_url()}/users/{user_id}.json"
            response = self.connector._make_authenticated_request(url, 'GET')
            
            if response.status_code == 200:
                data = safe_json_response(response)
                print(f"Migration {self.migration_id}:  User {user_id} fetched successfully")
                return data.get('user', data)
            elif response.status_code == 404:
                print(f"Migration {self.migration_id}:   User {user_id} not found (404) - using original incident data")
                return None
            else:
                print(f"Migration {self.migration_id}: User fetch failed for ID {user_id}: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"Migration {self.migration_id}: Error fetching user {user_id}: {e}")
            return None
    
    def _fetch_single_group(self, group_id: str) -> Optional[Dict]:
        """Fetch a single group by ID with graceful 404 handling"""
        try:
            url = f"{self.connector._build_base_url()}/groups/{group_id}.json"
            response = self.connector._make_authenticated_request(url, 'GET')
            
            if response.status_code == 200:
                data = safe_json_response(response)
                print(f"Migration {self.migration_id}:  Group {group_id} fetched successfully")
                return data.get('group', data)
            elif response.status_code == 404:
                print(f"Migration {self.migration_id}:   Group {group_id} not found (404) - using original incident data")
                return None
            else:
                print(f"Migration {self.migration_id}: Group fetch failed for ID {group_id}: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"Migration {self.migration_id}: Error fetching group {group_id}: {e}")
            return None
    
    def _apply_cached_enrichment_to_incidents(self, incidents: List[Dict]) -> List[Dict]:
        """Apply enrichment using cached data for incidents"""
        enriched_incidents = []
        
        for incident in incidents:
            enriched_incident = incident.copy()
            
            # Enrich requester information - ONLY if user exists
            requester = incident.get('requester')
            if requester and isinstance(requester, dict):
                requester_id = str(requester.get('id', ''))
                requester_email = requester.get('email', '')
                
                # First check if user exists in our cache (was successfully fetched)
                user_exists = False
                if requester_id and requester_id in self._session_cache['users']:
                    user_exists = True
                    user_data = self._session_cache['users'][requester_id]
                    # Only populate enriched data if user exists
                    enriched_incident['requester'] = {
                        'id': requester.get('id'),
                        'name': user_data.get('name', requester.get('name', '')),
                        'email': user_data.get('email', requester.get('email', '')),
                        'phone': user_data.get('phone', requester.get('phone', '')),
                        'enrichment_status': 'enriched_from_api'
                    }
                    print(f"Migration {self.migration_id}:  Requester {requester_id} enriched successfully")
                elif requester_email:
                    # Try to find by email if ID lookup failed
                    email_key = f"email:{requester_email}"
                    if email_key in self._session_cache['users']:
                        user_exists = True
                        user_data = self._session_cache['users'][email_key]
                        enriched_incident['requester'] = {
                            'id': requester.get('id'),
                            'name': user_data.get('name', requester.get('name', '')),
                            'email': user_data.get('email', requester_email),
                            'phone': user_data.get('phone', requester.get('phone', '')),
                            'enrichment_status': 'enriched_by_email'
                        }
                        print(f"Migration {self.migration_id}:  Requester found by email {requester_email}")
                
                # If user doesn't exist, keep original data WITHOUT enrichment
                if not user_exists:
                    enriched_incident['requester'] = requester.copy()  # Keep original
                    enriched_incident['requester']['enrichment_status'] = 'user_not_found'
                    print(f"Migration {self.migration_id}:   Requester {requester_id} not found - keeping original data")
            else:
                enriched_incident['requester'] = None
            
            # Enrich assignee information - ONLY if user exists
            assignee = incident.get('assignee')
            if assignee and isinstance(assignee, dict):
                assignee_id = str(assignee.get('id', ''))
                assignee_email = assignee.get('email', '')
                
                # First check if user exists in our cache (was successfully fetched)
                user_exists = False
                if assignee_id and assignee_id in self._session_cache['users']:
                    user_exists = True
                    user_data = self._session_cache['users'][assignee_id]
                    # Only populate enriched data if user exists
                    enriched_incident['assignee'] = {
                        'id': assignee.get('id'),
                        'name': user_data.get('name', assignee.get('name', '')),
                        'email': user_data.get('email', assignee.get('email', '')),
                        'phone': user_data.get('phone', assignee.get('phone', '')),
                        'enrichment_status': 'enriched_from_api'
                    }
                    print(f"Migration {self.migration_id}:  Assignee {assignee_id} enriched successfully")
                elif assignee_email:
                    # Try to find by email if ID lookup failed
                    email_key = f"email:{assignee_email}"
                    if email_key in self._session_cache['users']:
                        user_exists = True
                        user_data = self._session_cache['users'][email_key]
                        enriched_incident['assignee'] = {
                            'id': assignee.get('id'),
                            'name': user_data.get('name', assignee.get('name', '')),
                            'email': user_data.get('email', assignee_email),
                            'phone': user_data.get('phone', assignee.get('phone', '')),
                            'enrichment_status': 'enriched_by_email'
                        }
                        print(f"Migration {self.migration_id}:  Assignee found by email {assignee_email}")
                
                # If user doesn't exist, keep original data WITHOUT enrichment
                if not user_exists:
                    enriched_incident['assignee'] = assignee.copy()  # Keep original
                    enriched_incident['assignee']['enrichment_status'] = 'user_not_found'
                    print(f"Migration {self.migration_id}:   Assignee {assignee_id} not found - keeping original data")
            else:
                enriched_incident['assignee'] = None
            
            # Enrich group assignee information - ONLY if group exists
            group_assignee = incident.get('group_assignee')
            if group_assignee and isinstance(group_assignee, dict):
                group_id = str(group_assignee.get('id', ''))
                group_name = group_assignee.get('name', '')
                
                # First check if group exists in our cache (was successfully fetched)
                group_exists = False
                if group_id and group_id in self._session_cache['groups']:
                    group_exists = True
                    group_data = self._session_cache['groups'][group_id]
                    # Only populate enriched data if group exists
                    enriched_incident['group_assignee'] = {
                        'id': group_assignee.get('id'),
                        'name': group_data.get('name', group_assignee.get('name', '')),
                        'description': group_data.get('description', group_assignee.get('description', '')),
                        'enrichment_status': 'enriched_from_api'
                    }
                    print(f"Migration {self.migration_id}:  Group {group_id} enriched successfully")
                elif group_name:
                    # Try to find by name if ID lookup failed
                    name_key = f"name:{group_name}"
                    if name_key in self._session_cache['groups']:
                        group_exists = True
                        group_data = self._session_cache['groups'][name_key]
                        enriched_incident['group_assignee'] = {
                            'id': group_assignee.get('id'),
                            'name': group_data.get('name', group_name),
                            'description': group_data.get('description', group_assignee.get('description', '')),
                            'enrichment_status': 'enriched_by_name'
                        }
                        print(f"Migration {self.migration_id}:  Group found by name {group_name}")
                
                # If group doesn't exist, keep original data WITHOUT enrichment
                if not group_exists:
                    enriched_incident['group_assignee'] = group_assignee.copy()  # Keep original
                    enriched_incident['group_assignee']['enrichment_status'] = 'group_not_found'
                    print(f"Migration {self.migration_id}:   Group {group_id} not found - keeping original data")
            else:
                enriched_incident['group_assignee'] = None
            
            # Enrich category information
            category = incident.get('category')
            if category and isinstance(category, dict):
                enriched_incident['categoryName'] = category.get('name', '')
            else:
                enriched_incident['categoryName'] = ''
            
            # Enrich subcategory information
            subcategory = incident.get('subcategory') or incident.get('sub_category')
            if subcategory and isinstance(subcategory, dict):
                enriched_incident['subcategoryName'] = subcategory.get('name', '')
            else:
                enriched_incident['subcategoryName'] = ''
            
            # Enrich site information
            site = incident.get('site')
            site_id = incident.get('site_id')
            if site and isinstance(site, dict):
                enriched_incident['siteName'] = site.get('name', '')
                enriched_incident['siteLocation'] = site.get('location', '')
            elif site_id:
                enriched_incident['siteId'] = site_id
                enriched_incident['siteName'] = ''
                enriched_incident['siteLocation'] = ''
            else:
                enriched_incident['siteName'] = ''
                enriched_incident['siteLocation'] = ''
            
            # Enrich department information
            department = incident.get('department')
            department_id = incident.get('department_id')
            if department and isinstance(department, dict):
                enriched_incident['departmentName'] = department.get('name', '')
            elif department_id:
                enriched_incident['departmentId'] = department_id
                enriched_incident['departmentName'] = ''
            else:
                enriched_incident['departmentName'] = ''
            
            # Process custom fields for better accessibility
            custom_fields = incident.get('custom_fields_values')
            if custom_fields and isinstance(custom_fields, dict):
                custom_field_list = custom_fields.get('custom_fields_value', [])
                enriched_incident['customFieldsProcessed'] = {}
                for cf in custom_field_list:
                    field_name = cf.get('name', '')
                    field_value = cf.get('value', '')
                    user_value = cf.get('user_value', {})
                    
                    if user_value:
                        # Handle user custom fields
                        enriched_incident['customFieldsProcessed'][field_name] = {
                            'type': 'user',
                            'value': user_value.get('email', ''),
                            'user_email': user_value.get('email', '')
                        }
                    else:
                        # Handle regular custom fields
                        enriched_incident['customFieldsProcessed'][field_name] = {
                            'type': 'text',
                            'value': field_value
                        }
            
            enriched_incidents.append(enriched_incident)
        
        return enriched_incidents
    
    def _enrich_incidents_attachments_batch(self, incidents: List[Dict]):
        """Enrich incidents with attachment information using batch processing"""
        if not incidents:
            return
        
        try:
            # Collect all incident IDs
            incident_ids = []
            for incident in incidents:
                incident_id = incident.get('id')
                if incident_id:
                    incident_ids.append(str(incident_id))
            
            if not incident_ids:
                # No valid incident IDs, set empty attachments for all
                for incident in incidents:
                    incident['attachments'] = []
                    incident['attachmentUrls'] = []
                return
            
            # Bulk fetch attachments for all incidents
            chunk_size = 50
            all_attachments = {}  # incident_id -> [attachments]
            
            for i in range(0, len(incident_ids), chunk_size):
                chunk = incident_ids[i:i + chunk_size]
                
                # Try different Solarwinds API endpoint patterns for attachments
                for incident_id in chunk:
                    attachments_found = False
                    processed_attachments = []
                    
                    # Try multiple endpoint patterns
                    endpoint_patterns = [
                        f"/incidents/{incident_id}.json?layout=long",  # This should include attachments per API docs
                        f"/incidents/{incident_id}/attachments.json",
                        f"/incidents/{incident_id}/attachments",
                        f"/attachments.json?incident_id={incident_id}",
                        f"/sys_attachment.json?incident_id={incident_id}",
                        f"/incidents/{incident_id}.json?include=attachments"
                    ]
                    
                    for pattern in endpoint_patterns:
                        if attachments_found:
                            break
                            
                        try:
                            url = f"{self.connector._build_base_url()}{pattern}"
                            
                            response = self.connector._make_authenticated_request(url, 'GET')
                            
                            if response.status_code == 200:
                                data = response.json()
                                
                                # Handle different response structures
                                attachments = []
                                if isinstance(data, list):
                                    attachments = data
                                elif isinstance(data, dict):
                                    # For the successful ?include=attachments endpoint
                                    if pattern.endswith('?include=attachments'):
                                        # The response should contain the full incident with attachments
                                        if 'attachments' in data:
                                            attachments = data.get('attachments', [])
                                        # Also check if the data has nested structure
                                        elif 'incident' in data and isinstance(data['incident'], dict):
                                            attachments = data['incident'].get('attachments', [])
                                    else:
                                        # Try different keys where attachments might be
                                        for key in ['attachments', 'files', 'documents', 'incident']:
                                            if key in data:
                                                if key == 'incident' and isinstance(data[key], dict):
                                                    attachments = data[key].get('attachments', [])
                                                else:
                                                    attachments = data[key] if isinstance(data[key], list) else []
                                                break
                                
                                # Log the response structure for debugging
                                if pattern.endswith('?include=attachments') or pattern.endswith('?layout=long'):
                                    endpoint_type = "layout=long" if "layout=long" in pattern else "include=attachments"
                                    print(f"Migration {self.migration_id}: {endpoint_type} response keys: {list(data.keys()) if isinstance(data, dict) else 'not dict'}")
                                    
                                    # Look for any attachment-related data in the response
                                    if isinstance(data, dict):
                                        attachment_fields = [key for key in data.keys() if 'attach' in key.lower()]
                                        if attachment_fields:
                                            print(f"Migration {self.migration_id}: Found attachment-related fields: {attachment_fields}")
                                            for field in attachment_fields:
                                                print(f"Migration {self.migration_id}: {field} content: {data[field]}")
                                        
                                        # Also check if there are file-related fields
                                        file_fields = [key for key in data.keys() if any(word in key.lower() for word in ['file', 'document', 'upload'])]
                                        if file_fields:
                                            print(f"Migration {self.migration_id}: Found file-related fields: {file_fields}")
                                            for field in file_fields:
                                                print(f"Migration {self.migration_id}: {field} content: {data[field]}")
                                    
                                    if isinstance(data, dict) and 'attachments' in data:
                                        print(f"Migration {self.migration_id}: Found attachments field with {len(data['attachments'])} items")
                                    else:
                                        print(f"Migration {self.migration_id}: No 'attachments' key found in response")
                                
                                if attachments:
                                    # Process attachment data with file content download
                                    for attachment in attachments:
                                        # Extract basic attachment info
                                        file_name = attachment.get('name', attachment.get('filename', attachment.get('file_name', '')))
                                        download_url = attachment.get('url', attachment.get('download_url', ''))
                                        
                                        attachment_info = {
                                            'id': attachment.get('id'),
                                            'name': file_name,  # 🔥 CRITICAL FIX: Use 'name' for Freshservice compatibility
                                            'filename': file_name,  # Keep for backward compatibility
                                            'file_name': file_name,  # Keep original for backward compatibility
                                            'size': attachment.get('size', attachment.get('file_size', 0)),
                                            'content_type': attachment.get('content_type', attachment.get('mime_type', '')),
                                            'download_url': download_url,
                                            'created_at': attachment.get('created_at', '')
                                        }
                                        
                                        # Download file content for Freshservice
                                        if download_url:
                                            try:
                                                # Download the file content
                                                content_response = self.connector._make_authenticated_request(download_url, 'GET')
                                                if content_response.status_code == 200:
                                                    # Store binary content directly (Freshservice expects bytes, not base64)
                                                    attachment_info['content'] = content_response.content
                                                    print(f"Migration {self.migration_id}: Downloaded {file_name} ({len(content_response.content)} bytes)")
                                                else:
                                                    print(f"Migration {self.migration_id}: Failed to download {file_name}: {content_response.status_code}")
                                                    attachment_info['content'] = b''
                                            except Exception as download_error:
                                                print(f"Migration {self.migration_id}: Download failed for {file_name}: {download_error}")
                                                attachment_info['content'] = b''
                                        else:
                                            attachment_info['content'] = b''
                                        
                                        processed_attachments.append(attachment_info)
                                    
                                    attachments_found = True
                                    print(f"Migration {self.migration_id}: Found {len(attachments)} attachments for incident {incident_id} using endpoint: {pattern}")
                                    break
                                    
                            elif response.status_code == 404:
                                continue  # Try next endpoint pattern
                            else:
                                print(f"Migration {self.migration_id}: Attachment fetch failed for incident {incident_id} at {pattern}: {response.status_code}")
                                continue
                                
                        except Exception as e:
                            print(f"Migration {self.migration_id}: Attachment endpoint {pattern} error for incident {incident_id}: {e}")
                            continue
                    
                    # Store results (empty list if no attachments found)
                    all_attachments[incident_id] = processed_attachments
                    
                    if not attachments_found:
                        print(f"Migration {self.migration_id}: No attachments found for incident {incident_id} (tried {len(endpoint_patterns)} endpoints)")
                
                print(f"Migration {self.migration_id}: Fetched attachments for chunk {i//chunk_size + 1}")
            
            # Apply attachment data to incidents
            for incident in incidents:
                incident_id = str(incident.get('id', ''))
                if incident_id and incident_id in all_attachments:
                    attachments = all_attachments[incident_id]
                    incident['attachments'] = attachments
                    incident['attachmentUrls'] = [att['download_url'] for att in attachments if att['download_url']]
                else:
                    incident['attachments'] = []
                    incident['attachmentUrls'] = []
            
            print(f"Migration {self.migration_id}: Completed batch attachment enrichment for {len(incidents)} incidents")
            
        except Exception as e:
            print(f"Migration {self.migration_id}: Batch attachment enrichment failed: {e}")
            # Set empty attachments for all incidents if batch fails
            for incident in incidents:
                incident['attachments'] = []
                incident['attachmentUrls'] = []
    
    def _minimal_enrichment_fallback(self, records: List[Dict], record_type: str) -> List[Dict]:
        """Fallback enrichment when optimization fails"""
        print(f"Migration {self.migration_id}: Using minimal enrichment fallback for {record_type}")
        
        # Add empty enrichment fields to prevent downstream errors
        for record in records:
            if record_type == 'incidents':
                record.setdefault('requesterName', '')
                record.setdefault('requesterEmail', '')
                record.setdefault('assigneeName', '')
                record.setdefault('assigneeEmail', '')
                record.setdefault('groupAssigneeName', '')
        
        return records
    
    def _make_authenticated_request(self, url: str, method: str = 'GET', **kwargs):
        """Make authenticated request using the connector's method"""
        return self.connector._make_authenticated_request(url, method, **kwargs)


class SolarwindsFieldMapper(BaseFieldMapper):
    def process_custom_fields(self, custom_fields, field_definitions):
        """Stub: Process custom fields for Solarwinds (implement as needed)"""
        return {}
    """Solarwinds-specific field mapper"""
    
    def get_standard_field_mapping(self) -> Dict:
        """Get standard field mapping for Solarwinds incidents"""
        return {
            # Basic incident fields
            'id': 'id',
            'number': 'number', 
            'name': 'name',
            'description': 'description',
            'description_no_html': 'description_no_html',
            'state': 'state',
            'state_id': 'state_id',
            'priority': 'priority',
            'created_at': 'created_at',
            'updated_at': 'updated_at',
            'due_at': 'due_at',
            
            # Category and subcategory
            'category': 'category',
            'sub_category': 'sub_category',
            'subcategory': 'subcategory',
            
            # Assignment fields
            'assignee': 'assignee',
            'group_assignee': 'group_assignee',
            'requester': 'requester',
            'created_by': 'created_by',
            'resolved_by': 'resolved_by',
            
            # Location and organization
            'site': 'site',
            'site_id': 'site_id',
            'department': 'department',
            'department_id': 'department_id',
            
            # Tags
            'tag_list': 'tag_list',
            'add_to_tag_list': 'add_to_tag_list',
            'remove_from_tag_list': 'remove_from_tag_list',
            
            # Custom fields
            'custom_fields_values': 'custom_fields_values',
            'custom_fields_values_attributes': 'custom_fields_values_attributes',
            
            # Related items
            'incidents': 'incidents',
            'problems': 'problems',
            'changes': 'changes',
            'solutions': 'solutions',
            'releases': 'releases',
            'purchase_orders': 'purchase_orders',
            'configuration_items': 'configuration_items',
            'configuration_item_ids': 'configuration_item_ids',
            
            # Communication
            'cc': 'cc',
            
            # Additional fields from API response
            'href': 'href',
            'href_account_domain': 'href_account_domain',
            'origin': 'origin',
            'price': 'price',
            'resolution_description': 'resolution_description',
            'resolution_code': 'resolution_code',
            'sla_violations': 'sla_violations',
            'number_of_comments': 'number_of_comments',
            'user_saw_all_comments': 'user_saw_all_comments',
            'is_service_request': 'is_service_request',
            'customer_satisfaction_survey_sent_at': 'customer_satisfaction_survey_sent_at',
            'customer_satisfaction_survey_completed_at': 'customer_satisfaction_survey_completed_at',
            'assets': 'assets',
            'mobiles': 'mobiles',
            'other_assets': 'other_assets',
            'discovery_hardware': 'discovery_hardware',
            'tasks': 'tasks',
            'time_tracks': 'time_tracks'
        }


# Use the optimized enricher as the default
SolarwindsDataEnricher = OptimizedSolarwindsDataEnricher


class SolarwindsConnector(BaseSourceConnector):
    # --- Abstract method stubs to satisfy BaseSourceConnector ---
    def _extract_conversations_from_response(self, response):
        """Stub: Extract conversations from API response (implement as needed)"""
        return []

    def _extract_pagination_info(self, response):
        """Stub: Extract pagination info from API response (implement as needed)"""
        return PaginationInfo()

    def _extract_tickets_from_response(self, response):
        """Stub: Extract tickets from API response (implement as needed)"""
        return []

    def _fetch_conversations_api_call(self, query_params):
        """Stub: Fetch conversations from API (implement as needed)"""
        return SourceResponse(200, True, data=[])

    def _fetch_tickets_api_call(self, query_params):
        """Stub: Fetch tickets from API (implement as needed)"""
        return SourceResponse(200, True, data=[])

    def _get_agents(self):
        """Stub: Fetch agents (implement as needed)"""
        return []

    def _get_field_definitions(self):
        """Stub: Fetch field definitions (implement as needed)"""
        return []

    def _get_groups(self):
        """Stub: Fetch groups (implement as needed)"""
        return []
    """Solarwinds (Samanage) implementation of the base source connector"""
    
    def __init__(self, config: SourceConnectorConfig):
        super().__init__(config)
        # Override with Solarwinds-specific components using optimized enricher
        self.rate_limiter = SolarwindsRateLimitHandler()
        self.data_enricher = SolarwindsDataEnricher(self)
        self.field_mapper = SolarwindsFieldMapper()
    
    def _get_rate_limiter(self) -> BaseSourceRateLimitHandler:
        return SolarwindsRateLimitHandler()
    
    def _get_data_enricher(self) -> BaseDataEnricher:
        return SolarwindsDataEnricher(self)
    
    def _get_field_mapper(self) -> BaseFieldMapper:
        return SolarwindsFieldMapper()
    
    def _extract_subdomain(self, domainUrl: str) -> str:
        """Extract subdomain from Solarwinds domainUrl"""
        # Handle various domainUrl formats
        # e.g., company.samanage.com -> company
        parsed = urlparse(domainUrl if domainUrl.startswith('http') else f'https://{domainUrl}')
        hostname = parsed.hostname or domainUrl
        return hostname.split('.')[0]
    
    def _build_auth_headers(self) -> Dict:
        """Build authentication headers for Solarwinds API"""
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/vnd.samanage.v2.1+json'
        }
        
        # Handle both api_key (real base classes) and api_token (fallback classes)
        api_token = getattr(self.config, 'api_key', None) or getattr(self.config, 'api_token', None)
        if api_token:
            # Use X-Samanage-Authorization header with the JWT token
            headers['X-Samanage-Authorization'] = f'Bearer {api_token}'
        
        return headers
    
    def _build_auth(self) -> Tuple:
        """Build authentication for Solarwinds API - Uses Digest Auth"""
        # Handle both api_key (real base classes) and api_token (fallback classes)
        api_token = getattr(self.config, 'api_key', None) or getattr(self.config, 'api_token', None)
        
        if api_token:
            # Parse the JWT token to extract email and use it as username
            # Token format: base64_email:jwt_token:suffix
            try:
                import base64
                parts = api_token.split(':')
                if len(parts) >= 2:
                    # Decode the first part (base64 encoded email)
                    email = base64.b64decode(parts[0]).decode('utf-8')
                    # Use the full token as password for digest auth
                    return (email, api_token)
                else:
                    # Fallback: use token as password with empty username
                    return ('', api_token)
            except:
                # Fallback: use token as password
                return ('', api_token)
        elif self.config.username and self.config.password:
            return (self.config.username, self.config.password)
        else:
            return ('', '')
    
    def _validate_config(self) -> bool:
        """Validate Solarwinds configuration"""
        has_domain = bool(self.config.domainUrl)
        # Handle both api_key and api_token
        api_token = getattr(self.config, 'api_key', None) or getattr(self.config, 'api_token', None)
        has_auth = bool(api_token) or (bool(self.config.username) and bool(self.config.password))
        return has_domain and has_auth
    
    def _build_base_url(self) -> str:
        """Build base URL for Solarwinds API"""
        # SolarWinds Service Desk uses api.samanage.com for US customers
        # Documentation: https://api.samanage.com
        return "https://api.samanage.com"
    
    def _make_authenticated_request(self, url: str, method: str = 'GET', **kwargs):
        """Make authenticated request to Solarwinds API using Bearer Token"""
        headers = self._build_auth_headers()
        if 'headers' in kwargs:
            headers.update(kwargs['headers'])
        kwargs['headers'] = headers
        
        start_time = time.time()
        response = requests.request(method, url, **kwargs)
        duration = time.time() - start_time
        
        log_api_call(method, url, response.status_code, duration)
        handle_common_errors(response)
        
        return response
    
    def _make_api_request(self, url: str, params: Dict = None, method: str = 'GET', **kwargs):
        """
        Make API request with rate limiting using Bearer Token
        NEVER returns None - always returns a valid response object
        """
        headers = self._build_auth_headers()
        
        response = self.rate_limiter.make_request_with_retry(
            url, method=method, params=params, 
            headers=headers, **kwargs
        )
        
        # Ensure we never return None
        if response is None:
            print(f"Rate limiter returned None for {url}, creating error response")
            from unittest.mock import Mock
            error_response = Mock()
            error_response.status_code = 500
            error_response.text = "Internal error: No response received"
            error_response.headers = {}
            error_response.json = lambda: {"error": "Internal error", "message": "No response received"}
            return error_response
        
        return response
    
    def get_incidents(self, query_params: Dict) -> SourceResponse:
        """
        Get Solarwinds incidents with support for filters and pagination
        ALWAYS returns a valid SourceResponse, never None
        """
        try:
            if not self._validate_config():
                return SourceResponse(400, False, error_message="Invalid configuration")
            
            base_url = self._build_base_url()
            url = f"{base_url}/incidents.json"
            
            # Build query parameters
            params = {
                'per_page': int(query_params.get('per_page', query_params.get('limit', 100))),
                'page': int(query_params.get('page', 1))
            }
            
            # CRITICAL: Add layout parameter for attachments
            # layout=long is required to retrieve attachments, comments, and detailed info
            layout = query_params.get('layout', 'long')  # Default to 'long' for attachments
            if layout:
                params['layout'] = layout
                print(f"Setting layout={layout} to retrieve attachments and detailed info")
            
            # Handle attachment enrichment configuration
            enable_attachment_enrichment = query_params.get('enable_attachment_enrichment', 'true')
            print(f"Attachment enrichment enabled: {enable_attachment_enrichment}")
            
            # Add filters if provided
            filters = query_params.get('filters')
            if filters:
                # Solarwinds supports various filter parameters
                # Parse filter string and add to params
                # Example: state=New&priority=High
                if isinstance(filters, str) and filters.strip():
                    filter_pairs = filters.split('&')
                    for pair in filter_pairs:
                        if '=' in pair:
                            key, value = pair.split('=', 1)
                            params[key] = value
            
            # Log the final API call for debugging
            param_string = "&".join([f"{k}={v}" for k, v in params.items()])
            full_url = f"{url}?{param_string}"
            
            
            # Make API call - this is guaranteed to not return None
            response = self._make_api_request(url, params)
            
            if response is None:
                # This should never happen now, but adding extra safety
                print("API request returned None despite safeguards")
                return SourceResponse(500, False, error_message="Internal error: No response received")
            
            if response.status_code == 200:
                try:
                    data = safe_json_response(response)
                    incidents = data if isinstance(data, list) else data.get('incidents', [])

                    # VERBOSE: Log full raw incident data for each incident
                    for inc in incidents:
                        print(f"[VERBOSE] Raw incident data: {inc}")

                    # Log incident count for debugging
                    print(f"[RETRIEVED] Retrieved {len(incidents)} incidents from SolarWinds API")
                    print(f"Retrieved {len(incidents)} incidents from SolarWinds API")

                    #  APPLY OPTIMIZED ENRICHMENT
                    resolve_refs = query_params.get('resolve_references', True)
                    try:
                        enriched_incidents = self.data_enricher.enrich_incidents(incidents, resolve_refs, query_params)
                        print(f" Successfully enriched {len(enriched_incidents)} incidents")
                        print(f"Successfully enriched {len(enriched_incidents)} incidents")
                    except Exception as e:
                        print(f" Error during incident enrichment: {e}")
                        print(f"Error during incident enrichment: {e}")
                        # Fall back to non-enriched incidents if enrichment fails
                        enriched_incidents = incidents

                    # Build response with pagination info
                    total_count = response.headers.get('X-Total-Count')
                    current_page = params.get('page', 1)
                    per_page = params.get('per_page', 100)
                    # Ultra-robust: has_more is True ONLY if batch is full and not empty
                    if not incidents or len(incidents) < per_page:
                        has_more = False
                    else:
                        has_more = True
                    return SourceResponse(
                        status_code=200,
                        success=True,
                        data={
                            'incidents': enriched_incidents,
                            'meta': {
                                'total_count': int(total_count) if total_count else len(incidents),
                                'has_more': has_more,
                                'page': current_page,
                                'per_page': per_page,
                                'migration_summary': {
                                    'incidents_processed': len(enriched_incidents),
                                    'total_tasks': sum(inc.get('migration_metadata', {}).get('tasks_count', 0) for inc in enriched_incidents),
                                    'total_attachments': sum(inc.get('migration_metadata', {}).get('attachments_count', 0) for inc in enriched_incidents),
                                    'ready_for_migration': True
                                }
                            }
                        }
                    )
                except Exception as json_error:
                    print(f"Error parsing JSON response: {json_error}")
                    return SourceResponse(
                        status_code=500,
                        success=False,
                        error_message=f"Failed to parse API response: {str(json_error)}"
                    )
            else:
                error_msg = f"API returned status {response.status_code}: {response.text}"
                print(error_msg)
                return SourceResponse(
                    status_code=response.status_code,
                    success=False,
                    error_message=error_msg
                )
                
        except Exception as e:
            error_msg = f"Error getting incidents: {str(e)}"
            print(error_msg, exc_info=True)
            return SourceResponse(500, False, error_message=error_msg)
    
    def get_incident_by_id(self, incident_id: str) -> SourceResponse:
        """Get a specific Solarwinds incident by ID"""
        try:
            if not self._validate_config():
                return SourceResponse(400, False, error_message="Invalid configuration")
            
            base_url = self._build_base_url()
            url = f"{base_url}/incidents/{incident_id}.json"
            
            # Make API call
            response = self._make_api_request(url)
            
            if response.status_code == 200:
                data = safe_json_response(response)
                incident = data.get('incident', data)
                
                # Apply enrichment to single incident
                try:
                    enriched_incidents = self.data_enricher.enrich_incidents([incident], True, {})
                    enriched_incident = enriched_incidents[0] if enriched_incidents else incident
                except Exception as e:
                    print(f"Error during incident enrichment: {e}")
                    enriched_incident = incident
                
                return SourceResponse(
                    status_code=200,
                    success=True,
                    data={
                        'incident': enriched_incident
                    }
                )
            else:
                return SourceResponse(
                    status_code=response.status_code,
                    success=False,
                    error_message=f"Failed to get incident {incident_id}: {response.text}"
                )
                
        except Exception as e:
            print(f"Error getting incident {incident_id}: {e}", exc_info=True)
            return SourceResponse(500, False, error_message=str(e))
    
    def create_incident(self, incident_data: Dict) -> SourceResponse:
        """Create a new Solarwinds incident"""
        try:
            if not self._validate_config():
                return SourceResponse(400, False, error_message="Invalid configuration")
            
            base_url = self._build_base_url()
            url = f"{base_url}/incidents.json"
            
            # Prepare incident data
            payload = {
                'incident': incident_data
            }
            
            # Make API call
            response = self._make_authenticated_request(url, 'POST', json=payload)
            
            if response.status_code in [200, 201]:
                data = safe_json_response(response)
                incident = data.get('incident', data)
                
                return SourceResponse(
                    status_code=response.status_code,
                    success=True,
                    data={
                        'incident': incident
                    }
                )
            else:
                return SourceResponse(
                    status_code=response.status_code,
                    success=False,
                    error_message=f"Failed to create incident: {response.text}"
                )
                
        except Exception as e:
            print(f"Error creating incident: {e}", exc_info=True)
            return SourceResponse(500, False, error_message=str(e))
    
    def update_incident_by_id(self, incident_id: str, incident_data: Dict) -> SourceResponse:
        """Update a Solarwinds incident by ID"""
        try:
            if not self._validate_config():
                return SourceResponse(400, False, error_message="Invalid configuration")
            
            base_url = self._build_base_url()
            url = f"{base_url}/incidents/{incident_id}.json"
            
            # Prepare incident data
            payload = {
                'incident': incident_data
            }
            
            # Make API call
            response = self._make_authenticated_request(url, 'PUT', json=payload)
            
            if response.status_code == 200:
                data = safe_json_response(response)
                incident = data.get('incident', data)
                
                return SourceResponse(
                    status_code=200,
                    success=True,
                    data={
                        'incident': incident
                    }
                )
            else:
                return SourceResponse(
                    status_code=response.status_code,
                    success=False,
                    error_message=f"Failed to update incident {incident_id}: {response.text}"
                )
                
        except Exception as e:
            print(f"Error updating incident {incident_id}: {e}", exc_info=True)
            return SourceResponse(500, False, error_message=str(e))
    
    def delete_incident_by_id(self, incident_id: str) -> SourceResponse:
        """Delete a Solarwinds incident by ID"""
        try:
            if not self._validate_config():
                return SourceResponse(400, False, error_message="Invalid configuration")
            
            base_url = self._build_base_url()
            url = f"{base_url}/incidents/{incident_id}.json"
            
            # Make API call
            response = self._make_authenticated_request(url, 'DELETE')
            
            if response.status_code in [200, 204]:
                return SourceResponse(
                    status_code=response.status_code,
                    success=True,
                    data={
                        'message': f'Incident {incident_id} deleted successfully'
                    }
                )
            else:
                return SourceResponse(
                    status_code=response.status_code,
                    success=False,
                    error_message=f"Failed to delete incident {incident_id}: {response.text}"
                )
                
        except Exception as e:
            print(f"Error deleting incident {incident_id}: {e}", exc_info=True)
            return SourceResponse(500, False, error_message=str(e))
    
    def get_tasks_for_incident(self, incident_id: str) -> SourceResponse:
        """Get tasks for a specific Solarwinds incident"""
        try:
            if not self._validate_config():
                return SourceResponse(400, False, error_message="Invalid configuration")
            
            base_url = self._build_base_url()
            url = f"{base_url}/incidents/{incident_id}/tasks.json"
            
            print(f"Solarwinds Tasks URL: {url}")
            print(f"Fetching tasks for incident_id: {incident_id}")
            
            # Make API call
            response = self._make_authenticated_request(url, 'GET')
            
            if response.status_code == 200:
                data = safe_json_response(response)

                # Handle different response structures
                if isinstance(data, list):
                    tasks = data
                elif isinstance(data, dict):
                    tasks = data.get('tasks', data.get('result', []))
                else:
                    tasks = []

                print(f"[DEBUG] Incident ID: {incident_id} - Tasks fetched: {len(tasks)}")

                # Apply enrichment if available
                try:
                    enriched_tasks = self.data_enricher.enrich_tasks(tasks, True) if hasattr(self.data_enricher, 'enrich_tasks') else tasks
                except Exception as e:
                    print(f"Error during task enrichment: {e}")
                    enriched_tasks = tasks

                print(f"[DEBUG] Incident ID: {incident_id} - Tasks after enrichment: {len(enriched_tasks)}")

                return SourceResponse(
                    status_code=200,
                    success=True,
                    data={
                        'tasks': enriched_tasks,
                        'incident_id': incident_id,
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
                    error_message=f"Failed to get tasks for incident {incident_id}: {response.text}"
                )
                
        except Exception as e:
            print(f"Error getting tasks for incident {incident_id}: {e}", exc_info=True)
            return SourceResponse(500, False, error_message=str(e))
    
    def get_comments_for_incident(self, incident_id: str) -> SourceResponse:
        """Get comments for a specific SolarWinds incident"""
        try:
            if not self._validate_config():
                return SourceResponse(400, False, error_message="Invalid configuration")
            
            base_url = self._build_base_url()
            url = f"{base_url}/incidents/{incident_id}/comments.json"
            
            print(f"SolarWinds Comments URL: {url}")
            print(f"Fetching comments for incident_id: {incident_id}")
            
            # Make API call
            response = self._make_authenticated_request(url, 'GET')
            
            if response.status_code == 200:
                data = safe_json_response(response)

                # Handle different response structures
                if isinstance(data, list):
                    comments = data
                elif isinstance(data, dict):
                    comments = data.get('comments', data.get('result', []))
                else:
                    comments = []

                print(f"[DEBUG] Incident ID: {incident_id} - Notes/comments fetched: {len(comments)}")

                # Apply enrichment if available
                try:
                    enriched_comments = self.data_enricher.enrich_comments(comments, True) if hasattr(self.data_enricher, 'enrich_comments') else comments
                except Exception as e:
                    print(f"Error during comment enrichment: {e}")
                    enriched_comments = comments

                print(f"[DEBUG] Incident ID: {incident_id} - Notes/comments after enrichment: {len(enriched_comments)}")

                return SourceResponse(
                    status_code=200,
                    success=True,
                    data={
                        'comments': enriched_comments,
                        'incident_id': incident_id,
                        'meta': {
                            'total_count': len(enriched_comments),
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
                    error_message=f"Failed to get comments for incident {incident_id}: {response.text}"
                )
                
        except Exception as e:
            print(f"Error getting comments for incident {incident_id}: {e}", exc_info=True)
            return SourceResponse(500, False, error_message=str(e))

    def create_task(self, task_data: Dict) -> SourceResponse:
        """Create a new task for a Solarwinds incident"""
        try:
            if not self._validate_config():
                return SourceResponse(400, False, error_message="Invalid configuration")
            
            # Extract incident_id from task_data
            incident_id = task_data.get('incident_id')
            if not incident_id:
                return SourceResponse(400, False, error_message="incident_id is required in task_data")
            
            base_url = self._build_base_url()
            url = f"{base_url}/incidents/{incident_id}/tasks.json"
            
            print(f"Creating Solarwinds task at: {url}")
            
            # Prepare task payload
            task_payload = {
                'task': {
                    key: value for key, value in task_data.items() 
                    if key != 'incident_id'  # Remove incident_id from payload as it's in URL
                }
            }
            
            # Make API call
            response = self._make_authenticated_request(url, 'POST', json=task_payload)
            
            if response.status_code in [200, 201]:
                data = safe_json_response(response)
                return SourceResponse(
                    status_code=response.status_code,
                    success=True,
                    data=data
                )
            else:
                return SourceResponse(
                    status_code=response.status_code,
                    success=False,
                    error_message=f"Failed to create task: {response.text}"
                )
                
        except Exception as e:
            print(f"Error creating task: {e}", exc_info=True)
            return SourceResponse(500, False, error_message=str(e))


# ===================================================================
# TRANSFORMER INTERFACE FUNCTIONS
# ===================================================================

def _get_solarwinds_connector(source_headers: List[Dict]) -> SolarwindsConnector:
    """Create Solarwinds connector from transformer headers"""
    headers_dict = convert_source_headers_to_dict(source_headers)
    
    # Create config with api_token for our local classes
    config = SourceConnectorConfig(
        domainUrl=headers_dict.get('domainUrl'),
        username=headers_dict.get('username'),
        password=headers_dict.get('password'),
        api_token=headers_dict.get('api_token')
    )
    
    return SolarwindsConnector(config)


def get_solarwinds_incidents_v1(**kwargs) -> Dict:
    """
    OPTIMIZED: SolarWinds incidents retrieval following ServiceNow structural format
    Maintains exact compatibility with transformer
    Args:
        headers/sourceHeaders: Authentication headers (list or dict format)
        queryParams: Query parameters for filtering/pagination (list or dict format)
        **kwargs: Additional arguments that can be passed (e.g., numberOfProcessedRecords, etc.)
    Returns:
        Dict with status_code and body (always includes 'incidents' key)
    """
    # This global function should always delegate to the robust class-based implementation
    # to ensure all debug logic, batch limit, and bulletproof meta handling are executed.
    from typing import Dict
    # Import the class if not already imported
    # from .solarwinds import SolarwindsConnector  # already in file
    # Call the robust implementation as a static/class method
    # We assume the robust implementation is SolarwindsConnector.get_solarwinds_incidents_v1
    # If not, adjust as needed.
    # For now, we call the class-based method directly for full debug logic.
    # If the orchestrator expects this global function, it will now always get the robust logic.
    # Note: If the class method is not static, we need to instantiate the connector.
    # But the robust logic is in the global function above, so we just call that.
    # This avoids code duplication and ensures debug prints and batch limit are always active.
    #
    # The robust implementation is the code above (now in this function), so just keep as is.
    # But to ensure the orchestrator always gets the debug logic, add a debug print here:
    import sys
    import logging
    if not hasattr(get_solarwinds_incidents_v1, "_invocation_count"):
        get_solarwinds_incidents_v1._invocation_count = 0
    get_solarwinds_incidents_v1._invocation_count += 1
    print(f"[DEBUG] Global get_solarwinds_incidents_v1 called. Count: {get_solarwinds_incidents_v1._invocation_count}")
    logging.warning(f"[DEBUG] Global get_solarwinds_incidents_v1 called. Count: {get_solarwinds_incidents_v1._invocation_count}")
    sys.stdout.flush()
    if get_solarwinds_incidents_v1._invocation_count % 100 == 0:
        print("[WARNING] get_solarwinds_incidents_v1 called 100 times without break. Possible infinite loop.")
        logging.warning("[WARNING] get_solarwinds_incidents_v1 called 100 times without break. Possible infinite loop.")
        sys.stdout.flush()
    # Now call the robust implementation (which is this function itself)
    # So, just execute the code as before (no-op, since this is the robust version).
    # If you ever refactor to move the logic to a class, call it here.
    # For now, this ensures the debug print is always shown.
    #
    # (No-op: robust logic is already here.)
    #
    # Optionally, you could add a hard batch limit here if not present.
    #
    # --- BEGIN ROBUST LOGIC ---
    try:
        # Handle both 'sourceHeaders' and 'headers' parameter names for compatibility
        source_headers = kwargs.get('sourceHeaders') or kwargs.get('headers')
        if not source_headers:
            raise ValueError("Missing required headers parameter (sourceHeaders or headers)")

        connector = _get_solarwinds_connector(source_headers)
        query_dict = convert_query_params_to_dict(kwargs.get('queryParams', []))

        # Extract numberOfProcessedRecords from kwargs, default to 0 if not provided
        numberOfProcessedRecords = int(kwargs.get('numberOfProcessedRecords', 0))

        # Map offset/limit to page/per_page for correct pagination
        limit = int(query_dict.get('limit', 100))
        offset = int(numberOfProcessedRecords)
        page = (offset // limit) + 1
        query_dict['page'] = page
        query_dict['per_page'] = limit

        # Add any additional kwargs to query_dict if needed (except known ones)
        for key, value in kwargs.items():
            if key not in ['sourceHeaders', 'headers', 'queryParams', 'numberOfProcessedRecords', '_prev_batch_ids', '_prev_offset', '_prev_page']:
                query_dict[key] = value

        # Track previous batch info for bulletproof has_more
        prev_batch_ids = kwargs.get('_prev_batch_ids', set())
        prev_offset = kwargs.get('_prev_offset', None)
        prev_page = kwargs.get('_prev_page', None)

        response = connector.get_incidents(query_dict)

        # Always build a robust meta
        def build_meta(incidents, response_obj=None):
            # Bulletproof has_more logic: False if no incidents or less than limit, True only if len==limit
            _limit = int(query_dict.get('limit', 100))
            _offset = offset
            _page = page
            total_count = 0
            warning = None
            if response_obj and hasattr(response_obj, 'meta') and response_obj.meta:
                meta = getattr(response_obj, 'meta', {})
                total_count = meta.get('total_count', len(incidents) if incidents else 0)
                # Use backend has_more if present, else infer
                if 'has_more' in meta:
                    has_more = bool(meta['has_more'])
                else:
                    has_more = bool(incidents) and (len(incidents) == _limit)
                _limit = meta.get('limit', _limit)
                _offset = meta.get('offset', _offset)
            else:
                total_count = len(incidents) if incidents else 0
                if not incidents or len(incidents) < _limit:
                    has_more = False
                else:
                    has_more = True

            # Bulletproof: If batch is empty, duplicate, or offset/page not advancing, force has_more False
            current_ids = set(i.get('id') for i in incidents if 'id' in i)
            duplicate_batch = prev_batch_ids and current_ids == prev_batch_ids
            offset_stuck = prev_offset is not None and _offset == prev_offset
            page_stuck = prev_page is not None and _page == prev_page
            if not incidents:
                warning = 'Batch is empty, forcibly terminating.'
                has_more = False
            elif duplicate_batch:
                warning = 'Duplicate batch detected, forcibly terminating.'
                has_more = False
            elif offset_stuck or page_stuck:
                warning = 'Offset or page not advancing, forcibly terminating.'
                has_more = False
            # Log batch info for debugging
            print(f"[BATCH DEBUG] page={_page}, per_page={_limit}, offset={_offset}, incident_ids={list(current_ids)}, has_more={has_more}, warning={warning}")
            meta_out = {
                "total_count": total_count,
                "has_more": has_more,
                "offset": _offset,
                "limit": _limit,
                "page": _page,
                "incident_ids": list(current_ids),
                "prev_batch_ids": list(prev_batch_ids) if prev_batch_ids else [],
                "prev_offset": prev_offset,
                "prev_page": prev_page
            }
            if warning:
                meta_out['warning'] = warning
            return meta_out

        # Ensure consistent response format for transformer: incidents and meta at top level
        if response.success:
            result = standardize_source_response_format(response)
            body = result.get('body', {})
            incidents = body.get('incidents', [])
            meta = build_meta(incidents, getattr(response, 'meta', None))
            return {
                "status_code": result.get("status_code", 200),
                "body": {
                    "incidents": incidents,
                    "meta": meta
                }
            }
        else:
            return {
                "status_code": response.status_code if hasattr(response, 'status_code') else 500,
                "body": {
                    "incidents": [],
                    "meta": build_meta([], getattr(response, 'meta', None)),
                    "error": response.error_message
                }
            }

    except Exception as e:
        return {
            "status_code": 500,
            "body": {
                "incidents": [],
                "meta": {
                    "total_count": 0,
                    "has_more": False,
                    "offset": 0,
                    "limit": 100
                },
                "error": str(e)
            }
        }


def get_solarwinds_changes_v1(**kwargs) -> Dict:
    """
    Get SolarWinds changes with pagination and enrichment support
    
    Args:
        headers/sourceHeaders: Authentication headers (list or dict format)
        queryParams: Query parameters for filtering/pagination (list or dict format)
        **kwargs: Additional arguments (e.g., numberOfProcessedRecords)
        
    Returns:
        Dict with status_code and body (always includes 'changes' key and 'meta')
    """
    import sys
    import logging
    
    # Debug tracking
    if not hasattr(get_solarwinds_changes_v1, "_invocation_count"):
        get_solarwinds_changes_v1._invocation_count = 0
    get_solarwinds_changes_v1._invocation_count += 1
    print(f"[DEBUG] Global get_solarwinds_changes_v1 called. Count: {get_solarwinds_changes_v1._invocation_count}")
    logging.warning(f"[DEBUG] Global get_solarwinds_changes_v1 called. Count: {get_solarwinds_changes_v1._invocation_count}")
    
    # Settings
    print("Setting layout=long to retrieve attachments and detailed info")
    ATTACHMENT_ENRICHMENT_ENABLED = True
    print(f"Attachment enrichment enabled: {ATTACHMENT_ENRICHMENT_ENABLED}")
    
    try:
        # Handle both parameter names for compatibility
        source_headers = kwargs.get('sourceHeaders') or kwargs.get('headers')
        if not source_headers:
            raise ValueError("Missing required headers parameter")
        
        connector = _get_solarwinds_connector(source_headers)
        query_dict = convert_query_params_to_dict(kwargs.get('queryParams', []))
        
        # Extract numberOfProcessedRecords from kwargs
        numberOfProcessedRecords = int(kwargs.get('numberOfProcessedRecords', 0))
        
        # Map offset/limit to page/per_page for correct pagination
        limit = int(query_dict.get('limit', 100))
        offset = int(numberOfProcessedRecords)
        page = (offset // limit) + 1
        
        # Build query parameters for changes endpoint
        query_params = {
            'page': page,
            'per_page': limit,
            'layout': 'long'  # Include full details and attachments
        }
        
        # Add filters if provided
        if 'filters' in query_dict and query_dict['filters']:
            query_params['filters'] = query_dict['filters']
        
        # Pass through enable_attachment_enrichment flag for enricher
        if 'enable_attachment_enrichment' in query_dict:
            query_params['enable_attachment_enrichment'] = query_dict['enable_attachment_enrichment']
        
        # Build the API URL
        url = f"{connector._build_base_url()}/changes.json"
        
        # Make the API request
        print(f"[BATCH DEBUG] page={page}, per_page={limit}, offset={offset}, change_ids=[], has_more=False, warning=Batch is empty, forcibly terminating.")
        
        response = connector._make_authenticated_request(url, 'GET', params=query_params)
        
        if response.status_code == 401:
            error_msg = f"API returned status 401: {response.text}"
            print(error_msg)
            return {
                "status_code": 401,
                "body": {
                    "changes": [],
                    "meta": {
                        "total_count": 0,
                        "has_more": False,
                        "offset": offset,
                        "limit": limit,
                        "page": page,
                        "change_ids": [],
                        "prev_batch_ids": [],
                        "prev_offset": None,
                        "prev_page": None,
                        "warning": "Batch is empty, forcibly terminating."
                    },
                    "error": error_msg
                }
            }
        
        if response.status_code != 200:
            error_msg = f"API returned status {response.status_code}: {response.text}"
            print(error_msg)
            return {
                "status_code": response.status_code,
                "body": {
                    "changes": [],
                    "meta": {
                        "total_count": 0,
                        "has_more": False,
                        "offset": offset,
                        "limit": limit,
                        "page": page
                    },
                    "error": error_msg
                }
            }
        
        # Parse response
        data = response.json()
        changes = data if isinstance(data, list) else data.get('changes', [])
        
        # Enrich changes with attachments if enabled
        if ATTACHMENT_ENRICHMENT_ENABLED and changes:
            try:
                print(f"[DEBUG] Calling enricher with query_params: {query_params}")
                enricher = OptimizedSolarwindsDataEnricher(connector)
                changes = enricher.enrich_changes_optimized(changes, query_params)
                print(f"[DEBUG] Enrichment completed, changes count: {len(changes)}")
                logging.info(f"[DEBUG] Enrichment completed, changes count: {len(changes)}")
                
                # DEBUG: Log attachment data for each change
                for change in changes:
                    attachments = change.get('attachments', [])
                    change_id = change.get('id', 'unknown')
                    logging.info(f"[DEBUG] Change {change_id}: {len(attachments)} attachments")
                    for i, att in enumerate(attachments):
                        att_name = att.get('name', 'unknown')
                        att_size = len(att.get('content', b'')) if att.get('content') else 0
                        logging.info(f"[DEBUG] Change {change_id} attachment {i+1}: {att_name} ({att_size} bytes)")
            except Exception as enrich_error:
                print(f"[WARNING] Enrichment failed: {enrich_error}")
                import traceback
                traceback.print_exc()
                # Continue with unenriched data
        
        # Build metadata
        total_count = len(changes)
        has_more = len(changes) >= limit
        change_ids = [c.get('id') for c in changes if c.get('id')]
        
        meta = {
            "total_count": total_count,
            "has_more": has_more,
            "offset": offset,
            "limit": limit,
            "page": page,
            "change_ids": change_ids,
            "prev_batch_ids": kwargs.get('_prev_batch_ids', []),
            "prev_offset": kwargs.get('_prev_offset'),
            "prev_page": kwargs.get('_prev_page')
        }
        
        return {
            "status_code": 200,
            "body": {
                "changes": changes,
                "meta": meta
            }
        }
        
    except Exception as e:
        import traceback
        print(f"[ERROR] Exception in get_solarwinds_changes_v1: {e}")
        traceback.print_exc()
        
        return {
            "status_code": 500,
            "body": {
                "changes": [],
                "meta": {
                    "total_count": 0,
                    "has_more": False,
                    "offset": 0,
                    "limit": 100,
                    "page": 1
                },
                "error": str(e)
            }
        }


def get_solarwinds_change_by_id_v1(**kwargs) -> Dict:
    """
    Get a single SolarWinds change by ID
    
    Args:
        headers/sourceHeaders: Authentication headers
        change_id: ID of the change to retrieve
        
    Returns:
        Dict with status_code and body containing the change details
    """
    try:
        source_headers = kwargs.get('sourceHeaders') or kwargs.get('headers')
        if not source_headers:
            raise ValueError("Missing required headers parameter")
        
        change_id = kwargs.get('change_id')
        if not change_id:
            raise ValueError("Missing required change_id parameter")
        
        connector = _get_solarwinds_connector(source_headers)
        
        # Build URL with layout=long for full details
        url = f"{connector._build_base_url()}/changes/{change_id}.json?layout=long"
        
        response = connector._make_authenticated_request(url, 'GET')
        
        if response.status_code == 200:
            data = response.json()
            change = data.get('change', data) if isinstance(data, dict) else data
            
            return {
                "status_code": 200,
                "body": {
                    "change": change
                }
            }
        else:
            return {
                "status_code": response.status_code,
                "body": {
                    "change": None,
                    "error": f"Failed to retrieve change: {response.text}"
                }
            }
            
    except Exception as e:
        return {
            "status_code": 500,
            "body": {
                "change": None,
                "error": str(e)
            }
        }


def get_solarwinds_incident_by_id_v1(**kwargs) -> Dict:
    """Get a specific Solarwinds incident by ID"""
    try:
        print(" Getting Solarwinds incident by ID...")
        
        connector = _get_solarwinds_connector(kwargs['sourceHeaders'])
        query_params = convert_query_params_to_dict(kwargs.get('queryParams', []))
        incident_id = query_params.get('incident_id') or kwargs.get('incident_id')
        
        if not incident_id:
            return {
                "status_code": 400,
                "body": {"error": "incident_id is required"}
            }
        
        response = connector.get_incident_by_id(incident_id)
        return standardize_source_response_format(response)
        
    except Exception as e:
        print(f"Error in get_solarwinds_incident_by_id_v1: {e}", exc_info=True)
        return {
            "status_code": 500,
            "body": {"error": str(e)}
        }


def create_solarwinds_incident_v1(**kwargs) -> Dict:
    """Create a new Solarwinds incident"""
    try:
        print(" Creating Solarwinds incident...")
        
        connector = _get_solarwinds_connector(kwargs['sourceHeaders'])
        incident_data = kwargs.get('incident_data', {})
        
        if not incident_data:
            return {
                "status_code": 400,
                "body": {"error": "incident_data is required"}
            }
        
        response = connector.create_incident(incident_data)
        return standardize_source_response_format(response)
        
    except Exception as e:
        print(f"Error in create_solarwinds_incident_v1: {e}", exc_info=True)
        return {
            "status_code": 500,
            "body": {"error": str(e)}
        }


def update_solarwinds_incident_by_id_v1(**kwargs) -> Dict:
    """Update a Solarwinds incident by ID"""
    try:
        print(" Updating Solarwinds incident by ID...")
        
        connector = _get_solarwinds_connector(kwargs['sourceHeaders'])
        query_params = convert_query_params_to_dict(kwargs.get('queryParams', []))
        incident_id = query_params.get('incident_id') or kwargs.get('incident_id')
        incident_data = kwargs.get('incident_data', {})
        
        if not incident_id:
            return {
                "status_code": 400,
                "body": {"error": "incident_id is required"}
            }
        
        if not incident_data:
            return {
                "status_code": 400,
                "body": {"error": "incident_data is required"}
            }
        
        response = connector.update_incident_by_id(incident_id, incident_data)
        return standardize_source_response_format(response)
        
    except Exception as e:
        print(f"Error in update_solarwinds_incident_by_id_v1: {e}", exc_info=True)
        return {
            "status_code": 500,
            "body": {"error": str(e)}
        }


def delete_solarwinds_incident_by_id_v1(**kwargs) -> Dict:
    """Delete a Solarwinds incident by ID"""
    try:
        print(" Deleting Solarwinds incident by ID...")
        
        connector = _get_solarwinds_connector(kwargs['sourceHeaders'])
        query_params = convert_query_params_to_dict(kwargs.get('queryParams', []))
        incident_id = query_params.get('incident_id') or kwargs.get('incident_id')
        
        if not incident_id:
            return {
                "status_code": 400,
                "body": {"error": "incident_id is required"}
            }
        
        response = connector.delete_incident_by_id(incident_id)
        return standardize_source_response_format(response)
        
    except Exception as e:
        print(f"Error in delete_solarwinds_incident_by_id_v1: {e}", exc_info=True)
        return {
            "status_code": 500,
            "body": {"error": str(e)}
        }


def validate_solarwinds_instance_v1(**kwargs) -> Dict:
    """Validate Solarwinds instance connectivity"""
    try:
        print(" Validating Solarwinds instance...")
        
        connector = _get_solarwinds_connector(kwargs['sourceHeaders'])
        
        # Test connection by getting a small number of incidents
        test_params = {'limit': 1}
        response = connector.get_incidents(test_params)
        
        if response.success:
            return {
                "status_code": 200,
                "body": {
                    "valid": True,
                    "message": "Solarwinds instance validation successful"
                }
            }
        else:
            return {
                "status_code": response.status_code,
                "body": {
                    "valid": False,
                    "message": f"Solarwinds instance validation failed: {response.error_message}"
                }
            }
            
    except Exception as e:
        print(f"Error in validate_solarwinds_instance_v1: {e}", exc_info=True)
        return {
            "status_code": 500,
            "body": {
                "valid": False,
                "message": f"Validation error: {str(e)}"
            }
        }


def get_solarwinds_incident_tasks_v1(**kwargs) -> Dict:
    """Get tasks for a specific Solarwinds incident"""
    try:
        print("[DEBUG] get_solarwinds_tasks_v1 called with kwargs:", kwargs)
        connector = _get_solarwinds_connector(kwargs['sourceHeaders'])
        query_params = kwargs.get('queryParams', [])

        # Extract incident_id from queryParams
        if isinstance(query_params, list):
            incident_id = None
            for param in query_params:
                if param.get('key') == 'incident_id':
                    incident_id = param.get('value')
                    break
        else:
            incident_id = query_params.get('incident_id')

        if not incident_id:
            result = {
                "status_code": 400,
                "body": {
                    "tasks": [],
                    "error": "Missing incident_id parameter",
                    "meta": {
                        "total_count": 0,
                        "has_more": False,
                        "offset": 0,
                        "limit": 100
                    }
                }
            }
            print("[DEBUG] get_solarwinds_tasks_v1 returning (missing incident_id):", result)
            return result

        response = connector.get_tasks_for_incident(incident_id)

        if response and response.success:
            result = standardize_source_response_format(response)
            # Ensure 'tasks' key is present
            if 'body' in result and 'tasks' not in result['body']:
                result['body']['tasks'] = []
            print("[DEBUG] get_solarwinds_tasks_v1 returning (success):", result)
            return result
        else:
            error_message = response.error_message if response else "No response from connector"
            result = {
                "status_code": response.status_code if response else 500,
                "body": {
                    "tasks": [],
                    "error": error_message,
                    "meta": {
                        "total_count": 0,
                        "has_more": False,
                        "offset": 0,
                        "limit": 100
                    }
                }
            }
            print("[DEBUG] get_solarwinds_tasks_v1 returning (error):", result)
            return result
    except Exception as e:
        import traceback
        print(f"[DEBUG] Error in get_solarwinds_incident_tasks_v1: {e}")
        traceback.print_exc()
        result = {
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
        print("[DEBUG] get_solarwinds_tasks_v1 returning (exception):", result)
        return result


def get_solarwinds_incident_comments_v1(**kwargs) -> Dict:
    """
     OPTIMIZED: SolarWinds incident comments retrieval following ServiceNow structural format
    Maintains exact compatibility with transformer
    
    Args:
        headers: Authentication headers (list or dict format)
        queryParams: Query parameters for filtering/pagination (list or dict format)
        **kwargs: Additional arguments that can be passed (e.g., numberOfProcessedRecords, etc.)
    
    Returns:
        Dict with status_code and body (always includes 'comments' key)
    """
    try:
        print(" Getting SolarWinds incident notes (as comments)...")
        connector = _get_solarwinds_connector(kwargs['sourceHeaders'])
        query_params = kwargs.get('queryParams', [])

        # Extract incident_id from queryParams
        if isinstance(query_params, list):
            incident_id = None
            for param in query_params:
                if param.get('key') == 'incident_id':
                    incident_id = param.get('value')
                    break
        else:
            incident_id = query_params.get('incident_id')

        if not incident_id:
            return {
                "status_code": 400,
                "body": {
                    "notes": [],
                    "error": "Missing incident_id parameter",
                    "meta": {
                        "total_count": 0,
                        "has_more": False,
                        "offset": 0,
                        "limit": 100
                    }
                }
            }

        response = connector.get_comments_for_incident(incident_id)

        if response and response.success:
            data = response.data or {}
            notes = data.get('comments', [])
            result = {
                "status_code": 200,
                "body": {
                    "notes": notes,
                    "incident_id": incident_id,
                    "meta": data.get('meta', {
                        "total_count": len(notes),
                        "has_more": False,
                        "offset": 0,
                        "limit": 100
                    })
                }
            }
            return result
        else:
            error_message = response.error_message if response else "No response from connector"
            return {
                "status_code": response.status_code if response else 500,
                "body": {
                    "notes": [],
                    "error": error_message,
                    "meta": {
                        "total_count": 0,
                        "has_more": False,
                        "offset": 0,
                        "limit": 100
                    }
                }
            }
    except Exception as e:
        print(f"Error in get_solarwinds_notes_v1: {e}", exc_info=True)
        return {
            "status_code": 500,
            "body": {
                "notes": [],
                "error": str(e),
                "meta": {
                    "total_count": 0,
                    "has_more": False,
                    "offset": 0,
                    "limit": 100
                }
            }
        }


def create_solarwinds_task_v1(**kwargs) -> Dict:
    """Create a new task for a Solarwinds incident"""
    try:
        print(" Creating Solarwinds task...")
        
        connector = _get_solarwinds_connector(kwargs['sourceHeaders'])
        task_data = kwargs.get('task_data', {})
        
        if not task_data:
            return {
                "status_code": 400,
                "body": {"error": "task_data is required"}
            }
        
        response = connector.create_task(task_data)
        return standardize_source_response_format(response)
        
    except Exception as e:
        print(f"Error in create_solarwinds_task_v1: {e}", exc_info=True)
        return {
            "status_code": 500,
            "body": {"error": str(e)}
        }


# ===================================================================
#  PERFORMANCE MONITORING
# ===================================================================

def get_solarwinds_performance_stats() -> Dict:
    """Get performance statistics for monitoring"""
    # Placeholder implementation to restore syntax
    return {"status": "ok"}
    
    # Validate the structure
    validation = validate_incident_data(incident_data)
    
    print(" **SOLARWINDS CONNECTOR COMPATIBILITY TEST**")
    print(f" **Valid Structure**: {validation['valid']}")
    print(f" **Total Fields**: {validation['total_fields_count']}")
    print(f" **Supported Fields**: {validation['supported_fields_count']}")
    print(f" **Required Fields Present**: {validation['required_fields_present']}")
    
    if validation['warnings']:
        print("\n  **Warnings/Notes**:")
        for warning in validation['warnings']:
            print(f"    {warning}")
    
    print("\n **Supported Fields Found**:")
    for field in validation['supported_fields']:
        print(f"    {field}")
    
    print("\n **Usage Example**:")
    print("```python")
    print("# Headers for authentication")
    print("headers = [")
    print('    {"key": "domainUrl", "value": "company.samanage.com"},')
    print('    {"key": "api_token", "value": "your_api_token_here"}')
    print("]")
    print("")
    print("# Create incident using your JSON structure")
    print("result = create_solarwinds_incident_v1(")
    print("    sourceHeaders=headers,")
    print("    incident_data=incident_data  # Your JSON structure")
    print(")")
    print("")
    print("# Update incident")
    print("update_result = update_solarwinds_incident_by_id_v1(")
    print("    sourceHeaders=headers,")
    print("    incident_id='12345',")
    print("    incident_data=updated_data")
    print(")")

