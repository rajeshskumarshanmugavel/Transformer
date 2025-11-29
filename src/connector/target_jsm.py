"""
target_jsm.py
======================

Complete product-agnostic JSM (Jira Service Management) connector that works with ANY source platform.
ALL source-specific logic is handled by UniversalAttachmentService.

Complete version with:
- Event loop management fixes
- Multiple authentication handling (OAuth 2.0 and API Key)
- Enhanced error handling and logging
- Support for JSM Service Desk API and Jira Core API endpoints
- Proper JSM request formatting according to API specifications
"""

import requests
import logging
import asyncio
import atexit
import threading
import base64
from typing import List, Dict, Tuple, Optional
from datetime import datetime
import time
import concurrent.futures
import json

# Import base classes (your existing imports)
from src.connector.base_target_connector import (
    BaseTargetConnector, 
    BaseRateLimitHandler,
    ConnectorConfig,
    ConnectorResponse,
    InlineImage,
    convert_headers_to_dict,
    standardize_response_format
)

# Import the universal attachment service
from src.connector.utils.attachment_service import UniversalAttachmentService

# Import your existing user helper
from src.connector.utils.getfwuserid import FreshworksHelper


# ===================================================================
# PRODUCT-AGNOSTIC INLINE PROCESSOR
# ===================================================================

class UniversalInlineProcessor:
    """
    Product-agnostic inline image processor.
    ALL source-specific logic is delegated to UniversalAttachmentService.
    """
    
    def __init__(self, attachment_service: Optional[UniversalAttachmentService]):
        """Initialize with attachment service"""
        self.attachment_service = attachment_service
    
    async def process_inline_images_universal(self, description: str, 
                                            source_domain: Optional[str] = None,
                                            auth_config: Optional[Dict] = None) -> Tuple[str, List[Dict]]:
        """
        Universal inline image processing using AttachmentService.
        Works with ANY source platform automatically.
        
        Args:
            description: HTML content with inline images
            source_domain: Domain for relative URL resolution
            auth_config: Authentication configuration
            
        Returns:
            Tuple of (updated_description, processed_attachments)
        """
        # If no attachment service, return unchanged
        if not self.attachment_service:
            logging.warning("No AttachmentService available for inline image processing")
            return description, []
            
        try:
            # Use AttachmentService to handle ALL inline image processing
            result = await self.attachment_service.process_inline_images_universal(
                description, source_domain, auth_config
            )
            
            # Convert ProcessedAttachment objects to dict format
            processed_attachments = []
            for attachment in result.extracted_attachments:
                processed_attachments.append({
                    'name': attachment.name,
                    'content': attachment.content,
                    'content_type': attachment.content_type
                })
            
            if result.failed_extractions:
                logging.warning(f"Failed to extract {len(result.failed_extractions)} inline images")
            
            return result.updated_content, processed_attachments
            
        except Exception as e:
            logging.error(f"Error processing inline images: {e}")
            return description, []


# ===================================================================
# JSM RATE LIMIT HANDLER
# ===================================================================

class JSMRateLimitHandler(BaseRateLimitHandler):
    """JSM-specific rate limit handler"""
    
    def is_rate_limited(self, response: requests.Response) -> bool:
        """Check if response indicates rate limiting"""
        if response.status_code == 429:
            return True
        
        if response.status_code == 200:
            try:
                response_json = response.json()
                response_text = str(response_json).lower()
                if 'rate_limit' in response_text or 'too many requests' in response_text:
                    return True
            except:
                pass
        
        return False
    
    def get_retry_delay(self, response: requests.Response) -> int:
        """Extract retry delay from response headers"""
        retry_after = response.headers.get('Retry-After')
        if retry_after:
            try:
                return int(retry_after)
            except ValueError:
                try:
                    retry_date = datetime.strptime(retry_after, '%a, %d %b %Y %H:%M:%S GMT')
                    return max(0, int((retry_date - datetime.utcnow()).total_seconds()))
                except ValueError:
                    pass
        
        reset_time = response.headers.get('X-RateLimit-Reset') or response.headers.get('X-Rate-Limit-Reset')
        if reset_time:
            try:
                reset_timestamp = int(reset_time)
                current_timestamp = int(time.time())
                return max(0, reset_timestamp - current_timestamp)
            except ValueError:
                pass
        
        return 60


# ===================================================================
# JSM CONNECTOR CONFIG
# ===================================================================

class JSMConnectorConfig(ConnectorConfig):
    """JSM-specific connector configuration"""
    
    def __init__(self, domainUrl: str, api_key: str = None, username: str = None, password: str = None, 
                 oauth_token: str = None, service_desk_id: str = None, request_type_id: str = None):
        super().__init__(domainUrl=domainUrl, api_key=api_key)
        self.username = username
        self.password = password
        self.oauth_token = oauth_token
        self.service_desk_id = service_desk_id
        self.request_type_id = request_type_id
    
    @property
    def base_url(self) -> str:
        """Get base URL for JSM instance"""
        if self.domainUrl.startswith('http'):
            return self.domainUrl
        return f"https://{self.domainUrl}"
    
    @property
    def auth_headers(self) -> Dict[str, str]:
        """Get authentication headers based on available credentials"""
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        
        if self.oauth_token:
            headers['Authorization'] = f'Bearer {self.oauth_token}'
        
        return headers
    
    @property
    def auth_tuple(self) -> Optional[Tuple[str, str]]:
        """Get basic auth tuple if username/password or API key is available"""
        if self.username and self.password:
            return (self.username, self.password)
        elif self.api_key:
            return (self.api_key, '')
        return None


# ===================================================================
# MAIN UNIVERSAL JSM CONNECTOR
# ===================================================================

class UniversalJSMConnector(BaseTargetConnector):
    """
    Completely product-agnostic JSM connector.
    Works with ANY source platform without modification.
    """
    
    def __init__(self, config: JSMConnectorConfig, attachment_service: Optional[UniversalAttachmentService] = None):
        """
        Initialize connector
        
        Args:
            config: JSM Connector configuration
            attachment_service: Shared UniversalAttachmentService instance
        """
        super().__init__(config)
        self.jsm_config = config
        self.attachment_service = attachment_service
        self._external_attachment_service = attachment_service is not None
        
        # Always create inline processor (it can handle None attachment_service)
        self.inline_processor = UniversalInlineProcessor(self.attachment_service)
    
    @property
    def safe_attachment_service(self) -> Optional[UniversalAttachmentService]:
        """Safe property to get attachment service, returns None if not set"""
        return getattr(self, 'attachment_service', None)
    
    def _get_inline_processor(self):
        """Get universal inline processor (non-abstract version)"""
        return UniversalInlineProcessor(self.safe_attachment_service)
    
    def _get_rate_limiter(self) -> BaseRateLimitHandler:
        return JSMRateLimitHandler()
    
    def _validate_instance(self) -> bool:
        """Validate JSM instance/credentials"""
        try:
            # Try to get service desk info
            url = f"{self.jsm_config.base_url}/rest/servicedeskapi/servicedesk"
            headers = self.jsm_config.auth_headers
            auth = self.jsm_config.auth_tuple
            
            response = requests.get(url, headers=headers, auth=auth, timeout=10)
            return response.status_code != 401
        except:
            # Fallback to basic Jira API
            try:
                url = f"{self.jsm_config.base_url}/rest/api/2/myself"
                headers = self.jsm_config.auth_headers
                auth = self.jsm_config.auth_tuple
                
                response = requests.get(url, headers=headers, auth=auth, timeout=10)
                return response.status_code != 401
            except:
                return False
    
    def _build_universal_auth_config(self, headers: Dict) -> Optional[Dict]:
        """
        Simply pass headers to attachment service for auth extraction.
        NO source-specific logic here!
        """
        if not self.safe_attachment_service:
            logging.warning("No attachment service available for auth extraction")
            return None
        
        headers_dict = convert_headers_to_dict(headers) if isinstance(headers, list) else headers
        
        # Let the attachment service handle ALL source-specific logic
        _, auth_config = self.safe_attachment_service.normalize_attachment_data([], headers_dict)
        
        return auth_config
    
    async def _process_attachments_universal(self, attachments: List[Dict], 
                                           auth_config: Optional[Dict] = None) -> Tuple[List[Dict], List[str]]:
        """
        Universal attachment processing using AttachmentService.
        Enhanced with diagnostic logging.
        """
        if not attachments:
            logging.info("No attachments to process")
            return [], []
        
        logging.info(f"Processing {len(attachments)} attachments")
        
        if not self.safe_attachment_service:
            logging.error("No AttachmentService available for processing attachments")
            return [], [att.get('name', 'unknown') for att in attachments]
        
        try:
            # Log attachment details for debugging
            for i, att in enumerate(attachments):
                url = att.get('url') or att.get('content_url') or att.get('attachment_url')
                name = att.get('name') or att.get('file_name') or 'unknown'
                logging.info(f"Attachment {i+1}: {name}")
                logging.info(f"  URL: {url[:100]}..." if url else "  URL: None")
                logging.info(f"  Type: {att.get('content_type', 'unknown')}")
            
            # Log auth config
            if auth_config:
                logging.info(f"Auth config provided with keys: {list(auth_config.keys())}")
                logging.info(f"Source type: {auth_config.get('source_type', 'not set')}")
            else:
                logging.warning("No auth config provided")
            
            # Process all attachments using UniversalAttachmentService
            results = await self.safe_attachment_service.download_attachments_batch(
                attachments, auth_config
            )
            
            # Convert results to expected format
            successful_files = []
            failed_files = []
            
            for i, result in enumerate(results):
                att_name = attachments[i].get('name', attachments[i].get('file_name', 'unknown'))
                
                if result:
                    logging.info(f"Successfully downloaded: {att_name} ({len(result.content)} bytes)")
                    successful_files.append({
                        'name': result.name,
                        'content': result.content,
                        'content_type': result.content_type
                    })
                else:
                    logging.error(f"Failed to download: {att_name}")
                    failed_files.append(att_name)
            
            logging.info(f"Attachment processing complete: {len(successful_files)} successful, {len(failed_files)} failed")
            
            if failed_files:
                logging.error(f"Failed attachments: {failed_files}")
            
            return successful_files, failed_files
            
        except Exception as e:
            logging.error(f"Error in universal attachment processing: {e}")
            import traceback
            logging.error(f"Traceback: {traceback.format_exc()}")
            return [], [att.get('name', 'unknown') for att in attachments]
    
    def _get_service_desk_info(self) -> Dict:
        """Get service desk and request type information"""
        try:
            # If service_desk_id and request_type_id are provided in config, use them
            if self.jsm_config.service_desk_id and self.jsm_config.request_type_id:
                return {
                    'serviceDeskId': self.jsm_config.service_desk_id,
                    'requestTypeId': self.jsm_config.request_type_id
                }
            
            # Otherwise, get the first available service desk
            url = f"{self.jsm_config.base_url}/rest/servicedeskapi/servicedesk"
            headers = self.jsm_config.auth_headers
            auth = self.jsm_config.auth_tuple
            
            response = requests.get(url, headers=headers, auth=auth, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('values') and len(data['values']) > 0:
                    service_desk = data['values'][0]
                    service_desk_id = service_desk['id']
                    
                    # Get request types for this service desk
                    request_types_url = f"{self.jsm_config.base_url}/rest/servicedeskapi/servicedesk/{service_desk_id}/requesttype"
                    rt_response = requests.get(request_types_url, headers=headers, auth=auth, timeout=10)
                    
                    if rt_response.status_code == 200:
                        rt_data = rt_response.json()
                        if rt_data.get('values') and len(rt_data['values']) > 0:
                            request_type = rt_data['values'][0]  # Use first available request type
                            return {
                                'serviceDeskId': service_desk_id,
                                'requestTypeId': request_type['id']
                            }
            
            logging.warning("Could not automatically determine service desk info, using defaults")
            return {'serviceDeskId': '1', 'requestTypeId': '1'}
            
        except Exception as e:
            logging.error(f"Error getting service desk info: {e}")
            return {'serviceDeskId': '1', 'requestTypeId': '1'}
    
    def _transform_custom_fields_to_jsm_format(self, custom_fields: Dict) -> Dict:
        """
        Transform custom_fields object to JSM requestFieldValues format.
        Assumes custom field names are already in correct customfield_xxxxx format.
        """
        jsm_fields = {}
        
        if not custom_fields or not isinstance(custom_fields, dict):
            return jsm_fields
        
        for field_name, field_value in custom_fields.items():
            if field_value is None or field_value == '':
                continue
                
            # Pass through the field as-is since it should already be in correct format
            jsm_fields[field_name] = field_value
        
        return jsm_fields
    
    def _format_field_value_for_jsm(self, field_name: str, field_value) -> any:
        """
        Format field values according to JSM requirements.
        Some fields need specific formatting like {"value": "..."} or {"id": "..."}
        
        To add more fields that need {"value": "..."} format, add them to value_wrapped_fields list.
        To add more fields that need {"id": "..."} format, add them to id_wrapped_fields list.
        To add more fields that need {"name": "..."} format, add them to name_wrapped_fields list.
        """
        if field_value is None or field_value == '':
            return None
        
        # Fields that need {"value": "..."} format
        # Add any field names (including custom fields) that should be wrapped in {"value": "..."}
        value_wrapped_fields = [
            'priority', 
            'status', 
            'impact', 
            'urgency',
            'resolution',
            'customfield_10010',  # Example: if customfield_10010 needs {"value": "..."} format
            'customfield_10011',  # Example: if customfield_10011 needs {"value": "..."} format
            # Add more fields here as needed
        ]
        
        # Fields that need {"id": "..."} format  
        # Add any field names that should be wrapped in {"id": "..."}
        id_wrapped_fields = [
            'assignee', 
            'reporter', 
            'project',
            'customfield_10020',  # Example: if customfield_10020 needs {"id": "..."} format
            # Add more fields here as needed
        ]
        
        # Fields that need {"name": "..."} format
        # Add any field names that should be wrapped in {"name": "..."}
        name_wrapped_fields = [
            'issuetype', 
            'components',
            'customfield_10030',  # Example: if customfield_10030 needs {"name": "..."} format
            # Add more fields here as needed
        ]
        
        # Check if field needs special formatting
        if field_name in value_wrapped_fields:
            return {"value": str(field_value)}
        elif field_name in id_wrapped_fields:
            return {"id": str(field_value)}
        elif field_name in name_wrapped_fields:
            return {"name": str(field_value)}
        else:
            # For most fields, return the value as-is
            return field_value
    
    def _create_jsm_request_payload(self, request_data: Dict) -> Dict:
        """
        Create properly formatted JSM request payload according to API specifications.
        
        Expected format:
        {
          "serviceDeskId": "3",
          "requestTypeId": "25", 
          "raiseOnBehalfOf": "user@example.com",
          "requestFieldValues": {
            "summary": "Issue title",
            "description": "Issue description",
            "priority": {"value": "Medium"},
            "customfield_10011": "Network"
          }
        }
        """
        # Start with base payload
        payload = {
            'requestFieldValues': {}
        }
        
        # Get serviceDeskId and requestTypeId from request data
        if request_data.get('serviceDeskId'):
            payload['serviceDeskId'] = request_data['serviceDeskId']
        else:
            raise ValueError("serviceDeskId is required in request data")
            
        if request_data.get('requestTypeId'):
            payload['requestTypeId'] = request_data['requestTypeId']
        else:
            raise ValueError("requestTypeId is required in request data")
        
        # Add requester if provided
        if request_data.get('raiseOnBehalfOf'):
            payload['raiseOnBehalfOf'] = request_data['raiseOnBehalfOf']
        elif request_data.get('email'):
            payload['raiseOnBehalfOf'] = request_data['email']
        
        # Handle request participants
        if request_data.get('requestParticipants'):
            payload['requestParticipants'] = request_data['requestParticipants']
        
        # Build requestFieldValues
        field_values = payload['requestFieldValues']
        
        # Core fields
        if request_data.get('summary'):
            field_values['summary'] = request_data['summary']
        elif request_data.get('subject'):
            field_values['summary'] = request_data['subject']
        
        if request_data.get('description'):
            field_values['description'] = request_data['description']
        
        # Format special fields that need structured values
        if request_data.get('priority'):
            field_values['priority'] = self._format_field_value_for_jsm('priority', request_data['priority'])
        
        if request_data.get('status'):
            field_values['status'] = self._format_field_value_for_jsm('status', request_data['status'])
        
        # Handle other standard fields
        standard_fields = ['assignee', 'reporter', 'components', 'labels', 'environment']
        for field in standard_fields:
            if request_data.get(field):
                field_values[field] = self._format_field_value_for_jsm(field, request_data[field])
        
        # Handle custom fields
        if request_data.get('custom_fields'):
            custom_field_values = self._transform_custom_fields_to_jsm_format(request_data['custom_fields'])
            for cf_key, cf_value in custom_field_values.items():
                field_values[cf_key] = self._format_field_value_for_jsm(cf_key, cf_value)
        
        # Handle direct custom field entries (for backward compatibility)
        for key, value in request_data.items():
            if key.startswith('customfield_') and value is not None:
                field_values[key] = self._format_field_value_for_jsm(key, value)
        
        logging.info(f"Created JSM request payload: {payload}")
        return payload
    
    def _create_ticket_api_call(self, ticket_data: Dict, files: List[Dict], headers: Dict) -> requests.Response:
        """Create JSM request using Service Desk API (implementation of base class abstract method)"""
        # Create properly formatted JSM payload
        request_payload = self._create_jsm_request_payload(ticket_data)
        
        url = f"{self.jsm_config.base_url}/rest/servicedeskapi/request"
        req_headers = self.jsm_config.auth_headers.copy()
        auth = self.jsm_config.auth_tuple
        
        logging.info(f"Creating JSM request at {url}")
        logging.info(f"Request payload: {json.dumps(request_payload, indent=2)}")
        
        # Create the request first (without attachments)
        response = self.rate_limiter.make_request_with_retry(
            url, 'POST', auth=auth, headers=req_headers, json_data=request_payload
        )
        
        # If request creation was successful and we have files, add them separately
        if response.status_code in [200, 201] and files:
            try:
                response_data = response.json()
                issue_key = response_data.get('issueKey')
                if issue_key:
                    logging.info(f"Request created successfully with key: {issue_key}")
                    self._add_attachments_to_issue(issue_key, files)
                else:
                    logging.warning("No issueKey found in response, cannot add attachments")
            except Exception as e:
                logging.error(f"Error adding attachments to JSM request: {e}")
        
        return response
    
    def _create_jsm_comment_payload(self, comment_data: Dict) -> Dict:
        """
        Create properly formatted JSM comment payload.
        
        For JSM comments, we use Jira Core API with Atlassian Document Format.
        """
        # Prepare comment body
        comment_body = comment_data.get('body', comment_data.get('content', 'Comment from migration'))
        
        # For Jira Cloud, we need to use Atlassian Document Format
        comment_payload = {
            'body': {
                'version': 1,
                'type': 'doc',
                'content': [
                    {
                        'type': 'paragraph',
                        'content': [
                            {
                                'type': 'text',
                                'text': comment_body
                            }
                        ]
                    }
                ]
            }
        }
        
        # Handle visibility (private/public)
        if comment_data.get('private'):
            comment_payload['visibility'] = {
                'type': 'role',
                'value': 'Administrators'  # Make private comments visible only to admins
            }
        
        # Handle author information if provided
        if comment_data.get('author'):
            # Note: You can't directly set the author via API in most cases
            # This would need to be handled differently, perhaps via impersonation
            logging.info(f"Original comment author: {comment_data.get('author')}")
        
        return comment_payload
    
    def _create_conversation_api_call(self, ticket_id: str, conversation_data: Dict, 
                                    files: List[Dict], headers: Dict) -> requests.Response:
        """Create JSM comment using Jira Core API (implementation of base class abstract method)"""
        # Create properly formatted comment payload - Dom - 6/7 - Below line Not needed for JSM, but for other Jira products
        #comment_payload = self._create_jsm_comment_payload(conversation_data)
        
        url = f"{self.jsm_config.base_url}/rest/servicedeskapi/request/{ticket_id}/comment"
        req_headers = self.jsm_config.auth_headers.copy()
        auth = self.jsm_config.auth_tuple
        
        logging.info(f"Creating JSM comment for issue {ticket_id}")
        logging.info(f"Comment payload: {json.dumps(conversation_data, indent=2)}")
        
        # Create comment first
        response = self.rate_limiter.make_request_with_retry(
            url, 'POST', auth=auth, headers=req_headers, json_data=conversation_data
        )
        
        # If comment creation was successful and we have files, add them to the issue
        if response.status_code in [200, 201] and files:
            try:
                logging.info(f"Comment created successfully, adding {len(files)} attachments")
                self._add_attachments_to_issue(ticket_id, files)
            except Exception as e:
                logging.error(f"Error adding attachments to JSM comment: {e}")
        
        return response
    
    def _add_attachments_to_issue(self, issue_key: str, files: List[Dict]):
        """
        Add attachments to an existing JSM issue using Jira Core API.
        JSM requires attachments to be added separately after issue creation.
        """
        if not files:
            logging.info("No files to attach")
            return
            
        try:
            url = f"{self.jsm_config.base_url}/rest/api/2/issue/{issue_key}/attachments"
            
            # Prepare headers for file upload (no Content-Type for multipart)
            upload_headers = {'X-Atlassian-Token': 'no-check'}
            if self.jsm_config.oauth_token:
                upload_headers['Authorization'] = f'Bearer {self.jsm_config.oauth_token}'
            
            auth = self.jsm_config.auth_tuple
            
            logging.info(f"Adding {len(files)} attachments to issue {issue_key}")
            
            # Add each file separately (Jira requires separate calls for each file)
            successful_attachments = []
            failed_attachments = []
            
            for file_info in files:
                try:
                    file_name = file_info.get('name', 'unknown_file')
                    file_content = file_info['content']
                    file_type = file_info.get('content_type', 'application/octet-stream')
                    
                    # Handle base64 encoded content
                    if isinstance(file_content, str):
                        try:
                            file_content = base64.b64decode(file_content)
                        except Exception as e:
                            logging.error(f"Failed to decode base64 content for {file_name}: {e}")
                            failed_attachments.append(file_name)
                            continue
                    
                    # Prepare file data for upload
                    files_data = {'file': (file_name, file_content, file_type)}
                    
                    logging.info(f"Uploading attachment: {file_name} ({len(file_content)} bytes, {file_type})")
                    
                    attachment_response = requests.post(
                        url, 
                        headers=upload_headers, 
                        auth=auth, 
                        files=files_data,
                        timeout=60  # Increased timeout for large files
                    )
                    
                    if attachment_response.status_code in [200, 201]:
                        logging.info(f"Successfully attached file: {file_name}")
                        successful_attachments.append(file_name)
                        
                        # Log response for debugging
                        try:
                            response_data = attachment_response.json()
                            logging.info(f"Attachment response: {response_data}")
                        except:
                            pass
                            
                    else:
                        logging.error(f"Failed to attach file {file_name}: {attachment_response.status_code}")
                        logging.error(f"Response: {attachment_response.text}")
                        failed_attachments.append(file_name)
                        
                except Exception as e:
                    logging.error(f"Error attaching file {file_info.get('name', 'unknown')}: {e}")
                    failed_attachments.append(file_info.get('name', 'unknown'))
            
            logging.info(f"Attachment summary: {len(successful_attachments)} successful, {len(failed_attachments)} failed")
            if failed_attachments:
                logging.error(f"Failed attachments: {failed_attachments}")
                    
        except Exception as e:
            logging.error(f"Error in _add_attachments_to_issue: {e}")
            import traceback
            logging.error(f"Traceback: {traceback.format_exc()}")
    
    async def create_ticket_universal(self, ticket_data: Dict, headers: Dict) -> ConnectorResponse:
        """
        Universal JSM ticket creation method.
        Completely source-agnostic - ALL source logic is in attachment service.
        """
        try:
            # Make a copy to avoid modifying original
            ticket_copy = ticket_data.copy()
            
            # Extract attachments
            regular_attachments = ticket_copy.pop('attachments', [])
            
            # Let attachment service handle ALL normalization and auth extraction
            auth_config = None
            normalized_attachments = regular_attachments
            
            if self.safe_attachment_service and regular_attachments:
                headers_dict = convert_headers_to_dict(headers) if isinstance(headers, list) else headers
                normalized_attachments, auth_config = self.safe_attachment_service.normalize_attachment_data(
                    regular_attachments, headers_dict
                )
                
                logging.info(f"Attachment service normalized {len(normalized_attachments)} attachments")
                if auth_config:
                    logging.info(f"Attachment service extracted auth for source: {auth_config.get('source_type', 'unknown')}")
            
            # Process inline images using AttachmentService
            description = ticket_copy.get('description', '')
            inline_attachments = []
            
            if self.safe_attachment_service and description:
                try:
                    # Get source domainUrl from auth config if available
                    source_domain = auth_config.get('source_domain') if auth_config else None
                    
                    updated_description, inline_attachments = await self.inline_processor.process_inline_images_universal(
                        description, source_domain, auth_config
                    )
                    ticket_copy['description'] = updated_description
                except Exception as e:
                    logging.error(f"Error processing inline images in ticket description: {e}")
                    # Continue without inline image processing
            
            # Process attachments using UniversalAttachmentService
            regular_files, failed_files = await self._process_attachments_universal(
                normalized_attachments, auth_config
            )
            
            # Combine all files
            all_files = regular_files + inline_attachments
            
            logging.info(f"Creating JSM ticket with {len(all_files)} attachments, {len(failed_files)} failed")
            
            # Create ticket
            response = self._create_ticket_api_call(ticket_copy, all_files, headers)
            
            return ConnectorResponse(
                status_code=response.status_code,
                success=response.status_code in [200, 201],
                data=response.json() if response.status_code in [200, 201] and response.content else None,
                error_message=response.text if response.status_code not in [200, 201] else None,
                failed_attachments=failed_files
            )
            
        except Exception as e:
            logging.error(f"Error in create_ticket_universal: {e}")
            return ConnectorResponse(
                status_code=500,
                success=False,
                error_message=str(e)
            )
    
    async def create_conversation_universal(self, query_params: List[Dict], conversation_data: Dict, 
                                      headers: Dict) -> ConnectorResponse:
        """
        Universal JSM conversation creation method.
        Completely source-agnostic - ALL source logic is in attachment service.
        """
        try:
            # Extract issue_key from query params
            issue_key = None
            for param in query_params:
                if param.get('key') in ['issue_key', 'issueKey', 'ticket_id']:
                    issue_key = param.get('value')
                    break
            
            if not issue_key:
                raise ValueError("issue_key not found in query parameters")
            
            # Make a copy to avoid modifying original
            conv_copy = conversation_data.copy()
            
            # Extract attachments
            regular_attachments = conv_copy.pop('attachments', [])
            
            # Let attachment service handle ALL normalization and auth extraction
            auth_config = None
            normalized_attachments = regular_attachments
            
            if self.safe_attachment_service and regular_attachments:
                headers_dict = convert_headers_to_dict(headers) if isinstance(headers, list) else headers
                normalized_attachments, auth_config = self.safe_attachment_service.normalize_attachment_data(
                    regular_attachments, headers_dict
                )
                
                logging.info(f"Attachment service normalized {len(normalized_attachments)} attachments")
                if auth_config:
                    logging.info(f"Attachment service extracted auth for source: {auth_config.get('source_type', 'unknown')}")
            
            # Process inline images using AttachmentService
            body = conv_copy.get('body', conv_copy.get('content', ''))
            inline_attachments = []
            
            if self.safe_attachment_service and body:
                try:
                    # Get source domainUrl from auth config if available
                    source_domain = auth_config.get('source_domain') if auth_config else None
                    
                    updated_body, inline_attachments = await self.inline_processor.process_inline_images_universal(
                        body, source_domain, auth_config
                    )
                    conv_copy['body'] = updated_body
                except Exception as e:
                    logging.error(f"Error processing inline images: {e}")
                    # Continue without inline image processing
            
            # Process attachments using UniversalAttachmentService
            regular_files, failed_files = await self._process_attachments_universal(
                normalized_attachments, auth_config
            )
            
            # Combine all files
            all_files = regular_files + inline_attachments
            
            logging.info(f"Creating JSM conversation with {len(all_files)} attachments, {len(failed_files)} failed")
            
            # Create conversation
            response = self._create_conversation_api_call(issue_key, conv_copy, all_files, headers)
            
            return ConnectorResponse(
                status_code=response.status_code,
                success=response.status_code in [200, 201],
                data=response.json() if response.status_code in [200, 201] and response.content else None,
                error_message=response.text if response.status_code not in [200, 201] else None,
                failed_attachments=failed_files
            )
            
        except Exception as e:
            logging.error(f"Error in create_conversation_universal: {e}")
            import traceback
            logging.error(f"Full traceback: {traceback.format_exc()}")
            return ConnectorResponse(
                status_code=500,
                success=False,
                error_message=str(e)
            )


# ===================================================================
# GLOBAL ATTACHMENT SERVICE MANAGEMENT
# ===================================================================

# Dom - Commented to avoid caching. Global attachment service for reuse across requests
#_universal_attachment_service: Optional[UniversalAttachmentService] = None
#_service_lock: Optional[asyncio.Lock] = None
#_service_loop: Optional[asyncio.AbstractEventLoop] = None

# Thread-local storage for event loops
_thread_local = threading.local()

def get_or_create_event_loop():
    """Get the current event loop or create a new one if needed"""
    try:
        loop = asyncio.get_running_loop()
        return loop
    except RuntimeError:
        # No loop running, create a new one
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop

def get_thread_loop():
    """Get or create an event loop for the current thread"""
    if not hasattr(_thread_local, 'loop') or _thread_local.loop.is_closed():
        _thread_local.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_thread_local.loop)
    return _thread_local.loop

async def get_universal_attachment_service() -> Optional[UniversalAttachmentService]:
    """Create fresh UniversalAttachmentService instance"""
    try:
        service = UniversalAttachmentService(max_concurrent_downloads=10)
        await service.__aenter__()
        return service
    except Exception as e:
        logging.error(f"Failed to initialize UniversalAttachmentService: {e}")
        return None

def cleanup_attachment_service():
    """Cleanup function - no longer needed since we don't cache globally"""
    # logging.info("No global attachment service to clean up")
    pass

# Register cleanup function
atexit.register(cleanup_attachment_service)


# ===================================================================
# ASYNC FUNCTIONS FOR FASTAPI
# ===================================================================

async def create_jsm_ticket_async(body: Dict, headers) -> Dict:
    """
    Async version of JSM ticket creation.
    This should be called from FastAPI endpoints.
    
    Args:
        body: Ticket data with attachments from ANY source
        headers: Headers including source authentication info
        
    Returns:
        Standardized response format
    """
    attachment_service = None
    try:
        headers_dict = convert_headers_to_dict(headers)
        
        # Get shared universal attachment service
        attachment_service = await get_universal_attachment_service()
        
        if not attachment_service:
            logging.warning("Could not initialize AttachmentService - proceeding without attachment processing")
        
        # Create JSM config from headers
        config = JSMConnectorConfig(
            domainUrl=headers_dict.get('domainUrl'),
            api_key=headers_dict.get('apikey') or headers_dict.get('api_key'),
            username=headers_dict.get('username'),
            password=headers_dict.get('password'),
            oauth_token=headers_dict.get('oauth_token') or headers_dict.get('access_token'),
            service_desk_id=headers_dict.get('service_desk_id'),
            request_type_id=headers_dict.get('request_type_id')
        )
        
        # Create connector with shared attachment service
        connector = UniversalJSMConnector(config, attachment_service)
        
        # Create ticket using universal method
        response = await connector.create_ticket_universal(body, headers)
        return standardize_response_format(response, headers)
        
    except Exception as e:
        logging.error(f"Error in create_jsm_ticket_async: {e}")
        import traceback
        logging.error(f"Full traceback: {traceback.format_exc()}")
        return {
            "status_code": 500,
            "body": str(e),
        }
    finally:
        # Clean up the service
        if attachment_service:
            try:
                await attachment_service.__aexit__(None, None, None)
            except Exception as e:
                logging.error(f"Error cleaning up attachment service: {e}")

async def create_jsm_conversation_async(queryParams: List[Dict], body: Dict, headers) -> Dict:
    """
    Async version of JSM conversation creation.
    This should be called from FastAPI endpoints.
    
    Args:
        queryParams: List of query parameters (including issue_key)
        body: Conversation data with attachments from ANY source
        headers: Headers including source authentication info
        
    Returns:
        Standardized response format
    """
    attachment_service = None
    try:
        headers_dict = convert_headers_to_dict(headers)
        
        # Get shared universal attachment service
        attachment_service = await get_universal_attachment_service()
        
        if not attachment_service:
            logging.warning("Could not initialize AttachmentService - proceeding without attachment processing")
        
        # Create JSM config from headers
        config = JSMConnectorConfig(
            domainUrl=headers_dict.get('domainUrl'),
            api_key=headers_dict.get('apikey') or headers_dict.get('api_key'),
            username=headers_dict.get('username'),
            password=headers_dict.get('password'),
            oauth_token=headers_dict.get('oauth_token') or headers_dict.get('access_token'),
            service_desk_id=headers_dict.get('service_desk_id'),
            request_type_id=headers_dict.get('request_type_id')
        )
        
        # Create connector with shared attachment service
        connector = UniversalJSMConnector(config, attachment_service)
        
        # Create conversation using universal method
        response = await connector.create_conversation_universal(queryParams, body, headers)
        return standardize_response_format(response, headers)
        
    except Exception as e:
        logging.error(f"Error in create_jsm_conversation_async: {e}")
        import traceback
        logging.error(f"Full traceback: {traceback.format_exc()}")
        return {
            "status_code": 500,
            "body": str(e),
        }
    finally:
        # Clean up the service
        if attachment_service:
            try:
                await attachment_service.__aexit__(None, None, None)
            except Exception as e:
                logging.error(f"Error cleaning up attachment service: {e}")


# ===================================================================
# SYNCHRONOUS WRAPPER FUNCTIONS
# ===================================================================

def create_jsm_ticket_universal(body: Dict, headers) -> Dict:
    """
    Universal JSM ticket creation.
    Works with attachments from ANY source platform.
    """
    try:
        # Try to get existing event loop
        try:
            loop = asyncio.get_running_loop()
            # We're in an async context, use thread pool
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(
                    lambda: asyncio.run(create_jsm_ticket_async(body, headers))
                )
                return future.result()
        except RuntimeError:
            # No event loop running, use thread-local loop
            loop = get_thread_loop()
            return loop.run_until_complete(create_jsm_ticket_async(body, headers))
            
    except Exception as e:
        logging.error(f"Error in create_jsm_ticket_universal: {e}")
        import traceback
        logging.error(f"Traceback: {traceback.format_exc()}")
        return {
            "status_code": 500,
            "body": str(e),
        }

def create_jsm_conversation_universal(queryParams: List[Dict], body: Dict, headers) -> Dict:
    """
    Universal JSM conversation creation.
    Works with attachments from ANY source platform.
    """
    try:
        # Try to get existing event loop
        try:
            loop = asyncio.get_running_loop()
            # We're in an async context, use thread pool
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(
                    lambda: asyncio.run(create_jsm_conversation_async(queryParams, body, headers))
                )
                return future.result()
        except RuntimeError:
            # No event loop running, use thread-local loop
            loop = get_thread_loop()
            return loop.run_until_complete(create_jsm_conversation_async(queryParams, body, headers))
            
    except Exception as e:
        logging.error(f"Error in create_jsm_conversation_universal: {e}")
        import traceback
        logging.error(f"Traceback: {traceback.format_exc()}")
        return {
            "status_code": 500,
            "body": str(e),
        }


# ===================================================================
# BACKWARD COMPATIBILITY FUNCTIONS
# ===================================================================

# ===================================================================
# **KWARGS PATTERN FUNCTIONS (Updated signatures)
# ===================================================================

def create_jsm_incident(**kwargs) -> Dict:
    """
    Updated incident creation function using **kwargs pattern.
    Works with ANY source platform automatically.
    
    IMPORTANT: If calling from FastAPI, use create_jsm_ticket_async instead.
    """
    return create_jsm_ticket_universal(kwargs['body'], kwargs['targetHeaders'])

def create_jsm_request(**kwargs) -> Dict:
    """
    Updated request creation function using **kwargs pattern.
    Works with ANY source platform automatically.
    
    IMPORTANT: If calling from FastAPI, use create_jsm_ticket_async instead.
    """
    return create_jsm_ticket_universal(kwargs['body'], kwargs['targetHeaders'])

def create_jsm_comment(**kwargs) -> Dict:
    """
    Updated comment creation function using **kwargs pattern.
    Works with ANY source platform automatically.
    
    IMPORTANT: If calling from FastAPI, use create_jsm_conversation_async instead.
    """
    return create_jsm_conversation_universal(kwargs['queryParams'], kwargs['body'], kwargs['targetHeaders'])

def create_jsm_note(**kwargs) -> Dict:
    """
    Alias for create_jsm_comment using **kwargs pattern.
    """
    return create_jsm_conversation_universal(kwargs['queryParams'], kwargs['body'], kwargs['targetHeaders'])


# ===================================================================
# VALIDATION FUNCTIONS
# ===================================================================

def validate_jsm_instance_v1(headers) -> Dict:
    """Validate JSM instance and credentials"""
    try:
        headers_dict = convert_headers_to_dict(headers)
        config = JSMConnectorConfig(
            domainUrl=headers_dict.get('domainUrl'),
            api_key=headers_dict.get('apikey') or headers_dict.get('api_key'),
            username=headers_dict.get('username'),
            password=headers_dict.get('password'),
            oauth_token=headers_dict.get('oauth_token') or headers_dict.get('access_token')
        )
        connector = UniversalJSMConnector(config)
        is_valid = connector._validate_instance()
        
        return {
            "status_code": 200 if is_valid else 401,
            "body": {"valid": is_valid},
            "headers": headers
        }
    except Exception as e:
        logging.error(f"Error validating JSM instance: {e}")
        return {
            "status_code": 500,
            "body": {"valid": False, "error": str(e)}
        }

def get_jsm_mapping_objects_v1(headers) -> Dict:
    """Get JSM mapping objects (service desks, request types, users, etc.)"""
    try:
        headers_dict = convert_headers_to_dict(headers)
        config = JSMConnectorConfig(
            domainUrl=headers_dict.get('domainUrl'),
            api_key=headers_dict.get('apikey') or headers_dict.get('api_key'),
            username=headers_dict.get('username'),
            password=headers_dict.get('password'),
            oauth_token=headers_dict.get('oauth_token') or headers_dict.get('access_token')
        )
        
        connector = UniversalJSMConnector(config)
        
        # Get service desks
        sd_url = f"{config.base_url}/rest/servicedeskapi/servicedesk"
        sd_response = requests.get(sd_url, headers=config.auth_headers, auth=config.auth_tuple)
        
        # Get users via Jira API
        users_url = f"{config.base_url}/rest/api/2/users/search"
        users_response = requests.get(users_url, headers=config.auth_headers, auth=config.auth_tuple)
        
        mapping_data = {}
        
        if sd_response.status_code == 200:
            mapping_data['service_desks'] = sd_response.json()
        
        if users_response.status_code == 200:
            mapping_data['users'] = users_response.json()
        
        return {
            "status_code": 200,
            "body": mapping_data,
            "headers": headers
        }
        
    except Exception as e:
        logging.error(f"Error getting JSM mapping objects: {e}")
        return {
            "status_code": 500,
            "body": {"error": str(e)}
        }


# ===================================================================
# EXAMPLE USAGE
# ===================================================================

if __name__ == "__main__":
    # Example usage with proper JSM formatting
    import json
    
    # Example headers for JSM
    headers = [
        {"key": "domainUrl", "value": "devjsmtest.atlassian.net", "description": "", "req": False},
        {"key": "username", "value": "admin@example.com", "description": "", "req": True},
        {"key": "password", "value": "your-api-token", "description": "", "req": True},
        {"key": "service_desk_id", "value": "3", "description": "", "req": False},
        {"key": "request_type_id", "value": "25", "description": "", "req": False}
    ]
    
    # Example request data in the format expected by JSM
    request_data = {
        "summary": "Unable to connect to email",
        "description": "I am unable to connect to the email server. It appears to be down.",
        "priority": "Medium",  # Will be converted to {"value": "Medium"}
        "raiseOnBehalfOf": "employee@example.com",
        "custom_fields": {
            "source_ticket_id": "INC0000060",
            "impact": "Medium",
            "urgency": "Medium", 
            "assignment_group": "Network",
            "assignee": "David Loo",
            "category": "inquiry",
            "subcategory": "email",
            "business_service": ""
        },
        "attachments": [
            {
                "name": "network_issue.pdf",
                "content": "base64-encoded-content-here",
                "content_type": "application/pdf"
            }
        ]
    }
    
    # Test the function - this will create the JSM request with proper formatting
    result = create_jsm_request(request_data, headers)
    print("JSM Request Creation Result:")
    print(json.dumps(result, indent=2))
    
    # Example comment data
    comment_data = {
        "body": "This is a comment from migration",
        "private": False,
        "author": "john@example.com",
        "attachments": []
    }
    
    query_params = [{"key": "issue_key", "value": "HELP-123"}]
    
    # Test comment creation
    comment_result = create_jsm_comment(query_params, comment_data, headers)
    print("\nJSM Comment Creation Result:")
    print(json.dumps(comment_result, indent=2))