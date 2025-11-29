"""
freshservicev2.py
=========================

Complete product-agnostic Freshservice connector that works with ANY source platform.
ALL source-specific logic is handled by UniversalAttachmentService.

Complete version with:
- Event loop management fixes
- Zendesk authentication handling
- Enhanced error handling and logging
"""

from urllib import response
import requests
import logging
import asyncio
import atexit
import threading
from typing import List, Dict, Tuple, Optional
from datetime import datetime, timezone, timedelta
import time
import concurrent.futures

# Import base classes (your existing imports)
from src.connector.base_target_connector import (
    BaseTargetConnector, 
    BaseInlineImageProcessor, 
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

class UniversalInlineProcessor(BaseInlineImageProcessor):
    """
    Product-agnostic inline image processor.
    ALL source-specific logic is delegated to UniversalAttachmentService.
    """
    
    def __init__(self, attachment_service: UniversalAttachmentService):
        """Initialize with attachment service"""
        self.attachment_service = attachment_service
    
    def get_source_patterns(self) -> List[Dict]:
        """
        This method is no longer used - all patterns are in AttachmentService.
        Kept for backward compatibility.
        """
        return []  # Empty - all logic moved to AttachmentService
    
    def extract_inline_images(self, description: str) -> Tuple[str, List[InlineImage]]:
        """
        This method is no longer used - all extraction is in AttachmentService.
        Kept for backward compatibility.
        """
        return description, []  # Empty - all logic moved to AttachmentService
    
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
    
    def _get_source_url(self, image: InlineImage, headers: Dict) -> Optional[str]:
        """Legacy method - no longer used"""
        return None
    
    def _fetch_attachment_content(self, url: str, headers: Dict) -> Optional[bytes]:
        """Legacy method - no longer used"""
        return None


# ===================================================================
# FRESHSERVICE RATE LIMIT HANDLER
# ===================================================================

class FreshserviceRateLimitHandler(BaseRateLimitHandler):
    """Freshservice-specific rate limit handler"""
    
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
# MAIN UNIVERSAL FRESHSERVICE CONNECTOR
# ===================================================================

class UniversalFreshserviceConnector(BaseTargetConnector):
    """
    Completely product-agnostic Freshservice connector.
    Works with ANY source platform without modification.
    """
    
    def __init__(self, config: ConnectorConfig, attachment_service: Optional[UniversalAttachmentService] = None):
        """
        Initialize connector
        
        Args:
            config: Connector configuration
            attachment_service: Shared UniversalAttachmentService instance
        """
        super().__init__(config)
        self.attachment_service = attachment_service
        self._external_attachment_service = attachment_service is not None
        
        # Always create inline processor (it can handle None attachment_service)
        self.inline_processor = UniversalInlineProcessor(self.attachment_service)
    
    @property
    def safe_attachment_service(self) -> Optional[UniversalAttachmentService]:
        """Safe property to get attachment service, returns None if not set"""
        return getattr(self, 'attachment_service', None)
    
    def _get_inline_processor(self) -> BaseInlineImageProcessor:
        """Get universal inline processor"""
        # Always return a processor, even if attachment_service is None
        return UniversalInlineProcessor(self.safe_attachment_service)
    
    def _get_rate_limiter(self) -> BaseRateLimitHandler:
        return FreshserviceRateLimitHandler()
    
    def _validate_instance(self) -> bool:
        """Validate Freshservice instance/credentials"""
        try:
            url = f"https://{self.config.domainUrl}.freshservice.com/api/v2/tickets"
            response = requests.get(url, auth=(self.config.api_key, 'X'), timeout=10)
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
    
    def _extract_source_auth_from_context(self, headers: Dict, body: Dict) -> Optional[Dict]:
        """
        Extract source authentication from various places in the request context.
        Sometimes auth info is embedded in the migration context rather than headers.
        """
        auth_config = {}
        
        # First, try standard header extraction
        auth_from_headers = self._build_universal_auth_config(headers)
        if auth_from_headers:
            return auth_from_headers
        
        # Check if this is a Zendesk migration based on attachment URLs
        attachments = body.get('attachments', [])
        if attachments:
            first_attachment = attachments[0] if isinstance(attachments, list) else attachments
            attachment_url = (first_attachment.get('url') or 
                             first_attachment.get('content_url') or 
                             first_attachment.get('attachment_url') or '')
            
            if 'zendesk.com' in attachment_url:
                logging.info("Detected Zendesk attachments, checking for auth in headers")
                # For Zendesk, check headers again more thoroughly
                headers_dict = convert_headers_to_dict(headers) if isinstance(headers, list) else headers
                
                # Check for Zendesk auth in different locations
                username = headers_dict.get('username', '')
                password = headers_dict.get('password', '')
                
                if username and password:
                    auth_config = self._build_universal_auth_config(headers)
                    if auth_config:
                        return auth_config
                
                # If still no auth, mark as Zendesk for proper handling
                auth_config['source_type'] = 'zendesk'
                
                # Extract domainUrl from URL if possible
                import re
                domain_match = re.search(r'https://([^.]+)\.zendesk\.com', attachment_url)
                if domain_match:
                    auth_config['source_domain'] = domain_match.group(1)
                    
        return auth_config if auth_config else None
    
    async def _process_attachments_universal(self, attachments: List[Dict], 
                                           auth_config: Optional[Dict] = None) -> Tuple[List[Dict], List[str]]:
        """
        Universal attachment processing using AttachmentService.
        Enhanced with diagnostic logging plus minimal fallback retry.
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
                if url and 'zendesk.com' in url:
                    logging.info(f"  Zendesk attachment detected")
                    if '/api/v2/attachments/' in url:
                        logging.info(f"  API URL detected - will fetch metadata first")
            
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
            successful_files: List[Dict] = []
            failed_files: List[str] = []
            
            for i, result in enumerate(results):
                att = attachments[i]
                att_name = att.get('name', att.get('file_name', 'unknown'))
                att_url = att.get('url') or att.get('content_url') or att.get('attachment_url')
                
                if result:
                    logging.info(f"Successfully downloaded: {att_name} ({len(result.content)} bytes)")
                    successful_files.append({
                        'name': result.name,
                        'content': result.content,
                        'content_type': result.content_type
                    })
                else:
                    # Minimal fallback: try direct requests GET once (no over-engineering)
                    fallback_done = False
                    if att_url and att_url.startswith('http'):  # simple guard
                        try:
                            logging.info(f"Fallback HTTP fetch for failed attachment: {att_name}")
                            import requests as _rq
                            _headers = {}
                            # Reuse very basic auth if present in auth_config
                            if auth_config:
                                if auth_config.get('api_key') and 'Authorization' not in _headers:
                                    _headers['Authorization'] = f"Basic {auth_config['api_key']}"
                                elif auth_config.get('bearer_token'):
                                    _headers['Authorization'] = f"Bearer {auth_config['bearer_token']}"
                                elif auth_config.get('access_token'):
                                    _headers['Authorization'] = f"Bearer {auth_config['access_token']}"
                                elif auth_config.get('username') and auth_config.get('password'):
                                    import base64 as _b64
                                    cred = _b64.b64encode(f"{auth_config['username']}:{auth_config['password']}".encode()).decode()
                                    _headers['Authorization'] = f"Basic {cred}"
                            _resp = _rq.get(att_url, headers=_headers, timeout=20, stream=True)
                            if _resp.status_code == 200 and _resp.content:
                                ct = att.get('content_type') or _resp.headers.get('Content-Type', 'application/octet-stream')
                                successful_files.append({
                                    'name': att_name,
                                    'content': _resp.content,
                                    'content_type': ct
                                })
                                logging.info(f"Fallback succeeded for {att_name} ({len(_resp.content)} bytes)")
                                fallback_done = True
                            else:
                                logging.warning(f"Fallback failed for {att_name}: HTTP {_resp.status_code}")
                        except Exception as fe:  # pragma: no cover
                            logging.warning(f"Fallback error for {att_name}: {fe}")
                    
                    if not fallback_done:
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
    
    def _create_ticket_api_call(self, ticket_data: Dict, files: List[Dict], headers: Dict) -> requests.Response:
        """Create Freshservice ticket with proper custom_fields handling"""
        url = f"https://{self.config.domainUrl}.freshservice.com/api/v2/tickets"
        auth = (self.config.api_key, 'X')
        headersData = {
            'X-FW-Partner-Migration': 'd76f19b905897b629b89f83e4c6ac3d8e531091e8b994579c7b1addee2c2e693'
        }

        if ticket_data.get('description') and isinstance(ticket_data['description'], str):
            # Freshservice TEXT fields have an upper bound (approx 65,535 bytes). We'll target a safe byte cap.
            description = ticket_data['description']
            suffix = "\n\n[Description truncated due to length limit]"
            suffix_bytes = len(suffix.encode('utf-8'))
            # Allow some headroom (similar strategy used for conversations) to avoid edge overflows
            max_bytes = 50000  # safe threshold below 65535
            description_bytes = description.encode('utf-8')

            if len(description_bytes) > max_bytes:
                available_bytes = max_bytes - suffix_bytes
                if available_bytes <= 0:
                    # Fallback: extreme edge case, just keep a minimal notice
                    ticket_data['description'] = suffix.strip()
                else:
                    truncated_bytes = description_bytes[:available_bytes]
                    # Ensure we don't cut a multi-byte UTF-8 sequence
                    try:
                        truncated_description = truncated_bytes.decode('utf-8')
                    except UnicodeDecodeError:
                        # Back off up to 3 bytes to find a valid boundary
                        for i in range(1, 4):
                            try:
                                truncated_description = truncated_bytes[:-i].decode('utf-8')
                                break
                            except UnicodeDecodeError:
                                continue
                        else:  # As a last resort fall back to character slicing
                            truncated_description = description[:available_bytes // 2]
                    ticket_data['description'] = truncated_description + suffix
                logging.warning(
                    f"Description truncated from {len(description_bytes)} to {len(ticket_data['description'].encode('utf-8'))} bytes"
                )

        if files:
            form_files = [('attachments[]', (f['name'], f['content'], f['content_type'])) for f in files]
            
            form_data = []
            for key, value in ticket_data.items():
                if key == 'custom_fields' and isinstance(value, dict):
                    # Handle custom_fields as individual form fields
                    for cf_key, cf_value in value.items():
                        if cf_value is not None and cf_value != '':
                            form_data.append((f'custom_fields[{cf_key}]', str(cf_value)))
                            logging.info(f"Added custom field: custom_fields[{cf_key}] = {cf_value}")
                elif isinstance(value, list):
                    for item in value:
                        form_data.append((f"{key}[]", str(item)))
                elif isinstance(value, dict):
                    # For other dictionaries, JSON-encode them
                    import json
                    form_data.append((key, json.dumps(value)))
                elif value is not None:
                    form_data.append((key, str(value)))
            
            logging.info(f"Form data prepared with {len(form_data)} fields")
            logging.info("Form data preview:")
            for k, v in form_data:
                if isinstance(v, str) and len(v) > 100:
                    logging.info(f"  {k}: {v[:100]}...")
                else:
                    logging.info(f"  {k}: {v}")
            
                                 
            return self.rate_limiter.make_request_with_retry(
                url, 'POST', auth=auth, headers=headersData, form_data=form_data, files=form_files
            )
        else:
            for key, value in ticket_data.items():
                if key == 'custom_fields' and isinstance(value, dict):
                    # Remove keys with None or empty string values
                    for cf_key in list(value.keys()):
                        cf_value = value[cf_key]
                        if cf_value is None or cf_value == '':
                            del value[cf_key]
                            logging.info(f"Removed custom field: custom_fields[{cf_key}] (was None or empty)")
            return self.rate_limiter.make_request_with_retry(
                url, 'POST', auth=auth, headers=headersData, json_data=ticket_data
            )

    # def _create_conversation_api_call(self, ticket_id: str, conversation_data: Dict, 
    #                                 files: List[Dict], headers: Dict) -> requests.Response:
    #     """Create Freshservice conversation with proper form data handling"""
    #     url = f"https://{self.config.domainUrl}.freshservice.com/api/v2/tickets/{ticket_id}/notes"
    #     auth = (self.config.api_key, 'X')
    #     headersData = {
    #         'X-FW-Partner-Migration': 'd76f19b905897b629b89f83e4c6ac3d8e531091e8b994579c7b1addee2c2e693'
    #     }
        
    #     if files:
    #         form_files = [('attachments[]', (f['name'], f['content'], f['content_type'])) for f in files]
            
    #         form_data = []
    #         for key, value in conversation_data.items():
    #             if key == 'custom_fields' and isinstance(value, dict):
    #                 # Handle custom_fields as individual form fields (if conversations support them)
    #                 for cf_key, cf_value in value.items():
    #                     if cf_value is not None:
    #                         form_data.append((f'custom_fields[{cf_key}]', str(cf_value)))
    #             elif isinstance(value, list):
    #                 for item in value:
    #                     form_data.append((f"{key}[]", str(item)))
    #             elif isinstance(value, dict):
    #                 # For other dictionaries, JSON-encode them
    #                 import json
    #                 form_data.append((key, json.dumps(value)))
    #             elif key == 'private':
    #                 # Handle boolean fields properly
    #                 form_data.append((key, 'true' if str(value).lower() in ['true', '1', 'yes'] else 'false'))
    #             elif value is not None:
    #                 form_data.append((key, str(value)))
            
    #         return self.rate_limiter.make_request_with_retry(
    #             url, 'POST', auth=auth, headers=headersData, form_data=form_data, files=form_files
    #         )
    #     else:
    #         # Handle the private field for JSON requests
    #         if 'private' in conversation_data:
    #             conversation_copy = conversation_data.copy()
    #             conversation_copy['private'] = str(conversation_data['private']).lower() in ['true', '1', 'yes']
    #             return self.rate_limiter.make_request_with_retry(
    #                 url, 'POST', auth=auth, headers=headersData, json_data=conversation_copy
    #             )
    #         else:
    #             return self.rate_limiter.make_request_with_retry(
    #                 url, 'POST', auth=auth, headers=headersData, json_data=conversation_data
    #             )
    
    def _create_conversation_api_call(self, ticket_id: str, conversation_data: Dict, 
                                files: List[Dict], headers: Dict) -> requests.Response:
        """Create Freshservice conversation with proper form data handling"""
        url = f"https://{self.config.domainUrl}.freshservice.com/api/v2/tickets/{ticket_id}/notes"
        auth = (self.config.api_key, 'X')
        headersData = {
            'X-FW-Partner-Migration': 'd76f19b905897b629b89f83e4c6ac3d8e531091e8b994579c7b1addee2c2e693'
        }

        if conversation_data.get('user_id') and isinstance(conversation_data['user_id'], str):
            conversation_data.pop('user_id')  # Remove user_id if it's a string
            logging.warning("Removed string user_id from conversation data")

        if conversation_data.get('created_at') and conversation_data.get('updated_at'):
            if conversation_data['created_at'] == conversation_data['updated_at']:
                # Both are the same, set only created_at and update updated_at to 5 seconds later
                created_dt = datetime.fromisoformat(conversation_data['created_at'].replace('Z', '+00:00'))
                updated_dt = created_dt + timedelta(seconds=5)
                conversation_data['updated_at'] = updated_dt.isoformat().replace('+00:00', 'Z')

        if files:
            form_files = [('attachments[]', (f['name'], f['content'], f['content_type'])) for f in files]
            
            form_data = []
            for key, value in conversation_data.items():
                if isinstance(value, list):
                    for item in value:
                        form_data.append((f"{key}[]", str(item)))
                elif isinstance(value, dict):
                    # JSON-encode dictionaries
                    import json
                    form_data.append((key, json.dumps(value)))
                elif key == 'private':
                    # Handle boolean fields properly
                    form_data.append((key, 'true' if str(value).lower() in ['true', '1', 'yes'] else 'false'))
                elif value is not None:
                    # Convert other values to string
                    form_data.append((key, str(value)))
                # Skip None values
            
            logging.info(f"Form data for conversation with attachments: {[(k, v[:100] if isinstance(v, str) and len(v) > 100 else v) for k, v in form_data]}")
            
            response = self.rate_limiter.make_request_with_retry(
                url, 'POST', auth=auth, headers=headersData, form_data=form_data, files=form_files
            )

        else:
            response = self.rate_limiter.make_request_with_retry(
                url, 'POST', auth=auth, headers=headersData, json_data=conversation_data
            )

        if response is not None and getattr(response, "status_code", None) == 400:
            data = response.json()
            errors = data.get('errors', [])
            now_iso = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            for err in errors:
                if ('Has to be greater than ticket creation time' in str(err.get('message', '')) and err.get('field') in ['created_at', 'updated_at']):
                    if err['field'] == 'created_at':
                        conversation_data['created_at'] = now_iso
                    elif err['field'] == 'updated_at':
                        conversation_data['updated_at'] = now_iso

                    response = self.rate_limiter.make_request_with_retry(url, 'POST', auth=auth, headers=headersData, json_data=conversation_data)

        return response
    
    def _update_ticket_type_api_call(self, ticket_id: str, update_data: Dict, headers: Dict) -> requests.Response:
        """Update Freshservice ticket type using PUT request"""
        url = f"https://{self.config.domainUrl}.freshservice.com/api/v2/tickets/{ticket_id}"
        auth = (self.config.api_key, 'X')
        
        return self.rate_limiter.make_request_with_retry(
            url, 'PUT', auth=auth, json_data=update_data
        )

    # Dom - 9/10 - New method to create child service requests
    def _create_child_service_request_api_call(self, item_id: str, request_data: Dict, headers: Dict) -> requests.Response:
        """Create Child ServiceRequest using Freshservice Service Catalog API"""
        url = f"https://{self.config.domainUrl}.freshservice.com/api/v2/service_catalog/items/{item_id}/place_request"
        auth = (self.config.api_key, 'X')

        # print("*****", request_data)
        
        return self.rate_limiter.make_request_with_retry(
            url, 'POST', auth=auth, json_data=request_data
        )

    async def create_ticket_universal(self, ticket_data: Dict, target_headers: Dict, source_headers: Optional[Dict] = None) -> ConnectorResponse:
        """
        Universal ticket creation method.
        Completely source-agnostic - ALL source logic is in attachment service.
        """
        try:
            # Make a copy to avoid modifying original
            ticket_copy = ticket_data.copy()

            # Remove keys with "", 0, None, or []
            for k in list(ticket_copy.keys()):
                v = ticket_copy[k]
                if v == "" or v == 0 or v is None or (isinstance(v, list) and len(v) == 0):
                    del ticket_copy[k]

            # --- Begin: Dynamic key renaming logic ---
            key_map = {
                "department": "department_id",
                "product": "product_id",
            }
            for old_key, new_key in key_map.items():
                if old_key in ticket_copy:
                    # Avoid overwriting if new_key already exists
                    if new_key not in ticket_copy:
                        ticket_copy[new_key] = ticket_copy.pop(old_key)
                    else:
                        ticket_copy.pop(old_key)
            # --- End: Dynamic key renaming logic ---
            
            # Extract attachments
            regular_attachments = ticket_copy.pop('attachments', [])
            
            # Let attachment service handle ALL normalization and auth extraction
            auth_config = None
            normalized_attachments = regular_attachments
            
            if self.safe_attachment_service and regular_attachments:
                # Use source headers for auth extraction, fall back to target headers
                headers_for_auth = source_headers if source_headers else target_headers        
                headers_dict = convert_headers_to_dict(headers_for_auth) if isinstance(headers_for_auth, list) else headers_for_auth
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
            
            logging.info(f"Creating ticket with {len(all_files)} attachments, {len(failed_files)} failed")
            
            # Create ticket
            response = self._create_ticket_api_call(ticket_copy, all_files, target_headers)
            
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
    
    async def update_ticket_type_universal(self, ticket_id: str, ticket_type: str, target_headers: Dict) -> ConnectorResponse:
        """
        Universal ticket type update method.
        Updates an existing ticket's type (e.g., to 'Service Request').
        """
        try:
            update_data = {"type": ticket_type}
            
            logging.info(f"Updating ticket {ticket_id} type to: {ticket_type}")
            
            # Update ticket type
            response = self._update_ticket_type_api_call(ticket_id, update_data, target_headers)
            
            # Enhanced error handling for specific scenarios
            if response.status_code == 404:
                error_msg = f"Ticket with ID {ticket_id} not found"
                logging.error(error_msg)
                return ConnectorResponse(
                    status_code=404,
                    success=False,
                    error_message=error_msg
                )
            elif response.status_code == 403:
                error_msg = f"Access denied when updating ticket {ticket_id}"
                logging.error(error_msg)
                return ConnectorResponse(
                    status_code=403,
                    success=False,
                    error_message=error_msg
                )
            elif response.status_code == 422:
                error_msg = f"Invalid ticket type '{ticket_type}' or validation error for ticket {ticket_id}"
                logging.error(error_msg)
                return ConnectorResponse(
                    status_code=422,
                    success=False,
                    error_message=error_msg
                )
            
            return ConnectorResponse(
                status_code=response.status_code,
                success=response.status_code in [200, 201],
                data=response.json() if response.status_code in [200, 201] and response.content else None,
                error_message=response.text if response.status_code not in [200, 201] else None
            )
            
        except requests.exceptions.Timeout:
            error_msg = f"Timeout while updating ticket {ticket_id} type"
            logging.error(error_msg)
            return ConnectorResponse(
                status_code=408,
                success=False,
                error_message=error_msg
            )
        except requests.exceptions.ConnectionError:
            error_msg = f"Connection error while updating ticket {ticket_id} type"
            logging.error(error_msg)
            return ConnectorResponse(
                status_code=503,
                success=False,
                error_message=error_msg
            )
        except Exception as e:
            logging.error(f"Error in update_ticket_type_universal: {e}")
            import traceback
            logging.error(f"Full traceback: {traceback.format_exc()}")
            return ConnectorResponse(
                status_code=500,
                success=False,
                error_message=str(e)
            )

    async def create_conversation_universal(self, query_params: List[Dict], conversation_data: Dict, 
                                      target_headers: Dict, source_headers: Optional[Dict] = None) -> ConnectorResponse:
        """
        Universal conversation creation method.
        Completely source-agnostic - ALL source logic is in attachment service.
        """
        try:
            # Extract ticket_id from query params
            ticket_id = None
            for param in query_params:
                if param.get('key') == 'ticket_id':
                    ticket_id = param.get('value')
                    break
            
            if not ticket_id:
                raise ValueError("ticket_id not found in query parameters")
            
            # Make a copy to avoid modifying original
            conv_copy = conversation_data.copy()
            
            # Extract attachments
            regular_attachments = conv_copy.pop('attachments', [])
            
            # Let attachment service handle ALL normalization and auth extraction
            auth_config = None
            normalized_attachments = regular_attachments
            
            if self.safe_attachment_service and regular_attachments:
                headers_dict = convert_headers_to_dict(target_headers) if isinstance(target_headers, list) else target_headers
                normalized_attachments, auth_config = self.safe_attachment_service.normalize_attachment_data(
                    regular_attachments, headers_dict
                )
                
                logging.info(f"Attachment service normalized {len(normalized_attachments)} attachments")
                if auth_config:
                    logging.info(f"Attachment service extracted auth for source: {auth_config.get('source_type', 'unknown')}")
            
            # Process inline images using AttachmentService
            body = conv_copy.get('body', '')
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
            
            logging.info(f"Creating conversation with {len(all_files)} attachments, {len(failed_files)} failed")

            if body and isinstance(body, str):
                # Apply same robust byte-safe truncation strategy used for ticket descriptions
                description = body
                suffix = "\n\n[Description truncated due to length limit]"
                suffix_bytes = len(suffix.encode('utf-8'))
                max_bytes = 50000  # keep conservative cap below 65,535
                description_bytes = description.encode('utf-8')

                if len(description_bytes) > max_bytes:
                    available_bytes = max_bytes - suffix_bytes
                    if available_bytes <= 0:
                        body = suffix.strip()
                    else:
                        truncated_bytes = description_bytes[:available_bytes]
                        try:
                            truncated_description = truncated_bytes.decode('utf-8')
                        except UnicodeDecodeError:
                            for i in range(1, 4):
                                try:
                                    truncated_description = truncated_bytes[:-i].decode('utf-8')
                                    break
                                except UnicodeDecodeError:
                                    continue
                            else:
                                truncated_description = description[:available_bytes // 2]
                        body = truncated_description + suffix
                    logging.warning(
                        f"Conversation body truncated from {len(description_bytes)} to {len(body.encode('utf-8'))} bytes"
                    )
                    conv_copy['body'] = body
                    logging.info(
                        f"Final conversation body length: {len(conv_copy.get('body', ''))} characters"
                    )

            # Create conversation
            response = self._create_conversation_api_call(ticket_id, conv_copy, all_files, target_headers)
            
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

    # Dom - 9/10 - New method to create child service requests
    async def create_child_service_request_universal(self, query_params: List[Dict], data: Dict, target_headers: Dict) -> ConnectorResponse:
        
        """
        Universal child service request creation method.
        Creates a child service request using the Freshservice Service Catalog API.
        
        Args:
            query_params: List of query parameters containing parent_ticket_id
            data: Dict containing email, quantity, item_id, and custom_fields
            target_headers: Headers for Freshservice target
            
        Returns:
            ConnectorResponse with the result
        """
        try:
            # Extract parent_ticket_id from query_params
            parent_ticket_id = None
            
            for param in query_params:
                if param.get('key') == 'ticket_id':
                    parent_ticket_id = param.get('value')
                    break
            
            # Validate required parameters
            if not parent_ticket_id:
                raise ValueError("parent_ticket_id is required in query parameters")
            
            # Extract required fields from data
            email = data.get('email')
            quantity = data.get('quantity', 1)
            item_id = data.get('item_id')
            custom_fields = data.get('custom_fields', {})
            
            if not email:
                raise ValueError("email is required in data")
            if not item_id:
                raise ValueError("item_id is required in data")
            
            logging.info(f"Creating child service request for item {item_id} with parent ticket {parent_ticket_id}")
            
            # Prepare request data according to the API format
            request_data = {
                "quantity": quantity,
                "parent_ticket_id": parent_ticket_id,
                "custom_fields": custom_fields,
                "email": email
            }
            
            # Create child service request
            response = self._create_child_service_request_api_call(item_id, request_data, target_headers)
            return ConnectorResponse(
                status_code=response.status_code,
                success=response.status_code in [200, 201],
                data=response.json() if response.status_code in [200, 201] and response.content else None,
                error_message=response.text if response.status_code not in [200, 201] else None
            )
            
        except Exception as e:
            logging.error(f"Error in create_child_service_request_universal: {e}")
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

# Dom - Commented this to prevent caching - Global attachment service for reuse across requests
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
    logging.info("No global attachment service to clean up")
    pass

# Register cleanup function
atexit.register(cleanup_attachment_service)

def to_iso8601_z(dt):
    """Convert any datetime to ISO 8601 format with 'Z' suffix (UTC)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')

# Dom - 8/28 Helper function to extract cookies from response
def get_servicenow_cookies(base_url, headers):
    """Make a test API call to get session cookies"""
    try:
        test_url = f"{base_url}/api/now/table/change_request?sysparm_limit=1"
        test_resp = requests.get(test_url, headers=headers, timeout=10)
        
        if test_resp.status_code == 200:
            # Get the Set-Cookie header (it's a single header with comma-separated cookies)
            set_cookie_header = test_resp.headers.get('Set-Cookie', '')
            
            if set_cookie_header:
                # Split by comma, but be careful with expiry dates that contain commas
                # We want to split on ", " that comes before a cookie name (before =)
                cookies = []
                
                # Split the Set-Cookie header and extract just the cookie name=value part
                cookie_parts = set_cookie_header.split(', ')
                
                for part in cookie_parts:
                    # Get everything before the first semicolon
                    cookie_value = part.split(';')[0].strip()
                    # Only add if it contains = (valid cookie format) and has a value after =
                    if '=' in cookie_value and cookie_value.split('=')[1]:
                        cookies.append(cookie_value)
                
                # Join with semicolon and space
                return '; '.join(cookies) if cookies else None
        return None
    except Exception as e:
        logging.warning(f"Failed to get ServiceNow cookies: {e}")
        return None

def create_freshservice_changes(body: Dict, **kwargs) -> Dict:
    """
    Updated function that properly handles attachments with multipart form data
    """
    try:
        headers = kwargs.get('targetHeaders', {})
        source_headers = kwargs.get('sourceHeaders', {})
        headers_dict = convert_headers_to_dict(headers)
        source_headers_dict = convert_headers_to_dict(source_headers) if source_headers else {}
        
        domain = headers_dict.get("domainUrl")
        api_key = headers_dict.get("apikey") or headers_dict.get("api_key")
        auth = (api_key, "X")

        if not domain or not api_key:
            raise ValueError("Missing domainUrl or API key in headers.")

        url = f"https://{domain}.freshservice.com/api/v2/changes"

        # Make a copy to avoid modifying the original
        change_data = body.copy()

        date_conversion_fields = ['planned_start_date', 'planned_end_date', 'actual_start_date', 'actual_end_date', 'created_at', 'updated_at', 'created_date', 'updated_date', 'closed_at']
        
        # Convert datetime fields to ISO 8601 strings if present
        for field in date_conversion_fields:
            if field in change_data and change_data[field]:
                try:
                    # If already a datetime object, convert directly
                    if isinstance(change_data[field], datetime):
                        change_data[field] = to_iso8601_z(change_data[field])
                    else:
                        # Try parsing string to datetime, then convert
                        dt = datetime.fromisoformat(str(change_data[field]).replace('Z', '+00:00'))
                        change_data[field] = to_iso8601_z(dt)
                except Exception as e:
                    logging.warning(f"Could not convert field {field} to ISO 8601: {e}")

        requester_id = FreshworksHelper(
                    headers_dict.get('domainUrl'),
                    headers_dict.get('apikey') or headers_dict.get('api_key'),
                    change_data['requester_email'],
                    "freshservice"
                ).find_or_create_requester()
        
        change_data['requester_id'] = int(requester_id)

        change_data.pop('requester_email', None)

        actual_status = change_data.get('status')

        change_data['status'] = 1  # Default to 'Open' status
        
        # Resolve requester email to ID
        # email = change_data.get('requester_email') or change_data.get('email') or change_data.get('requester_id')
        # if email:
        #     # Create helper instance to resolve email
        #     helper = FreshworksHelper(domain, api_key, email, "freshservice")
        #     requester_id = helper.get_requester_id_by_email(email)
        #     if requester_id:
        #         change_data['requester_id'] = requester_id
        #         logging.info(f"Resolved requester email {email} to ID {requester_id}")
        #     else:
        #         logging.warning(f"Could not resolve requester ID for email: {email}")
        # else:
        #     logging.warning("No requester email found in change data")
        # Separate attachments if present
        attachments = change_data.pop("attachments", [])
        
        # Handle planning_fields specially
        planning_fields = change_data.pop("planning_fields", {})

        # Handle attachments with proper multipart form data
        if attachments:
            logging.info(f"Processing {len(attachments)} attachments for multipart form submission")
            
            # Prepare form data
            form_data = []
            files = []
            
            # Add regular fields to form data
            for key, value in change_data.items():
                if key == 'custom_fields' and isinstance(value, dict):
                    # Handle custom_fields as individual form fields (same as tickets)
                    for cf_key, cf_value in value.items():
                        if cf_value is not None and cf_value != '':
                            form_data.append((f'custom_fields[{cf_key}]', str(cf_value)))
                elif isinstance(value, dict):
                    import json
                    form_data.append((key, json.dumps(value)))
                elif isinstance(value, list):
                    for item in value:
                        form_data.append((f"{key}[]", str(item)))
                elif value is not None:
                    form_data.append((key, str(value)))
            
            # Add planning_fields to form data if present
            if planning_fields:
                for section_name, section_data in planning_fields.items():
                    if isinstance(section_data, dict):
                        for field_key, field_value in section_data.items():
                            if field_value is not None:
                                form_data.append((f'planning_fields[{section_name}][{field_key}]', str(field_value)))
            
            # Process attachments 
            for attachment in attachments:
                file_name = attachment.get("name") or attachment.get("filename") or attachment.get("file_name")
                file_url = attachment.get("attachment_url") or attachment.get("url") or attachment.get("download_url")

                if not file_url or not file_name:
                    logging.warning(f"Skipping attachment with missing info: {attachment}")
                    continue

                try:
                    # Add ServiceNow auth if needed
                    # auth_headers = {}
                    # if 'service-now.com' in file_url and source_headers_dict:
                    #     username = source_headers_dict.get('username')
                    #     password = source_headers_dict.get('password')
                    #     if username and password:
                    #         import base64
                    #         credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
                    #         auth_headers['Authorization'] = f'Basic {credentials}'
                    #         logging.info(f"Added ServiceNow auth for attachment: {file_name}")
                    # file_resp = requests.get(file_url, headers=auth_headers, stream=True, timeout=30)
                    
                    # Use UniversalAttachmentService auth logic
                    auth_headers = {}
                    if 'service-now.com' in file_url and source_headers_dict:
                        
                        # Build auth config like the attachment service does
                        auth_config = {}
                        
                        # Check for OAuth first
                        oauth_token = source_headers_dict.get('oauth_token')
                        if oauth_token:
                            auth_headers['Authorization'] = f'Bearer {oauth_token}'
                            logging.info(f"Using ServiceNow OAuth for attachment: {file_name}")
                        else:
                            # Check for API key auth
                            username = source_headers_dict.get('username')
                            password = source_headers_dict.get('password')
                            
                            if username == 'x-sn-apikey' and password:
                                # Use API key authentication
                                auth_headers['x-sn-apikey'] = password
                                logging.info(f"Using ServiceNow API key for attachment: {file_name}")
                            elif username and password:
                                # Use basic auth
                                import base64
                                credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
                                auth_headers['Authorization'] = f'Basic {credentials}'
                                logging.info(f"Using ServiceNow basic auth for attachment: {file_name}")
                            else:
                                # Try pre-encoded API key
                                api_key = source_headers_dict.get('api_key')
                                if api_key:
                                    auth_headers['Authorization'] = f'Basic {api_key}'
                                    logging.info(f"Using ServiceNow pre-encoded API key for attachment: {file_name}")
                    
                    # Dom - 8/28 Extract base URL from file_url
                    from urllib.parse import urlparse
                    parsed_url = urlparse(file_url)
                    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"                        
                    # Dom - 8/28 Get session cookies first - pass auth_headers as parameter
                    cookie_string = get_servicenow_cookies(base_url, auth_headers)
                    if cookie_string:
                        auth_headers['Cookie'] = cookie_string
                        logging.info(f"Added ServiceNow session cookies for attachment: {file_name}")
                                      
                    file_resp = requests.get(file_url, headers=auth_headers, stream=True, timeout=30)

                    if file_resp.status_code == 200:
                        files.append(('attachments[]', (file_name, file_resp.content, attachment.get('content_type', 'application/octet-stream'))))
                        logging.info(f"Added attachment to multipart form: {file_name}")
                    else:
                        logging.warning(f"Failed to download attachment {file_name}: {file_resp.status_code}")
                except Exception as e:
                    logging.error(f"Error downloading attachment {file_name}: {e}")
            
            if files:
                # Send as multipart form data
                resp = requests.post(url, auth=auth, data=form_data, files=files)
                logging.info(f"Sent multipart form data with {len(files)} attachments")
            else:
                # No attachments: send as JSON
                if planning_fields:
                    change_data['planning_fields'] = planning_fields
                    logging.info(f"Added planning_fields to JSON payload: {list(planning_fields.keys())}")
                resp = requests.post(url, auth=auth, json=change_data)
                logging.info("Sent JSON data (no attachments)")
        else:
            # No attachments: send as JSON
            if planning_fields:
                change_data['planning_fields'] = planning_fields
                logging.info(f"Added planning_fields to JSON payload: {list(planning_fields.keys())}")
            resp = requests.post(url, auth=auth, json=change_data)
            logging.info("Sent JSON data (no attachments)")

        # Standardize response
        if resp.status_code in (200, 201):
            if actual_status and actual_status != 1:
                created_resp_json = resp.json()
                change_resp = created_resp_json.get('change')
                update_url = f"https://{domain}.freshservice.com/api/v2/changes/{change_resp.get('id')}"
                update_payload = {
                    "status": actual_status
                }
                update_resp = requests.put(update_url, auth=auth, json=update_payload)

                if update_resp.status_code not in (200, 201):
                    logging.error(f"Failed to update change status: {update_resp.status_code} - {update_resp.text}")

            return {
                "status_code": resp.status_code,
                "body": resp.json(),
            }
        else:
            logging.error(f"Failed to create change: {resp.status_code} - {resp.text}")
            return {
                "status_code": resp.status_code,
                "body": resp.text,
            }

    except Exception as e:
        logging.error(f"Error in create_freshservice_changes: {e}", exc_info=True)
        import traceback
        return {
            "status_code": 500,
            "body": str(e),
            "trace": traceback.format_exc(),
        }


def create_freshservice_change_notes(body=None, headers=None, queryParams=None, **kwargs):
    """
    Creates a note on a Freshservice change using POST /changes/{id}/notes
    
    Args:
        body: Dict with note data (body, user_id, etc.)
        headers: Dict with 'domainUrl' and 'apikey'
        queryParams: List with change_id parameter
        **kwargs: Additional arguments
    
    Returns:
        Dict with status_code and response body
    """
    try:
        # Handle headers (similar to your changes function)
        headers = kwargs.get('targetHeaders', {})
        headers_dict = convert_headers_to_dict(headers)
        
        # # Handle user_id resolution
        # user_id = body.get('user_id', '')
        # if user_id:
        #     resolved_user_id = FreshworksHelper(
        #         headers_dict.get('domainUrl'),
        #         headers_dict.get('apikey') or headers_dict.get('api_key'),
        #         user_id,
        #         "freshservice"
        #     ).find_or_create_requester()
            
        #     if resolved_user_id:
        #         body['user_id'] = int(resolved_user_id)
        # else:
        #     body.pop('user_id', None)

        # Get domain and API key
        domain = headers_dict.get("domainUrl") or headers_dict.get("domain")
        api_key = headers_dict.get("apikey") or headers_dict.get("api_key")
        
        if not domain or not api_key:
            raise ValueError(f"Missing domainUrl or API key. Headers: {headers_dict}")
        
        # Extract change_id from queryParams
        change_id = None
        if queryParams:
            for param in queryParams:
                if param.get('key') == 'change_id':
                    change_id = param.get('value')
                    break
        
        if not change_id:
            raise ValueError("change_id is required in queryParams")
        

        auth = (api_key, "X")
        url = f"https://{domain}.freshservice.com/api/v2/changes/{change_id}/notes"
        
        # # Handle body
        # if body is None:
        #     request_body = body 
        # else:
        #     request_body = {}

        resp = requests.post(url, auth=auth, json=body, timeout=60)
        
        if resp.status_code in (200, 201):
            return {
                "status_code": resp.status_code,
                "body": resp.json(),
            }
        else:
            logging.error(f"Failed to create change note: {resp.text}")
            return {
                "status_code": resp.status_code,
                "body": resp.text,
            }
            
    except Exception as e:
        logging.error(f"Error in create_freshservice_change_notes: {e}", exc_info=True)
        import traceback
        return {
            "status_code": 500,
            "body": str(e),
            "trace": traceback.format_exc(),
        }

def create_freshservice_change_tasks(queryParams: List[Dict], body: Dict, headers=None, **kwargs) -> Dict:
    """
    Synchronously creates a Freshservice task under a ticket using POST /tickets/{id}/tasks.

    Args:
        queryParams: List of dicts (must contain 'ticket_id')
        body: Dictionary with task fields (title, due_date, etc.)
        headers: Dict with 'domainUrl' and 'apikey' for Freshservice target instance

    Returns:
        Dict with status_code and response body
    """
    try:
        headers = kwargs.get('targetHeaders', {})
        headers_dict = convert_headers_to_dict(headers)
        domain = headers_dict.get("domainUrl")
        api_key = headers_dict.get("apikey") or headers_dict.get("api_key")
        auth = (api_key, "X")

        # Extract ticket_id from queryParams
        query_dict =  {param["key"]: param["value"] for param in queryParams if "key" in param and "value" in param}
        change_id = query_dict.get("change_id")

        if not domain or not api_key:
            raise ValueError("Missing domainUrl, API key, or change_id.")
        
                # Extract change_id from queryParams
        change_id = None
        if queryParams:
            for param in queryParams:
                if param.get('key') == 'change_id':
                    change_id = param.get('value')
                    break
        
        if not change_id:
            raise ValueError("change_id is required in queryParams")

        url = f"https://{domain}.freshservice.com/api/v2/changes/{change_id}/tasks"

        # Direct POST with JSON
        resp = requests.post(url, auth=auth, json=body)

        if resp.status_code in (200, 201):
            return {
                "status_code": resp.status_code,
                "body": resp.json(),
            }
        else:
            logging.error(f"Failed to create task for ticket {change_id}: {resp.text}")
            return {
                "status_code": resp.status_code,
                "body": resp.text,
            }

    except Exception as e:
        logging.error(f"Error in create_freshservice_change_tasks: {e}", exc_info=True)
        import traceback
        return {
            "status_code": 500,
            "body": str(e),
            "trace": traceback.format_exc(),
        }

def create_freshservice_ticket_tasks(queryParams: List[Dict], body: Dict, headers=None, **kwargs) -> Dict:
    """
    Synchronously creates a Freshservice task under a ticket using POST /tickets/{id}/tasks.

    Args:
        queryParams: List of dicts (must contain 'ticket_id')
        body: Dictionary with task fields (title, due_date, etc.)
        headers: Dict with 'domainUrl' and 'apikey' for Freshservice target instance

    Returns:
        Dict with status_code and response body
    """
    try:
        headers = kwargs.get('targetHeaders', {})
        headers_dict = convert_headers_to_dict(headers)
        domain = headers_dict.get("domainUrl")
        api_key = headers_dict.get("apikey") or headers_dict.get("api_key")
        auth = (api_key, "X")

        # Extract ticket_id from queryParams
        query_dict =  {param["key"]: param["value"] for param in queryParams if "key" in param and "value" in param}
        ticket_id = query_dict.get("ticket_id")

        if not domain or not api_key or not ticket_id:
            raise ValueError("Missing domainUrl, API key, or ticket_id.")
        
                # Extract ticket_id from queryParams
        ticket_id = None
        if queryParams:
            for param in queryParams:
                if param.get('key') == 'ticket_id':
                    ticket_id = param.get('value')
                    break
        
        if not ticket_id:
            raise ValueError("ticket_id is required in queryParams")

        url = f"https://{domain}.freshservice.com/api/v2/tickets/{ticket_id}/tasks"

        # Direct POST with JSON
        resp = requests.post(url, auth=auth, json=body)

        if resp.status_code in (200, 201):
            return {
                "status_code": resp.status_code,
                "body": resp.json(),
            }
        else:
            logging.error(f"Failed to create incident task for ticket {ticket_id}: {resp.text}")
            return {
                "status_code": resp.status_code,
                "body": resp.text,
            }

    except Exception as e:
        logging.error(f"Error in create_freshservice_ticket_tasks: {e}", exc_info=True)
        import traceback
        return {
            "status_code": 500,
            "body": str(e),
            "trace": traceback.format_exc(),
        }

# Dom - Added new service request method
def create_freshservice_service_requests(**kwargs) -> Dict:
    """
    Alias for create_freshservice_service_request (handles both singular and plural)
    """
    return create_freshservice_service_request_universal(**kwargs)

# Dom - Added new service request item method
def create_freshservice_child_service_requests(**kwargs) -> Dict:
    """
    Backward compatible child service request creation function.
    
    Args:
        **kwargs: Should contain 'queryParams' and 'targetHeaders'
        
    Returns:
        Dict with status_code and response body
    """
   
    return create_freshservice_child_service_request_universal(query_params=kwargs['queryParams'], headers=kwargs['targetHeaders'], data=kwargs['body'])

def create_freshservice_child_service_request_universal(query_params: List[Dict], data: Dict, headers) -> Dict:
    """
    Universal Freshservice child service request creation.
    Creates a child service request using the Service Catalog API.
    
    Args:
        query_params: List of query parameters containing parent_ticket_id
        data: Dict containing email, quantity, item_id, and custom_fields
        headers: Headers with domainUrl and API key
        
    Returns:
        Dict with status_code and response body
    """
    try:
        # Try to get existing event loop
        try:
            loop = asyncio.get_running_loop()
            # We're in an async context, use thread pool
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(
                    lambda: asyncio.run(create_freshservice_child_service_request_async(query_params=query_params, data=data, target_headers=headers))
                )
                return future.result()
        except RuntimeError:
            # No event loop running, use thread-local loop
            loop = get_thread_loop()
            return loop.run_until_complete(create_freshservice_child_service_request_async(query_params=query_params, data=data, target_headers=headers))
    except Exception as e:
        logging.error(f"Error in create_freshservice_child_service_request_universal: {e}")
        import traceback
        logging.error(f"Traceback: {traceback.format_exc()}")
        return {
            "status_code": 500,
            "body": str(e)
        }
    


# ===================================================================
# ASYNC FUNCTIONS FOR FASTAPI
# ===================================================================

async def create_freshservice_ticket_async(body: Dict, target_headers, source_headers: Optional[Dict] = None) -> Dict:
    """
    Async version of Freshservice ticket creation.
    This should be called from FastAPI endpoints.
    
    Args:
        body: Ticket data with attachments from ANY source
        target_headers: Headers for Freshservice target
        source_headers: Headers for source system authentication
    Returns:
        Standardized response format
    """
    attachment_service = None
    try:
        target_headers_dict = convert_headers_to_dict(target_headers)
        source_headers_dict = convert_headers_to_dict(source_headers) if source_headers else {}
        
        # Get shared universal attachment service
        attachment_service = await get_universal_attachment_service()
        
        if not attachment_service:
            logging.warning("Could not initialize AttachmentService - proceeding without attachment processing")
        
        # Create connector with shared attachment service
        config = ConnectorConfig(
            domainUrl=target_headers_dict.get('domainUrl'),
            api_key=target_headers_dict.get('apikey') or target_headers_dict.get('api_key')
        )
        connector = UniversalFreshserviceConnector(config, attachment_service)
        
        # Create ticket using universal method - pass source headers
        response = await connector.create_ticket_universal(body, target_headers, source_headers_dict)
        return standardize_response_format(response, target_headers)
        
    except Exception as e:
        logging.error(f"Error in create_freshservice_ticket_async: {e}")
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

async def create_freshservice_conversation_async(queryParams: List[Dict], body: Dict, headers) -> Dict:
    """
    Async version of Freshservice conversation creation.
    This should be called from FastAPI endpoints.
    
    Args:
        queryParams: List of query parameters (including ticket_id)
        body: Conversation data with attachments from ANY source
        headers: Headers including source authentication info
        
    Returns:
        Standardized response format
    """
    attachment_service = None
    try:
        headers_dict = convert_headers_to_dict(headers)
        
        # Handle user_id resolution
        user_id = body.get('user_id', '')
        if user_id:
            # Run sync code in thread pool to avoid blocking
            loop = asyncio.get_running_loop()
            resolved_user_id = await loop.run_in_executor(
                None,
                lambda: FreshworksHelper(
                    headers_dict.get('domainUrl'),
                    headers_dict.get('apikey') or headers_dict.get('api_key'),
                    user_id,
                    "freshservice"
                ).find_or_create_requester()
            )
            
            if resolved_user_id:
                body_copy = body.copy()
                body_copy['user_id'] = int(resolved_user_id)
            else:
                body_copy = body.copy()
                body_copy.pop('user_id', None)
        else:
            body_copy = body.copy()
            body_copy.pop('user_id', None)
            
        
        # Get shared universal attachment service
        attachment_service = await get_universal_attachment_service()
        
        if not attachment_service:
            logging.warning("Could not initialize AttachmentService - proceeding without attachment processing")
        
        # Create connector with shared attachment service
        config = ConnectorConfig(
            domainUrl=headers_dict.get('domainUrl'),
            api_key=headers_dict.get('apikey') or headers_dict.get('api_key')
        )
        connector = UniversalFreshserviceConnector(config, attachment_service)
        
        # Create conversation using universal method
        response = await connector.create_conversation_universal(queryParams, body_copy, headers)
        final_resp = standardize_response_format(response, headers)
        return final_resp
        
    except Exception as e:
        logging.error(f"Error in create_freshservice_conversation_async: {e}")
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

async def update_service_request_type_async(ticket_id: str, target_headers) -> Dict:
    """
    Async version of service request type update.
    
    Args:
        ticket_id: ID of the ticket to update
        target_headers: Headers for Freshservice target
        
    Returns:
        Standardized response format
    """
    try:
        target_headers_dict = convert_headers_to_dict(target_headers)
        
        # Create connector
        config = ConnectorConfig(
            domainUrl=target_headers_dict.get('domainUrl'),
            api_key=target_headers_dict.get('apikey') or target_headers_dict.get('api_key')
        )
        connector = UniversalFreshserviceConnector(config)
        
        # Update ticket type
        response = await connector.update_ticket_type_universal(ticket_id, "Service Request", target_headers)
        return standardize_response_format(response, target_headers)
        
    except Exception as e:
        logging.error(f"Error in update_service_request_type_async: {e}")
        import traceback
        logging.error(f"Full traceback: {traceback.format_exc()}")
        return {
            "status_code": 500,
            "body": str(e),
        }

async def create_freshservice_child_service_request_async(query_params, data, target_headers) -> Dict:
    """
    Async version of Freshservice child service request creation.
    This should be called from FastAPI endpoints.
    
    Args:
        query_params: List of query parameters containing parent_ticket_id
        data: Dict containing email, quantity, item_id, and custom_fields
        target_headers: Headers for Freshservice target
        
    Returns:
        Standardized response format
    """
    try:
        target_headers_dict = convert_headers_to_dict(target_headers)
        # print("DATA :", data)
        # Create connector
        config = ConnectorConfig(
            domainUrl=target_headers_dict.get('domainUrl'),
            api_key=target_headers_dict.get('apikey') or target_headers_dict.get('api_key')
        )
        connector = UniversalFreshserviceConnector(config)
        
        # Create child service request using universal method
        response = await connector.create_child_service_request_universal(query_params=query_params, data=data, target_headers=target_headers)
        
        child_req_resp = standardize_response_format(response, target_headers)
        return child_req_resp
        
    except Exception as e:
        logging.error(f"Error in create_freshservice_child_service_request_async: {e}")
        import traceback
        logging.error(f"Full traceback: {traceback.format_exc()}")
        return {
            "status_code": 500,
            "body": str(e)
        }

async def create_freshservice_changes_async(body: Dict, **kwargs) -> Dict:
    """
    Async version that leverages UniversalAttachmentService for proper authentication
    """
    attachment_service = None
    try:
        headers = kwargs.get('targetHeaders', {})
        source_headers = kwargs.get('sourceHeaders', {})
        headers_dict = convert_headers_to_dict(headers)
        source_headers_dict = convert_headers_to_dict(source_headers) if source_headers else {}
        
        domain = headers_dict.get("domainUrl")
        api_key = headers_dict.get("apikey") or headers_dict.get("api_key")
        auth = (api_key, "X")

        if not domain or not api_key:
            raise ValueError("Missing domainUrl or API key in headers.")

        url = f"https://{domain}.freshservice.com/api/v2/changes"

        # Make a copy to avoid modifying the original
        change_data = body.copy()
        
        # Separate attachments if present
        attachments = change_data.pop("attachments", [])
        
        # Handle planning_fields specially
        planning_fields = change_data.pop("planning_fields", {})

        # Process attachments using UniversalAttachmentService if they exist
        if attachments:
            # Initialize attachment service
            attachment_service = await get_universal_attachment_service()
            
            if attachment_service:
                # Use attachment service to normalize and extract auth
                normalized_attachments, auth_config = attachment_service.normalize_attachment_data(
                    attachments, source_headers_dict
                )
                
                logging.info(f"Attachment service normalized {len(normalized_attachments)} attachments")
                if auth_config:
                    logging.info(f"Extracted auth for source: {auth_config.get('source_type', 'unknown')}")
                
                # Download attachments with proper authentication
                downloaded_attachments = await attachment_service.download_attachments_batch(
                    normalized_attachments, auth_config
                )
                
                # Prepare form data with attachments (multipart)
                form_data = []
                files = []
                
                # Add regular fields to form data
                for key, value in change_data.items():
                    if isinstance(value, dict):
                        import json
                        form_data.append((key, json.dumps(value)))
                    elif isinstance(value, list):
                        for item in value:
                            form_data.append((f"{key}[]", str(item)))
                    elif value is not None:
                        form_data.append((key, str(value)))
                
                # Add planning_fields to form data if present
                if planning_fields:
                    for section_name, section_data in planning_fields.items():
                        if isinstance(section_data, dict):
                            for field_key, field_value in section_data.items():
                                if field_value is not None:
                                    form_data.append((f'planning_fields[{section_name}][{field_key}]', str(field_value)))
                
                # Add downloaded attachments as files
                for processed_attachment in downloaded_attachments:
                    if processed_attachment:  # Skip failed downloads
                        files.append(('attachments[]', (
                            processed_attachment.name, 
                            processed_attachment.content, 
                            processed_attachment.content_type
                        )))
                        logging.info(f"Added attachment to form: {processed_attachment.name}")
                
                # Send as multipart form data
                resp = requests.post(url, auth=auth, data=form_data, files=files)
                logging.info(f"Sent multipart request with {len(files)} attachments")
            else:
                logging.error("Could not initialize UniversalAttachmentService")
                # Fall back to JSON without attachments
                if planning_fields:
                    change_data['planning_fields'] = planning_fields
                resp = requests.post(url, auth=auth, json=change_data)
        else:
            # No attachments: send as JSON
            if planning_fields:
                change_data['planning_fields'] = planning_fields
                logging.info(f"Added planning_fields to JSON payload: {list(planning_fields.keys())}")
            resp = requests.post(url, auth=auth, json=change_data)

        # Standardize response
        if resp.status_code in (200, 201):
            return {
                "status_code": resp.status_code,
                "body": resp.json(),
            }
        else:
            logging.error(f"Failed to create change: {resp.status_code} - {resp.text}")
            return {
                "status_code": resp.status_code,
                "body": resp.text,
            }

    except Exception as e:
        logging.error(f"Error in create_freshservice_changes_async: {e}", exc_info=True)
        import traceback
        return {
            "status_code": 500,
            "body": str(e),
            "trace": traceback.format_exc(),
        }
    finally:
        # Clean up the attachment service
        if attachment_service:
            try:
                await attachment_service.__aexit__(None, None, None)
            except Exception as e:
                logging.error(f"Error cleaning up attachment service: {e}")



# ===================================================================
# SYNCHRONOUS WRAPPER FUNCTIONS
# ===================================================================

def create_freshservice_ticket_universal(body: Dict, target_headers, source_headers: Optional[Dict] = None) -> Dict:
    """
    Universal Freshservice ticket creation.
    Works with attachments from ANY source platform.
    """
    try:
        # Try to get existing event loop
        try:
            loop = asyncio.get_running_loop()
            # We're in an async context, use thread pool
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(
                    lambda: asyncio.run(create_freshservice_ticket_async(body, target_headers, source_headers))
                )
                return future.result()
        except RuntimeError:
            # No event loop running, use thread-local loop
            loop = get_thread_loop()
            return loop.run_until_complete(create_freshservice_ticket_async(body, target_headers, source_headers))
            
    except Exception as e:
        logging.error(f"Error in create_freshservice_ticket_universal: {e}")
        import traceback
        logging.error(f"Traceback: {traceback.format_exc()}")
        return {
            "status_code": 500,
            "body": str(e),
        }

def create_freshservice_conversation_universal(queryParams: List[Dict], body: Dict, headers) -> Dict:
    """
    Universal Freshservice conversation creation.
    Works with attachments from ANY source platform.
    """
    try:
        # Try to get existing event loop
        try:
            loop = asyncio.get_running_loop()
            # We're in an async context, use thread pool
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(
                    lambda: asyncio.run(create_freshservice_conversation_async(queryParams, body, headers))
                )
                return future.result()
        except RuntimeError:
            # No event loop running, use thread-local loop
            loop = get_thread_loop()
            return loop.run_until_complete(create_freshservice_conversation_async(queryParams, body, headers))
            
    except Exception as e:
        logging.error(f"Error in create_freshservice_conversation_universal: {e}")
        import traceback
        logging.error(f"Traceback: {traceback.format_exc()}")
        return {
            "status_code": 500,
            "body": str(e),
        }

# Dom - New function to create service requests
def create_freshservice_service_request_universal(**kwargs) -> Dict:
    """
    Creates a Freshservice Service Request by:
    1. Creating a regular ticket
    2. Updating the ticket type to 'Service Request'
    
    Args:
        body: Dictionary with service request data (same as ticket data)
        **kwargs: Additional arguments including targetHeaders and sourceHeaders
        
    Returns:
        Dict with status_code and response body
    """
    try:
        target_headers = kwargs.get('targetHeaders', {})
        source_headers = kwargs.get('sourceHeaders', {})
        
        #body = kwargs.get('queryParams', {})
        
        # Dom - Changed to just body
        body = kwargs.get('body', {})
       

        # Step 1: Create the ticket first
        logging.info("Creating initial ticket for Service Request")
        ticket_result = create_freshservice_ticket_universal(body, target_headers, source_headers)
        
        if ticket_result.get('status_code') not in [200, 201]:
            logging.error(f"Failed to create initial ticket: {ticket_result}")
            return ticket_result
        
        # Step 2: Extract ticket ID from the response
        ticket_data = ticket_result.get('body', {})
        if isinstance(ticket_data, str):
            try:
                import json
                ticket_data = json.loads(ticket_data)
            except:
                logging.error("Could not parse ticket creation response")
                return {
                    "status_code": 500,
                    "body": "Could not parse ticket creation response"
                }
        print("*******",ticket_data)
        ticket_id = ticket_data['ticket'].get('id')
        if not ticket_id:
            logging.error(f"No ticket ID found in response: {ticket_data}")
            return {
                "status_code": 500,
                "body": "No ticket ID returned from ticket creation"
            }
        
        logging.info(f"Created ticket with ID: {ticket_id}, now updating to Service Request")
        
        # Step 3: Update ticket type to Service Request
        try:
            # Try to get existing event loop
            try:
                loop = asyncio.get_running_loop()
                # We're in an async context, use thread pool
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(
                        lambda: asyncio.run(update_service_request_type_async(ticket_id, target_headers))
                    )
                    update_result = future.result()
            except RuntimeError:
                # No event loop running, use thread-local loop
                loop = get_thread_loop()
                update_result = loop.run_until_complete(update_service_request_type_async(ticket_id, target_headers))
                
        except Exception as e:
            logging.error(f"Error updating ticket type: {e}")
            # Return the original ticket even if type update failed
            return ticket_result
        
        if update_result.get('status_code') not in [200, 201]:
            logging.warning(f"Failed to update ticket type but ticket was created: {update_result}")
            # Return original ticket data with warning
            result = ticket_result.copy()
            result['warning'] = f"Ticket created but type update failed: {update_result.get('body', 'Unknown error')}"
            return result
        
        # Step 4: Return the updated ticket data
        updated_ticket_data = update_result.get('body', {})
        if isinstance(updated_ticket_data, str):
            try:
                import json
                updated_ticket_data = json.loads(updated_ticket_data)
            except:
                updated_ticket_data = ticket_data
        
        logging.info(f"Successfully created Service Request with ID: {ticket_id}")
        
        return {
            "status_code": 200,
            "body": updated_ticket_data
        }
        
    except Exception as e:
        logging.error(f"Error in create_freshservice_service_request: {e}")
        import traceback
        logging.error(f"Full traceback: {traceback.format_exc()}")
        return {
            "status_code": 500,
            "body": str(e),
            "trace": traceback.format_exc(),
        }


# ===================================================================
# BACKWARD COMPATIBILITY FUNCTIONS
# ===================================================================

def create_freshservice_ticket(**kwargs) -> Dict:
    """
    Backward compatible ticket creation function that now uses universal processing.
    Works with ANY source platform automatically.
    
    IMPORTANT: If calling from FastAPI, use create_freshservice_ticket_async instead.
    """
    #return create_freshservice_ticket_universal(kwargs['body'], kwargs['targetHeaders'])

    # Dom - 8/18 CHANGE TO - pass both target and source headers
    source_headers = kwargs.get('sourceHeaders', {})
    target_headers = kwargs.get('targetHeaders', {})
    
    # Merge headers or pass them separately - see option below
    return create_freshservice_ticket_universal(kwargs['body'], target_headers, source_headers)

def create_freshservice_conversation(**kwargs) -> Dict:
    """
    Backward compatible conversation creation function that now uses universal processing.
    Works with ANY source platform automatically.
    
    IMPORTANT: If calling from FastAPI, use create_freshservice_conversation_async instead.
    """
    return create_freshservice_conversation_universal(kwargs['queryParams'], kwargs['body'], kwargs['targetHeaders'])

# def create_freshservice_conversations(queryParams: List[Dict], body: Dict, headers) -> Dict:
#     """
#     Alias for create_freshservice_conversation (handles both singular and plural)
#     """
#     return create_freshservice_conversation_universal(queryParams, body, headers)


# ===================================================================
# LEGACY COMPATIBILITY FUNCTIONS
# ===================================================================

def upload_ticket_regular_attachments(ticket_id: int, attachments: List[Dict], headers: Dict):
    """Legacy function for uploading regular attachments to existing tickets"""
    logging.warning("upload_ticket_regular_attachments is deprecated. Use create_freshservice_ticket with attachments instead.")
    # Implementation kept for backward compatibility but not recommended

def upload_note_regular_attachments(conv_id: int, attachments: List[Dict], headers: Dict):
    """Legacy function for uploading regular attachments to notes"""
    logging.warning("upload_note_regular_attachments is deprecated. Use create_freshservice_conversation with attachments instead.")
    # Implementation kept for backward compatibility but not recommended

# def create_freshservice_reply(queryParams: List[Dict], body: Dict, headers) -> Dict:
#     """
#     Create Freshservice reply (different endpoint from notes)
#     Uses the universal processing automatically.
#     """
#     # This can use the same universal logic as conversations
#     # Just need to change the endpoint in the connector
#     logging.info("create_freshservice_reply uses universal processing")
#     return create_freshservice_conversation_universal(queryParams, body, headers)


# ===================================================================
# VALIDATION FUNCTIONS
# ===================================================================

def validate_freshservice_instance_v1(headers) -> Dict:
    """Validate Freshservice instance and credentials"""
    try:
        headers_dict = convert_headers_to_dict(headers)
        config = ConnectorConfig(
            domainUrl=headers_dict.get('domainUrl'),
            api_key=headers_dict.get('apikey') or headers_dict.get('api_key')
        )
        connector = UniversalFreshserviceConnector(config)
        is_valid = connector._validate_instance()
        
        return {
            "status_code": 200 if is_valid else 401,
            "body": {"valid": is_valid},
            "headers": headers
        }
    except Exception as e:
        logging.error(f"Error validating Freshservice instance: {e}")
        return {
            "status_code": 500,
            "body": {"valid": False, "error": str(e)}
        }

def get_freshservice_mapping_objects_v1(headers) -> Dict:
    """Get Freshservice mapping objects (groups, users, departments, etc.)"""
    try:
        headers_dict = convert_headers_to_dict(headers)
        domainUrl = headers_dict['domainUrl']
        api_key = headers_dict.get('apikey') or headers_dict.get('api_key')
        auth = (api_key, 'X')
        headersData = {
            'X-FW-Partner-Migration': 'd76f19b905897b629b89f83e4c6ac3d8e531091e8b994579c7b1addee2c2e693'
        }

        config = ConnectorConfig(domainUrl=domainUrl, api_key=api_key)
        connector = UniversalFreshserviceConnector(config)
        
        # Get groups
        groups_url = f"https://{domainUrl}.freshservice.com/api/v2/groups"
        groups_response = connector.rate_limiter.make_request_with_retry(
            groups_url, 'GET', auth=auth, headers=headersData
        )
        
        # Get agents
        agents_url = f"https://{domainUrl}.freshservice.com/api/v2/agents"
        agents_response = connector.rate_limiter.make_request_with_retry(
            agents_url, 'GET', auth=auth, headers=headersData
        )
        
        # Get departments (Freshservice-specific)
        departments_url = f"https://{domainUrl}.freshservice.com/api/v2/departments"
        departments_response = connector.rate_limiter.make_request_with_retry(
            departments_url, 'GET', auth=auth, headers=headersData
        )
        
        mapping_data = {}
        
        if groups_response.status_code == 200:
            mapping_data['groups'] = groups_response.json()
        
        if agents_response.status_code == 200:
            mapping_data['agents'] = agents_response.json()
            
        if departments_response.status_code == 200:
            mapping_data['departments'] = departments_response.json()
        
        return {
            "status_code": 200,
            "body": mapping_data,
            "headers": headers
        }
        
    except Exception as e:
        logging.error(f"Error getting Freshservice mapping objects: {e}")
        return {
            "status_code": 500,
            "body": {"error": str(e)}
        }


# ===================================================================
# STATISTICS AND CLEANUP
# ===================================================================

def get_universal_attachment_statistics() -> Dict:
    """Get comprehensive attachment processing statistics"""
    global _universal_attachment_service
    if _universal_attachment_service:
        return _universal_attachment_service.get_statistics()
    return {
        'total_downloads': 0,
        'successful_downloads': 0,
        'failed_downloads': 0,
        'success_rate': 0,
        'total_bytes_downloaded': 0,
        'average_download_time': 0,
        'total_download_time': 0,
        'inline_images_processed': 0,
        'inline_images_extracted': 0
    }

async def cleanup_universal_attachment_service():
    """Cleanup global attachment service - no longer needed"""
    logging.info("No global attachment service to clean up")
    pass


# ===================================================================
# EXAMPLE USAGE
# ===================================================================

if __name__ == "__main__":
    # Example usage
    import json
    
    # Example Zendesk headers from your logs
    headers = [
        {"key": "domainUrl", "value": "saasgenie", "description": "", "req": False},
        {"key": "apikey", "value": "your-freshservice-api-key", "description": "", "req": False},
        {"key": "username", "value": "john@kodtod.com/token", "description": "", "req": True},
        {"key": "password", "value": "UR5AYT7lvxVesItOiVAqTlAUAs5IftCCzwuApJVf", "description": "", "req": True}
    ]
    
    # Example conversation data
    conversation_data = {
        "body": "<div class=\"zd-comment\" dir=\"auto\">Testing<br></div>",
        "private": True,
        "attachments": [{
            "url": "https://d3v-own153.zendesk.com/api/v2/attachments/23517201098130.json",
            "id": 23517201098130,
            "file_name": "Basic_Ui__28186_29.jpg",
            "content_url": "https://d3v-own153.zendesk.com/attachments/token/OYxJU9QmHuShTF417hKxY7pQV/?name=Basic_Ui__28186_29.jpg",
            "mapped_content_url": "https://d3v-own153.zendesk.com/attachments/token/OYxJU9QmHuShTF417hKxY7pQV/?name=Basic_Ui__28186_29.jpg",
            "content_type": "image/jpeg"
        }],
        "user_id": "john@kodtod.com"
    }
    
    query_params = [{"key": "ticket_id", "value": "1906"}]
    
    # Test the function
    result = create_freshservice_conversation(query_params, conversation_data, headers)
    print(json.dumps(result, indent=2))