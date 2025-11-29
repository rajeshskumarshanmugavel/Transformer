"""
base_target_connector.py
================

Reusable base classes and utilities for all target connectors.
This module contains the standardized interfaces and common functionality
that all platform-specific connectors inherit from.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Optional, Any, Union
from dataclasses import dataclass, field
import logging
import requests
import time
from datetime import datetime
import mimetypes
from urllib.parse import unquote


@dataclass
class Attachment:
    """Standardized attachment representation"""
    file_name: str
    content_url: Optional[str] = None
    content: Optional[bytes] = None
    content_type: Optional[str] = None
    size: Optional[int] = None
    is_inline: bool = False
    source_reference: Optional[str] = None  # For inline images


@dataclass
class InlineImage:
    """Standardized inline image representation"""
    source: str  # zendesk, servicenow, jira, etc.
    url: Optional[str] = None
    path: Optional[str] = None
    id: str = ""
    filename: str = ""
    token: Optional[str] = None
    attachment_info: Optional[str] = None


@dataclass
class ConnectorResponse:
    """Standardized response structure"""
    status_code: int
    success: bool
    data: Optional[Dict] = None
    error_message: Optional[str] = None
    failed_attachments: List[str] = field(default_factory=list)


@dataclass
class ConnectorConfig:
    """Standardized configuration for connectors"""
    domainUrl: str
    api_key: str
    additional_headers: Dict[str, str] = field(default_factory=dict)
    rate_limit_per_minute: int = 100
    max_retries: int = 3
    timeout: int = 30


class BaseInlineImageProcessor(ABC):
    """Base class for processing inline images from different source systems"""
    
    @abstractmethod
    def extract_inline_images(self, description: str) -> Tuple[str, List[InlineImage]]:
        """Extract inline images from description and return cleaned description + images"""
        pass
    
    @abstractmethod
    def get_source_patterns(self) -> List[Dict]:
        """Return regex patterns for different source systems"""
        pass
    
    def is_external_image(self, src_url: str) -> bool:
        """Check if image is from external source"""
        external_domains = [
            'google.com', 'googleapis.com', 'imgur.com', 'flickr.com',
            'amazonaws.com', 'cloudfront.net', 'gravatar.com', 'github.com',
            'dropbox.com', 'onedrive.com', 'drive.google.com'
        ]
        
        if src_url.startswith('data:image/'):
            return True
            
        return any(domain in src_url.lower() for domain in external_domains)
    
    def process_inline_attachments(self, description: str, inline_images: List[InlineImage], 
                                 headers: Dict) -> Tuple[str, List[Attachment]]:
        """Process inline images and convert them to attachments"""
        attachments_for_upload = []
        updated_description = description
        
        for image in inline_images:
            try:
                source_url = self._get_source_url(image, headers)
                if not source_url:
                    logging.warning(f"Could not determine source URL for image: {image}")
                    continue
                
                content = self._fetch_attachment_content(source_url, headers)
                if not content:
                    logging.warning(f"Failed to fetch content for: {image.filename}")
                    continue
                
                attachments_for_upload.append(Attachment(
                    file_name=image.filename,
                    content=content,
                    content_type=mimetypes.guess_type(image.filename)[0] or 'application/octet-stream',
                    is_inline=True,
                    source_reference=image.url or image.path
                ))
                
                # Remove inline image from description
                original_reference = image.url or image.path
                if original_reference:
                    import re
                    img_pattern = re.compile(
                        r'<img[^>]*?src=["\']' + re.escape(original_reference) + r'["\'][^>]*?/?>', 
                        re.IGNORECASE
                    )
                    updated_description = img_pattern.sub('', updated_description)
                
            except Exception as e:
                logging.error(f"Error processing inline image {image.filename}: {str(e)}")
                continue
        
        return updated_description, attachments_for_upload
    
    @abstractmethod
    def _get_source_url(self, image: InlineImage, headers: Dict) -> Optional[str]:
        """Get the source URL for fetching the attachment"""
        pass
    
    @abstractmethod
    def _fetch_attachment_content(self, url: str, headers: Dict) -> Optional[bytes]:
        """Fetch attachment content with appropriate authentication"""
        pass


class BaseRateLimitHandler(ABC):
    """Base class for handling rate limits across different platforms"""
    
    @abstractmethod
    def get_retry_delay(self, response: requests.Response) -> int:
        """Extract retry delay from response"""
        pass
    
    @abstractmethod
    def is_rate_limited(self, response: requests.Response) -> bool:
        """Check if response indicates rate limiting"""
        pass
    
    def make_request_with_retry(self, 
                               url: str, 
                               method: str = 'POST',
                               auth: Optional[Tuple] = None,
                               headers: Optional[Dict] = None,
                               json_data: Optional[Dict] = None,
                               form_data: Optional[List] = None,
                               files: Optional[List] = None,
                               max_retries: int = 3) -> requests.Response:
        """Make HTTP request with retry logic"""
        
        for attempt in range(max_retries + 1):
            try:
                kwargs = {
                    'timeout': 30,
                    'auth': auth,
                    'headers': headers
                }
                
                if files:
                    kwargs['files'] = files
                    if form_data:
                        kwargs['data'] = form_data
                elif json_data:
                    kwargs['json'] = json_data
                
                response = requests.request(method, url, **kwargs)
                
                if self.is_rate_limited(response):
                    if attempt < max_retries:
                        delay = self.get_retry_delay(response)
                        logging.warning(f"Rate limited. Retrying in {delay}s (attempt {attempt + 1})")
                        time.sleep(delay)
                        continue
                
                return response
                
            except requests.exceptions.RequestException as e:
                if attempt < max_retries:
                    logging.warning(f"Request failed: {e}. Retrying in 5s (attempt {attempt + 1})")
                    time.sleep(5)
                    continue
                raise
        
        return response


class BaseTargetConnector(ABC):
    """Base class for all target connectors"""
    
    def __init__(self, config: ConnectorConfig):
        self.config = config
        self.inline_processor = self._get_inline_processor()
        self.rate_limiter = self._get_rate_limiter()
    
    @abstractmethod
    def _get_inline_processor(self) -> BaseInlineImageProcessor:
        """Get platform-specific inline image processor"""
        pass
    
    @abstractmethod
    def _get_rate_limiter(self) -> BaseRateLimitHandler:
        """Get platform-specific rate limit handler"""
        pass
    
    @abstractmethod
    def _validate_instance(self) -> bool:
        """Validate connector instance/credentials"""
        pass
    
    def fetch_attachment(self, url: str, additional_headers: Dict = None) -> Optional[bytes]:
        """Fetch attachment content with platform-specific auth"""
        try:
            headers = self.config.additional_headers.copy()
            if additional_headers:
                headers.update(additional_headers)
            
            response = requests.get(url, headers=headers, timeout=self.config.timeout)
            response.raise_for_status()
            return response.content
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch attachment from {url}: {e}")
            return None
    
    def _process_attachments(self, attachments: List[Dict]) -> Tuple[List[Dict], List[str]]:
        """Process attachments for upload - expects dict format from transformer"""
        processed_files = []
        failed_files = []
        
        for att in attachments:
            try:
                if att.get('content') and isinstance(att['content'], bytes):
                    # Direct binary content
                    content = att['content']
                elif att.get('content_url') and not att['content_url'].startswith('data:'):
                    # Fetch from URL
                    content = self.fetch_attachment(att['content_url'])
                    if not content:
                        failed_files.append(att.get('file_name', 'unknown'))
                        continue
                else:
                    failed_files.append(att.get('file_name', 'unknown'))
                    continue
                
                file_name = unquote(att['file_name'])
                content_type = att.get('content_type') or mimetypes.guess_type(file_name)[0] or 'application/octet-stream'
                
                processed_files.append({
                    'name': file_name,
                    'content': content,
                    'content_type': content_type
                })
                
            except Exception as e:
                logging.error(f"Error processing attachment {att.get('file_name', 'unknown')}: {e}")
                failed_files.append(att.get('file_name', 'unknown'))
        
        return processed_files, failed_files
    
    def create_ticket(self, ticket_data: Dict, headers: Dict) -> ConnectorResponse:
        """
        Create a ticket with pre-transformed platform-specific data
        
        Args:
            ticket_data: Platform-specific ticket data from transformer
            headers: Authentication and configuration headers
        
        Returns:
            ConnectorResponse with creation result
        """
        try:
            # Make a copy to avoid modifying original
            ticket_copy = ticket_data.copy()
            
            # Extract and process attachments
            regular_attachments = ticket_copy.pop('attachments', [])
            
            # Process inline images in description
            description = ticket_copy.get('description', '')
            updated_description, inline_images = self.inline_processor.extract_inline_images(description)
            
            # Convert inline images to attachments
            inline_attachments = []
            if inline_images:
                updated_description, inline_attachments_objs = self.inline_processor.process_inline_attachments(
                    updated_description, inline_images, headers
                )
                # Convert Attachment objects back to dict format for processing
                inline_attachments = [
                    {
                        'file_name': att.file_name,
                        'content': att.content,
                        'content_type': att.content_type
                    }
                    for att in inline_attachments_objs
                ]
            
            # Update description
            ticket_copy['description'] = updated_description
            
            # Combine all attachments
            all_attachments = regular_attachments + inline_attachments
            
            # Process attachments for upload
            files, failed_files = self._process_attachments(all_attachments)
            
            # Create ticket via platform-specific implementation
            response = self._create_ticket_api_call(ticket_copy, files, headers)
            
            return ConnectorResponse(
                status_code=response.status_code,
                success=response.status_code in [200, 201],
                data=response.json() if response.status_code in [200, 201] and response.content else None,
                error_message=response.text if response.status_code not in [200, 201] else None,
                failed_attachments=failed_files
            )
            
        except Exception as e:
            logging.error(f"Error creating ticket: {e}")
            return ConnectorResponse(
                status_code=500,
                success=False,
                error_message=str(e)
            )
    
    def create_conversation(self, query_params: List[Dict], conversation_data: Dict, headers: Dict) -> ConnectorResponse:
        """
        Create a conversation/note with pre-transformed platform-specific data
        
        Args:
            query_params: List of query parameters (including ticket_id)
            conversation_data: Platform-specific conversation data from transformer
            headers: Authentication and configuration headers
        
        Returns:
            ConnectorResponse with creation result
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
            
            # Extract and process attachments
            regular_attachments = conv_copy.pop('attachments', [])
            
            # Process inline images in body
            body = conv_copy.get('body', '')
            updated_body, inline_images = self.inline_processor.extract_inline_images(body)
            
            # Convert inline images to attachments
            inline_attachments = []
            if inline_images:
                updated_body, inline_attachments_objs = self.inline_processor.process_inline_attachments(
                    updated_body, inline_images, headers
                )
                # Convert Attachment objects back to dict format for processing
                inline_attachments = [
                    {
                        'file_name': att.file_name,
                        'content': att.content,
                        'content_type': att.content_type
                    }
                    for att in inline_attachments_objs
                ]
            
            # Update body
            conv_copy['body'] = updated_body
            
            # Combine all attachments
            all_attachments = regular_attachments + inline_attachments
            
            # Process attachments for upload
            files, failed_files = self._process_attachments(all_attachments)
            
            # Create conversation via platform-specific implementation
            response = self._create_conversation_api_call(ticket_id, conv_copy, files, headers)
            
            return ConnectorResponse(
                status_code=response.status_code,
                success=response.status_code in [200, 201],
                data=response.json() if response.status_code in [200, 201] and response.content else None,
                error_message=response.text if response.status_code not in [200, 201] else None,
                failed_attachments=failed_files
            )
            
        except Exception as e:
            logging.error(f"Error creating conversation: {e}")
            return ConnectorResponse(
                status_code=500,
                success=False,
                error_message=str(e)
            )
    
    @abstractmethod
    def _create_ticket_api_call(self, ticket_data: Dict, files: List[Dict], headers: Dict) -> requests.Response:
        """Platform-specific ticket creation API call"""
        pass
    
    @abstractmethod
    def _create_conversation_api_call(self, ticket_id: str, conversation_data: Dict, 
                                    files: List[Dict], headers: Dict) -> requests.Response:
        """Platform-specific conversation creation API call"""
        pass


# ===================================================================
# COMMON UTILITY FUNCTIONS
# ===================================================================

def convert_headers_to_dict(headers):
    """Convert headers from list format to dict format"""
    if isinstance(headers, list):
        return {item["key"]: item["value"] for item in headers}
    return headers

def convert_query_params_to_dict(query_params):
    """Convert query parameters from list format to dict format"""
    if isinstance(query_params, list):
        return {param['key']: param['value'] for param in query_params}
    return query_params

def standardize_response_format(response: ConnectorResponse, headers) -> Dict:
    """Convert ConnectorResponse to transformer-expected format"""
    return {
        "status_code": response.status_code,
        "body": response.data if response.success else response.error_message,
        "headers": headers,
    }