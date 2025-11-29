"""
source_csv.py
=============

CSV-specific source connector implementation.
Reads CSV files from Azure Storage and provides paginated access to ticket and conversation data.
Following the same pattern as servicenow.py with support for Tickets and Conversations.
"""

import os
import json
import logging
import time
import csv
import io
import re
from typing import List, Dict, Tuple, Optional, Any
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions

# Dom - Added 8/2
import threading
from typing import Optional
import hashlib


class CSVFileCache:
    """Simple in-memory cache for CSV file contents"""

    def __init__(self, max_cache_size_mb: int = 500):
        self._cache = (
            {}
        )  # file_path -> {'content': str, 'size': int, 'timestamp': float}
        self._lock = threading.RLock()
        self.max_cache_size_bytes = max_cache_size_mb * 1024 * 1024
        self._current_size = 0

    def get(self, file_path: str) -> Optional[str]:
        """Get cached file content"""
        with self._lock:
            if file_path in self._cache:
                logging.info(f"Cache HIT for file: {file_path}")
                return self._cache[file_path]["content"]
            logging.info(f"Cache MISS for file: {file_path}")
            return None

    def put(self, file_path: str, content: str):
        """Cache file content with size management"""
        content_size = len(content.encode("utf-8"))

        with self._lock:
            # Check if we need to make room
            while (
                self._current_size + content_size > self.max_cache_size_bytes
                and len(self._cache) > 0
            ):
                self._evict_oldest()

            # Add to cache
            self._cache[file_path] = {
                "content": content,
                "size": content_size,
                "timestamp": time.time(),
            }
            self._current_size += content_size
            logging.info(
                f"Cached file {file_path} ({content_size} bytes). Total cache size: {self._current_size} bytes"
            )

    def _evict_oldest(self):
        """Remove oldest cached file"""
        if not self._cache:
            return

        oldest_key = min(self._cache.keys(), key=lambda k: self._cache[k]["timestamp"])
        removed_size = self._cache[oldest_key]["size"]
        del self._cache[oldest_key]
        self._current_size -= removed_size
        logging.info(f"Evicted cached file: {oldest_key} ({removed_size} bytes)")

    def clear(self):
        """Clear all cached content"""
        with self._lock:
            self._cache.clear()
            self._current_size = 0
            logging.info("Cache cleared")

    def get_stats(self):
        """Get cache statistics"""
        with self._lock:
            return {
                "cached_files": len(self._cache),
                "total_size_bytes": self._current_size,
                "total_size_mb": round(self._current_size / (1024 * 1024), 2),
                "max_size_mb": self.max_cache_size_bytes // (1024 * 1024),
            }


# Global cache instance
_csv_file_cache = CSVFileCache(max_cache_size_mb=150)  # 500MB cache


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
        safe_json_response,
        log_api_call,
    )
except ImportError:
    # Fallback - define minimal base classes if base_source_connector isn't available
    class BaseSourceRateLimitHandler:
        def is_rate_limited(self, response):
            return False

        def get_retry_delay(self, response):
            return 0

        def make_request_with_retry(self, url, method="GET", **kwargs):
            return None

    class BaseDataEnricher:
        def enrich_tickets(self, tickets, api_response):
            return tickets

        def enrich_conversations(self, conversations, users):
            return conversations

    class BaseFieldMapper:
        def get_standard_field_mapping(self):
            return {}

        def process_custom_fields(self, custom_fields, field_definitions):
            return {}

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
            return {param["key"]: param["value"] for param in query_params}
        return query_params

    def safe_json_response(response):
        return {}

    def log_api_call(method, url, status_code, duration):
        logging.info(f"{method} {url} - {status_code} ({duration:.2f}s)")

    def standardize_source_response_format(response_dict):
        """Convert SourceResponse to transformer-expected format"""
        if hasattr(response_dict, "success"):
            # It's a SourceResponse object
            if response_dict.success:
                return {
                    "status_code": response_dict.status_code,
                    "body": response_dict.data,
                }
            else:
                return {
                    "status_code": response_dict.status_code,
                    "body": {"error": response_dict.error_message},
                }
        else:
            # It's already a dict
            if response_dict.get("success", response_dict.get("status_code") == 200):
                return {
                    "status_code": response_dict.get("status_code", 200),
                    "body": response_dict.get("data", response_dict.get("body", {})),
                }
            else:
                return {
                    "status_code": response_dict.get("status_code", 500),
                    "body": {
                        "error": response_dict.get(
                            "error_message", response_dict.get("error", "Unknown error")
                        )
                    },
                }

    # Data classes
    class SourceConnectorConfig:
        def __init__(self, **kwargs):
            self.azure_connection_string = kwargs.get("azure_connection_string")
            self.azure_container_name = kwargs.get("azure_container_name")
            self.page_size = kwargs.get("page_size", 100)
            self.fix_date_logic = kwargs.get("fix_date_logic", True)
            self.date_format = kwargs.get("date_format", "auto")

    class SourceResponse:
        def __init__(
            self, status_code, success, data=None, error_message=None, **kwargs
        ):
            self.status_code = status_code
            self.success = success
            self.data = data
            self.error_message = error_message

    class PaginationInfo:
        def __init__(self, has_more=False, next_cursor=None, total_count=None):
            self.has_more = has_more
            self.next_cursor = next_cursor
            self.total_count = total_count


class CSVRateLimitHandler(BaseSourceRateLimitHandler):
    """CSV-specific rate limit handler (minimal since we're reading from storage)"""

    def is_rate_limited(self, response) -> bool:
        """CSV operations don't have rate limits"""
        return False

    def get_retry_delay(self, response) -> int:
        """No retry needed for CSV operations"""
        return 0

    def make_request_with_retry(self, operation_func, *args, **kwargs):
        """Execute CSV operation with basic retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                return operation_func(*args, **kwargs)
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e
                logging.warning(
                    f"CSV operation failed, attempt {attempt + 1}/{max_retries}: {e}"
                )
                time.sleep(1)  # Brief delay before retry


class CSVDataEnricher(BaseDataEnricher):
    """CSV-specific data enricher"""

    def __init__(self, connector):
        super().__init__()
        self.connector = connector

    def enrich_tickets(self, tickets: List[Dict], **kwargs) -> List[Dict]:
        """Enrich CSV ticket records"""
        enriched_tickets = []

        for ticket in tickets:
            try:
                enriched_ticket = self._enrich_single_ticket(ticket)
                enriched_tickets.append(enriched_ticket)
            except Exception as e:
                logging.warning(f"Failed to enrich ticket: {e}")
                # Add the ticket without enrichment to avoid data loss
                enriched_tickets.append(ticket)

        return enriched_tickets

    def _enrich_single_ticket(self, ticket: Dict) -> Dict:
        """Enrich a single ticket record"""
        enriched = ticket.copy()

        # Parse attachments field if present
        if "attachments" in enriched:
            enriched["attachments"] = self._parse_attachments_field(
                enriched["attachments"]
            )
        
        # CSVATTACHMENT RAJESH - Process CSV attachment URLs from "Attachment file url" field
        if "Attachment file url" in enriched:
            attachment_urls = enriched.get("Attachment file url", "")
            if attachment_urls and str(attachment_urls).strip():
                # Convert attachment URL(s) to standardized attachment objects
                enriched["attachments"] = self._parse_csv_attachment_urls(attachment_urls)
                logging.info(f"CSV attachment processing: Found {len(enriched['attachments'])} attachment(s) in field 'Attachment file url' for record")
            else:
                enriched["attachments"] = []

        # Standardize date fields using connector's method
        date_fields = [
            "created_at",
            "updated_at",
            "resolved_at",
            "closed_at",
            "due_date",
        ]
        for field in date_fields:
            if field in enriched and enriched[field]:
                # Only process if not already processed by CSV parser
                if not enriched.get(f"_original_{field}"):
                    original_value = enriched[field]
                    standardized_value = self.connector._standardize_date_format(
                        original_value
                    )
                    if standardized_value != original_value:
                        enriched[f"_original_{field}"] = original_value
                        enriched[field] = standardized_value

        # Validate date logic (if enabled in config)
        if getattr(self.connector.config, "fix_date_logic", True):
            enriched = self.connector._validate_date_logic_simple(enriched)

        # Add computed fields
        enriched["_source"] = "csv"
        enriched["_enriched_at"] = datetime.now().isoformat()

        return enriched

    def enrich_conversations(self, conversations: List[Dict], **kwargs) -> List[Dict]:
        """Enrich CSV conversation records"""
        enriched_conversations = []

        for conversation in conversations:
            try:
                enriched_conversation = self._enrich_single_conversation(conversation)
                enriched_conversations.append(enriched_conversation)
            except Exception as e:
                logging.warning(f"Failed to enrich conversation: {e}")
                # Add the conversation without enrichment to avoid data loss
                enriched_conversations.append(conversation)

        return enriched_conversations

    def _enrich_single_conversation(self, conversation: Dict) -> Dict:
        """Enrich a single conversation record"""
        enriched = conversation.copy()

        # Parse attachments field if present
        if "attachments" in enriched:
            enriched["attachments"] = self._parse_attachments_field(
                enriched["attachments"]
            )
        
        # CSVATTACHMENT RAJESH - Process CSV attachment URLs for conversations/notes
        if "Attachment file url" in enriched:
            attachment_urls = enriched.get("Attachment file url", "")
            if attachment_urls and str(attachment_urls).strip():
                enriched["attachments"] = self._parse_csv_attachment_urls(attachment_urls)
                logging.info(f"CSV attachment processing: Found {len(enriched['attachments'])} attachment(s) in conversation/note")
            else:
                enriched["attachments"] = []

        # Standardize date fields using connector's method
        date_fields = [
            "created_at",
            "updated_at",
            "timestamp",
            "date",
            "sent_at",
            "received_at",
        ]
        for field in date_fields:
            if field in enriched and enriched[field]:
                # Only process if not already processed by CSV parser
                if not enriched.get(f"_original_{field}"):
                    original_value = enriched[field]
                    standardized_value = self.connector._standardize_date_format(
                        original_value
                    )
                    if standardized_value != original_value:
                        enriched[f"_original_{field}"] = original_value
                        enriched[field] = standardized_value

        # Validate date logic (if enabled in config)
        if getattr(self.connector.config, "fix_date_logic", True):
            enriched = self.connector._validate_date_logic_simple(enriched)

        # Add computed fields
        enriched["_source"] = "csv"
        enriched["_enriched_at"] = datetime.now().isoformat()

        return enriched

    def _parse_attachments_field(self, attachments_value: Any) -> List[Dict]:
        """Parse attachments field into a standardized array format"""
        if not attachments_value:
            return []

        # Handle string values
        if isinstance(attachments_value, str):
            attachments_value = attachments_value.strip()
            if not attachments_value:
                return []

            # Try to parse as JSON
            try:
                parsed = json.loads(attachments_value)
                if isinstance(parsed, list):
                    return parsed
                elif isinstance(parsed, dict):
                    return [parsed]
                else:
                    # Treat as simple string attachment
                    return [{"content_url": str(parsed), "type": "unknown"}]
            except json.JSONDecodeError:
                # Not JSON, treat as comma-separated or single attachment
                if "," in attachments_value:
                    # Split by comma and create attachment objects
                    attachments = []
                    for attachment_name in attachments_value.split(","):
                        attachment_name = attachment_name.strip()
                        if attachment_name:
                            attachments.append(
                                {
                                    "content_url": attachment_name,
                                    "type": self._guess_attachment_type(
                                        attachment_name
                                    ),
                                }
                            )
                    return attachments
                else:
                    # Single attachment
                    return [
                        {
                            "content_url": attachments_value,
                            "type": self._guess_attachment_type(attachments_value),
                        }
                    ]

        # Handle list values
        elif isinstance(attachments_value, list):
            standardized = []
            for item in attachments_value:
                if isinstance(item, dict):
                    standardized.append(item)
                else:
                    standardized.append(
                        {
                            "content_url": str(item),
                            "type": self._guess_attachment_type(str(item)),
                        }
                    )
            return standardized

        # Handle dict values
        elif isinstance(attachments_value, dict):
            return [attachments_value]

        # Handle other types
        else:
            return [{"content_url": str(attachments_value), "type": "unknown"}]

    def _guess_attachment_type(self, filename: str) -> str:
        """Guess attachment type from filename extension"""
        if not filename or "." not in filename:
            return "unknown"

        extension = filename.lower().split(".")[-1]

        type_map = {
            "pdf": "document",
            "doc": "document",
            "docx": "document",
            "txt": "document",
            "rtf": "document",
            "jpg": "image",
            "jpeg": "image",
            "png": "image",
            "gif": "image",
            "bmp": "image",
            "svg": "image",
            "mp4": "video",
            "avi": "video",
            "mov": "video",
            "wmv": "video",
            "mp3": "audio",
            "wav": "audio",
            "flac": "audio",
            "zip": "archive",
            "rar": "archive",
            "7z": "archive",
            "tar": "archive",
            "gz": "archive",
            "xls": "spreadsheet",
            "xlsx": "spreadsheet",
            "csv": "spreadsheet",
            "ppt": "presentation",
            "pptx": "presentation",
        }

        return type_map.get(extension, "unknown")

    def _parse_csv_attachment_urls(self, attachment_urls: str) -> List[Dict]:
        """
        CSVATTACHMENT RAJESH - Parse CSV attachment URLs from 'Attachment file url' field.
        
        Converts attachment URL strings into standardized attachment objects compatible 
        with UniversalAttachmentService.
        
        Args:
            attachment_urls: String containing one or more attachment URLs
            
        Returns:
            List of attachment objects with url, content_url, name, etc.
        """
        if not attachment_urls or not str(attachment_urls).strip():
            return []
        
        attachment_urls = str(attachment_urls).strip()
        
        # Handle multiple URLs (comma or semicolon separated)
        if ',' in attachment_urls:
            url_list = [url.strip() for url in attachment_urls.split(',') if url.strip()]
        elif ';' in attachment_urls:
            url_list = [url.strip() for url in attachment_urls.split(';') if url.strip()]
        else:
            url_list = [attachment_urls]
        
        attachments = []
        for url in url_list:
            if not url:
                continue
                
            # Extract filename from URL
            filename = self._extract_filename_from_url(url)
            content_type = self._guess_content_type_from_filename(filename)
            
            # Check if this is an Azure Blob URL and generate SAS token if needed
            azure_components = {}
            if ".blob.core.windows.net" in url:
                azure_components = self._parse_azure_blob_url(url)
                if azure_components:
                    # Generate SAS URL for Azure Blob
                    url = self._generate_azure_blob_sas_url(url, azure_components)
            
            # Create standardized attachment object
            attachment = {
                "url": url,
                "content_url": url,
                "attachment_url": url,
                "name": filename,
                "file_name": filename,
                "content_type": content_type,
                "size": None,  # Unknown from CSV
                "source_type": "azure_blob" if ".blob.core.windows.net" in url else "generic",
                "_csv_source": True
            }
            
            # Add Azure Blob specific metadata if applicable
            if azure_components:
                attachment.update(azure_components)
            
            attachments.append(attachment)
            logging.debug(f"CSVATTACHMENT DEBUG: Parsed attachment from CSV: {filename} -> {url[:100]}...")
        
        return attachments
    
    def _extract_filename_from_url(self, url: str) -> str:
        """Extract filename from URL"""
        try:
            # Remove query parameters
            if '?' in url:
                url = url.split('?')[0]
            
            # Get the last part of the path
            filename = url.split('/')[-1]
            
            # URL decode if needed (handle %20 etc.)
            import urllib.parse
            filename = urllib.parse.unquote(filename)
            
            return filename if filename else "unknown_file"
        except:
            return "unknown_file"
    
    def _guess_content_type_from_filename(self, filename: str) -> str:
        """Guess MIME type from filename"""
        if not filename or '.' not in filename:
            return "application/octet-stream"
        
        extension = filename.lower().split('.')[-1]
        
        mime_types = {
            'pdf': 'application/pdf',
            'doc': 'application/msword',
            'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'txt': 'text/plain',
            'rtf': 'application/rtf',
            'jpg': 'image/jpeg',
            'jpeg': 'image/jpeg',
            'png': 'image/png',
            'gif': 'image/gif',
            'bmp': 'image/bmp',
            'svg': 'image/svg+xml',
            'webp': 'image/webp',
            'mp4': 'video/mp4',
            'avi': 'video/avi',
            'mov': 'video/quicktime',
            'wmv': 'video/x-ms-wmv',
            'webm': 'video/webm',
            'mp3': 'audio/mpeg',
            'wav': 'audio/wav',
            'flac': 'audio/flac',
            'ogg': 'audio/ogg',
            'zip': 'application/zip',
            'rar': 'application/x-rar-compressed',
            '7z': 'application/x-7z-compressed',
            'tar': 'application/x-tar',
            'gz': 'application/gzip',
            'xls': 'application/vnd.ms-excel',
            'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'csv': 'text/csv',
            'ppt': 'application/vnd.ms-powerpoint',
            'pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
            'html': 'text/html',
            'xml': 'application/xml',
            'json': 'application/json'
        }
        
        return mime_types.get(extension, 'application/octet-stream')
    
    def _parse_azure_blob_url(self, url: str) -> Dict:
        """Parse Azure Blob Storage URL to extract components"""
        try:
            import re
            # Pattern: https://{account}.blob.core.windows.net/{container}/{blob_path}
            match = re.match(r'https://([^.]+)\.blob\.core\.windows\.net/([^/]+)/(.+)', url)
            if match:
                account_name, container_name, blob_path = match.groups()
                return {
                    "_azure_storage_account": account_name,
                    "_azure_container": container_name,
                    "_azure_blob_path": blob_path
                }
        except:
            pass
        return {}
    
    def _generate_azure_blob_sas_url(self, url: str, azure_components: Dict) -> str:
        """Generate SAS URL for Azure Blob Storage if connection string is available"""
        try:
            from src.utils.config import CONFIG
            
            # Check if URL already has SAS token
            if '?' in url and ('sig=' in url or 'sv=' in url):
                logging.debug(f"Azure Blob URL already has SAS token: {url[:100]}...")
                return url
            
            # Get connection string from CONFIG
            connection_string = CONFIG.get('AZURE_CONNECTION_STRING')
            if not connection_string:
                logging.debug("No AZURE_CONNECTION_STRING in CONFIG, returning original URL")
                return url
            
            # Extract account key from connection string
            account_key = None
            for part in connection_string.split(';'):
                if part.startswith('AccountKey='):
                    account_key = part.split('=', 1)[1]
                    break
            
            if not account_key:
                logging.debug("No AccountKey found in connection string, returning original URL")
                return url
            
            account_name = azure_components.get('_azure_storage_account')
            container_name = azure_components.get('_azure_container')
            blob_path = azure_components.get('_azure_blob_path')
            
            if not all([account_name, container_name, blob_path]):
                logging.debug("Missing Azure components, returning original URL")
                return url
            
            # Generate SAS token with read permissions valid for 1 hour
            sas_token = generate_blob_sas(
                account_name=account_name,
                container_name=container_name,
                blob_name=blob_path,
                account_key=account_key,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.utcnow() + timedelta(hours=1)
            )
            
            # Append SAS token to URL
            sas_url = f"{url}?{sas_token}"
            logging.info(f"Generated SAS URL for Azure Blob: {url[:60]}... -> {sas_url[:100]}...")
            return sas_url
            
        except ImportError as e:
            logging.warning(f"Azure Storage Blob library not available: {e}")
            return url
        except Exception as e:
            logging.error(f"Error generating Azure Blob SAS URL: {e}")
            return url


class CSVFieldMapper(BaseFieldMapper):
    """CSV-specific field mapper"""

    def get_standard_field_mapping(self) -> Dict[str, str]:
        """Return mapping of CSV fields to standard field names"""
        return {
            # Common ticket fields
            "ticket_id": "id",
            "ticketid": "id",
            "id": "id",
            "ticket_number": "number",
            "ticketnumber": "number",
            "number": "number",
            "subject": "subject",
            "title": "subject",
            "description": "description",
            "status": "status",
            "state": "status",
            "priority": "priority",
            "severity": "severity",
            "category": "category",
            "type": "type",
            "assignee": "assignee",
            "assigned_to": "assignee",
            "reporter": "reporter",
            "created_by": "reporter",
            "created_date": "created_at",
            "created_at": "created_at",
            "updated_date": "updated_at",
            "updated_at": "updated_at",
            "resolved_date": "resolved_at",
            "resolved_at": "resolved_at",
            "closed_date": "closed_at",
            "closed_at": "closed_at",
            # Conversation fields
            "conversation_id": "id",
            "message": "content",
            "content": "content",
            "comment": "content",
            "author": "author",
            "created_by": "author",
            "timestamp": "created_at",
            "date": "created_at",
        }

    def process_custom_fields(self, record: Dict) -> Dict[str, Any]:
        """Process and identify custom fields in CSV record"""
        standard_fields = set(self.get_standard_field_mapping().values())
        custom_fields = {}

        for field_name, field_value in record.items():
            if field_name.lower() not in standard_fields:
                custom_fields[field_name] = field_value

        return custom_fields


class CSVStorageClient:
    """Azure Storage client for CSV operations"""

    def __init__(self, connection_string: str, container_name: str):
        self.connection_string = connection_string
        self.container_name = container_name
        self.blob_service_client = None
        self._init_client()

    def _init_client(self):
        """Initialize Azure Blob Service Client"""
        try:
            self.blob_service_client = BlobServiceClient.from_connection_string(
                self.connection_string
            )
        except Exception as e:
            logging.error(f"Failed to initialize Azure Blob Service Client: {e}")
            raise

    def read_csv_file(self, file_path: str) -> str:
        """Read CSV file content from Azure Storage"""
        try:

            # Try cache first - Dom added 8/2
            cached_content = _csv_file_cache.get(file_path)
            if cached_content is not None:
                return cached_content

            # Cache miss - download from Azure
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, blob=file_path
            )

            # Download blob content
            blob_data = blob_client.download_blob()
            content = blob_data.readall().decode("utf-8")

            # Cache the content
            _csv_file_cache.put(file_path, content)

            return content

        except Exception as e:
            logging.error(f"Error reading CSV file {file_path}: {e}")
            raise

    def file_exists(self, file_path: str) -> bool:
        """Check if file exists in Azure Storage"""
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, blob=file_path
            )
            return blob_client.exists()
        except Exception as e:
            logging.warning(f"Error checking file existence {file_path}: {e}")
            return False

    def get_file_size(self, file_path: str) -> int:
        """Get file size in bytes"""
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, blob=file_path
            )
            properties = blob_client.get_blob_properties()
            return properties.size
        except Exception as e:
            logging.warning(f"Error getting file size {file_path}: {e}")
            return 0


class CSVConnector(BaseSourceConnector):
    """CSV implementation of the base source connector"""

    def __init__(self, config: SourceConnectorConfig):
        super().__init__(config)
        # Override with CSV-specific components
        self.rate_limiter = CSVRateLimitHandler()
        self.data_enricher = CSVDataEnricher(self)
        self.field_mapper = CSVFieldMapper()

        # Initialize storage client
        self.storage_client = None
        self._init_storage_client()

    def _init_storage_client(self):
        """Initialize Azure Storage client"""
        try:
            self.storage_client = CSVStorageClient(
                self.config.azure_connection_string, self.config.azure_container_name
            )
        except Exception as e:
            logging.error(f"Failed to initialize storage client: {e}")
            # Don't raise here, let individual operations handle the error

    def _get_rate_limiter(self) -> BaseSourceRateLimitHandler:
        return CSVRateLimitHandler()

    def _get_data_enricher(self) -> BaseDataEnricher:
        return CSVDataEnricher(self)

    def _get_field_mapper(self) -> BaseFieldMapper:
        return CSVFieldMapper()

    def _validate_config(self) -> bool:
        """Validate CSV configuration"""
        return all(
            [self.config.azure_connection_string, self.config.azure_container_name]
        )

    def _build_file_path(self, file_path: str, file_name: str) -> str:
        """Build complete file path for Azure Storage"""
        if file_path and not file_path.endswith("/"):
            file_path += "/"
        return f"{file_path}{file_name}".lstrip("/")

    def _sanitize_key(self, key: str) -> str:
        """Sanitize field names by removing BOM and normalizing whitespace"""
        if not key:
            return 'unknown_field'
        
        # Remove BOM (Byte Order Mark) and other invisible Unicode characters
        key = key.replace('\ufeff', '').replace('\ufffe', '').replace('\u200b', '')
        
        # Strip leading/trailing whitespace
        key = key.strip()
        
        if not key:
            return 'unknown_field'
        
        # Collapse multiple spaces into single space but preserve spaces
        key = re.sub(r'\s+', ' ', key)
        
        return key

    def _parse_csv_content(
        self, content: str, offset: int = 0, page_size: int = None
    ) -> Tuple[List[Dict], int, bool]:
        """Parse CSV content and return paginated results"""
        try:
            # Create CSV reader
            csv_reader = csv.DictReader(io.StringIO(content))

            # Convert to list for easier manipulation
            all_rows = list(csv_reader)
            total_count = len(all_rows)

            # Apply offset
            if offset > 0:
                all_rows = all_rows[offset:]

            # Apply page size
            has_more = False
            if page_size and len(all_rows) > page_size:
                all_rows = all_rows[:page_size]
                has_more = True
            elif page_size and offset + page_size < total_count:
                has_more = True

            # Sanitize field names and process data
            processed_rows = []
            for row in all_rows:
                processed_row = {}
                for key, value in row.items():
                    # Sanitize field names to remove special characters
                    sanitized_key = self._sanitize_key(key)
                    processed_row[sanitized_key] = value

                # Apply date standardization immediately during parsing
                processed_row = self._apply_date_standardization(processed_row)
                processed_rows.append(processed_row)

            return processed_rows, total_count, has_more

        except Exception as e:
            logging.error(f"Error parsing CSV content: {e}")
            raise

    def _apply_date_standardization(self, record: Dict) -> Dict:
        """Apply date standardization and validation to a record"""
        # Common date field names to process
        date_fields = [
            "created_at",
            "updated_at",
            "resolved_at",
            "closed_at",
            "due_date",
            "timestamp",
            "date",
            "sent_at",
            "received_at",
            "opened_at",
            "last_updated",
            "modified_at",
            "created_date",
            "updated_date",
        ]

        for field in date_fields:
            if field in record and record[field]:
                original_value = record[field]
                standardized_value = self._standardize_date_format(original_value)
                if standardized_value != original_value:
                    # Store original for debugging
                    record[f"_original_{field}"] = original_value
                    record[field] = standardized_value

        # Apply date logic validation
        record = self._validate_date_logic_simple(record)

        return record

    def _validate_date_logic_simple(self, record: Dict) -> Dict:
        """Simplified date logic validation"""
        try:
            created_at_str = record.get("created_at")
            updated_at_str = record.get("updated_at")

            if not created_at_str or not updated_at_str:
                return record

            # Parse dates for comparison
            try:
                created_at = datetime.fromisoformat(
                    created_at_str.replace("Z", "+00:00")
                )
                updated_at = datetime.fromisoformat(
                    updated_at_str.replace("Z", "+00:00")
                )
            except ValueError:
                # If parsing fails, return as-is
                return record

            # Check if created_at > updated_at
            if created_at > updated_at:
                logging.warning(
                    f"Date logic issue detected: created_at ({created_at_str}) > updated_at ({updated_at_str}). "
                    f"Setting updated_at to created_at."
                )
                # Fix by setting updated_at to created_at
                record["updated_at"] = created_at_str
                record["_date_fix_applied"] = True

            return record

        except Exception as e:
            logging.warning(f"Error validating date logic: {e}")
            return record

    def _standardize_date_format(self, date_value: str) -> str:
        """Standardize date format to ISO 8601 format"""
        if not date_value or not isinstance(date_value, str):
            return date_value

        date_value = date_value.strip()
        if not date_value:
            return date_value

        # Common date format patterns
        date_patterns = [
            "%m/%d/%y %H:%M",  # 10/25/22 13:29
            "%m/%d/%Y %H:%M",  # 10/25/2022 13:29
            "%m/%d/%y %H:%M:%S",  # 10/25/22 13:29:30
            "%m/%d/%Y %H:%M:%S",  # 10/25/2022 13:29:30
            "%Y-%m-%d %H:%M:%S",  # 2022-10-25 13:29:30
            "%Y-%m-%dT%H:%M:%S",  # 2022-10-25T13:29:30
            "%Y-%m-%dT%H:%M:%SZ",  # 2022-10-25T13:29:30Z
            "%Y-%m-%d",  # 2022-10-25
            "%m/%d/%y",  # 10/25/22
            "%m/%d/%Y",  # 10/25/2022
        ]

        for pattern in date_patterns:
            try:
                # Parse the date
                parsed_date = datetime.strptime(date_value, pattern)

                # Handle 2-digit years (assume 2000s for years < 50, 1900s for >= 50)
                if pattern.find("%y") != -1:  # 2-digit year
                    if parsed_date.year < 1950:
                        parsed_date = parsed_date.replace(year=parsed_date.year + 100)

                # Return in ISO 8601 format
                return parsed_date.isoformat()

            except ValueError:
                continue

        # If no pattern matches, log warning and return original
        logging.warning(f"Could not parse date format: {date_value}")
        return date_value

    def get_tickets(self, query_params: Dict) -> SourceResponse:
        """Get tickets from CSV file with support for pagination"""
        try:
            if not self._validate_config():
                return SourceResponse(400, False, error_message="Invalid configuration")

            if not self.storage_client:
                return SourceResponse(
                    500, False, error_message="Storage client not initialized"
                )

            # Extract parameters
            file_path = query_params.get("filePath", "")
            file_name = query_params.get("fileName", "")
            offset = int(query_params.get("offset", 0))
            page_size = int(query_params.get("page[size]", 100))

            if not file_name:
                return SourceResponse(
                    400, False, error_message="fileName parameter is required"
                )

            # Build complete file path
            complete_path = self._build_file_path(file_path, file_name)

            # Check if file exists
            if not self.storage_client.file_exists(complete_path):
                return SourceResponse(
                    404, False, error_message=f"File not found: {complete_path}"
                )

            # Read CSV content
            start_time = time.time()

            def read_operation():
                return self.storage_client.read_csv_file(complete_path)

            content = self.rate_limiter.make_request_with_retry(read_operation)

            # Parse CSV content with pagination
            tickets, total_count, has_more = self._parse_csv_content(
                content, offset, page_size
            )

            # Enrich tickets
            try:
                enriched_tickets = self.data_enricher.enrich_tickets(tickets)
            except Exception as e:
                logging.error(f"Error during ticket enrichment: {e}")
                enriched_tickets = tickets

            duration = time.time() - start_time
            log_api_call("READ_CSV", complete_path, 200, duration)

            return SourceResponse(
                status_code=200,
                success=True,
                data={
                    "tickets": enriched_tickets,
                    "meta": {
                        "total_count": total_count,
                        "has_more": has_more,
                        "offset": offset,
                        "page_size": page_size,
                        "file_path": complete_path,
                        "records_returned": len(enriched_tickets),
                    },
                },
            )

        except Exception as e:
            logging.error(f"Error getting tickets from CSV: {e}", exc_info=True)
            return SourceResponse(500, False, error_message=str(e))

    def get_changes(self, query_params: Dict) -> SourceResponse:
        """Get changes from CSV file with support for pagination"""
        try:
            if not self._validate_config():
                return SourceResponse(400, False, error_message="Invalid configuration")

            if not self.storage_client:
                return SourceResponse(
                    500, False, error_message="Storage client not initialized"
                )

            # Extract parameters
            file_path = query_params.get("filePath", "")
            file_name = query_params.get("fileName", "")
            offset = int(query_params.get("offset", 0))
            page_size = int(query_params.get("page[size]", 100))

            if not file_name:
                return SourceResponse(
                    400, False, error_message="fileName parameter is required"
                )

            # Build complete file path
            complete_path = self._build_file_path(file_path, file_name)

            # Check if file exists
            if not self.storage_client.file_exists(complete_path):
                return SourceResponse(
                    404, False, error_message=f"File not found: {complete_path}"
                )

            # Read CSV content
            start_time = time.time()

            def read_operation():
                return self.storage_client.read_csv_file(complete_path)

            content = self.rate_limiter.make_request_with_retry(read_operation)

            # Parse CSV content with pagination
            tickets, total_count, has_more = self._parse_csv_content(
                content, offset, page_size
            )

            # Enrich changes
            try:
                enriched_tickets = self.data_enricher.enrich_tickets(tickets)
            except Exception as e:
                logging.error(f"Error during ticket enrichment: {e}")
                enriched_tickets = tickets

            duration = time.time() - start_time
            log_api_call("READ_CSV", complete_path, 200, duration)

            return SourceResponse(
                status_code=200,
                success=True,
                data={
                    "changes": enriched_tickets,
                    "meta": {
                        "total_count": total_count,
                        "has_more": has_more,
                        "offset": offset,
                        "page_size": page_size,
                        "file_path": complete_path,
                        "records_returned": len(enriched_tickets),
                    },
                },
            )

        except Exception as e:
            logging.error(f"Error getting tickets from CSV: {e}", exc_info=True)
            return SourceResponse(500, False, error_message=str(e))


    def extract_last_key(self, expression):
        # Remove leading/trailing whitespace and curly braces
        expr = expression.strip()
        if expr.startswith("{{") and expr.endswith("}}"):
            expr = expr[2:-2].strip()
        else:
            return None
        # Split by dot and return the last part
        parts = expr.split(".")
        return parts[-1] if parts else None

    def get_conversations(
        self,
        ticket_id: str,
        file_path: str,
        file_name: str,
        uniqueIdentifier: str = None,
    ) -> SourceResponse:
        """Get all conversations for a specific ticket ID from CSV file"""
        try:
            if not self._validate_config():
                return SourceResponse(400, False, error_message="Invalid configuration")

            if not self.storage_client:
                return SourceResponse(
                    500, False, error_message="Storage client not initialized"
                )

            if not ticket_id:
                return SourceResponse(
                    400, False, error_message="ticket_id parameter is required"
                )

            if not file_name:
                return SourceResponse(
                    400, False, error_message="fileName parameter is required"
                )

            # Build complete file path
            complete_path = self._build_file_path(file_path, file_name)

            # Check if file exists
            if not self.storage_client.file_exists(complete_path):
                return SourceResponse(
                    404, False, error_message=f"File not found: {complete_path}"
                )

            # Read CSV content
            start_time = time.time()

            def read_operation():
                return self.storage_client.read_csv_file(complete_path)

            content = self.rate_limiter.make_request_with_retry(read_operation)

            # Parse CSV content (no pagination for conversations)
            all_conversations, total_count, _ = self._parse_csv_content(content)

            # Filter conversations by ticket ID
            # Try different possible field names for ticket ID
            ticket_id_fields = ["TicketId", "ticketid", "ticket_id", "id", "parent_id"]
            extracted_uniqueIdentifier = (
                self.extract_last_key(uniqueIdentifier) if uniqueIdentifier else None
            )
            if extracted_uniqueIdentifier:
                ticket_id_fields.append(extracted_uniqueIdentifier)
            matching_conversations = []

            for conversation in all_conversations:
                for field in ticket_id_fields:
                    if field in conversation and str(conversation[field]) == str(
                        ticket_id
                    ):
                        matching_conversations.append(conversation)
                        break  # Found a match, no need to check other fields

            # Dom 8/2 - Skip enrichment if no matches found
            if not matching_conversations:
                duration = time.time() - start_time
                log_api_call("READ_CSV", complete_path, 200, duration)

                return SourceResponse(
                    status_code=200,
                    success=True,
                    data={
                        "conversations": [],
                        "ticket_id": ticket_id,
                        "meta": {
                            "total_conversations_in_file": total_count,
                            "matching_conversations": 0,
                            "file_path": complete_path,
                            "ticket_id_fields_checked": ticket_id_fields,
                            "enrichment_skipped": True,  # Debug flag
                        },
                    },
                )

            # Enrich conversations
            try:
                enriched_conversations = self.data_enricher.enrich_conversations(
                    matching_conversations
                )
            except Exception as e:
                logging.error(f"Error during conversation enrichment: {e}")
                enriched_conversations = matching_conversations

            duration = time.time() - start_time
            log_api_call("READ_CSV", complete_path, 200, duration)

            return SourceResponse(
                status_code=200,
                success=True,
                data={
                    "conversations": enriched_conversations,
                    "ticket_id": ticket_id,
                    "meta": {
                        "total_conversations_in_file": total_count,
                        "matching_conversations": len(enriched_conversations),
                        "file_path": complete_path,
                        "ticket_id_fields_checked": ticket_id_fields,
                    },
                },
            )

        except Exception as e:
            logging.error(f"Error getting conversations from CSV: {e}", exc_info=True)
            return SourceResponse(500, False, error_message=str(e))


# ===================================================================
# GLOBAL CONNECTOR INSTANCE
# ===================================================================

# Commented to  avoid global state issues in multi-threaded environments
# _csv_connector = None


def _get_csv_connector(headers):
    """Get or create CSV connector instance"""
    headers_dict = convert_source_headers_to_dict(headers)

    # Get Azure configuration from environment or headers
    azure_connection_string = headers_dict.get("azure_connection_string") or os.getenv(
        "AZURE_CONNECTION_STRING"
    )
    azure_container_name = headers_dict.get("azure_container_name") or os.getenv(
        "AZURE_STORAGE_CONTAINER_NAME"
    )

    config = SourceConnectorConfig(
        azure_connection_string=azure_connection_string,
        azure_container_name=azure_container_name,
        page_size=int(headers_dict.get("page_size", 100)),
    )
    return CSVConnector(config)  # Always create fresh


# ===================================================================
# TRANSFORMER-COMPATIBLE FUNCTION INTERFACES
# ===================================================================


def get_csv_tickets_v1(**kwargs) -> Dict:
    """
    CSV tickets retrieval function

    Args:
        headers: Authentication headers and configuration
        queryParams: Query parameters for file path and pagination
        **kwargs: Additional arguments (e.g., numberOfProcessedRecords)

    Returns:
        Dict with status_code and body (always includes 'tickets' key)
    """
    try:
        connector = _get_csv_connector(kwargs["sourceHeaders"])
        query_dict = convert_query_params_to_dict(kwargs["queryParams"])

        # Extract numberOfProcessedRecords from kwargs
        numberOfProcessedRecords = kwargs.get("numberOfProcessedRecords", 0)

        # Use numberOfProcessedRecords as offset if not explicitly set
        if "offset" not in query_dict:
            query_dict["offset"] = numberOfProcessedRecords

        # Add any additional kwargs to query_dict
        for key, value in kwargs.items():
            if key not in ["sourceHeaders", "queryParams", "numberOfProcessedRecords"]:
                query_dict[key] = value

        response = connector.get_tickets(query_dict)

        # Ensure consistent response format for transformer
        if response.success:
            result = standardize_source_response_format(response)
        else:
            # Error case - still return tickets key with empty list
            result = {
                "status_code": response.status_code,
                "body": {
                    "tickets": [],
                    "error": response.error_message,
                    "meta": {
                        "total_count": 0,
                        "has_more": False,
                        "offset": numberOfProcessedRecords,
                        "page_size": query_dict.get("page[size]", 100),
                    },
                },
            }

        return result

    except Exception as e:
        logging.error(f"Error in get_csv_tickets_v1: {e}", exc_info=True)
        return {
            "status_code": 500,
            "body": {
                "tickets": [],
                "error": str(e),
                "meta": {
                    "total_count": 0,
                    "has_more": False,
                    "offset": 0,
                    "page_size": 100,
                },
            },
        }

def get_csv_changes_v1(**kwargs) -> Dict:
    """
    CSV changes retrieval function

    Args:
        headers: Authentication headers and configuration
        queryParams: Query parameters for file path and pagination
        **kwargs: Additional arguments (e.g., numberOfProcessedRecords)

    Returns:
        Dict with status_code and body (always includes 'tickets' key)
    """
    try:
        connector = _get_csv_connector(kwargs["sourceHeaders"])
        query_dict = convert_query_params_to_dict(kwargs["queryParams"])

        # Extract numberOfProcessedRecords from kwargs
        numberOfProcessedRecords = kwargs.get("numberOfProcessedRecords", 0)

        # Use numberOfProcessedRecords as offset if not explicitly set
        if "offset" not in query_dict:
            query_dict["offset"] = numberOfProcessedRecords

        # Add any additional kwargs to query_dict
        for key, value in kwargs.items():
            if key not in ["sourceHeaders", "queryParams", "numberOfProcessedRecords"]:
                query_dict[key] = value

        response = connector.get_changes(query_dict)

        # Ensure consistent response format for transformer
        if response.success:
            result = standardize_source_response_format(response)
        else:
            # Error case - still return tickets key with empty list
            result = {
                "status_code": response.status_code,
                "body": {
                    "tickets": [],
                    "error": response.error_message,
                    "meta": {
                        "total_count": 0,
                        "has_more": False,
                        "offset": numberOfProcessedRecords,
                        "page_size": query_dict.get("page[size]", 100),
                    },
                },
            }

        return result

    except Exception as e:
        logging.error(f"Error in get_csv_tickets_v1: {e}", exc_info=True)
        return {
            "status_code": 500,
            "body": {
                "tickets": [],
                "error": str(e),
                "meta": {
                    "total_count": 0,
                    "has_more": False,
                    "offset": 0,
                    "page_size": 100,
                },
            },
        }



def get_csv_conversations_v1(**kwargs) -> Dict:
    """
    CSV conversations retrieval function

    Args:
        headers: Authentication headers and configuration
        queryParams: Contains ticket_id, filePath, and fileName parameters

    Returns:
        Dict with status_code and body (key name depends on the source type)
    """
    try:
        connector = _get_csv_connector(kwargs["sourceHeaders"])
        headers_dict = convert_source_headers_to_dict(kwargs["sourceHeaders"])
        query_dict = convert_query_params_to_dict(kwargs["queryParams"])
        uniqueIdentifier = kwargs.get("sourceConfigUniqueIdentifier", None)
        
        # Smart source type detection from fileName
        file_name = query_dict.get("fileName", "")
        if "task" in file_name.lower():
            source_key = "tasks"
            logging.info(f"Detected source type 'tasks' from fileName: {file_name}")
        else:
            source_key = "conversations"
            logging.info(f"Detected source type 'conversations' from fileName: {file_name}")

        # Extract parameters
        ticket_id = query_dict.get("ticket_id") or query_dict.get("ticketid")
        file_path = query_dict.get("filePath", "")

        if not ticket_id:
            return {
                "status_code": 400,
                "body": {
                    source_key: [],
                    "error": "ticket_id parameter is required",
                },
            }

        response = connector.get_conversations(
            ticket_id, file_path, file_name, uniqueIdentifier
        )

        # Ensure consistent response format
        if response.success:
            result = standardize_source_response_format(response)
            # Rename the key to match detected source type
            if 'conversations' in result['body'] and source_key != 'conversations':
                result['body'][source_key] = result['body'].pop('conversations')
                logging.info(f"Renamed response key from 'conversations' to '{source_key}' for framework compatibility")
        else:
            result = {
                "status_code": response.status_code,
                "body": {
                    source_key: [],  # Use detected source key
                    "error": response.error_message,
                    "ticket_id": ticket_id,
                },
            }

        return result

    except Exception as e:
        logging.error(f"Error in get_csv_conversations_v1: {e}", exc_info=True)
        # Try to detect source key from error context
        try:
            query_dict = convert_query_params_to_dict(kwargs.get("queryParams", []))
            file_name = query_dict.get("fileName", "")
            source_key = "tasks" if "task" in file_name.lower() else "conversations"
        except:
            source_key = "conversations"  # Fallback
        return {"status_code": 500, "body": {source_key: [], "error": str(e)}}


def validate_csv_configuration_v1(**kwargs) -> Dict:
    """Validate CSV configuration and Azure Storage connectivity"""
    try:
        connector = _get_csv_connector(kwargs["sourceHeaders"])

        if not connector._validate_config():
            return {
                "status_code": 400,
                "body": {
                    "valid": False,
                    "error": "Invalid configuration - missing Azure connection details",
                },
            }

        # Test Azure Storage connectivity
        try:
            # Try to list containers to verify connection
            blob_service_client = connector.storage_client.blob_service_client
            container_client = blob_service_client.get_container_client(
                connector.config.azure_container_name
            )
            container_client.get_container_properties()

            is_valid = True
            error_message = None
        except Exception as e:
            is_valid = False
            error_message = f"Azure Storage connection failed: {str(e)}"

        return {
            "status_code": 200,
            "body": {
                "valid": is_valid,
                "container": connector.config.azure_container_name,
                "error": error_message,
            },
        }

    except Exception as e:
        logging.error(f"Error validating CSV configuration: {e}")
        return {"status_code": 500, "body": {"valid": False, "error": str(e)}}


def get_csv_file_info_v1(**kwargs) -> Dict:
    """Get information about a CSV file (size, existence, etc.)"""
    try:
        connector = _get_csv_connector(kwargs["sourceHeaders"])
        query_dict = convert_query_params_to_dict(kwargs["queryParams"])

        file_path = query_dict.get("filePath", "")
        file_name = query_dict.get("fileName", "")

        if not file_name:
            return {
                "status_code": 400,
                "body": {"error": "fileName parameter is required"},
            }

        complete_path = connector._build_file_path(file_path, file_name)

        # Check file existence and get info
        exists = connector.storage_client.file_exists(complete_path)

        if exists:
            file_size = connector.storage_client.get_file_size(complete_path)

            # Try to get row count by reading the file
            try:
                content = connector.storage_client.read_csv_file(complete_path)
                row_count = len(content.splitlines()) - 1  # Subtract header row
            except Exception as e:
                row_count = None
                logging.warning(f"Could not determine row count: {e}")

            return {
                "status_code": 200,
                "body": {
                    "exists": True,
                    "file_path": complete_path,
                    "file_size_bytes": file_size,
                    "estimated_row_count": row_count,
                },
            }
        else:
            return {
                "status_code": 404,
                "body": {
                    "exists": False,
                    "file_path": complete_path,
                    "error": "File not found",
                },
            }

    except Exception as e:
        logging.error(f"Error getting CSV file info: {e}")
        return {"status_code": 500, "body": {"error": str(e)}}


# Dom - 8/2 Helper functions

# ADD this helper function at the end of the file for cache management:


def get_csv_cache_stats() -> Dict:
    """Get cache statistics - useful for monitoring"""
    return _csv_file_cache.get_stats()


def clear_csv_cache():
    """Clear the CSV cache - useful for testing or memory management"""
    _csv_file_cache.clear()


def get_csv_cache_info_v1(**kwargs) -> Dict:
    """Endpoint to check cache status"""
    try:
        stats = get_csv_cache_stats()
        return {
            "status_code": 200,
            "body": {"cache_stats": stats, "cache_enabled": True},
        }
    except Exception as e:
        return {"status_code": 500, "body": {"error": str(e)}}


# ===================================================================
# TESTING AND EXAMPLE USAGE
# ===================================================================


def example_usage():
    """Example showing how the CSV connector works"""

    # Headers (matching transformer format)
    headers = [
        {
            "key": "azure_connection_string",
            "value": "DefaultEndpointsProtocol=https;AccountName=...",
        },
        {"key": "azure_container_name", "value": "csv-files"},
        {"key": "filePath", "value": "CSV/Files"},  # For conversations
        {"key": "fileName", "value": "conversations.csv"},  # For conversations
    ]

    # Query parameters for tickets
    ticket_params = [
        {"key": "filePath", "value": "CSV/Files"},
        {"key": "fileName", "value": "tickets.csv"},
        {"key": "offset", "value": "0"},
        {"key": "page[size]", "value": "100"},
    ]

    # Get tickets
    print("Getting tickets...")
    tickets_result = get_csv_tickets_v1(
        sourceHeaders=headers, queryParams=ticket_params
    )
    print(f"Tickets result: {tickets_result.get('status_code')}")

    if tickets_result.get("status_code") == 200:
        tickets = tickets_result.get("body", {}).get("tickets", [])
        print(f"Found {len(tickets)} tickets")

        # Get conversations for first ticket
        if tickets:
            ticket_id = tickets[0].get("id") or tickets[0].get("ticketid")
            print(f"\nGetting conversations for ticket {ticket_id}...")

            conversation_params = [{"key": "ticket_id", "value": ticket_id}]
            conversations_result = get_csv_conversations_v1(
                headers, conversation_params
            )
            print(f"Conversations result: {conversations_result.get('status_code')}")

    # Validate configuration
    print("\nValidating CSV configuration...")
    validation_result = validate_csv_configuration_v1(headers)
    print(f"Validation result: {validation_result}")

    # Get file info
    print("\nGetting file info...")
    file_info_params = [
        {"key": "filePath", "value": "CSV/Files"},
        {"key": "fileName", "value": "tickets.csv"},
    ]
    file_info_result = get_csv_file_info_v1(headers, file_info_params)
    print(f"File info result: {file_info_result}")


if __name__ == "__main__":
    # For testing purposes only
    example_usage()