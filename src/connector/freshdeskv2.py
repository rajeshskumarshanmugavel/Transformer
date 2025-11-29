"""
freshdeskv2.py
=========================

Complete product-agnostic Freshdesk connector that works with ANY source platform.
ALL source-specific logic is handled by UniversalAttachmentService.

Based on freshservicev2.py architecture with Freshdesk-specific API endpoints.
"""

import requests
import logging
import asyncio
import atexit
import threading
from typing import List, Dict, Tuple, Optional
from datetime import datetime
import time
import concurrent.futures
from datetime import datetime, timezone, timedelta

# Import base classes
from src.connector.base_target_connector import (
    BaseTargetConnector,
    BaseInlineImageProcessor,
    BaseRateLimitHandler,
    ConnectorConfig,
    ConnectorResponse,
    InlineImage,
    convert_headers_to_dict,
    standardize_response_format,
)

# Import the universal attachment service
from src.connector.utils.attachment_service import UniversalAttachmentService

# Import user helper
from src.connector.utils.getfwuserid import FreshworksHelper


# ===================================================================
# PRODUCT-AGNOSTIC INLINE PROCESSOR (Same as Freshservice)
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
        return []

    def extract_inline_images(self, description: str) -> Tuple[str, List[InlineImage]]:
        """
        This method is no longer used - all extraction is in AttachmentService.
        Kept for backward compatibility.
        """
        return description, []

    async def process_inline_images_universal(
        self,
        description: str,
        source_domain: Optional[str] = None,
        auth_config: Optional[Dict] = None,
    ) -> Tuple[str, List[Dict]]:
        """
        Universal inline image processing using AttachmentService.
        Works with ANY source platform automatically.
        """
        if not self.attachment_service:
            logging.warning(
                "No AttachmentService available for inline image processing"
            )
            return description, []

        try:
            result = await self.attachment_service.process_inline_images_universal(
                description, source_domain, auth_config
            )

            processed_attachments = []
            for attachment in result.extracted_attachments:
                processed_attachments.append(
                    {
                        "name": attachment.name,
                        "content": attachment.content,
                        "content_type": attachment.content_type,
                    }
                )

            if result.failed_extractions:
                logging.warning(
                    f"Failed to extract {len(result.failed_extractions)} inline images"
                )

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
# FRESHDESK RATE LIMIT HANDLER
# ===================================================================


class FreshdeskRateLimitHandler(BaseRateLimitHandler):
    """Freshdesk-specific rate limit handler"""

    def is_rate_limited(self, response: requests.Response) -> bool:
        """Check if response indicates rate limiting"""
        if response.status_code == 429:
            return True

        if response.status_code == 200:
            try:
                response_json = response.json()
                response_text = str(response_json).lower()
                if (
                    "rate_limit" in response_text
                    or "too many requests" in response_text
                ):
                    return True
            except:
                pass

        return False

    def get_retry_delay(self, response: requests.Response) -> int:
        """Extract retry delay from response headers"""
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return int(retry_after)
            except ValueError:
                try:
                    retry_date = datetime.strptime(
                        retry_after, "%a, %d %b %Y %H:%M:%S GMT"
                    )
                    return max(0, int((retry_date - datetime.utcnow()).total_seconds()))
                except ValueError:
                    pass

        reset_time = response.headers.get("X-RateLimit-Reset") or response.headers.get(
            "X-Rate-Limit-Reset"
        )
        if reset_time:
            try:
                reset_timestamp = int(reset_time)
                current_timestamp = int(time.time())
                return max(0, reset_timestamp - current_timestamp)
            except ValueError:
                pass

        return 60


# ===================================================================
# MAIN UNIVERSAL FRESHDESK CONNECTOR
# ===================================================================


class UniversalFreshdeskConnector(BaseTargetConnector):
    """
    Completely product-agnostic Freshdesk connector.
    Works with ANY source platform without modification.
    """

    def __init__(
        self,
        config: ConnectorConfig,
        attachment_service: Optional[UniversalAttachmentService] = None,
    ):
        """
        Initialize connector

        Args:
            config: Connector configuration
            attachment_service: Shared UniversalAttachmentService instance
        """
        super().__init__(config)
        self.attachment_service = attachment_service
        self._external_attachment_service = attachment_service is not None

        # Always create inline processor
        self.inline_processor = UniversalInlineProcessor(self.attachment_service)

    @property
    def safe_attachment_service(self) -> Optional[UniversalAttachmentService]:
        """Safe property to get attachment service"""
        return getattr(self, "attachment_service", None)

    def _get_inline_processor(self) -> BaseInlineImageProcessor:
        """Get universal inline processor"""
        return UniversalInlineProcessor(self.safe_attachment_service)

    def _get_rate_limiter(self) -> BaseRateLimitHandler:
        return FreshdeskRateLimitHandler()

    def _validate_instance(self) -> bool:
        """Validate Freshdesk instance/credentials"""
        try:
            url = f"https://{self.config.domainUrl}.freshdesk.com/api/v2/tickets"
            response = requests.get(url, auth=(self.config.api_key, "X"), timeout=10)
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

        headers_dict = (
            convert_headers_to_dict(headers) if isinstance(headers, list) else headers
        )

        # Let the attachment service handle ALL source-specific logic
        _, auth_config = self.safe_attachment_service.normalize_attachment_data(
            [], headers_dict
        )

        return auth_config

    def _extract_source_auth_from_context(
        self, headers: Dict, body: Dict
    ) -> Optional[Dict]:
        """
        Extract source authentication from various places in the request context.
        """
        auth_config = {}

        # First, try standard header extraction
        auth_from_headers = self._build_universal_auth_config(headers)
        if auth_from_headers:
            return auth_from_headers

        # Check if this is a Zendesk migration based on attachment URLs
        attachments = body.get("attachments", [])
        if attachments:
            first_attachment = (
                attachments[0] if isinstance(attachments, list) else attachments
            )
            attachment_url = (
                first_attachment.get("url")
                or first_attachment.get("content_url")
                or first_attachment.get("attachment_url")
                or ""
            )

            if "zendesk.com" in attachment_url:
                logging.info(
                    "Detected Zendesk attachments, checking for auth in headers"
                )
                headers_dict = (
                    convert_headers_to_dict(headers)
                    if isinstance(headers, list)
                    else headers
                )

                # Check for Zendesk auth in different locations
                username = headers_dict.get("username", "")
                password = headers_dict.get("password", "")

                if username and password:
                    auth_config = self._build_universal_auth_config(headers)
                    if auth_config:
                        return auth_config

                # If still no auth, mark as Zendesk for proper handling
                auth_config["source_type"] = "zendesk"

                # Extract domain from URL if possible
                import re

                domain_match = re.search(
                    r"https://([^.]+)\.zendesk\.com", attachment_url
                )
                if domain_match:
                    auth_config["source_domain"] = domain_match.group(1)

        return auth_config if auth_config else None

    async def _process_attachments_universal(
        self, attachments: List[Dict], auth_config: Optional[Dict] = None
    ) -> Tuple[List[Dict], List[str]]:
        """
        Universal attachment processing using AttachmentService.
        """
        if not attachments:
            logging.info("No attachments to process")
            return [], []

        logging.info(f"Processing {len(attachments)} attachments")

        if not self.safe_attachment_service:
            logging.error("No AttachmentService available for processing attachments")
            return [], [att.get("name", "unknown") for att in attachments]

        try:
            # Log attachment details for debugging
            for i, att in enumerate(attachments):
                url = (
                    att.get("url")
                    or att.get("content_url")
                    or att.get("attachment_url")
                )
                name = att.get("name") or att.get("file_name") or "unknown"
                logging.info(f"Attachment {i+1}: {name}")
                if url:
                    logging.info(f"  URL: {url[:100]}...")

            # Process all attachments
            results = await self.safe_attachment_service.download_attachments_batch(
                attachments, auth_config
            )

            # Convert results to expected format
            successful_files = []
            failed_files = []

            for i, result in enumerate(results):
                att_name = attachments[i].get(
                    "name", attachments[i].get("file_name", "unknown")
                )

                if result:
                    logging.info(
                        f"Successfully downloaded: {att_name} ({len(result.content)} bytes)"
                    )
                    successful_files.append(
                        {
                            "name": result.name,
                            "content": result.content,
                            "content_type": result.content_type,
                        }
                    )
                else:
                    logging.error(f"Failed to download: {att_name}")
                    failed_files.append(att_name)

            logging.info(
                f"Attachment processing complete: {len(successful_files)} successful, {len(failed_files)} failed"
            )

            return successful_files, failed_files

        except Exception as e:
            logging.error(f"Error in universal attachment processing: {e}")
            import traceback

            logging.error(f"Traceback: {traceback.format_exc()}")
            return [], [att.get("name", "unknown") for att in attachments]

    def _create_ticket_api_call(
        self, ticket_data: Dict, files: List[Dict], headers: Dict
    ) -> requests.Response:
        """Create Freshdesk ticket with proper custom_fields handling"""
        url = f"https://{self.config.domainUrl}.freshdesk.com/api/channel/v2/tickets"
        auth = (self.config.api_key, "X")

        # Dom - Filter out empty/null custom field values before API call
        if "custom_fields" in ticket_data and isinstance(
            ticket_data["custom_fields"], dict
        ):
            filtered_custom_fields = {
                key: value
                for key, value in ticket_data["custom_fields"].items()
                if value is not None and value != "" and value != []
            }
            if filtered_custom_fields:
                ticket_data["custom_fields"] = filtered_custom_fields
            else:
                # Remove custom_fields entirely if all values are empty/null
                ticket_data.pop("custom_fields", None)

        if files:
            form_files = [
                ("attachments[]", (f["name"], f["content"], f["content_type"]))
                for f in files
            ]

            form_data = []
            for key, value in ticket_data.items():
                if key == "custom_fields" and isinstance(value, dict):
                    # Handle custom_fields as individual form fields
                    for cf_key, cf_value in value.items():
                        if cf_value is not None:
                            form_data.append(
                                (f"custom_fields[{cf_key}]", str(cf_value))
                            )
                            logging.info(
                                f"Added custom field: custom_fields[{cf_key}] = {cf_value}"
                            )
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
                url, "POST", auth=auth, form_data=form_data, files=form_files
            )
        else:
            return self.rate_limiter.make_request_with_retry(
                url, "POST", auth=auth, json_data=ticket_data
            )

    def _create_conversation_api_call(
        self, ticket_id: str, conversation_data: Dict, files: List[Dict], headers: Dict
    ) -> requests.Response:
        """Create Freshdesk conversation/note with attachments"""
        # Freshdesk uses 'notes' endpoint for conversations
        url = f"https://{self.config.domainUrl}.freshdesk.com/api/channel/v2/tickets/{ticket_id}/notes"
        auth = (self.config.api_key, "X")

        if files:
            # Freshdesk uses 'attachments[]' for file uploads
            form_files = [
                ("attachments[]", (f["name"], f["content"], f["content_type"]))
                for f in files
            ]

            # Convert conversation data to form data
            form_data = conversation_data.copy()
            if "private" in form_data:
                form_data["private"] = (
                    "true"
                    if str(form_data["private"]).lower() in ["true", "1", "yes"]
                    else "false"
                )

            form_data_list = [(k, str(v)) for k, v in form_data.items()]

            return self.rate_limiter.make_request_with_retry(
                url, "POST", auth=auth, form_data=form_data_list, files=form_files
            )
        else:
            return self.rate_limiter.make_request_with_retry(
                url, "POST", auth=auth, json_data=conversation_data
            )

    def _create_reply_api_call(
        self, ticket_id: str, reply_data: Dict, files: List[Dict], headers: Dict
    ) -> requests.Response:
        """Create Freshdesk reply (public conversation) with attachments"""
        # Freshdesk uses 'reply' endpoint for public conversations
        url = f"https://{self.config.domainUrl}.freshdesk.com/api/channel/v2/tickets/{ticket_id}/reply"
        auth = (self.config.api_key, "X")

        if files:
            form_files = [
                ("attachments[]", (f["name"], f["content"], f["content_type"]))
                for f in files
            ]

            form_data = reply_data.copy()
            form_data_list = [(k, str(v)) for k, v in form_data.items()]

            return self.rate_limiter.make_request_with_retry(
                url, "POST", auth=auth, form_data=form_data_list, files=form_files
            )
        else:
            return self.rate_limiter.make_request_with_retry(
                url, "POST", auth=auth, json_data=reply_data
            )

    async def create_ticket_universal(
        self, ticket_data: Dict, headers: Dict
    ) -> ConnectorResponse:
        """
        Universal ticket creation method.
        Completely source-agnostic - ALL source logic is in attachment service.
        """
        try:
            # Make a copy to avoid modifying original
            ticket_copy = ticket_data.copy()

            # --- Begin: Dynamic key renaming logic ---
            key_map = {
                "ticket_type": "type",
                "product": "product_id",
                "company": "company_id",
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
            regular_attachments = ticket_copy.pop("attachments", [])

            # Let attachment service handle ALL normalization and auth extraction
            auth_config = None
            normalized_attachments = regular_attachments

            if self.safe_attachment_service and regular_attachments:
                headers_dict = (
                    convert_headers_to_dict(headers)
                    if isinstance(headers, list)
                    else headers
                )
                normalized_attachments, auth_config = (
                    self.safe_attachment_service.normalize_attachment_data(
                        regular_attachments, headers_dict
                    )
                )

                logging.info(
                    f"Attachment service normalized {len(normalized_attachments)} attachments"
                )
                if auth_config:
                    logging.info(
                        f"Attachment service extracted auth for source: {auth_config.get('source_type', 'unknown')}"
                    )

            # Process inline images using AttachmentService
            description = ticket_copy.get("description", "")
            inline_attachments = []

            if self.safe_attachment_service and description:
                try:
                    source_domain = (
                        auth_config.get("source_domain") if auth_config else None
                    )

                    updated_description, inline_attachments = (
                        await self.inline_processor.process_inline_images_universal(
                            description, source_domain, auth_config
                        )
                    )
                    ticket_copy["description"] = updated_description
                except Exception as e:
                    logging.error(
                        f"Error processing inline images in ticket description: {e}"
                    )

            # Process attachments using UniversalAttachmentService
            regular_files, failed_files = await self._process_attachments_universal(
                normalized_attachments, auth_config
            )

            # Combine all files
            all_files = regular_files + inline_attachments

            logging.info(
                f"Creating ticket with {len(all_files)} attachments, {len(failed_files)} failed"
            )

            # Create ticket
            response = self._create_ticket_api_call(ticket_copy, all_files, headers)

            return ConnectorResponse(
                status_code=response.status_code,
                success=response.status_code in [200, 201],
                data=(
                    response.json()
                    if response.status_code in [200, 201] and response.content
                    else None
                ),
                error_message=(
                    response.text if response.status_code not in [200, 201] else None
                ),
                failed_attachments=failed_files,
            )

        except Exception as e:
            logging.error(f"Error in create_ticket_universal: {e}")
            return ConnectorResponse(
                status_code=500, success=False, error_message=str(e)
            )

    async def create_conversation_universal(
        self, query_params: List[Dict], conversation_data: Dict, headers: Dict
    ) -> ConnectorResponse:
        """
        Universal conversation/note creation method.
        Completely source-agnostic - ALL source logic is in attachment service.
        """
        try:
            # Extract ticket_id from query params
            ticket_id = None
            for param in query_params:
                if param.get("key") == "ticket_id":
                    ticket_id = param.get("value")
                    break

            if not ticket_id:
                raise ValueError("ticket_id not found in query parameters")

            if conversation_data.get("created_at") is None or (
                isinstance(conversation_data.get("created_at"), str)
                and conversation_data["created_at"].strip() == ""
            ):
                # Freshdesk requires created_at to be a string in ISO format
                conversation_data["created_at"] = datetime.now(timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )

            if conversation_data.get("updated_at") is None or (
                isinstance(conversation_data.get("updated_at"), str)
                and conversation_data["updated_at"].strip() == ""
            ):
                # Freshdesk requires updated_at to be a string in ISO format
                conversation_data["updated_at"] = (
                    datetime.now(timezone.utc) + timedelta(seconds=5)
                ).strftime("%Y-%m-%dT%H:%M:%SZ")

            # Make a copy to avoid modifying original
            conv_copy = conversation_data.copy()

            # Extract attachments
            regular_attachments = conv_copy.pop("attachments", [])

            # Let attachment service handle ALL normalization and auth extraction
            auth_config = None
            normalized_attachments = regular_attachments

            if self.safe_attachment_service and regular_attachments:
                headers_dict = (
                    convert_headers_to_dict(headers)
                    if isinstance(headers, list)
                    else headers
                )
                normalized_attachments, auth_config = (
                    self.safe_attachment_service.normalize_attachment_data(
                        regular_attachments, headers_dict
                    )
                )

                logging.info(
                    f"Attachment service normalized {len(normalized_attachments)} attachments"
                )
                if auth_config:
                    logging.info(
                        f"Attachment service extracted auth for source: {auth_config.get('source_type', 'unknown')}"
                    )

            # Process inline images using AttachmentService
            body = conv_copy.get("body", "")
            inline_attachments = []

            if self.safe_attachment_service and body:
                try:
                    source_domain = (
                        auth_config.get("source_domain") if auth_config else None
                    )

                    updated_body, inline_attachments = (
                        await self.inline_processor.process_inline_images_universal(
                            body, source_domain, auth_config
                        )
                    )
                    conv_copy["body"] = updated_body
                except Exception as e:
                    logging.error(f"Error processing inline images: {e}")

            # Process attachments using UniversalAttachmentService
            regular_files, failed_files = await self._process_attachments_universal(
                normalized_attachments, auth_config
            )

            # Combine all files
            all_files = regular_files + inline_attachments

            logging.info(
                f"Creating conversation with {len(all_files)} attachments, {len(failed_files)} failed"
            )

            # Create conversation
            response = self._create_conversation_api_call(
                ticket_id, conv_copy, all_files, headers
            )

            return ConnectorResponse(
                status_code=response.status_code,
                success=response.status_code in [200, 201],
                data=(
                    response.json()
                    if response.status_code in [200, 201] and response.content
                    else None
                ),
                error_message=(
                    response.text if response.status_code not in [200, 201] else None
                ),
                failed_attachments=failed_files,
            )

        except Exception as e:
            logging.error(f"Error in create_conversation_universal: {e}")
            import traceback

            logging.error(f"Full traceback: {traceback.format_exc()}")
            return ConnectorResponse(
                status_code=500, success=False, error_message=str(e)
            )

    async def create_reply_universal(
        self, query_params: List[Dict], reply_data: Dict, headers: Dict
    ) -> ConnectorResponse:
        """
        Universal reply creation method (Freshdesk-specific endpoint).
        Replies are public conversations in Freshdesk.
        """
        try:
            # Extract ticket_id from query params
            ticket_id = None
            for param in query_params:
                if param.get("key") == "ticket_id":
                    ticket_id = param.get("value")
                    break

            if not ticket_id:
                raise ValueError("ticket_id not found in query parameters")

            # Make a copy to avoid modifying original
            reply_copy = reply_data.copy()

            # Extract attachments
            regular_attachments = reply_copy.pop("attachments", [])

            # Let attachment service handle ALL normalization and auth extraction
            auth_config = None
            normalized_attachments = regular_attachments

            if self.safe_attachment_service and regular_attachments:
                headers_dict = (
                    convert_headers_to_dict(headers)
                    if isinstance(headers, list)
                    else headers
                )
                normalized_attachments, auth_config = (
                    self.safe_attachment_service.normalize_attachment_data(
                        regular_attachments, headers_dict
                    )
                )

            # Process inline images
            body = reply_copy.get("body", "")
            inline_attachments = []

            if self.safe_attachment_service and body:
                try:
                    source_domain = (
                        auth_config.get("source_domain") if auth_config else None
                    )
                    updated_body, inline_attachments = (
                        await self.inline_processor.process_inline_images_universal(
                            body, source_domain, auth_config
                        )
                    )
                    reply_copy["body"] = updated_body
                except Exception as e:
                    logging.error(f"Error processing inline images: {e}")

            # Process attachments
            regular_files, failed_files = await self._process_attachments_universal(
                normalized_attachments, auth_config
            )

            # Combine all files
            all_files = regular_files + inline_attachments

            # Create reply
            response = self._create_reply_api_call(
                ticket_id, reply_copy, all_files, headers
            )

            return ConnectorResponse(
                status_code=response.status_code,
                success=response.status_code in [200, 201],
                data=(
                    response.json()
                    if response.status_code in [200, 201] and response.content
                    else None
                ),
                error_message=(
                    response.text if response.status_code not in [200, 201] else None
                ),
                failed_attachments=failed_files,
            )

        except Exception as e:
            logging.error(f"Error in create_reply_universal: {e}")
            return ConnectorResponse(
                status_code=500, success=False, error_message=str(e)
            )


# ===================================================================
# GLOBAL ATTACHMENT SERVICE MANAGEMENT (Same as Freshservice)
# ===================================================================

# Commented to avoid conflicts - Global attachment service for reuse across requests
# _universal_attachment_service: Optional[UniversalAttachmentService] = None
# _service_lock: Optional[asyncio.Lock] = None
# _service_loop: Optional[asyncio.AbstractEventLoop] = None

# Thread-local storage for event loops
_thread_local = threading.local()


def get_or_create_event_loop():
    """Get the current event loop or create a new one if needed"""
    try:
        loop = asyncio.get_running_loop()
        return loop
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


def get_thread_loop():
    """Get or create an event loop for the current thread"""
    if not hasattr(_thread_local, "loop") or _thread_local.loop.is_closed():
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

    return _universal_attachment_service


def cleanup_attachment_service():
    """Cleanup function - no longer needed since we don't cache globally"""
    # logging.info("No global attachment service to clean up")
    pass


# Register cleanup function
atexit.register(cleanup_attachment_service)


# ===================================================================
# ASYNC FUNCTIONS FOR FASTAPI
# ===================================================================


async def create_freshdesk_ticket_async(body: Dict, headers) -> Dict:
    """Async version of Freshdesk ticket creation."""
    attachment_service = None
    try:
        headers_dict = convert_headers_to_dict(headers)

        # Get fresh attachment service
        attachment_service = await get_universal_attachment_service()

        if not attachment_service:
            logging.warning(
                "Could not initialize AttachmentService - proceeding without attachment processing"
            )

        # Create connector with attachment service
        config = ConnectorConfig(
            domainUrl=headers_dict.get("domainUrl"),
            api_key=headers_dict.get("apikey")
            or headers_dict.get("api_key")
            or headers_dict.get("username"),
        )
        connector = UniversalFreshdeskConnector(config, attachment_service)

        # Create ticket using universal method
        response = await connector.create_ticket_universal(body, headers)
        return standardize_response_format(response, headers)

    except Exception as e:
        logging.error(f"Error in create_freshdesk_ticket_async: {e}")
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


async def create_freshdesk_conversation_async(
    queryParams: List[Dict], body: Dict, headers
) -> Dict:
    """
    Async version of Freshdesk conversation/note creation.
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
        user_id = body.get("user_id", "")
        if user_id and isinstance(user_id, str) and user_id.strip():
            # Run sync code in thread pool to avoid blocking
            loop = asyncio.get_running_loop()
            resolved_user_id = await loop.run_in_executor(
                None,
                lambda: FreshworksHelper(
                    headers_dict.get("domainUrl"),
                    headers_dict.get("apikey")
                    or headers_dict.get("api_key")
                    or headers_dict.get("username"),
                    user_id,
                    "freshdesk",
                ).find_or_create_requester(),
            )

            if resolved_user_id:
                body_copy = body.copy()
                body_copy["user_id"] = int(resolved_user_id)
            else:
                body_copy = body.copy()
        else:
            body_copy = body.copy()

        # Get shared universal attachment service
        attachment_service = await get_universal_attachment_service()

        if not attachment_service:
            logging.warning(
                "Could not initialize AttachmentService - proceeding without attachment processing"
            )

        # Create connector with shared attachment service
        config = ConnectorConfig(
            domainUrl=headers_dict.get("domainUrl"),
            api_key=headers_dict.get("apikey")
            or headers_dict.get("api_key")
            or headers_dict.get("username"),
        )
        connector = UniversalFreshdeskConnector(config, attachment_service)

        # Create conversation using universal method
        response = await connector.create_conversation_universal(
            queryParams, body_copy, headers
        )
        return standardize_response_format(response, headers)

    except Exception as e:
        logging.error(f"Error in create_freshdesk_conversation_async: {e}")
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


async def create_freshdesk_reply_async(
    queryParams: List[Dict], body: Dict, headers
) -> Dict:
    """
    Async version of Freshdesk reply creation.
    This should be called from FastAPI endpoints.

    Args:
        queryParams: List of query parameters (including ticket_id)
        body: Reply data with attachments from ANY source
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
            logging.warning(
                "Could not initialize AttachmentService - proceeding without attachment processing"
            )

        # Create connector with shared attachment service
        config = ConnectorConfig(
            domainUrl=headers_dict.get("domainUrl"),
            api_key=headers_dict.get("apikey")
            or headers_dict.get("api_key")
            or headers_dict.get("username"),
        )
        connector = UniversalFreshdeskConnector(config, attachment_service)

        # Create reply using universal method
        response = await connector.create_reply_universal(queryParams, body, headers)
        return standardize_response_format(response, headers)

    except Exception as e:
        logging.error(f"Error in create_freshdesk_reply_async: {e}")
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


def create_freshdesk_ticket_universal(body: Dict, headers) -> Dict:
    """
    Universal Freshdesk ticket creation.
    Works with attachments from ANY source platform.
    """
    try:
        try:
            loop = asyncio.get_running_loop()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(
                    lambda: asyncio.run(create_freshdesk_ticket_async(body, headers))
                )
                return future.result()
        except RuntimeError:
            loop = get_thread_loop()
            return loop.run_until_complete(create_freshdesk_ticket_async(body, headers))

    except Exception as e:
        logging.error(f"Error in create_freshdesk_ticket_universal: {e}")
        import traceback

        logging.error(f"Traceback: {traceback.format_exc()}")
        return {
            "status_code": 500,
            "body": str(e),
        }


def create_freshdesk_conversation_universal(
    queryParams: List[Dict], body: Dict, headers
) -> Dict:
    """
    Universal Freshdesk conversation/note creation.
    Works with attachments from ANY source platform.
    """
    try:
        try:
            loop = asyncio.get_running_loop()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(
                    lambda: asyncio.run(
                        create_freshdesk_conversation_async(queryParams, body, headers)
                    )
                )
                return future.result()
        except RuntimeError:
            loop = get_thread_loop()
            return loop.run_until_complete(
                create_freshdesk_conversation_async(queryParams, body, headers)
            )

    except Exception as e:
        logging.error(f"Error in create_freshdesk_conversation_universal: {e}")
        import traceback

        logging.error(f"Traceback: {traceback.format_exc()}")
        return {
            "status_code": 500,
            "body": str(e),
        }


def create_freshdesk_reply_universal(
    queryParams: List[Dict], body: Dict, headers
) -> Dict:
    """
    Universal Freshdesk reply creation.
    Works with attachments from ANY source platform.
    """
    try:
        try:
            loop = asyncio.get_running_loop()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(
                    lambda: asyncio.run(
                        create_freshdesk_reply_async(queryParams, body, headers)
                    )
                )
                return future.result()
        except RuntimeError:
            loop = get_thread_loop()
            return loop.run_until_complete(
                create_freshdesk_reply_async(queryParams, body, headers)
            )

    except Exception as e:
        logging.error(f"Error in create_freshdesk_reply_universal: {e}")
        return {
            "status_code": 500,
            "body": str(e),
        }


# ===================================================================
# BACKWARD COMPATIBILITY FUNCTIONS
# ===================================================================


def create_freshdesk_ticket(**kwargs) -> Dict:
    """
    Backward compatible ticket creation function that now uses universal processing.
    Works with ANY source platform automatically.

    IMPORTANT: If calling from FastAPI, use create_freshdesk_ticket_async instead.
    """
    return create_freshdesk_ticket_universal(kwargs["body"], kwargs["targetHeaders"])


def create_freshdesk_conversation(**kwargs) -> Dict:
    """
    Backward compatible conversation creation function that now uses universal processing.
    Works with ANY source platform automatically.

    IMPORTANT: If calling from FastAPI, use create_freshdesk_conversation_async instead.
    """
    return create_freshdesk_conversation_universal(
        kwargs["queryParams"], kwargs["body"], kwargs["targetHeaders"]
    )


# def create_freshdesk_conversations(queryParams: List[Dict], body: Dict, headers) -> Dict:
#     """
#     Alias for create_freshdesk_conversation (handles both singular and plural)
#     """
#     return create_freshdesk_conversation_universal(queryParams, body, headers)

# def create_freshdesk_reply(queryParams: List[Dict], body: Dict, headers) -> Dict:
#     """
#     Create Freshdesk reply (public conversation).
#     Uses the universal processing automatically.
#     """
#     return create_freshdesk_reply_universal(queryParams, body, headers)


# ===================================================================
# LEGACY COMPATIBILITY FUNCTIONS
# ===================================================================


def upload_ticket_regular_attachments(
    ticket_id: int, attachments: List[Dict], headers: Dict
):
    """Legacy function for uploading regular attachments to existing tickets"""
    logging.warning(
        "upload_ticket_regular_attachments is deprecated. Use create_freshdesk_ticket with attachments instead."
    )


def upload_note_regular_attachments(
    conv_id: int, attachments: List[Dict], headers: Dict
):
    """Legacy function for uploading regular attachments to notes"""
    logging.warning(
        "upload_note_regular_attachments is deprecated. Use create_freshdesk_conversation with attachments instead."
    )


# ===================================================================
# VALIDATION FUNCTIONS
# ===================================================================


def validate_freshdesk_instance_v1(headers) -> Dict:
    """Validate Freshdesk instance and credentials"""
    try:
        headers_dict = convert_headers_to_dict(headers)
        config = ConnectorConfig(
            domainUrl=headers_dict.get("domainUrl"),
            api_key=headers_dict.get("apikey")
            or headers_dict.get("api_key")
            or headers_dict.get("username"),
        )
        connector = UniversalFreshdeskConnector(config)
        is_valid = connector._validate_instance()

        return {
            "status_code": 200 if is_valid else 401,
            "body": {"valid": is_valid},
            "headers": headers,
        }
    except Exception as e:
        logging.error(f"Error validating Freshdesk instance: {e}")
        return {"status_code": 500, "body": {"valid": False, "error": str(e)}}


def get_freshdesk_mapping_objects_v1(headers) -> Dict:
    """Get Freshdesk mapping objects (groups, users, etc.)"""
    try:
        headers_dict = convert_headers_to_dict(headers)
        domainUrl = headers_dict["domainUrl"]
        api_key = (
            headers_dict.get("apikey")
            or headers_dict.get("api_key")
            or headers_dict.get("username")
        )
        auth = (api_key, "X")

        config = ConnectorConfig(domainUrl=domainUrl, api_key=api_key)
        connector = UniversalFreshdeskConnector(config)

        # Get groups
        groups_url = f"https://{domainUrl}.freshdesk.com/api/v2/groups"
        groups_response = connector.rate_limiter.make_request_with_retry(
            groups_url, "GET", auth=auth
        )

        # Get agents
        agents_url = f"https://{domainUrl}.freshdesk.com/api/v2/agents"
        agents_response = connector.rate_limiter.make_request_with_retry(
            agents_url, "GET", auth=auth
        )

        # Get companies (Freshdesk has companies instead of departments)
        companies_url = f"https://{domainUrl}.freshdesk.com/api/v2/companies"
        companies_response = connector.rate_limiter.make_request_with_retry(
            companies_url, "GET", auth=auth
        )

        mapping_data = {}

        if groups_response.status_code == 200:
            mapping_data["groups"] = groups_response.json()

        if agents_response.status_code == 200:
            mapping_data["agents"] = agents_response.json()

        if companies_response.status_code == 200:
            mapping_data["companies"] = companies_response.json()

        return {"status_code": 200, "body": mapping_data, "headers": headers}

    except Exception as e:
        logging.error(f"Error getting Freshdesk mapping objects: {e}")
        return {"status_code": 500, "body": {"error": str(e)}}


# ===================================================================
# STATISTICS AND CLEANUP
# ===================================================================


def get_universal_attachment_statistics() -> Dict:
    """Get comprehensive attachment processing statistics"""
    global _universal_attachment_service
    if _universal_attachment_service:
        return _universal_attachment_service.get_statistics()
    return {
        "total_downloads": 0,
        "successful_downloads": 0,
        "failed_downloads": 0,
        "success_rate": 0,
        "total_bytes_downloaded": 0,
        "average_download_time": 0,
        "total_download_time": 0,
        "inline_images_processed": 0,
        "inline_images_extracted": 0,
    }


async def cleanup_universal_attachment_service():
    """Cleanup global attachment service - no longer needed"""
    logging.info("No global attachment service to clean up")
    pass


# ===================================================================
# EXAMPLE USAGE
# ===================================================================

if __name__ == "__main__":
    import json

    # Example Zendesk headers
    headers = [
        {"key": "domain", "value": "example", "description": "", "req": False},
        {
            "key": "apikey",
            "value": "your-freshdesk-api-key",
            "description": "",
            "req": False,
        },
        {
            "key": "username",
            "value": "john@example.com/token",
            "description": "",
            "req": True,
        },
        {
            "key": "password",
            "value": "zendesk-api-token",
            "description": "",
            "req": True,
        },
    ]

    # Example ticket data with attachments
    ticket_data = {
        "subject": "Test ticket with attachments",
        "description": "<div>Test ticket with inline images</div>",
        "email": "customer@example.com",
        "priority": 1,
        "status": 2,
        "attachments": [
            {
                "url": "https://example.zendesk.com/api/v2/attachments/12345.json",
                "id": 12345,
                "file_name": "test.jpg",
                "content_url": "https://example.zendesk.com/attachments/token/abc123/?name=test.jpg",
                "content_type": "image/jpeg",
            }
        ],
    }

    # Test ticket creation
    result = create_freshdesk_ticket(ticket_data, headers)
    print(json.dumps(result, indent=2))
