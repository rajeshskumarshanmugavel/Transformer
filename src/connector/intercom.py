"""
intercomv2.py
=========================

Complete product-agnostic Intercom connector that works with ANY source platform.
ALL source-specific logic is handled by UniversalAttachmentService.

Based on freshservicetv2.py architecture with Intercom-specific conversation handling,
contact management, and rich features from FreshChat migration script.
"""

import logging
import requests
import time
import re
import json
import base64
import asyncio
import concurrent.futures
import threading
import traceback
import redis
import os
from typing import List, Dict, Tuple, Optional, Union
from datetime import datetime
from dataclasses import dataclass
from collections import defaultdict
from .base_source_connector import convert_query_params_to_dict
from .base_target_connector import standardize_response_format

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


# ===================================================================
# GLOBAL VARIABLES
# ===================================================================

is_last_ticket_closed = {"id": None, "closed": False}

# Conversation-specific rate limiter for replies
# Intercom allows max 10 replies per conversation per 10 seconds
# and 100 total conversation replies per 10 seconds globally
conversation_reply_tracker = defaultdict(list)
global_reply_tracker = []
conversation_reply_lock = threading.Lock()

# ===================================================================
# IMPLEMENT REDIS CACHE 
# ===================================================================

# Module-level Redis client (shared across all instances)
_redis_client = None
_redis_available = False

def _get_redis_client():
    """Get or create Redis client (singleton pattern)"""
    global _redis_client, _redis_available
    
    if _redis_client is not None:
        return _redis_client
    
    try:
        redis_host = os.getenv('REDIS_HOST', 'localhost')
        redis_port = int(os.getenv('REDIS_PORT', 6379))
        redis_password = os.getenv('REDIS_PASSWORD')
        
        # Azure Redis Cache uses SSL on port 6380
        use_ssl = redis_port == 6380 or 'azure' in redis_host.lower() or 'windows.net' in redis_host.lower()
        
        logging.info(f"🔍 Connecting to Redis: host={redis_host}, port={redis_port}, ssl={use_ssl}")
        
        _redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            db=0,
            decode_responses=True,
            ssl=use_ssl,                    # Enable SSL for Azure
            ssl_cert_reqs=None,             # Don't verify SSL cert (Azure managed)
            socket_connect_timeout=10,
            socket_timeout=10,
            socket_keepalive=True,
            retry_on_timeout=True,
            health_check_interval=30
        )
        
        _redis_client.ping()  # Test connection
        _redis_available = True
        logging.info("✅ Redis cache connected successfully")
        return _redis_client
    except Exception as e:
        _redis_available = False
        logging.warning(f"⚠️ Redis not available, using instance-only cache: {e}")
        import traceback
        logging.debug(f"Redis connection traceback: {traceback.format_exc()}")
        return None


# ===================================================================
# CONVERSATION REPLY RATE LIMITING
# ===================================================================


def wait_for_conversation_rate_limit(conversation_id: str):
    """
    Ensure we don't exceed 1000 replies per conversation per 5-second window
    and 1000 total replies globally per 5-second window.
    This function will block if necessary to maintain rate limits.
    """
    with conversation_reply_lock:
        current_time = time.time()

        # Get the reply timestamps for this conversation
        reply_times = conversation_reply_tracker[conversation_id]

        # Remove timestamps older than 5 seconds
        reply_times[:] = [t for t in reply_times if current_time - t < 5]
        global_reply_tracker[:] = [t for t in global_reply_tracker if current_time - t < 5]

        # Check per-conversation limit (1000 per 5 seconds)
        if len(reply_times) >= 1000:
            oldest_in_window = min(reply_times)
            wait_time = 5 - (current_time - oldest_in_window)
            if wait_time > 0:
                logging.info(
                    f"Conversation {conversation_id} rate limit reached. Waiting {wait_time:.2f} seconds..."
                )
                time.sleep(wait_time)
                current_time = time.time()
                reply_times[:] = [t for t in reply_times if current_time - t < 5]
                global_reply_tracker[:] = [t for t in global_reply_tracker if current_time - t < 5]

        # Check global limit (1000 per 5 seconds)
        if len(global_reply_tracker) >= 1000:
            oldest_global = min(global_reply_tracker)
            wait_time = 5 - (current_time - oldest_global)
            if wait_time > 0:
                logging.info(
                    f"Global rate limit reached (1000 replies/5s). Waiting {wait_time:.2f} seconds..."
                )
                time.sleep(wait_time)
                current_time = time.time()
                reply_times[:] = [t for t in reply_times if current_time - t < 5]
                global_reply_tracker[:] = [t for t in global_reply_tracker if current_time - t < 5]

        # Record this reply in both trackers
        reply_times.append(current_time)
        global_reply_tracker.append(current_time)

        logging.debug(
            f"Conversation {conversation_id} now has {len(reply_times)} replies in the last 5 seconds (global: {len(global_reply_tracker)})"
        )

def cleanup_old_conversation_trackers():
    """
    Periodically clean up old conversation trackers to prevent memory leaks.
    Should be called periodically or when memory usage is a concern.
    """
    with conversation_reply_lock:
        current_time = time.time()
        conversations_to_remove = []

        for conversation_id, reply_times in conversation_reply_tracker.items():
            # Remove timestamps older than 5 seconds
            reply_times[:] = [t for t in reply_times if current_time - t < 5]

            # If no recent replies, mark for removal
            if not reply_times:
                conversations_to_remove.append(conversation_id)

        # Remove empty trackers
        for conversation_id in conversations_to_remove:
            del conversation_reply_tracker[conversation_id]

        # Clean up global tracker too
        global_reply_tracker[:] = [t for t in global_reply_tracker if current_time - t < 5]

        if conversations_to_remove:
            logging.debug(
                f"Cleaned up {len(conversations_to_remove)} old conversation trackers"
            )

# ===================================================================
# UTILITY FUNCTIONS
# ===================================================================


def convert_to_epoch(timestamp: Union[str, int]) -> int:
    """
    Convert UTC/ISO8601 timestamp to epoch.

    Args:
        timestamp: Timestamp string in format "2024-11-22T08:24:25+00:00" or epoch int

    Returns:
        Epoch timestamp as integer
    """
    if isinstance(timestamp, int):
        return timestamp

    if not isinstance(timestamp, str):
        logging.error(f"Invalid timestamp type: {type(timestamp)}")
        return int(time.time())

    try:
        # Handle ISO 8601 format with timezone
        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except ValueError:
        try:
            # Try alternative format without timezone
            dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")
            return int(dt.timestamp())
        except ValueError:
            logging.error(f"Invalid timestamp format: {timestamp}")
            return int(time.time())


# ===================================================================
# INTERCOM-SPECIFIC DATA STRUCTURES
# ===================================================================


@dataclass
class TokenBucket:
    """Rate limiter implementation for Intercom API calls"""

    capacity: int
    fill_rate: float
    tokens: float = 0.0
    last_update: float = time.time()
    lock: threading.Lock = threading.Lock()

    def consume(self, tokens: int = 1) -> bool:
        with self.lock:
            now = time.time()
            # Add new tokens based on time elapsed
            self.tokens = min(
                self.capacity, self.tokens + (now - self.last_update) * self.fill_rate
            )
            self.last_update = now

            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False

    def wait_for_token(self, tokens: int = 1):
        while not self.consume(tokens):
            time.sleep(0.1)


@dataclass
class IntercomMessage:
    """Represents a single message in an Intercom conversation"""

    message_id: Optional[str]
    conversation_id: Optional[str]
    message_type: str  # 'user', 'agent', 'bot', 'system'
    actor_email: str
    actor_name: Optional[str]
    body: str
    created_at: int
    attachments: List[Dict] = None
    is_private: bool = False
    admin_id: Optional[str] = None


# ===================================================================
# UNIVERSAL INLINE PROCESSOR FOR INTERCOM
# ===================================================================


class UniversalInlineProcessor(BaseInlineImageProcessor):
    """
    Product-agnostic inline image processor for Intercom.
    ALL source-specific logic is delegated to UniversalAttachmentService.
    """

    def __init__(self, attachment_service: UniversalAttachmentService):
        """Initialize with attachment service"""
        self.attachment_service = attachment_service

    def get_source_patterns(self) -> List[Dict]:
        """Legacy method - no longer used"""
        return []

    def extract_inline_images(self, description: str) -> Tuple[str, List[InlineImage]]:
        """Legacy method - no longer used"""
        return description, []

    def _get_source_url(self, image: InlineImage, headers: Dict) -> Optional[str]:
        """Legacy method - no longer used but required by abstract base class"""
        return None

    def _fetch_attachment_content(self, url: str, headers: Dict) -> Optional[bytes]:
        """Legacy method - no longer used but required by abstract base class"""
        return None

    async def process_inline_images_universal(
        self,
        content: str,
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
            return content, []

        try:
            result = await self.attachment_service.process_inline_images_universal(
                content, source_domain, auth_config
            )

            processed_attachments = []
            for attachment in result.extracted_attachments:
                # Convert to Intercom attachment format
                processed_attachments.append(
                    {
                        "type": "file",
                        "name": attachment.name,
                        "data": base64.b64encode(attachment.content).decode("utf-8"),
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
            return content, []


# ===================================================================
# INTERCOM RATE LIMIT HANDLER
# ===================================================================


class IntercomRateLimitHandler(BaseRateLimitHandler):
    """Intercom-specific rate limit handler with token bucket integration"""

    def __init__(self):
        # Dom - 8/24 Add session for connection reuse - USE THIS ONLY IF ONE MIGRATION IS RUNNING AT A TIME
        self.session = requests.Session()
        # Intercom allows 1000 requests per minute
        self.rate_limiter = TokenBucket(
            capacity=1000, fill_rate=1000 / 60  # Tokens per second
        )

    def is_rate_limited(self, response: requests.Response) -> bool:
        """Check if response indicates rate limiting"""
        if response.status_code == 429:
            return True
        else:
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

        reset_time = response.headers.get("X-RateLimit-Reset")
        if reset_time:
            try:
                reset_timestamp = int(reset_time)
                current_timestamp = int(time.time())
                return max(0, reset_timestamp - current_timestamp)
            except ValueError:
                pass

        return 60

    def make_request_with_retry(self, url: str, method: str, **kwargs):
        """Make rate-limited request with automatic retry"""
        self.rate_limiter.wait_for_token()

        try:

            # response = requests.request(method, url, **kwargs)
            # Dom - 8/24 USE BELOW IF ONLY ONE MIGRATION IS RUNNING AT A TIME, IF NOT USE ABOVE LINE
            response = self.session.request(method, url, **kwargs)

            if self.is_rate_limited(response):
                delay = self.get_retry_delay(response)
                logging.warning(f"Rate limit exceeded, waiting {delay} seconds...")
                time.sleep(delay)
                return self.make_request_with_retry(url, method, **kwargs)

            response.raise_for_status()
            return response

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                delay = self.get_retry_delay(e.response)
                logging.warning(f"Rate limit exceeded, waiting {delay} seconds...")
                time.sleep(delay)
                return self.make_request_with_retry(url, method, **kwargs)
            else:
                logging.error(f"HTTP error {e.response.status_code}: {e.response.text}")
                return e.response
        except requests.exceptions.RequestException as e:
            logging.error(f"Request error: {str(e)}")
            return e.response
            # raise


# ===================================================================
# MAIN UNIVERSAL INTERCOM CONNECTOR
# ===================================================================


class UniversalIntercomConnector(BaseTargetConnector):
    """
    Completely product-agnostic Intercom connector.
    Works with ANY source platform without modification.
    Includes rich conversation features from FreshChat migration script.
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

        # Dom - 8/24 Cache admin ID to avoid repeated lookups
        self._admin_cache = {}
        self._admin_cache_expiry = 0
        self._cache_duration = 300  # 5 minutes

        # Conversation rate limit cleanup tracking
        self._last_cleanup = time.time()
        self._cleanup_interval = 60  # Clean up every 60 seconds

        super().__init__(config)
        self.attachment_service = attachment_service
        self._external_attachment_service = attachment_service is not None

        # Intercom-specific configuration
        self.base_url = "https://api.intercom.io"
        self.api_version = "2.12"

        # Contact cache to avoid duplicate lookups
        self.contact_cache = {}

        # Default admin/team IDs (should be configurable)
        # self.default_admin_id = (
        #     config.admin_id
        #     if (hasattr(config, "admin_id") or hasattr(config, "admin_assignee_id"))
        #     else None
        # )
        self.default_team_id = config.team_id if hasattr(config, "team_id") else None
        # self.bot_admin_id = (
        #     config.bot_admin_id
        #     if hasattr(config, "bot_admin_id")
        #     else self.default_admin_id
        # )

        # Always create inline processor
        self.inline_processor = UniversalInlineProcessor(self.attachment_service)
        
        # Dom - 11/16: Extract migration_id from config for Redis cache scoping
        import hashlib
        if hasattr(config, 'api_key') and config.api_key:
            self.migration_id = hashlib.md5(config.api_key.encode()).hexdigest()[:16]
            logging.info(f"Cache scope (api_key hash): {self.migration_id}")
        else:
            self.migration_id = None
            logging.warning("No api_key available - Redis cache will NOT be scoped!")


    @property
    def safe_attachment_service(self) -> Optional[UniversalAttachmentService]:
        """Safe property to get attachment service"""
        return getattr(self, "attachment_service", None)

    def _get_inline_processor(self) -> BaseInlineImageProcessor:
        """Get universal inline processor"""
        return UniversalInlineProcessor(self.safe_attachment_service)

    def _get_rate_limiter(self) -> BaseRateLimitHandler:
        return IntercomRateLimitHandler()

    def _get_headers(self) -> Dict:
        """Get standard Intercom API headers"""
        return {
            "Intercom-Version": self.api_version,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {self.config.api_key}",
        }

    def _validate_instance(self) -> bool:
        """Validate Intercom instance/credentials"""
        try:
            url = f"{self.base_url}/contacts"
            response = requests.get(url, headers=self._get_headers(), timeout=10)
            return response.status_code != 401
        except:
            return False

    def _cleanup_conversation_trackers_if_needed(self):
        """Periodically clean up old conversation rate limit trackers"""
        current_time = time.time()
        if current_time - self._last_cleanup > self._cleanup_interval:
            cleanup_old_conversation_trackers()
            self._last_cleanup = current_time

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

    # def _verify_conversation_exists(self, conversation_id: str) -> bool:
    #     """
    #     Verify if a conversation exists in Intercom before trying to add replies.
    #     Returns True if conversation exists, False otherwise.
    #     """
    #     try:
    #         url = f"{self.base_url}/conversations/{conversation_id}"
    #         response = requests.get(url, headers=self._get_headers(), timeout=10)

    #         if response.status_code == 200:
    #             logging.debug(f"Conversation {conversation_id} exists")
    #             return True
    #         elif response.status_code == 404:
    #             logging.error(f"Conversation {conversation_id} does not exist (404)")
    #             return False
    #         else:
    #             logging.warning(
    #                 f"Unexpected response when checking conversation {conversation_id}: {response.status_code}"
    #             )
    #             return False  # Assume doesn't exist if we can't verify

    #     except Exception as e:
    #         logging.error(f"Error verifying conversation {conversation_id}: {e}")
    #         return False  # Assume doesn't exist on error

    def _get_admin_id_by_email(self, email: str) -> Optional[str]:
        """
        Get the admin ID for a user by email using /admins endpoint
        Now with Redis caching scoped to migration_id (including negative results)
        """
        try:
            # Only use Redis cache if migration_id is available (to prevent contamination)
            redis_client = None
            redis_key = None
            
            if self.migration_id:
                redis_client = _get_redis_client()
                redis_key = f"intercom:admin:{self.migration_id}:{email}"
                
                # Try Redis cache first (shared across all threads/processes in this migration)
                if redis_client and _redis_available:
                    try:
                        cached_admin_id = redis_client.get(redis_key)
                        if cached_admin_id:
                            if cached_admin_id == "NOT_FOUND":
                                logging.debug(f"✅ Redis cache hit: admin {email} not found (cached negative)")
                                return None
                            else:
                                logging.debug(f"✅ Redis cache hit for admin email: {email}")
                                return cached_admin_id
                    except Exception as e:
                        logging.warning(f"Redis read error for {email}: {e}")
            
            # Fall back to instance cache
            current_time = time.time()
            if (
                hasattr(self, "_admin_cache_expiry")
                and current_time < self._admin_cache_expiry
                and email in self._admin_cache
            ):
                logging.debug(f"Instance cache hit for admin email: {email}")
                return self._admin_cache[email]

            logging.debug(f"Cache miss for admin email: {email}, fetching all admins")
            url = f"{self.base_url}/admins"
            response = self.rate_limiter.make_request_with_retry(
                url, "GET", headers=self._get_headers()
            )

            if response.status_code == 200:
                admins_json = response.json()
                admins_list = admins_json.get("admins", [])

                # Initialize instance cache
                if not hasattr(self, "_admin_cache"):
                    self._admin_cache = {}

                self._admin_cache_expiry = current_time + self._cache_duration
                
                # Cache ALL admins at once in both instance cache AND Redis
                for admin in admins_list:
                    admin_email = admin.get("email")
                    admin_id = admin.get("id")
                    if admin_email and admin_id:
                        # Instance cache
                        self._admin_cache[admin_email] = admin_id
                        
                        # Redis cache (1 hour TTL, scoped to migration_id)
                        if self.migration_id and redis_client and _redis_available:
                            try:
                                redis_client.setex(
                                    f"intercom:admin:{self.migration_id}:{admin_email}",
                                    3600,  # 1 hour
                                    admin_id
                                )
                            except Exception as e:
                                logging.warning(f"Redis write error for {admin_email}: {e}")

                # Now return the requested admin ID from cache
                admin_id = self._admin_cache.get(email)
                if admin_id:
                    logging.info(f"Retrieved admin ID for {email}: {admin_id}")
                    return admin_id
                else:
                    logging.error(f"No admin found with email: {email}")
                    
                    # Cache the negative result to avoid repeated API calls
                    if self.migration_id and redis_client and _redis_available:
                        try:
                            redis_client.setex(
                                f"intercom:admin:{self.migration_id}:{email}",
                                3600,  # 1 hour
                                "NOT_FOUND"
                            )
                            logging.debug(f"Cached negative result for admin: {email}")
                        except Exception as e:
                            logging.warning(f"Redis write error for missing admin {email}: {e}")
            else:
                logging.error(
                    f"Failed to get admins list: {response.status_code} - {response.text}"
                )
        except Exception as e:
            logging.error(f"Error getting admin ID for {email}: {e}")

        return None

    def _get_current_admin_id(self) -> Optional[str]:
        """
        Get the admin ID for the current API key user using /me endpoint
        With Redis caching scoped to migration_id
        """
        try:
            # Only use Redis cache if migration_id is available
            redis_client = None
            redis_key = None
            
            if self.migration_id:
                redis_client = _get_redis_client()
                redis_key = f"intercom:current_admin:{self.migration_id}"
                
                # Try Redis cache first
                if redis_client and _redis_available:
                    try:
                        cached_admin_id = redis_client.get(redis_key)
                        if cached_admin_id:
                            logging.debug(f"✅ Redis cache hit for current admin ID")
                            return cached_admin_id
                    except Exception as e:
                        logging.warning(f"Redis read error for current admin: {e}")
            
            url = f"{self.base_url}/me"
            response = self.rate_limiter.make_request_with_retry(
                url, "GET", headers=self._get_headers()
            )

            if response.status_code == 200:
                user_data = response.json()
                admin_id = user_data.get("id")
                if admin_id:
                    logging.info(f"Retrieved current admin ID: {admin_id}")
                    
                    # Cache in Redis (scoped to migration_id)
                    if self.migration_id and redis_client and _redis_available:
                        try:
                            redis_client.setex(
                                redis_key,
                                3600,  # 1 hour
                                admin_id
                            )
                        except Exception as e:
                            logging.warning(f"Redis write error for current admin: {e}")
                    
                    return admin_id
                else:
                    logging.error("No admin ID found in /me response")
            else:
                logging.error(
                    f"Failed to get current admin ID: {response.status_code} - {response.text}"
                )
        except Exception as e:
            logging.error(f"Error getting current admin ID: {e}")

        return None

    def _get_or_fetch_admin_id(self) -> Optional[str]:
        """
        Get admin ID from config or fetch from API if not available
        """
        # if self.default_admin_id:
        #     return self.default_admin_id

        # Try to get admin ID from API
        admin_id = self._get_current_admin_id()
        if admin_id:
            # Cache it for future use
            # self.default_admin_id = admin_id
            return admin_id

        logging.error("Could not determine admin ID from config or API")
        return None

    # ===================================================================
    # CONTACT MANAGEMENT (FROM FRESHCHAT MIGRATION SCRIPT)
    # ===================================================================

    def search_contact(self, email: str) -> Optional[Dict]:
        """Search for contact by email with Redis caching scoped to migration_id"""
        if not email or str(email).lower() in ["nan", "none", ""]:
            logging.error(f"Invalid email value for contact search: {repr(email)}")
            return None

        email = str(email).strip()
        if not email:
            logging.error(f"Empty email after cleaning: {repr(email)}")
            return None

        # Only use Redis cache if migration_id is available
        redis_client = None
        redis_key = None
        
        if self.migration_id:
            redis_client = _get_redis_client()
            redis_key = f"intercom:contact:{self.migration_id}:{email}"
            
            # Try Redis cache first
            if redis_client and _redis_available:
                try:
                    cached_contact = redis_client.get(redis_key)
                    if cached_contact:
                        logging.debug(f"✅ Redis cache hit for contact: {email}")
                        return json.loads(cached_contact)
                except Exception as e:
                    logging.warning(f"Redis read error for contact {email}: {e}")

        logging.debug(f"Searching for contact with email: {email}")

        url = f"{self.base_url}/contacts/search"
        payload = {
            "query": {"field": "email", "operator": "~", "value": email},
            "pagination": {"per_page": 1},
        }

        try:
            response = self.rate_limiter.make_request_with_retry(
                url, "POST", headers=self._get_headers(), json=payload
            )
            result = response.json()

            if result.get("data") and len(result["data"]) > 0:
                contact = result["data"][0]
                logging.debug(f"Found existing contact for email: {email}")
                
                # Cache in Redis (scoped to migration_id)
                if self.migration_id and redis_client and _redis_available:
                    try:
                        redis_client.setex(
                            redis_key,
                            3600,  # 1 hour
                            json.dumps(contact)
                        )
                    except Exception as e:
                        logging.warning(f"Redis write error for contact {email}: {e}")
                
                return contact
                
            logging.debug(f"No existing contact found for email: {email}")
            return None

        except Exception as e:
            logging.error(f"Error searching for contact {email}: {str(e)}")
            return None

    def create_contact(self, email: str, name: str = None) -> Optional[Dict]:
        """Create a new contact and cache in Redis"""
        url = f"{self.base_url}/contacts"
        payload = {"email": email, "role": "lead"}

        if name:
            payload["name"] = name

        try:
            response = self.rate_limiter.make_request_with_retry(
                url, "POST", headers=self._get_headers(), json=payload
            )
            contact = response.json()
            
            # Cache the newly created contact in Redis (scoped to migration_id)
            if self.migration_id:
                redis_client = _get_redis_client()
                if redis_client and _redis_available and contact:
                    try:
                        redis_key = f"intercom:contact:{self.migration_id}:{email}"
                        redis_client.setex(
                            redis_key,
                            3600,  # 1 hour
                            json.dumps(contact)
                        )
                    except Exception as e:
                        logging.warning(f"Redis write error for new contact {email}: {e}")
            
            return contact
            
        except Exception as e:
            # Handle conflict (contact already exists)
            if hasattr(e, "response") and e.response.status_code == 409:
                try:
                    error_data = e.response.json()
                    if error_data.get("errors"):
                        # Extract contact ID from error message
                        match = re.search(
                            r"id=([a-f0-9]+)",
                            error_data["errors"][0].get("message", ""),
                        )
                        if match:
                            contact_id = match.group(1)
                            logging.info(
                                f"Contact already exists with ID {contact_id}, fetching details"
                            )
                            get_response = self.rate_limiter.make_request_with_retry(
                                f"{self.base_url}/contacts/{contact_id}",
                                "GET",
                                headers=self._get_headers(),
                            )
                            contact = get_response.json()
                            
                            # Cache the fetched contact in Redis
                            if self.migration_id:
                                redis_client = _get_redis_client()
                                if redis_client and _redis_available and contact:
                                    try:
                                        redis_key = f"intercom:contact:{self.migration_id}:{email}"
                                        redis_client.setex(
                                            redis_key,
                                            3600,
                                            json.dumps(contact)
                                        )
                                    except Exception as e2:
                                        logging.warning(f"Redis write error: {e2}")
                            
                            return contact
                except Exception as e2:
                    logging.error(
                        f"Error processing conflict response for {email}: {str(e2)}"
                    )

            logging.error(f"Error creating contact {email}: {str(e)}")
            return None

    def get_or_create_contact(self, email: str, name: str = None) -> Optional[Dict]:
        """Get existing contact or create new one (with Redis caching scoped to migration_id)"""
        if not email or str(email).lower() in ["nan", "none", ""]:
            logging.error(
                f"Invalid email value in get_or_create_contact: {repr(email)}"
            )
            return None

        email = str(email).strip()
        if not email:
            logging.error(f"Empty email after cleaning: {repr(email)}")
            return None

        # Only use Redis cache if migration_id is available
        if self.migration_id:
            redis_client = _get_redis_client()
            redis_key = f"intercom:contact:{self.migration_id}:{email}"
            
            # Try Redis cache first (shared across all threads/processes in this migration)
            if redis_client and _redis_available:
                try:
                    cached_contact = redis_client.get(redis_key)
                    if cached_contact:
                        logging.debug(f"✅ Redis cache hit for contact: {email}")
                        return json.loads(cached_contact)
                except Exception as e:
                    logging.warning(f"Redis read error for contact {email}: {e}")
        
        # Check instance cache
        if email in self.contact_cache:
            logging.debug(f"Instance cache hit for contact: {email}")
            return self.contact_cache[email]

        # Search for existing contact (this method now handles Redis caching internally)
        contact = self.search_contact(email)
        if contact:
            logging.debug(f"Found existing contact: {email}")
            # Cache in instance cache too
            self.contact_cache[email] = contact
            return contact

        # Create new contact (this method now handles Redis caching internally)
        logging.info(f"Creating new contact: {email}")
        contact = self.create_contact(email, name)
        if contact:
            # Cache in instance cache too
            self.contact_cache[email] = contact

        return contact

    # ===================================================================
    # ATTACHMENT PROCESSING
    # ===================================================================

    async def _process_attachments_universal(
        self, attachments: List[Dict], auth_config: Optional[Dict] = None
    ) -> Tuple[List[Dict], List[str]]:
        """
        Universal attachment processing using AttachmentService.
        Converts to Intercom format.
        """
        if not attachments:
            logging.info("No attachments to process")
            return [], []

        logging.info(f"Processing {len(attachments)} attachments")

        if not self.safe_attachment_service:
            logging.error("No AttachmentService available for processing attachments")
            return [], [att.get("name", "unknown") for att in attachments]

        try:
            # Process all attachments
            results = await self.safe_attachment_service.download_attachments_batch(
                attachments,
                auth_config,
            )

            # Convert results to Intercom format
            successful_files = []
            failed_files = []

            for i, result in enumerate(results):
                att_name = (
                    attachments[i].get("name")
                    or attachments[i].get("file_name")
                    or attachments[i].get("content_file_name")
                    or "unknown"
                )

                # Get original content type from source (Freshdesk uses content_content_type)
                original_content_type = (
                    attachments[i].get("content_type")
                    or attachments[i].get("content_content_type")
                    or None
                )

                if result:
                    logging.info(
                        f"Successfully downloaded: {att_name} ({len(result.content)} bytes)"
                    )
                    # Use detected content type, but prefer original if it's more specific
                    final_content_type = result.content_type

                    # If original has a specific type and detected is generic, use original
                    if (
                        original_content_type
                        and original_content_type != "application/octet-stream"
                    ):
                        if result.content_type in ["application/octet-stream", None]:
                            final_content_type = original_content_type
                            logging.debug(
                                f"Using original content_type '{original_content_type}' for {att_name}"
                            )
                        elif original_content_type != result.content_type:
                            logging.debug(
                                f"Content type mismatch for {att_name}: original='{original_content_type}' vs detected='{result.content_type}' - using detected"
                            )

                    # Convert to Intercom attachment format
                    successful_files.append(
                        {
                            "type": "file",
                            "name": att_name,
                            "data": base64.b64encode(result.content).decode("utf-8"),
                            "content_type": final_content_type,
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
            logging.error(f"Traceback: {traceback.format_exc()}")
            return [], [att.get("name", "unknown") for att in attachments]

    # ===================================================================
    # TICKET MANAGEMENT (NEW FLOW)
    # ===================================================================

    def _get_ticket_headers(self) -> Dict:
        """Get Intercom API headers specifically for ticket operations"""
        return {
            "Intercom-Version": self.api_version,  # Use 2.12 as requested
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {self.config.api_key}",
        }

    def _create_ticket_api_call_internal(
        self,
        contact_id: str,
        ticket_type_id: int,
        ticket_attributes: Dict,
        custom_attributes: Dict = None,
        admin_id: str = None,
        team_id: str = None,
        ticket_state_id: int = None,
    ) -> requests.Response:
        """
        Create a new Intercom ticket with optional initial assignment

        Args:
            contact_id: Intercom contact ID
            ticket_type_id: The ticket type ID
            ticket_attributes: Ticket attributes with _default_title_ and _default_description_
            custom_attributes: Optional custom attributes to include
            admin_id: Optional admin ID for initial assignment
            team_id: Optional team ID for initial assignment
        """
        url = f"{self.base_url}/tickets"
        payload = {
            "ticket_type_id": ticket_type_id,
            "contacts": [{"id": contact_id}],
            "ticket_attributes": ticket_attributes,
            "skip_notifications": True,
        }

        # Add custom attributes if provided
        if custom_attributes:
            # Filter out keys where value is None (null)
            filtered_custom_attributes = {
                k: v for k, v in custom_attributes.items() if v is not None
            }
            if filtered_custom_attributes:
                if "ticket_attributes" in payload and isinstance(
                    payload["ticket_attributes"], dict
                ):
                    payload["ticket_attributes"].update(filtered_custom_attributes)
                else:
                    payload["ticket_attributes"] = filtered_custom_attributes

        # Add initial assignment with enhanced team membership validation
        if admin_id or team_id:
            assignment = {}

            if team_id:
                # Enhanced team validation and assignment strategy
                try:
                    # print(f" CONSOLE DEBUG: [VALIDATE] Validating team {team_id} membership...")
                    team_response = self.rate_limiter.make_request_with_retry(
                        f"{self.base_url}/teams/{team_id}",
                        "GET",
                        headers=self._get_ticket_headers(),
                    )

                    if team_response.status_code == 200:
                        team_data = team_response.json()
                        team_admin_ids = team_data.get("admin_ids", [])
                        team_name = team_data.get("name", "Unknown")

                        # print(f" CONSOLE DEBUG: [INFO] Team '{team_name}' has {len(team_admin_ids)} members: {team_admin_ids}")

                        # Strategy 1: Admin is in team - use both admin and team
                        if admin_id and (
                            int(admin_id) in team_admin_ids
                            or str(admin_id) in team_admin_ids
                        ):
                            assignment["admin_assignee_id"] = int(admin_id)
                            assignment["team_assignee_id"] = int(team_id)
                            # print(f" CONSOLE DEBUG: [STRATEGY 1] Admin {admin_id} is in team {team_id} - using both")
                            logging.info(
                                f"Using Strategy 1: Admin {admin_id} is team member"
                            )

                        # # Strategy 2: Admin not in team - use first team member as assignee
                        # elif team_admin_ids:
                        #     team_member_id = team_admin_ids[
                        #         0
                        #     ]  # Use first available team member
                        #     assignment["admin_assignee_id"] = int(team_member_id)
                        #     assignment["team_assignee_id"] = int(team_id)
                        #     # print(f" CONSOLE DEBUG: [STRATEGY 2] Admin {admin_id} not in team, using team member {team_member_id} as assignee")
                        #     logging.info(
                        #         f"Using Strategy 2: Admin {admin_id} not in team, using team member {team_member_id}"
                        #     )

                        # Strategy 3: Fallback team-only assignment
                        else:
                            assignment["team_assignee_id"] = int(team_id)
                            # print(f" CONSOLE DEBUG: [STRATEGY 3] Team {team_id} has no members, team-only assignment")
                            logging.warning(
                                f"Using Strategy 3: Team {team_id} has no members"
                            )
                    else:
                        # print(f" CONSOLE DEBUG: [ERROR] Could not validate team {team_id}, using fallback assignment")
                        # Can't get team info - try team-only assignment
                        assignment["team_assignee_id"] = int(team_id)
                        logging.error(
                            f"Team validation failed for {team_id}, using fallback"
                        )

                except Exception as e:
                    # print(f" CONSOLE DEBUG: [ERROR] Error checking team membership: {e}")
                    logging.error(f"Error validating team membership: {e}")
                    # Fallback to team-only assignment
                    assignment["team_assignee_id"] = int(team_id)
                    # print(f" CONSOLE DEBUG: [FALLBACK] Exception occurred, using team-only assignment for team {team_id}")

            elif admin_id:
                # Admin-only assignment
                assignment["admin_assignee_id"] = int(admin_id)
                # print(f" CONSOLE DEBUG: Admin-only assignment: {admin_id}")
                logging.info(f"Admin-only assignment: {admin_id}")

            if assignment:
                payload["assignment"] = assignment
                logging.info(f"Creating ticket with validated assignment: {assignment}")
                # print(f" CONSOLE DEBUG: Final validated assignment: {assignment}")
            else:
                # print(f" CONSOLE DEBUG: [WARNING] No valid assignment could be created")
                logging.warning("No valid assignment could be created")
        # #print(f" CONSOLE DEBUG:  Final ticket creation payload: {payload}")
        # logging.info(f" Final ticket creation payload: {payload}")

        # # Directly place assignee IDs at root level (removed complex assignment logic)
        # if admin_id is not None:
        #     try:
        #         payload["admin_assignee_id"] = int(admin_id)
        #     except Exception:
        #         payload["admin_assignee_id"] = admin_id
        # if team_id is not None:
        #     try:
        #         payload["team_assignee_id"] = int(team_id)
        #     except Exception:
        #         payload["team_assignee_id"] = team_id

        # self.write_json_to_file(payload, "create_payload_ic.json")

        create_ticket_header = self._get_ticket_headers()
        create_ticket_header["Intercom-Version"] = "2.14"

        response = self.rate_limiter.make_request_with_retry(
            url, "POST", headers=create_ticket_header, json=payload
        )

        # print(f" CONSOLE DEBUG:  Response status: {response.status_code}")
        # print(f" CONSOLE DEBUG:  Response body: {response.text[:500]}...")
        logging.info(f" Ticket creation response: Status {response.status_code}")

        ticket_creation_response = response.json()
        created_ticket_id = ticket_creation_response.get("id")
        current_state_id = (
            ticket_creation_response.get("ticket_state").get("id")
            if ticket_creation_response.get("ticket_state")
            else None
        )

        if (
            created_ticket_id
            and ticket_state_id
            and str(current_state_id) != str(ticket_state_id)
        ):
            logging.info(
                f"Created ticket ID: {created_ticket_id}, current state: {current_state_id}, target state: {ticket_state_id}"
            )
            update_ticket_url = f"{self.base_url}/tickets/{created_ticket_id}"

            update_payload = {"ticket_state_id": ticket_state_id}

            update_ticket_response = self.rate_limiter.make_request_with_retry(
                update_ticket_url,
                "PUT",
                headers=self._get_ticket_headers(),
                json=update_payload,
            )

            if update_ticket_response.status_code in [200, 201]:
                logging.info(
                    f"Updated ticket {created_ticket_id} state from {current_state_id} to {ticket_state_id}"
                )
            else:
                logging.error(
                    f"Failed to update ticket {created_ticket_id}: {update_ticket_response.text}"
                )
        elif (
            created_ticket_id
            and ticket_state_id
            and str(current_state_id) == str(ticket_state_id)
        ):
            logging.info(
                f"Ticket {created_ticket_id} already in desired state {ticket_state_id}, skipping update"
            )
        else:
            logging.info(f"Ticket creation failed: {response.text}")

        return response

    # def write_json_to_file(self, data: dict, filename: str, indent: int = 4) -> bool:
    #     """
    #     Writes a dictionary to a local JSON file.

    #     Args:
    #         data (dict): The data to write as JSON.
    #         filename (str): The path to the file (e.g., 'output.json').
    #         indent (int): Indentation for pretty-printing (default 4).

    #     Returns:
    #         bool: True if successful, False otherwise.
    #     """
    #     try:
    #         with open(filename, 'w', encoding='utf-8') as f:
    #             json.dump(data, f, indent=indent, ensure_ascii=False)
    #         return True
    #     except Exception as e:
    #         print(f"Error writing to file {filename}: {e}")
    #         return False

    def _assign_ticket(
        self,
        ticket_id: str,
        admin_id: str = None,
        team_id: str = None,
        ticket_attributes: Dict = None,
    ):
        """
        Assign ticket to admin or team via PUT request (enhanced fallback method)

        Args:
            ticket_id: The ticket ID to assign
            admin_id: Admin ID to assign to
            team_id: Team ID to assign to
            ticket_attributes: Optional ticket attributes to update
        """
        url = f"{self.base_url}/tickets/{ticket_id}"

        payload = {}

        # Add ticket attributes if provided
        if ticket_attributes:
            payload["ticket_attributes"] = ticket_attributes

        # Enhanced assignment strategy for fallback
        if admin_id or team_id:
            assignment = {}

            if team_id:
                # Apply the same enhanced team assignment strategy
                try:
                    # print(f" CONSOLE DEBUG: [FALLBACK] Validating team {team_id} for enhanced assignment...")
                    team_response = self.rate_limiter.make_request_with_retry(
                        f"{self.base_url}/teams/{team_id}",
                        "GET",
                        headers=self._get_ticket_headers(),
                    )

                    if team_response.status_code == 200:
                        team_data = team_response.json()
                        team_admin_ids = team_data.get("admin_ids", [])
                        team_name = team_data.get("name", "Unknown")

                        # print(f" CONSOLE DEBUG: [FALLBACK] Team '{team_name}' members: {team_admin_ids}")

                        # Strategy 1: Admin is in team
                        if admin_id and (
                            int(admin_id) in team_admin_ids
                            or str(admin_id) in team_admin_ids
                        ):
                            assignment["assignee_id"] = int(admin_id)
                            assignment["team_id"] = int(team_id)
                            # print(f" CONSOLE DEBUG: [FALLBACK STRATEGY 1] Using admin {admin_id} (team member)")

                        # Strategy 2: Use team member as assignee
                        elif team_admin_ids:
                            team_member_id = team_admin_ids[0]
                            assignment["assignee_id"] = int(team_member_id)
                            assignment["team_id"] = int(team_id)
                            # print(f" CONSOLE DEBUG: [FALLBACK STRATEGY 2] Using team member {team_member_id} as assignee")

                        # Strategy 3: Team-only fallback
                        else:
                            assignment["team_id"] = int(team_id)
                            # print(f" CONSOLE DEBUG: [FALLBACK STRATEGY 3] Team-only assignment")
                    else:
                        # Team validation failed - use simple team assignment
                        assignment["team_id"] = int(team_id)
                        # print(f" CONSOLE DEBUG: [FALLBACK] Team validation failed, using simple assignment")

                except Exception as e:
                    # print(f" CONSOLE DEBUG: [FALLBACK ERROR] {e}")
                    assignment["team_id"] = int(team_id)

            elif admin_id:
                assignment["assignee_id"] = int(admin_id)

            payload["assignment"] = assignment

        # Set ticket as open
        payload["open"] = True

        try:
            logging.info(
                f"Enhanced fallback assignment for ticket {ticket_id} with payload: {payload}"
            )
            # print(f" CONSOLE DEBUG: Enhanced fallback assignment - Admin: {admin_id}, Team: {team_id}")
            # print(f" CONSOLE DEBUG: Enhanced fallback payload: {payload}")

            response = self.rate_limiter.make_request_with_retry(
                url, "PUT", headers=self._get_ticket_headers(), json=payload
            )

            # print(f" CONSOLE DEBUG: Enhanced fallback response status: {response.status_code}")
            # print(f" CONSOLE DEBUG: Enhanced fallback response: {response.text[:300]}...")

            if response.status_code in [200, 201]:
                fallback_result = response.json()
                assigned_admin = fallback_result.get("admin_assignee_id")
                assigned_team = fallback_result.get("team_assignee_id")
                logging.info(
                    f"[SUCCESS] Enhanced fallback assignment completed - Admin: {assigned_admin}, Team: {assigned_team}"
                )
                # print(f" CONSOLE DEBUG: [SUCCESS] Enhanced fallback successful - Admin: {assigned_admin}, Team: {assigned_team}")

                if team_id and str(assigned_team) == str(team_id):
                    # print(f" CONSOLE DEBUG: ✅ ENHANCED FALLBACK TEAM ASSIGNMENT SUCCESSFUL!")
                    logging.info(
                        f"✅ Enhanced fallback team assignment successful: {assigned_team}"
                    )
                elif team_id:
                    # print(f" CONSOLE DEBUG: [ERROR] Enhanced fallback team assignment failed. Expected: {team_id}, Got: {assigned_team}")
                    logging.error(
                        f"Enhanced fallback team assignment failed. Expected: {team_id}, Got: {assigned_team}"
                    )

            else:
                logging.error(
                    f"[ERROR] Enhanced fallback assignment failed: {response.status_code} - {response.text}"
                )
                # print(f" CONSOLE DEBUG: [ERROR] Enhanced fallback assignment failed: {response.status_code}")

            return response

        except Exception as e:
            logging.warning(
                f"Exception during enhanced fallback assignment for ticket {ticket_id}: {e}"
            )
            # print(f" CONSOLE DEBUG: [ERROR] Exception during enhanced fallback assignment: {e}")
            return None

    def _assign_ticket_tags(self, ticket_id: str, tag_name: str, admin_id: str):
        """
        Assign a tag to a ticket.
        Note: Intercom tickets API has limitations for tag assignment depending on ticket type configuration.
        This method attempts tag assignment but ensures ticket creation success is not affected.

        Args:
            ticket_id: The ticket ID
            tag_name: Name of the tag to assign
            admin_id: Admin ID performing the assignment
        """
        try:
            logging.info(f"Attempting to assign tag '{tag_name}' to ticket {ticket_id}")

            # NOTE: Tag assignment for tickets in Intercom depends on the ticket type configuration.
            # Many ticket types don't support arbitrary tags in ticket_attributes.
            # For production use, tags should be configured as custom attributes in the ticket type schema.

            logging.info(
                f"Tag assignment for ticket {ticket_id} with tag '{tag_name}' - requires ticket type configuration"
            )
            logging.info(
                f"Ticket {ticket_id} created successfully. Tag '{tag_name}' should be added manually or via ticket type configuration"
            )

            # Future improvement: Implement custom attributes based tagging if the ticket type supports it

        except Exception as e:
            logging.error(f"Error in tag assignment for ticket {ticket_id}: {e}")
            logging.info(
                f"Ticket {ticket_id} was created successfully despite tag assignment issue"
            )
            # Don't fail the entire operation for tag assignment issues

    async def create_ticket_universal(
        self, ticket_data: Dict, headers: Dict
    ) -> ConnectorResponse:
        """
        Universal ticket creation method for Intercom.
        Handles ticket creation, assignment, and tagging.
        """
        try:
            # Extract payload fields directly - support multiple field names for flexibility
            contact_email = (
                ticket_data.get("from")
                or ticket_data.get("email")
                or ticket_data.get("requester_email")
            )
            contact_name = ticket_data.get("name") or ticket_data.get("requester_name")

            # Get ticket_type_id from ticket_data or headers
            ticket_type_id = ticket_data.get("ticket_type_id")
            ticket_state_id = int(
                ticket_data.get("ticket_state", "1")
            )  # Default to 'Submitted' state
            if not ticket_type_id:
                headers_dict = convert_headers_to_dict(headers)
                ticket_type_id = headers_dict.get("ticket_type_id")

            default_title = ticket_data.get("_default_title_") or ticket_data.get(
                "default_title", ""
            )
            # Dom 11/4 - Added a check for custom attributes as fallback for description
            default_description = (
                ticket_data.get("_default_description_")
                or ticket_data.get("default_description", "")
                or ticket_data.get("custom_attributes", {}).get("_default_description_")
            )

            MAX_CHARS = 32767
            TRUNCATION_MARKER = "Truncated: "
            MARKER_LEN = len(TRUNCATION_MARKER)

            if len(repr(default_description)) > MAX_CHARS:
                # Reserve space for the marker
                allowed_len = MAX_CHARS - MARKER_LEN - 2

                # Encode to check inflated size
                encoded_size = len(repr(default_description.encode("unicode_escape")))

                if encoded_size > MAX_CHARS:
                    # Scale down more aggressively based on byte inflation
                    max_chars = min(
                        allowed_len,
                        len(repr(default_description)) * allowed_len // encoded_size,
                    )
                    truncated = default_description[:max_chars]
                else:
                    truncated = default_description[:allowed_len]

                # Add marker
                default_description = TRUNCATION_MARKER + truncated

                encoded_size = len(repr(default_description.encode("unicode_escape")))
                # Dom - 9/23 - Revised logic
                if encoded_size > MAX_CHARS:
                    # Iteratively trim until we're under the limit
                    while len(repr(default_description.encode("utf-8"))) > MAX_CHARS:
                        # Cut by 10% each time to avoid over-cutting
                        cut_len = int(len(default_description) * 0.9)
                        default_description = default_description[:cut_len]

            # ========================================
            # Dom - 11/1 - INLINE IMAGE PROCESSING - TICKET DESCRIPTION
            # ========================================
            processed_description = default_description
            inline_attachment_urls = []

            if default_description and self.safe_attachment_service:
                try:
                    inline_processor = UniversalInlineProcessor(
                        self.safe_attachment_service
                    )

                    # Build auth config from headers
                    headers_dict = convert_headers_to_dict(headers)
                    auth_config = self._build_universal_auth_config(headers_dict)

                    # Process inline images with source authentication
                    processed_description, inline_attachments = (
                        await inline_processor.process_inline_images_universal(
                            content=default_description,
                            source_domain=headers_dict.get("domainUrl"),
                            auth_config=auth_config,
                        )
                    )

                    if inline_attachments:
                        inline_attachment_urls = inline_attachments
                        logging.info(
                            f"📎 Extracted {len(inline_attachments)} inline images from ticket description"
                        )

                    # Use processed description (with updated image references)
                    default_description = processed_description

                except Exception as e:
                    logging.error(
                        f"❌ Failed to process inline images in ticket description: {e}"
                    )
                    # Continue with original description if processing fails
            # ========================================

            tags = ticket_data.get("tags", [])

            # Support multiple field names for assignment IDs (both new and legacy formats)
            admin_id = (
                ticket_data.get("admin_assignee_id")
                or ticket_data.get("admin_id")
                # or self.default_admin_id
            )
            team_id = (
                ticket_data.get("team_assignee_id")
                or ticket_data.get("team_id")
                or self.default_team_id
            )
            custom_attributes = ticket_data.get("custom_attributes", {})

            # # Enhanced debugging for team assignment
            # print(f"\n CONSOLE DEBUG: ===== TEAM ASSIGNMENT DEBUGGING =====")
            # print(f" CONSOLE DEBUG: Raw admin_assignee_id from ticket_data: {ticket_data.get('admin_assignee_id')}")
            # print(f" CONSOLE DEBUG: Raw admin_id from ticket_data: {ticket_data.get('admin_id')}")
            # print(f" CONSOLE DEBUG: Raw team_assignee_id from ticket_data: {ticket_data.get('team_assignee_id')}")
            # print(f" CONSOLE DEBUG: Raw team_id from ticket_data: {ticket_data.get('team_id')}")
            # print(f" CONSOLE DEBUG: self.default_admin_id: {self.default_admin_id}")
            # print(f" CONSOLE DEBUG: self.default_team_id: {self.default_team_id}")
            # print(f" CONSOLE DEBUG: Final admin_id to use: {admin_id}")
            # print(f" CONSOLE DEBUG: Final team_id to use: {team_id}")
            # print(f" CONSOLE DEBUG: Type of admin_id: {type(admin_id)}")
            # print(f" CONSOLE DEBUG: Type of team_id: {type(team_id)}")
            # print(f" CONSOLE DEBUG: ==========================================")

            # Debug logging to understand what values we're getting
            # logging.info(f" DEBUG: Ticket creation input data keys: {list(ticket_data.keys())}")
            # logging.info(f" DEBUG: Contact email: {contact_email}, ticket_type_id: {ticket_type_id}")
            # logging.info(f" DEBUG: Default title: '{default_title}' (length: {len(default_title)})")
            # logging.info(f" DEBUG: Default description: '{default_description}' (length: {len(default_description)})")
            # logging.info(f" DEBUG: Raw ticket_data for title fields: _default_title_='{ticket_data.get('_default_title_')}', default_title='{ticket_data.get('default_title')}'")
            # logging.info(f" DEBUG: Raw ticket_data for description fields: _default_description_='{ticket_data.get('_default_description_')}', default_description='{ticket_data.get('default_description')}'")
            # logging.info(f"🎯 TEAM DEBUG: admin_id={admin_id} (type: {type(admin_id)}), team_id={team_id} (type: {type(team_id)})")

            # Console output for immediate visibility
            # print(f"\n CONSOLE DEBUG: Ticket creation starting")
            # print(f" CONSOLE DEBUG: Default title: '{default_title}' (length: {len(default_title)})")
            # print(f" CONSOLE DEBUG: Default description: '{default_description}' (length: {len(default_description)})")
            # print(f" CONSOLE DEBUG: Input data keys: {list(ticket_data.keys())}")
            # print(f" CONSOLE DEBUG: Looking for description in: _default_description_='{ticket_data.get('_default_description_')}', default_description='{ticket_data.get('default_description')}'")
            # print(f" CONSOLE DEBUG: Contact: {contact_email}, Ticket Type: {ticket_type_id}")

            # Validate required fields
            if not contact_email or not ticket_type_id:
                return ConnectorResponse(
                    status_code=400,
                    success=False,
                    error_message="Missing required fields: email or ticket_type_id",
                )

            # Get or create contact (reuse existing logic)
            contact = self.get_or_create_contact(contact_email, contact_name)
            if not contact:
                return ConnectorResponse(
                    status_code=400,
                    success=False,
                    error_message=f"Could not create/find contact for email: {contact_email}",
                )

            # Build ticket attributes
            ticket_attributes = {}
            if default_title:
                ticket_attributes["_default_title_"] = default_title
                logging.info(
                    f" DEBUG: Added title to ticket_attributes: '{default_title}'"
                )
                # print(f" CONSOLE DEBUG: [SUCCESS] Added title to ticket_attributes: '{default_title}'")
            else:
                logging.warning(
                    f" DEBUG: No title found - default_title is empty or None"
                )
                default_title = "Untitled"
                # print(f" CONSOLE DEBUG: [ERROR] No title found - default_title is empty or None")

            if default_description:
                ticket_attributes["_default_description_"] = default_description
                # Dom - 11/4 - Also update in custom_attributes if present
                if "_default_description_" in custom_attributes:
                    custom_attributes["_default_description_"] = default_description
                logging.info(
                    f" DEBUG: Added description to ticket_attributes: '{default_description}'"
                )
                # print(f" CONSOLE DEBUG: [SUCCESS] Added description to ticket_attributes: '{default_description}'")
            else:
                logging.warning(
                    f" DEBUG: No description found - default_description is empty or None"
                )
                default_description = "No description provided"
                # print(f" CONSOLE DEBUG: [ERROR] No description found - default_description is empty or None")

            # logging.info(f" DEBUG: Final ticket_attributes: {ticket_attributes}")
            # print(f" CONSOLE DEBUG: Final ticket_attributes keys: {list(ticket_attributes.keys())}")
            # print(f" CONSOLE DEBUG: Final ticket_attributes: {ticket_attributes}")

            # Log team assignment details
            # if admin_id:
            #     logging.info(f" DEBUG: Creating ticket with admin assignment: {admin_id}")
            #     #print(f" CONSOLE DEBUG: Creating ticket with admin assignment: {admin_id}")
            # if team_id:
            #     logging.info(f" DEBUG: Creating ticket with team assignment: {team_id}")
            #     #print(f" CONSOLE DEBUG: Creating ticket with team assignment: {team_id}")

            # Initialize fallback_admin_id early to avoid UnboundLocalError
            # fallback_admin_id = self._get_or_fetch_admin_id()

            # Create ticket with initial assignment (more reliable than separate assignment)
            # print(f" CONSOLE DEBUG: Creating ticket with team_id: {team_id}, admin_id: {admin_id}")
            # logging.info(f"🎫 Creating ticket with initial assignment - Admin: {admin_id}, Team: {team_id}")

            response = self._create_ticket_api_call_internal(
                contact_id=contact["id"],
                ticket_type_id=ticket_type_id,
                ticket_attributes=ticket_attributes,
                custom_attributes=custom_attributes if custom_attributes else None,
                admin_id=admin_id,
                team_id=team_id,
                ticket_state_id=ticket_state_id,
            )

            # print(f" CONSOLE DEBUG: Ticket creation response status: {response.status_code}")
            # logging.info(f"🎫 Ticket creation API response status: {response.status_code}")

            if response.status_code not in [200, 201]:
                error_text = response.text
                # print(f" CONSOLE DEBUG: [ERROR] Ticket creation failed: {error_text}")
                logging.error(f"🎫 Ticket creation failed: {error_text}")
                return ConnectorResponse(
                    status_code=response.status_code,
                    success=False,
                    error_message=f"Failed to create ticket: {error_text}",
                )

            ticket_result = response.json()
            # print(f" CONSOLE DEBUG: Full ticket response: {ticket_result}")
            logging.info(f"🎫 Full ticket creation response: {ticket_result}")

            intercom_ticket_id = ticket_result.get("id")

            if not intercom_ticket_id:
                return ConnectorResponse(
                    status_code=500,
                    success=False,
                    error_message="No ticket ID returned from Intercom",
                )

            logging.info(f"Created Intercom ticket: {intercom_ticket_id}")

            # ========================================
            # CONSOLIDATED ATTACHMENT HANDLING FOR TICKETS
            # Tickets require all attachments to be base64-formatted
            # ========================================
            all_attachment_files = []

            # Collect inline attachments (already base64-formatted from processing)
            if inline_attachment_urls:
                # Filter out invalid entries (URL strings instead of objects)
                valid_inline_attachments = []
                for att in inline_attachment_urls:
                    if (
                        isinstance(att, dict)
                        and att.get("type") == "file"
                        and "data" in att
                    ):
                        valid_inline_attachments.append(att)
                    else:
                        logging.error(
                            f"❌ Skipping invalid inline attachment - Type: {type(att).__name__}, Value: {str(att)[:150]}"
                        )

                invalid_count = len(inline_attachment_urls) - len(
                    valid_inline_attachments
                )
                if invalid_count > 0:
                    logging.warning(
                        f"⚠️ Filtered out {invalid_count} invalid inline attachment(s) (URL strings or malformed objects)"
                    )

                all_attachment_files.extend(
                    valid_inline_attachments
                )  # ✅ FIXED: Use valid_inline_attachments
                logging.info(
                    f"Added {len(valid_inline_attachments)} valid inline image(s) from description"
                )

            # Collect and PROCESS explicit attachments (need to convert URLs to base64)
            explicit_attachments = ticket_data.get("attachments", [])
            if explicit_attachments and isinstance(explicit_attachments, list):
                # Build attachment objects for processing
                explicit_for_processing = []
                for att in explicit_attachments:
                    url = att.get("attachment_url")
                    if url:
                        explicit_for_processing.append(
                            {
                                "content_url": url,
                                "url": url,
                                "file_name": att.get("name")
                                or att.get("file_name")
                                or att.get("content_file_name")
                                or "attachment",
                                "name": att.get("name")
                                or att.get("file_name")
                                or "attachment",
                                "content_type": att.get("content_type")
                                or att.get("content_content_type")
                                or "application/octet-stream",
                            }
                        )

                # Process explicit attachments to base64 format
                if explicit_for_processing:
                    if self.safe_attachment_service:
                        try:
                            headers_dict = convert_headers_to_dict(headers)
                            auth_config = self._build_universal_auth_config(
                                headers_dict
                            )

                            logging.info(
                                f"Processing {len(explicit_for_processing)} explicit attachment(s) to base64 format"
                            )
                            processed_attachments, failed_attachments = (
                                await self._process_attachments_universal(
                                    explicit_for_processing, auth_config
                                )
                            )

                            if processed_attachments:
                                # ⭐ CRITICAL: Validate processed attachments before adding
                                valid_processed = []
                                for proc_att in processed_attachments:
                                    if (
                                        isinstance(proc_att, dict)
                                        and proc_att.get("type") == "file"
                                        and "data" in proc_att
                                    ):
                                        valid_processed.append(proc_att)
                                    else:
                                        logging.error(
                                            f"❌ Skipping invalid processed attachment - Type: {type(proc_att).__name__}, Value: {str(proc_att)[:150]}"
                                        )

                                all_attachment_files.extend(
                                    valid_processed
                                )  # ✅ FIXED: Use validated list
                                logging.info(
                                    f"Successfully processed {len(valid_processed)} explicit attachment(s)"
                                )

                            if failed_attachments:
                                logging.warning(
                                    f"Failed to process {len(failed_attachments)} explicit attachment(s): {failed_attachments}"
                                )
                        except Exception as e:
                            logging.error(
                                f"Failed to process explicit attachments: {e}"
                            )
                    else:
                        logging.warning(
                            f"No attachment service available - cannot process {len(explicit_for_processing)} explicit attachment(s)"
                        )

            # ⭐ FINAL VALIDATION: Double-check all attachments before upload
            final_validated_attachments = []
            seen_names = set()

            for att in all_attachment_files:
                # Must be a dict
                if not isinstance(att, dict):
                    logging.error(
                        f"❌ FINAL VALIDATION FAILED - Not a dict: {type(att).__name__} - {str(att)[:200]}"
                    )
                    continue

                # Must have required fields
                if att.get("type") != "file":
                    logging.error(
                        f"❌ FINAL VALIDATION FAILED - Invalid type: {att.get('type')} for {att.get('name', 'unknown')}"
                    )
                    continue

                if "data" not in att:
                    logging.error(
                        f"❌ FINAL VALIDATION FAILED - Missing data field: {att.get('name', 'unknown')}"
                    )
                    continue

                if "name" not in att:
                    logging.error(
                        f"❌ FINAL VALIDATION FAILED - Missing name field: {att}"
                    )
                    continue

                # Deduplicate by name
                name = att.get("name", "")
                if name in seen_names:
                    logging.debug(f"Skipping duplicate attachment: {name}")
                    continue

                seen_names.add(name)
                final_validated_attachments.append(att)

            all_attachment_files = final_validated_attachments

            # Create reply with all attachments if any exist
            if all_attachment_files:
                logging.info(
                    f"✅ Final validation complete: {len(all_attachment_files)} valid, unique attachment(s) ready for upload"
                )

                try:
                    # Use ticket creation timestamp for chronological order
                    ticket_created_at = ticket_data.get("created_at") or int(
                        time.time()
                    )

                    # Get admin ID (use ticket creator or fallback)
                    attachment_admin_id = admin_id or self._get_or_fetch_admin_id()

                    if attachment_admin_id:
                        logging.info(
                            f"Creating attachment reply for ticket {intercom_ticket_id} with {len(all_attachment_files)} base64-formatted attachment(s)"
                        )

                        response = self._add_conversation_reply_api_call(
                            conversation_id=intercom_ticket_id,
                            body="Ticket Description Attachments",
                            created_at=convert_to_epoch(ticket_created_at),
                            author_type="admin",
                            message_type="note",  # Internal note (not visible to customer)
                            admin_id=attachment_admin_id,
                            attachment_files=all_attachment_files,
                            is_formatted_attachments=True,  # Always True - all attachments are base64
                        )

                        if response and response.status_code in [200, 201]:
                            logging.info(
                                f"✅ Successfully added {len(all_attachment_files)} attachment(s) to ticket {intercom_ticket_id}"
                            )
                        else:
                            response_text = response.text if response else "No response"
                            logging.error(
                                f"❌ Failed to add attachments to ticket {intercom_ticket_id}: {response.status_code if response else 'No status'} - {response_text}"
                            )
                    else:
                        logging.warning(
                            f"⚠️ No admin ID available - cannot add attachments to ticket {intercom_ticket_id}"
                        )

                except Exception as e:
                    logging.error(
                        f"❌ Exception adding attachment reply to ticket {intercom_ticket_id}: {e}"
                    )
                    import traceback

                    logging.error(f"Traceback: {traceback.format_exc()}")
                    # Don't fail ticket creation - attachments are secondary
            else:
                logging.info(
                    f"No valid attachments to add for ticket {intercom_ticket_id}"
                )
            # ========================================

            # Assign team/admin
            if team_id:
                team_admin_id = admin_id  # or fallback_admin_id
                self._assign_conversation(
                    intercom_ticket_id, admin_id=team_admin_id, team_id=team_id
                )
            if admin_id:
                self._assign_conversation(intercom_ticket_id, admin_id=admin_id)
            # elif not team_id:
            #     # If no specific admin or team assignment, try to assign to current admin
            #     if fallback_admin_id:
            #         self._assign_conversation(
            #             intercom_ticket_id, admin_id=fallback_admin_id
            #         )

            # Validate assignment was successful and apply fallback if needed
            if admin_id or team_id:
                assigned_admin = ticket_result.get("admin_assignee_id")
                assigned_team = ticket_result.get("team_assignee_id")

                # print(f" CONSOLE DEBUG: Assignment check - Expected admin: {admin_id}, Got: {assigned_admin}")
                # print(f" CONSOLE DEBUG: Assignment check - Expected team: {team_id}, Got: {assigned_team}")
                logging.info(
                    f"Assignment verification - Expected admin: {admin_id}, Got: {assigned_admin}"
                )
                logging.info(
                    f"Assignment verification - Expected team: {team_id}, Got: {assigned_team}"
                )

                assignment_successful = True

                if admin_id and str(assigned_admin) != str(admin_id):
                    logging.warning(
                        f"Admin assignment may have failed. Expected: {admin_id}, Got: {assigned_admin}"
                    )
                    # print(f" CONSOLE DEBUG: [ERROR] Admin assignment mismatch!")
                    assignment_successful = False

                if team_id and (
                    assigned_team is None or str(assigned_team) != str(team_id)
                ):
                    logging.warning(
                        f"Team assignment failed. Expected: {team_id}, Got: {assigned_team}"
                    )
                    # print(f" CONSOLE DEBUG: [ERROR] Team assignment failed! Expected: {team_id}, Got: {assigned_team}")
                    assignment_successful = False

                # If initial assignment failed, try the separate assignment call as fallback
                if not assignment_successful:
                    logging.info("Attempting fallback assignment via PUT request...")
                    # print(f" CONSOLE DEBUG: 🔄 Attempting fallback assignment...")
                    self._assign_ticket(
                        ticket_id=intercom_ticket_id, admin_id=admin_id, team_id=team_id
                    )
                else:
                    logging.info(
                        f"[SUCCESS] Initial assignment successful - Admin: {assigned_admin}, Team: {assigned_team}"
                    )
                    # print(f" CONSOLE DEBUG: [SUCCESS] Team assignment successful! Team ID: {assigned_team}")

            # Assign tags
            # for tag in tags:
            #     tag_admin_id = admin_id or self._get_or_fetch_admin_id()
            #     if tag_admin_id:
            #         self._create_and_assign_tag_ticket(intercom_ticket_id, tag, tag_admin_id)
            #     else:
            #         logging.warning(
            #             f"Could not assign tag '{tag}' - no admin ID available"
            #         )

            # ========================================
            # Dom 11/1 - OLD ATTACHMENT HANDLING - COMMENTED OUT
            # Now handled in consolidated section below (after ticket creation)
            # ========================================
            # Call the internal API method with correct parameters
            # created_at = ticket_data.get("created_at") or int(time.time())
            # attachments = ticket_data.get("attachments", [])
            # if attachments and isinstance(attachments, list):
            #     # Process attachments using universal method
            #     attachment_urls = [att.get("attachment_url") for att in attachments if att.get("attachment_url")]
            #     if attachment_urls:
            #         response = self._add_conversation_reply_api_call(
            #             conversation_id=intercom_ticket_id,
            #             body="Ticket Attachments",
            #             created_at=convert_to_epoch(created_at),
            #             author_type="admin",
            #             message_type="comment", #body.get("public", True) and "comment" or "note",
            #             admin_id=admin_id, # if admin_id is not None else fallback_admin_id,
            #             attachment_files=attachment_urls,
            #             is_formatted_attachments=False,
            #         )

            if ticket_data.get("admin_assignee_id") and ticket_data.get("ticket_state"):
                ticket_state_url = f"{self.base_url}/ticket_states"
                response = self.rate_limiter.make_request_with_retry(
                    ticket_state_url, "GET", headers=self._get_headers()
                )
                if response.status_code == 200:
                    ticket_states = response.json().get("data", [])
                    for state in ticket_states:
                        if int(state.get("id")) == int(ticket_data.get("ticket_state")):
                            if (
                                state.get("category").lower() == "resolved"
                                or state.get("category").lower() == "closed"
                            ):
                                logging.info(
                                    f"Ticket {intercom_ticket_id} created with closed state {ticket_data.get('ticket_state')}. Will finalize after messages."
                                )
                                # self._close_conversation(
                                #   intercom_ticket_id, ticket_data.get("admin_assignee_id"), int(ticket_data.get("ticket_state")), "ticket"
                                # )

            return ConnectorResponse(
                status_code=200,
                success=True,
                data={
                    "ticket_id": intercom_ticket_id,
                    "id": intercom_ticket_id,
                    "display_id": intercom_ticket_id,  # For migration framework compatibility
                },
            )

        except Exception as e:
            logging.error(f"Error in create_ticket_universal: {e}")
            return ConnectorResponse(
                status_code=500, success=False, error_message=str(e)
            )

    def finalize_closed_ticket(
        self, conversation_id: str, admin_id: str = None, ticket_state_id: int = None
    ):
        """
        Explicitly close a ticket/conversation after all messages have been added.

        Args:
            conversation_id: The Intercom ticket/conversation ID
            admin_id: Admin ID to use for closing (optional)
            ticket_state_id: Optional specific ticket state ID to use for resolved/closed state
        """
        logging.info(f"Finalizing closed state for ticket {conversation_id}")

        get_ticket_url = f"{self.base_url}/tickets/{conversation_id}"
        response = self.rate_limiter.make_request_with_retry(
            get_ticket_url, "GET", headers=self._get_headers()
        )

        if response.status_code == 200:
            ticket_type = "ticket"
        else:
            ticket_type = "conversation"

        # Close the ticket/conversation
        self._close_conversation(
            conversation_id, admin_id, ticket_state_id, ticket_type
        )

        logging.info(f"Ticket {conversation_id} finalized as closed")

    # ===================================================================
    # CONVERSATION MANAGEMENT
    # ===================================================================

    def _create_conversation_api_call_internal(
        self,
        contact_id: str,
        body: str,
        created_at: Union[str, int],
        attachment_files: List[Dict] = None,
        custom_attributes: Dict = None,
        payloadData: Dict = None,
    ) -> requests.Response:
        """
        Create a new Intercom conversation

        Args:
            contact_id: Intercom contact ID
            body: Conversation message body
            created_at: Timestamp (string or epoch int)
            attachment_files: Optional list of attachment files
            custom_attributes: Optional custom attributes to set on the conversation (applied after creation)
        """
        global is_last_ticket_closed
        is_last_ticket_closed = {"id": None, "closed": False}  # Reset for next ticket
        # Convert timestamp to epoch if it's a string
        created_at_epoch = convert_to_epoch(created_at)

        url = f"{self.base_url}/conversations"
        payload = {
            "from": {"type": "user", "id": contact_id},
            "body": body,
            "created_at": created_at_epoch,
        }

        # Add admin/team assignment if configured
        # if self.default_admin_id:
        #     payload["admin_assignee_id"] = self.default_admin_id or payloadData.get(
        #         "admin_assignee_id"
        #     )

        if self.default_team_id:
            payload["team_assignee_id"] = self.default_team_id or payloadData.get(
                "team_assignee_id"
            )

        if payloadData.get("subject"):
            payload["subject"] = payloadData.get("subject", "New Conversation")

        # if payloadData.get('ticket_state'):
        #     payload["state"] = payloadData.get('ticket_state', 'open')
        # if payloadData.get('open') is not None:
        #     val = payloadData.get('open')
        #     if isinstance(val, str):
        #         payload["open"] = val.lower() == "true"
        #     else:
        #         payload["open"] = bool(val)

        # Add attachments if provided
        if attachment_files:
            payload["attachment_files"] = attachment_files or payloadData.get(
                "attachment_files", []
            )

        # Create the conversation first
        response = self.rate_limiter.make_request_with_retry(
            url, "POST", headers=self._get_headers(), json=payload
        )

        # If creation was successful and custom_attributes are provided, update them separately
        if response.status_code in [200, 201] and custom_attributes:
            try:
                response_data = response.json()
                conversation_id = response_data.get(
                    "conversation_id"
                ) or response_data.get("id")

                if conversation_id:
                    logging.info(
                        f"Updating custom attributes for conversation {conversation_id}"
                    )
                    self._update_conversation_attributes(
                        conversation_id, custom_attributes
                    )

                    if (
                        payloadData.get("ticket_state")
                        and payloadData.get("ticket_state") == "resolved"
                    ):
                        self._close_conversation(
                            conversation_id,
                            payloadData.get("admin_assignee_id"),
                            None,
                            ticket_type="conversation",
                        )
                else:
                    logging.warning(
                        "Could not extract conversation ID from response, skipping custom attributes update"
                    )

            except Exception as e:
                logging.error(
                    f"Failed to update custom attributes after conversation creation: {e}"
                )
                # Don't fail the entire operation - conversation was created successfully

        return response

    def _add_conversation_reply_api_call(
        self,
        conversation_id: str,
        body: str,
        created_at: Union[str, int],
        author_type: str = "user",
        message_type: str = "note",
        contact_id: str = None,
        admin_id: str = None,
        attachment_files: List[Dict] = None,
        is_formatted_attachments: bool = False,
    ) -> requests.Response:
        try:
            """Add a reply to an existing conversation"""

            # Validate conversation_id
            if not conversation_id or str(conversation_id).strip() == "":
                logging.error("Conversation ID is empty or None")

                class MockResponse:
                    def __init__(self, status_code, text):
                        self.status_code = status_code
                        self.text = text

                    def json(self):
                        return {"error": self.text}

                return MockResponse(400, "Conversation ID is required")

            # Clean conversation_id
            conversation_id = str(conversation_id).strip()

            # Convert timestamp to epoch if it's a string
            created_at_epoch = convert_to_epoch(created_at)

            url = f"{self.base_url}/conversations/{conversation_id}/reply"

            payload = {
                "body": body,
                "created_at": created_at_epoch,
                "type": "user",
                "message_type": "comment",
                "skip_notifications": True,
            }

            if admin_id:
                payload["admin_id"] = admin_id
                payload["type"] = "admin"
                if message_type:
                    payload["message_type"] = message_type
            elif contact_id:
                payload["intercom_user_id"] = contact_id
                payload["message_type"] = "comment"

            # Add attachments if provided
            if attachment_files and not is_formatted_attachments:
                payload["attachment_urls"] = attachment_files
            elif attachment_files and is_formatted_attachments:
                payload["attachment_files"] = attachment_files

            # Apply conversation-specific rate limiting
            wait_for_conversation_rate_limit(conversation_id)

            # Periodically clean up old conversation trackers
            self._cleanup_conversation_trackers_if_needed()

            data = self.rate_limiter.make_request_with_retry(
                url, "POST", headers=self._get_headers(), json=payload
            )

            return data

        except requests.exceptions.HTTPError as e:
            if e.response is not None:
                try:
                    error_data = e.response.json()
                    if error_data.get("errors") and len(error_data["errors"]) > 0:
                        err = error_data["errors"][0]
                        code = err.get("code")
                        message = err.get("message", "")

                        if code == "parameter_invalid":
                            payload = None

                            # Case 1: user can only create comments
                            if (
                                "User can only create comments" in message
                                and contact_id
                            ):
                                logging.warning(
                                    "User can only create comments. Switching message_type to 'comment'."
                                )
                                payload = {
                                    "message_type": "comment",
                                    "type": "user",
                                    "body": body,
                                    "created_at": created_at_epoch,
                                    "intercom_user_id": contact_id,
                                    "skip_notifications": True,  # avoid notifications
                                }

                            # Case 2: invalid created_at/updated_at → retry without timestamp
                            elif (
                                "is an invalid value for created_at" in message
                                or "is an invalid value for updated_at" in message
                            ):
                                payload = {
                                    "body": body,
                                    "type": "user",  # default
                                    "message_type": "comment",  # default
                                    "skip_notifications": True,  # avoid notifications
                                }

                                if admin_id:
                                    payload["admin_id"] = admin_id
                                    payload["type"] = "admin"
                                    if (
                                        message_type
                                    ):  # only override if explicitly passed
                                        payload["message_type"] = message_type

                                elif contact_id:
                                    payload["intercom_user_id"] = contact_id
                                    payload["message_type"] = (
                                        "comment"  # enforced for users
                                    )

                            # Attachments (common logic)
                            if payload:
                                if attachment_files:
                                    key = (
                                        "attachment_files"
                                        if is_formatted_attachments
                                        else "attachment_urls"
                                    )
                                    payload[key] = attachment_files

                                # Apply rate limiting before retry
                                wait_for_conversation_rate_limit(conversation_id)

                                return self.rate_limiter.make_request_with_retry(
                                    url,
                                    "POST",
                                    headers=self._get_headers(),
                                    json=payload,
                                )

                except Exception as parse_err:
                    logging.error(f"Error parsing error response: {parse_err}")

                # Check for "Too many recent replies" error
                if (
                    e.response
                    and e.response.status_code == 400
                    and "Too many recent replies to this conversation"
                    in str(e.response.text)
                ):
                    logging.warning(
                        f"Rate limit exceeded for conversation {conversation_id}. Retrying with extended delay..."
                    )

                    # Wait longer and retry once
                    time.sleep(
                        15
                    )  # Wait 15 seconds to ensure we're clear of the rate limit window

                    # Clean up the tracker for this conversation to reset the counter
                    with conversation_reply_lock:
                        if conversation_id in conversation_reply_tracker:
                            conversation_reply_tracker[conversation_id].clear()

                    try:
                        # Apply rate limiting again and retry
                        wait_for_conversation_rate_limit(conversation_id)

                        retry_data = self.rate_limiter.make_request_with_retry(
                            url, "POST", headers=self._get_headers(), json=payload
                        )
                        logging.info(
                            f"Retry successful for conversation {conversation_id}"
                        )
                        return retry_data
                    except Exception as retry_error:
                        logging.error(
                            f"Retry failed for conversation {conversation_id}: {retry_error}"
                        )
                        # Continue to original error handling

                # Check for 404 Not Found - conversation doesn't exist
                if e.response.status_code == 404:
                    logging.error(
                        f"Conversation {conversation_id} not found (404). This conversation may not exist in this Intercom workspace or may have been deleted."
                    )
                    return MockResponse(
                        404,
                        f"Conversation {conversation_id} not found. It may not exist in this workspace or may have been deleted.",
                    )

                # Check for 401/403 - authentication/authorization issues
                if e.response.status_code in [401, 403]:
                    logging.error(
                        f"Authentication/Authorization error for conversation {conversation_id}: {e.response.status_code}"
                    )
                    return MockResponse(
                        e.response.status_code,
                        f"Authentication/Authorization error: {e.response.text}",
                    )

            logging.error(f"Failed to add conversation reply: {e}")

            # Return a mock response object to avoid None return
            class MockResponse:
                def __init__(self, status_code, text):
                    self.status_code = status_code
                    self.text = text

                def json(self):
                    return {"error": self.text}

            return MockResponse(500, f"Failed to add conversation reply: {e}")
        except Exception as e:
            logging.error(f"Unexpected error in _add_conversation_reply_api_call: {e}")

            # Return a mock response object to avoid None return
            class MockResponse:
                def __init__(self, status_code, text):
                    self.status_code = status_code
                    self.text = text

                def json(self):
                    return {"error": self.text}

            return MockResponse(500, f"Unexpected error: {e}")

    def _assign_conversation(
        self, conversation_id: str, admin_id: str = None, team_id: str = None
    ):
        """Assign conversation to admin or team"""
        url = f"{self.base_url}/conversations/{conversation_id}/parts"

        if team_id and admin_id:
            payload = {
                "message_type": "assignment",
                "type": "team",
                "admin_id": admin_id,  # or self.default_admin_id
                "assignee_id": team_id,
            }

            try:
                response = self.rate_limiter.make_request_with_retry(
                    url, "POST", headers=self._get_headers(), json=payload
                )
                logging.info(
                    f"Assigned conversation {conversation_id} to team {team_id}"
                )
            except Exception as e:
                logging.error(f"Failed to assign conversation to team: {e}")

        if admin_id:
            payload = {
                "message_type": "assignment",
                "type": "admin",
                "admin_id": admin_id,  # or self.default_admin_id,
                "assignee_id": admin_id,
            }
            try:
                response = self.rate_limiter.make_request_with_retry(
                    url, "POST", headers=self._get_headers(), json=payload
                )
                logging.info(
                    f"Assigned conversation {conversation_id} to admin {admin_id}"
                )
            except Exception as e:
                logging.error(f"Failed to assign conversation to admin: {e}")

    def _close_conversation(
        self,
        conversation_id: str,
        admin_id: str = None,
        ticket_state_id: int = None,
        ticket_type: str = "conversation",
    ):
        """Close a conversation"""
        global is_last_ticket_closed

        # If admin_id not provided, fetch current admin as fallback
        if not admin_id:
            admin_id = self._get_current_admin_id()
            if admin_id:
                logging.info(f"Using current admin ID {admin_id} for closing conversation")
            else:
                logging.warning(f"No admin ID available for closing conversation {conversation_id}")

        if ticket_type.lower() == "ticket":

            if ticket_state_id is None:
                ticket_state_url = f"{self.base_url}/ticket_states"
                response = self.rate_limiter.make_request_with_retry(
                    ticket_state_url, "GET", headers=self._get_headers()
                )
                if response.status_code == 200:
                    ticket_states = response.json().get("data", [])
                    for state in ticket_states:
                        if state.get("category").lower() in ["resolved", "closed"]:
                            ticket_state_id = int(state.get("id"))

            get_ticket_url = f"{self.base_url}/tickets/{conversation_id}"
            response = self.rate_limiter.make_request_with_retry(
                get_ticket_url, "GET", headers=self._get_headers()
            )

            if response.status_code == 200:
                ticket_resp = response.json()
                ticket_state_id_retrieved = ticket_resp.get("ticket_state").get("id")

                if int(ticket_state_id_retrieved) == ticket_state_id:
                    logging.info(f"Ticket {conversation_id} already in closed state, closing conversation")
                    url = f"{self.base_url}/conversations/{conversation_id}/parts"

                    payload = {"message_type": "close", "type": "admin"}

                    if admin_id:
                        payload["admin_id"] = admin_id

                    response = self.rate_limiter.make_request_with_retry(
                        url, "POST", headers=self._get_headers(), json=payload
                    )

                    # Check response and log result before returning
                    try:
                        if response.status_code not in [200, 201]:
                            logging.error(
                                f"Failed to close conversation {conversation_id}: {response.status_code} - {response.text}"
                            )
                            is_last_ticket_closed = {"id": conversation_id, "closed": False}
                            return

                        is_last_ticket_closed = {"id": conversation_id, "closed": True}
                        logging.info(f"Closed conversation {conversation_id}")
                    except Exception as e:
                        logging.error(f"Failed to close conversation: {e}")
                        is_last_ticket_closed = {"id": conversation_id, "closed": False}

                    return  # Already closed

            url = f"{self.base_url}/tickets/{conversation_id}"

            payload = {}

            if ticket_state_id:
                payload["ticket_state_id"] = ticket_state_id

            if admin_id:
                payload["admin_id"] = admin_id

            if payload and payload != {}:
                response = self.rate_limiter.make_request_with_retry(
                    url, "PUT", headers=self._get_headers(), json=payload
                )

            url = f"{self.base_url}/conversations/{conversation_id}/parts"

            payload = {"message_type": "close", "type": "admin"}

            if admin_id:
                payload["admin_id"] = admin_id

            response = self.rate_limiter.make_request_with_retry(
                url, "POST", headers=self._get_headers(), json=payload
            )

        else:
            url = f"{self.base_url}/conversations/{conversation_id}/parts"

            payload = {"message_type": "close", "type": "admin"}

            if admin_id:
                payload["admin_id"] = admin_id

            response = self.rate_limiter.make_request_with_retry(
                url, "POST", headers=self._get_headers(), json=payload
            )

        try:
            if response.status_code not in [200, 201]:
                logging.error(
                    f"Failed to close conversation {conversation_id}: {response.status_code} - {response.text}"
                )
                is_last_ticket_closed = {"id": conversation_id, "closed": False}
                return

            is_last_ticket_closed = {"id": conversation_id, "closed": True}
            logging.info(f"Closed conversation {conversation_id}")
        except Exception as e:
            logging.error(f"Failed to close conversation: {e}")
            is_last_ticket_closed = {"id": conversation_id, "closed": False}

    def _update_conversation_attributes(
        self, conversation_id: str, custom_attributes: Dict
    ):
        """Update conversation custom attributes"""
        url = f"{self.base_url}/conversations/{conversation_id}"

        payload = {"custom_attributes": custom_attributes}

        try:
            response = self.rate_limiter.make_request_with_retry(
                url, "PUT", headers=self._get_headers(), json=payload
            )
            logging.info(
                f"Updated custom attributes for conversation {conversation_id}"
            )
        except Exception as e:
            logging.error(f"Failed to update custom attributes: {e}")

    # ===================================================================
    # MESSAGE PROCESSING LOGIC (FROM FRESHCHAT MIGRATION)
    # ===================================================================

    def _extract_messages_from_data(self, data: Dict) -> List[IntercomMessage]:
        """
        Extract and normalize messages from various data formats.
        Handles different message structures from different source platforms.
        """
        messages = []

        # Handle different data structures
        if "messages" in data:
            # Direct messages array
            for msg_data in data["messages"]:
                message = self._normalize_message(msg_data)
                if message:
                    messages.append(message)
        elif "conversations" in data:
            # Nested conversations structure
            for conv in data["conversations"]:
                if "messages" in conv:
                    for msg_data in conv["messages"]:
                        message = self._normalize_message(msg_data)
                        if message:
                            messages.append(message)
        else:
            # Single message in data
            message = self._normalize_message(data)
            if message:
                messages.append(message)

        # Sort by timestamp
        messages.sort(key=lambda x: x.created_at)
        return messages

    def _normalize_message(self, msg_data: Dict) -> Optional[IntercomMessage]:
        """
        Normalize message data from any source platform.
        """
        try:
            # Extract basic fields with fallbacks for different naming conventions
            actor_email = (
                msg_data.get("actor_email")
                or msg_data.get("author_email")
                or msg_data.get("from_email")
                or msg_data.get("email")
                or ""
            )

            if not actor_email or str(actor_email).lower() in ["nan", "none", ""]:
                logging.warning(
                    f"Skipping message due to invalid email: {repr(actor_email)}"
                )
                return None

            actor_name = (
                msg_data.get("actor_name")
                or msg_data.get("author_name")
                or msg_data.get("from_name")
                or msg_data.get("name")
                or ""
            )

            body = (
                msg_data.get("body")
                or msg_data.get("content")
                or msg_data.get("message")
                or msg_data.get("text")
                or ""
            )

            if not body:
                logging.warning(f"Skipping message due to empty body")
                return None

            # Determine message type
            actor_type = (
                msg_data.get("actor_type")
                or msg_data.get("author_type")
                or msg_data.get("type")
                or "user"
            ).lower()

            # Normalize actor types
            if actor_type in ["agent", "admin", "staff"]:
                message_type = "agent"
            elif actor_type in ["bot", "system", "automated"]:
                message_type = "bot"
            else:
                message_type = "user"

            # Handle timestamp
            created_at = (
                msg_data.get("created_at")
                or msg_data.get("timestamp")
                or int(time.time())
            )
            created_at_epoch = convert_to_epoch(created_at)

            # Extract attachments
            attachments = msg_data.get("attachments") or msg_data.get("files") or []

            # Check for private/internal notes
            is_private = msg_data.get("private", False) or msg_data.get(
                "internal", False
            )

            return IntercomMessage(
                message_id=msg_data.get("id") or msg_data.get("message_id"),
                conversation_id=msg_data.get("conversation_id"),
                message_type=message_type,
                actor_email=str(actor_email).strip(),
                actor_name=str(actor_name).strip() if actor_name else None,
                body=str(body),
                created_at=created_at_epoch,
                attachments=attachments if isinstance(attachments, list) else [],
                is_private=bool(is_private),
                admin_id=msg_data.get("admin_id"),
            )

        except Exception as e:
            logging.error(f"Error normalizing message: {e}")
            return None

    def _find_first_user_message(
        self, messages: List[IntercomMessage]
    ) -> Tuple[Optional[IntercomMessage], int]:
        """
        Find the first user message to create the conversation.
        Skips bot messages at the start (like FreshChat migration).
        """
        for idx, message in enumerate(messages):
            if message.message_type == "user":
                return message, idx
        return None, -1

    # ===================================================================
    # ABSTRACT METHOD IMPLEMENTATIONS (Required by BaseTargetConnector)
    # ===================================================================

    def _create_ticket_api_call(
        self, ticket_data: Dict, files: List[Dict], headers: Dict
    ) -> requests.Response:
        """
        Abstract method implementation - maps ticket creation to conversation creation.
        In Intercom, 'tickets' are conversations, so we create a conversation.
        """
        # Extract the first message data from ticket_data
        subject = ticket_data.get("subject", "")
        description = ticket_data.get("description", ticket_data.get("body", ""))
        requester_email = ticket_data.get(
            "email", ticket_data.get("requester_email", "")
        )
        requester_name = ticket_data.get("name", ticket_data.get("requester_name", ""))

        # Combine subject and description for conversation body
        body = f"{subject}\n\n{description}" if subject else description

        # Get or create contact
        contact = self.get_or_create_contact(requester_email, requester_name)
        if not contact:
            # Create a dummy response for error handling
            response = requests.Response()
            response.status_code = 400
            response._content = json.dumps(
                {"error": f"Could not create contact for {requester_email}"}
            ).encode()
            return response

        # Convert files to Intercom format
        attachment_files = []
        if files:
            for file_data in files:
                attachment_files.append(
                    {
                        "type": "file",
                        "name": file_data["name"],
                        "data": base64.b64encode(file_data["content"]).decode("utf-8"),
                        "content_type": file_data["content_type"],
                    }
                )

        # Create conversation
        created_at = ticket_data.get("created_at", int(time.time()))
        created_at_epoch = convert_to_epoch(created_at)

        # Extract custom attributes if provided
        custom_attributes = ticket_data.get("custom_attributes", {})

        return self._create_conversation_api_call_internal(
            contact_id=contact["id"],
            body=body or "[No content]",
            created_at=created_at_epoch,
            attachment_files=attachment_files if attachment_files else None,
            custom_attributes=custom_attributes if custom_attributes else None,
        )

    def _create_conversation_api_call(
        self, ticket_id: str, conversation_data: Dict, files: List[Dict], headers: Dict
    ) -> requests.Response:
        """
        Abstract method implementation - adds a message to existing conversation.
        This maps the legacy "conversation" concept to Intercom replies.
        """
        # In Intercom, adding a "conversation" to a ticket means adding a message to existing conversation
        body = conversation_data.get("body", conversation_data.get("content", ""))

        # Determine message type and author
        author_email = conversation_data.get(
            "author_email", conversation_data.get("email", "")
        )
        author_name = conversation_data.get(
            "author_name", conversation_data.get("name", "")
        )
        is_private = conversation_data.get("private", False)
        user_type = conversation_data.get("user_type", "user")

        # Convert files to Intercom format
        attachment_files = []
        if files:
            for file_data in files:
                attachment_files.append(
                    {
                        "type": "file",
                        "name": file_data["name"],
                        "data": base64.b64encode(file_data["content"]).decode("utf-8"),
                        "content_type": file_data["content_type"],
                    }
                )

        # Handle timestamp
        created_at = conversation_data.get("created_at", int(time.time()))
        created_at_epoch = convert_to_epoch(created_at)

        # Add message based on type
        if user_type in ["agent", "admin"] or is_private:
            # Admin/agent message
            admin_id = conversation_data.get("admin_id")  # or self.default_admin_id
            return self._add_conversation_reply_api_call(
                conversation_id=ticket_id,
                body=body or "[No content]",
                created_at=created_at_epoch,
                author_type="admin",
                message_type="note",
                admin_id=admin_id,
                attachment_files=attachment_files if attachment_files else None,
            )
        else:
            # User message
            contact = self.get_or_create_contact(author_email, author_name)
            if not contact:
                # Create error response
                response = requests.Response()
                response.status_code = 400
                response._content = json.dumps(
                    {"error": f"Could not create contact for {author_email}"}
                ).encode()
                return response

            return self._add_conversation_reply_api_call(
                conversation_id=ticket_id,
                body=body or "[No content]",
                created_at=created_at_epoch,
                author_type="user",
                contact_id=contact["id"],
                attachment_files=attachment_files if attachment_files else None,
            )

    def _create_reply_api_call(
        self, ticket_id: str, reply_data: Dict, files: List[Dict], headers: Dict
    ) -> requests.Response:
        """
        Abstract method implementation - creates a public reply (same as conversation in Intercom).
        """
        # In Intercom, replies are just conversation messages, same as conversations
        return self._create_conversation_api_call_

    async def create_conversation_universal(
        self, conversation_data: Dict, headers: Dict
    ) -> ConnectorResponse:
        """
        Refactored universal conversation creation method.
        Handles conversation creation, tag assignment, and team/agent assignment.
        """
        try:
            # Extract auth config for attachments
            auth_config = self._build_universal_auth_config(headers)

            # Extract payload fields directly
            contact_email = conversation_data.get("from") or conversation_data.get(
                "email"
            )
            contact_name = conversation_data.get("name") or conversation_data.get(
                "email"
            )
            body = (
                conversation_data.get("body")
                or conversation_data.get("default_description", "")
                or "No content"
            )

            tags = conversation_data.get("tags", [])
            admin_id = conversation_data.get(
                "admin_id"  # , self.default_admin_id
            ) or conversation_data.get("admin_assignee_id")
            team_id = conversation_data.get(
                "team_id", self.default_team_id
            ) or conversation_data.get("team_assignee_id")

            # Validate required fields
            if not contact_email or not body:
                return ConnectorResponse(
                    status_code=400,
                    success=False,
                    error_message="Missing required fields: email or body",
                )

            # Get or create contact
            contact = self.get_or_create_contact(contact_email, contact_name)
            if not contact:
                return ConnectorResponse(
                    status_code=400,
                    success=False,
                    error_message=f"Could not create/find contact for email: {contact_email}",
                )

            # Create conversation
            created_at = conversation_data.get("created_at", int(time.time()))
            custom_attributes = conversation_data.get("custom_attributes", {})

            response = self._create_conversation_api_call_internal(
                contact_id=contact["id"],
                body=body,
                created_at=created_at,
                custom_attributes=custom_attributes if custom_attributes else None,
                payloadData=conversation_data,
            )

            if response.status_code not in [200, 201]:
                return ConnectorResponse(
                    status_code=response.status_code,
                    success=False,
                    error_message=f"Failed to create conversation: {response.text}",
                )

            conversation_result = response.json()
            intercom_conversation_id = conversation_result.get(
                "conversation_id"
            ) or conversation_result.get("id")

            if not intercom_conversation_id:
                return ConnectorResponse(
                    status_code=500,
                    success=False,
                    error_message="No conversation ID returned from Intercom",
                )

            logging.info(f"Created Intercom conversation: {intercom_conversation_id}")

            if tags:
                # Assign tags
                for tag in tags:
                    tag_admin_id = self._get_or_fetch_admin_id()
                    if tag_admin_id:
                        self._create_and_assign_tag(
                            intercom_conversation_id, tag, tag_admin_id
                        )
                    else:
                        logging.warning(
                            f"Could not assign tag '{tag}' - no admin ID available"
                        )

            # Assign team/admin
            if team_id:
                team_admin_id = admin_id or self._get_or_fetch_admin_id()
                self._assign_conversation(
                    intercom_conversation_id, admin_id=team_admin_id, team_id=team_id
                )
            if admin_id:
                self._assign_conversation(intercom_conversation_id, admin_id=admin_id)
            # elif not team_id:
            #     # If no specific admin or team assignment, try to assign to current admin
            #     fallback_admin_id = self._get_or_fetch_admin_id()
            #     if fallback_admin_id:
            #         self._assign_conversation(
            #             intercom_conversation_id, admin_id=fallback_admin_id
            #         )

            logging.info(f"Conversation created - {intercom_conversation_id}")
            return ConnectorResponse(
                status_code=200, success=True, data={"id": intercom_conversation_id}
            )

        except Exception as e:
            logging.error(f"Error in create_conversation_universal: {e}")
            return ConnectorResponse(
                status_code=500, success=False, error_message=str(e)
            )

    def _assign_tag_to_conversation(self, conversation_id: str, tag: str):
        """
        Helper method to assign a tag to a conversation.
        """
        url = f"{self.base_url}/tags"
        payload = {"name": tag, "conversation_id": conversation_id}

        try:
            response = self.rate_limiter.make_request_with_retry(
                url, "POST", headers=self._get_headers(), json=payload
            )
            if response.status_code in [200, 201]:
                logging.info(f"Assigned tag '{tag}' to conversation {conversation_id}")
            else:
                logging.error(
                    f"Failed to assign tag '{tag}' to conversation {conversation_id}: {response.text}"
                )
        except Exception as e:
            logging.error(
                f"Error assigning tag '{tag}' to conversation {conversation_id}: {e}"
            )

    def _create_and_assign_tag_ticket(
        self, ticket_id: str, tag_name: str, admin_id: str
    ):
        """
        Helper method to create a tag and assign it to a conversation.
        """
        # Step 1: Create the tag
        create_tag_url = f"{self.base_url}/tags"
        create_tag_payload = {"name": tag_name}

        try:
            create_tag_response = self.rate_limiter.make_request_with_retry(
                create_tag_url,
                "POST",
                headers=self._get_headers(),
                json=create_tag_payload,
            )

            if create_tag_response.status_code not in [200, 201]:
                logging.error(
                    f"Failed to create tag '{tag_name}': {create_tag_response.text}"
                )
                return

            tag_id = create_tag_response.json().get("id")
            if not tag_id:
                logging.error(f"No tag ID returned for tag '{tag_name}'")
                return

            logging.info(f"Created tag '{tag_name}' with ID {tag_id}")

            # Step 2: Assign the tag to the conversation
            assign_tag_url = f"{self.base_url}/tickets/{ticket_id}/tags"
            assign_tag_payload = {"id": int(tag_id), "admin_id": int(admin_id)}

            assign_tag_response = self.rate_limiter.make_request_with_retry(
                assign_tag_url,
                "POST",
                headers=self._get_headers(),
                json=assign_tag_payload,
            )

            if assign_tag_response.status_code in [200, 201]:
                logging.info(f"Assigned tag '{tag_name}' to conversation {ticket_id}")
            else:
                logging.error(
                    f"Failed to assign tag '{tag_name}' to conversation {ticket_id}: {assign_tag_response.text}"
                )

        except Exception as e:
            logging.error(
                f"Error creating or assigning tag '{tag_name}' to conversation {ticket_id}: {e}"
            )

    def _create_and_assign_tag(
        self, conversation_id: str, tag_name: str, admin_id: str
    ):
        """
        Helper method to create a tag and assign it to a conversation.
        """
        # Step 1: Create the tag
        create_tag_url = f"{self.base_url}/tags"
        create_tag_payload = {"name": tag_name}

        try:
            create_tag_response = self.rate_limiter.make_request_with_retry(
                create_tag_url,
                "POST",
                headers=self._get_headers(),
                json=create_tag_payload,
            )

            if create_tag_response.status_code not in [200, 201]:
                logging.error(
                    f"Failed to create tag '{tag_name}': {create_tag_response.text}"
                )
                return

            tag_id = create_tag_response.json().get("id")
            if not tag_id:
                logging.error(f"No tag ID returned for tag '{tag_name}'")
                return

            logging.info(f"Created tag '{tag_name}' with ID {tag_id}")

            # Step 2: Assign the tag to the conversation
            assign_tag_url = f"{self.base_url}/conversations/{conversation_id}/tags"
            assign_tag_payload = {"id": tag_id, "admin_id": admin_id}

            assign_tag_response = self.rate_limiter.make_request_with_retry(
                assign_tag_url,
                "POST",
                headers=self._get_headers(),
                json=assign_tag_payload,
            )

            if assign_tag_response.status_code in [200, 201]:
                logging.info(
                    f"Assigned tag '{tag_name}' to conversation {conversation_id}"
                )
            else:
                logging.error(
                    f"Failed to assign tag '{tag_name}' to conversation {conversation_id}: {assign_tag_response.text}"
                )

        except Exception as e:
            logging.error(
                f"Error creating or assigning tag '{tag_name}' to conversation {conversation_id}: {e}"
            )

    # ===================================================================
    # ASYNC FUNCTIONS FOR FASTAPI
    # ===================================================================


async def get_universal_attachment_service() -> Optional[UniversalAttachmentService]:
    """Create fresh UniversalAttachmentService instance"""
    try:
        service = UniversalAttachmentService(max_concurrent_downloads=10)
        await service.__aenter__()
        return service
    except Exception as e:
        logging.error(f"Failed to initialize UniversalAttachmentService: {e}")
        return None


async def create_intercom_conversation_async(body: Dict, headers, defaultValues: Dict = None) -> Dict:
    """Async version of Intercom conversation creation."""
    if defaultValues is None:
        defaultValues = {}
    
    attachment_service = None
    try:
        # Log the received defaultValues for debugging
        if defaultValues:
            logging.info(f"Received defaultValues in create_intercom_conversation_async: {defaultValues}")
        
        headers_dict = convert_headers_to_dict(headers)

        # Validate required headers
        api_key = (
            headers_dict.get("token")
            or headers_dict.get("api_key")
            or headers_dict.get("apikey")
        )
        if not api_key:
            logging.error("Missing API key in headers")
            return {
                "status_code": 400,
                "body": "Missing API key in headers",
            }

        # Get fresh attachment service
        attachment_service = await get_universal_attachment_service()

        if not attachment_service:
            logging.warning(
                "Could not initialize AttachmentService - proceeding without attachment processing"
            )

        # Create connector with attachment service
        config = ConnectorConfig(
            domainUrl=headers_dict.get(
                "domainUrl"
            ),  # Not used for Intercom but kept for compatibility
            api_key=api_key,
        )

        # Add Intercom-specific config
        config.admin_id = headers_dict.get("admin_id")
        config.team_id = headers_dict.get("team_id")
        config.bot_admin_id = headers_dict.get("bot_admin_id")

        connector = UniversalIntercomConnector(config, attachment_service)

        # Create conversation using universal method
        response = await connector.create_conversation_universal(body, headers)
        return standardize_response_format(response, headers)

    except Exception as e:
        logging.error(f"Error in create_intercom_conversation_async: {e}")
        logging.error(f"Full traceback: {traceback.format_exc()}")
        return {
            "status_code": 500,
            "body": str(e),
        }
    finally:
        if attachment_service:
            try:
                await attachment_service.__aexit__(None, None, None)
            except Exception as e:
                logging.error(f"Error cleaning up attachment service: {e}")


async def create_intercom_ticket_async(body: Dict, headers, defaultValues: Dict = None) -> Dict:
    """Async version of Intercom ticket creation."""
    if defaultValues is None:
        defaultValues = {}
    
    attachment_service = None
    try:
        # Log the received defaultValues for debugging
        if defaultValues:
            logging.info(f"Received defaultValues in create_intercom_ticket_async: {defaultValues}")
        
        headers_dict = convert_headers_to_dict(headers)

        # Validate required headers
        api_key = (
            headers_dict.get("token")
            or headers_dict.get("api_key")
            or headers_dict.get("apikey")
        )
        if not api_key:
            logging.error("Missing API key in headers")
            return {
                "status_code": 400,
                "body": "Missing API key in headers",
            }

        # Get fresh attachment service
        attachment_service = await get_universal_attachment_service()

        if not attachment_service:
            logging.warning(
                "Could not initialize AttachmentService - proceeding without attachment processing"
            )

        # Create connector with attachment service
        config = ConnectorConfig(
            domainUrl=headers_dict.get(
                "domainUrl"
            ),  # Not used for Intercom but kept for compatibility
            api_key=api_key,
        )

        # Add Intercom-specific config
        config.admin_id = headers_dict.get("admin_id")
        config.team_id = headers_dict.get("team_id")
        config.bot_admin_id = headers_dict.get("bot_admin_id")

        connector = UniversalIntercomConnector(config, attachment_service)

        # Create ticket using universal method
        response = await connector.create_ticket_universal(body, headers)
        return standardize_response_format(response, headers)

    except Exception as e:
        logging.error(f"Error in create_intercom_ticket_async: {e}")
        logging.error(f"Full traceback: {traceback.format_exc()}")
        return {
            "status_code": 500,
            "body": str(e),
        }
    finally:
        if attachment_service:
            try:
                await attachment_service.__aexit__(None, None, None)
            except Exception as e:
                logging.error(f"Error cleaning up attachment service: {e}")


"""Async version of Intercom message creation."""


async def create_intercom_message_async(
    queryParams: List[Dict], body: Dict, headers, defaultValues: Dict = None
) -> Dict:
    if defaultValues is None:
        defaultValues = {}
    
    attachment_service = None
    try:
        # Log the received defaultValues for debugging
        if defaultValues:
            logging.info(f"Received defaultValues in create_intercom_message_async: {defaultValues}")
        
        headers_dict = convert_headers_to_dict(headers)
        query_dict = convert_query_params_to_dict(queryParams)

        # Extract conversation ID from query parameters with better template resolution checking
        conversation_id = (
            query_dict.get("conversation_id")
            or query_dict.get("ticket_id")
            or query_dict.get("ticketId")
        )

        # Check if conversation_id is still a template placeholder
        if not conversation_id or (
            isinstance(conversation_id, str)
            and conversation_id.startswith("{{")
            and conversation_id.endswith("}}")
        ):
            return {
                "status_code": 400,
                "body": f"Invalid conversation_id - template not resolved: {conversation_id}",
            }

        # Create connector
        config = ConnectorConfig(
            domainUrl=headers_dict.get("domainUrl"),
            api_key=headers_dict.get("token")
            or headers_dict.get("api_key")
            or headers_dict.get("apikey"),
        )

        # Get fresh attachment service
        attachment_service = await get_universal_attachment_service()

        config.admin_id = body.get("admin_id")
        config.team_id = body.get("team_id")
        config.bot_admin_id = body.get("bot_admin_id")

        connector = UniversalIntercomConnector(config, attachment_service)

        # ticket_state = None
        # url = f"https://api.intercom.io/conversations/{conversation_id}"

        # headers = {
        #   'Authorization': f'Bearer {config.api_key}',
        #   'Intercom-Version': '2.13',
        #   'Accept': 'application/json'
        # }

        # response = connector.rate_limiter.make_request_with_retry(url, 'GET', headers=headers)

        # if response.status_code == 200:
        #     conversation_data = response.json()
        #     ticket_state = conversation_data.get('state', 'open')

        # Extract message data from body
        message_body = (
            body.get("body") or body.get("message") or body.get("content", "")
        )
        if not message_body.strip():
            message_body = "[No content]"  # Fallback for empty messages

        # ========================================
        # Dom 11/1 - INLINE IMAGE PROCESSING - MESSAGE
        # ========================================
        attachment_files = []  # Initialize for both inline and explicit attachments
        if message_body and message_body != "[No content]" and attachment_service:
            try:
                inline_processor = UniversalInlineProcessor(attachment_service)

                # Build auth config from headers
                auth_config = connector._build_universal_auth_config(headers_dict)

                # Process inline images with source authentication
                processed_body, inline_attachments = (
                    await inline_processor.process_inline_images_universal(
                        content=message_body,
                        source_domain=headers_dict.get("domainUrl"),
                        auth_config=auth_config,
                    )
                )

                # Update message body with processed version (image refs replaced)
                message_body = processed_body

                # Add inline image attachments to the attachment_files list
                if inline_attachments:
                    # Validate inline attachments before adding
                    valid_inline = [
                        att
                        for att in inline_attachments
                        if isinstance(att, dict)
                        and att.get("type") == "file"
                        and "data" in att
                    ]
                    invalid_count = len(inline_attachments) - len(valid_inline)
                    if invalid_count > 0:
                        logging.error(
                            f"❌ Filtered out {invalid_count} invalid inline attachments"
                        )
                    attachment_files.extend(valid_inline)

            except Exception as e:
                logging.error(f"Failed to process inline images: {e}")

        # Handle admin/user identification and fallbacks
        created_at = body.get("created_at") or body.get("timestamp", int(time.time()))
        author_type = body.get("type", "user")  # Default to user
        contact_id = (
            body.get("contact_id")
            or body.get("user_id")
            or body.get("intercom_user_id")
        )
        admin_id = body.get("admin_id")
        DEFAULT_ADMIN_EMAIL = defaultValues.get("email")
        is_note = body.get("message_type") == "note" or body.get("is_private") == True
        author_email = (
            body.get("author_email") or body.get("from_email") or body.get("email")
        )
        author_name = body.get("author_name") or body.get("from_name", "Unknown User")

        # Handle private notes FIRST - they must always be from admins
        if is_note and author_type == "user":
            # Private note from a user - convert to admin
            logging.warning(
                f"Private note from user {author_name} ({author_email}) "
                f"- must convert to admin (Intercom constraint)"
            )
            
            # Try to find if this user is actually an admin
            if author_email:
                admin_id = connector._get_admin_id_by_email(author_email)
                if admin_id:
                    author_type = "admin"
                    print(f"User is actually an admin - using admin ID {admin_id}")
                else:
                    if DEFAULT_ADMIN_EMAIL:
                        # Use default admin for this private note
                        admin_id = connector._get_admin_id_by_email(DEFAULT_ADMIN_EMAIL)
                        if admin_id:
                            author_type = "admin"
                            logging.warning(
                                f"Converting to default admin {DEFAULT_ADMIN_EMAIL} for private note"
                            )
                        else:
                            logging.error(f"CRITICAL: Default admin {DEFAULT_ADMIN_EMAIL} not found!")
                            return {
                                "status_code": 400,
                                "body": f"Cannot create private note: default admin not found"
                            }

        # Now handle regular cases for missing IDs
        if author_type == "admin" and not admin_id:
            # Need to find admin_id for admin messages/notes
            if author_email:
                admin_id = connector._get_admin_id_by_email(author_email)
                if admin_id:
                    print(f"Found admin ID {admin_id} for {author_name}")
                else:
                    logging.error(f"Admin {author_name} ({author_email}) not found in Intercom")
                    # For notes, this was already handled above, but for regular admin messages:
                    if not is_note:
                        return {
                            "status_code": 400,
                            "body": f"Admin {author_email} not found in Intercom"
                        }

        if author_type == "user" and not contact_id:
            # Create or find contact for user messages
            if author_email:
                try:
                    contact = connector.get_or_create_contact(author_email, author_name)
                    if contact:
                        contact_id = contact["id"]
                        print(f"Created/found contact ID {contact_id} for {author_name}")
                    else:
                        logging.warning(
                            f"Could not create contact for {author_email}, using admin fallback"
                        )
                        admin_id = connector._get_admin_id_by_email(author_email)
                        if admin_id:
                            author_type = "admin"
                except Exception as e:
                    logging.warning(f"Failed to create contact: {e}, using admin fallback")
                    admin_id = connector._get_admin_id_by_email(author_email)
                    if admin_id:
                        author_type = "admin"
            else:
                logging.warning(
                    "No author_email provided for user message, using admin fallback"
                )
                # This shouldn't happen, but fallback to current admin
                admin_id = connector._get_current_admin_id()
                if admin_id:
                    author_type = "admin"

        if contact_id and "@" in str(contact_id):
            # If contact_id looks like an email, convert it to actual contact ID
            try:
                contact = connector.get_or_create_contact(
                    contact_id, body.get("author_name", "Unknown User")
                )
                if contact:
                    original_email = contact_id
                    contact_id = contact["id"]
                    author_type = "user"
                    logging.info(f"Converted email {original_email} to Intercom contact ID {contact_id}")
                else:
                    logging.warning(
                        f"Could not convert email {contact_id} to contact, using admin fallback"
                    )
                    admin_id = connector._get_admin_id_by_email(author_email)
                    if admin_id:
                        author_type = "admin"
                    contact_id = None
            except Exception as e:
                logging.warning(
                    f"Failed to convert email to contact: {e}, using admin fallback"
                )
                admin_id = connector._get_admin_id_by_email(author_email)
                if admin_id:
                    author_type = "admin"
                contact_id = None

        # Final validation: Ensure we have the required IDs (LAST RESORT)
        if author_type == "admin" and not admin_id:
            logging.error(
                f"FINAL CHECK: Admin message requires admin_id but none provided. Author: {author_name}, Email: {author_email}"
            )
            # Try to get default admin as absolute last resort
            admin_id = connector._get_current_admin_id()
            if not admin_id:
                return {
                    "status_code": 400,
                    "body": f"Cannot create admin message: no admin_id available for {author_name} ({author_email})",
                }
            
        elif author_type == "user" and not contact_id:
            logging.error(
                f"FINAL CHECK: User message requires contact_id but none provided. Author: {author_name}, Email: {author_email}"
            )
            # This should rarely happen since we tried above, but try one more time
            if author_email:
                try:
                    contact = connector.get_or_create_contact(
                        author_email, author_name or "Unknown User"
                    )
                    if contact:
                        contact_id = contact["id"]
                        logging.info(f"Created contact in final validation: {contact_id}")
                    else:
                        return {
                            "status_code": 400,
                            "body": f"Cannot create user message: unable to create contact for {author_email}",
                        }
                except Exception as e:
                    return {
                        "status_code": 400,
                        "body": f"Cannot create user message: failed to create contact for {author_email} - {e}",
                    }
            else:
                return {
                    "status_code": 400,
                    "body": f"Cannot create user message: no contact_id or email provided for {author_name}",
                }

        logging.info(
            f"Final message params: author_type={author_type}, admin_id={admin_id}, contact_id={contact_id}"
        )

        # Process attachments if present
        # Dom - 11/1 - # Note: attachment_files may already contain inline images from earlier processing
        # attachment_files = []
        isFormattedAttachments = False
        # Handle both plural and singular attachment fields
        if (
            body.get("attachment_files")
            or body.get("attachments")
            or body.get("attachment")
            or (attachment_files and len(attachment_files) > 0)
        ):
            try:
                raw_attachments = body.get("attachment_files") or body.get(
                    "attachments", []
                )
                if not raw_attachments and body.get("attachment"):
                    # Single attachment provided - convert to array
                    raw_attachments = [body.get("attachment")]

                if raw_attachments:
                    # Check if attachments have required fields for formatted processing
                    all_have_required_fields = True
                    for attachment in raw_attachments:
                        if isinstance(attachment, dict):
                            has_content_url = bool(
                                attachment.get("content_url")
                                or attachment.get("url")
                                or attachment.get("attachment_url")
                                or attachment.get("mapped_content_url")
                            )
                            has_content_type = bool(
                                attachment.get("content_type")
                            ) or bool(attachment.get("content_content_type"))
                            has_file_name = bool(
                                attachment.get("file_name")
                                or attachment.get("content_file_name")
                                or attachment.get("name")
                            )

                            if not (
                                has_content_url and has_content_type and has_file_name
                            ):
                                all_have_required_fields = False
                                break

                    if not all_have_required_fields:
                        # Just append content URLs if required fields are missing
                        isFormattedAttachments = False
                        logging.warning(
                            f"⚠️ Skipping {len(raw_attachments)} attachments with missing required fields"
                        )
                        # Dom 11/4 Skip attachments with missing fields - don't add URL strings
                        # for attachment in raw_attachments:
                        #     fileUrl = (
                        #         attachment.get("content_url")
                        #         or attachment.get("url")
                        #         or attachment.get("attachment_url")
                        #         or attachment.get("mapped_content_url")
                        #     )
                        #     if fileUrl:
                        #         attachment_files.append(fileUrl)  # APPEND
                    else:
                        # Process formatted attachments
                        auth_config = connector._build_universal_auth_config(
                            headers_dict
                        )
                        if raw_attachments and attachment_service:
                            # Process attachments using the universal service
                            processed_attachments, failed_attachments = (
                                await connector._process_attachments_universal(
                                    raw_attachments, auth_config
                                )
                            )

                            # Validate processed attachments before adding
                            valid_processed = [
                                att
                                for att in processed_attachments
                                if isinstance(att, dict)
                                and att.get("type") == "file"
                                and "data" in att
                            ]
                            invalid_count = len(processed_attachments) - len(
                                valid_processed
                            )
                            if invalid_count > 0:
                                logging.error(
                                    f"❌ Filtered out {invalid_count} invalid processed attachments"
                                )
                            attachment_files.extend(
                                valid_processed
                            )  # EXTEND only validated
                            logging.info(
                                f"Successfully processed {len(valid_processed)} explicit attachments"
                            )

                            if failed_attachments:
                                logging.warning(
                                    f"Failed to process {len(failed_attachments)} attachments: {failed_attachments}"
                                )

                        elif raw_attachments:
                            # Fallback: try to convert existing attachment data directly
                            # Don't reset attachment_files - keep inline attachments
                            for attachment in raw_attachments:
                                if isinstance(attachment, dict):
                                    attachment_files.append(  # APPEND
                                        {
                                            "type": "file",
                                            "name": (
                                                attachment.get("file_name")
                                                or attachment.get("content_file_name")
                                                or attachment.get("name")
                                                or "attachment"
                                            ),
                                            "url": (
                                                attachment.get("content_url")
                                                or attachment.get("attachment_url")
                                                or attachment.get("url")
                                                or ""
                                            ),
                                            "content_type": (
                                                attachment.get("content_type")
                                                or attachment.get(
                                                    "content_content_type"
                                                )
                                                or "application/octet-stream"
                                            ),
                                        }
                                    )

                        isFormattedAttachments = True

                elif attachment_files and len(attachment_files) > 0:
                    isFormattedAttachments = True
                    logging.info(
                        f"Using {len(attachment_files)} inline attachments (no explicit attachments)"
                    )

            except Exception as e:
                logging.warning(f"Failed to process explicit attachments: {e}")
                # Don't reset attachment_files - keep inline attachments even if explicit processing fails

        # Final validation: Remove any URL strings that got through
        validated_attachments = [
            att
            for att in attachment_files
            if isinstance(att, dict) and att.get("type") == "file" and "data" in att
        ]
        removed_count = len(attachment_files) - len(validated_attachments)
        if removed_count > 0:
            logging.error(
                f"❌ FINAL VALIDATION: Removed {removed_count} invalid attachments (URL strings or malformed objects)"
            )
        attachment_files = validated_attachments

        # Dom - 11/4 - Check if attachments have 'data' field (base64) or 'url' field
        if attachment_files:
            # If ANY attachment has 'data' field, they're formatted
            has_data = any(att.get("data") for att in attachment_files)
            has_url = any(att.get("url") for att in attachment_files)

            if has_data and not has_url:
                isFormattedAttachments = True
                logging.info(
                    f"✅ Attachments are base64-formatted (data field present)"
                )
            elif has_url and not has_data:
                isFormattedAttachments = False
                logging.info(f"✅ Attachments are URL-based (url field present)")
            else:
                # Mixed or unclear - default to True if any have data
                isFormattedAttachments = has_data
                logging.warning(
                    f"⚠️ Mixed attachment types - using isFormattedAttachments={isFormattedAttachments}"
                )
        else:
            isFormattedAttachments = False

        # Call the internal API method with correct parameters
        response = connector._add_conversation_reply_api_call(
            conversation_id=conversation_id,
            body=message_body,
            created_at=created_at,
            author_type=author_type,
            message_type=body.get(
                "message_type", "note"
            ),  # body.get("public", True) and "comment" or "note",
            contact_id=contact_id,  # if author_type == "user" else None,
            admin_id=admin_id,  # if author_type == "admin" else None,
            attachment_files=attachment_files,
            is_formatted_attachments=isFormattedAttachments,
        )

        # Check if response is None (API call failed)
        if response is None:
            logging.error("API call returned None response")
            return {
                "status_code": 500,
                "body": "Failed to create message - API call returned None response",
            }

        # Create ConnectorResponse for standardization
        if response.status_code in [200, 201]:
            response_data = response.json()
            # Add display_id for migration framework compatibility
            if "id" in response_data:
                response_data["display_id"] = response_data["id"]

            connector_response = ConnectorResponse(
                status_code=response.status_code, success=True, data=response_data
            )
            # Checking if the last ticket is closed
            if (
                is_last_ticket_closed.get("id")
                and is_last_ticket_closed.get("id") == conversation_id
            ) and is_last_ticket_closed.get("closed"):

                get_ticket_url = f"{connector.base_url}/tickets/{conversation_id}"
                response = connector.rate_limiter.make_request_with_retry(
                    get_ticket_url, "GET", headers=connector._get_headers()
                )
                if response.status_code == 200:
                    ticket_resp = response.json()
                    ticket_type = ticket_resp.get("type")
                else:
                    get_conversation_url = (
                        f"{connector.base_url}/conversations/{conversation_id}"
                    )
                    response = connector.rate_limiter.make_request_with_retry(
                        get_conversation_url, "GET", headers=connector._get_headers()
                    )
                    if response.status_code == 200:
                        conversation_resp = response.json()
                        ticket_type = conversation_resp.get("type")

                connector._close_conversation(
                    conversation_id, admin_id, None, ticket_type=ticket_type
                )
                logging.info(f"Conversation {conversation_id} is closed")
        else:
            connector_response = ConnectorResponse(
                status_code=response.status_code,
                success=False,
                error_message=f"Failed to create message: {response.text}",
            )

        return standardize_response_format(connector_response, headers)

    except Exception as e:
        logging.error(f"Error in create_intercom_message_async: {e}")
        logging.error(f"Traceback: {traceback.format_exc()}")
        return {
            "status_code": 500,
            "body": str(e),
        }
    finally:
        if attachment_service:
            try:
                await attachment_service.__aexit__(None, None, None)
            except Exception as e:
                logging.error(f"Error cleaning up attachment service: {e}")


# ===================================================================
# SYNCHRONOUS WRAPPER FUNCTIONS
# ===================================================================


def finalize_closed_ticket_universal(body: Dict, headers) -> Dict:
    """
    Universal function to close a ticket after all messages have been added.

    Call this from your migration framework AFTER all messages have been added
    to a ticket that should be closed.

    Expected body format:
    {
        "conversation_id": "12345",      # required - Intercom ticket/conversation ID
        "admin_id": "67890",              # optional - Admin ID for closing
        "ticket_state_id": 4              # optional - Specific ticket state ID
    }

    Returns:
        Dict with status_code and body
    """
    try:
        headers_dict = convert_headers_to_dict(headers)
        api_key = (
            headers_dict.get("token")
            or headers_dict.get("api_key")
            or headers_dict.get("apikey")
        )

        if not api_key:
            return {
                "status_code": 400,
                "body": "Missing API key in headers",
            }

        # Create connector instance
        config = ConnectorConfig(domainUrl="", api_key=api_key)
        config.admin_id = headers_dict.get("admin_id")
        config.team_id = headers_dict.get("team_id")

        connector = UniversalIntercomConnector(config)

        conversation_id = body.get("conversation_id")
        if not conversation_id:
            return {
                "status_code": 400,
                "body": "Missing required field: conversation_id",
            }

        admin_id = body.get("admin_id") or headers_dict.get("admin_id")
        ticket_state_id = body.get("ticket_state_id")

        # Call finalize method
        connector.finalize_closed_ticket(conversation_id, admin_id, ticket_state_id)

        return {
            "status_code": 200,
            "body": {
                "success": True,
                "conversation_id": conversation_id,
                "status": "closed",
            },
        }

    except Exception as e:
        logging.error(f"Error in finalize_closed_ticket_universal: {e}")
        logging.error(f"Traceback: {traceback.format_exc()}")
        return {
            "status_code": 500,
            "body": {"error": str(e)},
        }


def create_intercom_conversation_universal(body: Dict, headers, defaultValues: Dict = None) -> Dict:
    """
    Universal Intercom conversation creation.
    Works with conversations/messages from ANY source platform.
    """
    if defaultValues is None:
        defaultValues = {}
    
    try:
        # Check if we're in an async context
        try:
            loop = asyncio.get_running_loop()

            # We're in an async context, create a new event loop in a separate thread
            def run_in_new_loop():
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                try:
                    return new_loop.run_until_complete(
                        create_intercom_conversation_async(body, headers, defaultValues)
                    )
                finally:
                    new_loop.close()

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(run_in_new_loop)
                return future.result()

        except RuntimeError:
            # No running loop, we can run directly
            return asyncio.run(create_intercom_conversation_async(body, headers, defaultValues))

    except Exception as e:
        logging.error(f"Error in create_intercom_conversation_universal: {e}")
        logging.error(f"Traceback: {traceback.format_exc()}")
        return {
            "status_code": 500,
            "body": str(e),
        }


"""
Universal Intercom ticket creation.
Works with ticket data from ANY source platform.
"""


def create_intercom_ticket_universal(body: Dict, headers, defaultValues: Dict = None) -> Dict:
    if defaultValues is None:
        defaultValues = {}
    
    try:
        # Check if we're in an async context
        # try:
        #     loop = asyncio.get_running_loop()
        #     # We're in an async context, use ThreadPoolExecutor
        #     print(
        #         f"[DEBUG] CONSOLE DEBUG: Running in async context, using ThreadPoolExecutor"
        #     )
        #     with concurrent.futures.ThreadPoolExecutor() as executor:
        #         future = executor.submit(
        #             lambda: asyncio.run(create_intercom_ticket_async(body, headers))
        #         )
        #         result = future.result()
        #         print(f"[DEBUG] CONSOLE DEBUG: ThreadPoolExecutor result: {result}")
        #         return result
        # except RuntimeError:
        #     # No running loop, we can run directly
        #     print(f"[DEBUG] CONSOLE DEBUG: No running loop, running directly")
        #     result = asyncio.run(create_intercom_ticket_async(body, headers))
        #     print(f"[DEBUG] CONSOLE DEBUG: Direct run result: {result}")
        #     return result

        try:
            loop = asyncio.get_running_loop()

            # We're in an async context, create a new event loop in a separate thread
            def run_in_new_loop():
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                try:
                    return new_loop.run_until_complete(
                        create_intercom_ticket_async(body, headers, defaultValues)
                    )
                finally:
                    new_loop.close()

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(run_in_new_loop)
                return future.result()

        except RuntimeError:
            # No running loop, we can run directly
            return asyncio.run(create_intercom_ticket_async(body, headers, defaultValues))

    except Exception as e:
        logging.error(f"Error in create_intercom_ticket_universal: {e}")
        logging.error(f"Traceback: {traceback.format_exc()}")
        return {
            "status_code": 500,
            "body": str(e),
        }


"""
Universal Intercom message creation.
Works with messages from ANY source platform.
"""


def create_intercom_message_universal(
    queryParams: List[Dict], body: Dict, headers, defaultValues: Dict = None
) -> Dict:
    if defaultValues is None:
        defaultValues = {}
    
    try:
        # Check if we're in an async context
        try:
            loop = asyncio.get_running_loop()

            # We're in an async context, create a new event loop in a separate thread
            def run_in_new_loop():
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                try:
                    return new_loop.run_until_complete(
                        create_intercom_message_async(queryParams, body, headers, defaultValues)
                    )
                finally:
                    new_loop.close()

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(run_in_new_loop)
                return future.result()

        except RuntimeError:
            # No running loop, we can run directly
            return asyncio.run(
                create_intercom_message_async(queryParams, body, headers, defaultValues)
            )

    except Exception as e:
        logging.error(f"Error in create_intercom_message_universal: {e}")
        logging.error(f"Traceback: {traceback.format_exc()}")
        return {
            "status_code": 500,
            "body": str(e),
        }


# ===================================================================
# VALIDATION AND UTILITY FUNCTIONS
# ===================================================================


def validate_intercom_instance_v1(headers) -> Dict:
    """Validate Intercom instance and credentials"""
    try:
        headers_dict = convert_headers_to_dict(headers)
        config = ConnectorConfig(
            domainUrl="",  # Not needed for Intercom
            api_key=headers_dict.get("apikey") or headers_dict.get("api_key"),
        )
        connector = UniversalIntercomConnector(config)
        is_valid = connector._validate_instance()

        return {
            "status_code": 200 if is_valid else 401,
            "body": {"valid": is_valid},
            "headers": headers,
        }
    except Exception as e:
        logging.error(f"Error validating Intercom instance: {e}")
        return {"status_code": 500, "body": {"valid": False, "error": str(e)}}


def get_intercom_mapping_objects_v1(headers) -> Dict:
    """Get Intercom mapping objects (admins, teams, etc.)"""
    try:
        headers_dict = convert_headers_to_dict(headers)
        api_key = headers_dict.get("apikey") or headers_dict.get("api_key")

        config = ConnectorConfig(domainUrl="", api_key=api_key)
        connector = UniversalIntercomConnector(config)

        base_url = "https://api.intercom.io"
        headers_api = connector._get_headers()

        # Get admins
        admins_url = f"{base_url}/admins"
        admins_response = connector.rate_limiter.make_request_with_retry(
            admins_url, "GET", headers=headers_api
        )

        # Get teams
        teams_url = f"{base_url}/teams"
        teams_response = connector.rate_limiter.make_request_with_retry(
            teams_url, "GET", headers=headers_api
        )

        mapping_data = {}

        if admins_response.status_code == 200:
            mapping_data["admins"] = admins_response.json()

        if teams_response.status_code == 200:
            mapping_data["teams"] = teams_response.json()

        return {"status_code": 200, "body": mapping_data, "headers": headers}

    except Exception as e:
        logging.error(f"Error getting Intercom mapping objects: {e}")
        return {"status_code": 500, "body": {"error": str(e)}}


# ===================================================================
# BACKWARD COMPATIBILITY FUNCTIONS
# ===================================================================

"""Backward compatible conversation creation function"""


def create_intercom_conversation(**kwargs) -> Dict:
    return create_intercom_conversation_universal(
        kwargs["body"], kwargs["targetHeaders"], kwargs.get("defaultValues", {})
    )


"""Backward compatible message creation function"""


def create_intercom_message(**kwargs) -> Dict:
    return create_intercom_message_universal(
        kwargs["queryParams"], kwargs["body"], kwargs["targetHeaders"], kwargs.get("defaultValues", {})
    )


"""Backward compatible ticket creation function"""


def create_intercom_ticket(**kwargs) -> Dict:
    body = kwargs.get("body", {})
    headers = kwargs.get("targetHeaders") or kwargs.get("headers", [])
    defaultValues = kwargs.get("defaultValues", {})
    return create_intercom_ticket_universal(body, headers, defaultValues)


# ===================================================================
# EXAMPLE USAGE
# ===================================================================

if __name__ == "__main__":
    import json

    # Example headers
    headers = [
        {
            "key": "apikey",
            "value": "your-intercom-access-token",
            "description": "",
            "req": True,
        },
        {"key": "admin_id", "value": "12345", "description": "", "req": False},
        {"key": "team_id", "value": "67890", "description": "", "req": False},
        {
            "key": "username",
            "value": "john@example.com/token",
            "description": "",
            "req": False,
        },
        {
            "key": "password",
            "value": "source-api-token",
            "description": "",
            "req": False,
        },
    ]

    # Example conversation data with multiple messages
    conversation_data = {
        "messages": [
            {
                "actor_email": "customer@example.com",
                "actor_name": "John Doe",
                "actor_type": "user",
                "body": "Hello, I need help with my account",
                "created_at": int(time.time()) - 3600,
                "attachments": [],
            },
            {
                "actor_email": "agent@company.com",
                "actor_name": "Jane Agent",
                "actor_type": "agent",
                "body": "Hi John, I'd be happy to help you with your account. What specific issue are you experiencing?",
                "created_at": int(time.time()) - 3500,
                "attachments": [],
            },
            {
                "actor_email": "customer@example.com",
                "actor_name": "John Doe",
                "actor_type": "user",
                "body": "I can't access my billing section",
                "created_at": int(time.time()) - 3400,
                "attachments": [
                    {
                        "url": "https://example.com/screenshot.png",
                        "name": "screenshot.png",
                        "content_type": "image/png",
                    }
                ],
            },
        ],
        "auto_close": False,
        "custom_attributes": {
            "source_platform": "migration_tool",
            "priority": "medium",
        },
    }

    # Example ticket data
    ticket_data = {
        "email": "customer@example.com",
        "name": "John Doe",
        "tags": ["urgent", "billing"],
        "admin_id": "12345",
        "team_id": "67890",
        "ticket_type_id": 123,
        "_default_title_": "Billing Issue - Cannot Access Account",
        "_default_description_": "Customer reports they cannot access their billing section and need assistance with their account.",
        "custom_attributes": {
            "source_platform": "migration_tool",
            "priority": "high",
            "customer_tier": "premium",
        },
    }

    # Test conversation creation
    print("=== Testing Conversation Creation ===")
    result = create_intercom_conversation_universal(conversation_data, headers)
    print(json.dumps(result, indent=2))

    # Test ticket creation
    print("\n=== Testing Ticket Creation ===")
    ticket_result = create_intercom_ticket_universal(ticket_data, headers)
    print(json.dumps(ticket_result, indent=2))