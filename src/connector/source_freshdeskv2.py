"""
freshdesk.py
============

🚀 OPTIMIZED FRESHDESK CONNECTOR
- Handles Freshdesk's 300-page × 100-ticket limit with sliding window pagination
- Two-stage fetching: bulk IDs + parallel individual ticket details
- Conservative rate limiting (configurable, starting at 2 workers)
- Enhanced error handling with 429 retry logic
- Thread-safe and migration-safe for parallel execution
"""

import requests
import re
import logging
import time
import os
import json
import threading
import concurrent.futures
from typing import List, Dict, Tuple, Optional, Any, Set
from datetime import datetime, timedelta
from src.connector.utils.generic_record_filter import (
    RecordFilter,
    create_filters_from_params,
)
import base64

# Import base classes and utilities (same pattern as Zendesk)
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
        handle_common_errors,
    )
except ImportError:
    # Fallback - define minimal base classes if base_source_connector isn't available
    class BaseSourceRateLimitHandler:
        def is_rate_limited(self, response):
            return response.status_code == 429

        def get_retry_delay(self, response):
            return 60

        def make_request_with_retry(self, url, method="GET", **kwargs):
            return requests.request(method, url, **kwargs)

    class BaseDataEnricher:
        def enrich_tickets(self, tickets, api_response):
            return tickets

        def enrich_conversations(self, conversations, users):
            return conversations

        def find_object_by_id(self, objects, target_id, id_field="id"):
            return next(
                (obj for obj in objects if obj.get(id_field) == target_id), None
            )

        def safe_get_field(self, obj, field, default=None):
            return obj.get(field, default) if obj else default

    class BaseFieldMapper:
        def get_standard_field_mapping(self):
            return {}

        def process_custom_fields(self, custom_fields, field_definitions):
            return {}

        def find_object_by_id(self, objects, target_id, id_field="id"):
            return next(
                (obj for obj in objects if obj.get(id_field) == target_id), None
            )

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
            return {param["key"]: param["value"] for param in query_params}
        return query_params

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
                    "error": response_dict.error_message,
                }
        else:
            # It's already a dict, return as-is but ensure consistent format
            if response_dict.get("success", response_dict.get("status_code") == 200):
                return {
                    "status_code": response_dict.get("status_code", 200),
                    "body": response_dict.get("data", response_dict.get("body", {})),
                }
            else:
                return {
                    "status_code": response_dict.get(
                        "status_code", response_dict.get("status_code", 500)
                    ),
                    "error": response_dict.get(
                        "error_message", response_dict.get("error", "Unknown error")
                    ),
                }

    # Data classes
    class SourceConnectorConfig:
        def __init__(self, domainUrl=None, username=None, password=None, **kwargs):
            self.domainUrl = domainUrl
            self.username = username  # API key
            self.password = password  # "X"
            self.page_size = kwargs.get("page_size", 100)

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


class FreshdeskRateLimitHandler(BaseSourceRateLimitHandler):
    """
    🚀 FRESHDESK-SPECIFIC RATE LIMIT HANDLER
    Conservative approach with configurable thread pools and exponential backoff
    """

    def __init__(
        self, thread_pool_size=2, max_retries=5, base_delay=0.5
    ):  # Dom - Changed base_delay from 2 to 0.5 and Max Retries from 3 to 5
        self.thread_pool_size = thread_pool_size  # Very conservative default
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.logger = logging.getLogger(__name__)

    def is_rate_limited(self, response: requests.Response) -> bool:
        """Check if response indicates rate limiting"""
        return response.status_code == 429

    def get_retry_delay(self, response: requests.Response) -> int:
        """Extract retry delay from Freshdesk response headers"""
        # Freshdesk uses Retry-After header
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return int(retry_after)
            except ValueError:
                pass

        # Check for X-RateLimit headers
        rate_limit_reset = response.headers.get("X-RateLimit-Reset")
        if rate_limit_reset:
            try:
                reset_timestamp = int(rate_limit_reset)
                current_timestamp = int(time.time())
                return max(0, reset_timestamp - current_timestamp)
            except ValueError:
                pass

        return 60  # Default 1 minute

    def make_request_with_retry(
        self, url: str, method: str = "GET", **kwargs
    ) -> requests.Response:
        """
        🚀 ENHANCED RETRY LOGIC: Exponential backoff for 429s, log and continue for others
        """
        for attempt in range(self.max_retries + 1):
            try:
                response = requests.request(method, url, **kwargs)

                # Success - return immediately
                if response.status_code < 400:
                    return response

                # Rate limited - retry with exponential backoff
                if response.status_code == 429:
                    if attempt < self.max_retries:
                        retry_delay = self.get_retry_delay(response)
                        backoff_delay = self.base_delay * (2**attempt)
                        total_delay = max(retry_delay, backoff_delay)

                        self.logger.warning(
                            f"Rate limited (429) on {url}, retrying in {total_delay}s (attempt {attempt + 1}/{self.max_retries + 1})"
                        )
                        time.sleep(total_delay)
                        continue
                    else:
                        self.logger.error(
                            f"Rate limited (429) on {url}, max retries exceeded"
                        )
                        return response

                # Other 4xx/5xx errors - log and return (don't retry)
                else:
                    self.logger.warning(
                        f"HTTP {response.status_code} on {url}: {response.text[:200]}"
                    )
                    return response

            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries:
                    backoff_delay = self.base_delay * (2**attempt)
                    self.logger.warning(
                        f"Request exception on {url}: {e}, retrying in {backoff_delay}s"
                    )
                    time.sleep(backoff_delay)
                    continue
                else:
                    self.logger.error(
                        f"Request exception on {url}: {e}, max retries exceeded"
                    )
                    # Return a mock response for consistency
                    response = requests.Response()
                    response.status_code = 500
                    response._content = json.dumps({"error": str(e)}).encode()
                    return response

        # Should never reach here, but just in case
        response = requests.Response()
        response.status_code = 500
        response._content = json.dumps({"error": "Max retries exceeded"}).encode()
        return response


class FreshdeskPaginationManager:
    """
    🚀 FRESHDESK PAGINATION MANAGER
    Handles sliding window pagination to work around Freshdesk's 300-page limit
    """

    def __init__(self, per_page=100, max_pages=300):
        self.per_page = per_page
        self.max_pages = max_pages  # Freshdesk limit
        self.logger = logging.getLogger(__name__)

    def calculate_pagination_info(
        self, numberOfProcessedRecords: int, query_params: Dict
    ) -> Dict:
        """
        🚀 CALCULATE CURRENT POSITION: Determine page, window, and updated_since
        """
        try:
            per_page = int(query_params.get("per_page", self.per_page))
            numberOfProcessedRecords = int(numberOfProcessedRecords)
            # Get starting page from query params (defaults to 1)
            starting_page = int(query_params.get("page", 1))
        except (ValueError, TypeError):
            per_page = self.per_page
            numberOfProcessedRecords = 0
            starting_page = 1

        # Calculate global page number (accounting for starting page offset)
        records_based_page = (
            (numberOfProcessedRecords // per_page)
            if numberOfProcessedRecords > 0
            else 0
        )
        global_page = starting_page + records_based_page

        # Calculate which window we're in
        current_window = (
            ((numberOfProcessedRecords // (self.max_pages * per_page)) + 1)
            if numberOfProcessedRecords > 0
            else 1
        )

        # Calculate page within current window (accounting for starting page offset)
        records_in_current_window = numberOfProcessedRecords % (
            self.max_pages * per_page
        )
        page_offset_in_window = records_in_current_window // per_page

        # Honor starting page: if starting from 299, window should go 299, 300, 301... up to 598
        window_start_page = starting_page + (current_window - 1) * self.max_pages
        window_page = window_start_page + page_offset_in_window

        # Handle edge case where we're exactly at window boundary
        if window_page > self.max_pages:
            current_window += 1
            window_page = 1

        return {
            "per_page": per_page,
            "global_page": global_page,
            "current_window": current_window,
            "window_page": window_page,
            "numberOfProcessedRecords": numberOfProcessedRecords,
        }

    def should_slide_window(self, window_page: int, has_more: bool) -> bool:
        """Determine if we need to slide to the next window"""
        return window_page >= self.max_pages and has_more

    def calculate_next_updated_since(self, last_ticket_updated_at: str) -> str:
        """
        🚀 CALCULATE NEXT WINDOW START: Add 1 second to last ticket's updated_at
        """
        try:
            # Parse Freshdesk timestamp (ISO format)
            if last_ticket_updated_at:
                # Freshdesk uses ISO format: "2024-01-15T10:30:45Z"
                dt = datetime.fromisoformat(
                    last_ticket_updated_at.replace("Z", "+00:00")
                )
                next_dt = dt + timedelta(seconds=1)
                return next_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                return None
        except (ValueError, TypeError) as e:
            self.logger.error(
                f"Error parsing updated_at timestamp '{last_ticket_updated_at}': {e}"
            )
            return None


class FreshdeskCustomFieldProcessor:
    """
    🚀 ENHANCED FRESHDESK CUSTOM FIELD PROCESSOR
    Handles custom field title mapping with caching for performance
    Similar to Zendesk's custom field processing but adapted for Freshdesk API
    """

    def __init__(self, freshdesk_connector):
        self.connector = freshdesk_connector
        self.logger = logging.getLogger(__name__)

        # 🔒 MIGRATION-SAFE: Each processor has its own cache
        self._ticket_fields_cache = None
        self._cache_lock = threading.Lock()
        self.migration_id = getattr(freshdesk_connector, "migration_id", "unknown")

    def get_ticket_fields(self) -> List[Dict]:
        """
        🚀 CACHED TICKET FIELDS: Fetch Freshdesk ticket fields once and cache for the session
        """
        with self._cache_lock:
            if self._ticket_fields_cache is not None:
                return self._ticket_fields_cache

            try:
                self.logger.info(
                    f"Migration {self.migration_id}: Fetching Freshdesk ticket fields for custom field mapping"
                )

                domain_url = self.connector.config.domainUrl
                domain = self._extract_domain_from_url(domain_url)
                url = f"https://{domain}.freshdesk.com/api/v2/admin/ticket_fields"

                headers = {
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                }

                response = self._make_authenticated_request(url, "GET", headers=headers)

                if response.status_code == 200:
                    data = safe_json_response(response)
                    # Freshdesk returns ticket fields in different structure than Zendesk
                    ticket_fields = data if isinstance(data, list) else []

                    self._ticket_fields_cache = ticket_fields
                    self.logger.info(
                        f"Migration {self.migration_id}: Cached {len(ticket_fields)} Freshdesk ticket field definitions"
                    )

                    return ticket_fields
                else:
                    self.logger.error(
                        f"Migration {self.migration_id}: Failed to fetch Freshdesk ticket fields: {response.status_code}"
                    )
                    return []

            except Exception as e:
                self.logger.error(
                    f"Migration {self.migration_id}: Error fetching Freshdesk ticket fields: {e}"
                )
                return []

    def process_custom_fields_for_tickets(self, tickets: List[Dict]) -> List[Dict]:
        """
        🚀 PROCESS FRESHDESK CUSTOM FIELDS: Convert ID/value pairs to title/value pairs
        Handles Freshdesk's custom field structure which differs from Zendesk
        """
        if not tickets:
            return tickets

        # Get ticket field definitions once
        ticket_fields = self.get_ticket_fields()
        if not ticket_fields:
            self.logger.warning(
                f"Migration {self.migration_id}: No Freshdesk ticket fields available, skipping custom field processing"
            )
            return tickets

        # Create a lookup map for fast access - Freshdesk uses different field structure
        field_id_to_title = {}
        for field in ticket_fields:
            field_id = field.get("id")
            field_label = field.get("label") or field.get("name") or field.get("title")
            if field_id and field_label:
                field_id_to_title[field_id] = field_label

        self.logger.info(
            f"Migration {self.migration_id}: Processing custom fields for {len(tickets)} tickets using {len(field_id_to_title)} field definitions"
        )

        processed_tickets = []
        for ticket in tickets:
            processed_ticket = ticket.copy()

            # Process custom fields if they exist
            # Freshdesk structure: "custom_fields": {"field_123": "value", "field_456": "value"}
            custom_fields = ticket.get("custom_fields", {})
            if isinstance(custom_fields, dict) and custom_fields:
                custom_fields_obj = {}

                for field_key, field_value in custom_fields.items():
                    # Extract field ID from key (e.g., "field_123" -> 123)
                    try:
                        if field_key.startswith("field_"):
                            field_id = int(field_key.replace("field_", ""))
                        else:
                            field_id = int(field_key)

                        # Get the title for this custom field ID
                        field_title = field_id_to_title.get(field_id)

                        if field_title:
                            custom_fields_obj[field_title] = field_value
                        else:
                            # Fallback: use the original key if title not found
                            custom_fields_obj[field_key] = field_value

                    except (ValueError, TypeError):
                        # If field_key is not numeric, use as-is
                        custom_fields_obj[field_key] = field_value

                # Replace with processed object
                processed_ticket["custom_fields"] = custom_fields_obj
            else:
                # No custom fields or already processed - ensure consistent format
                processed_ticket["custom_fields"] = {}

            processed_tickets.append(processed_ticket)

        return processed_tickets

    def _extract_domain_from_url(self, domain_url: str) -> str:
        """Extract domain from full Freshdesk URL"""
        domain = domain_url.replace("https://", "").replace("http://", "")
        domain = domain.rstrip("/")
        return domain

    def _make_authenticated_request(
        self, url: str, method: str = "GET", **kwargs
    ) -> requests.Response:
        """Make authenticated request to Freshdesk API"""
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if "headers" in kwargs:
            headers.update(kwargs["headers"])
        kwargs["headers"] = headers

        auth = (self.connector.config.username, self.connector.config.password)
        kwargs["auth"] = auth

        return self.connector.rate_limiter.make_request_with_retry(
            url, method, **kwargs
        )


class FreshdeskTicketFetcher:
    """
    🚀 TWO-STAGE FRESHDESK TICKET FETCHER
    Stage 1: Bulk fetch ticket IDs
    Stage 2: Parallel fetch full ticket details
    """

    def __init__(self, freshdesk_connector):
        self.connector = freshdesk_connector
        self.logger = logging.getLogger(__name__)
        self.migration_id = getattr(freshdesk_connector, "migration_id", "unknown")

    def fetch_ticket_window(
        self, pagination_info: Dict, query_params: Dict
    ) -> Tuple[List[Dict], Dict]:
        """
        🚀 STAGE 1: Bulk fetch ticket IDs and basic data from /api/v2/tickets
        """
        try:
            # Build URL and parameters
            domain_url = self.connector.config.domainUrl
            # Extract domain from full URL
            domain = self._extract_domain_from_url(domain_url)
            url = f"https://{domain}.freshdesk.com/api/channel/v2/tickets"

            # Dom [TESTING] - For testing: Start from page 299 instead of page 1
            # test_start_page = 299
            # actual_page = test_start_page + pagination_info['window_page'] - 1

            # Build request parameters
            params = {
                "page": pagination_info["window_page"],
                #'page': actual_page,  # [TESTING] This will be 299 & 300 ← HARDCODED THIS FOR TESTING
                "per_page": pagination_info["per_page"],
                "include": "requester,stats",  # Include basic related data
            }

            # Add updated_since if provided (for window sliding)
            updated_since = query_params.get("updated_since")
            if updated_since:
                params["updated_since"] = updated_since

            # Add other query filters
            for key in ["order_by", "order_type", "company_id", "requester_id"]:
                if key in query_params:
                    params[key] = query_params[key]

            self.logger.info(
                f"Migration {self.migration_id}: Fetching ticket window - page {pagination_info['window_page']}, window {pagination_info['current_window']}"
            )

            # Make API call
            headers = self._build_headers()
            response = self.connector.rate_limiter.make_request_with_retry(
                url, "GET", headers=headers, params=params, auth=self._build_auth()
            )

            if response.status_code == 200:
                data = safe_json_response(response)
                tickets = data if isinstance(data, list) else []

                # Add requesterEmail field for each ticket
                for ticket in tickets:
                    if isinstance(ticket, dict):
                        requester = ticket.get("requester")
                        if isinstance(requester, dict):
                            ticket["requesterEmail"] = requester.get("email", "")
                        else:
                            ticket["requesterEmail"] = ""

                # Debug Info
                ticket_ids = [
                    ticket.get("id") for ticket in tickets if ticket.get("id")
                ]
                self.logger.info(
                    f"Migration {self.migration_id}: Page {pagination_info['window_page']} fetched ticket IDs: {ticket_ids}"
                )

                # Determine if there are more pages
                has_more = len(tickets) == pagination_info["per_page"]

                # Build meta information
                meta = {
                    "has_more": has_more,
                    "current_page": pagination_info["window_page"],
                    "current_window": pagination_info["current_window"],
                    "fetched_count": len(tickets),
                }

                self.logger.info(
                    f"Migration {self.migration_id}: Fetched {len(tickets)} tickets from bulk endpoint"
                )
                return tickets, meta
            else:
                self.logger.error(
                    f"Migration {self.migration_id}: Bulk ticket fetch failed: {response.status_code}"
                )
                return [], {"has_more": False, "error": f"HTTP {response.status_code}"}

        except Exception as e:
            self.logger.error(
                f"Migration {self.migration_id}: Error in bulk ticket fetch: {e}"
            )
            return [], {"has_more": False, "error": str(e)}

    def fetch_full_tickets_parallel(self, tickets: List[Dict]) -> List[Dict]:
        """
        🚀 STAGE 2: Parallel fetch full ticket details from /api/v2/tickets/{id}
        Conservative threading to respect rate limits
        """
        if not tickets:
            return []

        self.logger.info(
            f"Migration {self.migration_id}: Fetching full details for {len(tickets)} tickets with {self.connector.rate_limiter.thread_pool_size} workers"
        )

        full_tickets = []
        failed_ids = []

        def fetch_single_ticket(ticket):
            ticket_id = ticket.get("id")
            requesterEmail = ticket.get("requesterEmail", "")
            """Fetch individual ticket with full details"""
            try:
                domain_url = self.connector.config.domainUrl
                domain = self._extract_domain_from_url(domain_url)
                url = (
                    f"https://{domain}.freshdesk.com/api/channel/v2/tickets/{ticket_id}"
                )

                headers = self._build_headers()
                response = self.connector.rate_limiter.make_request_with_retry(
                    url, "GET", headers=headers, auth=self._build_auth()
                )

                if response.status_code == 200:
                    ticket_data = safe_json_response(response)
                    ticket_data["requesterEmail"] = ticket.get("requesterEmail", "")
                    return {"success": True, "data": ticket_data}
                else:
                    self.logger.warning(
                        f"Migration {self.migration_id}: Failed to fetch ticket {ticket_id}: {response.status_code}"
                    )
                    return {
                        "success": False,
                        "ticket_id": ticket_id,
                        "error": f"HTTP {response.status_code}",
                    }

            except Exception as e:
                self.logger.error(
                    f"Migration {self.migration_id}: Exception fetching ticket {ticket_id}: {e}"
                )
                return {"success": False, "ticket_id": ticket_id, "error": str(e)}

        # Use conservative thread pool
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.connector.rate_limiter.thread_pool_size
        ) as executor:
            # Submit all tasks
            future_to_id = {
                executor.submit(fetch_single_ticket, ticket): ticket
                for ticket in tickets
            }

            # Collect results
            for future in concurrent.futures.as_completed(
                future_to_id, timeout=300
            ):  # 5-minute timeout
                try:
                    result = future.result(timeout=30)  # 30-second timeout per ticket
                    if result["success"]:
                        full_tickets.append(result["data"])
                    else:
                        failed_ids.append(result.get("ticket_id", "unknown"))
                except Exception as e:
                    ticket_id = future_to_id[future]
                    self.logger.error(
                        f"Migration {self.migration_id}: Future exception for ticket {ticket_id}: {e}"
                    )
                    failed_ids.append(ticket_id)

        success_count = len(full_tickets)
        failure_count = len(failed_ids)

        self.logger.info(
            f"Migration {self.migration_id}: Individual ticket fetch complete: {success_count} success, {failure_count} failed"
        )

        if failed_ids:
            self.logger.warning(
                f"Migration {self.migration_id}: Failed ticket IDs: {failed_ids[:10]}{'...' if len(failed_ids) > 10 else ''}"
            )

        # Add requesterEmail field for each ticket
        # for ticket in full_tickets:
        #     if isinstance(ticket, dict):
        #         requester = ticket.get('requester')
        #         if isinstance(requester, dict):
        #             ticket['requesterEmail'] = requester.get('email', '')
        #         else:
        #             ticket['requesterEmail'] = ''

        return full_tickets

    def _build_headers(self) -> Dict:
        """Build standard headers for Freshdesk API"""
        return {"Content-Type": "application/json", "Accept": "application/json"}

    def _build_auth(self) -> Tuple:
        """Build authentication for Freshdesk API (API key + dummy password)"""
        # Freshdesk uses HTTP Basic Auth with API key as username and 'X' as password
        return (self.connector.config.username, self.connector.config.password)

    def _extract_domain_from_url(self, domain_url: str) -> str:
        """Extract domain from full Freshdesk URL"""
        # Remove protocol if present
        domain = domain_url.replace("https://", "").replace("http://", "")
        # Remove trailing slash if present
        domain = domain.rstrip("/")
        return domain


class OptimizedFreshdeskDataEnricher:
    """
    🚀 OPTIMIZED FRESHDESK DATA ENRICHMENT
    Similar to Zendesk optimized enricher but adapted for Freshdesk API endpoints
    """

    def __init__(self, freshdesk_connector):
        self.connector = freshdesk_connector
        self.logger = logging.getLogger(__name__)

        # 🔒 MIGRATION-SAFE: Each enricher instance has its own cache
        self._session_cache = {
            "agents": {},
            "groups": {},
            "companies": {},
            "contacts": {},
        }
        self._cache_lock = threading.Lock()

        # Migration-specific identifier for logging
        self.migration_id = getattr(freshdesk_connector, "migration_id", "unknown")

    def enrich_tickets_optimized(self, tickets: List[Dict]) -> List[Dict]:
        """
        🚀 OPTIMIZED ENRICHMENT: Bulk fetch + parallel processing for Freshdesk
        """
        if not tickets:
            return tickets

        start_time = time.time()
        self.logger.info(
            f"🚀 Migration {self.migration_id}: Starting optimized enrichment for {len(tickets)} tickets"
        )

        try:
            # Step 1: Extract all unique IDs that need enrichment # Dom - We are not caching, hence commented
            # agent_ids, group_ids, company_ids, contact_ids = self._extract_unique_ids(tickets)

            # Step 2: Bulk fetch all required data in parallel # Dom - We are not caching, hence commented
            # self._bulk_fetch_reference_data_parallel(agent_ids, group_ids, company_ids, contact_ids)

            # Step 3: Apply enrichment using cached data (very fast!)
            enriched_tickets = self._apply_cached_enrichment(tickets)

            duration = time.time() - start_time
            self.logger.info(
                f"🚀 Migration {self.migration_id}: Optimized enrichment complete: {len(tickets)} tickets in {duration:.2f}s"
            )

            return enriched_tickets

        except Exception as e:
            self.logger.error(
                f"Migration {self.migration_id}: Enrichment optimization failed: {e}"
            )
            # Return tickets as-is if enrichment fails
            return tickets

    def _extract_unique_ids(
        self, tickets: List[Dict]
    ) -> Tuple[Set[int], Set[int], Set[int], Set[int]]:
        """Extract all unique agent, group, company, and contact IDs from tickets"""
        agent_ids = set()
        group_ids = set()
        company_ids = set()
        contact_ids = set()

        for ticket in tickets:
            # Extract agent IDs
            if ticket.get("responder_id"):
                agent_ids.add(ticket["responder_id"])

            # Extract group IDs
            if ticket.get("group_id"):
                group_ids.add(ticket["group_id"])

            # Extract company IDs
            if ticket.get("company_id"):
                company_ids.add(ticket["company_id"])

            # Extract contact IDs (requester)
            if ticket.get("requester_id"):
                contact_ids.add(ticket["requester_id"])

        self.logger.info(
            f"Migration {self.migration_id}: IDs to fetch: "
            f"{len(agent_ids)} agents, {len(group_ids)} groups, "
            f"{len(company_ids)} companies, {len(contact_ids)} contacts"
        )
        return agent_ids, group_ids, company_ids, contact_ids

    def _bulk_fetch_reference_data_parallel(
        self,
        agent_ids: Set[int],
        group_ids: Set[int],
        company_ids: Set[int],
        contact_ids: Set[int],
    ):
        """
        🚀 PARALLEL BULK FETCHING: Fetch all reference data simultaneously
        """

        def fetch_agents_bulk():
            """Fetch all agents in bulk"""
            if not agent_ids:
                return

            try:
                # Freshdesk doesn't have bulk agent endpoint, so we fetch all and filter
                domain_url = self.connector.config.domainUrl
                domain = self._extract_domain_from_url(domain_url)
                url = f"https://{domain}.freshdesk.com/api/v2/agents"

                response = self._make_authenticated_request(url, "GET")

                if response.status_code == 200:
                    data = safe_json_response(response)
                    agents = data if isinstance(data, list) else []

                    with self._cache_lock:
                        for agent in agents:
                            if agent["id"] in agent_ids:
                                self._session_cache["agents"][agent["id"]] = {
                                    "id": agent["id"],
                                    "name": agent.get("contact", {}).get("name", ""),
                                    "email": agent.get("contact", {}).get("email", ""),
                                    "phone": agent.get("contact", {}).get("phone", ""),
                                    "role": agent.get("role", ""),
                                }

                    cached_count = len([a for a in agents if a["id"] in agent_ids])
                    self.logger.info(
                        f"Migration {self.migration_id}: Cached {cached_count} agents"
                    )
                else:
                    self.logger.warning(
                        f"Migration {self.migration_id}: Agent fetch failed: {response.status_code}"
                    )

            except Exception as e:
                self.logger.error(
                    f"Migration {self.migration_id}: Agent fetch error: {e}"
                )

        def fetch_groups_bulk():
            """Fetch all groups in bulk"""
            if not group_ids:
                return

            try:
                domain_url = self.connector.config.domainUrl
                domain = self._extract_domain_from_url(domain_url)
                url = f"https://{domain}.freshdesk.com/api/v2/groups"

                response = self._make_authenticated_request(url, "GET")

                if response.status_code == 200:
                    data = safe_json_response(response)
                    groups = data if isinstance(data, list) else []

                    with self._cache_lock:
                        for group in groups:
                            if group["id"] in group_ids:
                                self._session_cache["groups"][group["id"]] = {
                                    "id": group["id"],
                                    "name": group.get("name", ""),
                                }

                    cached_count = len([g for g in groups if g["id"] in group_ids])
                    self.logger.info(
                        f"Migration {self.migration_id}: Cached {cached_count} groups"
                    )
                else:
                    self.logger.warning(
                        f"Migration {self.migration_id}: Group fetch failed: {response.status_code}"
                    )

            except Exception as e:
                self.logger.error(
                    f"Migration {self.migration_id}: Group fetch error: {e}"
                )

        def fetch_companies_bulk():
            """Fetch companies - paginate if needed"""
            if not company_ids:
                return

            try:
                cached_companies = 0
                page = 1
                per_page = 100

                while (
                    cached_companies < len(company_ids) and page <= 10
                ):  # Limit to 10 pages
                    domain_url = self.connector.config.domainUrl
                    domain = self._extract_domain_from_url(domain_url)
                    url = f"https://{domain}.freshdesk.com/api/v2/companies"
                    params = {"page": page, "per_page": per_page}

                    response = self._make_authenticated_request(
                        url, "GET", params=params
                    )

                    if response.status_code == 200:
                        data = safe_json_response(response)
                        companies = data if isinstance(data, list) else []

                        if not companies:
                            break

                        with self._cache_lock:
                            for company in companies:
                                if company["id"] in company_ids:
                                    self._session_cache["companies"][company["id"]] = {
                                        "id": company["id"],
                                        "name": company.get("name", ""),
                                    }
                                    cached_companies += 1

                        page += 1
                    else:
                        self.logger.warning(
                            f"Migration {self.migration_id}: Company fetch failed: {response.status_code}"
                        )
                        break

                self.logger.info(
                    f"Migration {self.migration_id}: Cached {cached_companies} companies"
                )

            except Exception as e:
                self.logger.error(
                    f"Migration {self.migration_id}: Company fetch error: {e}"
                )

        def fetch_contacts_bulk():
            """Fetch contacts - paginate if needed"""
            if not contact_ids:
                return

            try:
                cached_contacts = 0
                page = 1
                per_page = 100

                while (
                    cached_contacts < len(contact_ids) and page <= 10
                ):  # Limit to 10 pages
                    domain_url = self.connector.config.domainUrl
                    domain = self._extract_domain_from_url(domain_url)
                    url = f"https://{domain}.freshdesk.com/api/v2/contacts"
                    params = {"page": page, "per_page": per_page}

                    response = self._make_authenticated_request(
                        url, "GET", params=params
                    )

                    if response.status_code == 200:
                        data = safe_json_response(response)
                        contacts = data if isinstance(data, list) else []

                        if not contacts:
                            break

                        with self._cache_lock:
                            for contact in contacts:
                                if contact["id"] in contact_ids:
                                    self._session_cache["contacts"][contact["id"]] = {
                                        "id": contact["id"],
                                        "name": contact.get("name", ""),
                                        "email": contact.get("email", ""),
                                        "phone": contact.get("phone", ""),
                                    }
                                    cached_contacts += 1

                        page += 1
                    else:
                        self.logger.warning(
                            f"Migration {self.migration_id}: Contact fetch failed: {response.status_code}"
                        )
                        break

                self.logger.info(
                    f"Migration {self.migration_id}: Cached {cached_contacts} contacts"
                )

            except Exception as e:
                self.logger.error(
                    f"Migration {self.migration_id}: Contact fetch error: {e}"
                )

        # Execute all bulk fetches in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(fetch_agents_bulk),
                executor.submit(fetch_groups_bulk),
                executor.submit(fetch_companies_bulk),
                executor.submit(fetch_contacts_bulk),
            ]

            # Wait for all to complete
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result(timeout=60)  # 60-second timeout per bulk fetch
                except Exception as e:
                    self.logger.error(
                        f"Migration {self.migration_id}: Parallel bulk fetch failed: {e}"
                    )

    def _apply_cached_enrichment(self, tickets: List[Dict]) -> List[Dict]:
        """
        Apply enrichment using cached data (very fast!)
        No API calls - just dictionary lookups
        """
        enriched_tickets = []

        for ticket in tickets:
            enriched_ticket = ticket.copy()

            # Process attachments
            attachments = tickets.get("attachments", [])
            ticket["attachmentUrls"] = [
                att.get("attachment_url")
                for att in attachments
                if att.get("attachment_url")
            ]

            # Enrich requester_id
            # Enhanced user lookup with on-demand fetching # Dom - Since requester ID is not a mapped field, we still need to perform this lookup.
            requester_id = ticket.get("requester_id")
            if requester_id:
                try:
                    requesterEmail = self._fetch_user_by_id(requester_id)
                except Exception as e:
                    logging.warning(
                        f"Failed to fetch Requester Email {requester_id}: {e}"
                    )

                if requesterEmail:
                    enriched_ticket["requesterEmail"] = requesterEmail.get("email", "")
                else:
                    enriched_ticket["requesterEmail"] = ""
            else:
                enriched_ticket["requesterEmail"] = ""

            # Enrich responder_id
            # Enhanced user lookup with on-demand fetching # Dom - Commenting this as we want to pass IDs as-is to the transformer
            # responder_id = ticket.get('responder_id')
            # if responder_id:
            #     try:
            #         agentName = self._fetch_agent_by_id(responder_id)
            #     except Exception as e:
            #         logging.warning(f"Failed to fetch Requester Email {requester_id}: {e}")

            #     if agentName:
            #         enriched_ticket['agentName'] = agentName.get('contact', {}).get('name', '')
            #     else:
            #         enriched_ticket['agentName'] = ''
            # else:
            #     enriched_ticket['agentName'] = ''

            # Enrich group_id and
            # Enhanced user lookup with on-demand fetching # Dom - Commenting this as we want to pass IDs as-is to the transformer
            # group_id = ticket.get('group_id')
            # if group_id:
            #     try:
            #         groupName = self._fetch_group_by_id(group_id)
            #     except Exception as e:
            #         logging.warning(f"Failed to fetch Requester Email {group_id}: {e}")

            #     if groupName:
            #         enriched_ticket['groupName'] = groupName.get('name', '')
            #     else:
            #         enriched_ticket['groupName'] = ''
            # else:
            #     enriched_ticket['groupName'] = ''

            # Dom - Commenting all lines below as we are not using cache any longer
            # Enrich with contact data (requester)
            # if ticket.get('requester_id') and ticket['requester_id'] in self._session_cache['contacts']:
            #     contact_data = self._session_cache['contacts'][ticket['requester_id']]
            #     enriched_ticket['requesterName'] = contact_data.get('name', '')
            #     enriched_ticket['requesterEmail'] = contact_data.get('email', '')
            #     enriched_ticket['requesterPhone'] = contact_data.get('phone', '')
            # else:
            #     enriched_ticket['requesterName'] = ''
            #     enriched_ticket['requesterEmail'] = ''
            #     enriched_ticket['requesterPhone'] = ''

            # # Enrich with agent data (responder)
            # if ticket.get('responder_id') and ticket['responder_id'] in self._session_cache['agents']:
            #     agent_data = self._session_cache['agents'][ticket['responder_id']]
            #     enriched_ticket['agentName'] = agent_data.get('name', '')
            #     enriched_ticket['agentEmail'] = agent_data.get('email', '')
            # else:
            #     enriched_ticket['agentName'] = ''
            #     enriched_ticket['agentEmail'] = ''

            # # Enrich with group data
            # if ticket.get('group_id') and ticket['group_id'] in self._session_cache['groups']:
            #     group_data = self._session_cache['groups'][ticket['group_id']]
            #     enriched_ticket['groupName'] = group_data.get('name', '')
            # else:
            #     enriched_ticket['groupName'] = ''

            # # Enrich with company data
            # if ticket.get('company_id') and ticket['company_id'] in self._session_cache['companies']:
            #     company_data = self._session_cache['companies'][ticket['company_id']]
            #     enriched_ticket['companyName'] = company_data.get('name', '')
            # else:
            #     enriched_ticket['companyName'] = ''

            enriched_tickets.append(enriched_ticket)

        return enriched_tickets

    def _make_authenticated_request(
        self, url: str, method: str = "GET", **kwargs
    ) -> requests.Response:
        """Make authenticated request to Freshdesk API"""
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if "headers" in kwargs:
            headers.update(kwargs["headers"])
        kwargs["headers"] = headers

        auth = (
            self.connector.config.username,
            self.connector.config.password,
        )  # API key + "X"
        kwargs["auth"] = auth

        return self.connector.rate_limiter.make_request_with_retry(
            url, method, **kwargs
        )

    def _extract_domain_from_url(self, domain_url: str) -> str:
        """Extract domain from full Freshdesk URL"""
        # Remove protocol if present
        domain = domain_url.replace("https://", "").replace("http://", "")
        # Remove trailing slash if present
        domain = domain.rstrip("/")
        return domain

    def _fetch_user_by_id(self, user_id: int) -> Optional[Dict]:
        """
        Fetch user data by ID - tries contacts first, then agents
        Returns user data with user_type field indicating 'contact' or 'agent'
        """
        try:
            domain = self._extract_domain()

            # Try as contact first
            contact_url = f"https://{domain}.freshdesk.com/api/v2/contacts/{user_id}"
            contact_data = self._make_api_request(contact_url)

            if contact_data:
                # Add user type and return
                contact_data["user_type"] = "contact"
                logging.debug(
                    f"Found user {user_id} as contact: {contact_data.get('name', 'Unknown')}"
                )
                return contact_data

            # If not found as contact, try as agent
            agent_url = f"https://{domain}.freshdesk.com/api/v2/agents/{user_id}"
            agent_data = self._make_api_request(agent_url)

            if agent_data:
                # Add user type and return
                agent_data["user_type"] = "agent"
                logging.debug(
                    f"Found user {user_id} as agent: {agent_data.get('name', 'Unknown')}"
                )
                return agent_data

            # Not found in either
            logging.warning(f"User {user_id} not found as contact or agent")
            return None

        except Exception as e:
            logging.error(f"Error fetching user {user_id}: {e}")
            return None

    def _fetch_agent_by_id(self, agent_id: int) -> Optional[Dict]:
        """
        Fetch agent data by ID using /api/v2/agents/{agent_id}
        Returns agent data with user_type field set to 'agent'
        """
        try:
            domain = self._extract_domain()

            agent_url = f"https://{domain}.freshdesk.com/api/v2/agents/{agent_id}"
            agent_data = self._make_api_request(agent_url)

            if agent_data:
                # Add user type and return
                agent_data["user_type"] = "agent"
                logging.debug(
                    f"Found agent {agent_id}: {agent_data.get('contact', {}).get('name', 'Unknown')}"
                )
                return agent_data

            # Not found
            logging.warning(f"Agent {agent_id} not found")
            return None

        except Exception as e:
            logging.error(f"Error fetching agent {agent_id}: {e}")
            return None

    def _fetch_group_by_id(self, group_id: int) -> Optional[Dict]:
        """
        Fetch group data by ID using /api/v2/groups/{group_id}
        Returns group data
        """
        try:
            domain = self._extract_domain()

            group_url = f"https://{domain}.freshdesk.com/api/v2/groups/{group_id}"
            group_data = self._make_api_request(group_url)

            if group_data:
                logging.debug(
                    f"Found group {group_id}: {group_data.get('name', 'Unknown')}"
                )
                return group_data

            # Not found
            logging.warning(f"Group {group_id} not found")
            return None

        except Exception as e:
            logging.error(f"Error fetching group {group_id}: {e}")
            return None

    def _make_api_request(self, url: str) -> Optional[Dict]:
        """Make authenticated API request and return parsed response"""
        try:
            headers = {"Content-Type": "application/json", "Accept": "application/json"}

            # Add authentication
            api_key = self.connector.config.username
            auth_string = f"{api_key}:X"
            encoded_auth = base64.b64encode(auth_string.encode()).decode()
            headers["Authorization"] = f"Basic {encoded_auth}"

            response = self.connector.rate_limiter.make_request_with_retry(
                url, "GET", headers=headers
            )

            if response.status_code == 200:
                return safe_json_response(response)
            elif response.status_code == 404:
                # Not found - this is expected when checking both contacts and agents
                return None
            else:
                logging.warning(f"API request failed for {url}: {response.status_code}")
                return None

        except Exception as e:
            logging.error(f"Error making API request to {url}: {e}")
            return None

    def _extract_domain(self) -> str:
        """Extract domain from connector config"""
        domain_url = self.connector.config.domainUrl
        if "://" in domain_url:
            domain_url = domain_url.split("://", 1)[1]
        return domain_url.rstrip("/")


class FreshdeskDataEnricher(BaseDataEnricher):
    """
    🚀 FRESHDESK DATA ENRICHER: Uses optimized enrichment
    """

    def __init__(self, connector):
        super().__init__()
        self.connector = connector
        # Use optimized enricher internally
        self.optimized_enricher = OptimizedFreshdeskDataEnricher(connector)

    def enrich_tickets(
        self, tickets: List[Dict], api_response: Dict = None
    ) -> List[Dict]:
        """🚀 Uses optimized enrichment"""
        return self.optimized_enricher.enrich_tickets_optimized(tickets)

    def enrich_conversations(
        self, conversations: List[Dict], users: List[Dict]
    ) -> List[Dict]:
        """Enrich Freshdesk conversations with enhanced user data fetching"""
        logging.info(
            f"FRESHDESK CONVERSATION ENRICHER: Processing {len(conversations)} conversations"
        )

        # Cache for fetched users to avoid duplicate API calls
        user_cache = {}

        for conversation in conversations:
            # Process attachments
            attachments = conversation.get("attachments", [])
            conversation["attachmentUrls"] = [
                att.get("attachment_url")
                for att in attachments
                if att.get("attachment_url")
            ]

            # Enhanced user lookup with on-demand fetching
            user_id = conversation.get("user_id")
            if user_id:
                # First try the provided users list
                user = next((u for u in users if u.get("id") == user_id), None)

                if not user and user_id not in user_cache:
                    # Fetch user data from API if not found
                    try:
                        user_data = self._fetch_user_by_id(user_id)
                        if user_data:
                            user_cache[user_id] = user_data
                            user = user_data
                    except Exception as e:
                        logging.warning(f"Failed to fetch user {user_id}: {e}")
                        user_cache[user_id] = None
                elif user_id in user_cache:
                    user = user_cache[user_id]

                if user:
                    # Dom - 11/1 - Adjusted to handle both agent and contact user types
                    if user.get("user_type") == "agent":
                        conversation["personEmail"] = user_data.get("contact", {}).get(
                            "email", ""
                        )
                        conversation["personName"] = user_data.get("contact", {}).get(
                            "name", ""
                        )
                    else:  # contact
                        conversation["personEmail"] = user_data.get("email", "")
                        conversation["personName"] = user_data.get("name", "")
                    conversation["personType"] = user.get(
                        "user_type", "contact"
                    )  # Track if contact or agent
                else:
                    conversation["personName"] = ""
                    conversation["personEmail"] = ""
                    conversation["personType"] = ""
            else:
                conversation["personName"] = ""
                conversation["personEmail"] = ""
                conversation["personType"] = ""

        return conversations

    def _fetch_user_by_id(self, user_id: int) -> Optional[Dict]:
        """
        Fetch user data by ID - tries contacts first, then agents
        Returns user data with user_type field indicating 'contact' or 'agent'
        """
        try:
            domain = self._extract_domain()

            # Try as contact first
            contact_url = f"https://{domain}.freshdesk.com/api/v2/contacts/{user_id}"
            contact_data = self._make_api_request(contact_url)

            if contact_data:
                # Add user type and return
                contact_data["user_type"] = "contact"
                logging.debug(
                    f"Found user {user_id} as contact: {contact_data.get('name', 'Unknown')}"
                )
                return contact_data

            # If not found as contact, try as agent
            agent_url = f"https://{domain}.freshdesk.com/api/v2/agents/{user_id}"
            agent_data = self._make_api_request(agent_url)

            if agent_data:
                # Add user type and return
                agent_data["user_type"] = "agent"
                logging.debug(
                    f"Found user {user_id} as agent: {agent_data.get('name', 'Unknown')}"
                )
                return agent_data

            # Not found in either
            logging.warning(f"User {user_id} not found as contact or agent")
            return None

        except Exception as e:
            logging.error(f"Error fetching user {user_id}: {e}")
            return None

    def _make_api_request(self, url: str) -> Optional[Dict]:
        """Make authenticated API request and return parsed response"""
        try:
            headers = {"Content-Type": "application/json", "Accept": "application/json"}

            # Add authentication
            api_key = self.connector.config.username
            auth_string = f"{api_key}:X"
            encoded_auth = base64.b64encode(auth_string.encode()).decode()
            headers["Authorization"] = f"Basic {encoded_auth}"

            response = self.connector.rate_limiter.make_request_with_retry(
                url, "GET", headers=headers
            )

            if response.status_code == 200:
                return safe_json_response(response)
            elif response.status_code == 404:
                # Not found - this is expected when checking both contacts and agents
                return None
            else:
                logging.warning(f"API request failed for {url}: {response.status_code}")
                return None

        except Exception as e:
            logging.error(f"Error making API request to {url}: {e}")
            return None

    def _extract_domain(self) -> str:
        """Extract domain from connector config"""
        domain_url = self.connector.config.domainUrl
        if "://" in domain_url:
            domain_url = domain_url.split("://", 1)[1]
        return domain_url.rstrip("/")


class FreshdeskFieldMapper(BaseFieldMapper):
    """Freshdesk-specific field mapper"""

    def get_standard_field_mapping(self) -> Dict[str, str]:
        """Return mapping of Freshdesk fields to standard field names"""
        return {
            "ticket_id": "id",
            "ticket_subject": "subject",
            "ticket_description": "description_text",
            "ticket_priority": "priority",
            "ticket_status": "status",
            "requester_email": "requester_id",
            "assignee_email": "responder_id",
            "group_name": "group_id",
            "created_date": "created_at",
            "updated_date": "updated_at",
            "due_date": "due_by",
            "ticket_type": "type",
            "ticket_tags": "tags",
        }


class FreshdeskConnector(BaseSourceConnector):
    """Freshdesk implementation of the base source connector"""

    def __init__(self, config: SourceConnectorConfig, thread_pool_size: int = 2):
        super().__init__(config)

        # Initialize components with conservative threading
        self.rate_limiter = FreshdeskRateLimitHandler(thread_pool_size=thread_pool_size)
        self.data_enricher = FreshdeskDataEnricher(self)
        self.field_mapper = FreshdeskFieldMapper()

        # Freshdesk-specific components
        self.pagination_manager = FreshdeskPaginationManager()
        self.ticket_fetcher = FreshdeskTicketFetcher(self)

        # Migration ID for logging
        self.migration_id = f"freshdesk_{int(time.time())}"

    def get_tickets(self, query_params: Dict) -> SourceResponse:
        """
        🚀 MAIN TICKET FETCHING: Windowed pagination + two-stage fetching + enrichment
        """
        try:
            # Step 1: Calculate pagination info
            numberOfProcessedRecords = query_params.pop("numberOfProcessedRecords", 0)
            pagination_info = self.pagination_manager.calculate_pagination_info(
                numberOfProcessedRecords, query_params
            )

            # Step 2: Check if we need to slide window
            if pagination_info["window_page"] > 1:
                # We're continuing within current window or starting new window
                # Extract updated_since for potential window sliding
                last_updated_at = query_params.get("last_ticket_updated_at")
                if last_updated_at and pagination_info["window_page"] == 1:
                    # Starting new window - update the updated_since parameter
                    next_updated_since = (
                        self.pagination_manager.calculate_next_updated_since(
                            last_updated_at
                        )
                    )
                    if next_updated_since:
                        query_params["updated_since"] = next_updated_since

            # Step 3: Fetch basic tickets (Stage 1)
            basic_tickets, meta = self.ticket_fetcher.fetch_ticket_window(
                pagination_info, query_params
            )

            if not basic_tickets:
                return SourceResponse(
                    status_code=200,
                    success=True,
                    data={
                        "tickets": [],
                        "meta": {
                            "has_more": False,
                            "current_page": pagination_info["global_page"],
                            "current_window": pagination_info["current_window"],
                        },
                    },
                )

            # Adding for testing purposes
            # basic_tickets = [{"id":11599,"requesterEmail":"dom@saasgenie.ai"}]
            ticket_ids = []
            # Step 4: Extract ticket IDs for full fetch
            for ticket in basic_tickets:
                if ticket.get("id"):
                    ticketData = {
                        "requesterEmail": ticket.get("requesterEmail", ""),
                        "id": ticket.get("id"),
                    }
                    ticket_ids.append(ticketData)

            # ticket_ids = [ticket.get('id') for ticket in basic_tickets if ticket.get('id')]

            # Step 5: Fetch full ticket details (Stage 2) - Dom commented temporarily
            full_tickets = self.ticket_fetcher.fetch_full_tickets_parallel(ticket_ids)

            # Step 6: Apply enrichment - Dom 7/12 - Commenting since enrichment done in Step 5
            # enriched_tickets = self.data_enricher.enrich_tickets(full_tickets)
            # enriched_tickets = full_tickets  # Skip enrichment - To be used for troubleshooting, if needed

            api_fetched_count = len(full_tickets)

            # NEW Step 6.5: Apply filtering AFTER enrichment but BEFORE pagination logic
            filter_only_params = self._extract_filter_params(query_params)

            if filter_only_params:
                # Use the generic library with clean filter parameters
                filter_configs = create_filters_from_params(filter_only_params)

                if filter_configs:
                    filtered_tickets = RecordFilter.apply(full_tickets, filter_configs)
                else:
                    filtered_tickets = full_tickets
            else:
                filtered_tickets = full_tickets

            # Step 7: Determine if we should slide window - Dom 7/12 - Commenting out the original logic to use max pages instead
            # should_slide = self.pagination_manager.should_slide_window(
            #     pagination_info['window_page'],
            #     meta.get('has_more', False)
            # )

            # Step 7: Dom 7/12 - Added this alternate logic to check based on max pages
            should_slide = (
                pagination_info["window_page"] >= self.pagination_manager.max_pages
            )

            # Step 8: Prepare response metadata
            response_meta = {
                "has_more": meta.get("has_more", False) or should_slide,
                "current_page": pagination_info["global_page"],
                "current_window": pagination_info["current_window"],
                "window_page": pagination_info["window_page"],
                "should_slide_window": should_slide,
                "number_of_processed_records": numberOfProcessedRecords
                + api_fetched_count,
            }

            # Add last ticket timestamp for potential window sliding. Dom - Only if should_slide is true
            if should_slide and full_tickets:
                last_ticket = max(full_tickets, key=lambda t: t.get("updated_at", ""))
                response_meta["last_ticket_updated_at"] = last_ticket.get("updated_at")

            return SourceResponse(
                status_code=200,
                success=True,
                data={"tickets": filtered_tickets, "meta": response_meta},
            )

        except Exception as e:
            logging.error(f"Error in Freshdesk get_tickets: {e}", exc_info=True)
            return SourceResponse(status_code=500, success=False, error_message=str(e))

    def get_conversations(self, ticket_id: str) -> SourceResponse:
        """
        🚀 CONVERSATION FETCHING: Get all conversations for a ticket
        """
        try:
            conversations = []
            page = 1
            per_page = 100

            while True:

                # Dom - Added to limit notes to under 200 (2 pages)
                if page >= 3:
                    break

                domain_url = self.config.domainUrl
                domain = self._extract_domain_from_url(domain_url)
                url = f"https://{domain}.freshdesk.com/api/channel/v2/tickets/{ticket_id}/conversations"
                params = {"page": page, "per_page": per_page}

                headers = {
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                }
                auth = (self.config.username, self.config.password)  # API key + "X"

                response = self.rate_limiter.make_request_with_retry(
                    url, "GET", headers=headers, params=params, auth=auth
                )

                if response.status_code == 200:
                    data = safe_json_response(response)
                    page_conversations = data if isinstance(data, list) else []

                    if not page_conversations:
                        break

                    conversations.extend(page_conversations)

                    # Check if there are more pages
                    if len(page_conversations) < per_page:
                        break

                    page += 1
                else:
                    logging.error(
                        f"Failed to fetch conversations for ticket {ticket_id}: {response.status_code}"
                    )
                    break

            # Enrich conversations
            enriched_conversations = self.data_enricher.enrich_conversations(
                conversations, []
            )

            # Add ticket_id to each conversation
            for conversation in enriched_conversations:
                conversation["ticket_id"] = ticket_id

            return SourceResponse(
                status_code=200,
                success=True,
                data={"conversations": enriched_conversations},
            )

        except Exception as e:
            logging.error(f"Error in Freshdesk get_conversations: {e}", exc_info=True)
            return SourceResponse(status_code=500, success=False, error_message=str(e))

    def _validate_config(self) -> bool:
        """Validate Freshdesk configuration"""
        return all([self.config.domainUrl, self.config.username])

    def _extract_domain_from_url(self, domain_url: str) -> str:
        """Extract domain from full Freshdesk URL"""
        # Remove protocol if present
        domain = domain_url.replace("https://", "").replace("http://", "")
        # Remove trailing slash if present
        domain = domain.rstrip("/")
        return domain

    def _extract_filter_params(self, query_params: Dict) -> Dict:
        """
        Extract only filter parameters, excluding Freshdesk pagination and API-specific parameters
        """
        # Freshdesk-specific parameters to exclude from filtering
        FRESHDESK_EXCLUDED_PARAMS = {
            # Pagination parameters
            "page",
            "per_page",
            "pageSize",
            "numberOfProcessedRecords",
            "domainUrl",
            "username",
            "password",
            "instance_url",
            # Freshdesk API control parameters
            "updated_since",
            "order_by",
            "order_type",
            "include",
            # File/path parameters (for JSON connector compatibility)
            "filePath",
            "fileName",
            # Internal pagination parameters
            "last_ticket_updated_at",
            "ticket_id",
            "display_id",
            # Freshdesk-specific API parameters
            "company_id",  # 'requester_id',  - These are API filters, not record filters
            # Add any other Freshdesk-specific pagination params here
        }

        # Filter out excluded parameters
        filter_only_params = {
            k: v for k, v in query_params.items() if k not in FRESHDESK_EXCLUDED_PARAMS
        }

        # self.logger.debug(f"Migration {self.migration_id}: Original params: {list(query_params.keys())}")
        # self.logger.debug(f"Migration {self.migration_id}: Filter params: {list(filter_only_params.keys())}")

        return filter_only_params


# ===================================================================
# GLOBAL CONNECTOR INSTANCE (MIGRATION-SAFE)
# ===================================================================


def _get_freshdesk_connector(headers, thread_pool_size=4):
    """
    🔒 MIGRATION-SAFE: Always create fresh connector instance
    """
    headers_dict = convert_source_headers_to_dict(headers)
    config = SourceConnectorConfig(
        domainUrl=headers_dict.get("domainUrl"),
        username=headers_dict.get("username"),  # API key
        password=headers_dict.get("password"),  # "X"
    )
    connector = FreshdeskConnector(config, thread_pool_size=thread_pool_size)

    # Add migration ID for better logging
    connector.migration_id = headers_dict.get(
        "migration_id", f"freshdesk_{int(time.time())}"
    )

    return connector


# ===================================================================
# 🚀 TRANSFORMER-COMPATIBLE FUNCTION INTERFACES
# ===================================================================


def get_freshdesk_tickets(**kwargs) -> Dict:
    """
    🚀 FRESHDESK TICKETS: Main entry point for ticket fetching
    Handles windowed pagination, two-stage fetching, and enrichment
    """
    try:
        # Extract thread pool size from config if provided
        source_headers = kwargs.get("sourceHeaders", [])
        headers_dict = convert_source_headers_to_dict(source_headers)
        # Dom - Set thread pool to 10 instead of the default 2
        thread_pool_size = int(headers_dict.get("thread_pool_size", 10))

        connector = _get_freshdesk_connector(source_headers, thread_pool_size)
        query_dict = convert_query_params_to_dict(kwargs.get("queryParams", []))

        # Dom - Extract numberOfProcessedRecords from root kwargs
        numberOfProcessedRecords = kwargs.get("numberOfProcessedRecords", 0)
        query_dict["numberOfProcessedRecords"] = numberOfProcessedRecords

        response = connector.get_tickets(query_dict)
        result = standardize_source_response_format(response)

        return result

    except Exception as e:
        logging.error(f"Error in get_freshdesk_tickets: {e}", exc_info=True)
        return {"status_code": 500, "error": str(e)}


def get_freshdesk_conversations(**kwargs) -> Dict:
    """
    🚀 FRESHDESK CONVERSATIONS: Get conversations for a specific ticket
    """
    try:
        source_headers = kwargs.get("sourceHeaders", [])
        query_params = kwargs.get("queryParams", [])

        # Extract ticket_id from queryParams
        if isinstance(query_params, list):
            ticket_id = (
                query_params[0].get("value")
                if query_params and "value" in query_params[0]
                else None
            )
        else:
            ticket_id = query_params.get("ticket_id")

        if not ticket_id:
            return {"status_code": 400, "error": "Missing ticket_id parameter"}

        connector = _get_freshdesk_connector(source_headers)
        response = connector.get_conversations(ticket_id)
        result = standardize_source_response_format(response)

        return result

    except Exception as e:
        logging.error(f"Error in get_freshdesk_conversations: {e}", exc_info=True)
        return {"status_code": 500, "error": str(e)}


def validate_freshdesk_instance(**kwargs) -> Dict:
    """Validate Freshdesk instance and credentials"""
    try:
        headers = kwargs.get("sourceHeaders", [])
        connector = _get_freshdesk_connector(headers)

        if not connector._validate_config():
            return {
                "status_code": 400,
                "body": {"valid": False, "error": "Invalid configuration"},
            }

        # Test connection by fetching a single agent
        domain_url = connector.config.domainUrl
        domain = connector._extract_domain_from_url(domain_url)
        url = f"https://{domain}.freshdesk.com/api/v2/agents"
        auth = (connector.config.username, connector.config.password)  # API key + "X"

        response = connector.rate_limiter.make_request_with_retry(
            url,
            "GET",
            auth=auth,
            headers={"Content-Type": "application/json"},
            params={"per_page": 1},
        )

        is_valid = response.status_code == 200

        return {
            "status_code": 200 if is_valid else response.status_code,
            "body": {"valid": is_valid},
        }

    except Exception as e:
        logging.error(f"Error validating Freshdesk instance: {e}")
        return {"status_code": 500, "body": {"valid": False, "error": str(e)}}


# ===================================================================
# 📊 PERFORMANCE MONITORING
# ===================================================================


def get_freshdesk_performance_stats() -> Dict:
    """Get performance statistics for monitoring"""
    return {
        "connector_type": "freshdesk_optimized",
        "pagination_strategy": "sliding_window_300_pages",
        "fetching_strategy": "two_stage_bulk_plus_individual",
        "threading": "conservative_configurable",
        "default_workers": 2,
        "rate_limiting": "429_retry_with_exponential_backoff",
        "enrichment_type": "optimized_bulk_parallel",
        "migration_safe": True,
        "thread_safe": True,
    }


if __name__ == "__main__":
    # Test example
    logging.basicConfig(level=logging.INFO)

    headers = [
        {"key": "domainUrl", "value": "https://your-domain.freshdesk.com"},
        {"key": "username", "value": "your-api-key"},
        {"key": "password", "value": "X"},
        {"key": "thread_pool_size", "value": "2"},
    ]

    query_params = [
        {"key": "per_page", "value": "50"},
        {"key": "numberOfProcessedRecords", "value": "0"},
    ]

    print("🚀 Testing Freshdesk connector with conservative threading...")
    start_time = time.time()

    result = get_freshdesk_tickets(sourceHeaders=headers, queryParams=query_params)

    duration = time.time() - start_time
    print(f"⚡ Request completed in {duration:.2f} seconds")
    print(f"📊 Status: {result.get('status_code')}")

    if result.get("status_code") == 200:
        tickets = result.get("body", {}).get("tickets", [])
        print(
            f"🎯 Successfully fetched {len(tickets)} tickets with two-stage fetching!"
        )
    else:
        print(f"❌ Error: {result.get('error')}")
