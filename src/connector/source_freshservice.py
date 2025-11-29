"""
freshservice.py
============

FRESHSERVICE CONNECTOR
"""

import requests
import json
import time
import re
import logging
from typing import List, Dict, Tuple, Optional, Any, Set
from datetime import datetime, timedelta
import base64
from src.connector.utils.generic_record_filter import (
    RecordFilter,
    create_filters_from_params,
)


# # ===================================================================
# # RATE LIMITER
# # ===================================================================
class FreshserviceRateLimitHandler:
    """
    🚦 FRESHSERVICE RATE LIMIT HANDLER with retry & backoff
    Mirrors Freshdesk behavior for consistent retry logic
    """

    def __init__(self, thread_pool_size=2, max_retries=3, base_delay=2):
        self.thread_pool_size = thread_pool_size
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.logger = logging.getLogger(__name__)

    def is_rate_limited(self, response: requests.Response) -> bool:
        return response.status_code == 429

    def get_retry_delay(self, response: requests.Response) -> int:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return int(retry_after)
            except ValueError:
                pass
        return 60

    def make_request_with_retry(
        self, url: str, method: str = "GET", headers: Dict = None, **kwargs
    ) -> requests.Response:
        self.logger.info(f"🚦 FreshserviceRateLimiter → {method} {url}")
        
        # Merge any provided headers with kwargs
        if headers:
            kwargs['headers'] = headers
            
        for attempt in range(self.max_retries + 1):
            try:
                response = requests.request(method, url, **kwargs)
                if response.status_code < 400:
                    return response
                if self.is_rate_limited(response) and attempt < self.max_retries:
                    delay = max(
                        self.get_retry_delay(response), self.base_delay * (2**attempt)
                    )
                    self.logger.warning(
                        f"Rate limited (429) on {url}, retrying in {delay}s (attempt {attempt + 1}/{self.max_retries + 1})"
                    )
                    time.sleep(delay)
                    continue
                else:
                    self.logger.warning(
                        f"HTTP {response.status_code} on {url}: {response.text[:200]}"
                    )
                    return response
            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries:
                    delay = self.base_delay * (2**attempt)
                    self.logger.warning(
                        f"Request failed on {url}: {e}, retrying in {delay}s"
                    )
                    time.sleep(delay)
                else:
                    self.logger.error(f"Final attempt failed on {url}: {e}")
                    mock_response = requests.Response()
                    mock_response.status_code = 500
                    mock_response._content = str(e).encode()
                    return mock_response


# # ===================================================================
# # PAGINATION MANAGEMENT
# # ===================================================================


class FreshservicePaginationManager:
    def __init__(self, per_page=100, max_pages=100):
        self.per_page = per_page
        self.max_pages = max_pages  # Freshservice limit
        self.logger = logging.getLogger(__name__)

    # Dom - 9/13 - Replaced the logic
    def calculate_pagination_info(
    self, numberOfProcessedRecords: int, query_params: Dict
) -> Dict:
        try:
            per_page = int(query_params.get("per_page", self.per_page))
            numberOfProcessedRecords = int(numberOfProcessedRecords)
            # Get starting page from query params (defaults to 1)
            starting_page = int(query_params.get('page', 1))
        except (ValueError, TypeError):
            per_page = self.per_page
            numberOfProcessedRecords = 0
            starting_page = 1
        
        # Calculate additional pages based on processed records
        additional_pages = numberOfProcessedRecords // per_page
        
        # Final page = starting page + additional pages from processing
        global_page = starting_page + additional_pages
        
        return {
            "per_page": per_page,
            "global_page": global_page,
            "numberOfProcessedRecords": numberOfProcessedRecords,
            "starting_page": starting_page,
        }


# # ===================================================================
# # FETCHING TICKETS AND CONVERSATIONS
# # ===================================================================


def extract_freshservice_filter_params(query_dict: Dict) -> Dict:
    """
    Extract only filter-related parameters, excluding Freshservice-specific pagination, auth, and API control parameters
    """
    FRESHSERVICE_EXCLUDED_PARAMS = {
        # Pagination
        "page",
        "per_page",
        "pageSize",
        "numberOfProcessedRecords",
        # Auth and config
        "domainUrl",
        "username",
        "password",
        "instance_url",
        # API control parameters
        "updated_since",
        "order_by",
        "order_type",
        "include",
        # Internal tracking / state
        "last_ticket_updated_at",
        "ticket_id",
        "display_id",
        # Filtering used only by the API and not meant for in-memory filtering
        "company_id",
        "requester_id",
        "responder_id",
        "group_id_api",
        "workspace_id",
        "type",
        "email",
        "filter",
    }

    return {
        k: v for k, v in query_dict.items() if k not in FRESHSERVICE_EXCLUDED_PARAMS
    }

# ─────────────────────────────────────────────────────────────────────────────
# Converts a list of source headers into a flat dictionary.
# - Handles both list[dict] and already-formatted dict input
# Params: headers (List[Dict] or Dict)
# Returns: Dict
def convert_source_headers_to_dict(headers):
    """
    Converts a list of header key-value pairs to a dictionary.

    - Checks if headers are a list of dicts
    - Converts to a flat dictionary with key-value pairs
    - If already a dict, returns as is

    Params: headers (List[Dict] or Dict)
    Returns: Dict
    """
    if isinstance(headers, list):
        return {item["key"]: item["value"] for item in headers}
    return headers


# ─────────────────────────────────────────────────────────────────────────────
# Converts a list of query parameter objects into a flat dictionary.
# - Ensures compatibility between list-based config and internal access
# - Fallbacks to original object if already a dict
# Params: query_params (List[Dict] or Dict)
# Returns: Dict
def convert_query_params_to_dict(query_params):

    if isinstance(query_params, list):
        return {param["key"]: param["value"] for param in query_params}
    return query_params


# ─────────────────────────────────────────────────────────────────────────────
# Retrieves requester details (email, full name) for a given Freshservice user ID.
# - Constructs requester lookup URL
# - Makes a retry-safe GET request to the Freshservice API
# - Parses first name, last name, and primary email fields
# Params: requester_id (int), headers (Dict), rate_limiter (FreshserviceRateLimitHandler)
# Returns: Dict with 'email' and 'name'
def get_freshservice_contact_details(requester_id, headers, rate_limiter):
    domain = headers.get("domainUrl")
    api_key = headers.get("username")
    auth = (api_key, "X")
    headersData = {
        'X-FW-Partner-Migration': 'd76f19b905897b629b89f83e4c6ac3d8e531091e8b994579c7b1addee2c2e693'
    }
    url = f"https://{domain}.freshservice.com/api/v2/requesters/{requester_id}"
    resp = rate_limiter.make_request_with_retry(url, "GET", auth=auth, headers=headersData)
    if resp.status_code == 200:
        requester = resp.json().get("requester", {})
        return {
            "email": requester.get("primary_email", ""),
            "name": f"{requester.get('first_name', '')} {requester.get('last_name', '')}".strip(),
        }
    return {"email": "", "name": ""}


# ─────────────────────────────────────────────────────────────────────────────
# Fetches a list of Freshservice tickets (paginated) with full details.
# - Uses sliding window pagination and updated_since for incremental sync
# - Enriches each ticket with requester email and name via secondary API call
# - Applies retry-safe logic for all requests (including sub-requests per ticket)
# - Constructs meta: has_more + last_ticket_updated_at for downstream use
# Params: kwargs with sourceHeaders, queryParams, numberOfProcessedRecords
# Returns: Dict with status_code, body.tickets, body.meta, total_records
def get_freshservice_tickets(**kwargs) -> Dict:
    try:
        rate_limiter = FreshserviceRateLimitHandler()
        source_headers = kwargs.get("sourceHeaders", [])
        query_params = kwargs.get("queryParams", [])
        number_of_processed = kwargs.get("numberOfProcessedRecords", 0)

        headers_dict = convert_source_headers_to_dict(source_headers)
        query_dict = convert_query_params_to_dict(query_params)

        pagination_manager = FreshservicePaginationManager()
        pagination_info = pagination_manager.calculate_pagination_info(
            number_of_processed, query_dict
        )
        page = pagination_info.get("global_page", 1)
        per_page = pagination_info["per_page"]

        domain = headers_dict.get("domainUrl")
        api_key = headers_dict.get("username")
        auth = (api_key, "X")
        headersData = {
            'X-FW-Partner-Migration': 'd76f19b905897b629b89f83e4c6ac3d8e531091e8b994579c7b1addee2c2e693'
        }

        # Build base URL
        list_url = f"https://{domain}.freshservice.com/api/v2/tickets?page={page}&per_page={per_page}"

        
        temp_exclusion = [ "page","per_page",
        "pageSize",
        "numberOfProcessedRecords",
        "domainUrl",
        "username",
        "password",
        "instance_url",
        # "order_type", # This should not be here
        "ticket_id",
        "display_id",
        "company_id",
        "responder_id",
        "group_id_api"]
        
        filter_only_params = extract_freshservice_filter_params(query_dict)
        # Keys in query_dict NOT used for filtering
        for key, value in query_dict.items():
            if key not in filter_only_params :
                if key not in temp_exclusion:
                    list_url += f"&{key}={value}"

        resp = rate_limiter.make_request_with_retry(list_url, "GET", auth=auth, headers=headersData)
        if resp.status_code != 200:
            return {
                "status_code": resp.status_code,
                "body": {"tickets": []},
                "total_records": 0,
            }

        ticket_summaries = resp.json().get("tickets", [])
        # Dom 9/2: Apply preliminary filtering on fields available in the summary API response
        ticket_filtered_summaries = ticket_summaries  # Default to all tickets  
        full_tickets = []
        base_url = f"https://{domain}.freshservice.com/api/v2/tickets"

        # Dom 9/13 - Track API records vs filtered records separately (unchanged for pagination)
        api_records_fetched = len(ticket_summaries)  # Records from API
        #ticket_summaries = [{"id":292764}] # Was added for testing

        # Dom 9/20 - Preliminary filtering logic
        if filter_only_params:
            # Import the parameter parser to handle suffixes and prefixes
            from src.connector.utils.generic_record_filter import ParameterParser  # Adjust import path as needed
            
            # Separate filters that can be applied to summary data vs those requiring full ticket details
            summary_filterable_fields = {
                # Basic ticket fields available in /api/v2/tickets response
                'id', 'subject', 'group_id', 'department_id', 'category', 'sub_category', 
                'item_category', 'requester_id', 'responder_id', 'due_by', 'fr_escalated', 
                'deleted', 'spam', 'email_config_id', 'is_escalated', 'fr_due_by', 
                'priority', 'status', 'source', 'created_at', 'updated_at', 
                'workspace_id', 'requested_for_id', 'type', 'description', 'description_text',
                'tasks_dependency_type'
            }
            
            # Split filter params into those that can be applied early vs those requiring full details
            preliminary_filter_params = {}
            detail_filter_params = {}
            
            for param_name, param_value in filter_only_params.items():
                # Parse parameter to extract the actual field name (handling suffixes/prefixes)
                include_flag = True
                processed_param = param_name
                
                # Check for exclusion prefixes first
                if param_name.startswith("exclude_"):
                    include_flag = False
                    processed_param = param_name[8:]
                elif param_name.startswith("not_"):
                    include_flag = False
                    processed_param = param_name[4:]
                
                # Parse the parameter to get the base field name
                field, operator, value = ParameterParser.parse_parameter(processed_param, param_value)
                
                # Check if the base field is available in summary data
                if field in summary_filterable_fields:
                    preliminary_filter_params[param_name] = param_value
                else:
                    detail_filter_params[param_name] = param_value
            
            # Apply preliminary filtering if we have filterable params
            if preliminary_filter_params:
                prelim_filter_config = create_filters_from_params(preliminary_filter_params)
                if prelim_filter_config:
                    print(f"\n\n\nApplying preliminary filter config: {prelim_filter_config}\n\n\n")
                    ticket_filtered_summaries = RecordFilter.apply(ticket_summaries, prelim_filter_config)
                    print(f"\n\n\nPreliminary filtering: {len(ticket_summaries)} -> {len(ticket_filtered_summaries)} tickets\n\n\n")
            
            # Store detail filters for later use
            filter_only_params = detail_filter_params

        #ticket_filtered_summaries = [{"id":112068}] # Was added for testing
        #ticket_filtered_summaries = [{"id":176687},{"id":188310},{"id":207509},{"id":207674},{"id":182546},{"id":212598},{"id":229146}]
        
        for ticket in ticket_filtered_summaries:
            tid = ticket.get("id")
            if not tid:
                continue
            detail_resp = rate_limiter.make_request_with_retry(
                f"{base_url}/{tid}", "GET", auth=auth, headers=headersData
            )
            if detail_resp.status_code == 200:
                ticket_data = detail_resp.json()
                full_ticket = ticket_data.get("ticket", {})
                requester_id = full_ticket.get("requester_id")
                if requester_id:
                    contact_info = get_freshservice_contact_details(
                        requester_id, headers_dict, rate_limiter
                    )
                    full_ticket["requesterEmail"] = contact_info["email"]
                    full_ticket["requesterName"] = contact_info["name"]
                    full_ticket["email"] = contact_info["email"]
                    full_ticket["name"] = contact_info["name"]
                full_ticket.pop("requester_id", None)
                full_ticket.setdefault("requesterEmail", "")
                full_ticket.setdefault("requesterName", "")
                full_tickets.append(full_ticket)


        if filter_only_params:
            filter_config = create_filters_from_params(filter_only_params)
            if filter_config:
                print(f"\n\n\nApplying filter config: {filter_config}\n\n\n")
                filtered_tickets = RecordFilter.apply(full_tickets, filter_config)
                print(f"\n\n\n 1 - Filtered tickets : \n{filtered_tickets}\n\n\n")
                
                # If filtering results in no tickets but API returned tickets, still advance pagination
                if len(filtered_tickets) == 0 and len(full_tickets) > 0:
                    print(f"\n\n\nFiltering returned no results, but API had {len(full_tickets)} tickets - advancing pagination\n\n\n")
            else:
                filtered_tickets = full_tickets
        else:
            filtered_tickets = full_tickets

        # Flatten custom_fields and custom_attributes to root level
        for ticket in filtered_tickets:
            for field in ("custom_fields", "custom_attributes"):
                ticket.update(ticket.pop(field, {}))

        # Dom - 9/13 - Count API records fetched (for pagination logic)
        api_records_fetched = len(ticket_summaries)  # Raw API response count

        # Dom - 9/13 - Always advance by API page size, not filtered results
        next_processed_records = number_of_processed + per_page

        data = {
            "status_code": 200,
            "body": {
                "tickets": filtered_tickets,
                "meta": {
                    "has_more": len(ticket_summaries) == per_page,  # Based on API response
                    "number_of_processed_records": next_processed_records,  # Always advance by per_page
                    "api_records_fetched": api_records_fetched,
                    "filtered_records_returned": len(filtered_tickets),
                    "preliminary_filtered_count": len(ticket_filtered_summaries),
                },
            },
            "total_records": len(filtered_tickets),
        }
        return data

    except Exception as e:
        logging.error(f"Error in get_freshservice_tickets: {e}", exc_info=True)
        import traceback

        return {
            "status_code": 500,
            "body": {"tickets": []},
            "error": str(e),
            "trace": traceback.format_exc(),
            "total_records": 0,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Retrieves all conversations (e.g., notes, replies) for a specific Freshservice ticket.
# - Expects ticket_id to be passed in queryParams
# - Uses retry-safe GET request to fetch from `/tickets/{id}/conversations`
# - Returns empty list and logs warning if ticket_id is missing
# Params: kwargs with sourceHeaders, queryParams (must include ticket_id)
# Returns: Dict with status_code and body.conversations
def get_freshservice_conversations(**kwargs) -> Dict:
    try:
        rate_limiter = FreshserviceRateLimitHandler()
        source_headers = kwargs.get("sourceHeaders", [])

        headers_dict = convert_source_headers_to_dict(source_headers)
        query_params_list = kwargs.get("queryParams", [])
        query_dict = {
            qp["key"]: qp["value"]
            for qp in query_params_list
            if "key" in qp and "value" in qp
        }
        ticket_id = query_dict.get("ticket_id")

        if not ticket_id:
            logging.warning(
                "\u2757 Missing ticket_id in queryParams: %s", kwargs.get("queryParams")
            )
            return {
                "status_code": 400,
                "error": "Missing ticket_id in queryParams",
                "body": {"conversations": []},
            }

        domain = headers_dict.get("domainUrl")
        api_key = headers_dict.get("username")
        auth = (api_key, "X")
        headersData = {
            'X-FW-Partner-Migration': 'd76f19b905897b629b89f83e4c6ac3d8e531091e8b994579c7b1addee2c2e693'
        }

        # Initialize variables for pagination
        all_conversations = []
        page = 1
        per_page = 100  # Maximum allowed by Freshservice API
        
        while True:
            # Add pagination parameters to the URL
            conv_url = f"https://{domain}.freshservice.com/api/v2/tickets/{ticket_id}/conversations"
            conv_url += f"?page={page}&per_page={per_page}"
            
            resp = rate_limiter.make_request_with_retry(conv_url, "GET", auth=auth, headers=headersData)

            if resp.status_code != 200:
                if page == 1:  # If first page fails, return error
                    return {"status_code": resp.status_code, "body": {"conversations": []}}
                else:  # If subsequent page fails, break and return what we have
                    logging.warning(f"Failed to fetch page {page} of conversations: {resp.text}")
                    break

            page_conversations = resp.json().get("conversations", [])
            
            # If no conversations returned, we've reached the end
            if not page_conversations:
                break
                
            all_conversations.extend(page_conversations)
            
            # If we got fewer conversations than per_page, we've reached the end
            if len(page_conversations) < per_page:
                break
                
            page += 1

        print(f"\n\n\nTotal conversations retrieved: {len(all_conversations)}")
        print("Full response JSON from /convos:")
        print(json.dumps(all_conversations, indent=2)) 

        # Process each conversation to add personEmail
        for conv in all_conversations:
            user_id = conv.get("user_id")
            if user_id:
                get_users_url = f"https://{domain}.freshservice.com/api/v2/requesters/{user_id}"
                resp = rate_limiter.make_request_with_retry(get_users_url, "GET", auth=auth, headers=headersData)

                if resp.status_code == 200:
                    requester = resp.json().get("requester", {})
                    conv["personEmail"] = requester.get("primary_email", "")
                else:
                    logging.warning(f"Trying to fetch agent details for user_id {user_id}: {resp.text}")
                    get_agents_url = f"https://{domain}.freshservice.com/api/v2/agents/{user_id}"
                    resp = rate_limiter.make_request_with_retry(get_agents_url, "GET", auth=auth, headers=headersData)
                    if resp.status_code == 200:
                        agent = resp.json().get("agent", {})
                        conv["personEmail"] = agent.get("email", "")
                    else:
                        logging.warning(f"Failed to fetch agent details for user_id {user_id}: {resp.text}")
                        conv["personEmail"] = ""
            else:
                conv["personEmail"] = ""

        return {
            "status_code": 200,
            "body": {"conversations": all_conversations},
        }

    except Exception as e:
        logging.error(f"Error in get_freshservice_conversations: {e}", exc_info=True)
        import traceback

        return {
            "status_code": 500,
            "body": {"conversations": []},
            "error": str(e),
            "trace": traceback.format_exc(),
        }


def get_freshservice_changes(**kwargs) -> Dict:
    try:
        rate_limiter = FreshserviceRateLimitHandler()
        source_headers = kwargs.get("sourceHeaders", [])
        query_params = kwargs.get("queryParams", [])
        number_of_processed = kwargs.get("numberOfProcessedRecords", 0)

        headers_dict = convert_source_headers_to_dict(source_headers)
        query_dict = convert_query_params_to_dict(query_params)

        pagination_manager = FreshservicePaginationManager()
        pagination_info = pagination_manager.calculate_pagination_info(
            number_of_processed, query_dict
        )
        #page = pagination_info.get("window_page", 1)
        page = pagination_info.get("global_page", 1)
        per_page = pagination_info["per_page"]

        domain = headers_dict.get("domainUrl")
        api_key = headers_dict.get("username") or headers_dict.get("apikey")
        auth = (api_key, "X")
        headersData = {
            'X-FW-Partner-Migration': 'd76f19b905897b629b89f83e4c6ac3d8e531091e8b994579c7b1addee2c2e693'
        }

        list_url = f"https://{domain}.freshservice.com/api/v2/changes?page={page}&per_page={per_page}"

        resp = rate_limiter.make_request_with_retry(list_url, "GET", auth=auth, headers=headersData)
        if resp.status_code != 200:
            return {
                "status_code": resp.status_code,
                "body": {"changes": []},
                "total_records": 0,
            }

        changes = resp.json().get("changes", [])

        for change in changes:
            change_id = change.get("id")
            attachment_url = f"https://{domain}.freshservice.com/api/v2/changes/{change_id}/attachments"
            attachment_resp = rate_limiter.make_request_with_retry(attachment_url, "GET", auth=auth, headers=headersData)

            if attachment_resp.status_code == 200:
                change["attachments"] = attachment_resp.json().get("attachments", [])
            else:
                logging.warning(f"Failed to fetch attachments for change {change_id}: {attachment_resp.text}")
                change["attachments"] = []

        return {
            "status_code": 200,
            "body": {
                "changes": changes,
                "meta": {
                    "has_more": len(changes) == per_page,
                    "last_change_updated_at": max(
                        [c.get("updated_at", "") for c in changes if "updated_at" in c],
                        default="",
                    ),
                },
            },
            "total_records": len(changes),
        }

    except Exception as e:
        logging.error(f"Error in get_freshservice_changes: {e}", exc_info=True)
        import traceback
        return {
            "status_code": 500,
            "body": {"changes": []},
            "error": str(e),
            "trace": traceback.format_exc(),
            "total_records": 0,
        }


def get_freshservice_tasks(**kwargs) -> Dict:
    try:
        rate_limiter = FreshserviceRateLimitHandler()
        source_headers = kwargs.get("sourceHeaders", [])
        query_params_list = kwargs.get("queryParams", [])
        query_dict = {
            qp["key"]: qp["value"]
            for qp in query_params_list
            if "key" in qp and "value" in qp
        }
        ticket_id = query_dict.get("ticket_id")

        if not ticket_id:
            logging.warning(
                "❗ Missing ticket_id in queryParams for get_freshservice_tasks"
            )
            return {
                "status_code": 400,
                "error": "Missing ticket_id in queryParams",
                "body": {"tasks": []},
            }

        headers_dict = convert_source_headers_to_dict(source_headers)
        domain = headers_dict.get("domainUrl")
        api_key = headers_dict.get("username")
        auth = (api_key, "X")
        headersData = {
            'X-FW-Partner-Migration': 'd76f19b905897b629b89f83e4c6ac3d8e531091e8b994579c7b1addee2c2e693'
        }

        url = f"https://{domain}.freshservice.com/api/v2/tickets/{ticket_id}/tasks"
        resp = rate_limiter.make_request_with_retry(url, "GET", auth=auth, headers=headersData)

        print("\n\n\nFull response JSON from /tasks:")
        print(json.dumps(resp.json().get("tasks"), indent=2)) 

        if resp.status_code != 200:
            return {"status_code": resp.status_code, "body": {"tasks": []}}

        return {"status_code": 200, "body": {"tasks": resp.json().get("tasks", [])}}

    except Exception as e:
        logging.error(f"Error in get_freshservice_tasks: {e}", exc_info=True)
        import traceback

        return {
            "status_code": 500,
            "body": {"tasks": []},
            "error": str(e),
            "trace": traceback.format_exc(),
        }