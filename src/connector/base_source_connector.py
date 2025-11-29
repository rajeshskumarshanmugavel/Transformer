"""
base_source_connector.py
========================

Reusable base classes and utilities for all source connectors.
This module contains the standardized interfaces and common functionality
that all platform-specific source connectors inherit from.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Optional, Any, Union
from dataclasses import dataclass, field
import logging
import requests
import time
import re
from datetime import datetime
from urllib.parse import urlencode


@dataclass
class SourceConnectorConfig:
    """Standardized configuration for source connectors"""
    domainUrl: str
    username: Optional[str] = None
    password: Optional[str] = None
    api_key: Optional[str] = None
    token: Optional[str] = None
    additional_headers: Dict[str, str] = field(default_factory=dict)
    rate_limit_per_minute: int = 100
    max_retries: int = 3
    timeout: int = 30
    page_size: int = 100


@dataclass
class SourceResponse:
    """Standardized response structure for source connectors"""
    status_code: int
    success: bool
    data: Optional[Dict] = None
    error_message: Optional[str] = None
    has_more: bool = False
    next_cursor: Optional[str] = None
    total_count: Optional[int] = None


@dataclass
class PaginationInfo:
    """Standardized pagination information"""
    has_more: bool = False
    next_cursor: Optional[str] = None
    page_size: int = 100
    total_count: Optional[int] = None
    current_page: int = 1


class BaseSourceRateLimitHandler(ABC):
    """Base class for handling rate limits in source connectors"""
    
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
                               method: str = 'GET',
                               auth: Optional[Tuple] = None,
                               headers: Optional[Dict] = None,
                               params: Optional[Dict] = None,
                               json_data: Optional[Dict] = None,
                               max_retries: int = 3) -> requests.Response:
        """Make HTTP request with retry logic for source connectors"""
        
        for attempt in range(max_retries + 1):
            try:
                kwargs = {
                    'timeout': 30,
                    'auth': auth,
                    'headers': headers,
                    'params': params
                }
                
                if json_data:
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


class BaseDataEnricher(ABC):
    """Base class for enriching data with related objects"""
    
    @abstractmethod
    def enrich_tickets(self, tickets: List[Dict], api_response: Dict) -> List[Dict]:
        """Enrich ticket data with related information"""
        pass
    
    @abstractmethod
    def enrich_conversations(self, conversations: List[Dict], users: List[Dict]) -> List[Dict]:
        """Enrich conversation data with user information"""
        pass
    
    def find_object_by_id(self, objects: List[Dict], target_id: int, id_field: str = 'id') -> Optional[Dict]:
        """Helper to find object by ID in a list"""
        if target_id is None:
            return None
        return next((obj for obj in objects if obj.get(id_field) == target_id), None)
    
    def safe_get_field(self, obj: Optional[Dict], field: str, default: Any = None) -> Any:
        """Safely get field from object"""
        return obj.get(field, default) if obj else default


class BaseFieldMapper(ABC):
    """Base class for mapping platform-specific fields to standardized format"""
    
    @abstractmethod
    def get_standard_field_mapping(self) -> Dict[str, str]:
        """Return mapping of platform fields to standard field names"""
        pass
    
    @abstractmethod
    def process_custom_fields(self, custom_fields: List[Dict], field_definitions: List[Dict]) -> Dict[str, Any]:
        """Process custom fields into key-value pairs"""
        pass
    
    def map_fields(self, data: Dict, mapping: Dict[str, str]) -> Dict[str, Any]:
        """Apply field mapping to data"""
        mapped_data = {}
        for standard_field, platform_field in mapping.items():
            if platform_field in data:
                mapped_data[standard_field] = data[platform_field]
        return mapped_data


class BaseSourceConnector(ABC):
    """Base class for all source connectors"""
    
    def __init__(self, config: SourceConnectorConfig):
        self.config = config
        self.rate_limiter = self._get_rate_limiter()
        self.data_enricher = self._get_data_enricher()
        self.field_mapper = self._get_field_mapper()
    
    @abstractmethod
    def _get_rate_limiter(self) -> BaseSourceRateLimitHandler:
        """Get platform-specific rate limit handler"""
        pass
    
    @abstractmethod
    def _get_data_enricher(self) -> BaseDataEnricher:
        """Get platform-specific data enricher"""
        pass
    
    @abstractmethod
    def _get_field_mapper(self) -> BaseFieldMapper:
        """Get platform-specific field mapper"""
        pass
    
    @abstractmethod
    def _extract_subdomain(self, domainUrl: str) -> str:
        """Extract subdomain from domain URL"""
        pass
    
    @abstractmethod
    def _build_auth(self) -> Tuple:
        """Build authentication tuple for requests"""
        pass
    
    @abstractmethod
    def _validate_config(self) -> bool:
        """Validate connector configuration"""
        pass
    
    def get_tickets(self, query_params: Dict) -> SourceResponse:
        """
        Get tickets from source platform with standardized interface
        
        Args:
            query_params: Query parameters for filtering/pagination
        
        Returns:
            SourceResponse with ticket data
        """
        try:
            if not self._validate_config():
                return SourceResponse(
                    status_code=400,
                    success=False,
                    error_message="Invalid configuration"
                )
            
            # Get field definitions first (needed for custom field mapping)
            field_definitions = self._get_field_definitions()
            
            # Fetch tickets
            response = self._fetch_tickets_api_call(query_params)
            
            if response.status_code == 200:
                api_data = response.json()
                tickets = self._extract_tickets_from_response(api_data)
                
                # Enrich tickets with related data
                enriched_tickets = self.data_enricher.enrich_tickets(tickets, api_data)
                
                # Process custom fields for each ticket
                for ticket in enriched_tickets:
                    if 'custom_fields' in ticket:
                        ticket['custom_fields'] = self.field_mapper.process_custom_fields(
                            ticket['custom_fields'], 
                            field_definitions
                        )
                
                # Extract pagination info
                pagination = self._extract_pagination_info(api_data)
                
                return SourceResponse(
                    status_code=200,
                    success=True,
                    data={
                        "tickets": enriched_tickets,
                        "meta": {"has_more": pagination.has_more},
                        "links": {"next": pagination.next_cursor} if pagination.next_cursor else {}
                    },
                    has_more=pagination.has_more,
                    next_cursor=pagination.next_cursor,
                    total_count=pagination.total_count
                )
            else:
                return SourceResponse(
                    status_code=response.status_code,
                    success=False,
                    error_message=response.text
                )
                
        except Exception as e:
            logging.error(f"Error getting tickets: {e}", exc_info=True)
            return SourceResponse(
                status_code=500,
                success=False,
                error_message=str(e)
            )
    
    def get_conversations(self, ticket_id: str) -> SourceResponse:
        """
        Get conversations for a specific ticket
        
        Args:
            ticket_id: ID of the ticket to get conversations for
        
        Returns:
            SourceResponse with conversation data
        """
        try:
            if not self._validate_config():
                return SourceResponse(
                    status_code=400,
                    success=False,
                    error_message="Invalid configuration"
                )
            
            all_conversations = []
            has_more = True
            cursor = None
            
            while has_more:
                response = self._fetch_conversations_api_call(ticket_id, cursor)
                
                if response.status_code == 200:
                    api_data = response.json()
                    conversations = self._extract_conversations_from_response(api_data)
                    users = api_data.get('users', [])
                    
                    # Enrich conversations with user data
                    enriched_conversations = self.data_enricher.enrich_conversations(conversations, users)
                    all_conversations.extend(enriched_conversations)
                    
                    # Check for more pages
                    pagination = self._extract_pagination_info(api_data)
                    has_more = pagination.has_more
                    cursor = pagination.next_cursor
                    
                else:
                    return SourceResponse(
                        status_code=response.status_code,
                        success=False,
                        error_message=response.text
                    )
            
            return SourceResponse(
                status_code=200,
                success=True,
                data={"conversations": all_conversations}
            )
            
        except Exception as e:
            logging.error(f"Error getting conversations for ticket {ticket_id}: {e}", exc_info=True)
            return SourceResponse(
                status_code=500,
                success=False,
                error_message=str(e)
            )
    
    def get_mapping_objects(self) -> SourceResponse:
        """
        Get mapping objects (groups, agents, fields, etc.) from source platform
        
        Returns:
            SourceResponse with mapping data
        """
        try:
            if not self._validate_config():
                return SourceResponse(
                    status_code=400,
                    success=False,
                    error_message="Invalid configuration"
                )
            
            # Fetch all mapping objects
            field_definitions = self._get_field_definitions()
            groups = self._get_groups()
            agents = self._get_agents()
            
            mapping_data = {
                "Groups": groups,
                "Agents": agents,
                "ticketFields": field_definitions
            }
            
            return SourceResponse(
                status_code=200,
                success=True,
                data=mapping_data
            )
            
        except Exception as e:
            logging.error(f"Error getting mapping objects: {e}", exc_info=True)
            return SourceResponse(
                status_code=500,
                success=False,
                error_message=str(e)
            )
    
    # Abstract methods that must be implemented by platform-specific connectors
    @abstractmethod
    def _fetch_tickets_api_call(self, query_params: Dict) -> requests.Response:
        """Make API call to fetch tickets"""
        pass
    
    @abstractmethod
    def _fetch_conversations_api_call(self, ticket_id: str, cursor: Optional[str] = None) -> requests.Response:
        """Make API call to fetch conversations"""
        pass
    
    @abstractmethod
    def _extract_tickets_from_response(self, api_response: Dict) -> List[Dict]:
        """Extract tickets list from API response"""
        pass
    
    @abstractmethod
    def _extract_conversations_from_response(self, api_response: Dict) -> List[Dict]:
        """Extract conversations list from API response"""
        pass
    
    @abstractmethod
    def _extract_pagination_info(self, api_response: Dict) -> PaginationInfo:
        """Extract pagination information from API response"""
        pass
    
    @abstractmethod
    def _get_field_definitions(self) -> List[Dict]:
        """Get field definitions for custom field mapping"""
        pass
    
    @abstractmethod
    def _get_groups(self) -> List[Dict]:
        """Get groups/teams from platform"""
        pass
    
    @abstractmethod
    def _get_agents(self) -> List[Dict]:
        """Get agents/users from platform"""
        pass


# ===================================================================
# UTILITY FUNCTIONS
# ===================================================================

def convert_source_headers_to_dict(headers):
    """Convert headers from list format to dict format"""
    if isinstance(headers, list):
        return {item["key"]: item["value"] for item in headers}
    return headers


def convert_query_params_to_dict(query_params):
    """Convert query parameters from list format to dict format"""
    if isinstance(query_params, list):
        return {param['key']: param['value'] for param in query_params}
    return query_params


def standardize_source_response_format(response: SourceResponse) -> Dict:
    """Convert SourceResponse to transformer-expected format"""
    if response.success:
        return {
            "status_code": response.status_code,
            "body": response.data
        }
    else:
        return {
            "status_code": response.status_code,
            "error": response.error_message
        }


def extract_subdomain_generic(domainUrl: str) -> str:
    """Generic subdomain extraction for most platforms"""
    no_protocol = re.sub(r"(^\w+:|^)//", "", domainUrl)
    return no_protocol.split(".")[0]


def build_api_url(base_url: str, endpoint: str, params: Optional[Dict] = None) -> str:
    """Build complete API URL with parameters"""
    url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"
    if params:
        # Filter out None values
        filtered_params = {k: v for k, v in params.items() if v is not None}
        if filtered_params:
            url += f"?{urlencode(filtered_params)}"
    return url


def safe_json_response(response: requests.Response) -> Dict:
    """Safely parse JSON response with error handling"""
    try:
        return response.json()
    except ValueError as e:
        logging.error(f"Failed to parse JSON response: {e}")
        return {}


def log_api_call(method: str, url: str, status_code: int, duration: float):
    """Log API call details for debugging"""
    logging.info(f"{method} {url} - {status_code} ({duration:.2f}s)")


# ===================================================================
# COMMON ERROR HANDLING
# ===================================================================

class SourceConnectorError(Exception):
    """Base exception for source connector errors"""
    def __init__(self, message: str, status_code: int = 500):
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)


class AuthenticationError(SourceConnectorError):
    """Authentication failed"""
    def __init__(self, message: str = "Authentication failed"):
        super().__init__(message, 401)


class RateLimitError(SourceConnectorError):
    """Rate limit exceeded"""
    def __init__(self, message: str = "Rate limit exceeded"):
        super().__init__(message, 429)


class ValidationError(SourceConnectorError):
    """Invalid configuration or parameters"""
    def __init__(self, message: str = "Validation failed"):
        super().__init__(message, 400)


def handle_common_errors(response: requests.Response) -> None:
    """Handle common HTTP errors across all platforms"""
    if response.status_code == 401:
        raise AuthenticationError("Invalid credentials")
    elif response.status_code == 403:
        raise AuthenticationError("Access forbidden")
    elif response.status_code == 429:
        raise RateLimitError("Rate limit exceeded")
    elif response.status_code >= 500:
        raise SourceConnectorError(f"Server error: {response.status_code}")