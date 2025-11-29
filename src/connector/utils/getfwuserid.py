import json
import base64
import requests
import time
import logging
from typing import Optional, Dict, Any


class FreshworksHelper:
    def __init__(self, domain: str, api_key: str, email: str, product: str):
        """
        Initialize the Fresh API client for Freshservice or Freshdesk.
        
        Args:
            domain: Fresh domain (without .freshservice.com or .freshdesk.com)
            api_key: API key for authentication
            email: Email address to search/create requester for
            product: Either 'freshservice' or 'freshdesk'
        """
        self.domain = domain
        self.api_key = api_key
        self.email = email
        self.product = product.lower()
        self.password = 'x'
        self.auth_string = f"{api_key}:{self.password}"
        self.encoded_auth_string = base64.b64encode(self.auth_string.encode()).decode()
        
        # API URLs
        if self.product == 'freshservice':
            self.get_url = f"https://{domain}.freshservice.com/api/v2/requesters?email={email}"
            self.get_agent_url = f"https://{domain}.freshservice.com/api/v2/agents?email={email}"
            self.post_url = f"https://{domain}.freshservice.com/api/v2/requesters"
        elif self.product == 'freshdesk':
            self.get_url = f"https://{domain}.freshdesk.com/api/v2/contacts?email={email}"
            self.get_agent_url = f"https://{domain}.freshdesk.com/api/v2/agents?email={email}"
            self.post_url = f"https://{domain}.freshdesk.com/api/v2/contacts"
        else:
            raise ValueError("Product must be either 'freshservice' or 'freshdesk'")
        
        # Headers for requests
        self.headers = {
            'Authorization': f'Basic {self.encoded_auth_string}',
            'Content-Type': 'application/json'
        }

    def _make_request_with_retry(self, method: str, url: str, **kwargs) -> Optional[requests.Response]:
        """Make HTTP request with rate limit handling and retries."""
        max_retries = 5
        base_delay = 15
        
        for attempt in range(max_retries):
            try:
                if method.upper() == 'GET':
                    response = requests.get(url, headers=self.headers, timeout=30, **kwargs)
                elif method.upper() == 'POST':
                    response = requests.post(url, headers=self.headers, timeout=30, **kwargs)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")
                
                # Handle rate limiting
                if response.status_code == 429:
                    retry_delay = self._get_retry_delay(response, base_delay * (2 ** attempt))
                    logging.warning(f"FreshworksHelper rate limited. Retrying in {retry_delay}s (attempt {attempt + 1})")
                    time.sleep(retry_delay)
                    continue
                
                # Return response for both success and other errors
                return response
                
            except requests.RequestException as e:
                if attempt == max_retries - 1:
                    logging.error(f"FreshworksHelper request failed after {max_retries} attempts: {e}")
                    return None
                else:
                    wait_time = base_delay * (2 ** attempt)
                    logging.warning(f"FreshworksHelper request failed, retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
        
        return None

    def _get_retry_delay(self, response: requests.Response, default_delay: int = 60) -> int:
        """Extract retry delay from response headers."""
        # Check Retry-After header
        retry_after = response.headers.get('Retry-After')
        if retry_after:
            try:
                return int(retry_after)
            except ValueError:
                pass
        
        # Check X-RateLimit-Reset headers
        reset_time = response.headers.get('X-RateLimit-Reset') or response.headers.get('X-Rate-Limit-Reset')
        if reset_time:
            try:
                reset_timestamp = int(reset_time)
                current_timestamp = int(time.time())
                return max(5, reset_timestamp - current_timestamp)
            except ValueError:
                pass
        
        return default_delay

    def extract_first_name(self, email: str) -> str:
        """Extract first name from email address."""
        parts = email.split('@')[0].split('.')
        return parts[0]

    def extract_requester_id_from_get_response(self, response_data) -> Optional[str]:
        """Extract requester/contact ID from GET response."""
        try:
            # Handle case where response_data might be a list or dict
            if isinstance(response_data, list):
                # If it's a list and has items, get the first one's ID
                if len(response_data) > 0 and 'id' in response_data[0]:
                    return str(response_data[0]['id'])
            elif isinstance(response_data, dict):
                # Check based on product type
                if self.product == 'freshservice':
                    if response_data.get('requesters') and len(response_data['requesters']) > 0:
                        return str(response_data['requesters'][0]['id'])
                elif self.product == 'freshdesk':
                    if response_data.get('contacts') and len(response_data['contacts']) > 0:
                        return str(response_data['contacts'][0]['id'])
        except (KeyError, IndexError, TypeError) as e:
            logging.error(f"Error extracting requester ID: {e}")
        
        return None

    def extract_agent_id_from_get_response(self, response_data) -> Optional[str]:
        """Extract agent ID from GET response."""
        try:
            # Handle case where response_data might be a list or dict
            if isinstance(response_data, list):
                # If it's a list and has items, get the first one's ID
                if len(response_data) > 0 and 'id' in response_data[0]:
                    return str(response_data[0]['id'])
            elif isinstance(response_data, dict):
                # If it's a dict, check for 'agents' key
                if response_data.get('agents') and len(response_data['agents']) > 0:
                    return str(response_data['agents'][0]['id'])
        except (KeyError, IndexError, TypeError) as e:
            logging.error(f"Error extracting agent ID: {e}")
        
        return None

    def extract_requester_id_from_post_response(self, response_data: Dict[str, Any]) -> Optional[str]:
        """Extract requester/contact ID from POST response."""
        try:
            if self.product == 'freshservice':
                if response_data and response_data.get('requester') and response_data['requester'].get('id'):
                    return str(response_data['requester']['id'])
            elif self.product == 'freshdesk':
                # Freshdesk returns the contact object directly
                if response_data and response_data.get('id'):
                    return str(response_data['id'])
        except (KeyError, TypeError) as e:
            logging.error(f"Error extracting requester ID from POST response: {e}")
        
        return None

    def get_requester_id(self) -> Optional[str]:
        """Get requester by email with improved error handling."""
        response = self._make_request_with_retry('GET', self.get_url)
        
        if response is None:
            logging.error("Failed to get requester - no response received")
            return None
        
        if response.status_code == 200:
            try:
                response_data = response.json()
                
                if (response_data == [] or response_data is None or (isinstance(response_data, dict) and response_data.get('requesters') == [])
                    or (isinstance(response_data, list) and len(response_data) == 0)):
                    states = ['deleted', 'unverified', 'blocked', 'verified']

                    for state in states:
                        # Build URL with state parameter
                        url = f"{self.get_url}&state={state}"

                        response = self._make_request_with_retry('GET', url)

                        if response is None:
                            logging.error(f"Failed to get requester (state: {state}) - no response received")
                            continue
                        
                        if response.status_code == 200:
                            try:
                                response_data = response.json()
                                if response_data != [] and response_data is not None and response_data.get('requesters') != []:
                                    break  # Exit loop if we successfully get a response                           
                            except json.JSONDecodeError as e:
                                logging.error(f"Failed to parse requester response (state: {state}): {e}")
                                continue
                        else:
                            logging.warning(f"Get requester failed for state {state} with status {response.status_code}")

                return self.extract_requester_id_from_get_response(response_data)
            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse requester response: {e}")
                return None
        else:
            logging.error(f"Get requester failed with status {response.status_code}: {response.text}")
            return None

    def get_requester_id_by_email(self, email: Optional[str] = None) -> Optional[str]:
        """Get requester by email with improved error handling."""
        if email is None:
            email = self.email

        # Dynamically construct the URL with the provided email
        url = f"https://{self.domain}.freshservice.com/api/v2/requesters?email={email}"

        response = self._make_request_with_retry('GET', url)

        if response is None:
            logging.error("Failed to get requester - no response received")
            return None

        if response.status_code == 200:
            try:
                response_data = response.json()

                if response_data == [] or response_data is None or response_data.get('requesters') == []:
                    states = ['deleted', 'unverified', 'blocked', 'verified']

                    for state in states:
                        # Build URL with state parameter
                        url_with_state = f"{url}&state={state}"

                        response = self._make_request_with_retry('GET', url_with_state)

                        if response is None:
                            logging.error(f"Failed to get requester (state: {state}) - no response received")
                            continue
                        
                        if response.status_code == 200:
                            try:
                                response_data = response.json()     
                                break  # Exit loop if we successfully get a response                           
                            except json.JSONDecodeError as e:
                                logging.error(f"Failed to parse requester response (state: {state}): {e}")
                                continue
                        else:
                            logging.warning(f"Get requester failed for state {state} with status {response.status_code}")

                return self.extract_requester_id_from_get_response(response_data)
            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse requester response: {e}")
                return None
        else:
            logging.error(f"Get requester failed with status {response.status_code}: {response.text}")
            return None

    def get_agent_id(self) -> Optional[str]:
        """Get agent by email with improved error handling."""
        response = self._make_request_with_retry('GET', self.get_agent_url)
        
        if response is None:
            logging.error("Failed to get agent - no response received")
            return None
        
        if response.status_code == 200:
            try:
                response_data = response.json()
                return self.extract_agent_id_from_get_response(response_data)
            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse agent response: {e}")
                return None
        else:
            logging.error(f"Get agent failed with status {response.status_code}: {response.text}")
            return None

    def post_requester(self) -> Optional[str]:
        """Create a new requester/contact with improved error handling."""
        first_name = self.extract_first_name(self.email)
        
        # Different payload structure for each product
        if self.product == 'freshservice':
            post_payload = {
                'first_name': first_name,
                'primary_email': self.email
            }
        elif self.product == 'freshdesk':
            post_payload = {
                'name': first_name,
                'email': self.email
            }
        
        response = self._make_request_with_retry('POST', self.post_url, data=json.dumps(post_payload))
        
        if response is None:
            logging.error("Failed to create requester - no response received")
            return None
        
        if response.status_code == 201:
            try:
                response_data = response.json()
                return self.extract_requester_id_from_post_response(response_data)
            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse create requester response: {e}")
                return None
        else:
            logging.error(f"Create requester failed with status {response.status_code}: {response.text}")
            return None

    def find_or_create_requester(self) -> Optional[str]:
        """
        Main method to find or create a requester with comprehensive error handling.
        
        Returns:
            Optional[str]: The requester ID if found or created successfully, None otherwise
        """
        logging.info(f"Finding or creating requester for email: {self.email}")
        
        # Try to get requester by email
        requester_id = self.get_requester_id()
        if requester_id:
            logging.info(f"Found existing requester with ID: {requester_id}")
            return requester_id
        
        # Try to get agent by email
        agent_id = self.get_agent_id()
        if agent_id:
            logging.info(f"Found existing agent with ID: {agent_id}")
            return agent_id
        
        # Create new requester if not found
        logging.info(f"Creating new requester for email: {self.email}")
        new_requester_id = self.post_requester()
        if new_requester_id:
            logging.info(f"Created new requester with ID: {new_requester_id}")
            return new_requester_id
        
        logging.error(f"Failed to find or create requester for email: {self.email}")
        return None