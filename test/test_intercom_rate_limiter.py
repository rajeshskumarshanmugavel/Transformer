"""
Unit tests for IntercomRateLimitHandler.make_request_with_retry method
Tests the scenario where error responses are returned instead of None
"""
import pytest
import requests
from unittest.mock import Mock, patch, MagicMock
from src.connector.intercom import IntercomRateLimitHandler


class TestIntercomRateLimitHandler:
    """Test suite for IntercomRateLimitHandler error handling"""

    @pytest.fixture
    def rate_limiter(self):
        """Create a rate limiter instance for testing"""
        return IntercomRateLimitHandler()

    def test_make_request_returns_success_response(self, rate_limiter):
        """Test that successful requests return the response object"""
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.text = '{"success": true}'
        
        with patch.object(rate_limiter.session, 'request', return_value=mock_response):
            result = rate_limiter.make_request_with_retry(
                url="https://api.intercom.io/test",
                method="GET"
            )
        
        assert result is not None
        assert result.status_code == 200
        assert result == mock_response

    def test_make_request_returns_error_response_for_http_errors(self, rate_limiter):
        """Test that HTTP errors (non-429) return the error response object"""
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 400
        mock_response.text = '{"error": "Bad Request"}'
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)
        
        with patch.object(rate_limiter.session, 'request', return_value=mock_response):
            result = rate_limiter.make_request_with_retry(
                url="https://api.intercom.io/test",
                method="GET"
            )
        
        # Should return the error response, not None
        assert result is not None
        assert result.status_code == 400
        assert result == mock_response

    def test_make_request_returns_error_response_for_404(self, rate_limiter):
        """Test that 404 errors return the error response object"""
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 404
        mock_response.text = '{"error": "Not Found"}'
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)
        
        with patch.object(rate_limiter.session, 'request', return_value=mock_response):
            result = rate_limiter.make_request_with_retry(
                url="https://api.intercom.io/test",
                method="GET"
            )
        
        assert result is not None
        assert result.status_code == 404
        assert result.text == '{"error": "Not Found"}'

    def test_make_request_returns_error_response_for_500(self, rate_limiter):
        """Test that 500 errors return the error response object"""
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 500
        mock_response.text = '{"error": "Internal Server Error"}'
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)
        
        with patch.object(rate_limiter.session, 'request', return_value=mock_response):
            result = rate_limiter.make_request_with_retry(
                url="https://api.intercom.io/test",
                method="GET"
            )
        
        assert result is not None
        assert result.status_code == 500
        assert '{"error": "Internal Server Error"}' in result.text

    def test_make_request_returns_error_response_for_403(self, rate_limiter):
        """Test that 403 Forbidden errors return the error response object"""
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 403
        mock_response.text = '{"error": "Forbidden", "message": "Access denied"}'
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)
        
        with patch.object(rate_limiter.session, 'request', return_value=mock_response):
            result = rate_limiter.make_request_with_retry(
                url="https://api.intercom.io/test",
                method="GET"
            )
        
        assert result is not None
        assert result.status_code == 403
        assert "Forbidden" in result.text

    def test_make_request_returns_error_response_for_401(self, rate_limiter):
        """Test that 401 Unauthorized errors return the error response object"""
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 401
        mock_response.text = '{"error": "Unauthorized", "message": "Invalid API key"}'
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)
        
        with patch.object(rate_limiter.session, 'request', return_value=mock_response):
            result = rate_limiter.make_request_with_retry(
                url="https://api.intercom.io/test",
                method="GET"
            )
        
        assert result is not None
        assert result.status_code == 401
        assert "Unauthorized" in result.text

    def test_make_request_returns_error_response_on_request_exception_with_response(self, rate_limiter):
        """Test that RequestException with response returns the response object"""
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 503
        mock_response.text = '{"error": "Service Unavailable"}'
        
        request_exception = requests.exceptions.RequestException("Connection error")
        request_exception.response = mock_response
        
        with patch.object(rate_limiter.session, 'request', side_effect=request_exception):
            result = rate_limiter.make_request_with_retry(
                url="https://api.intercom.io/test",
                method="GET"
            )
        
        # Should return the error response from the exception
        assert result is not None
        assert result == mock_response

    def test_make_request_retries_on_429_rate_limit(self, rate_limiter):
        """Test that 429 rate limit errors trigger retry logic"""
        # First call returns 429, second call succeeds
        mock_rate_limit_response = Mock(spec=requests.Response)
        mock_rate_limit_response.status_code = 429
        mock_rate_limit_response.headers = {"Retry-After": "1"}
        
        mock_success_response = Mock(spec=requests.Response)
        mock_success_response.status_code = 200
        mock_success_response.text = '{"success": true}'
        
        with patch.object(rate_limiter.session, 'request', side_effect=[
            mock_rate_limit_response,
            mock_success_response
        ]):
            with patch('time.sleep'):  # Mock sleep to speed up test
                result = rate_limiter.make_request_with_retry(
                    url="https://api.intercom.io/test",
                    method="GET"
                )
        
        # Should eventually return success after retry
        assert result is not None
        assert result.status_code == 200

    def test_make_request_response_preserves_error_details(self, rate_limiter):
        """Test that error response preserves all error details for caller inspection"""
        mock_response = Mock(spec=requests.Response)
        mock_response.status_code = 422
        mock_response.text = '{"errors": [{"field": "email", "message": "Invalid email format"}]}'
        mock_response.json.return_value = {
            "errors": [{"field": "email", "message": "Invalid email format"}]
        }
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)
        
        with patch.object(rate_limiter.session, 'request', return_value=mock_response):
            result = rate_limiter.make_request_with_retry(
                url="https://api.intercom.io/test",
                method="POST",
                json={"email": "invalid"}
            )
        
        # Verify caller can inspect the error response
        assert result is not None
        assert result.status_code == 422
        assert "errors" in result.text
        error_data = result.json()
        assert error_data["errors"][0]["field"] == "email"
        assert error_data["errors"][0]["message"] == "Invalid email format"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
