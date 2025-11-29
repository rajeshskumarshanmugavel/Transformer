import pytest
from unittest.mock import MagicMock, patch
from src.utils.redis_client import RedisClient


@pytest.fixture
def mock_redis_client():
    """Fixture to create a mocked Redis client"""
    with patch('src.utils.redis_client.r') as mock_r:
        mock_r.get = MagicMock()
        mock_r.set = MagicMock()
        mock_r.delete = MagicMock()
        mock_r.exists = MagicMock()
        yield mock_r


class TestRedisClient:
    """Test suite for RedisClient class"""
    
    def test_redis_client_initialization(self, mock_redis_client):
        """Test RedisClient initialization"""
        client = RedisClient()
        assert client.client is not None
    
    def test_get_existing_key(self, mock_redis_client):
        """Test get method with existing key"""
        mock_redis_client.exists.return_value = True
        mock_redis_client.get.return_value = b'test_value'
        
        client = RedisClient()
        result = client.get('test_key')
        
        assert result == 'test_value'
        mock_redis_client.exists.assert_called_once_with('test_key')
        mock_redis_client.get.assert_called_once_with('test_key')
    
    def test_get_non_existing_key(self, mock_redis_client):
        """Test get method with non-existing key"""
        mock_redis_client.exists.return_value = False
        
        client = RedisClient()
        result = client.get('non_existing_key')
        
        assert result is None
        mock_redis_client.exists.assert_called_once_with('non_existing_key')
        mock_redis_client.get.assert_not_called()
    
    def test_set_without_expiration(self, mock_redis_client):
        """Test set method without expiration"""
        mock_redis_client.set.return_value = True
        
        client = RedisClient()
        result = client.set('test_key', 'test_value')
        
        assert result is True
        mock_redis_client.set.assert_called_once_with('test_key', 'test_value', ex=None)
    
    def test_set_with_expiration(self, mock_redis_client):
        """Test set method with expiration time"""
        mock_redis_client.set.return_value = True
        
        client = RedisClient()
        result = client.set('test_key', 'test_value', ex=3600)
        
        assert result is True
        mock_redis_client.set.assert_called_once_with('test_key', 'test_value', ex=3600)
    
    def test_delete_key(self, mock_redis_client):
        """Test delete method"""
        mock_redis_client.delete.return_value = 1
        
        client = RedisClient()
        result = client.delete('test_key')
        
        assert result == 1
        mock_redis_client.delete.assert_called_once_with('test_key')
    
    def test_delete_non_existing_key(self, mock_redis_client):
        """Test delete method with non-existing key"""
        mock_redis_client.delete.return_value = 0
        
        client = RedisClient()
        result = client.delete('non_existing_key')
        
        assert result == 0
        mock_redis_client.delete.assert_called_once_with('non_existing_key')
    
    def test_get_with_special_characters(self, mock_redis_client):
        """Test get method with special characters in value"""
        special_value = 'test_value_with_special_chars_@#$%'
        mock_redis_client.exists.return_value = True
        mock_redis_client.get.return_value = special_value.encode('utf-8')
        
        client = RedisClient()
        result = client.get('test_key')
        
        assert result == special_value
    
    def test_set_with_complex_value(self, mock_redis_client):
        """Test set method with complex JSON value"""
        import json
        complex_value = json.dumps({'key': 'value', 'nested': {'data': 123}})
        mock_redis_client.set.return_value = True
        
        client = RedisClient()
        result = client.set('test_key', complex_value, ex=7200)
        
        assert result is True
        mock_redis_client.set.assert_called_once_with('test_key', complex_value, ex=7200)
