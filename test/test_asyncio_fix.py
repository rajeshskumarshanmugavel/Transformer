#!/usr/bin/env python3
"""
Test script to verify the asyncio fix works correctly.
This tests both scenarios:
1. Calling from a context with no running event loop
2. Calling from a context with an already running event loop
"""

import asyncio
import sys
import os

# Add the project root to the Python path so src imports work
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.connector.intercom import create_intercom_message_universal

def test_no_event_loop():
    """Test calling the universal function when no event loop is running."""
    print("Testing with no event loop...")
    
    # Mock data for testing
    queryParams = []
    body = {
        "conversation_id": "test_conv_123",
        "message_body": "Test message",
        "author_type": "admin",
        "admin_id": "test_admin_123"
    }
    headers = {"Authorization": "Bearer test_token"}
    
    # This should work without asyncio errors
    result = create_intercom_message_universal(queryParams, body, headers)
    print(f"✅ No event loop test passed. Result type: {type(result)}")
    
    # Assert that we get a dictionary response (even if it's an error response)
    assert isinstance(result, dict), f"Expected dict, got {type(result)}"

def test_with_event_loop():
    """Test calling the universal function from within an async context."""
    
    async def async_test():
        print("Testing with existing event loop...")
        
        # Mock data for testing
        queryParams = []
        body = {
            "conversation_id": "test_conv_456", 
            "message_body": "Test async message",
            "author_type": "admin",
            "admin_id": "test_admin_456"
        }
        headers = {"Authorization": "Bearer test_token"}
        
        # This should work without "RuntimeError: no running event loop" 
        result = create_intercom_message_universal(queryParams, body, headers)
        print(f"✅ With event loop test passed. Result type: {type(result)}")
        
        # Assert that we get a dictionary response (even if it's an error response)
        assert isinstance(result, dict), f"Expected dict, got {type(result)}"
        return result
    
    # Run the async test
    result = asyncio.run(async_test())
    assert isinstance(result, dict)

if __name__ == "__main__":
    # Run the tests manually when called as a script
    print("🧪 Testing asyncio fixes for intercom.py...")
    print("=" * 50)
    
    # Test 1: No event loop context
    try:
        test_no_event_loop()
        print("Test 1 PASSED ✅")
        test1_passed = True
    except Exception as e:
        print(f"Test 1 FAILED ❌: {e}")
        test1_passed = False
    
    print()
    
    # Test 2: With event loop context  
    try:
        test_with_event_loop()
        print("Test 2 PASSED ✅")
        test2_passed = True
    except Exception as e:
        print(f"Test 2 FAILED ❌: {e}")
        test2_passed = False
        
    print()
    print("=" * 50)
    if test1_passed and test2_passed:
        print("🎉 All asyncio tests passed!")
        sys.exit(0)
    else:
        print("💥 Some asyncio tests failed!")
        sys.exit(1)
