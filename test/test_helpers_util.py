import pytest
import json
from datetime import datetime
from src.utils.helpers import canonical_str, type_cast


class TestCanonicalStr:
    """Test suite for canonical_str function"""
    
    def test_canonical_str_with_dict(self):
        """Test canonical_str with dictionary input"""
        value = {"name": "test", "age": 30}
        result = canonical_str(value)
        expected = json.dumps(value, sort_keys=True)
        assert result == expected
        assert result == '{"age": 30, "name": "test"}'
    
    def test_canonical_str_with_list(self):
        """Test canonical_str with list input"""
        value = [3, 1, 2]
        result = canonical_str(value)
        expected = json.dumps(sorted(value))
        assert result == expected
        assert result == '[1, 2, 3]'
    
    def test_canonical_str_with_string(self):
        """Test canonical_str with string input"""
        value = "hello"
        result = canonical_str(value)
        assert result == "hello"
    
    def test_canonical_str_with_number(self):
        """Test canonical_str with number input"""
        value = 42
        result = canonical_str(value)
        assert result == "42"
    
    def test_canonical_str_with_none(self):
        """Test canonical_str with None input"""
        value = None
        result = canonical_str(value)
        assert result == "None"


class TestTypeCast:
    """Test suite for type_cast function"""
    
    def test_type_cast_none_value(self):
        """Test type_cast returns None for None input"""
        assert type_cast(None, "string") is None
        assert type_cast(None, "number") is None
        assert type_cast(None, "boolean") is None
    
    def test_type_cast_string(self):
        """Test type_cast for string type"""
        assert type_cast(123, "string") == "123"
        assert type_cast(True, "string") == "True"
        assert type_cast("hello", "string") == "hello"
    
    def test_type_cast_number_int(self):
        """Test type_cast for number type (integer)"""
        assert type_cast("42", "number") == 42
        assert type_cast("42.0", "number") == 42
        assert type_cast(42, "number") == 42
    
    def test_type_cast_number_float(self):
        """Test type_cast for number type (float)"""
        assert type_cast("42.5", "number") == 42.5
        assert type_cast(42.5, "number") == 42.5
    
    def test_type_cast_boolean_true(self):
        """Test type_cast for boolean type (true values)"""
        assert type_cast(True, "boolean") is True
        assert type_cast("true", "boolean") is True
        assert type_cast("TRUE", "boolean") is True
        assert type_cast("1", "boolean") is True
        assert type_cast("yes", "boolean") is True
        assert type_cast("YES", "boolean") is True
    
    def test_type_cast_boolean_false(self):
        """Test type_cast for boolean type (false values)"""
        assert type_cast(False, "boolean") is False
        assert type_cast("false", "boolean") is False
        assert type_cast("0", "boolean") is False
        assert type_cast("no", "boolean") is False
    
    def test_type_cast_datetime_from_datetime(self):
        """Test type_cast for datetime type with datetime object"""
        dt = datetime(2023, 1, 15, 12, 30, 45)
        result = type_cast(dt, "datetime")
        assert result == dt.isoformat()
    
    def test_type_cast_datetime_from_string(self):
        """Test type_cast for datetime type with ISO string"""
        dt_str = "2023-01-15T12:30:45"
        result = type_cast(dt_str, "datetime")
        assert result == dt_str
    
    def test_type_cast_datetime_from_timestamp(self):
        """Test type_cast for datetime type with timestamp"""
        timestamp = 1673784645
        result = type_cast(timestamp, "datetime")
        expected = datetime.fromtimestamp(timestamp).isoformat()
        assert result == expected
    
    def test_type_cast_array_from_list(self):
        """Test type_cast for array type with list input"""
        value = [1, 2, 3]
        result = type_cast(value, "array")
        assert result == [1, 2, 3]
    
    def test_type_cast_array_from_string(self):
        """Test type_cast for array type with comma-separated string"""
        value = "apple, banana, cherry"
        result = type_cast(value, "array")
        assert result == ["apple", "banana", "cherry"]
    
    def test_type_cast_array_from_single_value(self):
        """Test type_cast for array type with single value"""
        value = 42
        result = type_cast(value, "array")
        assert result == [42]
    
    def test_type_cast_array_of_objects_from_list(self):
        """Test type_cast for array of objects with list input"""
        value = [{"name": "test"}]
        result = type_cast(value, "array of objects")
        assert result == [{"name": "test"}]
    
    def test_type_cast_array_of_objects_from_json_string_list(self):
        """Test type_cast for array of objects with JSON string (list)"""
        value = '[{"name": "test"}, {"name": "test2"}]'
        result = type_cast(value, "array of objects")
        assert result == [{"name": "test"}, {"name": "test2"}]
    
    def test_type_cast_array_of_objects_from_json_string_dict(self):
        """Test type_cast for array of objects with JSON string (dict)"""
        value = '{"name": "test"}'
        result = type_cast(value, "array of objects")
        assert result == [{"name": "test"}]
    
    def test_type_cast_array_of_objects_from_json_string_single(self):
        """Test type_cast for array of objects with JSON string (single value)"""
        value = '"http://example.com/file.pdf"'
        result = type_cast(value, "array of objects")
        assert result == [{"content_url": "http://example.com/file.pdf", "type": "unknown"}]
    
    def test_type_cast_array_of_objects_from_single_url(self):
        """Test type_cast for array of objects with single URL string"""
        value = "http://example.com/file.pdf"
        result = type_cast(value, "array of objects")
        assert result == [{"content_url": "http://example.com/file.pdf", "type": "unknown"}]
    
    def test_type_cast_array_of_objects_from_multiple_urls(self):
        """Test type_cast for array of objects with multiple comma-separated URLs"""
        value = "http://example.com/file1.pdf, http://example.com/file2.pdf"
        result = type_cast(value, "array of objects")
        expected = [
            {"content_url": "http://example.com/file1.pdf", "type": "unknown"},
            {"content_url": "http://example.com/file2.pdf", "type": "unknown"}
        ]
        assert result == expected
    
    def test_type_cast_array_of_objects_from_empty_string(self):
        """Test type_cast for array of objects with empty string"""
        value = ""
        result = type_cast(value, "array of objects")
        assert result == []
    
    def test_type_cast_array_of_objects_from_whitespace_string(self):
        """Test type_cast for array of objects with whitespace string"""
        value = "   "
        result = type_cast(value, "array of objects")
        assert result == []
    
    def test_type_cast_array_of_objects_from_non_empty_value(self):
        """Test type_cast for array of objects with non-empty value"""
        value = 123
        result = type_cast(value, "array of objects")
        assert result == [123]
    
    def test_type_cast_array_of_objects_from_empty_value(self):
        """Test type_cast for array of objects with falsy value"""
        value = 0
        result = type_cast(value, "array of objects")
        assert result == []
    
    def test_type_cast_invalid_json_handling(self):
        """Test type_cast handles invalid JSON in array of objects"""
        value = "{invalid json"
        result = type_cast(value, "array of objects")
        # Should treat as single URL since JSON parsing fails
        assert result == [{"content_url": "{invalid json", "type": "unknown"}]
    
    def test_type_cast_exception_handling(self):
        """Test type_cast returns original value on exception"""
        value = "not a number"
        result = type_cast(value, "number")
        # Should return original value when casting fails
        assert result == "not a number"
    
    def test_type_cast_unknown_type(self):
        """Test type_cast with unknown field type"""
        value = "test"
        result = type_cast(value, "unknown_type")
        assert result == "test"
