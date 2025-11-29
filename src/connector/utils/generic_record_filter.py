"""
generic_record_filter.py - FIXED VERSION
==========================================

Key fixes applied:
1. Fixed ParameterParser to properly handle exclusion prefixes
2. Fixed create_filters_from_params to process exclusion prefixes correctly
3. Fixed operator mapping for text operations
4. Fixed date range handling to avoid overwriting parameters
5. Fixed pattern operator handling

Issues found and fixed:
- Example 2: subject_not_contains was being overwritten by duplicate keys
- Example 3: Wrong operator for pattern matching and incorrect field name
- Example 5: Status filtering was broken due to value conversion
- Exclusion logic wasn't being properly applied in parameter parsing
"""

import re
import fnmatch
from datetime import datetime
from typing import List, Dict, Any, Union, Optional, Tuple
import logging


class FilterOperator:
    """Standard filter operators"""

    # Equality operators
    EQ = "eq"  # Equal
    NE = "ne"  # Not equal

    # Comparison operators
    GT = "gt"  # Greater than
    GTE = "gte"  # Greater than or equal
    LT = "lt"  # Less than
    LTE = "lte"  # Less than or equal

    # Collection operators
    IN = "in"  # Value in list
    NOT_IN = "not_in"  # Value not in list

    # Text operators
    CONTAINS = "contains"  # String contains substring
    NOT_CONTAINS = "not_contains"  # String does not contain substring
    STARTS_WITH = "starts_with"  # String starts with
    ENDS_WITH = "ends_with"  # String ends with
    PATTERN = "pattern"  # Wildcard pattern matching (*, ?)
    REGEX = "regex"  # Regular expression matching

    # Date operators
    DATE_RANGE = "date_range"  # Date between start and end
    DATE_BEFORE = "date_before"  # Date before value
    DATE_AFTER = "date_after"  # Date after value

    # Boolean operators
    IS_TRUE = "is_true"  # Field is True
    IS_FALSE = "is_false"  # Field is False
    IS_NULL = "is_null"  # Field is None/null
    IS_NOT_NULL = "is_not_null"  # Field is not None/null

    # Advanced operators
    EXISTS = "exists"  # Field exists in record
    NOT_EXISTS = "not_exists"  # Field does not exist in record


class FilterConfig:
    """Configuration for a single field filter"""

    def __init__(
        self,
        field: str,
        operator: str,
        value: Any,
        case_sensitive: bool = True,
        include: bool = True,
        nested_path: str = None,
        transform: callable = None,
    ):
        self.field = field
        self.operator = operator
        self.value = value
        self.case_sensitive = case_sensitive
        self.include = include  # True = include matching, False = exclude matching
        self.nested_path = nested_path  # Support for nested fields like "user.email"
        self.transform = transform  # Optional value transformation function

    @classmethod
    def from_dict(cls, field: str, config: Dict) -> "FilterConfig":
        """Create FilterConfig from dictionary"""
        return cls(
            field=field,
            operator=config.get("op", FilterOperator.EQ),
            value=config.get("value"),
            case_sensitive=config.get("case_sensitive", True),
            include=config.get("include", True),
            nested_path=config.get("nested_path"),
            transform=config.get("transform"),
        )


class ParameterParser:
    """FIXED: Enhanced parameter parser supporting suffix detection and exclusion logic"""

    # Map suffixes to operators
    SUFFIX_TO_OPERATOR = {
        "contains": "contains",
        "not_contains": "not_contains",
        "pattern": "pattern",
        "not_pattern": "not_pattern",
        "regex": "regex",
        "starts_with": "starts_with",
        "ends_with": "ends_with",
        "start": "date_after",
        "end": "date_before",
        "before": "date_before",
        "after": "date_after",
        "gt": "gt",
        "gte": "gte",
        "lt": "lt",
        "lte": "lte",
        "ne": "ne",
        "not_in": "not_in",
        "is_null": "is_null",
        "is_not_null": "is_not_null",
        "exists": "exists",
        "not_exists": "not_exists",
    }

    @classmethod
    def parse_parameter(cls, param_name: str, param_value: str) -> Tuple[str, str, Any]:
        """
        FIXED: Parse parameter name to extract field and operator

        Returns: (field_name, operator, processed_value)
        """

        # Method 1: Explicit operator (field__operator)
        if "__" in param_name:
            field, operator = param_name.split("__", 1)
            return field, operator, param_value

        # Method 2: Suffix convention (field_suffix)
        elif "_" in param_name:
            parts = param_name.split("_")

            # Handle multi-word suffixes - work backwards from end
            if len(parts) >= 3:
                # Check for two-word suffixes first
                two_word_suffix = "_".join(parts[-2:])
                if two_word_suffix in cls.SUFFIX_TO_OPERATOR:
                    field = "_".join(parts[:-2])
                    operator = cls.SUFFIX_TO_OPERATOR[two_word_suffix]
                    return field, operator, param_value

            # Check for single-word suffix
            if len(parts) >= 2:
                single_word_suffix = parts[-1]
                if single_word_suffix in cls.SUFFIX_TO_OPERATOR:
                    field = "_".join(parts[:-1])
                    operator = cls.SUFFIX_TO_OPERATOR[single_word_suffix]
                    return field, operator, param_value

        # Method 3: No suffix - default behavior
        # Check if value contains commas (multiple values)
        if isinstance(param_value, str) and "," in param_value:
            return param_name, "in", param_value
        else:
            return param_name, "eq", param_value


class DateParser:
    """Enhanced utility for parsing various date formats"""

    COMMON_FORMATS = [
        "%Y-%m-%dT%H:%M:%S%z",  # ISO with timezone
        "%Y-%m-%dT%H:%M:%SZ",  # ISO with Z
        "%Y-%m-%dT%H:%M:%S",  # ISO without timezone
        "%Y-%m-%d %H:%M:%S",  # Space separated
        "%Y-%m-%d",  # Date only
        "%m/%d/%Y",  # US format
        "%d/%m/%Y",  # EU format
        "%Y/%m/%d",  # Alternative format
    ]

    @classmethod
    def parse(cls, date_str: str) -> Optional[datetime]:
        """Parse date string into datetime object with enhanced error handling"""
        if not date_str or not isinstance(date_str, str):
            return None

        # Clean the date string
        date_str = date_str.strip()
        if not date_str:
            return None

        # Remove timezone info for comparison if present
        date_clean = re.split(r"[+-]\d{2}:\d{2}$", date_str)[0].replace("Z", "")

        for fmt in cls.COMMON_FORMATS:
            try:
                return datetime.strptime(date_clean, fmt)
            except ValueError:
                continue

        # Try parsing common variations
        try:
            # Handle ISO format without time
            if "T" not in date_clean and len(date_clean) == 10:
                return datetime.strptime(date_clean, "%Y-%m-%d")
        except ValueError:
            pass

        logging.warning(f"Could not parse date: {date_str}")
        return None


class FieldExtractor:
    """Enhanced utility for extracting values from nested fields"""

    @staticmethod
    def get_field_value(record: Dict, field_path: str) -> Any:
        """
        Extract field value supporting nested paths like 'user.email' or 'custom_fields.priority'
        Enhanced with better error handling
        """
        if not record or not field_path:
            return None

        try:
            # Handle nested field paths
            if "." in field_path:
                parts = field_path.split(".")
                current = record

                for part in parts:
                    if isinstance(current, dict) and part in current:
                        current = current[part]
                    else:
                        return None
                return current
            else:
                return record.get(field_path)
        except (TypeError, AttributeError):
            return None


class RecordFilter:
    """Enhanced main filtering engine for record collections"""

    def __init__(self, logic: str = "AND"):
        """
        Initialize filter with logic type

        Args:
            logic: 'AND' or 'OR' for combining multiple filters
        """
        self.logic = logic.upper()
        self.logger = logging.getLogger(__name__)
        self.stats = {"total_processed": 0, "total_filtered": 0, "filter_time": 0.0}

    @classmethod
    def apply(
        cls, records: List[Dict], filter_configs: Dict, logic: str = "AND"
    ) -> List[Dict]:
        """
        Apply filters to a list of records

        Args:
            records: List of dictionaries to filter
            filter_configs: Dictionary of field -> filter configuration
            logic: 'AND' or 'OR' for combining filters

        Returns:
            Filtered list of records
        """
        if not records or not filter_configs:
            return records

        filter_engine = cls(logic)

        # Convert dict configs to FilterConfig objects
        filters = []
        for field, config in filter_configs.items():
            if isinstance(config, dict):
                filters.append(FilterConfig.from_dict(field, config))
            elif isinstance(config, FilterConfig):
                filters.append(config)

        return filter_engine.filter_records(records, filters)

    def filter_records(
        self, records: List[Dict], filters: List[FilterConfig]
    ) -> List[Dict]:
        """Filter records using provided filter configurations with performance tracking"""
        if not filters:
            return records

        import time

        start_time = time.time()

        filtered_records = []

        for record in records:
            if self._record_matches(record, filters):
                filtered_records.append(record)

        # Update stats
        filter_time = time.time() - start_time
        self.stats["total_processed"] = len(records)
        self.stats["total_filtered"] = len(filtered_records)
        self.stats["filter_time"] = filter_time

        self.logger.info(
            f"Filtered {len(records)} → {len(filtered_records)} records in {filter_time:.3f}s"
        )
        return filtered_records

    def _record_matches(self, record: Dict, filters: List[FilterConfig]) -> bool:
        """Check if a record matches all/any filters based on logic"""
        if not filters:
            return True

        results = []

        for filter_config in filters:
            match_result = self._apply_single_filter(record, filter_config)
            results.append(match_result)

            # Early exit optimization for AND logic
            if self.logic == "AND" and not match_result:
                return False
            # Early exit optimization for OR logic
            elif self.logic == "OR" and match_result:
                return True

        # Apply logic
        if self.logic == "AND":
            return all(results)
        elif self.logic == "OR":
            return any(results)
        else:
            raise ValueError(f"Unsupported logic: {self.logic}")

    def _apply_single_filter(self, record: Dict, filter_config: FilterConfig) -> bool:
        """Apply a single filter to a record with enhanced error handling"""
        try:
            # Extract field value (supports nested paths)
            field_value = FieldExtractor.get_field_value(record, filter_config.field)

            # Apply transformation if provided
            if filter_config.transform and field_value is not None:
                try:
                    field_value = filter_config.transform(field_value)
                except Exception as e:
                    self.logger.warning(
                        f"Transform error for field {filter_config.field}: {e}"
                    )
                    return True  # Don't filter out on transform errors

            # Apply the operator
            match_result = self._apply_operator(
                field_value,
                filter_config.operator,
                filter_config.value,
                filter_config.case_sensitive,
            )

            # Apply include/exclude logic
            return match_result if filter_config.include else not match_result

        except Exception as e:
            self.logger.warning(f"Error applying filter {filter_config.field}: {e}")
            return True  # Don't filter out on errors

    def _apply_operator(
        self,
        field_value: Any,
        operator: str,
        filter_value: Any,
        case_sensitive: bool = True,
    ) -> bool:
        """Apply specific operator comparison with enhanced handling"""

        # Handle null checks first
        if operator == FilterOperator.IS_NULL:
            return field_value is None
        elif operator == FilterOperator.IS_NOT_NULL:
            return field_value is not None
        elif operator == FilterOperator.EXISTS:
            return field_value is not None
        elif operator == FilterOperator.NOT_EXISTS:
            return field_value is None

        # If field_value is None and we're not checking for null, return False
        if field_value is None:
            return False

        # Boolean operators
        if operator == FilterOperator.IS_TRUE:
            return bool(field_value) is True
        elif operator == FilterOperator.IS_FALSE:
            return bool(field_value) is False

        # Equality operators
        elif operator == FilterOperator.EQ:
            return field_value == filter_value
        elif operator == FilterOperator.NE:
            return field_value != filter_value

        # Comparison operators (with type safety)
        elif operator == FilterOperator.GT:
            try:
                return field_value > filter_value
            except TypeError:
                return False
        elif operator == FilterOperator.GTE:
            try:
                return field_value >= filter_value
            except TypeError:
                return False
        elif operator == FilterOperator.LT:
            try:
                return field_value < filter_value
            except TypeError:
                return False
        elif operator == FilterOperator.LTE:
            try:
                return field_value <= filter_value
            except TypeError:
                return False

        # Collection operators
        elif operator == FilterOperator.IN:
            return field_value in (
                filter_value
                if isinstance(filter_value, (list, tuple))
                else [filter_value]
            )
        elif operator == FilterOperator.NOT_IN:
            return field_value not in (
                filter_value
                if isinstance(filter_value, (list, tuple))
                else [filter_value]
            )

        # Text operators
        elif operator in [
            FilterOperator.CONTAINS,
            FilterOperator.NOT_CONTAINS,
            FilterOperator.STARTS_WITH,
            FilterOperator.ENDS_WITH,
        ]:
            return self._apply_text_operator(
                field_value, operator, filter_value, case_sensitive
            )

        # Pattern operators
        elif operator == FilterOperator.PATTERN:
            return self._apply_pattern_operator(
                field_value, filter_value, case_sensitive
            )
        elif operator == FilterOperator.REGEX:
            return self._apply_regex_operator(field_value, filter_value, case_sensitive)

        # Date operators
        elif operator in [
            FilterOperator.DATE_RANGE,
            FilterOperator.DATE_BEFORE,
            FilterOperator.DATE_AFTER,
        ]:
            return self._apply_date_operator(field_value, operator, filter_value)

        else:
            raise ValueError(f"Unsupported operator: {operator}")

    def _apply_text_operator(
        self, field_value: Any, operator: str, filter_value: str, case_sensitive: bool
    ) -> bool:
        """Apply text-based operators with enhanced handling"""
        try:
            field_str = str(field_value)
            filter_str = str(filter_value)

            if not case_sensitive:
                field_str = field_str.lower()
                filter_str = filter_str.lower()

            if operator == FilterOperator.CONTAINS:
                return filter_str in field_str
            elif operator == FilterOperator.NOT_CONTAINS:
                return filter_str not in field_str
            elif operator == FilterOperator.STARTS_WITH:
                return field_str.startswith(filter_str)
            elif operator == FilterOperator.ENDS_WITH:
                return field_str.endswith(filter_str)
        except Exception:
            return False

        return False

    # def _apply_pattern_operator(
    #     self, field_value: Any, pattern: str, case_sensitive: bool
    # ) -> bool:
    #     """Apply wildcard pattern matching (*, ?) with enhanced handling"""
    #     try:
    #         field_str = str(field_value)

    #         if not case_sensitive:
    #             field_str = field_str.lower()
    #             pattern = pattern.lower()

    #         return fnmatch.fnmatch(field_str, pattern)
    #     except Exception:
    #         return False

    def _apply_pattern_operator(
        self, field_value: Any, pattern: str, case_sensitive: bool
    ) -> bool:
        """Apply wildcard pattern matching (*, ?) with enhanced handling for multiple patterns"""
        try:
            field_str = str(field_value)

            if not case_sensitive:
                field_str = field_str.lower()

            # Handle multiple patterns (list)
            if isinstance(pattern, list):
                patterns = pattern
                if not case_sensitive:
                    patterns = [p.lower() for p in patterns]

                # Return True if field matches ANY of the patterns
                return any(fnmatch.fnmatch(field_str, p) for p in patterns)

            # Handle single pattern (string)
            else:
                if not case_sensitive:
                    pattern = pattern.lower()
                return fnmatch.fnmatch(field_str, pattern)

        except Exception:
            return False

    def _apply_regex_operator(
        self, field_value: Any, pattern: str, case_sensitive: bool
    ) -> bool:
        """Apply regular expression matching with enhanced error handling"""
        try:
            field_str = str(field_value)
            flags = 0 if case_sensitive else re.IGNORECASE
            return bool(re.search(pattern, field_str, flags))
        except re.error as e:
            self.logger.warning(f"Invalid regex pattern '{pattern}': {e}")
            return False
        except Exception:
            return False

    def _apply_date_operator(
        self, field_value: Any, operator: str, filter_value: Any
    ) -> bool:
        """Apply date-based operators with enhanced parsing"""
        field_date = DateParser.parse(str(field_value)) if field_value else None

        if not field_date:
            return False

        try:
            if operator == FilterOperator.DATE_RANGE:
                if (
                    not isinstance(filter_value, (list, tuple))
                    or len(filter_value) != 2
                ):
                    return False

                start_date = DateParser.parse(str(filter_value[0]))
                end_date = DateParser.parse(str(filter_value[1]))

                if not start_date or not end_date:
                    return False

                return start_date <= field_date <= end_date

            elif operator == FilterOperator.DATE_BEFORE:
                compare_date = DateParser.parse(str(filter_value))
                return field_date < compare_date if compare_date else False

            elif operator == FilterOperator.DATE_AFTER:
                compare_date = DateParser.parse(str(filter_value))
                return field_date > compare_date if compare_date else False
        except Exception:
            return False

        return False

    def get_stats(self) -> Dict:
        """Get filtering performance statistics"""
        return self.stats.copy()


# Enhanced convenience functions
def filter_records(records: List[Dict], **filters) -> List[Dict]:
    """
    Enhanced convenience function for simple filtering with suffix support

    Example:
        filtered = filter_records(tickets,
                                status__in=[1,2,3],
                                priority__gte=3,
                                subject_contains='urgent')
    """
    filter_configs = {}

    for field_op, value in filters.items():
        field, operator, processed_value = ParameterParser.parse_parameter(
            field_op, str(value)
        )

        # Handle special value processing
        if operator == "in" and isinstance(value, str):
            processed_value = [v.strip() for v in value.split(",")]
        else:
            processed_value = value

        filter_configs[field] = {"op": operator, "value": processed_value}

    return RecordFilter.apply(records, filter_configs)


def create_filters_from_params(filter_params: Dict) -> Dict:
    """
    FIXED: Enhanced Freshdesk filter creation with proper exclusion handling
    """
    filter_configs = {}

    # Track date range parameters
    date_ranges = {}

    for param_name, param_value in filter_params.items():
        # FIXED: Check for exclusion prefixes BEFORE parsing
        include_flag = True
        original_param = param_name

        if param_name.startswith("exclude_"):
            include_flag = False
            param_name = param_name[8:]  # Remove 'exclude_' prefix
        elif param_name.startswith("not_"):
            include_flag = False
            param_name = param_name[4:]  # Remove 'not_' prefix

        # Parse the parameter after removing exclusion prefixes
        field, operator, value = ParameterParser.parse_parameter(
            param_name, param_value
        )

        # CRITICAL FIX: For built-in negation operators, convert to positive operator + exclude
        if operator in ["not_contains", "not_in", "not_exists", "not_pattern"]:
            if operator == "not_contains":
                operator = "contains"
            elif operator == "not_in":
                operator = "in"
            elif operator == "not_exists":
                operator = "exists"
            elif operator == "not_pattern":
                operator = "pattern"
            # Flip the include flag
            include_flag = not include_flag

        # Handle date range parameters specially
        if operator in ["date_after", "date_before"] and field.endswith("_at"):
            # Initialize date range tracking for this field
            if field not in date_ranges:
                date_ranges[field] = {}

            if operator == "date_after":
                date_ranges[field]["start"] = value
            elif operator == "date_before":
                date_ranges[field]["end"] = value

            # Skip individual processing - we'll handle ranges below
            continue

        # FIXED: Handle value conversion for 'in' operator
        if operator == "in":
            # Convert comma-separated values to appropriate list
            if isinstance(value, str) and "," in value:
                if field in ["status", "priority", "source", "group_id"]:
                    # Integer fields
                    try:
                        value = [int(v.strip()) for v in value.split(",")]
                    except ValueError:
                        continue  # Skip invalid integer values
                else:
                    # String fields
                    value = [v.strip() for v in value.split(",")]
            # FIXED: Handle single integer values for integer fields
            elif field in ["status", "priority", "source", "group_id"]:
                try:
                    value = int(value)
                except ValueError:
                    continue

        # FIXED: Handle comparison operators with integer conversion
        elif operator in ["eq", "ne", "gt", "gte", "lt", "lte"]:
            if field in ["status", "priority", "source", "group_id"]:
                try:
                    value = int(value)
                except ValueError:
                    continue
            elif field == "spam":
                # Handle spam boolean conversion
                if isinstance(value, str):
                    value = value.lower() == "true"
            elif field == "deleted":
                # Handle deleted boolean conversion
                if isinstance(value, str):
                    value = value.lower() == "true"

        # Build filter config
        filter_config = {
            "op": operator,
            "value": value,
            "include": include_flag,  # FIXED: Apply the exclusion flag
        }

        # Add case sensitivity for text operations
        if operator in ["contains", "pattern", "starts_with", "ends_with", "regex"]:
            filter_config["case_sensitive"] = False

        filter_configs[field] = filter_config

    # Process date ranges after all parameters
    for field, range_data in date_ranges.items():
        if "start" in range_data and "end" in range_data:
            # Both start and end provided - use date_range operator
            filter_configs[field] = {
                "op": "date_range",
                "value": [range_data["start"], range_data["end"]],
                "include": True,
            }
        elif "start" in range_data:
            # Only start provided - use date_after
            filter_configs[field] = {
                "op": "date_after",
                "value": range_data["start"],
                "include": True,
            }
        elif "end" in range_data:
            # Only end provided - use date_before
            filter_configs[field] = {
                "op": "date_before",
                "value": range_data["end"],
                "include": True,
            }

    return filter_configs


# def create_freshservice_filters(filter_params: Dict) -> Dict:
#     """
#     Enhanced Freshservice filter creation that properly handles types and date ranges.
#     """
#     filter_configs = {}
#     date_ranges = {}

#     for param_name, param_value in filter_params.items():
#         include_flag = True

#         if param_name.startswith("exclude_"):
#             include_flag = False
#             param_name = param_name[8:]
#         elif param_name.startswith("not_"):
#             include_flag = False
#             param_name = param_name[4:]

#         field, operator, value = ParameterParser.parse_parameter(
#             param_name, param_value
#         )

#         # Special handling for date range parts
#         if operator in ["date_after", "date_before"] and field.endswith("_at"):
#             if field not in date_ranges:
#                 date_ranges[field] = {}
#             if operator == "date_after":
#                 date_ranges[field]["start"] = value
#             elif operator == "date_before":
#                 date_ranges[field]["end"] = value
#             continue

#         # Parse comma-separated lists
#         if operator == "in" and isinstance(value, str) and "," in value:
#             if field in ["priority", "status", "group_id", "impact", "urgency"]:
#                 try:
#                     value = [int(v.strip()) for v in value.split(",")]
#                 except ValueError:
#                     continue
#             else:
#                 value = [v.strip() for v in value.split(",")]

#         # Boolean normalization (if needed)
#         if operator == "eq" and field in ["spam", "is_escalated"]:
#             if isinstance(value, str):
#                 value = value.lower() == "true"

#         filter_config = {"op": operator, "value": value, "include": include_flag}

#         if operator in ["contains", "starts_with", "ends_with", "pattern", "regex"]:
#             filter_config["case_sensitive"] = False

#         filter_configs[field] = filter_config

#     # Handle collected date ranges
#     for field, range_data in date_ranges.items():
#         if "start" in range_data and "end" in range_data:
#             filter_configs[field] = {
#                 "op": "date_range",
#                 "value": [range_data["start"], range_data["end"]],
#                 "include": True,
#             }
#         elif "start" in range_data:
#             filter_configs[field] = {
#                 "op": "date_after",
#                 "value": range_data["start"],
#                 "include": True,
#             }
#         elif "end" in range_data:
#             filter_configs[field] = {
#                 "op": "date_before",
#                 "value": range_data["end"],
#                 "include": True,
#             }

#     return filter_configs


def create_filters_from_params(filter_params: Dict) -> Dict:
    """
    FIXED: Enhanced Freshdesk filter creation with proper exclusion handling
    """
    filter_configs = {}

    # Track date range parameters
    date_ranges = {}

    for param_name, param_value in filter_params.items():
        # FIXED: Check for exclusion prefixes BEFORE parsing
        include_flag = True
        original_param = param_name

        if param_name.startswith("exclude_"):
            include_flag = False
            param_name = param_name[8:]  # Remove 'exclude_' prefix
        elif param_name.startswith("not_"):
            include_flag = False
            param_name = param_name[4:]  # Remove 'not_' prefix

        # Parse the parameter after removing exclusion prefixes
        field, operator, value = ParameterParser.parse_parameter(
            param_name, param_value
        )

        # CRITICAL FIX: For built-in negation operators, convert to positive operator + exclude
        if operator in ["not_contains", "not_in", "not_exists", "not_pattern"]:
            if operator == "not_contains":
                operator = "contains"
            elif operator == "not_in":
                operator = "in"
            elif operator == "not_exists":
                operator = "exists"
            elif operator == "not_pattern":
                operator = "pattern"
            # Flip the include flag
            include_flag = not include_flag

        # Handle date range parameters specially
        if operator in ["date_after", "date_before"] and field.endswith("_at"):
            # Initialize date range tracking for this field
            if field not in date_ranges:
                date_ranges[field] = {}

            if operator == "date_after":
                date_ranges[field]["start"] = value
            elif operator == "date_before":
                date_ranges[field]["end"] = value

            # Skip individual processing - we'll handle ranges below
            continue

        # FIXED: Handle value conversion for 'in' operator
        if operator == "in":
            # Convert comma-separated values to appropriate list
            if isinstance(value, str) and "," in value:
                if field in ["status", "priority", "source", "group_id"]:
                    # Integer fields
                    try:
                        value = [int(v.strip()) for v in value.split(",")]
                    except ValueError:
                        continue  # Skip invalid integer values
                else:
                    # String fields
                    value = [v.strip() for v in value.split(",")]
            # FIXED: Handle single integer values for integer fields
            elif field in ["status", "priority", "source", "group_id"]:
                try:
                    value = int(value)
                except ValueError:
                    continue

        # NEW: Handle pattern operator with multiple comma-separated patterns
        elif operator == "pattern":
            if isinstance(value, str) and "," in value:
                # Split comma-separated patterns into a list
                value = [pattern.strip() for pattern in value.split(",")]

        # FIXED: Handle comparison operators with integer conversion
        elif operator in ["eq", "ne", "gt", "gte", "lt", "lte"]:
            if field in ["status", "priority", "source", "group_id"]:
                try:
                    value = int(value)
                except ValueError:
                    continue
            elif field == "spam":
                # Handle spam boolean conversion
                if isinstance(value, str):
                    value = value.lower() == "true"
            elif field == "deleted":
                # Handle deleted boolean conversion
                if isinstance(value, str):
                    value = value.lower() == "true"

        # Build filter config
        filter_config = {
            "op": operator,
            "value": value,
            "include": include_flag,  # FIXED: Apply the exclusion flag
        }

        # Add case sensitivity for text operations
        if operator in ["contains", "pattern", "starts_with", "ends_with", "regex"]:
            filter_config["case_sensitive"] = False

        filter_configs[field] = filter_config

    # Process date ranges after all parameters
    for field, range_data in date_ranges.items():
        if "start" in range_data and "end" in range_data:
            # Both start and end provided - use date_range operator
            filter_configs[field] = {
                "op": "date_range",
                "value": [range_data["start"], range_data["end"]],
                "include": True,
            }
        elif "start" in range_data:
            # Only start provided - use date_after
            filter_configs[field] = {
                "op": "date_after",
                "value": range_data["start"],
                "include": True,
            }
        elif "end" in range_data:
            # Only end provided - use date_before
            filter_configs[field] = {
                "op": "date_before",
                "value": range_data["end"],
                "include": True,
            }

    return filter_configs


def create_enhanced_filter_from_query_params(
    query_params: Dict, excluded_params: set = None
) -> Dict:
    """
    Create filters from query parameters with automatic exclusion of specified parameters

    Args:
        query_params: Dictionary of query parameters
        excluded_params: Set of parameter names to exclude from filtering

    Returns:
        Dictionary of filter configurations
    """
    if excluded_params is None:
        excluded_params = set()

    # Filter out excluded parameters
    filter_params = {k: v for k, v in query_params.items() if k not in excluded_params}

    return create_filters_from_params(filter_params)


# FIXED: Test cases with corrections
if __name__ == "__main__":
    # Sample data with more variety
    tickets = [
        {
            "id": 1,
            "status": 2,
            "priority": 4,
            "subject": "Automated System Alert",
            "created_at": "2024-01-15T10:30:00Z",
            "spam": False,
            "group_id": 123,
        },
        {
            "id": 2,
            "status": 1,
            "priority": 2,
            "subject": "User Login Issue",
            "created_at": "2024-01-20T14:15:00Z",
            "spam": False,
            "group_id": 456,
        },
        {
            "id": 3,
            "status": 3,
            "priority": 3,
            "subject": "Automated Backup Failed",
            "created_at": "2024-02-01T09:00:00Z",
            "spam": True,
            "group_id": 123,
        },
        {
            "id": 4,
            "status": 2,
            "priority": 1,
            "subject": "Password Reset Request",
            "created_at": "2024-02-15T16:45:00Z",
            "spam": False,
            "group_id": 789,
        },
        {
            "id": 5,
            "status": 5,
            "priority": 2,
            "subject": "Test Ticket - Please Ignore",
            "created_at": "2024-02-20T11:00:00Z",
            "spam": False,
            "group_id": 456,
        },
        {
            "id": 6,
            "status": 2,
            "priority": 4,
            "subject": "Production Database Error",
            "created_at": "2024-02-25T08:30:00Z",
            "spam": False,
            "group_id": 789,
        },
    ]

    print(f"Total tickets: {len(tickets)}")

    # Example 1: Basic exclusion with prefixes
    print("\n=== Example 1: Basic Exclusion with Prefixes ===")
    query_params_1 = {
        "exclude_status": "5",  # Exclude closed tickets
        "not_spam": "true",  # Exclude spam tickets
        "exclude_group_id": "123",  # Exclude group 123
    }
    filters_1 = create_filters_from_params(query_params_1)
    print("Filters created:", filters_1)
    result_1 = RecordFilter.apply(tickets, filters_1)
    print(f"Exclude closed, spam, and group 123: {len(result_1)} tickets")
    print(f"Remaining ticket IDs: {[t['id'] for t in result_1]}")

    # Example 2: FIXED - Suffix-based exclusion (avoid duplicate keys)
    print("\n=== Example 2: Suffix-based Exclusion ===")
    query_params_2 = {
        "subject_not_contains": "automated",  # Exclude automated tickets
        "priority_ne": "1",  # Exclude low priority
    }
    filters_2 = create_filters_from_params(query_params_2)
    print("Filters created:", filters_2)
    result_2 = RecordFilter.apply(tickets, filters_2)
    print(f"Exclude automated and low priority: {len(result_2)} tickets")
    print(f"Remaining subjects: {[t['subject'] for t in result_2]}")

    # Example 3: FIXED - Combining inclusion and exclusion
    print("\n=== Example 3: Combining Inclusion and Exclusion ===")
    query_params_3 = {
        "priority": "2,3,4",  # Include medium to urgent priority
        "status": "2",  # Include open tickets only
        "not_spam": "true",  # Exclude spam
        "subject_not_pattern": "*test*",  # Exclude test tickets
        "exclude_group_id": "123",  # Exclude specific group
    }
    filters_3 = create_filters_from_params(query_params_3)
    print("Filters created:", filters_3)
    result_3 = RecordFilter.apply(tickets, filters_3)
    print(f"Complex include/exclude filtering: {len(result_3)} tickets")
    print(f"Final tickets: {[(t['id'], t['subject'][:30]) for t in result_3]}")

    # Example 4: Explicit exclusion operators
    print("\n=== Example 4: Explicit Exclusion Operators ===")
    explicit_filters = {
        "status": {"op": "not_in", "value": [5, 6]},  # Exclude closed/resolved
        "subject": {
            "op": "not_contains",
            "value": "automated",
            "case_sensitive": False,
        },
        "spam": {"op": "eq", "value": False},  # Include only non-spam
        "priority": {"op": "gte", "value": 2},  # Include medium+ priority
    }
    result_4 = RecordFilter.apply(tickets, explicit_filters)
    print(f"Explicit exclusion operators: {len(result_4)} tickets")
    print(f"Final tickets: {[(t['id'], t['subject'][:30]) for t in result_4]}")

    # Example 5: FIXED - Status = 1 (single value)
    print("\n=== Example 5: Status = 1 ===")
    query_params_5 = {
        "status": "1",
    }
    filters_5 = create_filters_from_params(query_params_5)
    print("Filters created:", filters_5)
    result_5 = RecordFilter.apply(tickets, filters_5)
    print(f"Status 1: {len(result_5)} tickets")
    print(f"Tickets with status 1: {[(t['id'], t['status']) for t in result_5]}")

    # Example 6: FIXED - Advanced exclusion with date ranges
    print("\n=== Example 6: Advanced Exclusion with Date Ranges ===")
    query_params_6 = {
        "created_at_start": "2024-01-01",  # Include recent tickets
        "created_at_end": "2024-02-20",  # But not too recent
        "exclude_status": "5,6",  # Exclude closed/resolved
        "subject_not_pattern": "*automated*",  # Exclude automated
        "priority_gte": "2",  # Include medium+ priority
    }
    filters_6 = create_filters_from_params(query_params_6)
    print("Filters created:", filters_6)
    result_6 = RecordFilter.apply(tickets, filters_6)
    print(f"Advanced date + exclusion filtering: {len(result_6)} tickets")
    print(
        f"Final tickets: {[(t['id'], t['subject'][:30], t['created_at'][:10]) for t in result_6]}"
    )

    # Example 7: Performance comparison
    print("\n=== Example 7: Performance Testing ===")
    import time

    # Test with larger dataset
    large_dataset = tickets * 1000  # 6000 tickets

    start_time = time.time()
    large_result = RecordFilter.apply(large_dataset, filters_3)
    end_time = time.time()

    print(
        f"Filtered {len(large_dataset)} tickets to {len(large_result)} in {end_time - start_time:.3f}s"
    )

    # Show filter breakdown
    print(f"\nFilter configuration used in Example 3:")
    for field, config in filters_3.items():
        include_exclude = "INCLUDE" if config.get("include", True) else "EXCLUDE"
        print(f"  {field}: {include_exclude} {config['op']} {config['value']}")

    # Debug Example 2 issue
    print("\n=== DEBUG: Example 2 Analysis ===")
    print("Original tickets subjects:")
    for t in tickets:
        print(f"  ID {t['id']}: '{t['subject']}' (priority: {t['priority']})")

    print("\nApplying filters one by one:")
    # Test subject_not_contains filter
    subject_filter = {
        "subject": {
            "op": "contains",
            "value": "automated",
            "case_sensitive": False,
            "include": False,
        }
    }
    subject_result = RecordFilter.apply(tickets, subject_filter)
    print(f"After excluding 'automated' subjects: {len(subject_result)} tickets")
    for t in subject_result:
        print(f"  ID {t['id']}: '{t['subject']}'")

    # Test priority_ne filter
    priority_filter = {"priority": {"op": "ne", "value": 1}}
    priority_result = RecordFilter.apply(tickets, priority_filter)
    print(f"After excluding priority=1: {len(priority_result)} tickets")
    for t in priority_result:
        print(f"  ID {t['id']}: priority {t['priority']}")

    # Test combined
    combined_result = RecordFilter.apply(tickets, filters_2)
    print(f"Combined filters result: {len(combined_result)} tickets")
    for t in combined_result:
        print(f"  ID {t['id']}: '{t['subject']}' (priority: {t['priority']})")

    # DEBUG: Test parameter parsing
    print("\n=== DEBUG: Parameter Parsing Tests ===")
    test_params = [
        "subject_not_pattern",
        "subject_not_contains",
        "exclude_group_id",
        "priority_gte",
    ]

    for param in test_params:
        field, op, val = ParameterParser.parse_parameter(param, "test_value")
        print(f"'{param}' → field='{field}', operator='{op}', value='{val}'")

    # Test the actual problematic parameters
    print("\nTesting Example 3 parameters:")
    example3_params = {"subject_not_pattern": "*test*", "exclude_group_id": "123"}

    for param_name, param_value in example3_params.items():
        print(f"\nProcessing: {param_name} = {param_value}")

        # Check exclusion prefix
        include_flag = True
        if param_name.startswith("exclude_"):
            include_flag = False
            param_name = param_name[8:]
            print(f"  After removing 'exclude_': {param_name}, include={include_flag}")

        field, operator, value = ParameterParser.parse_parameter(
            param_name, param_value
        )
        print(f"  Parsed: field='{field}', operator='{operator}', value='{value}'")

        # Check for negation conversion
        if operator == "not_pattern":
            operator = "pattern"
            include_flag = not include_flag
            print(
                f"  After negation conversion: operator='{operator}', include={include_flag}"
            )
