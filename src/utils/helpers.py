from datetime import datetime
import json

def canonical_str(value):
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True)
    elif isinstance(value, list):
        return json.dumps(sorted(value))
    else:
        return str(value)

def type_cast(value, field_type):
    if value is None:
        return None

    try:
        if field_type == "string":
            # if isinstance(value, str):
            return str(value)

        elif field_type == 'number':
            if isinstance(value, (int, float)):
                return value
            return float(value) if '.' in str(value) else int(value)

        elif field_type == 'boolean':
            if isinstance(value, bool):
                return value
            return str(value).lower() in ['true', '1', 'yes']

        elif field_type == 'datetime':
            if isinstance(value, datetime):
                return value.isoformat()
            if isinstance(value, str):
                return datetime.fromisoformat(value).isoformat()
            return datetime.fromtimestamp(value).isoformat()

        elif field_type == 'array':
            if isinstance(value, list):
                return value
            elif isinstance(value, str):
                return [v.strip() for v in value.split(',')]
            return [value]

        elif field_type == 'array of objects':
            # Special handling for attachments and similar fields
            if isinstance(value, list):
                return value
            elif isinstance(value, str):
                value = value.strip()
                if not value:
                    return []
                # Try to parse as JSON first
                try:
                    import json
                    parsed = json.loads(value)
                    if isinstance(parsed, list):
                        return parsed
                    elif isinstance(parsed, dict):
                        return [parsed]
                    else:
                        # Single string URL, convert to attachment object
                        return [{"content_url": str(parsed), "type": "unknown"}]
                except json.JSONDecodeError:
                    # Not JSON, treat as single URL or comma-separated URLs
                    if "," in value:
                        # Multiple URLs
                        urls = [url.strip() for url in value.split(",") if url.strip()]
                        return [{"content_url": url, "type": "unknown"} for url in urls]
                    else:
                        # Single URL
                        return [{"content_url": value, "type": "unknown"}]
            return [value] if value else []

    except Exception:
        # Return original if casting fails
        return value

    return value