# Tap Definition Schema Reference

This document describes all possible values and fields supported in a `tap_definition` JSON file for tap-hotglue.

## Root-Level Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Display name of the tap (e.g., "DonorPerfect", "Braintree") |
| `id` | string | No | Unique identifier for the tap (e.g., "donorperfect", "braintree") |
| `domain` | string | No | Domain associated with the tap (e.g., "donorperfect.com") |
| `icon` | string | No | URL to the tap's icon image |
| `type` | string | No | Type of data source. Values: `"api"` |
| `base_url` | string | Yes | Base URL for API requests. Supports config variables: `"https://api.example.com/{config.subdomain}"` |
| `streams` | array | Yes | Array of stream definitions (see [Streams](#streams)) |
| `authentication` | object | Yes | Authentication configuration (see [Authentication](#authentication)) |
| `headers` | array | No | Global HTTP headers to include in all requests (see [Headers](#headers)) |
| `connect_ui_params` | object | No | UI configuration for connection parameters |
| `cloudflare_bypass` | boolean | No | Enable Cloudflare bypass mode using cloudscraper |
| `status` | string | No | Publication status. Values: `"published"`, `"draft"` |
| `isConnector` | boolean | No | Whether this is a connector |
| `isSource` | boolean | No | Whether this can be used as a data source |
| `isTarget` | boolean | No | Whether this can be used as a data target |
| `flowType` | string | No | Flow type. Values: `"all"` |
| `airbyte_yaml` | string | No | Reference to Airbyte YAML configuration (empty string if not used) |

---

## Authentication

The `authentication` object configures how the tap authenticates with the API.

### Authentication Types

#### API Key Authentication
```json
{
    "type": "api",
    "location": "header" | "params" | "request_parameter" | "query",
    "key": "x-api-key",
    "value": "{config.api_token}"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | Yes | `"api"` |
| `location` | string | No | Where to place the API key. Values: `"header"` (default), `"params"`, `"request_parameter"`, `"query"` |
| `key` | string | No | Header/parameter name (default: `"x-api-key"`) |
| `value` | string | Yes | API key value, supports config variables: `"{config.api_token}"` |

#### Basic Authentication
```json
{
    "type": "basic",
    "username": "{config.username}",
    "password": "{config.password}"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | Yes | `"basic"` |
| `username` | string | Yes | Username, supports config variables |
| `password` | string | Yes | Password, supports config variables |

#### Bearer Token Authentication
```json
{
    "type": "bearer",
    "value": "{config.access_token}",
    "token_type": "request",
    "endpoint": "https://api.example.com/oauth/token",
    "request_payload": [
        {"name": "client_id", "value": "{config.client_id}"},
        {"name": "client_secret", "value": "{config.client_secret}"}
    ],
    "token_expiry_time": 3600
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | Yes | `"bearer"` |
| `value` | string | Yes | Token value or JSON path to extract token from response |
| `token_type` | string | No | `"request"` for token obtained via request, omit for static token |
| `endpoint` | string | Conditional | Token endpoint URL (required if `token_type` is `"request"`) |
| `request_payload` | array | Conditional | Payload for token request |
| `token_expiry_time` | integer | No | Token expiry time in seconds (default: 3600) |

#### OAuth2 Authentication
```json
{
    "type": "oauth",
    "token_url": "https://api.example.com/oauth/token",
    "request_payload": [
        {"name": "client_id", "value": "{config.client_id}"},
        {"name": "client_secret", "value": "{config.client_secret}"},
        {"name": "refresh_token", "value": "{config.refresh_token}"},
        {"name": "grant_type", "value": "refresh_token"}
    ]
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | Yes | `"oauth"` |
| `token_url` | string | Yes | OAuth token endpoint URL |
| `request_payload` | array/object | Yes | OAuth request body parameters |

---

## Headers

Global headers to include in all API requests.

```json
"headers": [
    {
        "name": "X-ApiVersion",
        "value": "6"
    },
    {
        "name": "User-Agent",
        "value": "{config.user_agent}",
        "required": false
    }
]
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Header name |
| `value` | string | Yes | Header value, supports config variables |
| `required` | boolean | No | Whether the header is required |

---

## Streams

Each stream represents an API endpoint/resource to sync.

```json
{
    "name": "Donors",
    "id": "donors",
    "path": "/donors",
    "method": "GET",
    "primary_keys": "donor_id",
    "schema": { ... },
    "record_selector": { ... },
    "pagination": { ... },
    "incremental_sync": { ... },
    "custom_query_params": [ ... ],
    "custom_request_payload": [ ... ],
    "parent_stream": "parent_stream_id",
    "child_context": [ ... ],
    "isParent": false,
    "isChild": false,
    "hasIncrementalSync": true,
    "response_format": "application/json",
    "encode_path": false
}
```

### Stream Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Display name of the stream |
| `id` | string | No | Unique identifier (defaults to snake_case of name) |
| `path` | string | Yes | API endpoint path (e.g., `"/donors"`, `"/users/{user_id}/posts"`) |
| `method` | string | No | HTTP method. Values: `"GET"` (default), `"POST"`, `"STATIC"` |
| `primary_keys` | string/array | No | Primary key field(s) for the stream |
| `schema` | object | No | JSON Schema for the stream's records (auto-detected if not provided) |
| `record_selector` | object | No | Configuration for extracting records from response |
| `error_message_path` | string | No | JSON path to error message in a JSON response. Will raise error message if found |
| `pagination` | object | No | Pagination configuration |
| `incremental_sync` | object | No | Incremental sync configuration |
| `custom_query_params` | array | No | Custom query parameters to include in requests |
| `custom_request_payload` | array | No | Custom request body for POST requests |
| `parent_stream` | string | No | ID of parent stream for child streams |
| `child_context` | array | No | Context data to pass to child streams |
| `isParent` | boolean | No | Whether this stream is a parent stream |
| `isChild` | boolean | No | Whether this stream is a child stream |
| `hasIncrementalSync` | boolean | No | Whether incremental sync is enabled |
| `response_format` | string | No | Expected response format. Values: `"application/json"`, `"application/xml"`, `"text/xml"` |
| `encode_path` | boolean | No | Whether to URL-encode path variables |

---

## Record Selector

Defines how to extract records from the API response.

```json
"record_selector": {
    "field_path": "$.data[*]"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `field_path` | string | Yes | JSONPath expression to extract records. Examples: `"$.data[*]"`, `"$.result.record[*]"`, `"Suppliers"` |

---

## Error message selector

Defines how to extract and raise errors from API responses when an error message is present in the body.

```json
"error_message_path": "$.error.message"
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `error_message_path` | string | No | JSONPath expression pointing to the error message in the response. If found, the tap will raise this error and halt sync for the stream. Example: `"$.error.message"` for `{ "error": { "message": "Invalid API key" }}` |

If specified, the tap will check the given JSONPath in each response. If a value is found, it will raise an Exception containing the retrieved error message.


---

## Pagination

Configures how to paginate through API results.

### Pagination Types

#### Page Increment
```json
"pagination": {
    "type": "page-increment",
    "location": "request_parameter",
    "start_page": 1,
    "page_name": "page"
}
```

#### Offset-Based
```json
"pagination": {
    "type": "offset",
    "location": "request_parameter",
    "page_name": "cursor",
    "next_page_jsonpath": "$.meta.next_cursor",
    "page_value": "{{ response.get('next_cursor') }}"
}
```

#### Incremental Offset
```json
"pagination": {
    "type": "incremental_offset",
    "location": "request_parameter",
    "page_name": "offset",
    "page_size": 100,
    "page_size_parameter": "limit",
    "embedded": true
}
```

### Pagination Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | Yes | Pagination type. Values: `"page-increment"`, `"offset"`, `"incremental_offset"` |
| `location` | string | Yes | Where to add pagination params. Values: `"request_parameter"`, `"body"` |
| `start_page` | integer | No | Starting page number (default: 1 for page-increment, 0 for offset) |
| `page_name` | string | Yes | Parameter name for page/offset value |
| `page_size` | integer | No | Number of records per page |
| `page_size_parameter` | string | No | Parameter name for page size |
| `next_page_jsonpath` | string | No | JSONPath to extract next page token from response |
| `page_value` | string | No | Jinja expression to extract next page value |
| `embedded` | boolean | No | Whether pagination is embedded in custom query params |

---

## Incremental Sync

Configures incremental data synchronization based on a replication key.

```json
"incremental_sync": {
    "replication_key": "modified_date",
    "field_name": "since",
    "datetime_format": "%Y-%m-%dT%H:%M:%SZ",
    "state_datetime_format": "%m/%d/%Y %I:%M:%S %p",
    "location": "request_parameter",
    "embedded": true,
    "value_template": "findByDate;auditDate={replication_key_value}",
    "replication_key_sources": ["updated_at", "created_at"],
    "is_inclusive": true,
    "step": "P1D"
}
```

### Incremental Sync Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `replication_key` | string | Yes | Field in the record to use for incremental sync |
| `field_name` | string | No | Parameter name to send the start date (if different from replication_key) |
| `cursor_field` | string | No | Alias for replication_key (Airbyte compatibility) |
| `datetime_format` | string | No | Format for datetime in API requests. Values: `"%Y-%m-%dT%H:%M:%SZ"`, `"%Y-%m-%d"`, `"timestamp"`, `"timestamp_ms"` |
| `state_datetime_format` | string | No | Format used to parse rep key value and convert to store in state (if different from API format) |
| `location` | string | No | Where to include the date filter. Values: `"request_parameter"`, `"body"`, `"base_url"` |
| `path` | string | No | JSONPath for nested placement in request body |
| `embedded` | boolean | No | Whether the filter is embedded in custom query params |
| `value_template` | string | No | When set, the parameter value is this string with `{replication_key_value}` replaced by the formatted date. Use when the API expects a composite value (e.g. `findByDate;auditDate={replication_key_value}`) instead of the raw date. |
| `replication_key_sources` | array of strings | No | When set, the replication key is a synthetic field: each record gets `replication_key` = first non-null value from these source fields (e.g. `["updated_at", "created_at"]` for “updated or created”). Use when the API returns only one of the dates per record. Remember to include the synthetic field in the stream schema. |
| `is_inclusive` | boolean | No | Whether the filter is inclusive (>=) or exclusive (>) |
| `step` | string | No | ISO 8601 duration for stepping through date ranges (e.g., `"P1D"` for 1 day) |

### Special Replication Key Values

- `"time_extracted"`: Uses current timestamp as the replication key (adds `time_extracted` field to records)

---

## Custom Query Parameters

Additional query parameters to include in API requests.

```json
"custom_query_params": [
    {
        "name": "action",
        "value": "SELECT * FROM table WHERE date > '{replication_key_value}'"
    },
    {
        "name": "filter",
        "value": "{config.filter_value}",
        "required": false
    }
]
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Query parameter name |
| `value` | string | Yes | Parameter value (supports variables - see [Variables](#variables)) |
| `required` | boolean | No | Whether the parameter is required |

---

## Custom Request Payload

Request body for POST requests.

```json
"custom_request_payload": [
    {
        "name": "query",
        "value": "{ users { id name } }"
    },
    {
        "name": "variables",
        "value": {"page": 1}
    }
]
```

---

## Child Context

Defines context data to pass from parent to child streams.

```json
"child_context": [
    {
        "name": "parent_id",
        "value": "$.id"
    }
]
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Context variable name (used in child stream path/params) |
| `value` | string | Yes | JSONPath to extract value from parent record |

---

## Schema

JSON Schema definition for stream records. If not provided, schema is auto-detected from the first response.

```json
"schema": {
    "type": "object",
    "properties": {
        "id": {
            "type": ["string", "null"]
        },
        "name": {
            "type": ["string", "null"]
        },
        "created_at": {
            "type": ["string", "null"],
            "format": "date-time"
        },
        "amount": {
            "type": ["number", "null"]
        },
        "is_active": {
            "type": ["boolean", "null"]
        },
        "metadata": {
            "type": ["object", "null"],
            "properties": {
                "key": {"type": ["string", "null"]}
            }
        },
        "tags": {
            "type": ["array", "null"],
            "items": {"type": "string"}
        }
    }
}
```

### Supported Types

- `"string"` - Text values
- `"integer"` - Whole numbers
- `"number"` - Decimal numbers
- `"boolean"` - True/false values
- `"object"` - Nested objects
- `"array"` - Arrays/lists
- `"null"` - Null values (combine with other types for nullable fields)

### Supported Formats

- `"date-time"` - ISO 8601 datetime
- `"date"` - ISO 8601 date
- `"email"` - Email address
- `"uuid"` - UUID string

---

## Variables

The tap_definition supports variable substitution in many fields using the following syntax:

### Config Variables
Reference values from the config file:
```
{config.api_key}
{config.subdomain}
{{ config["api_key"] }}
```

### Context Variables
Reference values from parent stream context:
```
{context.parent_id}
{user_id}
```

### Stream Variables
Reference stream-specific values:
```
{replication_key}
{replication_key_value}
{next_page_token}
{next_page_token | 0}  (with default value)
```

### Jinja Templates
Full Jinja template support:
```
{{ now_utc() }}
{{ format_datetime(now_utc(), '%Y-%m-%d') }}
{{ config["start_date"] }}
```

---

## Connect UI Parameters

Configuration for the connection UI (optional).

```json
"connect_ui_params": {
    "api_key": {
        "type": "password",
        "label": "API Key",
        "description": "Your API key from the settings page",
        "required": true,
        "params": {}
    },
    "subdomain": {
        "type": "string",
        "label": "Subdomain",
        "description": "Your account subdomain",
        "required": true,
        "params": {}
    }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | Input type. Values: `"string"`, `"password"` |
| `label` | string | Display label |
| `description` | string | Help text |
| `required` | boolean | Whether the field is required |
| `params` | object | Additional parameters |

---

## Complete Example

```json
{
    "name": "Example API",
    "id": "example_api",
    "domain": "example.com",
    "icon": "https://example.com/icon.png",
    "type": "api",
    "base_url": "https://api.example.com/v1",
    "authentication": {
        "type": "bearer",
        "value": "{config.access_token}"
    },
    "headers": [
        {
            "name": "Accept",
            "value": "application/json"
        }
    ],
    "streams": [
        {
            "name": "Users",
            "id": "users",
            "path": "/users",
            "method": "GET",
            "primary_keys": "id",
            "record_selector": {
                "field_path": "$.data[*]"
            },
            "pagination": {
                "type": "page-increment",
                "location": "request_parameter",
                "start_page": 1,
                "page_name": "page",
                "page_size": 100,
                "page_size_parameter": "per_page"
            },
            "incremental_sync": {
                "replication_key": "updated_at",
                "field_name": "since",
                "datetime_format": "%Y-%m-%dT%H:%M:%SZ",
                "location": "request_parameter"
            },
            "hasIncrementalSync": true,
            "isParent": true,
            "child_context": [
                {
                    "name": "user_id",
                    "value": "$.id"
                }
            ]
        },
        {
            "name": "User Posts",
            "id": "user_posts",
            "path": "/users/{user_id}/posts",
            "method": "GET",
            "primary_keys": "id",
            "parent_stream": "users",
            "record_selector": {
                "field_path": "$.posts[*]"
            },
            "isChild": true
        }
    ],
    "connect_ui_params": {
        "access_token": {
            "type": "password",
            "label": "Access Token",
            "required": true
        }
    },
    "status": "published"
}
```

---

## Airbyte YAML Support

The tap also supports Airbyte-style YAML definitions. When using a `.yaml` file extension, the tap will parse it as an Airbyte manifest. Key differences:

- Streams are defined under `definitions.streams`
- Schemas are defined under `schemas`
- Authentication is defined under `definitions.base_requester.authenticator`
- Uses `retriever.requester.path` instead of `path`
- Uses `retriever.record_selector.extractor.field_path` for record selection
- Supports Airbyte-specific pagination types: `CursorPagination`, `PageIncrement`, `OffsetIncrement`
