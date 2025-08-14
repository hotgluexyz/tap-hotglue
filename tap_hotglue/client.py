"""REST client handling, including HotglueStream base class."""

import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from memoization import cached
from cached_property import cached_property
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import APIKeyAuthenticator, BasicAuthenticator, BearerTokenAuthenticator
import re
from singer_sdk import typing as th
from urllib.parse import urlparse, parse_qs
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable, Callable, cast
import backoff
from tap_hotglue.exceptions import TooManyRequestsError
from pendulum import parse
from datetime import datetime
from tap_hotglue.utils import get_json_path
from tap_hotglue.auth import BearerTokenRequestAuthenticator, OAuth2Authenticator
from singer_sdk.helpers._typing import is_datetime_type
import json
import ast
import copy
import cloudscraper


class HotglueStream(RESTStream):
    """Hotglue stream class."""

    def __init__(
        self,
        tap,
        name: Union[str, None] = None,
        schema = None,
        path: Union[str, None] = None,
    ) -> None:
        """Initialize the REST stream.

        Args:
            tap: Singer Tap this stream belongs to.
            schema: JSON schema for records in this stream.
            name: Name of this stream.
            path: URL path for this entity stream.
        """
        super().__init__(tap=tap, name=name, schema=schema, path=path)
        # Need to parse path in case there are variables in it
        if self.rest_method == "STATIC":
            self.path = "/"
        self.path = self.get_field_value(self.path)

    @cached_property
    def tap_definition(self):
        return self._tap._tap_definitions

    @property
    def url_base(self) -> str:
        # get base url from config file
        return self.tap_definition["definitions"].get("base_requester").get("url_base") if self._tap.airbyte_tap else self.get_field_value(self.tap_definition["base_url"])

    records_jsonpath = "$.[*]"
    next_page_token_jsonpath = "$.next_page"
    params = None
    payload = None
    incremental_sync = {}

    @cached_property
    def authentication(self):
        return self.tap_definition["definitions"].get("base_requester").get("authenticator") if self._tap.airbyte_tap else self.tap_definition.get("authentication")

    @property
    def authenticator(self):
        """Return a new authenticator object."""
        type = self.authentication["type"]

        if self._tap.airbyte_tap:
            # TODO: need to handle other auth types
            match type:
                case "BasicHttpAuthenticator":
                    type = "basic"

        if type == "api":
            # get api key field used in config
            return APIKeyAuthenticator.create_for_stream(
                self,
                key=self.authentication.get("name", "x-api-key"),
                value=self.get_field_value(self.authentication["value"]),
                location=self.authentication.get("location", "header")
            )
        elif type == "basic":
            return BasicAuthenticator.create_for_stream(
                self,
                username=self.get_field_value(self.authentication.get("username", "")),
                password=self.get_field_value(self.authentication.get("password", "")),
            )
        elif type == "bearer":
            token_type = self.authentication.get("token_type", "request")
            if token_type == "request":
                # get request payload definition
                request_payload = self.authentication.get("request_payload", [])

                # build payload, filling config values
                if isinstance(request_payload, dict):
                    payload = {k:self.get_field_value(v) for k,v in request_payload.items() if v}
                else:
                    payload = {self.get_field_value(obj["name"]):self.get_field_value(obj["value"]) for obj in request_payload}

                return BearerTokenRequestAuthenticator(
                    self,
                    auth_endpoint=self.get_field_value(self.authentication.get("endpoint", "")),
                    request_payload=payload,
                    token_path=self.authentication.get("value"),
                    token_expiry_time=self.authentication.get("token_expiry_time", 3600)
                )
            else:
                return BearerTokenAuthenticator.create_for_stream(
                    self,
                    token=self.get_field_value(self.authentication.get("value", ""))
                )
        elif type == "oauth":
            oauth_url = self.get_field_value(self.authentication.get("token_url"))
            request_payload = self.authentication.get("request_payload", [])
            if isinstance(request_payload, dict):
                oauth_request_body = {k:self.get_field_value(v) for k,v in request_payload.items() if v}
            else:
                oauth_request_body = {self.get_field_value(obj["name"]):self.get_field_value(obj["value"]) for obj in request_payload}

            return OAuth2Authenticator(self, self.config, auth_endpoint=oauth_url, oauth_request_body=oauth_request_body)

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        tap_headers = self.tap_definition.get("headers", [])
        for header in tap_headers:
            header_value = self.get_field_value(header.get("value"))
            header_name = header.get("name")
            if header_value and header_name:
                headers[header_name] = header_value
        return headers

    @cached_property
    def datetime_fields(self):
        """Get all datetime fields from schema, including nested ones."""
        datetime_fields = []

        def find_datetime_fields(schema, path=""):
            if not isinstance(schema, dict):
                return

            if schema.get("format") == "date-time":
                datetime_fields.append(path)
                return

            if "properties" in schema:
                for prop_name, prop_schema in schema["properties"].items():
                    new_path = f"{path}.{prop_name}" if path else prop_name
                    find_datetime_fields(prop_schema, new_path)

            if "items" in schema:
                find_datetime_fields(schema["items"], path)

        find_datetime_fields(self.schema)
        return datetime_fields
    
    def parse_objs(self, obj):
        if isinstance(obj, str):
            try:
                return json.loads(obj)
            except:
                return ast.literal_eval(obj)   
        return obj

    def _process_datetime_fields(self, data, path=""):
        """Recursively process datetime fields in the data structure."""
        if not isinstance(data, (dict, list)):
            return data

        if isinstance(data, list):
            return [self._process_datetime_fields(item, path) for item in data]

        result = {}
        for key, value in data.items():
            current_path = f"{path}.{key}" if path else key
            if (
                current_path in self.datetime_fields
                and value
                and self.incremental_sync.get("datetime_format") in ["timestamp", "timestamp_ms"]
            ):
                try:
                    timestamp = int(value)

                    # if timestamp_ms, convert to timestamp
                    if self.incremental_sync.get("datetime_format") == "timestamp_ms":
                        timestamp = timestamp / 1000
                    
                    dt_field = datetime.utcfromtimestamp(timestamp)
                    result[key] = dt_field.isoformat()
                except (ValueError, TypeError):
                    result[key] = value
            elif isinstance(value, (dict, list)):
                result[key] = self._process_datetime_fields(value, current_path)
            else:
                result[key] = value
        return result

    def get_field_value(self, path, context=dict(), parse=False):
        # Handle Airbyte tap format: {{ config['field_name'] }}
        if hasattr(self, '_tap') and self._tap.airbyte_tap:
            match = re.search(r"\{\{\s*config\['([^']+)'\]\s*\}\}", path)
            if match:
                field = match.group(1).strip()
                value = self.config.get(field)
                if value:
                    return path.replace(match.group(0), str(value))
                else:
                    return path
        
        # Match either config or context variables (original format)
        match = re.search(r"\{((?:config|context)\.(.*?))(?:\s*\|\s*(.*?))?\}", path)

        # There's no variable reference to replace
        if not match:
            return path

        source = match.group(1).split('.')[0]  # Get the source (config or context)
        field = match.group(2).strip()  # Get the field name
        default_value = match.group(3)  # Get the default value

        # Get value from appropriate source
        if source == "config":
            value = self.config.get(field)
        else:  # context
            value = context.get(field)

        if value:
            # replace variable value in string
            value = path.replace(match.group(0), str(value))
        elif default_value:
            # return default value
            value = default_value.strip()
        else:
            value = path
    
        if parse:
            return self.parse_objs(value)
        return value

    def get_pagination_type(self):
        pagination = self.tap_definition.get("streams", [])
        if pagination:
            pagination_type = [pag.get("pagination") for pag in pagination if pag["id"] == self.name and pag.get("pagination")]
            if pagination_type:
                return pagination_type[0]

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        # TODO: add support for Airbyte
        return None
        """Return a token for identifying next page or None if no more pages."""
        pagination_type = self.get_pagination_type()
        if not pagination_type:
            self.logger.info(f"No pagination method defined for stream {self.name}")
            return
        if pagination_type.get("type") == "page-increment":
            start_page = pagination_type.get("start_page", 1)
            if not start_page:
                self.logger.info(f"No start page provided for stream {self.name}, using 1 as default")
            previous_token = previous_token or start_page
            if next(self.parse_response(response), None):
                return previous_token + 1
        if pagination_type.get("type") == "offset":
            page_jsonpath = pagination_type.get("next_page_jsonpath")
            offset = next(extract_jsonpath(get_json_path(page_jsonpath), input=response.json()), None)

            # If offset is a url, extract the paging query parameter
            if offset and offset.startswith("http"):
                parsed_url = urlparse(offset)
                # Extract the query parameters
                query_params = parse_qs(parsed_url.query)
                cursor = query_params.get(pagination_type.get("page_name"))
                if cursor:
                    if len(cursor)>0:
                        offset = cursor[0]
            
            return offset

    def get_starting_time(self, context):
        start_date = self.config.get("start_date")
        if start_date:
            start_date = parse(self.config.get("start_date"))
        rep_key = self.get_starting_timestamp(context)
        return rep_key or start_date

    def get_incremental_sync_params(self, incremental_data, context):
        if not incremental_data.get("field_name"):
            self.logger.warning(f"Incremental sync filter field name was not provided for stream {self.name}, running a fullsync.")
            return {}
        datetime_format = incremental_data.get("datetime_format")
        if not datetime_format:
            datetime_format = "%Y-%m-%dT%H:%M:%SZ"
            self.logger.warning(f"Datetime format for incremental_sync not provided for stream {self.name}, using '{datetime_format}' as default")
        # get start_date
        start_date = self.get_starting_time(context)
        if datetime_format == "timestamp":
            start_date = int(start_date.timestamp())
        elif datetime_format == "timestamp_ms":
            start_date = int(start_date.timestamp() * 1000)
        else:
            start_date = start_date.strftime(datetime_format)
        return {incremental_data["field_name"]: start_date}

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        # TODO: add support for Airbyte
        return params
        if self.incremental_sync and self.incremental_sync.get("location") == "request_parameter":
            params.update(self.get_incremental_sync_params(self.incremental_sync, context))
        if self.params:
            for param in self.params:
                value = self.get_field_value(param["value"], context)
                if value:
                    params[param["name"]] = value
        if next_page_token:
            pagination_type = self.get_pagination_type()
            if pagination_type.get("page_size") and  pagination_type.get("page_size_parameter"):
                params[pagination_type["page_size_parameter"]] = pagination_type["page_size"] 
            if pagination_type.get("page_name"):
                params[pagination_type["page_name"]] = next_page_token
        return params

    def request_decorator(self, func: Callable) -> Callable:
        decorator: Callable = backoff.on_exception(
            backoff.expo,
            (
                RetriableAPIError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.RequestException,
            ),
            max_tries=7,
            factor=2,
        )(func)

        # increase backoff for connection errors
        decorator = backoff.on_exception(
            backoff.constant,
            (
                requests.exceptions.ConnectionError,
                ConnectionRefusedError,
                TooManyRequestsError
            ),
            max_tries=15,
            interval=30,
        )(decorator)
        return decorator

    def response_error_message(self, response: requests.Response) -> str:
        """Build error message for invalid http statuses.

        Args:
            response: A `requests.Response`_ object.

        Returns:
            str: The error message
        """
        if 400 <= response.status_code < 500:
            error_type = "Client"
        else:
            error_type = "Server"

        return (
            f"{response.status_code} {error_type} Error: "
            f"{response.reason} for path: {self.path}. Response {response.text}"
        )

    def validate_response(self, response: requests.Response) -> None:
        if response.status_code in [429]:
            raise TooManyRequestsError(response.text)
        if (
            response.status_code in self.extra_retry_statuses
            or 500 <= response.status_code < 600
        ):
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)
        elif 400 <= response.status_code < 500:
            msg = self.response_error_message(response)
            raise FatalAPIError(msg)

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """Process datetime fields in the data structure, including nested ones."""
        if (
            self.incremental_sync
            and self.incremental_sync.get("datetime_format") in ["timestamp", "timestamp_ms"]
        ):
            return self._process_datetime_fields(row)
        

        # Add time_extracted field if specified as rep key
        if self.incremental_sync and self.incremental_sync.get("replication_key") == "time_extracted":
            row["time_extracted"] = datetime.now().isoformat()
        return row

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        # Get the stream definition for the current stream
        stream_definition = self.stream_data
        if not stream_definition:
            return {}
        # Build the child context based on the stream definition
        child_context = {}
        for child in stream_definition.get("child_context", []):
            # Get the value from the record using the jsonpath
            child_context[child["name"]] = next(extract_jsonpath(get_json_path(child["value"]), input=record), None)
        return child_context

    def prepare_request_payload(
        self, context, next_page_token
    ):
        if self.payload:
            payload = {}

            for param in self.payload:
                value = self.get_field_value(param["value"], context) if isinstance(param["value"], str) else param["value"]
                if value is not None:
                    payload[param["name"]] = value

            # add incremental sync params
            if self.incremental_sync and self.incremental_sync.get("location") == "body":
                payload.update(self.get_incremental_sync_params(self.incremental_sync, context))

            return payload

        return None
    
    def get_records(self, context) -> Iterable[Dict[str, Any]]:
        if self.rest_method == "STATIC":
            records_path = self.stream_data.get("record_selector", {}).get("field_path")
            if not records_path:
                raise Exception(f"Stream is of type STATIC but no record_selector was provided for stream {self.name}")
            records = self.get_field_value(records_path, context, parse=True)
            for record in records:
                yield record
        else:
            yield from super().get_records(context)
    
    def get_url(self, context: Optional[dict], encode=True) -> str:
        url = "".join([self.url_base, self.path or ""])
        vals = copy.copy(dict(self.config))
        vals.update(context or {})
        for k, v in vals.items():
            search_text = "".join(["{{ config['", k, "'] }}"]) if self._tap.airbyte_tap else "".join(["{", k, "}"])
            if search_text in url:
                v = str(v) if v else ""
                url = url.replace(search_text, v if not self.stream_data.get("encode_path") else self._url_encode(v))

        # If the incremental_sync location is base_url, we handle it here
        if self.incremental_sync and self.incremental_sync.get("location") == "base_url":
            params = self.get_incremental_sync_params(self.incremental_sync, context)
            url = url.replace("{" + self.incremental_sync.get("field_name") + "}", params[self.incremental_sync.get("field_name")])

        return url

    def _request(
        self, prepared_request: requests.PreparedRequest, context: dict | None
    ) -> requests.Response:
        if self.tap_definition.get("cloudflare_bypass"):
            self.logger.info("Using cloudflare_bypass mode instead of default request method")
            scraper = cloudscraper.create_scraper()

            # NOTE: if this doesn't work, we can use self.rest_method
            if prepared_request.method == "POST":
                response = scraper.post(
                    prepared_request.url,
                    headers=prepared_request.headers,
                )
            else:
                response = scraper.get(
                    prepared_request.url,
                    headers=prepared_request.headers,
                )

            self.validate_response(response)

            return response
        else:
            return super()._request(prepared_request, context)
        
    @property
    def is_timestamp_replication_key(self) -> bool:
        if not self.replication_key:
            return False
        
        # time_extracted requires override as it is not in catalog
        if self.replication_key == "time_extracted":
            return True
        type_dict = self.schema.get("properties", {}).get(self.replication_key)
        return is_datetime_type(type_dict)
