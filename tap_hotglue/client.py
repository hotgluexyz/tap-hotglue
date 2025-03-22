"""REST client handling, including HotglueStream base class."""

import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from memoization import cached

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import APIKeyAuthenticator, BasicAuthenticator
from functools import cached_property
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

class HotglueStream(RESTStream):
    """Hotglue stream class."""

    @cached_property
    def tap_definition(self):
        return self._tap._tap_definitions

    @property
    def url_base(self) -> str:
        # get base url from config file
        return self.get_field_value(self.tap_definition["base_url"])

    records_jsonpath = "$.[*]"
    next_page_token_jsonpath = "$.next_page"
    params = None
    incremental_sync = {}

    @cached_property
    def authentication(self):
        return self.tap_definition.get("authentication")

    @property
    def authenticator(self):
        """Return a new authenticator object."""
        type = self.authentication["type"]
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
                and self.incremental_sync.get("datetime_format") == "timestamp"
            ):
                try:
                    dt_field = datetime.utcfromtimestamp(int(value))
                    result[key] = dt_field.isoformat()
                except (ValueError, TypeError):
                    result[key] = value
            elif isinstance(value, (dict, list)):
                result[key] = self._process_datetime_fields(value, current_path)
            else:
                result[key] = value
        return result

    def get_field_value(self, path):
        match = re.search(r"\{config\.(.*?)(?:\s*\|\s*(.*?))?\}", path)

        # There's no config reference to replace
        if not match:
            return path

        field = match.group(1).strip() # Get the field name
        default_value = match.group(2) # Get the default value

        value = self.config.get(field)  # Use the default value if field is not found
        if value:
            # replace config value in string
            return path.replace(match.group(0), value) 
        if default_value:
            # return default value
            return default_value.strip()  

    def get_pagination_type(self):
        pagination = self.tap_definition.get("streams", [])
        if pagination:
            pagination_type = [pag.get("pagination") for pag in pagination if pag["id"] == self.name and pag.get("pagination")]
            if pagination_type:
                return pagination_type[0]

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
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
        if not incremental_data.get("location") == "request_parameter":
            return {}
        datetime_format = incremental_data.get("datetime_format")
        if not datetime_format:
            datetime_format = "%Y-%m-%dT%H:%M:%SZ"
            self.logger.warning(f"Datetime format for incremental_sync not provided for stream {self.name}, using '{datetime_format}' as default")
        # get start_date
        start_date = self.get_starting_time(context)
        if datetime_format == "timestamp":
            start_date = int(start_date.timestamp())
        else:
            start_date = start_date.strftime(datetime_format)
        return {incremental_data["field_name"]: start_date}

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if self.incremental_sync:
            params.update(self.get_incremental_sync_params(self.incremental_sync, context))
        if self.params:
            for param in self.params:
                value = self.get_field_value(param["value"])
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
            and self.incremental_sync.get("datetime_format") == "timestamp"
        ):
            return self._process_datetime_fields(row)
        return row
