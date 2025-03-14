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
from urllib.parse import urlparse
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable, Callable, cast
import backoff
from tap_hotglue.exceptions import TooManyRequestsError
from pendulum import parse


class HotglueStream(RESTStream):
    """Hotglue stream class."""

    @cached_property
    def tap_definition(self):
        return self._tap._tap_definitions

    @property
    def url_base(self) -> str:
        # check if default base url was passed in tap definitions
        is_base_url = urlparse(self.tap_definition["base_url"])
        if all([is_base_url.scheme, is_base_url.netloc]):
            return self.tap_definition["base_url"]
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
                username=self.get_field_value(self.authentication["username"]),
                password=self.get_field_value(self.authentication["password"]),
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
        start_date = self.get_starting_time(context).strftime(datetime_format)
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