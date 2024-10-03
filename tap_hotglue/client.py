"""REST client handling, including HotglueStream base class."""

import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from memoization import cached

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import APIKeyAuthenticator
from functools import cached_property
import re
from singer_sdk import typing as th


class HotglueStream(RESTStream):
    """Hotglue stream class."""

    @cached_property
    def tap_definition(self):
        return self._tap._tap_definitions

    @property
    def url_base(self) -> str:
        return self.tap_definition["base_url"]

    records_jsonpath = "$.[*]"
    next_page_token_jsonpath = "$.next_page"
    params = None

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
        field = re.search(r"\{config\.(.*?)\}", path)
        if not field:
            self.logger.info(f"Value not found for {path}")
            return
        field = field.group(1)
        return self.config.get(field)

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        if self.next_page_token_jsonpath:
            all_matches = extract_jsonpath(
                self.next_page_token_jsonpath, response.json()
            )
            first_match = next(iter(all_matches), None)
            next_page_token = first_match
        else:
            next_page_token = response.headers.get("X-Next-Page", None)

        return next_page_token

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if self.params:
            for param in self.params:
                params[param["name"]] = self.get_field_value(param["value"])
        if next_page_token:
            params["page"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params