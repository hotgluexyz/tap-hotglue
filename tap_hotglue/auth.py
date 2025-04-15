"""hubspot Authentication."""

import json
from datetime import datetime
from typing import Any, Dict, Optional

import requests
from singer_sdk.authenticators import APIAuthenticatorBase
from singer_sdk.streams import Stream as RESTStreamBase
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.helpers.jsonpath import extract_jsonpath
import backoff


class BearerTokenRequestAuthenticator(APIAuthenticatorBase):
    """API Authenticator for OAuth 2.0 flows."""

    def __init__(
        self,
        stream: RESTStreamBase,
        auth_endpoint: Optional[str] = None,
        request_payload: Optional[dict] = None,
        token_path: Optional[str] = None,
        token_expiry_time: Optional[int] = None,
    ) -> None:
        super().__init__(stream=stream)
        self._auth_endpoint = auth_endpoint
        self._tap = stream._tap
        self._request_payload = request_payload
        self._token_path = token_path
        self._token_expiry_time = token_expiry_time
        self.access_token = None
        self.expires_in = None

    @property
    def auth_headers(self) -> dict:
        """Return a dictionary of auth headers to be applied.

        These will be merged with any `http_headers` specified in the stream.

        Returns:
            HTTP headers for authentication.
        """
        if not self.is_token_valid():
            self.update_access_token()
        result = super().auth_headers
        result["Authorization"] = f"Bearer {self.access_token}"
        return result

    @property
    def auth_endpoint(self) -> str:
        """Get the authorization endpoint.

        Returns:
            The API authorization endpoint if it is set.

        Raises:
            ValueError: If the endpoint is not set.
        """
        if not self._auth_endpoint:
            raise ValueError("Authorization endpoint not set.")
        return self._auth_endpoint

    def is_token_valid(self) -> bool:
        now = round(datetime.utcnow().timestamp())

        return not bool(
            (not self.access_token) or (not self.expires_in) or ((self.expires_in - now) < 60)
        )

    @backoff.on_exception(backoff.expo, RetriableAPIError, max_tries=5)
    def request_token(self, endpoint, data):
        token_response = requests.post(endpoint, json=data)
        if 500 <= token_response.status_code <= 600:
            raise RetriableAPIError(f"Auth error: {token_response.text}")
        elif 400 <= token_response.status_code < 500:
            raise FatalAPIError(f"Auth error: {token_response.text}")
        return token_response

    # Authentication and refresh
    def update_access_token(self) -> None:
        token_response = self.request_token(self.auth_endpoint, data=self._request_payload)

        try:
            token_response.raise_for_status()
            self.logger.info("Authorization attempt was successful.")
        except Exception as ex:
            raise RuntimeError(
                f"Failed to get bearer token, response was '{token_response.text}'. {ex}"
            )

        request_time = round(datetime.utcnow().timestamp())
        self.access_token = next(extract_jsonpath(self._token_path, input=token_response.json()), None)
        self.expires_in = request_time + self._token_expiry_time
