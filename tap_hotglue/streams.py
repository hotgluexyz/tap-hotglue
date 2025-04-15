"""Stream type classes for tap-hotglue."""

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_hotglue.client import HotglueStream

import requests
from tap_hotglue.utils import get_jsonschema_type
from functools import cached_property

class BaseStream(HotglueStream):
    """Define custom stream."""

    def get_schema(self) -> dict:
        """Dynamically detect the json schema for the stream.
        This is evaluated prior to any records being retrieved.
        """

        self._requests_session = requests.Session()
        # Get the data
        headers = self.http_headers
        headers.update(self.authenticator._auth_headers)

        prepared_request = self.prepare_request({}, None)
        response = self.requests_session.send(prepared_request, timeout=self.timeout)
        records = list(self.parse_response(response))
        
        if len(records)>0:
            properties = []
            property_names = set()

            for record in records:
                # Loop through each key in the object
                for name in record.keys():
                    if name in property_names:
                        continue
                    # Add the new property to our list
                    property_names.add(name)
                    properties.append(
                        th.Property(name, get_jsonschema_type(record[name]))
                    )
                #we need to process only first record        
                break        
            # Return the list as a JSON Schema dictionary object
            property_list = th.PropertiesList(*properties).to_dict()

            return property_list
        else:
            self.logger.info(f"Unable to create schema for stream {self.name} because there is no records for these creds.")
            return th.PropertiesList(th.Property("placeholder", th.StringType)).to_dict()

    @cached_property
    def schema(self) -> dict:
        if hasattr(self, "json_schema"):
            return self.json_schema
        return self.get_schema()