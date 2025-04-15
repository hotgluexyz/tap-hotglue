"""Hotglue tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
from pathlib import Path, PurePath
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union, cast
from singer_sdk.helpers._util import read_json_file
from tap_hotglue.utils import get_json_path, snakecase

from tap_hotglue.streams import (
    BaseStream
)

STREAM_TYPES = [
    BaseStream
]

class TapHotglue(Tap):
    """Hotglue tap class."""

    name = "tap-hotglue"

    @property
    def _tap_definitions(self):
        return read_json_file(self.config.get("tap_definition"))

    config_jsonschema = th.PropertiesList(
        th.Property(
            "tap_definition",
            th.StringType,
            required=True,
            description="Tap definition json file"
        )
    ).to_dict()

    @property
    def catalog(self):
        """Get the tap's working catalog.

        Returns:
            A Singer catalog object.
        """
        if self._catalog is None:
            self._catalog = self._singer_catalog

        return self._catalog

    def create_streams(self):
        streams = self._tap_definitions.get("streams", [])
        stream_classes = {}
        for stream_data in streams:
            # validate all fields needed to create a stream exist:
            try:
                name = stream_data["name"]
                path = stream_data["path"]
            except KeyError as e:
                raise Exception(f"Name and/or path values were not found when trying to build the stream for stream_data {stream_data}")
            
            if not name or not path:
                raise Exception(f"Name and/or path values were not found when trying to build the stream for stream_data {stream_data}")

            id = stream_data.get("id") or snakecase(name)
            name = name.replace(" ", "")

            # add required fields
            stream_fields = {
                "name": id,
                "path": path
            }

            # add REST method if it's specified
            if stream_data.get("method"):
                stream_fields.update({"rest_method": stream_data["method"]})

            if stream_data.get("primary_keys"):
                stream_fields["primary_keys"] = stream_data["primary_keys"]

            if stream_data.get("incremental_sync", {}).get("replication_key"):
                stream_fields.update({"replication_key": stream_data["incremental_sync"]["replication_key"]})
                stream_fields.update({"incremental_sync": stream_data["incremental_sync"]})

            # add custom params
            if stream_data.get("custom_query_params"):
                stream_fields.update({"params": stream_data["custom_query_params"]})

            # add custom request payload
            if stream_data.get("custom_request_payload"):
                stream_fields.update({"payload": stream_data["custom_request_payload"]})

            # add records_jsonpath
            if stream_data.get("record_selector", {}).get("field_path"):
                json_path = stream_data["record_selector"]["field_path"]
                stream_fields.update({"records_jsonpath": get_json_path(json_path)})
            
            if stream_data.get("schema"):
                stream_fields.update({"json_schema": stream_data["schema"]})

            if stream_data.get("parent_stream"):
                parent_stream_name = stream_data["parent_stream"]
                parent_stream_class = stream_classes.get(parent_stream_name)
                if not parent_stream_class:
                    raise Exception(f"Parent stream {parent_stream_name} not found for stream {id}")

                stream_fields.update({"parent_stream_type": parent_stream_class})

            # keep a mapping of stream name to stream class
            stream_class = type(
                name,
                (BaseStream,),
                stream_fields,
            )
            stream_classes[id] = stream_class

            # yield init version of stream class
            yield stream_class(tap=self)

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream for stream in self.create_streams()]

if __name__ == "__main__":
    TapHotglue.cli()