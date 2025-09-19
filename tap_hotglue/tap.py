"""Hotglue tap class."""

from typing import List
from functools import cached_property
from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
from pathlib import Path, PurePath
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union, cast
from singer_sdk.helpers._util import read_json_file
from tap_hotglue.utils import get_json_path, snakecase
import yaml
import re

from tap_hotglue.streams import (
    BaseStream
)

STREAM_TYPES = [
    BaseStream
]

def read_yaml_file(file_path):
    with open(file_path, "r") as file:
        return yaml.safe_load(file)

class TapHotglue(Tap):
    """Hotglue tap class."""

    name = "tap-hotglue"

    def __init__(
        self,
        config=None,
        catalog=None,
        state=None,
        parse_env_config=False,
        validate_config=True,
    ) -> None:
        self.config_file = config[0]
        super().__init__(config, catalog, state, parse_env_config, validate_config)

    @cached_property
    def _tap_definitions(self):
        if self.config.get("tap_definition").endswith(".yaml"):
            return read_yaml_file(self.config.get("tap_definition"))
        return read_json_file(self.config.get("tap_definition"))

    @cached_property
    def airbyte_tap(self):
        return self.config.get("tap_definition").endswith(".yaml")

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
    
    def get_streams_child_context(self, streams):
        child_to_parent = {}
        parent_child_context = {}
        for stream_data in streams:
            stream_data = streams[stream_data] 
            # TODO: should we handle multiple partition routers?
            partition_router = stream_data.get("retriever", {}).get("partition_router", {})
            partition_router = partition_router[0] if isinstance(partition_router, list) else partition_router
            parent_stream = partition_router.get("parent_stream_configs")
            if parent_stream:
                parent_stream = parent_stream[0] # singer sdk only allows one parent stream per stream
                parent_stream_name  = parent_stream.get("stream").get("name") or parent_stream.get("stream").get("$ref").split("/")[-1]
                if not parent_child_context.get(parent_stream_name):
                    parent_child_context[parent_stream_name] = []
                
                parent_child_context[parent_stream_name].append({"name": f"{parent_stream['partition_field']}", "value": f"$.{parent_stream['parent_key']}"})

                child_to_parent[stream_data.get("name")] = parent_stream_name
        
        return child_to_parent, parent_child_context
        
    def sort_streams(self, streams):
        child_to_parent, parent_child_context = self.get_streams_child_context(streams)

        ordered = []
        visited = set()
        visiting = set()

        def visit(stream):
            if stream in visited:
                return
            if stream in visiting:
                raise ValueError(f"Cycle detected involving {stream}")
            visiting.add(stream)

            parent = child_to_parent.get(stream)
            if parent:
                visit(parent)

            if stream not in ordered:
                ordered.append(stream)

            visiting.remove(stream)
            visited.add(stream)

        for s in streams.keys():  # every stream name
            visit(s)

        return ordered, parent_child_context
    
    def normalize_path(self, path: str) -> str:
        """
        Convert a Jinja-style path with stream_partition.item_id
        into a format with {item_id}.
        """
        return re.sub(r"\{\{\s*stream_partition\.(\w+)\s*\}\}", r"{\1}", path)

    def create_streams(self):
        streams = self._tap_definitions.get("definitions").get("streams") if self.airbyte_tap else self._tap_definitions.get("streams", [])
        stream_classes = {}

        # standarize streams to be a dictionary
        streams = {stream['name']:stream for stream in streams}

        # order streams to process parent streams first
        ordered_streams, parent_streams_child_context = self.sort_streams(streams)

        if ordered_streams:
            streams = {name:streams[name] for name in ordered_streams}

        for stream_data in streams:
            stream_data = streams[stream_data]

            # validate all fields needed to create a stream exist:
            try:
                name = stream_data["name"]
                path = stream_data['retriever']['requester']['path'] if self.airbyte_tap else stream_data.get("path")
            except KeyError as e:
                raise Exception(f"Name and/or path values were not found when trying to build the stream for stream_data {stream_data}")
            
            http_method = stream_data['retriever']['requester']['http_method'] if self.airbyte_tap else stream_data.get("method")
            if not name or (not path and http_method != "STATIC"):
                raise Exception(f"Name and/or path values were not found when trying to build the stream for stream_data {stream_data}")

            id = stream_data.get("id") or snakecase(name)
            name = name.replace(" ", "")

            # add required fields
            stream_fields = {
                "name": id,
                "path": path,
                "stream_data": stream_data
            }

            # add REST method if it's specified
            if http_method:
                stream_fields.update({"rest_method": http_method})

            primary_keys = stream_data.get('primary_key') if self.airbyte_tap else stream_data.get("primary_keys")

            if primary_keys:
                stream_fields["primary_keys"] = primary_keys

            if stream_data.get("incremental_sync"):
                replication_key = stream_data["incremental_sync"].get('cursor_field') if self.airbyte_tap else stream_data["incremental_sync"].get("replication_key")
                if replication_key:
                    stream_fields.update({"replication_key": replication_key})
                    stream_fields.update({"incremental_sync": stream_data["incremental_sync"]})

            # add custom params
            if not self.airbyte_tap and stream_data.get("custom_query_params"):
                stream_fields.update({"params": stream_data["custom_query_params"]})
            
            if self.airbyte_tap and stream_data.get("retriever", {}).get("requester", {}).get("request_parameters"):
                airbyte_params = stream_data.get("retriever", {}).get("requester", {}).get("request_parameters")
                params = [{"name": key, "value": value} for key, value in airbyte_params.items()]
                stream_fields.update({"params": params})

            # add custom request payload
            if stream_data.get("custom_request_payload"):
                stream_fields.update({"payload": stream_data["custom_request_payload"]})
            
            if self.airbyte_tap and (request_body := stream_data.get("retriever", {}).get("requester", {}).get("request_body_json")):
                request_body = [{"name": key, "value": value} for key, value in request_body.items()]

            # add records_jsonpath
            if not self.airbyte_tap and stream_data.get("record_selector", {}).get("field_path"):
                json_path = stream_data["record_selector"]["field_path"]
                stream_fields.update({"records_jsonpath": get_json_path(json_path)})

            if self.airbyte_tap and stream_data.get("retriever", {}).get("record_selector", {}).get("extractor", {}).get("field_path"):
                # this is an array, we need to process it to be a valid json path
                json_path = stream_data["retriever"]["record_selector"]["extractor"]["field_path"]
                json_path = ".".join(json_path)
                stream_fields.update({"records_jsonpath": get_json_path(json_path)})
            
            # add schema
            if not self.airbyte_tap and stream_data.get("schema"):
                stream_fields.update({"json_schema": stream_data["schema"]})
            
            if self.airbyte_tap and self._tap_definitions.get("schemas").get(name):
                stream_fields.update({"json_schema": self._tap_definitions["schemas"][name]})

            # get parent stream
            parent_stream_name = None
            if stream_data.get("parent_stream"):
                parent_stream_name = stream_data["parent_stream"]
            
            if self.airbyte_tap:
                # TODO: should we handle multiple partition routers?
                partition_router = stream_data.get("retriever", {}).get("partition_router", {})
                partition_router = partition_router[0] if isinstance(partition_router, list) else partition_router
                parent_stream = partition_router.get("parent_stream_configs")
                if parent_stream:
                    parent_stream_name  = parent_stream[0].get("stream").get("name") or parent_stream[0].get("stream").get("$ref").split("/")[-1]

                    # update path if it's a child stream
                    stream_fields["path"] = self.normalize_path(path)
            
            if parent_stream_name:
                parent_stream_class = stream_classes.get(parent_stream_name)
                if not parent_stream_class:
                    raise Exception(f"Parent stream {parent_stream_name} not found for stream {id}")
                stream_fields.update({"parent_stream_type": parent_stream_class})
            
            # add child context data to parent stream if exists
            if parent_streams_child_context.get(id):
                stream_fields["stream_data"]["child_context"] = parent_streams_child_context[id]

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