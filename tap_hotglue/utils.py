import re
from singer_sdk import typing as th
from pendulum import parse
from datetime import datetime
import isodate
from datetime import timedelta
import json
from lxml import etree
import xmltodict

def get_json_path(path):
    if not "*" in path:
        path = f"{path}.*"
    path_parts = path.split(".")
    if len(path_parts) > 1:
        path = path.replace(".*", "[*]")
        return f"$.{path}"
    else:
        return path

def snakecase(string: str) -> str:
    """Convert string into snake case.

    Args:
        string: String to convert.

    Returns:
        string: Snake cased string.
    """
    if string.isupper():
        string = string.lower()
    string = re.sub(r"[\-\.\s]", "_", string)
    string = (
        string[0].lower()
        + re.sub(
            r"[A-Z]", lambda matched: "_" + str(matched.group(0).lower()), string[1:]
        )
        if string
        else string
    )
    return re.sub(r"_{2,}", "_", string).rstrip("_")

def is_unix_timestamp(self, number):
    try:
        converted_date = datetime.utcfromtimestamp(number)
        if converted_date.year > 1970:
            return True
        else:
            return False
    except:
        return False

def get_jsonschema_type(obj):
        dtype = type(obj)

        if dtype == int:
            return th.IntegerType()
        if dtype == float:
            return th.NumberType()
        if dtype == str:
            try:
                parse(obj)
                return th.DateTimeType
            except:
                return th.StringType()
        if dtype == bool:
            return th.BooleanType()
        if dtype == list:
            if len(obj) > 0:
                return th.ArrayType(get_jsonschema_type(obj[0]))
            else:
                return th.ArrayType(
                    th.CustomType({"type": ["number", "string", "object"]})
                )
        if dtype == dict:
            obj_props = []
            for key in obj.keys():
                obj_props.append(th.Property(key, get_jsonschema_type(obj[key])))
            return th.ObjectType(*obj_props)

        raise ValueError(f"Unmappable data type '{dtype}'.")

def iso_duration_to_timedelta(duration_str: str) -> timedelta:
    duration = isodate.parse_duration(duration_str)
    # `parse_duration` returns either timedelta or Duration (with months/years)
    if isinstance(duration, timedelta):
        return duration
    # Approximate months and years
    days = duration.years * 365 + duration.months * 30
    return timedelta(days=days, seconds=duration.tdelta.seconds, microseconds=duration.tdelta.microseconds)

def xml_to_dict(response):
    try:
        #clean invalid xml characters
        my_parser = etree.XMLParser(recover=True)
        xml = etree.fromstring(response.content, parser=my_parser)
        cleaned_xml_string = etree.tostring(xml)
        #parse xml to dict
        data = json.loads(json.dumps(xmltodict.parse(cleaned_xml_string)))
    except:
        data = json.loads(json.dumps(xmltodict.parse(response.content.decode("utf-8-sig").encode("utf-8"))))
    return data