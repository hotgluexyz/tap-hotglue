import re
from singer_sdk import typing as th
from pendulum import parse
from datetime import datetime

def get_json_path(path):
    path_parts = path.split(".")
    if len(path_parts) == 1:
        return f"$.{path_parts[0]}[*]"
    if len(path_parts) > 1:
        path = path.replace(".*", "[*]")
        return f"$.{path}"

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