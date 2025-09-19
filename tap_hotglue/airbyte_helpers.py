from pendulum import parse
import datetime

def now_utc():
    """Helper for UTC datetime."""
    return datetime.datetime.now(datetime.timezone.utc)

def format_datetime(dt_str, fmt):
    """Helper for converting a datetime string to a specific format."""
    dt = parse(dt_str)
    return dt.strftime(fmt)