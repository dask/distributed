import datetime
import os.path

from jinja2 import Environment, FileSystemLoader, Template

from dask.utils import format_bytes, format_time_ago


def get_environment() -> Environment:
    loader = FileSystemLoader(
        [os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")]
    )
    environment = Environment(loader=loader)
    environment.filters["format_bytes"] = format_bytes
    environment.filters["format_time_ago"] = format_time_ago
    environment.filters["datetime_from_timestamp"] = datetime.datetime.fromtimestamp
    environment.filters["type"] = lambda cls: type(cls).__name__

    return environment


def get_template(name: str) -> Template:
    return get_environment().get_template(name)
