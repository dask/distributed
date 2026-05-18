from __future__ import annotations

import importlib.metadata
from functools import wraps
from typing import Any

import click

CLICK_VERSION = tuple(map(int, importlib.metadata.version("click").split(".")[:2]))


@wraps(click.option)
def deprecated_option(*args: Any, **kwargs: Any) -> Any:
    if CLICK_VERSION >= (8, 2):
        return click.option(*args, **kwargs, deprecated=True)
    help = kwargs.pop("help", "") + "(DEPRECATED)"
    return click.option(*args, help=help, **kwargs)
