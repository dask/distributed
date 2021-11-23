from __future__ import annotations

import filecmp
import inspect
import logging
import os
import shutil
import sys
import urllib.request
from collections.abc import Iterable
from importlib import import_module
from types import ModuleType
from typing import cast

import click

from dask.utils import tmpfile

from .core import Server
from .utils import import_file

logger = logging.getLogger(__name__)


def validate_preload_argv(ctx, param, value):
    """Click option callback providing validation of preload subcommand arguments."""
    if not value and not ctx.params.get("preload", None):
        # No preload argv provided and no preload modules specified.
        return value

    if value and not ctx.params.get("preload", None):
        # Report a usage error matching standard click error conventions.
        unexpected_args = [v for v in value if v.startswith("-")]
        for a in unexpected_args:
            raise click.NoSuchOption(a)
        raise click.UsageError(
            "Got unexpected extra argument%s: (%s)"
            % ("s" if len(value) > 1 else "", " ".join(value))
        )

    preload_modules = {
        name: _import_module(name)
        for name in ctx.params.get("preload")
        if not is_webaddress(name)
    }

    preload_commands = [
        getattr(m, "dask_setup", None)
        for m in preload_modules.values()
        if isinstance(getattr(m, "dask_setup", None), click.Command)
    ]

    if len(preload_commands) > 1:
        raise click.UsageError(
            "Multiple --preload modules with click-configurable setup: %s"
            % list(preload_modules.keys())
        )

    if value and not preload_commands:
        raise click.UsageError(
            "Unknown argument specified: %r Was click-configurable --preload target provided?"
        )
    if not preload_commands:
        return value
    else:
        preload_command = preload_commands[0]

    ctx = click.Context(preload_command, allow_extra_args=False)
    preload_command.parse_args(ctx, list(value))

    return value


def is_webaddress(s: str) -> bool:
    return any(s.startswith(prefix) for prefix in ("http://", "https://"))


def _import_module(name, file_dir=None) -> ModuleType:
    """Imports module and extract preload interface functions.

    Import modules specified by name and extract 'dask_setup'
    and 'dask_teardown' if present.

    Parameters
    ----------
    name : str
        Module name, file path, or text of module or script
    file_dir : string
        Path of a directory where files should be copied

    Returns
    -------
    Nest dict of names to extracted module interface components if present
    in imported module.
    """
    if name.endswith(".py"):
        # name is a file path
        if file_dir is not None:
            basename = os.path.basename(name)
            copy_dst = os.path.join(file_dir, basename)
            if os.path.exists(copy_dst):
                if not filecmp.cmp(name, copy_dst):
                    logger.error("File name collision: %s", basename)
            shutil.copy(name, copy_dst)
            module = import_file(copy_dst)[0]
        else:
            module = import_file(name)[0]

    elif " " not in name:
        # name is a module name
        if name not in sys.modules:
            import_module(name)
        module = sys.modules[name]

    else:
        # not a name, actually the text of the script
        with tmpfile(extension=".py") as fn:
            with open(fn, mode="w") as f:
                f.write(name)
            return _import_module(fn, file_dir=file_dir)

    logger.info("Import preload module: %s", name)
    return module


def _download_module(url: str) -> ModuleType:
    logger.info("Downloading preload at %s", url)
    assert is_webaddress(url)

    request = urllib.request.Request(url, method="GET")
    response = urllib.request.urlopen(request)
    source = response.read().decode()

    compiled = compile(source, url, "exec")
    module = ModuleType(url)
    exec(compiled, module.__dict__)
    return module


class Preload:
    """
    Manage state for setup/teardown of a preload module

    Parameters
    ----------
    dask_server: dask.distributed.Server
        The Worker or Scheduler
    name: str
        module name, file name, or web address to load
    argv: [str]
        List of string arguments passed to click-configurable `dask_setup`.
    file_dir: str
        Path of a directory where files should be copied
    """

    dask_server: Server
    name: str
    argv: list[str]
    file_dir: str | None
    module: ModuleType

    def __init__(
        self, dask_server: Server, name: str, argv: Iterable[str], file_dir: str | None
    ):
        self.dask_server = dask_server
        self.name = name
        self.argv = list(argv)
        self.file_dir = file_dir

        if is_webaddress(name):
            self.module = _download_module(name)
        else:
            self.module = _import_module(name, file_dir)

    async def start(self):
        """Run when the server finishes its start method"""
        dask_setup = getattr(self.module, "dask_setup", None)

        if dask_setup:
            if isinstance(dask_setup, click.Command):
                context = dask_setup.make_context(
                    "dask_setup", self.argv, allow_extra_args=False
                )
                result = dask_setup.callback(
                    self.dask_server, *context.args, **context.params
                )
                if inspect.isawaitable(result):
                    await result
                logger.info("Run preload setup click command: %s", self.name)
            else:
                future = dask_setup(self.dask_server)
                if inspect.isawaitable(future):
                    await future
                logger.info("Run preload setup function: %s", self.name)

    async def teardown(self):
        """Run when the server starts its close method"""
        dask_teardown = getattr(self.module, "dask_teardown", None)
        if dask_teardown:
            future = dask_teardown(self.dask_server)
            if inspect.isawaitable(future):
                await future


def process_preloads(
    dask_server,
    preload: str | list[str],
    preload_argv: list[str] | list[list[str]],
    *,
    file_dir: str | None = None,
) -> list[Preload]:
    if isinstance(preload, str):
        preload = [preload]
    if preload_argv and isinstance(preload_argv[0], str):
        preload_argv = [cast("list[str]", preload_argv)] * len(preload)
    elif not preload_argv:
        preload_argv = [cast("list[str]", [])] * len(preload)
    if len(preload) != len(preload_argv):
        raise ValueError(
            "preload and preload_argv have mismatched lengths "
            f"{len(preload)} != {len(preload_argv)}"
        )

    return [
        Preload(dask_server, p, argv, file_dir)
        for p, argv in zip(preload, preload_argv)
    ]
