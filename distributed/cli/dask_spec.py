from __future__ import annotations

import asyncio
import json
import sys

import click
import yaml

from distributed.compatibility import asyncio_run
from distributed.config import get_loop_factory
from distributed.deploy.spec import run_spec


@click.command(name="spec", context_settings=dict(ignore_unknown_options=True))
@click.argument("args", nargs=-1)
@click.option("--spec", type=str, default="", help="")
@click.option("--spec-file", type=str, default=None, help="")
@click.version_option()
def main(args: list, spec: str, spec_file: str) -> None:
    """Launch a Dask process defined by a JSON/YAML specification"""

    if spec and spec_file or not spec and not spec_file:
        print("Must specify exactly one of --spec and --spec-file")
        sys.exit(1)
    _spec = {}
    if spec_file:
        with open(spec_file) as f:
            _spec.update(yaml.safe_load(f))

    if spec:
        _spec.update(json.loads(spec))

    if "cls" in _spec:  # single worker spec
        _spec = {_spec["opts"].get("name", 0): _spec}

    async def run():
        servers = await run_spec(_spec, *args)
        try:
            await asyncio.gather(*(w.finished() for w in servers.values()))
        except KeyboardInterrupt:
            await asyncio.gather(*(w.close() for w in servers.values()))

    asyncio_run(run(), loop_factory=get_loop_factory())


if __name__ == "__main__":
    main()
