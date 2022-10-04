from __future__ import annotations

import asyncio
import atexit
import gc
import logging
import os
import re
import sys
import stat
import warnings
import subprocess 
from pathlib import Path

import click

from distributed  import Client
from dask_ctl import get_cluster


@click.group
def job():
    ...


@job.command()
@click.argument("name")
@click.argument("path", type=Path)
@click.option("--run-on-scheduler", type=bool, )
def submit(name, path):
    cluster = get_cluster(name)
    client = Client(cluster)

    with open(path, 'r') as f:
        script = f.read()

    def runscript():
        # TODO: use a tempfile
        filepath = '/tmp/tmpfile'
        with open(filepath, 'w') as f:
            f.write(script)

        st = os.stat(filepath)
        os.chmod(filepath, st.st_mode | stat.S_IEXEC | stat.S_IREAD)

        # TODO: need to set DASK_SCHEDULER_ADDRESS env variable in call to execute
        # for our script; should be the address of the scheduler
        # might need to use another approach in subprocess for this
        return subprocess.check_output(filepath)

    result = client.run_on_scheduler(runscript)
    print(result)


@job.command()
def gather():
    ...


