import os
import pytest
import shutil
import sys
import tempfile
import pytest

import dask
from distributed import Client, Scheduler, Worker, Nanny
from distributed.utils_test import cluster
from distributed.utils_test import loop, cleanup  # noqa F401


PRELOAD_TEXT = """
_worker_info = {}

def dask_setup(worker):
    _worker_info['address'] = worker.address

def get_worker_address():
    return _worker_info['address']
"""


def test_worker_preload_file(loop):
    def check_worker():
        import worker_info

        return worker_info.get_worker_address()

    tmpdir = tempfile.mkdtemp()
    try:
        path = os.path.join(tmpdir, "worker_info.py")
        with open(path, "w") as f:
            f.write(PRELOAD_TEXT)

        with cluster(worker_kwargs={"preload": [path]}) as (s, workers), Client(
            s["address"], loop=loop
        ) as c:

            assert c.run(check_worker) == {
                worker["address"]: worker["address"] for worker in workers
            }
    finally:
        shutil.rmtree(tmpdir)


@pytest.mark.asyncio
async def test_worker_preload_text(cleanup):
    text = """
def dask_setup(worker):
    worker.foo = 'setup'
"""
    async with Scheduler(port=0, preload=text) as s:
        assert s.foo == "setup"
        async with Worker(s.address, preload=[text]) as w:
            assert w.foo == "setup"


@pytest.mark.asyncio
async def test_worker_preload_config(cleanup):
    text = """
def dask_setup(worker):
    worker.foo = 'setup'
"""
    with dask.config.set({"distributed.worker.preload": text}):
        async with Scheduler(port=0) as s:
            async with Nanny(s.address) as w:
                async with Client(s.address, asynchronous=True) as c:
                    d = await c.run(lambda dask_worker: dask_worker.foo)
                    assert d == {w.worker_address: "setup"}


def test_worker_preload_module(loop):
    def check_worker():
        import worker_info

        return worker_info.get_worker_address()

    tmpdir = tempfile.mkdtemp()
    sys.path.insert(0, tmpdir)
    try:
        path = os.path.join(tmpdir, "worker_info.py")
        with open(path, "w") as f:
            f.write(PRELOAD_TEXT)

        with cluster(worker_kwargs={"preload": ["worker_info"]}) as (
            s,
            workers,
        ), Client(s["address"], loop=loop) as c:

            assert c.run(check_worker) == {
                worker["address"]: worker["address"] for worker in workers
            }
    finally:
        sys.path.remove(tmpdir)
        shutil.rmtree(tmpdir)
