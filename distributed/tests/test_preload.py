import os
import shutil
import sys
import tempfile

import pytest
from tornado import web

import dask

from distributed import Client, Nanny, Scheduler, Worker
from distributed.utils_test import captured_logger, cluster, gen_cluster, gen_test

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


@gen_test()
async def test_worker_preload_text():
    text = """
def dask_setup(worker):
    worker.foo = 'setup'
"""
    async with Scheduler(dashboard_address=":0", preload=text) as s:
        assert s.foo == "setup"
        async with Worker(s.address, preload=[text]) as w:
            assert w.foo == "setup"


@gen_cluster(nthreads=[])
async def test_worker_preload_config(s):
    text = """
def dask_setup(worker):
    worker.foo = 'setup'

def dask_teardown(worker):
    worker.foo = 'teardown'
"""
    with dask.config.set(
        {"distributed.worker.preload": text, "distributed.nanny.preload": text}
    ):
        async with Nanny(s.address) as w:
            assert w.foo == "setup"
            async with Client(s.address, asynchronous=True) as c:
                d = await c.run(lambda dask_worker: dask_worker.foo)
                assert d == {w.worker_address: "setup"}
        assert w.foo == "teardown"


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


@gen_cluster(nthreads=[])
async def test_worker_preload_click(s):
    text = """
import click

@click.command()
def dask_setup(worker):
    worker.foo = 'setup'
"""

    async with Worker(s.address, preload=text) as w:
        assert w.foo == "setup"


@gen_cluster(nthreads=[])
async def test_worker_preload_click_async(s, tmpdir):
    # Ensure we allow for click commands wrapping coroutines
    # https://github.com/dask/distributed/issues/4169
    text = """
import click

@click.command()
async def dask_setup(worker):
    worker.foo = 'setup'
"""
    async with Worker(s.address, preload=text) as w:
        assert w.foo == "setup"


@pytest.mark.asyncio
async def test_preload_import_time(cleanup):
    text = """
from distributed.comm.registry import backends
from distributed.comm.tcp import TCPBackend

backends["foo"] = TCPBackend()
""".strip()
    try:
        async with Scheduler(dashboard_address=":0", preload=text, protocol="foo") as s:
            async with Nanny(s.address, preload=text, protocol="foo") as n:
                async with Client(s.address, asynchronous=True) as c:
                    await c.wait_for_workers(1)
    finally:
        from distributed.comm.registry import backends

        del backends["foo"]


@pytest.mark.asyncio
async def test_web_preload(cleanup):
    class MyHandler(web.RequestHandler):
        def get(self):
            self.write(
                """
def dask_setup(dask_server):
    dask_server.foo = 1
""".strip()
            )

    app = web.Application([(r"/preload", MyHandler)])
    server = app.listen(12345)
    try:
        with captured_logger("distributed.preloading") as log:
            async with Scheduler(
                dashboard_address=":0",
                preload=["http://localhost:12345/preload"],
            ) as s:
                assert s.foo == 1
        assert "12345/preload" in log.getvalue()
    finally:
        server.stop()


@gen_cluster(nthreads=[])
async def test_scheduler_startup(s):
    text = f"""
import dask
dask.config.set(scheduler_address="{s.address}")
"""
    async with Worker(preload=text) as w:
        assert w.scheduler.address == s.address


@gen_cluster(nthreads=[])
async def test_scheduler_startup_nanny(s):
    text = f"""
import dask
dask.config.set(scheduler_address="{s.address}")
"""
    async with Nanny(preload_nanny=text) as w:
        assert w.scheduler.address == s.address
