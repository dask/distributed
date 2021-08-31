import multiprocessing
import os
import shutil
import sys
import tempfile
import time
import urllib.error
import urllib.request

import pytest
import tornado
from tornado import web

import dask

from distributed import Client, Nanny, Scheduler, Worker
from distributed.compatibility import MACOS
from distributed.utils_test import captured_logger, cluster, gen_cluster, gen_test

PY_VERSION = sys.version_info[:2]

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


class MyHandler(web.RequestHandler):
    def get(self):
        self.write(
            """
def dask_setup(dask_server):
    dask_server.foo = 1
""".strip()
        )


def create_preload_application():
    app = web.Application([(r"/preload", MyHandler)])
    server = app.listen(12345, address="127.0.0.1")
    tornado.ioloop.IOLoop.instance().start()


@pytest.fixture
def scheduler_preload():
    p = multiprocessing.Process(target=create_preload_application)
    p.start()
    start = time.time()
    while not p.is_alive():
        if time.time() > start + 5:
            raise AssertionError("Process didn't come up")
        time.sleep(0.5)
    # Make sure we can query the server
    start = time.time()
    request = urllib.request.Request("http://127.0.0.1:12345/preload", method="GET")
    while True:
        try:
            response = urllib.request.urlopen(request)
            if response.status == 200:
                break
        except urllib.error.URLError as e:
            if time.time() > start + 10:
                raise AssertionError("Webserver didn't come up", e)
            time.sleep(0.5)

    yield
    p.kill()
    p.join(timeout=5)


@pytest.mark.skipif(
    MACOS and PY_VERSION == (3, 7), reason="HTTP Server doesn't come up"
)
@pytest.mark.asyncio
async def test_web_preload(cleanup, scheduler_preload):
    with captured_logger("distributed.preloading") as log:
        async with Scheduler(
            host="localhost",
            preload=["http://127.0.0.1:12345/preload"],
        ) as s:
            assert s.foo == 1
    assert "12345/preload" in log.getvalue()


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


class WorkerPreloadHandler(web.RequestHandler):
    def get(self):
        self.write(
            """
import dask
dask.config.set(scheduler_address="tcp://127.0.0.1:8786")
""".strip()
        )


def create_worker_preload_application():
    application = web.Application([(r"/preload", WorkerPreloadHandler)])
    server = application.listen(12346, address="127.0.0.1")
    tornado.ioloop.IOLoop.instance().start()


@pytest.fixture
def worker_preload():
    p = multiprocessing.Process(target=create_worker_preload_application)
    p.start()
    start = time.time()
    while not p.is_alive():
        if time.time() > start + 5:
            raise AssertionError("Process didn't come up")
        time.sleep(0.5)
    # Make sure we can query the server
    request = urllib.request.Request("http://127.0.0.1:12346/preload", method="GET")
    start = time.time()
    while True:
        try:
            response = urllib.request.urlopen(request)
            if response.status == 200:
                break
        except urllib.error.URLError as e:
            if time.time() > start + 10:
                raise AssertionError("Webserver didn't come up", e)
            time.sleep(0.5)

    yield
    p.kill()
    p.join(timeout=5)


@pytest.mark.skipif(
    MACOS and PY_VERSION == (3, 7), reason="HTTP Server doesn't come up"
)
@pytest.mark.asyncio
async def test_web_preload_worker(cleanup, worker_preload):
    async with Scheduler(port=8786, host="localhost") as s:
        async with Nanny(preload_nanny=["http://127.0.0.1:12346/preload"]) as nanny:
            assert nanny.scheduler_addr == s.address
