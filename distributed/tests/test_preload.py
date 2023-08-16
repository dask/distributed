from __future__ import annotations

import os
import re
import shutil
import sys
import tempfile
from textwrap import dedent
from unittest import mock

import pytest

import dask

from distributed import Client, Nanny, Scheduler, Worker
from distributed.preloading import Preload
from distributed.utils import open_port
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


@gen_test()
async def test_preload_manager_sequence():
    text = """
def dask_setup(worker):
    worker.foo = 'setup'
"""
    async with Scheduler(dashboard_address=":0", preload=text) as s:
        assert len(s.preloads) == 1
        assert isinstance(s.preloads[0], Preload)
        # Make sure list comprehensions return the correct # of items
        assert len([x for x in s.preloads]) == len(s.preloads)


@gen_cluster(nthreads=[])
async def test_worker_preload_config(s):
    text = """
def dask_setup(worker):
    worker.foo = 'setup'

def dask_teardown(worker):
    worker.foo = 'teardown'
"""
    with dask.config.set(
        {"distributed.worker.preload": [text], "distributed.nanny.preload": [text]}
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
async def test_worker_preload_click_async(s, tmp_path):
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


@gen_test()
async def test_preload_import_time():
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


@gen_test()
async def test_web_preload():
    with mock.patch(
        "urllib3.PoolManager.request",
        **{
            "return_value.data": b"def dask_setup(dask_server):"
            b"\n    dask_server.foo = 1"
            b"\n"
        },
    ) as request, captured_logger("distributed.preloading") as log:
        async with Scheduler(
            host="localhost", preload=["http://example.com/preload"]
        ) as s:
            assert s.foo == 1
        assert (
            re.match(
                r"(?s).*Downloading preload at http://example.com/preload\n"
                r".*Run preload setup: http://example.com/preload\n"
                r".*",
                log.getvalue(),
            )
            is not None
        )
    assert request.mock_calls == [
        mock.call(method="GET", url="http://example.com/preload", retries=mock.ANY)
    ]


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


@gen_test()
async def test_web_preload_worker():
    port = open_port()
    data = dedent(
        f"""\
        import dask
        dask.config.set(scheduler_address="tcp://127.0.0.1:{port}")
        """
    ).encode()
    with mock.patch(
        "urllib3.PoolManager.request",
        **{"return_value.data": data},
    ) as request:
        async with Scheduler(port=port, host="localhost") as s:
            async with Nanny(preload_nanny=["http://example.com/preload"]) as nanny:
                assert nanny.scheduler_addr == s.address
    assert request.mock_calls == [
        mock.call(method="GET", url="http://example.com/preload", retries=mock.ANY)
    ]


# This test is blocked on https://github.com/dask/distributed/issues/5819
@pytest.mark.xfail(
    reason="The preload argument to the client isn't supported yet", strict=True
)
@gen_cluster(nthreads=[])
async def test_client_preload_text(s):
    text = dedent(
        """\
        def dask_setup(client):
            client.foo = "setup"


        def dask_teardown(client):
            client.foo = "teardown"
        """
    )
    async with Client(address=s.address, asynchronous=True, preload=text) as c:
        assert c.foo == "setup"
    assert c.foo == "teardown"


@gen_cluster(nthreads=[])
async def test_client_preload_config(s):
    text = dedent(
        """\
        def dask_setup(client):
            client.foo = "setup"


        def dask_teardown(client):
            client.foo = "teardown"
        """
    )
    with dask.config.set({"distributed.client.preload": [text]}):
        async with Client(address=s.address, asynchronous=True) as c:
            assert c.foo == "setup"
        assert c.foo == "teardown"


# This test is blocked on https://github.com/dask/distributed/issues/5819
@pytest.mark.xfail(
    reason="The preload argument to the client isn't supported yet", strict=True
)
@gen_cluster(nthreads=[])
async def test_client_preload_click(s):
    text = dedent(
        """\
        import click

        @click.command()
        @click.argument("value")
        def dask_setup(client, value):
            client.foo = value
        """
    )
    value = "setup"
    async with Client(
        address=s.address, asynchronous=True, preload=text, preload_argv=[[value]]
    ) as c:
        assert c.foo == value


@gen_test()
async def test_failure_doesnt_crash_scheduler():
    text = """
def dask_setup(worker):
    raise Exception(123)

def dask_teardown(worker):
    raise Exception(456)
"""

    with captured_logger("distributed.preloading") as logger:
        async with Scheduler(dashboard_address=":0", preload=text):
            pass

    logs = logger.getvalue()
    assert "123" in logs
    assert "456" in logs


@gen_cluster(client=False, nthreads=[])
async def test_failure_doesnt_crash_worker(s):
    text = """
def dask_setup(worker):
    raise Exception(123)

def dask_teardown(worker):
    raise Exception(456)
"""

    with captured_logger("distributed.preloading") as logger:
        async with Worker(s.address, preload=[text]):
            pass

    logs = logger.getvalue()
    assert "123" in logs
    assert "456" in logs


@gen_cluster(client=False, nthreads=[])
async def test_failure_doesnt_crash_nanny(s):
    text = """
def dask_setup(worker):
    raise Exception(123)

def dask_teardown(worker):
    raise Exception(456)
"""

    with captured_logger("distributed.preloading") as logger:
        async with Nanny(s.address, preload_nanny=[text]):
            pass

    logs = logger.getvalue()
    assert "123" in logs
    assert "456" in logs


@gen_cluster(client=False)
async def test_failure_doesnt_crash_client(s, a, b):
    text = """
def dask_setup(worker):
    raise Exception(123)

def dask_teardown(worker):
    raise Exception(456)
"""

    with dask.config.set({"distributed.client.preload": [text]}):
        with captured_logger("distributed.preloading") as logger:
            async with Client(s.address, asynchronous=True):
                pass

    logs = logger.getvalue()
    assert "123" in logs
    assert "456" in logs


@gen_cluster(nthreads=[])
async def test_client_preload_config_click(s):
    text = dedent(
        """\
        import click

        @click.command()
        @click.argument("value")
        def dask_setup(client, value):
            client.foo = value
        """
    )
    value = "setup"
    with dask.config.set(
        {
            "distributed.client.preload": [text],
            "distributed.client.preload-argv": [[value]],
        }
    ):
        async with Client(address=s.address, asynchronous=True) as c:
            assert c.foo == value
