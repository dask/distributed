from __future__ import print_function, division, absolute_import

import subprocess

from distributed import (Client, Scheduler, Worker)
from distributed.utils_test import gen_cluster, inc, loop

import pytest
import os

@pytest.fixture(scope="function")
def ssl_kwargs(tmpdir):

    ssl_dir = tmpdir.mkdir("ssl")
    dirname = ssl_dir.dirname

    keyfile = os.path.join(dirname, 'test.key')
    certfile = os.path.join(dirname, 'test.pem')

    p = subprocess.Popen([
        'openssl', 'req',
        '-x509',
        '-nodes',
        '-newkey',
        'rsa:2048',
        '-days', '365',
        '-subj', "/O=dask/CN=localhost"
        '-nodes',
        '-passout', 'pass:',
        '-keyout', 'test.key',
        '-out', 'test.pem'],
        cwd=ssl_dir.dirname,
    )

    p.communicate()
    assert p.returncode == 0

    return {'ssl_options': {
        'certfile': certfile,
        'keyfile': keyfile,
    }}


def test_ssl(ssl_kwargs, loop):

    @gen_cluster(
        scheduler_kwargs={'connection_kwargs': ssl_kwargs},
        worker_kwargs={'connection_kwargs': ssl_kwargs},
        client=True)
    def f(c, s, a, b):

        assert isinstance(c, Client)
        assert isinstance(c, Scheduler)
        assert isinstance(a, Worker)
        assert isinstance(b, Worker)



        future = c.submit(inc, 1)
        assert future.key in c.futures

        # result = future.result()  # This synchronous API call would block
        result = yield future._result()
        assert result == 2

        assert future.key in s.tasks
        assert future.key in a.data or future.key in b.data

    loop.run_sync(f)
