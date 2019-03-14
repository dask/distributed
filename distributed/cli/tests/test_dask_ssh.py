from __future__ import print_function, division, absolute_import

import subprocess
from time import sleep

import pytest
pytest.importorskip('paramiko')

from distributed import Client
from distributed.metrics import time
from distributed.utils_test import popen
from distributed.utils_test import loop  # noqa: F401


def test_basic(loop):
    with popen(['dask-ssh', '--scheduler-port', '54321',
                '127.0.0.1', '127.0.0.1'],
               stdin=subprocess.DEVNULL):
        with Client("tcp://127.0.0.1:54321") as c:
            start = time()
            while len(c.scheduler_info()['workers']) != 2:
                assert time() < start + 10
                sleep(0.2)

            assert c.submit(lambda x: x + 1, 10, workers=1).result() == 11
