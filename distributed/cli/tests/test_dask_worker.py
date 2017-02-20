from __future__ import print_function, division, absolute_import

import pytest
pytest.importorskip('requests')

import os
import requests
import signal
from time import sleep
from toolz import first

from distributed import Scheduler, Client
from distributed.core import rpc
from distributed.metrics import time
from distributed.utils import sync, ignoring, tmpfile
from distributed.utils_test import (loop, popen, slow, terminate_process,
                                    wait_for_port, randominc)


def test_nanny_worker_ports(loop):
    with popen(['dask-scheduler', '--port', '9359', '--no-bokeh']) as sched:
        with popen(['dask-worker', '127.0.0.1:9359', '--host', '127.0.0.1',
                    '--worker-port', '9684', '--nanny-port', '5273',
                    '--no-bokeh']) as worker:
            with Client('127.0.0.1:9359', loop=loop) as c:
                start = time()
                while True:
                    d = sync(c.loop, c.scheduler.identity)
                    if d['workers']:
                        break
                    else:
                        assert time() - start < 5
                        sleep(0.1)
                assert d['workers']['tcp://127.0.0.1:9684']['services']['nanny'] == 5273


def test_memory_limit(loop):
    with popen(['dask-scheduler', '--no-bokeh']) as sched:
        with popen(['dask-worker', '127.0.0.1:8786', '--memory-limit', '2e9',
                    '--no-bokeh']) as worker:
            with Client('127.0.0.1:8786', loop=loop) as c:
                while not c.ncores():
                    sleep(0.1)
                info = c.scheduler_info()
                d = first(info['workers'].values())
                assert isinstance(d['memory_limit'], float)
                assert d['memory_limit'] == 2e9


def test_no_nanny(loop):
    with popen(['dask-scheduler', '--no-bokeh']) as sched:
        with popen(['dask-worker', '127.0.0.1:8786', '--no-nanny',
                    '--no-bokeh']) as worker:
            assert any(b'Registered' in worker.stderr.readline()
                       for i in range(15))


@slow
@pytest.mark.parametrize('nanny', ['--nanny', '--no-nanny'])
def test_no_reconnect(nanny, loop):
    with popen(['dask-scheduler', '--no-bokeh']) as sched:
        wait_for_port(('127.0.0.1', 8786))
        with popen(['dask-worker', 'tcp://127.0.0.1:8786', '--no-reconnect', nanny,
                    '--no-bokeh']) as worker:
            sleep(2)
            terminate_process(sched)
        start = time()
        while worker.poll() is None:
            sleep(0.1)
            assert time() < start + 10


def test_resources(loop):
    with popen(['dask-scheduler', '--no-bokeh']) as sched:
        with popen(['dask-worker', 'tcp://127.0.0.1:8786', '--no-bokeh',
                    '--resources', 'A=1 B=2,C=3']) as worker:
            with Client('127.0.0.1:8786', loop=loop) as c:
                while not c.scheduler_info()['workers']:
                    sleep(0.1)
                info = c.scheduler_info()
                worker = list(info['workers'].values())[0]
                assert worker['resources'] == {'A': 1, 'B': 2, 'C': 3}


@pytest.mark.parametrize('nanny', ['--nanny', '--no-nanny'])
def test_local_directory(loop, nanny):
    with tmpfile() as fn:
        with popen(['dask-scheduler', '--no-bokeh']) as sched:
            with popen(['dask-worker', '127.0.0.1:8786', nanny,
                        '--no-bokeh', '--local-directory', fn]) as worker:
                with Client('127.0.0.1:8786', loop=loop) as c:
                    while not c.scheduler_info()['workers']:
                        sleep(0.1)
                    info = c.scheduler_info()
                    worker = list(info['workers'].values())[0]
                    assert worker['local_directory'] == fn


@pytest.mark.parametrize('nanny', ['--nanny', '--no-nanny'])
def test_lifetime(loop, nanny):
    worker1_cmd = ['dask-worker', '127.0.0.1:8786', nanny, '--no-bokeh',
                   '--nprocs=1', '--name=a', '--lifetime=s=5']
    worker2_cmd = ['dask-worker', '127.0.0.1:8786', nanny, '--no-bokeh',
                   '--nprocs=1', '--name=b']
    with popen(['dask-scheduler', '--no-bokeh']) as w1:
        with Client('127.0.0.1:8786', loop=loop) as c:
            with popen(worker1_cmd) as w1, popen(worker2_cmd) as w2:
                # submit random function to ensure replication
                f = c.submit(randominc, 1, workers=['a'])
                r = f.result()

                start = time()
                while len(c.has_what()) != 1:
                    sleep(0.1)
                    if time() - start > 10:
                        raise TimeoutError

                assert f.status != 'lost'
                assert f.result() == r


@pytest.mark.parametrize('nanny', ['--nanny', '--no-nanny'])
def test_auto_retire_multi_worker(loop, nanny):
    worker1_cmd = ['dask-worker', '127.0.0.1:8786', nanny, '--no-bokeh',
                   '--nprocs=2', '--lifetime=s=5']
    worker2_cmd = ['dask-worker', '127.0.0.1:8786', nanny, '--no-bokeh',
                   '--nprocs=1', '--worker-port=22222']
    with popen(['dask-scheduler', '--no-bokeh']) as _:
        with Client('127.0.0.1:8786', loop=loop) as c:
            with popen(worker1_cmd) as _:

                start = time()
                while len(c.has_what()) != 2:
                    sleep(0.1)
                    if time() - start > 10:
                        raise TimeoutError

                worker1_addresses = c.has_what().keys()
                fs = [c.submit(randominc, i, workers=worker1_addresses)
                      for i in range(4)]
                rs = [f.result() for f in fs]

                with popen(worker2_cmd) as _:

                    start = time()
                    while len(c.has_what()) != 1:
                        sleep(0.1)
                        if time() - start > 10:
                            raise TimeoutError

                    assert all(f.status != 'lost' for f in fs)
                    assert [f.result() for f in fs] == rs
