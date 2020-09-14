import pytest
import threading
from time import sleep
from distributed import LocalCluster, Client
from distributed.utils import LoopRunner


def run_client(loop):
    with LocalCluster(loop=loop, asynchronous=False) as cluster:
        client = Client(cluster, loop=loop, asynchronous=False)
        sleep(1)  # Allow IO loop process events.
        client.close()


# Test if Client correctly tears down LoopRunner on close.
@pytest.mark.parametrize('with_own_loop', [True, False])
def test_close_loop_sync(with_own_loop):
    loop_runner = loop = None

    # Number of threads before and after the Cluster running.
    threads_before = []
    threads_after = []

    for i in range(3):
        threads_before.append(threading.enumerate())

        if with_own_loop:
            loop_runner = LoopRunner(asynchronous=False)
            loop_runner.start()
            loop = loop_runner.loop

        run_client(loop)

        if with_own_loop:
            loop_runner.stop()

        # Allow IO loop process events and wait until daemon threads finished.
        sleep(1)
        threads_after.append(threading.enumerate())

    # TCP backend starts globally and runs few own threads on demand.
    # 'threads_before' populates before that and contains less threads on first
    # iteration. That's why we drop first values.
    threads_before = threads_before[1:]
    threads_after = threads_after[1:]

    # In all other iterations there must be same number of threads before and
    # after the Client running.
    assert all([x == y for (x, y) in zip(threads_before, threads_after)])
