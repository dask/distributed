from __future__ import annotations

import contextlib

import pytest

from distributed import Client, LocalCluster
from distributed.utils import LoopRunner


@contextlib.contextmanager
def _check_loop_runner():
    loops_before = LoopRunner._all_loops.copy()
    yield
    # Internal loops registry must the same as before cluster running.
    # This means loop runners in LocalCluster and Client correctly stopped.
    # See LoopRunner._stop_unlocked().
    assert loops_before == LoopRunner._all_loops


def _check_cluster_and_client_loop(loop):
    # Setup simple cluster with one threaded worker.
    # Complex setup is not required here since we test only IO loop teardown.
    with LocalCluster(
        loop=loop, n_workers=1, dashboard_address=":0", processes=False
    ) as cluster:
        with Client(cluster, loop=loop) as client:
            client.run(max, 1, 2)


# Test if Client stops LoopRunner on close.
@pytest.mark.filterwarnings("ignore:There is no current event loop:DeprecationWarning")
@pytest.mark.filterwarnings("ignore:make_current is deprecated:DeprecationWarning")
def test_close_loop_sync_start_new_loop(cleanup):
    with _check_loop_runner():
        _check_cluster_and_client_loop(loop=None)


# Test if Client stops LoopRunner on close.
@pytest.mark.filterwarnings("ignore:There is no current event loop:DeprecationWarning")
@pytest.mark.filterwarnings("ignore:make_current is deprecated:DeprecationWarning")
def test_close_loop_sync_use_running_loop(cleanup):
    with _check_loop_runner():
        # Start own loop or use current thread's one.
        loop_runner = LoopRunner()
        loop_runner.start()

        try:
            _check_cluster_and_client_loop(loop=loop_runner.loop)
        finally:
            # own loop must be explicitly stopped.
            loop_runner.stop()
