from __future__ import print_function, division, absolute_import

from distributed.cluster import Cluster
from distributed.core import rpc
from distributed.utils_test import slow
import pytest


@slow
@pytest.mark.avoid_travis
def test_cluster():
    with Cluster(center_addr = '127.0.0.1',
                 center_port = 8787,
                 worker_addrs = ['127.0.0.1', '127.0.0.1']) as c:
        c = rpc(ip='127.0.0.1', port=8787)
        result = []
        while len(result) != 2:
            result = loop.run_sync(c.ncores)

        c.add_worker('127.0.0.1')

        while len(result) != 3:
            result = loop.run_sync(c.ncores)
