from __future__ import print_function, division, absolute_import


from distributed.cli.utils import create_ssl_context
from distributed.utils_test import gen_cluster, inc, loop

import pytest
import os

@pytest.fixture(scope="module")
def ssl_kwargs():
    """
    In order to regenerate the cert files run the `continuous_integration/genereate_test_cert.sh` script

    """

    root = os.path.dirname(__file__)
    certfile = os.path.join(root, 'test.pem')
    keyfile = os.path.join(root, 'test.key')

    return {'ssl_options': create_ssl_context(certfile, keyfile)}


def test_ssl(ssl_kwargs, loop):

    @gen_cluster(
        scheduler_kwargs={'connection_kwargs': ssl_kwargs},
        worker_kwargs={'connection_kwargs': ssl_kwargs},
        client_kwargs={'connection_kwargs': ssl_kwargs},
        client=True)
    def f(c, s, a, b):

        future = c.submit(inc, 1)
        assert future.key in c.futures

        # result = future.result()  # This synchronous API call would block
        result = yield future._result()
        assert result == 2

        assert future.key in s.tasks
        assert future.key in a.data or future.key in b.data

    loop.run_sync(f)
