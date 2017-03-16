from __future__ import print_function, division, absolute_import


from distributed.cli.utils import create_ssl_context
from distributed.utils_test import gen_cluster, inc, loop
from distributed import config

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

    assert os.path.exists(certfile)
    assert os.path.exists(keyfile)

    return {'certfile': certfile, 'keyfile': keyfile}


def test_ssl(ssl_kwargs, loop):

    backup = {}
    for k in ['default-scheme', 'tls-certfile', 'tls-keyfile']:
        backup[k] = config[k]

    config['default-scheme'] = 'tls'
    config['tls-certfile'] = ssl_kwargs['certfile']
    config['tls-keyfile'] = ssl_kwargs['keyfile']

    @gen_cluster(
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

    # Unset these
    for k, v in backup.items():
        config[k] = v
