from __future__ import print_function, division, absolute_import

from distributed.utils_test import gen_cluster, inc, loop, ssl_config, ssl_config_no_verify


def test_ssl(ssl_config_no_verify, loop):

    @gen_cluster(client=True)
    def f(c, s, a, b):

        for w in s.workers:
            assert w.startswith('tls')

        future = c.submit(inc, 1)
        assert future.key in c.futures

        result = yield future._result()
        assert result == 2

        assert future.key in s.tasks
        assert future.key in a.data or future.key in b.data

    loop.run_sync(f)
