from __future__ import print_function, division, absolute_import

from distributed.utils_test import gen_cluster, inc, loop, ssl_config
import distributed.comm.utils as comm_utils

import ssl


old_ssl_ctx_fx = comm_utils.create_ssl_context


def create_ssl_context_mock():
    ctx = old_ssl_ctx_fx()
    ctx.check_hostname = False
    # ssl_ctx.verify_flags =
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def test_ssl(ssl_config, loop, monkeypatch):
    monkeypatch.setattr(comm_utils, "create_ssl_context", create_ssl_context_mock)

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
