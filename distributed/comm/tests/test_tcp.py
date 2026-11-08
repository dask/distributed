from __future__ import annotations

import socket
import sys

import pytest
from tornado.iostream import IOStream

import dask

from distributed.comm.tcp import TCPConnector, set_tcp_timeout
from distributed.utils_test import gen_test


# cover the `if e.errno != socket.EAI_NONAME` branch of tcp._getaddrinfo
@pytest.mark.skipif(
    sys.platform == "win32" or sys.platform == "darwin",
    reason="getaddrinfo raises EAI_NONAME instead of EAI_ADDRFAMILY on Windows and macOS",
)
def test_getaddrinfo_invalid_af():
    with pytest.raises(socket.gaierror) as exc_info:
        (
            TCPConnector()
            .client.connect(host="::1", port=1234, af=socket.AF_INET)
            .__await__()
            # test that getaddrinfo is called without yielding to the event loop
            .send(None)
        )
    assert exc_info.value.errno == socket.EAI_ADDRFAMILY


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux TCP options")
@pytest.mark.parametrize(
    "timeout, idle, interval, user_timeout",
    [("11s", 1, 1, 11000), ("30s", 10, 2, 30000), ("2m", 30, 9, 120000)],
)
@gen_test()
async def test_tcp_timeout_kernel_options(timeout, idle, interval, user_timeout):
    stream = IOStream(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
    try:
        with dask.config.set({"distributed.comm.timeouts.tcp": timeout}):
            set_tcp_timeout(stream)

        sock = stream.socket
        assert sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) == 1
        assert sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE) == idle
        assert sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL) == interval
        assert sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT) == 10
        assert (
            sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_USER_TIMEOUT) == user_timeout
        )
    finally:
        stream.close()


@gen_test()
async def test_tcp_timeout_too_low():
    stream = IOStream(socket.socket(socket.AF_INET, socket.SOCK_STREAM))
    try:
        with dask.config.set({"distributed.comm.timeouts.tcp": "10s"}):
            with pytest.raises(AssertionError, match="Timeout too low"):
                set_tcp_timeout(stream)
    finally:
        stream.close()
