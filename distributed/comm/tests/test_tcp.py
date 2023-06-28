from __future__ import annotations

import socket
import sys

import pytest

from distributed.comm.tcp import TCPConnector


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
