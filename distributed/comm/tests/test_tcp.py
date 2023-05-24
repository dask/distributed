from __future__ import annotations

import socket

import pytest

from distributed.comm.tcp import TCPConnector


# cover the `if e.errno != socket.EAI_NONAME` branch of tcp._getaddrinfo
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
