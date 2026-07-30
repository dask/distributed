import pytest

from distributed.compatibility import WINDOWS

from distributed.comm.addressing import parse_address, unparse_address
from distributed.comm.registry import backends, get_backend
from distributed.comm.uds import UDSBackend


@pytest.mark.skipif(WINDOWS, reason="No unix sockets on Windows")
def test_registered():
    assert "unix" in backends
    backend = get_backend("unix")
    assert isinstance(backend, UDSBackend)


@pytest.mark.skipif(WINDOWS, reason="No unix sockets on Windows")
def test_parse_uds_address():
    addr = "unix:///tmp/dask-test.sock"
    scheme, loc = parse_address(addr)
    assert scheme == "unix"
    assert loc == "/tmp/dask-test.sock"
    assert unparse_address(scheme, loc) == addr
