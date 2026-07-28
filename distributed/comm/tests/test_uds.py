from distributed.comm.addressing import parse_address, unparse_address
from distributed.comm.registry import backends, get_backend
from distributed.comm.uds import UDSBackend


def test_registered():
    assert "unix" in backends
    backend = get_backend("unix")
    assert isinstance(backend, UDSBackend)


def test_parse_uds_address():
    addr = "unix:///tmp/dask-test.sock"
    scheme, loc = parse_address(addr)
    assert scheme == "unix"
    assert loc == "/tmp/dask-test.sock"
    assert unparse_address(scheme, loc) == addr
