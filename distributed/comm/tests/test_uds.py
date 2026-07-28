from distributed.comm.addressing import parse_address, unparse_address
from distributed.comm.registry import backends, get_backend
from distributed.comm.uds import UDSBackend


def test_registered():
    assert "uds" in backends
    backend = get_backend("uds")
    assert isinstance(backend, UDSBackend)


def test_parse_uds_address():
    addr = "uds:///tmp/dask-test.sock"
    scheme, loc = parse_address(addr)
    assert scheme == "uds"
    assert loc == "/tmp/dask-test.sock"
    assert unparse_address(scheme, loc) == addr
