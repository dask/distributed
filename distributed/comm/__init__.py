from .addressing import (
    get_address_host,
    get_address_host_port,
    get_local_address_for,
    normalize_address,
    parse_address,
    parse_host_port,
    resolve_address,
    unparse_address,
    unparse_host_port,
)
from .core import Comm, CommClosedError, connect, listen
from .registry import backends
from .utils import get_tcp_server_address, get_tcp_server_addresses


def _register_transports():
    import dask.config

    from . import inproc, ws

    tcp_backend = dask.config.get("distributed.comm.tcp.backend")

    if tcp_backend == "asyncio":
        from . import asyncio_tcp

        backends["tcp"] = asyncio_tcp.TCPBackend()
        backends["tls"] = asyncio_tcp.TLSBackend()
    elif tcp_backend == "tornado":
        from . import tcp

        backends["tcp"] = tcp.TCPBackend()
        backends["tls"] = tcp.TLSBackend()
    else:
        raise ValueError(
            f"Expected `distributed.comm.tcp.backend` to be in `('asyncio', "
            f"'tornado')`, got {tcp_backend}"
        )

    try:
        from . import ucx
    except ImportError:
        pass


_register_transports()
