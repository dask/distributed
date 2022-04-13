from distributed.comm.addressing import (
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
from distributed.comm.core import Comm, CommClosedError, connect, listen
from distributed.comm.registry import backends
from distributed.comm.utils import get_tcp_server_address, get_tcp_server_addresses


def _register_transports():
    import dask.config

    from distributed.comm import inproc, ws

    tcp_backend = dask.config.get("distributed.comm.tcp.backend")

    if tcp_backend == "asyncio":
        from distributed.comm import asyncio_tcp

        backends["tcp"] = asyncio_tcp.TCPBackend()
        backends["tls"] = asyncio_tcp.TLSBackend()
    elif tcp_backend == "tornado":
        from distributed.comm import tcp

        backends["tcp"] = tcp.TCPBackend()
        backends["tls"] = tcp.TLSBackend()
    else:
        raise ValueError(
            f"Expected `distributed.comm.tcp.backend` to be in `('asyncio', "
            f"'tornado')`, got {tcp_backend}"
        )

    try:
        from distributed.comm import ucx
    except ImportError:
        pass


_register_transports()
