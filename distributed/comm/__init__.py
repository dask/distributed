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
    from . import asyncio_tcp, inproc, tcp, ws

    if True:  # TODO: some kind of config
        backends["tcp"] = asyncio_tcp.TCPBackend()
        backends["tls"] = asyncio_tcp.TLSBackend()
    else:
        backends["tcp"] = tcp.TCPBackend()
        backends["tls"] = tcp.TLSBackend()

    try:
        from . import ucx
    except ImportError:
        pass


_register_transports()
