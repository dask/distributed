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
from .utils import get_tcp_server_address


def _register_transports():
    from . import inproc, tcp, ws

    try:
        from . import ucx
    except ImportError:
        pass


_register_transports()
