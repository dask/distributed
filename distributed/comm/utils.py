
import logging

from .. import protocol


logger = logging.getLogger(__name__)


def to_frames(msg):
    """
    """
    try:
        return list(protocol.dumps(msg))
    except Exception as e:
        logger.info("Unserializable Message: %s", msg)
        logger.exception(e)
        raise


def from_frames(frames, deserialize=True):
    """
    """
    return protocol.loads(frames, deserialize=deserialize)


def parse_host_port(address, default_port=None):
    """
    Parse an endpoint address given in the form "host:port".
    """
    if isinstance(address, tuple):
        return address
    if address.startswith('tcp:'):
        address = address[4:]
    host, sep, port = address.rpartition(':')
    # TODO allow square brackets around host, for IPv6 (e.g. '[::1]:8080')
    if not sep:
        if default_port is None:
            raise ValueError("missing port number in TCP address %r" % (address,))
        host = port
        port = default_port
    else:
        port = int(port)
    return host, port
