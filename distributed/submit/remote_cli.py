import socket
import logging

from tornado.ioloop import IOLoop
from distributed.submit.remote_client import RemoteClient

from distributed.utils import get_ip

logger = logging.getLogger('distributed.remote')


def _remote(host, port, loop=IOLoop.current(), client=RemoteClient):
    host = host or get_ip()
    if ':' in host and port == 8788:
        host, port = host.rsplit(':', 1)
        port = int(port)
    ip = socket.gethostbyname(host)
    remote_client = client(ip=ip, loop=loop)
    remote_client.start(port=port)
    loop.start()
    loop.close()
    remote_client.stop()
    logger.info("End remote client at %s:%d", host, port)
