from __future__ import print_function, division, absolute_import

import logging
import socket

import click

from tornado.ioloop import IOLoop

from distributed.remote import RemoteClient
from distributed.utils import get_ip
from distributed.cli.utils import check_python_3

logger = logging.getLogger('distributed.remote')

@click.command()
@click.option('--host', type=str, default=None,
              help="IP or hostname of this server")
@click.option('--port', type=int, default=8788, help="Remote Client Port")
def remote(host, port):
    _remote(host, port)
    logger.info("End remote client at %s:%d", host, port)


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


def go():
    check_python_3()
    remote()


if __name__ == '__main__':
    go()
