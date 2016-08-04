from __future__ import print_function, division, absolute_import

import click
from tornado.ioloop import IOLoop

from distributed.cli.utils import check_python_3
from distributed.submit import _remote

import signal


def handle_signal(sig, frame):
    IOLoop.instance().add_callback(IOLoop.instance().stop)


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


@click.command()
@click.option('--host', type=str, default=None,
              help="IP or hostname of this server")
@click.option('--port', type=int, default=8788, help="Remote Client Port")
def main(host, port):
    _remote(host, port)


def go():
    check_python_3()
    main()


if __name__ == '__main__':
    go()
