from __future__ import print_function, division, absolute_import

import click
from distributed.cli.utils import check_python_3, install_signal_handlers
from distributed.submit import _remote


@click.command()
@click.option('--host', type=str, default=None,
              help="IP or hostname of this server")
@click.option('--port', type=int, default=8788, help="Remote Client Port")
@click.option('--certfile', default=None,
              help='path to certfile to use for ssl connection')
@click.option('--keyfile', default=None,
              help='path to keyfile to use for ssl connection')
def main(host, port, certfile, keyfile):
    _remote(host, port, certfile=certfile, keyfile=keyfile)


def go():
    install_signal_handlers()
    check_python_3()
    main()


if __name__ == '__main__':
    go()
