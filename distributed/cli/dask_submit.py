import sys
import click
from tornado import gen
from tornado.ioloop import IOLoop
from distributed.cli.utils import check_python_3
from distributed.submit import _submit

import signal


def handle_signal(sig, frame):
    IOLoop.instance().add_callback(IOLoop.instance().stop)


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


@click.command()
@click.argument('remote_client_address', type=str, required=True)
@click.argument('filepath', type=str, required=True)
def main(remote_client_address, filepath):
    @gen.coroutine
    def f():
        stdout, stderr = yield _submit(remote_client_address, filepath)
        if stdout:
            sys.stdout.write(str(stdout))
        if stderr:
            sys.stderr.write(str(stderr))

    IOLoop.instance().run_sync(f)


def go():
    check_python_3()
    main()


if __name__ == '__main__':
    go()
