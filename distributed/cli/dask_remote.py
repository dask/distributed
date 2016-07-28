from __future__ import print_function, division, absolute_import

from distributed.cli.utils import check_python_3
from distributed.submit.remote_cli import remote


def go():
    check_python_3()
    remote()


if __name__ == '__main__':
    go()
