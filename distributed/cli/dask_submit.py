from distributed.cli.utils import check_python_3
from distributed.submit.submit_cli import submit


def go():
    check_python_3()
    submit()


if __name__ == '__main__':
    go()
