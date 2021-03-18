import IPython


def f():
    f()


try:
    f()
except RecursionError:
    pass

