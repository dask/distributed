import ipywidgets


def f():
    f()


try:
    f()
except RecursionError:
    pass

