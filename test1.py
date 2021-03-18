import pytest


def test_ipython():
    import IPython


def test_ipywidgets():
    import ipywidgets


def test1():
    from dask.sizeof import sizeof

    n = 500
    original = outer = {}
    inner = {}

    for i in range(n):
        outer["children"] = inner
        outer, inner = inner, {}

    msg = {"data": original}
    with pytest.raises(RecursionError):
        sizeof(msg)


def test2():
    def f():
        f()

    with pytest.raises(RecursionError):
        f()
