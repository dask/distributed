import pytest


def test_ipython():
    import IPython


def test_ipywidgets():
    import ipywidgets


def test1():
    def f():
        f()

    with pytest.raises(RecursionError):
        f()
