import time
import unittest

import pytest


def test_fail():
    assert False


@pytest.fixture
def broken_setup():
    assert False
    yield


@pytest.fixture
def broken_teardown():
    yield
    assert False


def test_error_in_setup(broken_setup):
    pass


def test_pass_and_then_error_in_teardown(broken_teardown):
    pass


def test_fail_and_then_error_in_teardown(broken_teardown):
    assert False


@pytest.mark.skip
def test_skip():
    ...


@pytest.mark.xfail
def test_xfail():
    assert False


i = 0


@pytest.mark.flaky(reruns=1)
def test_flaky():
    global i
    i += 1
    assert i == 2


def test_leaking(tmpdir):
    global fh
    fh = open(f"{tmpdir}/foo", "w")


def test_with_stdout():
    print("Hello world!")


def test_pass_before():
    pass


@pytest.mark.parametrize("p", ["a a", "b b"])
def test_params(p):
    pass


class MyTest(unittest.TestCase):
    def test_unittest(self):
        pass


@pytest.mark.timeout(1)
def test_timeout():
    time.sleep(36000)


def test_pass_after():
    pass
