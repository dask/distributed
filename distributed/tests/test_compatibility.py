import pytest


def test_deprecated_periodic_callback():
    from tornado.ioloop import PeriodicCallback

    with pytest.warns(DeprecationWarning):
        from distributed.compatibility import PeriodicCallback as compat
    assert compat is PeriodicCallback
