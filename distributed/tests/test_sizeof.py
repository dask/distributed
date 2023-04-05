from __future__ import annotations

import pytest

from dask.sizeof import sizeof

from distributed.sizeof import safe_sizeof
from distributed.utils_test import captured_logger


@pytest.mark.parametrize("obj", [list(range(10)), tuple(range(10)), set(range(10))])
def test_safe_sizeof(obj):
    assert safe_sizeof(obj) == sizeof(obj)


def test_safe_sizeof_logs_on_failure():
    class BadlySized:
        def __sizeof__(self):
            raise ValueError("bar")

    foo = BadlySized()

    # Defaults to 0.95 MiB by default
    with captured_logger("distributed.sizeof") as logs:
        assert safe_sizeof(foo) == 1e6

    assert "Sizeof calculation failed. Defaulting to 0.95 MiB" in logs.getvalue()

    # Can provide custom `default_size`
    with captured_logger("distributed.sizeof") as logs:
        default_size = 2 * (1024**2)  # 2 MiB
        assert safe_sizeof(foo, default_size=default_size) == default_size

    assert "Defaulting to 2.00 MiB" in logs.getvalue()
