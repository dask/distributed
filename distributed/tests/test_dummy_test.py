import pytest


@pytest.mark.parametrize("value", [1])
def test_dummy_test(value):
    # This is a dummy test and should not be merged
    assert value == 1
