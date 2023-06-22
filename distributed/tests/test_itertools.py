from __future__ import annotations

from distributed.itertools import ffill


def test_ffill():
    actual = "".join(ffill([1, 1.5, 2, 2, 2.5, 3], [1, 2, 3], "abc", "-"))
    assert actual == "aabbbc"

    # Test left and right edges
    actual = "".join(ffill([-1, 0, 1, 2.9, 3, 4, 5], [1, 2, 3], "abc", "-"))
    assert actual == "--abccc"

    # Test edge cases
    actual = "".join(ffill([1, 2, 3], [], "", "-"))
    assert actual == "---"

    actual = "".join(ffill([], [], "", "-"))
    assert actual == ""

    actual = "".join(ffill([], [1, 2, 3], "abc", "-"))
    assert actual == ""

    # xp is shorter than fp or vice versa
    actual = "".join(ffill([1, 2], [1], "ab", "-"))
    assert actual == "aa"
    actual = "".join(ffill([1, 2], [1, 2], "a", "-"))
    assert actual == "aa"

    # x can be any non-numerical sortable
    # y can be anything
    # Inputs don't need to be sequences
    yp = [object(), object()]
    left = object()
    actual = list(ffill(iter("abcde"), iter("bd"), iter(yp), left))
    assert actual == [left, yp[0], yp[0], yp[1], yp[1]]
