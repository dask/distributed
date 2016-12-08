from distributed.counter import Counter

from distributed.utils_test import loop


def test_counter(loop):
    c = Counter(loop=loop)
    c.add(1)
    c.add(2)
    assert c.digests[0].count() == 2

    c.shift()
    c.add(3)

    assert c.digests[0].count() == 1
    assert c.digests[1].count() == 2
