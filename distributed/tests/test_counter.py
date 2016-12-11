from __future__ import print_function, division, absolute_import

import pytest

from distributed.counter import Counter, Digest

from distributed.utils_test import loop


@pytest.mark.parametrize('CD,size', [(Counter, lambda d: sum(d.values())),
                                     (Digest, lambda x: x.size())])
def test_digest(loop, CD, size):
    c = CD(loop=loop)
    c.add(1)
    c.add(2)
    assert size(c.components[0]) == 2

    c.shift()
    assert 0 < size(c.components[0]) < 2
    assert 0 < size(c.components[1]) < 1
    assert sum(size(d) for d in c.components) == 2

    for i in range(len(c.components) - 1):
        assert size(c.components[i]) >= size(c.components[i + 1])

    c.add(3)

    assert sum(size(d) for d in c.components) == 3
