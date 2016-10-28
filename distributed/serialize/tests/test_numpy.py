from __future__ import print_function, division, absolute_import

import numpy as np
import pytest

from distributed.serialize.core import serialize, deserialize
from distributed.protocol import decompress

import distributed.serialize.numpy

def test_serialize():
    x = np.ones((5, 5))
    header, frames = serialize(x)
    assert header['type']
    assert len(frames) == 1

    if 'compression' in header:
        frames = decompress(header, frames)
    result = deserialize(header, frames)
    assert (result == x).all()


@pytest.mark.parametrize('x',
        [np.ones(5),
         np.asfortranarray(np.random.random((5, 5))),
         np.random.random(5).astype('f4'),
         np.empty(shape=(5, 3), dtype=[('total', '<f8'), ('n', '<f8')])])
def test_dumps_serialize_numpy(x):
    header, frames = serialize(x)
    if 'compression' in header:
        frames = decompress(header, frames)
    y = deserialize(header, frames)

    np.testing.assert_equal(x, y)

