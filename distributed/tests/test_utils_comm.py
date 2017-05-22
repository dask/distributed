from __future__ import print_function, division, absolute_import

import pytest

from distributed.core import rpc
from distributed.utils_test import gen_cluster
from distributed.utils_comm import (scatter_to_workers, pack_data,
        gather_from_workers)


def test_pack_data():
    data = {'x': 1}
    assert pack_data(('x', 'y'), data) == (1, 'y')
    assert pack_data({'a': 'x', 'b': 'y'}, data) == {'a': 1, 'b': 'y'}
    assert pack_data({'a': ['x'], 'b': 'y'}, data) == {'a': [1], 'b': 'y'}


@gen_cluster()
def test_gather_from_workers_permissive(s, a, b):
    while not a.batched_stream:
        yield gen.sleep(0.01)
    a.update_data(data={'x': 1})

    data, missing, bad_workers = yield gather_from_workers(
            {'x': [a.address], 'y': [b.address]}, rpc=rpc)

    assert data == {'x': 1}
    assert list(missing) == ['y']
