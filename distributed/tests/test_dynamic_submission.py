from operator import sub

from dask import delayed

from distributed.utils_test import gen_cluster

def range(x, y):
    a, b = yield delayed(min)(x, y), delayed(max)(x, y)
    diff = yield delayed(sub)(a, b)
    return abs(diff)


@gen_cluster(executor=True)
def test_simple(e, s, a, b):
    x, y = yield e._scatter([10, 20])

    future = e.submit(range, x, y)
    result = yield future._result()
    assert result == 10

    assert len(s.task_state) >= 3
    assert s.dependencies[future.key]
    assert s.who_wants[future.key] == {e.id}


@gen_cluster(executor=True)
def test_resilience(e, s, a, b):
    future = e.submit(range, 10, 20)
    result = yield future._result()
    assert result == 10

    if s.who_has[future.key] == {a.address}:
        yield a._close()
    elif s.who_has[future.key] == {b.address}:
        yield b._close()

    result = yield future._result()
    import pdb; pdb.set_trace()
    assert result == 10

"""
i-- range_x
j--/

    min -----\
i-- range_0 -- range_x
j-/ max -----/

                 a-\
    min -----\   b--sub --\
i-- range_0  --   range_1 -- range_x
j-/ max -----/
"""
