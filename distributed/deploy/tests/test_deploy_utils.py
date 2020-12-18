from distributed.deploy.utils import nprocesses_nthreads


def test_default_process_thread_breakdown():
    assert nprocesses_nthreads(1) == (1, 1)
    assert nprocesses_nthreads(4) == (4, 1)
    assert nprocesses_nthreads(5) == (5, 1)
    assert nprocesses_nthreads(8) == (4, 2)
    assert nprocesses_nthreads(12) in ((6, 2), (4, 3))
    assert nprocesses_nthreads(20) == (5, 4)
    assert nprocesses_nthreads(24) in ((6, 4), (8, 3))
    assert nprocesses_nthreads(32) == (8, 4)
    assert nprocesses_nthreads(40) in ((8, 5), (10, 4))
    assert nprocesses_nthreads(80) in ((10, 8), (16, 5))
