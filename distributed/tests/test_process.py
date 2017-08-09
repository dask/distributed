from distributed.process import AsyncProcess
from distributed.utils import mp_context
from distributed.utils_test import gen_test, process_state
from distributed.metrics import time
from time import sleep

from tornado import gen

proc = mp_context.Process(target=sleep, args=(0,))
proc.start()  # fire off process to spin up file descriptors

@gen_test()
def test_basic():
    L = [False]
    def f(future):
        L[0] = True

    a = process_state()
    process = AsyncProcess(target=sleep, args=(0.1,))
    process.daemon = True
    process.set_exit_callback(f)

    start = time()
    b = process_state()
    yield process.start()
    c = process_state()
    yield process.join()
    d = process_state()
    end = time()

    process.close()
    e = process_state()

    assert end - start < 2

    assert L[0] is True
