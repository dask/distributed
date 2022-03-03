import threading
from time import sleep

from distributed.metrics import time
from distributed.threadpoolexecutor import ThreadPoolExecutor, rejoin, secede


def test_tpe():
    with ThreadPoolExecutor(2) as e:
        list(e.map(sleep, [0.01] * 4))

        threads = e._threads.copy()
        assert len(threads) == 2

        def f():
            secede()
            return 1

        assert e.submit(f).result() == 1

        list(e.map(sleep, [0.01] * 4))
        assert len(threads | e._threads) == 3

        start = time()
        while all(t.is_alive() for t in threads):
            sleep(0.01)
            assert time() < start + 1


def test_shutdown_wait():
    e = ThreadPoolExecutor(1)
    future = e.submit(sleep, 1)
    sleep(0.01)

    start = time()
    e.shutdown(wait=False)
    end = time()
    assert end - start < 1


def test_secede_rejoin_busy():
    with ThreadPoolExecutor(2) as e:
        # Prime threads
        f1 = e.submit(sleep, 0.1)
        f2 = e.submit(sleep, 0.1)
        f1.result()
        f2.result()

        def f():
            assert threading.current_thread() in e._threads
            secede()
            sleep(0.2)
            assert threading.current_thread() not in e._threads
            rejoin()
            assert len(e._threads) == 2
            assert threading.current_thread() in e._threads
            return threading.current_thread()

        future = e.submit(f)
        for _ in range(6):
            e.submit(sleep, 0.4)
        start = time()
        special_thread = future.result()
        stop = time()

        assert 0.2 < stop - start < 0.6

        assert len(e._threads) == 2
        assert special_thread in e._threads

        def f():
            sleep(0.01)
            return threading.current_thread()

        futures = [e.submit(f) for _ in range(10)]
        assert special_thread in {future.result() for future in futures}


def test_secede_rejoin_quiet():
    with ThreadPoolExecutor(2) as e:

        def f():
            assert threading.current_thread() in e._threads
            secede()
            sleep(0.1)
            assert threading.current_thread() not in e._threads
            rejoin()
            assert len(e._threads) == 2
            assert threading.current_thread() in e._threads
            return threading.current_thread()

        future = e.submit(f)
        result = future.result()


def test_rejoin_idempotent():
    with ThreadPoolExecutor(2) as e:

        def f():
            secede()
            for i in range(5):
                rejoin()
            return 1

        future = e.submit(f)
        result = future.result()


def test_thread_name():
    with ThreadPoolExecutor(2) as e:
        e.map(id, range(10))
        assert len({thread.name for thread in e._threads}) == 2


def test_rejoin_joins_pool():
    with ThreadPoolExecutor(1) as e:

        def f():
            ident = threading.get_ident()
            secede()
            assert ident not in {th.ident for th in e._threads}
            rejoin()
            return ident

        ident = e.submit(f).result()
        assert ident in {th.ident for th in e._threads}

        def g():
            assert ident == threading.get_ident()

        e.submit(g).result()
