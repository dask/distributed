
from contextlib import contextmanager
from datetime import timedelta
import gc
import random
import sys
import weakref

import pytest
from toolz import assoc
from tornado import gen

from distributed.batched import BatchedSend
from distributed.core import listen, connect, CommClosedError
from distributed.metrics import time
from distributed.utils import All
from distributed.utils_test import gen_test, slow, gen_cluster, captured_logger
from distributed.protocol import to_serialize


class EchoServer(object):
    count = 0

    @gen.coroutine
    def handle_comm(self, comm):
        while True:
            try:
                msg = yield comm.read()
                self.count += 1
                yield comm.write(msg)
            except CommClosedError as e:
                return

    def listen(self):
        listener = listen('', self.handle_comm)
        listener.start()
        self.address = listener.contact_address
        self.stop = listener.stop


@contextmanager
def echo_server():
    server = EchoServer()
    server.listen()

    try:
        yield server
    finally:
        server.stop()


@gen_test()
def test_BatchedSend():
    with echo_server() as e:
        comm = yield connect(e.address)

        b = BatchedSend(interval=10)
        assert str(len(b.buffer)) in str(b)
        assert str(len(b.buffer)) in repr(b)
        b.start(comm)

        yield gen.sleep(0.020)

        b.send('hello')
        b.send('hello')
        b.send('world')
        yield gen.sleep(0.020)
        b.send('HELLO')
        b.send('HELLO')

        result = yield comm.read()
        assert result == ('hello', 'hello', 'world')
        result = yield comm.read()
        assert result == ('HELLO', 'HELLO')

        assert b.byte_count > 1


@gen_test()
def test_send_before_start():
    with echo_server() as e:
        comm = yield connect(e.address)

        b = BatchedSend(interval=10)

        b.send('hello')
        b.send('world')

        b.start(comm)
        result = yield comm.read()
        assert result == ('hello', 'world')


@gen_test()
def test_send_after_stream_start():
    with echo_server() as e:
        comm = yield connect(e.address)

        b = BatchedSend(interval=10)

        b.start(comm)
        b.send('hello')
        b.send('world')
        result = yield comm.read()
        if len(result) < 2:
            result += yield comm.read()
        assert result == ('hello', 'world')


@gen_test()
def test_send_before_close():
    with echo_server() as e:
        comm = yield connect(e.address)

        b = BatchedSend(interval=10)
        b.start(comm)

        cnt = int(e.count)
        b.send('hello')
        yield b.close()         # close immediately after sending
        assert not b.buffer

        start = time()
        while e.count != cnt + 1:
            yield gen.sleep(0.01)
            assert time() < start + 5

        with pytest.raises(CommClosedError):
            b.send('123')


@gen_test()
def test_close_closed():
    with echo_server() as e:
        comm = yield connect(e.address)

        b = BatchedSend(interval=10)
        b.start(comm)

        b.send(123)
        comm.close()  # external closing

        yield b.close()
        assert 'closed' in repr(b)
        assert 'closed' in str(b)


@gen_test()
def test_close_not_started():
    b = BatchedSend(interval=10)
    yield b.close()


@gen_test()
def test_close_twice():
    with echo_server() as e:
        comm = yield connect(e.address)

        b = BatchedSend(interval=10)
        b.start(comm)
        yield b.close()
        yield b.close()


@slow
@gen_test(timeout=50)
def test_stress():
    with echo_server() as e:
        comm = yield connect(e.address)
        L = []

        @gen.coroutine
        def send():
            b = BatchedSend(interval=3)
            b.start(comm)
            for i in range(0, 10000, 2):
                b.send(i)
                b.send(i + 1)
                yield gen.sleep(0.00001 * random.randint(1, 10))

        @gen.coroutine
        def recv():
            while True:
                result = yield gen.with_timeout(timedelta(seconds=1), comm.read())
                L.extend(result)
                if result[-1] == 9999:
                    break

        yield All([send(), recv()])

        assert L == list(range(0, 10000, 1))
        comm.close()


@gen.coroutine
def run_traffic_jam(nsends, nbytes):
    # This test eats `nsends * nbytes` bytes in RAM
    np = pytest.importorskip('numpy')
    from distributed.protocol import to_serialize
    data = bytes(np.random.randint(0, 255, size=(nbytes,)).astype('u1').data)
    with echo_server() as e:
        comm = yield connect(e.address)

        b = BatchedSend(interval=0.01)
        b.start(comm)

        msg = {'x': to_serialize(data)}
        for i in range(nsends):
            b.send(assoc(msg, 'i', i))
            if np.random.random() > 0.5:
                yield gen.sleep(0.001)

        results = []
        count = 0
        while len(results) < nsends:
            # If this times out then I think it's a backpressure issue
            # Somehow we're able to flood the socket so that the receiving end
            # loses some of our messages
            L = yield gen.with_timeout(timedelta(seconds=5), comm.read())
            count += 1
            results.extend(r['i'] for r in L)

        assert count == b.batch_count == e.count
        assert b.message_count == nsends

        assert results == list(range(nsends))

        comm.close()  # external closing
        yield b.close()


@gen_test()
def test_sending_traffic_jam():
    yield run_traffic_jam(50, 300000)


@slow
@gen_test()
def test_large_traffic_jam():
    yield run_traffic_jam(500, 1500000)


@pytest.mark.skipif(sys.version_info[0] < 3, reason="intermittent failure")
@gen_cluster(client=True)
def test_dont_hold_on_to_large_messages(c, s, a, b):
    np = pytest.importorskip('numpy')
    da = pytest.importorskip('dask.array')
    x = np.random.random(1000000)
    xr = weakref.ref(x)

    d = da.from_array(x, chunks=(100000,))
    d = d.persist()
    del x

    start = time()
    while xr() is not None:
        if time() > start + 5:
            # Help diagnosing
            from types import FrameType
            x = xr()
            if x is not None:
                del x
                rc = sys.getrefcount(xr())
                refs = gc.get_referrers(xr())
                print("refs to x:", rc, refs, gc.isenabled())
                frames = [r for r in refs if isinstance(r, FrameType)]
                for i, f in enumerate(frames):
                    print("frames #%d:" % i,
                          f.f_code.co_name, f.f_code.co_filename, sorted(f.f_locals))
            pytest.fail("array should have been destroyed")

        yield gen.sleep(0.05)


@gen_test()
def test_serializers():
    with echo_server() as e:
        comm = yield connect(e.address)

        b = BatchedSend(interval='10ms', serializers=['msgpack'])
        b.start(comm)

        b.send({'x': to_serialize(123)})
        b.send({'x': to_serialize('hello')})
        yield gen.sleep(0.100)

        b.send({'x': to_serialize(lambda x: x + 1)})

        with captured_logger('distributed.protocol') as sio:
            yield gen.sleep(0.100)

        value = sio.getvalue()
        assert 'serialize' in value
        assert 'type' in value
        assert 'function' in value

        msg = yield comm.read()
        assert list(msg) == [{'x': 123}, {'x': 'hello'}]

        with pytest.raises(gen.TimeoutError):
            msg = yield gen.with_timeout(timedelta(milliseconds=100), comm.read())
