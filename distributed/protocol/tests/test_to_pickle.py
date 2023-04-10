from __future__ import annotations

from distributed.protocol import dumps, loads
from distributed.protocol.serialize import ToPickle


def test_ToPickle():
    class Foo:
        def __init__(self, data):
            self.data = data

    msg = {"x": ToPickle(Foo(123))}
    frames = dumps(msg)
    out = loads(frames)
    assert out["x"].data == 123
