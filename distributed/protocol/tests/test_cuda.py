import pytest


cupy = pytest.importorskip("cupy")
from distributed.protocol import serialize, deserialize


def test_serialize():
    x = cupy.ones((5000, 50))
    header, frames = serialize(x)
    type_ = 'cupy.core.core.ndarray'
    _, [type_serialized] = serialize(type_)

    expected_header = {
        'shape': (5000, 50),
        'typestr': "<f8",
        'descr': [('', '<f8')],
        'data': (x.data.ptr, False),
        'version': 0,
        'type': type_,
        # 'type-serialized': type_serialized,
        'serializer': 'dask',
        'device': x.device.id,
    }
    type_ser = header.pop('type-serialized')  # TODO

    assert header == expected_header
    header['type-serialized'] = type_ser
    assert len(frames) == 1
    frame, = frames

    result = deserialize(header, frames)
    cupy.testing.assert_array_equal(x, result)
