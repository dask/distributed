from distributed.protocol.utils import merge_frames

def test_merge_frames():
    result = merge_frames({'lengths': [3, 4]}, [b'12', b'34', b'567'])
    expected = [b'123', b'4567']

    assert list(map(bytes, result)) == expected

    b = b'123'
    assert merge_frames({'lengths': [3]}, [b])[0] is b
