from distributed.protocol.utils import pack_frames, unpack_frames


def test_pack_frames():
    frames = [b"123", b"asdf"]
    b = pack_frames(frames)
    assert isinstance(b, bytes)
    prelude_size, frames2 = unpack_frames(b)

    assert frames == frames2
    assert prelude_size == len(b) - sum(len(x) for x in frames)
