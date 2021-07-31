import pytest

from distributed.protocol.utils import copy_frames, pack_frames, unpack_frames


def test_pack_frames():
    frames = [b"123", b"asdf"]
    b = pack_frames(frames)
    assert isinstance(b, bytes)
    prelude_size, frames2 = unpack_frames(b)

    assert frames == frames2
    assert prelude_size == len(b) - sum(len(x) for x in frames)


@pytest.mark.parametrize(
    "frames",
    [
        [],
        [b"123"],
        [b"123", b"asdf"],
        [memoryview(b"abcd")[1:], b"x", memoryview(b"12345678")],
    ],
)
def test_copy_frames(frames):
    new = copy_frames(frames)
    assert [bytes(f) for f in new] == [bytes(x) for x in frames]
    assert all(isinstance(f, memoryview) for f in new)
    if frames:
        new_buffers = set(f.obj for f in new)
        assert len(new_buffers) == 1
        if len(frames) != 1 and not isinstance(frames[0], bytes):
            # `b"".join([b"123"])` is zero-copy. We are okay allowing this optimization.
            assert not new_buffers.intersection(
                f.obj if isinstance(f, memoryview) else f for f in frames
            )
