import dask

from distributed.protocol.utils import frame_split_size, pack_frames, unpack_frames


def test_pack_frames():
    frames = [b"123", b"asdf"]
    b = pack_frames(frames)
    assert isinstance(b, bytes)
    frames2 = unpack_frames(b)

    assert frames == frames2


def test_frame_split_is_configurable():
    frame = b"1234abcd" * (2 ** 20)  # 8 MiB
    with dask.config.set({"distributed.comm.shard": "3MiB"}):
        split_frames = frame_split_size(frame)
        assert len(split_frames) == 3
    with dask.config.set({"distributed.comm.shard": "5MiB"}):
        split_frames = frame_split_size(frame)
        assert len(split_frames) == 2
