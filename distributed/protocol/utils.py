BIG_BYTES_SHARD_SIZE = 2**28


def frame_split_size(frames, n=BIG_BYTES_SHARD_SIZE):
    """
    Split a list of frames into a list of frames of maximum size

    This helps us to avoid passing around very large bytestrings.

    Examples
    --------
    >>> frame_split_size([b'12345', b'678'], n=3)  # doctest: +SKIP
    [b'123', b'45', b'678']
    """
    if max(map(len, frames)) <= n:
        return frames

    out = []
    for frame in frames:
        if len(frame) > n:
            if isinstance(frame, bytes):
                frame = memoryview(frame)
            for i in range(0, len(frame), n):
                out.append(frame[i: i + n])
        else:
            out.append(frame)
    return out

