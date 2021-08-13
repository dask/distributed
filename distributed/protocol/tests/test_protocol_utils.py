from __future__ import annotations

import pytest

from distributed.protocol.utils import merge_memoryviews, pack_frames, unpack_frames


def test_pack_frames():
    frames = [b"123", b"asdf"]
    b = pack_frames(frames)
    assert isinstance(b, bytes)
    frames2 = unpack_frames(b)

    assert frames == frames2


class TestMergeMemroyviews:
    def test_empty(self):
        empty = merge_memoryviews([])
        assert isinstance(empty, memoryview) and len(empty) == 0

    def test_one(self):
        base = bytearray(range(10))
        base_mv = memoryview(base)
        assert merge_memoryviews([base_mv]) is base_mv

    @pytest.mark.parametrize(
        "slices",
        [
            [slice(None, 3), slice(3, None)],
            [slice(1, 3), slice(3, None)],
            [slice(1, 3), slice(3, -1)],
            [slice(0, 0), slice(None)],
            [slice(None), slice(-1, -1)],
            [slice(0, 0), slice(0, 0)],
            [slice(None, 3), slice(3, 7), slice(7, None)],
            [slice(2, 3), slice(3, 7), slice(7, 9)],
            [slice(2, 3), slice(3, 7), slice(7, 9), slice(9, 9)],
            [slice(1, 2), slice(2, 5), slice(5, 8), slice(8, None)],
        ],
    )
    def test_parts(self, slices: list[slice]):
        base = bytearray(range(10))
        base_mv = memoryview(base)

        equiv_start = min(s.indices(10)[0] for s in slices)
        equiv_stop = max(s.indices(10)[1] for s in slices)
        equiv = base_mv[equiv_start:equiv_stop]

        parts = [base_mv[s] for s in slices]
        result = merge_memoryviews(parts)
        assert result.obj is base
        assert len(result) == len(equiv)
        assert result == equiv

    def test_readonly_buffer(self):
        base = bytes(range(10))
        base_mv = memoryview(base)

        result = merge_memoryviews([base_mv[:4], base_mv[4:]])
        assert result.obj is base
        assert len(result) == len(base)
        assert result == base

    @pytest.mark.parametrize(
        "slices",
        [
            [slice(None, 3), slice(4, None)],
            [slice(None, 3), slice(2, None)],
            [slice(1, 3), slice(3, 6), slice(9, None)],
        ],
    )
    def test_catch_gaps(self, slices: list[slice]):
        base = bytearray(range(10))
        base_mv = memoryview(base)

        parts = [base_mv[s] for s in slices]
        with pytest.raises(ValueError, match="does not start where the previous ends"):
            merge_memoryviews(parts)

    def test_catch_different_buffer(self):
        base = bytearray(range(8))
        base_mv = memoryview(base)
        with pytest.raises(ValueError, match="different buffer"):
            merge_memoryviews([base_mv, memoryview(base.copy())])

    def test_catch_different_non_contiguous(self):
        base = bytearray(range(8))
        base_mv = memoryview(base)[::-1]
        with pytest.raises(ValueError, match="non-contiguous"):
            merge_memoryviews([base_mv[:3], base_mv[3:]])

    def test_catch_multidimensional(self):
        base = bytearray(range(6))
        base_mv = memoryview(base).cast("B", [3, 2])
        with pytest.raises(ValueError, match="has 2 dimensions, not 1"):
            merge_memoryviews([base_mv[:1], base_mv[1:]])

    def test_catch_different_formats(self):
        base = bytearray(range(8))
        base_mv = memoryview(base)
        with pytest.raises(ValueError, match="inconsistent format: I vs B"):
            merge_memoryviews([base_mv[:4], base_mv[4:].cast("I")])
