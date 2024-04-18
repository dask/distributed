from __future__ import annotations

import pickle
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from toolz import first

from distributed.protocol.utils import pack_frames_prelude, unpack_frames

if TYPE_CHECKING:
    import pandas as pd


def pickle_bytelist(obj: object, prelude: bool = True) -> list[pickle.PickleBuffer]:
    """Variant of :func:`serialize_bytelist`, that doesn't support compression, locally
    defined classes, or any of its other fancy features but runs 10x faster for numpy
    arrays

    See Also
    --------
    serialize_bytelist
    unpickle_bytestream
    """
    frames: list = []
    pik = pickle.dumps(obj, protocol=5, buffer_callback=frames.append)
    frames.insert(0, pickle.PickleBuffer(pik))
    if prelude:
        frames.insert(0, pickle.PickleBuffer(pack_frames_prelude(frames)))
    return frames


def unpickle_bytestream(b: bytes | bytearray | memoryview) -> Iterator[Any]:
    """Unpickle the concatenated output of multiple calls to :func:`pickle_bytelist`

    See Also
    --------
    pickle_bytelist
    deserialize_bytes
    """
    while True:
        pik, *buffers, remainder = unpack_frames(b, remainder=True)
        yield pickle.loads(pik, buffers=buffers)
        if remainder.nbytes == 0:
            break
        b = remainder


def pickle_dataframe_shard(
    input_part_id: int,
    shard: pd.DataFrame,
) -> list[pickle.PickleBuffer]:
    """Optimized pickler for pandas Dataframes. DIscard all unnecessary metadata
    (like the columns header).

    Parameters:
        obj: pandas
    """
    return pickle_bytelist(
        (input_part_id, shard.index, *shard._mgr.blocks), prelude=False
    )


def unpickle_and_concat_dataframe_shards(
    b: bytes | bytearray | memoryview, meta: pd.DataFrame
) -> pd.DataFrame:
    """Optimized unpickler for pandas Dataframes.

    Parameters
    ----------
    b:
        raw buffer, containing the concatenation of the outputs of
        :func:`pickle_dataframe_shard`, in arbitrary order
    meta:
        DataFrame header

    Returns
    -------
    Reconstructed output shard, sorted by input partition ID

    **Roundtrip example**

    >>> import random
    >>> import pandas as pd
    >>> from toolz import concat

    >>> df = pd.DataFrame(...)  # Input partition
    >>> meta = df.iloc[:0].copy()
    >>> shards = df.iloc[0:10], df.iloc[10:20], ...
    >>> frames = [pickle_dataframe_shard(i, shard) for i, shard in enumerate(shards)]
    >>> random.shuffle(frames)  # Simulate the frames arriving in arbitrary order
    >>> blob = bytearray(b"".join(concat(frames)))  # Simulate disk roundtrip
    >>> df2 = unpickle_and_concat_dataframe_shards(blob, meta)
    """
    import pandas as pd
    from pandas.core.internals import BlockManager, make_block

    parts = list(unpickle_bytestream(b))
    # [(input_part_id, index, *blocks), ...]
    parts.sort(key=first)
    shards = []
    for _, idx, *blocks in parts:
        blocks = [
            blk
            if not (
                isinstance(blk.dtype, pd.StringDtype) and blk.dtype.storage == "pyarrow"
            )
            else make_block(
                pd.arrays.ArrowStringArray(blk.values._pa_array.combine_chunks()),
                blk.mgr_locs,
            )
            for blk in blocks
        ]
        axes = [meta.columns, idx]
        df = pd.DataFrame._from_mgr(  # type: ignore[attr-defined]
            BlockManager(blocks, axes, verify_integrity=False), axes
        )
        shards.append(df)

    # Actually load memory-mapped buffers into memory and close the file
    # descriptors
    return pd.concat(shards, copy=True)
