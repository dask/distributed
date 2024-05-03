from __future__ import annotations

import logging
import os
from collections import defaultdict
from collections.abc import (
    Callable,
    Collection,
    Generator,
    Hashable,
    Iterable,
    Iterator,
    Sequence,
)
from dataclasses import dataclass
from pickle import PickleBuffer
from typing import TYPE_CHECKING, Any

from tornado.ioloop import IOLoop

import dask
from dask.base import tokenize
from dask.highlevelgraph import HighLevelGraph
from dask.layers import Layer
from dask.typing import Key

from distributed.core import PooledRPCCall
from distributed.exceptions import Reschedule
from distributed.metrics import context_meter
from distributed.protocol.utils import pack_frames_prelude
from distributed.shuffle._core import (
    NDIndex,
    ShuffleId,
    ShuffleRun,
    ShuffleSpec,
    barrier_key,
    get_worker_plugin,
    handle_transfer_errors,
    handle_unpack_errors,
)
from distributed.shuffle._exceptions import DataUnavailable
from distributed.shuffle._limiter import ResourceLimiter
from distributed.shuffle._pickle import (
    pickle_dataframe_shard,
    unpickle_and_concat_dataframe_shards,
)
from distributed.shuffle._worker_plugin import ShuffleWorkerPlugin
from distributed.utils import nbytes

logger = logging.getLogger("distributed.shuffle")
if TYPE_CHECKING:
    import pandas as pd

    # TODO import from typing (requires Python >=3.10)
    from typing_extensions import TypeAlias

    from dask.dataframe import DataFrame


def shuffle_transfer(
    input: pd.DataFrame,
    id: ShuffleId,
    input_partition: int,
    npartitions: int,
    column: str,
    meta: pd.DataFrame,
    parts_out: set[int],
    disk: bool,
    drop_column: bool,
) -> int:
    with handle_transfer_errors(id):
        return get_worker_plugin().add_partition(
            input,
            input_partition,
            spec=DataFrameShuffleSpec(
                id=id,
                npartitions=npartitions,
                column=column,
                meta=meta,
                parts_out=parts_out,
                disk=disk,
                drop_column=drop_column,
            ),
        )


def shuffle_unpack(
    id: ShuffleId, output_partition: int, barrier_run_id: int
) -> pd.DataFrame:
    with handle_unpack_errors(id):
        return get_worker_plugin().get_output_partition(
            id, barrier_run_id, output_partition
        )


def shuffle_barrier(id: ShuffleId, run_ids: list[int]) -> int:
    try:
        return get_worker_plugin().barrier(id, run_ids)
    except Reschedule as e:
        raise e
    except Exception as e:
        raise RuntimeError(f"shuffle_barrier failed during shuffle {id}") from e


def rearrange_by_column_p2p(
    df: DataFrame,
    column: str,
    npartitions: int | None = None,
) -> DataFrame:
    import pandas as pd

    from dask.dataframe.core import new_dd_object

    meta = df._meta
    if not pd.api.types.is_integer_dtype(meta[column].dtype):
        raise TypeError(
            f"Expected meta {column=} to be an integer column, is {meta[column].dtype}."
        )
    npartitions = npartitions or df.npartitions
    token = tokenize(df, column, npartitions)

    if any(not isinstance(c, str) for c in meta.columns):
        unsupported = {c: type(c) for c in meta.columns if not isinstance(c, str)}
        raise TypeError(
            f"p2p requires all column names to be str, found: {unsupported}",
        )

    name = f"shuffle_p2p-{token}"
    disk: bool = dask.config.get("distributed.p2p.disk")

    layer = P2PShuffleLayer(
        name,
        column,
        npartitions,
        npartitions_input=df.npartitions,
        name_input=df._name,
        meta_input=meta,
        disk=disk,
    )
    return new_dd_object(
        HighLevelGraph.from_collections(name, layer, [df]),
        name,
        meta,
        [None] * (npartitions + 1),
    )


_T_LowLevelGraph: TypeAlias = dict[Key, tuple]


class P2PShuffleLayer(Layer):
    name: str
    column: str
    npartitions: int
    npartitions_input: int
    name_input: str
    meta_input: pd.DataFrame
    disk: bool
    parts_out: set[int]
    drop_column: bool

    def __init__(
        self,
        name: str,
        column: str,
        npartitions: int,
        npartitions_input: int,
        name_input: str,
        meta_input: pd.DataFrame,
        disk: bool,
        parts_out: Iterable[int] | None = None,
        annotations: dict | None = None,
        drop_column: bool = False,
    ):
        self.name = name
        self.column = column
        self.npartitions = npartitions
        self.name_input = name_input
        self.meta_input = meta_input
        self.disk = disk
        if parts_out:
            self.parts_out = set(parts_out)
        else:
            self.parts_out = set(range(self.npartitions))
        self.npartitions_input = npartitions_input
        self.drop_column = drop_column
        super().__init__(annotations=annotations)

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}<name='{self.name}', npartitions={self.npartitions}>"
        )

    def get_output_keys(self) -> set[Key]:
        return {(self.name, part) for part in self.parts_out}

    def is_materialized(self) -> bool:
        return hasattr(self, "_cached_dict")

    @property
    def _dict(self) -> _T_LowLevelGraph:
        """Materialize full dict representation"""
        self._cached_dict: _T_LowLevelGraph
        dsk: _T_LowLevelGraph
        if hasattr(self, "_cached_dict"):
            return self._cached_dict
        else:
            dsk = self._construct_graph()
            self._cached_dict = dsk
        return self._cached_dict

    def __getitem__(self, key: Key) -> tuple:
        return self._dict[key]

    def __iter__(self) -> Iterator[Key]:
        return iter(self._dict)

    def __len__(self) -> int:
        return len(self._dict)

    def _cull(self, parts_out: Iterable[int]) -> P2PShuffleLayer:
        return P2PShuffleLayer(
            self.name,
            self.column,
            self.npartitions,
            self.npartitions_input,
            self.name_input,
            self.meta_input,
            self.disk,
            parts_out=parts_out,
        )

    def _keys_to_parts(self, keys: Iterable[Key]) -> set[int]:
        """Simple utility to convert keys to partition indices."""
        parts = set()
        for key in keys:
            if isinstance(key, tuple) and len(key) == 2:
                name, part = key
                if name == self.name:
                    assert isinstance(part, int)
                    parts.add(part)
        return parts

    def cull(
        self, keys: set[Key], all_keys: Collection[Key]
    ) -> tuple[P2PShuffleLayer, dict]:
        """Cull a P2PShuffleLayer HighLevelGraph layer.

        The underlying graph will only include the necessary
        tasks to produce the keys (indices) included in `parts_out`.
        Therefore, "culling" the layer only requires us to reset this
        parameter.
        """
        parts_out = self._keys_to_parts(keys)
        # Protect against mutations later on with frozenset
        input_parts = frozenset(
            {(self.name_input, i) for i in range(self.npartitions_input)}
        )
        culled_deps = {(self.name, part): input_parts for part in parts_out}

        if parts_out != set(self.parts_out):
            culled_layer = self._cull(parts_out)
            return culled_layer, culled_deps
        else:
            return self, culled_deps

    def _construct_graph(self) -> _T_LowLevelGraph:
        token = tokenize(self.name_input, self.column, self.npartitions, self.parts_out)
        dsk: _T_LowLevelGraph = {}
        _barrier_key = barrier_key(ShuffleId(token))
        name = "shuffle-transfer-" + token
        transfer_keys = list()
        for i in range(self.npartitions_input):
            transfer_keys.append((name, i))
            dsk[(name, i)] = (
                shuffle_transfer,
                (self.name_input, i),
                token,
                i,
                self.npartitions,
                self.column,
                self.meta_input,
                self.parts_out,
                self.disk,
                self.drop_column,
            )

        dsk[_barrier_key] = (shuffle_barrier, token, transfer_keys)

        name = self.name
        for part_out in self.parts_out:
            dsk[(name, part_out)] = (
                shuffle_unpack,
                token,
                part_out,
                _barrier_key,
            )
        return dsk


def split_by_worker(
    df: pd.DataFrame,
    column: str,
    drop_column: bool,
    worker_for: dict[int, str],
    input_part_id: int,
) -> dict[str, tuple[int, list[tuple[int, list[PickleBuffer]]]]]:
    """Split data into many horizontal slices, partitioned by destination worker,
    and serialize them once.

    Returns
    -------
    {worker addr: (input_part_id, [(output_part_id, buffers), ...]), ...}

    where buffers is the serialized output (pickle bytes, buffer, buffer, ...) of
    (input_part_id, index, *blocks)

    **Notes**

    - The pickle header, which is a bytes object, is wrapped in PickleBuffer so
      that it's not unnecessarily deep-copied when it's deserialized by the network
      stack.
    - We are not delegating serialization to the network stack because (1) it's quicker
      with plain pickle and (2) we want to avoid deserializing everything on receive()
      only to re-serialize it again immediately afterwards when writing it to disk.
      So we serialize it once now and deserialize it once after reading back from disk.

    See Also
    --------
    distributed.protocol.serialize._deserialize_bytes
    distributed.protocol.serialize._deserialize_picklebuffer
    """
    out: defaultdict[str, list[tuple[int, list[PickleBuffer]]]] = defaultdict(list)

    for output_part_id, part in df.groupby(column, observed=False):
        assert isinstance(output_part_id, int)
        if drop_column:
            del part[column]
        frames = pickle_dataframe_shard(input_part_id, part)
        out[worker_for[output_part_id]].append((output_part_id, frames))

    return {k: (input_part_id, v) for k, v in out.items()}


class DataFrameShuffleRun(ShuffleRun[int, "pd.DataFrame"]):
    """State for a single active shuffle execution

    This object is responsible for splitting, sending, receiving and combining
    data shards.

    It is entirely agnostic to the distributed system and can perform a shuffle
    with other run instances using `rpc`.

    The user of this needs to guarantee that only `DataFrameShuffleRun`s of the
    same unique `ShuffleID` and `run_id` interact.

    Parameters
    ----------
    worker_for:
        A mapping partition_id -> worker_address.
    column:
        The data column we split the input partition by.
    id:
        A unique `ShuffleID` this belongs to.
    run_id:
        A unique identifier of the specific execution of the shuffle this belongs to.
    span_id:
        Span identifier; see :doc:`spans`
    local_address:
        The local address this Shuffle can be contacted by using `rpc`.
    directory:
        The scratch directory to buffer data in.
    rpc:
        A callable returning a PooledRPCCall to contact other Shuffle instances.
        Typically a ConnectionPool.
    digest_metric:
        A callable to ingest a performance metric.
        Typically Server.digest_metric.
    scheduler:
        A PooledRPCCall to contact the scheduler.
    memory_limiter_disk:
    memory_limiter_comm:
        A ``ResourceLimiter`` limiting the total amount of memory used in either
        buffer.
    """

    column: str
    meta: pd.DataFrame
    partitions_of: dict[str, list[int]]
    worker_for: dict[int, str]
    drop_column: bool

    def __init__(
        self,
        worker_for: dict[int, str],
        column: str,
        meta: pd.DataFrame,
        id: ShuffleId,
        run_id: int,
        span_id: str | None,
        local_address: str,
        directory: str,
        rpc: Callable[[str], PooledRPCCall],
        digest_metric: Callable[[Hashable, float], None],
        scheduler: PooledRPCCall,
        memory_limiter_disk: ResourceLimiter,
        memory_limiter_comms: ResourceLimiter,
        disk: bool,
        drop_column: bool,
        loop: IOLoop,
    ):
        super().__init__(
            id=id,
            run_id=run_id,
            span_id=span_id,
            local_address=local_address,
            directory=directory,
            rpc=rpc,
            digest_metric=digest_metric,
            scheduler=scheduler,
            memory_limiter_comms=memory_limiter_comms,
            memory_limiter_disk=memory_limiter_disk,
            disk=disk,
            loop=loop,
        )
        self.column = column
        self.meta = meta
        partitions_of = defaultdict(list)
        for part, addr in worker_for.items():
            partitions_of[addr].append(part)
        self.partitions_of = dict(partitions_of)
        self.worker_for = worker_for
        self.drop_column = drop_column

    async def _receive(
        # See split_by_worker to understand annotation of data.
        # PickleBuffer objects may have been converted to bytearray by the
        # pickle roundtrip that is done by _core.py when buffers are too small
        self,
        data: Iterable[list[PickleBuffer | bytes | bytearray]],
    ) -> None:
        self.raise_if_closed()

        to_write: defaultdict[
            NDIndex, list[bytes | bytearray | memoryview]
        ] = defaultdict(list)

        for output_partition_id, frames in data:
            frames_raw = [
                frame.raw() if isinstance(frame, PickleBuffer) else frame
                for frame in frames
            ]
            self.total_recvd += sum(map(nbytes, frames_raw))
            to_write[output_partition_id,] += [
                pack_frames_prelude(frames_raw),
                *frames_raw,
            ]

        try:
            await self._write_to_disk(to_write)
        except Exception as e:
            self._exception = e
            raise

    def _shard_partition(
        self,
        data: pd.DataFrame,
        partition_id: int,
        # See split_by_worker to understand annotation
    ) -> dict[str, tuple[int, list[tuple[int, list[PickleBuffer]]]]]:
        out = split_by_worker(
            df=data,
            column=self.column,
            drop_column=self.drop_column,
            worker_for=self.worker_for,
            input_part_id=partition_id,
        )

        # Log metrics
        # Note: more metrics for this function call are logged by _core.add_partitiion()
        overhead_nbytes = 0
        buffers_nbytes = 0
        shards_count = 0
        buffers_count = 0
        for _, shards in out.values():
            shards_count += len(shards)
            for _, frames in shards:
                # frames = [pickle bytes, buffer, buffer, ...]
                buffers_count += len(frames) - 2
                overhead_nbytes += frames[0].raw().nbytes
                buffers_nbytes += sum(frame.raw().nbytes for frame in frames[1:])

        context_meter.digest_metric("p2p-shards-overhead", overhead_nbytes, "bytes")
        context_meter.digest_metric("p2p-shards-buffers", buffers_nbytes, "bytes")
        context_meter.digest_metric("p2p-shards-buffers", buffers_count, "count")
        context_meter.digest_metric("p2p-shards", shards_count, "count")
        # End log metrics

        return out

    def _get_output_partition(
        self,
        partition_id: int,
        key: Key,
        **kwargs: Any,
    ) -> pd.DataFrame:
        meta = self.meta.copy()
        if self.drop_column:
            meta = self.meta.drop(columns=self.column)

        try:
            buffer = self._read_from_disk((partition_id,))
        except DataUnavailable:
            return meta

        return unpickle_and_concat_dataframe_shards(buffer, meta)

    def _get_assigned_worker(self, id: int) -> str:
        return self.worker_for[id]


@dataclass(frozen=True)
class DataFrameShuffleSpec(ShuffleSpec[int]):
    npartitions: int
    column: str
    meta: pd.DataFrame
    parts_out: set[int]
    drop_column: bool

    @property
    def output_partitions(self) -> Generator[int, None, None]:
        yield from self.parts_out

    def pick_worker(self, partition: int, workers: Sequence[str]) -> str:
        return _get_worker_for_range_sharding(self.npartitions, partition, workers)

    def validate_data(self, data: pd.DataFrame) -> None:
        if set(data.columns) != set(self.meta.columns):
            raise ValueError(f"Expected {self.meta.columns=} to match {data.columns=}.")

    def create_run_on_worker(
        self,
        run_id: int,
        span_id: str | None,
        worker_for: dict[int, str],
        plugin: ShuffleWorkerPlugin,
    ) -> ShuffleRun:
        return DataFrameShuffleRun(
            column=self.column,
            meta=self.meta,
            worker_for=worker_for,
            id=self.id,
            run_id=run_id,
            span_id=span_id,
            directory=os.path.join(
                plugin.worker.local_directory,
                f"shuffle-{self.id}-{run_id}",
            ),
            local_address=plugin.worker.address,
            rpc=plugin.worker.rpc,
            digest_metric=plugin.worker.digest_metric,
            scheduler=plugin.worker.scheduler,
            memory_limiter_disk=plugin.memory_limiter_disk
            if self.disk
            else ResourceLimiter(None),
            memory_limiter_comms=plugin.memory_limiter_comms,
            disk=self.disk,
            drop_column=self.drop_column,
            loop=plugin.worker.loop,
        )


def _get_worker_for_range_sharding(
    npartitions: int, output_partition: int, workers: Sequence[str]
) -> str:
    """Get address of target worker for this output partition using range sharding"""
    i = len(workers) * output_partition // npartitions
    return workers[i]
