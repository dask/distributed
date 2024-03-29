from __future__ import annotations

import contextlib
import mmap
import pathlib
import shutil
import threading
from collections.abc import Generator, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from distributed.metrics import context_meter, thread_time
from distributed.shuffle._buffer import ShardsBuffer
from distributed.shuffle._exceptions import DataUnavailable
from distributed.shuffle._limiter import ResourceLimiter
from distributed.shuffle._pickle import pickle_bytelist, unpickle_bytestream
from distributed.utils import Deadline, log_errors, nbytes


class ReadWriteLock:
    _condition: threading.Condition
    _n_reads: int
    _write_pending: bool

    def __init__(self) -> None:
        self._condition = threading.Condition(threading.Lock())
        self._n_reads = 0
        self._write_pending = False
        self._write_active = False

    def acquire_write(self, timeout: float = -1) -> bool:
        deadline = Deadline.after(timeout if timeout >= 0 else None)
        with self._condition:
            result = self._condition.wait_for(
                lambda: not self._write_pending, timeout=deadline.remaining
            )
            if result is False:
                return False

            self._write_pending = True
            result = self._condition.wait_for(
                lambda: self._n_reads == 0, timeout=deadline.remaining
            )

            if result is False:
                self._write_pending = False
                self._condition.notify_all()
                return False
            self._write_active = True
            return True

    def release_write(self) -> None:
        with self._condition:
            if self._write_active is False:
                raise RuntimeError("Tried releasing unlocked write lock")
            self._write_pending = False
            self._write_active = False
            self._condition.notify_all()

    def acquire_read(self, timeout: float = -1) -> bool:
        deadline = Deadline.after(timeout if timeout >= 0 else None)
        with self._condition:
            result = self._condition.wait_for(
                lambda: not self._write_pending, timeout=deadline.remaining
            )
            if result is False:
                return False
            self._n_reads += 1
            return True

    def release_read(self) -> None:
        with self._condition:
            if self._n_reads == 0:
                raise RuntimeError("Tired releasing unlocked read lock")
            self._n_reads -= 1
            if self._n_reads == 0:
                self._condition.notify_all()

    @contextmanager
    def write(self) -> Generator[None, None, None]:
        self.acquire_write()
        try:
            yield
        finally:
            self.release_write()

    @contextmanager
    def read(self) -> Generator[None, None, None]:
        self.acquire_read()
        try:
            yield
        finally:
            self.release_read()


class DiskShardsBuffer(ShardsBuffer):
    """Accept, buffer, and write many small objects to many files

    This takes in lots of small objects, writes them to a local directory, and
    then reads them back when all writes are complete.  It buffers these
    objects in memory so that it can optimize disk access for larger writes.

    **State**

    -   shards: dict[str, list[bytes]]

        This is our in-memory buffer of data waiting to be written to files.

    -   sizes: dict[str, int]

        The size of each list of shards.  We find the largest and write data from that buffer

    Parameters
    ----------
    directory : str or pathlib.Path
        Where to write and read data.  Ideally points to fast disk.
    memory_limiter : ResourceLimiter
        Limiter for in-memory buffering (at most this much data)
        before writes to disk occur. If the incoming data that has yet
        to be processed exceeds this limit, then the buffer will block
        until below the threshold. See :meth:`.write` for the
        implementation of this scheme.
    """

    def __init__(
        self,
        directory: str | pathlib.Path,
        memory_limiter: ResourceLimiter,
    ):
        super().__init__(
            memory_limiter=memory_limiter,
            # Disk is not able to run concurrently atm
            concurrency_limit=1,
        )
        self.directory = pathlib.Path(directory)
        self.directory.mkdir(exist_ok=True)
        self._closed = False
        self._directory_lock = ReadWriteLock()

    @log_errors
    async def _process(self, id: str, shards: list[Any]) -> None:
        """Write one buffer to file

        This function was built to offload the disk IO, but since then we've
        decided to keep this within the event loop (disk bandwidth should be
        prioritized, and writes are typically small enough to not be a big
        deal).

        Most of the logic here is about possibly going back to a separate
        thread, or about diagnostics.  If things don't change much in the
        future then we should consider simplifying this considerably and
        dropping the write into communicate above.
        """
        nbytes_acc = 0

        def pickle_and_tally() -> Iterator[bytes | bytearray | memoryview]:
            nonlocal nbytes_acc
            for shard in shards:
                if isinstance(shard, list) and isinstance(
                    shard[0], (bytes, bytearray, memoryview)
                ):
                    # list[bytes | bytearray | memoryview] for dataframe shuffle
                    # Shard was pre-serialized before being sent over the network.
                    nbytes_acc += sum(map(nbytes, shard))
                    yield from shard
                else:
                    # tuple[NDIndex, ndarray] for array rechunk
                    frames = [s.raw() for s in pickle_bytelist(shard)]
                    nbytes_acc += sum(frame.nbytes for frame in frames)
                    yield from frames

        with (
            self._directory_lock.read(),
            context_meter.meter("disk-write"),
            context_meter.meter("serialize", func=thread_time),
        ):
            if self._closed:
                raise RuntimeError("Already closed")

            with open(self.directory / str(id), mode="ab") as f:
                f.writelines(pickle_and_tally())

        context_meter.digest_metric("disk-write", 1, "count")
        context_meter.digest_metric("disk-write", nbytes_acc, "bytes")

    def read(self, id: str) -> list[Any]:
        """Read a complete file back into memory"""
        self.raise_on_exception()
        if not self._inputs_done:
            raise RuntimeError("Tried to read from file before done.")

        try:
            with self._directory_lock.read():
                if self._closed:
                    raise RuntimeError("Already closed")
                fname = (self.directory / str(id)).resolve()
                # Note: don't add `with context_meter.meter("p2p-disk-read"):` to
                # measure seconds here, as it would shadow "p2p-get-output-cpu" and
                # "p2p-get-output-noncpu". Also, for rechunk it would not measure
                # the whole disk access, as _read returns memory-mapped buffers.
                data, size = self._read(fname)
                context_meter.digest_metric("p2p-disk-read", 1, "count")
                context_meter.digest_metric("p2p-disk-read", size, "bytes")
        except FileNotFoundError:
            raise DataUnavailable(id)

        if data:
            self.bytes_read += size
            return data
        else:
            raise DataUnavailable(id)

    @staticmethod
    def _read(path: Path) -> tuple[list[Any], int]:
        """Open a memory-mapped file descriptor to disk, read all metadata, and unpickle
        all arrays. This is a fast sequence of short reads interleaved with seeks.
        Do not read in memory the actual data; the arrays' buffers will point to the
        memory-mapped area.

        The file descriptor will be automatically closed by the kernel when all the
        returned arrays are dereferenced, which will happen after the call to
        concatenate3.
        """
        with path.open(mode="r+b") as fh:
            buffer = memoryview(mmap.mmap(fh.fileno(), 0))

        # The file descriptor has *not* been closed!
        shards = list(unpickle_bytestream(buffer))
        return shards, buffer.nbytes

    async def close(self) -> None:
        await super().close()
        with self._directory_lock.write():
            self._closed = True
            with contextlib.suppress(FileNotFoundError):
                shutil.rmtree(self.directory)
