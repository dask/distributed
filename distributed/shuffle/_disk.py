from __future__ import annotations

import contextlib
import pathlib
import shutil
import threading
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, Callable

from distributed.shuffle._buffer import ShardsBuffer
from distributed.shuffle._limiter import ResourceLimiter
from distributed.utils import Deadline, log_errors


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
    memory_limiter : ResourceLimiter, optional
        Limiter for in-memory buffering (at most this much data)
        before writes to disk occur. If the incoming data that has yet
        to be processed exceeds this limit, then the buffer will block
        until below the threshold. See :meth:`.write` for the
        implementation of this scheme.
    """

    def __init__(
        self,
        directory: str | pathlib.Path,
        read: Callable[[pathlib.Path], tuple[Any, int]],
        memory_limiter: ResourceLimiter | None = None,
    ):
        super().__init__(
            memory_limiter=memory_limiter,
            # Disk is not able to run concurrently atm
            concurrency_limit=1,
        )
        self.directory = pathlib.Path(directory)
        self.directory.mkdir(exist_ok=True)
        self._closed = False
        self._read = read
        self._directory_lock = ReadWriteLock()

    async def _process(self, id: str, shards: list[bytes]) -> None:
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

        with log_errors():
            # Consider boosting total_size a bit here to account for duplication
            with self.time("write"):
                # We only need shared (i.e., read) access to the directory to write
                # to a file inside of it.
                with self._directory_lock.read():
                    if self._closed:
                        raise RuntimeError("Already closed")
                    with open(
                        self.directory / str(id), mode="ab", buffering=100_000_000
                    ) as f:
                        for shard in shards:
                            f.write(shard)

    def read(self, id: int | str) -> Any:
        """Read a complete file back into memory"""
        self.raise_on_exception()
        if not self._inputs_done:
            raise RuntimeError("Tried to read from file before done.")

        try:
            with self.time("read"):
                with self._directory_lock.read():
                    if self._closed:
                        raise RuntimeError("Already closed")
                    data, size = self._read((self.directory / str(id)).resolve())
        except FileNotFoundError:
            raise KeyError(id)

        if data:
            self.bytes_read += size
            return data
        else:
            raise KeyError(id)

    async def close(self) -> None:
        await super().close()
        with self._directory_lock.write():
            self._closed = True
            with contextlib.suppress(FileNotFoundError):
                shutil.rmtree(self.directory)
