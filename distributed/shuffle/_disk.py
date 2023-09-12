from __future__ import annotations

import contextlib
import pathlib
import shutil
from collections import defaultdict
from io import BytesIO
from types import TracebackType
from typing import BinaryIO

from distributed.shuffle._buffer import ShardsBuffer
from distributed.shuffle._limiter import ResourceLimiter
from distributed.utils import log_errors


class FileShardsBuffer(ShardsBuffer):
    """An abstract buffering object backed by a "file"

    Parameters
    ----------
    memory_limiter : ResourceLimiter, optional
        Resource limiter.

    Notes
    -----
    Currently, a concurrency limit of one is hard-coded.
    """

    def __init__(self, memory_limiter: ResourceLimiter | None = None) -> None:
        super().__init__(
            memory_limiter=memory_limiter,
            # FileShardsBuffer not able to run concurrently
            concurrency_limit=1,
        )

    def writer(self, id: int | str) -> BinaryIO:
        """Return a file-like object for writing in append-mode.

        Parameters
        ----------
        id
            The shard id (will normalised to a string)

        Returns
        -------
        An object implementing the BinaryIO interface.
        """
        raise NotImplementedError("Abstract class can't provide this")

    def reader(self, id: int | str) -> BinaryIO:
        """Return a file-like object for reading from byte-0.

        Parameters
        ----------
        id
            The shard id (will be normalised to a string)

        Returns
        -------
        An object implementing the BinaryIO interface.

        Raises
        ------
        FileNotFoundError
            If no shard with requested id exists.
        """
        raise NotImplementedError("Abstract class can't provide this")

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
                with self.writer(id) as f:
                    for shard in shards:
                        f.write(shard)

    def read(self, id: int | str) -> bytes:
        """Read a complete file back into memory"""
        self.raise_on_exception()
        if not self._inputs_done:
            raise RuntimeError("Tried to read from file before done.")

        try:
            with self.time("read"):
                with self.reader(id) as f:
                    data = f.read()
                    size = f.tell()
        except FileNotFoundError:
            raise KeyError(id)

        if data:
            self.bytes_read += size
            return data
        else:
            raise KeyError(id)


class _PersistentBytesIO(BytesIO):
    """A BytesIO object that does not close itself when used in a with block."""

    def __enter__(self) -> _PersistentBytesIO:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        pass


class MemoryShardsBuffer(FileShardsBuffer):
    """Accept and buffer many small objects into memory.

    This implements in-memory "file" buffering with no resource limit
    with the same interface as :class:`DiskShardsBuffer`.

    """

    def __init__(self) -> None:
        super().__init__(memory_limiter=None)
        self._memory_buffers: defaultdict[str, _PersistentBytesIO] = defaultdict(
            _PersistentBytesIO
        )

    def writer(self, id: int | str) -> BinaryIO:
        buf = self._memory_buffers[str(id)]
        buf.seek(buf.tell())
        return buf

    def reader(self, id: int | str) -> BinaryIO:
        key = str(id)
        if key not in self._memory_buffers:
            raise FileNotFoundError(f"Shard with {id=} is unknown")
        buf = self._memory_buffers[str(id)]
        buf.seek(0)
        return buf


class DiskShardsBuffer(FileShardsBuffer):
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
        memory_limiter: ResourceLimiter | None = None,
    ):
        super().__init__(memory_limiter=memory_limiter)
        self.directory = pathlib.Path(directory)
        self.directory.mkdir(exist_ok=True)

    def writer(self, id: int | str) -> BinaryIO:
        return open(self.directory / str(id), mode="ab", buffering=100_000_000)

    def reader(self, id: int | str) -> BinaryIO:
        return open(self.directory / str(id), mode="rb", buffering=100_000_000)

    async def close(self) -> None:
        await super().close()
        with contextlib.suppress(FileNotFoundError):
            shutil.rmtree(self.directory)
