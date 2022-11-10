from __future__ import annotations

import contextlib
import os
import pathlib
import shutil
from typing import TYPE_CHECKING, Any, BinaryIO, Callable

if TYPE_CHECKING:
    import pyarrow as pa

from distributed.shuffle._buffer import ShardsBuffer
from distributed.shuffle._limiter import ResourceLimiter
from distributed.utils import log_errors


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

    State
    -----
    memory_limit: str
        A maximum amount of memory to use, like "1 GiB"

    Parameters
    ----------
    directory: pathlib.Path
        Where to write and read data.  Ideally points to fast disk.
    dump: callable
        Writes an object to a file, like pickle.dump
    load: callable
        Reads an object from that file, like pickle.load
    sizeof: callable
        Measures the size of an object in memory
    """

    concurrency_limit = 2

    def __init__(
        self,
        directory: str,
        dump: Callable[[Any, BinaryIO], None],
        load: Callable[[BinaryIO], Any],
        memory_limiter: ResourceLimiter | None = None,
    ):
        super().__init__(
            memory_limiter=memory_limiter,
            # Disk is not able to run concurrently atm
            concurrency_limit=1,
        )
        self.directory = pathlib.Path(directory)
        if not os.path.exists(self.directory):
            os.mkdir(self.directory)
        self.dump = dump
        self.load = load

    async def _process(self, id: str, shards: list[pa.Buffer]) -> None:
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
                with open(
                    self.directory / str(id), mode="ab", buffering=100_000_000
                ) as f:
                    for shard in shards:
                        self.dump(shard, f)
                    # os.fsync(f)  # TODO: maybe?

    def read(self, id: int | str) -> pa.Table:
        """Read a complete file back into memory"""
        self.raise_on_exception()
        if not self._done:
            raise RuntimeError("Tried to read from file before done.")
        parts = []

        try:
            with self.time("read"):
                with open(
                    self.directory / str(id), mode="rb", buffering=100_000_000
                ) as f:
                    while True:
                        try:
                            parts.append(self.load(f))
                        except EOFError:
                            break
                    size = f.tell()
        except FileNotFoundError:
            raise KeyError(id)

        # TODO: We could consider deleting the file at this point
        if parts:
            self.bytes_read += size
            assert len(parts) == 1
            return parts[0]
        else:
            raise KeyError(id)

    async def close(self) -> None:
        await super().close()
        with contextlib.suppress(FileNotFoundError):
            shutil.rmtree(self.directory)
