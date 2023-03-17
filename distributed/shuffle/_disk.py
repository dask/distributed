from __future__ import annotations

import contextlib
import pathlib
import shutil
from collections import defaultdict

from dask.utils import parse_bytes

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

    # TODO: Configure based on worker memory limits (possibly dynamically?)
    max_buffer_size = parse_bytes("1 GiB")

    def __init__(
        self,
        directory: str | pathlib.Path,
        memory_limiter: ResourceLimiter | None = None,
    ):
        super().__init__(
            memory_limiter=memory_limiter,
            # Disk is not able to run concurrently atm
            concurrency_limit=1,
        )
        self.directory = pathlib.Path(directory)
        self.directory.mkdir(exist_ok=True)
        self._in_memory = 0
        self._memory_buf: defaultdict[str, list[bytes]] = defaultdict(list)

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

        # Normalisation for safety
        id = str(id)
        with log_errors():
            # Consider boosting total_size a bit here to account for duplication
            with self.time("write"):
                if not self.max_buffer_size:
                    # Fast path if we're always hitting the disk
                    self._write(id, shards)
                else:
                    while shards:
                        if self._in_memory < self.max_buffer_size:
                            self._memory_buf[id].append(newdata := shards.pop())
                            self._in_memory += len(newdata)
                        else:
                            # Flush old data
                            # This could be offloaded to a background
                            # task at the cost of going further over
                            # the soft memory limit.
                            for id, bufs in self._memory_buf.items():
                                self._write(id, bufs)
                            self._memory_buf.clear()
                            self._in_memory = 0

    def _write(self, id: str, shards: list[bytes]) -> None:
        with open(self.directory / str(id), mode="ab", buffering=100_000_000) as f:
            for s in shards:
                f.write(s)

    async def close(self) -> None:
        await super().close()
        with contextlib.suppress(FileNotFoundError):
            shutil.rmtree(self.directory)

    def read(self, id: int | str) -> bytes:
        """Read a complete file back into memory, concatting with any
        in memory parts"""
        self.raise_on_exception()
        if not self._inputs_done:
            raise RuntimeError("Tried to read from file before done.")

        id = str(id)
        data = self._memory_buf[id]
        try:
            with self.time("read"):
                with open(
                    self.directory / str(id), mode="rb", buffering=100_000_000
                ) as f:
                    data.append(f.read())
        except FileNotFoundError:
            if not data:
                # Neither disk nor in memory
                raise KeyError(id)

        if data:
            buf = b"".join(data)
            self.bytes_read += len(buf)
            return buf
        else:
            raise KeyError(id)
