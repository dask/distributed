from __future__ import annotations

from typing import Awaitable, Callable

from dask.utils import parse_bytes

from distributed.shuffle._disk import ShardsBuffer
from distributed.shuffle._limiter import ResourceLimiter
from distributed.utils import log_errors


class CommShardsBuffer(ShardsBuffer):
    """Accept, buffer, and send many small messages to many workers

    This takes in lots of small messages destined for remote workers, buffers
    those messages in memory, and then sends out batches of them when possible
    to different workers.  This tries to send larger messages when possible,
    while also respecting a memory bound

    **State**

    -   shards: dict[str, list[bytes]]

        This is our in-memory buffer of data waiting to be sent to other workers.

    -   sizes: dict[str, int]

        The size of each list of shards.  We find the largest and send data from that buffer

    State
    -----
    memory_limit: str
        A maximum amount of memory to use across the process, like "1 GiB"
        This includes both data in shards and also in network communications
    max_connections: int
        The maximum number of connections to have out at once
    max_message_size: str
        The maximum size of a single message that we want to send

    Parameters
    ----------
    send: callable
        How to send a list of shards to a worker
        Expects an address of the target worker (string)
        and a payload of shards (list of bytes) to send to that worker
    """

    max_message_size = parse_bytes("2 MiB")
    memory_limit = parse_bytes("100 MiB")

    def __init__(
        self,
        send: Callable[[str, list[bytes]], Awaitable[None]],
        memory_limiter: ResourceLimiter | None = None,
        concurrency_limit: int = 10,
    ):
        super().__init__(
            memory_limiter=memory_limiter,
            concurrency_limit=concurrency_limit,
            max_message_size=CommShardsBuffer.max_message_size,
        )
        self.send = send

    async def _process(self, address: str, shards: list[bytes]) -> None:
        """Send one message off to a neighboring worker"""
        with log_errors():

            # Consider boosting total_size a bit here to account for duplication
            with self.time("send"):
                await self.send(address, [_join_shards(shards)])


def _join_shards(shards: list[bytes]) -> bytes:
    # This is just there for easier profiling
    return b"".join(shards)
