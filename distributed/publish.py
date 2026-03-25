from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, TypedDict
from contextlib import suppress
from dask.typing import Key
from dask.utils import stringify

from distributed.protocol.serialize import Serialized
from distributed.utils import log_errors

if TYPE_CHECKING:
    from distributed.scheduler import Scheduler


class PublishedDataset(TypedDict):
    data: Serialized
    keys: tuple[Key, ...]


class PublishExtension:
    """An extension for the scheduler to manage collections

    *  publish_list
    *  publish_put
    *  publish_get
    *  publish_delete
    """

    scheduler: Scheduler
    datasets: dict[Key, PublishedDataset]
    _flush_received: defaultdict[bytes, tuple[asyncio.Event, asyncio.Event]]

    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.datasets = {}

        handlers = {
            "publish_list": self.list,
            "publish_put": self.put,
            "publish_get": self.get,
            "publish_delete": self.delete,
        }
        stream_handlers = {
            "publish_flush_batched_send": self.flush_batched_send,
        }

        self.scheduler.handlers.update(handlers)
        self.scheduler.stream_handlers.update(stream_handlers)
        self._flush_received = defaultdict(lambda: (asyncio.Event(), asyncio.Event()))

    async def flush_batched_send(self, client: str, uid: bytes) -> None:
        ev_flush, ev_sync = self._flush_received[uid]
        ev_flush.set()
        while client in self.scheduler.clients:
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(ev_sync.wait(), timeout=1)
                return

        # Client disconnected between flush and sync
        del self._flush_received[uid]  # pragma: no cover

    async def sync_batched_send(self, client: str, uid: bytes) -> bool:
        """Wait for the client's batched-send to catch up with the same client's RPC
        calls. Return True if the client is still connected; False otherwise.
        """
        ev_flush, ev_sync = self._flush_received[uid]
        ev_sync.set()
        try:
            while client in self.scheduler.clients:
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(ev_flush.wait(), timeout=1)
                    return True
            return False
        finally:
            del self._flush_received[uid]

    @log_errors
    async def put(
        self,
        names: tuple[Key, ...],
        keys: tuple[tuple[Key, ...], ...],
        data: tuple[Serialized, ...],
        override: bool,
        client: str,
        uid: bytes,
    ) -> None:
        if not await self.sync_batched_send(client, uid):
            return

        for name, keys_i, data_i in zip(names, keys, data, strict=True):
            if name in self.datasets:
                if override:
                    old = self.datasets.pop(name)
                    self.scheduler.client_releases_keys(
                        old["keys"], f"published-{stringify(name)}"
                    )
                else:
                    raise KeyError("Dataset %s already exists" % name)

            self.scheduler.client_desires_keys(keys_i, f"published-{stringify(name)}")
            self.datasets[name] = {"data": data_i, "keys": keys_i}

    @log_errors
    async def delete(self, names: tuple[Key, ...], client: str, uid: bytes) -> None:
        if not await self.sync_batched_send(client, uid):
            return

        for name in names:
            out = self.datasets.pop(name, None)
            if out is not None:
                self.scheduler.client_releases_keys(
                    out["keys"], f"published-{stringify(name)}"
                )

    @log_errors
    def list(self) -> list[Key]:
        return list(sorted(self.datasets, key=str))

    @log_errors
    def get(self, names: tuple[Key, ...]) -> list[PublishedDataset | None]:  # type: ignore[valid-type]
        return [self.datasets.get(name, None) for name in names]


class Datasets(MutableMapping):
    """A dict-like wrapper around :class:`Client` dataset methods.

    Parameters
    ----------
    client : distributed.client.Client

    """

    __slots__ = ("_client",)

    def __init__(self, client):
        self._client = client

    def __getitem__(self, key):
        # When client is asynchronous, it returns a coroutine
        return self._client.get_dataset(key)

    def __setitem__(self, key, value):
        if self._client.asynchronous:
            # 'await obj[key] = value' is not supported by Python as of 3.8
            raise TypeError(
                "Can't use 'client.datasets[name] = value' when client is "
                "asynchronous; please use 'client.publish_dataset(name=value)' instead"
            )
        self._client.publish_dataset(value, name=key)

    def __delitem__(self, key):
        if self._client.asynchronous:
            # 'await del obj[key]' is not supported by Python as of 3.8
            raise TypeError(
                "Can't use 'del client.datasets[name]' when client is asynchronous; "
                "please use 'client.unpublish_dataset(name)' instead"
            )
        return self._client.unpublish_dataset(key)

    def __iter__(self):
        if self._client.asynchronous:
            raise TypeError(
                "Can't invoke iter() or 'for' on client.datasets when client is "
                "asynchronous; use 'async for' instead"
            )
        yield from self._client.list_datasets()

    def __aiter__(self):
        if not self._client.asynchronous:
            raise TypeError(
                "Can't invoke 'async for' on client.datasets when client is "
                "synchronous; use iter() or 'for' instead"
            )

        async def _():
            for key in await self._client.list_datasets():
                yield key

        return _()

    def __len__(self):
        if self._client.asynchronous:
            # 'await len(obj)' is not supported by Python as of 3.8
            raise TypeError(
                "Can't use 'len(client.datasets)' when client is asynchronous; "
                "please use 'len(await client.list_datasets())' instead"
            )
        return len(self._client.list_datasets())
