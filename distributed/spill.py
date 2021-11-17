from __future__ import annotations

from collections.abc import Hashable, Mapping
from ctypes import sizeof  # noqa: F401   do we use this?
from functools import (  # noqa: F401 do we use this total ordering
    partial,
    total_ordering,
)
from typing import Any, Literal

from zict import Buffer, File, Func

from .protocol import deserialize_bytes, serialize_bytelist
from .sizeof import safe_sizeof


class SpillBuffer(Buffer):
    """MutableMapping that automatically spills out dask key/value pairs to disk when
    the total size of the stored data exceeds the target
    """

    spilled_by_key: dict[Hashable, int]
    spilled_total: int

    def __init__(
        self,
        spill_directory: str,
        target: int,
        disk_limit: int | Literal[False] | None = None,
    ):
        self.spilled_by_key = {}
        self.spilled_total = 0

        self.disk_limit = disk_limit
        self.spilled_total_disk = 0  # MAYBE CHOOSE A DIFFERENT NAME
        self.storage = Func(
            partial(serialize_bytelist, on_error="raise"),
            deserialize_bytes,
            File(spill_directory),
        )
        super().__init__(
            {},
            self.storage,
            target,
            weight=self._weight,
            fast_to_slow_callbacks=[self._on_evict],
            slow_to_fast_callbacks=[self._on_retrieve],
        )

    @property
    def memory(self) -> Mapping[Hashable, Any]:
        """Key/value pairs stored in RAM. Alias of zict.Buffer.fast.
        For inspection only - do not modify directly!
        """
        return self.fast

    @property
    def disk(self) -> Mapping[Hashable, Any]:
        """Key/value pairs spilled out to disk. Alias of zict.Buffer.slow.
        For inspection only - do not modify directly!
        """
        return self.slow

    # @staticmethod
    def _weight(self, key: Hashable, value: Any) -> int:
        # Disk limit will be false by default so we need to check we have a limit
        # otherwise the second condition is always true
        # this triggers the right path but will record -1 on the tracking of what's
        # on fast so not really working
        if self.disk_limit and (
            safe_sizeof(value) + self.spilled_total_disk > self.disk_limit
        ):
            print("spill-limit reached keeping task in memory")
            return -1  # this should keep the key in fast
        else:
            return safe_sizeof(value)

    def _on_evict(self, key: Hashable, value: Any) -> None:
        b = safe_sizeof(value)
        self.spilled_by_key[key] = b
        self.spilled_total += b

    def _on_retrieve(self, key: Hashable, value: Any) -> None:
        self.spilled_total -= self.spilled_by_key.pop(key)

    def __setitem__(self, key: Hashable, value: Any) -> None:
        self.spilled_total -= self.spilled_by_key.pop(key, 0)
        super().__setitem__(key, value)
        if key in self.slow:
            # value is individually larger than target so it went directly to slow.
            # _on_evict was not called.
            b = safe_sizeof(value)
            self.spilled_by_key[key] = b
            self.spilled_total += b

            if self.disk_limit:
                # track total spilled to disk (on disk) if limit is provided
                self.spilled_total_disk += len(self.storage.d.get(key))

    def __delitem__(self, key: Hashable) -> None:
        self.spilled_total -= self.spilled_by_key.pop(key, 0)
        super().__delitem__(key)
