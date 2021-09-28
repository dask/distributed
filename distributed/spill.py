from __future__ import annotations

from collections.abc import Hashable, Mapping
from functools import partial
from typing import Any

from .protocol import deserialize_bytes, serialize_bytelist
from .sizeof import safe_sizeof

try:
    from zict import Buffer, File, Func
except ImportError:
    raise ImportError("Please `python -m pip install zict` for spill-to-disk workers")


class SpillBuffer(Buffer):
    """MutableMapping that automatically spills out dask key/value pairs to disk when
    the total size of the stored data exceeds the target
    """

    spilled_by_key: dict[Hashable, int]
    spilled_total: int

    def __init__(self, spill_directory: str, target: int):
        self.spilled_by_key = {}
        self.spilled_total = 0
        storage = Func(
            partial(serialize_bytelist, on_error="raise"),
            deserialize_bytes,
            File(spill_directory),
        )
        super().__init__(
            {},
            storage,
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

    @staticmethod
    def _weight(key: Hashable, value: Any) -> int:
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

    def __delitem__(self, key: Hashable) -> None:
        self.spilled_total -= self.spilled_by_key.pop(key, 0)
        super().__delitem__(key)
