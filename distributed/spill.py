from __future__ import annotations

from collections.abc import Hashable
from functools import partial

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

    @staticmethod
    def _weight(k: Hashable, v):
        return safe_sizeof(v)

    def _on_evict(self, k: Hashable, v) -> None:
        b = safe_sizeof(v)
        self.spilled_by_key[k] = b
        self.spilled_total += b

    def _on_retrieve(self, k: Hashable, v) -> None:
        b = self.spilled_by_key.pop(k)
        self.spilled_total -= b

    def __delitem__(self, key: Hashable):
        if key in self.slow:
            self._on_retrieve(key, None)
        super().__delitem__(key)
