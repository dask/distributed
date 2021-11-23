from __future__ import annotations

import logging
from collections.abc import Hashable, Mapping
from distutils.version import LooseVersion
from functools import partial
from typing import Any

import zict

from .protocol import deserialize_bytes, serialize_bytelist
from .sizeof import safe_sizeof

logger = logging.getLogger(__name__)


class SpillBuffer(zict.Buffer):
    """MutableMapping that automatically spills out dask key/value pairs to disk when
    the total size of the stored data exceeds the target
    """

    def __init__(self, spill_directory: str, target: int, max_spill: int | None = None):
        if max_spill is not None and LooseVersion(zict.__version__) <= "2.0":
            raise ValueError("zict > 2.0 required to set max_weight")

        super().__init__(
            fast={},
            slow=Slow(spill_directory, max_spill),
            n=target,
            weight=_in_memory_weight,
        )

    def __setitem__(self, key, value):
        try:
            super().__setitem__(key, value)
        except MaxSpillExceeded:
            # key is in self.fast; no keys have been lost on eviction
            # Note: requires zict > 2.0
            # THIS ONLY WORKS WHEN TARGET < MAX SPILL AND KEY < TOT_WEIGHT BUT KEY> MAX SPILL
            # WE TRY TO WRITE TO FAST CAN'T GO TO DISK CAN'T KEEP IN FAST
            #
            # WHEN THE TARGET  < MAX SPILL AND KEY > TARGET AND KEY >MAX SPILL
            # WE SKIP LRU, WE GO THROUGH BUFFER SET ITEM WHICH IS PASSING WHEN RAISED
            # MAX SPILL AND KEY IS NEITHER IN SLOW NOR FAST
            pass

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

    @property
    def spilled_total(self) -> int:
        return self.slow.total_weight


def _in_memory_weight(key: Hashable, value: Any) -> int:
    return safe_sizeof(value)


class MaxSpillExceeded(Exception):
    pass


class Slow(zict.Func):
    weight_by_key: dict[Hashable, int]
    total_weight: int

    def __init__(self, spill_directory: str, max_weight: int | None = None):
        super().__init__(
            partial(serialize_bytelist, on_error="raise"),
            deserialize_bytes,
            zict.File(spill_directory),
        )
        self.max_weight = max_weight
        self.weight_by_key = {}
        self.total_weight = 0

    def __setitem__(self, key, value):
        pickled = self.dump(value)
        pickled_size = sum(
            map(safe_sizeof, pickled)
        )  # self.dump(value)  #this returns a list of len 3 then the size pof pickle is not the value we want
        # print(f"{pickled= }")
        print(f"{pickled_size= }")
        if self.total_weight + pickled_size > self.max_weight:
            # TODO don't spam the log file with hundreds of messages per second
            logger.warning(
                "Spill file on disk reached capacity; keeping data in memory"
            )
            # Stop callbacks and ensure that the key ends up in SpillBuffer.fast
            raise MaxSpillExceeded()
        self.weight_by_key[key] = pickled_size
        self.total_weight += pickled_size
        self.d[
            key
        ] = pickled  # this goes under a set item on File which does something to pickle

    def __delitem__(self, key):
        super().__delitem__(key)
        self.total_weight -= self.weight_by_key.pop(key)
