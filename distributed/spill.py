from __future__ import annotations

import logging
from collections.abc import Hashable, Mapping
from distutils.version import LooseVersion
from functools import partial
from typing import TYPE_CHECKING, Any

import zict

if TYPE_CHECKING:
    from typing_extensions import Literal

from .protocol import deserialize_bytes, serialize_bytelist
from .sizeof import safe_sizeof

logger = logging.getLogger(__name__)


class SpillBuffer(zict.Buffer):
    """MutableMapping that automatically spills out dask key/value pairs to disk when
    the total size of the stored data exceeds the target. If max_spill is provided the
    key/value pairs won't be spilled once this threshold has been reached.
    """

    def __init__(
        self, spill_directory: str, target: int, max_spill: int | Literal[False] = False
    ):
        if (
            max_spill is not False and LooseVersion(zict.__version__) <= "2.0.0"
        ):  # FIX ME WHEN zict is released us LooseVersion(zict.__version__) <= "2.0.0"
            # is not False allows spill limit 0, decide if this case is ok
            raise ValueError("zict > 2.0.0 required to set max_weight")

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
            # TODO don't spam the log file with hundreds of messages per second
            logger.warning(
                "Spill file on disk reached capacity; keeping data in memory"
            )
            pass
        except OSError:
            logger.warning(
                "Spill file on disk failed",
                exec_info=True,
            )
            pass
        except PickleError as e:
            logger.warning(
                "Failed to pickle",
                exc_info=True,
            )
            k, orig_e = e.args
            if k == key:
                del self[
                    key
                ]  # if the key that is bad is the one being added we delete it
                raise orig_e
            else:
                # if you get here, key is fine something else is wrong then we pass
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


class PickleError(Exception):
    pass


class Slow(zict.Func):
    max_weight: int | Literal[False]
    weight_by_key: dict[Hashable, int]
    total_weight: int

    def __init__(self, spill_directory: str, max_weight: int | Literal[False] = False):
        super().__init__(
            partial(serialize_bytelist, on_error="raise"),
            deserialize_bytes,
            zict.File(spill_directory),
        )
        self.max_weight = max_weight
        self.weight_by_key = {}
        self.total_weight = 0

    def __setitem__(self, key, value):
        try:
            pickled = self.dump(value)
            pickled_size = sum(len(frame) for frame in pickled)
        except Exception as e:  # This is the error raised when fail to serialize
            raise PickleError(
                key, e
            )  # It's ok to loose the bad key, cluster remains usable

        if (
            self.max_weight is not False
            and self.total_weight + pickled_size > self.max_weight
        ):
            # Stop callbacks and ensure that the key ends up in SpillBuffer.fast
            self.total_weight -= self.weight_by_key.pop(
                key, 0
            )  # isn't this taken care when we pop an item? triggering del
            self.d.pop(key, None)
            # To be caught by SpillBuffer.__setitem__
            raise MaxSpillExceeded()

        assert key not in self.weight_by_key  # Thanks to Buffer.__setitem__
        self.weight_by_key[key] = pickled_size
        self.total_weight += pickled_size
        self.d[key] = pickled  # pickle and store to disk through File

    def __delitem__(self, key):
        super().__delitem__(key)
        self.total_weight -= self.weight_by_key.pop(key)
