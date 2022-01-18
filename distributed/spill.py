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

    parameter
    """

    def __init__(
        self, spill_directory: str, target: int, max_spill: int | Literal[False] = False
    ):
        if max_spill is not False and LooseVersion(zict.__version__) <= "2.0.0":
            raise ValueError("zict > 2.0.0 required to set max_weight")

        super().__init__(
            fast={},
            slow=Slow(spill_directory, max_spill),
            n=target,
            weight=_in_memory_weight,
        )
        self.logged_keys: list = []  # list to track logged keys to avoid spaming

    def __setitem__(self, key, value):
        try:
            super().__setitem__(key, value)
        except MaxSpillExceeded as e:
            key_e = e.args[0]  # otherwise it returns (key_e, )
            # key is in self.fast; no keys have been lost on eviction
            # Note: requires zict > 2.0
            # TODO don't spam the log file with hundreds of messages per second
            if key_e not in self.logged_keys:
                logger.warning(
                    "Spill file on disk reached capacity; keeping data in memory"
                )
                self.logged_keys.append(key_e)
            pass
        except OSError:
            logger.error("Spill to disk failed; keeping data in memory", exc_info=True)
            pass
        except PickleError as e:
            key_e, orig_e = e.args
            if key_e == key:
                # The key we just inserted failed to serialize.
                # This happens only when the key is individually larger than target.
                # The exception will be caught by Worker and logged; the status of
                # the task will be set to error.
                del self[key]
                raise orig_e
            else:
                # The key we just inserted is smaller than target, but it caused another,
                # unrelated key to be spilled out of the LRU, and that key failed to serialize.
                # There's nothing wrong with the new key. The older key is still in memory.
                assert key_e in self.fast
                assert key_e not in self.slow
                # TODO don't flood the log every time ANY key is added to the LRU!
                if key_e not in self.logged_keys:
                    logger.error(f"Failed to pickle {key!r}", exc_info=True)
                    self.logged_keys.append(key_e)
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
        except Exception as e:
            # zict.LRU ensures that the key remains in fast if we raise.
            # Wrap the exception so that it's recognizable by SpillBuffer,
            # which will then unwrap it.
            raise PickleError(key, e)

        pickled_size = sum(len(frame) for frame in pickled)

        if (
            self.max_weight is not False
            and self.total_weight + pickled_size > self.max_weight
        ):
            # Stop callbacks and ensure that the key ends up in SpillBuffer.fast
            self.total_weight -= self.weight_by_key.pop(key, 0)
            self.d.pop(key, None)
            # To be caught by SpillBuffer.__setitem__
            raise MaxSpillExceeded(key)

        # Thanks to Buffer.__setitem__, we never update existing keys in slow,
        # but always delete them and reinsert them.
        assert key not in self.weight_by_key
        # Store to disk through File.
        # This may raise OSError, which is caught by SpillBuffer above.
        self.d[key] = pickled

        self.weight_by_key[key] = pickled_size
        self.total_weight += pickled_size

    def __delitem__(self, key):
        super().__delitem__(key)
        self.total_weight -= self.weight_by_key.pop(key)
