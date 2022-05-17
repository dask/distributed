from __future__ import annotations

import logging
import time
from collections.abc import Mapping, MutableMapping, Sized
from contextlib import contextmanager
from functools import partial
from typing import Any, Literal, NamedTuple, Protocol, cast

from packaging.version import parse as parse_version

import zict

from distributed.protocol import deserialize_bytes, serialize_bytelist
from distributed.sizeof import safe_sizeof

logger = logging.getLogger(__name__)
has_zict_210 = parse_version(zict.__version__) >= parse_version("2.1.0")
has_zict_220 = parse_version(zict.__version__) >= parse_version("2.2.0")


class SpilledSize(NamedTuple):
    """Size of a key/value pair when spilled to disk, in bytes"""

    # output of sizeof()
    memory: int
    # pickled size
    disk: int

    def __add__(self, other: SpilledSize) -> SpilledSize:  # type: ignore
        return SpilledSize(self.memory + other.memory, self.disk + other.disk)

    def __sub__(self, other: SpilledSize) -> SpilledSize:
        return SpilledSize(self.memory - other.memory, self.disk - other.disk)


class ManualEvictProto(Protocol):
    """Duck-type API that a third-party alternative to SpillBuffer must respect (in
    addition to MutableMapping) if it wishes to support spilling when the
    ``distributed.worker.memory.spill`` threshold is surpassed.

    This is public API. At the moment of writing, Dask-CUDA implements this protocol in
    the ProxifyHostFile class.
    """

    @property
    def fast(self) -> Sized | bool:
        """Access to fast memory. This is normally a MutableMapping, but for the purpose
        of the manual eviction API it is just tested for emptiness to know if there is
        anything to evict.
        """
        ...  # pragma: nocover

    def evict(self) -> int:
        """Manually evict a key/value pair from fast to slow memory.
        Return size of the evicted value in fast memory.

        If the eviction failed for whatever reason, return -1. This method must
        guarantee that the key/value pair that caused the issue has been retained in
        fast memory and that the problem has been logged internally.

        This method never raises.
        """
        ...  # pragma: nocover


# zict.Buffer[str, Any] requires zict >= 2.2.0
class SpillBuffer(zict.Buffer):
    """MutableMapping that automatically spills out dask key/value pairs to disk when
    the total size of the stored data exceeds the target. If max_spill is provided the
    key/value pairs won't be spilled once this threshold has been reached.

    Parameters
    ----------
    spill_directory: str
        Location on disk to write the spill files to
    target: int
        Managed memory, in bytes, to start spilling at
    max_spill: int | False, optional
        Limit of number of bytes to be spilled on disk. Set to False to disable.
    min_log_interval: float, optional
        Minimum interval, in seconds, between warnings on the log file about full disk
    """

    last_logged: float
    min_log_interval: float
    logged_pickle_errors: set[str]

    def __init__(
        self,
        spill_directory: str,
        target: int,
        max_spill: int | Literal[False] = False,
        min_log_interval: float = 2,
    ):

        if max_spill is not False and not has_zict_210:
            raise ValueError("zict >= 2.1.0 required to set max-spill")

        slow: MutableMapping[str, Any] = Slow(spill_directory, max_spill)
        if has_zict_220:
            # If a value is still in use somewhere on the worker since the last time it
            # was unspilled, don't duplicate it
            slow = zict.Cache(slow, zict.WeakValueMapping())

        super().__init__(fast={}, slow=slow, n=target, weight=_in_memory_weight)
        self.last_logged = 0
        self.min_log_interval = min_log_interval
        self.logged_pickle_errors = set()  # keys logged with pickle error

    @contextmanager
    def handle_errors(self, key: str | None):
        try:
            yield
        except MaxSpillExceeded as e:
            # key is in self.fast; no keys have been lost on eviction
            # Note: requires zict > 2.0
            (key_e,) = e.args
            assert key_e in self.fast
            assert key_e not in self.slow
            now = time.time()
            if now - self.last_logged >= self.min_log_interval:
                logger.warning(
                    "Spill file on disk reached capacity; keeping data in memory"
                )
                self.last_logged = now
            raise HandledError()
        except OSError:
            # Typically, this is a disk full error
            now = time.time()
            if now - self.last_logged >= self.min_log_interval:
                logger.error(
                    "Spill to disk failed; keeping data in memory", exc_info=True
                )
                self.last_logged = now
            raise HandledError()
        except PickleError as e:
            key_e, orig_e = e.args
            if parse_version(zict.__version__) <= parse_version("2.0.0"):
                pass
            else:
                assert key_e in self.fast
            assert key_e not in self.slow
            if key_e == key:
                assert key is not None
                # The key we just inserted failed to serialize.
                # This happens only when the key is individually larger than target.
                # The exception will be caught by Worker and logged; the status of
                # the task will be set to error.
                if has_zict_210:
                    del self[key]
                else:
                    assert key not in self.fast
                    assert key not in self.slow
                raise orig_e
            else:
                # The key we just inserted is smaller than target, but it caused
                # another, unrelated key to be spilled out of the LRU, and that key
                # failed to serialize. There's nothing wrong with the new key. The older
                # key is still in memory.
                if key_e not in self.logged_pickle_errors:
                    logger.error(f"Failed to pickle {key_e!r}", exc_info=True)
                    self.logged_pickle_errors.add(key_e)
                raise HandledError()

    def __setitem__(self, key: str, value: Any) -> None:
        """If sizeof(value) < target, write key/value pair to self.fast; this may in
        turn cause older keys to be spilled from fast to slow.
        If sizeof(value) >= target, write key/value pair directly to self.slow instead.

        Raises
        ------
        Exception
            sizeof(value) >= target, and value failed to pickle.
            The key/value pair has been forgotten.

        In all other cases:

        - an older value was evicted and failed to pickle,
        - this value or an older one caused the disk to fill and raise OSError,
        - this value or an older one caused the max_spill threshold to be exceeded,

        this method does not raise and guarantees that the key/value that caused the
        issue remained in fast.
        """
        try:
            with self.handle_errors(key):
                super().__setitem__(key, value)
                self.logged_pickle_errors.discard(key)
        except HandledError:
            if has_zict_210:
                assert key in self.fast
            else:
                assert key not in self.fast
                logger.error("Key %s lost. Please upgrade to zict >= 2.1.0", key)
            assert key not in self.slow

    def evict(self) -> int:
        """Implementation of :meth:`ManualEvictProto.evict`.

        Manually evict the oldest key/value pair, even if target has not been
        reached. Returns sizeof(value).
        If the eviction failed (value failed to pickle, disk full, or max_spill
        exceeded), return -1; the key/value pair that caused the issue will remain in
        fast. The exception has been logged internally.
        This method never raises.
        """
        try:
            with self.handle_errors(None):
                _, _, weight = self.fast.evict()
                return cast(int, weight)
        except HandledError:
            return -1

    def __delitem__(self, key: str) -> None:
        super().__delitem__(key)
        self.logged_pickle_errors.discard(key)

    @property
    def memory(self) -> Mapping[str, Any]:
        """Key/value pairs stored in RAM. Alias of zict.Buffer.fast.
        For inspection only - do not modify directly!
        """
        return self.fast

    @property
    def disk(self) -> Mapping[str, Any]:
        """Key/value pairs spilled out to disk. Alias of zict.Buffer.slow.
        For inspection only - do not modify directly!
        """
        return self.slow

    @property
    def spilled_total(self) -> SpilledSize:
        """Number of bytes spilled to disk. Tuple of

        - output of sizeof()
        - pickled size

        The two may differ substantially, e.g. if sizeof() is inaccurate or in case of
        compression.
        """
        slow = cast(zict.Cache, self.slow).data if has_zict_220 else self.slow
        return cast(Slow, slow).total_weight


def _in_memory_weight(key: str, value: Any) -> int:
    return safe_sizeof(value)


# Internal exceptions. These are never raised by SpillBuffer.
class MaxSpillExceeded(Exception):
    pass


class PickleError(Exception):
    pass


class HandledError(Exception):
    pass


# zict.Func[str, Any] requires zict >= 2.2.0
class Slow(zict.Func):
    max_weight: int | Literal[False]
    weight_by_key: dict[str, SpilledSize]
    total_weight: SpilledSize

    def __init__(self, spill_directory: str, max_weight: int | Literal[False] = False):
        super().__init__(
            partial(serialize_bytelist, on_error="raise"),
            deserialize_bytes,
            zict.File(spill_directory),
        )
        self.max_weight = max_weight
        self.weight_by_key = {}
        self.total_weight = SpilledSize(0, 0)

    def __setitem__(self, key: str, value: Any) -> None:
        try:
            # FIXME https://github.com/python/mypy/issues/708
            pickled = self.dump(value)  # type: ignore
        except Exception as e:
            # zict.LRU ensures that the key remains in fast if we raise.
            # Wrap the exception so that it's recognizable by SpillBuffer,
            # which will then unwrap it.
            raise PickleError(key, e)

        pickled_size = sum(len(frame) for frame in pickled)

        if has_zict_210:
            # Thanks to Buffer.__setitem__, we never update existing
            # keys in slow, but always delete them and reinsert them.
            assert key not in self.d
            assert key not in self.weight_by_key
        else:
            self.d.pop(key, None)
            self.total_weight -= self.weight_by_key.pop(key, SpilledSize(0, 0))

        if (
            self.max_weight is not False
            and self.total_weight.disk + pickled_size > self.max_weight
        ):
            # Stop callbacks and ensure that the key ends up in SpillBuffer.fast
            # To be caught by SpillBuffer.__setitem__
            raise MaxSpillExceeded(key)

        # Store to disk through File.
        # This may raise OSError, which is caught by SpillBuffer above.
        self.d[key] = pickled

        weight = SpilledSize(safe_sizeof(value), pickled_size)
        self.weight_by_key[key] = weight
        self.total_weight += weight

    def __delitem__(self, key: str) -> None:
        super().__delitem__(key)
        self.total_weight -= self.weight_by_key.pop(key)
