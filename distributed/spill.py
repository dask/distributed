from __future__ import annotations

import logging
import os.path
import weakref
from collections.abc import Iterator, Mapping, MutableMapping, Sized
from contextlib import contextmanager
from dataclasses import dataclass
from functools import partial
from time import perf_counter
from typing import Any, Literal, NamedTuple, Protocol, cast

import psutil
from packaging.version import parse as parse_version

import zict

from distributed.protocol import deserialize_bytes, serialize_bytelist
from distributed.sizeof import safe_sizeof

logger = logging.getLogger(__name__)
has_zict_220 = parse_version(zict.__version__) >= parse_version("2.2.0")
has_zict_230 = parse_version(zict.__version__) >= parse_version("2.3.0")


class SpilledSize(NamedTuple):
    """Size of a key/value pair when spilled to disk, in bytes, broken down into

    - what is going to be deep-copied into pure-Python structures by deserialize_bytes,
      e.g. pickle.dumps() output
    - what is going to remain referenced by the deserialized object, e.g numpy buffers
    """

    by_value: int
    by_ref: int

    def __add__(self, other: SpilledSize) -> SpilledSize:  # type: ignore
        return SpilledSize(self.by_value + other.by_value, self.by_ref + other.by_ref)

    def __sub__(self, other: SpilledSize) -> SpilledSize:
        return SpilledSize(self.by_value - other.by_value, self.by_ref - other.by_ref)

    @classmethod
    def from_frames(cls, frames: list[bytes | memoryview]) -> SpilledSize:
        """Heuristic to predict how much of the serialized data is going to be
        deep-copied by deserialize_bytes (e.g. into pure Python objects) and how much
        is going to remain referenced by the deserialized object.

        This algorithm is not foolproof and could be misled by some exotic data types: a
        bytes object could be used as the underlying buffer of an object and a non-bytes
        object could be deep-copied upon deserialization for some reason (namely, if you
        want a writeable numpy array but the buffer you're given is read-only).
        """
        by_value = 0
        by_ref = 0

        for frame in frames:
            if isinstance(frame, bytes):
                # - pack_frames_prelude() output
                # - header
                by_value += len(frame)
            else:
                assert isinstance(frame, memoryview)
                if isinstance(frame.obj, bytes):
                    # output of pickle.dumps or of one of the compression algorithms
                    by_value += frame.nbytes
                else:
                    # numpy.ndarray, pyarrow.Buffer, or some more exotic types which we
                    # blindly assume won't be deep-copied upon deserialization.
                    by_ref += frame.nbytes

        return cls(by_value, by_ref)


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

    def evict(self, key: str | None = None) -> int:
        """Manually evict a key/value pair from fast to slow memory.
        If no key is specified, the buffer must choose one.
        Return size of the evicted value in fast memory.

        If the eviction failed for whatever reason, return -1. This method must
        guarantee that the key/value pair that caused the issue has been retained in
        fast memory and that the problem has been logged internally.

        This method never raises.
        """
        ...  # pragma: nocover


@dataclass
class FastMetrics:
    """Cumulative metrics for SpillBuffer.fast since the latest worker restart"""

    read_count_total: int = 0
    read_bytes_total: int = 0

    def log_read(self, key_bytes: int) -> None:
        self.read_count_total += 1
        self.read_bytes_total += key_bytes


@dataclass
class SlowMetrics:
    """Cumulative metrics for SpillBuffer.slow since the latest worker restart"""

    read_count_total: int = 0
    read_bytes_total: int = 0
    read_time_total: float = 0
    write_count_total: int = 0
    write_bytes_total: int = 0
    write_time_total: float = 0
    pickle_time_total: float = 0
    unpickle_time_total: float = 0

    def log_write(
        self,
        key_bytes: int,
        pickle_time: float,
        write_time: float,
    ) -> None:
        self.write_count_total += 1
        self.write_bytes_total += key_bytes
        self.pickle_time_total += pickle_time
        self.write_time_total += write_time

    def log_read(self, key_bytes: int, read_time: float, unpickle_time: float) -> None:
        self.read_count_total += 1
        self.read_bytes_total += key_bytes
        self.read_time_total += read_time
        self.unpickle_time_total += unpickle_time


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
    shared_memory: bool, optional
        Enable shared memory model.
        Read TODO link to documentation

        This has the following effects:
        - set keep_spill=True in zict.Buffer
        - set memmap=True in zict.File
        - disable compression (otherwise, it uses the same settings as network comms)

        Design note
            Technically, these three settings are independent, and there are some edge
            cases where it may be useful to switch some but not the others.
            A single all-or-nothing toggle was chosen in order to reduce complexity.

    max_spill: int | False, optional
        Limit of number of bytes to be spilled on disk. Set to False to disable.
    min_log_interval: float, optional
        Minimum interval, in seconds, between warnings on the log file about full disk
    """

    last_logged: float
    min_log_interval: float
    logged_pickle_errors: set[str]
    fast_metrics: FastMetrics

    def __init__(
        self,
        spill_directory: str,
        target: int,
        *,
        shared_memory: bool = False,
        max_spill: int | Literal[False] = False,
        min_log_interval: float = 2,
    ):
        if shared_memory and not has_zict_230:
            raise ValueError(
                "zict >= 2.3.0 required to set distributed.worker.memory.shared: true"
            )

        slow: MutableMapping[str, Any] = Slow(
            spill_directory, memmap=shared_memory, max_weight=max_spill
        )
        if has_zict_220:
            # If a value is still in use somewhere on the worker since the last time it
            # was unspilled, don't duplicate it
            slow = zict.Cache(slow, zict.WeakValueMapping())

        super().__init__(
            fast={},
            slow=slow,
            n=target,
            weight=partial(_in_memory_weight, weakref.ref(self)),
            keep_slow=shared_memory,
        )
        self.last_logged = 0
        self.min_log_interval = min_log_interval
        self.logged_pickle_errors = set()  # keys logged with pickle error

        self.fast_metrics = FastMetrics()

    @property
    def shared_memory(self) -> bool:
        return self.keep_slow

    @contextmanager
    def handle_errors(self, key: str | None) -> Iterator[None]:
        try:
            yield
        except MaxSpillExceeded as e:
            # key is in self.fast; no keys have been lost on eviction
            # Note: requires zict > 2.0
            (key_e,) = e.args
            assert key_e in self.fast
            assert key_e not in self.slow
            now = perf_counter()
            if now - self.last_logged >= self.min_log_interval:
                logger.warning(
                    "Spill file on disk reached capacity; keeping data in memory"
                )
                self.last_logged = now
            raise HandledError()
        except OSError:
            # Typically, this is a disk full error
            now = perf_counter()
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
                del self[key]
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
            assert key in self.fast
            assert key not in self.slow

    def evict(self, key: str | None = None) -> int:
        """Implementation of :meth:`ManualEvictProto.evict`.

        Manually evict a key/value pair, or the oldest key/value pair if not specified,
        even if target has not been reached. Returns sizeof(value).
        If the eviction failed (value failed to pickle, disk full, or max_spill
        exceeded), return -1; the key/value pair that caused the issue will remain in
        fast. The exception has been logged internally.
        This method never raises.
        """
        try:
            with self.handle_errors(None):
                _, _, weight = self.fast.evict(key)
                return cast(int, weight)
        except HandledError:
            return -1

    def shm_export(self, key: str) -> tuple[str, int, int] | None:
        """Ensure that the file has been spilled to disk and is available for another
        worker to pick up through the shared memory model.

        If the key is found exclusively in memory, force eviction to disk, overriding
        LRU priority.

        Returns
        -------
        - file path on disk for this worker
        - pickled size, in bytes (without buffers)
        - buffers size, in bytes

        If eviction failed (e.g. disk full), then return None instead.

        See also
        --------
        SpillBuffer.shm_import
        """
        if not has_zict_230:
            raise AttributeError("shm_export requires zict >= 2.3.0")  # pragma: nocover

        if key not in self.slow:
            if key not in self.fast:
                raise KeyError(key)
            if self.evict(key) == -1:
                return None

        slow = self._slow_uncached
        return slow.d.get_path(key), *slow.weight_by_key[key]

    def shm_import(self, paths: Mapping[str, tuple[str, int, int]]) -> Iterator[str]:
        """Hardlink a spilled file from another worker's local_directory into our own
        and add its key to self.

        Parameters
        ----------
        paths: dict[str, tuple[str, int, int]]
            {key : (
                file path on disk in the local_directory of the peer worker,
                pickled size, in bytes (without buffers)
                buffers size, in bytes
            )}

        Returns
        -------
        Iterator of keys that were successfully imported

        See also
        --------
        SpillBuffer.shm_export
        """
        if not has_zict_230:
            raise AttributeError("shm_import requires zict >= 2.3.0")  # pragma: nocover

        slow = self._slow_uncached
        for key, (path, pickled_size, buffers_size) in paths.items():
            if key in self:
                # This should never happen
                logger.error(  # pragma: nocover
                    f"SpillBuffer.link(): {key} is already in buffer; skipping"
                )
                continue  # pragma: nocover

            weight = SpilledSize(pickled_size, buffers_size)
            try:
                slow.d.link(key, path)
                self._len += 1
                slow.weight_by_key[key] = weight
                slow.total_weight += weight
                yield key
            except FileNotFoundError:
                # This can happen sporadically due to race conditions
                pass
            except OSError as e:
                # - [Errno 18] Invalid cross-device link
                # - PermissionError caused by workers running as different users
                logger.error(
                    "Failed to import memory-mapped data from peer worker: "
                    f"{e.__class__.__name__}: {e} ({key=}, {path=})"
                )

    def __getitem__(self, key: str) -> Any:
        if key in self.fast:
            # Note: don't log from self.fast.__getitem__, because that's called every
            # time a key is evicted, and we don't want to count those events here.
            nbytes = cast(int, self.fast.weights[key])
            self.fast_metrics.log_read(nbytes)

        return super().__getitem__(key)

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
    def _slow_uncached(self) -> Slow:
        slow = cast(zict.Cache, self.slow).data if has_zict_220 else self.slow
        return cast(Slow, slow)

    @property
    def spill_directory(self) -> str:
        return self._slow_uncached.d.directory

    @property
    def nbytes(self) -> tuple[int, int]:
        """Number of bytes in memory and spilled to disk. Tuple of

        - bytes in the process memory
        - bytes on disk or shared memory

        The sum of the two may different substantially from WorkerStateMachine.nbytes,
        e.g. if the output of sizeof() is inaccurate or in case of compression.

        If shared_memory=True, then pure-Python objects (such as object string columns
        in pandas DataFrames) may end up occupying both process and shared memory, in
        their live and pickled formats, at the same time.

        Also, in the case of shared memory, this property does not return just the
        portion known by the current worker, but the total divided by the number of
        workers on the host. This assumes that no other applications (significantly)
        uses the same mountpoint.
        """
        fast = cast(int, self.fast.total_weight)

        if not self.shared_memory:
            slow = sum(self._slow_uncached.total_weight)
        else:
            spill_root = os.path.join(self.spill_directory, "..", "..")
            n_workers = sum(not d.endswith("lock") for d in os.listdir(spill_root))
            slow = psutil.disk_usage(spill_root).used // n_workers

        return fast, slow

    def get_metrics(self) -> dict[str, float]:
        """Metrics to be exported to Prometheus or to be parsed directly.

        From these you may generate derived metrics:

        cache hit ratio:
          by keys  = memory_read_count_total / (memory_read_count_total + disk_read_count_total)
          by bytes = memory_read_bytes_total / (memory_read_bytes_total + disk_read_bytes_total)

        mean times per key:
          pickle   = pickle_time_total     / disk_write_count_total
          write    = disk_write_time_total / disk_write_count_total
          unpickle = unpickle_time_total   / disk_read_count_total
          read     = disk_read_time_total  / disk_read_count_total

        mean bytes per key:
          write    = disk_write_bytes_total / disk_write_count_total
          read     = disk_read_bytes_total  / disk_read_count_total

        mean bytes per second:
          write    = disk_write_bytes_total / disk_write_time_total
          read     = disk_read_bytes_total  / disk_read_time_total
        """
        fm = self.fast_metrics
        sm = self._slow_uncached.metrics

        out = {
            "memory_count": len(self.fast),
            "memory_bytes": self.fast.total_weight,
            "disk_count": len(self.slow),
            "disk_bytes": sum(self._slow_uncached.total_weight),
        }

        for k, v in fm.__dict__.items():
            out[f"memory_{k}"] = v
        for k, v in sm.__dict__.items():
            out[k if "pickle" in k else f"disk_{k}"] = v
        return out


def _in_memory_weight(sb_ref: weakref.ref[SpillBuffer], key: str, value: Any) -> int:
    nbytes = safe_sizeof(value)

    sb = sb_ref()
    assert sb is not None
    if sb.shared_memory:
        # If pickle5 buffers are backed by shared memory and are uncompressed, then they
        # won't use any process memory. This assumes that the deserialization of the
        # object won't create a deep copy of the pickle5 buffer.
        try:
            by_ref = sb._slow_uncached.weight_by_key[key].by_ref
            if by_ref:
                # Bare minimum size of a CPython PyObject with a buffer on 64 bit:
                # 8 bytes for class ID
                # 8 bytes for reference counter
                # 8 bytes for pointer to buffer
                return max(24, nbytes - by_ref)
        except KeyError:
            pass

    return nbytes


# Internal exceptions. These are never raised by SpillBuffer.
class MaxSpillExceeded(Exception):
    pass


class PickleError(Exception):
    pass


class HandledError(Exception):
    pass


# zict.Func[str, Any] requires zict >= 2.2.0
class Slow(zict.Func):
    d: zict.File
    max_weight: int | Literal[False]
    weight_by_key: dict[str, SpilledSize]
    total_weight: SpilledSize
    metrics: SlowMetrics

    def __init__(
        self, spill_directory: str, memmap: bool, max_weight: int | Literal[False]
    ):
        if memmap:
            dump = partial(serialize_bytelist, on_error="raise", compression=False)
        else:
            dump = partial(serialize_bytelist, on_error="raise")

        super().__init__(
            dump,
            deserialize_bytes,
            zict.File(spill_directory, memmap=memmap),
        )
        self.max_weight = max_weight
        self.weight_by_key = {}
        self.total_weight = SpilledSize(0, 0)
        self.metrics = SlowMetrics()

    def __getitem__(self, key: str) -> Any:
        t0 = perf_counter()
        pickled = self.d[key]
        w = self.weight_by_key[key]
        assert isinstance(pickled, (bytearray, memoryview) if has_zict_230 else bytes)
        assert len(pickled) == w.by_value + w.by_ref
        t1 = perf_counter()
        out = self.load(pickled)  # type: ignore
        t2 = perf_counter()

        if self.d.memmap:
            # pickle5 buffers have not been touched from the shared memory. This
            # assumes that the deserialization of the object didn't perform a
            # deep copy.
            key_bytes = w.by_value
        else:
            key_bytes = w.by_value + w.by_ref

        # For the sake of simplicity, we're not metering failure use cases.
        self.metrics.log_read(
            key_bytes=key_bytes,
            read_time=t1 - t0,
            unpickle_time=t2 - t1,
        )

        return out

    def __setitem__(self, key: str, value: Any) -> None:
        t0 = perf_counter()
        try:
            # FIXME https://github.com/python/mypy/issues/708
            frames = self.dump(value)  # type: ignore
        except Exception as e:
            # zict.LRU ensures that the key remains in fast if we raise.
            # Wrap the exception so that it's recognizable by SpillBuffer,
            # which will then unwrap it.
            raise PickleError(key, e)

        weight = SpilledSize.from_frames(frames)

        t1 = perf_counter()

        # Thanks to Buffer.__setitem__, we never update existing
        # keys in slow, but always delete them and reinsert them.
        assert key not in self.d
        assert key not in self.weight_by_key

        if (
            self.max_weight is not False
            and sum(self.total_weight) + sum(weight) > self.max_weight
        ):
            # Stop callbacks and ensure that the key ends up in SpillBuffer.fast
            # To be caught by SpillBuffer.__setitem__
            raise MaxSpillExceeded(key)

        # Store to disk through File.
        # This may raise OSError, which is caught by SpillBuffer above.
        self.d[key] = frames

        t2 = perf_counter()

        self.weight_by_key[key] = weight
        self.total_weight += weight

        # For the sake of simplicity, we're not metering failure use cases.
        self.metrics.log_write(
            key_bytes=sum(weight),
            pickle_time=t1 - t0,
            write_time=t2 - t1,
        )

    def __delitem__(self, key: str) -> None:
        super().__delitem__(key)
        self.total_weight -= self.weight_by_key.pop(key)
