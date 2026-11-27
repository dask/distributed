from __future__ import annotations

import asyncio
import contextvars
import os
import sys
import threading
import uuid
import warnings
import weakref
from collections import deque
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager, nullcontext
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from distributed.comm.core import Comm

# Python 3.14t makes warnings filters context-local instead of process-global.
# This is also available as an opt-in on GIL-enabled Python 3.14+; see
# https://docs.python.org/3/using/cmdline.html#envvar-PYTHON_CONTEXT_AWARE_WARNINGS
#
# pytest.warns and pytest.mark.filterwarnings enter warnings.catch_warnings() on
# the test thread. Distributed runs clients, schedulers, and workers on long-lived
# event loop threads and tasks which were started before the test entered that
# context, so they would otherwise keep the global ``-W error`` filter and either
# raise or fail to record warnings issued by RPC handlers and tasks.
#
# Warning filters are meaningful only within the process that created them.
# During the comm handshake, peers exchange an ephemeral token; when both
# endpoints live in the same process, they pair up over an in-memory FIFO side
# channel without altering the wire protocol or frame layout for any transport.
# Remote peers ignore the unknown handshake key and incur zero per-message cost.

_HANDSHAKE_KEY = "_warnings_peer"
_T = TypeVar("_T")

_WARNINGS_CONTEXT_VAR: contextvars.ContextVar[object] | None = getattr(
    warnings, "_warnings_context", None
)
CONTEXT_AWARE_WARNINGS = bool(
    getattr(sys.flags, "context_aware_warnings", False)
) and isinstance(_WARNINGS_CONTEXT_VAR, contextvars.ContextVar)

_NO_CONTEXT = nullcontext()
_batched_contexts_var: contextvars.ContextVar[list[object | None] | None] = (
    contextvars.ContextVar("_distributed_batched_warning_contexts", default=None)
)


class _Channel:
    __slots__ = ("incoming", "__weakref__")

    def __init__(self) -> None:
        self.incoming: deque[Any] = deque()


_pending_handshakes: weakref.WeakValueDictionary[bytes, _Channel] = (
    weakref.WeakValueDictionary()
)
_pending_lock: Any = threading.Lock()


def _clear_after_fork() -> None:
    # A forked child must not resolve tokens created by its parent, and the lock
    # may have been held by another thread at the time of the fork.
    global _pending_handshakes, _pending_lock
    _pending_handshakes = weakref.WeakValueDictionary()
    _pending_lock = threading.Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_clear_after_fork)


class ContextMessage(dict):
    """A decoded message carrying the warnings context of its sender."""

    __slots__ = ("warning_context",)

    def __init__(self, msg: dict, warning_context: object | None = None) -> None:
        super().__init__(msg)
        self.warning_context = warning_context


@contextmanager
def _set_contextvar(var: contextvars.ContextVar[_T], value: _T) -> Iterator[None]:
    token = var.set(value)
    try:
        yield
    finally:
        try:
            var.reset(token)
        except ValueError:
            # A suspended generator/coroutine may be garbage-collected in a
            # different Context; its Context is already being discarded.
            pass


def capture_warning_context() -> object | None:
    """Return the context-local warnings state, if set by catch_warnings()."""
    if CONTEXT_AWARE_WARNINGS and _WARNINGS_CONTEXT_VAR is not None:
        return _WARNINGS_CONTEXT_VAR.get(None)
    return None


def use_batched_warning_contexts(
    contexts: list[object | None],
) -> AbstractContextManager[None]:
    """Pass enqueue-time warning contexts from BatchedSend to its comm write."""
    if not CONTEXT_AWARE_WARNINGS:
        return _NO_CONTEXT
    return _set_contextvar(_batched_contexts_var, contexts)


def _wrap_item(item: Any, ctx: object | None) -> Any:
    if ctx is None:
        return dict(item) if type(item) is ContextMessage else item
    if type(item) in (dict, ContextMessage):
        return ContextMessage(item, ctx)
    return item


def _wrap_decoded_message(msg: Any, entry: Any) -> Any:
    if entry is None:
        return dict(msg) if type(msg) is ContextMessage else msg
    if type(msg) in (dict, ContextMessage) and not isinstance(entry, list):
        return ContextMessage(msg, entry)
    if type(msg) in (list, tuple):
        if isinstance(entry, list):
            if len(msg) != len(entry):
                return msg
            return type(msg)(_wrap_item(item, ctx) for item, ctx in zip(msg, entry))
        return type(msg)(_wrap_item(item, entry) for item in msg)
    return msg


def _attach_local_comm(
    comm: Comm,
    channel: _Channel,
    outgoing: deque[Any],
) -> None:
    """Wrap an in-process comm pair to propagate warnings contexts out of band."""
    comm_ref = weakref.ref(comm)
    incoming = channel.incoming
    write_lock = asyncio.Lock()

    def write(msg: Any, *args: Any, **kwargs: Any) -> Any:
        self = comm_ref()
        assert self is not None
        batched = _batched_contexts_var.get(None)
        if batched is not None:
            _batched_contexts_var.set(None)
            entry: Any = batched if any(c is not None for c in batched) else None
        else:
            entry = capture_warning_context()

        async def _write() -> Any:
            async with write_lock:
                outgoing.append(entry)
                try:
                    return await type(self).write(self, msg, *args, **kwargs)
                except BaseException:
                    # Nothing was written (closed comm, serialization error, or
                    # cancellation before writing) or the comm was aborted.
                    try:
                        outgoing.pop()
                    except IndexError:
                        pass
                    raise

        return _write()

    def read(*args: Any, **kwargs: Any) -> Any:
        self = comm_ref()
        assert self is not None

        async def _read() -> Any:
            # Hold `channel` for the lifetime of `comm` so the peer can still
            # resolve our handshake token if it finishes handshake after us.
            _ = channel
            msg = await type(self).read(self, *args, **kwargs)
            try:
                entry = incoming.popleft()
            except IndexError:
                return msg
            return _wrap_decoded_message(msg, entry)

        return _read()

    comm.write = write  # type: ignore[method-assign]
    comm.read = read  # type: ignore[method-assign]


async def exchange_handshake(comm: Comm, local_info: dict[str, Any]) -> dict[str, Any]:
    """Exchange comm handshake info and pair in-process peers for warnings context."""
    if not CONTEXT_AWARE_WARNINGS:
        await comm.write(local_info)
        return await comm.read()

    token = uuid.uuid4().bytes
    channel = _Channel()
    with _pending_lock:
        _pending_handshakes[token] = channel
    try:
        # Copy `local_info` so InProc comms do not share a mutated dict.
        await comm.write({**local_info, _HANDSHAKE_KEY: token})
        handshake = await comm.read()
    except BaseException:
        with _pending_lock:
            _pending_handshakes.pop(token, None)
        raise

    peer_token = (
        handshake.pop(_HANDSHAKE_KEY, None) if isinstance(handshake, dict) else None
    )
    if isinstance(peer_token, bytes):
        with _pending_lock:
            peer_channel = _pending_handshakes.pop(peer_token, None)
    else:
        peer_channel = None

    if peer_channel is not None:
        _attach_local_comm(comm, channel, peer_channel.incoming)
    else:
        # Remote peer (different process or older version) will not pop our token.
        with _pending_lock:
            _pending_handshakes.pop(token, None)

    return handshake


def warning_context_for(msg: Any) -> AbstractContextManager[None]:
    """Apply the sender's warnings context in the current event loop task."""
    if (
        isinstance(msg, ContextMessage)
        and msg.warning_context is not None
        and _WARNINGS_CONTEXT_VAR is not None
    ):
        return _set_contextvar(_WARNINGS_CONTEXT_VAR, msg.warning_context)
    return _NO_CONTEXT
