from __future__ import annotations

import contextvars
import os
import sys
import threading
import uuid
import warnings
from collections import OrderedDict
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager, nullcontext
from typing import Any

# Python 3.14t makes warnings filters context-local instead of process-global.
# This is also available as an opt-in on GIL-enabled Python 3.14+; see
# https://docs.python.org/3/using/cmdline.html#envvar-PYTHON_CONTEXT_AWARE_WARNINGS
#
# pytest.warns and pytest.mark.filterwarnings enter warnings.catch_warnings() on
# the test thread. Distributed runs clients, schedulers, and workers on long-lived
# event loop threads which were started before the test entered that context, so
# they would otherwise keep the global ``-W error`` filter and either raise or
# fail to record warnings issued by RPC handlers and tasks.
#
# The current warnings context is carried out of band: as an extra protocol frame
# for serialized comms, and as an envelope object for InProc comms. Adding a key
# to the messages themselves would change their serialized header, which in turn
# changes zero-copy buffer-sharing decisions.
#
# The context reference is deliberately not serialized: warning filters are
# meaningful only within the process that created them, and warnings from a
# genuinely remote process are surfaced to clients through the existing "warn"
# event mechanism.

_FRAME_MARKER = b"__distributed_warnings_context__"
_FRAME_SIZE = len(_FRAME_MARKER) + 16  # UUID bytes
_MAX_REGISTRY_SIZE = 4096

_WARNINGS_CONTEXT_VAR: contextvars.ContextVar[object] | None = getattr(
    warnings, "_warnings_context", None
)
CONTEXT_AWARE_WARNINGS = bool(
    getattr(sys.flags, "context_aware_warnings", False)
) and isinstance(_WARNINGS_CONTEXT_VAR, contextvars.ContextVar)

_NO_CONTEXT = nullcontext()
_registry: OrderedDict[bytes, Any] = OrderedDict()
_registry_lock: Any = threading.Lock()


def _clear_registry_after_fork() -> None:
    # A forked child must not resolve tokens created by its parent, and the lock
    # may have been held by another thread at the time of the fork.
    global _registry, _registry_lock
    _registry = OrderedDict()
    _registry_lock = threading.Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_clear_registry_after_fork)


class ContextMessage(dict):
    """A decoded message carrying the warnings context of its sender."""

    __slots__ = ("warning_context",)

    def __init__(self, msg: dict, warning_context: object | None = None) -> None:
        super().__init__(msg)
        self.warning_context = warning_context


class DirectMessage:
    """Envelope used by InProc comms, which do not serialize their messages."""

    def __init__(self, message: Any, context_tree: Any) -> None:
        self.message = message
        self.context_tree = context_tree


class ContextFrames(list):
    """Protocol frames with a trailing warnings context reference."""


class BatchedMessages(list):
    """Use the context at enqueue time, not the batch writer's context."""


def capture_warning_context() -> object | None:
    """Return the context-local warnings state, if set by catch_warnings()."""
    if CONTEXT_AWARE_WARNINGS and _WARNINGS_CONTEXT_VAR is not None:
        return _WARNINGS_CONTEXT_VAR.get(None)
    return None


def encode_context_frame(context_tree: Any) -> bytes:
    """Serialize a process-local reference to the warnings context tree."""
    token = uuid.uuid4().bytes
    with _registry_lock:
        if len(_registry) >= _MAX_REGISTRY_SIZE:
            _registry.popitem(last=False)
        _registry[token] = context_tree
    return _FRAME_MARKER + token


def pop_context_frame(frames: list) -> tuple[list, Any]:
    """Detach a local context reference without copying ordinary payload buffers."""
    if not CONTEXT_AWARE_WARNINGS or len(frames) < 2:
        return frames, None
    frame = frames[-1]
    if isinstance(frame, memoryview) and frame.nbytes == _FRAME_SIZE:
        frame = bytes(frame)
    if not (
        isinstance(frame, bytes)
        and len(frame) == _FRAME_SIZE
        and frame.startswith(_FRAME_MARKER)
    ):
        return frames, None
    with _registry_lock:
        context_tree = _registry.pop(frame[len(_FRAME_MARKER) :], None)
    # Remote and expired references are unreferenced trailing frames, which the
    # object protocol already ignores. Do not strip an ordinary payload frame.
    if context_tree is None:
        return frames, None
    return frames[:-1], context_tree


def _wrap_outgoing(msg: Any, warning_context: object | None) -> Any:
    if isinstance(msg, ContextMessage):
        return msg
    if type(msg) is dict:
        return ContextMessage(msg, warning_context)
    if type(msg) is list:
        return [_wrap_outgoing(item, warning_context) for item in msg]
    if type(msg) is tuple:
        return tuple(_wrap_outgoing(item, warning_context) for item in msg)
    return msg


def wrap_outgoing_message(msg: Any) -> Any:
    """Capture the warnings context when a message is enqueued by BatchedSend."""
    context = capture_warning_context()
    return msg if context is None else _wrap_outgoing(msg, context)


def _split_outgoing(msg: Any, current: object | None) -> tuple[Any, Any]:
    if isinstance(msg, ContextMessage):
        return dict(msg), msg.warning_context
    if type(msg) is dict:
        return msg, current
    if type(msg) in (list, tuple, BatchedMessages):
        if isinstance(msg, BatchedMessages):
            current = None
        items, contexts = [], []
        for item in msg:
            clean, context = _split_outgoing(item, current)
            items.append(clean)
            contexts.append(context)
        return (tuple(items) if type(msg) is tuple else items), (
            contexts if any(c is not None for c in contexts) else None
        )
    return msg, None


def split_outgoing_message(msg: Any) -> tuple[Any, Any]:
    """Strip context wrappers and return the context tree for serialization."""
    if not CONTEXT_AWARE_WARNINGS:
        return msg, None
    return _split_outgoing(msg, capture_warning_context())


def wrap_decoded_message(msg: Any, context_tree: Any) -> Any:
    """Attach the resolved warnings context to decoded top-level messages."""
    if not CONTEXT_AWARE_WARNINGS or context_tree is None:
        return msg
    if (
        isinstance(context_tree, list)
        and type(msg) in (list, tuple)
        and len(msg) == len(context_tree)
    ):
        return type(msg)(
            wrap_decoded_message(item, context)
            for item, context in zip(msg, context_tree)
        )
    if type(msg) is dict and not isinstance(context_tree, list):
        return ContextMessage(msg, context_tree)
    return msg


@contextmanager
def _use_warning_context(context: object) -> Iterator[None]:
    assert _WARNINGS_CONTEXT_VAR is not None
    token = _WARNINGS_CONTEXT_VAR.set(context)
    try:
        yield
    finally:
        _WARNINGS_CONTEXT_VAR.reset(token)


def warning_context_for(msg: Any) -> AbstractContextManager[None]:
    """Apply the sender's warnings context in the current event loop task."""
    if isinstance(msg, ContextMessage) and msg.warning_context is not None:
        return _use_warning_context(msg.warning_context)
    return _NO_CONTEXT
