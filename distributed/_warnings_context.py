from __future__ import annotations

import contextvars
import inspect
import os
import sys
import threading
import uuid
import warnings
from collections import OrderedDict
from collections.abc import Callable
from typing import Any, Literal, cast

import msgpack

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

FRAME_MARKER = b"__distributed_warnings_context__"
CONTEXT_KEY = "__distributed_warnings_context__"  # legacy keys are stripped
_MARKER = "distributed.warnings_context"
_MAX_REGISTRY_SIZE = 4096

_WARNINGS_CONTEXT_VAR: contextvars.ContextVar[object] | None = cast(
    Any, getattr(warnings, "_warnings_context", None)
)
_WARNINGS_GLOBAL_CONTEXT: Any = getattr(warnings, "_global_context", None)
CONTEXT_AWARE_WARNINGS = bool(
    getattr(sys.flags, "context_aware_warnings", False)
) and isinstance(_WARNINGS_CONTEXT_VAR, contextvars.ContextVar)

_MISSING = object()
_registry: OrderedDict[str, object] = OrderedDict()
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

    warning_context: object | None

    def __init__(self, msg: dict, warning_context: object | None = None) -> None:
        super().__init__(msg)
        self.warning_context = warning_context


class DirectMessage:
    """Envelope used by InProc comms, which do not serialize their messages."""

    def __init__(self, message: Any, context_tree: Any) -> None:
        self.message = message
        self.context_tree = context_tree


class Frames(list):
    """Protocol frames carrying their warnings context as out-of-band metadata."""

    def __init__(self, frames: list, context_tree: Any = None) -> None:
        super().__init__(frames)
        self.context_tree = context_tree


def get_warning_context(msg: Any) -> object | None:
    """Return the warnings context attached to a decoded message, if any."""
    if isinstance(msg, ContextMessage):
        return msg.warning_context
    return None


def capture_warning_context() -> object | None:
    """Return the context-local warnings state, or None when there is none.

    When ``warnings.catch_warnings()`` has never been entered, or after it has
    exited, the context variable is unset or points at the global context. In
    either case remote threads already observe the process-global filters and
    there is nothing to propagate.
    """
    if not CONTEXT_AWARE_WARNINGS or _WARNINGS_CONTEXT_VAR is None:
        return None
    warning_context = _WARNINGS_CONTEXT_VAR.get(_MISSING)
    if (
        warning_context is _MISSING
        or warning_context is None
        or warning_context is _WARNINGS_GLOBAL_CONTEXT
        or type(warning_context).__name__ == "_GlobalContext"
    ):
        return None
    return warning_context


def _register_context(value: object) -> dict[str, object]:
    token = uuid.uuid4().hex
    with _registry_lock:
        _registry[token] = value
        _registry.move_to_end(token)
        while len(_registry) > _MAX_REGISTRY_SIZE:
            _registry.popitem(last=False)
    return {"marker": _MARKER, "pid": os.getpid(), "token": token}


def _is_context_token(payload: Any) -> bool:
    return (
        isinstance(payload, dict)
        and payload.get("marker") == _MARKER
        and isinstance(payload.get("pid"), int)
        and isinstance(payload.get("token"), str)
    )


def _resolve_context(payload: dict[str, object]) -> object | None:
    if payload["pid"] != os.getpid():
        return None
    token = cast(str, payload["token"])
    with _registry_lock:
        value = _registry.get(token)
        if value is not None:
            _registry.move_to_end(token)
    return value


def encode_context_frame(context_tree: Any) -> bytes:
    """Serialize a process-local reference to the warnings context tree."""
    payload = _register_context(context_tree)
    return FRAME_MARKER + msgpack.dumps(payload, use_bin_type=True)


def pop_context_frame(frames: list) -> tuple[list, Any]:
    """Remove the trailing context frame, if present, and resolve its payload."""
    if not frames:
        return frames, None
    try:
        data = bytes(frames[-1])
    except (TypeError, ValueError):
        return frames, None
    if not data.startswith(FRAME_MARKER):
        return frames, None
    try:
        payload = msgpack.loads(data[len(FRAME_MARKER) :], raw=False)
    except Exception:
        return frames, None
    if not _is_context_token(payload):
        return frames, None
    return frames[:-1], _resolve_context(payload)


def has_warning_context(context_tree: Any) -> bool:
    if isinstance(context_tree, list):
        return any(has_warning_context(item) for item in context_tree)
    return context_tree is not None


def _context_frame(frames: Any) -> bytes | None:
    context_tree = getattr(frames, "context_tree", None)
    if not CONTEXT_AWARE_WARNINGS or not has_warning_context(context_tree):
        return None
    return encode_context_frame(context_tree)


def append_context_frame(frames: Any) -> list:
    """Return plain frames with the context metadata in a trailing frame."""
    context_frame = _context_frame(frames)
    if context_frame is None:
        return list(frames)
    return [*frames, context_frame]


def detach_context_frame(frames: Any) -> tuple[list, bytes | None]:
    """Return plain frames and the context metadata as a separate frame."""
    return list(frames), _context_frame(frames)


def _clean_dict(msg: dict) -> dict:
    out = dict(msg)
    out.pop(CONTEXT_KEY, None)
    return out


def _wrap_outgoing(msg: Any, warning_context: object | None) -> Any:
    if isinstance(msg, ContextMessage):
        return msg
    if type(msg) is dict:
        out = _clean_dict(msg)
        return ContextMessage(out, warning_context) if warning_context else out
    if type(msg) is list:
        return [_wrap_outgoing(item, warning_context) for item in msg]
    if type(msg) is tuple:
        return tuple(_wrap_outgoing(item, warning_context) for item in msg)
    return msg


def wrap_outgoing_message(msg: Any, warning_context: object | None = None) -> Any:
    """Capture the warnings context when a message is enqueued by BatchedSend."""
    if not CONTEXT_AWARE_WARNINGS:
        return msg
    if warning_context is None:
        warning_context = capture_warning_context()
    return _wrap_outgoing(msg, warning_context)


def _split_outgoing(msg: Any, current: object | None) -> tuple[Any, Any]:
    if isinstance(msg, ContextMessage):
        return _clean_dict(msg), msg.warning_context
    if type(msg) is dict:
        return _clean_dict(msg), current
    if type(msg) is list:
        items = []
        contexts = []
        for item in msg:
            clean, context = _split_outgoing(item, current)
            items.append(clean)
            contexts.append(context)
        return items, contexts if has_warning_context(contexts) else None
    if type(msg) is tuple:
        items = []
        contexts = []
        for item in msg:
            clean, context = _split_outgoing(item, current)
            items.append(clean)
            contexts.append(context)
        return tuple(items), contexts if has_warning_context(contexts) else None
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
    if isinstance(context_tree, list) and type(msg) in (list, tuple):
        return type(msg)(
            wrap_decoded_message(item, context)
            for item, context in zip(msg, context_tree)
        )
    if type(msg) is dict and not isinstance(context_tree, list):
        return ContextMessage(msg, context_tree)
    return msg


def _set_warning_context(warning_context: object | None) -> Any:
    """Override the warnings context of the current event loop task."""
    if (
        warning_context is None
        or not CONTEXT_AWARE_WARNINGS
        or _WARNINGS_CONTEXT_VAR is None
    ):
        return None
    return _WARNINGS_CONTEXT_VAR.set(warning_context)


def _reset_warning_context(token: Any) -> None:
    if token is not None:
        assert _WARNINGS_CONTEXT_VAR is not None
        _WARNINGS_CONTEXT_VAR.reset(token)


def call_sync_in_warning_context(
    func: Callable[..., Any],
    warning_context: object | None,
    /,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Call a synchronous handler with the sender's warnings context."""
    token = _set_warning_context(warning_context)
    try:
        return func(*args, **kwargs)
    finally:
        _reset_warning_context(token)


async def call_in_warning_context(
    func: Callable[..., Any],
    warning_context: object | None,
    /,
    *args: Any,
    await_mode: Literal["none", "coroutine", "awaitable"] = "awaitable",
    **kwargs: Any,
) -> Any:
    """Call a handler with the sender's warnings context.

    Set the context variable on the current Task instead of running the coroutine
    in a new Task. Creating a Task would change ``asyncio.current_task()``, which
    server handlers such as ``Server.close`` rely on.
    """
    token = _set_warning_context(warning_context)
    try:
        result = func(*args, **kwargs)
        if await_mode == "none" or not inspect.isawaitable(result):
            return result
        if await_mode == "coroutine" and not inspect.iscoroutine(result):
            raise RuntimeError(
                "Comm handler returned unknown awaitable. Expected coroutine, "
                f"instead got {type(result)}"
            )
        return await result
    finally:
        _reset_warning_context(token)
