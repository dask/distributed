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
# This module attaches a process-local reference to the current warnings context
# to outgoing messages and restores it around the matching remote handler. The
# reference is deliberately not serialized: warning filters are meaningful only
# within the process that created them, and warnings from a genuinely remote
# process are surfaced to clients through the existing "warn" event mechanism.

CONTEXT_KEY = "__distributed_warnings_context__"
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


def _register_warning_context(warning_context: object) -> dict[str, object]:
    token = uuid.uuid4().hex
    with _registry_lock:
        _registry[token] = warning_context
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


def _resolve_warning_context(payload: dict[str, object]) -> object | None:
    if payload["pid"] != os.getpid():
        return None
    token = cast(str, payload["token"])
    with _registry_lock:
        warning_context = _registry.get(token)
        if warning_context is not None:
            _registry.move_to_end(token)
    return warning_context


def pop_message_context(msg: dict) -> tuple[dict, object | None]:
    """Strip the internal context key from a decoded msgpack object."""
    payload = msg.get(CONTEXT_KEY, _MISSING)
    if not _is_context_token(payload):
        return msg, None
    out = dict(msg)
    del out[CONTEXT_KEY]
    return out, _resolve_warning_context(cast(dict[str, object], payload))


def _attach_warning_context(msg: Any, token: dict[str, object]) -> Any:
    if isinstance(msg, ContextMessage):
        if CONTEXT_KEY in msg:
            return msg
        out = dict(msg)
        if msg.warning_context is None:
            out[CONTEXT_KEY] = token
        else:
            out[CONTEXT_KEY] = _register_warning_context(msg.warning_context)
        return out
    # Only top-level messages and the elements of a BatchedSend payload receive
    # the key. Adding it to arbitrary nested user data would change its meaning.
    if type(msg) is dict:
        if CONTEXT_KEY in msg:
            return msg
        out = dict(msg)
        out[CONTEXT_KEY] = token
        return out
    if type(msg) is list:
        return [_attach_warning_context(item, token) for item in msg]
    if type(msg) is tuple:
        return tuple(_attach_warning_context(item, token) for item in msg)
    return msg


def attach_warning_context(msg: Any, warning_context: object | None = None) -> Any:
    """Attach the current warnings context to an outgoing message or batch.

    BatchedSend calls this when messages are enqueued; to_frames calls it as a
    fallback for direct comm writes. The latter must not overwrite a context
    captured earlier at enqueue time.
    """
    if not CONTEXT_AWARE_WARNINGS:
        return msg
    if warning_context is None:
        warning_context = capture_warning_context()
    if warning_context is None:
        return msg
    return _attach_warning_context(msg, _register_warning_context(warning_context))


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
