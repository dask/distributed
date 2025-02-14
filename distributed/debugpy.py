from __future__ import annotations

import importlib.util
import logging
import sys
import threading

import dask.config

logger = logging.getLogger(__name__)

DEBUGPY_ENABLED: bool = dask.config.get("distributed.diagnostics.debugpy.enabled")
DEBUGPY_PORT: int = dask.config.get("distributed.diagnostics.debugpy.port")


def _check_debugpy_installed():
    if importlib.util.find_spec("debugpy") is None:
        raise ModuleNotFoundError(
            "Dask debugger requires debugpy. Please make sure it is installed."
        )


LOCK = threading.Lock()


def _ensure_debugpy_listens() -> tuple[str, int]:
    import debugpy

    from distributed.worker import get_worker

    worker = get_worker()

    with LOCK:
        if endpoint := worker.extensions.get("debugpy", None):
            return endpoint
        endpoint = debugpy.listen(("0.0.0.0", DEBUGPY_PORT))
        worker.extensions["debugpy"] = endpoint
        return endpoint


def breakpointhook() -> None:
    import debugpy

    host, port = _ensure_debugpy_listens()
    if not debugpy.is_client_connected():
        logger.warning(
            "Breakpoint encountered; waiting for client to attach to %s:%d...",
            host,
            port,
        )
        debugpy.wait_for_client()

    debugpy.breakpoint()


def post_mortem() -> None:
    # Based on https://github.com/microsoft/debugpy/issues/723
    import debugpy

    host, port = _ensure_debugpy_listens()
    if not debugpy.is_client_connected():
        logger.warning(
            "Exception encountered; waiting for client to attach to %s:%d...",
            host,
            port,
        )
        debugpy.wait_for_client()

    import pydevd

    py_db = pydevd.get_global_debugger()
    thread = threading.current_thread()
    additional_info = py_db.set_additional_thread_info(thread)
    additional_info.is_tracing += 1
    try:
        error = sys.exc_info()
        py_db.stop_on_unhandled_exception(py_db, thread, additional_info, error)
    finally:
        additional_info.is_tracing -= 1


if DEBUGPY_ENABLED:
    _check_debugpy_installed()
    sys.breakpointhook = breakpointhook
