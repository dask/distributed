import inspect
from contextvars import ContextVar, Token
from typing import Any, Callable

from dask.utils import funcname

from distributed.metrics import time


class STIMULUS_ID:
    STIMULUS_ID_CTXVAR: ContextVar = ContextVar("stimulus_id")

    @classmethod
    def from_function(cls, func: Callable[..., Any]):
        return cls.from_name(funcname(func).replace("_", "-"))

    @classmethod
    def from_name(cls, name: str):
        return f"{name}-{time()}"

    @classmethod
    def from_callstack(cls, depth: int = 1):
        """ """
        original_frame = caller = inspect.currentframe()
        original_depth = depth

        while caller and depth:
            caller = caller.f_back
            depth -= 1

        if depth:
            raise ValueError(
                f"Could not traverse up {original_depth} "
                f"frames from {original_frame}"
            )

        assert caller
        name = caller.f_code.co_name.replace("_", "-")
        return f"{name}-{time()}"

    @classmethod
    def get(cls, on_error: str = "generate"):
        try:
            return cls.STIMULUS_ID_CTXVAR.get()
        except LookupError:
            if on_error == "generate":
                stimulus_id = cls.from_callstack(depth=2)
                cls.STIMULUS_ID_CTXVAR.set(stimulus_id)
                return stimulus_id
            elif on_error == "raise":
                raise
            else:
                raise ValueError(f"'{on_error}' not in {'raise', 'generate'}")

    @classmethod
    def set(cls, value: str):
        return cls.STIMULUS_ID_CTXVAR.set(value)

    @classmethod
    def reset(cls, token: Token):
        return cls.STIMULUS_ID_CTXVAR.reset(token)
