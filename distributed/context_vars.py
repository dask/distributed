import inspect
from contextvars import ContextVar, Token
from typing import Any, Callable, Tuple, Union

from dask.utils import funcname

from distributed.metrics import time

STIMULUS_ID_CTXVAR: ContextVar = ContextVar("stimulus_id")


class STIMULUS_ID:
    """Mediates access to STIMULUS_ID_CTXVAR"""

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
    def get(cls, on_error: Union[str, bool] = "generate", default: Any = None):
        """Gets the stimulus_id from the current context.

        Parameters
        ----------
        on_error: {"generate", "raise", "default", True, False}
            If the context does not have a stimulus_id, this will be used to
            generate one. If the value is "generate" or False, a new stimulus_id
            will be generated. If the value is "raise" or True, the
            original LookupError will be re-reraised.
            If `on_error` is "default", `default` will be returned.
        default : optional
            The default value to return if the context does not have a stimulus_id.

        Returns
        -------
        stimulus_id : str
            The stimulus_id from the current context.
        """
        try:
            return STIMULUS_ID_CTXVAR.get()
        except LookupError:
            if on_error is False or on_error == "generate":
                stimulus_id = cls.from_callstack(depth=2)
                STIMULUS_ID_CTXVAR.set(stimulus_id)
                return stimulus_id
            elif on_error is True or on_error == "raise":
                raise
            elif on_error == "default":
                return default
            else:
                raise ValueError(
                    f"'{on_error}' not in {'raise', 'generate', 'default', True, False}"
                )

    @classmethod
    def set(cls, value: str):
        return STIMULUS_ID_CTXVAR.set(value)

    def setdefault(cls, value: str, return_token=False) -> Union[str, Tuple[str, Any]]:
        token: Any

        try:
            stimulus_id = STIMULUS_ID_CTXVAR.get()
        except LookupError:
            stimulus_id = value
            token = STIMULUS_ID_CTXVAR.set(value)
        else:
            token = Token.MISSING

        if return_token:
            return stimulus_id, token

        return stimulus_id

    @classmethod
    def reset(cls, token: Token):
        return STIMULUS_ID_CTXVAR.reset(token)
