"""
This module implements graph computations based on the specification in Dask[1]:
> A computation may be one of the following:
>   - Any key present in the Dask graph like `'x'`
>   - Any other value like `1`, to be interpreted literally
>   - A task like `(inc, 'x')`
>   - A list of computations, like `[1, 'x', (inc, 'x')]`

In order to support efficient and flexible task serialization, this module introduces
classes for computations, tasks, data, functions, etc.

Notable Classes
---------------

- `PickledObject` - An object that are serialized using `protocol.pickle`.
  This object isn't a computation by itself instead users can build pickled
  computations that contains pickled objects. This object is automatically
  de-serialized by the Worker before execution.

- `Computation` - A computation that the Worker can execute. The Scheduler sees
  this as a black box. A computation **cannot** contain pickled objects but it may
  contain `Serialize` and/or `Serialized` objects, which will be de-serialize when
  arriving on the Worker automatically.

- `PickledComputation` - A computation that are serialized using `protocol.pickle`.
  The class is derived from `Computation` but **can** contain pickled objects.
  Pickled objects and itself will be de-serialize by the Worker before execution.

Notable Functions
-----------------

- `typeset_dask_graph()` - Use to typeset a Dask graph, which wrap computations in
  either the `Data` or `Task` class. This should be done before communicating the graph.
  Note, this replaces the old `tlz.valmap(dumps_task, dsk)` operation.

[1] <https://docs.dask.org/en/latest/spec.html>
"""

import threading
import warnings
from typing import Any, Callable, Dict, Iterable, Mapping, MutableMapping, Tuple

import tlz

from dask.core import istask
from dask.utils import apply, format_bytes

from ..utils import LRU
from . import pickle


def identity(x, *args_ignored):
    return x


def execute_task(task, *args_ignored):
    """Evaluate a nested task

    >>> inc = lambda x: x + 1
    >>> execute_task((inc, 1))
    2
    >>> execute_task((sum, [1, 2, (inc, 3)]))
    7
    """
    if istask(task):
        func, args = task[0], task[1:]
        return func(*map(execute_task, args))
    elif isinstance(task, list):
        return list(map(execute_task, task))
    else:
        return task


class PickledObject:
    _value: bytes

    def __init__(self, value: bytes):
        self._value = value

    def __reduce__(self):
        return (type(self), (self._value,))

    @classmethod
    def msgpack_decode(cls, state: Mapping):
        return cls(state["value"])

    def msgpack_encode(self) -> dict:
        return {
            f"__{type(self).__name__}__": True,
            "value": self._value,
        }

    @classmethod
    def serialize(cls, obj) -> "PickledObject":
        return cls(pickle.dumps(obj))

    def deserialize(self):
        return pickle.loads(self._value)


class PickledCallable(PickledObject):
    cache_dumps: MutableMapping[int, bytes] = LRU(maxsize=100)
    cache_loads: MutableMapping[int, Callable] = LRU(maxsize=100)
    cache_max_sized_obj = 1_000_000
    cache_dumps_lock = threading.Lock()

    @classmethod
    def dumps_function(cls, func: Callable) -> bytes:
        """Dump a function to bytes, cache functions"""

        try:
            with cls.cache_dumps_lock:
                ret = cls.cache_dumps[func]
        except KeyError:
            ret = pickle.dumps(func)
            if len(ret) <= cls.cache_max_sized_obj:
                with cls.cache_dumps_lock:
                    cls.cache_dumps[func] = ret
        except TypeError:  # Unhashable function
            ret = pickle.dumps(func)
        return ret

    @classmethod
    def loads_function(cls, dumped_func: bytes):
        """Load a function from bytes, cache bytes"""
        if len(dumped_func) > cls.cache_max_sized_obj:
            return pickle.loads(dumped_func)

        try:
            ret = cls.cache_loads[dumped_func]
        except KeyError:
            cls.cache_loads[dumped_func] = ret = pickle.loads(dumped_func)
        return ret

    @classmethod
    def serialize(cls, func: Callable) -> "PickledCallable":
        if isinstance(func, cls):
            return func
        else:
            return cls(cls.dumps_function(func))

    def deserialize(self) -> Callable:
        return self.loads_function(self._value)

    def __call__(self, *args, **kwargs):
        return self.deserialize()(*args, **kwargs)


class Computation:
    def __init__(self, value, is_a_task: bool):
        self._value = value
        self._is_a_task = is_a_task

    @classmethod
    def msgpack_decode(cls, state: Mapping):
        return cls(state["value"], state["is_a_task"])

    def msgpack_encode(self) -> dict:
        return {
            f"__{type(self).__name__}__": True,
            "value": self._value,
            "is_a_task": self._is_a_task,
        }

    def get_func_and_args(self) -> Tuple[Callable, Iterable, Mapping]:
        if self._is_a_task:
            return (execute_task, (self._value,), {})
        else:
            return (identity, (self._value,), {})

    def get_computation(self) -> "Computation":
        return self


class PickledComputation(Computation):
    _size_warning_triggered: bool = False
    _size_warning_limit: int = 1_000_000

    @classmethod
    def serialize(cls, value, is_a_task: bool):
        data = pickle.dumps(value)
        ret = cls(data, is_a_task)
        if not cls._size_warning_triggered and len(data) > cls._size_warning_limit:
            cls._size_warning_triggered = True
            s = str(value)
            if len(s) > 70:
                s = s[:50] + " ... " + s[-15:]
            warnings.warn(
                "Large object of size %s detected in task graph: \n"
                "  %s\n"
                "Consider scattering large objects ahead of time\n"
                "with client.scatter to reduce scheduler burden and \n"
                "keep data on workers\n\n"
                "    future = client.submit(func, big_data)    # bad\n\n"
                "    big_future = client.scatter(big_data)     # good\n"
                "    future = client.submit(func, big_future)  # good"
                % (format_bytes(len(data)), s)
            )
        return ret

    def deserialize(self):
        def inner_deserialize(obj):
            if isinstance(obj, list):
                return [inner_deserialize(o) for o in obj]
            elif istask(obj):
                return tuple(inner_deserialize(o) for o in obj)
            elif isinstance(obj, PickledObject):
                return obj.deserialize()
            else:
                return obj

        return inner_deserialize(pickle.loads(self._value))

    def get_computation(self) -> Computation:
        return Computation(self.deserialize(), self._is_a_task)

    def get_func_and_args(self) -> Tuple[Callable, Iterable, Mapping]:
        return self.get_computation().get_func_and_args()


def typeset_computation(computation) -> Computation:
    from .serialize import Serialize, Serialized

    if isinstance(computation, Computation):
        return computation  # Already a computation

    contain_pickled = [False]
    contain_tasks = [False]

    def serialize_callables(obj):
        if isinstance(obj, list):
            return [serialize_callables(o) for o in obj]
        elif istask(obj):
            contain_tasks[0] = True
            if obj[0] is apply:
                return (apply, PickledCallable.serialize(obj[1])) + tuple(
                    map(serialize_callables, obj[2:])
                )
            else:
                return (PickledCallable.serialize(obj[0]),) + tuple(
                    map(serialize_callables, obj[1:])
                )
        elif isinstance(obj, PickledObject):
            contain_pickled[0] = True
            return obj
        else:
            assert not isinstance(obj, (Serialize, Serialized)), obj
            return obj

    computation = serialize_callables(computation)
    if contain_tasks[0]:
        return PickledComputation.serialize(computation, is_a_task=True)
    elif contain_pickled[0]:
        return PickledComputation.serialize(computation, is_a_task=False)
    else:
        return Computation(Serialize(computation), is_a_task=False)


def typeset_dask_graph(dsk: Mapping[str, Any]) -> Dict[str, Computation]:
    return tlz.valmap(typeset_computation, dsk)
