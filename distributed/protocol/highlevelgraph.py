import msgpack
from tlz import valmap

from dask.core import keys_in_tasks
from dask.highlevelgraph import HighLevelGraph, Layer

from ..utils_comm import unpack_remotedata, subs_multiple
from ..worker import dumps_task

from ..utils import (
    str_graph,
    tokey,
    CancelledError,
)

from .utils import (
    msgpack_opts,
)
from .serialize import (
    import_allowed_module,
    msgpack_encode_default,
    msgpack_decode_default,
)


def _materialized_layer_pack(
    layer: Layer,
    all_keys,
    known_key_dependencies,
    client_keys,
    allowed_client,
    allowed_futures,
):
    from ..client import Future

    dsk = dict(layer)

    # Find aliases not in `client_keys` and substitute all matching keys
    # with its Future
    values = {
        k: v for k, v in dsk.items() if isinstance(v, Future) and k not in client_keys
    }
    if values:
        dsk = subs_multiple(dsk, values)

    # Unpack remote data re record its dependencies
    dsk = {k: unpack_remotedata(v, byte_keys=True) for k, v in layer.items()}
    unpacked_futures = set.union(*[v[1] for v in dsk.values()]) if dsk else set()
    for future in unpacked_futures:
        if future.client is not allowed_client:
            raise ValueError(
                "Inputs contain futures that were created by another client."
            )
        if tokey(future.key) not in allowed_futures:
            raise CancelledError(tokey(future.key))
    unpacked_futures_deps = {}
    for k, v in dsk.items():
        if len(v[1]):
            unpacked_futures_deps[k] = {f.key for f in v[1]}
    dsk = {k: v[0] for k, v in dsk.items()}

    # Calculate dependencies without re-calculating already known dependencies
    missing_keys = set(dsk.keys()).difference(known_key_dependencies.keys())
    dependencies = {
        k: keys_in_tasks(all_keys, [dsk[k]], as_list=False) for k in missing_keys
    }
    for k, v in unpacked_futures_deps.items():
        dependencies[k] = set(dependencies.get(k, ())) | v

    # The scheduler expect all keys to be strings
    dependencies = {
        tokey(k): [tokey(dep) for dep in deps] for k, deps in dependencies.items()
    }
    dsk = str_graph(dsk, extra_values=all_keys)
    dsk = valmap(dumps_task, dsk)
    return {"dsk": dsk, "dependencies": dependencies}


def highlevelgraph_pack(
    hlg: HighLevelGraph, client_keys, allowed_client, allowed_futures
):
    layers = []

    # Dump each layer (in topological order)
    for layer in (hlg.layers[name] for name in hlg._toposort_layers()):
        if not layer.is_materialized():
            state = layer.__dask_distributed_pack__()
            if state is not None:
                layers.append(
                    {
                        "__module__": layer.__module__,
                        "__name__": type(layer).__name__,
                        "state": state,
                    }
                )
                continue

        # Falling back to the default serialization, which will materialize the layer
        layers.append(
            {
                "__module__": None,
                "__name__": None,
                "state": _materialized_layer_pack(
                    layer,
                    hlg.get_all_external_keys(),
                    hlg.key_dependencies,
                    client_keys,
                    allowed_client,
                    allowed_futures,
                ),
            }
        )

    return msgpack.dumps({"layers": layers}, default=msgpack_encode_default)


def _materialized_layer_unpack(state, dsk, dependencies):
    dsk.update(state["dsk"])
    for k, v in state["dependencies"].items():
        dependencies[k] = list(set(dependencies.get(k, ())) | set(v))


def highlevelgraph_unpack(dumped_hlg):
    # Notice, we set `use_list=False`, which makes msgpack convert lists to tuples
    hlg = msgpack.loads(
        dumped_hlg, object_hook=msgpack_decode_default, use_list=False, **msgpack_opts
    )
    dsk = {}
    deps = {}
    for layer in hlg["layers"]:
        if layer["__module__"] is None:  # Default implementation
            unpack_func = _materialized_layer_unpack
        else:
            mod = import_allowed_module(layer["__module__"])
            unpack_func = getattr(mod, layer["__name__"]).__dask_distributed_unpack__
        unpack_func(layer["state"], dsk, deps)
    return dsk, deps
