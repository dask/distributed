import msgpack
from tlz import valmap

from dask.core import keys_in_tasks
from dask.highlevelgraph import HighLevelGraph, Layer

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


def _dump_materialized_layer(
    layer: Layer, all_keys, known_key_dependencies, allowed_client, allows_futures
):
    from distributed.utils_comm import unpack_remotedata
    from distributed.worker import dumps_task

    dsk = {k: unpack_remotedata(v, byte_keys=True) for k, v in layer.items()}
    unpacked_futures = set.union(*[v[1] for v in dsk.values()]) if dsk else set()
    for future in unpacked_futures:
        if future.client is not allowed_client:
            raise ValueError(
                "Inputs contain futures that were created by another client."
            )
        if tokey(future.key) not in allows_futures:
            raise CancelledError(tokey(future.key))
    unpacked_futures_deps = {}
    for k, v in dsk.items():
        if len(v[1]):
            unpacked_futures_deps[k] = {f.key for f in v[1]}
    dsk = {k: v[0] for k, v in dsk.items()}

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


def dumps_highlevelgraph(hlg: HighLevelGraph, allowed_client, allows_futures):
    layers = []

    # Dump each layer (in topological order)
    for layer in (hlg.layers[name] for name in hlg._toposort_layers()):
        if not layer.is_materialized():
            state = layer.distributed_pack()
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
                "state": _dump_materialized_layer(
                    layer,
                    hlg.get_all_external_keys(),
                    hlg.key_dependencies,
                    allowed_client,
                    allows_futures,
                ),
            }
        )

    return msgpack.dumps({"layers": layers}, default=msgpack_encode_default)


def _load_materialized_layer(state, dsk, dependencies):
    dsk.update(state["dsk"])
    for k, v in state["dependencies"].items():
        dependencies[k] = list(set(dependencies.get(k, ())) | set(v))


def loads_highlevelgraph(dumped_hlg):
    # Notice, we set `use_list=True` to prevent msgpack converting lists to tuples
    hlg = msgpack.loads(
        dumped_hlg, object_hook=msgpack_decode_default, use_list=True, **msgpack_opts
    )
    dsk = {}
    deps = {}
    for layer in hlg["layers"]:
        if layer["__module__"] is None:  # Default implementation
            unpack_func = _load_materialized_layer
        else:
            mod = import_allowed_module(layer["__module__"])
            unpack_func = getattr(mod, layer["__name__"]).distributed_unpack
        unpack_func(layer["state"], dsk, deps)
    return dsk, deps
