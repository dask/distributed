from tlz import valmap

from dask.core import keys_in_tasks
from dask.highlevelgraph import HighLevelGraph, Layer
from dask.utils import stringify

from ..utils_comm import unpack_remotedata, subs_multiple
from ..worker import dumps_task

from ..utils import CancelledError

from .core import dumps_msgpack, loads_msgpack
from .serialize import import_allowed_module


def _materialized_layer_pack(
    layer: Layer,
    all_keys,
    known_key_dependencies,
    client,
    client_keys,
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

    # Unpack remote data and record its dependencies
    dsk = {k: unpack_remotedata(v, byte_keys=True) for k, v in layer.items()}
    unpacked_futures = set.union(*[v[1] for v in dsk.values()]) if dsk else set()
    for future in unpacked_futures:
        if future.client is not client:
            raise ValueError(
                "Inputs contain futures that were created by another client."
            )
        if stringify(future.key) not in client.futures:
            raise CancelledError(stringify(future.key))
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
        stringify(k): [stringify(dep) for dep in deps]
        for k, deps in dependencies.items()
    }

    annotations = layer.pack_annotations()
    all_keys = all_keys.union(dsk)
    dsk = {stringify(k): stringify(v, exclusive=all_keys) for k, v in dsk.items()}
    dsk = valmap(dumps_task, dsk)
    return {"dsk": dsk, "dependencies": dependencies, "annotations": annotations}


def highlevelgraph_pack(hlg: HighLevelGraph, client, client_keys):
    """Pack the high level graph for Scheduler -> Worker communication

    The approach is to delegate the packaging to each layer in the high
    level graph by calling .__dask_distributed_pack__() on each layer.
    If the layer doesn't implement packaging, we materialize the layer
    and pack it.

    Parameters
    ----------
    hlg: HighLevelGraph
        The high level graph to pack
    client: distributed.Client
        The client calling this function.
    client_keys: Iterable
        List of keys requested by the client.

    Returns
    -------
    data: list of header and payload
        Packed high level graph serialized by dumps_msgpack
    """
    layers = []

    # Dump each layer (in topological order)
    for layer in (hlg.layers[name] for name in hlg._toposort_layers()):
        if not layer.is_materialized():
            state = layer.__dask_distributed_pack__(client)
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
                    client,
                    client_keys,
                ),
            }
        )
    return dumps_msgpack({"layers": layers})


def _materialized_layer_unpack(state, dsk, dependencies, annotations):
    dsk.update(state["dsk"])
    for k, v in state["dependencies"].items():
        dependencies[k] = list(set(dependencies.get(k, ())) | set(v))

    if state["annotations"]:
        annotations.update(
            Layer.expand_annotations(state["annotations"], state["dsk"].keys())
        )


def highlevelgraph_unpack(dumped_hlg):
    """Unpack the high level graph for Scheduler -> Worker communication

    The approach is to delegate the packaging to each layer in the high
    level graph by calling .__dask_distributed_pack__() on each layer.
    If the layer doesn't implement a specialized packaging, we materialize
    the layer can pack it.

    Parameters
    ----------
    dumped_hlg: list of header and payload
        Packed high level graph serialized by dumps_msgpack

    Returns
    -------
    dsk: dict
        Materialized graph of all nodes in the high level graph
    deps: dict
        Dependencies of each key in `dsk`
    annotations: dict
        Annotations for `dsk`
    """

    hlg = loads_msgpack(*dumped_hlg)

    dsk = {}
    deps = {}
    annotations = {}
    for layer in hlg["layers"]:
        if layer["__module__"] is None:  # Default implementation
            unpack_func = _materialized_layer_unpack
        else:
            mod = import_allowed_module(layer["__module__"])
            unpack_func = getattr(mod, layer["__name__"]).__dask_distributed_unpack__
        unpack_func(layer["state"], dsk, deps, annotations)

    return dsk, deps, annotations
