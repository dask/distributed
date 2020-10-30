import msgpack
from tlz import valmap

from dask.blockwise import Blockwise
from dask.core import keys_in_tasks
from dask.highlevelgraph import HighLevelGraph, Layer
from dask.dataframe.shuffle import SimpleShuffleLayer, ShuffleLayer

from ..utils import (
    str_graph,
    tokey,
    CancelledError,
)

from .utils import (
    msgpack_opts,
)
from .serialize import (
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
        if isinstance(layer, Blockwise):  # TODO: implement
            pass
        elif isinstance(layer, SimpleShuffleLayer) and not hasattr(
            layer, "_cached_dict"
        ):  # Not materialized
            attrs = [
                "name",
                "column",
                "npartitions",
                "npartitions_input",
                "ignore_index",
                "name_input",
            ]
            if isinstance(layer, ShuffleLayer):
                attrs += [
                    "inputs",
                    "stage",
                    "nsplits",
                    "meta_input",
                    "parts_out",
                ]

            data = {attr: getattr(layer, attr) for attr in attrs}
            data["parts_out"] = list(layer.parts_out)
            data["meta_input"] = layer.meta_input.to_json()
            data["layer-type"] = "SimpleShuffleLayer"
            data["simple"] = False if isinstance(layer, ShuffleLayer) else True
            layers.append(data)
            continue

        # Falling back to the default serialization, which will materialize the layer
        layers.append(
            {
                "layer-type": "BasicLayer",
                "data": _dump_materialized_layer(
                    layer,
                    hlg.get_all_external_keys(),
                    hlg.key_dependencies,
                    allowed_client,
                    allows_futures,
                ),
            }
        )

    return msgpack.dumps({"layers": layers}, default=msgpack_encode_default)


def _load_materialized_layer(layer):
    return layer["dsk"], layer["dependencies"]


def loads_highlevelgraph(dumped_hlg):
    from distributed.worker import dumps_task

    # Notice, we set `use_list=True` to prevent msgpack converting lists to tuples
    hlg = msgpack.loads(
        dumped_hlg, object_hook=msgpack_decode_default, use_list=True, **msgpack_opts
    )
    dsk = {}
    deps = {}
    for layer in hlg["layers"]:
        layer_type = layer.pop("layer-type")
        if layer_type == "BasicLayer":
            d1, d2 = _load_materialized_layer(layer["data"])
            dsk.update(d1)
            for k, v in d2.items():
                deps[k] = list(set(deps.get(k, ())) | set(v))
        elif layer_type == "SimpleShuffleLayer":
            import pandas

            layer["meta_input"] = pandas.read_json(layer["meta_input"])
            if layer.pop("simple"):
                obj = SimpleShuffleLayer(**layer)
            else:
                layer["inputs"] = [tuple(k) for k in layer["inputs"]]
                obj = ShuffleLayer(**layer)

            input_keys = set()
            for v in obj._cull_dependencies(
                [(layer["name"], i) for i in range(layer["npartitions_input"])]
            ).values():
                input_keys.update(v)

            raw = dict(obj)
            raw = str_graph(raw, extra_values=input_keys)
            dsk.update(valmap(dumps_task, raw))

            # TODO: use shuffle-knowledge to calculate dependencies more efficiently
            deps.update(
                {k: keys_in_tasks(dsk, [v], as_list=True) for k, v in raw.items()}
            )
        else:
            assert False
    return dsk, deps
