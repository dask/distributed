import cloudpickle
import skorch
from .serialize import dask_serialize, dask_deserialize

@dask_serialize.register(skorch.NeuralNet)
def serialize_skorch(x):
    has_module = hasattr(x, "module_")
    headers = {"has_module": has_module}
    if has_module:
        module = x.__dict__.pop("module_")
        try:
            frames = [cloudpickle.dumps(x)]
            frames = frames + [cloudpickle.dumps(module)]
        finally:
            x.__dict__["module_"] = module
    else:
        frames = [cloudpickle.dumps(x)]

    return headers, frames


@dask_deserialize.register(skorch.NeuralNet)
def deserialize_skorch(header, frames):
    model = cloudpickle.loads(frames[0])
    if header["has_module"]:
        module = cloudpickle.loads(frames[1])
        model.module_ = module
    return model