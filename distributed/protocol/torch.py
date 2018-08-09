from .serialize import (serialize, dask_serialize, dask_deserialize,
        register_attributes)

import torch
import numpy as np


@dask_serialize.register(torch.Tensor)
def serialize_torch_Tensor(t):
    header, frames = serialize(t.numpy())
    header['requires_grad'] = t.requires_grad
    header['device'] = t.device.type
    return header, frames


@dask_deserialize.register(torch.Tensor)
def deserialize_torch_Tensor(header, frames):
    x = dask_deserialize.dispatch(np.ndarray)(header, frames)
    return torch.tensor(data=x,
                        device=header['device'],
                        requires_grad=header['requires_grad'])


@dask_serialize.register(torch.nn.Parameter)
def serialize_torch_Parameters(p):
    header, frames = serialize(p.detach())
    header['requires_grad'] = p.requires_grad
    return header, frames


@dask_deserialize.register(torch.nn.Parameter)
def deserialize_torch_Parameters(header, frames):
    t = dask_deserialize.dispatch(torch.Tensor)(header, frames)
    return torch.nn.Parameter(data=t, requires_grad=header['requires_grad'])


register_attributes(torch.nn.Module)
