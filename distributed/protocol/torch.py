import numpy as np
import torch

from .serialize import dask_deserialize, dask_serialize, register_generic, serialize


@dask_serialize.register(torch.Tensor)
def serialize_torch_Tensor(t):
    requires_grad_ = t.requires_grad

    if requires_grad_:
        header, frames = serialize(t.detach().numpy())
    else:
        header, frames = serialize(t.numpy())

    if t.grad is not None:
        grad_header, grad_frames = serialize(t.grad.numpy())
        header["grad"] = {"header": grad_header, "start": len(frames)}
        frames += grad_frames
    header["requires_grad"] = requires_grad_
    header["device"] = t.device.type
    return header, frames


@dask_deserialize.register(torch.Tensor)
def deserialize_torch_Tensor(header, frames):
    if header.get("grad", False):
        i = header["grad"]["start"]
        frames, grad_frames = frames[:i], frames[i:]
        grad = dask_deserialize.dispatch(np.ndarray)(
            header["grad"]["header"], grad_frames
        )
    else:
        grad = None

    x = dask_deserialize.dispatch(np.ndarray)(header, frames)
    if header["device"] == "cpu":
        t = torch.from_numpy(x)
        if header["requires_grad"]:
            t = t.requires_grad_(True)
    else:
        t = torch.tensor(
            data=x, device=header["device"], requires_grad=header["requires_grad"]
        )
    if grad is not None:
        t.grad = torch.from_numpy(grad)
    return t


@dask_serialize.register(torch.nn.Parameter)
def serialize_torch_Parameters(p):
    header, frames = serialize(p.detach())
    header["requires_grad"] = p.requires_grad
    return header, frames


@dask_deserialize.register(torch.nn.Parameter)
def deserialize_torch_Parameters(header, frames):
    t = dask_deserialize.dispatch(torch.Tensor)(header, frames)
    return torch.nn.Parameter(data=t, requires_grad=header["requires_grad"])


register_generic(torch.nn.Module)
