from __future__ import annotations

import pickle
from typing import Any

from toolz import first

import dask.config

try:
    from pyarrow.plasma import ObjectID, connect
except ImportError:

    def connect():
        raise ImportError("pyarrow.plasma was not importable")


PLASMA_PATH = dask.config.get("distributed.protocol.shared.plasma_path", "tmp/plasma")

plasma: list[Any] = []


def _get_plasma():
    if not plasma:
        plasma.append(connect("/tmp/plasma"))
    return plasma[0]


def _put_buffer(buf):
    client = _get_plasma()
    object_id = ObjectID.from_random()
    buffer = memoryview(client.create(object_id, buf.nbytes))
    buffer[:] = buf.cast("b")[:]
    client.seal(object_id)
    return b"plasma:" + object_id.binary()


def ser(x, context=None):
    from distributed.worker import get_worker

    plasma_size_limit = dask.config.get("distributed.protocol.shared.minsize", 1)

    frames: list[bytes | memoryview] = [b""]
    try:
        worker = get_worker()
    except ValueError:
        # on client; must be scattering
        worker = None

    if worker and id(x) in worker.shared_data:
        return worker.shared_data[id(x)]

    def add_buf(buf):
        frames.append(memoryview(buf))

    frames[0] = pickle.dumps(x, protocol=-1, buffer_callback=add_buf)
    head = {"serializer": "plasma"}
    if any(buf.nbytes > plasma_size_limit for buf in frames[1:]):

        frames[1:] = [
            _put_buffer(buf) if buf.nbytes > plasma_size_limit else buf
            for buf in frames[1:]
        ]
        if worker is not None:
            # find object's dask key; it ought to exist
            seq = [k for k, d in worker.data.items() if d is x]
            # if key is not in data, this is probably a test in-process worker
            if seq:
                k = first(seq)
                new_obj = deser(
                    None, frames
                )  # version of object pointing at shared buffers
                worker.shared_data[id(new_obj)] = head, frames  # save shared buffers
                worker.data[k] = new_obj  # replace original object
    return head, frames


def deser(header, frames):
    client = _get_plasma()
    for i, buf in enumerate(frames.copy()):
        if isinstance(buf, (bytes, memoryview)) and buf[:7] == b"plasma:":
            # probably faster to get all the buffers in a single call
            ob = ObjectID(bytes(buf)[7:])
            bufs = client.get_buffers([ob])
            frames[i] = bufs[0]
    return pickle.loads(frames[0], buffers=frames[1:])
