from __future__ import annotations

import pickle
from typing import Any
from urllib import parse

from toolz import first

import dask.config
from dask.base import tokenize

try:
    from pyarrow.plasma import ObjectID, PlasmaObjectExists, PlasmaStoreFull, connect
except ImportError:

    def connect(*_):
        raise ImportError("pyarrow.plasma was not importable")


plasma: list[Any] = []  # module global to cache client


def _get_plasma():
    # TODO: cache error so we don't try this every time?
    if not plasma:
        PLASMA_PATH = dask.config.get(
            "distributed.protocol.shared.plasma_path", "/tmp/plasma"
        )
        plasma.append(connect(PLASMA_PATH))
    return plasma[0]


def _put_buffer(buf):
    client = _get_plasma()
    try:
        object_id = ObjectID(tokenize(buf)[:20].encode())  # use id()?
        buffer = memoryview(client.create(object_id, buf.nbytes))
        buffer[:] = buf.cast("b")[:]
        client.seal(object_id)
    except PlasmaObjectExists:
        pass
    except PlasmaStoreFull:
        # warn?
        return buf
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
        # cache hit
        return worker.shared_data[id(x)]

    def add_buf(buf):
        frames.append(memoryview(buf))

    frames[0] = pickle.dumps(x, protocol=-1, buffer_callback=add_buf)
    if on_node(context) != on_node(context, "recipient"):
        # across nodes
        head = {"serializer": "pickle"}
    else:
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
                    worker.data[k] = new_obj  # replace original object
    return head, frames


def on_node(context, which="sender"):
    if context is None:
        return None
    info = context.get(which, {})
    if isinstance(info, dict):
        info = info.get("address", "")
    return parse.urlparse(info).hostname


def deser(header, frames):
    from distributed.worker import get_worker

    try:
        worker = get_worker()
    except ValueError:
        # on client; must be scattering
        worker = None
    client = _get_plasma()
    bufs = None
    frames0 = frames.copy()
    for i, buf in enumerate(frames.copy()):
        if isinstance(buf, (bytes, memoryview)) and buf[:7] == b"plasma:":
            # probably faster to get all the buffers in a single call
            ob = ObjectID(bytes(buf)[7:])
            bufs = client.get_buffers([ob])
            frames[i] = bufs[0]
    out = pickle.loads(frames[0], buffers=frames[1:])
    if worker and bufs:
        # "bufs" is a poor condition, maybe store in header
        worker.shared_data[id(out)] = (
            header or {"serializer": "plasma"},
            frames0,
        )  # save shared buffers
    return out
