from __future__ import annotations

import os
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


try:
    from lmdb import open as lmdb_connect
except ImportError:

    def lmdb_connect(**kwargs):
        raise ImportError("lmdb was not importable")


try:
    import vineyard
    from vineyard import connect as vineyard_connect
except ImportError:

    def vineyard_connect(buf):
        raise ImportError("vineyard was not importable")


backends: dict[str, Any] = {}  # module global to cache client


def _get_lmdb():
    if "lmdb" not in backends:
        # TODO: options to be available via config
        backends["lmdb"] = lmdb_connect(
            path="/tmp/lmdb",
            map_size=10 * 2**30,
            sync=False,
            readahead=False,
            writemap=True,
            meminit=False,
            max_spare_txns=4,
            max_readers=16,
        )
    return backends["lmdb"]


def _put_lmdb_buffer(buf):
    client = _get_lmdb()
    key = os.urandom(16)
    with client.begin(write=True) as txn:
        txn.put(key, buf)
    return b"lmdb:" + key


# by default, don't share under 1MB
size_limit = dask.config.get("distributed.protocol.shared.minsize", 2**20)


def ser_lmdb(x, context=None):
    from distributed.worker import get_worker

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
        head = {"serializer": "lmdb"}
        if any(buf.nbytes > size_limit for buf in frames[1:]):
            # TODO: make DB keys deterministic by using dask key, if available
            # TODO: use putmulti method of cursor
            frames[1:] = [
                _put_lmdb_buffer(buf) if buf.nbytes > size_limit else buf
                for buf in frames[1:]
            ]
            if worker is not None:
                # find object's dask key; it ought to exist
                seq = [k for k, d in worker.data.items() if d is x]
                # if key is not in data, this is probably a test in-process worker
                if seq:
                    k = first(seq)
                    new_obj = deser_lmdb(
                        None, frames
                    )  # version of object pointing at shared buffers
                    worker.data[k] = new_obj  # replace original object
    return head, frames


def deser_lmdb(header, frames):
    from distributed.worker import get_worker

    try:
        worker = get_worker()
    except ValueError:
        # on client; must be scattering
        worker = None
    client = _get_lmdb()
    didsome = False
    frames0 = frames.copy()
    for i, buf in enumerate(frames.copy()):
        if isinstance(buf, (bytes, memoryview)) and buf[:5] == b"lmdb:":
            # TODO: a cursor has getmulti method
            ob = bytes(buf)[5:]
            with client.begin(buffers=True) as tcx:
                frames[i] = tcx.get(ob)
                didsome = True
    out = pickle.loads(frames[0], buffers=frames[1:])
    if worker and didsome:
        # "bufs" is a poor condition, maybe store in header
        worker.shared_data[id(out)] = (
            header or {"serializer": "lmdb"},
            frames0,
        )  # save shared buffers
    return out


def _get_plasma():
    if "plasma" not in backends:
        PLASMA_PATH = dask.config.get(
            "distributed.protocol.shared.plasma_path", "/tmp/plasma"
        )
        backends["plasma"] = connect(PLASMA_PATH)
    return backends["plasma"]


def _put_plasma_buffer(buf):
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


def ser_plasma(x, context=None):
    from distributed.worker import get_worker

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
        if any(buf.nbytes > size_limit for buf in frames[1:]):

            frames[1:] = [
                _put_plasma_buffer(buf) if buf.nbytes > size_limit else buf
                for buf in frames[1:]
            ]
            if worker is not None:
                # find object's dask key; it ought to exist
                seq = [k for k, d in worker.data.items() if d is x]
                # if key is not in data, this is probably a test in-process worker
                if seq:
                    k = first(seq)
                    new_obj = deser_plasma(
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


def deser_plasma(header, frames):
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


def _get_vineyard():
    if "vineyard" not in backends:
        VINEYARD_PATH = dask.config.get(
            "distributed.protocol.shared.vineyard_path", "/tmp/vineyard.sock"
        )
        backends["vineyard"] = vineyard_connect(VINEYARD_PATH)
    return backends["vineyard"]


def _put_vineyard_buffer(buf):
    client = _get_vineyard()
    try:
        buffer_writer = client.create_blob(buf.nbytes)
        buffer_writer.copy(0, memoryview(buf.cast("b")[:]))
        vineyard_object_id = buffer_writer.seal(client).id
    except vineyard.VineyardException:
        # warn?
        return buf
    return b"vineyard:" + int(vineyard_object_id).to_bytes(8, "little")


def ser_vineyard(x, context=None):
    from distributed.worker import get_worker

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
        head = {"serializer": "vineyard"}
        if any(buf.nbytes > size_limit for buf in frames[1:]):

            frames[1:] = [
                _put_vineyard_buffer(buf) if buf.nbytes > size_limit else buf
                for buf in frames[1:]
            ]
            if worker is not None:
                # find object's dask key; it ought to exist
                seq = [k for k, d in worker.data.items() if d is x]
                # if key is not in data, this is probably a test in-process worker
                if seq:
                    k = first(seq)
                    new_obj = deser_vineyard(
                        None, frames
                    )  # version of object pointing at shared buffers
                    worker.data[k] = new_obj  # replace original object
    return head, frames


def deser_vineyard(header, frames):
    from distributed.worker import get_worker

    try:
        worker = get_worker()
    except ValueError:
        # on client; must be scattering
        worker = None
    client = _get_vineyard()
    ids, indices, buffers = [], [], None
    for i, buf in enumerate(frames):
        if isinstance(buf, (bytes, memoryview)) and buf[:9] == b"vineyard:":
            ids.append(vineyard.ObjectID(int.from_bytes(bytes(buf)[9:], "little")))
            indices.append(i)
    if ids:
        buffers = client.get_blobs(ids)
        for i, blob in zip(indices, buffers):
            frames[i] = memoryview(blob)
    out = pickle.loads(frames[0], buffers=frames[1:])
    if worker and buffers:
        worker.shared_data[id(out)] = (
            header or {"serializer": "vineyard"},
            frames,
        )  # save shared buffers
    return out
