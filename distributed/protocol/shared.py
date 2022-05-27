import pickle

from toolz import first

import dask.config

try:
    from pyarrow.plasma import ObjectID, connect
except ImportError:

    def connect():
        raise ImportError("pyarrow.plasma was not importable")


PLASMA_SIZE_LIMIT = dask.config.get("distributed.protocol.shared.minsize", 1)
PLASMA_PATH = dask.config.get("distributed.protocol.shared.plasma_path", "tmp/plasma")
PLASMA_ON = dask.config.get("distributed.protocol.shared.plasma", "true") == "true"

plasma = [None]


def _get_plasma():
    if plasma[0] is None:
        plasma[0] = connect("/tmp/plasma")
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

    frames = [None]
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
    if any(buf.nbytes > PLASMA_SIZE_LIMIT for buf in frames[1:]):

        frames[1:] = [
            _put_buffer(buf) if buf.nbytes > PLASMA_SIZE_LIMIT else buf
            for buf in frames[1:]
        ]
        if worker is not None:
            # find object's dask key; it ought to exist
            k = first((k for k, d in worker.data.items() if d is x))
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
