import logging
import threading

import msgpack

from ..utils import LRU
from . import pickle
from .compression import decompress, maybe_compress
from .serialize import (
    MsgpackList,
    Serialize,
    Serialized,
    SerializedCallable,
    TaskGraphValue,
    merge_and_deserialize,
    msgpack_decode_default,
    msgpack_encode_default,
    serialize_and_split,
)
from .utils import msgpack_opts

logger = logging.getLogger(__name__)

cache_dumps = LRU(maxsize=100)
cache_loads = LRU(maxsize=100)
cache_dumps_lock = threading.Lock()
cache_loads_lock = threading.Lock()


def dumps_function(func):
    """ Dump a function to bytes, cache functions """
    try:
        with cache_dumps_lock:
            result = cache_dumps[func]
    except KeyError:
        result = pickle.dumps(func, protocol=4)
        if len(result) < 100000:
            with cache_dumps_lock:
                cache_dumps[func] = result
    except TypeError:  # Unhashable function
        result = pickle.dumps(func, protocol=4)
    return result


def loads_function(bytes_object):
    """ Load a function from bytes, cache bytes """
    if len(bytes_object) < 100000:
        with cache_dumps_lock:
            if bytes_object in cache_loads:
                return cache_loads[bytes_object]
            else:
                result = pickle.loads(bytes_object)
                cache_loads[bytes_object] = result
                return result
    return pickle.loads(bytes_object)


def dumps(msg, serializers=None, on_error="message", context=None) -> list:
    """Transform Python message to bytestream suitable for communication

    Developer Notes
    ---------------
    The approach here is to use `msgpack.dumps()` to serialize `msg` and
    write the result to the first output frame. If `msgpack.dumps()`
    encounters an object it cannot serialize like a NumPy array, it is handled
    out-of-band by `_encode_default()` and appended to the output frame list.
    """
    try:
        if context and "compression" in context:
            compress_opts = {"compression": context["compression"]}
        else:
            compress_opts = {}

        def _inplace_compress_frames(header, frames):
            compression = list(header.get("compression", [None] * len(frames)))

            for i in range(len(frames)):
                if compression[i] is None:
                    compression[i], frames[i] = maybe_compress(
                        frames[i], **compress_opts
                    )

            header["compression"] = tuple(compression)

        frames = [None]

        def _encode_default(obj):
            typ = type(obj)

            ret = msgpack_encode_default(obj)
            if ret is not obj:
                return ret

            if typ is Serialize:
                obj = obj.data  # TODO: remove Serialize/to_serialize completely

            offset = len(frames)
            if typ in (Serialized, SerializedCallable):
                sub_header, sub_frames = obj.header, obj.frames
            elif callable(obj):
                sub_header, sub_frames = {"callable": dumps_function(obj)}, []
            else:
                sub_header, sub_frames = serialize_and_split(
                    obj, serializers=serializers, on_error=on_error, context=context
                )
                _inplace_compress_frames(sub_header, sub_frames)
            sub_header["num-sub-frames"] = len(sub_frames)
            frames.append(
                msgpack.dumps(
                    sub_header, default=msgpack_encode_default, use_bin_type=True
                )
            )
            frames.extend(sub_frames)
            return {"__Serialized__": offset}

        frames[0] = msgpack.dumps(msg, default=_encode_default, use_bin_type=True)
        return frames

    except Exception:
        logger.critical("Failed to Serialize", exc_info=True)
        raise


class DelayedExceptionRaise:
    def __init__(self, err):
        self.err = err


def loads(frames, deserialize=True, deserializers=None):
    """ Transform bytestream back into Python value """

    try:

        def _decode_default(obj):
            offset = obj.get("__Serialized__", 0)
            if offset > 0:
                sub_header = msgpack.loads(
                    frames[offset],
                    object_hook=msgpack_decode_default,
                    use_list=False,
                    **msgpack_opts,
                )
                offset += 1
                sub_frames = frames[offset : offset + sub_header["num-sub-frames"]]
                if "callable" in sub_header:
                    if deserialize:
                        try:
                            return loads_function(sub_header["callable"])
                        except Exception as e:
                            if deserialize == "delay-exception":
                                return DelayedExceptionRaise(e)
                            else:
                                raise
                    else:
                        return SerializedCallable(sub_header, sub_frames)
                if deserialize:
                    if "compression" in sub_header:
                        sub_frames = decompress(sub_header, sub_frames)
                    try:
                        return merge_and_deserialize(
                            sub_header, sub_frames, deserializers=deserializers
                        )
                    except Exception as e:
                        if deserialize == "delay-exception":
                            return DelayedExceptionRaise(e)
                        else:
                            raise
                else:
                    return Serialized(sub_header, sub_frames)
            else:
                # Notice, even though `msgpack_decode_default()` supports
                # `__MsgpackList__`, we decode it here explicitly. This way
                # we can delay the convertion to a regular `list` until it
                # gets to a worker.
                if "__MsgpackList__" in obj:
                    if deserialize:
                        return list(obj["as-tuple"])
                    else:
                        return MsgpackList(obj["as-tuple"])
                if "__TaskGraphValue__" in obj:
                    if deserialize:
                        return obj["data"]
                    else:
                        return TaskGraphValue(obj["data"])
                return msgpack_decode_default(obj)

        return msgpack.loads(
            frames[0], object_hook=_decode_default, use_list=False, **msgpack_opts
        )

    except Exception:
        logger.critical("Failed to deserialize", exc_info=True)
        raise
