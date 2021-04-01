import logging

import msgpack

from .compression import compressions, decompress, maybe_compress
from .serialize import (
    Serialize,
    Serialized,
    merge_and_deserialize,
    msgpack_decode_default,
    msgpack_encode_default,
    serialize_and_split,
)
from .utils import msgpack_opts

logger = logging.getLogger(__name__)


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
            if typ is Serialize or typ is Serialized:
                if typ is Serialize:
                    obj = obj.data
                offset = len(frames)
                if typ is Serialized:
                    sub_header, sub_frames = obj.header, obj.frames
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
            else:
                return msgpack_encode_default(obj)

        frames[0] = msgpack.dumps(msg, default=_encode_default, use_bin_type=True)
        return frames

    except Exception:
        logger.critical("Failed to Serialize", exc_info=True)
        raise


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
                    **msgpack_opts
                )
                offset += 1
                sub_frames = frames[offset : offset + sub_header["num-sub-frames"]]
                if deserialize:
                    if "compression" in sub_header:
                        sub_frames = decompress(sub_header, sub_frames)
                    return merge_and_deserialize(
                        sub_header, sub_frames, deserializers=deserializers
                    )
                else:
                    return Serialized(sub_header, sub_frames)
            else:
                return msgpack_decode_default(obj)

        return msgpack.loads(
            frames[0], object_hook=_decode_default, use_list=False, **msgpack_opts
        )

    except Exception:
        logger.critical("Failed to deserialize", exc_info=True)
        raise


def dumps_msgpack(msg, compression=None):
    """Dump msg into header and payload, both bytestrings

    All of the message must be msgpack encodable

    See Also:
        loads_msgpack
    """
    header = {}
    payload = msgpack.dumps(msg, default=msgpack_encode_default, use_bin_type=True)

    fmt, payload = maybe_compress(payload, compression=compression)
    if fmt:
        header["compression"] = fmt

    if header:
        header_bytes = msgpack.dumps(header, use_bin_type=True)
    else:
        header_bytes = b""

    return [header_bytes, payload]


def loads_msgpack(header, payload):
    """Read msgpack header and payload back to Python object

    See Also:
        dumps_msgpack
    """
    header = bytes(header)
    if header:
        header = msgpack.loads(
            header, object_hook=msgpack_decode_default, use_list=False, **msgpack_opts
        )
    else:
        header = {}

    if header.get("compression"):
        try:
            decompress = compressions[header["compression"]]["decompress"]
            payload = decompress(payload)
        except KeyError:
            raise ValueError(
                "Data is compressed as %s but we don't have this"
                " installed" % str(header["compression"])
            )

    return msgpack.loads(
        payload, object_hook=msgpack_decode_default, use_list=False, **msgpack_opts
    )
