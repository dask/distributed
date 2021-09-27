import functools
import logging

import msgpack

from .compression import compressions, maybe_compress
from .serialize import Serialize, Serialized
from .serialize import deserialize as _deserialize
from .serialize import msgpack_decode_default, msgpack_encode_default, serialize
from .utils import msgpack_opts

logger = logging.getLogger(__name__)


def dumps(
    msg, serializers=None, on_error="message", context=None, frame_split_size=None
) -> list:
    """Transform Python message to bytestream suitable for communication

    Developer Notes
    ---------------
    The approach here is to use `msgpack.dumps()` to serialize `msg` and
    write the result to the first output frame. If `msgpack.dumps()`
    encounters an object it cannot serialize like a NumPy array, it is handled
    out-of-band by `_encode_default()` and appended to the output frame list.
    """
    try:
        frames = [None]

        def _encode_default(obj):
            typ = type(obj)
            if typ is Serialize or typ is Serialized:
                offset = len(frames)
                if typ is Serialized:
                    sub_header, sub_frames = obj.header, obj.frames
                else:
                    sub_header, sub_frames = serialize(
                        obj,
                        serializers=serializers,
                        on_error=on_error,
                        context=context,
                    )
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

        # Split large frames
        frames2 = []
        lengths = []
        compressions = []
        from distributed.protocol.utils import frame_split_size as split

        for frame in frames:
            if frame_split_size and len(frame) > frame_split_size:
                sub_frames = split(frame, n=frame_split_size)
                frames2.extend(sub_frames)
                lengths.append(len(sub_frames))
            else:
                frames2.append(frame)
                lengths.append(1)

        frames = frames2

        # Compress frames
        if context and "compression" in context:
            compress_opts = {"compression": context["compression"]}
        else:
            compress_opts = {}

        for i, frame in enumerate(frames):
            compression, compressed = maybe_compress(frame, **compress_opts)
            frames[i] = compressed  # work in-place to avoid memory buildup
            compressions.append(compression)

        if any(length > 1 for length in lengths) or any(compressions):
            header = {
                "split-and-compressed": True,
                "lengths": lengths,
                "compressions": compressions,
            }
            return [msgpack.dumps(header)] + frames

        else:
            return frames

    except Exception:
        logger.critical("Failed to Serialize", exc_info=True)
        raise


def loads(frames, deserialize=True, deserializers=None):
    """Transform bytestream back into Python value"""

    try:

        def _decode_default(obj, frames=[]):
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
                if deserialize:
                    return _deserialize(
                        sub_header, sub_frames, deserializers=deserializers
                    )
                else:
                    return Serialized(sub_header, sub_frames)
            else:
                return msgpack_decode_default(obj)

        result = msgpack.loads(
            frames[0],
            object_hook=functools.partial(_decode_default, frames=frames),
            use_list=False,
            **msgpack_opts,
        )

        # Complex case, need to merge and decompress frames
        if isinstance(result, dict) and "split-and-compressed" in result:
            frames = frames[1:]

            # Decompress all compressed frames
            for i, (frame, compression) in enumerate(
                zip(frames, result["compressions"])
            ):
                if compression:
                    decompress = compressions[compression]["decompress"]
                    decompressed = decompress(frame)
                    frames[i] = decompressed

            # Join all split frames
            frames2 = []
            i = 0
            for length in result["lengths"]:
                if length == 1:
                    frames2.append(frames[i])
                    frames[i] = None  # keep memory footprint low
                else:
                    frames2.append(bytearray().join(frames[i : i + length]))
                    for j in range(i, i + length):
                        frames[j] = None  # keep memory footprint low
                i += length

            frames = frames2

            result = msgpack.loads(
                frames[0],
                object_hook=functools.partial(_decode_default, frames=frames),
                use_list=False,
                **msgpack_opts,
            )

        return result

    except Exception:
        logger.critical("Failed to deserialize", exc_info=True)
        raise
