import collections.abc
import importlib
import traceback
from array import array
from enum import Enum
from functools import partial

import msgpack

import dask
from dask.base import normalize_token, tokenize

from ..utils import ensure_bytes, has_keyword, typename
from . import pickle
from .compression import decompress, maybe_compress
from .utils import frame_split_size, msgpack_opts, pack_frames_prelude, unpack_frames

lazy_registrations = {}

dask_serialize = dask.utils.Dispatch("dask_serialize")
dask_deserialize = dask.utils.Dispatch("dask_deserialize")

_cached_allowed_modules = {}
non_list_collection_types = (tuple, set, frozenset)
collection_types = (list,) + non_list_collection_types


def dask_dumps(x, context=None):
    """Serialize object using the class-based registry"""
    type_name = typename(type(x))
    try:
        dumps = dask_serialize.dispatch(type(x))
    except TypeError:
        raise NotImplementedError(type_name)
    if has_keyword(dumps, "context"):
        header, frames = dumps(x, context=context)
    else:
        header, frames = dumps(x)

    header["type"] = type_name
    header["type-serialized"] = pickle.dumps(type(x), protocol=4)
    header["serializer"] = "dask"
    return header, frames


def dask_loads(header, frames):
    typ = pickle.loads(header["type-serialized"])
    loads = dask_deserialize.dispatch(typ)
    return loads(header, frames)


def pickle_dumps(x, context=None):
    frames = [None]
    buffer_callback = lambda f: frames.append(memoryview(f))
    frames[0] = pickle.dumps(
        x,
        buffer_callback=buffer_callback,
        protocol=context.get("pickle-protocol", None) if context else None,
    )
    header = {
        "serializer": "pickle",
        "writeable": tuple(not f.readonly for f in frames[1:]),
    }
    return header, frames


def pickle_loads(header, frames):
    x, buffers = frames[0], frames[1:]

    writeable = header.get("writeable")
    if not writeable:
        writeable = len(buffers) * (None,)

    new = []
    memoryviews = map(memoryview, buffers)
    for w, mv in zip(writeable, memoryviews):
        if w == mv.readonly:
            if mv.readonly:
                mv = memoryview(bytearray(mv))
            else:
                mv = memoryview(bytes(mv))
            if mv.nbytes > 0:
                mv = mv.cast(mv.format, mv.shape)
            else:
                mv = mv.cast(mv.format)
        new.append(mv)

    return pickle.loads(x, buffers=new)


def import_allowed_module(name):
    if name in _cached_allowed_modules:
        return _cached_allowed_modules[name]

    # Check for non-ASCII characters
    name = name.encode("ascii").decode()
    # We only compare the root module
    root = name.split(".", 1)[0]

    # Note, if an empty string creeps into allowed-imports it is disallowed explicitly
    if root and root in dask.config.get("distributed.scheduler.allowed-imports"):
        _cached_allowed_modules[name] = importlib.import_module(name)
        return _cached_allowed_modules[name]
    else:
        raise RuntimeError(
            f"Importing {repr(name)} is not allowed, please add it to the list of "
            "allowed modules the scheduler can import via the "
            "distributed.scheduler.allowed-imports configuration setting."
        )


class MsgpackList(collections.abc.MutableSequence):
    def __init__(self, x):
        self.data = x

    def __getitem__(self, i):
        return self.data[i]

    def __delitem__(self, i):
        del self.data[i]

    def __setitem__(self, i, v):
        self.data[i] = v

    def __len__(self):
        return len(self.data)

    def insert(self, i, v):
        return self.data.insert(i, v)


class TaskGraphValue:
    def __init__(self, x):
        self.data = x


def msgpack_persist_lists(obj):
    typ = type(obj)
    if typ is list:
        return MsgpackList([msgpack_persist_lists(o) for o in obj])
    if typ in non_list_collection_types:
        return typ(msgpack_persist_lists(o) for o in obj)
    if typ is dict:
        return {k: msgpack_persist_lists(v) for k, v in obj.items()}
    return obj


def msgpack_decode_default(obj):
    """
    Custom packer/unpacker for msgpack
    """
    if "__Enum__" in obj:
        mod = import_allowed_module(obj["__module__"])
        typ = getattr(mod, obj["__name__"])
        return getattr(typ, obj["name"])

    if "__Set__" in obj:
        return set(obj["as-tuple"])

    if "__MsgpackList__" in obj:
        return list(obj["as-tuple"])

    if "__TaskGraphValue__" in obj:
        return obj["data"]

    return obj


def msgpack_encode_default(obj):
    """
    Custom packer/unpacker for msgpack
    """
    typ = type(obj)

    if isinstance(obj, Enum):
        return {
            "__Enum__": True,
            "name": obj.name,
            "__module__": obj.__module__,
            "__name__": type(obj).__name__,
        }

    if isinstance(obj, set):
        return {"__Set__": True, "as-tuple": tuple(obj)}

    if typ is MsgpackList:
        return {"__MsgpackList__": True, "as-tuple": tuple(obj.data)}

    if typ is TaskGraphValue:
        return {"__TaskGraphValue__": True, "data": obj.data}

    return obj


def msgpack_dumps(x):
    try:
        frame = msgpack.dumps(x, default=msgpack_encode_default, use_bin_type=True)
    except Exception:
        raise NotImplementedError()
    else:
        return {"serializer": "msgpack"}, [frame]


def msgpack_loads(header, frames):
    return msgpack.loads(
        b"".join(frames),
        object_hook=msgpack_decode_default,
        use_list=False,
        **msgpack_opts,
    )


def serialization_error_loads(header, frames):
    msg = "\n".join([ensure_bytes(frame).decode("utf8") for frame in frames])
    raise TypeError(msg)


families = {}


def register_serialization_family(name, dumps, loads):
    families[name] = (dumps, loads, dumps and has_keyword(dumps, "context"))


register_serialization_family("dask", dask_dumps, dask_loads)
register_serialization_family("pickle", pickle_dumps, pickle_loads)
register_serialization_family("msgpack", msgpack_dumps, msgpack_loads)
register_serialization_family("error", None, serialization_error_loads)


def check_dask_serializable(x):
    if type(x) in (list, set, tuple) and len(x):
        return check_dask_serializable(next(iter(x)))
    elif type(x) is dict and len(x):
        return check_dask_serializable(next(iter(x.items()))[1])
    else:
        try:
            dask_serialize.dispatch(type(x))
            return True
        except TypeError:
            pass
    return False


def serialize(x, serializers=None, on_error="message", context=None):
    r"""
    Convert object to a header and list of bytestrings

    This takes in an arbitrary Python object and returns a msgpack serializable
    header and a list of bytes or memoryview objects.

    The serialization protocols to use are configurable: a list of names
    define the set of serializers to use, in order. These names are keys in
    the ``serializer_registry`` dict (e.g., 'pickle', 'msgpack'), which maps
    to the de/serialize functions. The name 'dask' is special, and will use the
    per-class serialization methods. ``None`` gives the default list
    ``['dask', 'pickle']``.

    Examples
    --------
    >>> serialize(1)
    ({}, [b'\x80\x04\x95\x03\x00\x00\x00\x00\x00\x00\x00K\x01.'])

    >>> serialize(b'123')  # some special types get custom treatment
    ({'type': 'builtins.bytes'}, [b'123'])

    >>> deserialize(*serialize(1))
    1

    Returns
    -------
    header: dictionary containing any msgpack-serializable metadata
    frames: list of bytes or memoryviews, commonly of length one

    See Also
    --------
    deserialize : Convert header and frames back to object
    to_serialize : Mark that data in a message should be serialized
    register_serialization : Register custom serialization functions
    """
    if serializers is None:
        serializers = ("dask", "pickle")  # TODO: get from configuration

    if isinstance(x, Serialized):
        return x.header, x.frames

    # Note: "msgpack" will always convert lists to tuple (see GitHub #3716),
    #       so we should persist lists if "msgpack" comes before "pickle"
    #       in the list of serializers.
    if (
        type(x) is list
        and "msgpack" in serializers
        and (
            "pickle" not in serializers
            or serializers.index("pickle") > serializers.index("msgpack")
        )
    ):
        x = msgpack_persist_lists(x)

    tb = ""

    for name in serializers:
        dumps, loads, wants_context = families[name]
        try:
            header, frames = dumps(x, context=context) if wants_context else dumps(x)
            header["serializer"] = name
            return header, frames
        except NotImplementedError:
            continue
        except Exception as e:
            tb = traceback.format_exc()
            break

    msg = "Could not serialize object of type %s." % type(x).__name__
    if on_error == "message":
        frames = [msg]
        if tb:
            frames.append(tb[:100000])

        frames = [frame.encode() for frame in frames]

        return {"serializer": "error"}, frames
    elif on_error == "raise":
        raise TypeError(msg, str(x)[:10000])


def deserialize(header, frames, deserializers=None):
    """
    Convert serialized header and list of bytestrings back to a Python object

    Parameters
    ----------
    header : dict
    frames : list of bytes
    deserializers : Optional[Dict[str, Tuple[Callable, Callable, bool]]]
        An optional dict mapping a name to a (de)serializer.
        See `dask_serialize` and `dask_deserialize` for more.

    See Also
    --------
    serialize
    """

    name = header.get("serializer")
    if deserializers is not None and name not in deserializers:
        raise TypeError(
            "Data serialized with %s but only able to deserialize "
            "data with %s" % (name, str(list(deserializers)))
        )
    dumps, loads, wants_context = families[name]
    return loads(header, frames)


def serialize_and_split(x, serializers=None, on_error="message", context=None):
    """Serialize and split compressable frames

    This function is a drop-in replacement of `serialize()` that calls `serialize()`
    followed by `frame_split_size()` on frames that should be compressed.

    Use `merge_and_deserialize()` to merge and deserialize the frames back.

    See Also
    --------
    serialize
    merge_and_deserialize
    """
    header, frames = serialize(x, serializers, on_error, context)
    num_sub_frames = []
    offsets = []
    out_frames = []
    out_compression = []
    for frame, compression in zip(
        frames, header.get("compression") or [None] * len(frames)
    ):
        if compression is None:  # default behavior
            sub_frames = frame_split_size(frame)
            num_sub_frames.append(len(sub_frames))
            offsets.append(len(out_frames))
            out_frames.extend(sub_frames)
            out_compression.extend([None] * len(sub_frames))
        else:
            num_sub_frames.append(1)
            offsets.append(len(out_frames))
            out_frames.append(frame)
            out_compression.append(compression)
    assert len(out_compression) == len(out_frames)

    # Notice, in order to match msgpack's implicit convertion to tuples,
    # we convert to tuples here as well.
    header["split-num-sub-frames"] = tuple(num_sub_frames)
    header["split-offsets"] = tuple(offsets)
    header["compression"] = tuple(out_compression)
    return header, out_frames


def merge_and_deserialize(header, frames, deserializers=None):
    """Merge and deserialize frames

    This function is a drop-in replacement of `deserialize()` that merges
    frames that were split by `serialize_and_split()`

    See Also
    --------
    deserialize
    serialize_and_split
    """
    merged_frames = []
    if "split-num-sub-frames" not in header:
        merged_frames = frames
    else:
        for n, offset in zip(header["split-num-sub-frames"], header["split-offsets"]):
            if n == 1:
                merged_frames.append(frames[offset])
            else:
                merged_frames.append(bytearray().join(frames[offset : offset + n]))

    return deserialize(header, merged_frames, deserializers=deserializers)


class Serialize:
    """Mark an object that should be serialized

    Examples
    --------
    >>> msg = {'op': 'update', 'data': to_serialize(123)}
    >>> msg  # doctest: +SKIP
    {'op': 'update', 'data': <Serialize: 123>}

    See also
    --------
    distributed.protocol.dumps
    """

    def __init__(self, data):
        self.data = data

    def __repr__(self):
        return "<Serialize: %s>" % str(self.data)

    def __eq__(self, other):
        return isinstance(other, Serialize) and other.data == self.data

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return hash(self.data)


to_serialize = Serialize


class Serialized:
    """
    An object that is already serialized into header and frames

    Normal serialization operations pass these objects through.  This is
    typically used within the scheduler which accepts messages that contain
    data without actually unpacking that data.
    """

    def __init__(self, header, frames):
        self.header = header
        self.frames = frames

    def __eq__(self, other):
        return (
            isinstance(other, Serialized)
            and other.header == self.header
            and other.frames == self.frames
        )

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return hash(tokenize((self.header, self.frames)))


class SerializedCallable(Serialized):
    def __call__(self) -> None:
        raise NotImplementedError


def nested_deserialize(x):
    """
    Replace all Serialize and Serialized values nested in *x*
    with the original values.  Returns a copy of *x*.

    >>> msg = {'op': 'update', 'data': to_serialize(123)}
    >>> nested_deserialize(msg)
    {'op': 'update', 'data': 123}
    """

    typ = type(x)
    if typ is dict:
        return {k: nested_deserialize(v) for k, v in x.items()}
    if typ is MsgpackList:
        return list(nested_deserialize(x.data))
    if typ is TaskGraphValue:
        return x.data
    if typ is Serialize:
        return x.data
    if isinstance(x, Serialized):
        return deserialize(x.header, x.frames)
    if typ in collection_types:
        return typ(nested_deserialize(o) for o in x)
    return x


def serialize_bytelist(x, **kwargs):
    header, frames = serialize_and_split(x, **kwargs)
    if frames:
        compression, frames = zip(*map(maybe_compress, frames))
    else:
        compression = []
    header["compression"] = compression
    header["count"] = len(frames)

    header = msgpack.dumps(header, use_bin_type=True)
    frames2 = [header, *frames]
    frames2.insert(0, pack_frames_prelude(frames2))
    return frames2


def serialize_bytes(x, **kwargs):
    L = serialize_bytelist(x, **kwargs)
    return b"".join(L)


def deserialize_bytes(b):
    frames = unpack_frames(b)
    header, frames = frames[0], frames[1:]
    if header:
        header = msgpack.loads(header, raw=False, use_list=False)
    else:
        header = {}
    frames = decompress(header, frames)
    return merge_and_deserialize(header, frames)


################################
# Class specific serialization #
################################


def register_serialization(cls, serialize, deserialize):
    """Register a new class for dask-custom serialization

    Parameters
    ----------
    cls : type
    serialize : callable(cls) -> Tuple[Dict, List[bytes]]
    deserialize : callable(header: Dict, frames: List[bytes]) -> cls

    Examples
    --------
    >>> class Human:
    ...     def __init__(self, name):
    ...         self.name = name

    >>> def serialize(human):
    ...     header = {}
    ...     frames = [human.name.encode()]
    ...     return header, frames

    >>> def deserialize(header, frames):
    ...     return Human(frames[0].decode())

    >>> register_serialization(Human, serialize, deserialize)
    >>> serialize(Human('Alice'))
    ({}, [b'Alice'])

    See Also
    --------
    serialize
    deserialize
    """
    if isinstance(cls, str):
        raise TypeError(
            "Strings are no longer accepted for type registration. "
            "Use dask_serialize.register_lazy instead"
        )
    dask_serialize.register(cls)(serialize)
    dask_deserialize.register(cls)(deserialize)


def register_serialization_lazy(toplevel, func):
    """Register a registration function to be called if *toplevel*
    module is ever loaded.
    """
    raise Exception("Serialization registration has changed. See documentation")


@partial(normalize_token.register, Serialized)
def normalize_Serialized(o):
    return [o.header] + o.frames  # for dask.base.tokenize


# Teach serialize how to handle bytes
@dask_serialize.register(bytes)
def _serialize_bytes(obj):
    header = {}  # no special metadata
    frames = [obj]
    return header, frames


# Teach serialize how to handle bytestrings
@dask_serialize.register(bytearray)
def _serialize_bytearray(obj):
    header = {}  # no special metadata
    frames = [obj]
    return header, frames


@dask_deserialize.register(bytes)
def _deserialize_bytes(header, frames):
    if len(frames) == 1 and isinstance(frames[0], bytes):
        return frames[0]
    else:
        return bytes().join(frames)


@dask_deserialize.register(bytearray)
def _deserialize_bytearray(header, frames):
    if len(frames) == 1 and isinstance(frames[0], bytearray):
        return frames[0]
    else:
        return bytearray().join(frames)


@dask_serialize.register(array)
def _serialize_array(obj):
    header = {"typecode": obj.typecode, "writeable": (None,)}
    frames = [memoryview(obj)]
    return header, frames


@dask_deserialize.register(array)
def _deserialize_array(header, frames):
    a = array(header["typecode"])
    for f in map(memoryview, frames):
        try:
            f = f.cast("B")
        except TypeError:
            f = f.tobytes()
        a.frombytes(f)
    return a


@dask_serialize.register(memoryview)
def _serialize_memoryview(obj):
    if obj.format == "O":
        raise ValueError("Cannot serialize `memoryview` containing Python objects")
    header = {"format": obj.format, "shape": obj.shape}
    frames = [obj]
    return header, frames


@dask_deserialize.register(memoryview)
def _deserialize_memoryview(header, frames):
    if len(frames) == 1:
        out = memoryview(frames[0]).cast("B")
    else:
        out = memoryview(b"".join(frames))
    out = out.cast(header["format"], header["shape"])
    return out


#########################
# Descend into __dict__ #
#########################


def _is_msgpack_serializable(v):
    typ = type(v)
    return (
        v is None
        or typ is str
        or typ is bool
        or typ is int
        or typ is float
        or isinstance(v, dict)
        and all(map(_is_msgpack_serializable, v.values()))
        and all(typ is str for x in v.keys())
        or isinstance(v, (list, tuple))
        and all(map(_is_msgpack_serializable, v))
    )


class ObjectDictSerializer:
    def __init__(self, serializer):
        self.serializer = serializer

    def serialize(self, est):
        header = {
            "serializer": self.serializer,
            "type-serialized": pickle.dumps(type(est), protocol=4),
            "simple": {},
            "complex": {},
        }
        frames = []

        if isinstance(est, dict):
            d = est
        else:
            d = est.__dict__

        for k, v in d.items():
            if _is_msgpack_serializable(v):
                header["simple"][k] = v
            else:
                if isinstance(v, dict):
                    h, f = self.serialize(v)
                else:
                    h, f = serialize(v, serializers=(self.serializer, "pickle"))
                header["complex"][k] = {
                    "header": h,
                    "start": len(frames),
                    "stop": len(frames) + len(f),
                }
                frames += f
        return header, frames

    def deserialize(self, header, frames):
        cls = pickle.loads(header["type-serialized"])
        if issubclass(cls, dict):
            dd = obj = {}
        else:
            obj = object.__new__(cls)
            dd = obj.__dict__
        dd.update(header["simple"])
        for k, d in header["complex"].items():
            h = d["header"]
            f = frames[d["start"] : d["stop"]]
            v = deserialize(h, f)
            dd[k] = v

        return obj


dask_object_with_dict_serializer = ObjectDictSerializer("dask")

dask_deserialize.register(dict)(dask_object_with_dict_serializer.deserialize)


def register_generic(
    cls,
    serializer_name="dask",
    serialize_func=dask_serialize,
    deserialize_func=dask_deserialize,
):
    """Register (de)serialize to traverse through __dict__

    Normally when registering new classes for Dask's custom serialization you
    need to manage headers and frames, which can be tedious.  If all you want
    to do is traverse through your object and apply serialize to all of your
    object's attributes then this function may provide an easier path.

    This registers a class for the custom Dask serialization family.  It
    serializes it by traversing through its __dict__ of attributes and applying
    ``serialize`` and ``deserialize`` recursively.  It collects a set of frames
    and keeps small attributes in the header.  Deserialization reverses this
    process.

    This is a good idea if the following hold:

    1.  Most of the bytes of your object are composed of data types that Dask's
        custom serializtion already handles well, like Numpy arrays.
    2.  Your object doesn't require any special constructor logic, other than
        object.__new__(cls)

    Examples
    --------
    >>> import sklearn.base
    >>> from distributed.protocol import register_generic
    >>> register_generic(sklearn.base.BaseEstimator)

    See Also
    --------
    dask_serialize
    dask_deserialize
    """
    object_with_dict_serializer = ObjectDictSerializer(serializer_name)
    serialize_func.register(cls)(object_with_dict_serializer.serialize)
    deserialize_func.register(cls)(object_with_dict_serializer.deserialize)
