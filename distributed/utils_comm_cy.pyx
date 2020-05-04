from dask.optimization import SubgraphCallable
from .utils import All, tokey

collection_types = (tuple, list, set, frozenset)

cdef class WrappedKey:
    """ Interface for a key in a dask graph.

    Subclasses must have .key attribute that refers to a key in a dask graph.

    Sometimes we want to associate metadata to keys in a dask graph.  For
    example we might know that that key lives on a particular machine or can
    only be accessed in a certain way.  Schedulers may have particular needs
    that can only be addressed by additional metadata.
    """
    cdef public:
        cdef object key

    def __init__(self, key):
        self.key = key

    def __repr__(self):
        return "%s('%s')" % (type(self).__name__, self.key)


def unpack_remotedata(o, byte_keys=False, myset=None):
    """ Unpack WrappedKey objects from collection

    Returns original collection and set of all found WrappedKey objects

    Examples
    --------
    >>> rd = WrappedKey('mykey')
    >>> unpack_remotedata(1)
    (1, set())
    >>> unpack_remotedata(())
    ((), set())
    >>> unpack_remotedata(rd)
    ('mykey', {WrappedKey('mykey')})
    >>> unpack_remotedata([1, rd])
    ([1, 'mykey'], {WrappedKey('mykey')})
    >>> unpack_remotedata({1: rd})
    ({1: 'mykey'}, {WrappedKey('mykey')})
    >>> unpack_remotedata({1: [rd]})
    ({1: ['mykey']}, {WrappedKey('mykey')})

    Use the ``byte_keys=True`` keyword to force string keys

    >>> rd = WrappedKey(('x', 1))
    >>> unpack_remotedata(rd, byte_keys=True)
    ("('x', 1)", {WrappedKey('('x', 1)')})
    """
    if myset is None:
        myset = set()
        out = unpack_remotedata(o, byte_keys, myset)
        return out, myset

    typ = type(o)

    if typ is tuple:
        if not o:
            return o
        if type(o[0]) is SubgraphCallable:
            sc = o[0]
            futures = set()
            dsk = {
                k: unpack_remotedata(v, byte_keys, futures) for k, v in sc.dsk.items()
            }
            args = tuple(unpack_remotedata(i, byte_keys, futures) for i in o[1:])
            if futures:
                myset.update(futures)
                futures = (
                    tuple(tokey(f.key) for f in futures)
                    if byte_keys
                    else tuple(f.key for f in futures)
                )
                inkeys = sc.inkeys + futures
                return (
                    (SubgraphCallable(dsk, sc.outkey, inkeys, sc.name),)
                    + args
                    + futures
                )
            else:
                return o
        else:
            return tuple(unpack_remotedata(item, byte_keys, myset) for item in o)
    if typ in collection_types:
        if not o:
            return o
        outs = [unpack_remotedata(item, byte_keys, myset) for item in o]
        return typ(outs)
    elif typ is dict:
        if o:
            values = [unpack_remotedata(v, byte_keys, myset) for v in o.values()]
            return dict(zip(o.keys(), values))
        else:
            return o
    elif issubclass(typ, WrappedKey):  # TODO use type is Future
        k = o.key
        if byte_keys:
            k = tokey(k)
        myset.add(o)
        return k
    else:
        return o


def pack_data(o, d, key_types=object):
    """ Merge known data into tuple or dict

    Parameters
    ----------
    o:
        core data structures containing literals and keys
    d: dict
        mapping of keys to data

    Examples
    --------
    >>> data = {'x': 1}
    >>> pack_data(('x', 'y'), data)
    (1, 'y')
    >>> pack_data({'a': 'x', 'b': 'y'}, data)  # doctest: +SKIP
    {'a': 1, 'b': 'y'}
    >>> pack_data({'a': ['x'], 'b': 'y'}, data)  # doctest: +SKIP
    {'a': [1], 'b': 'y'}
    """
    typ = type(o)
    try:
        if isinstance(o, key_types) and o in d:
            return d[o]
    except TypeError:
        pass

    if typ in collection_types:
        return typ([pack_data(x, d, key_types=key_types) for x in o])
    elif typ is dict:
        return {k: pack_data(v, d, key_types=key_types) for k, v in o.items()}
    else:
        return o


def subs_multiple(o, d):
    """ Perform substitutions on a tasks

    Parameters
    ----------
    o:
        Core data structures containing literals and keys
    d: dict
        Mapping of keys to values

    Examples
    --------
    >>> dsk = {"a": (sum, ["x", 2])}
    >>> data = {"x": 1}
    >>> subs_multiple(dsk, data)  # doctest: +SKIP
    {'a': (sum, [1, 2])}

    """
    typ = type(o)
    if typ is tuple and o and callable(o[0]):  # istask(o)
        return (o[0],) + tuple(subs_multiple(i, d) for i in o[1:])
    elif typ is list:
        return [subs_multiple(i, d) for i in o]
    elif typ is dict:
        return {k: subs_multiple(v, d) for (k, v) in o.items()}
    else:
        try:
            return d.get(o, o)
        except TypeError:
            return o

