from __future__ import print_function, division, absolute_import

import logging

from toolz import valmap, merge
from tornado import gen

from .progress import AllProgress

from ..core import connect, write, coerce_to_address
from ..scheduler import Scheduler
from ..worker import dumps_function


logger = logging.getLogger(__name__)


def counts(scheduler, allprogress):
    return merge({'all': valmap(len, allprogress.all)},
                 {state: valmap(len, allprogress.state[state])
                     for state in ['memory', 'erred', 'released']})


@gen.coroutine
def progress_stream(address, interval):
    """ Open a TCP connection to scheduler, receive progress messages

    The messages coming back are dicts containing counts of key groups::

        {'inc': {'all': 5, 'memory': 2, 'erred': 0, 'released': 1},
         'dec': {'all': 1, 'memory': 0, 'erred': 0, 'released': 0}}

    Parameters
    ----------
    address: address of scheduler
    interval: time between batches, in seconds

    Examples
    --------
    >>> stream = yield eventstream('127.0.0.1:8786', 0.100)  # doctest: +SKIP
    >>> print(yield read(stream))  # doctest: +SKIP
    """
    ip, port = coerce_to_address(address, out=tuple)
    stream = yield connect(ip, port)
    yield write(stream, {'op': 'feed',
                         'setup': dumps_function(AllProgress),
                         'function': dumps_function(counts),
                         'interval': interval,
                         'teardown': dumps_function(Scheduler.remove_plugin)})
    raise gen.Return(stream)


def progress_quads(msg):
    """

    Consumes messages like the following::

          {'all': {'inc': 5, 'dec': 1},
           'memory': {'inc': 2, 'dec': 0},
           'erred': {'inc': 0, 'dec': 1},
           'released': {'inc': 1, 'dec': 0}}

    """
    names = sorted(msg['all'], key=msg['all'].get, reverse=True)
    d = {k: [v.get(name, 0) for name in names] for k, v in msg.items()}
    d['name'] = names
    d['top'] = [i + 0.7 for i in range(len(names))]
    d['center'] = [i + 0.5 for i in range(len(names))]
    d['bottom'] = [i + 0.3 for i in range(len(names))]
    d['released_right'] = [r / a for r, a in zip(d['released'], d['all'])]
    d['memory_right'] = [(r + im) / a for r, im, a in
            zip(d['released'], d['memory'], d['all'])]
    d['fraction'] = ['%d / %d' % (im + r, a)
                   for im, r, a in zip(d['memory'], d['released'], d['all'])]
    d['erred_left'] = [1 - e / a for e, a in zip(d['erred'], d['all'])]
    return d


def progress_wedge(msg, row_length=10):
    """

    >>> msg = {'all': {'inc': 4, 'dec': 1},
    ...        'memory': {'inc': 2, 'dec': 0},
    ...        'erred': {'inc': 0, 'dec': 1},
    ...        'released': {'inc': 1, 'dec': 0}}

    >>> progress_wedge(msg)  # doctest: +SKIP
    {'x': [0, 1],
     'y': [0, 0],
     'lower-y': [-.5, -.5],
     'name': ['inc', 'dec'],
     'memory': [2, 0],
     'released': [1, 0],
     'erred': [0, 1],
     'memory-angle': [180, 180]  # start at 90 go clockwise
     'released-angle': [0, 90],
     'erred-angle': [90, 180]}
    """
    names = sorted(msg['all'], key=msg['all'].get, reverse=True)
    n = len(names)
    d = {k: [v.get(name, 0) for name in names] for k, v in msg.items()}
    d['name'] = names
    d['x'] = [i % row_length for i in range(n)]
    d['y'] = [i // row_length for i in range(n)]
    d['lower-y'] = [y - 0.5 for y in d['y']]
    for state in ['memory', 'erred', 'released', 'all']:
        d[state] = [msg[state].get(name, 0) for name in names]
    d['done'] = [m + r + e for m, r, e in zip(d['memory'], d['released'], d['erred'])]

    for angle in ['released-angle', 'memory-angle', 'erred-angle']:
        d[angle] = []

    for a, r, m, e in zip(d['all'], d['released'], d['memory'], d['erred']):
        ra = (90 - 360 * (r / a)) % 360 if r < a else 90.001
        ma = (ra - 360 * (m / a)) % 360 if m + r < a else 90.001
        ea = (ma - 360 * (e / a)) % 360 if e + m + r < a else 90.001
        d['released-angle'].append(ra)
        d['memory-angle'].append(ma)
        d['erred-angle'].append(ea)
    return d
