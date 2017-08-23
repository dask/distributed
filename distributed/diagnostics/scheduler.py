from __future__ import print_function, division, absolute_import


try:
    from cytoolz import valmap
except ImportError:
    from toolz import valmap

from toolz import concat, countby

from ..metrics import time
from ..utils import key_split, log_errors


def tasks(s):
    """ Task and worker status of scheduler """
    processing = sum(map(len, s.processing.values()))

    with log_errors():
        return {'processing': processing,
                'total': len(s.tasks),
                'in-memory': len(s.who_has),
                'waiting': len(s.waiting),
                'failed': len(s.exceptions_blame)}


def workers(s):
    """ Information about workers

    Examples
    --------
    >>> workers(my_scheduler)  # doctest: +SKIP
    {'127.0.0.1': {'cores': 3,
                   'cpu': 0.0,
                   'last-seen': 0.003068,
                   'latency': 0.01584628690034151,
                   'addresses': ['tcp://127.0.0.1:54871', 'tcp://127.0.0.1:50943'],
                   'processing': {'inc': 2, 'add': 1},
                   'disk-read': 1234,
                   'disk-write': 1234,
                   'network-send': 1234,
                   'network-recv': 1234,
                   'memory': 16701911040,
                   'memory_percent': 85}}
    """
    hosts = {host: d['addresses']
             for host, d in s.host_info.items()}

    processing = {host: countby(key_split, concat(s.processing[w] for w in addrs))
                  for host, addrs in hosts.items()}

    now = time()

    result = {}
    for host, info in s.host_info.items():
        info = info.copy()
        # info = dissoc(info, 'heartbeat', 'heartbeat-port')
        info['processing'] = processing[host]
        result[host] = info
        info['addresses'] = sorted(info['addresses'])
        if 'last-seen' in info:
            info['last-seen'] = (now - info['last-seen'])

    return result


def processing(s):
    return {'processing': valmap(len, s.processing),
            'waiting': len(s.waiting),
            'memory': len(s.who_has),
            'ncores': s.ncores}
