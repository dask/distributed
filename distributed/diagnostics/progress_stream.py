import logging

from tlz import valmap, merge

from .progress import AllProgress

from ..core import connect, coerce_to_address
from ..scheduler import Scheduler
from ..utils import key_split, color_of
from ..worker import dumps_function


logger = logging.getLogger(__name__)


def counts(scheduler, allprogress):
    return merge(
        {"all": valmap(len, allprogress.all), "nbytes": allprogress.nbytes},
        {
            state: valmap(len, allprogress.state[state])
            for state in ["memory", "erred", "released", "processing"]
        },
    )


async def progress_stream(address, interval):
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
    >>> stream = await eventstream('127.0.0.1:8786', 0.100)  # doctest: +SKIP
    >>> print(await read(stream))  # doctest: +SKIP
    """
    address = coerce_to_address(address)
    comm = await connect(address)
    await comm.write(
        {
            "op": "feed",
            "setup": dumps_function(AllProgress),
            "function": dumps_function(counts),
            "interval": interval,
            "teardown": dumps_function(Scheduler.remove_plugin),
        }
    )
    return comm


def progress_quads(msg, nrows=8, ncols=3):
    """

    >>> msg = {'all': {'inc': 5, 'dec': 1, 'add': 4},
    ...        'memory': {'inc': 2, 'dec': 0, 'add': 1},
    ...        'erred': {'inc': 0, 'dec': 1, 'add': 0},
    ...        'released': {'inc': 1, 'dec': 0, 'add': 1},
    ...        'processing': {'inc': 1, 'dec': 0, 'add': 2}}

    >>> progress_quads(msg, nrows=2)  # doctest: +SKIP
    {'name': ['inc', 'add', 'dec'],
     'left': [0, 0, 1],
     'right': [0.9, 0.9, 1.9],
     'top': [0, -1, 0],
     'bottom': [-.8, -1.8, -.8],
     'released': [1, 1, 0],
     'memory': [2, 1, 0],
     'erred': [0, 0, 1],
     'processing': [1, 0, 2],
     'done': ['3 / 5', '2 / 4', '1 / 1'],
     'released-loc': [.2/.9, .25 / 0.9, 1],
     'memory-loc': [3 / 5 / .9, .5 / 0.9, 1],
     'erred-loc': [3 / 5 / .9, .5 / 0.9, 1.9],
     'processing-loc': [4 / 5, 1 / 1, 1]}}
    """
    width = 0.9
    names = sorted(msg["all"], key=msg["all"].get, reverse=True)
    names = names[: nrows * ncols]
    n = len(names)
    d = {k: [v.get(name, 0) for name in names] for k, v in msg.items()}

    d["name"] = names
    d["show-name"] = [name if len(name) <= 15 else name[:12] + "..." for name in names]
    d["left"] = [i // nrows for i in range(n)]
    d["right"] = [i // nrows + width for i in range(n)]
    d["top"] = [-(i % nrows) for i in range(n)]
    d["bottom"] = [-(i % nrows) - 0.8 for i in range(n)]
    d["color"] = [color_of(name) for name in names]

    d["released-loc"] = []
    d["memory-loc"] = []
    d["erred-loc"] = []
    d["processing-loc"] = []
    d["done"] = []
    for r, m, e, p, a, l in zip(
        d["released"], d["memory"], d["erred"], d["processing"], d["all"], d["left"]
    ):
        rl = width * r / a + l
        ml = width * (r + m) / a + l
        el = width * (r + m + e) / a + l
        pl = width * (p + r + m + e) / a + l
        done = "%d / %d" % (r + m + e, a)
        d["released-loc"].append(rl)
        d["memory-loc"].append(ml)
        d["erred-loc"].append(el)
        d["processing-loc"].append(pl)
        d["done"].append(done)

    return d


def color_of_message(msg):
    if msg["status"] == "OK":
        split = key_split(msg["key"])
        return color_of(split)
    else:
        return "black"


colors = {
    "transfer": "red",
    "disk-write": "orange",
    "disk-read": "orange",
    "deserialize": "gray",
    "compute": color_of_message,
}


alphas = {
    "transfer": 0.4,
    "compute": 1,
    "deserialize": 0.4,
    "disk-write": 0.4,
    "disk-read": 0.4,
}


prefix = {
    "transfer": "transfer-",
    "disk-write": "disk-write-",
    "disk-read": "disk-read-",
    "deserialize": "deserialize-",
    "compute": "",
}


def task_stream_append(lists, msg, workers):
    key = msg["key"]
    name = key_split(key)
    startstops = msg.get("startstops", [])

    for startstop in startstops:
        color = colors[startstop["action"]]
        if type(color) is not str:
            color = color(msg)

        lists["start"].append((startstop["start"] + startstop["stop"]) / 2 * 1000)
        lists["duration"].append(1000 * (startstop["stop"] - startstop["start"]))
        lists["key"].append(key)
        lists["name"].append(prefix[startstop["action"]] + name)
        lists["color"].append(color)
        lists["alpha"].append(alphas[startstop["action"]])
        lists["worker"].append(msg["worker"])

        worker_thread = "%s-%d" % (msg["worker"], msg["thread"])
        lists["worker_thread"].append(worker_thread)
        if worker_thread not in workers:
            workers[worker_thread] = len(workers) / 2
        lists["y"].append(workers[worker_thread])

    return len(startstops)
