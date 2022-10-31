from __future__ import annotations

from datetime import datetime
from typing import Iterable, TypeVar


def scheduler_story(
    keys: set, transition_log: Iterable, datetimes: bool = False
) -> list:
    """Creates a story from the scheduler transition log given a set of keys
    describing tasks or stimuli.

    Parameters
    ----------
    keys : set
        A set of task `keys` or `stimulus_id`'s
    log : iterable
        The scheduler transition log
    datetimes : bool
        Whether to convert timestamps into `datetime.datetime` objects
        (default False)

    Returns
    -------
    story : list
    """
    return [
        msg_with_datetime(t) if datetimes else t
        for t in transition_log
        if t[0] in keys or keys.intersection(t[3])
    ]


def worker_story(keys: set, log: Iterable, datetimes: bool = False) -> list:
    """Creates a story from the worker log given a set of keys
    describing tasks or stimuli.

    Parameters
    ----------
    keys : set
        A set of task `keys` or `stimulus_id`'s
    log : iterable
        The worker log
    datetimes : bool
        Whether to convert timestamps into `datetime.datetime` objects
        (default False)

    Returns
    -------
    story : list
    """
    return [
        msg_with_datetime(msg) if datetimes else msg
        for msg in log
        if any(key in msg for key in keys)
        or any(
            key in c for key in keys for c in msg if isinstance(c, (tuple, list, set))
        )
    ]


T = TypeVar("T", list, tuple)


def msg_with_datetime(msg: T, idx: int = -1) -> T:
    if idx < 0:
        idx = len(msg) + idx
    dt = msg[idx]
    try:
        dt = datetime.fromtimestamp(dt)
    except (TypeError, ValueError):
        pass
    return msg[:idx] + type(msg)((dt,)) + msg[idx + 1 :]
