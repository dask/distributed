from typing import Iterable


def scheduler_story(keys: set, transition_log: Iterable) -> list:
    """Creates a story from the scheduler transition log given a set of keys
    describing tasks or stimuli.

    Parameters
    ----------
    keys : set
        A set of task `keys` or `stimulus_id`'s
    log : iterable
        The scheduler transition log

    Returns
    -------
    story : list
    """
    return [t for t in transition_log if t[0] in keys or keys.intersection(t[3])]


def worker_story(keys: set, log: Iterable) -> list:
    """Creates a story from the worker log given a set of keys
    describing tasks or stimuli.

    Parameters
    ----------
    keys : set
        A set of task `keys` or `stimulus_id`'s
    log : iterable
        The worker log

    Returns
    -------
    story : list
    """
    return [
        msg
        for msg in log
        if any(key in msg for key in keys)
        or any(
            key in c for key in keys for c in msg if isinstance(c, (tuple, list, set))
        )
    ]
