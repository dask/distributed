from typing import Iterable


def scheduler_story(keys: set, transition_log: Iterable):
    return [t for t in transition_log if t[0] in keys or keys.intersection(t[3])]


def worker_story(keys: set, log: Iterable):
    return [
        msg
        for msg in log
        if any(key in msg for key in keys)
        or any(
            key in c for key in keys for c in msg if isinstance(c, (tuple, list, set))
        )
    ]
