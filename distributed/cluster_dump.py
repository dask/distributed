"Utilities for generating and analyzing cluster dumps"

from __future__ import annotations

from typing import IO, Any, Awaitable, Callable, Literal

import fsspec
import msgpack

from distributed.compatibility import to_thread
from distributed.stories import scheduler_story, worker_story


def _tuple_to_list(node):
    if isinstance(node, (list, tuple)):
        return [_tuple_to_list(el) for el in node]
    elif isinstance(node, dict):
        return {k: _tuple_to_list(v) for k, v in node.items()}
    else:
        return node


async def write_state(
    get_state: Callable[[], Awaitable[Any]],
    url: str,
    format: Literal["msgpack", "yaml"],
    **storage_options: dict[str, Any],
) -> None:
    "Await a cluster dump, then serialize and write it to a path"
    if format == "msgpack":
        mode = "wb"
        suffix = ".msgpack.gz"
        if not url.endswith(suffix):
            url += suffix
        writer = msgpack.pack
    elif format == "yaml":
        import yaml

        mode = "w"
        suffix = ".yaml"
        if not url.endswith(suffix):
            url += suffix

        def writer(state: dict, f: IO):
            # YAML adds unnecessary `!!python/tuple` tags; convert tuples to lists to avoid them.
            # Unnecessary for msgpack, since tuples and lists are encoded the same.
            yaml.dump(_tuple_to_list(state), f)

    else:
        raise ValueError(
            f"Unsupported format {format!r}. Possible values are 'msgpack' or 'yaml'."
        )

    # Eagerly open the file to catch any errors before doing the full dump
    # NOTE: `compression="infer"` will automatically use gzip via the `.gz` suffix
    with fsspec.open(url, mode, compression="infer", **storage_options) as f:
        state = await get_state()
        # Write from a thread so we don't block the event loop quite as badly
        # (the writer will still hold the GIL a lot though).
        await to_thread(writer, state, f)


def load_cluster_dump(url: str):
    if url.endswith(".msgpack.gz"):
        mode = "rb"
        reader = msgpack.unpack
    elif url.endswith(".yaml"):
        import yaml

        mode = "r"
        reader = yaml.safe_load
    else:
        raise ValueError(f"url ({url}) must have a .msgpack.gz or .yaml suffix")

    with fsspec.open(url, mode, compression="infer") as f:
        return reader(f)


class DumpInspector:
    """
    Utility class for inspecting the state of a cluster dump

    .. code-block:: python

        inspector = DumpInspect("dump.msgpack.gz")
        memory_tasks = inspector.tasks_in_state("memory")
        released_tasks = inspector.tasks_in_state("released)
    """

    def __init__(self, url_or_state: str | dict):
        if isinstance(url_or_state, str):
            self.dump = load_cluster_dump(url_or_state)
        elif isinstance(url_or_state, dict):
            self.dump = url_or_state
        else:
            raise TypeError("'url_or_state' must be a str or dict")

    def tasks_in_state(self, state: str = "", workers: bool = False) -> dict:
        """
        Returns
        -------
        tasks : dict
            A dictionary of scheduler tasks with state `state`.
            worker tasks are included if `workers=True`
        """
        stasks = self.dump["scheduler"]["tasks"]

        if state:
            tasks = {k: v for k, v in stasks.items() if v["state"] == state}
        else:
            tasks = stasks.copy()

        if not workers:
            return tasks

        for worker_dump in self.dump["workers"].values():
            if state:
                tasks.update(
                    (k, v)
                    for k, v in worker_dump["tasks"].items()
                    if v["state"] == state
                )
            else:
                tasks.update(worker_dump["tasks"])

        return tasks

    def story(self, *key_or_stimulus_id: str, workers: bool = False) -> list:
        """
        Returns
        -------
        stories : list
            A list of stories for the keys/stimulus ID's in `*key_or_stimulus_id`.
            worker stories are included if `workers=True`
        """
        keys = set(key_or_stimulus_id)
        story = scheduler_story(keys, self.dump["scheduler"]["transition_log"])

        if not workers:
            return story

        for wdump in self.dump["workers"].values():
            story.extend(worker_story(keys, wdump["log"]))

        return story

    def missing_workers(self) -> list:
        """
        Returns
        -------
        missing : list
            A list of workers connected to the scheduler, but which
            did not respond to requests for a state dump.
        """
        scheduler_workers = self.dump["scheduler"]["workers"]
        responsive_workers = set(self.dump["workers"].keys())
        return [w for w in scheduler_workers.keys() if w not in responsive_workers]
