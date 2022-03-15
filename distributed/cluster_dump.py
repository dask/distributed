"Utilities for generating and analyzing cluster dumps"

from __future__ import annotations

from pathlib import Path
from typing import IO, Any, Awaitable, Callable, Collection, Literal

import fsspec
import msgpack

from distributed.compatibility import to_thread
from distributed.stories import scheduler_story as _scheduler_story
from distributed.stories import worker_story as _worker_story


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


def load_cluster_dump(url: str) -> dict:
    """Loads a cluster dump from a disk artefact

    Parameters
    ----------
    url : str
        Name of the disk artefact. This should have either a
        `.msgpack.gz` or `yaml` suffix, depending on the dump format.

    Returns
    -------
    state : dict
        The cluster state at the time of the dump.
    """
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


class DumpArtefact:
    """
    Utility class for inspecting the state of a cluster dump

    .. code-block:: python

        dump = DumpArtefact("dump.msgpack.gz")
        memory_tasks = dump.scheduler_tasks("memory")
        executing_tasks = dump.worker_tasks("executing")
    """

    def __init__(self, url_or_state: str | dict):
        if isinstance(url_or_state, str):
            self.dump = load_cluster_dump(url_or_state)
        elif isinstance(url_or_state, dict):
            self.dump = url_or_state
        else:
            raise TypeError("'url_or_state' must be a str or dict")

    def _extract_tasks(self, state: str, context: dict):
        if state:
            return [v for v in context.values() if v["state"] == state]
        else:
            return list(context.values())

    def scheduler_tasks(self, state: str = "") -> list:
        """
        Returns
        -------
        tasks : list
            The list of scheduler tasks in `state`.
        """
        return self._extract_tasks(state, self.dump["scheduler"]["tasks"])

    def worker_tasks(self, state: str = "") -> list:
        """
        Returns
        -------
        tasks : list
            The list of worker tasks in `state`
        """
        tasks = []

        for worker_dump in self.dump["workers"].values():
            if self._valid_worker_dump(worker_dump):
                tasks.extend(self._extract_tasks(state, worker_dump["tasks"]))

        return tasks

    def _valid_worker_dump(self, worker_dump):
        # Worker dumps should be a dictionaries but can also be
        # strings describing comm Failures
        return isinstance(worker_dump, dict)

    def scheduler_story(self, *key_or_stimulus_id: str) -> list:
        """
        Returns
        -------
        stories : list
            A list of stories for the keys/stimulus ID's in `*key_or_stimulus_id`.
        """
        keys = set(key_or_stimulus_id)
        story = _scheduler_story(keys, self.dump["scheduler"]["transition_log"])
        return list(map(tuple, story))

    def worker_story(self, *key_or_stimulus_id: str) -> list:
        """
        Returns
        -------
        stories : list
            A list of stories for the keys/stimulus ID's in `*key_or_stimulus_id`.
        """
        keys = set(key_or_stimulus_id)
        stories: list = []

        for worker_dump in self.dump["workers"].values():
            if self._valid_worker_dump(worker_dump):
                # Stories are tuples, not lists
                story = _worker_story(keys, worker_dump["log"])
                stories.extend(map(tuple, story))

        return stories

    def missing_workers(self) -> list:
        """
        Returns
        -------
        missing : list
            A list of workers connected to the scheduler, but which
            did not respond to requests for a state dump.
        """
        scheduler_workers = self.dump["scheduler"]["workers"]
        responsive_workers = self.dump["workers"]
        return [
            w
            for w in scheduler_workers.keys()
            if w not in responsive_workers
            or not self._valid_worker_dump(responsive_workers[w])
        ]

    def split(
        self,
        root_dir: str | Path | None = None,
        worker_expand_keys: Collection[str] = ("config", "log", "logs", "tasks"),
        scheduler_expand_keys: Collection[str] = (
            "events",
            "extensions",
            "log",
            "task_groups",
            "tasks",
            "transition_log",
            "workers",
        ),
    ):
        """
        Splits the Dump Artefact into a tree of yaml files with
        `root_dir` as it's base.

        The root level of the tree contains a directory for the scheduler
        and directories for each individual worker.
        Each directory contains yaml files describing the state of the scheduler
        or worker when the artefact was created.

        In general, keys associated with the state are compacted into a `general.yaml`
        file, unless they are in `scheduler_expand_keys` and `worker_expand_keys`.

        Parameters
        ----------
        root_dir : str or Path
            The root directory into which the tree is written.
            Defaults to the current working directory if `None`.
        worker_expand_keys : iterable of str
            An iterable of artefact worker keys that will be expanded
            into separate yaml files.
            Keys that are not in this iterable are compacted into a
            `general.yaml` file.
        scheduler_expand_keys : iterable of str
            An iterable of artefact scheduler keys that will be expanded
            into separate yaml files.
            Keys that are not in this iterable are compacted into a
            `general.yaml` file.
        """
        import yaml

        root_dir = Path(root_dir) if root_dir else Path.cwd()
        dumper = yaml.CSafeDumper
        scheduler_expand_keys = set(scheduler_expand_keys)
        worker_expand_keys = set(worker_expand_keys)

        workers = self.dump["workers"]
        for info in workers.values():
            worker_id = info["id"]
            assert "general" not in info

            # Compact smaller keys into a general dict
            general = {
                k: info.pop(k) for k in list(info.keys()) if k not in worker_expand_keys
            }
            info["general"] = general

            log_dir = root_dir / worker_id
            log_dir.mkdir(parents=True, exist_ok=True)

            for name, _logs in info.items():
                filename = str(log_dir / f"{name}.yaml")
                with open(filename, "w") as fd:
                    yaml.dump(_logs, fd, Dumper=dumper)

        context = "scheduler"
        info = self.dump[context]
        assert "general" not in info
        general = {
            k: info.pop(k) for k in list(info.keys()) if k not in scheduler_expand_keys
        }
        info["general"] = general

        log_dir = root_dir / context
        log_dir.mkdir(parents=True, exist_ok=True)
        # Compact smaller keys into a general dict

        for name, _logs in info.items():
            filename = str(log_dir / f"{name}.yaml")

            with open(filename, "w") as fd:
                yaml.dump(_logs, fd, Dumper=dumper)
