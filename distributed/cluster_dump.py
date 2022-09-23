"Utilities for generating and analyzing cluster dumps"

from __future__ import annotations

import threading
from collections.abc import Mapping
from contextlib import contextmanager, nullcontext
from functools import partial
from pathlib import Path
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Collection,
    Iterator,
    Literal,
)

import fsspec
import msgpack

from distributed._stories import msg_with_datetime
from distributed._stories import scheduler_story as _scheduler_story
from distributed._stories import worker_story as _worker_story
from distributed.compatibility import to_thread

if TYPE_CHECKING:
    import yaml

DEFAULT_CLUSTER_DUMP_FORMAT: Literal["msgpack", "yaml"] = "msgpack"
DEFAULT_CLUSTER_DUMP_EXCLUDE: Collection[str] = ("run_spec",)


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
    format: Literal["msgpack", "yaml"] = DEFAULT_CLUSTER_DUMP_FORMAT,
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

        def writer(state: dict, f: IO) -> None:
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


def load_cluster_dump(url: str, **kwargs: Any) -> dict:
    """Loads a cluster dump from a disk artefact

    Parameters
    ----------
    url : str
        Name of the disk artefact. This should have either a
        ``.msgpack.gz`` or ``yaml`` suffix, depending on the dump format.
    **kwargs :
        Extra arguments passed to :func:`fsspec.open`.

    Returns
    -------
    state : dict
        The cluster state at the time of the dump.
    """
    if url.endswith(".msgpack.gz"):
        mode = "rb"
        reader = partial(msgpack.unpack, strict_map_key=False)
    elif url.endswith(".yaml"):
        import yaml

        mode = "r"
        reader = yaml.safe_load
    else:
        raise ValueError(f"url ({url}) must have a .msgpack.gz or .yaml suffix")

    kwargs.setdefault("compression", "infer")

    with fsspec.open(url, mode, **kwargs) as f:
        return reader(f)


class DumpArtefact(Mapping):
    """
    Utility class for inspecting the state of a cluster dump

    .. code-block:: python

        dump = DumpArtefact.from_url("dump.msgpack.gz")
        memory_tasks = dump.scheduler_tasks("memory")
        executing_tasks = dump.worker_tasks("executing")
    """

    def __init__(self, state: dict):
        self.dump = state

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> DumpArtefact:
        """Loads a cluster dump from a disk artefact

        Parameters
        ----------
        url : str
            Name of the disk artefact. This should have either a
            ``.msgpack.gz`` or ``yaml`` suffix, depending on the dump format.
        **kwargs :
            Extra arguments passed to :func:`fsspec.open`.

        Returns
        -------
        state : dict
            The cluster state at the time of the dump.
        """
        return DumpArtefact(load_cluster_dump(url, **kwargs))

    def __getitem__(self, key):
        return self.dump[key]

    def __iter__(self):
        return iter(self.dump)

    def __len__(self):
        return len(self.dump)

    def _extract_tasks(self, state: str | None, context: dict[str, dict]) -> list[dict]:
        if state:
            return [v for v in context.values() if v["state"] == state]
        else:
            return list(context.values())

    @staticmethod
    def _slugify_addr(addr: str) -> str:
        return addr.replace("://", "-").replace("/", "_")

    def processing_on(self) -> dict[str, str]:
        "Tasks currently in ``processing`` on the scheduler, and which worker they're processing on"
        return {
            t["key"]: t["processing_on"]
            for t in self.scheduler_tasks_in_state("processing")
        }

    def scheduler_tasks_in_state(self, state: str | None = None) -> list:
        """
        Parameters
        ----------
        state : optional, str
            If provided, only tasks in the given state are returned.
            Otherwise, all tasks are returned.

        Returns
        -------
        tasks : list
            The list of scheduler tasks in ``state``.
        """
        return self._extract_tasks(state, self.dump["scheduler"]["tasks"])

    def worker_tasks_in_state(self, state: str | None = None) -> list:
        """
        Parameters
        ----------
        state : optional, str
            If provided, only tasks in the given state are returned.
            Otherwise, all tasks are returned.

        Returns
        -------
        tasks : list
            The list of worker tasks in ``state``
        """
        tasks = []

        for worker_dump in self.dump["workers"].values():
            if isinstance(worker_dump, dict) and "tasks" in worker_dump:
                tasks.extend(self._extract_tasks(state, worker_dump["tasks"]))

        return tasks

    def scheduler_story(self, *key_or_stimulus_id: str, datetimes: bool = True) -> list:
        """
        Returns
        -------
        story : list
            A list of events for the keys/stimulus ID's in ``*key_or_stimulus_id``.
        """
        keys = set(key_or_stimulus_id)
        return [
            tuple(s)
            for s in _scheduler_story(
                keys, self.dump["scheduler"]["transition_log"], datetimes=datetimes
            )
        ]

    def scheduler_short_story(self, *key_or_stimulus_id: str) -> list[str]:
        """
        Returns
        -------
        story : list
            A list of just the final states for the keys/stimulus ID's in ``*key_or_stimulus_id``.
        """
        return [x[2] for x in self.scheduler_story(*key_or_stimulus_id)]

    def scheduler_short_stories(self, *key_or_stimulus_id: str) -> dict[str, list[str]]:
        """
        Returns
        -------
        stories : dict
            A dict of the short story for each key or stimulus ID. Keys missing from the logs are dropped.
        """
        return {
            k: s
            for k, s in ((k, self.scheduler_short_story(k)) for k in key_or_stimulus_id)
            if s
        }

    def worker_stories(self, *key_or_stimulus_id: str, datetimes: bool = True) -> dict:
        """
        Returns
        -------
        stories : dict
            A dict for each worker of the story for all the keys/stimulus IDs
            in ``*key_or_stimulus_id`.`
        """
        keys = set(key_or_stimulus_id)
        return {
            addr: [tuple(s) for s in _worker_story(keys, wlog, datetimes=datetimes)]
            for addr, worker_dump in self.dump["workers"].items()
            if isinstance(worker_dump, dict) and (wlog := worker_dump.get("log"))
        }

    def worker_short_stories(self, key_or_stimulus_id: str) -> dict[str, list[str]]:
        """
        Returns
        -------
        stories : dict
            A dict of the short story for the key or stimulus ID, for each worker.
            Workers missing from the logs are dropped.
        """
        return {
            addr: [
                x[2]
                for x in story
                if x[0] == key_or_stimulus_id or x[-1] == key_or_stimulus_id
            ]
            for addr, story in self.worker_stories(
                key_or_stimulus_id, datetimes=False
            ).items()
            if story
        }

    def worker_stories_to_yamls(
        self, root_dir: str | Path | None = None, *key_or_stimulus_id: str
    ) -> None:
        """
        Write the results of `worker_stories` to separate YAML files per worker.
        """
        import yaml

        root_dir = Path(root_dir) if root_dir else Path.cwd()

        stories = self.worker_stories(*key_or_stimulus_id)
        for i, (addr, story) in enumerate(stories.items(), 1):
            worker_dir = root_dir / self._slugify_addr(addr)
            worker_dir.mkdir(parents=True, exist_ok=True)
            path = (
                worker_dir
                / f"story-{key_or_stimulus_id[0] if len(key_or_stimulus_id) == 1 else key_or_stimulus_id}.yaml"
            )

            print(f"Dumping story {i:>3}/{len(stories)} to {path}")
            with open(path, "w") as f:
                yaml.dump([list(s) for s in story], f, Dumper=yaml.CSafeDumper)

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
            for w in scheduler_workers
            if w not in responsive_workers
            or not isinstance(responsive_workers[w], dict)
        ]

    def _compact_state(self, state: dict, expand_keys: set[str]) -> dict[str, Any]:
        """Compacts ``state`` keys into a general key,
        unless the key is in ``expand_keys``"""
        assert "general" not in state
        result = {}
        general = {}

        for k, v in state.items():
            if k in expand_keys:
                result[k] = v
            else:
                general[k] = v

        result["general"] = general
        return result

    def to_yamls(
        self,
        root_dir: str | Path | None = None,
        worker_expand_keys: Collection[str] = (
            "config",
            "data",
            "incoming_transfer_log",
            "outgoing_transfer_log",
            "pending_data_per_worker",
            "log",
            "logs",
            "stimulus_log",
            "tasks",
        ),
        scheduler_expand_keys: Collection[str] = (
            "clients",
            "events",
            "extensions",
            "log",
            "task_groups",
            "tasks",
            "transition_log",
            "workers",
        ),
        background: bool = False,
        log: bool | None = None,
    ) -> None | threading.Thread:
        """
        Splits the Dump Artefact into a tree of yaml files with
        ``root_dir`` as it's base.

        The root level of the tree contains a directory for the scheduler
        and directories for each individual worker.
        Each directory contains yaml files describing the state of the scheduler
        or worker when the artefact was created.

        In general, keys associated with the state are compacted into a ``general.yaml``
        file, unless they are in ``scheduler_expand_keys`` and ``worker_expand_keys``.

        Parameters
        ----------
        root_dir : str or Path
            The root directory into which the tree is written.
            Defaults to the current working directory if ``None``.
        worker_expand_keys : iterable of str
            An iterable of artefact worker keys that will be expanded
            into separate yaml files.
            Keys that are not in this iterable are compacted into a
            `general.yaml` file.
        scheduler_expand_keys : iterable of str
            An iterable of artefact scheduler keys that will be expanded
            into separate yaml files.
            Keys that are not in this iterable are compacted into a
            ``general.yaml`` file.
        background:
            If True, run in a separate daemon thread in the background.
            Returns the `threading.Thread` object immediately.
        log:
            Print progress updates if True. Defaults to None, which means
            False if ``background`` is True, and True otherwise.
        """
        if background:
            t = threading.Thread(
                target=self.to_yamls,
                name="to-yamls",
                kwargs=dict(
                    root_dir=root_dir,
                    worker_expand_keys=worker_expand_keys,
                    scheduler_expand_keys=scheduler_expand_keys,
                    background=False,
                    log=log if log is not None else False,
                ),
                daemon=True,
            )
            t.start()
            return t

        if log is None:
            log = True

        import yaml

        root_dir = Path(root_dir) if root_dir else Path.cwd()
        dumper = yaml.CSafeDumper
        scheduler_expand_keys = set(scheduler_expand_keys)
        worker_expand_keys = set(worker_expand_keys)

        workers = self.dump["workers"]
        for i, (addr, info) in enumerate(workers.items(), 1):
            if not isinstance(info, dict):
                if log:
                    print(f"Skipping worker {i:>3}/{len(workers)} - {info}")
                continue

            log_dir = root_dir / self._slugify_addr(addr)
            log_dir.mkdir(parents=True, exist_ok=True)

            if log:
                print(f"Dumping worker {i:>4}/{len(workers)} to {log_dir}")

            worker_state = self._compact_state(info, worker_expand_keys)

            for name, _logs in worker_state.items():
                filename = str(log_dir / f"{name}.yaml")
                if name == "log":
                    _logs = list(map(msg_with_datetime, _logs))
                with open(filename, "w") as fd:

                    with _block_literals(dumper) if name == "logs" else nullcontext():
                        yaml.dump(_logs, fd, Dumper=dumper)

        context = "scheduler"
        log_dir = root_dir / context
        log_dir.mkdir(parents=True, exist_ok=True)

        if log:
            print(f"Dumping scheduler to {log_dir}")

        # Compact smaller keys into a general dict
        scheduler_state = self._compact_state(self.dump[context], scheduler_expand_keys)
        for i, (name, _logs) in enumerate(scheduler_state.items(), 1):
            filename = str(log_dir / f"{name}.yaml")
            if log:
                print(f"    Dumping {i:>2}/{len(scheduler_state)} {filename}")

            if name == "transition_log":
                _logs = [msg_with_datetime(e) for e in _logs]

            if name == "events":
                _logs = {
                    k: [msg_with_datetime(e, idx=0) for e in events]
                    for k, events in _logs.items()
                }

            with open(filename, "w") as fd:
                with _block_literals(dumper) if name == "logs" else nullcontext():
                    yaml.dump(_logs, fd, Dumper=dumper)
        return None


@contextmanager
def _block_literals(dumper: type[yaml.Dumper | yaml.CDumper]) -> Iterator[None]:
    "Contextmanager to use literal-block YAML syntax for multiline strings. Not thread-safe."
    # based on https://stackoverflow.com/a/33300001/17100540
    original_respresenter = dumper.yaml_representers[str]

    def represent_str(self, data):
        if "\n" in data:
            return self.represent_scalar("tag:yaml.org,2002:str", data, style="|")
        return self.represent_scalar("tag:yaml.org,2002:str", data)

    dumper.add_representer(str, represent_str)

    try:
        yield
    finally:
        dumper.add_representer(str, original_respresenter)
