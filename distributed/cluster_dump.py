"Utilities for generating and analyzing cluster dumps"

from __future__ import annotations

from typing import IO, Any, Callable, Literal


def _tuple_to_list(node):
    if isinstance(node, (list, tuple)):
        return [_tuple_to_list(el) for el in node]
    elif isinstance(node, dict):
        return {k: _tuple_to_list(v) for k, v in node.items()}
    else:
        return node


def url_and_writer(
    url: str, format: Literal["msgpack", "yaml"]
) -> tuple[str, Literal["w", "wb"], Callable[[Any, IO], None]]:
    "Get the URL/path and a serialization function for a cluster dump format"
    mode: Literal["w", "wb"]
    writer: Callable[[Any, IO], None]
    if format == "msgpack":
        import msgpack

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

    return url, mode, writer
