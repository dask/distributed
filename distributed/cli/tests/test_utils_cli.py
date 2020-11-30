import pytest

from distributed.cli.utils import format_table


def test_table_format():
    headers = ["name", "type", "workers", "threads", "memory", "created", "status"]
    rows = [
        ["abc123", "FooCluster", 4, 12, "17.18 GB", "2 minutes ago", "Running"],
        ["def456", "FooCluster", 2, 6, "4 GB", "8 minutes ago", "Running"],
    ]
    expected_output = (
        "NAME    TYPE        WORKERS  THREADS  MEMORY    CREATED        STATUS \n"
        "abc123  FooCluster        4       12  17.18 GB  2 minutes ago  Running\n"
        "def456  FooCluster        2        6  4 GB      8 minutes ago  Running"
    )
    actual_output = format_table(rows, headers=headers)
    assert actual_output == expected_output

    with pytest.raises(RuntimeError):
        format_table([["hello", "world"]], headers=["foo", "bar", "baz"])

    with pytest.raises(RuntimeError):
        format_table([["hello", "world"]], headers=["foo", 1])
