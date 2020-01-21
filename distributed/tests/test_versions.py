import re

import pytest

from distributed.versions import get_versions, error_message
from distributed import Client, Worker
from distributed.utils_test import gen_cluster


# if every node (client, scheduler, workers) have this version, we're good
this_version = get_versions()

# if one of the nodes reports this version, there's a mismatch
mismatched_version = get_versions()
mismatched_version["packages"]["distributed"] = "0.0.0.dev0"

# for really old versions, the `package` key is missing - version is UNKNOWN
key_err_version = {}

# if no key is available for one package, we assume it's MISSING
missing_version = get_versions()
del missing_version["packages"]["distributed"]

# if a node doesn't report any version info, we treat them as UNKNOWN
# the happens if the node is pre-32cb96e, i.e. <=2.9.1
unknown_version = None


@pytest.fixture
def kwargs_matching():
    return dict(
        scheduler=this_version,
        workers={f"worker-{i}": this_version for i in range(3)},
        client=this_version,
    )


def test_versions_match(kwargs_matching):
    assert error_message(**kwargs_matching) == ""


@pytest.fixture(params=["client", "scheduler", "worker-1"])
def node(request):
    """Node affected by version mismatch."""
    return request.param


@pytest.fixture(params=["MISMATCHED", "MISSING", "KEY_ERROR", "NONE"])
def effect(request):
    """Specify type of mismatch."""
    return request.param


@pytest.fixture
def kwargs_not_matching(kwargs_matching, node, effect):
    affected_version = {
        "MISMATCHED": mismatched_version,
        "MISSING": missing_version,
        "KEY_ERROR": key_err_version,
        "NONE": unknown_version,
    }[effect]
    kwargs = kwargs_matching
    if node in kwargs["workers"]:
        kwargs["workers"][node] = affected_version
    else:
        kwargs[node] = affected_version
    return kwargs


@pytest.fixture
def pattern(effect):
    """Pattern to match in the right-hand column."""
    return {
        "MISMATCHED": r"0\.0\.0\.dev0",
        "MISSING": "MISSING",
        "KEY_ERROR": "UNKNOWN",
        "NONE": "UNKNOWN",
    }[effect]


def test_version_mismatch(node, effect, kwargs_not_matching, pattern):
    msg = error_message(**kwargs_not_matching)

    assert "Mismatched versions found" in msg
    assert "distributed" in msg
    assert re.search(node + r"\s+\|\s+" + pattern, msg)


@gen_cluster()
async def test_version_warning_in_cluster(s, a, b):
    s.workers[a.address].versions["packages"]["dask"] = "0.0.0"

    with pytest.warns(None) as record:
        async with Client(s.address, asynchronous=True) as client:
            pass

    assert record
    assert any("dask" in str(r.message) for r in record)
    assert any("0.0.0" in str(r.message) for r in record)
    assert any(a.address in str(r.message) for r in record)

    async with Worker(s.address) as w:
        assert any("This Worker" in line.message for line in w.logs)
        assert any("dask" in line.message for line in w.logs)
        assert any(
            "0.0.0" in line.message and a.address in line.message for line in w.logs
        )
