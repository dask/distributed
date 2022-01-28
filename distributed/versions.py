""" utilities for package version introspection """

from __future__ import annotations

import os
import platform
import struct
import sys
from collections.abc import Iterable
from itertools import chain
from typing import Any

if sys.version_info >= (3, 8):
    from importlib.metadata import version as _version
else:
    import pkg_resources

    def _version(distribution_name: str) -> str:
        return pkg_resources.get_distribution(distribution_name).version


required_packages = [
    "dask",
    "distributed",
    "msgpack",
    "cloudpickle",
    "tornado",
    "toolz",
]

optional_packages = [
    "numpy",
    "pandas",
    "lz4",
    "blosc",
]


# only these scheduler packages will be checked for version mismatch
scheduler_relevant_packages = set(required_packages) | {"lz4", "blosc"}


# notes to be displayed for mismatch packages
notes_mismatch_package = {
    "msgpack": "Variation is ok, as long as everything is above 0.6"
}


def get_versions(packages: Iterable[str] = ()) -> dict[str, dict[str, Any]]:
    """Return basic information on our software installation, and our installed versions
    of packages
    """
    return {
        "host": get_system_info(),
        "packages": {
            "python": ".".join(map(str, sys.version_info)),
            **get_package_info(chain(required_packages, optional_packages, packages)),
        },
    }


def get_system_info() -> dict[str, Any]:
    uname = platform.uname()
    return {
        "python": "%d.%d.%d.%s.%s" % sys.version_info,
        "python-bits": struct.calcsize("P") * 8,
        "OS": uname.system,
        "OS-release": uname.release,
        "machine": uname.machine,
        "processor": uname.processor,
        "byteorder": sys.byteorder,
        "LC_ALL": os.environ.get("LC_ALL", "None"),
        "LANG": os.environ.get("LANG", "None"),
    }


def get_package_info(pkgs: Iterable[str]) -> dict[str, str | None]:
    """get package versions for the passed required & optional packages"""

    pversions: dict[str, str | None] = {}
    for modname in pkgs:
        try:
            pversions[modname] = _version(modname)
        except Exception:
            pversions[modname] = None

    return pversions


def error_message(scheduler, workers, client, client_name="client"):
    from .utils import asciitable

    client = client.get("packages") if client else "UNKNOWN"
    scheduler = scheduler.get("packages") if scheduler else "UNKNOWN"
    workers = {k: v.get("packages") if v else "UNKNOWN" for k, v in workers.items()}

    packages = set()
    packages.update(client)
    packages.update(scheduler)
    for worker in workers:
        packages.update(workers.get(worker))

    errs = []
    notes = []
    for pkg in sorted(packages):
        versions = set()
        scheduler_version = (
            scheduler.get(pkg, "MISSING") if isinstance(scheduler, dict) else scheduler
        )
        if pkg in scheduler_relevant_packages:
            versions.add(scheduler_version)

        client_version = (
            client.get(pkg, "MISSING") if isinstance(client, dict) else client
        )
        versions.add(client_version)

        worker_versions = {
            workers[w].get(pkg, "MISSING")
            if isinstance(workers[w], dict)
            else workers[w]
            for w in workers
        }
        versions |= worker_versions

        if len(versions) <= 1:
            continue
        if len(worker_versions) == 1:
            worker_versions = list(worker_versions)[0]
        elif len(worker_versions) == 0:
            worker_versions = None

        errs.append((pkg, client_version, scheduler_version, worker_versions))
        if pkg in notes_mismatch_package.keys():
            notes.append(f"-  {pkg}: {notes_mismatch_package[pkg]}")

    out = {"warning": "", "error": ""}

    if errs:
        err_table = asciitable(["Package", client_name, "scheduler", "workers"], errs)
        err_msg = f"Mismatched versions found\n\n{err_table}"
        if notes:
            err_msg += "\nNotes: \n{}".format("\n".join(notes))
        out["warning"] += err_msg

    return out


class VersionMismatchWarning(Warning):
    """Indicates version mismatch between nodes"""
