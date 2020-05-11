""" utilities for package version introspection """

from __future__ import print_function, division, absolute_import

from collections import defaultdict
import platform
import struct
import os
import sys
import importlib


required_packages = [
    ("dask", lambda p: p.__version__),
    ("distributed", lambda p: p.__version__),
    ("msgpack", lambda p: ".".join([str(v) for v in p.version])),
    ("cloudpickle", lambda p: p.__version__),
    ("tornado", lambda p: p.version),
    ("toolz", lambda p: p.__version__),
]

optional_packages = [
    ("numpy", lambda p: p.__version__),
    ("lz4", lambda p: p.__version__),
    ("blosc", lambda p: p.__version__),
]


# only these scheduler packages will be checked for version mismatch
scheduler_relevant_packages = set(pkg for pkg, _ in required_packages) | set(
    ["lz4", "blosc"]
)


# notes to be displayed for mismatch packages
notes_mismatch_package = {
    "msgpack": "Variation is ok, as long as everything is above 0.6",
    "lz4": "Variation is ok, but missing libraries are not",
    "python": "Variation is sometimes ok, sometimes not.  It depends on your workloads",
}


def get_versions(packages=None):
    """
    Return basic information on our software installation, and our installed versions of packages.
    """
    if packages is None:
        packages = []

    d = {
        "host": get_system_info(),
        "packages": get_package_info(
            required_packages + optional_packages + list(packages)
        ),
    }

    return d


def get_system_info():
    (sysname, nodename, release, version, machine, processor) = platform.uname()
    host = {
        "python": "%d.%d.%d.%s.%s" % sys.version_info[:],
        "python-bits": struct.calcsize("P") * 8,
        "OS": "%s" % sysname,
        "OS-release": "%s" % release,
        "machine": "%s" % machine,
        "processor": "%s" % processor,
        "byteorder": "%s" % sys.byteorder,
        "LC_ALL": "%s" % os.environ.get("LC_ALL", "None"),
        "LANG": "%s" % os.environ.get("LANG", "None"),
    }

    return host


def version_of_package(pkg):
    """ Try a variety of common ways to get the version of a package """
    from .utils import ignoring

    with ignoring(AttributeError):
        return pkg.__version__
    with ignoring(AttributeError):
        return str(pkg.version)
    with ignoring(AttributeError):
        return ".".join(map(str, pkg.version_info))
    return None


def get_package_info(pkgs):
    """ get package versions for the passed required & optional packages """

    pversions = [("python", ".".join(map(str, sys.version_info)))]
    for pkg in pkgs:
        if isinstance(pkg, (tuple, list)):
            modname, ver_f = pkg
        else:
            modname = pkg
            ver_f = version_of_package

        if ver_f is None:
            ver_f = version_of_package

        try:
            mod = importlib.import_module(modname)
            ver = ver_f(mod)
            pversions.append((modname, ver))
        except Exception:
            pversions.append((modname, None))

    return dict(pversions)


def error_message(scheduler, workers, client, client_name="client"):
    from .utils import asciitable

    nodes = {**{client_name: client}, **{"scheduler": scheduler}, **workers}

    # Hold all versions, e.g. versions["scheduler"]["distributed"] = 2.9.3
    node_packages = defaultdict(dict)

    # Collect all package versions
    packages = set()
    for node, info in nodes.items():
        if info is None or not (isinstance(info, dict)) or "packages" not in info:
            node_packages[node] = defaultdict(lambda: "UNKNOWN")
        else:
            node_packages[node] = defaultdict(lambda: "MISSING")
            for pkg, version in info["packages"].items():
                node_packages[node][pkg] = version
                packages.add(pkg)

    errs = []
    notes = []
    for pkg in sorted(packages):
        versions = set(
            node_packages[node][pkg]
            for node in nodes
            if node != "scheduler" or pkg in scheduler_relevant_packages
        )
        if len(versions) <= 1:
            continue

        client_version = None
        scheduler_version = None
        workers_version = set()
        for node_name in nodes.keys():
            if node_name == "client":
                client_version = node_packages[node_name][pkg]
            elif node_name == "scheduler":
                scheduler_version = node_packages[node_name][pkg]
            else:
                workers_version.add(node_packages[node_name][pkg])

        if len(workers_version) == 1:
            workers_version = list(workers_version)[0]
        elif len(workers_version) == 0:
            workers_version = None

        errs.append((pkg, client_version, scheduler_version, workers_version))
        if pkg in notes_mismatch_package.keys():
            notes.append(f"-  {pkg}: {notes_mismatch_package[pkg]}")

    if errs:
        err_table = asciitable(["Package", "client", "scheduler", "workers"], errs)
        err_msg = f"Mismatched versions found\n\n{err_table}"
        if notes:
            err_msg += "\nNotes: \n{}".format("\n".join(notes))
        return err_msg
    else:
        return ""


class VersionMismatchWarning(Warning):
    """Indicates version mismatch between nodes"""
