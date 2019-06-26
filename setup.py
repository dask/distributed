#!/usr/bin/env python

import os
import sys
from setuptools import setup
import versioneer


CURRENT_PYTHON = sys.version_info[:2]
REQUIRED_PYTHON = (3, 5)

# Borrowed from
# https://github.com/django/django/commit/32ade4d73b50aed77efdb9dd7371c17f89061afc
# This check and everything above must remain compatible with Python 2.7.
if CURRENT_PYTHON < REQUIRED_PYTHON:
    sys.stderr.write(
        """
==========================
Unsupported Python version
==========================

This version of distributed requires Python {}.{}, but you're trying to
install it on Python {}.{}.

This may be because you are using a version of pip that doesn't
understand the python_requires classifier. Make sure you
have pip >= 9.0 and setuptools >= 24.2, then try again:

    $ python -m pip install --upgrade pip setuptools
    $ python -m pip install distributed

This will install the latest version of distributed that works on your
version of Python. If you can't upgrade your pip (or Python), request
an older version of distributed:

    $ python -m pip install "distributed<2"
""".format(
            *(REQUIRED_PYTHON + CURRENT_PYTHON)
        )
    )
    sys.exit(1)


requires = open("requirements.txt").read().strip().split("\n")
install_requires = []
extras_require = {}
for r in requires:
    if ";" in r:
        # requirements.txt conditional dependencies need to be reformatted for wheels
        # to the form: `'[extra_name]:condition' : ['requirements']`
        req, cond = r.split(";", 1)
        cond = ":" + cond
        cond_reqs = extras_require.setdefault(cond, [])
        cond_reqs.append(req)
    else:
        install_requires.append(r)

setup(
    name="distributed",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Distributed scheduler for Dask",
    url="https://distributed.readthedocs.io/en/latest/",
    maintainer="Matthew Rocklin",
    maintainer_email="mrocklin@gmail.com",
    license="BSD",
    package_data={
        "": ["templates/index.html", "template.html"],
        "distributed": ["dashboard/templates/*.html"],
    },
    include_package_data=True,
    install_requires=install_requires,
    python_requires=">={}.{}".format(*REQUIRED_PYTHON),
    extras_require=extras_require,
    packages=[
        "distributed",
        "distributed.dashboard",
        "distributed.cli",
        "distributed.comm",
        "distributed.deploy",
        "distributed.diagnostics",
        "distributed.protocol",
    ],
    long_description=(
        open("README.rst").read() if os.path.exists("README.rst") else ""
    ),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Topic :: Scientific/Engineering",
        "Topic :: System :: Distributed Computing",
    ],
    entry_points="""
        [console_scripts]
        dask-ssh=distributed.cli.dask_ssh:go
        dask-submit=distributed.cli.dask_submit:go
        dask-remote=distributed.cli.dask_remote:go
        dask-scheduler=distributed.cli.dask_scheduler:go
        dask-worker=distributed.cli.dask_worker:go
        dask-mpi=distributed.cli.dask_mpi:go
      """,
    zip_safe=False,
)
