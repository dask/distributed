#!/usr/bin/env python

import os
import sys
from setuptools import setup, find_packages
from setuptools.extension import Extension
import versioneer

requires = open("requirements.txt").read().strip().split("\n")
setup_requires = []
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

try:
    sys.argv.remove("--with-cython")
    cython = True
except ValueError:
    cython = False

ext_modules = []
if cython:
    try:
        import cython
    except ImportError:
        setup_requires.append("cython")

    cyext_modules = [
        Extension(
            "distributed.scheduler",
            sources=["distributed/scheduler.py"],
        ),
    ]
    for e in cyext_modules:
        e.cython_directives = {
            "annotation_typing": True,
            "binding": False,
            "embedsignature": True,
            "language_level": 3,
        }
    ext_modules.extend(cyext_modules)


setup(
    name="distributed",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Distributed scheduler for Dask",
    url="https://distributed.dask.org",
    maintainer="Matthew Rocklin",
    maintainer_email="mrocklin@gmail.com",
    python_requires=">=3.6",
    license="BSD",
    package_data={
        "": ["templates/index.html", "template.html"],
        "distributed": ["http/templates/*.html"],
    },
    include_package_data=True,
    setup_requires=setup_requires,
    install_requires=install_requires,
    extras_require=extras_require,
    packages=find_packages(exclude=["*tests*"]),
    ext_modules=ext_modules,
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
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Scientific/Engineering",
        "Topic :: System :: Distributed Computing",
    ],
    entry_points="""
        [console_scripts]
        dask-ssh=distributed.cli.dask_ssh:go
        dask-scheduler=distributed.cli.dask_scheduler:go
        dask-worker=distributed.cli.dask_worker:go
      """,
    zip_safe=False,
)
