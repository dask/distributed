#!/usr/bin/env python

import os

from setuptools import find_packages, setup

import versioneer

requires = open("requirements.txt").read().strip().split("\n")
install_requires = []
extras_require: dict = {}
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
    url="https://distributed.dask.org",
    project_urls={
        "Source": "https://github.com/dask/distributed",
    },
    maintainer="Matthew Rocklin",
    maintainer_email="mrocklin@gmail.com",
    python_requires=">=3.8",
    license="BSD",
    package_data={
        "": ["templates/index.html", "template.html"],
        "distributed": ["http/templates/*.html", "py.typed"],
    },
    include_package_data=True,
    install_requires=install_requires,
    extras_require=extras_require,
    packages=find_packages(exclude=["*tests*"]),
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
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Scientific/Engineering",
        "Topic :: System :: Distributed Computing",
    ],
    entry_points="""
        [console_scripts]
        dask-ssh=distributed.cli.dask_ssh:main
        dask-scheduler=distributed.cli.dask_scheduler:main
        dask-worker=distributed.cli.dask_worker:main
      """,
    # https://mypy.readthedocs.io/en/latest/installed_packages.html
    zip_safe=False,
)
