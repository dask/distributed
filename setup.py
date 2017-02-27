#!/usr/bin/env python

import os
from setuptools import setup
import sys
import versioneer

requires = open('requirements.txt').read().strip().split('\n')
install_requires = []
extras_require = {}
for r in requires:
    if ';' in r:
        # requirements.txt conditional dependencies need to be reformatted for wheels
        # to the form: `'[extra_name]:condition' : ['requirements']`
        req, cond = r.split(';', 1)
        cond = ':' + cond
        cond_reqs = extras_require.setdefault(cond, [])
        cond_reqs.append(req)
    else:
        install_requires.append(r)

setup(name='distributed',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      description='Distributed computing',
      url='https://distributed.readthedocs.io/en/latest/',
      maintainer='Matthew Rocklin',
      maintainer_email='mrocklin@gmail.com',
      license='BSD',
      package_data={ '': ['templates/index.html'], },
      include_package_data=True,
      install_requires=install_requires,
      extras_require=extras_require,
      packages=['distributed',
                'distributed.bokeh',
                'distributed.bokeh.background',
                'distributed.bokeh.status',
                'distributed.bokeh.tasks',
                'distributed.bokeh.workers',
                'distributed.cli',
                'distributed.deploy',
                'distributed.diagnostics',
                'distributed.protocol',
                'distributed.http'],
      long_description=(open('README.rst').read() if os.path.exists('README.rst')
                        else ''),
      entry_points='''
        [console_scripts]
        dask-ssh=distributed.cli.dask_ssh:go
        dask-submit=distributed.cli.dask_submit:go
        dask-remote=distributed.cli.dask_remote:go
        dask-scheduler=distributed.cli.dask_scheduler:go
        dask-worker=distributed.cli.dask_worker:go
      ''',
      zip_safe=False)
