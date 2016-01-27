#!/usr/bin/env python

import os
from setuptools import setup
import sys

requires = open('requirements.txt').read().strip().split('\n')
if sys.version_info[0] < 3:
    requires.append('futures')
    requires.append('locket')
if sys.version_info < (3, 4):
    requires.append('singledispatch')

setup(name='distributed',
      version='1.7.4',
      description='Distributed computing',
      url='http://distributed.readthedocs.org/en/latest/',
      maintainer='Matthew Rocklin',
      maintainer_email='mrocklin@gmail.com',
      license='BSD',
      install_requires=requires,
      packages=['distributed', 'distributed.diagnostics', 'distributed.http'],
      long_description=(open('README.md').read() if os.path.exists('README.md')
                        else ''),
      scripts=[os.path.join('bin', name)
               for name in ['dcenter', 'dworker', 'dcluster', 'dscheduler']],
      zip_safe=False)
