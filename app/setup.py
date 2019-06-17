#!/usr/bin/env python
#
# setup.py
#
# installs frivenmeld package so you
# can import the modules without
# having to be in any specific directory
#
# this is installed while in the venv
# with:
#
#   pip install -e .
# 
# (which is called from setup.sh)
#
from distutils.core import setup

setup(name='frivenmeld',
      version='1.0',
      description='Friendly Vender Meld ETL',
      author='Matthew Flood',
      author_email='matthew.data.flood@gmail.com',
      url='https://www.github.com/mflood/',
      packages=['frivenmeld'],
      install_requires=[
          'requests',
          'PyMySQL',
      ],
     )
