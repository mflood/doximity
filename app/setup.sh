#!/bin/bash
#
# setup.sh
#
# Setss up python3 venv
#
# NOTE: Requires that python3 is available.
#
virtualenv venv -p python3
source venv/bin/activate

# install dependencies
pip install -r requirements.txt

# this installs the package that is
# defined in setup.py
pip install -e .
