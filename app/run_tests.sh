#!/bin/sh
#
# run_tests.sh
#
# Runs unit tests
#
# Note: run ./setup.sh first
#
source venv/bin/activate
pip install -e .

# find and run tests in the ./tests/ directory
pytest tests

# use these args if you want to see logging output
#pytest -vv --log-cli-level 1 tests
