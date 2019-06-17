#!/bin/sh
#
# run_validation.sh
#
# Runs validation tests
# by running queries against the
# target database table
#
# Note: run ./setup.sh first
# also, the job needs to run in order
# to populate the table that gets validated
#
source venv/bin/activate
pip install -e .
source local_env.sh

# find and run tests in the ./validation/ directory
pytest -vv --log-cli-level 1 validation
