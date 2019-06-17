#!/bin/sh
#
# run_coverage.sh
#
# Runs unit tests and reports
# on test coverage
#
# Note: run ./setup.sh first
#
source venv/bin/activate
pip install -e .
coverage run --source frivenmeld -m pytest tests
coverage report -m
