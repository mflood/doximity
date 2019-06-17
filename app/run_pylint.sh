#!/bin/sh
#
# run_pylint.sh
#
# runs pylint against all
# the python files
#
# Before running this, you need
# to have run ./setup.sh
# to set up the venv
#
source venv/bin/activate

pylint frivenmeld/friendly_vendor/__init__.py
pylint frivenmeld/friendly_vendor/friven_loader.py
pylint frivenmeld/friendly_vendor/friendly_vendor_api.py
pylint frivenmeld/combining_engine.py
pylint frivenmeld/driver.py
pylint frivenmeld/loggingsetup.py
pylint frivenmeld/melder.py
pylint frivenmeld/__init__.py
pylint frivenmeld/metrics_collector.py
pylint frivenmeld/doximity/__init__.py
pylint frivenmeld/doximity/mysql_writer.py
pylint frivenmeld/doximity/mysql_loader.py

pylint tests/friendly_vendor/test_friendly_vendor_api.py
pylint tests/test_metrics_collector.py
pylint tests/test_loggingsetup.py
pylint tests/test_melder.py

pylint validation/validation_test.py
