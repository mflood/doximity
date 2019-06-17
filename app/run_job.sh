#!/bin/bash
#
# run_job.sh
#
# Demonstrates how to kick off the ETL 
# using a single worker
#
# Prereqs:
#
# 0. Be in a python 3 environment
# 1. Setup venv with ./setup.sh
# 2. configure variables in local_env.sh
# 3. create target table by running DDL in schema/friendly_vendor_match.ddl.sql 
#
source local_env.sh
source venv/bin/activate

# delete-existing will allow you to re-run the report
python frivenmeld/driver.py --delete-existing --report-date=2017-02-02

