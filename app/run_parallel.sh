#!/bin/bash
#
# run_parallel.sh
#
# Demonstrates how to kick off the ETL 
# using a multiple workers - each
# responsible for a range of pages
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
# runs 2017-02-03 instead of 2017-02-02 so it does not conflict
# with records created by running the job in single worker mode.

echo "kicking off worker 1 (See /tmp/worker1.log)"
python frivenmeld/driver.py --report-date=2017-02-03 --delete-existing --workerid=1 --startpage=1   --endpage=50  > /tmp/worker1.log 2>&1 &

echo "kicking off worker 2 (See /tmp/worker2.log)"
python frivenmeld/driver.py --report-date=2017-02-03 --delete-existing --workerid=2 --startpage=51  --endpage=100 > /tmp/worker2.log 2>&1 &

echo "kicking off worker 3 (See /tmp/worker3.log)"
python frivenmeld/driver.py --report-date=2017-02-03 --delete-existing --workerid=3 --startpage=101               > /tmp/worker3.log 2>&1 &

echo "waiting for all workers to finish"
wait $(jobs -p)

echo "all workers complete."

grep "Elapsed Time" /tmp/worker[123].log
grep "Total Matches" /tmp/worker[123].log
