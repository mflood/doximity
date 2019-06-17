#!/bin/sh
#
# run_mysql_writehost.sh
#
# Utility script to connect directly 
# to mysql target instance
#
source ../local_env.sh
mysql -h $WRITE_MYSQL_HOST -P $WRITE_MYSQL_PORT -u $WRITE_MYSQL_USER -p$WRITE_MYSQL_PASS $WRITE_MYSQL_SCHEMA
