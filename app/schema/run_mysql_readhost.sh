#!/bin/sh
#
# run_mysql_readhost.sh
#
# Utility script to connect directly 
# to mysql source instance
#
source ../local_env.sh
mysql -h $MYSQL_HOST -P $MYSQL_PORT -u $MYSQL_USER -p$MYSQL_PASS $MYSQL_SCHEMA
