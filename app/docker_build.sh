#!/bin/sh
#
# docker_build.sh
#
# Run this script to create
# a docker image containing the
# frivenmeld job
#
docker build -t frivenmeld:latest --no-cache .
