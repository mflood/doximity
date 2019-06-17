#!/bin/sh
#
# docker_run.sh
#
# Use this to launch the job
# in a docker container
# that has already been built
# using ./docker_build.sh
#
# Note: You also need to fully
# configure the file named "docker.env"
# before running this script
#
docker run --name frivenmeld \
  --env-file=docker.env \
  --rm \
  -i \
  --log-driver=none \
  -a stdout \
  -a stderr \
  frivenmeld:latest
