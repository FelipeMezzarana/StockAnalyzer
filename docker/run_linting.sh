#!/bin/sh
# Usage: ./run_linting.sh [tox options, e.g. -e typing to only run typing]
TAG=tox-worker
docker build -f docker/Dockerfile.linting -t $TAG .
docker run $TAG tox
