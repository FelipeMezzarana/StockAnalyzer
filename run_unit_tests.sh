#!/usr/bin/env bash
TAG=stock_analyzer-unit-tests
docker build --file Dockerfile.unittests --tag $TAG .
mkdir -p coverage
docker run --rm --volume="$PWD/coverage/":/var/coverage/ $TAG