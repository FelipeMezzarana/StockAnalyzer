#!/usr/bin/env bash

# Check if temp directory exists, create if not.
directory="temp"
if [ ! -d "$directory" ]; then
    echo "The directory $directory does not exist. Creating..."
    mkdir "$directory"
    echo "Directory created successfully!"
else
    echo "The directory $directory already exists."
fi

TAG=stock_analyzer-unit-tests
docker build --file Dockerfile.unittests --tag $TAG .
mkdir -p coverage
docker run --rm --volume="$PWD/coverage/":/var/coverage/ $TAG