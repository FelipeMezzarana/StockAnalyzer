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

TAG=stock_analyzer-integration-test
docker build --file Dockerfile.integration --tag $TAG .
mkdir -p coverage
docker run  --env-file secrets.env  $TAG