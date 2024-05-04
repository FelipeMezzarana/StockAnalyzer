#!/usr/bin/env bash

# Check if temp and database directory exists, create if not.
directories=("temp" "src/database")
for directory in "${directories[@]}"; do
    if [ ! -d "$directory" ]; then
        echo "The directory $directory does not exist. Creating..."
        mkdir -p "$directory"
        echo "Directory created successfully!"
    else
        echo "The directory $directory already exists."
    fi
done

TAG=stock_analyzer-unit-tests
docker build --file Dockerfile.unittests --tag $TAG .
mkdir -p coverage
docker run --rm --volume="$PWD/coverage/":/var/coverage/ $TAG