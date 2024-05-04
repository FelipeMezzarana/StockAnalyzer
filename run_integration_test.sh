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

TAG=stock_analyzer-integration-test
docker build --file Dockerfile.tests --tag $TAG .
mkdir -p coverage
docker run  --env-file secrets.env  $TAG \
 bash -c "python3 -m pytest 'tests/integration' -s"