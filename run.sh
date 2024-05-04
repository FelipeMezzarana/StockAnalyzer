#!/bin/sh

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

TAG=stock-analizer
docker build -f Dockerfile -t $TAG .
docker run \
 --volume="./src/database/":/src/database \
 --volume="./temp/":/temp \
 --env-file secrets.env \
   $TAG 


 