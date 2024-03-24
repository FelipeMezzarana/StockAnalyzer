#!/bin/sh

# Check if temp directory exists, create if not.
directory="temp"
if [ ! -d "$directory" ]; then
    echo "The directory $directory does not exist. Creating..."
    mkdir "$directory"
    echo "Directory created successfully!"
else
    echo "The directory $directory already exists."
fi

TAG=stock-analizer
docker build -f Dockerfile -t $TAG .
docker run \
 --volume="./src/database/":/src/database \
 --volume="./temp/":/temp \
 --env-file secrets.env \
   $TAG 


 