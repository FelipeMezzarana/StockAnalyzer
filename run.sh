#!/bin/sh

TAG=stock-analizer
docker build -f Dockerfile -t $TAG .
docker run \
 --volume="./src/database/":/src/database \
 --volume="./temp/":/temp \
 --env-file secrets.env \
   $TAG 


 