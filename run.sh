#!/bin/sh

TAG=stock-analizer
docker build -f Dockerfile -t $TAG .
docker run \
 --volume="./src/database/":/src/database \
 --env-file secrets.env \
   $TAG 


 