#!/bin/bash

PGDATA_DIR="${HOME}/Documents/postgres/stock_analyzer_volume"
if [ ! -d "$PGDATA_DIR" ]; then
  mkdir -p "$PGDATA_DIR"
  echo "Created directory: $PGDATA_DIR"
fi

docker-compose up -d