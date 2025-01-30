#!/bin/sh

# Check if temp and database directory exists, create if not.
directories=("temp" "database")
for directory in "${directories[@]}"; do
    if [ ! -d "$directory" ]; then
        echo "The directory $directory does not exist. Creating..."
        mkdir -p "$directory"
        echo "Directory created successfully!"
    else
        echo "The directory $directory already exists."
    fi
done

# Check if local instance network exists
NETWORK_NAME="stockanalyzer_stock_network"
if docker network ls | grep -q "$NETWORK_NAME"; then
    NETWORK_OPTION="--network $NETWORK_NAME"
    echo "Using network $NETWORK_NAME."
else
    NETWORK_OPTION=""
    echo "Network $NETWORK_NAME does not exist. Proceeding without a network."
fi


# Initialize variables
SCOPE=""
SUB_SCOPE=""
SKIP=""


# Parse CLI arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --sub-scope) SUB_SCOPE="$2"; shift ;;
        --skip) SKIP="$2"; shift ;;
        *) SCOPE="$1" ;;  # Any unnamed argument is treated as SCOPE (mandatory)
    esac
    shift
done

# Ensure SCOPE is provided (mandatory)
if [ -z "$SCOPE" ]; then
    echo "Error: The first argument (scope) is required."
    exit 1
fi

TAG=stock-analyzer
docker build -f Dockerfile -t $TAG .
docker run \
 --volume="./database/":/database \
 --volume="./temp/":/temp \
 --env-file secrets.env \
 $NETWORK_OPTION \
   $TAG python3 -m src.run "$SCOPE" ${SUB_SCOPE:+--sub-scope "$SUB_SCOPE"} ${SKIP:+--skip "$SKIP"}


 