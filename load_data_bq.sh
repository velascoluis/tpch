#!/bin/bash

# Check if both arguments are provided
if [ $# -ne 3 ]; then
    echo "Usage: $0 <project_id> <dataset_id> <scale_factor>"
    exit 1
fi

# Get parameters
PROJECT_ID=$1
DATASET_ID=$2
SCALE_FACTOR=$3

DATA_FOLDER="data/tables/scale-${SCALE_FACTOR}" 

# Iterate through CSV files in the folder
for file in "$DATA_FOLDER"/*.csv; do

    filename=$(basename "$file" .csv)

    echo "Loading $file into table: $DATASET_ID.$filename"
    bq load --autodetect --replace=true --source_format=CSV "$PROJECT_ID:$DATASET_ID.$filename" "$file"                 
    if [ $? -ne 0 ]; then
        echo "Error loading $file. Check the logs for details."
        continue
    fi

done

