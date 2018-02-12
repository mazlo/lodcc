#!/bin/bash

FOLDER_ROOT=${1:-"dumps/"}
USE_CACHE=${2:-true}

# remove backslash from FOLDER if present
if [[ "${FOLDER_ROOT%/}" != "$FOLDER_ROOT" ]]; then
    FOLDER_ROOT=${FOLDER_ROOT%/}
fi

FILES_COUNT=`find "$FOLDER_ROOT" -name "*.edgelist.csv" -type f | wc -l`

# removedata.edgelist, if present
if [[ $FILES_COUNT > 1 ]]; then
    rm "$FOLDER_ROOT/data.edgelist.csv"
else
    # if data.edgelist is the only file, we're fine
    if [ -f "$FOLDER_ROOT/data.edgelist.csv" ]; then
        exit 0
    fi
fi

FILES=`find "$FOLDER_ROOT" -name "*.edgelist.csv" -type f`

for file in $FILES; do
    
    # ignore data.edgelist, if present
    if [[ "${file%*/data.edgelist.csv}" == "$file" ]]; then
        FOLDER=${file%/*}
        cat $file >> "$FOLDER/data.edgelist.csv" &&  
        rm $file
    fi

done
