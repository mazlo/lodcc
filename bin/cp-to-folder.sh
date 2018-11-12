#!/bin/bash
#
# this is to cp all data.graph.gt.gz in dumps/* (final graphs) to a folder called "datasets".
# this folder may than be published, e.g.

set -e

find dumps/* -name "data.graph.gt.gz" -type f -print0 | while read -d $'\0' file 
do
    FILE=$file
    echo "Doing $FILE"
    FOLDER=`echo ${FILE%/*}`
    DATASET=`echo ${FOLDER##*/}`
    FOLDER_DEST="datasets/$DATASET"

    mkdir "$FOLDER_DEST"
    cp -p $FILE "$FOLDER_DEST/."
done

