#!/bin/bash
#
# this is to zip all data edgelists (csv files) to save space
#

set -e

find dumps/* -name "data.edgelist.csv" -type f -print0 | while read -d $'\0' file 
do
    FILE=$file
    echo "Doing $FILE"
    FOLDER=`echo ${FILE%/*}`
    tar -czf $FOLDER/data.edgelist.tar.gz $FILE
done

