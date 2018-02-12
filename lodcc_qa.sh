#!/bin/bash

FOLDER=${1:-"dumps/"}
FILES_LIST=`find $FOLDER* -name "*.edgelist.csv" -type f`

for f in $FILES_LIST; do
    echo -n "Analyzing $f.."

    ORIG_FILEPATH=$f
    NT_FILEPATH="${f%*.edgelist.csv}.nt"

    if [ -f $NT_FILEPATH ]; then
        ORIG_LINES=`wc -l $ORIG_FILEPATH | cut -d ' ' -f1 &`  
        NT_LINES=`wc -l $NT_FILEPATH | cut -d ' ' -f1 &` 
        wait

        if [[ $ORIG_LINES = '' || $NT_LINES = '' ]]; then
            printf "\n!! Couldn't obtain line numbers\n"
        fi

        if [[ $ORIG_LINES != $NT_LINES ]]; then
            printf "\n!! Number of lines is not the same:\n\tEdgelist: $ORIG_LINES\n\tntriple-file: $NT_LINES\n"
        else
            echo " $NT_LINES lines both. done"
        fi
    else
        echo "Could not find ntriple-file for $f (I was looking for $NT_FILEPATH)"
    fi

done
