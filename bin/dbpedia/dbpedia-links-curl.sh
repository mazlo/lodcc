#!/bin/bash
linename='dbpedia-links.txt'

while read line; do
    echo "$line"
    FILE=`echo $line | sed 's#^.*/##'`
    EXT_FILE=${FILE%.bz2}
    OUTPUT_FILE=dbpedia-all-en.ttl.nt
   
    # curl and extract if FILE does not exist
    if [ ! -f $FILE ]
    then
        echo "cURLing"
        curl --silent -L $line -O

        # extract if necessary
        if [ ! -f $EXT_FILE ]
        then
            echo "dtrx'ing"
            dtrx $FILE
        fi
    fi
    
    # append to file if exists
    if [ -f $EXT_FILE ]
    then
        echo "appending"
        cat $EXT_FILE >> $OUTPUT_FILE
        
        echo "removing extracted file"
        rm $EXT_FILE
    else
        echo "ERROR: Extracted line $EXT_FILE not found"
    fi
done < $linename
