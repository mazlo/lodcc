#!/bin/bash
#
# given a filename called 'foo.bar', this process will write the data into a file named: 'foo.bar.csv'
#

INPUT="$1"
OUTPUT="$1.csv"
NO_CACHE=${2:-false}
INPUT_EXT=${3:-".nt"}

# we're reading from a file with INPUT_EXT, so
# add extension if the file does not have it already
if [[ "$INPUT" == "${INPUT%$INPUT_EXT}" ]]; then
    INPUT=$1$INPUT_EXT
fi

file_exists() 
{
    if [ ! -f "$OUTPUT" ]; then
        return 1 # exit failure
    fi

    SIZE=`ls -s "$OUTPUT" | cut -d ' ' -f1`
    if [[ $NO_CACHE = false && $SIZE > 1000 ]]; then
        return 0 # exit success
    fi

    return 1 # exit failure
}

# do not transform when file exists, i.e. file size > 1000
if file_exists; then
    exit 0 # exit success
fi

# paste the content | add a . at the end | as long as there are lines, rewrite their position
cat "$INPUT" | sed 's#.$##' | while read -r s p o; do echo "$s $o {'edge':'$p'}"; done > "$OUTPUT"

