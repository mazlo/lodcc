#!/bin/bash
# 
# given a filename called 'foo.bar', this process will write the data into a file named: 'foo.bar.nt'
#

INPUT_FORMAT="${2:-rdfxml}"
INPUT=$1
OUTPUT="$INPUT.nt"
NO_CACHE=${3:-false}

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

rapper --ignore-errors --quiet --input "$INPUT_FORMAT" --output "ntriples" "$INPUT" > "$OUTPUT"
