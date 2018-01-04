#!/bin/bash
# 
# given a filename called 'foo.bar', this process will write the data into a file named: 'foo.bar.nt'
#

INPUT_FORMAT="${2:-rdfxml}"
FILE=$1

rapper --ignore-errors --quiet --input "$INPUT_FORMAT" --output "ntriples" "$FILE" > "$FILE.nt"