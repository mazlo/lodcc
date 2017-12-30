#!/bin/bash
# 
# given a filename called 'foo.bar', this process will write the data into a file named: 'foo.bar.nt'
#

INPUT_FORMAT="${2:-rdfxml}"
FILE=$1

rapper --quiet --input "$INPUT_FORMAT" --output "ntriples" "$FILE" | sort > "$FILE.nt"