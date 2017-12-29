#!/bin/bash
INPUT_FORMAT="${2:-rdfxml}"
FILE=$1
rapper --quiet --input "$INPUT_FORMAT" --output "ntriples" "$FILE" | sort > "$FILE.nt"
